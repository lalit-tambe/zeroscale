package interceptor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/lalit-tambe/zeroscale/internal/shared"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Proxy is a simple HTTP reverse proxy that updates shared state and blocks cold starts.
type Proxy struct {
	stateManager *shared.StateManager
	client       client.Client
}

// NewProxy creates a new interceptor proxy.
func NewProxy(sm *shared.StateManager, c client.Client) *Proxy {
	return &Proxy{
		stateManager: sm,
		client:       c,
	}
}

// Start runs the HTTP server. This conforms to controller-runtime's Runnable interface.
func (p *Proxy) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("interceptor")

	mux := http.NewServeMux()
	mux.HandleFunc("/", p.handleRequest)

	server := &http.Server{
		Addr:    ":8082", // Hardcoded for simplicity in Phase 1
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		logger.Info("Shutting down interceptor proxy")
		_ = server.Close()
	}()

	logger.Info("Starting interceptor proxy on :8082")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (p *Proxy) handleRequest(w http.ResponseWriter, r *http.Request) {
	// 1. Determine Target
	targetStr := r.URL.Query().Get("target")
	if targetStr == "" {
		targetStr = r.Header.Get("X-ScaleGate-Target")
	}

	if targetStr == "" {
		http.Error(w, "Missing target information in 'target' query param or 'X-ScaleGate-Target' header", http.StatusBadRequest)
		return
	}

	parts := strings.Split(targetStr, "/")
	if len(parts) != 2 {
		http.Error(w, "Target must be in format <namespace>/<name>", http.StatusBadRequest)
		return
	}

	targetNN := types.NamespacedName{
		Namespace: parts[0],
		Name:      parts[1],
	}

	// 2. Record request time to wake up or keep awake
	p.stateManager.RecordRequest(targetNN)

	// 3. Check readiness
	replicas := p.stateManager.GetCurrentReplicas(targetNN)
	ctrlLog := log.Log.WithName("proxy")
	ctrlLog.Info("Handling request", "target", targetNN.String(), "currentReplicas", replicas)

	if replicas == 0 {
		ctrlLog.Info("Target is scaled to zero, triggering wakeup", "target", targetNN.String())
		// Signal controller to wake up by annotating the target Deployment
		go func() {
			deploy := &appsv1.Deployment{}
			if err := p.client.Get(context.Background(), targetNN, deploy); err == nil {
				if deploy.Annotations == nil {
					deploy.Annotations = make(map[string]string)
				}
				deploy.Annotations["zeroscale.dev/wakeup"] = time.Now().String()
				if err := p.client.Update(context.Background(), deploy); err != nil {
					ctrlLog.Error(err, "Failed to annotate deployment for wakeup", "target", targetNN.String())
				} else {
					ctrlLog.Info("Successfully annotated deployment to trigger controller wakeup", "target", targetNN.String())
				}
			} else {
				ctrlLog.Error(err, "Failed to get deployment for wakeup", "target", targetNN.String())
			}
		}()

		// Wait for pod to become ready
		bufferTimeout := p.stateManager.GetBufferTimeout(targetNN)
		ctrlLog.Info("Buffering request while waiting for target to wake up", "target", targetNN.String(), "timeout", bufferTimeout.String())

		ctx, cancel := context.WithTimeout(r.Context(), bufferTimeout)
		defer cancel()

		ready := false
		pollTicker := time.NewTicker(200 * time.Millisecond)
		defer pollTicker.Stop()

	WaitLoop:
		for {
			select {
			case <-ctx.Done():
				break WaitLoop
			case <-pollTicker.C:
				if p.stateManager.GetCurrentReplicas(targetNN) > 0 {
					ready = true
					break WaitLoop
				}
			}
		}

		if !ready {
			ctrlLog.Error(ctx.Err(), "Timeout waiting for target to wake up", "target", targetNN.String())
			http.Error(w, "Gateway Timeout: Service failed to start in time.", http.StatusGatewayTimeout)
			return
		}

		// Add a slight delay to allow Kubernetes to propagate the Endpoint to kube-proxy
		// This prevents "connection refused" if we proxy the exact millisecond ReadyReplicas > 0
		time.Sleep(1500 * time.Millisecond)

		ctrlLog.Info("Target is now ready, proceeding to proxy", "target", targetNN.String())
	}

	// 4. Proxy to actual service if replicas > 0
	// Target URL inside Kubernetes: http://<service>.<namespace>.svc.cluster.local:<port>
	// Note: We assume the target Service name matches the target Deployment name for this v1
	port := r.URL.Query().Get("port")
	if port == "" {
		port = r.Header.Get("X-ScaleGate-Port")
	}
	if port == "" {
		port = "80" // default HTTP port
	}

	targetURLStr := fmt.Sprintf("http://%s.%s.svc.cluster.local:%s", targetNN.Name, targetNN.Namespace, port)
	targetURL, err := url.Parse(targetURLStr)
	if err != nil {
		http.Error(w, "Failed to parse target URL", http.StatusInternalServerError)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	// We need to strip the 'target' query param or header so it doesn't get passed to the backend?
	// Or we can just leave it. Leaving it is simpler for now.

	proxy.ServeHTTP(w, r)
}
