/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	zeroscalev1alpha1 "github.com/lalit-tambe/zeroscale/api/v1alpha1"
	"github.com/lalit-tambe/zeroscale/internal/shared"
)

// ScaleGateReconciler reconciles a ScaleGate object
type ScaleGateReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	StateManager *shared.StateManager
}

// +kubebuilder:rbac:groups=zeroscale.zeroscale.dev,resources=scalegates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=zeroscale.zeroscale.dev,resources=scalegates/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=zeroscale.zeroscale.dev,resources=scalegates/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete

func (r *ScaleGateReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the ScaleGate instance
	sg := &zeroscalev1alpha1.ScaleGate{}
	if err := r.Get(ctx, req.NamespacedName, sg); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Fetch Target Deployment
	targetNN := types.NamespacedName{
		Namespace: req.Namespace,
		Name:      sg.Spec.TargetRef.Name, // Assuming same namespace for v1
	}

	deploy := &appsv1.Deployment{}
	if err := r.Get(ctx, targetNN, deploy); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Target deployment not found", "deployment", targetNN.Name)
			// Requeue in 10s to see if it's created later
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	// Read state
	lastReqTime := r.StateManager.GetLastRequestTime(targetNN)
	currentReplicas := *deploy.Spec.Replicas
	readyReplicas := deploy.Status.ReadyReplicas

	// Update interceptor knowledge
	r.StateManager.SetCurrentReplicas(targetNN, readyReplicas)
	bufferTimeout := time.Duration(sg.Spec.BufferTimeoutSeconds) * time.Second
	if bufferTimeout == 0 {
		bufferTimeout = 60 * time.Second
	}
	r.StateManager.SetBufferTimeout(targetNN, bufferTimeout)

	// State logic
	idleTimeout := time.Duration(sg.Spec.IdleTimeoutSeconds) * time.Second
	timeSinceLastReq := time.Since(lastReqTime)

	// If no request has been seen yet, assume it was just started.
	if lastReqTime.IsZero() {
		lastReqTime = sg.CreationTimestamp.Time
		timeSinceLastReq = time.Since(lastReqTime)
	}

	logger.Info("Reconciling ScaleGate",
		"target", targetNN.Name,
		"currentReplicas", currentReplicas,
		"readyReplicas", readyReplicas,
		"timeSinceLastReq", timeSinceLastReq.String(),
		"idleTimeout", idleTimeout.String(),
		"state", sg.Status.State)

	statusChanged := false
	if sg.Status.CurrentReplicas != currentReplicas {
		sg.Status.CurrentReplicas = currentReplicas
		statusChanged = true
	}

	if !lastReqTime.IsZero() && (sg.Status.LastRequestTime == nil || !sg.Status.LastRequestTime.Time.Equal(lastReqTime)) {
		t := metav1.NewTime(lastReqTime)
		sg.Status.LastRequestTime = &t
		statusChanged = true
	}

	// Scale Logic
	if currentReplicas > 0 && timeSinceLastReq > idleTimeout {
		// Idle Timeout Reached -> Scale to Zero
		logger.Info("Idle timeout reached. Scaling deployment to zero.", "deployment", targetNN.Name)
		zero := int32(0)
		deploy.Spec.Replicas = &zero
		if err := r.Update(ctx, deploy); err != nil {
			logger.Error(err, "Failed to update deployment to zero")
			return ctrl.Result{}, err
		}
		sg.Status.State = "Sleeping"
		sg.Status.CurrentReplicas = 0
		r.StateManager.SetCurrentReplicas(targetNN, 0) // immediate update
		statusChanged = true
		logger.Info("Successfully scaled deployment to zero")

	} else if currentReplicas == 0 && timeSinceLastReq <= idleTimeout {
		// New Request -> Scale Up
		logger.Info("New request detected. Scaling deployment up.", "deployment", targetNN.Name)
		scaledReplicas := max(int32(1), sg.Spec.ScaledReplicas)
		deploy.Spec.Replicas = &scaledReplicas
		if err := r.Update(ctx, deploy); err != nil {
			logger.Error(err, "Failed to scale deployment up")
			return ctrl.Result{}, err
		}
		sg.Status.State = "WakingUp"
		statusChanged = true
		logger.Info("Successfully scaled deployment up")
	} else {
		logger.Info("No scale action required")
	}

	// If it's WakingUp and readyReplicas matches scaledReplicas, transition to Active
	if *deploy.Spec.Replicas > 0 && readyReplicas > 0 && sg.Status.State != "Active" {
		sg.Status.State = "Active"
		statusChanged = true
	}

	if statusChanged {
		if err := r.Status().Update(ctx, sg); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Calculate when we need to wake up to scale down
	// Requeue a bit after idle timeout would expire
	timeUntilIdle := idleTimeout - timeSinceLastReq
	if timeUntilIdle < 0 {
		timeUntilIdle = idleTimeout // Fallback
	}

	// Also poll every 2 seconds if we are scaling up to check readiness,
	// unless we implement the proper watch on Deployment, which we did via SetupWithManager.

	return ctrl.Result{RequeueAfter: timeUntilIdle}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ScaleGateReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Map Deployments back to ScaleGates that reference them
	mapFn := func(ctx context.Context, obj client.Object) []reconcile.Request {
		deploy := obj.(*appsv1.Deployment)

		var scaleGates zeroscalev1alpha1.ScaleGateList
		if err := mgr.GetClient().List(ctx, &scaleGates, client.InNamespace(deploy.GetNamespace())); err != nil {
			return nil
		}

		var reqs []reconcile.Request
		for _, sg := range scaleGates.Items {
			if sg.Spec.TargetRef.Name == deploy.GetName() {
				reqs = append(reqs, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Namespace: sg.Namespace,
						Name:      sg.Name,
					},
				})
			}
		}
		return reqs
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&zeroscalev1alpha1.ScaleGate{}).
		Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(mapFn)).
		Named("scalegate").
		Complete(r)
}
