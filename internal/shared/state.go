package shared

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// TargetState holds the dynamic proxy-tracked state for a given target.
type TargetState struct {
	LastRequestTime time.Time
	CurrentReplicas int32
	BufferTimeout   time.Duration
}

// StateManager manages thread-safe access to TargetState.
type StateManager struct {
	mu     sync.RWMutex
	states map[types.NamespacedName]*TargetState
}

// NewStateManager creates a new StateManager.
func NewStateManager() *StateManager {
	return &StateManager{
		states: make(map[types.NamespacedName]*TargetState),
	}
}

// RecordRequest updates the last request time for a given target.
func (s *StateManager) RecordRequest(target types.NamespacedName) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.states[target]; !ok {
		s.states[target] = &TargetState{}
	}
	s.states[target].LastRequestTime = time.Now()
}

// SetCurrentReplicas updates the known replicas for a target.
func (s *StateManager) SetCurrentReplicas(target types.NamespacedName, replicas int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.states[target]; !ok {
		s.states[target] = &TargetState{}
	}
	s.states[target].CurrentReplicas = replicas
}

// SetBufferTimeout updates the configured buffer timeout for a target.
func (s *StateManager) SetBufferTimeout(target types.NamespacedName, timeout time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.states[target]; !ok {
		s.states[target] = &TargetState{}
	}
	s.states[target].BufferTimeout = timeout
}

// GetLastRequestTime retrieves the last request time for a target.
// Returns a zero time value if no requests have been recorded.
func (s *StateManager) GetLastRequestTime(target types.NamespacedName) time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if state, ok := s.states[target]; ok {
		return state.LastRequestTime
	}
	return time.Time{}
}

// GetCurrentReplicas retrieves the current replicas for a target.
func (s *StateManager) GetCurrentReplicas(target types.NamespacedName) int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if state, ok := s.states[target]; ok {
		return state.CurrentReplicas
	}
	return 0
}

// GetBufferTimeout retrieves the configured buffer timeout for a target.
func (s *StateManager) GetBufferTimeout(target types.NamespacedName) time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if state, ok := s.states[target]; ok {
		return state.BufferTimeout
	}
	return 60 * time.Second // Default fallback
}
