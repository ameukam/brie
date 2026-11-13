// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package execinventory holds the executor-verified execution inventory. The
// executor publishes, after each pass that reads the runtime, only the
// executions whose journal record, owner labels, and runtime objects agree.
// Serving and log rotation touch only these runtime objects, never a sandbox
// merely because it carries an owner label.
package execinventory

import (
	"errors"
	"slices"
	"sync"
)

// Execution is one verified execution and its runtime objects.
type Execution struct {
	PodUID, Namespace, Name string
	SandboxID               string
	// ContainerName and ContainerID are empty until the container is verified.
	ContainerName, ContainerID string
}

// Lookup errors.
var (
	ErrUnavailable = errors.New("the executor has not published a verified inventory")
	ErrNotFound    = errors.New("no verified execution")
	ErrAmbiguous   = errors.New("several verified executions share the Pod name")
)

// Store is the latest published inventory. The zero value is empty and
// unavailable.
type Store struct {
	mu         sync.RWMutex
	published  bool
	executions []Execution
}

// Set replaces the inventory with a copy of executions.
func (s *Store) Set(executions []Execution) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.published, s.executions = true, slices.Clone(executions)
}

// Lookup returns the single verified execution of namespace/name. Several
// executions with that name are refused as ambiguous, never resolved by age.
func (s *Store) Lookup(namespace, name string) (Execution, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.published {
		return Execution{}, ErrUnavailable
	}
	var found []Execution
	for _, execution := range s.executions {
		if execution.Namespace == namespace && execution.Name == name {
			found = append(found, execution)
		}
	}
	switch len(found) {
	case 0:
		return Execution{}, ErrNotFound
	case 1:
		return found[0], nil
	}
	return Execution{}, ErrAmbiguous
}

// Executions returns a copy of the inventory, or ErrUnavailable before the
// first publication.
func (s *Store) Executions() ([]Execution, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.published {
		return nil, ErrUnavailable
	}
	return slices.Clone(s.executions), nil
}
