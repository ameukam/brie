// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"sync"
)

// Worker bounds for per-Pod reconciliation (section 6.4 of DESIGN.md). Each
// pool has its own CRI client with the same number of call slots, so creation
// work never occupies a stop, delete, or status slot. Image pulls run on
// separate pull workers (execution_pulls_linux.go).
const (
	priorityWorkers = 4
	creationWorkers = 2
)

// podFault is a failure confined to one Pod's inputs. It is published on that
// Pod and does not raise the node-level admission barrier.
type podFault struct {
	reason, message string
}

func (f *podFault) Error() string { return f.reason + ": " + f.message }

// podWorkers runs at most one reconciliation operation per Pod UID on a fixed
// number of goroutines. A pass never waits for a free slot: work that does not
// fit is retried on the next pass, and Pods with an operation in flight are
// skipped until it ends.
type podWorkers struct {
	mu       sync.Mutex
	busy     map[string]bool
	faults   map[string]error
	priority chan struct{}
	creation chan struct{}
	running  sync.WaitGroup
}

func newPodWorkers() *podWorkers {
	return &podWorkers{
		busy: make(map[string]bool), faults: make(map[string]error),
		priority: make(chan struct{}, priorityWorkers), creation: make(chan struct{}, creationWorkers),
	}
}

// busySet returns the UIDs with an operation in flight.
func (w *podWorkers) busySet() map[string]bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	busy := make(map[string]bool, len(w.busy))
	for uid := range w.busy {
		busy[uid] = true
	}
	return busy
}

// start runs task in a free slot of pool. It returns false without running
// task when uid already has an operation in flight or the pool is full.
func (w *podWorkers) start(uid string, pool chan struct{}, pass *sync.WaitGroup, task func() error) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.busy[uid] {
		return false
	}
	select {
	case pool <- struct{}{}:
	default:
		return false
	}
	w.busy[uid] = true
	w.running.Add(1)
	pass.Add(1)
	go func() {
		defer pass.Done()
		defer w.running.Done()
		err := task()
		w.mu.Lock()
		defer w.mu.Unlock()
		delete(w.busy, uid)
		w.setFault(uid, err)
		<-pool
	}()
	return true
}

// setFault records the outcome of the last operation for uid. A nil error
// clears it. The caller holds w.mu.
func (w *podWorkers) setFault(uid string, err error) {
	if err == nil {
		delete(w.faults, uid)
		return
	}
	w.faults[uid] = err
}

// outstanding drops state for UIDs that are no longer present and reports
// whether any unattributed fault remains.
func (w *podWorkers) outstanding(present func(string) bool) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for uid := range w.faults {
		if !present(uid) && !w.busy[uid] {
			delete(w.faults, uid)
		}
	}
	return len(w.faults) != 0
}
