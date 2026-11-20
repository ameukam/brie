// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"math/rand/v2"
	"sync"
	"time"
)

// Worker bounds for per-Pod reconciliation (section 6.4 of DESIGN.md). Each
// pool has its own CRI client with the same number of call slots, so creation
// work, including image pulls, never occupies a stop, delete, or status slot.
const (
	priorityWorkers = 4
	creationWorkers = 2
)

// Image pull backoff for a Pod whose pull failed. The delay doubles from the
// base up to the limit, with up to 50% added jitter.
const (
	pullBackoffBase  = 10 * time.Second
	pullBackoffLimit = 5 * time.Minute
)

// podFault is a failure confined to one Pod's inputs. It is published on that
// Pod and does not raise the node-level admission barrier.
type podFault struct {
	reason, message string
}

func (f *podFault) Error() string { return f.reason + ": " + f.message }

type pullBackoff struct {
	failures int
	next     time.Time
}

// podWorkers runs at most one reconciliation operation per Pod UID on a fixed
// number of goroutines. A pass never waits for a free slot: work that does not
// fit is retried on the next pass, and Pods with an operation in flight are
// skipped until it ends.
type podWorkers struct {
	mu       sync.Mutex
	busy     map[string]bool
	faults   map[string]error
	pulls    map[string]pullBackoff
	priority chan struct{}
	creation chan struct{}
	running  sync.WaitGroup
}

func newPodWorkers() *podWorkers {
	return &podWorkers{
		busy: make(map[string]bool), faults: make(map[string]error), pulls: make(map[string]pullBackoff),
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
	for uid := range w.pulls {
		if !present(uid) {
			delete(w.pulls, uid)
		}
	}
	return len(w.faults) != 0
}

// pullAllowed reports whether a pull for uid may be issued at now.
func (w *podWorkers) pullAllowed(uid string, now time.Time) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return !now.Before(w.pulls[uid].next)
}

// pullFailed schedules the next pull attempt for uid and returns its delay.
func (w *podWorkers) pullFailed(uid string, now time.Time) time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	backoff := w.pulls[uid]
	delay := pullBackoffBase << min(backoff.failures, 5)
	delay = min(delay, pullBackoffLimit)
	delay += rand.N(delay/2 + 1)
	backoff.failures++
	backoff.next = now.Add(delay)
	w.pulls[uid] = backoff
	return delay
}

// pullSucceeded clears the backoff for uid.
func (w *podWorkers) pullSucceeded(uid string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.pulls, uid)
}
