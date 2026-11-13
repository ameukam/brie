// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package checkpoint

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
)

// fakeSandboxes enforces UID and resource-version preconditions like the
// Kubernetes API server. beforeUpdate runs before each update to model a
// concurrent writer.
type fakeSandboxes struct {
	state        *SandboxCheckpointState
	version      int
	updates      int
	beforeUpdate func(*fakeSandboxes)
	getErr       error
	updateErr    error
}

func (f *fakeSandboxes) GetCheckpointState(context.Context, SandboxKey) (SandboxCheckpointState, error) {
	if f.getErr != nil {
		return SandboxCheckpointState{}, f.getErr
	}
	if f.state == nil {
		return SandboxCheckpointState{}, ErrSandboxNotFound
	}
	state := *f.state
	state.ResourceVersion = strconv.Itoa(f.version)
	return state, nil
}

func (f *fakeSandboxes) UpdateCheckpointReference(_ context.Context, _ SandboxKey, preconditions Preconditions, reference Reference) error {
	if f.beforeUpdate != nil {
		f.beforeUpdate(f)
	}
	if f.updateErr != nil {
		return f.updateErr
	}
	if f.state == nil {
		return ErrSandboxNotFound
	}
	if preconditions.UID != f.state.UID || preconditions.ResourceVersion != strconv.Itoa(f.version) {
		return ErrConflict
	}
	committed := reference
	f.state.Committed = &committed
	f.version++
	f.updates++
	return nil
}

// touch models an unrelated concurrent write, such as a status update.
func (f *fakeSandboxes) touch() { f.version++ }

func testReference(generation, epoch int64, operationID string) Reference {
	return Reference{
		Manifest:             ObjectRef{Bucket: "brie-checkpoints", Name: operationID + "/manifest.json", Generation: 100 + generation},
		CheckpointGeneration: generation, PlacementEpoch: epoch, OperationID: operationID,
	}
}

func TestCommitCheckpointReference(t *testing.T) {
	previous := testReference(1, 2, "op-0")
	reference := testReference(2, 3, "op-1")
	suspending := func() *SandboxCheckpointState {
		committed := previous
		return &SandboxCheckpointState{UID: testSandboxUID, PlacementEpoch: 3, SuspendOperationID: "op-1", Committed: &committed}
	}
	for _, tt := range []struct {
		name      string
		sandboxes *fakeSandboxes
		reference Reference
		uid       string
		want      error
		text      string
		updates   int
		committed *Reference
	}{
		{name: "commits", sandboxes: &fakeSandboxes{state: suspending()}, updates: 1, committed: &reference},
		{name: "first checkpoint", sandboxes: &fakeSandboxes{state: func() *SandboxCheckpointState {
			s := suspending()
			s.Committed = nil
			return s
		}()}, updates: 1, committed: &reference},
		{name: "unrelated write is retried", sandboxes: &fakeSandboxes{state: suspending(), beforeUpdate: func(f *fakeSandboxes) {
			if f.updates == 0 && f.version == 0 {
				f.touch()
			}
		}}, updates: 1, committed: &reference},
		{name: "persistent conflicts give up", sandboxes: &fakeSandboxes{state: suspending(), beforeUpdate: (*fakeSandboxes).touch}, want: ErrConflict, committed: &previous},
		{name: "already committed after lost reply", sandboxes: &fakeSandboxes{state: func() *SandboxCheckpointState {
			s := suspending()
			s.Committed = &reference
			return s
		}()}, committed: &reference},
		{name: "already committed but deleting", sandboxes: &fakeSandboxes{state: func() *SandboxCheckpointState {
			s := suspending()
			s.Committed = &reference
			s.Deleting = true
			return s
		}()}, want: ErrSandboxDeleting, committed: &reference},
		{name: "already committed but epoch advanced", sandboxes: &fakeSandboxes{state: func() *SandboxCheckpointState {
			s := suspending()
			s.Committed = &reference
			s.PlacementEpoch = 4
			return s
		}()}, want: ErrStaleEpoch, committed: &reference},
		{name: "already committed but suspension withdrawn", sandboxes: &fakeSandboxes{state: func() *SandboxCheckpointState {
			s := suspending()
			s.Committed = &reference
			s.SuspendOperationID = ""
			return s
		}()}, want: ErrNotSuspending, committed: &reference},
		{name: "already committed but another suspension requested", sandboxes: &fakeSandboxes{state: func() *SandboxCheckpointState {
			s := suspending()
			s.Committed = &reference
			s.SuspendOperationID = "op-2"
			return s
		}()}, want: ErrNotSuspending, committed: &reference},
		{name: "sandbox gone", sandboxes: &fakeSandboxes{}, want: ErrSandboxNotFound},
		{name: "sandbox replaced", sandboxes: &fakeSandboxes{state: suspending()}, uid: "7c9e6679-7425-40de-944b-e07fc1f90ae7", want: ErrSandboxReplaced, committed: &previous},
		{name: "deleting", sandboxes: &fakeSandboxes{state: func() *SandboxCheckpointState {
			s := suspending()
			s.Deleting = true
			return s
		}()}, want: ErrSandboxDeleting, committed: &previous},
		{name: "epoch advanced", sandboxes: &fakeSandboxes{state: func() *SandboxCheckpointState {
			s := suspending()
			s.PlacementEpoch = 4
			return s
		}()}, want: ErrStaleEpoch, committed: &previous},
		{name: "epoch advanced during commit", sandboxes: &fakeSandboxes{state: suspending(), beforeUpdate: func(f *fakeSandboxes) {
			if f.state.PlacementEpoch == 3 {
				f.state.PlacementEpoch = 4
				f.touch()
			}
		}}, want: ErrStaleEpoch, committed: &previous},
		{name: "suspension withdrawn", sandboxes: &fakeSandboxes{state: func() *SandboxCheckpointState {
			s := suspending()
			s.SuspendOperationID = ""
			return s
		}()}, want: ErrNotSuspending, committed: &previous},
		{name: "deleted during commit", sandboxes: &fakeSandboxes{state: suspending(), beforeUpdate: func(f *fakeSandboxes) {
			f.state = nil
		}}, want: ErrSandboxNotFound},
		{name: "newer generation committed", sandboxes: &fakeSandboxes{state: func() *SandboxCheckpointState {
			s := suspending()
			newer := testReference(5, 3, "op-9")
			s.Committed = &newer
			return s
		}()}, want: ErrStaleGeneration},
		{name: "same generation different manifest", sandboxes: &fakeSandboxes{state: func() *SandboxCheckpointState {
			s := suspending()
			other := reference
			other.Manifest.Generation++
			s.Committed = &other
			return s
		}()}, want: ErrStaleGeneration},
		{name: "read failure", sandboxes: &fakeSandboxes{getErr: errors.New("API unavailable")}, text: "read sandbox ns/s: API unavailable"},
		{name: "update failure", sandboxes: &fakeSandboxes{state: suspending(), updateErr: context.DeadlineExceeded}, want: context.DeadlineExceeded, committed: &previous},
		{name: "invalid reference", sandboxes: &fakeSandboxes{state: suspending()}, reference: Reference{}, want: errInvalidReference, committed: &previous},
		{name: "invalid UID", sandboxes: &fakeSandboxes{state: suspending()}, uid: "sandbox", text: "sandbox UID", committed: &previous},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ref := reference
			if tt.name == "invalid reference" {
				ref = tt.reference
			}
			uid := testSandboxUID
			if tt.uid != "" {
				uid = tt.uid
			}
			err := CommitReference(t.Context(), tt.sandboxes, SandboxKey{Namespace: "ns", Name: "s"}, uid, ref)
			switch {
			case tt.want != nil:
				if !errors.Is(err, tt.want) {
					t.Fatalf("error = %v, want %v", err, tt.want)
				}
			case tt.text != "":
				if err == nil || !strings.Contains(err.Error(), tt.text) {
					t.Fatalf("error = %v, want %q", err, tt.text)
				}
			case err != nil:
				t.Fatal(err)
			}
			if tt.sandboxes.updates != tt.updates {
				t.Fatalf("updates = %d, want %d", tt.sandboxes.updates, tt.updates)
			}
			if tt.committed != nil && (tt.sandboxes.state == nil || tt.sandboxes.state.Committed == nil || *tt.sandboxes.state.Committed != *tt.committed) {
				t.Fatalf("committed = %+v, want %+v", tt.sandboxes.state, *tt.committed)
			}
		})
	}
}

func TestCheckpointReferenceValidate(t *testing.T) {
	valid := testReference(1, 1, "op-1")
	for _, mutate := range []func(*Reference){
		func(r *Reference) { r.Manifest.Bucket = "" },
		func(r *Reference) { r.CheckpointGeneration = 0 },
		func(r *Reference) { r.PlacementEpoch = 0 },
		func(r *Reference) { r.OperationID = "" },
	} {
		reference := valid
		mutate(&reference)
		if err := reference.Validate(); !errors.Is(err, errInvalidReference) {
			t.Fatalf("Validate(%+v) = %v", reference, err)
		}
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
}
