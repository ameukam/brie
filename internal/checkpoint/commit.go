// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package checkpoint

import (
	"context"
	"errors"
	"fmt"

	"github.com/ameukam/brie/internal/lifecycleid"
)

// maxCommitAttempts bounds re-reads after resource-version conflicts.
const maxCommitAttempts = 3

// Errors from CommitReference. Only ErrConflict is transient.
var (
	ErrConflict         = errors.New("sandbox changed since it was read")
	ErrSandboxNotFound  = errors.New("sandbox not found")
	ErrSandboxReplaced  = errors.New("sandbox UID differs from the checkpointed sandbox")
	ErrSandboxDeleting  = errors.New("sandbox is being deleted")
	ErrStaleEpoch       = errors.New("checkpoint placement epoch is not the sandbox's current epoch")
	ErrNotSuspending    = errors.New("sandbox no longer requests suspension by this operation")
	ErrStaleGeneration  = errors.New("checkpoint generation is not newer than the committed reference")
	errInvalidReference = errors.New("invalid checkpoint reference")
)

// SandboxKey names a Sandbox object.
type SandboxKey struct {
	Namespace string
	Name      string
}

// Reference is the committed-checkpoint reference a Sandbox
// records: the published manifest generation and the operation that made it.
type Reference struct {
	Manifest             ObjectRef
	CheckpointGeneration int64
	PlacementEpoch       int64
	OperationID          string
}

// SandboxCheckpointState is the part of a Sandbox that the commit checks.
// SuspendOperationID is the desired suspension the controller recorded
// before the operation started (DESIGN 5.2 step 1); it is empty when no
// suspension is requested.
type SandboxCheckpointState struct {
	UID                string
	ResourceVersion    string
	Deleting           bool
	PlacementEpoch     int64
	SuspendOperationID string
	Committed          *Reference
}

// Preconditions are the object identity and version an update requires.
type Preconditions struct {
	UID             string
	ResourceVersion string
}

// SandboxCheckpointClient reads and conditionally updates a Sandbox's
// committed-checkpoint reference. The Sandbox controller owns the real
// implementation, which must send both preconditions to the API server so
// that a replaced or concurrently modified Sandbox rejects the write.
type SandboxCheckpointClient interface {
	// GetCheckpointState returns ErrSandboxNotFound when the Sandbox is gone.
	GetCheckpointState(ctx context.Context, key SandboxKey) (SandboxCheckpointState, error)
	// UpdateCheckpointReference returns ErrConflict when the UID or resource
	// version no longer match and ErrSandboxNotFound when the Sandbox is gone.
	UpdateCheckpointReference(ctx context.Context, key SandboxKey, preconditions Preconditions, reference Reference) error
}

// Validate checks the reference fields.
func (r Reference) Validate() error {
	if err := r.Manifest.Validate(); err != nil {
		return fmt.Errorf("%w: manifest %w", errInvalidReference, err)
	}
	if r.CheckpointGeneration <= 0 {
		return fmt.Errorf("%w: checkpoint generation must be positive", errInvalidReference)
	}
	if err := lifecycleid.Epoch("placement epoch", r.PlacementEpoch); err != nil {
		return fmt.Errorf("%w: %w", errInvalidReference, err)
	}
	if err := lifecycleid.Token("operation ID", r.OperationID); err != nil {
		return fmt.Errorf("%w: %w", errInvalidReference, err)
	}
	return nil
}

// CommitReference makes reference the Sandbox's committed
// checkpoint (DESIGN 5.2 step 5). It writes only if the Sandbox still has
// sandboxUID, is not being deleted, is at the reference's placement epoch,
// still requests suspension by the reference's operation, and has no newer
// committed generation. The write carries the UID and resource version that
// were checked; a conflict re-reads and re-checks, at most three times.
//
// A retry after a lost reply succeeds without writing if the reference is
// already committed. A rejected commit leaves the previous checkpoint
// authoritative and the uploaded objects unreferenced.
func CommitReference(ctx context.Context, client SandboxCheckpointClient, key SandboxKey, sandboxUID string, reference Reference) error {
	if err := lifecycleid.UID("sandbox UID", sandboxUID); err != nil {
		return err
	}
	if err := reference.Validate(); err != nil {
		return err
	}
	for attempt := 0; attempt < maxCommitAttempts; attempt++ {
		state, err := client.GetCheckpointState(ctx, key)
		if err != nil {
			return fmt.Errorf("read sandbox %s/%s: %w", key.Namespace, key.Name, err)
		}
		if state.UID != sandboxUID {
			return ErrSandboxReplaced
		}
		if state.Committed != nil && *state.Committed == reference {
			return nil
		}
		switch {
		case state.Deleting:
			return ErrSandboxDeleting
		case state.PlacementEpoch != reference.PlacementEpoch:
			return ErrStaleEpoch
		case state.SuspendOperationID != reference.OperationID:
			return ErrNotSuspending
		case state.Committed != nil && state.Committed.CheckpointGeneration >= reference.CheckpointGeneration:
			return ErrStaleGeneration
		}
		err = client.UpdateCheckpointReference(ctx, key,
			Preconditions{UID: state.UID, ResourceVersion: state.ResourceVersion}, reference)
		if !errors.Is(err, ErrConflict) {
			if err != nil {
				return fmt.Errorf("update sandbox %s/%s: %w", key.Namespace, key.Name, err)
			}
			return nil
		}
	}
	return fmt.Errorf("update sandbox %s/%s after %d attempts: %w", key.Namespace, key.Name, maxCommitAttempts, ErrConflict)
}
