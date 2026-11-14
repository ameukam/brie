// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package execinventory

import (
	"errors"
	"testing"
)

func TestStore(t *testing.T) {
	t.Parallel()
	web := Execution{PodUID: "uid-1", Namespace: "team", Name: "web", SandboxID: "s1", ContainerName: "app", ContainerID: "c1"}
	for _, tt := range []struct {
		name       string
		executions []Execution
		publish    bool
		namespace  string
		pod        string
		want       Execution
		err        error
	}{
		{name: "unpublished", namespace: "team", pod: "web", err: ErrUnavailable},
		{name: "empty", publish: true, namespace: "team", pod: "web", err: ErrNotFound},
		{name: "found", publish: true, executions: []Execution{web}, namespace: "team", pod: "web", want: web},
		{name: "other namespace", publish: true, executions: []Execution{web}, namespace: "other", pod: "web", err: ErrNotFound},
		{name: "duplicate name", publish: true, namespace: "team", pod: "web", err: ErrAmbiguous,
			executions: []Execution{web, {PodUID: "uid-2", Namespace: "team", Name: "web", SandboxID: "s2"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var store Store
			if _, err := store.Executions(); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("Executions before publication = %v", err)
			}
			if tt.publish {
				store.Set(tt.executions)
				if len(tt.executions) > 0 {
					tt.executions[0].SandboxID = "mutated"
				}
				all, err := store.Executions()
				if err != nil || len(all) != len(tt.executions) {
					t.Fatalf("Executions = %v, %v", all, err)
				}
			}
			got, err := store.Lookup(tt.namespace, tt.pod)
			if !errors.Is(err, tt.err) || got != tt.want {
				t.Fatalf("Lookup = %+v, %v, want %+v, %v", got, err, tt.want, tt.err)
			}
		})
	}
}
