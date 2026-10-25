// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package owner

import (
	"encoding/json"
	"net/http"
	"reflect"
	"sync"
	"testing"

	"github.com/ameukam/brie/internal/runtime/internal/firecracker"
)

func TestBootWithVsock(t *testing.T) {
	for _, outcome := range []string{"success", "API failure", "lost reply"} {
		t.Run(outcome, func(t *testing.T) {
			o, _ := startOwner(t)
			var mu sync.Mutex
			var calls []apiRequest
			client := bootClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				mu.Lock()
				calls = append(calls, apiRequest{method: r.Method, path: r.URL.Path, body: body})
				mu.Unlock()
				if r.URL.Path == "/vsock" {
					assertJournalState(t, o.journal.path, Running, BootConfiguringVsock)
					switch outcome {
					case "API failure":
						w.WriteHeader(http.StatusInternalServerError)
						return
					case "lost reply":
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						if err := conn.Close(); err != nil {
							t.Error(err)
						}
						return
					}
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			config := validBootConfig()
			config.Vsock = &firecracker.VsockConfig{GuestCID: 3, UDSPath: "/private/vsock.sock"}
			err := o.Boot(t.Context(), client, config)
			if (err == nil) != (outcome == "success") {
				t.Fatalf("Boot() = %v", err)
			}
			expected := expectedBootRequests()
			start := expected[len(expected)-1]
			expected = append(expected[:len(expected)-1], apiRequest{
				method: http.MethodPut, path: "/vsock",
				body: map[string]any{"guest_cid": float64(3), "uds_path": "/private/vsock.sock"},
			})
			if outcome == "success" {
				expected = append(expected, start)
				assertJournalState(t, o.journal.path, Running, BootStartAcknowledged)
			} else {
				assertJournalState(t, o.journal.path, Running, BootUnknown)
			}
			if err := o.Boot(t.Context(), client, config); err == nil {
				t.Fatal("boot replay accepted")
			}
			mu.Lock()
			defer mu.Unlock()
			if !reflect.DeepEqual(calls, expected) {
				t.Fatalf("requests = %+v, want %+v", calls, expected)
			}
		})
	}
}

func TestBootRejectsInvalidVsockBeforeMutation(t *testing.T) {
	client := bootClient(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("invalid vsock profile sent an API request")
	}))
	o, _ := startOwner(t)
	config := validBootConfig()
	config.Vsock = &firecracker.VsockConfig{GuestCID: 2, UDSPath: "/vsock"}
	if err := o.Boot(t.Context(), client, config); err == nil {
		t.Fatal("invalid vsock accepted")
	}
	assertJournalState(t, o.journal.path, Running, BootUnconfigured)
}
