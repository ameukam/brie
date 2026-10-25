// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package firecracker

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfigureVsock(t *testing.T) {
	var calls atomic.Int32
	socket := socketServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if r.Method != http.MethodPut || r.URL.Path != "/vsock" ||
			len(body) != 2 || body["guest_cid"] != float64(3) || body["uds_path"] != "/control.sock" {
			t.Errorf("unexpected vsock request: %s %s %+v", r.Method, r.URL, body)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	if err := testClient(t, socket, time.Second).ConfigureVsock(t.Context(), VsockConfig{
		GuestCID: 3, UDSPath: "/control.sock",
	}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("request count = %d", calls.Load())
	}
}

func TestVsockValidation(t *testing.T) {
	for _, tt := range []struct {
		name string
		cid  uint32
		path string
		ok   bool
	}{
		{name: "minimum", cid: 3, path: "/vsock", ok: true},
		{name: "maximum CID", cid: ^uint32(0) - 1, path: "/vsock", ok: true},
		{name: "maximum path", cid: 3, path: "/" + strings.Repeat("a", 95), ok: true},
		{name: "hypervisor CID", cid: 0, path: "/vsock"},
		{name: "local CID", cid: 1, path: "/vsock"},
		{name: "host CID", cid: 2, path: "/vsock"},
		{name: "any CID", cid: ^uint32(0), path: "/vsock"},
		{name: "empty path", cid: 3},
		{name: "relative path", cid: 3, path: "vsock"},
		{name: "NUL path", cid: 3, path: "/vsock\x00"},
		{name: "invalid UTF-8", cid: 3, path: "/vsock\xff"},
		{name: "long path", cid: 3, path: "/" + strings.Repeat("a", 96)},
		{name: "byte length", cid: 3, path: "/" + strings.Repeat("\u00e9", 48)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			config := VsockConfig{GuestCID: tt.cid, UDSPath: tt.path}
			if err := config.Validate(); (err == nil) != tt.ok {
				t.Fatalf("Validate() = %v, want valid=%t", err, tt.ok)
			}
			if !tt.ok {
				client := testClient(t, "/unused.sock", time.Second)
				if err := client.ConfigureVsock(t.Context(), config); err == nil ||
					!strings.Contains(err.Error(), "vsock") {
					t.Fatalf("invalid configuration reached transport: %v", err)
				}
			}
		})
	}
}

func FuzzVsockConfig(f *testing.F) {
	f.Add(uint32(3), "/vsock.sock")
	f.Add(^uint32(0), "/vsock\x00")
	f.Fuzz(func(t *testing.T, cid uint32, path string) {
		config := VsockConfig{GuestCID: cid, UDSPath: path}
		if config.Validate() == nil {
			if cid < 3 || cid == ^uint32(0) || !validArtifactPath(path) || len(path) > 96 {
				t.Fatalf("invalid vsock configuration accepted: %+v", config)
			}
		}
	})
}
