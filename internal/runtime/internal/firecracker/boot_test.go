// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package firecracker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBootRequests(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		path string
		want string
		call func(context.Context, *Client) error
	}{
		{
			name: "kernel and arguments", path: "/boot-source",
			want: `{"kernel_image_path":"/kernel","boot_args":"console=ttyS0 root=/dev/vda ro"}`,
			call: func(ctx context.Context, c *Client) error {
				return c.ConfigureBootSource(ctx, BootSource{
					KernelImagePath: "/kernel", BootArgs: "console=ttyS0 root=/dev/vda ro",
				})
			},
		},
		{
			name: "JSON escaping", path: "/boot-source",
			want: `{"kernel_image_path":"/kernel \"test\"","boot_args":"test=\"a\\b\"\n"}`,
			call: func(ctx context.Context, c *Client) error {
				return c.ConfigureBootSource(ctx, BootSource{
					KernelImagePath: `/kernel "test"`, BootArgs: "test=\"a\\b\"\n",
				})
			},
		},
		{
			name: "empty arguments", path: "/boot-source",
			want: `{"kernel_image_path":"/kernel","boot_args":""}`,
			call: func(ctx context.Context, c *Client) error {
				return c.ConfigureBootSource(ctx, BootSource{KernelImagePath: "/kernel"})
			},
		},
		{
			name: "read only root", path: "/drives/rootfs",
			want: `{"drive_id":"rootfs","path_on_host":"/disk.ext4","is_root_device":true,"is_read_only":true,"cache_type":"Writeback","io_engine":"Sync"}`,
			call: func(ctx context.Context, c *Client) error {
				return c.ConfigureRootDrive(ctx, RootDrive{PathOnHost: "/disk.ext4", ReadOnly: true})
			},
		},
		{
			name: "writable root with literal path", path: "/drives/rootfs",
			want: `{"drive_id":"rootfs","path_on_host":"/disk ?#%.ext4","is_root_device":true,"is_read_only":false,"cache_type":"Writeback","io_engine":"Sync"}`,
			call: func(ctx context.Context, c *Client) error {
				return c.ConfigureRootDrive(ctx, RootDrive{PathOnHost: "/disk ?#%.ext4"})
			},
		},
		{
			name: "start", path: "/actions",
			want: `{"action_type":"InstanceStart"}`,
			call: func(ctx context.Context, c *Client) error {
				return c.Start(ctx)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			socket := socketServer(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodPut || r.URL.RequestURI() != tt.path ||
					r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("unexpected request: %s %s headers=%v", r.Method, r.URL, r.Header)
				}
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				var got, want map[string]any
				if err := json.Unmarshal(body, &got); err != nil {
					t.Error(err)
				}
				if err := json.Unmarshal([]byte(tt.want), &want); err != nil {
					t.Error(err)
					return
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("body = %s; want %s", body, tt.want)
				}
				w.WriteHeader(http.StatusNoContent)
			})
			if err := tt.call(t.Context(), testClient(t, socket, time.Second)); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 {
				t.Errorf("calls = %d; want 1", calls.Load())
			}
		})
	}
}

func TestInvalidBootConfiguration(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	socket := socketServer(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})
	client := testClient(t, socket, time.Second)
	for _, path := range []string{"", "relative", "/kernel\x00image", "/kernel\xffimage"} {
		t.Run(fmt.Sprintf("path %q", path), func(t *testing.T) {
			if err := client.ConfigureBootSource(t.Context(), BootSource{KernelImagePath: path}); err == nil {
				t.Error("invalid kernel path accepted")
			}
			if err := client.ConfigureRootDrive(t.Context(), RootDrive{PathOnHost: path}); err == nil {
				t.Error("invalid drive path accepted")
			}
		})
	}
	for _, args := range []string{"console=ttyS0\x00init=/bin/sh", "console=\xff"} {
		if err := client.ConfigureBootSource(t.Context(), BootSource{
			KernelImagePath: "/kernel", BootArgs: args,
		}); err == nil {
			t.Errorf("invalid boot arguments accepted: %q", args)
		}
	}
	if calls.Load() != 0 {
		t.Errorf("invalid configuration sent %d requests", calls.Load())
	}
}

func bootOperations() map[string]func(context.Context, *Client) error {
	return map[string]func(context.Context, *Client) error{
		"vsock": func(ctx context.Context, c *Client) error {
			return c.ConfigureVsock(ctx, VsockConfig{GuestCID: 3, UDSPath: "/vsock.sock"})
		},
		"boot source": func(ctx context.Context, c *Client) error {
			return c.ConfigureBootSource(ctx, BootSource{KernelImagePath: "/kernel"})
		},
		"root drive": func(ctx context.Context, c *Client) error {
			return c.ConfigureRootDrive(ctx, RootDrive{PathOnHost: "/rootfs"})
		},
		"start": func(ctx context.Context, c *Client) error { return c.Start(ctx) },
	}
}

func TestBootFailures(t *testing.T) {
	t.Parallel()
	for name, call := range bootOperations() {
		t.Run(name, func(t *testing.T) {
			for _, status := range []int{http.StatusBadRequest, http.StatusInternalServerError, http.StatusAccepted} {
				t.Run(fmt.Sprint(status), func(t *testing.T) {
					var calls atomic.Int32
					socket := socketServer(t, func(w http.ResponseWriter, _ *http.Request) {
						calls.Add(1)
						w.WriteHeader(status)
						if _, err := fmt.Fprint(w, `{"fault_message":"rejected"}`); err != nil {
							t.Error(err)
						}
					})
					err := call(t.Context(), testClient(t, socket, time.Second))
					var apiErr *APIError
					if !errors.As(err, &apiErr) || apiErr.StatusCode != status || apiErr.FaultMessage != "rejected" {
						t.Fatalf("error = %v; want HTTP %d fault", err, status)
					}
					if calls.Load() != 1 {
						t.Errorf("calls = %d; want 1", calls.Load())
					}
				})
			}
			t.Run("canceled", func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				client := testClient(t, filepath.Join(t.TempDir(), "unused.sock"), time.Second)
				if err := call(ctx, client); !errors.Is(err, context.Canceled) {
					t.Fatalf("error = %v; want context.Canceled", err)
				}
			})
			t.Run("lost reply", func(t *testing.T) {
				var calls atomic.Int32
				socket := socketServer(t, func(w http.ResponseWriter, _ *http.Request) {
					calls.Add(1)
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					if err := conn.Close(); err != nil {
						t.Error(err)
					}
				})
				if err := call(t.Context(), testClient(t, socket, time.Second)); err == nil {
					t.Fatal("lost reply returned success")
				}
				if calls.Load() != 1 {
					t.Errorf("mutation retried: %d calls", calls.Load())
				}
			})
			t.Run("deadline", func(t *testing.T) {
				socket := socketServer(t, func(_ http.ResponseWriter, r *http.Request) {
					// Read the PUT body so net/http can observe peer cancellation.
					if _, err := io.Copy(io.Discard, r.Body); err != nil {
						t.Error(err)
					}
					<-r.Context().Done()
				})
				if err := call(t.Context(), testClient(t, socket, 50*time.Millisecond)); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("error = %v; want DeadlineExceeded", err)
				}
			})
		})
	}
}

func FuzzArtifactPath(f *testing.F) {
	for _, seed := range []string{"", "/", "/kernel", "relative", "/disk ?#%.ext4", "/kernel\x00image", "/kernel\xffimage"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, path string) {
		valid := validArtifactPath(path)
		if valid && (!filepath.IsAbs(path) || strings.ContainsRune(path, '\x00')) {
			t.Fatalf("accepted invalid artifact path %q", path)
		}
		if valid {
			body, err := json.Marshal(BootSource{KernelImagePath: path})
			if err != nil {
				t.Fatal(err)
			}
			var decoded BootSource
			if err := json.Unmarshal(body, &decoded); err != nil {
				t.Fatal(err)
			}
			// encoding/json replaces invalid UTF-8; valid paths must round-trip.
			if decoded.KernelImagePath != path {
				t.Fatalf("artifact path changed during JSON encoding: %q -> %q", path, decoded.KernelImagePath)
			}
		}
	})
}
