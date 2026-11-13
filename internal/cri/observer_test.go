// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package cri

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestObserver(t *testing.T) {
	t.Parallel()
	fake := &streamingRuntime{url: "http://127.0.0.1/x"}
	client := streamingClient(t, fake)
	var mu sync.Mutex
	var methods []string
	var failures int
	client.SetObserver(func(method string, duration time.Duration, err error) {
		mu.Lock()
		defer mu.Unlock()
		methods = append(methods, method)
		if duration < 0 {
			t.Error("negative duration")
		}
		if err != nil {
			failures++
		}
	})
	if err := client.ReopenContainerLog(context.Background(), "c"); err != nil {
		t.Fatal(err)
	}
	fake.failure = status.Error(codes.Unavailable, "down")
	if err := client.ReopenContainerLog(context.Background(), "c"); err == nil {
		t.Fatal("expected error")
	}
	client.SetObserver(nil)
	if err := client.ReopenContainerLog(context.Background(), "c"); err == nil {
		t.Fatal("expected error")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(methods) != 2 || methods[0] != "ReopenContainerLog" || failures != 1 {
		t.Fatalf("observed %v with %d failures", methods, failures)
	}
}
