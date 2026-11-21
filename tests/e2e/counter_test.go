//go:build e2e

// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

type counterStatus struct {
	Count     uint64 `json:"count"`
	Instance  string `json:"instance"`
	Admitting bool   `json:"admitting"`
	Identity  struct {
		SandboxUID string `json:"sandbox_uid"`
		PodUID     string `json:"pod_uid"`
		Epoch      int64  `json:"epoch"`
	} `json:"identity"`
}

// startCounter runs brie-counter until the test ends or stop is called.
func startCounter(t *testing.T, binary, address, state, socket string) (stop func()) {
	t.Helper()
	cmd := exec.Command(binary, "--listen="+address, "--state="+state, "--supervisor-socket="+socket)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Errorf("signal counter: %v", err)
		}
		if err := cmd.Wait(); err != nil {
			t.Errorf("counter exit: %v", err)
		}
	}
	t.Cleanup(stop)
	deadline := time.Now().Add(20 * time.Second)
	for {
		response, err := http.Get("http://" + address + "/readyz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return stop
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("counter did not become ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

func increment(t *testing.T, address string) int {
	t.Helper()
	response, err := http.Post("http://"+address+"/increment", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	return response.StatusCode
}

func readStatus(t *testing.T, address string) counterStatus {
	t.Helper()
	response, err := http.Get("http://" + address + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	var status counterStatus
	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func guestctl(t *testing.T, binary, socket, request string) string {
	t.Helper()
	code, stdout, stderr := runCommand(t, binary, "--socket="+socket, request)
	if code != 0 {
		t.Fatalf("guestctl exit=%d stderr=%q", code, stderr)
	}
	return strings.TrimSuffix(stdout, "\n")
}

// TestCounterPrepareResume drives the sample workload through the same
// guestctl relay the host runs through CRI ExecSync. It runs both binaries
// as local processes; it does not involve Kata, a VM, or a snapshot.
func TestCounterPrepareResume(t *testing.T) {
	counter := buildCommand(t, "brie-counter")
	ctl := buildCommand(t, "brie-guestctl")
	dir, err := os.MkdirTemp("", "e2e")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	state, socket, address := filepath.Join(dir, "counter.json"), filepath.Join(dir, "s.sock"), freeAddress(t)
	stop := startCounter(t, counter, address, state, socket)

	for range 3 {
		if code := increment(t, address); code != http.StatusOK {
			t.Fatalf("increment status = %d", code)
		}
	}
	before := readStatus(t, address)

	deadline := time.Now().UTC().Add(30 * time.Second).Truncate(time.Second).Format(time.RFC3339)
	prepare := fmt.Sprintf(`{"version":"brie.guestsupervisor/v1","operation":"prepare","prepare":{"operation_id":"op-1","epoch":1,"deadline":%q}}`, deadline)
	want := `{"version":"brie.guestsupervisor/v1","outcome":"safe_point","operation_id":"op-1","safe_point":{"id":"counter-3","external_effect_watermark":3}}`
	if got := guestctl(t, ctl, socket, prepare); got != want {
		t.Fatalf("prepare reply = %s, want %s", got, want)
	}
	if code := increment(t, address); code != http.StatusServiceUnavailable {
		t.Fatalf("increment while prepared = %d", code)
	}

	wallClock := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
	resume := fmt.Sprintf(`{"version":"brie.guestsupervisor/v1","operation":"resume","resume":{"operation_id":"op-1","sandbox_uid":"0f8fad5b-d9cb-469f-a165-70867728950e","pod_uid":"7c9e6679-7425-40de-944b-e07fc1f90ae7","epoch":2,"wall_clock":%q,"entropy_nonce":"00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"}}`, wallClock)
	if got := guestctl(t, ctl, socket, resume); got != `{"version":"brie.guestsupervisor/v1","outcome":"ready","operation_id":"op-1"}` {
		t.Fatalf("resume reply = %s", got)
	}
	after := readStatus(t, address)
	if !after.Admitting || after.Instance == before.Instance || after.Identity.PodUID != "7c9e6679-7425-40de-944b-e07fc1f90ae7" || after.Identity.Epoch != 2 {
		t.Fatalf("status after resume = %+v, before %+v", after, before)
	}
	if code := increment(t, address); code != http.StatusOK {
		t.Fatalf("increment after resume = %d", code)
	}

	stop()
	startCounter(t, counter, address, state, socket)
	if got := readStatus(t, address).Count; got != 4 {
		t.Fatalf("count after restart = %d, want 4", got)
	}
}
