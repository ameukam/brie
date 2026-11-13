// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ameukam/brie/internal/events"
	"github.com/ameukam/brie/internal/metrics"
	"github.com/ameukam/brie/internal/nodeagent"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestBarrierCause(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		state nodeagent.HealthState
		want  metrics.BarrierCause
	}{
		{nodeagent.HealthState{RuntimeHealthy: true, Synced: true}, metrics.BarrierNone},
		{nodeagent.HealthState{RuntimeHealthy: true}, metrics.BarrierIncompletePass},
		{nodeagent.HealthState{Synced: true}, metrics.BarrierRuntimeUnhealthy},
		{nodeagent.HealthState{RuntimeHealthy: true, Synced: true, RecoveryRequired: true}, metrics.BarrierRecoveryRequired},
	} {
		if got := barrierCause(tt.state); got != tt.want {
			t.Errorf("barrierCause(%+v) = %q, want %q", tt.state, got, tt.want)
		}
	}
}

func TestBarrierTrackerEventsOnTransitions(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset()
	var recorded, deduplicated atomic.Int32
	recorder := events.New(t.Context(), client, "node-1", events.Options{Observe: func(result string) {
		switch result {
		case "recorded":
			recorded.Add(1)
		case "deduplicated":
			deduplicated.Add(1)
		}
	}})
	defer recorder.Shutdown()
	tracker := &barrierTracker{metrics: metrics.New(nil), recorder: recorder}
	unsynced := nodeagent.HealthState{RuntimeHealthy: true}
	ready := nodeagent.HealthState{RuntimeHealthy: true, Synced: true}
	for _, state := range []nodeagent.HealthState{unsynced, unsynced, ready, ready, unsynced} {
		tracker.observe(state)
	}
	// Raised, lowered, raised again: repeated observations emit nothing, and
	// the identical second raise within the window is deduplicated.
	if recorded.Load() != 2 || deduplicated.Load() != 1 {
		t.Fatalf("recorded %d, deduplicated %d; want 2 and 1", recorded.Load(), deduplicated.Load())
	}
	(&barrierTracker{}).observe(ready)
}

func TestRunWithServices(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stopped := errors.New("listener failed")
	var cancelled atomic.Bool
	err := runWithServices(ctx, cancel, []func(context.Context) error{
		func(context.Context) error { return stopped },
		func(ctx context.Context) error { <-ctx.Done(); cancelled.Store(true); return nil },
	}, func(ctx context.Context) error { <-ctx.Done(); return nil })
	if !errors.Is(err, stopped) || !cancelled.Load() {
		t.Fatalf("service failure = %v (other service cancelled %v)", err, cancelled.Load())
	}
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	agentErr := errors.New("agent failed")
	err = runWithServices(ctx, cancel, []func(context.Context) error{
		func(ctx context.Context) error { <-ctx.Done(); return nil },
	}, func(context.Context) error { return agentErr })
	if !errors.Is(err, agentErr) {
		t.Fatalf("agent failure = %v", err)
	}
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	err = runWithServices(ctx, cancel, []func(context.Context) error{
		func(context.Context) error { return nil },
	}, func(ctx context.Context) error { <-ctx.Done(); return nil })
	if err == nil || !strings.Contains(err.Error(), "stopped unexpectedly") {
		t.Fatalf("silent service exit = %v", err)
	}
}

func writeTestPKI(t *testing.T, dir string) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "node-1"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

func writeKubeconfig(t *testing.T, dir, server string) string {
	t.Helper()
	path := filepath.Join(dir, "kubeconfig")
	if err := clientcmd.WriteToFile(clientcmdapi.Config{
		Clusters:  map[string]*clientcmdapi.Cluster{"c": {Server: server}},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{"u": {Token: "t"}},
		Contexts:  map[string]*clientcmdapi.Context{"x": {Cluster: "c", AuthInfo: "u"}}, CurrentContext: "x",
	}, path); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestRunExecutionWiresServices starts every service against unreachable
// dependencies: the executor fails its CRI Version check, and runExecution
// stops the services and returns that failure.
func TestRunExecutionWiresServices(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	certFile, keyFile := writeTestPKI(t, dir)
	kubeconfig := writeKubeconfig(t, dir, "https://127.0.0.1:1")
	execution := nodeagent.ExecutionConfig{
		RuntimeClass: "kata-fc", RuntimeHandler: "kata-fc", CPU: "4", Memory: "8Gi", MaxPods: 10,
		NodeIP: "192.0.2.10", ClusterDNS: "10.96.0.10", ClusterDomain: "cluster.local",
		StateDir: filepath.Join(dir, "state"), LogDir: filepath.Join(dir, "logs"),
	}
	node := nodeagent.Config{NodeName: "node-1", RuntimeSocket: filepath.Join(dir, "missing.sock")}
	options := &observabilityOptions{
		logFormat: "json", serving: true, address: "127.0.0.1:0", certFile: certFile, keyFile: keyFile,
		clientCAFile: certFile, events: true, logMaxSize: "10Mi", logMaxFiles: 3,
	}
	if err := options.validate(map[string]bool{}); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	err := runExecution(ctx, node, execution, kubeconfig, &stderr, options)
	if err == nil || !strings.Contains(err.Error(), "CRI Version") {
		t.Fatalf("runExecution = %v", err)
	}
	for _, tt := range []struct {
		name   string
		change func(*observabilityOptions, *nodeagent.Config, *string)
		want   string
	}{
		{"missing key", func(o *observabilityOptions, _ *nodeagent.Config, _ *string) {
			o.keyFile = filepath.Join(dir, "absent")
		}, "configure serving"},
		{"relative cert", func(o *observabilityOptions, _ *nodeagent.Config, _ *string) { o.certFile = "tls.crt" }, "configure serving"},
		{"plain kubeconfig", func(_ *observabilityOptions, _ *nodeagent.Config, k *string) {
			*k = writeKubeconfig(t, t.TempDir(), "http://127.0.0.1:1")
		}, "HTTPS"},
		{"missing socket", func(_ *observabilityOptions, n *nodeagent.Config, _ *string) { n.RuntimeSocket = "" }, "--runtime-socket"},
		{"invalid node", func(_ *observabilityOptions, n *nodeagent.Config, _ *string) { n.NodeName = "Bad" }, "validate agent configuration"},
		{"relative socket", func(_ *observabilityOptions, n *nodeagent.Config, _ *string) { n.RuntimeSocket = "relative.sock" }, "socket"},
	} {
		changed, changedNode, changedKubeconfig := *options, node, kubeconfig
		tt.change(&changed, &changedNode, &changedKubeconfig)
		if err := runExecution(ctx, changedNode, execution, changedKubeconfig, &stderr, &changed); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%s: %v, want %q", tt.name, err, tt.want)
		}
	}
	rotationOnly := &observabilityOptions{logFormat: "text", logMaxSize: "10Mi", logMaxFiles: 3}
	if err := rotationOnly.validate(map[string]bool{}); err != nil {
		t.Fatal(err)
	}
	if err := runExecution(ctx, node, execution, kubeconfig, &stderr, rotationOnly); err == nil || !strings.Contains(err.Error(), "CRI Version") {
		t.Fatalf("rotation-only runExecution = %v", err)
	}
}

func TestBudget(t *testing.T) {
	t.Parallel()
	cpu, memory := budgetMillicoresAndBytes(nodeagent.ExecutionConfig{CPU: "1500m", Memory: "1Gi"})
	if cpu != 1500 || memory != 1<<30 {
		t.Fatalf("budget = %d, %d", cpu, memory)
	}
	if cpu, memory := budgetMillicoresAndBytes(nodeagent.ExecutionConfig{CPU: "x"}); cpu != 0 || memory != 0 {
		t.Fatal("invalid budget parsed")
	}
}
