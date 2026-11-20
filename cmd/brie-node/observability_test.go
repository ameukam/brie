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
	"fmt"
	"math"
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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		StateDir: filepath.Join(dir, "state"), LogDir: filepath.Join(dir, "logs"), Probes: true,
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

// TestPodHooksFeedMetricsAndEvents covers every executor decision hook: the
// metrics it feeds and the Event it records, without tenant data.
func TestPodHooksFeedMetricsAndEvents(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset()
	registry := metrics.New(nil)
	recorder := events.New(t.Context(), client, "node-1", events.Options{Observe: registry.Event})
	defer recorder.Shutdown()
	var hooks nodeagent.ExecutionHooks
	addPodHooks(&hooks, registry, recorder)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "team", Name: "web", UID: "uid-1"}}
	for _, tt := range []struct {
		name   string
		call   func()
		reason string
	}{
		{"admitted", func() { hooks.Admission(pod, nodeagent.AdmissionAdmitted, "") }, ""},
		{"refused", func() {
			hooks.Admission(pod, "InsufficientResources", "Configured CPU, memory, or Pod budget exhausted")
		}, "InsufficientResources"},
		{"pull failed", func() { hooks.ImagePullFailed(pod, false, "NotFound") }, events.ReasonImagePullFailed},
		{"pull backoff", func() { hooks.ImagePullFailed(pod, true, "NotFound") }, events.ReasonImagePullBackOff},
		{"blocked", func() { hooks.ExecutionBlocked(pod, "ExecutionConfigChanged", "delete the Pod to replace it") }, "ExecutionConfigChanged"},
		{"cleanup", func() { hooks.CleanupBlocked(pod, "Cleanup waits for positive deletion evidence") }, events.ReasonCleanupBlocked},
		{"liveness", func() { hooks.LivenessFailed(pod, "Liveness probe failed") }, events.ReasonLivenessProbeFailed},
		{"probe result", func() { hooks.ProbeResult("liveness", "exec", "failure") }, ""},
		{"journal", func() {
			hooks.Journal(nodeagent.JournalState{Records: 3, Bytes: 2048, ReservedCPUMillicores: 1500,
				ReservedMemoryBytes: 1 << 20, ReservedExecutions: 2, PendingSince: map[string]time.Time{"reserved": {}}})
		}, ""},
	} {
		tt.call()
		if tt.reason == "" {
			continue
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			list, err := client.CoreV1().Events("team").List(t.Context(), metav1.ListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, event := range list.Items {
				found = found || event.Reason == tt.reason && event.InvolvedObject.UID == "uid-1"
				if strings.Contains(event.Message, "registry.example") {
					t.Fatalf("%s: Event carries an image reference: %q", tt.name, event.Message)
				}
			}
			if found {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: no %s Event in %v", tt.name, tt.reason, list.Items)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	samples := gatherSamples(t, registry)
	for name, want := range map[string]float64{
		`brie_node_admission_decisions_total{reason="Admitted"}`:                          1,
		`brie_node_admission_decisions_total{reason="InsufficientResources"}`:             1,
		`brie_node_probe_results_total{handler="exec",probe="liveness",result="failure"}`: 1,
		"brie_node_reserved_cpu_cores":                                                    1.5,
		"brie_node_reserved_executions":                                                   2,
		"brie_node_journal_records":                                                       3,
		"brie_node_journal_bytes":                                                         2048,
	} {
		if got, found := samples[name]; !found || got != want {
			t.Errorf("%s = %v (found %v), want %v", name, got, found, want)
		}
	}
	if got, found := samples[`brie_node_oldest_pending_stage_age_seconds{stage="reserved"}`]; !found || !math.IsNaN(got) {
		t.Errorf("unknown pending age = %v (found %v), want NaN", got, found)
	}
	// Without a registry, only Events are recorded; without a recorder, only metrics.
	var eventsOnly, metricsOnly nodeagent.ExecutionHooks
	addPodHooks(&eventsOnly, nil, recorder)
	addPodHooks(&metricsOnly, registry, nil)
	eventsOnly.Admission(pod, "UnsupportedPod", "unsupported")
	metricsOnly.Admission(pod, "UnsupportedPod", "unsupported")
	if eventsOnly.Journal != nil || metricsOnly.ImagePullFailed != nil {
		t.Fatal("hooks installed without their sink")
	}
}

// gatherSamples returns gauge and counter values by "name{labels}".
func gatherSamples(t *testing.T, registry *metrics.Metrics) map[string]float64 {
	t.Helper()
	families, err := registry.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	samples := make(map[string]float64)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			var labels []string
			for _, label := range metric.GetLabel() {
				labels = append(labels, fmt.Sprintf("%s=%q", label.GetName(), label.GetValue()))
			}
			name := family.GetName()
			if len(labels) != 0 {
				name += "{" + strings.Join(labels, ",") + "}"
			}
			switch {
			case metric.GetGauge() != nil:
				samples[name] = metric.GetGauge().GetValue()
			case metric.GetCounter() != nil:
				samples[name] = metric.GetCounter().GetValue()
			}
		}
	}
	return samples
}
