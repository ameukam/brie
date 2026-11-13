// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// TestExecutionJournalCompaction covers item 3 of the requirements in section
// 6.4 of DESIGN.md: a finished record is dropped only after RemovePodSandbox
// succeeded and a complete inventory shows none of its runtime objects.
func TestExecutionJournalCompaction(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// finish brings the running execution of Pod "app" to its finished
		// stage, then prepare changes the world before the compaction pass.
		finish  func(*testing.T, *executor, *executionRuntime)
		prepare func(*testing.T, *executor, *executionRuntime)
		compact bool
	}{
		{
			name:    "deleted Pod after sandbox removal",
			finish:  deleteRunningPod,
			compact: true,
		},
		{
			name: "terminal Pod still listed",
			finish: func(t *testing.T, e *executor, service *executionRuntime) {
				service.mu.Lock()
				for _, container := range service.containers {
					container.State = runtimeapi.ContainerState_CONTAINER_EXITED
				}
				service.mu.Unlock()
				reconcilePasses(t, e, 2)
			},
		},
		{
			name: "sandbox removed outside the agent",
			finish: func(t *testing.T, e *executor, service *executionRuntime) {
				service.mu.Lock()
				clear(service.sandboxes)
				clear(service.containers)
				service.mu.Unlock()
				deleteRunningPod(t, e, service)
			},
		},
		{
			name:   "foreign sandbox claims the Pod UID",
			finish: deleteRunningPod,
			prepare: func(_ *testing.T, _ *executor, service *executionRuntime) {
				service.mu.Lock()
				defer service.mu.Unlock()
				service.sandboxes["foreign"] = &runtimeapi.PodSandbox{Id: "foreign", CreatedAt: 1,
					Metadata:       &runtimeapi.PodSandboxMetadata{Name: "app", Namespace: "default", Uid: "other-uid"},
					Labels:         map[string]string{podUIDLabel: "app-uid", executionMarker: "another-owner"},
					RuntimeHandler: "kata-fc", State: runtimeapi.PodSandboxState_SANDBOX_NOTREADY}
			},
		},
		{
			name:   "container in another sandbox claims the Pod UID",
			finish: deleteRunningPod,
			prepare: func(_ *testing.T, _ *executor, service *executionRuntime) {
				service.mu.Lock()
				defer service.mu.Unlock()
				service.containers["stray"] = &runtimeapi.Container{Id: "stray", PodSandboxId: "foreign", CreatedAt: 1,
					Metadata: &runtimeapi.ContainerMetadata{Name: "app"}, Labels: map[string]string{podUIDLabel: "app-uid"},
					Image: &runtimeapi.ImageSpec{Image: "image"}, ImageRef: "image"}
			},
		},
		{
			name:   "assigned-Pod list unavailable",
			finish: deleteRunningPod,
			prepare: func(_ *testing.T, e *executor, _ *executionRuntime) {
				e.api.(*fake.Clientset).PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewServiceUnavailable("etcd unavailable")
				})
			},
		},
		{
			name:   "runtime inventory unavailable",
			finish: deleteRunningPod,
			prepare: func(_ *testing.T, _ *executor, service *executionRuntime) {
				service.mu.Lock()
				service.listError = true
				service.mu.Unlock()
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, service := newExecutor(t, executionPod("app"))
			reconcilePasses(t, e, 5)
			tc.finish(t, e, service)
			record := e.journal.Records["app-uid"]
			if record == nil || record.Stage != "finished" {
				t.Fatalf("execution not finished: %+v", record)
			}
			if tc.prepare != nil {
				tc.prepare(t, e, service)
			}
			for range 3 {
				_ = e.reconcile(t.Context())
			}
			restartJournal(t, e)
			if compacted := e.journal.Records["app-uid"] == nil; compacted != tc.compact {
				t.Fatalf("compacted=%t, want %t: %+v", compacted, tc.compact, record)
			}
		})
	}
}

// deleteRunningPod deletes Pod "app" from the API and lets one pass clean up
// its execution on list evidence.
func deleteRunningPod(t *testing.T, e *executor, _ *executionRuntime) {
	t.Helper()
	if err := e.api.CoreV1().Pods("default").Delete(t.Context(), "app", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	reconcilePasses(t, e, 1)
}

func TestExecutionJournalCompactionReleasesUnstartedReservation(t *testing.T) {
	t.Parallel()
	e, service := newExecutor(t, executionPod("app"))
	// Delete the Pod during its image pull, before any sandbox exists.
	service.onPull = func() {
		_ = e.api.CoreV1().Pods("default").Delete(t.Context(), "app", metav1.DeleteOptions{})
	}
	reconcilePasses(t, e, 1)
	_ = e.reconcile(t.Context())
	_ = e.reconcile(t.Context())
	if record := e.journal.Records["app-uid"]; record == nil || record.Stage != "finished" || record.SandboxRemoved {
		t.Fatalf("unstarted reservation not finished without a sandbox: %+v", record)
	}
	reconcilePasses(t, e, 1)
	if e.journal.Records["app-uid"] != nil || countCalls(service, "RemovePodSandbox") != 0 {
		t.Fatal("finished record without a sandbox was not compacted")
	}
}

func TestExecutionJournalCompactionSaveFailure(t *testing.T) {
	t.Parallel()
	e, service := newExecutor(t, executionPod("app"))
	reconcilePasses(t, e, 5)
	deleteRunningPod(t, e, service)
	dir := e.journal.dir
	e.journal.dir = filepath.Join(dir, "missing")
	if err := e.reconcile(t.Context()); err == nil || !strings.Contains(err.Error(), "compact execution journal") {
		t.Fatalf("failed compaction not reported: %v", err)
	}
	if record := e.journal.Records["app-uid"]; record == nil || record.Stage != "finished" || e.synced.Load() {
		t.Fatalf("record lost or pass synced after a failed compaction: %+v", record)
	}
	e.journal.dir = dir
	reconcilePasses(t, e, 1)
	restartJournal(t, e)
	if len(e.journal.Records) != 0 {
		t.Fatalf("compaction not durable: %+v", e.journal.Records)
	}
}

func TestExecutionJournalCompactionKeepsTerminalPodUntilDeleted(t *testing.T) {
	t.Parallel()
	e, service := newExecutor(t, executionPod("app"))
	reconcilePasses(t, e, 5)
	service.mu.Lock()
	for _, container := range service.containers {
		container.State = runtimeapi.ContainerState_CONTAINER_EXITED
	}
	service.mu.Unlock()
	reconcilePasses(t, e, 4)
	if record := e.journal.Records["app-uid"]; record == nil || !record.SandboxRemoved {
		t.Fatalf("listed terminal Pod lost its record: %+v", record)
	}
	if err := e.api.CoreV1().Pods("default").Delete(t.Context(), "app", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	reconcilePasses(t, e, 1)
	if e.journal.Records["app-uid"] != nil {
		t.Fatal("deleted terminal Pod record was not compacted")
	}
	// The same name with a new UID is admitted as a new execution.
	if _, err := e.api.CoreV1().Pods("default").Create(t.Context(), executionPod("app"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	reconcilePasses(t, e, 5)
	if record := e.journal.Records["app-uid"]; record == nil || record.Stage != "running" {
		t.Fatalf("Pod not admitted after compaction: %+v", record)
	}
}

// writeJournalPayload writes payload as the journal state file with a valid
// envelope and checksum.
func writeJournalPayload(t *testing.T, dir string, payload any) {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), journalEnvelope(t, data), 0600); err != nil {
		t.Fatal(err)
	}
}

func journalEnvelope(t testing.TB, data []byte) []byte {
	t.Helper()
	sum := sha256.Sum256(data)
	envelope, err := json.Marshal(struct {
		Data   []byte
		SHA256 string
	}{Data: data, SHA256: hex.EncodeToString(sum[:])})
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

// versionOneJournal returns a journal payload in the version 1 schema, which
// has no SandboxRemoved field.
func versionOneJournal(config ExecutionConfig) map[string]any {
	hash := strings.Repeat("a", 64)
	record := func(uid, stage, sandbox, container string) map[string]any {
		return map[string]any{"UID": uid, "Namespace": "default", "Name": uid, "Hash": hash, "Stage": stage,
			"SandboxID": sandbox, "ContainerID": container, "CPU": 100, "Memory": 1024}
	}
	return map[string]any{
		"Version": 1, "Owner": "owner", "Node": "worker-1", "Class": config.RuntimeClass, "NodeUID": "",
		"NodeIP": config.NodeIP, "DNS": config.ClusterDNS, "Domain": config.ClusterDomain, "LogDir": config.LogDir,
		"Handler": config.RuntimeHandler, "ClassUID": "",
		"Records": map[string]any{
			"running":           record("running", "running", "sandbox-running", "container-running"),
			"finished-sandbox":  record("finished-sandbox", "finished", "sandbox-finished", "container-finished"),
			"finished-reserved": record("finished-reserved", "finished", "", ""),
		},
	}
}

// TestExecutionJournalMigration covers reading the version 1 schema, writing it
// back as version 2, and the removal-evidence rules of each version.
func TestExecutionJournalMigration(t *testing.T) {
	t.Parallel()
	t.Run("version 1 is migrated", func(t *testing.T) {
		t.Parallel()
		config := executionConfig(t)
		if err := os.MkdirAll(config.StateDir, 0700); err != nil {
			t.Fatal(err)
		}
		writeJournalPayload(t, config.StateDir, versionOneJournal(config))
		journal, err := openExecutionJournal(config.StateDir, "worker-1", config)
		if err != nil {
			t.Fatal(err)
		}
		if journal.Version != executionJournalVersion || len(journal.Records) != 3 || journal.Owner != "owner" ||
			journal.Records["running"].ContainerID != "container-running" {
			t.Fatalf("version 1 journal not preserved: %+v", journal)
		}
		// A version 1 finished record carries no removal evidence: one that
		// names a sandbox is retained; one that never named a sandbox is not.
		if compactable(journal.Records["finished-sandbox"], false, nil, nil) ||
			!compactable(journal.Records["finished-reserved"], false, nil, nil) {
			t.Fatal("version 1 finished records compacted without removal evidence")
		}
		if err := journal.save(); err != nil {
			t.Fatal(err)
		}
		if err := journal.close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := openExecutionJournal(config.StateDir, "worker-1", config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reopened.close() }()
		raw, err := os.ReadFile(filepath.Join(config.StateDir, "state.json"))
		if err != nil {
			t.Fatal(err)
		}
		var envelope struct{ Data []byte }
		if err := json.Unmarshal(raw, &envelope); err != nil || !strings.Contains(string(envelope.Data), `"Version":2`) {
			t.Fatalf("migrated journal not written as version 2: %v %s", err, envelope.Data)
		}
		if len(reopened.Records) != 3 {
			t.Fatalf("migration lost records: %+v", reopened.Records)
		}
	})
	for _, tc := range []struct {
		name   string
		modify func(map[string]any)
	}{
		{"version 0", func(payload map[string]any) { payload["Version"] = 0 }},
		{"version 3", func(payload map[string]any) { payload["Version"] = 3 }},
		{"version 1 with removal evidence", func(payload map[string]any) {
			payload["Records"].(map[string]any)["finished-sandbox"].(map[string]any)["SandboxRemoved"] = true
		}},
		{"removal evidence on a running record", func(payload map[string]any) {
			payload["Version"] = 2
			payload["Records"].(map[string]any)["running"].(map[string]any)["SandboxRemoved"] = true
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			config := executionConfig(t)
			if err := os.MkdirAll(config.StateDir, 0700); err != nil {
				t.Fatal(err)
			}
			payload := versionOneJournal(config)
			tc.modify(payload)
			writeJournalPayload(t, config.StateDir, payload)
			if _, err := openExecutionJournal(config.StateDir, "worker-1", config); err == nil {
				t.Fatal("unsupported journal schema accepted")
			}
		})
	}
	t.Run("version 2 removal evidence round trip", func(t *testing.T) {
		t.Parallel()
		config := executionConfig(t)
		if err := os.MkdirAll(config.StateDir, 0700); err != nil {
			t.Fatal(err)
		}
		payload := versionOneJournal(config)
		payload["Version"] = 2
		payload["Records"].(map[string]any)["finished-sandbox"].(map[string]any)["SandboxRemoved"] = true
		writeJournalPayload(t, config.StateDir, payload)
		journal, err := openExecutionJournal(config.StateDir, "worker-1", config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = journal.close() }()
		record := journal.Records["finished-sandbox"]
		if !record.SandboxRemoved || !compactable(record, false, nil, nil) || compactable(record, true, nil, nil) {
			t.Fatalf("removal evidence not honored: %+v", record)
		}
		sandbox := &runtimeapi.PodSandbox{Id: "sandbox-finished", Metadata: &runtimeapi.PodSandboxMetadata{}}
		container := &runtimeapi.Container{Id: "other", PodSandboxId: "sandbox-finished"}
		if compactable(record, false, []*runtimeapi.PodSandbox{sandbox}, nil) ||
			compactable(record, false, nil, []*runtimeapi.Container{container}) {
			t.Fatal("record compacted while the inventory names its objects")
		}
	})
}

// FuzzExecutionJournal fuzzes the journal payload behind a valid envelope, so
// inputs reach schema validation and migration instead of the checksum.
func FuzzExecutionJournal(f *testing.F) {
	config := ExecutionConfig{RuntimeClass: "c", RuntimeHandler: "h", NodeIP: "192.0.2.10",
		ClusterDNS: "10.96.0.10", ClusterDomain: "d", LogDir: "/l"}
	hash := strings.Repeat("a", 64)
	for _, record := range []string{
		`"r":{"UID":"r","Namespace":"n","Name":"r","Hash":"` + hash + `","Stage":"running","SandboxID":"s","ContainerID":"c","CPU":1,"Memory":1}`,
		`"f":{"UID":"f","Namespace":"n","Name":"f","Hash":"` + hash + `","Stage":"finished","SandboxID":"s","CPU":1,"Memory":1,"SandboxRemoved":true}`,
	} {
		for _, version := range []string{"1", "2"} {
			f.Add([]byte(`{"Version":` + version + `,"Owner":"o","Node":"w","Class":"c","Handler":"h","NodeIP":"192.0.2.10",` +
				`"DNS":"10.96.0.10","Domain":"d","LogDir":"/l","Records":{` + record + `}}`))
		}
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		journal := &executionJournal{}
		if err := journal.decode(journalEnvelope(t, payload), "w", config); err != nil {
			return
		}
		if journal.Version != executionJournalVersion || journal.Records == nil {
			t.Fatalf("accepted journal not migrated: %+v", journal)
		}
		for uid, record := range journal.Records {
			if record.SandboxRemoved && record.Stage != "finished" || uid != record.UID {
				t.Fatalf("invalid record accepted: %+v", record)
			}
		}
		encoded, err := json.Marshal(journal)
		if err != nil {
			t.Fatal(err)
		}
		again := &executionJournal{}
		if err := again.decode(journalEnvelope(t, encoded), "w", config); err != nil {
			t.Fatalf("accepted journal does not round trip: %v", err)
		}
	})
}
