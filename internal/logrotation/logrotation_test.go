// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package logrotation

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ameukam/brie/internal/execinventory"
	"github.com/ameukam/brie/internal/podlog"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const handler = "kata-fc"

type fakeRuntime struct {
	sandboxes  []*runtimeapi.PodSandbox
	containers []*runtimeapi.Container
	logPaths   map[string]string
	reopenErr  error
	listErr    error
	statusErr  error
	reopened   []string
	onReopen   func(id string)
	// reopenLost runs onReopen and still returns reopenErr: the runtime
	// reopened the log but its reply was lost.
	reopenLost bool
}

func (f *fakeRuntime) ListPodSandbox(context.Context) ([]*runtimeapi.PodSandbox, error) {
	return f.sandboxes, f.listErr
}

func (f *fakeRuntime) ListContainers(context.Context) ([]*runtimeapi.Container, error) {
	return f.containers, nil
}

func (f *fakeRuntime) ContainerStatus(_ context.Context, id string) (*runtimeapi.ContainerStatus, error) {
	return &runtimeapi.ContainerStatus{Id: id, LogPath: f.logPaths[id]}, f.statusErr
}

func (f *fakeRuntime) ReopenContainerLog(_ context.Context, id string) error {
	f.reopened = append(f.reopened, id)
	if (f.reopenErr == nil || f.reopenLost) && f.onReopen != nil {
		f.onReopen(id)
	}
	return f.reopenErr
}

func sandbox(id, uid string, owned bool, handlerName string) *runtimeapi.PodSandbox {
	labels := map[string]string{}
	if owned {
		labels["brie.dev/execution-owner"] = "owner"
	}
	return &runtimeapi.PodSandbox{Id: id, Metadata: &runtimeapi.PodSandboxMetadata{Uid: uid, Name: id, Namespace: "ns"},
		Labels: labels, RuntimeHandler: handlerName}
}

type setup struct {
	root       string
	runtime    *fakeRuntime
	executions *execinventory.Store
	rotator    *Rotator
	logFile    string
	results    []string
}

func newSetup(t *testing.T, size int, maxFiles int) *setup {
	t.Helper()
	s := &setup{root: t.TempDir()}
	dir := podlog.Directory(s.root, "uid-1")
	if err := os.MkdirAll(dir, 0750); err != nil {
		t.Fatal(err)
	}
	s.logFile = filepath.Join(dir, "0.log")
	if err := os.WriteFile(s.logFile, bytes.Repeat([]byte("x"), size), 0600); err != nil {
		t.Fatal(err)
	}
	s.runtime = &fakeRuntime{
		sandboxes: []*runtimeapi.PodSandbox{sandbox("s1", "uid-1", true, handler), sandbox("foreign", "uid-2", false, handler)},
		containers: []*runtimeapi.Container{
			{Id: "c1", PodSandboxId: "s1", State: runtimeapi.ContainerState_CONTAINER_RUNNING},
			{Id: "exited", PodSandboxId: "s1", State: runtimeapi.ContainerState_CONTAINER_EXITED},
			{Id: "c2", PodSandboxId: "foreign", State: runtimeapi.ContainerState_CONTAINER_RUNNING},
		},
		logPaths: map[string]string{"c1": s.logFile, "exited": s.logFile, "c2": s.logFile},
	}
	s.runtime.onReopen = func(string) {
		if err := os.WriteFile(s.logFile, nil, 0600); err != nil {
			t.Error(err)
		}
	}
	s.executions = &execinventory.Store{}
	s.executions.Set([]execinventory.Execution{{PodUID: "uid-1", Namespace: "ns", Name: "s1", SandboxID: "s1", ContainerName: "app", ContainerID: "c1"}})
	rotator, err := New(Config{LogDir: s.root, RuntimeHandler: handler, MaxSize: MinMaxSize, MaxFiles: maxFiles}, s.runtime, s.executions, nil,
		func(result string) { s.results = append(s.results, result) })
	if err != nil {
		t.Fatal(err)
	}
	s.rotator = rotator
	return s
}

func rotatedFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "0.log.") {
			names = append(names, entry.Name())
		}
	}
	return names
}

func TestRotateBySize(t *testing.T) {
	t.Parallel()
	s := newSetup(t, MinMaxSize+1, 3)
	clock := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	s.rotator.now = func() time.Time { return clock }
	if err := s.rotator.RotateOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(s.runtime.reopened, []string{"c1"}) {
		t.Fatalf("reopened = %v; only the running owned container rotates", s.runtime.reopened)
	}
	dir := filepath.Dir(s.logFile)
	if got := rotatedFiles(t, dir); !slices.Equal(got, []string{"0.log.20261113-100000"}) {
		t.Fatalf("rotated = %v", got)
	}
	// Under the limit, nothing happens.
	if err := s.rotator.RotateOnce(context.Background()); err != nil || len(s.runtime.reopened) != 1 {
		t.Fatalf("small log rotated: %v %v", err, s.runtime.reopened)
	}
	// MaxFiles=3 keeps the current file and two rotated files.
	for i := 1; i <= 3; i++ {
		clock = clock.Add(time.Duration(i) * time.Minute)
		if err := os.WriteFile(s.logFile, bytes.Repeat([]byte("y"), MinMaxSize+1), 0600); err != nil {
			t.Fatal(err)
		}
		if err := s.rotator.RotateOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := rotatedFiles(t, dir); len(got) != 2 || got[0] != "0.log.20261113-100300" {
		t.Fatalf("rotated after pruning = %v", got)
	}
	if slices.Index(s.results, "rotated") < 0 {
		t.Fatalf("results = %v", s.results)
	}
	// A second rotation in the same second waits for the next pass.
	if err := os.WriteFile(s.logFile, bytes.Repeat([]byte("z"), MinMaxSize+1), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.rotator.RotateOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.rotator.RotateOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestReopenFailure covers both outcomes a reopen error can hide. The current
// log written by the runtime must never be replaced or unlinked.
func TestReopenFailure(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name        string
		reopenLost  bool
		wantCurrent func(*testing.T, *setup, os.FileInfo)
		wantRotated int
		wantErr     string
	}{
		{name: "reopen did not happen", wantRotated: 0, wantErr: "unavailable",
			wantCurrent: func(t *testing.T, _ *setup, info os.FileInfo) {
				if info.Size() != MinMaxSize+1 {
					t.Fatalf("current log size = %d; the original name was not restored", info.Size())
				}
			}},
		{name: "reopen happened and its reply was lost", reopenLost: true, wantRotated: 1, wantErr: "both files were kept",
			wantCurrent: func(t *testing.T, s *setup, _ os.FileInfo) {
				content, err := os.ReadFile(s.logFile)
				if err != nil || string(content) != "new output\n" {
					t.Fatalf("current log = %q, %v; the runtime's new log was replaced", content, err)
				}
			}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := newSetup(t, MinMaxSize+1, 2)
			s.runtime.reopenErr = errors.New("CRI ReopenContainerLog: unavailable")
			s.runtime.reopenLost = tt.reopenLost
			var runtimeFile *os.File
			s.runtime.onReopen = func(string) {
				file, err := os.OpenFile(s.logFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
				if err != nil {
					t.Error(err)
					return
				}
				runtimeFile = file
			}
			err := s.rotator.RotateOnce(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want %q", err, tt.wantErr)
			}
			if runtimeFile != nil {
				defer func() { _ = runtimeFile.Close() }()
				if _, err := runtimeFile.WriteString("new output\n"); err != nil {
					t.Fatal(err)
				}
				held, err := runtimeFile.Stat()
				if err != nil {
					t.Fatal(err)
				}
				current, err := os.Lstat(s.logFile)
				if err != nil || !os.SameFile(held, current) {
					t.Fatalf("the runtime's open log is no longer at the current path: %v", err)
				}
			}
			info, err := os.Lstat(s.logFile)
			if err != nil {
				t.Fatalf("current log missing: %v", err)
			}
			tt.wantCurrent(t, s, info)
			if got := rotatedFiles(t, filepath.Dir(s.logFile)); len(got) != tt.wantRotated {
				t.Fatalf("rotated files = %v, want %d", got, tt.wantRotated)
			}
			if !slices.Contains(s.results, "reopen_failed") {
				t.Fatalf("results = %v", s.results)
			}
		})
	}
}

// TestRotationRequiresVerifiedExecution is a regression test: a container in
// an owner-labeled sandbox is rotated, and its rotated logs pruned, only when
// its IDs and Pod UID match an execution the executor verified.
func TestRotationRequiresVerifiedExecution(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		executions []execinventory.Execution
		unset      bool
		reopened   []string
		err        string
	}{
		{name: "verified", reopened: []string{"c1"},
			executions: []execinventory.Execution{{PodUID: "uid-1", SandboxID: "s1", ContainerID: "c1"}}},
		{name: "owner label only", executions: []execinventory.Execution{}},
		{name: "container of another sandbox",
			executions: []execinventory.Execution{{PodUID: "uid-1", SandboxID: "s1", ContainerID: "c2"}}},
		{name: "sandbox of another Pod",
			executions: []execinventory.Execution{{PodUID: "uid-2", SandboxID: "s1", ContainerID: "c1"}}},
		{name: "sandbox not verified yet",
			executions: []execinventory.Execution{{PodUID: "uid-1", SandboxID: "s1"}}},
		{name: "inventory unavailable", unset: true, err: "verified executions"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := newSetup(t, MinMaxSize+1, 2)
			// Another owner's sandbox for the same Pod log directory.
			s.runtime.sandboxes = append(s.runtime.sandboxes, sandbox("rogue", "uid-1", true, handler))
			s.runtime.containers = append(s.runtime.containers,
				&runtimeapi.Container{Id: "c3", PodSandboxId: "rogue", State: runtimeapi.ContainerState_CONTAINER_RUNNING})
			s.runtime.logPaths["c3"] = s.logFile
			store := &execinventory.Store{}
			if !tt.unset {
				store.Set(tt.executions)
			}
			s.rotator.executions = store
			err := s.rotator.RotateOnce(context.Background())
			if tt.err != "" && (err == nil || !strings.Contains(err.Error(), tt.err)) || tt.err == "" && err != nil {
				t.Fatalf("error = %v, want %q", err, tt.err)
			}
			if !slices.Equal(s.runtime.reopened, tt.reopened) {
				t.Fatalf("reopened = %v, want %v", s.runtime.reopened, tt.reopened)
			}
		})
	}
}

func TestRotationRefusals(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		modify func(*testing.T, *setup)
		want   string
	}{
		{name: "path outside", modify: func(_ *testing.T, s *setup) {
			s.runtime.logPaths["c1"] = filepath.Join(s.root, "0.log")
		}, want: "outside"},
		{name: "symlink log", modify: func(t *testing.T, s *setup) {
			target := filepath.Join(s.root, "target")
			if err := os.WriteFile(target, bytes.Repeat([]byte("x"), MinMaxSize+1), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(s.logFile); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, s.logFile); err != nil {
				t.Fatal(err)
			}
		}, want: "regular file"},
		{name: "symlink directory", modify: func(t *testing.T, s *setup) {
			dir := filepath.Dir(s.logFile)
			moved := filepath.Join(s.root, "moved")
			if err := os.Rename(dir, moved); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(moved, dir); err != nil {
				t.Fatal(err)
			}
		}, want: "not a directory"},
		{name: "missing directory", modify: func(t *testing.T, s *setup) {
			if err := os.RemoveAll(filepath.Dir(s.logFile)); err != nil {
				t.Fatal(err)
			}
		}, want: "inspect"},
		{name: "status error", modify: func(_ *testing.T, s *setup) { s.runtime.statusErr = errors.New("gone") }, want: "gone"},
		{name: "list error", modify: func(_ *testing.T, s *setup) { s.runtime.listErr = errors.New("down") }, want: "down"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := newSetup(t, MinMaxSize+1, 2)
			tt.modify(t, s)
			if err := s.rotator.RotateOnce(context.Background()); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
			if len(s.runtime.reopened) != 0 {
				t.Fatal("refused rotation reopened a log")
			}
		})
	}
	s := newSetup(t, 0, 2)
	if err := os.Remove(s.logFile); err != nil {
		t.Fatal(err)
	}
	if err := s.rotator.RotateOnce(context.Background()); err != nil {
		t.Fatalf("missing log = %v", err)
	}
}

func TestCleanupOnlyFinishedPods(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, uid := range []string{"finished", "still-present", "unknown"} {
		if err := os.MkdirAll(podlog.Directory(root, uid), 0750); err != nil {
			t.Fatal(err)
		}
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, podlog.Directory(root, "linked")); err != nil {
		t.Fatal(err)
	}
	runtime := &fakeRuntime{sandboxes: []*runtimeapi.PodSandbox{sandbox("s", "still-present", true, handler)}}
	finished := func(context.Context) ([]string, error) {
		return []string{"finished", "still-present", "never-existed", "linked", ""}, nil
	}
	rotator, err := New(Config{LogDir: root, RuntimeHandler: handler, MaxSize: MinMaxSize, MaxFiles: 2}, runtime, &execinventory.Store{}, finished, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = rotator.CleanupOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("cleanup error = %v; the symlinked entry must be refused", err)
	}
	for uid, want := range map[string]bool{"finished": false, "still-present": true, "unknown": true} {
		_, statErr := os.Stat(podlog.Directory(root, uid))
		if exists := statErr == nil; exists != want {
			t.Errorf("%s exists = %v, want %v", uid, exists, want)
		}
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatal("symlink target removed")
	}
	disabled, err := New(Config{LogDir: root, RuntimeHandler: handler, MaxSize: MinMaxSize, MaxFiles: 2}, runtime, &execinventory.Store{}, nil, nil)
	if err != nil || disabled.CleanupOnce(context.Background()) != nil {
		t.Fatal("cleanup without a finished source must do nothing")
	}
	failing, _ := New(Config{LogDir: root, RuntimeHandler: handler, MaxSize: MinMaxSize, MaxFiles: 2}, runtime, &execinventory.Store{},
		func(context.Context) ([]string, error) { return nil, errors.New("journal locked") }, nil)
	if err := failing.CleanupOnce(context.Background()); err == nil {
		t.Fatal("finished-source error ignored")
	}
	runtime.listErr = errors.New("down")
	if err := rotator.CleanupOnce(context.Background()); err == nil {
		t.Fatal("inventory error ignored; cleanup must not proceed without it")
	}
}

func TestNewAndParse(t *testing.T) {
	t.Parallel()
	runtime := &fakeRuntime{}
	valid := Config{LogDir: "/var/log/brie", RuntimeHandler: handler, MaxSize: MinMaxSize, MaxFiles: 2}
	executions := &execinventory.Store{}
	rotator, err := New(valid, runtime, executions, nil, nil)
	if err != nil || rotator.config.Interval != 10*time.Second {
		t.Fatalf("New = %v", err)
	}
	for _, change := range []func(*Config){
		func(c *Config) { c.LogDir = "relative" },
		func(c *Config) { c.LogDir = "/" },
		func(c *Config) { c.RuntimeHandler = "" },
		func(c *Config) { c.MaxSize = MinMaxSize - 1 },
		func(c *Config) { c.MaxFiles = 1 },
		func(c *Config) { c.MaxFiles = 101 },
	} {
		config := valid
		change(&config)
		if _, err := New(config, runtime, executions, nil, nil); err == nil {
			t.Errorf("accepted %+v", config)
		}
	}
	if _, err := New(valid, nil, executions, nil, nil); err == nil {
		t.Fatal("accepted nil runtime")
	}
	if _, err := New(valid, runtime, nil, nil, nil); err == nil {
		t.Fatal("accepted a nil execution inventory")
	}
	for value, want := range map[string]int64{"10Mi": 10 << 20, "1Mi": MinMaxSize, "10Gi": MaxMaxSize} {
		if got, err := ParseMaxSize(value); err != nil || got != want {
			t.Errorf("ParseMaxSize(%q) = %d, %v", value, got, err)
		}
	}
	for _, value := range []string{"", "x", "1Ki", "11Gi", "1.5", "-10Mi"} {
		if _, err := ParseMaxSize(value); err == nil {
			t.Errorf("ParseMaxSize(%q) accepted", value)
		}
	}
}

func TestRun(t *testing.T) {
	t.Parallel()
	s := newSetup(t, 0, 2)
	s.runtime.listErr = errors.New("down")
	s.rotator.config.Interval = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	reported := make(chan error, 1)
	done := make(chan error, 1)
	go func() {
		done <- s.rotator.Run(ctx, func(err error) {
			select {
			case reported <- err:
			default:
			}
		})
	}()
	if err := <-reported; err == nil {
		t.Fatal("no error reported")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func FuzzParseMaxSize(f *testing.F) {
	for _, seed := range []string{"10Mi", "1Gi", "1048576", "1e6", "x", "-1", "10Gi"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		size, err := ParseMaxSize(value)
		if err == nil && (size < MinMaxSize || size > MaxMaxSize) {
			t.Fatalf("accepted out-of-bounds size %d", size)
		}
	})
}
