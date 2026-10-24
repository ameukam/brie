// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package owner

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestJournalRecordsLifecycleAndPreventsReuse(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "journal")
	config := testConfig()
	config.JournalDir = dir
	process := newProcess(t)
	owner, err := New(config, launchFunc(func(context.Context, Config) (Process, error) {
		return process, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err == nil {
		t.Fatal("released owner fence while process was running")
	}
	assertJournalState(t, owner.journal.path, Running, BootUnconfigured)

	if err := owner.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertJournalState(t, owner.journal.path, Exited, BootUnknown)
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := New(config, launchFunc(func(context.Context, Config) (Process, error) {
		t.Fatal("reused owner launched a process")
		return nil, nil
	})); err == nil || !strings.Contains(err.Error(), "journal already exists") {
		t.Fatalf("New() after exit = %v, want stale journal rejection", err)
	}
}

func TestJournalFenceExcludesAnotherProcess(t *testing.T) {
	config := testConfig()
	config.JournalDir = filepath.Join(t.TempDir(), "journal")
	config.PodUID = "pod-fence-child"
	owner, err := New(config, launchFunc(func(context.Context, Config) (Process, error) {
		t.Fatal("fence test launched a process")
		return nil, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	}()

	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestJournalFenceHelper$")
	cmd.Env = append(os.Environ(),
		"BRIE_OWNER_FENCE_HELPER=1",
		"BRIE_OWNER_FENCE_DIR="+owner.config.JournalDir,
		"BRIE_OWNER_FENCE_POD="+owner.config.PodUID,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("fence helper failed: %v\n%s", err, output)
	}
}

func TestJournalFenceHelper(t *testing.T) {
	if os.Getenv("BRIE_OWNER_FENCE_HELPER") != "1" {
		return
	}
	config := testConfig()
	config.JournalDir = os.Getenv("BRIE_OWNER_FENCE_DIR")
	config.PodUID = os.Getenv("BRIE_OWNER_FENCE_POD")
	owner, err := New(config, launchFunc(func(context.Context, Config) (Process, error) {
		return nil, errors.New("unexpected launch")
	}))
	if err == nil {
		_ = owner.Close()
		t.Fatal("second process acquired the owner fence")
	}
	if !strings.Contains(err.Error(), "acquire owner fence") {
		t.Fatalf("New() error = %v, want fence conflict", err)
	}
}

func TestJournalDirectoryRequirements(t *testing.T) {
	for _, tt := range []struct {
		name string
		dir  func(*testing.T) string
	}{
		{
			name: "relative",
			dir:  func(*testing.T) string { return "relative" },
		},
		{
			name: "symlink",
			dir: func(t *testing.T) string {
				target := t.TempDir()
				link := filepath.Join(t.TempDir(), "link")
				if err := os.Symlink(target, link); err != nil {
					t.Fatal(err)
				}
				return link
			},
		},
		{
			name: "permissive",
			dir: func(t *testing.T) string {
				dir := filepath.Join(t.TempDir(), "open")
				if err := os.Mkdir(dir, 0o755); err != nil {
					t.Fatal(err)
				}
				return dir
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			config := testConfig()
			config.JournalDir = tt.dir(t)
			if _, err := New(config, launchFunc(func(context.Context, Config) (Process, error) {
				t.Fatal("invalid journal directory launched a process")
				return nil, nil
			})); err == nil {
				t.Fatal("invalid journal directory accepted")
			}
		})
	}
}

func assertJournalState(t *testing.T, path string, process State, boot BootState) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var record journalRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if record.Version != journalVersion || record.Process != process || record.Boot != boot {
		t.Fatalf("journal = %+v; want process=%s boot=%s", record, process, boot)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("journal permissions = %o; want 600", info.Mode().Perm())
	}
}
