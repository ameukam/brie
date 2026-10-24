// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package owner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const journalVersion = 1

type journalRecord struct {
	Version    int       `json:"version"`
	Identity   string    `json:"identity"`
	Process    State     `json:"process_state"`
	Boot       BootState `json:"boot_state"`
	ProcessPID int       `json:"process_pid,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type lifecycleJournal struct {
	mu       sync.Mutex
	dir      string
	path     string
	identity string
	lock     *os.File
}

func openLifecycleJournal(dir, sandboxUID, podUID string) (*lifecycleJournal, error) {
	if !filepath.IsAbs(dir) {
		return nil, errors.New("owner journal directory must be absolute")
	}
	dir = filepath.Clean(dir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create owner journal directory: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, fmt.Errorf("inspect owner journal directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("owner journal path must be a real directory")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("owner journal directory must not be accessible by group or others")
	}

	identity := executionIdentity(sandboxUID, podUID)
	base := "owner-" + identity
	lockPath := filepath.Join(dir, base+".lock")
	fd, err := syscall.Open(lockPath, syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open owner fence: %w", err)
	}
	lock := os.NewFile(uintptr(fd), lockPath)
	closeLock := func(cause error) error {
		if err := lock.Close(); err != nil {
			return errors.Join(cause, fmt.Errorf("close owner fence: %w", err))
		}
		return cause
	}
	lockInfo, err := lock.Stat()
	if err != nil {
		return nil, closeLock(fmt.Errorf("inspect owner fence: %w", err))
	}
	if !lockInfo.Mode().IsRegular() || lockInfo.Mode().Perm()&0o077 != 0 {
		return nil, closeLock(errors.New("owner fence must be a private regular file"))
	}
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, closeLock(fmt.Errorf("acquire owner fence: %w", err))
	}

	journal := &lifecycleJournal{
		dir: dir, path: filepath.Join(dir, base+".json"), identity: identity, lock: lock,
	}
	if _, err := os.Lstat(journal.path); err == nil {
		return nil, closeLock(errors.New("owner journal already exists; refusing to reuse this execution identity"))
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, closeLock(fmt.Errorf("inspect owner journal: %w", err))
	}
	initial := journalRecord{
		Version: journalVersion, Identity: identity, Process: Idle,
		Boot: BootUnconfigured, UpdatedAt: time.Now().UTC(),
	}
	if err := journal.write(initial); err != nil {
		return nil, closeLock(fmt.Errorf("initialize owner journal: %w", err))
	}
	return journal, nil
}

func executionIdentity(sandboxUID, podUID string) string {
	sum := sha256.Sum256([]byte(sandboxUID + "\x00" + podUID))
	return hex.EncodeToString(sum[:])
}

func (j *lifecycleJournal) write(record journalRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	record.Version = journalVersion
	record.Identity = j.identity
	record.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode owner journal: %w", err)
	}
	data = append(data, '\n')

	tmp, err := os.CreateTemp(j.dir, ".owner-journal-*")
	if err != nil {
		return fmt.Errorf("create owner journal temporary file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err := tmp.Chmod(0o600); err != nil {
		return errors.Join(fmt.Errorf("set owner journal permissions: %w", err), tmp.Close())
	}
	if _, err := tmp.Write(data); err != nil {
		return errors.Join(fmt.Errorf("write owner journal: %w", err), tmp.Close())
	}
	if err := tmp.Sync(); err != nil {
		return errors.Join(fmt.Errorf("sync owner journal: %w", err), tmp.Close())
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close owner journal: %w", err)
	}
	if err := os.Rename(tmpPath, j.path); err != nil {
		return fmt.Errorf("replace owner journal: %w", err)
	}
	dir, err := os.Open(j.dir)
	if err != nil {
		return fmt.Errorf("open owner journal directory for sync: %w", err)
	}
	if err := dir.Sync(); err != nil {
		return errors.Join(fmt.Errorf("sync owner journal directory: %w", err), dir.Close())
	}
	if err := dir.Close(); err != nil {
		return fmt.Errorf("close owner journal directory: %w", err)
	}
	return nil
}

func (j *lifecycleJournal) close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.lock == nil {
		return nil
	}
	lock := j.lock
	j.lock = nil
	if err := lock.Close(); err != nil {
		return fmt.Errorf("release owner fence: %w", err)
	}
	return nil
}
