// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/ameukam/brie/internal/cri"
)

// maxResourceVersion bounds the opaque list resource version kept as deletion
// evidence.
const maxResourceVersion = 128

type executionRecord struct {
	UID, Namespace, Name, Hash, Stage string
	SandboxID, ContainerID            string
	CPU, Memory                       int64
	// DeletionObserved is the resource version of the complete assigned-Pod
	// list that proved the Pod deleted. Once set, it authorizes cleanup
	// without a live Pod read. Older journals omit it.
	DeletionObserved string `json:",omitempty"`
	// CreateRejected is the gRPC status code with which the runtime refused
	// the create call of the current pending stage. It is journaled before
	// the inventory that proves nothing was created is read, and it stays set
	// after that proof so the create is never retried. Older journals omit it.
	CreateRejected string `json:",omitempty"`
}

type executionJournal struct {
	Version                     int
	Owner, Node, Class          string
	NodeUID                     string
	NodeIP, DNS, Domain, LogDir string
	Handler, ClassUID           string
	Records                     map[string]*executionRecord
	dir                         string
	lock                        *os.File
}

func openExecutionJournal(dir, node string, config ExecutionConfig) (_ *executionJournal, retErr error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create execution state directory: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 ||
		info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		return nil, errors.New("execution state directory must be a private directory, not a symlink")
	}
	lock, err := os.OpenFile(filepath.Join(dir, "execution.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open execution lock: %w", err)
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, lock.Close())
		}
	}()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("lock execution state: %w", err)
	}
	journal := &executionJournal{dir: dir, lock: lock}
	file, err := os.Open(filepath.Join(dir, "state.json"))
	if errors.Is(err, os.ErrNotExist) {
		journal.Version, journal.Owner, journal.Node = 1, rand.Text(), node
		journal.Class, journal.Handler = config.RuntimeClass, config.RuntimeHandler
		journal.NodeIP, journal.DNS, journal.Domain, journal.LogDir = config.NodeIP, config.ClusterDNS, config.ClusterDomain, config.LogDir
		journal.Records = make(map[string]*executionRecord)
		if err := journal.save(); err != nil {
			return nil, err
		}
		return journal, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open execution journal: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, file.Close()) }()
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() >= 4*1024*1024 {
		return nil, errors.New("execution journal is not a bounded regular file")
	}
	decoder := json.NewDecoder(io.LimitReader(file, 4*1024*1024))
	decoder.DisallowUnknownFields()
	var envelope struct {
		Data   []byte
		SHA256 string
	}
	if err := decoder.Decode(&envelope); err != nil {
		return nil, fmt.Errorf("decode execution journal: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("execution journal has trailing or oversized data")
	}
	sum := sha256.Sum256(envelope.Data)
	if envelope.SHA256 != hex.EncodeToString(sum[:]) {
		return nil, errors.New("execution journal checksum mismatch")
	}
	payload := json.NewDecoder(bytes.NewReader(envelope.Data))
	payload.DisallowUnknownFields()
	if err := payload.Decode(journal); err != nil {
		return nil, fmt.Errorf("decode execution journal payload: %w", err)
	}
	if err := payload.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("execution journal payload has trailing data")
	}
	if journal.Version != 1 || journal.Owner == "" || journal.Node != node ||
		journal.Class != config.RuntimeClass || journal.Handler != config.RuntimeHandler || journal.Records == nil ||
		journal.NodeIP != config.NodeIP || journal.DNS != config.ClusterDNS ||
		journal.Domain != config.ClusterDomain || journal.LogDir != config.LogDir {
		return nil, errors.New("execution journal identity, configuration, or schema mismatch")
	}
	for uid, record := range journal.Records {
		if record == nil || uid == "" || uid != record.UID || record.Namespace == "" || record.Name == "" ||
			len(record.Hash) != 64 || record.CPU <= 0 || record.Memory <= 0 ||
			len(record.DeletionObserved) > maxResourceVersion {
			return nil, errors.New("invalid execution journal record")
		}
		switch record.Stage {
		case "reserved", "sandbox-pending", "sandbox", "container-pending", "created", "start-pending", "running", "finished":
		default:
			return nil, errors.New("unknown execution journal stage")
		}
		if record.CreateRejected != "" && (!cri.IsRejectionCode(record.CreateRejected) ||
			record.Stage == "created" || record.Stage == "start-pending" || record.Stage == "running") {
			return nil, errors.New("invalid execution journal creation evidence")
		}
		if record.Stage != "reserved" && record.Stage != "sandbox-pending" && record.Stage != "finished" && record.SandboxID == "" {
			return nil, errors.New("execution journal lost sandbox identity")
		}
		if (record.Stage == "created" || record.Stage == "start-pending" || record.Stage == "running") && record.ContainerID == "" {
			return nil, errors.New("execution journal lost container identity")
		}
	}
	return journal, nil
}

func (j *executionJournal) close() error { return j.lock.Close() }

func (j *executionJournal) save() (retErr error) {
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("encode execution journal: %w", err)
	}
	sum := sha256.Sum256(data)
	data, err = json.Marshal(struct {
		Data   []byte
		SHA256 string
	}{Data: data, SHA256: hex.EncodeToString(sum[:])})
	if err != nil {
		return fmt.Errorf("encode execution journal envelope: %w", err)
	}
	if len(data) >= 4*1024*1024 || len(j.Records) > 10000 {
		return errors.New("execution journal limit reached; preserve state for operator recovery")
	}
	file, err := os.CreateTemp(j.dir, ".state-")
	if err != nil {
		return fmt.Errorf("create execution journal: %w", err)
	}
	name := file.Name()
	defer func() {
		retErr = errors.Join(retErr, file.Close())
		if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			retErr = errors.Join(retErr, err)
		}
	}()
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write execution journal: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync execution journal: %w", err)
	}
	if err := os.Rename(name, filepath.Join(j.dir, "state.json")); err != nil {
		return fmt.Errorf("commit execution journal: %w", err)
	}
	dir, err := os.Open(j.dir)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
