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

// executionJournalVersion is the journal schema this agent writes. Version 1
// journals predate SandboxRemoved; they are read and rewritten as version 2,
// and their finished records that name a sandbox stay retained because they
// carry no removal evidence. A newer agent never reads a version it does not
// know, and an older agent refuses version 2.
const executionJournalVersion = 2

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
	// SandboxRemoved records that RemovePodSandbox succeeded for this
	// execution's sandbox. Compaction requires it for a record that names a
	// sandbox. Version 1 journals omit it.
	SandboxRemoved bool `json:",omitempty"`
	// Image is the admitted container image reference. Image collection
	// never removes an image that an unfinished record names, and refuses to
	// run while an unfinished record lacks it. Older journals omit it.
	Image string `json:",omitempty"`
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
		journal.Version, journal.Owner, journal.Node = executionJournalVersion, rand.Text(), node
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
	data, err := io.ReadAll(io.LimitReader(file, 4*1024*1024))
	if err != nil {
		return nil, fmt.Errorf("read execution journal: %w", err)
	}
	if err := journal.decode(data, node, config); err != nil {
		return nil, err
	}
	return journal, nil
}

// decode parses and validates a journal file for node and config and migrates
// a version 1 payload to the current schema in memory. The migrated schema is
// written by the next save.
func (j *executionJournal) decode(data []byte, node string, config ExecutionConfig) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var envelope struct {
		Data   []byte
		SHA256 string
	}
	if err := decoder.Decode(&envelope); err != nil {
		return fmt.Errorf("decode execution journal: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("execution journal has trailing or oversized data")
	}
	sum := sha256.Sum256(envelope.Data)
	if envelope.SHA256 != hex.EncodeToString(sum[:]) {
		return errors.New("execution journal checksum mismatch")
	}
	payload := json.NewDecoder(bytes.NewReader(envelope.Data))
	payload.DisallowUnknownFields()
	if err := payload.Decode(j); err != nil {
		return fmt.Errorf("decode execution journal payload: %w", err)
	}
	if err := payload.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("execution journal payload has trailing data")
	}
	if j.Version != 1 && j.Version != executionJournalVersion || j.Owner == "" || j.Node != node ||
		j.Class != config.RuntimeClass || j.Handler != config.RuntimeHandler || j.Records == nil ||
		j.NodeIP != config.NodeIP || j.DNS != config.ClusterDNS ||
		j.Domain != config.ClusterDomain || j.LogDir != config.LogDir {
		return errors.New("execution journal identity, configuration, or schema mismatch")
	}
	for uid, record := range j.Records {
		if record == nil || uid == "" || uid != record.UID || record.Namespace == "" || record.Name == "" ||
			len(record.Hash) != 64 || record.CPU <= 0 || record.Memory <= 0 ||
			len(record.DeletionObserved) > maxResourceVersion {
			return errors.New("invalid execution journal record")
		}
		switch record.Stage {
		case "reserved", "sandbox-pending", "sandbox", "container-pending", "created", "start-pending", "running", "finished":
		default:
			return errors.New("unknown execution journal stage")
		}
		if record.SandboxRemoved && (j.Version == 1 || record.Stage != "finished") {
			return errors.New("invalid execution journal removal evidence")
		}
		if record.CreateRejected != "" && (!cri.IsRejectionCode(record.CreateRejected) ||
			record.Stage == "created" || record.Stage == "start-pending" || record.Stage == "running") {
			return errors.New("invalid execution journal creation evidence")
		}
		if record.Stage != "reserved" && record.Stage != "sandbox-pending" && record.Stage != "finished" && record.SandboxID == "" {
			return errors.New("execution journal lost sandbox identity")
		}
		if (record.Stage == "created" || record.Stage == "start-pending" || record.Stage == "running") && record.ContainerID == "" {
			return errors.New("execution journal lost container identity")
		}
	}
	j.Version = executionJournalVersion
	return nil
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
