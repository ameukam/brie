// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package podlog locates, reads, and parses CRI container log files written by
// the runtime under the agent's Pod log root. It never follows symlinks and
// refuses files outside the Pod's own log directory.
package podlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Directory returns the Pod's log directory under root. The name is a hash of
// the Pod UID, matching the sandbox LogDirectory chosen by the executor, so
// tenant-controlled UIDs never become path components.
func Directory(root, podUID string) string {
	sum := sha256.Sum256([]byte(podUID))
	return filepath.Join(root, hex.EncodeToString(sum[:]))
}

// Stream names a CRI log stream.
type Stream string

// CRI log streams.
const (
	Stdout Stream = "stdout"
	Stderr Stream = "stderr"
)

// Line is one parsed CRI log record. Partial records are fragments of a longer
// line that continues in the next record.
type Line struct {
	Time    time.Time
	Stream  Stream
	Partial bool
	Content []byte
}

// MaxLineBytes bounds one CRI log record. containerd splits longer output.
const MaxLineBytes = 1024 * 1024

// ParseLine parses "<RFC3339Nano> <stream> <tags> <content>" without its
// trailing newline. The first tag is F (full) or P (partial); further
// colon-separated tags are ignored. Content may be empty.
func ParseLine(line []byte) (Line, error) {
	if len(line) > MaxLineBytes {
		return Line{}, errors.New("CRI log record exceeds its size bound")
	}
	timestamp, rest, found := bytes.Cut(line, []byte{' '})
	if !found {
		return Line{}, errors.New("CRI log record has no stream")
	}
	parsed, err := time.Parse(time.RFC3339Nano, string(timestamp))
	if err != nil {
		return Line{}, errors.New("CRI log record has an invalid timestamp")
	}
	stream, rest, found := bytes.Cut(rest, []byte{' '})
	if !found || (Stream(stream) != Stdout && Stream(stream) != Stderr) {
		return Line{}, errors.New("CRI log record has an invalid stream")
	}
	tags, content, _ := bytes.Cut(rest, []byte{' '})
	tag, _, _ := strings.Cut(string(tags), ":")
	if tag != "F" && tag != "P" {
		return Line{}, errors.New("CRI log record has an invalid tag")
	}
	return Line{Time: parsed, Stream: Stream(stream), Partial: tag == "P", Content: content}, nil
}

// Open opens a regular log file named base directly inside dir. It refuses
// path separators, symlinks for the directory or the file, and a file replaced
// between inspection and opening.
func Open(dir, base string) (*os.File, error) {
	if base == "" || base == "." || base == ".." || strings.ContainsAny(base, "/\\\x00") {
		return nil, errors.New("log file name must be a plain file name")
	}
	dirInfo, err := os.Lstat(dir)
	if err != nil {
		return nil, fmt.Errorf("inspect Pod log directory: %w", err)
	}
	if !dirInfo.IsDir() {
		return nil, errors.New("the Pod log directory is not a directory")
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open Pod log directory: %w", err)
	}
	defer func() { _ = root.Close() }()
	before, err := root.Lstat(base)
	if err != nil {
		return nil, fmt.Errorf("inspect container log: %w", err)
	}
	if !before.Mode().IsRegular() {
		return nil, errors.New("container log is not a regular file")
	}
	file, err := root.Open(base)
	if err != nil {
		return nil, fmt.Errorf("open container log: %w", err)
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) {
		_ = file.Close()
		return nil, errors.New("container log changed while opening")
	}
	return file, nil
}

// ErrTailScanBound reports that the requested tail lines start further back
// than the scan bound allows.
var ErrTailScanBound = errors.New("tail exceeds the log scan bound")

// TailOffset returns the offset of the last n lines of file, scanning at most
// maxScan bytes back from the end. A negative n selects the whole file.
func TailOffset(file io.ReaderAt, size int64, n int64, maxScan int64) (int64, error) {
	if n < 0 {
		return 0, nil
	}
	if n == 0 {
		return size, nil
	}
	const chunk = 32 * 1024
	buffer := make([]byte, chunk)
	end := size
	// A trailing newline terminates the last line rather than starting a new one.
	newlines := int64(0)
	skipTrailing := true
	for end > 0 && size-end < maxScan {
		start := max(0, end-chunk)
		read := buffer[:end-start]
		if _, err := file.ReadAt(read, start); err != nil && !errors.Is(err, io.EOF) {
			return 0, fmt.Errorf("read container log: %w", err)
		}
		for i := len(read) - 1; i >= 0; i-- {
			if read[i] != '\n' {
				skipTrailing = false
				continue
			}
			if skipTrailing {
				skipTrailing = false
				continue
			}
			newlines++
			if newlines == n {
				return start + int64(i) + 1, nil
			}
		}
		end = start
	}
	if end > 0 {
		return 0, ErrTailScanBound
	}
	return 0, nil
}

// Reader yields parsed records, skipping malformed and oversized records.
// Memory is bounded by MaxLineBytes plus one read chunk.
type Reader struct {
	source     io.Reader
	buffer     []byte
	discarding bool
	// Skipped counts malformed or oversized records.
	Skipped int
}

// NewReader reads CRI log records from r.
func NewReader(r io.Reader) *Reader { return &Reader{source: r} }

// Next returns the next complete record. io.EOF means no complete record is
// available yet; an incomplete trailing record is kept for the next call, so
// a follower can call Next again after the file grows.
func (r *Reader) Next() (Line, error) {
	for {
		raw, err := r.readRecord()
		if err != nil {
			return Line{}, err
		}
		line, err := ParseLine(raw)
		if err != nil {
			r.Skipped++
			continue
		}
		return line, nil
	}
}

const readChunk = 64 * 1024

func (r *Reader) readRecord() ([]byte, error) {
	for {
		if i := bytes.IndexByte(r.buffer, '\n'); i >= 0 {
			record := bytes.Clone(r.buffer[:i])
			r.buffer = r.buffer[i+1:]
			if r.discarding {
				r.discarding = false
				r.Skipped++
				continue
			}
			return record, nil
		}
		if len(r.buffer) > MaxLineBytes {
			r.discarding, r.buffer = true, r.buffer[:0]
		}
		if cap(r.buffer)-len(r.buffer) < readChunk/2 {
			grown := make([]byte, len(r.buffer), len(r.buffer)+readChunk)
			copy(grown, r.buffer)
			r.buffer = grown
		}
		n, err := r.source.Read(r.buffer[len(r.buffer):cap(r.buffer)])
		r.buffer = r.buffer[:len(r.buffer)+n]
		if n == 0 && err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("read container log: %w", err)
		}
	}
}
