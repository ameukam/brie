// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package podlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDirectory(t *testing.T) {
	t.Parallel()
	sum := sha256.Sum256([]byte("../uid"))
	if got, want := Directory("/var/log/brie", "../uid"), "/var/log/brie/"+hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("Directory = %q, want %q", got, want)
	}
}

func TestParseLine(t *testing.T) {
	t.Parallel()
	stamp := "2026-11-13T10:00:00.123456789Z"
	for _, tt := range []struct {
		name, line string
		want       Line
		err        string
	}{
		{name: "full", line: stamp + " stdout F hello world", want: Line{Stream: Stdout, Content: []byte("hello world")}},
		{name: "partial", line: stamp + " stderr P part", want: Line{Stream: Stderr, Partial: true, Content: []byte("part")}},
		{name: "empty content", line: stamp + " stdout F", want: Line{Stream: Stdout}},
		{name: "extra tags", line: stamp + " stdout F:x:y text", want: Line{Stream: Stdout, Content: []byte("text")}},
		{name: "content spaces", line: stamp + " stdout F  a  b ", want: Line{Stream: Stdout, Content: []byte(" a  b ")}},
		{name: "no stream", line: stamp, err: "no stream"},
		{name: "bad time", line: "yesterday stdout F x", err: "timestamp"},
		{name: "bad stream", line: stamp + " stdin F x", err: "stream"},
		{name: "missing tag", line: stamp + " stdout", err: "stream"},
		{name: "bad tag", line: stamp + " stdout X x", err: "tag"},
		{name: "oversized", line: stamp + " stdout F " + strings.Repeat("x", MaxLineBytes), err: "size bound"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseLine([]byte(tt.line))
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("error = %v, want %q", err, tt.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Time.IsZero() || got.Stream != tt.want.Stream || got.Partial != tt.want.Partial || !bytes.Equal(got.Content, tt.want.Content) {
				t.Fatalf("ParseLine = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func FuzzParseLine(f *testing.F) {
	for _, seed := range []string{
		"2026-11-13T10:00:00Z stdout F hello", "2026-11-13T10:00:00.1Z stderr P ", "x", "",
		"2026-11-13T10:00:00Z stdout F:a text", "2026-11-13T10:00:00+02:00 stdout F x",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		line, err := ParseLine(data)
		if err != nil {
			return
		}
		if line.Stream != Stdout && line.Stream != Stderr {
			t.Fatalf("accepted stream %q", line.Stream)
		}
		if len(line.Content) > len(data) {
			t.Fatal("content longer than input")
		}
		// Records parsed by the Reader match ParseLine exactly.
		reader := NewReader(bytes.NewReader(append(bytes.Clone(data), '\n')))
		again, err := reader.Next()
		if !bytes.ContainsRune(data, '\n') && (err != nil || !bytes.Equal(again.Content, line.Content)) {
			t.Fatalf("Reader disagrees with ParseLine: %v", err)
		}
	})
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestOpen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "0.log"), "data")
	outside := filepath.Join(t.TempDir(), "secret")
	writeFile(t, outside, "secret")
	if err := os.Symlink(outside, filepath.Join(dir, "link.log")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0700); err != nil {
		t.Fatal(err)
	}
	linkedDir := filepath.Join(t.TempDir(), "linked")
	if err := os.Symlink(dir, linkedDir); err != nil {
		t.Fatal(err)
	}
	file, err := Open(dir, "0.log")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(file)
	if err != nil || string(data) != "data" {
		t.Fatalf("read = %q, %v", data, err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct{ dir, base, want string }{
		{dir, "", "plain file name"},
		{dir, "..", "plain file name"},
		{dir, "../0.log", "plain file name"},
		{dir, "link.log", "regular file"},
		{dir, "sub", "regular file"},
		{dir, "missing.log", "inspect container log"},
		{linkedDir, "0.log", "not a directory"},
		{filepath.Join(dir, "0.log"), "x", "not a directory"},
		{filepath.Join(dir, "absent"), "x", "inspect Pod log directory"},
	} {
		if _, err := Open(tt.dir, tt.base); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("Open(%q, %q) error = %v, want %q", tt.dir, tt.base, err, tt.want)
		}
	}
}

func TestTailOffset(t *testing.T) {
	t.Parallel()
	content := "a\nbb\nccc\n"
	for _, tt := range []struct {
		name    string
		content string
		n       int64
		maxScan int64
		want    string
		err     string
	}{
		{name: "all", content: content, n: -1, maxScan: 100, want: content},
		{name: "none", content: content, n: 0, maxScan: 100, want: ""},
		{name: "one", content: content, n: 1, maxScan: 100, want: "ccc\n"},
		{name: "two", content: content, n: 2, maxScan: 100, want: "bb\nccc\n"},
		{name: "more than present", content: content, n: 10, maxScan: 100, want: content},
		{name: "no trailing newline", content: "a\nb", n: 1, maxScan: 100, want: "b"},
		{name: "empty", content: "", n: 3, maxScan: 100, want: ""},
		{name: "large", content: strings.Repeat("x\n", 40000), n: 2, maxScan: 1 << 20, want: "x\nx\n"},
		{name: "scan bound", content: strings.Repeat("x", 100000) + "\n", n: 2, maxScan: 1000, err: "scan bound"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			reader := strings.NewReader(tt.content)
			offset, err := TailOffset(reader, int64(len(tt.content)), tt.n, tt.maxScan)
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("error = %v, want %q", err, tt.err)
				}
				return
			}
			if err != nil || tt.content[offset:] != tt.want {
				t.Fatalf("tail = %q, %v, want %q", tt.content[offset:], err, tt.want)
			}
		})
	}
	if _, err := TailOffset(failingReaderAt{}, 10, 1, 100); err == nil {
		t.Fatal("TailOffset ignored a read error")
	}
}

type failingReaderAt struct{}

func (failingReaderAt) ReadAt([]byte, int64) (int, error) { return 0, errors.New("io failure") }

type chunkReader struct {
	chunks [][]byte
	err    error
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(r.chunks) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		return 0, io.EOF
	}
	n := copy(p, r.chunks[0])
	r.chunks[0] = r.chunks[0][n:]
	if len(r.chunks[0]) == 0 {
		r.chunks = r.chunks[1:]
	}
	return n, nil
}

func TestReader(t *testing.T) {
	t.Parallel()
	stamp := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	source := &chunkReader{chunks: [][]byte{
		[]byte(stamp + " stdout F one\n" + "garbage\n" + stamp + " std"),
		[]byte("err P two"),
	}}
	reader := NewReader(source)
	line, err := reader.Next()
	if err != nil || string(line.Content) != "one" {
		t.Fatalf("first = %+v, %v", line, err)
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("incomplete record = %v, want EOF", err)
	}
	if reader.Skipped != 1 {
		t.Fatalf("skipped = %d, want 1", reader.Skipped)
	}
	source.chunks = append(source.chunks, []byte("\n"+stamp+" stdout F "+strings.Repeat("x", MaxLineBytes+10)+"\n"+stamp+" stdout F three\n"))
	line, err = reader.Next()
	if err != nil || string(line.Content) != "two" || !line.Partial || line.Stream != Stderr {
		t.Fatalf("resumed = %+v, %v", line, err)
	}
	line, err = reader.Next()
	if err != nil || string(line.Content) != "three" || reader.Skipped != 2 {
		t.Fatalf("after oversized = %+v, %v, skipped %d", line, err, reader.Skipped)
	}
	failing := NewReader(&chunkReader{err: errors.New("disk")})
	if _, err := failing.Next(); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("read error = %v", err)
	}
}
