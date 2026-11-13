// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package serving

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/ameukam/brie/internal/podlog"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Log request bounds.
const (
	// MaxLogBytes caps every log response, including follow sessions.
	MaxLogBytes = 128 * 1024 * 1024
	// MaxTailLines caps tailLines.
	MaxTailLines = 100000
	// maxTailScan bounds the bytes scanned backwards to find tail lines.
	maxTailScan = 64 * 1024 * 1024
	// followPoll is the follow polling interval.
	followPoll = 250 * time.Millisecond
)

// LogOptions are the supported kubelet containerLogs query parameters.
type LogOptions struct {
	Follow     bool
	Timestamps bool
	// TailLines is negative when unset.
	TailLines int64
	// LimitBytes is the response cap, at most MaxLogBytes.
	LimitBytes int64
	// Since is zero when unset.
	Since time.Time
}

// ParseLogOptions parses and bounds containerLogs query parameters. Unknown
// parameters are ignored, as by kubelet; previous=true is refused because
// Brie never restarts containers.
func ParseLogOptions(query url.Values, now time.Time) (LogOptions, error) {
	options := LogOptions{TailLines: -1, LimitBytes: MaxLogBytes}
	boolean := func(name string) (bool, error) {
		value := query.Get(name)
		if value == "" {
			return false, nil
		}
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return false, fmt.Errorf("%s must be a boolean", name)
		}
		return parsed, nil
	}
	var err error
	if options.Follow, err = boolean("follow"); err != nil {
		return LogOptions{}, err
	}
	if options.Timestamps, err = boolean("timestamps"); err != nil {
		return LogOptions{}, err
	}
	previous, err := boolean("previous")
	if err != nil {
		return LogOptions{}, err
	}
	if previous {
		return LogOptions{}, errors.New("previous container logs do not exist: Brie never restarts containers")
	}
	if value := query.Get("tailLines"); value != "" {
		lines, err := strconv.ParseInt(value, 10, 64)
		if err != nil || lines < 0 || lines > MaxTailLines {
			return LogOptions{}, fmt.Errorf("tailLines must be between 0 and %d", MaxTailLines)
		}
		options.TailLines = lines
	}
	if value := query.Get("limitBytes"); value != "" {
		limit, err := strconv.ParseInt(value, 10, 64)
		if err != nil || limit < 1 {
			return LogOptions{}, errors.New("limitBytes must be a positive integer")
		}
		options.LimitBytes = min(limit, MaxLogBytes)
	}
	seconds, sinceTime := query.Get("sinceSeconds"), query.Get("sinceTime")
	if seconds != "" && sinceTime != "" {
		return LogOptions{}, errors.New("sinceSeconds and sinceTime are mutually exclusive")
	}
	if seconds != "" {
		parsed, err := strconv.ParseInt(seconds, 10, 64)
		if err != nil || parsed < 1 || parsed > 10*365*24*3600 {
			return LogOptions{}, errors.New("sinceSeconds must be a positive number of seconds up to ten years")
		}
		options.Since = now.Add(-time.Duration(parsed) * time.Second)
	}
	if sinceTime != "" {
		parsed, err := time.Parse(time.RFC3339, sinceTime)
		if err != nil {
			return LogOptions{}, errors.New("sinceTime must be an RFC 3339 timestamp")
		}
		options.Since = parsed
	}
	return options, nil
}

func validObjectNames(namespace, pod, container string) bool {
	return len(validation.IsDNS1123Label(namespace)) == 0 && len(validation.IsDNS1123Subdomain(pod)) == 0 &&
		(container == "" || len(validation.IsDNS1123Label(container)) == 0)
}

func (s *Server) containerLogs(w http.ResponseWriter, r *http.Request) {
	namespace, pod, container := r.PathValue("namespace"), r.PathValue("pod"), r.PathValue("container")
	if !validObjectNames(namespace, pod, container) {
		http.Error(w, "invalid namespace, pod, or container name", http.StatusBadRequest)
		return
	}
	options, err := ParseLogOptions(r.URL.Query(), time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	file, err := s.openContainerLog(r.Context(), namespace, pod, container)
	if err != nil {
		s.fail(w, r, "container log open", err)
		return
	}
	defer func() { _ = file.Close() }()
	// Every failure that can precede output is reported before the status is
	// committed, so a failed tail is never sent as an empty success.
	if err := seekLogStart(file, options.TailLines); err != nil {
		s.fail(w, r, "container log seek", err)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	if err := s.copyLogs(r.Context(), w, file, options); err != nil && r.Context().Err() == nil {
		s.logger.WarnContext(r.Context(), "container log stream ended",
			"operation", "container log stream", "code", errorCode(err))
	}
}

// openContainerLog opens the container's current CRI log only when the
// runtime-reported path is a plain file directly inside the Pod's own log
// directory under the configured root.
func (s *Server) openContainerLog(ctx context.Context, namespace, pod, container string) (*os.File, error) {
	t, err := s.locate(ctx, namespace, pod, container)
	if err != nil {
		return nil, err
	}
	status, err := s.runtime.ContainerStatus(ctx, t.container.GetId())
	if err != nil {
		return nil, fmt.Errorf("container status: %w", err)
	}
	dir := podlog.Directory(s.config.LogDir, t.podUID)
	logPath := status.GetLogPath()
	if logPath == "" || filepath.Clean(logPath) != logPath || filepath.Dir(logPath) != dir {
		return nil, refuse(http.StatusForbidden, "container log path is outside the Pod log directory")
	}
	file, err := podlog.Open(dir, filepath.Base(logPath))
	if err != nil {
		return nil, refuse(http.StatusNotFound, "container log is unavailable: %v", err)
	}
	return file, nil
}

type limitedWriter struct {
	w         io.Writer
	remaining int64
}

var errLogLimit = errors.New("log byte limit reached")

func (l *limitedWriter) Write(p []byte) (int, error) {
	if l.remaining <= 0 {
		return 0, errLogLimit
	}
	truncated := int64(len(p)) > l.remaining
	if truncated {
		p = p[:l.remaining]
	}
	n, err := l.w.Write(p)
	l.remaining -= int64(n)
	if err == nil && truncated {
		err = errLogLimit
	}
	return n, err
}

// seekLogStart positions file at the first of its last tailLines lines, or at
// the start when tailLines is negative. A tail beyond the scan bound is refused.
func seekLogStart(file *os.File, tailLines int64) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat container log: %w", err)
	}
	offset, err := podlog.TailOffset(file, info.Size(), tailLines, maxTailScan)
	if errors.Is(err, podlog.ErrTailScanBound) {
		return refuse(http.StatusUnprocessableEntity,
			"tailLines=%d reaches further back than the %d-byte log scan bound; request fewer lines", tailLines, maxTailScan)
	}
	if err != nil {
		return err
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek container log: %w", err)
	}
	return nil
}

// copyLogs writes the selected records from file's current offset as kubelet
// does: content only, or prefixed by the record's RFC 3339 timestamp. Follow
// polls for growth and reopens the current file after rotation until the
// client leaves or the byte limit is reached.
func (s *Server) copyLogs(ctx context.Context, w http.ResponseWriter, file *os.File, options LogOptions) error {
	out := &limitedWriter{w: w, remaining: options.LimitBytes}
	flusher, _ := w.(http.Flusher)
	current := file
	defer func() {
		if current != file {
			_ = current.Close()
		}
	}()
	reader := podlog.NewReader(current)
	for {
		line, err := reader.Next()
		if err == nil {
			if !options.Since.IsZero() && line.Time.Before(options.Since) {
				continue
			}
			if err := writeRecord(out, line, options.Timestamps); err != nil {
				if errors.Is(err, errLogLimit) {
					return nil
				}
				return err
			}
			continue
		}
		if !errors.Is(err, io.EOF) {
			return err
		}
		if !options.Follow {
			return nil
		}
		if flusher != nil {
			flusher.Flush()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(followPoll):
		}
		next, rotated := reopenRotated(current)
		if rotated {
			// Drain the rotated file before switching to the new one.
			for {
				line, err := reader.Next()
				if err != nil {
					break
				}
				if options.Since.IsZero() || !line.Time.Before(options.Since) {
					if err := writeRecord(out, line, options.Timestamps); err != nil {
						_ = next.Close()
						if errors.Is(err, errLogLimit) {
							return nil
						}
						return err
					}
				}
			}
			if current != file {
				_ = current.Close()
			}
			current, reader = next, podlog.NewReader(next)
		}
	}
}

// reopenRotated opens the file now at the original path when rotation
// replaced it. Symlinks and changed directories are refused as on open.
func reopenRotated(current *os.File) (*os.File, bool) {
	name := current.Name()
	before, err := current.Stat()
	if err != nil {
		return nil, false
	}
	now, err := os.Lstat(name)
	if err != nil || os.SameFile(before, now) {
		return nil, false
	}
	next, err := podlog.Open(filepath.Dir(name), filepath.Base(name))
	if err != nil {
		return nil, false
	}
	return next, true
}

func writeRecord(w io.Writer, line podlog.Line, timestamps bool) error {
	buffer := make([]byte, 0, len(line.Content)+40)
	if timestamps {
		buffer = line.Time.UTC().AppendFormat(buffer, time.RFC3339Nano)
		buffer = append(buffer, ' ')
	}
	buffer = append(buffer, line.Content...)
	if !line.Partial {
		buffer = append(buffer, '\n')
	}
	_, err := w.Write(buffer)
	return err
}

// fail answers a refused or failed request. Unexpected failures are logged by
// operation name and bounded error code only.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, operation string, err error) {
	status, message := errorStatus(err)
	if status == http.StatusInternalServerError {
		s.logger.WarnContext(r.Context(), "serving request failed", "operation", operation, "code", errorCode(err))
	}
	http.Error(w, message, status)
}
