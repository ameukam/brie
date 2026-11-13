// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package agentlog builds the node agent's structured JSON logger.
// Records carry Pod UID, operation ID, and journal stage fields; attributes
// whose keys name environment values, commands, arguments, request bodies, or
// credentials are redacted; and repeated errors are rate-limited.
//
// Redaction is key-based. Messages and error strings are emitted verbatim, so
// callers must never format tenant data into them.
package agentlog

import (
	"context"
	"io"
	"log"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode"
)

// Stable field names correlating agent logs with journal records (DESIGN 12.3).
const (
	KeyPodUID       = "pod_uid"
	KeyOperationID  = "operation_id"
	KeyJournalStage = "journal_stage"
	// KeySuppressed counts identical records dropped since the previous emission.
	KeySuppressed = "suppressed_repeats"
	// Redacted replaces every sensitive attribute value.
	Redacted = "[REDACTED]"
)

// PodUID returns the Pod UID correlation field.
func PodUID(uid string) slog.Attr { return slog.String(KeyPodUID, uid) }

// OperationID returns the lifecycle operation correlation field.
func OperationID(id string) slog.Attr { return slog.String(KeyOperationID, id) }

// JournalStage returns the execution journal stage field.
func JournalStage(stage string) slog.Attr { return slog.String(KeyJournalStage, stage) }

// Options configures the logger. Zero values select conservative defaults.
type Options struct {
	// Level is the minimum emitted level; the default is Info.
	Level slog.Leveler
	// RepeatWindow and RepeatBurst allow at most RepeatBurst identical
	// warning-or-higher records per window. Defaults: one minute, five records.
	RepeatWindow time.Duration
	RepeatBurst  int
	// Now overrides the clock for tests.
	Now func() time.Time
}

// sensitiveTokens are key words whose attribute values are never logged.
// Keys are split on case changes and non-alphanumeric runes before matching.
var sensitiveTokens = map[string]bool{
	"env": true, "envs": true, "environ": true, "environment": true,
	"command": true, "commands": true, "cmd": true, "args": true, "arg": true, "argv": true,
	"body": true, "payload": true, "header": true, "headers": true,
	"token": true, "tokens": true, "password": true, "passwd": true, "secret": true, "secrets": true,
	"credential": true, "credentials": true, "authorization": true, "auth": true,
	"cookie": true, "cookies": true, "bearer": true, "key": true, "apikey": true, "privatekey": true,
	"stdin": true, "snapshot": true,
}

// Sensitive reports whether an attribute key names data that must not be logged.
func Sensitive(key string) bool {
	for _, token := range keyTokens(key) {
		if sensitiveTokens[token] {
			return true
		}
	}
	return false
}

func keyTokens(key string) []string {
	var tokens []string
	var current strings.Builder
	var previous rune
	flush := func() {
		if current.Len() > 0 {
			tokens = append(tokens, current.String())
			current.Reset()
		}
	}
	for _, r := range key {
		switch {
		case unicode.IsUpper(r):
			if unicode.IsLower(previous) || unicode.IsDigit(previous) {
				flush()
			}
			current.WriteRune(unicode.ToLower(r))
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			current.WriteRune(r)
		default:
			flush()
		}
		previous = r
	}
	flush()
	return tokens
}

func redact(groups []string, attr slog.Attr) slog.Attr {
	for _, group := range groups {
		if Sensitive(group) {
			return slog.String(attr.Key, Redacted)
		}
	}
	if Sensitive(attr.Key) {
		return slog.String(attr.Key, Redacted)
	}
	return attr
}

// NewHandler returns a redacting, rate-limited JSON handler writing to w.
func NewHandler(w io.Writer, options Options) slog.Handler {
	level := options.Level
	if level == nil {
		level = slog.LevelInfo
	}
	window, burst, now := options.RepeatWindow, options.RepeatBurst, options.Now
	if window <= 0 {
		window = time.Minute
	}
	if burst <= 0 {
		burst = 5
	}
	if now == nil {
		now = time.Now
	}
	encoder := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level, ReplaceAttr: redact})
	return &limitHandler{next: encoder, limiter: &repeatLimiter{
		window: window, burst: burst, now: now, entries: make(map[string]*repeatEntry),
	}}
}

// New returns a structured logger; see NewHandler.
func New(w io.Writer, options Options) *slog.Logger {
	return slog.New(NewHandler(w, options))
}

// NewErrorLogger adapts a structured handler for code that still prints
// errors through the standard log package. Each line becomes an error record.
func NewErrorLogger(handler slog.Handler) *log.Logger {
	return slog.NewLogLogger(handler, slog.LevelError)
}

// maxRepeatKeys bounds the limiter's memory; when full, the oldest window is evicted.
const maxRepeatKeys = 1024

type repeatEntry struct {
	start      time.Time
	count      int
	suppressed int
}

type repeatLimiter struct {
	mu      sync.Mutex
	window  time.Duration
	burst   int
	now     func() time.Time
	entries map[string]*repeatEntry
}

// admit reports whether a record may be emitted and how many identical
// records were suppressed before it.
func (l *repeatLimiter) admit(key string) (bool, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	entry, found := l.entries[key]
	if !found {
		if len(l.entries) >= maxRepeatKeys {
			l.evictOldest()
		}
		l.entries[key] = &repeatEntry{start: now, count: 1}
		return true, 0
	}
	if now.Sub(entry.start) >= l.window {
		suppressed := entry.suppressed
		*entry = repeatEntry{start: now, count: 1}
		return true, suppressed
	}
	if entry.count < l.burst {
		entry.count++
		return true, 0
	}
	entry.suppressed++
	return false, 0
}

func (l *repeatLimiter) evictOldest() {
	var oldestKey string
	var oldest time.Time
	for key, entry := range l.entries {
		if oldestKey == "" || entry.start.Before(oldest) {
			oldestKey, oldest = key, entry.start
		}
	}
	delete(l.entries, oldestKey)
}

type limitHandler struct {
	next    slog.Handler
	limiter *repeatLimiter
	scope   string
}

func (h *limitHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *limitHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Level < slog.LevelWarn {
		return h.next.Handle(ctx, record)
	}
	var key strings.Builder
	key.WriteString(h.scope)
	key.WriteString(record.Level.String())
	key.WriteByte(0)
	key.WriteString(record.Message)
	record.Attrs(func(attr slog.Attr) bool {
		key.WriteByte(0)
		key.WriteString(attr.Key)
		key.WriteByte('=')
		key.WriteString(attr.Value.String())
		return true
	})
	admitted, suppressed := h.limiter.admit(key.String())
	if !admitted {
		return nil
	}
	if suppressed > 0 {
		record = record.Clone()
		record.AddAttrs(slog.Int(KeySuppressed, suppressed))
	}
	return h.next.Handle(ctx, record)
}

func (h *limitHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	var scope strings.Builder
	scope.WriteString(h.scope)
	for _, attr := range attrs {
		scope.WriteString(attr.Key)
		scope.WriteByte('=')
		scope.WriteString(attr.Value.String())
		scope.WriteByte(0)
	}
	return &limitHandler{next: h.next.WithAttrs(attrs), limiter: h.limiter, scope: scope.String()}
}

func (h *limitHandler) WithGroup(name string) slog.Handler {
	return &limitHandler{next: h.next.WithGroup(name), limiter: h.limiter, scope: h.scope + name + "\x00"}
}
