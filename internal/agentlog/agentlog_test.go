// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package agentlog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func decodeLines(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

func TestCorrelationFields(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	logger := New(&out, Options{})
	logger.Info("stage committed", PodUID("uid-1"), OperationID("op-7"), JournalStage("sandbox-pending"))
	records := decodeLines(t, out.Bytes())
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
	for key, want := range map[string]string{
		KeyPodUID: "uid-1", KeyOperationID: "op-7", KeyJournalStage: "sandbox-pending",
		"msg": "stage committed", "level": "INFO",
	} {
		if got := records[0][key]; got != want {
			t.Errorf("%s = %v, want %q", key, got, want)
		}
	}
	if _, ok := records[0]["time"]; !ok {
		t.Error("record has no timestamp")
	}
}

// TestRedaction is the regression test for DESIGN 12.3: environment values,
// commands and arguments, request bodies, and credentials never reach the log.
func TestRedaction(t *testing.T) {
	t.Parallel()
	const secret = "tenant-secret-value"
	for _, tt := range []struct {
		name string
		attr slog.Attr
	}{
		{name: "env", attr: slog.String("env", secret)},
		{name: "env list", attr: slog.Any("envs", []string{"A=" + secret})},
		{name: "env group", attr: slog.Group("env", slog.String("DATABASE_URL", secret))},
		{name: "nested group", attr: slog.Group("request", slog.Group("headers", slog.String("x", secret)))},
		{name: "command", attr: slog.Any("command", []string{"/bin/sh", "-c", secret})},
		{name: "cmd", attr: slog.String("cmd", secret)},
		{name: "args", attr: slog.Any("args", []string{secret})},
		{name: "request body", attr: slog.String("request_body", secret)},
		{name: "camel body", attr: slog.String("requestBody", secret)},
		{name: "body", attr: slog.String("body", secret)},
		{name: "token", attr: slog.String("bearerToken", secret)},
		{name: "password", attr: slog.String("password", secret)},
		{name: "authorization", attr: slog.String("Authorization", secret)},
		{name: "credentials", attr: slog.String("registry-credentials", secret)},
		{name: "private key", attr: slog.String("tls.private_key", secret)},
		{name: "cookie", attr: slog.String("Cookie", secret)},
		{name: "secret", attr: slog.String("secretData", secret)},
		{name: "stdin", attr: slog.String("stdin", secret)},
		{name: "snapshot", attr: slog.String("snapshot_contents", secret)},
		{name: "valuer", attr: slog.Any("env", valuer{secret})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var out bytes.Buffer
			logger := New(&out, Options{})
			logger.Info("exec request", tt.attr)
			logger.With(tt.attr).Error("bound attribute")
			logger.WithGroup("group").Warn("grouped", tt.attr)
			if strings.Contains(out.String(), secret) {
				t.Fatalf("sensitive value logged: %s", out.String())
			}
			if !strings.Contains(out.String(), Redacted) {
				t.Fatalf("redaction marker missing: %s", out.String())
			}
		})
	}
}

type valuer struct{ value string }

func (v valuer) LogValue() slog.Value { return slog.StringValue(v.value) }

func TestNonSensitiveKeysKept(t *testing.T) {
	t.Parallel()
	for _, key := range []string{KeyPodUID, KeyOperationID, KeyJournalStage, "method", "namespace", "reason", "error", "monkey", "envoy"} {
		if Sensitive(key) {
			t.Errorf("Sensitive(%q) = true", key)
		}
	}
	var out bytes.Buffer
	New(&out, Options{}).Info("ok", slog.String("method", "RunPodSandbox"))
	if !strings.Contains(out.String(), "RunPodSandbox") {
		t.Fatalf("ordinary attribute removed: %s", out.String())
	}
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

func TestRepeatedErrorRateLimit(t *testing.T) {
	t.Parallel()
	clock := &fakeClock{now: time.Unix(1000, 0)}
	var out bytes.Buffer
	logger := New(&out, Options{RepeatWindow: time.Minute, RepeatBurst: 2, Now: clock.Now})
	for range 10 {
		logger.Error("CRI ListPodSandbox failed", slog.String("error", "unavailable"))
	}
	logger.Error("different error")
	for range 5 {
		logger.Info("informational records are not limited")
	}
	if got := len(decodeLines(t, out.Bytes())); got != 2+1+5 {
		t.Fatalf("records = %d, want 8: %s", got, out.String())
	}
	out.Reset()
	clock.now = clock.now.Add(time.Minute)
	logger.Error("CRI ListPodSandbox failed", slog.String("error", "unavailable"))
	records := decodeLines(t, out.Bytes())
	if len(records) != 1 || records[0][KeySuppressed] != float64(8) {
		t.Fatalf("after window = %v, want one record reporting 8 suppressed", records)
	}
}

func TestRepeatLimiterScopesAndEviction(t *testing.T) {
	t.Parallel()
	clock := &fakeClock{now: time.Unix(1000, 0)}
	var out bytes.Buffer
	logger := New(&out, Options{RepeatBurst: 1, Now: clock.Now})
	logger.With(PodUID("a")).Error("failed")
	logger.With(PodUID("b")).Error("failed")
	logger.WithGroup("g").Error("failed")
	logger.With(PodUID("a")).Error("failed")
	if got := len(decodeLines(t, out.Bytes())); got != 3 {
		t.Fatalf("records = %d, want 3 distinct scopes", got)
	}
	handler := NewHandler(&out, Options{RepeatBurst: 1, Now: clock.Now}).(*limitHandler)
	for i := range maxRepeatKeys + 10 {
		clock.now = clock.now.Add(time.Millisecond)
		handler.limiter.admit(fmt.Sprint(i))
	}
	if got := len(handler.limiter.entries); got != maxRepeatKeys {
		t.Fatalf("limiter entries = %d, want bound %d", got, maxRepeatKeys)
	}
	if _, found := handler.limiter.entries["0"]; found {
		t.Fatal("oldest entry was not evicted")
	}
}

func TestNewErrorLogger(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	NewErrorLogger(NewHandler(&out, Options{Level: slog.LevelDebug})).Print(errors.New("reconcile failed"))
	records := decodeLines(t, out.Bytes())
	if len(records) != 1 || records[0]["level"] != "ERROR" || records[0]["msg"] != "reconcile failed" {
		t.Fatalf("records = %v", records)
	}
}

func TestLevelFilter(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	logger := New(&out, Options{Level: slog.LevelWarn})
	logger.Info("hidden")
	logger.Warn("shown")
	if records := decodeLines(t, out.Bytes()); len(records) != 1 || records[0]["msg"] != "shown" {
		t.Fatalf("records = %v", records)
	}
}

func TestNewLineWriter(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	writer := NewLineWriter(NewHandler(&out, Options{}), slog.LevelError)
	if _, err := fmt.Fprint(writer, "brie-node: first failure\nsecond failure\n\n"); err != nil {
		t.Fatal(err)
	}
	records := decodeLines(t, out.Bytes())
	if len(records) != 2 || records[0]["msg"] != "first failure" || records[1]["msg"] != "second failure" || records[0]["level"] != "ERROR" {
		t.Fatalf("records = %v", records)
	}
}

func TestTextFormatRedacts(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	New(&out, Options{Text: true}).Warn("exec", slog.String("command", "secret-arg"), PodUID("u1"))
	if text := out.String(); strings.Contains(text, "secret-arg") || !strings.Contains(text, "pod_uid=u1") || strings.HasPrefix(text, "{") {
		t.Fatalf("text record = %q", text)
	}
}
