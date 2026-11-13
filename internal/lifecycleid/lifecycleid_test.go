// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycleid

import (
	"strings"
	"testing"
)

func TestUID(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value string
		ok    bool
	}{
		{name: "valid", value: "0f8fad5b-d9cb-469f-a165-70867728950e", ok: true},
		{name: "empty"},
		{name: "uppercase", value: "0F8FAD5B-D9CB-469F-A165-70867728950E"},
		{name: "missing dash", value: "0f8fad5bxd9cb-469f-a165-70867728950e"},
		{name: "dash in digit position", value: "-f8fad5b-d9cb-469f-a165-70867728950e"},
		{name: "too long", value: "0f8fad5b-d9cb-469f-a165-70867728950e0"},
		{name: "braced", value: "{f8fad5b-d9cb-469f-a165-70867728950}"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := UID("uid", tt.value); (err == nil) != tt.ok {
				t.Fatalf("UID(%q) error = %v, want ok %v", tt.value, err, tt.ok)
			}
		})
	}
}

func TestToken(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value string
		ok    bool
	}{
		{name: "simple", value: "op-1", ok: true},
		{name: "all punctuation", value: "a.b_c:d-e", ok: true},
		{name: "maximum", value: strings.Repeat("a", MaxTokenBytes), ok: true},
		{name: "empty"},
		{name: "too long", value: strings.Repeat("a", MaxTokenBytes+1)},
		{name: "leading dot", value: ".op"},
		{name: "slash", value: "op/1"},
		{name: "space", value: "op 1"},
		{name: "non ASCII", value: "opé"},
		{name: "NUL", value: "op\x00"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := Token("token", tt.value); (err == nil) != tt.ok {
				t.Fatalf("Token(%q) error = %v, want ok %v", tt.value, err, tt.ok)
			}
		})
	}
}

func TestLowerHex(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value string
		ok    bool
	}{
		{name: "valid", value: "00ff", ok: true},
		{name: "short", value: "00f"},
		{name: "uppercase", value: "00FF"},
		{name: "non hex", value: "00fg"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := LowerHex("hex", tt.value, 4); (err == nil) != tt.ok {
				t.Fatalf("LowerHex(%q) error = %v, want ok %v", tt.value, err, tt.ok)
			}
		})
	}
}

func TestEpoch(t *testing.T) {
	for _, tt := range []struct {
		value int64
		ok    bool
	}{{value: 1, ok: true}, {value: 0}, {value: -1}} {
		if err := Epoch("epoch", tt.value); (err == nil) != tt.ok {
			t.Fatalf("Epoch(%d) error = %v, want ok %v", tt.value, err, tt.ok)
		}
	}
}

func TestText(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value string
		ok    bool
	}{
		{name: "printable", value: "draining timed out", ok: true},
		{name: "empty"},
		{name: "too long", value: strings.Repeat("a", 21)},
		{name: "newline", value: "a\nb"},
		{name: "escape", value: "\x1b[2J"},
		{name: "non ASCII", value: "é"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := Text("text", tt.value, 20); (err == nil) != tt.ok {
				t.Fatalf("Text(%q) error = %v, want ok %v", tt.value, err, tt.ok)
			}
		})
	}
}
