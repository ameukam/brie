// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRun(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{name: "help", args: []string{"--help"}},
		{name: "unknown flag", args: []string{"--unknown"}, want: "parse owner flags"},
		{name: "positional", args: []string{"unexpected"}, want: "no positional"},
		{name: "missing configuration", want: "absolute fresh directory"},
		{name: "invalid duration", args: []string{"--timeout=bad"}, want: "parse owner flags"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer
			err := run(t.Context(), tt.args, &output)
			if tt.want == "" {
				if err != nil || !strings.Contains(output.String(), "Usage of brie-owner") {
					t.Fatalf("help: %v %q", err, output.String())
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("run: %v, want %s", err, tt.want)
			}
		})
	}
}
