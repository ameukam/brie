//go:build brie_guest

// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Command guest-init is a synthetic PID 1 for the disposable boot experiment.
package main

import (
	"fmt"
	"os"
	"time"
)

func main() {
	if _, err := fmt.Fprintln(os.Stdout, "BRIE_SYNTHETIC_GUEST_BOOT_OK"); err != nil {
		panic(err)
	}
	for {
		time.Sleep(time.Hour)
	}
}
