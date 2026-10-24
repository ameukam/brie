// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Command brie-node exposes version information for the Brie node-agent prototype.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
)

// Set by GoReleaser at link time.
var version = "dev"

func main() {
	if err := run(os.Args[1:], version, os.Stdout, os.Stderr); err != nil {
		log.New(os.Stderr, "brie-node: ", 0).Print(err)
		os.Exit(1)
	}
}

func run(args []string, buildVersion string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("brie-node", flag.ContinueOnError)
	flags.SetOutput(stderr)
	showVersion := flags.Bool("version", false, "Print the version and exit")

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("parse flags: %w", err)
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %q", flags.Args())
	}
	if !*showVersion {
		return errors.New("node execution is not implemented yet; use --help or --version")
	}

	if _, err := fmt.Fprintf(stdout, "brie-node %s\n", buildVersion); err != nil {
		return fmt.Errorf("write version: %w", err)
	}
	return nil
}
