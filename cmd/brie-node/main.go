// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Command brie-node exposes version information and opt-in host registration.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ameukam/brie/internal/nodeagent"
)

// Set by GoReleaser at link time.
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	if err := run(ctx, os.Args[1:], version, os.Stdout, os.Stderr); err != nil {
		stop()
		log.New(os.Stderr, "brie-node: ", 0).Print(err)
		os.Exit(1)
	}
	stop()
}

func run(ctx context.Context, args []string, buildVersion string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("brie-node", flag.ContinueOnError)
	flags.SetOutput(stderr)
	showVersion := flags.Bool("version", false, "Print the version and exit")
	runAgent := flags.Bool("run", false, "Run until SIGINT or SIGTERM (registration only when --kubeconfig is set)")
	nodeName := flags.String("node-name", "", "Explicit lowercase DNS node name (required with --run)")
	kubeconfig := flags.String("kubeconfig", "", "Explicit kubeconfig for registration only (requires --run; never accepts workloads)")

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("parse flags: %w", err)
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %q", flags.Args())
	}
	if *runAgent && *showVersion {
		return errors.New("--run and --version cannot be combined")
	}
	if !*runAgent {
		var hasAgentConfig bool
		flags.Visit(func(f *flag.Flag) {
			if f.Name == "node-name" || f.Name == "kubeconfig" {
				hasAgentConfig = true
			}
		})
		if hasAgentConfig {
			return errors.New("--node-name requires --run; --kubeconfig also requires --run")
		}
	}
	var emptyKubeconfig bool
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "kubeconfig" && *kubeconfig == "" {
			emptyKubeconfig = true
		}
	})
	if emptyKubeconfig {
		return errors.New("--kubeconfig must not be empty when supplied")
	}
	if *runAgent {
		if *kubeconfig != "" {
			return nodeagent.Register(ctx, nodeagent.Config{NodeName: *nodeName}, *kubeconfig, stderr)
		}
		return nodeagent.Run(ctx, nodeagent.Config{NodeName: *nodeName}, stderr)
	}
	if !*showVersion {
		return errors.New("node execution is not implemented yet; use --help or --version")
	}

	if _, err := fmt.Fprintf(stdout, "brie-node %s\n", buildVersion); err != nil {
		return fmt.Errorf("write version: %w", err)
	}
	return nil
}
