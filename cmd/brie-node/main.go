// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Command brie-node exposes version information and opt-in node execution.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
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
	runAgent := flags.Bool("run", false, "Run until SIGINT or SIGTERM; workloads require --enable-workloads")
	nodeName := flags.String("node-name", "", "Explicit lowercase DNS node name (required with --run)")
	kubeconfig := flags.String("kubeconfig", "", "Explicit kubeconfig (requires --run; registration-only by default)")
	runtimeSocket := flags.String("runtime-socket", "", "Absolute CRI Unix socket (read-only unless --enable-workloads)")
	workloads := flags.Bool("enable-workloads", false, "Enable restricted Pod scheduling and CRI execution (requires explicit execution configuration)")
	execution := nodeagent.ExecutionConfig{}
	flags.StringVar(&execution.RuntimeClass, "runtime-class", "", "Approved Kata RuntimeClass name (requires --enable-workloads)")
	flags.StringVar(&execution.RuntimeHandler, "runtime-handler", "", "Approved containerd Kata/Firecracker handler")
	flags.StringVar(&execution.CPU, "allocatable-cpu", "", "Schedulable CPU budget after host reservations")
	flags.StringVar(&execution.Memory, "allocatable-memory", "", "Schedulable memory budget after host reservations")
	flags.IntVar(&execution.MaxPods, "max-pods", 0, "Maximum admitted Pods, including pending and unknown execution")
	flags.StringVar(&execution.NodeIP, "node-ip", "", "Node InternalIP")
	flags.StringVar(&execution.ClusterDNS, "cluster-dns", "", "Comma-separated cluster DNS IPs")
	flags.StringVar(&execution.ClusterDomain, "cluster-domain", "", "Cluster DNS domain")
	flags.StringVar(&execution.StateDir, "state-dir", "", "Private durable execution journal directory")
	flags.StringVar(&execution.LogDir, "pod-log-dir", "", "Dedicated host directory for CRI container logs")
	flags.StringVar(&execution.ImageGCThreshold, "image-gc-threshold", "",
		"Remove unreferenced images through CRI while image filesystem usage exceeds this many bytes, such as 50Gi (default disabled)")
	flags.BoolVar(&execution.Probes, "enable-probes", false,
		"Accept readiness and liveness probes, gate Pod readiness on them, and fail a Pod whose liveness probe fails (requires --enable-workloads)")
	executionFlags := map[string]bool{
		"runtime-class": true, "runtime-handler": true, "allocatable-cpu": true, "allocatable-memory": true,
		"max-pods": true, "node-ip": true, "cluster-dns": true, "cluster-domain": true, "state-dir": true, "pod-log-dir": true,
		"image-gc-threshold": true, "enable-probes": true,
	}
	observability := &observabilityOptions{}
	observability.register(flags)
	for _, name := range observabilityWorkloadFlags {
		executionFlags[name] = true
	}

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
			if f.Name == "node-name" || f.Name == "kubeconfig" || f.Name == "runtime-socket" ||
				f.Name == "enable-workloads" || f.Name == "log-format" || executionFlags[f.Name] {
				hasAgentConfig = true
			}
		})
		if hasAgentConfig {
			return errors.New("--node-name requires --run; --kubeconfig, --runtime-socket, and execution options also require --run")
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
	var emptyRuntimeSocket bool
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "runtime-socket" && *runtimeSocket == "" {
			emptyRuntimeSocket = true
		}
	})
	if emptyRuntimeSocket {
		return errors.New("--runtime-socket must not be empty when supplied")
	}
	var executionOnlyOption bool
	flags.Visit(func(f *flag.Flag) {
		executionOnlyOption = executionOnlyOption || executionFlags[f.Name]
	})
	if executionOnlyOption && !*workloads {
		return errors.New("execution configuration requires --enable-workloads")
	}
	visited := make(map[string]bool)
	flags.Visit(func(f *flag.Flag) { visited[f.Name] = true })
	if err := observability.validate(visited); err != nil {
		return err
	}
	if *runAgent {
		config := nodeagent.Config{NodeName: *nodeName, RuntimeSocket: *runtimeSocket}
		if *workloads {
			return runExecution(ctx, config, execution, *kubeconfig, stderr, observability)
		}
		output, _ := observability.agentOutput(stderr, slog.LevelInfo)
		if *kubeconfig != "" {
			return nodeagent.Register(ctx, config, *kubeconfig, output)
		}
		return nodeagent.Run(ctx, config, output)
	}
	if !*showVersion {
		return errors.New("no mode selected; use --help, --version, or --run")
	}

	if _, err := fmt.Fprintf(stdout, "brie-node %s\n", buildVersion); err != nil {
		return fmt.Errorf("write version: %w", err)
	}
	return nil
}
