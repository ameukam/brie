// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/ameukam/brie/internal/agentlog"
	"github.com/ameukam/brie/internal/cri"
	"github.com/ameukam/brie/internal/events"
	"github.com/ameukam/brie/internal/logrotation"
	"github.com/ameukam/brie/internal/metrics"
	"github.com/ameukam/brie/internal/nodeagent"
	"github.com/ameukam/brie/internal/serving"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"
)

// observabilityOptions are the opt-in serving, Events, rotation, and log
// format settings. Every service is off by default.
type observabilityOptions struct {
	logFormat    string
	serving      bool
	address      string
	certFile     string
	keyFile      string
	clientCAFile string
	events       bool
	logMaxSize   string
	logMaxFiles  int
	maxSizeBytes int64
}

// observabilityWorkloadFlags require --enable-workloads.
var observabilityWorkloadFlags = []string{
	"enable-serving", "serving-address", "tls-cert-file", "tls-private-key-file", "client-ca-file",
	"enable-events", "container-log-max-size", "container-log-max-files",
}

func (o *observabilityOptions) register(flags *flag.FlagSet) {
	flags.StringVar(&o.logFormat, "log-format", "text", "Agent log format: text or json (json is structured, redacted, and rate-limited)")
	flags.BoolVar(&o.serving, "enable-serving", false, "Serve the authenticated kubelet API subset: healthz, metrics, logs, exec, attach, port-forward (requires --enable-workloads)")
	flags.StringVar(&o.address, "serving-address", serving.DefaultAddress, "HTTPS serving address (requires --enable-serving)")
	flags.StringVar(&o.certFile, "tls-cert-file", "", "Absolute PEM serving certificate (requires --enable-serving)")
	flags.StringVar(&o.keyFile, "tls-private-key-file", "", "Absolute PEM serving private key (requires --enable-serving)")
	flags.StringVar(&o.clientCAFile, "client-ca-file", "", "Absolute PEM CA bundle verifying client certificates (requires --enable-serving)")
	flags.BoolVar(&o.events, "enable-events", false, "Record bounded Kubernetes Events (requires --enable-workloads)")
	flags.StringVar(&o.logMaxSize, "container-log-max-size", "", "Rotate container logs above this size, such as 10Mi (enables rotation; requires --enable-workloads)")
	flags.IntVar(&o.logMaxFiles, "container-log-max-files", 5, "Container log files kept per container, including the current one (requires --container-log-max-size)")
}

// validate checks flag combinations after parsing; visited names the flags set.
func (o *observabilityOptions) validate(visited map[string]bool) error {
	if o.logFormat != "text" && o.logFormat != "json" {
		return errors.New("--log-format must be text or json")
	}
	for _, name := range []string{"serving-address", "tls-cert-file", "tls-private-key-file", "client-ca-file"} {
		if visited[name] && !o.serving {
			return errors.New("--serving-address and TLS files require --enable-serving")
		}
	}
	if o.serving && (o.certFile == "" || o.keyFile == "" || o.clientCAFile == "") {
		return errors.New("--enable-serving requires --tls-cert-file, --tls-private-key-file, and --client-ca-file")
	}
	if visited["container-log-max-files"] && o.logMaxSize == "" {
		return errors.New("--container-log-max-files requires --container-log-max-size")
	}
	if o.logMaxSize != "" {
		size, err := logrotation.ParseMaxSize(o.logMaxSize)
		if err != nil {
			return err
		}
		if o.logMaxFiles < logrotation.MinMaxFiles || o.logMaxFiles > logrotation.MaxMaxFiles {
			return fmt.Errorf("--container-log-max-files must be between %d and %d", logrotation.MinMaxFiles, logrotation.MaxMaxFiles)
		}
		o.maxSizeBytes = size
	}
	return nil
}

func (o *observabilityOptions) services() bool {
	return o.serving || o.events || o.logMaxSize != ""
}

// agentOutput returns the writer the agent logs to and a logger for the
// observability services. JSON turns each agent line into a record at level.
func (o *observabilityOptions) agentOutput(stderr io.Writer, level slog.Level) (io.Writer, *slog.Logger) {
	handler := agentlog.NewHandler(stderr, agentlog.Options{Text: o.logFormat != "json"})
	if o.logFormat == "json" {
		return agentlog.NewLineWriter(handler, level), slog.New(handler)
	}
	return stderr, slog.New(handler)
}

// barrierTracker turns readiness gate inputs into the barrier metric and
// Events on entry and exit.
type barrierTracker struct {
	mu       sync.Mutex
	observed bool
	cause    metrics.BarrierCause
	metrics  *metrics.Metrics
	recorder *events.Recorder
}

func barrierCause(state nodeagent.HealthState) metrics.BarrierCause {
	switch {
	case state.RecoveryRequired:
		return metrics.BarrierRecoveryRequired
	case !state.RuntimeHealthy:
		return metrics.BarrierRuntimeUnhealthy
	case !state.Synced:
		return metrics.BarrierIncompletePass
	}
	return metrics.BarrierNone
}

func (b *barrierTracker) observe(state nodeagent.HealthState) {
	cause := barrierCause(state)
	b.mu.Lock()
	changed := !b.observed || cause != b.cause
	b.observed, b.cause = true, cause
	b.mu.Unlock()
	if b.metrics != nil {
		b.metrics.SetBarrier(cause)
	}
	if !changed || b.recorder == nil {
		return
	}
	if cause == metrics.BarrierNone {
		b.recorder.BarrierLowered()
		return
	}
	b.recorder.BarrierRaised(string(cause))
}

func budgetMillicoresAndBytes(execution nodeagent.ExecutionConfig) (int64, int64) {
	cpu, cpuErr := resource.ParseQuantity(execution.CPU)
	memory, memoryErr := resource.ParseQuantity(execution.Memory)
	if cpuErr != nil || memoryErr != nil {
		return 0, 0
	}
	return cpu.MilliValue(), memory.Value()
}

// runExecution runs the executor and the enabled observability services.
// Without services it is exactly nodeagent.Execute. A service that stops
// before shutdown stops the agent; workloads keep running, as on any exit.
func runExecution(ctx context.Context, node nodeagent.Config, execution nodeagent.ExecutionConfig,
	kubeconfig string, stderr io.Writer, o *observabilityOptions) (retErr error) {
	// The executor writes only failures to its log.
	output, logger := o.agentOutput(stderr, slog.LevelError)
	if !o.services() {
		return nodeagent.Execute(ctx, node, execution, kubeconfig, output)
	}
	if err := node.Validate(); err != nil {
		return fmt.Errorf("validate agent configuration: %w", err)
	}
	if err := execution.Validate(); err != nil {
		return fmt.Errorf("validate execution configuration: %w", err)
	}
	if node.RuntimeSocket == "" || kubeconfig == "" {
		return errors.New("execution requires --runtime-socket and --kubeconfig")
	}
	if ctx.Err() != nil {
		// Report shutdown exactly as the executor does, without starting services.
		return nodeagent.ExecuteObserved(ctx, node, execution, kubeconfig, output, nodeagent.ExecutionHooks{})
	}
	restConfig, err := nodeagent.APIConfig(kubeconfig, "brie-node/observability")
	if err != nil {
		return err
	}
	api, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("create observability API client: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var hooks nodeagent.ExecutionHooks
	var services []func(context.Context) error
	tracker := &barrierTracker{}
	var registry *metrics.Metrics
	if o.serving {
		registry = metrics.New(nil)
		cpu, memory := budgetMillicoresAndBytes(execution)
		registry.SetBudget(cpu, memory, execution.MaxPods)
		hooks.CRI = registry.ObserveCRI
		hooks.Pass = func(err error, at time.Time) {
			if err == nil {
				registry.PassCompleted(at)
			}
		}
		hooks.LeaseRenewal = func(err error) {
			if err != nil {
				registry.LeaseRenewalFailed()
			}
		}
		tracker.metrics = registry
		runtime, err := cri.New(node.RuntimeSocket, 30*time.Second)
		if err != nil {
			return err
		}
		defer func() { retErr = errors.Join(retErr, runtime.Close()) }()
		runtime.SetObserver(registry.ObserveCRI)
		config := serving.Config{
			Address: o.address, CertFile: o.certFile, KeyFile: o.keyFile, ClientCAFile: o.clientCAFile,
			NodeName: node.NodeName, RuntimeHandler: execution.RuntimeHandler, LogDir: execution.LogDir,
		}
		server, err := serving.New(config, serving.Dependencies{
			API: api, Runtime: runtime, Gatherer: registry.Gatherer(), Logger: logger,
		})
		if err != nil {
			return fmt.Errorf("configure serving: %w", err)
		}
		if _, err := serving.TLSConfig(config); err != nil {
			return fmt.Errorf("configure serving: %w", err)
		}
		services = append(services, server.Run)
	}
	if o.events {
		options := events.Options{}
		if registry != nil {
			options.Observe = registry.Event
		}
		recorder := events.New(ctx, api, node.NodeName, options)
		defer recorder.Shutdown()
		tracker.recorder = recorder
	}
	if tracker.metrics != nil || tracker.recorder != nil {
		hooks.Health = tracker.observe
	}
	if o.logMaxSize != "" {
		runtime, err := cri.New(node.RuntimeSocket, 30*time.Second)
		if err != nil {
			return err
		}
		defer func() { retErr = errors.Join(retErr, runtime.Close()) }()
		var observe func(string)
		if registry != nil {
			runtime.SetObserver(registry.ObserveCRI)
			observe = registry.LogRotation
		}
		// Directory cleanup needs journal-finished UIDs from the executor,
		// which it does not expose yet, so only rotation runs.
		rotator, err := logrotation.New(logrotation.Config{
			LogDir: execution.LogDir, RuntimeHandler: execution.RuntimeHandler,
			MaxSize: o.maxSizeBytes, MaxFiles: o.logMaxFiles,
		}, runtime, nil, observe)
		if err != nil {
			return fmt.Errorf("configure log rotation: %w", err)
		}
		services = append(services, func(ctx context.Context) error {
			return rotator.Run(ctx, func(err error) {
				logger.Warn("container log rotation failed", "error", err.Error())
			})
		})
	}
	return runWithServices(ctx, cancel, services, func(ctx context.Context) error {
		return nodeagent.ExecuteObserved(ctx, node, execution, kubeconfig, output, hooks)
	})
}

// runWithServices runs the agent and its services until the agent returns or
// a service stops; it then cancels the rest and waits for them.
func runWithServices(ctx context.Context, cancel context.CancelFunc, services []func(context.Context) error,
	agent func(context.Context) error) error {
	serviceErrors := make(chan error, len(services))
	var wg sync.WaitGroup
	for _, service := range services {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := service(ctx)
			if ctx.Err() == nil {
				if err == nil {
					err = errors.New("stopped unexpectedly")
				}
				serviceErrors <- err
			}
		}()
	}
	agentDone := make(chan error, 1)
	go func() { agentDone <- agent(ctx) }()
	var result error
	select {
	case result = <-agentDone:
		cancel()
	case err := <-serviceErrors:
		cancel()
		result = errors.Join(fmt.Errorf("observability service stopped: %w", err), <-agentDone)
	}
	wg.Wait()
	return result
}
