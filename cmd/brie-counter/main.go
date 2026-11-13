// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Command brie-counter is a sample cooperative HTTP workload for the guest
// supervisor protocol. It persists a counter to a state file on the
// container's block-backed root filesystem, stops accepting work on prepare,
// and rebinds identity and entropy on resume. It holds no credentials.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ameukam/brie/internal/guestsupervisor"
)

const shutdownTimeout = 5 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("brie-counter", flag.ContinueOnError)
	flags.SetOutput(stderr)
	listen := flags.String("listen", ":8080", "HTTP listen address")
	statePath := flags.String("state", "/brie/counter.json", "absolute path of the durable counter state file")
	socket := flags.String("supervisor-socket", guestsupervisor.DefaultSocketPath, "absolute path of the in-container supervisor socket")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		_, _ = fmt.Fprintf(stderr, "brie-counter: unexpected arguments: %q\n", flags.Args())
		return 2
	}
	if err := serve(ctx, *listen, *statePath, *socket, stderr); err != nil {
		_, _ = fmt.Fprintf(stderr, "brie-counter: %v\n", err)
		return 1
	}
	return 0
}

func serve(ctx context.Context, listen, statePath, socket string, stderr io.Writer) error {
	app, err := openCounter(statePath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	supervisorListener, err := guestsupervisor.Listen(ctx, socket)
	if err != nil {
		return err
	}
	httpListener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", listen)
	if err != nil {
		_ = supervisorListener.Close()
		return fmt.Errorf("listen for HTTP: %w", err)
	}
	server := &http.Server{
		Handler:           app.handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       time.Minute,
		MaxHeaderBytes:    8 << 10,
	}
	_, _ = fmt.Fprintf(stderr, "brie-counter: serving HTTP on %s and the supervisor on %s\n", httpListener.Addr(), socket)

	supervisorDone := make(chan error, 1)
	go func() { supervisorDone <- guestsupervisor.NewSupervisor(app).Serve(ctx, supervisorListener) }()
	httpDone := make(chan error, 1)
	go func() { httpDone <- server.Serve(httpListener) }()

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-supervisorDone:
		supervisorDone <- nil
	case serveErr = <-httpDone:
		httpDone <- nil
	}
	cancel()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancelShutdown()
	if err := server.Shutdown(shutdownCtx); err != nil && serveErr == nil {
		serveErr = fmt.Errorf("shut down HTTP: %w", err)
	}
	if err := <-httpDone; err != nil && !errors.Is(err, http.ErrServerClosed) && serveErr == nil {
		serveErr = fmt.Errorf("serve HTTP: %w", err)
	}
	if err := <-supervisorDone; err != nil && serveErr == nil {
		serveErr = err
	}
	if errors.Is(serveErr, http.ErrServerClosed) {
		return nil
	}
	return serveErr
}
