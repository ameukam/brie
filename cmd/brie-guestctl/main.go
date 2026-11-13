// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Command brie-guestctl relays one guest supervisor request from its argument
// to the supervisor's in-container Unix socket and prints the reply. The host
// runs it through CRI ExecSync, so Kata's agent remains the only guest-control
// channel; it opens no other listener or network connection.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ameukam/brie/internal/guestsupervisor"
)

// callTimeout bounds one relayed request, including the longest prepare.
const callTimeout = guestsupervisor.MaxPrepareDuration + time.Minute

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("brie-guestctl", flag.ContinueOnError)
	flags.SetOutput(stderr)
	socket := flags.String("socket", guestsupervisor.DefaultSocketPath, "absolute path of the supervisor Unix socket")
	flags.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "Usage: brie-guestctl [--socket=PATH] REQUEST_JSON")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 1 {
		_, _ = fmt.Fprintln(stderr, "brie-guestctl: expected exactly one request argument")
		return 2
	}
	request, err := guestsupervisor.DecodeRequest([]byte(flags.Arg(0)))
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "brie-guestctl: %v\n", err)
		return 2
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	response, err := guestsupervisor.Call(ctx, *socket, request)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "brie-guestctl: %v\n", err)
		return 1
	}
	encoded, err := guestsupervisor.EncodeResponse(response)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "brie-guestctl: %v\n", err)
		return 1
	}
	if _, err := fmt.Fprintf(stdout, "%s\n", encoded); err != nil {
		return 1
	}
	return 0
}
