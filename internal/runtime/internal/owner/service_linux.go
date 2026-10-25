// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package owner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/ameukam/brie/internal/runtime/internal/firecracker"
	"golang.org/x/net/netutil"
)

// ServiceConfig fixes one execution identity and boot profile for a disposable
// owner process. Directory must be fresh and beneath an operator-trusted parent.
type ServiceConfig struct {
	Owner     Config
	Binary    string
	Directory string
	Boot      BootConfig
}

// Observation describes owner supervision, not guest or workload readiness.
type Observation struct {
	Version    int       `json:"version"`
	SandboxUID string    `json:"sandbox_uid"`
	PodUID     string    `json:"pod_uid"`
	Process    State     `json:"process_state"`
	Boot       BootState `json:"boot_state"`
	Error      string    `json:"error,omitempty"`
}

// Serve runs a private local API independently of its clients. Cancellation is
// an explicit owner shutdown: in-flight operations finish, then the owned VMM is
// stopped and reaped. Artifacts remain; interrupted identities cannot be reused.
func Serve(ctx context.Context, config ServiceConfig, logger *log.Logger) (result error) {
	if logger == nil {
		return errors.New("owner service requires a logger")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !filepath.IsAbs(config.Directory) || config.Owner.JournalDir != "" {
		return errors.New("owner service requires an absolute fresh directory and manages its own journal path")
	}
	if err := config.Boot.Validate(); err != nil {
		return fmt.Errorf("validate owner service boot profile: %w", err)
	}
	socket := filepath.Join(config.Directory, "control.sock")
	if len(socket) > 107 {
		return errors.New("owner control socket path exceeds the Linux Unix-socket limit")
	}
	launcher, err := NewFirecrackerLauncher(config.Binary, filepath.Join(config.Directory, "vmm"))
	if err != nil {
		return err
	}
	if err := os.Mkdir(config.Directory, 0o700); err != nil {
		return fmt.Errorf("create fresh owner service directory: %w", err)
	}
	config.Owner.JournalDir = filepath.Join(config.Directory, "journal")
	o, err := New(config.Owner, launcher)
	if err != nil {
		return err
	}
	defer func() {
		result = errors.Join(result, o.Close())
	}()
	client, err := firecracker.New(launcher.SocketPath(), config.Owner.OperationTimeout)
	if err != nil {
		return err
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return fmt.Errorf("listen on owner socket: %w", err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		return errors.Join(err, listener.Close())
	}
	return serveOwner(ctx, listener, o, client, config, logger)
}

func serveOwner(ctx context.Context, listener net.Listener, o *Owner, client *firecracker.Client, config ServiceConfig, logger *log.Logger) error {
	// Client disconnects must not cancel accepted mutations.
	operations := context.WithoutCancel(ctx)
	mutation := make(chan struct{}, 1)
	server := &http.Server{
		ReadHeaderTimeout: time.Second, ReadTimeout: time.Second,
		WriteTimeout: config.Owner.OperationTimeout + time.Second,
		IdleTimeout:  time.Second, MaxHeaderBytes: 4096, ErrorLog: logger,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			respond := func(code int, cause error) {
				o.transitionMu.Lock()
				state, boot := o.Status(), o.BootStatus()
				o.transitionMu.Unlock()
				result := Observation{
					Version: 1, SandboxUID: config.Owner.SandboxUID, PodUID: config.Owner.PodUID,
					Process: state.State, Boot: boot.State,
				}
				if cause == nil {
					cause = errors.Join(state.Err, boot.Err)
				}
				if cause != nil {
					result.Error = cause.Error()
				}
				w.WriteHeader(code)
				if err := json.NewEncoder(w).Encode(result); err != nil {
					logger.Printf("write owner observation: %v", err)
				}
			}
			if r.ContentLength != 0 || len(r.TransferEncoding) != 0 || r.URL.RawQuery != "" {
				respond(http.StatusBadRequest, errors.New("owner requests accept no body or query"))
				return
			}
			if r.Method == http.MethodGet && r.URL.Path == "/v1/status" {
				respond(http.StatusOK, nil)
				return
			}
			var operation func(context.Context) error
			switch r.URL.Path {
			case "/v1/start":
				operation = o.Start
			case "/v1/boot":
				operation = func(ctx context.Context) error { return o.Boot(ctx, client, config.Boot) }
			case "/v1/stop":
				operation = o.Stop
			default:
				respond(http.StatusNotFound, errors.New("unknown owner endpoint"))
				return
			}
			if r.Method != http.MethodPost {
				w.Header().Set("Allow", http.MethodPost)
				respond(http.StatusMethodNotAllowed, errors.New("mutation requires POST"))
				return
			}
			if r.Header.Get("X-Brie-Sandbox-UID") != config.Owner.SandboxUID ||
				r.Header.Get("X-Brie-Pod-UID") != config.Owner.PodUID {
				respond(http.StatusConflict, errors.New("owner execution identity mismatch"))
				return
			}
			select {
			case mutation <- struct{}{}:
				defer func() { <-mutation }()
			default:
				respond(http.StatusServiceUnavailable, errors.New("owner mutation already in progress; observe status before retrying"))
				return
			}
			if err := operation(operations); err != nil {
				respond(http.StatusConflict, err)
				return
			}
			respond(http.StatusOK, nil)
		}),
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(netutil.LimitListener(listener, 16)) }()
	var serveErr error
	select {
	case serveErr = <-done:
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), config.Owner.OperationTimeout+time.Second)
	defer cancel()
	shutdownErr := server.Shutdown(shutdown)
	if shutdownErr != nil {
		shutdownErr = errors.Join(shutdownErr, server.Close())
	}
	var stopErr error
	if o.Status().State != Idle {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), config.Owner.OperationTimeout)
		defer stopCancel()
		stopErr = o.Stop(stopCtx)
	}
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return errors.Join(serveErr, shutdownErr, stopErr)
}
