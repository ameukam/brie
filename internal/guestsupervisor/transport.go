// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package guestsupervisor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

// maxConnections bounds concurrently handled supervisor connections.
const maxConnections = 4

// connectionTimeout bounds one connection, including the longest prepare.
const connectionTimeout = MaxPrepareDuration + 30*time.Second

// ValidateSocketPath requires an absolute UTF-8 path that fits sun_path.
func ValidateSocketPath(path string) error {
	return validateGuestPath("supervisor socket", path)
}

func validateGuestPath(name, path string) error {
	if !filepath.IsAbs(path) || !utf8.ValidString(path) || strings.ContainsRune(path, '\x00') || len(path) > 107 {
		return fmt.Errorf("%s path must be absolute UTF-8, at most 107 bytes, without NUL", name)
	}
	return nil
}

// Listen creates the supervisor socket with owner-only permissions. It first
// takes an exclusive lock on path+".lock", held until the listener closes, so
// concurrent starters cannot both inspect and replace the socket. A leftover
// socket file is replaced only when a connection to it is refused. A live
// listener, a probe failure of any other kind, and any other existing file
// are errors that leave the path untouched.
func Listen(ctx context.Context, path string) (net.Listener, error) {
	if err := ValidateSocketPath(path); err != nil {
		return nil, err
	}
	lock, err := lockSocketPath(path + ".lock")
	if err != nil {
		return nil, err
	}
	listener, err := listenLocked(ctx, path)
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	return &ownedListener{UnixListener: listener, lock: lock}, nil
}

// lockSocketPath opens and exclusively locks the private regular file that
// guards a socket path. The lock ends when the file is closed or the process
// exits.
func lockSocketPath(lockPath string) (*os.File, error) {
	fd, err := syscall.Open(lockPath, syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open supervisor socket lock: %w", err)
	}
	lock := os.NewFile(uintptr(fd), lockPath)
	info, err := lock.Stat()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("inspect supervisor socket lock: %w", err), lock.Close())
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.Join(errors.New("supervisor socket lock must be a private regular file"), lock.Close())
	}
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			err = errors.New("another supervisor owns the socket path")
		}
		return nil, errors.Join(fmt.Errorf("lock supervisor socket: %w", err), lock.Close())
	}
	return lock, nil
}

// listenLocked replaces a stale socket and listens; the caller holds the
// socket path lock.
func listenLocked(ctx context.Context, path string) (*net.UnixListener, error) {
	if info, err := os.Lstat(path); err == nil {
		if info.Mode().Type() != fs.ModeSocket {
			return nil, fmt.Errorf("supervisor socket path %s exists and is not a socket", path)
		}
		dialCtx, cancel := context.WithTimeout(ctx, time.Second)
		conn, dialErr := (&net.Dialer{}).DialContext(dialCtx, "unix", path)
		cancel()
		if dialErr == nil {
			_ = conn.Close()
			return nil, fmt.Errorf("another supervisor is listening on %s", path)
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) {
			return nil, fmt.Errorf("cannot tell whether supervisor socket %s is in use: %w", path, dialErr)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("remove stale supervisor socket: %w", err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("inspect supervisor socket path: %w", err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("listen on supervisor socket: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("restrict supervisor socket: %w", err)
	}
	return listener, nil
}

// ownedListener releases the socket path lock after the socket closes.
type ownedListener struct {
	*net.UnixListener
	lock    *os.File
	release sync.Once
}

func (l *ownedListener) Close() error {
	err := l.UnixListener.Close()
	l.release.Do(func() { err = errors.Join(err, l.lock.Close()) })
	return err
}

// Serve answers one request per connection until ctx ends, then closes
// listener and waits for in-flight connections.
func (s *Supervisor) Serve(ctx context.Context, listener net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stop()

	var wg sync.WaitGroup
	defer wg.Wait()
	slots := make(chan struct{}, maxConnections)
	for {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return nil
		}
		conn, err := listener.Accept()
		if err != nil {
			<-slots
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept supervisor connection: %w", err)
		}
		wg.Go(func() {
			defer func() { <-slots }()
			s.serveConn(ctx, conn)
		})
	}
}

func (s *Supervisor) serveConn(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(ctx, connectionTimeout)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer stop()

	data, err := io.ReadAll(io.LimitReader(conn, MaxMessageBytes+1))
	var response Response
	if err != nil {
		return
	}
	if request, decodeErr := DecodeRequest(data); decodeErr != nil {
		response = refusal("", "invalid request")
	} else {
		response = s.Handle(ctx, request)
	}
	encoded, err := EncodeResponse(response)
	if err != nil {
		return
	}
	_, _ = conn.Write(encoded)
}

// Call sends one request to the supervisor socket and returns its validated
// reply. ctx bounds the whole exchange.
func Call(ctx context.Context, socketPath string, request Request) (Response, error) {
	if err := ValidateSocketPath(socketPath); err != nil {
		return Response{}, err
	}
	encoded, err := EncodeRequest(request)
	if err != nil {
		return Response{}, err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	if err != nil {
		return Response{}, fmt.Errorf("connect to supervisor: %w", err)
	}
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer stop()

	if _, err := conn.Write(encoded); err != nil {
		return Response{}, callError(ctx, "send request", err)
	}
	if unixConn, ok := conn.(*net.UnixConn); ok {
		if err := unixConn.CloseWrite(); err != nil {
			return Response{}, callError(ctx, "finish request", err)
		}
	}
	data, err := io.ReadAll(io.LimitReader(conn, MaxMessageBytes+1))
	if err != nil {
		return Response{}, callError(ctx, "read response", err)
	}
	response, err := DecodeResponse(data)
	if err != nil {
		return Response{}, err
	}
	if err := response.ValidateFor(request); err != nil {
		return Response{}, err
	}
	return response, nil
}

func callError(ctx context.Context, action string, err error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("%s: %w", action, ctx.Err())
	}
	return fmt.Errorf("%s: %w", action, err)
}
