// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package serving implements Brie's kubelet-compatible HTTPS endpoint: health,
// Prometheus metrics, container logs, and exec, attach, and port-forward
// through CRI streaming. Every request is authenticated with a client
// certificate or webhook TokenReview and authorized with SubjectAccessReview
// on the node's subresources; review errors refuse the request.
//
// The server reads CRI observations and log files only. It never resumes,
// pauses, or otherwise mutates a sandbox and never contacts a VMM.
package serving

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

// DefaultAddress is kubelet's authenticated serving port.
const DefaultAddress = ":10250"

// Config is the explicit serving configuration.
type Config struct {
	// Address is the TCP listen address, such as ":10250".
	Address string
	// CertFile and KeyFile hold the PEM serving certificate and private key.
	CertFile, KeyFile string
	// ClientCAFile holds PEM CA certificates that verify client certificates.
	ClientCAFile string
	// NodeName is the Node named in SubjectAccessReviews.
	NodeName string
	// RuntimeHandler restricts served sandboxes to the configured handler.
	RuntimeHandler string
	// LogDir is the executor's Pod log root.
	LogDir string
	// TokenAudiences optionally restricts accepted token audiences.
	TokenAudiences []string
	// MaxConcurrent bounds in-flight requests, including streams. Default 64.
	MaxConcurrent int
}

// Dependencies are the collaborators a Server uses.
type Dependencies struct {
	// API performs TokenReview and SubjectAccessReview.
	API kubernetes.Interface
	// Runtime is a CRI client dedicated to serving.
	Runtime Runtime
	// Gatherer supplies /metrics; nil serves an empty registry.
	Gatherer prometheus.Gatherer
	// States reports lifecycle-extension states; nil means none exist.
	States ExtensionStates
	// Logger receives request errors; nil discards them.
	Logger *slog.Logger
}

// Server is an authenticated kubelet-compatible endpoint.
type Server struct {
	config  Config
	api     kubernetes.Interface
	runtime Runtime
	states  ExtensionStates
	logger  *slog.Logger
	mux     *http.ServeMux
	slots   chan struct{}
	// streamTransport reaches the runtime's local streaming server.
	streamTransport http.RoundTripper
}

// New validates the configuration and builds the request handler.
func New(config Config, deps Dependencies) (*Server, error) {
	if config.Address == "" {
		config.Address = DefaultAddress
	}
	if _, _, err := net.SplitHostPort(config.Address); err != nil {
		return nil, fmt.Errorf("serving address: %w", err)
	}
	for _, file := range []string{config.CertFile, config.KeyFile, config.ClientCAFile} {
		if !filepath.IsAbs(file) {
			return nil, errors.New("serving requires absolute certificate, key, and client CA file paths")
		}
	}
	if len(validation.IsDNS1123Subdomain(config.NodeName)) != 0 || config.RuntimeHandler == "" {
		return nil, errors.New("serving requires a valid node name and runtime handler")
	}
	if !filepath.IsAbs(config.LogDir) || filepath.Clean(config.LogDir) == "/" {
		return nil, errors.New("serving requires an absolute non-root Pod log directory")
	}
	if deps.API == nil || deps.Runtime == nil {
		return nil, errors.New("serving requires API and runtime clients")
	}
	if config.MaxConcurrent <= 0 {
		config.MaxConcurrent = 64
	}
	if deps.Gatherer == nil {
		deps.Gatherer = prometheus.NewRegistry()
	}
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}
	s := &Server{
		config: config, api: deps.API, runtime: deps.Runtime, states: deps.States, logger: deps.Logger,
		mux: http.NewServeMux(), slots: make(chan struct{}, config.MaxConcurrent),
		streamTransport: &http.Transport{
			Proxy: nil, DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
			TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 30 * time.Second,
			MaxIdleConns: 4, IdleConnTimeout: 30 * time.Second,
		},
	}
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok"))
	})
	s.mux.Handle("GET /metrics", promhttp.HandlerFor(deps.Gatherer, promhttp.HandlerOpts{
		ErrorHandling: promhttp.HTTPErrorOnError, MaxRequestsInFlight: 4,
	}))
	s.mux.HandleFunc("GET /containerLogs/{namespace}/{pod}/{container}", s.containerLogs)
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		s.mux.HandleFunc(method+" /exec/{namespace}/{pod}/{container}", s.exec)
		s.mux.HandleFunc(method+" /attach/{namespace}/{pod}/{container}", s.attach)
		s.mux.HandleFunc(method+" /portForward/{namespace}/{pod}", s.portForward)
	}
	return s, nil
}

// ServeHTTP bounds concurrency, then authenticates and authorizes every
// request before routing it.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	if r.URL.Path == "" || path.Clean(r.URL.Path) != r.URL.Path || r.URL.RawPath != "" {
		http.Error(w, "request path must be clean and unescaped", http.StatusBadRequest)
		return
	}
	access, err := RequestAccess(r.Method, r.URL.Path)
	if err != nil {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	user, err := authenticate(ctx, s.api, s.config.TokenAudiences, r)
	if err != nil {
		cancel()
		s.logger.LogAttrs(r.Context(), slog.LevelWarn, "serving request unauthenticated", failureAttrs(err)...)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	err = authorize(ctx, s.api, s.config.NodeName, user, access)
	cancel()
	if err != nil {
		s.logger.LogAttrs(r.Context(), slog.LevelWarn, "serving request forbidden",
			append([]slog.Attr{slog.String("user", user.Name)}, failureAttrs(err)...)...)
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	s.mux.ServeHTTP(w, r)
}

// TLSConfig loads the serving key pair and client CA. Client certificates are
// verified when presented; requests without one need a bearer token.
func TLSConfig(config Config) (*tls.Config, error) {
	pair, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load serving certificate: %w", err)
	}
	caData, err := os.ReadFile(config.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("read client CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caData) {
		return nil, errors.New("client CA file contains no PEM certificates")
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair},
		ClientAuth: tls.VerifyClientCertIfGiven, ClientCAs: pool,
		NextProtos: []string{"http/1.1"},
	}, nil
}

// Run serves HTTPS until ctx is cancelled, then shuts down within ten seconds.
func (s *Server) Run(ctx context.Context) error {
	tlsConfig, err := TLSConfig(s.config)
	if err != nil {
		return err
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("listen on serving address: %w", err)
	}
	return s.serve(ctx, tls.NewListener(listener, tlsConfig))
}

func (s *Server) serve(ctx context.Context, listener net.Listener) error {
	server := &http.Server{
		Handler: s, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute,
		MaxHeaderBytes: 64 * 1024, ErrorLog: slog.NewLogLogger(s.logger.Handler(), slog.LevelWarn),
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err := <-done:
		return fmt.Errorf("serving stopped: %w", err)
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	err := server.Shutdown(shutdown)
	if serveErr := <-done; !errors.Is(serveErr, http.ErrServerClosed) {
		err = errors.Join(err, serveErr)
	}
	if err != nil {
		return fmt.Errorf("shut down serving: %w", err)
	}
	return nil
}
