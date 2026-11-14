// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package serving

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ameukam/brie/internal/cri"
	"github.com/ameukam/brie/internal/podlog"
	"github.com/prometheus/client_golang/prometheus"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type harness struct {
	api     *fakeAPI
	runtime *fakeRuntime
	pki     *testPKI
	logDir  string
	base    string
	address string
	stream  *streamBackend
}

type stateSource struct {
	state ExtensionState
	err   error
}

func (s stateSource) ExtensionState(context.Context, string) (ExtensionState, error) {
	return s.state, s.err
}

func newHarness(t *testing.T, modify func(*Config, *Dependencies)) *harness {
	t.Helper()
	h := &harness{pki: newTestPKI(t), logDir: t.TempDir()}
	h.api = &fakeAPI{
		tokens: map[string]authenticationv1.UserInfo{
			"good-token": {Username: "alice", UID: "u-1", Groups: []string{"dev", authenticatedGroup}, Extra: map[string]authenticationv1.ExtraValue{"scope": {"a"}}},
			"reader":     {Username: "reader", Groups: []string{authenticatedGroup}},
		},
		allow: func(attributes authorizationv1.ResourceAttributes, user string) bool {
			switch user {
			case "alice", "kube-apiserver-kubelet-client":
				return true
			case "reader":
				return attributes.Verb == "get"
			}
			return false
		},
		failSAR: map[string]bool{},
	}
	h.runtime = ownedRuntime(h.logDir)
	h.stream = newStreamBackend(t)
	h.runtime.streamURL = h.stream.server.URL + "/exec/token"
	podDir := podlog.Directory(h.logDir, testUID)
	if err := os.MkdirAll(podDir, 0750); err != nil {
		t.Fatal(err)
	}
	writeLog(t, filepath.Join(podDir, "0.log"), sampleLog)
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewCounter(prometheus.CounterOpts{Name: "brie_test_total", Help: "test"}))
	config := Config{
		Address: "127.0.0.1:0", CertFile: h.pki.certFile, KeyFile: h.pki.keyFile, ClientCAFile: h.pki.caFile,
		NodeName: testNode, RuntimeHandler: testHandler, LogDir: h.logDir,
	}
	deps := Dependencies{API: newAPIClient(t, h.api), Runtime: h.runtime, Gatherer: registry,
		Logger: slog.New(slog.NewTextHandler(testWriter{t}, nil))}
	if modify != nil {
		modify(&config, &deps)
	}
	server, err := New(config, deps)
	if err != nil {
		t.Fatal(err)
	}
	tlsConfig, err := TLSConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h.address = listener.Addr().String()
	h.base = "https://" + h.address
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.serve(ctx, tls.NewListener(listener, tlsConfig)) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve: %v", err)
		}
	})
	return h
}

const sampleLog = "2026-11-13T10:00:00.000000001Z stdout F first\n" +
	"2026-11-13T10:00:01Z stderr P par\n" +
	"2026-11-13T10:00:01.5Z stderr F tial\n" +
	"malformed record\n" +
	"2026-11-13T10:00:02Z stdout F last\n"

func writeLog(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func (h *harness) client(certificate *tls.Certificate) *http.Client {
	config := &tls.Config{RootCAs: h.pki.serverPool, MinVersion: tls.VersionTLS12}
	if certificate != nil {
		config.Certificates = []tls.Certificate{*certificate}
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: config}, Timeout: 10 * time.Second}
}

func (h *harness) do(t *testing.T, client *http.Client, method, path, token string) (int, string) {
	t.Helper()
	request, err := http.NewRequest(method, h.base+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, string(body)
}

func TestRequestAccess(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		method, path string
		want         Access
		err          bool
	}{
		{method: "GET", path: "/healthz", want: Access{Verb: "get", Subresources: []string{"healthz", "proxy"}}},
		{method: "GET", path: "/healthz/ping", want: Access{Verb: "get", Subresources: []string{"healthz", "proxy"}}},
		{method: "GET", path: "/metrics", want: Access{Verb: "get", Subresources: []string{"metrics"}}},
		{method: "GET", path: "/metrics/cadvisor", want: Access{Verb: "get", Subresources: []string{"metrics"}}},
		{method: "GET", path: "/metricsx", want: Access{Verb: "get", Subresources: []string{"proxy"}}},
		{method: "GET", path: "/stats/summary", want: Access{Verb: "get", Subresources: []string{"stats"}}},
		{method: "GET", path: "/logs/syslog", want: Access{Verb: "get", Subresources: []string{"log"}}},
		{method: "GET", path: "/containerLogs/ns/pod/c", want: Access{Verb: "get", Subresources: []string{"proxy"}}},
		{method: "HEAD", path: "/healthz", err: true},
		{method: "GET", path: "/exec/ns/pod/c", want: Access{Verb: "create", Subresources: []string{"proxy"}}},
		{method: "POST", path: "/exec/ns/pod/c", want: Access{Verb: "create", Subresources: []string{"proxy"}}},
		{method: "GET", path: "/attach/ns/pod/c", want: Access{Verb: "create", Subresources: []string{"proxy"}}},
		{method: "GET", path: "/portForward/ns/pod", want: Access{Verb: "create", Subresources: []string{"proxy"}}},
		{method: "PUT", path: "/pods", want: Access{Verb: "update", Subresources: []string{"proxy"}}},
		{method: "PATCH", path: "/x", want: Access{Verb: "patch", Subresources: []string{"proxy"}}},
		{method: "DELETE", path: "/x", want: Access{Verb: "delete", Subresources: []string{"proxy"}}},
		{method: "OPTIONS", path: "/healthz", err: true},
	} {
		got, err := RequestAccess(tt.method, tt.path)
		if (err != nil) != tt.err || got.Verb != tt.want.Verb || !slices.Equal(got.Subresources, tt.want.Subresources) {
			t.Errorf("RequestAccess(%s %s) = %+v, %v; want %+v", tt.method, tt.path, got, err, tt.want)
		}
	}
}

func TestAuthentication(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	plain := h.client(nil)
	for _, tt := range []struct {
		name   string
		client *http.Client
		token  string
		header string
		want   int
	}{
		{name: "anonymous", client: plain, want: http.StatusUnauthorized},
		{name: "unknown token", client: plain, token: "bad", want: http.StatusUnauthorized},
		{name: "basic auth", client: plain, header: "Basic YTpi", want: http.StatusUnauthorized},
		{name: "empty bearer", client: plain, header: "Bearer  ", want: http.StatusUnauthorized},
		{name: "valid token", client: plain, token: "good-token", want: http.StatusOK},
		{name: "client certificate", client: h.client(&h.pki.client), want: http.StatusOK},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request, err := http.NewRequest(http.MethodGet, h.base+"/healthz", nil)
			if err != nil {
				t.Fatal(err)
			}
			if tt.token != "" {
				request.Header.Set("Authorization", "Bearer "+tt.token)
			}
			if tt.header != "" {
				request.Header.Set("Authorization", tt.header)
			}
			response, err := tt.client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if response.StatusCode != tt.want {
				t.Fatalf("status = %d, want %d", response.StatusCode, tt.want)
			}
		})
	}
	// A certificate from another CA is never accepted: the client withholds it
	// because the server names only its CA, and a forced one fails the handshake.
	if response, err := h.client(&h.pki.rogue).Get(h.base + "/healthz"); err != nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("rogue client certificate = %v, %v", response, err)
	}
	rogue := h.pki.rogue
	forced := &tls.Config{RootCAs: h.pki.serverPool, MinVersion: tls.VersionTLS12,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &rogue, nil }}
	if conn, err := tls.Dial("tcp", h.address, forced); err == nil {
		_, err = conn.Write([]byte("GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n"))
		if err == nil {
			_, err = conn.Read(make([]byte, 1))
		}
		_ = conn.Close()
		if err == nil {
			t.Fatal("forced rogue client certificate accepted")
		}
	}
	reviews := h.api.lastReviews()
	var certReview *authorizationv1.SubjectAccessReviewSpec
	for i := range reviews {
		if reviews[i].User == "kube-apiserver-kubelet-client" {
			certReview = &reviews[i]
		}
	}
	if certReview == nil || !slices.Contains(certReview.Groups, "system:masters") || !slices.Contains(certReview.Groups, authenticatedGroup) {
		t.Fatalf("certificate review = %+v", certReview)
	}
}

func TestTokenReviewFailureFailsClosed(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	h.api.failTR = true
	if status, _ := h.do(t, h.client(nil), http.MethodGet, "/healthz", "good-token"); status != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 on TokenReview error", status)
	}
}

func TestTokenAudiences(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(c *Config, _ *Dependencies) { c.TokenAudiences = []string{"kubelet"} })
	if status, _ := h.do(t, h.client(nil), http.MethodGet, "/healthz", "good-token"); status != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 without a matching audience", status)
	}
	h.api.mu.Lock()
	h.api.audOut = []string{"kubelet"}
	h.api.mu.Unlock()
	if status, _ := h.do(t, h.client(nil), http.MethodGet, "/healthz", "good-token"); status != http.StatusOK {
		t.Fatalf("status = %d, want 200 with a matching audience", status)
	}
	if got := h.api.tokenAt[0].Audiences; !slices.Equal(got, []string{"kubelet"}) {
		t.Fatalf("TokenReview audiences = %v", got)
	}
}

func TestAuthorization(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	plain := h.client(nil)
	if status, _ := h.do(t, plain, http.MethodGet, "/metrics", "reader"); status != http.StatusOK {
		t.Fatalf("reader metrics status = %d", status)
	}
	if status, _ := h.do(t, plain, http.MethodPost, "/exec/team/web/app?command=id&output=1", "reader"); status != http.StatusForbidden {
		t.Fatalf("reader exec status = %d, want 403", status)
	}
	if status, _ := h.do(t, plain, http.MethodGet, "/exec/team/web/app?command=id&output=1", "reader"); status != http.StatusForbidden {
		t.Fatalf("reader WebSocket-style GET exec status = %d, want 403", status)
	}
	reviews := h.api.lastReviews()
	first := reviews[0]
	if first.User != "reader" || first.ResourceAttributes.Resource != "nodes" || first.ResourceAttributes.Name != testNode ||
		first.ResourceAttributes.Subresource != "metrics" || first.ResourceAttributes.Verb != "get" || first.ResourceAttributes.Version != "v1" {
		t.Fatalf("metrics review = %+v", first)
	}
	last := reviews[len(reviews)-1]
	if last.ResourceAttributes.Verb != "create" || last.ResourceAttributes.Subresource != "proxy" {
		t.Fatalf("exec review = %+v", last.ResourceAttributes)
	}
	if status, _ := h.do(t, plain, http.MethodGet, "/healthz", "good-token"); status != http.StatusOK {
		t.Fatal("healthz denied")
	}
	reviews = h.api.lastReviews()
	alice := reviews[len(reviews)-1]
	if alice.UID != "u-1" || !slices.Equal(alice.Groups, []string{"dev", authenticatedGroup}) || !slices.Equal(alice.Extra["scope"], []string{"a"}) {
		t.Fatalf("user attributes not forwarded: %+v", alice)
	}
}

func TestHealthzFallsBackToProxy(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	h.api.allow = func(attributes authorizationv1.ResourceAttributes, _ string) bool {
		return attributes.Subresource == "proxy"
	}
	if status, body := h.do(t, h.client(nil), http.MethodGet, "/healthz", "good-token"); status != http.StatusOK || body != "ok" {
		t.Fatalf("healthz with proxy access = %d %q", status, body)
	}
	reviews := h.api.lastReviews()
	if len(reviews) != 2 || reviews[0].ResourceAttributes.Subresource != "healthz" || reviews[1].ResourceAttributes.Subresource != "proxy" {
		t.Fatalf("reviews = %+v", reviews)
	}
}

func TestSubjectAccessReviewErrorFailsClosed(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	h.api.failSAR["healthz"] = true
	h.api.failSAR["metrics"] = true
	plain := h.client(nil)
	for _, path := range []string{"/healthz", "/metrics"} {
		if status, _ := h.do(t, plain, http.MethodGet, path, "good-token"); status != http.StatusForbidden {
			t.Fatalf("%s status = %d, want 403 on review error", path, status)
		}
	}
	// No fallback review follows an error.
	for _, review := range h.api.lastReviews() {
		if review.ResourceAttributes.Subresource == "proxy" {
			t.Fatal("proxy fallback attempted after a review error")
		}
	}
}

func TestRequestRejections(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	plain := h.client(nil)
	for _, tt := range []struct {
		method, path string
		want         int
	}{
		{http.MethodOptions, "/healthz", http.StatusMethodNotAllowed},
		{http.MethodGet, "/metrics/../exec/team/web/app", http.StatusBadRequest},
		{http.MethodGet, "/containerLogs/team/web%2Fx/app", http.StatusBadRequest},
		{http.MethodGet, "/unknown", http.StatusNotFound},
		{http.MethodGet, "/containerLogs/Team/web/app", http.StatusBadRequest},
		{http.MethodGet, "/exec/team/web/APP?command=x&output=1", http.StatusBadRequest},
		{http.MethodGet, "/portForward/team/-web", http.StatusBadRequest},
	} {
		request, err := http.NewRequest(tt.method, h.base+tt.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.URL.Opaque = "//" + h.address + tt.path
		request.Header.Set("Authorization", "Bearer good-token")
		response, err := plain.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != tt.want {
			t.Errorf("%s %s = %d, want %d", tt.method, tt.path, response.StatusCode, tt.want)
		}
	}
}

func TestConcurrencyBound(t *testing.T) {
	t.Parallel()
	server, err := New(Config{
		CertFile: "/c", KeyFile: "/k", ClientCAFile: "/ca", NodeName: testNode, RuntimeHandler: testHandler,
		LogDir: "/var/log/brie", MaxConcurrent: 1,
	}, Dependencies{API: newAPIClient(t, &fakeAPI{}), Runtime: &fakeRuntime{}})
	if err != nil {
		t.Fatal(err)
	}
	server.slots <- struct{}{}
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", recorder.Code)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	status, body := h.do(t, h.client(nil), http.MethodGet, "/metrics", "good-token")
	if status != http.StatusOK || !strings.Contains(body, "brie_test_total 0") {
		t.Fatalf("metrics = %d %q", status, body)
	}
}

func TestNewValidation(t *testing.T) {
	t.Parallel()
	valid := Config{CertFile: "/c", KeyFile: "/k", ClientCAFile: "/ca", NodeName: testNode, RuntimeHandler: testHandler, LogDir: "/var/log/brie"}
	deps := Dependencies{API: newAPIClient(t, &fakeAPI{}), Runtime: &fakeRuntime{}}
	server, err := New(valid, deps)
	if err != nil || server.config.Address != DefaultAddress || server.config.MaxConcurrent != 64 {
		t.Fatalf("defaults = %+v, %v", server, err)
	}
	for _, tt := range []struct {
		name   string
		change func(*Config, *Dependencies)
		want   string
	}{
		{"address", func(c *Config, _ *Dependencies) { c.Address = "10250" }, "address"},
		{"relative cert", func(c *Config, _ *Dependencies) { c.CertFile = "server.crt" }, "absolute"},
		{"missing CA", func(c *Config, _ *Dependencies) { c.ClientCAFile = "" }, "absolute"},
		{"node", func(c *Config, _ *Dependencies) { c.NodeName = "Worker" }, "node name"},
		{"handler", func(c *Config, _ *Dependencies) { c.RuntimeHandler = "" }, "runtime handler"},
		{"log root", func(c *Config, _ *Dependencies) { c.LogDir = "/" }, "log directory"},
		{"api", func(_ *Config, d *Dependencies) { d.API = nil }, "API and runtime"},
		{"runtime", func(_ *Config, d *Dependencies) { d.Runtime = nil }, "API and runtime"},
	} {
		config, deps := valid, deps
		tt.change(&config, &deps)
		if _, err := New(config, deps); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%s: error = %v, want %q", tt.name, err, tt.want)
		}
	}
}

func TestTLSConfig(t *testing.T) {
	t.Parallel()
	pki := newTestPKI(t)
	config := Config{CertFile: pki.certFile, KeyFile: pki.keyFile, ClientCAFile: pki.caFile}
	tlsConfig, err := TLSConfig(config)
	if err != nil || tlsConfig.MinVersion != tls.VersionTLS12 || tlsConfig.ClientAuth != tls.VerifyClientCertIfGiven {
		t.Fatalf("TLSConfig = %+v, %v", tlsConfig, err)
	}
	empty := filepath.Join(pki.dir, "empty.crt")
	writeLog(t, empty, "not pem")
	for _, tt := range []struct {
		name   string
		config Config
		want   string
	}{
		{"missing key", Config{CertFile: pki.certFile, KeyFile: filepath.Join(pki.dir, "absent"), ClientCAFile: pki.caFile}, "serving certificate"},
		{"missing CA", Config{CertFile: pki.certFile, KeyFile: pki.keyFile, ClientCAFile: filepath.Join(pki.dir, "absent")}, "client CA"},
		{"empty CA", Config{CertFile: pki.certFile, KeyFile: pki.keyFile, ClientCAFile: empty}, "no PEM"},
	} {
		if _, err := TLSConfig(tt.config); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%s: error = %v, want %q", tt.name, err, tt.want)
		}
	}
}

func TestTLSVersionFloor(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	conn, err := tls.Dial("tcp", h.address, &tls.Config{RootCAs: h.pki.serverPool, MaxVersion: tls.VersionTLS11, MinVersion: tls.VersionTLS10})
	if err == nil {
		_ = conn.Close()
		t.Fatal("TLS 1.1 handshake accepted")
	}
}

func TestRun(t *testing.T) {
	t.Parallel()
	pki := newTestPKI(t)
	config := Config{
		Address: "127.0.0.1:0", CertFile: pki.certFile, KeyFile: pki.keyFile, ClientCAFile: pki.caFile,
		NodeName: testNode, RuntimeHandler: testHandler, LogDir: "/var/log/brie",
	}
	server, err := New(config, Dependencies{API: newAPIClient(t, &fakeAPI{}), Runtime: &fakeRuntime{}, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
	bad := config
	bad.KeyFile = filepath.Join(pki.dir, "absent")
	server, err = New(bad, Dependencies{API: newAPIClient(t, &fakeAPI{}), Runtime: &fakeRuntime{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Run(context.Background()); err == nil {
		t.Fatal("Run accepted a missing key")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	busy := config
	busy.Address = listener.Addr().String()
	server, err = New(busy, Dependencies{API: newAPIClient(t, &fakeAPI{}), Runtime: &fakeRuntime{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "listen") {
		t.Fatalf("Run on a busy address = %v", err)
	}
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = closed.Close()
	if err := server.serve(context.Background(), closed); err == nil || !strings.Contains(err.Error(), "serving stopped") {
		t.Fatalf("serve on a closed listener = %v", err)
	}
}

func TestContainerLogs(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	plain := h.client(nil)
	base := "/containerLogs/team/web/app"
	for _, tt := range []struct {
		name  string
		query string
		code  int
		want  string
	}{
		{name: "all", want: "first\npartial\nlast\n"},
		{name: "tail", query: "tailLines=1", want: "last\n"},
		{name: "tail zero", query: "tailLines=0", want: ""},
		{name: "limit", query: "limitBytes=3", want: "fir"},
		{name: "timestamps", query: "timestamps=true&tailLines=1", want: "2026-11-13T10:00:02Z last\n"},
		{name: "since time", query: "sinceTime=2026-11-13T10:00:01Z", want: "partial\nlast\n"},
		{name: "since seconds", query: "sinceSeconds=60", want: ""},
		{name: "previous", query: "previous=true", code: http.StatusBadRequest, want: "never restarts"},
		{name: "bad tail", query: "tailLines=-1", code: http.StatusBadRequest, want: "tailLines"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := base
			if tt.query != "" {
				path += "?" + tt.query
			}
			status, body := h.do(t, plain, http.MethodGet, path, "good-token")
			code := tt.code
			if code == 0 {
				code = http.StatusOK
			}
			if status != code || (code == http.StatusOK && body != tt.want) || !strings.Contains(body, tt.want) {
				t.Fatalf("status %d body %q, want %d %q", status, body, code, tt.want)
			}
		})
	}
}

// TestContainerLogTailBeyondScanBound is a regression test: a tail that
// cannot be located within the scan bound must fail before HTTP 200 is sent.
func TestContainerLogTailBeyondScanBound(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	line := "2026-11-13T10:00:00Z stdout F " + strings.Repeat("x", 994) + "\n"
	lines := maxTailScan/len(line) + 16
	logFile := filepath.Join(podlog.Directory(h.logDir, testUID), "0.log")
	writeLog(t, logFile, strings.Repeat(line, lines))
	for _, tt := range []struct {
		name  string
		query string
		code  int
		want  string
	}{
		{name: "beyond bound", query: "tailLines=" + strconv.Itoa(MaxTailLines), code: http.StatusUnprocessableEntity, want: "scan bound"},
		{name: "beyond bound with follow", query: "follow=true&tailLines=" + strconv.Itoa(MaxTailLines), code: http.StatusUnprocessableEntity, want: "scan bound"},
		{name: "within bound", query: "tailLines=2", code: http.StatusOK, want: strings.Repeat("x", 994) + "\n" + strings.Repeat("x", 994) + "\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			status, body := h.do(t, h.client(nil), http.MethodGet, "/containerLogs/team/web/app?"+tt.query, "good-token")
			if status != tt.code || (tt.code == http.StatusOK && body != tt.want) || !strings.Contains(body, tt.want) {
				t.Fatalf("status %d body %.80q, want %d %.80q", status, body, tt.code, tt.want)
			}
		})
	}
}

func TestContainerLogRefusals(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		path   string
		modify func(*harness)
		code   int
		want   string
	}{
		{name: "unknown pod", path: "/containerLogs/team/absent/app", code: http.StatusNotFound, want: "not found"},
		{name: "unknown container", path: "/containerLogs/team/web/other", code: http.StatusNotFound, want: "container other"},
		{name: "path outside pod directory", path: "/containerLogs/team/web/app", modify: func(h *harness) {
			h.runtime.logPath = filepath.Join(h.logDir, "0.log")
		}, code: http.StatusForbidden, want: "outside"},
		{name: "unclean path", path: "/containerLogs/team/web/app", modify: func(h *harness) {
			h.runtime.logPath = podlog.Directory(h.logDir, testUID) + "/../" + podlogDirName(testUID) + "/0.log"
		}, code: http.StatusForbidden, want: "outside"},
		{name: "symlink", path: "/containerLogs/team/web/app", modify: func(h *harness) {
			outside := filepath.Join(h.logDir, "secret")
			writeLog(t, outside, "2026-11-13T10:00:00Z stdout F host secret\n")
			logFile := filepath.Join(podlog.Directory(h.logDir, testUID), "0.log")
			if err := os.Remove(logFile); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, logFile); err != nil {
				t.Fatal(err)
			}
		}, code: http.StatusNotFound, want: "regular file"},
		{name: "runtime list error", path: "/containerLogs/team/web/app", modify: func(h *harness) {
			h.runtime.listErr = errors.New("unavailable")
		}, code: http.StatusInternalServerError, want: "runtime observation failed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, nil)
			if tt.modify != nil {
				tt.modify(h)
			}
			status, body := h.do(t, h.client(nil), http.MethodGet, tt.path, "good-token")
			if status != tt.code || !strings.Contains(body, tt.want) || strings.Contains(body, "host secret") {
				t.Fatalf("status %d body %q, want %d %q", status, body, tt.code, tt.want)
			}
		})
	}
}

func TestExtensionStateRefusesWithoutResuming(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		state stateSource
		code  int
		want  string
	}{
		{name: "paused", state: stateSource{state: "Paused"}, code: http.StatusConflict, want: `extension state "Paused"`},
		{name: "checkpointing", state: stateSource{state: "Checkpointing"}, code: http.StatusConflict, want: "not resumed"},
		{name: "unknown", state: stateSource{err: errors.New("journal unavailable")}, code: http.StatusServiceUnavailable, want: "unavailable"},
		{name: "running", state: stateSource{state: ExtensionRunning}, code: http.StatusOK},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(_ *Config, d *Dependencies) { d.States = tt.state })
			for _, path := range []string{
				"/containerLogs/team/web/app", "/exec/team/web/app?command=id&output=1", "/attach/team/web/app?output=1", "/portForward/team/web?ports=80",
			} {
				method := http.MethodGet
				if !strings.HasPrefix(path, "/containerLogs") {
					method = http.MethodPost
				}
				status, body := h.do(t, h.client(nil), method, path, "good-token")
				if status != tt.code || !strings.Contains(body, tt.want) {
					t.Fatalf("%s: status %d body %q, want %d %q", path, status, body, tt.code, tt.want)
				}
			}
			if tt.code != http.StatusOK && h.stream.requests() != 0 {
				t.Fatal("refused request reached the runtime streaming server")
			}
		})
	}
}

func TestFollowLogs(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	logFile := filepath.Join(podlog.Directory(h.logDir, testUID), "0.log")
	request, err := http.NewRequest(http.MethodGet, h.base+"/containerLogs/team/web/app?follow=true&tailLines=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer good-token")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	response, err := h.client(nil).Do(request.WithContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	reader := bufio.NewReader(response.Body)
	expect := func(want string) {
		t.Helper()
		line, err := reader.ReadString('\n')
		if err != nil || line != want {
			t.Fatalf("line = %q, %v, want %q", line, err, want)
		}
	}
	expect("last\n")
	file, err := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("2026-11-13T10:00:03Z stdout F appended\n"); err != nil {
		t.Fatal(err)
	}
	expect("appended\n")
	// Rotation: rename, keep writing to the old file, then create a new one.
	if err := os.Rename(logFile, logFile+".20261113-100004"); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("2026-11-13T10:00:04Z stdout F before reopen\n"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	writeLog(t, logFile, "2026-11-13T10:00:05Z stdout F after reopen\n")
	expect("before reopen\n")
	expect("after reopen\n")
	cancel()
}

func TestFollowStopsAtLimit(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	status, body := h.do(t, h.client(nil), http.MethodGet, "/containerLogs/team/web/app?follow=true&limitBytes=7", "good-token")
	if status != http.StatusOK || body != "first\np" {
		t.Fatalf("follow with limit = %d %q", status, body)
	}
}

// streamBackend stands in for the runtime's loopback streaming server.
type streamBackend struct {
	server *httptest.Server
	seen   chan http.Header
	count  chan struct{}
}

func newStreamBackend(t *testing.T) *streamBackend {
	t.Helper()
	b := &streamBackend{seen: make(chan http.Header, 16), count: make(chan struct{}, 16)}
	b.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.count <- struct{}{}
		b.seen <- r.Header.Clone()
		if r.Header.Get("Upgrade") == "" {
			_, _ = fmt.Fprintf(w, "plain %s", r.URL.Path)
			return
		}
		conn, buffered, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = buffered.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: " + r.Header.Get("Upgrade") + "\r\n\r\n")
		_ = buffered.Flush()
		line, err := buffered.ReadString('\n')
		if err == nil {
			_, _ = conn.Write([]byte("echo " + line))
		}
	}))
	t.Cleanup(b.server.Close)
	return b
}

func (b *streamBackend) requests() int { return len(b.count) }

func TestExecProxiesUpgrade(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	conn, err := tls.Dial("tcp", h.address, &tls.Config{RootCAs: h.pki.serverPool, MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_, err = fmt.Fprintf(conn, "GET /exec/team/web/app?command=sh&command=-c&command=id&input=1&output=1&tty=1 HTTP/1.1\r\n"+
		"Host: %s\r\nAuthorization: Bearer good-token\r\nImpersonate-User: admin\r\nConnection: Upgrade\r\nUpgrade: SPDY/3.1\r\n\r\n", h.address)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	if err != nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade response = %v, %v", response, err)
	}
	if _, err := conn.Write([]byte("ping\n")); err != nil {
		t.Fatal(err)
	}
	line, err := reader.ReadString('\n')
	if err != nil || line != "echo ping\n" {
		t.Fatalf("stream = %q, %v", line, err)
	}
	headers := <-h.stream.seen
	if headers.Get("Authorization") != "" || headers.Get("Impersonate-User") != "" {
		t.Fatalf("credentials forwarded to the runtime: %v", headers)
	}
	h.runtime.mu.Lock()
	defer h.runtime.mu.Unlock()
	if !slices.Equal(h.runtime.execCmd, []string{"sh", "-c", "id"}) || !h.runtime.execOpts.TTY || !h.runtime.execOpts.Stdin {
		t.Fatalf("exec request = %v %+v", h.runtime.execCmd, h.runtime.execOpts)
	}
}

func TestStreamingEndpoints(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		path   string
		modify func(*harness)
		code   int
		want   string
	}{
		{name: "exec", path: "/exec/team/web/app?command=id&output=true", code: http.StatusOK, want: "plain /exec/token"},
		{name: "attach", path: "/attach/team/web/app?output=true", code: http.StatusOK, want: "plain /exec/token"},
		{name: "port forward", path: "/portForward/team/web?ports=80,8080", code: http.StatusOK, want: "plain /exec/token"},
		{name: "exec without command", path: "/exec/team/web/app?output=true", code: http.StatusBadRequest, want: "command"},
		{name: "attach with command", path: "/attach/team/web/app?output=true&command=x", code: http.StatusBadRequest, want: "does not take"},
		{name: "no streams", path: "/attach/team/web/app", code: http.StatusBadRequest, want: "at least one"},
		{name: "bad port", path: "/portForward/team/web?port=70000", code: http.StatusBadRequest, want: "between 1"},
		{name: "container not running", path: "/exec/team/web/app?command=id&output=1", modify: func(h *harness) {
			h.runtime.containers[0].State = runtimeapi.ContainerState_CONTAINER_EXITED
		}, code: http.StatusConflict, want: "not running"},
		{name: "sandbox not ready", path: "/portForward/team/web", modify: func(h *harness) {
			h.runtime.sandboxes[1].State = runtimeapi.PodSandboxState_SANDBOX_NOTREADY
		}, code: http.StatusConflict, want: "not ready"},
		{name: "runtime error", path: "/exec/team/web/app?command=id&output=1", modify: func(h *harness) {
			h.runtime.streamErr = errors.New("CRI Exec: unavailable")
		}, code: http.StatusInternalServerError, want: "runtime observation failed"},
		{name: "attach runtime error", path: "/attach/team/web/app?output=1", modify: func(h *harness) {
			h.runtime.streamErr = errors.New("CRI Attach: unavailable")
		}, code: http.StatusInternalServerError, want: "runtime observation failed"},
		{name: "forward runtime error", path: "/portForward/team/web", modify: func(h *harness) {
			h.runtime.streamErr = errors.New("CRI PortForward: unavailable")
		}, code: http.StatusInternalServerError, want: "runtime observation failed"},
		{name: "non-loopback streaming URL", path: "/exec/team/web/app?command=id&output=1", modify: func(h *harness) {
			h.runtime.streamURL = "http://192.0.2.10:10010/exec/token"
		}, code: http.StatusInternalServerError, want: "runtime observation failed"},
		{name: "unreachable streaming server", path: "/exec/team/web/app?command=id&output=1", modify: func(h *harness) {
			h.runtime.streamURL = "http://127.0.0.1:1/exec/token"
		}, code: http.StatusBadGateway, want: "runtime streaming failed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, nil)
			if tt.modify != nil {
				tt.modify(h)
			}
			status, body := h.do(t, h.client(nil), http.MethodPost, tt.path, "good-token")
			if status != tt.code || !strings.Contains(body, tt.want) {
				t.Fatalf("status %d body %q, want %d %q", status, body, tt.code, tt.want)
			}
		})
	}
}

func TestParsePorts(t *testing.T) {
	t.Parallel()
	ports, err := ParsePorts(url.Values{"port": {"80"}, "ports": {"443, 8080"}})
	if err != nil || !slices.Equal(ports, []int32{80, 443, 8080}) {
		t.Fatalf("ParsePorts = %v, %v", ports, err)
	}
	many := strings.TrimSuffix(strings.Repeat("1,", maxForwardedPorts+1), ",")
	if _, err := ParsePorts(url.Values{"ports": {many}}); err == nil {
		t.Fatal("too many ports accepted")
	}
	for _, bad := range []string{"0", "65536", "x", ""} {
		if _, err := ParsePorts(url.Values{"port": {bad}}); err == nil {
			t.Errorf("port %q accepted", bad)
		}
	}
}

func TestParseExecQuery(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		query string
		exec  bool
		want  string
	}{
		{query: "output=maybe&command=x", exec: true, want: "boolean"},
		{query: "tty=1&error=1&command=x", exec: true, want: "TTY"},
		{query: "output=1", exec: true, want: "command"},
		{query: "output=1&command=x", exec: false, want: "does not take"},
	} {
		values, err := url.ParseQuery(tt.query)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ParseExecQuery(values, tt.exec); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%q: error = %v, want %q", tt.query, err, tt.want)
		}
	}
}

// TestParseExecQueryKubeletParameters uses the query an API server sends to
// kubelet for kubectl exec and attach.
func TestParseExecQueryKubeletParameters(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		query string
		exec  bool
		want  ExecRequest
		err   string
	}{
		{name: "exec", exec: true, query: "command=sh&command=-c&command=id&input=1&output=1&error=1",
			want: ExecRequest{Command: []string{"sh", "-c", "id"}, Options: cri.ExecOptions{Stdin: true, Stdout: true, Stderr: true}}},
		{name: "exec tty", exec: true, query: "command=sh&input=true&output=true&tty=true",
			want: ExecRequest{Command: []string{"sh"}, Options: cri.ExecOptions{Stdin: true, Stdout: true, TTY: true}}},
		{name: "attach", query: "input=1&output=1&error=1",
			want: ExecRequest{Options: cri.ExecOptions{Stdin: true, Stdout: true, Stderr: true}}},
		{name: "attach output only", query: "output=1", want: ExecRequest{Options: cri.ExecOptions{Stdout: true}}},
		{name: "legacy names are not kubelet parameters", query: "stdin=1&stdout=1&stderr=1", err: "at least one of input"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			values, err := url.ParseQuery(tt.query)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ParseExecQuery(values, tt.exec)
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("error = %v, want %q", err, tt.err)
				}
				return
			}
			if err != nil || !slices.Equal(got.Command, tt.want.Command) || got.Options != tt.want.Options {
				t.Fatalf("ParseExecQuery = %+v, %v, want %+v", got, err, tt.want)
			}
		})
	}
}

func TestParseLogOptions(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	options, err := ParseLogOptions(url.Values{}, now)
	if err != nil || options.TailLines != -1 || options.LimitBytes != MaxLogBytes || options.Follow || !options.Since.IsZero() {
		t.Fatalf("defaults = %+v, %v", options, err)
	}
	options, err = ParseLogOptions(url.Values{"limitBytes": {"999999999999"}, "sinceSeconds": {"10"}, "follow": {"1"}}, now)
	if err != nil || options.LimitBytes != MaxLogBytes || !options.Since.Equal(now.Add(-10*time.Second)) || !options.Follow {
		t.Fatalf("bounded = %+v, %v", options, err)
	}
	for _, query := range []string{
		"follow=x", "timestamps=x", "previous=x", "previous=true", "tailLines=100001", "tailLines=x",
		"limitBytes=0", "sinceSeconds=0", "sinceSeconds=999999999999", "sinceTime=yesterday",
		"sinceSeconds=1&sinceTime=2026-11-13T10:00:00Z",
	} {
		values, err := url.ParseQuery(query)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ParseLogOptions(values, now); err == nil {
			t.Errorf("%q accepted", query)
		}
	}
}

func FuzzServingQueries(f *testing.F) {
	for _, seed := range []string{
		"tailLines=10&follow=true", "limitBytes=1&timestamps=1", "sinceTime=2026-11-13T10:00:00Z",
		"command=sh&command=-c&input=1&tty=1", "ports=80,443&port=8080", "%zz", "previous=true",
	} {
		f.Add(seed)
	}
	now := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	f.Fuzz(func(t *testing.T, raw string) {
		query, err := url.ParseQuery(raw)
		if err != nil {
			return
		}
		if options, err := ParseLogOptions(query, now); err == nil {
			if options.LimitBytes < 1 || options.LimitBytes > MaxLogBytes || options.TailLines > MaxTailLines || options.TailLines < -1 {
				t.Fatalf("unbounded log options %+v", options)
			}
		}
		if request, err := ParseExecQuery(query, true); err == nil {
			if len(request.Command) == 0 || (request.Options.TTY && request.Options.Stderr) {
				t.Fatalf("invalid exec request accepted: %+v", request)
			}
		}
		if ports, err := ParsePorts(query); err == nil {
			if len(ports) > maxForwardedPorts {
				t.Fatalf("too many ports: %d", len(ports))
			}
			for _, port := range ports {
				if port < 1 || port > 65535 {
					t.Fatalf("port %d accepted", port)
				}
			}
		}
	})
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}
