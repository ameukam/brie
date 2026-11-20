// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package serving

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ameukam/brie/internal/cri"
	"github.com/ameukam/brie/internal/podlog"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// fakeAPI is an httptest API server answering TokenReview and SubjectAccessReview.
type fakeAPI struct {
	mu      sync.Mutex
	tokens  map[string]authenticationv1.UserInfo
	allow   func(authorizationv1.ResourceAttributes, string) bool
	failSAR map[string]bool
	failTR  bool
	reviews []authorizationv1.SubjectAccessReviewSpec
	tokenAt []authenticationv1.TokenReviewSpec
	audOut  []string
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/apis/authentication.k8s.io/v1/tokenreviews":
		var review authenticationv1.TokenReview
		if err := json.NewDecoder(r.Body).Decode(&review); err != nil || f.failTR {
			http.Error(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","code":500,"message":"echo `+canarySecret+`"}`, http.StatusInternalServerError)
			return
		}
		f.tokenAt = append(f.tokenAt, review.Spec)
		if user, ok := f.tokens[review.Spec.Token]; ok {
			review.Status = authenticationv1.TokenReviewStatus{Authenticated: true, User: user, Audiences: f.audOut}
		}
		_ = json.NewEncoder(w).Encode(review)
	case "/apis/authorization.k8s.io/v1/subjectaccessreviews":
		var review authorizationv1.SubjectAccessReview
		if err := json.NewDecoder(r.Body).Decode(&review); err != nil || review.Spec.ResourceAttributes == nil {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		f.reviews = append(f.reviews, review.Spec)
		if f.failSAR[review.Spec.ResourceAttributes.Subresource] {
			http.Error(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","code":500,"message":"echo `+canarySecret+`"}`, http.StatusInternalServerError)
			return
		}
		review.Status.Allowed = f.allow != nil && f.allow(*review.Spec.ResourceAttributes, review.Spec.User)
		_ = json.NewEncoder(w).Encode(review)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeAPI) lastReviews() []authorizationv1.SubjectAccessReviewSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]authorizationv1.SubjectAccessReviewSpec(nil), f.reviews...)
}

func newAPIClient(t *testing.T, api *fakeAPI) kubernetes.Interface {
	t.Helper()
	server := httptest.NewServer(api)
	t.Cleanup(server.Close)
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL, ContentConfig: rest.ContentConfig{ContentType: "application/json"}})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// fakeRuntime implements Runtime with fixed observations.
type fakeRuntime struct {
	mu         sync.Mutex
	sandboxes  []*runtimeapi.PodSandbox
	containers []*runtimeapi.Container
	logPath    string
	listErr    error
	streamErr  error
	streamURL  string
	execCmd    []string
	execOpts   cri.ExecOptions
	ports      []int32
}

func (f *fakeRuntime) ListPodSandbox(context.Context) ([]*runtimeapi.PodSandbox, error) {
	return f.sandboxes, f.listErr
}

func (f *fakeRuntime) ListContainers(context.Context) ([]*runtimeapi.Container, error) {
	return f.containers, f.listErr
}

func (f *fakeRuntime) ContainerStatus(_ context.Context, id string) (*runtimeapi.ContainerStatus, error) {
	return &runtimeapi.ContainerStatus{Id: id, LogPath: f.logPath}, nil
}

func (f *fakeRuntime) Exec(_ context.Context, _ string, command []string, options cri.ExecOptions) (*url.URL, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execCmd, f.execOpts = command, options
	return f.url()
}

func (f *fakeRuntime) Attach(_ context.Context, _ string, options cri.ExecOptions) (*url.URL, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execOpts = options
	return f.url()
}

func (f *fakeRuntime) PortForward(_ context.Context, _ string, ports []int32) (*url.URL, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ports = ports
	return f.url()
}

func (f *fakeRuntime) url() (*url.URL, error) {
	if f.streamErr != nil {
		return nil, f.streamErr
	}
	return url.Parse(f.streamURL)
}

// canarySecret stands for tenant data or a credential that backend errors
// may echo; it must never reach agent logs.
const canarySecret = "canary-secret-7f3a9c"

// syncBuffer collects agent log output for assertions.
type syncBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

const (
	testNode    = "worker-1"
	testHandler = "kata-fc"
	testUID     = "pod-uid-1"
)

func podlogDirName(uid string) string { return filepath.Base(podlog.Directory("/", uid)) }

func ownedRuntime(logDir string) *fakeRuntime {
	return &fakeRuntime{
		sandboxes: []*runtimeapi.PodSandbox{
			{
				Id: "old-sandbox", Metadata: &runtimeapi.PodSandboxMetadata{Name: "web", Namespace: "team", Uid: "old-uid"},
				State: runtimeapi.PodSandboxState_SANDBOX_NOTREADY, CreatedAt: 1, RuntimeHandler: testHandler,
				Labels: map[string]string{executionOwnerLabel: "owner"},
			},
			{
				Id: "sandbox-1", Metadata: &runtimeapi.PodSandboxMetadata{Name: "web", Namespace: "team", Uid: testUID},
				State: runtimeapi.PodSandboxState_SANDBOX_READY, CreatedAt: 2, RuntimeHandler: testHandler,
				Labels: map[string]string{executionOwnerLabel: "owner"},
			},
			{
				Id: "foreign", Metadata: &runtimeapi.PodSandboxMetadata{Name: "web", Namespace: "team", Uid: "foreign-uid"},
				State: runtimeapi.PodSandboxState_SANDBOX_READY, CreatedAt: 3, RuntimeHandler: "runc",
				Labels: map[string]string{executionOwnerLabel: "owner"},
			},
			{
				Id: "unlabeled", Metadata: &runtimeapi.PodSandboxMetadata{Name: "web", Namespace: "team", Uid: "unlabeled-uid"},
				State: runtimeapi.PodSandboxState_SANDBOX_READY, CreatedAt: 4, RuntimeHandler: testHandler,
			},
		},
		containers: []*runtimeapi.Container{
			{Id: "container-1", PodSandboxId: "sandbox-1", Metadata: &runtimeapi.ContainerMetadata{Name: "app"},
				State: runtimeapi.ContainerState_CONTAINER_RUNNING, CreatedAt: 2},
			{Id: "other", PodSandboxId: "foreign", Metadata: &runtimeapi.ContainerMetadata{Name: "app"},
				State: runtimeapi.ContainerState_CONTAINER_RUNNING, CreatedAt: 3},
		},
		logPath: filepath.Join(logDir, podlogDirName(testUID), "0.log"),
	}
}

type testPKI struct {
	dir                       string
	certFile, keyFile, caFile string
	client, rogue             tls.Certificate
	serverPool                *x509.CertPool
	caCert                    *x509.Certificate
	caKey                     *ecdsa.PrivateKey
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	dir := t.TempDir()
	caCert, caKey := newCertificate(t, &x509.Certificate{
		Subject: pkix.Name{CommonName: "test-ca"}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign,
	}, nil, nil)
	serverCert, serverKey := newCertificate(t, &x509.Certificate{
		Subject: pkix.Name{CommonName: testNode}, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, KeyUsage: x509.KeyUsageDigitalSignature,
	}, caCert, caKey)
	clientCert, clientKey := newCertificate(t, &x509.Certificate{
		Subject:     pkix.Name{CommonName: "kube-apiserver-kubelet-client", Organization: []string{"system:masters"}},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature,
	}, caCert, caKey)
	rogueCA, rogueCAKey := newCertificate(t, &x509.Certificate{
		Subject: pkix.Name{CommonName: "rogue-ca"}, IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign,
	}, nil, nil)
	rogueCert, rogueKey := newCertificate(t, &x509.Certificate{
		Subject:     pkix.Name{CommonName: "rogue"},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature,
	}, rogueCA, rogueCAKey)
	p := &testPKI{
		dir: dir, certFile: filepath.Join(dir, "server.crt"), keyFile: filepath.Join(dir, "server.key"),
		caFile: filepath.Join(dir, "ca.crt"), serverPool: x509.NewCertPool(), caCert: caCert, caKey: caKey,
	}
	p.serverPool.AddCert(caCert)
	writePEM(t, p.certFile, "CERTIFICATE", serverCert.Raw)
	writePEM(t, p.keyFile, "PRIVATE KEY", marshalKey(t, serverKey))
	writePEM(t, p.caFile, "CERTIFICATE", caCert.Raw)
	p.client = tls.Certificate{Certificate: [][]byte{clientCert.Raw}, PrivateKey: clientKey}
	p.rogue = tls.Certificate{Certificate: [][]byte{rogueCert.Raw}, PrivateKey: rogueKey}
	return p
}

func newCertificate(t *testing.T, template, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template.SerialNumber, err = rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	template.NotBefore, template.NotAfter = time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	if parent == nil {
		parent, parentKey = template, key
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return certificate, key
}

func marshalKey(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func writePEM(t *testing.T, path, kind string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
}
