// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestExecuteAPITransportAndShutdown(t *testing.T) {
	t.Parallel()
	e, _ := newExecutor(t)
	if err := e.journal.close(); err != nil {
		t.Fatal(err)
	}
	events := make(chan string, 16)
	notify := func(event string) {
		select {
		case events <- event:
		default:
		}
	}
	decode := func(r *http.Request, object runtime.Object) error {
		data, err := io.ReadAll(io.LimitReader(r.Body, 1024*1024))
		if err != nil {
			return err
		}
		_, _, err = scheme.Codecs.UniversalDeserializer().Decode(data, nil, object)
		return err
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var object any
		var err error
		switch {
		case r.URL.Path == "/apis/node.k8s.io/v1/runtimeclasses/kata-fc":
			class, getErr := e.api.NodeV1().RuntimeClasses().Get(r.Context(), "kata-fc", metav1.GetOptions{})
			class.TypeMeta = metav1.TypeMeta{APIVersion: "node.k8s.io/v1", Kind: "RuntimeClass"}
			object, err = class, getErr
		case r.URL.Path == "/api/v1/nodes/worker-1":
			if r.Method == http.MethodPut {
				var node corev1.Node
				err = decode(r, &node)
				if err == nil {
					object, err = e.api.CoreV1().Nodes().Update(r.Context(), &node, metav1.UpdateOptions{})
				}
			} else {
				object, err = e.api.CoreV1().Nodes().Get(r.Context(), "worker-1", metav1.GetOptions{})
			}
		case r.URL.Path == "/api/v1/nodes/worker-1/status":
			var node corev1.Node
			err = decode(r, &node)
			if err == nil {
				object, err = e.api.CoreV1().Nodes().UpdateStatus(r.Context(), &node, metav1.UpdateOptions{})
				notify("status")
			}
		case r.URL.Path == "/api/v1/pods":
			if r.URL.Query().Get("fieldSelector") != "spec.nodeName=worker-1" || r.URL.Query().Get("limit") != "100" {
				t.Error("Pod inventory was not scoped and paginated")
			}
			object = &corev1.PodList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"}}
			notify("pods")
		case r.URL.Path == "/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases/worker-1":
			object, err = e.api.CoordinationV1().Leases(leaseNamespace).Get(r.Context(), "worker-1", metav1.GetOptions{})
		case r.URL.Path == "/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases" && r.Method == http.MethodPost:
			var lease coordinationv1.Lease
			err = decode(r, &lease)
			if err == nil {
				object, err = e.api.CoordinationV1().Leases(leaseNamespace).Create(r.Context(), &lease, metav1.CreateOptions{})
				notify("lease")
			}
		default:
			http.NotFound(w, r)
			return
		}
		if err != nil {
			if apierrors.IsNotFound(err) {
				w.WriteHeader(http.StatusNotFound)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
			_ = json.NewEncoder(w).Encode(&metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
				Status: "Failure", Reason: metav1.StatusReasonNotFound, Code: 404})
			return
		}
		if node, ok := object.(*corev1.Node); ok {
			node.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Node"}
		}
		if lease, ok := object.(*coordinationv1.Lease); ok {
			lease.TypeMeta = metav1.TypeMeta{APIVersion: "coordination.k8s.io/v1", Kind: "Lease"}
		}
		_ = json.NewEncoder(w).Encode(object)
	}))
	defer server.Close()
	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.TLS.Certificates[0].Certificate[0]})
	if err := clientcmd.WriteToFile(clientcmdapi.Config{
		Clusters:  map[string]*clientcmdapi.Cluster{"test": {Server: server.URL, CertificateAuthorityData: ca}},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{"test": {}},
		Contexts:  map[string]*clientcmdapi.Context{"test": {Cluster: "test", AuthInfo: "test"}}, CurrentContext: "test",
	}, kubeconfig); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Execute(ctx, e.node, e.config, kubeconfig, io.Discard) }()
	seen := make(map[string]bool)
	for len(seen) < 3 {
		select {
		case event := <-events:
			seen[event] = true
		case err := <-done:
			t.Fatalf("execution stopped before API synchronization: %v", err)
		case <-ctx.Done():
			t.Fatalf("execution loops did not synchronize: %v", seen)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
