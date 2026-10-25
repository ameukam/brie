//go:build registration_e2e

// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package registration_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	certificatesv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestRegistration(t *testing.T) {
	if os.Getenv("BRIE_REGISTRATION_E2E") != "1" {
		t.Skip("run make registration-e2e to create a disposable cluster")
	}
	configPath := os.Getenv("KUBECONFIG")
	if !filepath.IsAbs(configPath) {
		t.Fatal("requires an explicit absolute disposable kubeconfig")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	adminConfig, err := clientcmd.BuildConfigFromFlags("", configPath)
	if err != nil {
		t.Fatal(err)
	}
	adminConfig.Timeout = 10 * time.Second
	admin, err := kubernetes.NewForConfig(adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	// All credentials and process output stay in the test's private directory.
	dir := t.TempDir()
	name := "brie-registration"
	nodeConfig := issueIdentity(ctx, t, admin, adminConfig, name)
	nodeConfigPath := filepath.Join(dir, "node.kubeconfig")
	if err := clientcmd.WriteToFile(*nodeConfig, nodeConfigPath); err != nil {
		t.Fatal(err)
	}
	nodeREST, err := clientcmd.BuildConfigFromFlags("", nodeConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	nodeREST.Timeout = 10 * time.Second
	nodeClient, err := kubernetes.NewForConfig(nodeREST)
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "brie-node")
	build := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", binary, "./cmd/brie-node")
	build.Dir = filepath.Join("..", "..")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build agent: %v\n%s", err, output)
	}
	start := func() (*exec.Cmd, <-chan error) {
		t.Helper()
		log, err := os.CreateTemp(dir, "agent-*.log")
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, binary, "--run", "--node-name="+name, "--kubeconfig="+nodeConfigPath)
		cmd.Stdout, cmd.Stderr = log, log
		if err := cmd.Start(); err != nil {
			if closeErr := log.Close(); closeErr != nil {
				t.Error(closeErr)
			}
			t.Fatal(err)
		}
		done := make(chan error, 1)
		exited := make(chan struct{})
		go func() {
			done <- errors.Join(cmd.Wait(), log.Close())
			close(exited)
		}()
		t.Cleanup(func() {
			select {
			case <-exited:
			default:
				if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
					t.Error(err)
				}
				<-exited
			}
			if t.Failed() {
				output, err := os.ReadFile(log.Name())
				if err != nil {
					t.Error(err)
				} else {
					t.Logf("agent output:\n%s", output)
				}
			}
		})
		return cmd, done
	}
	cmd, done := start()
	until(ctx, t, func(ctx context.Context) (bool, error) {
		select {
		case err := <-done:
			return false, errors.Join(errors.New("agent exited before registration"), err)
		default:
		}
		_, err := admin.CoordinationV1().Leases("kube-node-lease").Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return err == nil, err
	})
	node := getNode(ctx, t, admin, name)
	assertSafeNode(t, node)
	uid := node.UID
	externalConditions := []corev1.NodeCondition{}
	for _, kind := range []corev1.NodeConditionType{
		corev1.NodeDrainInProgress, corev1.NodeDrained, corev1.NodeMaintenancePlanned,
		corev1.NodeMaintenanceInProgress, corev1.NodeGracefulNodeShutdownInProgress,
	} {
		externalConditions = append(externalConditions, corev1.NodeCondition{
			Type: kind, Status: corev1.ConditionFalse, Reason: "LifecycleTest",
			Message:            "administrator-owned test observation",
			LastTransitionTime: metav1.NewTime(time.Now().UTC().Truncate(time.Second)),
		})
	}
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"uid": uid, "resourceVersion": node.ResourceVersion},
		"status":   map[string]any{"conditions": externalConditions},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.CoreV1().Nodes().Patch(ctx, name, types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "status"); err != nil {
		t.Fatal(err)
	}
	assertExternalConditions := func() {
		t.Helper()
		current := getNode(ctx, t, admin, name)
		for _, expected := range externalConditions {
			found := false
			for _, condition := range current.Status.Conditions {
				if condition.Type == expected.Type {
					found = equality.Semantic.DeepEqual(condition, expected)
				}
			}
			if !found {
				t.Fatalf("external condition %s lost or changed: %+v", expected.Type, current.Status.Conditions)
			}
		}
	}
	lease, err := admin.CoordinationV1().Leases("kube-node-lease").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if lease.Spec.RenewTime == nil || lease.Spec.HolderIdentity == nil ||
		*lease.Spec.HolderIdentity != name || lease.Spec.LeaseDurationSeconds == nil ||
		*lease.Spec.LeaseDurationSeconds != 40 || len(lease.OwnerReferences) != 1 ||
		lease.OwnerReferences[0].UID != uid {
		t.Fatalf("invalid registration Lease: %+v", lease)
	}
	firstRenew := lease.Spec.RenewTime.Time
	until(ctx, t, func(ctx context.Context) (bool, error) {
		current, err := admin.CoordinationV1().Leases("kube-node-lease").Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return current.Spec.RenewTime != nil && current.Spec.RenewTime.After(firstRenew), nil
	})
	assertExternalConditions()
	controlNodes, err := admin.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: "node-role.kubernetes.io/control-plane"})
	if err != nil || len(controlNodes.Items) != 1 {
		t.Fatalf("expected one disposable control-plane node: %v", err)
	}
	control := controlNodes.Items[0].Name
	_, err = nodeClient.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("own Node access: %v", err)
	}
	_, err = nodeClient.CoreV1().Nodes().Get(ctx, control, metav1.GetOptions{})
	assertForbidden(t, err)
	_, err = nodeClient.CoreV1().Secrets("kube-system").List(ctx, metav1.ListOptions{})
	assertForbidden(t, err)
	_, err = nodeClient.CoordinationV1().Leases("kube-node-lease").Get(ctx, control, metav1.GetOptions{})
	assertForbidden(t, err)
	foreign := controlNodes.Items[0].DeepCopy()
	foreign.Spec.Unschedulable = true
	_, err = nodeClient.CoreV1().Nodes().Update(ctx, foreign, metav1.UpdateOptions{})
	assertForbidden(t, err)
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	awaitExit(ctx, t, done, false)
	until(ctx, t, func(ctx context.Context) (bool, error) {
		n, err := admin.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, condition := range n.Status.Conditions {
			if condition.Type == corev1.NodeReady {
				return condition.Status == corev1.ConditionUnknown, nil
			}
		}
		return false, nil
	})
	_, done = start()
	until(ctx, t, func(ctx context.Context) (bool, error) {
		n, err := admin.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, condition := range n.Status.Conditions {
			if condition.Type == corev1.NodeReady {
				return condition.Status == corev1.ConditionFalse && condition.Reason == "RuntimeNotImplemented", nil
			}
		}
		return false, nil
	})
	node = getNode(ctx, t, admin, name)
	assertSafeNode(t, node)
	assertExternalConditions()
	if node.UID != uid {
		t.Fatal("restart replaced the Node")
	}
	pods, err := admin.CoreV1().Pods("").List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + name})
	if err != nil || len(pods.Items) != 0 {
		t.Fatalf("expected no assigned Pods: %v", err)
	}
	node.Spec.Unschedulable = false
	if _, err := admin.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	awaitExit(ctx, t, done, true)
	t.Log("registration, renewal, scoped permissions, heartbeat loss, restart, and unsafe-state exit passed")
}

func issueIdentity(ctx context.Context, t *testing.T, admin kubernetes.Interface, adminConfig *rest.Config, name string) *clientcmdapi.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: "system:node:" + name, Organization: []string{"system:nodes"}},
	}, key)
	if err != nil {
		t.Fatal(err)
	}
	expiry := int32(600)
	csr, err := admin.CertificatesV1().CertificateSigningRequests().Create(ctx, &certificatesv1.CertificateSigningRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: certificatesv1.CertificateSigningRequestSpec{
			Request:    pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER}),
			SignerName: certificatesv1.KubeAPIServerClientKubeletSignerName, ExpirationSeconds: &expiry,
			Usages: []certificatesv1.KeyUsage{certificatesv1.UsageDigitalSignature, certificatesv1.UsageClientAuth},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	csr.Status.Conditions = []certificatesv1.CertificateSigningRequestCondition{{
		Type: certificatesv1.CertificateApproved, Status: corev1.ConditionTrue,
		Reason: "DisposableRegistrationTest", Message: "Approve only the request generated by this test",
	}}
	if _, err := admin.CertificatesV1().CertificateSigningRequests().UpdateApproval(ctx, name, csr, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	var cert []byte
	until(ctx, t, func(ctx context.Context) (bool, error) {
		signed, err := admin.CertificatesV1().CertificateSigningRequests().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		cert = signed.Status.Certificate
		return len(cert) > 0, nil
	})
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	ca := adminConfig.CAData
	if len(ca) == 0 {
		ca, err = os.ReadFile(adminConfig.CAFile)
		if err != nil {
			t.Fatal(err)
		}
	}
	return &clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{"test": {Server: adminConfig.Host, CertificateAuthorityData: ca}},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{"node": {
			ClientCertificateData: cert,
			ClientKeyData:         pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		}},
		Contexts: map[string]*clientcmdapi.Context{"test": {Cluster: "test", AuthInfo: "node"}}, CurrentContext: "test",
	}
}

func until(ctx context.Context, t *testing.T, condition wait.ConditionWithContextFunc) {
	t.Helper()
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true, condition); err != nil {
		t.Fatal(err)
	}
}

func getNode(ctx context.Context, t *testing.T, client kubernetes.Interface, name string) *corev1.Node {
	t.Helper()
	node, err := client.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return node
}

func assertSafeNode(t *testing.T, node *corev1.Node) {
	t.Helper()
	if !node.Spec.Unschedulable || node.Annotations["brie.dev/registration-only"] != "true" {
		t.Fatal("Node is not cordoned and marked registration-only")
	}
	tainted := false
	for _, taint := range node.Spec.Taints {
		if taint.Key == "brie.dev/registration-only" && taint.Value == "true" && taint.Effect == corev1.TaintEffectNoExecute {
			tainted = true
		}
	}
	if !tainted {
		t.Fatal("registration-only NoExecute taint missing")
	}
	ready := false
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionFalse && condition.Reason == "RuntimeNotImplemented" {
			ready = true
		}
	}
	if !ready {
		t.Fatal("expected Ready=False with RuntimeNotImplemented")
	}
	for _, resources := range []corev1.ResourceList{node.Status.Capacity, node.Status.Allocatable} {
		for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourcePods} {
			quantity, ok := resources[name]
			if !ok || !quantity.IsZero() {
				t.Fatalf("expected explicit zero %s", name)
			}
		}
	}
}

func assertForbidden(t *testing.T, err error) {
	t.Helper()
	if !apierrors.IsForbidden(err) {
		t.Fatalf("expected forbidden, got %v", err)
	}
}

func awaitExit(ctx context.Context, t *testing.T, done <-chan error, wantFailure bool) {
	t.Helper()
	select {
	case err := <-done:
		if wantFailure {
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
				t.Fatalf("expected exit 1, got %v", err)
			}
		} else if err != nil {
			t.Fatalf("expected clean SIGTERM shutdown, got %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("agent did not exit within 20 seconds")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
