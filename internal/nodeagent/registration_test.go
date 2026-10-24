// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func testNode() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker-1", UID: "node-uid", Annotations: map[string]string{registrationMarker: "true"},
		},
		Spec: corev1.NodeSpec{
			Unschedulable: true,
			Taints:        []corev1.Taint{{Key: registrationMarker, Value: "true", Effect: corev1.TaintEffectNoExecute}},
		},
	}
}

func TestRegistrationHeartbeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := fake.NewClientset(testNode())
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var stderr bytes.Buffer
		done := make(chan error, 1)
		go func() {
			done <- register(ctx, Config{NodeName: "worker-1"}, client.CoreV1().Nodes(),
				client.CoordinationV1().Leases(leaseNamespace), &stderr)
		}()
		synctest.Wait()
		node, err := client.CoreV1().Nodes().Get(ctx, "worker-1", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(node.Status.Conditions) != 1 || node.Status.Conditions[0].Status != corev1.ConditionFalse ||
			node.Status.Conditions[0].Reason != "RuntimeNotImplemented" || !node.Spec.Unschedulable {
			t.Fatalf("unsafe Node: %+v", node)
		}
		for _, list := range []corev1.ResourceList{node.Status.Capacity, node.Status.Allocatable} {
			for _, name := range []corev1.ResourceName{corev1.ResourcePods, corev1.ResourceCPU, corev1.ResourceMemory} {
				quantity, ok := list[name]
				if !ok || !quantity.IsZero() {
					t.Fatalf("resource %s = %v, want explicit zero", name, quantity)
				}
			}
		}
		lease, err := client.CoordinationV1().Leases(leaseNamespace).Get(ctx, "worker-1", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		firstRenew := lease.Spec.RenewTime.DeepCopy()
		transition := node.Status.Conditions[0].LastTransitionTime
		time.Sleep(heartbeatInterval)
		synctest.Wait()
		lease, err = client.CoordinationV1().Leases(leaseNamespace).Get(ctx, "worker-1", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !lease.Spec.RenewTime.After(firstRenew.Time) || *lease.Spec.LeaseDurationSeconds != 40 ||
			*lease.Spec.HolderIdentity != "worker-1" || lease.OwnerReferences[0].UID != node.UID {
			t.Fatalf("invalid Lease: %+v", lease)
		}
		node, err = client.CoreV1().Nodes().Get(ctx, "worker-1", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !node.Status.Conditions[0].LastTransitionTime.Equal(&transition) {
			t.Fatal("heartbeat changed the transition time")
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(stderr.String(), "zero Pod capacity") {
			t.Fatalf("missing truthful startup message: %q", stderr.String())
		}
	})
}

func TestHeartbeatPreservesOtherStatus(t *testing.T) {
	for _, tt := range []struct {
		name     string
		hasReady bool
		conflict bool
	}{
		{name: "adds Ready"},
		{name: "updates Ready", hasReady: true},
		{name: "preserves concurrent maintenance update", hasReady: true, conflict: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			node := testNode()
			before := metav1.NewTime(time.Now().Add(-time.Hour))
			for _, kind := range []corev1.NodeConditionType{
				corev1.NodeDrainInProgress, corev1.NodeDrained,
				corev1.NodeMaintenancePlanned, corev1.NodeMaintenanceInProgress,
				corev1.NodeGracefulNodeShutdownInProgress, corev1.NodeNetworkUnavailable,
				"example.com/CustomCondition",
			} {
				node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{
					Type: kind, Status: corev1.ConditionUnknown, Reason: "ExternalObservation",
					Message: "owned by another controller", LastHeartbeatTime: before, LastTransitionTime: before,
				})
			}
			external := append([]corev1.NodeCondition(nil), node.Status.Conditions...)
			if tt.hasReady {
				node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{
					Type: corev1.NodeReady, Status: corev1.ConditionTrue, LastTransitionTime: before,
				})
			}
			node.Status.Addresses = []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.1"}}
			node.Status.NodeInfo.BootID = "observed-boot"
			want := node.Status.DeepCopy()
			client := fake.NewClientset(node)
			calls := 0
			client.PrependReactor("update", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() != "status" {
					return false, nil, nil
				}
				calls++
				if tt.conflict && calls == 1 {
					changed := node.DeepCopy()
					changed.Status.Conditions[0].Status = corev1.ConditionTrue
					changed.Status.Conditions[0].Reason = "DrainStarted"
					external[0] = changed.Status.Conditions[0]
					if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), changed, ""); err != nil {
						t.Fatal(err)
					}
					return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "nodes"}, node.Name, errors.New("maintenance updated"))
				}
				return false, nil, nil
			})
			for range 2 {
				if err := heartbeat(t.Context(), client.CoreV1().Nodes(), client.CoordinationV1().Leases(leaseNamespace), node.Name, node.UID); err != nil {
					t.Fatal(err)
				}
			}
			got, err := client.CoreV1().Nodes().Get(t.Context(), node.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Status.Conditions) != len(external)+1 ||
				!reflect.DeepEqual(got.Status.Conditions[:len(external)], external) {
				t.Fatalf("external conditions changed: %+v", got.Status.Conditions)
			}
			ready := got.Status.Conditions[len(external)]
			if ready.Type != corev1.NodeReady || ready.Status != corev1.ConditionFalse ||
				ready.Reason != "RuntimeNotImplemented" || !ready.LastTransitionTime.After(before.Time) {
				t.Fatalf("unsafe Ready condition: %+v", ready)
			}
			// Normalize only fields registration owns to compare all remaining status.
			got.Status.Conditions = want.Conditions
			got.Status.Capacity = want.Capacity
			got.Status.Allocatable = want.Allocatable
			got.Status.NodeInfo.OperatingSystem = want.NodeInfo.OperatingSystem
			got.Status.NodeInfo.Architecture = want.NodeInfo.Architecture
			if !reflect.DeepEqual(&got.Status, want) {
				t.Fatalf("unrelated Node status changed: %+v", got.Status)
			}
		})
	}
}
func TestRegistrationRefusesUnsafeState(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*corev1.Node)
		lease  *coordinationv1.Lease
	}{
		{name: "foreign Node", change: func(n *corev1.Node) { n.Annotations = nil }},
		{name: "uncordoned", change: func(n *corev1.Node) { n.Spec.Unschedulable = false }},
		{name: "untainted", change: func(n *corev1.Node) { n.Spec.Taints = nil }},
		{name: "wrong effect", change: func(n *corev1.Node) { n.Spec.Taints[0].Effect = corev1.TaintEffectNoSchedule }},
		{name: "foreign Lease", lease: &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "worker-1", Namespace: leaseNamespace}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			node := testNode()
			if tt.change != nil {
				tt.change(node)
			}
			client := fake.NewClientset(node)
			if tt.lease != nil {
				if err := client.Tracker().Add(tt.lease); err != nil {
					t.Fatal(err)
				}
			}
			var stderr bytes.Buffer
			err := register(t.Context(), Config{NodeName: node.Name}, client.CoreV1().Nodes(),
				client.CoordinationV1().Leases(leaseNamespace), &stderr)
			if err == nil || !strings.Contains(err.Error(), "refusing") || stderr.Len() != 0 {
				t.Fatalf("register() = %v, stderr=%q", err, stderr.String())
			}
		})
	}
}

func TestRegistrationAPIErrors(t *testing.T) {
	for _, tt := range []struct {
		name, verb, resource, subresource string
	}{
		{name: "Node get", verb: "get", resource: "nodes"},
		{name: "Node create", verb: "create", resource: "nodes"},
		{name: "status", verb: "update", resource: "nodes", subresource: "status"},
		{name: "Lease get", verb: "get", resource: "leases"},
		{name: "Lease create", verb: "create", resource: "leases"},
		{name: "Lease update", verb: "update", resource: "leases"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewClientset(testNode())
			if tt.verb == "create" && tt.resource == "nodes" {
				if err := client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("nodes"), "", "worker-1"); err != nil {
					t.Fatal(err)
				}
			}
			if tt.name == "Lease update" {
				if err := heartbeat(t.Context(), client.CoreV1().Nodes(), client.CoordinationV1().Leases(leaseNamespace), "worker-1", "node-uid"); err != nil {
					t.Fatal(err)
				}
			}
			denied := apierrors.NewForbidden(schema.GroupResource{Resource: tt.resource}, "worker-1", errors.New("denied"))
			client.PrependReactor(tt.verb, tt.resource, func(action ktesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() != tt.subresource {
					return false, nil, nil
				}
				return true, nil, denied
			})
			err := register(t.Context(), Config{NodeName: "worker-1"}, client.CoreV1().Nodes(),
				client.CoordinationV1().Leases(leaseNamespace), io.Discard)
			if !apierrors.IsForbidden(err) {
				t.Fatalf("register() = %v, want forbidden", err)
			}
		})
	}
}

func TestRegistrationCreate(t *testing.T) {
	client := fake.NewClientset()
	client.PrependReactor("create", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
		create, ok := action.(ktesting.CreateAction)
		if !ok {
			t.Fatal("expected create action")
		}
		node, ok := create.GetObject().(*corev1.Node)
		if !ok {
			t.Fatal("expected Node")
		}
		node.UID = "node-uid"
		return false, nil, nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	writer := cancelWriter{cancel: cancel}
	if err := register(ctx, Config{NodeName: "worker-1"}, client.CoreV1().Nodes(),
		client.CoordinationV1().Leases(leaseNamespace), writer); err != nil {
		t.Fatal(err)
	}
}

type cancelWriter struct{ cancel context.CancelFunc }

func (w cancelWriter) Write(p []byte) (int, error) {
	w.cancel()
	return len(p), nil
}

func TestHeartbeatConflictRetries(t *testing.T) {
	for _, tt := range []struct {
		name       string
		persistent bool
		replace    bool
		wantCalls  int
	}{
		{name: "transient status conflict", wantCalls: 2},
		{name: "persistent status conflict", persistent: true, wantCalls: 5},
		{name: "identity changes after conflict", replace: true, wantCalls: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewClientset(testNode())
			calls := 0
			client.PrependReactor("update", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() != "status" {
					return false, nil, nil
				}
				calls++
				if calls == 1 || tt.persistent {
					if tt.replace {
						node := testNode()
						node.UID = "replacement"
						if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""); err != nil {
							t.Fatal(err)
						}
					}
					return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "nodes"}, "worker-1", errors.New("changed"))
				}
				return false, nil, nil
			})
			err := heartbeat(t.Context(), client.CoreV1().Nodes(), client.CoordinationV1().Leases(leaseNamespace), "worker-1", "node-uid")
			switch {
			case tt.persistent:
				if !apierrors.IsConflict(err) {
					t.Fatalf("expected exhausted conflict, got %v", err)
				}
			case tt.replace:
				if err == nil || !strings.Contains(err.Error(), "changed identity") {
					t.Fatalf("expected identity refusal, got %v", err)
				}
			case err != nil:
				t.Fatal(err)
			}
			if calls != tt.wantCalls {
				t.Fatalf("status attempts = %d, want %d", calls, tt.wantCalls)
			}
		})
	}
}

func TestRegistrationConfiguration(t *testing.T) {
	for _, tt := range []struct{ name, config, want string }{
		{name: "missing file", want: "load registration kubeconfig"},
		{name: "HTTP", config: "server: http://127.0.0.1:6443", want: "HTTPS"},
		{name: "insecure", config: "server: https://127.0.0.1:6443\n    insecure-skip-tls-verify: true", want: "verified TLS"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config")
			if tt.config != "" {
				data := "apiVersion: v1\nkind: Config\ncurrent-context: test\ncontexts:\n- name: test\n  context:\n    cluster: test\nclusters:\n- name: test\n  cluster:\n    " + tt.config + "\n"
				if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
					t.Fatal(err)
				}

			}
			err := Register(t.Context(), Config{NodeName: "worker-1"}, path, io.Discard)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Register() = %v, want %s", err, tt.want)
			}
		})
	}
}

func TestHeartbeatIdentityChanged(t *testing.T) {
	node := testNode()
	client := fake.NewClientset(node)
	err := heartbeat(t.Context(), client.CoreV1().Nodes(), client.CoordinationV1().Leases(leaseNamespace),
		node.Name, "old-uid")
	if err == nil || !strings.Contains(err.Error(), "changed identity") {
		t.Fatalf("heartbeat() = %v, want identity error", err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" {
			t.Fatalf("unexpected mutation after identity change: %v", action)
		}
	}
}

func TestRegistrationStartupFailure(t *testing.T) {
	for _, tt := range []struct {
		name   string
		noUID  bool
		writer io.Writer
		want   string
	}{
		{name: "missing UID", noUID: true, writer: io.Discard, want: "no UID"},
		{name: "startup logging failure", writer: failingWriter{err: errors.New("unavailable")}, want: "write registration message"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			node := testNode()
			if tt.noUID {
				node.UID = ""
			}
			client := fake.NewClientset(node)
			err := register(t.Context(), Config{NodeName: node.Name}, client.CoreV1().Nodes(),
				client.CoordinationV1().Leases(leaseNamespace), tt.writer)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("register() = %v, want %s", err, tt.want)
			}
		})
	}
}

func TestRegistrationHeartbeatFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := fake.NewClientset(testNode())
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- register(ctx, Config{NodeName: "worker-1"}, client.CoreV1().Nodes(),
				client.CoordinationV1().Leases(leaseNamespace), io.Discard)
		}()
		synctest.Wait()
		node, err := client.CoreV1().Nodes().Get(ctx, "worker-1", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		node.Spec.Unschedulable = false
		if _, err := client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(heartbeatInterval)
		synctest.Wait()
		if err := <-done; err == nil || !strings.Contains(err.Error(), "scheduling enabled") {
			t.Fatalf("register() = %v, want unsafe state error", err)
		}
	})
}
