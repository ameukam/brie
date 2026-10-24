// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"runtime"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coordinationclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
)

const (
	registrationMarker = "brie.dev/registration-only"
	leaseNamespace     = "kube-node-lease"
	heartbeatInterval  = 10 * time.Second
)

// Register uses only the explicitly supplied kubeconfig, never ambient credentials.
// It advertises no schedulable resources and exits on API errors.
func Register(ctx context.Context, config Config, kubeconfig string, stderr io.Writer) error {
	if err := config.Validate(); err != nil {
		return fmt.Errorf("validate agent configuration: %w", err)
	}
	if !filepath.IsAbs(kubeconfig) {
		return errors.New("registration requires an explicit absolute kubeconfig path")
	}
	if err := ctx.Err(); err != nil {
		return shutdownError(err)
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return fmt.Errorf("load registration kubeconfig: %w", err)
	}
	endpoint, err := url.Parse(cfg.Host)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil {
		return errors.New("registration requires an HTTPS API endpoint without embedded credentials")
	}
	if cfg.Insecure || cfg.ExecProvider != nil || cfg.AuthProvider != nil ||
		cfg.Impersonate.UserName != "" || cfg.Impersonate.UID != "" || len(cfg.Impersonate.Groups) != 0 ||
		len(cfg.Impersonate.Extra) != 0 {
		return errors.New("registration requires verified TLS and static credentials without impersonation or credential plugins")
	}
	cfg.Timeout = 10 * time.Second
	cfg.UserAgent = "brie-node/registration-only"
	nodes, err := coreclient.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("create Node client: %w", err)
	}
	leases, err := coordinationclient.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("create Lease client: %w", err)
	}
	return register(ctx, config, nodes.Nodes(), leases.Leases(leaseNamespace), stderr)
}

func register(ctx context.Context, config Config, nodes coreclient.NodeInterface, leases coordinationclient.LeaseInterface, stderr io.Writer) error {
	node, err := nodes.Get(ctx, config.NodeName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		node, err = nodes.Create(ctx, &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:        config.NodeName,
				Annotations: map[string]string{registrationMarker: "true"},
				Labels: map[string]string{
					"kubernetes.io/os": runtime.GOOS, "kubernetes.io/arch": runtime.GOARCH,
				},
			},
			Spec: corev1.NodeSpec{
				Unschedulable: true,
				Taints: []corev1.Taint{{
					Key: registrationMarker, Value: "true", Effect: corev1.TaintEffectNoExecute,
				}},
			},
		}, metav1.CreateOptions{})
	}
	if err != nil {
		if ctx.Err() != nil {
			return shutdownError(ctx.Err())
		}
		return fmt.Errorf("register Node: %w", err)
	}
	uid := node.UID
	if uid == "" {
		return errors.New("registered Node has no UID")
	}
	if err := heartbeat(ctx, nodes, leases, config.NodeName, uid); err != nil {
		if ctx.Err() != nil {
			return shutdownError(ctx.Err())
		}
		return err
	}
	if _, err := fmt.Fprintf(stderr,
		"brie-node: registered %s as NotReady and unschedulable with zero Pod capacity; workload execution is not implemented\n",
		config.NodeName); err != nil {
		return fmt.Errorf("write registration message: %w", err)
	}
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return shutdownError(ctx.Err())
		case <-ticker.C:
			if err := heartbeat(ctx, nodes, leases, config.NodeName, uid); err != nil {
				if ctx.Err() != nil {
					return shutdownError(ctx.Err())
				}
				return err
			}
		}
	}
}

func heartbeat(ctx context.Context, nodes coreclient.NodeInterface, leases coordinationclient.LeaseInterface, name string, uid types.UID) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return heartbeatOnce(ctx, nodes, leases, name, uid)
	})
}

func heartbeatOnce(ctx context.Context, nodes coreclient.NodeInterface, leases coordinationclient.LeaseInterface, name string, uid types.UID) error {
	node, err := nodes.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read Node for heartbeat: %w", err)
	}
	if node.UID != uid || node.Annotations[registrationMarker] != "true" || !node.Spec.Unschedulable {
		return errors.New("refusing Node with changed identity, missing registration marker, or scheduling enabled")
	}
	tainted := false
	for _, taint := range node.Spec.Taints {
		if taint.Key == registrationMarker && taint.Value == "true" && taint.Effect == corev1.TaintEffectNoExecute {
			tainted = true
		}
	}
	if !tainted {
		return errors.New("refusing Node without registration-only NoExecute taint")
	}
	zero := corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("0"), corev1.ResourceMemory: resource.MustParse("0"),
		corev1.ResourcePods: resource.MustParse("0"),
	}
	now := metav1.Now()
	transition := now
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionFalse &&
			condition.Reason == "RuntimeNotImplemented" {
			transition = condition.LastTransitionTime
		}
	}
	node.Status = corev1.NodeStatus{
		Capacity: zero.DeepCopy(), Allocatable: zero.DeepCopy(),
		NodeInfo: corev1.NodeSystemInfo{OperatingSystem: runtime.GOOS, Architecture: runtime.GOARCH},
		Conditions: []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: corev1.ConditionFalse,
			Reason: "RuntimeNotImplemented", Message: "Brie registration only; workload execution is not implemented",
			LastHeartbeatTime: now, LastTransitionTime: transition,
		}},
	}
	if _, err := nodes.UpdateStatus(ctx, node, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update Node status: %w", err)
	}
	lease, err := leases.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		lease = &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: leaseNamespace},
		}
	} else if err != nil {
		return fmt.Errorf("read Node Lease: %w", err)
	} else if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != name ||
		len(lease.OwnerReferences) != 1 || lease.OwnerReferences[0].UID != uid ||
		lease.OwnerReferences[0].Kind != "Node" || lease.OwnerReferences[0].APIVersion != "v1" ||
		lease.OwnerReferences[0].Name != name {
		return errors.New("refusing Lease with a different owner or holder")
	}
	lease.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: name, UID: uid}}
	duration := int32(40)
	renew := metav1.NewMicroTime(time.Now())
	lease.Spec = coordinationv1.LeaseSpec{
		HolderIdentity: &name, LeaseDurationSeconds: &duration, RenewTime: &renew,
	}
	if apierrors.IsNotFound(err) {
		_, err = leases.Create(ctx, lease, metav1.CreateOptions{})
	} else {
		_, err = leases.Update(ctx, lease, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("renew Node Lease: %w", err)
	}
	return nil
}
