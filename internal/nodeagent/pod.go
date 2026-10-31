// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/validation"
)

// ValidatePodProfile checks the initial one-container experiment contract.
// The node and RuntimeClass names must come from trusted configuration, not the
// Pod. A match does not verify the Kata/Firecracker handler or admit execution.
// This validator is not yet wired to a Pod watch or runtime mutation path.
func ValidatePodProfile(pod *corev1.Pod, nodeName, runtimeClassName string) error {
	if err := (Config{NodeName: nodeName}).Validate(); err != nil {
		return fmt.Errorf("pod profile node: %w", err)
	}
	if runtimeClassName == "" || len(validation.IsDNS1123Subdomain(runtimeClassName)) != 0 {
		return errors.New("pod profile requires an explicit valid RuntimeClass name")
	}
	if pod == nil || pod.Name == "" || pod.Namespace == "" || pod.UID == "" {
		return errors.New("pod profile requires Pod name, namespace, and UID")
	}
	if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return errors.New("deleting or terminal Pods cannot enter the initial Pod profile")
	}
	spec := pod.Spec
	if spec.NodeName != nodeName {
		return errors.New("spec.nodeName must match the configured node")
	}
	if spec.RuntimeClassName == nil || *spec.RuntimeClassName != runtimeClassName {
		return errors.New("spec.runtimeClassName must match the configured Kata/Firecracker RuntimeClass")
	}
	if spec.OS == nil || spec.OS.Name != corev1.Linux {
		return errors.New("spec.os.name must explicitly be linux")
	}
	if len(spec.Containers) != 1 || spec.RestartPolicy != corev1.RestartPolicyNever {
		return errors.New("pod profile requires exactly one container and restartPolicy Never")
	}
	if spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken ||
		spec.EnableServiceLinks == nil || *spec.EnableServiceLinks {
		return errors.New("pod profile requires automountServiceAccountToken and enableServiceLinks explicitly false")
	}
	if spec.DNSPolicy != corev1.DNSClusterFirst {
		return errors.New("spec.dnsPolicy must be ClusterFirst")
	}
	if spec.TerminationGracePeriodSeconds != nil && *spec.TerminationGracePeriodSeconds < 0 {
		return errors.New("spec.terminationGracePeriodSeconds must not be negative")
	}
	if spec.HostUsers != nil && !*spec.HostUsers {
		return errors.New("spec.hostUsers=false is not supported by the initial Pod profile")
	}
	// Only scheduler-owned placement fields and the selected execution fields
	// pass through. New Kubernetes fields fail closed until reviewed.
	if err := onlyProfileFields("spec", spec, corev1.PodSpec{
		Containers: spec.Containers, RestartPolicy: spec.RestartPolicy,
		TerminationGracePeriodSeconds: spec.TerminationGracePeriodSeconds,
		DNSPolicy:                     spec.DNSPolicy, NodeName: spec.NodeName, OS: spec.OS,
		RuntimeClassName: spec.RuntimeClassName, SecurityContext: spec.SecurityContext,
		AutomountServiceAccountToken: spec.AutomountServiceAccountToken,
		EnableServiceLinks:           spec.EnableServiceLinks, HostUsers: spec.HostUsers,
		ServiceAccountName: spec.ServiceAccountName, DeprecatedServiceAccount: spec.DeprecatedServiceAccount,
		NodeSelector: spec.NodeSelector, Affinity: spec.Affinity, SchedulerName: spec.SchedulerName,
		Tolerations: spec.Tolerations, PriorityClassName: spec.PriorityClassName,
		Priority: spec.Priority, PreemptionPolicy: spec.PreemptionPolicy,
		TopologySpreadConstraints: spec.TopologySpreadConstraints,
	}); err != nil {
		return err
	}
	if err := validatePodSecurity(spec.SecurityContext); err != nil {
		return err
	}
	return validateProfileContainer(spec.Containers[0])
}

func validatePodSecurity(security *corev1.PodSecurityContext) error {
	if security == nil || security.RunAsNonRoot == nil || !*security.RunAsNonRoot ||
		security.RunAsUser == nil || *security.RunAsUser <= 0 ||
		security.RunAsGroup == nil || *security.RunAsGroup <= 0 {
		return errors.New("spec.securityContext requires runAsNonRoot=true and positive runAsUser/runAsGroup")
	}
	if !equality.Semantic.DeepEqual(security.SeccompProfile, &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}) {
		return errors.New("spec.securityContext.seccompProfile must be RuntimeDefault")
	}
	return onlyProfileFields("spec.securityContext", *security, corev1.PodSecurityContext{
		RunAsNonRoot: security.RunAsNonRoot, RunAsUser: security.RunAsUser,
		RunAsGroup: security.RunAsGroup, SeccompProfile: security.SeccompProfile,
	})
}

func validateProfileContainer(container corev1.Container) error {
	if container.Name == "" || !digestPinnedImage(container.Image) {
		return errors.New("container requires a name and an image pinned with @sha256:<64 lowercase hex digits>")
	}
	if container.ImagePullPolicy != corev1.PullIfNotPresent {
		return errors.New("container.imagePullPolicy must be IfNotPresent")
	}
	if container.TerminationMessagePath != "" && container.TerminationMessagePath != "/dev/termination-log" ||
		container.TerminationMessagePolicy != "" && container.TerminationMessagePolicy != corev1.TerminationMessageReadFile {
		return errors.New("only default container termination-message settings are supported")
	}
	if err := onlyProfileFields("container", container, corev1.Container{
		Name: container.Name, Image: container.Image, Command: container.Command, Args: container.Args,
		WorkingDir: container.WorkingDir, Env: container.Env, Resources: container.Resources,
		ImagePullPolicy: container.ImagePullPolicy, SecurityContext: container.SecurityContext,
		TerminationMessagePath: container.TerminationMessagePath, TerminationMessagePolicy: container.TerminationMessagePolicy,
	}); err != nil {
		return err
	}
	for _, env := range container.Env {
		if env.ValueFrom != nil {
			return errors.New("container.env.valueFrom is not supported; only literal environment values are allowed")
		}
	}
	resources := container.Resources
	if len(resources.Requests) != 2 || len(resources.Limits) != 2 || len(resources.Claims) != 0 {
		return errors.New("container.resources must contain only CPU and memory requests and limits")
	}
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		request, requested := resources.Requests[name]
		limit, limited := resources.Limits[name]
		if !requested || !limited || request.Sign() <= 0 || request.Cmp(limit) != 0 {
			return fmt.Errorf("container.resources.%s requires equal positive request and limit", name)
		}
	}
	security := container.SecurityContext
	if security == nil || security.AllowPrivilegeEscalation == nil || *security.AllowPrivilegeEscalation ||
		security.Privileged != nil && *security.Privileged {
		return errors.New("container.securityContext must disable privilege escalation and privileged mode")
	}
	if !equality.Semantic.DeepEqual(security.Capabilities, &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}) {
		return errors.New("container.securityContext.capabilities must drop ALL and add nothing")
	}
	return onlyProfileFields("container.securityContext", *security, corev1.SecurityContext{
		AllowPrivilegeEscalation: security.AllowPrivilegeEscalation, Privileged: security.Privileged,
		Capabilities: security.Capabilities, ReadOnlyRootFilesystem: security.ReadOnlyRootFilesystem,
	})
}

func digestPinnedImage(image string) bool {
	repository, digest, found := strings.Cut(image, "@sha256:")
	if !found || repository == "" || len(digest) != 64 {
		return false
	}
	for _, ch := range repository {
		if ch <= ' ' || ch > '~' || ch == '@' {
			return false
		}
	}
	for _, ch := range digest {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

func onlyProfileFields(path string, value, allowed any) error {
	actual, supported := reflect.ValueOf(value), reflect.ValueOf(allowed)
	for i := 0; i < actual.NumField(); i++ {
		if !equality.Semantic.DeepEqual(actual.Field(i).Interface(), supported.Field(i).Interface()) {
			name := strings.Split(actual.Type().Field(i).Tag.Get("json"), ",")[0]
			return fmt.Errorf("%s.%s is not supported by the initial Pod profile", path, name)
		}
	}
	return nil
}
