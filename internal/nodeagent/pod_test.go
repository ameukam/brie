// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func profilePod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "experiment", Namespace: "default", UID: "pod-uid"},
		Spec: corev1.PodSpec{
			NodeName: "worker-1", RuntimeClassName: ptr.To("kata-fc"),
			OS: &corev1.PodOS{Name: corev1.Linux}, RestartPolicy: corev1.RestartPolicyNever,
			DNSPolicy: corev1.DNSClusterFirst, TerminationGracePeriodSeconds: ptr.To(int64(30)),
			AutomountServiceAccountToken: ptr.To(false), EnableServiceLinks: ptr.To(false),
			ServiceAccountName: "default", DeprecatedServiceAccount: "default",
			SchedulerName: corev1.DefaultSchedulerName,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To(int64(1000)), RunAsGroup: ptr.To(int64(1000)),
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name: "app", Image: "example.test/app@sha256:" + strings.Repeat("a", 64),
				Command: []string{"/app"}, Args: []string{"serve"}, WorkingDir: "/",
				Env: []corev1.EnvVar{{Name: "MODE", Value: "test"}}, ImagePullPolicy: corev1.PullIfNotPresent,
				TerminationMessagePath: "/dev/termination-log", TerminationMessagePolicy: corev1.TerminationMessageReadFile,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("128Mi")},
					Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1000m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
				},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: ptr.To(false),
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
			}},
		},
	}
}

func TestValidatePodProfile(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		change func(*corev1.Pod)
		want   string
	}{
		{name: "defaulted pod"},
		{name: "scheduler fields", change: func(p *corev1.Pod) {
			p.Spec.NodeSelector = map[string]string{"kubernetes.io/arch": "arm64"}
			p.Spec.Affinity = &corev1.Affinity{}
			p.Spec.Tolerations = []corev1.Toleration{{Key: "experiment", Operator: corev1.TolerationOpExists}}
			p.Spec.PriorityClassName, p.Spec.Priority = "test", ptr.To(int32(10))
			p.Spec.PreemptionPolicy = ptr.To(corev1.PreemptLowerPriority)
			p.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{TopologyKey: "kubernetes.io/hostname"}}
		}},
		{name: "explicit safe defaults", change: func(p *corev1.Pod) {
			p.Spec.HostUsers = ptr.To(true)
			p.Spec.Containers[0].SecurityContext.Privileged = ptr.To(false)
			p.Spec.Containers[0].SecurityContext.ReadOnlyRootFilesystem = ptr.To(true)
		}},
		{name: "missing uid", change: func(p *corev1.Pod) { p.UID = "" }, want: "UID"},
		{name: "missing name", change: func(p *corev1.Pod) { p.Name = "" }, want: "name"},
		{name: "missing namespace", change: func(p *corev1.Pod) { p.Namespace = "" }, want: "namespace"},
		{name: "deleting", change: func(p *corev1.Pod) { p.DeletionTimestamp = ptr.To(metav1.Now()) }, want: "deleting"},
		{name: "terminal", change: func(p *corev1.Pod) { p.Status.Phase = corev1.PodSucceeded }, want: "terminal"},
		{name: "unassigned", change: func(p *corev1.Pod) { p.Spec.NodeName = "" }, want: "nodeName"},
		{name: "wrong node", change: func(p *corev1.Pod) { p.Spec.NodeName = "other" }, want: "nodeName"},
		{name: "missing runtime class", change: func(p *corev1.Pod) { p.Spec.RuntimeClassName = nil }, want: "runtimeClassName"},
		{name: "wrong runtime class", change: func(p *corev1.Pod) { p.Spec.RuntimeClassName = ptr.To("runc") }, want: "runtimeClassName"},
		{name: "missing os", change: func(p *corev1.Pod) { p.Spec.OS = nil }, want: "linux"},
		{name: "windows", change: func(p *corev1.Pod) { p.Spec.OS.Name = corev1.Windows }, want: "linux"},
		{name: "no container", change: func(p *corev1.Pod) { p.Spec.Containers = nil }, want: "exactly one"},
		{name: "multiple containers", change: func(p *corev1.Pod) { p.Spec.Containers = append(p.Spec.Containers, p.Spec.Containers[0]) }, want: "exactly one"},
		{name: "restart", change: func(p *corev1.Pod) { p.Spec.RestartPolicy = corev1.RestartPolicyAlways }, want: "Never"},
		{name: "implicit tokens", change: func(p *corev1.Pod) { p.Spec.AutomountServiceAccountToken = nil }, want: "automount"},
		{name: "tokens", change: func(p *corev1.Pod) { p.Spec.AutomountServiceAccountToken = ptr.To(true) }, want: "automount"},
		{name: "service links", change: func(p *corev1.Pod) { p.Spec.EnableServiceLinks = ptr.To(true) }, want: "enableServiceLinks"},
		{name: "custom dns", change: func(p *corev1.Pod) { p.Spec.DNSPolicy = corev1.DNSNone }, want: "dnsPolicy"},
		{name: "negative grace", change: func(p *corev1.Pod) { p.Spec.TerminationGracePeriodSeconds = ptr.To(int64(-1)) }, want: "terminationGrace"},
		{name: "user namespaces", change: func(p *corev1.Pod) { p.Spec.HostUsers = ptr.To(false) }, want: "hostUsers"},
		{name: "host networking", change: func(p *corev1.Pod) { p.Spec.HostNetwork = true }, want: "hostNetwork"},
		{name: "host pid", change: func(p *corev1.Pod) { p.Spec.HostPID = true }, want: "hostPID"},
		{name: "host ipc", change: func(p *corev1.Pod) { p.Spec.HostIPC = true }, want: "hostIPC"},
		{name: "shared processes", change: func(p *corev1.Pod) { p.Spec.ShareProcessNamespace = ptr.To(true) }, want: "shareProcessNamespace"},
		{name: "volumes", change: func(p *corev1.Pod) { p.Spec.Volumes = []corev1.Volume{{Name: "scratch"}} }, want: "volumes"},
		{name: "init", change: func(p *corev1.Pod) { p.Spec.InitContainers = []corev1.Container{{Name: "init"}} }, want: "initContainers"},
		{name: "ephemeral", change: func(p *corev1.Pod) { p.Spec.EphemeralContainers = []corev1.EphemeralContainer{{}} }, want: "ephemeralContainers"},
		{name: "image secrets", change: func(p *corev1.Pod) { p.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "secret"}} }, want: "imagePullSecrets"},
		{name: "deadline", change: func(p *corev1.Pod) { p.Spec.ActiveDeadlineSeconds = ptr.To(int64(10)) }, want: "activeDeadlineSeconds"},
		{name: "readiness gates", change: func(p *corev1.Pod) { p.Spec.ReadinessGates = []corev1.PodReadinessGate{{ConditionType: "test"}} }, want: "readinessGates"},
		{name: "overhead", change: func(p *corev1.Pod) {
			p.Spec.Overhead = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
		}, want: "overhead"},
		{name: "pod resources", change: func(p *corev1.Pod) { p.Spec.Resources = &corev1.ResourceRequirements{} }, want: "spec.resources"},
		{name: "dra", change: func(p *corev1.Pod) { p.Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: "device"}} }, want: "resourceClaims"},
		{name: "missing pod security", change: func(p *corev1.Pod) { p.Spec.SecurityContext = nil }, want: "runAsNonRoot"},
		{name: "allow root", change: func(p *corev1.Pod) { p.Spec.SecurityContext.RunAsNonRoot = ptr.To(false) }, want: "runAsNonRoot"},
		{name: "root user", change: func(p *corev1.Pod) { p.Spec.SecurityContext.RunAsUser = ptr.To(int64(0)) }, want: "runAsUser"},
		{name: "negative group", change: func(p *corev1.Pod) { p.Spec.SecurityContext.RunAsGroup = ptr.To(int64(-1)) }, want: "runAsGroup"},
		{name: "unconfined", change: func(p *corev1.Pod) { p.Spec.SecurityContext.SeccompProfile.Type = corev1.SeccompProfileTypeUnconfined }, want: "seccompProfile"},
		{name: "seccomp local path", change: func(p *corev1.Pod) { p.Spec.SecurityContext.SeccompProfile.LocalhostProfile = ptr.To("local") }, want: "seccompProfile"},
		{name: "sysctl", change: func(p *corev1.Pod) {
			p.Spec.SecurityContext.Sysctls = []corev1.Sysctl{{Name: "kernel.test", Value: "1"}}
		}, want: "sysctls"},
		{name: "supplemental groups", change: func(p *corev1.Pod) { p.Spec.SecurityContext.SupplementalGroups = []int64{1000} }, want: "supplementalGroups"},
		{name: "unnamed container", change: func(p *corev1.Pod) { p.Spec.Containers[0].Name = "" }, want: "name"},
		{name: "mutable image", change: func(p *corev1.Pod) { p.Spec.Containers[0].Image = "example.test/app:latest" }, want: "sha256"},
		{name: "short digest", change: func(p *corev1.Pod) { p.Spec.Containers[0].Image = "example.test/app@sha256:abc" }, want: "sha256"},
		{name: "uppercase digest", change: func(p *corev1.Pod) { p.Spec.Containers[0].Image = "example.test/app@sha256:" + strings.Repeat("A", 64) }, want: "sha256"},
		{name: "empty repository", change: func(p *corev1.Pod) { p.Spec.Containers[0].Image = "@sha256:" + strings.Repeat("a", 64) }, want: "sha256"},
		{name: "image whitespace", change: func(p *corev1.Pod) {
			p.Spec.Containers[0].Image = " example.test/app@sha256:" + strings.Repeat("a", 64)
		}, want: "sha256"},
		{name: "extra digest", change: func(p *corev1.Pod) { p.Spec.Containers[0].Image = "app@other@sha256:" + strings.Repeat("a", 64) }, want: "sha256"},
		{name: "pull always", change: func(p *corev1.Pod) { p.Spec.Containers[0].ImagePullPolicy = corev1.PullAlways }, want: "imagePullPolicy"},
		{name: "custom termination path", change: func(p *corev1.Pod) { p.Spec.Containers[0].TerminationMessagePath = "/custom" }, want: "termination-message"},
		{name: "termination logs", change: func(p *corev1.Pod) {
			p.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError
		}, want: "termination-message"},
		{name: "port", change: func(p *corev1.Pod) { p.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 8080}} }, want: "ports"},
		{name: "env refs", change: func(p *corev1.Pod) { p.Spec.Containers[0].Env[0].ValueFrom = &corev1.EnvVarSource{} }, want: "valueFrom"},
		{name: "env from", change: func(p *corev1.Pod) { p.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{}} }, want: "envFrom"},
		{name: "probe", change: func(p *corev1.Pod) { p.Spec.Containers[0].ReadinessProbe = &corev1.Probe{} }, want: "readinessProbe"},
		{name: "lifecycle", change: func(p *corev1.Pod) { p.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{} }, want: "lifecycle"},
		{name: "mount", change: func(p *corev1.Pod) {
			p.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "v", MountPath: "/v"}}
		}, want: "volumeMounts"},
		{name: "block device", change: func(p *corev1.Pod) {
			p.Spec.Containers[0].VolumeDevices = []corev1.VolumeDevice{{Name: "v", DevicePath: "/dev/v"}}
		}, want: "volumeDevices"},
		{name: "streaming", change: func(p *corev1.Pod) { p.Spec.Containers[0].Stdin = true }, want: "stdin"},
		{name: "resize", change: func(p *corev1.Pod) {
			p.Spec.Containers[0].ResizePolicy = []corev1.ContainerResizePolicy{{ResourceName: corev1.ResourceCPU}}
		}, want: "resizePolicy"},
		{name: "no requests", change: func(p *corev1.Pod) { p.Spec.Containers[0].Resources.Requests = nil }, want: "CPU and memory"},
		{name: "missing memory", change: func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), "example.test/gpu": resource.MustParse("1")}
		}, want: "memory"},
		{name: "zero cpu", change: func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("0")
		}, want: "cpu"},
		{name: "unequal limits", change: func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory] = resource.MustParse("256Mi")
		}, want: "memory"},
		{name: "extended resources", change: func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Limits["example.test/gpu"] = resource.MustParse("1")
		}, want: "CPU and memory"},
		{name: "missing container security", change: func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext = nil }, want: "privilege"},
		{name: "privileged", change: func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.Privileged = ptr.To(true) }, want: "privileged"},
		{name: "escalation", change: func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.AllowPrivilegeEscalation = ptr.To(true) }, want: "privilege"},
		{name: "capability add", change: func(p *corev1.Pod) {
			p.Spec.Containers[0].SecurityContext.Capabilities.Add = []corev1.Capability{"NET_ADMIN"}
		}, want: "drop ALL"},
		{name: "capability drop", change: func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.Capabilities = nil }, want: "drop ALL"},
		{name: "user override", change: func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.RunAsUser = ptr.To(int64(0)) }, want: "runAsUser"},
		{name: "seccomp override", change: func(p *corev1.Pod) {
			p.Spec.Containers[0].SecurityContext.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeUnconfined}
		}, want: "seccompProfile"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pod := profilePod()
			if tt.change != nil {
				tt.change(pod)
			}
			before := pod.DeepCopy()
			err := ValidatePodProfile(pod, "worker-1", "kata-fc")
			if tt.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
			if !equality.Semantic.DeepEqual(pod, before) {
				t.Fatal("profile validation mutated the Pod")
			}
		})
	}
}

func TestPodProfileConfiguration(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		pod   *corev1.Pod
		node  string
		class string
	}{
		{pod: nil, node: "worker-1", class: "kata-fc"},
		{pod: profilePod(), node: "", class: "kata-fc"},
		{pod: profilePod(), node: "worker-1", class: ""},
		{pod: profilePod(), node: "worker-1", class: "invalid/class"},
	} {
		if err := ValidatePodProfile(tt.pod, tt.node, tt.class); err == nil {
			t.Fatal("accepted missing Pod or invalid trusted profile configuration")
		}
	}
}

func FuzzPodProfile(f *testing.F) {
	for _, image := range []string{
		"example.test/app@sha256:" + strings.Repeat("a", 64),
		"example.test/app:latest", "@sha256:", "app\x00", "app\xff",
	} {
		f.Add(image, int64(1000), uint8(0))
	}
	f.Fuzz(func(t *testing.T, image string, uid int64, feature uint8) {
		pod := profilePod()
		pod.Spec.Containers[0].Image = image
		pod.Spec.SecurityContext.RunAsUser = ptr.To(uid)
		switch feature % 4 {
		case 1:
			pod.Spec.HostNetwork = true
		case 2:
			pod.Spec.Volumes = []corev1.Volume{{Name: "unsupported"}}
		case 3:
			pod.Spec.Containers[0].SecurityContext.RunAsUser = ptr.To(int64(0))
		}
		before := pod.DeepCopy()
		if err := ValidatePodProfile(pod, "worker-1", "kata-fc"); err == nil && (uid <= 0 || feature%4 != 0) {
			t.Fatal("accepted root execution or an unsupported feature")
		}
		if !equality.Semantic.DeepEqual(pod, before) {
			t.Fatal("profile validation mutated the Pod")
		}
	})
}
