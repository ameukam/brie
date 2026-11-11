// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const (
	executionMarker = "brie.dev/execution-owner"
	runtimeLabel    = "brie.dev/runtime"
	podUIDLabel     = "io.kubernetes.pod.uid"
	configHashLabel = "brie.dev/config-hash"
)

func podExecutionHash(pod *corev1.Pod) (string, error) {
	spec := pod.Spec.DeepCopy()
	spec.NodeSelector, spec.Affinity, spec.Tolerations = nil, nil, nil
	spec.SchedulerName, spec.PriorityClassName = "", ""
	spec.Priority, spec.PreemptionPolicy, spec.TopologySpreadConstraints = nil, nil, nil
	data, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func podRuntimeConfigs(pod *corev1.Pod, config ExecutionConfig, owner, hash string) (*runtimeapi.PodSandboxConfig, *runtimeapi.ContainerConfig) {
	container := pod.Spec.Containers[0]
	security := pod.Spec.SecurityContext
	uidHash := sha256.Sum256([]byte(pod.UID))
	labels := map[string]string{
		executionMarker: owner, podUIDLabel: string(pod.UID), configHashLabel: hash,
	}
	namespaces := &runtimeapi.NamespaceOption{
		Network: runtimeapi.NamespaceMode_POD, Pid: runtimeapi.NamespaceMode_CONTAINER, Ipc: runtimeapi.NamespaceMode_POD,
	}
	sandbox := &runtimeapi.PodSandboxConfig{
		Metadata:     &runtimeapi.PodSandboxMetadata{Name: pod.Name, Namespace: pod.Namespace, Uid: string(pod.UID)},
		Hostname:     pod.Name[:min(63, len(pod.Name))],
		LogDirectory: filepath.Join(config.LogDir, hex.EncodeToString(uidHash[:])),
		Labels:       labels,
		DnsConfig: &runtimeapi.DNSConfig{
			Servers:  strings.Split(config.ClusterDNS, ","),
			Searches: []string{pod.Namespace + ".svc." + config.ClusterDomain, "svc." + config.ClusterDomain, config.ClusterDomain},
			Options:  []string{"ndots:5"},
		},
		Linux: &runtimeapi.LinuxPodSandboxConfig{
			SecurityContext: &runtimeapi.LinuxSandboxSecurityContext{
				NamespaceOptions: namespaces,
				RunAsUser:        &runtimeapi.Int64Value{Value: *security.RunAsUser},
				RunAsGroup:       &runtimeapi.Int64Value{Value: *security.RunAsGroup},
				Seccomp:          &runtimeapi.SecurityProfile{ProfileType: runtimeapi.SecurityProfile_RuntimeDefault},
			},
			Resources: criResources(container.Resources.Limits),
			Overhead:  criResources(pod.Spec.Overhead),
		},
	}
	readonly := container.SecurityContext.ReadOnlyRootFilesystem
	workload := &runtimeapi.ContainerConfig{
		Metadata: &runtimeapi.ContainerMetadata{Name: container.Name},
		Image:    &runtimeapi.ImageSpec{Image: container.Image},
		Command:  container.Command, Args: container.Args, WorkingDir: container.WorkingDir,
		Labels: labels, LogPath: "0.log",
		Linux: &runtimeapi.LinuxContainerConfig{
			Resources: criResources(container.Resources.Limits),
			SecurityContext: &runtimeapi.LinuxContainerSecurityContext{
				NamespaceOptions: namespaces, NoNewPrivs: true,
				RunAsUser:      &runtimeapi.Int64Value{Value: *security.RunAsUser},
				RunAsGroup:     &runtimeapi.Int64Value{Value: *security.RunAsGroup},
				ReadonlyRootfs: readonly != nil && *readonly,
				Capabilities:   &runtimeapi.Capability{DropCapabilities: []string{"ALL"}},
				Seccomp:        &runtimeapi.SecurityProfile{ProfileType: runtimeapi.SecurityProfile_RuntimeDefault},
				MaskedPaths: []string{"/proc/acpi", "/proc/asound", "/proc/kcore", "/proc/keys",
					"/proc/latency_stats", "/proc/sched_debug", "/proc/scsi", "/proc/timer_list", "/proc/timer_stats", "/sys/firmware"},
				ReadonlyPaths: []string{"/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger"},
			},
		},
	}
	for _, env := range container.Env {
		workload.Envs = append(workload.Envs, &runtimeapi.KeyValue{Key: env.Name, Value: []byte(env.Value)})
	}
	return sandbox, workload
}

func criResources(resources corev1.ResourceList) *runtimeapi.LinuxContainerResources {
	cpu, memory, err := executionResources(resources)
	if err != nil {
		return nil
	}
	shares := int64(262144)
	if cpu < 256000 {
		shares = max(2, cpu*1024/1000)
	}
	return &runtimeapi.LinuxContainerResources{
		CpuPeriod: 100000, CpuQuota: cpu * 100, CpuShares: shares,
		MemoryLimitInBytes:     memory,
		MemorySwapLimitInBytes: memory,
	}
}

func addBudget(cpu, memory, overheadCPU, overheadMemory int64) (int64, int64, bool) {
	if cpu > math.MaxInt64-overheadCPU || memory > math.MaxInt64-overheadMemory {
		return 0, 0, false
	}
	return cpu + overheadCPU, memory + overheadMemory, true
}
