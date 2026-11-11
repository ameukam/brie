// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func FuzzExecutionResources(f *testing.F) {
	for _, seed := range [][2]string{{"1", "128Mi"}, {"0.0001", "1"}, {"92233720368547758m", "9223372036854775807"}, {"-1", "1"}, {"1", "invalid"}} {
		f.Add(seed[0], seed[1])
	}
	f.Fuzz(func(t *testing.T, cpu, memory string) {
		resources := corev1.ResourceList{corev1.ResourceCPU: parseQuantity(cpu), corev1.ResourceMemory: parseQuantity(memory)}
		if _, _, err := executionResources(resources); err != nil {
			return
		}
		limits := criResources(resources)
		if limits == nil || limits.CpuQuota <= 0 || limits.CpuShares < 2 || limits.CpuShares > 262144 ||
			limits.MemoryLimitInBytes <= 0 {
			t.Fatalf("accepted resources overflowed CRI limits: %v", limits)
		}
	})
}
