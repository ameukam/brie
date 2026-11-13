// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
	"path/filepath"
	"runtime"
	"strings"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

// ExecutionConfig is an explicit opt-in envelope, not discovered host capacity.
// Operators must reserve host services and qualify the runtime's VM overhead.
type ExecutionConfig struct {
	RuntimeClass   string
	RuntimeHandler string
	CPU            string
	Memory         string
	MaxPods        int
	NodeIP         string
	ClusterDNS     string
	ClusterDomain  string
	StateDir       string
	LogDir         string
	// ImageGCThreshold, when set, is the image filesystem usage in bytes
	// above which unreferenced images are removed through CRI. Empty
	// disables image collection.
	ImageGCThreshold string
}

// Validate rejects incomplete execution configuration before contacting services.
func (c ExecutionConfig) Validate() error {
	if runtime.GOOS != "linux" {
		return errors.New("workload execution requires Linux")
	}
	if c.RuntimeClass == "" || len(validation.IsDNS1123Subdomain(c.RuntimeClass)) != 0 ||
		c.RuntimeHandler == "" || len(validation.IsDNS1123Label(c.RuntimeHandler)) != 0 {
		return errors.New("execution requires explicit valid runtime-class and runtime-handler names")
	}
	if c.MaxPods < 1 || c.MaxPods > 10000 {
		return errors.New("max-pods must be between 1 and 10000")
	}
	if _, _, err := executionResources(corev1.ResourceList{
		corev1.ResourceCPU: parseQuantity(c.CPU), corev1.ResourceMemory: parseQuantity(c.Memory),
	}); err != nil {
		return fmt.Errorf("execution capacity: %w", err)
	}
	for _, path := range []string{c.StateDir, c.LogDir} {
		if !filepath.IsAbs(path) || !utf8.ValidString(path) || strings.ContainsRune(path, '\x00') || filepath.Clean(path) == "/" {
			return errors.New("execution requires explicit absolute non-root state-dir and pod-log-dir paths")
		}
	}
	if filepath.Clean(c.StateDir) == filepath.Clean(c.LogDir) {
		return errors.New("state-dir and pod-log-dir must be distinct")
	}
	ip, err := netip.ParseAddr(c.NodeIP)
	if err != nil || !ip.IsGlobalUnicast() {
		return errors.New("execution requires an explicit unicast node-ip")
	}
	servers := strings.Split(c.ClusterDNS, ",")
	if len(servers) > 3 {
		return errors.New("cluster-dns accepts at most three IP addresses")
	}
	for _, server := range servers {
		ip, err := netip.ParseAddr(server)
		if err != nil || !ip.IsGlobalUnicast() {
			return errors.New("cluster-dns requires explicit unicast IP addresses")
		}
	}
	if c.ClusterDomain == "" || len(validation.IsDNS1123Subdomain(c.ClusterDomain)) != 0 {
		return errors.New("execution requires an explicit valid cluster-domain")
	}
	if _, err := imageGCThresholdBytes(c.ImageGCThreshold); err != nil {
		return err
	}
	return nil
}

// imageGCThresholdBytes parses the image collection threshold. Empty means
// disabled and returns zero; otherwise the value is a positive whole number
// of bytes, such as 50Gi.
func imageGCThresholdBytes(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		return 0, errors.New("image-gc-threshold must be a byte quantity such as 50Gi")
	}
	bytes, exact := quantity.AsInt64()
	if !exact || bytes <= 0 {
		return 0, errors.New("image-gc-threshold must be a positive whole number of bytes fitting int64")
	}
	return bytes, nil
}

func parseQuantity(value string) resource.Quantity {
	q, _ := resource.ParseQuantity(value)
	return q
}

func executionResources(resources corev1.ResourceList) (int64, int64, error) {
	cpu, memory := resources[corev1.ResourceCPU], resources[corev1.ResourceMemory]
	maxCPU := resource.NewMilliQuantity(math.MaxInt64/100, resource.DecimalSI)
	if cpu.Sign() <= 0 || cpu.Cmp(*maxCPU) > 0 {
		return 0, 0, errors.New("CPU must be positive and representable as a CRI quota")
	}
	milli := cpu.MilliValue()
	if cpu.Cmp(*resource.NewMilliQuantity(milli, resource.DecimalSI)) != 0 {
		return 0, 0, errors.New("CPU precision must not be smaller than one millicore")
	}
	bytes, exact := memory.AsInt64()
	if !exact || bytes <= 0 {
		return 0, 0, errors.New("memory must be a positive whole number of bytes fitting int64")
	}
	return milli, bytes, nil
}
