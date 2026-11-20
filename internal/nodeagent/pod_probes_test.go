// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
)

// probeOf returns an API-defaulted probe.
func probeOf(handler corev1.ProbeHandler) *corev1.Probe {
	return &corev1.Probe{ProbeHandler: handler, TimeoutSeconds: 1, PeriodSeconds: 10, SuccessThreshold: 1, FailureThreshold: 3}
}

func probedPod() *corev1.Pod {
	pod := profilePod()
	container := &pod.Spec.Containers[0]
	container.Ports = []corev1.ContainerPort{
		{Name: "http", ContainerPort: 8080, Protocol: corev1.ProtocolTCP},
		{ContainerPort: 9090, Protocol: corev1.ProtocolTCP},
	}
	container.ReadinessProbe = probeOf(corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
		Path: "/ready", Port: intstr.FromString("http"), Scheme: corev1.URISchemeHTTP,
		HTTPHeaders: []corev1.HTTPHeader{{Name: "X-Probe", Value: "1"}},
	}})
	container.LivenessProbe = probeOf(corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(9090)}})
	return pod
}

func TestValidatePodProfileProbes(t *testing.T) {
	t.Parallel()
	readiness := func(p *corev1.Pod) *corev1.Probe { return p.Spec.Containers[0].ReadinessProbe }
	liveness := func(p *corev1.Pod) *corev1.Probe { return p.Spec.Containers[0].LivenessProbe }
	for _, tt := range []struct {
		name   string
		change func(*corev1.Pod)
		want   string
	}{
		{name: "httpGet and tcpSocket"},
		{name: "exec", change: func(p *corev1.Pod) {
			p.Spec.Containers[0].LivenessProbe = probeOf(corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"/bin/check", "--live"}}})
		}},
		{name: "https numeric port", change: func(p *corev1.Pod) {
			readiness(p).HTTPGet.Scheme, readiness(p).HTTPGet.Port = corev1.URISchemeHTTPS, intstr.FromInt32(8080)
		}},
		{name: "bounded timing", change: func(p *corev1.Pod) {
			probe := readiness(p)
			probe.InitialDelaySeconds, probe.TimeoutSeconds, probe.PeriodSeconds = 3600, 60, 3600
			probe.SuccessThreshold, probe.FailureThreshold = 10, 100
		}},
		{name: "ports only", change: func(p *corev1.Pod) {
			p.Spec.Containers[0].ReadinessProbe, p.Spec.Containers[0].LivenessProbe = nil, nil
		}},
		{name: "startup probe", change: func(p *corev1.Pod) {
			p.Spec.Containers[0].StartupProbe = probeOf(corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"x"}}})
		}, want: "startupProbe"},
		{name: "grpc", change: func(p *corev1.Pod) {
			readiness(p).HTTPGet, readiness(p).GRPC = nil, &corev1.GRPCAction{Port: 8080}
		}, want: "grpc"},
		{name: "no handler", change: func(p *corev1.Pod) { readiness(p).HTTPGet = nil }, want: "exactly one"},
		{name: "two handlers", change: func(p *corev1.Pod) {
			readiness(p).TCPSocket = &corev1.TCPSocketAction{Port: intstr.FromInt32(8080)}
		}, want: "exactly one"},
		{name: "probe grace", change: func(p *corev1.Pod) { liveness(p).TerminationGracePeriodSeconds = ptr.To(int64(1)) }, want: "terminationGracePeriodSeconds"},
		{name: "zero timeout", change: func(p *corev1.Pod) { readiness(p).TimeoutSeconds = 0 }, want: "timing"},
		{name: "long timeout", change: func(p *corev1.Pod) { readiness(p).TimeoutSeconds = 61 }, want: "timing"},
		{name: "zero period", change: func(p *corev1.Pod) { readiness(p).PeriodSeconds = 0 }, want: "timing"},
		{name: "negative delay", change: func(p *corev1.Pod) { readiness(p).InitialDelaySeconds = -1 }, want: "timing"},
		{name: "long delay", change: func(p *corev1.Pod) { readiness(p).InitialDelaySeconds = 3601 }, want: "timing"},
		{name: "success threshold", change: func(p *corev1.Pod) { readiness(p).SuccessThreshold = 11 }, want: "timing"},
		{name: "failure threshold", change: func(p *corev1.Pod) { readiness(p).FailureThreshold = 0 }, want: "timing"},
		{name: "liveness success threshold", change: func(p *corev1.Pod) { liveness(p).SuccessThreshold = 2 }, want: "successThreshold must be 1"},
		{name: "http host", change: func(p *corev1.Pod) { readiness(p).HTTPGet.Host = "10.0.0.1" }, want: "host is not supported"},
		{name: "tcp host", change: func(p *corev1.Pod) { liveness(p).TCPSocket.Host = "metadata" }, want: "host is not supported"},
		{name: "host header", change: func(p *corev1.Pod) {
			readiness(p).HTTPGet.HTTPHeaders = []corev1.HTTPHeader{{Name: "host", Value: "internal"}}
		}, want: "Host or carry credentials"},
		{name: "authorization header", change: func(p *corev1.Pod) {
			readiness(p).HTTPGet.HTTPHeaders = []corev1.HTTPHeader{{Name: "Authorization", Value: "Bearer x"}}
		}, want: "credentials"},
		{name: "api key header", change: func(p *corev1.Pod) {
			readiness(p).HTTPGet.HTTPHeaders = []corev1.HTTPHeader{{Name: "X-Api-Key", Value: "x"}}
		}, want: "credentials"},
		{name: "cookie header", change: func(p *corev1.Pod) {
			readiness(p).HTTPGet.HTTPHeaders = []corev1.HTTPHeader{{Name: "Cookie", Value: "x"}}
		}, want: "credentials"},
		{name: "header injection", change: func(p *corev1.Pod) {
			readiness(p).HTTPGet.HTTPHeaders = []corev1.HTTPHeader{{Name: "X-A", Value: "1\r\nX-B: 2"}}
		}, want: "valid names"},
		{name: "bad header name", change: func(p *corev1.Pod) {
			readiness(p).HTTPGet.HTTPHeaders = []corev1.HTTPHeader{{Name: "X A", Value: "1"}}
		}, want: "valid names"},
		{name: "empty header name", change: func(p *corev1.Pod) {
			readiness(p).HTTPGet.HTTPHeaders = []corev1.HTTPHeader{{Name: "", Value: "1"}}
		}, want: "valid names"},
		{name: "too many headers", change: func(p *corev1.Pod) {
			for i := range 9 {
				readiness(p).HTTPGet.HTTPHeaders = append(readiness(p).HTTPGet.HTTPHeaders, corev1.HTTPHeader{Name: "X-" + string(rune('a'+i)), Value: "1"})
			}
		}, want: "at most 8"},
		{name: "relative path", change: func(p *corev1.Pod) { readiness(p).HTTPGet.Path = "ready" }, want: "absolute path"},
		{name: "path spaces", change: func(p *corev1.Pod) { readiness(p).HTTPGet.Path = "/a b" }, want: "spaces"},
		{name: "long path", change: func(p *corev1.Pod) { readiness(p).HTTPGet.Path = "/" + strings.Repeat("a", 1024) }, want: "absolute path"},
		{name: "scheme", change: func(p *corev1.Pod) { readiness(p).HTTPGet.Scheme = "FTP" }, want: "scheme"},
		{name: "undeclared named port", change: func(p *corev1.Pod) { readiness(p).HTTPGet.Port = intstr.FromString("admin") }, want: "declared container port"},
		{name: "undeclared numeric port", change: func(p *corev1.Pod) { liveness(p).TCPSocket.Port = intstr.FromInt32(22) }, want: "declared container port"},
		{name: "empty exec", change: func(p *corev1.Pod) {
			p.Spec.Containers[0].LivenessProbe = probeOf(corev1.ProbeHandler{Exec: &corev1.ExecAction{}})
		}, want: "command requires"},
		{name: "long exec", change: func(p *corev1.Pod) {
			p.Spec.Containers[0].LivenessProbe = probeOf(corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{strings.Repeat("a", 4097)}}})
		}, want: "exceeds"},
		{name: "exec nul", change: func(p *corev1.Pod) {
			p.Spec.Containers[0].LivenessProbe = probeOf(corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"a\x00"}}})
		}, want: "NUL"},
		{name: "host port", change: func(p *corev1.Pod) { p.Spec.Containers[0].Ports[0].HostPort = 80 }, want: "hostPort"},
		{name: "host ip", change: func(p *corev1.Pod) { p.Spec.Containers[0].Ports[0].HostIP = "0.0.0.0" }, want: "hostIP"},
		{name: "udp port", change: func(p *corev1.Pod) { p.Spec.Containers[0].Ports[1].Protocol = corev1.ProtocolUDP }, want: "only TCP"},
		{name: "duplicate port", change: func(p *corev1.Pod) { p.Spec.Containers[0].Ports[1].ContainerPort = 8080 }, want: "unique containerPort"},
		{name: "zero port", change: func(p *corev1.Pod) { p.Spec.Containers[0].Ports[1].ContainerPort = 0 }, want: "unique containerPort"},
		{name: "duplicate name", change: func(p *corev1.Pod) { p.Spec.Containers[0].Ports[1].Name = "http" }, want: "unique valid port names"},
		{name: "invalid name", change: func(p *corev1.Pod) { p.Spec.Containers[0].Ports[1].Name = "Not_Valid" }, want: "unique valid port names"},
		{name: "too many ports", change: func(p *corev1.Pod) {
			for i := range 15 {
				p.Spec.Containers[0].Ports = append(p.Spec.Containers[0].Ports, corev1.ContainerPort{ContainerPort: int32(10000 + i)})
			}
		}, want: "at most 16"},
		{name: "lifecycle still refused", change: func(p *corev1.Pod) { p.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{} }, want: "lifecycle"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pod := probedPod()
			if tt.change != nil {
				tt.change(pod)
			}
			before := pod.DeepCopy()
			err := ValidatePodProfile(pod, "worker-1", "kata-fc", ProfileProbes)
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

// TestValidatePodProfileProbesNeedFeature is a regression test: until the
// executor runs probes and gates readiness on them, its unchanged call keeps
// refusing probes and ports, so no probed Pod is admitted and reported Ready
// without evidence.
func TestValidatePodProfileProbesNeedFeature(t *testing.T) {
	t.Parallel()
	for _, change := range []func(*corev1.Pod){
		func(p *corev1.Pod) { p.Spec.Containers[0].LivenessProbe = nil },
		func(p *corev1.Pod) { p.Spec.Containers[0].ReadinessProbe = nil },
		func(p *corev1.Pod) {
			p.Spec.Containers[0].ReadinessProbe, p.Spec.Containers[0].LivenessProbe = nil, nil
		},
	} {
		pod := probedPod()
		change(pod)
		if err := ValidatePodProfile(pod, "worker-1", "kata-fc"); err == nil {
			t.Fatal("probes or ports accepted without ProfileProbes")
		}
	}
}
