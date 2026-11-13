// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
)

// ProfileFeature accepts a reviewed Pod profile increment. A caller passes a
// feature only once it enforces that increment at runtime.
type ProfileFeature int

const (
	// ProfileProbes accepts readiness and liveness probes (exec, httpGet, and
	// tcpSocket) and the TCP container ports they name. The caller must run
	// the probes, gate readiness on them, and fail the Pod, never restart it,
	// after a liveness failure.
	ProfileProbes ProfileFeature = iota + 1
)

// Probe bounds of the profile. The API server defaults timeoutSeconds,
// periodSeconds, successThreshold, and failureThreshold, so zero is refused.
const (
	maxProbeInitialDelay  = 3600
	maxProbeTimeout       = 60
	maxProbePeriod        = 3600
	maxProbeSuccesses     = 10
	maxProbeFailures      = 100
	maxProfilePorts       = 16
	maxProbeCommandItems  = 32
	maxProbeCommandBytes  = 4096
	maxProbePathBytes     = 1024
	maxProbeHeaders       = 8
	maxProbeHeaderValue   = 256
	sensitiveHeaderTokens = "authorization,auth,cookie,token,secret,password,passwd,key,credential,session,bearer"
)

func validateProfileProbes(container corev1.Container) error {
	ports, err := validateProfilePorts(container.Ports)
	if err != nil {
		return err
	}
	if err := validateProfileProbe("container.readinessProbe", container.ReadinessProbe, ports, false); err != nil {
		return err
	}
	return validateProfileProbe("container.livenessProbe", container.LivenessProbe, ports, true)
}

type declaredPorts struct {
	numbers map[int32]bool
	names   map[string]bool
}

func validateProfilePorts(ports []corev1.ContainerPort) (declaredPorts, error) {
	declared := declaredPorts{numbers: map[int32]bool{}, names: map[string]bool{}}
	if len(ports) > maxProfilePorts {
		return declared, fmt.Errorf("container.ports accepts at most %d ports", maxProfilePorts)
	}
	for _, port := range ports {
		if err := onlyProfileFields("container.ports", port, corev1.ContainerPort{
			Name: port.Name, ContainerPort: port.ContainerPort, Protocol: port.Protocol,
		}); err != nil {
			return declared, err
		}
		if port.ContainerPort < 1 || port.ContainerPort > 65535 || declared.numbers[port.ContainerPort] {
			return declared, errors.New("container.ports requires unique containerPort values between 1 and 65535")
		}
		if port.Protocol != "" && port.Protocol != corev1.ProtocolTCP {
			return declared, errors.New("container.ports supports only TCP")
		}
		if port.Name != "" {
			if len(validation.IsValidPortName(port.Name)) != 0 || declared.names[port.Name] {
				return declared, errors.New("container.ports names must be unique valid port names")
			}
			declared.names[port.Name] = true
		}
		declared.numbers[port.ContainerPort] = true
	}
	return declared, nil
}

func validateProfileProbe(path string, probe *corev1.Probe, ports declaredPorts, liveness bool) error {
	if probe == nil {
		return nil
	}
	if err := onlyProfileFields(path, *probe, corev1.Probe{
		ProbeHandler: probe.ProbeHandler, InitialDelaySeconds: probe.InitialDelaySeconds,
		TimeoutSeconds: probe.TimeoutSeconds, PeriodSeconds: probe.PeriodSeconds,
		SuccessThreshold: probe.SuccessThreshold, FailureThreshold: probe.FailureThreshold,
	}); err != nil {
		return err
	}
	if probe.InitialDelaySeconds < 0 || probe.InitialDelaySeconds > maxProbeInitialDelay ||
		probe.TimeoutSeconds < 1 || probe.TimeoutSeconds > maxProbeTimeout ||
		probe.PeriodSeconds < 1 || probe.PeriodSeconds > maxProbePeriod ||
		probe.SuccessThreshold < 1 || probe.SuccessThreshold > maxProbeSuccesses ||
		probe.FailureThreshold < 1 || probe.FailureThreshold > maxProbeFailures {
		return fmt.Errorf("%s timing must be within initialDelaySeconds 0-%d, timeoutSeconds 1-%d, periodSeconds 1-%d, successThreshold 1-%d, and failureThreshold 1-%d",
			path, maxProbeInitialDelay, maxProbeTimeout, maxProbePeriod, maxProbeSuccesses, maxProbeFailures)
	}
	if liveness && probe.SuccessThreshold != 1 {
		return fmt.Errorf("%s.successThreshold must be 1", path)
	}
	handler := probe.ProbeHandler
	if err := onlyProfileFields(path, handler, corev1.ProbeHandler{
		Exec: handler.Exec, HTTPGet: handler.HTTPGet, TCPSocket: handler.TCPSocket,
	}); err != nil {
		return err
	}
	switch {
	case handler.Exec != nil && handler.HTTPGet == nil && handler.TCPSocket == nil:
		return validateExecProbe(path+".exec", handler.Exec)
	case handler.Exec == nil && handler.HTTPGet != nil && handler.TCPSocket == nil:
		return validateHTTPGetProbe(path+".httpGet", handler.HTTPGet, ports)
	case handler.Exec == nil && handler.HTTPGet == nil && handler.TCPSocket != nil:
		action := handler.TCPSocket
		if action.Host != "" {
			return fmt.Errorf("%s.tcpSocket.host is not supported; probes target the Pod IP", path)
		}
		return validateProbePort(path+".tcpSocket.port", action.Port, ports)
	}
	return fmt.Errorf("%s requires exactly one exec, httpGet, or tcpSocket handler", path)
}

func validateExecProbe(path string, action *corev1.ExecAction) error {
	if len(action.Command) == 0 || len(action.Command) > maxProbeCommandItems {
		return fmt.Errorf("%s.command requires 1-%d entries", path, maxProbeCommandItems)
	}
	total := 0
	for _, item := range action.Command {
		total += len(item)
		if strings.ContainsRune(item, '\x00') {
			return fmt.Errorf("%s.command must not contain NUL", path)
		}
	}
	if total > maxProbeCommandBytes {
		return fmt.Errorf("%s.command exceeds %d bytes", path, maxProbeCommandBytes)
	}
	return nil
}

func validateHTTPGetProbe(path string, action *corev1.HTTPGetAction, ports declaredPorts) error {
	if action.Host != "" {
		return fmt.Errorf("%s.host is not supported; probes target the Pod IP", path)
	}
	if action.Scheme != "" && action.Scheme != corev1.URISchemeHTTP && action.Scheme != corev1.URISchemeHTTPS {
		return fmt.Errorf("%s.scheme must be HTTP or HTTPS", path)
	}
	if len(action.Path) > maxProbePathBytes || (action.Path != "" && action.Path[0] != '/') {
		return fmt.Errorf("%s.path must be empty or an absolute path of at most %d bytes", path, maxProbePathBytes)
	}
	for _, ch := range action.Path {
		if ch <= ' ' || ch == 0x7f {
			return fmt.Errorf("%s.path must not contain spaces or control characters", path)
		}
	}
	if len(action.HTTPHeaders) > maxProbeHeaders {
		return fmt.Errorf("%s.httpHeaders accepts at most %d headers", path, maxProbeHeaders)
	}
	for _, header := range action.HTTPHeaders {
		if !validHeaderName(header.Name) || len(header.Value) > maxProbeHeaderValue ||
			strings.ContainsAny(header.Value, "\r\n\x00") {
			return fmt.Errorf("%s.httpHeaders requires valid names and values of at most %d bytes", path, maxProbeHeaderValue)
		}
		if strings.EqualFold(header.Name, "Host") || sensitiveHeader(header.Name) {
			return fmt.Errorf("%s.httpHeaders must not override Host or carry credentials", path)
		}
	}
	return validateProbePort(path+".port", action.Port, ports)
}

func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for _, ch := range name {
		if (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') && !strings.ContainsRune("!#$%&'*+-.^_`|~", ch) {
			return false
		}
	}
	return true
}

func sensitiveHeader(name string) bool {
	lower := strings.ToLower(name)
	for _, token := range strings.Split(sensitiveHeaderTokens, ",") {
		if strings.Contains(lower, token) {
			return true
		}
	}
	return false
}

func validateProbePort(path string, port intstr.IntOrString, ports declaredPorts) error {
	switch port.Type {
	case intstr.Int:
		if ports.numbers[port.IntVal] {
			return nil
		}
	case intstr.String:
		if ports.names[port.StrVal] {
			return nil
		}
	}
	return fmt.Errorf("%s must name or number a declared container port", path)
}
