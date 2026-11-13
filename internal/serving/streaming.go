// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package serving

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"github.com/ameukam/brie/internal/cri"
	corev1 "k8s.io/api/core/v1"
)

// maxForwardedPorts bounds the ports of one port-forward request.
const maxForwardedPorts = 16

// ExecRequest is a parsed kubelet exec or attach query.
type ExecRequest struct {
	Command []string
	Options cri.ExecOptions
}

// ParseExecQuery parses kubelet's input, output, error, tty, and repeated
// command parameters, the wire names the API server sends. Attach takes no
// command.
func ParseExecQuery(query url.Values, needCommand bool) (ExecRequest, error) {
	var request ExecRequest
	for name, target := range map[string]*bool{
		corev1.ExecStdinParam: &request.Options.Stdin, corev1.ExecStdoutParam: &request.Options.Stdout,
		corev1.ExecStderrParam: &request.Options.Stderr, corev1.ExecTTYParam: &request.Options.TTY,
	} {
		value := query.Get(name)
		if value == "" {
			continue
		}
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return ExecRequest{}, fmt.Errorf("%s must be a boolean", name)
		}
		*target = parsed
	}
	if !request.Options.Stdin && !request.Options.Stdout && !request.Options.Stderr {
		return ExecRequest{}, errors.New("at least one of input, output, and error is required")
	}
	if request.Options.TTY && request.Options.Stderr {
		return ExecRequest{}, errors.New("error cannot be separate from a TTY")
	}
	request.Command = query[corev1.ExecCommandParam]
	if needCommand && len(request.Command) == 0 {
		return ExecRequest{}, errors.New("command is required")
	}
	if !needCommand && len(request.Command) != 0 {
		return ExecRequest{}, errors.New("attach does not take a command")
	}
	return request, nil
}

// ParsePorts parses port-forward ports from repeated or comma-separated
// "port" and "ports" parameters.
func ParsePorts(query url.Values) ([]int32, error) {
	var ports []int32
	for _, name := range []string{"port", "ports"} {
		for _, value := range query[name] {
			for _, field := range strings.Split(value, ",") {
				port, err := strconv.ParseUint(strings.TrimSpace(field), 10, 16)
				if err != nil || port == 0 {
					return nil, errors.New("ports must be integers between 1 and 65535")
				}
				ports = append(ports, int32(port))
				if len(ports) > maxForwardedPorts {
					return nil, fmt.Errorf("at most %d ports may be forwarded", maxForwardedPorts)
				}
			}
		}
	}
	return ports, nil
}

func (s *Server) exec(w http.ResponseWriter, r *http.Request) {
	s.streamContainer(w, r, true)
}

func (s *Server) attach(w http.ResponseWriter, r *http.Request) {
	s.streamContainer(w, r, false)
}

func (s *Server) streamContainer(w http.ResponseWriter, r *http.Request, exec bool) {
	namespace, pod, container := r.PathValue("namespace"), r.PathValue("pod"), r.PathValue("container")
	if !validObjectNames(namespace, pod, container) {
		http.Error(w, "invalid namespace, pod, or container name", http.StatusBadRequest)
		return
	}
	request, err := ParseExecQuery(r.URL.Query(), exec)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	operation := "attach"
	if exec {
		operation = "exec"
	}
	t, err := s.locate(r.Context(), namespace, pod, container)
	if err == nil {
		err = requireRunningContainer(t)
	}
	if err != nil {
		s.fail(w, r, operation+" locate", err)
		return
	}
	var target *url.URL
	if exec {
		target, err = s.runtime.Exec(r.Context(), t.container.GetId(), request.Command, request.Options)
	} else {
		target, err = s.runtime.Attach(r.Context(), t.container.GetId(), request.Options)
	}
	if err != nil {
		s.fail(w, r, operation, err)
		return
	}
	s.proxyStream(w, r, target)
}

func (s *Server) portForward(w http.ResponseWriter, r *http.Request) {
	namespace, pod := r.PathValue("namespace"), r.PathValue("pod")
	if !validObjectNames(namespace, pod, "") {
		http.Error(w, "invalid namespace or pod name", http.StatusBadRequest)
		return
	}
	ports, err := ParsePorts(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	t, err := s.locate(r.Context(), namespace, pod, "")
	if err == nil {
		err = requireReadySandbox(t)
	}
	if err != nil {
		s.fail(w, r, "port forward locate", err)
		return
	}
	target, err := s.runtime.PortForward(r.Context(), t.sandbox.GetId(), ports)
	if err != nil {
		s.fail(w, r, "port forward", err)
		return
	}
	s.proxyStream(w, r, target)
}

// proxyStream relays the client's upgrade request to the runtime's streaming
// server. Brie proxies rather than redirecting, so clients never reach the
// runtime directly, and it only proxies to loopback addresses. Caller
// credentials are not forwarded.
func (s *Server) proxyStream(w http.ResponseWriter, r *http.Request, target *url.URL) {
	host := target.Hostname()
	address, err := netip.ParseAddr(host)
	if err != nil || !address.IsLoopback() {
		s.fail(w, r, "streaming URL check", errors.New("runtime streaming URL is not a loopback address"))
		return
	}
	proxy := &httputil.ReverseProxy{
		Rewrite: func(out *httputil.ProxyRequest) {
			location := *target
			if location.RawQuery == "" {
				location.RawQuery = out.In.URL.RawQuery
			}
			out.Out.URL, out.Out.Host = &location, location.Host
			for _, header := range []string{"Authorization", "Cookie", "Proxy-Authorization"} {
				out.Out.Header.Del(header)
			}
			for name := range out.Out.Header {
				if strings.HasPrefix(name, "Impersonate-") || strings.HasPrefix(name, "X-Remote-") {
					out.Out.Header.Del(name)
				}
			}
		},
		Transport: s.streamTransport, FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			s.logger.WarnContext(r.Context(), "runtime streaming proxy failed",
				"operation", "streaming proxy", "code", errorCode(err))
			http.Error(w, "runtime streaming failed", http.StatusBadGateway)
		},
	}
	proxy.ServeHTTP(w, r)
}
