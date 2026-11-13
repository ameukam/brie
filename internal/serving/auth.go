// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package serving

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// User is an authenticated identity, as returned by TokenReview or a client
// certificate verified against the configured CA.
type User struct {
	Name   string
	UID    string
	Groups []string
	Extra  map[string][]string
}

const authenticatedGroup = "system:authenticated"

// authenticate identifies the caller from a verified client certificate or a
// bearer token reviewed by the API server. Anonymous requests and every review
// error are rejected.
func authenticate(ctx context.Context, api kubernetes.Interface, audiences []string, r *http.Request) (*User, error) {
	if r.TLS != nil && len(r.TLS.VerifiedChains) > 0 && len(r.TLS.VerifiedChains[0]) > 0 {
		return certificateUser(r.TLS.VerifiedChains[0][0])
	}
	header := r.Header.Get("Authorization")
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
		return nil, errors.New("no client certificate or bearer token")
	}
	review, err := api.AuthenticationV1().TokenReviews().Create(ctx, &authenticationv1.TokenReview{
		Spec: authenticationv1.TokenReviewSpec{Token: strings.TrimSpace(token), Audiences: audiences},
	}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("TokenReview failed: %w", err)
	}
	status := review.Status
	if !status.Authenticated || status.Error != "" || status.User.Username == "" {
		return nil, errors.New("token not authenticated")
	}
	if len(audiences) > 0 && !slices.ContainsFunc(status.Audiences, func(a string) bool { return slices.Contains(audiences, a) }) {
		return nil, errors.New("token audience not accepted")
	}
	extra := make(map[string][]string, len(status.User.Extra))
	for key, values := range status.User.Extra {
		extra[key] = slices.Clone(values)
	}
	return &User{Name: status.User.Username, UID: status.User.UID, Groups: slices.Clone(status.User.Groups), Extra: extra}, nil
}

func certificateUser(certificate *x509.Certificate) (*User, error) {
	if certificate.Subject.CommonName == "" {
		return nil, errors.New("client certificate has no common name")
	}
	groups := slices.Clone(certificate.Subject.Organization)
	if !slices.Contains(groups, authenticatedGroup) {
		groups = append(groups, authenticatedGroup)
	}
	return &User{Name: certificate.Subject.CommonName, Groups: groups}, nil
}

// Access is the node subresource and verb a request needs, following
// kubelet's mapping. Alternatives are tried in order; the first allowed wins.
type Access struct {
	Verb         string
	Subresources []string
}

// RequestAccess maps a request to kubelet's SubjectAccessReview attributes:
// verbs come from the HTTP method; /stats, /metrics, /logs, and /healthz use
// the stats, metrics, log, and healthz subresources (healthz falls back to
// proxy, as in kubelet's fine-grained authorization); everything else is
// nodes/proxy. Streaming sessions always require create, so a WebSocket GET
// cannot reach exec, attach, or port-forward with read-only access.
func RequestAccess(method, path string) (Access, error) {
	verbs := map[string]string{
		http.MethodGet: "get", http.MethodHead: "get", http.MethodPost: "create",
		http.MethodPut: "update", http.MethodPatch: "patch", http.MethodDelete: "delete",
	}
	verb, ok := verbs[method]
	if !ok {
		return Access{}, errors.New("unsupported method")
	}
	switch {
	case subpath(path, "/stats"):
		return Access{Verb: verb, Subresources: []string{"stats"}}, nil
	case subpath(path, "/metrics"):
		return Access{Verb: verb, Subresources: []string{"metrics"}}, nil
	case subpath(path, "/logs"):
		return Access{Verb: verb, Subresources: []string{"log"}}, nil
	case subpath(path, "/healthz"):
		return Access{Verb: verb, Subresources: []string{"healthz", "proxy"}}, nil
	case subpath(path, "/exec"), subpath(path, "/attach"), subpath(path, "/portForward"):
		return Access{Verb: "create", Subresources: []string{"proxy"}}, nil
	}
	return Access{Verb: verb, Subresources: []string{"proxy"}}, nil
}

func subpath(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

// authorize asks the API server whether user may access the node subresource.
// Denials, evaluation errors without an allow, and any review request error
// refuse; an error is never followed by a fallback review.
func authorize(ctx context.Context, api kubernetes.Interface, nodeName string, user *User, access Access) error {
	extra := make(map[string]authorizationv1.ExtraValue, len(user.Extra))
	for key, values := range user.Extra {
		extra[key] = authorizationv1.ExtraValue(values)
	}
	for _, subresource := range access.Subresources {
		review, err := api.AuthorizationV1().SubjectAccessReviews().Create(ctx, &authorizationv1.SubjectAccessReview{
			Spec: authorizationv1.SubjectAccessReviewSpec{
				User: user.Name, UID: user.UID, Groups: user.Groups, Extra: extra,
				ResourceAttributes: &authorizationv1.ResourceAttributes{
					Verb: access.Verb, Version: "v1", Resource: "nodes", Subresource: subresource, Name: nodeName,
				},
			},
		}, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("SubjectAccessReview failed: %w", err)
		}
		if review.Status.Allowed && !review.Status.Denied {
			return nil
		}
	}
	return fmt.Errorf("user %q cannot %s nodes/%s", user.Name, access.Verb, access.Subresources[0])
}
