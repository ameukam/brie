// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

// DefaultMetadataTokenURL is the GCE metadata server endpoint for the
// default service account's access token.
const DefaultMetadataTokenURL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"

const (
	maxTokenResponseBytes = 64 << 10
	maxAccessTokenBytes   = 8 << 10
	// tokenRefreshMargin refreshes a cached token before it expires.
	tokenRefreshMargin = time.Minute
	redacted           = "[redacted]"
)

// AccessToken is an OAuth 2.0 bearer token. Every formatting and logging
// path prints a placeholder; only the request authorizer reads the value.
type AccessToken struct {
	value  string
	expiry time.Time
}

// NewAccessToken wraps a bearer token value and its expiry.
func NewAccessToken(value string, expiry time.Time) (AccessToken, error) {
	if !validTokenValue(value) {
		return AccessToken{}, errors.New("access token must be 1 to 8192 bytes of visible ASCII")
	}
	return AccessToken{value: value, expiry: expiry}, nil
}

// Expiry reports when the token stops being valid.
func (t AccessToken) Expiry() time.Time { return t.expiry }

// String redacts the token.
func (AccessToken) String() string { return redacted }

// GoString redacts the token.
func (AccessToken) GoString() string { return redacted }

// Format redacts the token for every verb and flag.
func (AccessToken) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, redacted) }

// LogValue redacts the token in structured logs.
func (AccessToken) LogValue() slog.Value { return slog.StringValue(redacted) }

// MarshalJSON redacts the token.
func (AccessToken) MarshalJSON() ([]byte, error) { return json.Marshal(redacted) }

func validTokenValue(value string) bool {
	if value == "" || len(value) > maxAccessTokenBytes {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] <= 0x20 || value[i] > 0x7e {
			return false
		}
	}
	return true
}

// TokenSource supplies bearer tokens for storage requests.
type TokenSource interface {
	Token(ctx context.Context) (AccessToken, error)
}

// MetadataTokenSource reads the default service account token from the GCE
// metadata server and caches it until shortly before expiry. Errors carry
// only status codes and fixed text, never response bodies.
type MetadataTokenSource struct {
	// HTTPClient must not be shared with untrusted redirects; redirects are
	// refused regardless.
	HTTPClient *http.Client
	// URL defaults to DefaultMetadataTokenURL.
	URL string
	// Now defaults to time.Now.
	Now func() time.Time

	mu     sync.Mutex
	cached AccessToken
}

// Token returns a cached token or fetches a new one.
func (s *MetadataTokenSource) Token(ctx context.Context) (AccessToken, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	if s.cached.value != "" && now().Add(tokenRefreshMargin).Before(s.cached.expiry) {
		return s.cached, nil
	}
	endpoint := s.URL
	if endpoint == "" {
		endpoint = DefaultMetadataTokenURL
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return AccessToken{}, errors.New("build metadata token request")
	}
	request.Header.Set("Metadata-Flavor", "Google")
	response, err := noRedirects(s.HTTPClient).Do(request)
	if err != nil {
		return AccessToken{}, fmt.Errorf("metadata token request: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return AccessToken{}, fmt.Errorf("metadata token request: HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxTokenResponseBytes+1))
	if err != nil {
		return AccessToken{}, errors.New("read metadata token response")
	}
	token, err := decodeMetadataToken(body, now())
	if err != nil {
		return AccessToken{}, err
	}
	s.cached = token
	return token, nil
}

type metadataTokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

// decodeMetadataToken parses a metadata server token response. Its errors
// never include the response.
func decodeMetadataToken(body []byte, now time.Time) (AccessToken, error) {
	if len(body) > maxTokenResponseBytes {
		return AccessToken{}, errors.New("metadata token response is too large")
	}
	var parsed metadataTokenResponse
	if json.Unmarshal(body, &parsed) != nil {
		return AccessToken{}, errors.New("metadata token response is not valid JSON")
	}
	if !strings.EqualFold(parsed.TokenType, "Bearer") {
		return AccessToken{}, errors.New("metadata token response is not a bearer token")
	}
	if parsed.ExpiresIn <= 0 || parsed.ExpiresIn > int64(24*time.Hour/time.Second) {
		return AccessToken{}, errors.New("metadata token response has an invalid lifetime")
	}
	token, err := NewAccessToken(parsed.AccessToken, now.Add(time.Duration(parsed.ExpiresIn)*time.Second))
	if err != nil {
		return AccessToken{}, errors.New("metadata token response has an invalid token")
	}
	return token, nil
}

// noRedirects copies client and refuses redirects, so a bearer token is
// sent only to the configured endpoint.
func noRedirects(client *http.Client) *http.Client {
	copied := http.Client{}
	if client != nil {
		copied = *client
	}
	copied.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copied
}
