// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAccessTokenIsRedacted(t *testing.T) {
	token, err := NewAccessToken(fakeToken, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	slog.New(slog.NewTextHandler(&logs, nil)).Info("token", "token", token)
	encoded, err := json.Marshal(struct{ Token AccessToken }{token})
	if err != nil {
		t.Fatal(err)
	}
	outputs := []string{
		fmt.Sprint(token), fmt.Sprintf("%v %+v %#v %s %q %x", token, token, token, token, token, token),
		fmt.Sprintf("%+v", struct{ T AccessToken }{token}), token.String(), token.GoString(),
		logs.String(), string(encoded),
	}
	for _, output := range outputs {
		if strings.Contains(output, "SENTINEL") || !strings.Contains(output, redacted) {
			t.Fatalf("token formatting not redacted: %q", output)
		}
	}
	if !token.Expiry().Equal(time.Unix(100, 0)) {
		t.Fatal("expiry not preserved")
	}
	for _, value := range []string{"", "has space", "tab\t", strings.Repeat("a", maxAccessTokenBytes+1), "é"} {
		if _, err := NewAccessToken(value, time.Time{}); err == nil {
			t.Fatalf("NewAccessToken accepted %q", value)
		}
	}
}

func TestMetadataTokenSource(t *testing.T) {
	var requests atomic.Int32
	status := http.StatusOK
	body := `{"access_token":"` + fakeToken + `","expires_in":3599,"token_type":"Bearer"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("Metadata-Flavor") != "Google" {
			t.Errorf("missing Metadata-Flavor header")
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	now := time.Unix(1000, 0)
	source := &MetadataTokenSource{URL: server.URL, Now: func() time.Time { return now }}

	token, err := source.Token(t.Context())
	if err != nil || token.value != fakeToken || !token.Expiry().Equal(now.Add(3599*time.Second)) {
		t.Fatalf("Token = %v, %v", token.Expiry(), err)
	}
	if _, err := source.Token(t.Context()); err != nil || requests.Load() != 1 {
		t.Fatalf("cached token refetched: %d requests, %v", requests.Load(), err)
	}
	now = now.Add(3599*time.Second - tokenRefreshMargin)
	if _, err := source.Token(t.Context()); err != nil || requests.Load() != 2 {
		t.Fatalf("expiring token not refreshed: %d requests, %v", requests.Load(), err)
	}

	for _, tt := range []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{name: "server error", status: 500, body: fakeToken, want: "HTTP 500"},
		{name: "not JSON", status: 200, body: fakeToken, want: "not valid JSON"},
		{name: "wrong type", status: 200, body: `{"access_token":"` + fakeToken + `","expires_in":10,"token_type":"MAC"}`, want: "bearer"},
		{name: "no lifetime", status: 200, body: `{"access_token":"` + fakeToken + `","expires_in":0,"token_type":"Bearer"}`, want: "lifetime"},
		{name: "bad token", status: 200, body: `{"access_token":"` + fakeToken + ` x","expires_in":10,"token_type":"Bearer"}`, want: "invalid token"},
		{name: "oversized", status: 200, body: strings.Repeat(" ", maxTokenResponseBytes+1), want: "too large"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			status, body = tt.status, tt.body
			fresh := &MetadataTokenSource{URL: server.URL}
			_, err := fresh.Token(t.Context())
			if err == nil || !strings.Contains(err.Error(), tt.want) || strings.Contains(err.Error(), "SENTINEL") {
				t.Fatalf("Token error = %v, want %q without the token", err, tt.want)
			}
		})
	}

	unreachable := &MetadataTokenSource{URL: "http://127.0.0.1:1/token"}
	if _, err := unreachable.Token(t.Context()); err == nil || !strings.Contains(err.Error(), "metadata token request") {
		t.Fatalf("unreachable error = %v", err)
	}
	invalid := &MetadataTokenSource{URL: "http://[::1"}
	if _, err := invalid.Token(t.Context()); err == nil {
		t.Fatal("invalid URL accepted")
	}
}

func TestMetadataTokenSourceRefusesRedirect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://example.invalid/", http.StatusFound)
	}))
	defer server.Close()
	source := &MetadataTokenSource{URL: server.URL, HTTPClient: &http.Client{}}
	if _, err := source.Token(t.Context()); err == nil || !strings.Contains(err.Error(), "HTTP 302") {
		t.Fatalf("redirect error = %v", err)
	}
}

func FuzzDecodeMetadataToken(f *testing.F) {
	f.Add([]byte(`{"access_token":"ya29.a","expires_in":3599,"token_type":"Bearer"}`))
	f.Add([]byte(`{"access_token":"","expires_in":-1}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		token, err := decodeMetadataToken(data, time.Unix(0, 0))
		if err != nil {
			if token.value != "" {
				t.Fatal("token returned with an error")
			}
			if strings.Contains(err.Error(), "{") {
				t.Fatalf("error echoes the response: %v", err)
			}
			return
		}
		if !validTokenValue(token.value) || !token.Expiry().After(time.Unix(0, 0)) {
			t.Fatalf("accepted invalid token response %q", data)
		}
	})
}
