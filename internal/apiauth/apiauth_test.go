// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package apiauth

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeTokenFile(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

func TestNewVerifier(t *testing.T) {
	tokenPath := writeTokenFile(t, "s3cret\n")

	tests := []struct {
		name    string
		policy  Policy
		wantErr string
	}{
		{name: "empty mode is none", policy: Policy{}},
		{name: "explicit none", policy: Policy{Mode: ModeNone}},
		{
			name:   "token mode with valid file",
			policy: Policy{Mode: ModeToken, TokenFilePath: tokenPath},
		},
		{
			name:    "token mode without path",
			policy:  Policy{Mode: ModeToken},
			wantErr: "tokenFilePath is required",
		},
		{
			name: "token mode with missing file",
			policy: Policy{
				Mode:          ModeToken,
				TokenFilePath: filepath.Join(t.TempDir(), "missing"),
			},
			wantErr: "reading auth token file",
		},
		{
			name: "token mode with empty file",
			policy: Policy{
				Mode:          ModeToken,
				TokenFilePath: writeTokenFile(t, "   \n"),
			},
			wantErr: "is empty",
		},
		{
			name:    "invalid mode",
			policy:  Policy{Mode: "bogus"},
			wantErr: "invalid auth mode",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := NewVerifier(tt.policy)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				require.Nil(t, v)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, v)
		})
	}
}

func TestVerifierMiddlewareModeNone(t *testing.T) {
	for _, v := range []*Verifier{nil, mustVerifier(t, Policy{})} {
		called := false
		handler := v.Middleware(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				called = true
				w.WriteHeader(http.StatusOK)
			},
		))
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		require.True(t, called)
		require.Equal(t, http.StatusOK, rr.Code)
	}
}

func TestVerifierMiddlewareModeToken(t *testing.T) {
	tokenPath := writeTokenFile(t, "s3cret")
	v := mustVerifier(
		t,
		Policy{Mode: ModeToken, TokenFilePath: tokenPath},
	)
	handler := v.Middleware(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		},
	))

	tests := []struct {
		name       string
		headers    map[string]string
		wantStatus int
	}{
		{
			name:       "missing credential",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "correct bearer token",
			headers:    map[string]string{"Authorization": "Bearer s3cret"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "wrong bearer token",
			headers:    map[string]string{"Authorization": "Bearer wrong"},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "correct project_id alias",
			headers:    map[string]string{"project_id": "s3cret"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "wrong project_id alias",
			headers:    map[string]string{"project_id": "wrong"},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "malformed authorization header",
			headers:    map[string]string{"Authorization": "s3cret"},
			wantStatus: http.StatusUnauthorized,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			for k, val := range tt.headers {
				req.Header.Set(k, val)
			}
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			require.Equal(t, tt.wantStatus, rr.Code)
			if tt.wantStatus == http.StatusUnauthorized {
				require.NotEmpty(t, rr.Header().Get("WWW-Authenticate"))
			}
		})
	}
}

// TestVerifierRedactsToken guards the "never log auth material" invariant:
// String and LogValue must never expose the loaded token, even via %v/%+v
// reflection-based formatting.
func TestVerifierRedactsToken(t *testing.T) {
	tokenPath := writeTokenFile(t, "s3cret-value")
	v := mustVerifier(
		t,
		Policy{Mode: ModeToken, TokenFilePath: tokenPath},
	)
	for _, rendered := range []string{
		v.String(),
		v.LogValue().String(),
	} {
		require.NotContains(t, rendered, "s3cret-value")
	}
}

func mustVerifier(t *testing.T, policy Policy) *Verifier {
	t.Helper()
	v, err := NewVerifier(policy)
	require.NoError(t, err)
	return v
}
