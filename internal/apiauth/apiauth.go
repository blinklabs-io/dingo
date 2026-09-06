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

// Package apiauth is the single credential-verification implementation
// shared by every built-in API provider (Blockfrost, Kupo, Mesh, UTxORPC). Only
// the wire transport differs per protocol: http.go adapts it to HTTP
// request headers (failing closed with 401), connect.go adapts it to
// Connect/gRPC request/streaming headers (failing closed with
// connect.CodeUnauthenticated). Neither adapter re-implements credential
// comparison; both call through to Verifier.Verify.
package apiauth

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
)

// Verifier holds a resolved shared-secret token and compares presented
// credentials against it in constant time. A nil *Verifier always
// succeeds -- callers construct one only when authentication is enabled,
// and skip installing the HTTP/Connect adapters entirely otherwise, so a
// nil Verifier is only ever reached by code that also treats it as
// "authentication disabled" (see Verify).
type Verifier struct {
	token []byte
}

// NewVerifier builds a Verifier from an EffectiveAuth. It returns (nil,
// nil) when cfg.Enabled is false: authentication is disabled, and callers
// should skip installing any credential check at all rather than call
// Verify with an always-succeeding nil Verifier implicitly. TokenFilePath
// (if set instead of an inline Token) is read here, at construction --
// i.e. at listener startup, matching TLSPolicy's own deferral of
// certificate loading to listener startup rather than config resolution.
func NewVerifier(cfg apiconfig.EffectiveAuth) (*Verifier, error) {
	if !cfg.Enabled {
		return nil, nil //nolint:nilnil // nil Verifier is the documented "auth disabled" value
	}
	token := cfg.Token
	if cfg.TokenFilePath != "" {
		data, err := os.ReadFile(cfg.TokenFilePath)
		if err != nil {
			return nil, fmt.Errorf("reading auth tokenFilePath: %w", err)
		}
		token = strings.TrimSpace(string(data))
	}
	if token == "" {
		return nil, errors.New(
			"auth token resolves to an empty value",
		)
	}
	return &Verifier{token: []byte(token)}, nil
}

// Verify reports whether presented matches the configured token, using a
// constant-time comparison so response timing cannot be used to guess the
// token byte by byte. A nil Verifier (authentication disabled) always
// succeeds.
func (v *Verifier) Verify(presented string) bool {
	if v == nil {
		return true
	}
	if presented == "" {
		return false
	}
	return subtle.ConstantTimeCompare(v.token, []byte(presented)) == 1
}

// bearerToken extracts the credential from a standard
// "Authorization: Bearer <token>" header value, or "" if header does not
// use the Bearer scheme. The credential portion is trimmed of surrounding
// whitespace, since some HTTP/Connect/gRPC client defaults insert extra
// internal whitespace after "Bearer" that would otherwise reject an
// otherwise-correct token.
func bearerToken(header string) string {
	const prefix = "Bearer "
	if len(header) <= len(prefix) {
		return ""
	}
	if !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}
