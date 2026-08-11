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

// Package apiauth is the single, shared credential-verification
// implementation used by every built-in API provider (Blockfrost, Mesh, and
// UTxO RPC -- dingo #2996/#2998). Composition/config wiring resolves the
// effective per-provider policy (internal/config.ResolveAPISecurity) and
// hands each provider a concrete Policy; this package is the only place
// that actually compares a request's credential against the configured
// token, so all three providers fail closed identically.
//
// dingo's UTxO RPC provider serves Connect (not raw gRPC-trailers)
// requests as ordinary HTTP handlers -- there is no separate wire
// transport to adapt for credential extraction, so a single net/http
// middleware protects all three providers' listeners uniformly. Connect's
// own client-side protocol implementation treats a bare, non-Connect-
// envelope HTTP 401 response as CodeUnauthenticated, so this HTTP-level
// enforcement point also answers "Unauthenticated" for Connect/gRPC-Web
// clients without a dedicated connect.Interceptor. Native gRPC
// trailers-only transport is not served by dingo's API providers today, so
// mapping this middleware to it is out of scope.
package apiauth

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
)

// Mode identifies a supported API authentication mode.
type Mode string

const (
	// ModeNone performs no credential verification. This is the default
	// when Mode is left empty.
	ModeNone Mode = "none"
	// ModeToken requires a bearer credential matching the contents of
	// Policy.TokenFilePath.
	ModeToken Mode = "token"
)

func (m Mode) normalized() Mode {
	if m == "" {
		return ModeNone
	}
	return m
}

// Policy is the effective (already merged) authentication policy for one
// API listener.
type Policy struct {
	Mode Mode
	// TokenFilePath is the path to a file containing the bearer credential
	// clients must present when Mode is ModeToken. The file's trimmed
	// contents are read once, at NewVerifier time, and never logged.
	TokenFilePath string
}

// Verifier enforces a resolved Policy against inbound HTTP requests. A nil
// *Verifier is treated as ModeNone (no enforcement), which lets a provider
// default to open access without a nil check at every call site.
type Verifier struct {
	mode  Mode
	token string
}

// NewVerifier loads and validates policy, returning a Verifier ready to
// wrap a listener's handler. ModeNone (or an empty Mode) never rejects a
// request and does not require TokenFilePath. ModeToken requires a
// readable, non-empty token file -- the token is read once here so a
// later-unreadable file cannot silently disable authentication at request
// time.
func NewVerifier(policy Policy) (*Verifier, error) {
	switch policy.Mode.normalized() {
	case ModeNone:
		return &Verifier{mode: ModeNone}, nil
	case ModeToken:
		if policy.TokenFilePath == "" {
			return nil, errors.New(
				"apiauth: tokenFilePath is required when auth mode is \"token\"",
			)
		}
		raw, err := os.ReadFile(
			policy.TokenFilePath,
		) //nolint:gosec // operator-configured path
		if err != nil {
			return nil, fmt.Errorf("apiauth: reading auth token file: %w", err)
		}
		token := strings.TrimSpace(string(raw))
		if token == "" {
			return nil, fmt.Errorf(
				"apiauth: auth token file %q is empty",
				policy.TokenFilePath,
			)
		}
		return &Verifier{mode: ModeToken, token: token}, nil
	default:
		return nil, fmt.Errorf(
			"apiauth: invalid auth mode %q (must be \"none\" or \"token\")",
			policy.Mode,
		)
	}
}

// String implements fmt.Stringer so accidental %v/%+v logging of a
// Verifier (or a struct embedding one) never reflects into the unexported
// token field.
func (v *Verifier) String() string {
	if v == nil {
		return "apiauth.Verifier(none)"
	}
	return fmt.Sprintf("apiauth.Verifier{mode=%s}", v.mode)
}

// LogValue implements slog.LogValuer for the same reason as String.
func (v *Verifier) LogValue() slog.Value {
	if v == nil {
		return slog.StringValue(string(ModeNone))
	}
	return slog.StringValue(string(v.mode))
}

const (
	headerAuthorization = "Authorization"
	// headerProjectID is Blockfrost's own convention for presenting an API
	// key. It is accepted as an alias for the shared bearer token on every
	// provider (not just Blockfrost), per #2996's scope decision: operators
	// running Blockfrost-compatible client tooling (e.g. blockfrost-js)
	// that only knows how to send "project_id" should not need a second,
	// provider-specific auth mechanism.
	headerProjectID = "project_id"
	bearerPrefix    = "Bearer "
)

// Middleware wraps next with credential enforcement. In ModeNone (or for a
// nil Verifier), requests are passed through unconditionally. In
// ModeToken, a request must present the configured token either as a
// standard "Authorization: Bearer <token>" header or, as a
// Blockfrost-compatible alias, a "project_id: <token>" header. A missing
// or incorrect credential fails closed with 401 Unauthorized.
func (v *Verifier) Middleware(next http.Handler) http.Handler {
	if v == nil || v.mode != ModeToken {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !v.authorized(r) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="dingo-api"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (v *Verifier) authorized(r *http.Request) bool {
	if token, ok := bearerToken(r.Header.Get(headerAuthorization)); ok &&
		v.equalToken(token) {
		return true
	}
	if projectID := r.Header.Get(headerProjectID); projectID != "" &&
		v.equalToken(projectID) {
		return true
	}
	return false
}

func (v *Verifier) equalToken(candidate string) bool {
	// Constant-time comparison: a credential-verification implementation
	// must not leak the token's length or contents through timing.
	return subtle.ConstantTimeCompare([]byte(candidate), []byte(v.token)) == 1
}

func bearerToken(header string) (string, bool) {
	if len(header) < len(bearerPrefix) ||
		!strings.EqualFold(header[:len(bearerPrefix)], bearerPrefix) {
		return "", false
	}
	return header[len(bearerPrefix):], true
}
