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

import "net/http"

// options configures Middleware's credential extraction.
type options struct {
	aliasHeaders []string
}

// Option configures Middleware.
type Option func(*options)

// WithAliasHeader accepts header's raw value as a bearer-equivalent
// credential, in addition to the standard "Authorization: Bearer <token>"
// header. This is how Blockfrost's real `project_id` header authenticates
// against the same shared token mechanism every other built-in API
// provider uses: a Blockfrost-compatible client sends
// "project_id: <token>" instead of an Authorization header, and dingo
// treats presenting the correct value there as equivalent to presenting
// it as a bearer credential. See ARCHITECTURE.md/README.md's "API
// security" section for this compatibility decision.
func WithAliasHeader(header string) Option {
	return func(o *options) {
		o.aliasHeaders = append(o.aliasHeaders, header)
	}
}

// Middleware wraps next so every request must present a credential
// verifier accepts, responding 401 and never calling next otherwise. A
// nil verifier (authentication disabled) returns next completely
// unwrapped -- Middleware(nil) is a documented no-op, matching Verifier's
// own "nil means disabled" contract.
//
// Middleware must be installed inside (nearer the mux than) CORS
// middleware, not outside it: a CORS preflight (OPTIONS with
// Access-Control-Request-Method) never carries the caller's real
// credential -- browsers do not attach Authorization to preflight
// requests -- so it must be answered by CORS before authentication would
// see it, or every preflight would fail closed and break browser access
// entirely. httpcors.Handler already fully answers OPTIONS preflights
// itself and never calls its wrapped handler for one, so wrapping
// Middleware's result in httpcors.Handler (not the reverse) is what
// achieves this. Every non-preflight request -- including a plain
// unauthenticated OPTIONS with no CORS negotiation headers -- still
// reaches Middleware and must authenticate like any other request.
func Middleware(
	verifier *Verifier,
	opts ...Option,
) func(http.Handler) http.Handler {
	if verifier == nil {
		return func(next http.Handler) http.Handler { return next }
	}
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			credential := bearerToken(r.Header.Get("Authorization"))
			if credential == "" {
				for _, header := range o.aliasHeaders {
					if v := r.Header.Get(header); v != "" {
						credential = v
						break
					}
				}
			}
			if !verifier.Verify(credential) {
				w.Header().Set("WWW-Authenticate", `Bearer realm="dingo"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
