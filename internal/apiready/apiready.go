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

// Package apiready gates API routes on IndexerDB readiness. A route is
// either ungated (served from the ledger or static) or reads current state or
// history; the API packages classify their own routes and share the gate.
package apiready

import (
	"net/http"

	"github.com/blinklabs-io/dingo/database/indexerdb"
)

// Check reports whether endpoints of kind may answer.
type Check func(kind indexerdb.Kind) (bool, error)

// Classifier maps a request to the readiness kind its route needs. gated is
// false for routes the IndexerDB does not back.
type Classifier func(r *http.Request) (kind indexerdb.Kind, gated bool)

// Deny writes an API's own not-ready response.
type Deny func(w http.ResponseWriter, status int, message string)

// RetryAfterSeconds is the Retry-After value sent with a not-ready response.
const RetryAfterSeconds = "5"

// Middleware answers 503 for a gated request whose kind is not ready. A nil
// check serves everything, which is the shared layout. A check that errors
// refuses the request: serving from an IndexerDB whose tip cannot be read
// could answer from rows the tip does not cover.
func Middleware(
	check Check,
	classify Classifier,
	deny Deny,
	next http.Handler,
) http.Handler {
	if check == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kind, gated := classify(r)
		if gated {
			ok, err := check(kind)
			if err != nil || !ok {
				w.Header().Set("Retry-After", RetryAfterSeconds)
				deny(
					w,
					http.StatusServiceUnavailable,
					"indexer is not ready for this endpoint",
				)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
