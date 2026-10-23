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

package blockfrost

import (
	"net/http"
	"net/url"
	"strings"
)

// These are the operations defined by Blockfrost OpenAPI 0.1.93 (openapi.yaml
// in blockfrost/openapi) that have no handler. The pinned contract test checks
// that every upstream operation is either registered or listed here.
// Implemented operations take precedence over this list. A final {name...}
// parameter matches the remaining path, as an IPFS path may name an object
// inside a directory. Specification paths sit under the /api/v0 server prefix;
// /health/clock is also listed unprefixed beside the unprefixed GET /health
// handler.
var unsupportedOperations = []string{
	"GET /api/v0/",
	"GET /api/v0/health",
	"GET /api/v0/health/clock",
	"GET /health/clock",
	"GET /api/v0/metadata/txs/labels",
	"GET /api/v0/utils/addresses/xpub/{xpub}/{role}/{index}",
	"POST /api/v0/ipfs/add",
	"GET /api/v0/ipfs/gateway/{IPFS_path...}",
	"POST /api/v0/ipfs/pin/add/{IPFS_path...}",
	"GET /api/v0/ipfs/pin/list",
	"GET /api/v0/ipfs/pin/list/{IPFS_path...}",
	"POST /api/v0/ipfs/pin/remove/{IPFS_path...}",
	"GET /api/v0/metrics",
	"GET /api/v0/metrics/endpoints",
	"GET /api/v0/nutlink/{address}",
	"GET /api/v0/nutlink/{address}/tickers",
	"GET /api/v0/nutlink/{address}/tickers/{ticker}",
	"GET /api/v0/nutlink/tickers/{ticker}",
}

func (b *Blockfrost) registerUnsupportedLiterals(mux *http.ServeMux) {
	for _, operation := range unsupportedOperations {
		_, path, _ := strings.Cut(operation, " ")
		// Reserved literals must outrank implemented parameter routes, such as
		// pools/retired versus pools/{pool_id}. Leave wildcard operations to the
		// catch-all: overlapping upstream wildcards are ambiguous to ServeMux.
		if strings.Contains(path, "{") || strings.HasSuffix(path, "/") {
			continue
		}
		mux.HandleFunc(operation, b.handleUnsupported)
	}
}

func (b *Blockfrost) handleUnsupported(w http.ResponseWriter, _ *http.Request) {
	writeError(
		w,
		http.StatusNotImplemented,
		"Not Implemented",
		"The requested endpoint is not implemented.",
	)
}

func (b *Blockfrost) writeUnsupportedOperation(
	w http.ResponseWriter,
	r *http.Request,
) bool {
	for _, operation := range unsupportedOperations {
		method, path, _ := strings.Cut(operation, " ")
		if !matchesOperationPath(path, r.URL.EscapedPath()) {
			continue
		}
		if r.Method == method ||
			(method == http.MethodGet && r.Method == http.MethodHead) {
			b.handleUnsupported(w, r)
		} else {
			allow := method
			if method == http.MethodGet {
				allow += ", HEAD"
			}
			w.Header().Set("Allow", allow)
			writeError(w, http.StatusMethodNotAllowed, "Method Not Allowed", "The requested method is not allowed for this endpoint.")
		}
		return true
	}
	return false
}

func (b *Blockfrost) writeUnsupportedMethod(
	w http.ResponseWriter,
	r *http.Request,
) bool {
	for _, operation := range unsupportedOperations {
		method, path, _ := strings.Cut(operation, " ")
		if !matchesOperationPath(path, r.URL.EscapedPath()) ||
			r.Method == method || (method == http.MethodGet && r.Method == http.MethodHead) {
			continue
		}
		allow := method
		if method == http.MethodGet {
			allow += ", HEAD"
		}
		w.Header().Set("Allow", allow)
		writeError(
			w,
			http.StatusMethodNotAllowed,
			"Method Not Allowed",
			"The requested method is not allowed for this endpoint.",
		)
		return true
	}
	return false
}

func matchesOperationPath(pattern, path string) bool {
	for {
		expected, nextPattern, morePattern := strings.Cut(pattern, "/")
		actual, nextPath, morePath := strings.Cut(path, "/")
		if strings.HasPrefix(expected, "{") &&
			strings.HasSuffix(expected, "...}") {
			return !morePattern && actual != ""
		}
		if morePattern != morePath {
			return false
		}
		if strings.HasPrefix(expected, "{") &&
			strings.HasSuffix(expected, "}") {
			if actual == "" {
				return false
			}
		} else {
			decoded, err := url.PathUnescape(actual)
			if err != nil || expected != decoded {
				return false
			}
		}
		if !morePattern {
			return true
		}
		pattern, path = nextPattern, nextPath
	}
}
