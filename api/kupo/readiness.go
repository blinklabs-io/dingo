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

package kupo

import (
	"net/http"
	"strings"

	"github.com/blinklabs-io/dingo/database/indexerdb"
)

// classifyPath reports the readiness class of a request. Matches are filtered
// by spent state, so reads of them need current state; datums, scripts,
// metadata and checkpoints read history. Health, metrics and the pattern
// registry are served without the IndexerDB. known is false for a first
// segment no case names, so a new route family cannot be served unclassified.
func classifyPath(
	method, path string,
) (kind indexerdb.Kind, gated, known bool) {
	path = strings.TrimPrefix(path, "/v1")
	first, _, _ := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	switch first {
	case "matches":
		return indexerdb.KindCurrent,
			method == http.MethodGet || method == http.MethodHead, true
	case "datums", "scripts", "metadata", "checkpoints":
		return indexerdb.KindHistory, true, true
	case "patterns", "health", "metrics":
		return 0, false, true
	}
	return 0, false, false
}

// classifyRoute reports which IndexerDB readiness a request needs.
func classifyRoute(r *http.Request) (indexerdb.Kind, bool) {
	kind, gated, _ := classifyPath(r.Method, r.URL.Path)
	return kind, gated
}
