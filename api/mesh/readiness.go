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

package mesh

import (
	"net/http"
	"strings"

	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/internal/apiready"
)

// classifyRoute reports which IndexerDB readiness a Mesh request needs.
// Account and network status answers carry current state, blocks and
// transactions read history, and the rest (network list and options,
// construction, mempool) is served without the IndexerDB.
func classifyRoute(r *http.Request) (indexerdb.Kind, bool) {
	path := r.URL.Path
	switch {
	case path == "/network/status", strings.HasPrefix(path, "/account/"):
		return indexerdb.KindCurrent, true
	case path == "/block", path == "/block/transaction",
		path == "/search/transactions":
		return indexerdb.KindHistory, true
	}
	return 0, false
}

// handler routes the Mesh API, answering 503 for a gated route until the
// IndexerDB is ready. A nil Ready serves everything.
func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	s.registerRoutes(mux)
	return apiready.Middleware(
		s.config.Ready, classifyRoute,
		func(w http.ResponseWriter, status int, _ string) {
			writeJSON(w, status, ErrUnavailable)
		},
		mux,
	)
}
