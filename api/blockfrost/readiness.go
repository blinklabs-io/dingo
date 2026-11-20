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
	"strings"

	"github.com/blinklabs-io/dingo/database/indexerdb"
)

// ungatedSegments are the first path segments, after /api/v0/, of routes the
// IndexerDB does not back: ledger, mempool and static routes, plus reserved
// operations that answer without reading any store.
var ungatedSegments = map[string]bool{
	"": true, "health": true, "genesis": true,
	"tx": true, "utils": true, "mempool": true, "metrics": true,
	"ipfs": true, "nutlink": true,
}

// currentSegments read current state; historySegments read history.
var (
	currentSegments = map[string]bool{
		"accounts": true, "addresses": true, "assets": true,
		"pools": true, "governance": true, "network": true,
	}
	historySegments = map[string]bool{
		"epochs": true, "blocks": true, "txs": true,
		"scripts": true, "metadata": true,
	}
)

// classifyPath reports the readiness class of an API path. known is false for
// a first segment no set names, so a new route family cannot be served
// unclassified.
func classifyPath(path string) (kind indexerdb.Kind, gated, known bool) {
	trimmed := strings.TrimPrefix(path, "/api/v0/")
	if trimmed == path {
		return 0, false, path == "/" || path == "/health" ||
			path == "/health/clock"
	}
	segments := strings.Split(strings.Trim(trimmed, "/"), "/")
	first := segments[0]
	switch {
	case first == "epochs" && len(segments) > 1 && segments[1] == "latest":
		return indexerdb.KindCurrent, true, true
	case currentSegments[first]:
		return indexerdb.KindCurrent, true, true
	case historySegments[first]:
		return indexerdb.KindHistory, true, true
	}
	return 0, false, ungatedSegments[first]
}

// classifyRoute reports which IndexerDB readiness a request needs. Account,
// address, asset, pool and governance routes and the latest epoch read
// current state; blocks, transactions, scripts, metadata and finished epochs
// read history.
func classifyRoute(r *http.Request) (indexerdb.Kind, bool) {
	kind, gated, _ := classifyPath(r.URL.Path)
	return kind, gated
}
