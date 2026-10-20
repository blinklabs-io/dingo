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

package mcp

const (
	// maxConcurrentSQLiteQueries bounds the expensive database reads that
	// may run at once on one server.
	maxConcurrentSQLiteQueries = 4
	// maxAssetInfoRows bounds the live asset rows one get_asset_info call
	// scans. A call that would exceed it fails instead of returning a
	// partial supply or holder count.
	maxAssetInfoRows = 50_000
)

// readLimits holds the resource limits shared by the MCP tools that run
// unbounded-cost database reads. Every tool registered on one server must
// share one value so the concurrency slots are a server-wide budget.
type readLimits struct {
	slots        chan struct{}
	maxAssetRows int
}

func newReadLimits() *readLimits {
	return &readLimits{
		slots:        make(chan struct{}, maxConcurrentSQLiteQueries),
		maxAssetRows: maxAssetInfoRows,
	}
}

// acquire takes a query slot without blocking and reports whether it got one.
func (l *readLimits) acquire() bool {
	select {
	case l.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (l *readLimits) release() { <-l.slots }
