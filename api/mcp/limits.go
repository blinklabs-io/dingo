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

import (
	"context"
	"errors"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// maxConcurrentSQLiteQueries bounds the expensive database reads that
	// may run at once on one server.
	maxConcurrentSQLiteQueries = 4
	// maxAssetInfoRows bounds the live asset rows one get_asset_info call
	// scans. A call that would exceed it fails instead of returning a
	// partial supply or holder count.
	maxAssetInfoRows = 50_000
	// maxSQLiteWALBytes is the metadata WAL size above which new database
	// reads are refused. It is twice the 64 MiB journal_size_limit the
	// metadata store sets, so a WAL this large has been neither reset nor
	// truncated since it grew past the limit, and an open reader is one
	// thing that prevents both.
	maxSQLiteWALBytes = 128 << 20
)

var (
	errReadsBusy = errors.New("too many concurrent SQLite queries")
	// errWALPressure is returned while the metadata WAL exceeds
	// maxSQLiteWALBytes. Refusing new reads lets the in-flight ones finish
	// within their query timeout, which gives the periodic TRUNCATE
	// checkpoint the reader-free interval it needs.
	errWALPressure = errors.New(
		"SQLite metadata WAL is above its size limit; " +
			"inspection queries are paused until it is checkpointed",
	)
)

// readLimits holds the resource limits shared by the MCP tools that run
// unbounded-cost database reads. Every tool registered on one server must
// share one value so the concurrency slots are a server-wide budget.
type readLimits struct {
	slots        chan struct{}
	maxAssetRows int
	// walBytes reports the metadata WAL size. Nil disables the pressure
	// check, as when the server has no file-backed metadata store.
	walBytes    func() int64
	maxWALBytes int64
}

func newReadLimits() *readLimits {
	return &readLimits{
		slots:        make(chan struct{}, maxConcurrentSQLiteQueries),
		maxAssetRows: maxAssetInfoRows,
		maxWALBytes:  maxSQLiteWALBytes,
	}
}

// walFileBytes returns a WAL size probe for the SQLite database at path. A
// missing or unreadable WAL file reads as zero.
func walFileBytes(path string) func() int64 {
	walPath := path + "-wal"
	return func() int64 {
		info, err := os.Stat(walPath)
		if err != nil {
			return 0
		}
		return info.Size()
	}
}

// walPressure reports errWALPressure while the metadata WAL exceeds
// maxWALBytes.
func (l *readLimits) walPressure() error {
	if l.walBytes != nil && l.walBytes() > l.maxWALBytes {
		return errWALPressure
	}
	return nil
}

// walPressureMiddleware refuses every tool call and resource read while the
// metadata WAL is over its limit. Gating at the method level covers the
// tools and resources that read SQLite without a query slot, including
// those served through the metadata store's own pool, so no MCP request can
// keep a read snapshot open while the WAL waits for a reader-free interval.
func (l *readLimits) walPressureMiddleware() mcp.Middleware {
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(
			ctx context.Context,
			method string,
			req mcp.Request,
		) (mcp.Result, error) {
			if method != "tools/call" && method != "resources/read" {
				return next(ctx, method, req)
			}
			err := l.walPressure()
			if err == nil {
				return next(ctx, method, req)
			}
			if method == "tools/call" {
				return &mcp.CallToolResult{
					IsError: true,
					Content: []mcp.Content{
						&mcp.TextContent{Text: err.Error()},
					},
				}, nil
			}
			return nil, err
		}
	}
}

// admit takes a query slot for a database read, or refuses it under WAL
// pressure or when every slot is busy.
func (l *readLimits) admit() error {
	if err := l.walPressure(); err != nil {
		return err
	}
	if !l.acquire() {
		return errReadsBusy
	}
	return nil
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
