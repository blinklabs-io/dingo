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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mcp

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newWALPressureSession(
	t *testing.T,
	limits *readLimits,
) *mcp.ClientSession {
	t.Helper()
	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1"}, nil,
	)
	registerSQLiteTools(server, newFixtureDB(t), time.Second, 10, limits)
	ct, st := mcp.NewInMemoryTransports()
	_, err := server.Connect(t.Context(), st, nil)
	require.NoError(t, err)
	cs, err := mcp.NewClient(
		&mcp.Implementation{Name: "test", Version: "1"}, nil,
	).Connect(t.Context(), ct, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func TestSQLiteToolsRefuseReadsUnderWALPressure(t *testing.T) {
	t.Parallel()

	var walSize atomic.Int64
	limits := newReadLimits()
	limits.walBytes = walSize.Load
	cs := newWALPressureSession(t, limits)

	calls := map[string]map[string]any{
		"sqlite_query":        {"query": "SELECT 1"},
		"sqlite_explain":      {"query": "SELECT 1"},
		"sqlite_table_schema": {"table_name": "blocks"},
	}
	for name, args := range calls {
		t.Run(name, func(t *testing.T) {
			walSize.Store(limits.maxWALBytes + 1)
			res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
				Name: name, Arguments: args,
			})
			require.NoError(t, err)
			require.True(t, res.IsError)
			assert.Contains(t, res.Content[0].(*mcp.TextContent).Text, "WAL")

			// At the limit reads are still admitted.
			walSize.Store(limits.maxWALBytes)
			res, err = cs.CallTool(context.Background(), &mcp.CallToolParams{
				Name: name, Arguments: args,
			})
			require.NoError(t, err)
			assert.False(t, res.IsError, "%v", res.Content)
		})
	}

	// Refused calls must not leak slots.
	for range maxConcurrentSQLiteQueries {
		require.True(t, limits.acquire())
	}
}

func TestAssetInfoRefusesReadsUnderWALPressure(t *testing.T) {
	t.Parallel()

	limits := newReadLimits()
	limits.walBytes = func() int64 { return limits.maxWALBytes + 1 }
	cs := newAssetInfoLimitSession(t, limits, 1)
	res, err := callAssetInfo(t, cs)
	require.NoError(t, err)
	require.True(t, res.IsError)
	assert.Contains(t, res.Content[0].(*mcp.TextContent).Text, "WAL")
}

func TestWALFileBytes(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	probe := walFileBytes(path)
	assert.Zero(t, probe(), "missing WAL reads as zero")

	require.NoError(t, os.WriteFile(path+"-wal", make([]byte, 4096), 0o600))
	assert.Equal(t, int64(4096), probe())
}
