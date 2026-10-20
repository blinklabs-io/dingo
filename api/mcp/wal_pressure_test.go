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
	"bytes"
	"context"
	"os"
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
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
	probe := walFileBytes(path, nil)
	assert.Zero(t, probe(), "missing WAL reads as zero")

	require.NoError(t, os.WriteFile(path+"-wal", make([]byte, 4096), 0o600))
	assert.Equal(t, int64(4096), probe())
}

// TestNewMCPServerRefusesDatabaseReadsUnderWALPressure drives the composed
// server against a file-backed metadata store whose -wal file is grown past
// the limit, so it covers the probe path NewMCPServer derives as well as
// every database-backed tool and resource, not only the slot-limited ones.
func TestNewMCPServerRefusesDatabaseReadsUnderWALPressure(t *testing.T) {
	t.Parallel()

	nodeDB, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{DataDir: t.TempDir()},
		Metadata: dbtest.StorageProvider{
			Config: map[string]any{"dataDir": t.TempDir()},
		},
	})
	require.NoError(t, err)
	provider, ok := nodeDB.Metadata().(interface{ SQLitePath() string })
	require.True(t, ok)
	walPath := provider.SQLitePath() + "-wal"

	server, ro, err := NewMCPServer(
		t.Context(),
		DefaultProviderConfig(),
		ProviderDependencies{Database: nodeDB},
	)
	require.NoError(t, err)
	require.NotNil(t, ro)
	t.Cleanup(func() { _ = ro.Close() })
	ct, st := mcp.NewInMemoryTransports()
	_, err = server.Connect(t.Context(), st, nil)
	require.NoError(t, err)
	cs, err := mcp.NewClient(
		&mcp.Implementation{Name: "test", Version: "1"}, nil,
	).Connect(t.Context(), ct, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })

	tools := map[string]map[string]any{
		"sqlite_query":    {"query": "SELECT 1"},
		"get_cardano_tip": {},
		"get_block":       {"hash_or_slot": "1"},
	}
	setWAL := func(size int64) {
		f, err := os.OpenFile(walPath, os.O_RDWR, 0)
		require.NoError(t, err)
		require.NoError(t, f.Truncate(size))
		require.NoError(t, f.Close())
	}

	// Grow the live -wal file with a sparse tail past the limit, then
	// restore its original length: SQLite ignores bytes past its last valid
	// frame, while truncating valid frames away would corrupt the store.
	info, err := os.Stat(walPath)
	require.NoError(t, err)
	setWAL(maxSQLiteWALBytes + 1)
	for name, args := range tools {
		res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{
			Name: name, Arguments: args,
		})
		require.NoError(t, err, name)
		require.True(t, res.IsError, "%s admitted under WAL pressure", name)
		assert.Contains(t, res.Content[0].(*mcp.TextContent).Text, "WAL", name)
	}
	_, err = cs.ReadResource(t.Context(), &mcp.ReadResourceParams{
		URI: "dingo://node/status",
	})
	require.ErrorContains(t, err, "WAL")

	// Once the WAL is back under the limit, reads resume.
	setWAL(info.Size())
	for name, args := range tools {
		res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{
			Name: name, Arguments: args,
		})
		require.NoError(t, err, name)
		if res.IsError {
			assert.NotContains(
				t, res.Content[0].(*mcp.TextContent).Text, "WAL", name,
			)
		}
	}
	_, err = cs.ReadResource(t.Context(), &mcp.ReadResourceParams{
		URI: "dingo://node/status",
	})
	require.NoError(t, err)
}

func TestWALFileBytesFailsClosedOnStatError(t *testing.T) {
	t.Parallel()

	// A regular file where the database directory should be makes stat of
	// the -wal path fail with an error other than "does not exist".
	parent := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(parent, nil, 0o600))
	var logs bytes.Buffer
	probe := walFileBytes(
		filepath.Join(parent, "metadata.sqlite"),
		slog.New(slog.NewTextHandler(&logs, nil)),
	)
	for range 3 {
		assert.Greater(t, probe(), int64(maxSQLiteWALBytes))
	}
	assert.Equal(t, 1, strings.Count(logs.String(), "cannot stat"),
		"a persistent stat error is logged once per interval")
}
