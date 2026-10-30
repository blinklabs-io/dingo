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
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const runawayQuery = "WITH RECURSIVE x(n) AS (VALUES(1) UNION ALL " +
	"SELECT n+1 FROM x) SELECT sum(n) FROM x"

// TestCallerSQLCannotTakeEveryReadSlot holds more runaway sqlite_query calls
// than the shared pool has slots and checks that the surplus is refused while
// the slots left over still admit a built-in read.
func TestCallerSQLCannotTakeEveryReadSlot(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "m.db"))
	require.NoError(t, err)
	db.SetMaxOpenConns(2 * maxConcurrentSQLiteQueries)
	t.Cleanup(func() { _ = db.Close() })

	limits := newReadLimits()
	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1"}, nil,
	)
	registerSQLiteTools(server, db, time.Minute, 10, limits)
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ss, err := server.Connect(t.Context(), serverTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(
		&mcp.Implementation{Name: "test", Version: "1"}, nil,
	).Connect(t.Context(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })

	callCtx, cancelCalls := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	t.Cleanup(wg.Wait)
	t.Cleanup(cancelCalls)

	type outcome struct {
		res *mcp.CallToolResult
		err error
	}
	returned := make(chan outcome, maxConcurrentSQLiteQueries)
	for range maxConcurrentSQLiteQueries {
		wg.Go(func() {
			res, err := cs.CallTool(callCtx, &mcp.CallToolParams{
				Name:      "sqlite_query",
				Arguments: map[string]any{"query": runawayQuery},
			})
			returned <- outcome{res, err}
		})
	}

	// The runaway queries only return when refused, so each early return
	// is a refusal. The budget must refuse at least one of them.
	select {
	case o := <-returned:
		require.NoError(t, o.err)
		require.True(t, o.res.IsError)
		require.NotEmpty(t, o.res.Content)
		text, ok := o.res.Content[0].(*mcp.TextContent)
		require.True(t, ok)
		assert.Contains(t, text.Text, errReadsBusy.Error())
	case <-time.After(30 * time.Second):
		t.Fatal("every caller SQL query was admitted: the shared read " +
			"pool has no slot left for built-in reads")
	}

	require.NoError(t, limits.admit(),
		"a built-in read must still be admitted while caller SQL is held")
	limits.release()
}

func TestAdmitCallerSQLBudgetAccounting(t *testing.T) {
	t.Parallel()
	limits := newReadLimits()
	for range maxConcurrentCallerSQLQueries {
		require.NoError(t, limits.admitCallerSQL())
	}
	require.ErrorIs(t, limits.admitCallerSQL(), errReadsBusy)
	assert.Len(t, limits.slots, maxConcurrentCallerSQLQueries,
		"a refused caller query must not hold a shared slot")

	limits.releaseCallerSQL()
	require.NoError(t, limits.admitCallerSQL())

	// A full shared pool refuses caller SQL without leaking a caller slot.
	for range maxConcurrentCallerSQLQueries {
		limits.releaseCallerSQL()
	}
	for limits.acquire() {
	}
	require.ErrorIs(t, limits.admitCallerSQL(), errReadsBusy)
	assert.Empty(t, limits.callerSlots)
}
