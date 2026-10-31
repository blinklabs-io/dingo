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
	"encoding/hex"
	"maps"
	"strconv"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// utxoAdmissionDeadline bounds the server's queries and the client session.
// The tests assert admission and paging behaviour, not latency, and a query
// that pages past maxToolOffset rows scans that many, which takes seconds on
// a loaded race-detector run. A deadline near that cost turns the scan time
// into a failure of the behaviour under test.
const utxoAdmissionDeadline = 5 * time.Minute

const (
	utxoAdmissionCredential = "5e68f6d9c3203ea691221f59a3988477cfaeda716e2b7bf60b216ae0"
	utxoAdmissionPolicy     = "a0000000000000000000000000000000000000000000000000000001"
)

// newUTxOAdmissionSession serves get_utxos, get_utxos_by_asset and
// sqlite_query over an in-memory database holding rows live UTxOs, each paid
// to utxoAdmissionCredential and carrying one utxoAdmissionPolicy asset. With
// closed set, the database is closed after setup, so any query that reaches
// it fails with "database is closed" rather than with an admission error.
func newUTxOAdmissionSession(
	t *testing.T,
	limits *readLimits,
	closed bool,
	rows int,
) *mcp.ClientSession {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`
		CREATE TABLE utxo (
			id INTEGER PRIMARY KEY AUTOINCREMENT, tx_id BLOB,
			output_idx INTEGER, payment_key BLOB, staking_key BLOB,
			amount TEXT, added_slot INTEGER,
			deleted_slot INTEGER NOT NULL DEFAULT 0, datum_hash BLOB);
		CREATE TABLE asset (
			id INTEGER PRIMARY KEY AUTOINCREMENT, utxo_id INTEGER NOT NULL,
			policy_id BLOB NOT NULL, name BLOB NOT NULL,
			fingerprint BLOB NOT NULL, amount TEXT NOT NULL);
	`)
	require.NoError(t, err)
	if rows > 0 {
		_, err = db.Exec(`
			WITH RECURSIVE n(i) AS (
				SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?)
			INSERT INTO utxo (tx_id, output_idx, payment_key, amount, added_slot)
			SELECT randomblob(32), 0, ?, '1000000', i FROM n`,
			rows, mustDecodeHex(t, utxoAdmissionCredential),
		)
		require.NoError(t, err)
		_, err = db.Exec(`
			INSERT INTO asset (utxo_id, policy_id, name, fingerprint, amount)
			SELECT id, ?, X'', 'asset1', '1' FROM utxo`,
			mustDecodeHex(t, utxoAdmissionPolicy),
		)
		require.NoError(t, err)
	}
	if closed {
		require.NoError(t, db.Close())
	}

	server := mcp.NewServer(
		&mcp.Implementation{Name: "test-server", Version: "1.0"}, nil,
	)
	registerCardanoTools(
		server, db, nil, nil, "preview", utxoAdmissionDeadline, limits,
	)
	registerSQLiteTools(server, db, utxoAdmissionDeadline, 200, limits)
	ctx, cancel := context.WithTimeout(context.Background(), utxoAdmissionDeadline)
	t.Cleanup(cancel)
	ct, st := mcp.NewInMemoryTransports()
	_, err = server.Connect(ctx, st, nil)
	require.NoError(t, err)
	cs, err := mcp.NewClient(
		&mcp.Implementation{Name: "client", Version: "1.0"}, nil,
	).Connect(ctx, ct, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func utxoAdmissionCalls() map[string]map[string]any {
	return map[string]map[string]any{
		"get_utxos": {
			"address_or_credential": utxoAdmissionCredential,
		},
		"get_utxos_by_asset": {"policy_id": utxoAdmissionPolicy},
	}
}

func callUTxOAdmission(
	t *testing.T,
	cs *mcp.ClientSession,
	tool string,
	extra map[string]any,
) string {
	t.Helper()
	args := maps.Clone(utxoAdmissionCalls()[tool])
	maps.Copy(args, extra)
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name: tool, Arguments: args,
	})
	require.NoError(t, err)
	require.True(t, res.IsError)
	return res.Content[0].(*mcp.TextContent).Text
}

func TestUTxOToolsRefusedWhenReadSlotsBusy(t *testing.T) {
	t.Parallel()
	for tool := range utxoAdmissionCalls() {
		t.Run(tool, func(t *testing.T) {
			t.Parallel()
			limits := newReadLimits()
			cs := newUTxOAdmissionSession(t, limits, true, 0)
			for limits.acquire() {
			}
			text := callUTxOAdmission(t, cs, tool, nil)
			assert.Contains(t, text, errReadsBusy.Error())
			assert.NotContains(t, text, "database is closed")
		})
	}
}

func TestUTxOToolsRejectOffsetAboveBound(t *testing.T) {
	t.Parallel()
	for tool := range utxoAdmissionCalls() {
		t.Run(tool, func(t *testing.T) {
			t.Parallel()
			limits := newReadLimits()
			cs := newUTxOAdmissionSession(t, limits, true, 0)
			text := callUTxOAdmission(
				t, cs, tool, map[string]any{"offset": maxToolOffset + 1},
			)
			assert.Contains(t, text, errOffsetTooLarge.Error())
			assert.Contains(t, text, strconv.Itoa(maxToolOffset))
			assert.NotContains(t, text, "database is closed")
		})
	}
}

func TestUTxOToolsAcceptOffsetAtBoundAndReleaseSlot(t *testing.T) {
	t.Parallel()
	for tool := range utxoAdmissionCalls() {
		t.Run(tool, func(t *testing.T) {
			t.Parallel()
			limits := newReadLimits()
			cs := newUTxOAdmissionSession(t, limits, false, 0)
			res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
				Name: tool,
				Arguments: func() map[string]any {
					args := map[string]any{"offset": maxToolOffset}
					maps.Copy(args, utxoAdmissionCalls()[tool])
					return args
				}(),
			})
			require.NoError(t, err)
			assert.False(t, res.IsError)
			held := 0
			for limits.acquire() {
				held++
			}
			assert.Equal(t, maxConcurrentSQLiteQueries, held,
				"the call must give its slot back")
		})
	}
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

// TestOffsetToolsReportOffsetLimit pins the notice a full page carries when
// the next offset would be rejected, so a caller paging until a short page
// can tell a result cut off by maxToolOffset from a complete one.
func TestOffsetToolsReportOffsetLimit(t *testing.T) {
	t.Parallel()
	cs := newUTxOAdmissionSession(
		t, newReadLimits(), false, maxToolOffset+1,
	)
	calls := utxoAdmissionCalls()
	calls["sqlite_query"] = map[string]any{
		"query": "SELECT id FROM utxo ORDER BY id",
	}
	for tool, base := range calls {
		for _, tc := range []struct {
			offset int
			limit  int
			want   bool
		}{
			// The next offset, maxToolOffset, is still accepted.
			{offset: maxToolOffset - 1, limit: 1, want: false},
			// The next offset, maxToolOffset+1, is rejected.
			{offset: maxToolOffset, limit: 1, want: true},
			// A short page is the last one, so nothing was cut off.
			{offset: maxToolOffset, limit: 2, want: false},
		} {
			name := tool + "/" + strconv.Itoa(tc.offset) + "/" +
				strconv.Itoa(tc.limit)
			t.Run(name, func(t *testing.T) {
				args := maps.Clone(base)
				args["offset"] = tc.offset
				args["limit"] = tc.limit
				res, err := cs.CallTool(
					context.Background(),
					&mcp.CallToolParams{Name: tool, Arguments: args},
				)
				require.NoError(t, err)
				text := res.Content[0].(*mcp.TextContent).Text
				require.False(t, res.IsError, text)
				if tc.want {
					assert.Contains(t, text, "Offset limit reached")
				} else {
					assert.NotContains(t, text, "Offset limit reached")
				}
			})
		}
	}
}
