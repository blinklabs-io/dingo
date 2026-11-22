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

package sqlstore

import (
	"bytes"
	"context"
	rtrace "runtime/trace"
	"testing"

	"github.com/stretchr/testify/require"
)

const traceTestQuery = "-- name: TraceTestStatement :exec\nINSERT INTO t VALUES (1)"

// Not t.Parallel: only one execution trace can run per process.
func TestStatementAndPhaseRegionsAreRecorded(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, rtrace.Start(&buf))
	ctx := context.Background()
	statementRegion(ctx, traceTestQuery)()
	p := startPhases(ctx, "dingo.sqlstore.tx.first")
	p.next(ctx, "dingo.sqlstore.tx.second")
	p.end()
	rtrace.Stop()

	for _, name := range []string{
		"dingo.sql.TraceTestStatement",
		"dingo.sqlstore.tx.first",
		"dingo.sqlstore.tx.second",
	} {
		require.Truef(
			t,
			bytes.Contains(buf.Bytes(), []byte(name)),
			"execution trace has no region %q",
			name,
		)
	}
}

// Not t.Parallel: only one execution trace can run per process.
func TestSetTransactionRecordsPhaseRegions(t *testing.T) {
	store := newMigratedSQLiteStore(t)
	tx, point, _, _ := assetOutputTx(t, 0x61)

	var buf bytes.Buffer
	require.NoError(t, rtrace.Start(&buf))
	err := store.SetTransaction(tx, point, 0, nil, true, nil, 0)
	rtrace.Stop()
	require.NoError(t, err)

	for _, name := range []string{
		"dingo.sqlstore.tx.prepare",
		"dingo.sqlstore.tx.identity",
		"dingo.sqlstore.tx.produced",
		"dingo.sqlstore.tx.api_details",
		"dingo.sqlstore.tx.spend",
		"dingo.sqlstore.tx.supply_stake",
	} {
		require.Truef(
			t,
			bytes.Contains(buf.Bytes(), []byte(name)),
			"execution trace has no region %q",
			name,
		)
	}
}

// Not t.Parallel: testing.AllocsPerRun measures the whole process.
func TestRegionsDoNotAllocateWhenTracingIsOff(t *testing.T) {
	require.False(t, rtrace.IsEnabled())
	ctx := context.Background()
	allocs := testing.AllocsPerRun(100, func() {
		statementRegion(ctx, traceTestQuery)()
		p := startPhases(ctx, "dingo.sqlstore.tx.first")
		p.next(ctx, "dingo.sqlstore.tx.second")
		p.end()
	})
	require.Zero(t, allocs)
}
