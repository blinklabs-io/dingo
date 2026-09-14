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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// benchmarkOpenDBQueryLoop exercises OpenDB with tracingEnabled and issues a
// representative number of simple, real queries against the resulting pool,
// mirroring the query-per-block-application-step traffic pattern
// (ledgerProcessBlocksFromSource issues many small point queries within a
// batch). It isolates OpenDB's own instrumentation-wrapping cost from
// everything else: same driver, same query, same data, only tracingEnabled
// differs between runs.
func benchmarkOpenDBQueryLoop(b *testing.B, tracingEnabled bool) {
	db, err := OpenDB(
		"sqlite",
		fmt.Sprintf(
			"file:open_bench_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
		"sqlite",
		tracingEnabled,
	)
	require.NoError(b, err)
	b.Cleanup(func() {
		_ = db.Close()
	})
	require.NoError(b, db.Ping())
	_, err = db.Exec(
		"CREATE TABLE bench_rows (id INTEGER PRIMARY KEY, value INTEGER)",
	)
	require.NoError(b, err)
	const rowCount = 64
	for i := range rowCount {
		_, err := db.Exec(
			"INSERT INTO bench_rows (id, value) VALUES (?, ?)", i, i*2,
		)
		require.NoError(b, err)
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		var value int
		err := db.QueryRowContext(
			ctx,
			"SELECT value FROM bench_rows WHERE id = ?",
			i%rowCount,
		).Scan(&value)
		if err != nil {
			b.Fatalf("QueryRowContext: %v", err)
		}
	}
}

// BenchmarkOpenDBQueryLoop_TracingDisabled is the production default
// (config.Tracing == false): OpenDB now returns a plain sql.Open pool
// instead of wrapping it with otelsql.
func BenchmarkOpenDBQueryLoop_TracingDisabled(b *testing.B) {
	benchmarkOpenDBQueryLoop(b, false)
}

// BenchmarkOpenDBQueryLoop_TracingEnabled reproduces the otelsql-wrapped
// behavior every query paid before this change, and that an operator who
// explicitly enables --tracing still gets today.
func BenchmarkOpenDBQueryLoop_TracingEnabled(b *testing.B) {
	benchmarkOpenDBQueryLoop(b, true)
}
