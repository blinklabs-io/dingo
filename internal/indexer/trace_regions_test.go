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

package indexer

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/tracetest"
	"github.com/blinklabs-io/dingo/internal/tracing"
	"github.com/stretchr/testify/require"
)

// requireBalanced fails when a region was left open or ended out of order:
// either makes go tool trace drop every region after it.
func requireBalanced(t *testing.T, regions tracetest.Regions) {
	t.Helper()
	require.Empty(t, regions.Misuse)
	require.Empty(t, regions.Open)
	require.Equal(t, regions.Begins, regions.Ends)
}

// Not t.Parallel: only one execution trace can run per process.
func TestWriterStepRecordsTraceRegions(t *testing.T) {
	store := newTestStore(t)
	chain := newFakeChain(4, "a")
	facts := &recordingFacts{log: &[]string{}}
	w := NewWriter(store, chain, &syncRowIndexer{}, nil, 3).WithFacts(facts)

	var err error
	regions := tracetest.Record(t, func() { err = w.CatchUp(t.Context()) })
	require.NoError(t, err)

	for _, name := range []string{
		tracing.RegionIndexerReadBlocks,
		tracing.RegionIndexerBatch,
		tracing.RegionIndexerCopyFacts,
		tracing.RegionIndexerIndexBlocks,
		tracing.RegionIndexerPatchFacts,
		tracing.RegionSQLCommit,
		tracing.RegionBlobCommit,
	} {
		require.Positivef(
			t,
			regions.Begins[name],
			"execution trace has no region %q",
			name,
		)
	}
	requireBalanced(t, regions)
}

type panickingIndexer struct{ syncRowIndexer }

func (p *panickingIndexer) IndexBlocks(
	context.Context, *database.Txn, []models.Block,
) error {
	panic("index blocks")
}

// Not t.Parallel: only one execution trace can run per process.
func TestWriterRecoveredPanicEndsTraceRegions(t *testing.T) {
	store := newTestStore(t)
	chain := newFakeChain(4, "a")
	w := NewWriter(store, chain, &panickingIndexer{}, nil, 3)

	var err error
	regions := tracetest.Record(t, func() { err = w.CatchUp(t.Context()) })
	require.ErrorIs(t, err, database.ErrTxnPanic)

	require.Positive(t, regions.Begins[tracing.RegionIndexerIndexBlocks])
	requireBalanced(t, regions)
}
