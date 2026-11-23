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

package node

import (
	"context"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/indexer"
	"github.com/stretchr/testify/require"
)

// failingPatchFacts fails PatchFacts once, on its failOn-th call.
type failingPatchFacts struct {
	indexer.FactsSource
	calls  int
	failOn int
}

func (f *failingPatchFacts) PatchFacts(
	ctx context.Context,
	dst *database.Txn,
	from, through uint64,
) error {
	f.calls++
	if f.calls == f.failOn {
		return errors.New("database or disk is full")
	}
	return f.FactsSource.PatchFacts(ctx, dst, from, through)
}

// A failure after IndexBlocks succeeded rolls the batch back; the retry must
// not continue from the running nonce and epoch state of the lost batch, so
// it leaves the rows of a pass that never failed.
func TestIndexerWriterRetryAfterLatePatchFailureMatchesAnUnfailedPass(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()

	consensus := chainStoreTemplate.openEquivalence(t)
	ls, _ := openFactLedger(t, consensus)

	idxDB, idxRaw := newEquivalenceIndexer(t, consensus)
	store, ledgerFacts, bf := equivalenceParts(t, consensus, idxDB, ls)
	facts := &failingPatchFacts{FactsSource: ledgerFacts, failOn: 3}
	w := newEquivalenceWriter(
		store, indexer.NewDatabaseSource(consensus), facts, bf,
	)

	require.Error(t, w.CatchUp(ctx))
	require.NoError(t, w.CatchUp(ctx))
	require.GreaterOrEqual(t, facts.calls, 4)

	requireSameTables(t, chainReference(t), dumpTables(t, idxRaw))
}
