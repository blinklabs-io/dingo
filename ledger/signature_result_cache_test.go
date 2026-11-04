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

package ledger

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/ledger/eras"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestBlockSignaturePreverifyReusesCachedResults checks that block
// application reuses the signature outcome an earlier validation, such as
// mempool admission, cached for the same transaction.
func TestBlockSignaturePreverifyReusesCachedResults(t *testing.T) {
	t.Parallel()
	tx := preverifyTestTransactionWithWitness(t, 11, 1)
	cache := eras.NewSignatureResultCache()
	admitted := cache.Verify(tx)
	require.NotNil(t, admitted)

	p := startSignaturePreverifyAhead(
		context.Background(),
		[]gledger.Block{signatureAheadBlock(tx)},
		[]bool{true},
		cache,
	)
	defer stopSignaturesWithin(t, p)
	results := p.forBlock(0)
	require.Len(t, results, 1)
	require.Same(t, admitted, results[0])

	set := preverifyTransactionSignatures([]lcommon.Transaction{tx}, cache)
	require.Same(t, admitted, set[tx.LeiosHash()])

	// Without a cache the block verifies afresh.
	uncached := preverifyTransactionSignatures([]lcommon.Transaction{tx}, nil)
	require.NotSame(t, admitted, uncached[tx.LeiosHash()])
}

// TestNewLedgerStateSharesSignatureResultCache checks that a LedgerState
// built by NewLedgerState holds a signature result cache and that every
// LedgerView exposes that same cache to the era rules.
func TestNewLedgerStateSharesSignatureResultCache(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	cm, err := chain.NewManager(t.Context(), db, nil)
	require.NoError(t, err)
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	cache := ls.SignatureResultCache()
	require.NotNil(t, cache)
	for range 2 {
		lv := &LedgerView{ls: ls}
		require.Same(t, cache, lv.SignatureResultCache())
		var state lcommon.LedgerState = lv
		provider, ok := lcommon.UnwrapLedgerState(state).(eras.SignatureResultCacheProvider)
		require.True(t, ok)
		require.Same(t, cache, provider.SignatureResultCache())
	}
}
