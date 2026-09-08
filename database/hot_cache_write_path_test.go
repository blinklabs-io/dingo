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

package database

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// applyProducerForWarmCacheTest writes one producer transaction through the
// same top-level write path (SetTransactionWithOpts) ordinary chain-sync
// uses, so the hot-cache warm hook (warmHotUtxoCache, called from within
// SetTransactionWithOpts) runs exactly as it would in production.
//
// BatchedTxIngestOpts.SkipConsumedInputRecovery is set because these fixture
// producer transactions have their own real inputs from earlier in the
// immutable testdata chain that this minimal two-transaction fixture does
// not seed; skipping consumed-input recovery for the producer's own spends
// is what the option exists for (see its doc comment on
// SetTransactionWithOpts) and does not affect the produced-output warming
// path under test, which runs unconditionally.
func applyProducerForWarmCacheTest(
	t *testing.T,
	db *Database,
	producer gapProducerTx,
) {
	t.Helper()
	require.NoError(t, db.BlockCreate(producer.block, nil))
	require.NoError(t, db.SetTransactionWithOpts(
		producer.tx,
		producer.point,
		0,
		0,
		nil,
		nil,
		mustBlockOffsets(t, producer.block),
		nil,
		BatchedTxIngestOpts{SkipConsumedInputRecovery: true},
	))
}

// TestSetTransactionWithOptsWarmsHotCacheOnProduce is a regression test for
// blinklabs-io/dingo#4082: a UTxO's hot-cache entry must be populated
// immediately when it is produced during block application, not left for
// TieredCborCache to backfill lazily on first read. Without this, a
// whole-UTxO-set query (GetUTxOWhole) forces one cold block extraction per
// distinct originating block for every UTxO nobody has queried yet in this
// process's lifetime -- the root cause of #4082's 120s NtC mux read timeout.
func TestSetTransactionWithOptsWarmsHotCacheOnProduce(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	candidate := findGapConsumeCandidateWithoutCertificates(t)
	require.NotEmpty(t, candidate.producers)

	var totalProduced int
	before := db.CborCache().Metrics().ColdExtractions.Load()
	for _, producer := range candidate.producers {
		applyProducerForWarmCacheTest(t, db, producer)

		produced := producer.tx.Produced()
		for _, utxo := range produced {
			totalProduced++
			txId := utxo.Id.Id().Bytes()
			cbor, err := db.CborCache().ResolveUtxoCbor(txId, utxo.Id.Index())
			require.NoError(t, err)
			require.NotEmpty(t, cbor)
		}
	}
	require.NotZero(t, totalProduced, "fixture producers must produce UTxOs")
	after := db.CborCache().Metrics().ColdExtractions.Load()
	require.Equal(
		t,
		before,
		after,
		"resolving a just-produced UTxO must be a pure hot-cache hit, "+
			"not a cold block extraction",
	)
}

// TestSetTransactionWithOptsEvictsHotCacheOnConsume is the eviction half of
// the same regression test: once a UTxO is spent, its hot-cache entry must
// be removed so the cache's membership tracks the live set instead of
// growing with total historical volume. The blob-level offset entry itself
// is untouched by a spend (blob pruning is a separate, later cleanup pass --
// see UtxosDeleteConsumed), so re-resolving an evicted, already-spent ref
// must still succeed, but now via a fresh cold extraction rather than a hot
// hit -- the inverse assertion from the produce-side test above proves the
// entry was actually removed rather than just never having existed.
func TestSetTransactionWithOptsEvictsHotCacheOnConsume(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	candidate := findGapConsumeCandidateWithoutCertificates(t)
	require.NotEmpty(t, candidate.producers)
	for _, producer := range candidate.producers {
		applyProducerForWarmCacheTest(t, db, producer)
	}

	consumed := candidate.consumerTx.Consumed()
	require.NotEmpty(t, consumed, "fixture consumer tx must consume UTxOs")

	// Confirm each consumed ref is hot before the consuming transaction is
	// applied -- otherwise a "no hit after consume" observation downstream
	// would be meaningless (it could just never have been warmed at all).
	beforeConsume := db.CborCache().Metrics().ColdExtractions.Load()
	for _, input := range consumed {
		cbor, err := db.CborCache().ResolveUtxoCbor(
			input.Id().Bytes(),
			input.Index(),
		)
		require.NoError(t, err)
		require.NotEmpty(t, cbor)
	}
	afterPreCheck := db.CborCache().Metrics().ColdExtractions.Load()
	require.Equal(
		t,
		beforeConsume,
		afterPreCheck,
		"setup: every consumed ref must already be hot before the spend",
	)

	require.NoError(t, db.BlockCreate(candidate.consumerBlock, nil))
	require.NoError(t, db.SetTransactionWithOpts(
		candidate.consumerTx,
		candidate.consumerPoint,
		0,
		0,
		nil,
		nil,
		mustBlockOffsets(t, candidate.consumerBlock),
		nil,
		BatchedTxIngestOpts{},
	))

	beforePostCheck := db.CborCache().Metrics().ColdExtractions.Load()
	for _, input := range consumed {
		cbor, err := db.CborCache().ResolveUtxoCbor(
			input.Id().Bytes(),
			input.Index(),
		)
		require.NoError(
			t,
			err,
			"a spent UTxO's blob offset is untouched by the spend itself "+
				"(pruning is a separate later pass), so it must still resolve",
		)
		require.NotEmpty(t, cbor)
	}
	afterPostCheck := db.CborCache().Metrics().ColdExtractions.Load()
	require.Greater(
		t,
		afterPostCheck,
		beforePostCheck,
		"resolving a just-spent ref must now cost a cold extraction, "+
			"proving its hot-cache entry was evicted on spend",
	)
}
