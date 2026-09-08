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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// seedLiveProducerForWarmTest writes one producer transaction's block, blob
// offsets, and metadata rows directly -- bypassing Database.SetTransaction/
// SetGapBlockTransaction (and therefore their warmHotUtxoCache call) -- so
// the produced UTxOs are live with an offset reference but the hot cache
// has never seen them. This is the same bypass
// TestSetTransactionRecoveryPopulatesProducerFK uses, and models the real
// scenario WarmHotUtxoCache exists for: a UTxO that became part of the live
// set before this process's write-path warming had any chance to observe
// it (e.g. a Mithril-bootstrapped snapshot, or resuming a data dir written
// by a pre-#4082 binary).
func seedLiveProducerForWarmTest(t *testing.T, db *Database, p gapProducerTx) {
	t.Helper()
	storeBlockOffsetsOnly(t, db, p.block)
	metaTxn := db.MetadataTxn(true)
	require.NoError(
		t,
		metaTxn.Do(func(txn *Txn) error {
			return db.Metadata().SetGapBlockTransaction(
				p.tx, p.point, 0, txn.Metadata(),
			)
		}),
	)
	metaTxn.Release()
}

// TestWarmHotUtxoCachePopulatesForPreExistingLiveSet is a regression test
// for blinklabs-io/dingo#4082's startup/bootstrap warming half of the fix:
// a UTxO that became live before this process's write-path warming had a
// chance to see it must still end up hot after one WarmHotUtxoCache pass,
// without requiring a caller to query it first. Before the pass, resolving
// any of these refs costs a real cold block extraction (the offset
// reference is real and has never been resolved); after it, every one of
// them must be a pure hot-cache hit.
func TestWarmHotUtxoCachePopulatesForPreExistingLiveSet(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	candidate := findGapConsumeCandidateWithoutCertificates(t)
	require.NotEmpty(t, candidate.producers)

	var wantRefs []UtxoRef
	for _, p := range candidate.producers {
		seedLiveProducerForWarmTest(t, db, p)
		for _, utxo := range p.tx.Produced() {
			var ref UtxoRef
			copy(ref.TxId[:], utxo.Id.Id().Bytes())
			ref.OutputIdx = utxo.Id.Index()
			wantRefs = append(wantRefs, ref)
		}
	}
	require.NotEmpty(t, wantRefs, "fixture producers must produce UTxOs")

	// Sanity check the premise: nothing has been resolved yet, so the hot
	// cache is genuinely cold for every one of these refs before warming.
	beforeWarm := db.CborCache().Metrics().ColdExtractions.Load()

	warmed, err := db.WarmHotUtxoCache(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, len(wantRefs), warmed)

	afterWarm := db.CborCache().Metrics().ColdExtractions.Load()
	require.Greater(
		t,
		afterWarm,
		beforeWarm,
		"warming a genuinely cold live set must cost at least one cold "+
			"extraction -- otherwise this test's premise (nothing was "+
			"resolved before the warm pass) does not hold",
	)

	// The real assertion: after warming, every ref is a pure hot-cache hit
	// with no further cold extraction.
	before := db.CborCache().Metrics().ColdExtractions.Load()
	for _, ref := range wantRefs {
		cbor, err := db.CborCache().ResolveUtxoCbor(ref.TxId[:], ref.OutputIdx)
		require.NoError(t, err)
		require.NotEmpty(t, cbor)
	}
	after := db.CborCache().Metrics().ColdExtractions.Load()
	require.Equal(
		t,
		before,
		after,
		"every ref must resolve from the hot cache after WarmHotUtxoCache, "+
			"with zero additional cold extractions",
	)
}

// TestWarmHotUtxoCacheEmptyLiveSet covers the trivial case: no live UTxOs at
// all must not error and must report zero warmed.
func TestWarmHotUtxoCacheEmptyLiveSet(t *testing.T) {
	db := newTestDB(t)
	warmed, err := db.WarmHotUtxoCache(context.Background(), 0)
	require.NoError(t, err)
	require.Zero(t, warmed)
}

// TestWarmHotUtxoCacheRespectsCancellation covers ctx cancellation: an
// already-cancelled context must not be treated as success (a caller
// distinguishing a clean pass from a cut-short one, e.g. for retry or
// logging, must be able to tell the difference).
func TestWarmHotUtxoCacheRespectsCancellation(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	candidate := findGapConsumeCandidateWithoutCertificates(t)
	require.NotEmpty(t, candidate.producers)
	seedLiveProducerForWarmTest(t, db, candidate.producers[0])

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = db.WarmHotUtxoCache(ctx, 2)
	require.ErrorIs(t, err, context.Canceled)
}
