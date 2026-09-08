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

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// TestWarmHotUtxoCacheDoesNotResurrectConcurrentlySpentRef is a regression
// test for a race a bot reviewer identified in blinklabs-io/dingo#4082's
// fix: ResolveUtxoCbor's hot-cache Put is not gated on the live-set snapshot
// IterateLiveUtxoRefs took to build the warm pass's job list. If a ref is
// spent by a concurrent write-path transaction strictly between that
// snapshot and this worker's resolve, the write path's own
// evictHotUtxoCache call can already have run (removing any prior hot
// entry) before this Put executes -- silently resurrecting a now-spent ref
// into the hot cache with no further spend event left to ever evict it
// again.
//
// True goroutine timing can't force this exact interleaving
// deterministically, so this test uses resolveLiveUtxoRefsTestHook (a
// test-only seam, warm_hot_cache.go) to inject the concurrent spend at the
// precise point between the worker's resolve and its liveness recheck --
// proving the recheck actually observes and corrects for it, not just that
// it exists in the source.
func TestWarmHotUtxoCacheDoesNotResurrectConcurrentlySpentRef(t *testing.T) {
	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	candidate := findGapConsumeCandidateWithoutCertificates(t)
	require.NotEmpty(t, candidate.producers)
	producer := candidate.producers[0]
	seedLiveProducerForWarmTest(t, db, producer)

	produced := producer.tx.Produced()
	require.NotEmpty(t, produced)
	utxo := produced[0]
	var ref UtxoRef
	copy(ref.TxId[:], utxo.Id.Id().Bytes())
	ref.OutputIdx = utxo.Id.Index()

	t.Cleanup(func() { resolveLiveUtxoRefsTestHook = nil })
	resolveLiveUtxoRefsTestHook = func(hookRef UtxoRef) {
		if hookRef != ref {
			return
		}
		// Simulate the concurrent write path: mark the ref spent in
		// metadata (what the real spend's transactionStore().SetTransaction
		// call does) right in the window between this worker's resolve
		// (which already re-populated the hot cache) and its liveness
		// recheck below.
		require.NoError(t, db.MarkUtxosDeletedAtSlot(
			nil,
			[]types.UtxoKey{{TxId: ref.TxId[:], OutputIdx: ref.OutputIdx}},
			producer.point.Slot+1,
		))
	}

	warmed, err := db.WarmHotUtxoCache(context.Background(), 1)
	require.NoError(t, err)
	require.Zero(
		t,
		warmed,
		"the concurrently-spent ref must not count as warmed",
	)

	before := db.CborCache().Metrics().ColdExtractions.Load()
	cbor, err := db.CborCache().ResolveUtxoCbor(ref.TxId[:], ref.OutputIdx)
	require.NoError(
		t,
		err,
		"the blob offset is untouched by a spend, so it must still resolve",
	)
	require.NotEmpty(t, cbor)
	after := db.CborCache().Metrics().ColdExtractions.Load()
	require.Greater(
		t,
		after,
		before,
		"resolving the ref again must cost a fresh cold extraction, "+
			"proving the warm pass's recheck forgot the resurrected hot "+
			"entry rather than leaving it stuck in the cache forever",
	)
}
