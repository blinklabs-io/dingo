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
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// leiosWaitTestSlotLen is the Shelley slot length these tests pin, so the
// slot-denominated diffusion window (EndorserBlockWaitSlots) converts to a
// wall-clock window short enough to assert on but long enough to separate
// "returned immediately" from "waited a window" without flaking.
const leiosWaitTestSlotLen = 10 * time.Millisecond

// leiosWaitTestWaitSlots matches the production default
// (CertifyByDeadlineSlots), so the window under test is
// leiosWaitTestWaitSlots * leiosWaitTestSlotLen = 200ms.
const leiosWaitTestWaitSlots = 20

const leiosWaitTestWindow = leiosWaitTestWaitSlots * leiosWaitTestSlotLen

// withLeiosWaitTestSlotLength gives a bare-constructed LedgerState a Shelley
// slot length, which is what ensureReferencedEndorserBlocks converts the
// slot-denominated wait window with. Without it the wait is disabled outright
// and the timing assertions below would pass vacuously.
func withLeiosWaitTestSlotLength(ls *LedgerState) {
	ls.timeConverter = NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis: func() *shelley.ShelleyGenesis {
			return &shelley.ShelleyGenesis{
				SlotLength: lcommon.GenesisRat{
					Rat: big.NewRat(
						int64(leiosWaitTestSlotLen/time.Millisecond),
						1000,
					),
				},
			}
		},
	})
	ls.timeConverterOnce.Do(func() {})
}

// leiosWaitTestAnnouncingBlock builds a Dijkstra ranking block that announces
// ebHash and certifies nothing.
func leiosWaitTestAnnouncingBlock(
	t *testing.T,
	blockNumber, slot uint64,
	ebHash lcommon.Blake2b256,
) *dijkstra.DijkstraBlock {
	t.Helper()
	return &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: blockNumber,
					Slot:        slot,
				},
			},
			LeiosHeaderExtension: []cbor.RawMessage{
				leiosTestRaw(t, false),
				leiosTestRaw(t, []any{ebHash.Bytes(), uint64(4096)}),
			},
		},
	}
}

// TestEnsureReferencedEndorserBlocksDoesNotBlockOnUnreadAnnouncement is the
// apply-lag regression. On the Haskell-conformant (Musashi) path, ledger
// application of a ranking block reads only the certified closure announced by
// a certifying block's PARENT; a block's own announcement is never read when
// that block is applied. Blocking the single ledger pipeline on it stalled
// every block queued behind it for a whole diffusion window and then applied
// the block unchanged anyway, which is where the multi-second apply lag and
// the resulting stale-tip forge came from.
//
// Before the fix this returns after the full window; after it, immediately.
func TestEnsureReferencedEndorserBlocksDoesNotBlockOnUnreadAnnouncement(
	t *testing.T,
) {
	ebHash := lcommon.NewBlake2b256(leiosTestHash(0xA1))
	block := leiosWaitTestAnnouncingBlock(t, 1, 100, ebHash)

	var fetched atomic.Int64
	fetchedCh := make(chan struct{}, 1)
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			// The endorser block never arrives.
			return nil, false
		},
		EndorserBlockFetcher: func(
			_ context.Context,
			_ uint64,
			_ []byte,
		) error {
			fetched.Add(1)
			select {
			case fetchedCh <- struct{}{}:
			default:
			}
			return nil
		},
		EndorserBlockWaitSlots: leiosWaitTestWaitSlots,
		// Haskell-conformant path: application reads only certified closures.
		LeiosApplyEndorserBlockTxs: false,
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)
	withLeiosWaitTestSlotLength(ls)
	require.Equal(t, leiosWaitTestSlotLen, ls.shelleySlotLength())

	start := time.Now()
	require.NoError(t, ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{block},
	))
	elapsed := time.Since(start)
	require.Less(
		t,
		elapsed,
		leiosWaitTestWindow/2,
		"apply gate blocked on an announcement ledger application never reads",
	)

	// The announcement is prefetched in the background rather than dropped, so
	// it is cached before anything actually depends on it.
	select {
	case <-fetchedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("background by-point fetch was never dispatched")
	}
	require.Positive(t, fetched.Load())
}

// TestEnsureReferencedEndorserBlocksWaitsForCertifiedClosureArrivingLate pins
// the other half of the contract: a certifying ranking block's closure IS read
// at apply time and committing without it would permanently omit the endorser
// block's effects, so that wait is load-bearing and is kept. An endorser block
// that lands part-way through the window must be picked up, not skipped.
func TestEnsureReferencedEndorserBlocksWaitsForCertifiedClosureArrivingLate(
	t *testing.T,
) {
	parent, certifier, ebHash := leiosTestCertifiedBlockPair(t)
	var available atomic.Bool
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func(
			hash []byte,
			slot uint64,
		) ([]cbor.RawMessage, bool) {
			if slot != parent.SlotNumber() {
				return nil, false
			}
			if string(hash) != string(ebHash.Bytes()) {
				return nil, false
			}
			return nil, available.Load()
		},
		EndorserBlockWaitSlots:     leiosWaitTestWaitSlots,
		LeiosApplyEndorserBlockTxs: false,
	}
	ls := &LedgerState{config: cfg}
	withLeiosWaitTestSlotLength(ls)

	arrival := leiosWaitTestWindow / 4
	timer := time.AfterFunc(arrival, func() { available.Store(true) })
	defer timer.Stop()

	start := time.Now()
	require.NoError(t, ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{parent, certifier},
	))
	elapsed := time.Since(start)
	require.GreaterOrEqual(
		t,
		elapsed,
		arrival,
		"mandatory certified closure must be waited for, not skipped",
	)
	require.Less(t, elapsed, leiosWaitTestWindow)
}

// TestEnsureReferencedEndorserBlocksSharesOneWindowAcrossMissingBlocks is the
// serial-stacking regression. The per-endorser-block waits are independent --
// none observes another's result -- so running them back to back charged the
// ledger pipeline one full diffusion window per missing endorser block. A
// batch referencing k missing endorser blocks cost k windows, which is the
// long tail of the measured apply stalls. They must share one window.
//
// The CIP-conformant path is used because every reference there is read at
// apply time, so all three stay blocking and only the concurrency changes.
func TestEnsureReferencedEndorserBlocksSharesOneWindowAcrossMissingBlocks(
	t *testing.T,
) {
	const missing = 3
	blocks := make([]gledger.Block, 0, missing)
	for i := range missing {
		blocks = append(blocks, leiosWaitTestAnnouncingBlock(
			t,
			uint64(i+1),
			uint64(100+i),
			lcommon.NewBlake2b256(leiosTestHash(byte(0xB0+i))),
		))
	}
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, false
		},
		EndorserBlockWaitSlots: leiosWaitTestWaitSlots,
		// CIP-conformant path: every announcement is read at apply time.
		LeiosApplyEndorserBlockTxs: true,
	}
	ls := &LedgerState{config: cfg}
	withLeiosWaitTestSlotLength(ls)

	start := time.Now()
	require.NoError(t, ls.ensureReferencedEndorserBlocks(
		t.Context(),
		blocks,
	))
	elapsed := time.Since(start)
	require.GreaterOrEqual(
		t,
		elapsed,
		leiosWaitTestWindow,
		"the window must still be honoured for references application reads",
	)
	require.Less(
		t,
		elapsed,
		2*leiosWaitTestWindow,
		"per-endorser-block waits stacked serially instead of sharing a window",
	)
}

// TestSplitTipWaitByApplyDependency pins the apply-path contract itself,
// independently of timing: on the CIP path every reference is read at apply
// time and stays blocking; on the Musashi path only the mandatory certified
// closures are read, and a block's own announcement is demoted to background
// prefetch.
func TestSplitTipWaitByApplyDependency(t *testing.T) {
	certified := leiosEbRef{
		slot: 100,
		hash: lcommon.NewBlake2b256(leiosTestHash(0xC1)),
	}
	announced := leiosEbRef{
		slot: 140,
		hash: lcommon.NewBlake2b256(leiosTestHash(0xC2)),
	}
	tipWait := []leiosEbRef{certified, announced}

	blocking, prefetch := splitTipWaitByApplyDependency(
		tipWait,
		[]leiosEbRef{certified},
		true,
	)
	require.Equal(t, []leiosEbRef{certified}, blocking)
	require.Equal(t, []leiosEbRef{announced}, prefetch)

	blocking, prefetch = splitTipWaitByApplyDependency(tipWait, nil, false)
	require.Equal(t, tipWait, blocking)
	require.Empty(t, prefetch)
}

// TestAwaitEndorserBlocksFetchesUpFront pins the second half of the wait fix:
// a reference the batch does block on has its by-point fetch dispatched up
// front, concurrently with the wait, instead of the wait polling passively and
// only falling back to a fetch once the whole diffusion window had already been
// spent. It also pins that an already-available reference costs neither a wait
// nor a fetch.
func TestAwaitEndorserBlocksFetchesUpFront(t *testing.T) {
	var cached atomic.Bool
	var fetches atomic.Int64
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, cached.Load()
		},
		EndorserBlockFetcher: func(
			_ context.Context,
			_ uint64,
			_ []byte,
		) error {
			fetches.Add(1)
			// The fetch is what makes the endorser block available; nothing
			// else will deliver it during this test.
			cached.Store(true)
			return nil
		},
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)
	ref := leiosEbRef{
		slot: 100,
		hash: lcommon.NewBlake2b256(leiosTestHash(0xD1)),
	}

	start := time.Now()
	ls.awaitEndorserBlocks(
		t.Context(),
		[]leiosEbRef{ref},
		leiosWaitTestWindow,
		time.Millisecond,
	)
	require.Less(
		t,
		time.Since(start),
		leiosWaitTestWindow/2,
		"wait did not dispatch the by-point fetch until the window expired",
	)
	require.Equal(t, int64(1), fetches.Load())

	// Already cached: no second fetch, no wait.
	start = time.Now()
	ls.awaitEndorserBlocks(
		t.Context(),
		[]leiosEbRef{ref},
		leiosWaitTestWindow,
		time.Millisecond,
	)
	require.Less(t, time.Since(start), leiosWaitTestWindow/2)
	require.Equal(t, int64(1), fetches.Load())
}
