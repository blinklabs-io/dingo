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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

type leiosPrefixFixture struct {
	ls             *LedgerState
	blocks         []gledger.Block
	epoch          models.Epoch
	ctx            context.Context
	cancel         context.CancelFunc
	release        chan struct{}
	started        chan struct{}
	frontAvailable atomic.Bool
	lateAvailable  atomic.Bool
	certCalls      atomic.Int32
	fetchCertCalls atomic.Int32
}

func newLeiosPrefixFixture(t *testing.T) *leiosPrefixFixture {
	t.Helper()
	f := &leiosPrefixFixture{
		release: make(chan struct{}),
		started: make(chan struct{}, 2),
	}
	f.ctx, f.cancel = context.WithCancel(t.Context())
	parent, certifier, _ := leiosTestCertifiedBlockPairAt(t, 0xE1, 20)
	// Reuse the hash at a different slot to exercise occurrence-bound reads.
	lateParent, lateCertifier, _ := leiosTestCertifiedBlockPairAt(t, 0xE1, 100)
	// Hashes use stored header bytes; distinct slots need distinct encodings.
	for _, pair := range [][2]*dijkstra.DijkstraBlock{{parent, certifier}, {lateParent, lateCertifier}} {
		for i, block := range pair {
			if i == 1 {
				block.BlockHeader.Body.PrevHash = pair[0].Hash()
			}
			block.BlockHeader.SetCbor(nil)
			raw, err := block.BlockHeader.MarshalCBOR()
			require.NoError(t, err)
			block.BlockHeader.SetCbor(raw)
		}
	}
	require.NotEqual(t, parent.Hash(), lateParent.Hash())
	f.blocks = []gledger.Block{parent, certifier, lateParent, lateCertifier}
	f.ls = &LedgerState{}
	leiosTestEnableCertifiedBlock(t, f.ls, certifier)
	leiosTestEnableCertifiedBlock(t, f.ls, lateCertifier)
	snapshot := leiosTestCertificationSnapshot()
	f.epoch = snapshot.currentEpoch
	f.ls.currentEpoch = snapshot.currentEpoch
	f.ls.currentEra = snapshot.currentEra
	f.ls.currentPParams = snapshot.currentPParams
	f.ls.chainsyncState = SyncingChainsyncState
	f.ls.config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	f.ls.config.EndorserBlockWaitSlots = leiosWaitTestWaitSlots
	f.ls.config.EndorserBlockProvider = func(_ []byte, slot uint64) ([]cbor.RawMessage, bool) {
		switch slot {
		case 20:
			return nil, f.frontAvailable.Load()
		case 100:
			return nil, f.lateAvailable.Load()
		default:
			return nil, false
		}
	}
	f.ls.config.ValidateLeiosCertificate = func(uint64, []byte, []byte, []byte) error {
		f.certCalls.Add(1)
		return nil
	}
	f.ls.config.EndorserBlockFetcher = func(ctx context.Context, slot uint64, _ []byte) error {
		f.fetchCertCalls.CompareAndSwap(0, f.certCalls.Load())
		select {
		case f.started <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.release:
			if slot == 20 {
				f.frontAvailable.Store(true)
				announcements := map[string]leiosEbRef{
					string(parent.Hash().Bytes()):     {slot: 20},
					string(lateParent.Hash().Bytes()): {slot: 100},
				}
				for _, block := range f.blocks {
					require.NoError(
						t,
						f.ls.validateDijkstraLeiosCertificate(
							f.ctx,
							block,
							announcements,
						),
					)
				}
				f.certCalls.Store(0)
			} else {
				f.lateAvailable.Store(true)
			}
			return nil
		}
	}
	f.ls.leiosBackfill = newLeiosBackfiller(f.ls.config)
	withLeiosWaitTestSlotLength(t, f.ls)
	f.ls.slotClock = NewSlotClock(
		newMockSlotTimeProvider(
			time.Now().Add(-24*time.Hour),
			time.Second,
			200,
		),
		DefaultSlotClockConfig(),
	)
	f.frontAvailable.Store(true)
	t.Cleanup(func() {
		f.cancel()
		testutil.WaitForCondition(t, func() bool {
			clear := true
			f.ls.leiosBackfill.inflight.Range(func(_, _ any) bool {
				clear = false
				return false
			})
			return clear
		}, testutil.AsyncWait, "prefetch goroutines stop")
	})
	return f
}

func TestLeiosApplyPrefixContinuesWhileLaterClosureFetches(t *testing.T) {
	t.Parallel()
	f := newLeiosPrefixFixture(t)
	type result struct {
		ready int
		err   error
	}
	finished := make(chan result, 1)
	go func() {
		_, ready, err := f.ls.prepareBlockApplyPrefixSnapshot(
			f.ctx,
			f.blocks,
			f.epoch,
			false,
			true,
		)
		finished <- result{ready, err}
	}()
	testutil.RequireReceive(
		t,
		f.started,
		testutil.AsyncWait,
		"later closure fetch starts",
	)
	got := testutil.RequireReceive(
		t,
		finished,
		testutil.AsyncWait,
		"apply gate returns",
	)
	require.NoError(t, got.err)
	require.Equal(
		t,
		3,
		got.ready,
		"the ready announcement may apply, but its uncached certifier must wait",
	)
	require.False(t, f.lateAvailable.Load())
	require.Equal(
		t,
		int32(2),
		f.fetchCertCalls.Load(),
		"both certificates must verify before prefetch starts",
	)
	require.Nil(
		t,
		f.ls.leiosApplyAnnouncer,
		"a historical prefix must not become a live announcement",
	)

	close(f.release)
	testutil.WaitForCondition(
		t,
		f.lateAvailable.Load,
		testutil.AsyncWait,
		"later closure becomes available",
	)
	_, ready, err := f.ls.prepareBlockApplyPrefixSnapshot(
		f.ctx,
		f.blocks,
		f.epoch,
		false,
		true,
	)
	require.NoError(t, err)
	require.Len(
		t,
		f.blocks,
		ready,
		"a fully cached chunk must keep the existing transaction grouping",
	)
}

func TestLeiosApplyPrefixRejectsCanceledReadyPrefix(t *testing.T) {
	t.Parallel()
	for _, duringAvailability := range []bool{false, true} {
		t.Run(fmt.Sprint(duringAvailability), func(t *testing.T) {
			t.Parallel()
			f := newLeiosPrefixFixture(t)
			if duringAvailability {
				provider := f.ls.config.EndorserBlockProvider
				f.ls.config.EndorserBlockProvider = func(hash []byte, slot uint64) ([]cbor.RawMessage, bool) {
					if slot == 100 {
						f.cancel()
					}
					return provider(hash, slot)
				}
			} else {
				f.cancel()
			}
			_, ready, err := f.ls.prepareBlockApplyPrefixSnapshot(
				f.ctx, f.blocks, f.epoch, false, true,
			)
			require.ErrorIs(t, err, context.Canceled,
				"a canceled apply must not return a successful ready prefix")
			require.ErrorIs(t, err, errCertifiedEndorserBlockUnavailable)
			require.Zero(t, ready)
		})
	}
}

func TestLeiosApplyPrefixPreservesFullGate(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"live", "disabled", "zero_wait", "small_prefix"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newLeiosPrefixFixture(t)
			allow := true
			switch mode {
			case "live":
				f.ls.slotClock = nil
			case "disabled":
				allow = false
			case "zero_wait":
				f.ls.config.EndorserBlockWaitSlots = 0
			case "small_prefix":
				f.frontAvailable.Store(false)
			}
			finished := make(chan error, 1)
			go func() {
				_, _, err := f.ls.prepareBlockApplyPrefixSnapshot(
					f.ctx,
					f.blocks,
					f.epoch,
					false,
					allow,
				)
				finished <- err
			}()
			testutil.RequireReceive(
				t,
				f.started,
				testutil.AsyncWait,
				"later closure fetch starts",
			)
			f.cancel()
			require.ErrorIs(
				t,
				testutil.RequireReceive(
					t,
					finished,
					testutil.AsyncWait,
					"apply gate returns",
				),
				errCertifiedEndorserBlockUnavailable,
			)
		})
	}
}

func TestLeiosApplyPrefixRejectsLaterCertificateBeforeFetch(t *testing.T) {
	t.Parallel()
	f := newLeiosPrefixFixture(t)
	lateParent := f.blocks[2].Hash().Bytes()
	f.ls.config.ValidateLeiosCertificate = func(_ uint64, parent, _, _ []byte) error {
		if bytes.Equal(parent, lateParent) {
			return ErrLeiosInvalidCertificate
		}
		return nil
	}
	_, ready, err := f.ls.prepareBlockApplyPrefixSnapshot(
		f.ctx,
		f.blocks,
		f.epoch,
		false,
		true,
	)
	require.ErrorIs(t, err, ErrLeiosInvalidCertificate)
	require.Zero(t, ready)
	select {
	case <-f.started:
		t.Fatal(
			"an invalid later certificate must prevent every certified fetch",
		)
	default:
	}
}

func newLeiosPrefixApplyFixture(
	t *testing.T,
) (*leiosPrefixFixture, *leiosCertApplyFixture, []gledger.Block) {
	t.Helper()
	f := newLeiosPrefixFixture(t)
	ledger := newLeiosCertApplyFixture(t)
	ls := ledger.ls
	ls.currentEpoch.SlotLength = 1000
	ls.epochCache = []models.Epoch{ls.currentEpoch}
	ls.config.EndorserBlockProvider = f.ls.config.EndorserBlockProvider
	ls.config.EndorserBlockFetcher = f.ls.config.EndorserBlockFetcher
	ls.config.ValidateLeiosCertificate = f.ls.config.ValidateLeiosCertificate
	ls.config.EndorserBlockWaitSlots = f.ls.config.EndorserBlockWaitSlots
	ls.leiosBackfill = f.ls.leiosBackfill
	ls.timeConverter = f.ls.timeConverter
	ls.timeConverterOnce.Do(func() {})
	ls.slotClock = f.ls.slotClock
	ls.publishSnapshotsLocked()
	ledger.storeOriginAndBlock(false)
	blocks := make([]gledger.Block, 0, 4)
	previous := ledger.originHash
	for i, slot := range []uint64{20, 60, 100, 140} {
		block, _ := ledger.block(
			uint64(i+1),
			slot,
			previous,
			i%2 == 1,
			i%2 == 0,
		)
		block.BlockHeader.SetCbor(nil)
		header, err := block.BlockHeader.MarshalCBOR()
		require.NoError(t, err)
		block.BlockHeader.SetCbor(header)
		block.SetCbor(nil)
		raw, err := block.MarshalCBOR()
		require.NoError(t, err)
		block.SetCbor(raw)
		previous = block.Hash().Bytes()
		blocks = append(blocks, block)
		require.NoError(t, ledger.db.BlockCreate(models.Block{
			Slot: slot, Hash: block.Hash().Bytes(), PrevHash: block.PrevHash().Bytes(),
			Number: block.BlockNumber(), Type: gledger.BlockTypeDijkstra, Cbor: raw,
		}, nil))
	}
	return f, ledger, blocks
}

func TestLeiosApplyPrefixCommitsBeforeLaterClosureArrives(t *testing.T) {
	t.Parallel()
	f, ledger, blocks := newLeiosPrefixApplyFixture(t)
	ls := ledger.ls
	results := make(chan readChainResult, 1)
	done := make(chan struct{})
	results <- readChainResult{blocks: blocks, done: done}
	close(results)
	beforeCommit := make(chan []uint64, 2)
	finished := make(chan error, 1)
	go func() {
		finished <- ls.processBlocksFromSource(f.ctx, results, blockSourceOptions{
			trusted: true,
			gated:   true,
			beforeCommit: func(_ ledgerdb.Txn, applied []gledger.Block) error {
				if len(applied) > 0 {
					slots := make([]uint64, len(applied))
					for i, block := range applied {
						slots[i] = block.SlotNumber()
					}
					beforeCommit <- slots
				}
				return nil
			},
		})
	}()
	select {
	case slots := <-beforeCommit:
		require.Equal(t, []uint64{20, 60, 100}, slots)
	case err := <-finished:
		t.Fatalf("block application ended before its ready prefix: %v", err)
	case <-time.After(testutil.AsyncWait):
		t.Fatal("ready prefix never reached the write transaction")
	}
	testutil.WaitForCondition(
		t,
		func() bool { return ls.Tip().Point.Slot == 100 },
		testutil.AsyncWait,
		"ready prefix commits",
	)
	tip, err := ledger.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(100), tip.Point.Slot)
	require.False(t, f.lateAvailable.Load())
	select {
	case <-done:
		t.Fatal("the reader result was released before its deferred suffix")
	default:
	}
	close(f.release)
	require.NoError(
		t,
		testutil.RequireReceive(
			t,
			finished,
			testutil.AsyncWait,
			"deferred suffix applies",
		),
	)
	require.Equal(t, uint64(140), ls.Tip().Point.Slot)
	tip, err = ledger.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(140), tip.Point.Slot)
	select {
	case <-done:
	default:
		t.Fatal("completed reader result was not released")
	}
}

func TestLeiosApplyPrefixPreservesSourceCommitBoundaries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		gated       bool
		load        bool
		wantApplied int32
	}{
		{name: "chain_sync", gated: true, wantApplied: 3},
		{name: "trusted_batch"},
		{name: "immutable_load", load: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, ledger, blocks := newLeiosPrefixApplyFixture(t)
			ls := ledger.ls
			initialTip, err := ledger.db.GetTip(nil)
			require.NoError(t, err)
			ls.leiosBackfill.fetch = func(context.Context, uint64, []byte) error {
				return errors.New("test certified closure unavailable")
			}
			var applied atomic.Int32
			if tc.load {
				ls.config.TrustedReplay = true
				batches := make(chan []gledger.Block, 1)
				batches <- blocks
				close(batches)
				err = ls.ProcessTrustedBlockBatches(f.ctx, batches)
			} else {
				results := make(chan readChainResult, 1)
				results <- readChainResult{blocks: blocks}
				close(results)
				err = ls.processBlocksFromSource(f.ctx, results, blockSourceOptions{
					trusted:   true,
					gated:     tc.gated,
					chunkSize: len(blocks),
					beforeCommit: func(_ ledgerdb.Txn, blocks []gledger.Block) error {
						applied.Add(int32(len(blocks)))
						return nil
					},
				})
			}
			require.ErrorIs(t, err, errCertifiedEndorserBlockUnavailable)
			if !tc.load {
				require.Equal(
					t,
					tc.wantApplied,
					applied.Load(),
					"source commit boundary",
				)
			}
			tip, err := ledger.db.GetTip(nil)
			require.NoError(t, err)
			if tc.gated {
				require.Equal(t, uint64(100), tip.Point.Slot)
			} else {
				require.Equal(t, initialTip.Point, tip.Point, "trusted import must not commit a partial chunk")
			}
		})
	}
}
