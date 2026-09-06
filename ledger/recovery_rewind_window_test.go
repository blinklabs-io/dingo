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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// newTestShelleyGenesisCfgWithK is newTestShelleyGenesisCfg with a caller
// chosen security parameter, so a windowed rewind can be exercised with a
// window small enough to need several steps over a short test chain.
func newTestShelleyGenesisCfgWithK(
	t testing.TB,
	k int,
) *cardano.CardanoNodeConfig {
	t.Helper()
	shelleyGenesisJSON := fmt.Sprintf(`{
		"activeSlotsCoeff": 0.05,
		"securityParam": %d,
		"slotsPerKESPeriod": 129600,
		"systemStart": "2022-10-25T00:00:00Z"
	}`, k)
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)
	return cfg
}

// seedTestChain appends count linked blocks to the primary chain, spaced ten
// slots apart so an appended block can always claim tip.Slot+1 without
// colliding with a block that is already stored.
func seedTestChain(
	t testing.TB,
	pc *chain.Chain,
	prefix string,
	count int,
) []chain.RawBlock {
	t.Helper()
	raw := make([]chain.RawBlock, 0, count)
	var prev []byte
	for i := 1; i <= count; i++ {
		h := testHashBytes(fmt.Sprintf("%s-%d", prefix, i))
		raw = append(raw, chain.RawBlock{
			Slot:        uint64(i * 10), //nolint:gosec
			Hash:        h,
			BlockNumber: uint64(i), //nolint:gosec
			Type:        1,
			PrevHash:    prev,
			Cbor:        []byte{0x80},
		})
		prev = h
	}
	require.NoError(t, pc.AddRawBlocks(raw))
	return raw
}

// TestWindowedRewindConvergesWhilePrimaryChainExtends pins the descent
// schedule in rollbackPrimaryChainInSecurityParamWindows against a primary
// chain that keeps growing underneath it, which is what a recovery rewind
// races with on a syncing node: blockfetch appends to the chain under
// chainsyncMutex while the ledger pipeline runs recovery under
// transactionEventMutex, so nothing serialises the two.
//
// The function used to read the chain tip once and then derive every
// intermediate target as snapshot-n*window. One block appended after that
// snapshot makes the next target window+1 below the chain's live tip, and
// Chain.Rollback refuses it as exceeding K. The whole rewind then fails, the
// pipeline restarts, and recovery recomputes the same doomed schedule against
// a tip that has grown further -- issue #3889, where that loop ran for nine
// hours and 1150 restarts without the chain ever being truncated.
//
// Each step must therefore be derived from the chain's live tip, so it is a
// legal K-bounded rollback by construction no matter how far the chain has
// advanced since the rewind began.
func TestWindowedRewindConvergesWhilePrimaryChainExtends(t *testing.T) {
	const (
		securityParam = 8
		blockCount    = 240
	)

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: securityParam}),
	)
	pc := cm.PrimaryChain()
	raw := seedTestChain(t, pc, "windowed-race", blockCount)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfgWithK(t, securityParam),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	// SecurityParam() is era-derived; without this the Byron fallback window
	// dwarfs the chain and no intermediate step is ever taken.
	ls.currentEra = eras.ShelleyEraDesc
	require.Equal(
		t,
		securityParam,
		ls.SecurityParam(),
		"rewind window must match the chain manager's k",
	)

	// Extend the primary chain by one block every time the rewind moves the
	// tip, mimicking blockfetch appending while recovery descends. One block
	// per step is slower than the window each step covers, so a rewind that
	// re-reads the live tip still converges.
	stop := make(chan struct{})
	var appender sync.WaitGroup
	appender.Add(1)
	go func() {
		defer appender.Done()
		lastPoint := pc.Tip().Point
		for seq := 0; ; seq++ {
			select {
			case <-stop:
				return
			default:
			}
			tip := pc.Tip()
			if tip.Point.Slot == lastPoint.Slot &&
				bytes.Equal(tip.Point.Hash, lastPoint.Hash) {
				continue
			}
			next := chain.RawBlock{
				Slot: tip.Point.Slot + 1,
				Hash: testHashBytes(
					fmt.Sprintf("windowed-race-append-%d", seq),
				),
				BlockNumber: tip.BlockNumber + 1,
				Type:        1,
				PrevHash:    tip.Point.Hash,
				Cbor:        []byte{0x80},
			}
			if err := pc.AddRawBlocks([]chain.RawBlock{next}); err != nil {
				// The tip moved again between the read and the add;
				// re-read and try the next one.
				continue
			}
			lastPoint = ocommon.NewPoint(next.Slot, next.Hash)
		}
	}()

	target := ocommon.NewPoint(raw[0].Slot, raw[0].Hash)
	rewindErr := ls.rollbackPrimaryChainInSecurityParamWindows(target)
	close(stop)
	appender.Wait()

	require.NotErrorIs(
		t,
		rewindErr,
		chain.ErrRollbackExceedsSecurityParam,
		"a windowed step must stay within K of the chain's live tip",
	)
	require.NoError(t, rewindErr)
}

// TestDeterministicTxRecoveryHaltsOnUnreachableRewind pins the second half of
// issue #3889: a recovery rewind the chain refuses as exceeding K is not a
// transient failure, so repeating it at an applied tip that never advances
// must become terminal instead of restarting the pipeline forever.
//
// recoverFromDeterministicTxValidationError used to return the refusal as a
// plain error. ledgerProcessBlocks treats anything that is not
// errHaltLedgerPipeline as retryable, so the node logged "block processing
// failed, restarting pipeline", waited out the backoff, and recomputed the
// same impossible rewind -- 1150 times over nine hours in the report, with the
// stuck-pipeline watchdog correctly announcing that the failure was
// deterministic while the node kept retrying anyway.
func TestDeterministicTxRecoveryHaltsOnUnreachableRewind(t *testing.T) {
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	// A chain k of 2 against the ledger's much larger window means the
	// single rewind step is refused on fork depth, which is the refusal the
	// recovery path has to classify.
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))
	raw := seedTestChain(t, cm.PrimaryChain(), "halt-on-unreachable", 5)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	require.Greater(t, ls.SecurityParam(), 2, "ledger window must exceed k")

	// The applied tip sits at the first block and the rejected block is the
	// chain tip, so the rewind target is four blocks below a chain whose k
	// is 2 and every rewind to it is refused.
	ls.currentTip.Point = ocommon.NewPoint(raw[0].Slot, raw[0].Hash)
	validationErr := &txValidationError{
		BlockPoint: ocommon.NewPoint(raw[4].Slot, raw[4].Hash),
		TxHash:     testHashBytes("halt-on-unreachable-tx"),
		Cause: conway.PlutusScriptFailedError{
			Err: errors.New("error explicitly called"),
		},
	}
	require.True(t, isDeterministicTxValidationError(validationErr.Cause))

	var lastErr error
	halted := false
	// Well past any bounded retry budget: an unreachable rewind still being
	// retried after this many attempts is the nine-hour loop.
	for range 32 {
		_, lastErr = ls.recoverFromDeterministicTxValidationError(
			validationErr,
		)
		require.Error(t, lastErr)
		require.ErrorIs(t, lastErr, chain.ErrRollbackExceedsSecurityParam)
		if errors.Is(lastErr, errHaltLedgerPipeline) {
			halted = true
			break
		}
	}
	require.True(
		t,
		halted,
		"a recovery rewind refused as exceeding K must stop the pipeline "+
			"rather than be retried forever: %v",
		lastErr,
	)
}

// TestRecoveryRewindHaltBudgetResetsOnTipProgress pins the other side of that
// budget. The halt is for a rewind that stays unreachable at an applied tip
// that never moves; once the ledger advances past that tip the situation is a
// different one and must start with a fresh budget rather than inherit a tally
// that has nothing to do with it.
func TestRecoveryRewindHaltBudgetResetsOnTipProgress(t *testing.T) {
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))
	raw := seedTestChain(t, cm.PrimaryChain(), "halt-budget-reset", 5)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	target := ocommon.NewPoint(raw[0].Slot, raw[0].Hash)
	for range maxRecoveryRewindRejections {
		rewindErr := ls.rewindPrimaryChainForRecovery(target)
		require.ErrorIs(
			t,
			rewindErr,
			chain.ErrRollbackExceedsSecurityParam,
		)
		require.NotErrorIs(t, rewindErr, errHaltLedgerPipeline)
	}
	// Forward progress past the tip the refusals could not cross clears the
	// tally, so the budget starts over instead of halting on the next one.
	ls.resetRecoveryRewindRejections(raw[3].Slot)
	rewindErr := ls.rewindPrimaryChainForRecovery(target)
	require.ErrorIs(t, rewindErr, chain.ErrRollbackExceedsSecurityParam)
	require.NotErrorIs(t, rewindErr, errHaltLedgerPipeline)
}

// TestRecoveryRewindHaltsThoughTargetMovesAndDepthGrows pins the shape the
// live reproduction on Preview showed (issue #3889: a replay wedged at slot
// 41098815 for over twenty minutes across 97 rejection attempts on one
// transaction).
//
// Two properties of that run decide whether a fix works:
//
//   - The rewind target is recomputed on every attempt and differs every time
//     -- intermediate points 1768501, 1774034, 1773427, 1779454, 1782037,
//     1769017 -- all beyond K. So the terminal condition cannot key on one
//     target that keeps failing; there is no such target. It keys on the
//     applied ledger tip, which is the thing that is not moving.
//   - The depth that must be rolled back grows monotonically, because the peer
//     keeps extending the fork while the local tip is pinned
//     (fork_path_headers 2462, 5031, 7615; fork_depth 5010 then 6020). A
//     rewind already beyond K never comes back into range on its own, which is
//     what makes the loop unrecoverable rather than merely slow.
//
// The chain is therefore extended between attempts, so every attempt computes
// a different step target against a larger gap, and the halt must still
// arrive.
func TestRecoveryRewindHaltsThoughTargetMovesAndDepthGrows(t *testing.T) {
	const (
		chainK        = 4
		ledgerWindow  = 8
		startingChain = 40
		growthPerTry  = 5
		maxAttempts   = 16
	)

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	// The chain enforces a smaller k than the ledger's rewind window, so every
	// step the descent computes is refused wherever the tip has moved to.
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: chainK}),
	)
	pc := cm.PrimaryChain()
	raw := seedTestChain(t, pc, "moving-target", startingChain)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfgWithK(t, ledgerWindow),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	ls.currentEra = eras.ShelleyEraDesc
	require.Equal(t, ledgerWindow, ls.SecurityParam())

	// The applied ledger tip stays at the first block for the whole test:
	// that is the "tip pinned at slot 41098815" half of the report.
	appliedTip := ocommon.NewPoint(raw[0].Slot, raw[0].Hash)
	ls.currentTip.Point = appliedTip
	validationErr := &txValidationError{
		BlockPoint: ocommon.NewPoint(
			raw[startingChain-1].Slot,
			raw[startingChain-1].Hash,
		),
		TxHash: testHashBytes("moving-target-tx"),
		Cause: conway.PlutusScriptFailedError{
			Err: errors.New("error explicitly called"),
		},
	}
	require.True(t, isDeterministicTxValidationError(validationErr.Cause))

	seenTargets := map[string]struct{}{}
	var (
		chainTips []uint64
		halted    bool
		attempts  int
		lastErr   error
	)
	for range maxAttempts {
		attempts++
		chainTips = append(chainTips, pc.Tip().Point.Slot)
		_, lastErr = ls.recoverFromDeterministicTxValidationError(
			validationErr,
		)
		require.ErrorIs(t, lastErr, chain.ErrRollbackExceedsSecurityParam)
		seenTargets[lastErr.Error()] = struct{}{}
		if errors.Is(lastErr, errHaltLedgerPipeline) {
			halted = true
			break
		}
		// The peer keeps serving the fork while the ledger tip is stuck, so
		// the next attempt faces a deeper rollback than this one did.
		grow := make([]chain.RawBlock, 0, growthPerTry)
		prev := pc.Tip()
		for i := range growthPerTry {
			h := testHashBytes(
				fmt.Sprintf("moving-target-grow-%d-%d", attempts, i),
			)
			grow = append(grow, chain.RawBlock{
				Slot:        prev.Point.Slot + 10,
				Hash:        h,
				BlockNumber: prev.BlockNumber + 1,
				Type:        1,
				PrevHash:    prev.Point.Hash,
				Cbor:        []byte{0x80},
			})
			prev = ochainsync.Tip{
				Point:       ocommon.NewPoint(prev.Point.Slot+10, h),
				BlockNumber: prev.BlockNumber + 1,
			}
		}
		require.NoError(t, pc.AddRawBlocks(grow))
	}

	require.True(
		t,
		halted,
		"a rewind that stays beyond K must stop the pipeline even though "+
			"every attempt computes a different target: %v",
		lastErr,
	)
	require.Greater(
		t,
		len(seenTargets),
		1,
		"the test must exercise a target that moves between attempts",
	)
	require.Greater(
		t,
		chainTips[len(chainTips)-1],
		chainTips[0],
		"the fork must extend while the applied ledger tip stays pinned",
	)
	require.True(
		t,
		slices.IsSorted(chainTips),
		"required rollback depth must only grow across attempts: %v",
		chainTips,
	)
}

// TestWindowedRewindRefusesRecoveryTargetTheChainDoesNotHold pins that the
// entry check on rollbackPrimaryChainInSecurityParamWindows establishes
// primary-chain membership, not store presence.
//
// The descent commits each step as it goes, so a target it can never reach
// has to be refused before the first truncation. The check used to be a
// database.BlockByPoint lookup, which a target the store still holds but the
// chain has abandoned passes -- the retained-index shape rollbackPointBlock
// documents. The descent then truncated every intermediate step and
// Chain.Rollback refused the final one with ErrRollbackPointNotOnChain,
// leaving the chain shortened for a rewind that never happened.
//
// The target below is written straight into the block store at an index above
// the chain tip, so it is present by point and absent from the chain: the
// store lookup accepts it and the chain's own membership check does not.
func TestWindowedRewindRefusesRecoveryTargetTheChainDoesNotHold(t *testing.T) {
	const (
		securityParam = 8
		blockCount    = 60
	)

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: securityParam}),
	)
	pc := cm.PrimaryChain()
	raw := seedTestChain(t, pc, "target-not-on-chain", blockCount)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfgWithK(t, securityParam),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	ls.currentEra = eras.ShelleyEraDesc

	// A block the store holds at an index the chain does not: its slot falls
	// inside the chain's span, so the descent would start, and its index sits
	// above the chain tip, so no chain block occupies it.
	orphan := models.Block{
		ID:       blockCount + 5,
		Slot:     raw[0].Slot + 5,
		Hash:     testHashBytes("target-not-on-chain-orphan"),
		Number:   raw[0].BlockNumber,
		Type:     1,
		PrevHash: raw[0].Hash,
		Cbor:     []byte{0x80},
	}
	require.NoError(t, db.BlockCreate(orphan, nil))
	target := ocommon.NewPoint(orphan.Slot, orphan.Hash)
	_, err = database.BlockByPoint(db, target)
	require.NoError(t, err, "the store must hold the target for this to test anything")

	tipBefore := pc.Tip()
	err = ls.rollbackPrimaryChainInSecurityParamWindows(target)
	require.ErrorIs(t, err, chain.ErrRollbackPointNotOnChain)
	require.Equal(
		t,
		tipBefore,
		pc.Tip(),
		"a target the chain does not hold must be refused before any step is committed",
	)
}

// TestWindowedRewindRefusesSlotZeroTargetTheStoreDoesNotHold covers the one
// target shape Chain.ValidateRollback cannot speak for.
//
// ValidateRollback reads every slot-zero point as origin and skips its
// membership check there, and Chain.Rollback does the same: it truncates to
// index zero and sets currentTip to the point it was given. A slot-zero point
// carrying a hash would therefore pass the entry check and take the descent
// all the way down, leaving the chain empty and its tip naming a block the
// store need not hold, so the entry check keeps the store lookup for it.
func TestWindowedRewindRefusesSlotZeroTargetTheStoreDoesNotHold(t *testing.T) {
	const (
		securityParam = 8
		blockCount    = 30
	)

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: securityParam}),
	)
	pc := cm.PrimaryChain()
	seedTestChain(t, pc, "slot-zero-target", blockCount)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfgWithK(t, securityParam),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	ls.currentEra = eras.ShelleyEraDesc

	target := ocommon.NewPoint(0, testHashBytes("slot-zero-target-absent"))
	tipBefore := pc.Tip()
	err = ls.rollbackPrimaryChainInSecurityParamWindows(target)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
	require.Equal(
		t,
		tipBefore,
		pc.Tip(),
		"a slot-zero target the store does not hold must not truncate the chain",
	)
}
