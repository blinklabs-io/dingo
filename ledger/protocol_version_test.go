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
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newBabbageQuorum1Cfg builds a CardanoNodeConfig with every era transition
// left at its default TriggerAtVersion (no TestXHardForkAtEpoch override, no
// ExperimentalHardForksEnabled), matching a real Preview/mainnet-shaped
// config for a classic, quorum-based hard fork. epochLength=100,
// securityParam=1, activeSlotsCoeff=0.1 give a safe zone of
// ceil(3*1/0.1)=30 slots, small enough that the ordinary tip-anchored safe
// zone lands exactly at the era boundary with zero margin past it -- the
// same shape as the real Babbage/Conway boundary computed against the
// live-incident numbers (55814400, 522 slots short of the failing
// transaction's TTL), just scaled down for a fast, readable test.
func newBabbageQuorum1Cfg(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	return newBabbageQuorumCfg(t, 1, "0.1")
}

// newBabbageQuorumCfg is newBabbageQuorum1Cfg with the security parameter
// and active slot coefficient chosen by the caller, which sets the stability
// window (3k/f) and with it the voting deadline.
func newBabbageQuorumCfg(
	t *testing.T,
	securityParam int,
	activeSlotsCoeff string,
) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, loadByronGenesisForTest(t, cfg, strings.NewReader(`{
		"protocolConsts": {
			"k": 1,
			"protocolMagic": 42
		}
	}`)))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(
		fmt.Sprintf(`{
		"systemStart": "2026-01-01T00:00:00Z",
		"securityParam": %d,
		"activeSlotsCoeff": %s,
		"epochLength": 100,
		"slotLength": 1,
		"updateQuorum": 1
	}`, securityParam, activeSlotsCoeff),
	)))
	return cfg
}

// TestEvaluateProtocolVersionBump_DetectsQuorumMetUpdate is a direct unit
// test of evaluateProtocolVersionBump: a pending protocol-parameter update
// submitted in the current (Babbage) epoch's submission window, from enough
// distinct genesis-key delegates to meet quorum, and bumping the protocol
// major version into Conway, must set transitionInfo to
// TransitionKnown(currentEpoch+1) -- without touching currentEra, the
// snapshot's currentPParams, or performing any enactment (no PParams row is
// written for the target epoch; ComputeAndApplyPParamUpdates alone does
// that, at the real rollover).
func TestEvaluateProtocolVersionBump_DetectsQuorumMetUpdate(t *testing.T) {
	t.Parallel()

	cfg := newBabbageQuorum1Cfg(t)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	// A single genesis-key delegate proposes bumping the protocol major
	// version from Babbage (8) to Conway (9), submitted in epoch 0 (the
	// submission epoch for enactment at the epoch 0->1 boundary). Shelley
	// genesis updateQuorum=1, so this one proposal already meets quorum.
	updateCbor, err := cbor.Encode(map[uint64]any{
		14: lcommon.ProtocolParametersProtocolVersion{Major: 9, Minor: 0},
	})
	require.NoError(t, err)
	require.NoError(t, db.SetPParamUpdate(
		[]byte{0xaa}, // genesis key delegate hash
		updateCbor,
		10, // slot within epoch 0
		0,  // submission epoch
		nil,
	))

	pparams := &babbage.BabbageProtocolParameters{
		ProtocolMajor: 8,
		ProtocolMinor: 0,
		// Block sizes the votedFuturePParams guard accepts.
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
	}
	// The voting deadline is slot 40 (100 - 2*30); k=1 block lies past it.
	testChain, tips := newVersionBumpChain(t, db, 30, 15, 2)
	ls := &LedgerState{
		db:         db,
		chain:      testChain,
		currentTip: tips[1],
		currentEra: eras.BabbageEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			LengthInSlots: 100,
			SlotLength:    1000,
			EraId:         eras.BabbageEraDesc.Id,
		},
		currentPParams: pparams,
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	ls.evaluateProtocolVersionBump()

	require.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State,
		"quorum-met version-bumping update must set TransitionKnown")
	require.Equal(t, uint64(1), ls.transitionInfo.KnownEpoch,
		"the known transition targets the next epoch (the 0->1 boundary)")
	// Peeking must not mutate currentEra or currentPParams: enactment stays
	// exclusively processEpochRollover's job, at the real boundary.
	require.Equal(t, eras.BabbageEraDesc.Id, ls.currentEra.Id)
	require.Equal(t, uint(8), pparams.ProtocolMajor,
		"the peek must not mutate the live currentPParams pointer")
}

// TestEvaluateProtocolVersionBump_NoUpdateStaysUnknown is the negative half:
// with no pending update proposal at all, transitionInfo must stay
// TransitionUnknown.
func TestEvaluateProtocolVersionBump_NoUpdateStaysUnknown(t *testing.T) {
	t.Parallel()

	cfg := newBabbageQuorum1Cfg(t)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	pparams := &babbage.BabbageProtocolParameters{ProtocolMajor: 8}
	ls := &LedgerState{
		db:         db,
		currentEra: eras.BabbageEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			LengthInSlots: 100,
			SlotLength:    1000,
			EraId:         eras.BabbageEraDesc.Id,
		},
		currentPParams: pparams,
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	ls.evaluateProtocolVersionBump()

	require.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"no pending update must leave transitionInfo Unknown")
}

// TestHardForkSummary_ProtocolVersionBumpExtendsHorizonPastBoundary is the
// end-to-end regression test for the live incident: a transaction whose
// validity interval crosses slightly past a classic (pre-Conway,
// quorum-triggered) era boundary must resolve once the quorum-met update is
// detected, reproducing the same shape as the real Babbage/Conway boundary
// verified against Koios (epoch 646 starting exactly at the safe-zone-only
// horizon, 522 slots short of the failing transaction's TTL) -- scaled down
// to a 100-slot epoch and a 30-slot safe zone so the test runs in
// microseconds.
//
// Without evaluateProtocolVersionBump, transitionInfo never leaves
// TransitionUnknown before the boundary, so HardForkSummary's ordinary
// tip-anchored safe zone snaps up to exactly the epoch boundary (slot 100)
// with no margin past it, and a TTL just beyond that boundary (slot 105)
// returns hardfork.ErrPastHorizon -- the literal error the live incident
// reported, for a transaction that is otherwise perfectly canonical.
func TestHardForkSummary_ProtocolVersionBumpExtendsHorizonPastBoundary(
	t *testing.T,
) {
	t.Parallel()

	const (
		ttlSlot = uint64(105) // 5 slots into the new (Conway) epoch
		tipSlot = uint64(50)  // mid-epoch-0, well before the boundary
	)

	cfg := newBabbageQuorum1Cfg(t)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	updateCbor, err := cbor.Encode(map[uint64]any{
		14: lcommon.ProtocolParametersProtocolVersion{Major: 9, Minor: 0},
	})
	require.NoError(t, err)
	require.NoError(t, db.SetPParamUpdate(
		[]byte{0xaa}, updateCbor, 10, 0, nil,
	))

	pparams := &babbage.BabbageProtocolParameters{
		ProtocolMajor: 8,
		ProtocolMinor: 0,
		// Block sizes the votedFuturePParams guard accepts.
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
	}
	// Blocks at slots 35 and 50: the tip is k=1 block past the slot-40
	// voting deadline.
	testChain, tips := newVersionBumpChain(t, db, 35, 15, 2)
	require.Equal(t, tipSlot, tips[1].Point.Slot)
	ls := &LedgerState{
		db:    db,
		chain: testChain,
		epochCache: []models.Epoch{{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1000,
			LengthInSlots: 100,
			EraId:         eras.BabbageEraDesc.Id,
		}},
		currentEra: eras.BabbageEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			LengthInSlots: 100,
			SlotLength:    1000,
			EraId:         eras.BabbageEraDesc.Id,
		},
		currentPParams: pparams,
		transitionInfo: hardfork.NewTransitionUnknown(),
		currentTip:     tips[1],
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	// Before the fix is exercised: transitionInfo is still Unknown, so the
	// ordinary safe zone snaps to exactly the boundary and the TTL fails.
	before, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.NotNil(t, before.Eras[len(before.Eras)-1].End)
	assert.Equal(
		t,
		uint64(100),
		before.Eras[len(before.Eras)-1].End.Slot,
		"the ordinary (TransitionUnknown) horizon must land exactly at the "+
			"boundary, reproducing the live incident's zero-margin shortfall",
	)
	_, err = before.SlotToTime(ttlSlot)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon,
		"before detection, the TTL just past the boundary must be rejected "+
			"-- the exact live-incident failure")

	// Run the new evaluator: the quorum-met update is detected and
	// transitionInfo becomes TransitionKnown(1).
	ls.evaluateProtocolVersionBump()
	ls.publishSnapshotsLocked()

	after, err := ls.HardForkSummary()
	require.NoError(t, err)
	_, err = after.SlotToTime(ttlSlot)
	require.NoError(
		t, err,
		"once the quorum-met update is detected, the appended successor "+
			"era must cover the TTL just past the boundary",
	)
}

// newVersionBumpChain adds count connected Babbage blocks, numbered from 1,
// at startSlot and every slotIncrement slots after it. It returns the chain
// and, for each block, the tip a ledger that had applied up to it would hold.
func newVersionBumpChain(
	t *testing.T,
	db *database.Database,
	startSlot, slotIncrement uint64,
	count int,
) (*chain.Chain, []ochainsync.Tip) {
	t.Helper()
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	testChain := cm.PrimaryChain()
	blocks, err := fixtures.GenerateBabbageChain(
		1,
		lcommon.Blake2b256{},
		startSlot,
		slotIncrement,
		count,
	)
	require.NoError(t, err)
	tips := make([]ochainsync.Tip, 0, len(blocks))
	for _, block := range blocks {
		require.NoError(t, testChain.AddBlock(block, nil))
		tips = append(tips, ochainsync.Tip{
			Point: ocommon.NewPoint(
				block.SlotNumber(),
				block.Hash().Bytes(),
			),
			BlockNumber: block.BlockNumber(),
		})
	}
	return testChain, tips
}

// newQuorumMetVersionBumpLedger returns a Babbage ledger in epoch 0 with a
// pending update, already meeting the quorum of 1, that bumps the protocol
// major version into Conway. The caller supplies the chain and tip.
func newQuorumMetVersionBumpLedger(
	t *testing.T,
	cfg *cardano.CardanoNodeConfig,
	db *database.Database,
) *LedgerState {
	t.Helper()
	updateCbor, err := cbor.Encode(map[uint64]any{
		14: lcommon.ProtocolParametersProtocolVersion{Major: 9, Minor: 0},
	})
	require.NoError(t, err)
	require.NoError(t, db.SetPParamUpdate(
		[]byte{0xaa}, updateCbor, 10, 0, nil,
	))
	ls := &LedgerState{
		db:         db,
		currentEra: eras.BabbageEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			LengthInSlots: 100,
			SlotLength:    1000,
			EraId:         eras.BabbageEraDesc.Id,
		},
		currentPParams: &babbage.BabbageProtocolParameters{
			ProtocolMajor:      8,
			MaxBlockBodySize:   65536,
			MaxTxSize:          16384,
			MaxBlockHeaderSize: 1100,
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()
	return ls
}

// A quorum-met proposal read before the voting deadline can still be
// superseded, so it must not make the era end known. With k=1 and a 30-slot
// stability window the deadline is slot 40, and the first block at or past
// it is what makes the transition known.
func TestEvaluateProtocolVersionBump_WaitsForVotingDeadline(t *testing.T) {
	t.Parallel()

	cfg := newBabbageQuorum1Cfg(t)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	ls := newQuorumMetVersionBumpLedger(t, cfg, db)
	testChain, tips := newVersionBumpChain(t, db, 30, 15, 2)
	ls.chain = testChain

	ls.currentTip = tips[0]
	ls.evaluateProtocolVersionBump()
	require.Equal(
		t,
		hardfork.TransitionUnknown,
		ls.transitionInfo.State,
		"a quorum-met proposal before the voting deadline must not make the transition known",
	)

	ls.currentTip = tips[1]
	ls.evaluateProtocolVersionBump()
	require.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)
	require.Equal(t, uint64(1), ls.transitionInfo.KnownEpoch)
}

// The rule counts blocks, not slots: with k=2 and a 30-slot stability window
// the deadline is slot 40, a block exactly at it counts, and the transition
// is known only once two blocks lie at or past it.
func TestEvaluateProtocolVersionBump_RequiresKBlocksPastDeadline(
	t *testing.T,
) {
	t.Parallel()

	cfg := newBabbageQuorumCfg(t, 2, "0.2")
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	ls := newQuorumMetVersionBumpLedger(t, cfg, db)
	// Blocks 1-4 at slots 30, 40, 50 and 60.
	testChain, tips := newVersionBumpChain(t, db, 30, 10, 4)
	ls.chain = testChain

	ls.currentTip = tips[1]
	ls.evaluateProtocolVersionBump()
	require.Equal(
		t,
		hardfork.TransitionUnknown,
		ls.transitionInfo.State,
		"one block at the deadline is fewer than k=2",
	)

	ls.currentTip = tips[2]
	ls.evaluateProtocolVersionBump()
	require.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)
	require.Equal(t, uint64(1), ls.transitionInfo.KnownEpoch)
}

// The failure scenario: a quorum-met bump is replaced by the same genesis key
// with a non-bumping update before the voting deadline. No era end may be
// reported at any point, neither from the early reading nor after the
// deadline.
func TestEvaluateProtocolVersionBump_SupersededBeforeDeadlineNeverKnown(
	t *testing.T,
) {
	t.Parallel()

	cfg := newBabbageQuorum1Cfg(t)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	ls := newQuorumMetVersionBumpLedger(t, cfg, db)
	// Blocks at slots 30 and 45, either side of the slot-40 deadline.
	testChain, tips := newVersionBumpChain(t, db, 30, 15, 2)
	ls.chain = testChain

	ls.currentTip = tips[0]
	ls.evaluateProtocolVersionBump()
	require.Equal(
		t,
		hardfork.TransitionUnknown,
		ls.transitionInfo.State,
		"the early quorum-met bump must not be reported before the deadline",
	)

	nonBumpCbor, err := cbor.Encode(map[uint64]any{0: uint64(44)})
	require.NoError(t, err)
	require.NoError(t, db.SetPParamUpdate(
		[]byte{0xaa}, nonBumpCbor, 35, 0, nil,
	))
	ls.currentTip = tips[1]
	ls.evaluateProtocolVersionBump()
	require.Equal(
		t,
		hardfork.TransitionUnknown,
		ls.transitionInfo.State,
		"the replacement no longer bumps the major version",
	)
}

// Once reported, the transition cannot change on the same chain: a proposal
// made after the voting deadline targets the epoch after next, so it does not
// alter the reading for this boundary, even when the state is re-derived from
// scratch as on a restart.
func TestEvaluateProtocolVersionBump_PostDeadlineProposalKeepsTransition(
	t *testing.T,
) {
	t.Parallel()

	cfg := newBabbageQuorum1Cfg(t)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	ls := newQuorumMetVersionBumpLedger(t, cfg, db)
	// Blocks at slots 30, 45 and 60.
	testChain, tips := newVersionBumpChain(t, db, 30, 15, 3)
	ls.chain = testChain

	ls.currentTip = tips[1]
	ls.evaluateProtocolVersionBump()
	require.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)

	nonBumpCbor, err := cbor.Encode(map[uint64]any{0: uint64(44)})
	require.NoError(t, err)
	require.NoError(t, db.SetPParamUpdate(
		[]byte{0xaa}, nonBumpCbor, 50, 1, nil,
	))
	ls.currentTip = tips[2]
	ls.transitionInfo = hardfork.NewTransitionUnknown()
	ls.evaluateProtocolVersionBump()
	require.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)
	require.Equal(t, uint64(1), ls.transitionInfo.KnownEpoch)
}

// A rollback to before the voting deadline leaves no counted block on the
// surviving chain, so the real rollback path must re-derive Unknown, and a
// different block past the deadline on that chain makes the transition known
// again. The count is read from the chain, so nothing stored can carry the
// abandoned blocks' reading across the rollback.
func TestEvaluateProtocolVersionBump_RollbackRecountsFromSurvivingChain(
	t *testing.T,
) {
	t.Parallel()

	cfg := newBabbageQuorum1Cfg(t)
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))

	// The voting deadline is slot 40 (100 - 2*30).
	preDeadline := chain.RawBlock{
		Slot:        30,
		Hash:        testHashBytes("ppup-pre-deadline"),
		BlockNumber: 1,
		Type:        1,
		Cbor:        []byte{0x80},
	}
	pastDeadline := chain.RawBlock{
		Slot:        45,
		Hash:        testHashBytes("ppup-past-deadline"),
		PrevHash:    preDeadline.Hash,
		BlockNumber: 2,
		Type:        1,
		Cbor:        []byte{0x80},
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(
		[]chain.RawBlock{preDeadline, pastDeadline},
	))
	for _, block := range []chain.RawBlock{preDeadline, pastDeadline} {
		require.NoError(t, db.SetBlockNonce(
			block.Hash, block.Slot, testHashBytes("nonce"), true, nil,
		))
	}

	// The rollback reloads the epoch and its pparams from the database, so
	// both must be stored for the re-evaluation to have a Babbage epoch.
	require.NoError(t, db.SetEpoch(
		0, 0, testHashBytes("epoch-nonce"), nil, nil, nil,
		eras.BabbageEraDesc.Id, 1000, 100, nil,
	))
	pparams := &babbage.BabbageProtocolParameters{
		ProtocolMajor:      8,
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		pparamsCbor, 0, 0, eras.BabbageEraDesc.Id, nil,
	))
	bumpCbor, err := cbor.Encode(map[uint64]any{
		14: lcommon.ProtocolParametersProtocolVersion{Major: 9, Minor: 0},
	})
	require.NoError(t, err)
	require.NoError(t, db.SetPParamUpdate(
		[]byte{0xaa}, bumpCbor, 10, 0, nil,
	))

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: cfg,
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	ls.metrics.init(prometheus.NewRegistry())

	tipOf := func(block chain.RawBlock) ochainsync.Tip {
		return ochainsync.Tip{
			Point:       ocommon.NewPoint(block.Slot, block.Hash),
			BlockNumber: block.BlockNumber,
		}
	}
	evaluate := func() hardfork.TransitionInfo {
		ls.Lock()
		defer ls.Unlock()
		ls.evaluateProtocolVersionBump()
		return ls.transitionInfo
	}

	ls.Lock()
	ls.currentEra = eras.BabbageEraDesc
	ls.currentEpoch = models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		LengthInSlots: 100,
		SlotLength:    1000,
		EraId:         eras.BabbageEraDesc.Id,
	}
	ls.currentPParams = pparams
	ls.currentTip = tipOf(pastDeadline)
	ls.transitionInfo = hardfork.NewTransitionUnknown()
	ls.Unlock()
	require.NoError(t, db.SetTip(tipOf(pastDeadline), nil))
	require.Equal(t, hardfork.TransitionKnown, evaluate().State)

	// The chain rolls back first and the ledger follows it, as in chainsync.
	rollbackPoint := ocommon.NewPoint(preDeadline.Slot, preDeadline.Hash)
	require.NoError(t, cm.PrimaryChain().Rollback(rollbackPoint))
	require.NoError(t, ls.rollbackWithBlocks(rollbackPoint, nil, false))
	ls.RLock()
	afterRollback := ls.transitionInfo
	reloadedEra := ls.currentEra.Id
	reloadedPParams := ls.currentPParams
	ls.RUnlock()
	require.Equal(t, eras.BabbageEraDesc.Id, reloadedEra,
		"the rollback must reload a Babbage epoch for the re-evaluation")
	require.NotNil(t, reloadedPParams,
		"the rollback must reload the epoch's pparams for the re-evaluation")
	require.Equal(
		t,
		hardfork.TransitionUnknown,
		afterRollback.State,
		"no block of the surviving chain lies past the deadline",
	)

	forkPastDeadline := chain.RawBlock{
		Slot:        50,
		Hash:        testHashBytes("ppup-fork-past-deadline"),
		PrevHash:    preDeadline.Hash,
		BlockNumber: 2,
		Type:        1,
		Cbor:        []byte{0x80},
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(
		[]chain.RawBlock{forkPastDeadline},
	))
	ls.Lock()
	ls.currentTip = tipOf(forkPastDeadline)
	ls.Unlock()
	known := evaluate()
	require.Equal(t, hardfork.TransitionKnown, known.State,
		"a block past the deadline on the surviving chain counts")
	require.Equal(t, uint64(1), known.KnownEpoch)
}

// Upstream counts only blocks of the current epoch, so an epoch shorter than
// twice the stability window counts from its own first slot rather than from
// a deadline that falls in the previous epoch.
func TestPParamVotingCountStartSlot(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name             string
		securityParam    int
		activeSlotsCoeff string
		want             uint64
	}{
		{
			name:             "deadline inside the epoch",
			securityParam:    1,
			activeSlotsCoeff: "0.1",
			want:             140,
		},
		{
			name:             "epoch shorter than twice the stability window",
			securityParam:    2,
			activeSlotsCoeff: "0.1",
			want:             100,
		},
	} {
		ls := &LedgerState{
			currentEra: eras.BabbageEraDesc,
			currentEpoch: models.Epoch{
				EpochId:       1,
				StartSlot:     100,
				LengthInSlots: 100,
				EraId:         eras.BabbageEraDesc.Id,
			},
			config: LedgerStateConfig{
				CardanoNodeConfig: newBabbageQuorumCfg(
					t,
					tc.securityParam,
					tc.activeSlotsCoeff,
				),
				Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			},
		}
		got, ok := ls.pparamVotingCountStartSlot()
		require.True(t, ok, tc.name)
		assert.Equal(t, tc.want, got, tc.name)
	}
}

func TestGetProtocolVersion_Shelley(t *testing.T) {
	t.Parallel()

	pp := &shelley.ShelleyProtocolParameters{
		ProtocolMajor: 2,
		ProtocolMinor: 0,
	}
	pv, err := GetProtocolVersion(pp)
	require.NoError(t, err)
	assert.Equal(t, uint(2), pv.Major)
	assert.Equal(t, uint(0), pv.Minor)
}

func TestGetProtocolVersion_Allegra(t *testing.T) {
	t.Parallel()

	// allegra.AllegraProtocolParameters is a type alias for
	// shelley.ShelleyProtocolParameters, so Allegra pparams
	// are handled by the Shelley case in the type switch.
	pp := &allegra.AllegraProtocolParameters{
		ProtocolMajor: 3,
		ProtocolMinor: 0,
	}
	pv, err := GetProtocolVersion(pp)
	require.NoError(t, err)
	assert.Equal(t, uint(3), pv.Major)
	assert.Equal(t, uint(0), pv.Minor)
}

func TestGetProtocolVersion_Mary(t *testing.T) {
	t.Parallel()

	pp := &mary.MaryProtocolParameters{
		ProtocolMajor: 4,
		ProtocolMinor: 0,
	}
	pv, err := GetProtocolVersion(pp)
	require.NoError(t, err)
	assert.Equal(t, uint(4), pv.Major)
	assert.Equal(t, uint(0), pv.Minor)
}

func TestGetProtocolVersion_Alonzo(t *testing.T) {
	t.Parallel()

	pp := &alonzo.AlonzoProtocolParameters{
		ProtocolMajor: 6,
		ProtocolMinor: 0,
	}
	pv, err := GetProtocolVersion(pp)
	require.NoError(t, err)
	assert.Equal(t, uint(6), pv.Major)
	assert.Equal(t, uint(0), pv.Minor)
}

func TestGetProtocolVersion_Babbage(t *testing.T) {
	t.Parallel()

	pp := &babbage.BabbageProtocolParameters{
		ProtocolMajor: 8,
		ProtocolMinor: 0,
	}
	pv, err := GetProtocolVersion(pp)
	require.NoError(t, err)
	assert.Equal(t, uint(8), pv.Major)
	assert.Equal(t, uint(0), pv.Minor)
}

func TestGetProtocolVersion_Conway(t *testing.T) {
	t.Parallel()

	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 9,
			Minor: 0,
		},
	}
	pv, err := GetProtocolVersion(pp)
	require.NoError(t, err)
	assert.Equal(t, uint(9), pv.Major)
	assert.Equal(t, uint(0), pv.Minor)
}

func TestGetProtocolVersion_Dijkstra(t *testing.T) {
	t.Parallel()

	pp := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 12,
				Minor: 0,
			},
		},
	}
	pv, err := GetProtocolVersion(pp)
	require.NoError(t, err)
	assert.Equal(t, uint(12), pv.Major)
	assert.Equal(t, uint(0), pv.Minor)
}

func TestGetProtocolVersion_Nil(t *testing.T) {
	t.Parallel()

	_, err := GetProtocolVersion(nil)
	require.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"protocol parameters are nil",
	)
}

func TestEraForVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		eraList       []eras.EraDesc
		majorVersion  uint
		expectedEraId uint
		expectedOk    bool
	}{
		{
			name:          "Byron version 0",
			majorVersion:  0,
			expectedEraId: 0,
			expectedOk:    true,
		},
		{
			name:          "Byron version 1",
			majorVersion:  1,
			expectedEraId: 0,
			expectedOk:    true,
		},
		{
			name:          "Shelley version 2",
			majorVersion:  2,
			expectedEraId: 1,
			expectedOk:    true,
		},
		{
			name:          "Allegra version 3",
			majorVersion:  3,
			expectedEraId: 2,
			expectedOk:    true,
		},
		{
			name:          "Mary version 4",
			majorVersion:  4,
			expectedEraId: 3,
			expectedOk:    true,
		},
		{
			name:          "Alonzo version 5",
			majorVersion:  5,
			expectedEraId: 4,
			expectedOk:    true,
		},
		{
			name:          "Alonzo version 6",
			majorVersion:  6,
			expectedEraId: 4,
			expectedOk:    true,
		},
		{
			name:          "Babbage version 7",
			majorVersion:  7,
			expectedEraId: 5,
			expectedOk:    true,
		},
		{
			name:          "Babbage version 8",
			majorVersion:  8,
			expectedEraId: 5,
			expectedOk:    true,
		},
		{
			name:          "Conway version 9",
			majorVersion:  9,
			expectedEraId: 6,
			expectedOk:    true,
		},
		{
			name:          "Conway version 10",
			majorVersion:  10,
			expectedEraId: 6,
			expectedOk:    true,
		},
		{
			name:         "Dijkstra version gated off by default",
			majorVersion: 12,
			expectedOk:   false,
		},
		{
			name:          "Dijkstra version enabled",
			eraList:       eras.ErasWithDijkstra,
			majorVersion:  12,
			expectedEraId: 7,
			expectedOk:    true,
		},
		{
			name:         "Unknown version 99",
			majorVersion: 99,
			expectedOk:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eraList := eras.Eras
			if tt.eraList != nil {
				eraList = tt.eraList
			}
			eraId, ok := EraForVersion(eraList, tt.majorVersion)
			assert.Equal(t, tt.expectedOk, ok)
			if tt.expectedOk {
				assert.Equal(
					t,
					tt.expectedEraId,
					eraId,
				)
			}
		})
	}
}

func TestIsHardForkTransition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		eraList  []eras.EraDesc
		old      ProtocolVersion
		new      ProtocolVersion
		expected bool
	}{
		{
			name:     "same version no transition",
			old:      ProtocolVersion{Major: 8, Minor: 0},
			new:      ProtocolVersion{Major: 8, Minor: 0},
			expected: false,
		},
		{
			name:     "minor version change only",
			old:      ProtocolVersion{Major: 8, Minor: 0},
			new:      ProtocolVersion{Major: 8, Minor: 1},
			expected: false,
		},
		{
			name:     "Babbage to Conway transition",
			old:      ProtocolVersion{Major: 8, Minor: 0},
			new:      ProtocolVersion{Major: 9, Minor: 0},
			expected: true,
		},
		{
			name:     "Alonzo to Babbage transition",
			old:      ProtocolVersion{Major: 6, Minor: 0},
			new:      ProtocolVersion{Major: 7, Minor: 0},
			expected: true,
		},
		{
			name:     "intra-era Alonzo version bump",
			old:      ProtocolVersion{Major: 5, Minor: 0},
			new:      ProtocolVersion{Major: 6, Minor: 0},
			expected: false,
		},
		{
			name:     "intra-era Babbage version bump",
			old:      ProtocolVersion{Major: 7, Minor: 0},
			new:      ProtocolVersion{Major: 8, Minor: 0},
			expected: false,
		},
		{
			name:     "intra-era Conway version bump",
			old:      ProtocolVersion{Major: 9, Minor: 0},
			new:      ProtocolVersion{Major: 10, Minor: 0},
			expected: false,
		},
		{
			name:     "Shelley to Allegra transition",
			old:      ProtocolVersion{Major: 2, Minor: 0},
			new:      ProtocolVersion{Major: 3, Minor: 0},
			expected: true,
		},
		{
			name:     "unknown old version",
			old:      ProtocolVersion{Major: 99, Minor: 0},
			new:      ProtocolVersion{Major: 9, Minor: 0},
			expected: false,
		},
		{
			name:     "unknown new version",
			old:      ProtocolVersion{Major: 9, Minor: 0},
			new:      ProtocolVersion{Major: 99, Minor: 0},
			expected: false,
		},
		{
			name:     "Conway to Dijkstra gated off",
			old:      ProtocolVersion{Major: 10, Minor: 0},
			new:      ProtocolVersion{Major: 12, Minor: 0},
			expected: false,
		},
		{
			name:     "Conway to Dijkstra enabled",
			eraList:  eras.ErasWithDijkstra,
			old:      ProtocolVersion{Major: 10, Minor: 0},
			new:      ProtocolVersion{Major: 12, Minor: 0},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eraList := tt.eraList
			if eraList == nil {
				eraList = eras.Eras
			}
			result := IsHardForkTransition(eraList, tt.old, tt.new)
			assert.Equal(t, tt.expected, result)
		})
	}
}
