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
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, loadByronGenesisForTest(t, cfg, strings.NewReader(`{
		"protocolConsts": {
			"k": 1,
			"protocolMagic": 42
		}
	}`)))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"systemStart": "2026-01-01T00:00:00Z",
		"securityParam": 1,
		"activeSlotsCoeff": 0.1,
		"epochLength": 100,
		"slotLength": 1,
		"updateQuorum": 1
	}`)))
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
	ls := &LedgerState{
		db: db,
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
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
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
