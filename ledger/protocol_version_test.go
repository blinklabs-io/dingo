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

func TestGetProtocolVersion_TypedNil(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pparams lcommon.ProtocolParameters
	}{
		{name: "Shelley", pparams: (*shelley.ShelleyProtocolParameters)(nil)},
		{name: "Mary", pparams: (*mary.MaryProtocolParameters)(nil)},
		{name: "Alonzo", pparams: (*alonzo.AlonzoProtocolParameters)(nil)},
		{name: "Babbage", pparams: (*babbage.BabbageProtocolParameters)(nil)},
		{name: "Conway", pparams: (*conway.ConwayProtocolParameters)(nil)},
		{name: "Dijkstra", pparams: (*dijkstra.DijkstraProtocolParameters)(nil)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.NotPanics(t, func() {
				_, err := GetProtocolVersion(test.pparams)
				require.ErrorContains(t, err, "protocol parameters are a nil")
			})
		})
	}
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
