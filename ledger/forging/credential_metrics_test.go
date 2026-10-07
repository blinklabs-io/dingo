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

package forging

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// forgerNeverLeader declines every slot.
type forgerNeverLeader struct{}

func (forgerNeverLeader) ShouldProduceBlock(uint64) bool { return false }

func (forgerNeverLeader) NextLeaderSlot(
	uint64,
) (uint64, bool) {
	return 0, false
}

// TestMissedLeaderSlotsCountsWonSlotsWithoutAnAdoptedBlock drives whole forge
// attempts. The counter is the leadership-won-but-no-block series: it moves
// when the node was elected and the slot did not end with its own block
// adopted, and stays still for slots it was never elected for.
func TestMissedLeaderSlotsCountsWonSlotsWithoutAnAdoptedBlock(t *testing.T) {
	t.Parallel()

	t.Run("adopted block is not a miss", func(t *testing.T) {
		t.Parallel()
		forger, _ := newForgerWithValidator(
			t,
			newForgerTestBlock(10, 2),
			nil,
			&forgerTestBroadcaster{},
			&forgerTestValidator{},
		)
		require.NoError(t, forger.checkAndForgeProduction(context.Background()))
		require.Equal(
			t,
			float64(1),
			testutil.ToFloat64(forger.metrics.forgeAdopted),
		)
		require.Zero(
			t,
			testutil.ToFloat64(forger.metrics.forgeMissedLeaderSlots),
		)
	})

	t.Run("block dropped by self-validation is a miss", func(t *testing.T) {
		t.Parallel()
		forger, _ := newForgerWithValidator(
			t,
			newForgerTestBlock(10, 2),
			nil,
			&forgerTestBroadcaster{},
			&forgerTestValidator{err: errors.New("invalid KES signature")},
		)
		require.Error(t, forger.checkAndForgeProduction(context.Background()))
		require.Equal(
			t,
			float64(1),
			testutil.ToFloat64(forger.metrics.forgeNodeIsLeader),
		)
		require.Equal(
			t,
			float64(1),
			testutil.ToFloat64(forger.metrics.forgeMissedLeaderSlots),
		)
	})

	t.Run("won slot refused by the counter gate is a miss", func(t *testing.T) {
		t.Parallel()
		// The gate runs after leader selection and before node_is_leader
		// moves, so only the miss counter sees this slot as lost.
		builder, broadcaster := newOpCertSequenceGateTestBuilder()
		var logs bytes.Buffer
		forger := opCertSequenceGateForger(
			t,
			&fakeLedgerView{seqFound: true, latestSeq: 1},
			&mockPParamsProvider{pparams: &babbage.BabbageProtocolParameters{}},
			&forgerCountingLeader{},
			builder,
			broadcaster,
			&logs,
		)
		require.NoError(t, forger.checkAndForgeProduction(context.Background()))
		require.Zero(t, broadcaster.calls)
		require.Zero(t, testutil.ToFloat64(forger.metrics.forgeNodeIsLeader))
		require.Equal(
			t,
			float64(1),
			testutil.ToFloat64(forger.metrics.forgeMissedLeaderSlots),
		)
	})

	t.Run(
		"slot the fence says was already committed is not counted again",
		func(t *testing.T) {
			t.Parallel()
			// A repeat attempt for a slot an earlier attempt already took, as the
			// ticker loop makes within one slot, is refused by the fence. The
			// earlier attempt was the one that won or lost it.
			builder := &fenceTestBuilder{block: newForgerTestBlock(10, 2)}
			forger, err := newFenceTestForger(
				t,
				&fenceTestStore{slot: 10, present: true},
				10,
				builder,
				&forgerTestBroadcaster{},
				nil,
			)
			require.NoError(t, err)
			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)
			require.Equal(
				t,
				float64(1),
				testutil.ToFloat64(forger.metrics.forgeFenceBlocked),
			)
			require.Zero(t, builder.calls)
			require.Zero(
				t,
				testutil.ToFloat64(forger.metrics.forgeMissedLeaderSlots),
			)
		},
	)

	t.Run("slot the node did not win is not a miss", func(t *testing.T) {
		t.Parallel()
		forger, _ := newForgerWithValidator(
			t,
			newForgerTestBlock(10, 2),
			nil,
			&forgerTestBroadcaster{},
			&forgerTestValidator{},
		)
		forger.leaderChecker = forgerNeverLeader{}
		require.NoError(t, forger.checkAndForgeProduction(context.Background()))
		require.Equal(
			t,
			float64(1),
			testutil.ToFloat64(forger.metrics.forgeNotLeader),
		)
		require.Zero(
			t,
			testutil.ToFloat64(forger.metrics.forgeMissedLeaderSlots),
		)
	})
}

func TestBlockForgerCredentialsUsableTracksTheKESWindow(t *testing.T) {
	t.Parallel()
	forger, clock := newForgerWithValidator(
		t,
		newForgerTestBlock(10, 2),
		nil,
		&forgerTestBroadcaster{},
		&forgerTestValidator{},
	)
	require.NoError(t, forger.CredentialsUsable())

	// 62 evolutions of 100 slots each: period 62 is the first expired one.
	clock.currentSlot = 100 * 62
	require.ErrorContains(t, forger.CredentialsUsable(), "expired")

	clock.currentSlot = 10
	forger.creds.Close()
	require.Error(t, forger.CredentialsUsable())
}

// TestBlockForgerCredentialsUsableRefusesACertificateNotYetValid covers the
// future side of the window: a certificate that starts after the current KES
// period, as after a clock regression, is not usable.
func TestBlockForgerCredentialsUsableRefusesACertificateNotYetValid(
	t *testing.T,
) {
	t.Parallel()
	fixture := newCredentialsRotationFixture(t)
	creds := NewPoolCredentials()
	require.NoError(t, creds.LoadFromFiles(
		fixture.vrfPath, fixture.kesPath, fixture.opCert(t, 1, 5),
	))
	require.NoError(t, creds.ValidateOpCert())
	// Period 5 starts at slot 500 with 100-slot periods.
	require.NoError(t, creds.ValidateKESPeriod(
		synthGenesis(100, 62, time.Second, time.Unix(0, 0)),
		500,
	))
	clock := &forgerTestSlotClock{currentSlot: 500, slotsPerKESPeriod: 100}
	forger := &BlockForger{creds: creds, slotClock: clock}
	require.NoError(t, forger.CredentialsUsable())

	clock.currentSlot = 10
	require.ErrorContains(t, forger.CredentialsUsable(), "not valid before")
}

// gaugeValue reads one unlabeled gauge out of a registry.
func gaugeValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == name {
			require.Len(t, family.GetMetric(), 1)
			return family.GetMetric()[0].GetGauge().GetValue()
		}
	}
	require.Failf(t, "metric not exported", "%s is not registered", name)
	return 0
}

// TestForgerExportsOpCertCounterAndCredentialGauges reads the series an
// operator needs to size the next certificate counter from /metrics alone,
// across an opcert rotation: the counter in the loaded certificate and the
// on-chain counter the ledger last applied for the pool.
func TestForgerExportsOpCertCounterAndCredentialGauges(t *testing.T) {
	t.Parallel()
	fixture := newCredentialsRotationFixture(t)
	creds := fixture.validated(t, 1)
	view := &fakeLedgerView{seqFound: true, latestSeq: 1}
	clock := &forgerTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
	}
	reg := prometheus.NewRegistry()
	_, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock:        clock,
		OpCertLedgerView: view,
		EraParams: &mockPParamsProvider{
			pparams: &babbage.BabbageProtocolParameters{},
		},
		PromRegistry: reg,
	})
	require.NoError(t, err)

	const (
		loaded  = "dingo_forge_opcert_counter_loaded"
		onchain = "dingo_forge_opcert_counter_onchain"
		perKES  = "dingo_forge_slots_per_kes_period"
		valid   = "dingo_forge_credentials_valid"
	)
	require.Equal(t, float64(1), gaugeValue(t, reg, loaded))
	require.Equal(t, float64(1), gaugeValue(t, reg, onchain))
	require.Equal(t, float64(100), gaugeValue(t, reg, perKES))
	require.Equal(t, float64(1), gaugeValue(t, reg, valid))

	// Rotation: the operator issues counter 2 and the chain applies a block
	// carrying it.
	require.NoError(t, creds.ReplaceWith(fixture.validated(t, 2)))
	view.latestSeq = 2
	require.Equal(t, float64(2), gaugeValue(t, reg, loaded))
	require.Equal(t, float64(2), gaugeValue(t, reg, onchain))
	require.Equal(t, float64(1), gaugeValue(t, reg, valid))

	// A pool the ledger has never seen a counter for has no on-chain value,
	// which must not read as counter 0.
	view.seqFound = false
	require.True(t, math.IsNaN(gaugeValue(t, reg, onchain)))

	clock.currentSlot = 100 * 62
	require.Equal(t, float64(0), gaugeValue(t, reg, valid))
}

// TestBlockForgerCredentialsUsableRefusesUnvalidatedCredentials covers
// material that is loaded but whose certificate was never validated, which
// must not read as usable even though every key is present.
func TestBlockForgerCredentialsUsableRefusesUnvalidatedCredentials(
	t *testing.T,
) {
	t.Parallel()
	fixture := newCredentialsRotationFixture(t)
	creds := NewPoolCredentials()
	require.NoError(t, creds.LoadFromFiles(
		fixture.vrfPath, fixture.kesPath, fixture.opCert(t, 1, 0),
	))
	require.True(t, creds.IsLoaded())
	forger := &BlockForger{
		creds: creds,
		slotClock: &forgerTestSlotClock{
			currentSlot:       10,
			slotsPerKESPeriod: 100,
		},
	}
	require.ErrorContains(
		t,
		forger.CredentialsUsable(),
		"operational certificate is not validated",
	)
}
