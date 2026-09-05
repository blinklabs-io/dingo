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
	"log/slog"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// opCertSequenceGateForger builds a production BlockForger whose credentials'
// OpCert issue number is the fixed value baked into testOpCertJSON (0, see
// TestValidateOpCertUnsafe_ValidCertificate), wired with the given ledger
// view / era params so tests can drive the pre-flight counter gate. The
// caller retains leader so it can assert whether leader selection ran.
func opCertSequenceGateForger(
	t *testing.T,
	view LedgerView,
	eraParams ProtocolParamsProvider,
	leader LeaderChecker,
	builder *forgerTestBuilder,
	broadcaster *forgerTestBroadcaster,
	logs *bytes.Buffer,
) *BlockForger {
	t.Helper()
	creds := setupTestCredentials(t)
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(logs, nil)),
		Credentials:      creds,
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       1,
			chainTipSlot:      0,
			slotsPerKESPeriod: 100,
		},
		OpCertLedgerView: view,
		EraParams:        eraParams,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger
}

func newOpCertSequenceGateTestBuilder() (*forgerTestBuilder, *forgerTestBroadcaster) {
	block := newForgerTestBlock(0, 1)
	return &forgerTestBuilder{block: block, cbor: block.cbor},
		&forgerTestBroadcaster{}
}

// TestNewBlockForgerRequiresEraParamsWithOpCertLedgerView pins the
// construction-time contract: the pre-flight counter check cannot resolve
// the era-scoped rule (no-gap vs. stale-only) without EraParams, so wiring
// one provider without the other must fail closed at startup rather than at
// the first forge attempt.
func TestNewBlockForgerRequiresEraParamsWithOpCertLedgerView(t *testing.T) {
	creds := setupTestCredentials(t)
	_, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock:        forgerTestSlotClock{slotsPerKESPeriod: 1},
		OpCertLedgerView: &fakeLedgerView{},
	})
	require.ErrorContains(t, err, "EraParams")
}

// TestCheckAndForgeProductionSkipsOnStaleOpCertCounter covers a stale
// counter -- the ledger has observed a higher issue number for this pool
// than the loaded credentials carry -- which block application would
// reject regardless of era. It must be caught after leader selection (so
// the ledger read it performs is skipped for slots this pool does not
// lead) but before block build or the forge-slot fence.
func TestCheckAndForgeProductionSkipsOnStaleOpCertCounter(t *testing.T) {
	builder, broadcaster := newOpCertSequenceGateTestBuilder()
	leader := &forgerCountingLeader{}
	var logs bytes.Buffer
	view := &fakeLedgerView{seqFound: true, latestSeq: 1}
	eraParams := &mockPParamsProvider{
		pparams: &babbage.BabbageProtocolParameters{},
	}
	forger := opCertSequenceGateForger(
		t, view, eraParams, leader, builder, broadcaster, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(
		t,
		1,
		leader.callCount(),
		"leader selection must run before the counter check",
	)
	require.Zero(t, builder.calls, "stale counter must not build a block")
	require.Zero(
		t,
		broadcaster.calls,
		"stale counter must not adopt a block",
	)
	require.Contains(t, logs.String(), "operational certificate counter")
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
}

// TestCheckAndForgeProductionAllowsUnobservedOpCertCounter covers the
// baseline case: the ledger has never observed a counter for this pool
// (fresh registration, or a Mithril-restored start), so there is nothing to
// compare against and the candidate is accepted.
func TestCheckAndForgeProductionAllowsUnobservedOpCertCounter(t *testing.T) {
	builder, broadcaster := newOpCertSequenceGateTestBuilder()
	var logs bytes.Buffer
	view := &fakeLedgerView{seqFound: false}
	eraParams := &mockPParamsProvider{
		pparams: &babbage.BabbageProtocolParameters{},
	}
	forger := opCertSequenceGateForger(
		t,
		view,
		eraParams,
		&forgerCountingLeader{},
		builder,
		broadcaster,
		&logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.calls)
	require.Equal(t, 1, broadcaster.calls)
}

// TestCheckAndForgeProductionSkipsWhenEraUnresolvable covers a provider
// that cannot yet supply protocol parameters for the slot (e.g. very early
// startup). The gate fails closed rather than guessing an era-scoped rule.
func TestCheckAndForgeProductionSkipsWhenEraUnresolvable(t *testing.T) {
	builder, broadcaster := newOpCertSequenceGateTestBuilder()
	leader := &forgerCountingLeader{}
	var logs bytes.Buffer
	view := &fakeLedgerView{seqFound: true, latestSeq: 0}
	eraParams := &mockPParamsProvider{pparams: nil}
	forger := opCertSequenceGateForger(
		t, view, eraParams, leader, builder, broadcaster, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(
		t,
		1,
		leader.callCount(),
		"leader selection must run before the counter check",
	)
	require.Zero(
		t,
		builder.calls,
		"an unresolvable era must not build a block",
	)
	require.Zero(t, broadcaster.calls)
}

// TestCheckAndForgeProductionEraScopedOpCertCounterRule covers the era-
// scoped no-gap rule end to end through the running forger: TPraos
// (Shelley-Alonzo) accepts a counter that jumps ahead of the last observed
// value, Praos (Babbage onward) does not. Boundary values (exactly one
// ahead, exactly equal) are covered for both eras.
func TestCheckAndForgeProductionEraScopedOpCertCounterRule(t *testing.T) {
	tests := []struct {
		name       string
		pparams    lcommon.ProtocolParameters
		candidate  uint64
		stored     uint64
		wantForged bool
	}{
		{
			name:       "tpraos era equal to last seen",
			pparams:    &shelley.ShelleyProtocolParameters{ProtocolMajor: 2},
			candidate:  5,
			stored:     5,
			wantForged: true,
		},
		{
			name:       "tpraos era exactly one ahead (boundary)",
			pparams:    &shelley.ShelleyProtocolParameters{ProtocolMajor: 2},
			candidate:  6,
			stored:     5,
			wantForged: true,
		},
		{
			name:       "tpraos era gap of two accepted (era change from praos)",
			pparams:    &shelley.ShelleyProtocolParameters{ProtocolMajor: 2},
			candidate:  7,
			stored:     5,
			wantForged: true,
		},
		{
			name:       "praos era exactly one ahead (boundary)",
			pparams:    &babbage.BabbageProtocolParameters{},
			candidate:  6,
			stored:     5,
			wantForged: true,
		},
		{
			name:       "praos era gap of two rejected (era change from tpraos)",
			pparams:    &babbage.BabbageProtocolParameters{},
			candidate:  7,
			stored:     5,
			wantForged: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder, broadcaster := newOpCertSequenceGateTestBuilder()
			leader := &forgerCountingLeader{}
			var logs bytes.Buffer
			view := &fakeLedgerView{seqFound: true, latestSeq: tt.stored}
			eraParams := &mockPParamsProvider{pparams: tt.pparams}
			forger := opCertSequenceGateForger(
				t, view, eraParams, leader, builder, broadcaster, &logs,
			)
			forger.creds.mu.Lock()
			forger.creds.opCert.IssueNumber = tt.candidate
			forger.creds.mu.Unlock()

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)

			require.Equal(
				t,
				1,
				leader.callCount(),
				"leader selection must run before the counter check",
			)
			if tt.wantForged {
				require.Equal(t, 1, builder.calls)
				require.Equal(t, 1, broadcaster.calls)
			} else {
				require.Zero(t, builder.calls)
				require.Zero(t, broadcaster.calls)
				require.Contains(
					t,
					logs.String(),
					"operational certificate counter",
				)
			}
		})
	}
}

// TestCheckAndForgeProductionSkipsOpCertSequenceCheckWhenLedgerViewNil
// confirms the pre-flight counter gate is opt-in: embedders and dev-mode
// wiring that leave OpCertLedgerView nil (every other forger test in this
// package) are unaffected by this change.
func TestCheckAndForgeProductionSkipsOpCertSequenceCheckWhenLedgerViewNil(
	t *testing.T,
) {
	builder, broadcaster := newOpCertSequenceGateTestBuilder()
	var logs bytes.Buffer
	forger := opCertSequenceGateForger(
		t, nil, nil, &forgerCountingLeader{}, builder, broadcaster, &logs,
	)
	// A counter far ahead of any plausible on-chain value would be rejected
	// under the gate if it were active; with no LedgerView wired it is not
	// evaluated at all.
	forger.creds.mu.Lock()
	forger.creds.opCert.IssueNumber = 1000
	forger.creds.mu.Unlock()

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.calls)
	require.Equal(t, 1, broadcaster.calls)
}
