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
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

// leiosAggregateCost is what one test transaction declares and resolves.
type leiosAggregateCost struct {
	memory         int64
	steps          int64
	refScriptBytes uint64
}

// leiosAggregateTx returns a mempool transaction whose single redeemer
// declares the cost's execution units.
func leiosAggregateTx(
	t *testing.T,
	id byte,
	cost leiosAggregateCost,
) MempoolTransaction {
	t.Helper()
	txHash := make([]byte, 32)
	txHash[0] = id
	body := map[uint]any{
		0: cbor.Tag{
			Number:  258,
			Content: []any{[]any{txHash, uint64(0)}},
		},
		1: []any{
			[]any{append([]byte{0x61}, make([]byte, 28)...), uint64(1000000)},
		},
		2: uint64(200000),
	}
	witnesses := map[uint]any{}
	if cost.memory != 0 || cost.steps != 0 {
		witnesses[5] = []any{
			[]any{uint64(0), uint64(0), uint64(0),
				[]any{cost.memory, cost.steps}},
		}
	}
	raw, err := cbor.Encode([]any{body, witnesses, true, nil})
	require.NoError(t, err)
	decoded, err := conway.NewConwayTransactionFromCbor(raw)
	require.NoError(t, err)
	return MempoolTransaction{
		Hash: decoded.Hash().String(),
		Cbor: raw,
		Type: conway.TxTypeConway,
	}
}

// leiosMeasuringValidator accepts every transaction and measures reference
// scripts from a table keyed by transaction hash.
type leiosMeasuringValidator struct {
	refScriptBytes map[string]uint64
	measured       []string
}

func (v *leiosMeasuringValidator) ValidateTx(ledger.Transaction) error {
	return nil
}

func (v *leiosMeasuringValidator) ValidateTxWithOverlay(
	ledger.Transaction,
	map[utxoref.Key]struct{},
	map[utxoref.Key]lcommon.Utxo,
	*utxoref.StateOverlay,
) error {
	return nil
}

func (v *leiosMeasuringValidator) WithLeiosSelectionSession(
	ctx context.Context,
	_ []byte,
	fn func(TxValidationFunc, TxRefScriptSizeFunc, func() bool) error,
) error {
	validate := func(
		ledger.Transaction,
		map[utxoref.Key]struct{},
		map[utxoref.Key]lcommon.Utxo,
		*utxoref.StateOverlay,
	) error {
		return nil
	}
	measure := func(
		tx ledger.Transaction,
		_ map[utxoref.Key]struct{},
		_ map[utxoref.Key]lcommon.Utxo,
	) (uint64, error) {
		v.measured = append(v.measured, tx.Hash().String())
		return v.refScriptBytes[tx.Hash().String()], nil
	}
	return fn(validate, measure, func() bool { return ctx.Err() == nil })
}

func leiosAggregateCandidates(
	t *testing.T,
	costs []leiosAggregateCost,
) ([]MempoolTransaction, *leiosMeasuringValidator) {
	t.Helper()
	validator := &leiosMeasuringValidator{
		refScriptBytes: make(map[string]uint64),
	}
	txs := make([]MempoolTransaction, 0, len(costs))
	for i, cost := range costs {
		tx := leiosAggregateTx(t, byte(i+1), cost)
		validator.refScriptBytes[tx.Hash] = cost.refScriptBytes
		txs = append(txs, tx)
	}
	return txs, validator
}

func repeatLeiosCost(cost leiosAggregateCost, n int) []leiosAggregateCost {
	out := make([]leiosAggregateCost, n)
	for i := range out {
		out[i] = cost
	}
	return out
}

// Selection takes the longest prefix whose summed cost stays at or below
// every aggregate bound: memory, steps and reference-script bytes are each
// compared on their own, and equality fits. The reference mempool splits an
// endorser block's candidates the same way (splitAfterTxSizeOn over the
// pointwise Measure order).
func TestLeiosEBSelectionBoundsAggregateResources(t *testing.T) {
	t.Parallel()
	each := leiosAggregateCost{memory: 10, steps: 100, refScriptBytes: 1000}
	for _, tc := range []struct {
		name     string
		costs    []leiosAggregateCost
		maxMem   int64
		maxSteps int64
		maxRef   uint64
		want     int
	}{
		{
			name:     "memory exceeded alone",
			costs:    repeatLeiosCost(each, 5),
			maxMem:   35,
			maxSteps: 1 << 40,
			maxRef:   1 << 40,
			want:     3,
		},
		{
			name:     "steps exceeded alone",
			costs:    repeatLeiosCost(each, 5),
			maxMem:   1 << 40,
			maxSteps: 350,
			maxRef:   1 << 40,
			want:     3,
		},
		{
			name:     "reference scripts exceeded alone",
			costs:    repeatLeiosCost(each, 5),
			maxMem:   1 << 40,
			maxSteps: 1 << 40,
			maxRef:   3500,
			want:     3,
		},
		{
			name:     "exactly at every bound",
			costs:    repeatLeiosCost(each, 4),
			maxMem:   40,
			maxSteps: 400,
			maxRef:   4000,
			want:     4,
		},
		{
			name:     "one under memory",
			costs:    repeatLeiosCost(each, 4),
			maxMem:   39,
			maxSteps: 400,
			maxRef:   4000,
			want:     3,
		},
		{
			name:     "one under steps",
			costs:    repeatLeiosCost(each, 4),
			maxMem:   40,
			maxSteps: 399,
			maxRef:   4000,
			want:     3,
		},
		{
			name:     "one under reference scripts",
			costs:    repeatLeiosCost(each, 4),
			maxMem:   40,
			maxSteps: 400,
			maxRef:   3999,
			want:     3,
		},
		{
			// Selection stops at the first transaction that does not
			// fit rather than skipping it for a smaller one, so a
			// descendant never outlives its parent.
			name: "stops at the first misfit",
			costs: []leiosAggregateCost{
				{memory: 10}, {memory: 30}, {memory: 1},
			},
			maxMem:   35,
			maxSteps: 1 << 40,
			maxRef:   1 << 40,
			want:     1,
		},
		{
			// A zero bound is a bound: only free transactions fit.
			name: "zero bounds admit only free transactions",
			costs: []leiosAggregateCost{
				{}, {}, {memory: 1}, {},
			},
			want: 2,
		},
		{
			name:  "zero reference-script bound refuses a script",
			costs: []leiosAggregateCost{{}, {refScriptBytes: 1}},
			want:  1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			txs, validator := leiosAggregateCandidates(t, tc.costs)
			selected, truncated, err := selectValidLeiosTransactions(
				context.Background(),
				txs,
				validator,
				leiosSelectionLimits{caps: leiosEBCaps{
					aggregateBounded: true,
					maxExUnits: lcommon.ExUnits{
						Memory: tc.maxMem,
						Steps:  tc.maxSteps,
					},
					maxRefScriptBytes: tc.maxRef,
				}},
				leiosSelectionBasis{},
			)
			require.NoError(t, err)
			require.False(t, truncated)
			require.Equal(t, txs[:tc.want], selected)
		})
	}
}

// Without protocol bounds the measure is never taken, so earlier eras keep
// their behaviour and need no reference-script resolver.
func TestLeiosEBSelectionUnboundedDoesNotMeasure(t *testing.T) {
	t.Parallel()
	txs, validator := leiosAggregateCandidates(t, repeatLeiosCost(
		leiosAggregateCost{memory: 10, steps: 10, refScriptBytes: 10}, 3,
	))
	selected, _, err := selectValidLeiosTransactions(
		context.Background(),
		txs,
		validator,
		leiosSelectionLimits{caps: leiosEBCaps{maxRefs: 10}},
		leiosSelectionBasis{},
	)
	require.NoError(t, err)
	require.Equal(t, txs, selected)
	require.Empty(t, validator.measured)
}

// A bound that cannot be measured must stop selection, not be skipped.
func TestLeiosEBSelectionRequiresMeasureWhenBounded(t *testing.T) {
	t.Parallel()
	txs := leiosCandidateTxs(t, 2)
	_, _, err := selectValidLeiosTransactions(
		context.Background(),
		txs,
		&sessionMockTxValidator{},
		leiosSelectionLimits{caps: leiosEBCaps{aggregateBounded: true}},
		leiosSelectionBasis{},
	)
	require.ErrorIs(t, err, errLeiosRefScriptMeasureUnavailable)
}

// The protocol's endorser-block execution and reference-script bounds reach
// the selection caps, zero included, and the operator caps stay as they were.
func TestEffectiveLeiosEBCapsCarriesAggregateBounds(t *testing.T) {
	t.Parallel()
	caps, ok := effectiveLeiosEBCaps(leiosEBCaps{maxRefs: 7}, &dijkstra.DijkstraProtocolParameters{
		MaxEndorserBlockTxsSize:          260000,
		MaxEndorserBlockReferencesSize:   40000,
		MaxEndorserBlockExUnits:          lcommon.ExUnits{Memory: 5, Steps: 6},
		MaxRefScriptSizePerEndorserBlock: 7,
	})
	require.True(t, ok)
	require.True(t, caps.aggregateBounded)
	require.Equal(t, lcommon.ExUnits{Memory: 5, Steps: 6}, caps.maxExUnits)
	require.Equal(t, uint64(7), caps.maxRefScriptBytes)
	require.Equal(t, uint64(7), caps.maxRefs)

	// Zero stays a bound; it is not the operator's unlimited sentinel.
	caps, ok = effectiveLeiosEBCaps(leiosEBCaps{}, &dijkstra.DijkstraProtocolParameters{
		MaxEndorserBlockTxsSize:        260000,
		MaxEndorserBlockReferencesSize: 40000,
	})
	require.True(t, ok)
	require.True(t, caps.aggregateBounded)
	require.Equal(t, lcommon.ExUnits{}, caps.maxExUnits)
	require.Zero(t, caps.maxRefScriptBytes)

	caps, ok = effectiveLeiosEBCaps(leiosEBCaps{}, &conway.ConwayProtocolParameters{})
	require.True(t, ok)
	require.False(t, caps.aggregateBounded)
}

// The forging entry point applies the slot's protocol bounds to the
// endorser block it announces.
func TestCheckAndForgeLeiosEBRespectsAggregateBounds(t *testing.T) {
	t.Parallel()
	each := leiosAggregateCost{memory: 10, steps: 100, refScriptBytes: 1000}
	for _, tc := range []struct {
		name   string
		units  lcommon.ExUnits
		refMax uint32
		want   int
	}{
		{"memory", lcommon.ExUnits{Memory: 25, Steps: 1 << 40}, 1 << 30, 2},
		{"steps", lcommon.ExUnits{Memory: 1 << 40, Steps: 250}, 1 << 30, 2},
		{"reference scripts", lcommon.ExUnits{Memory: 1 << 40, Steps: 1 << 40}, 2500, 2},
		{"exactly at the limits", lcommon.ExUnits{Memory: 30, Steps: 300}, 3000, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			txs, validator := leiosAggregateCandidates(t,
				repeatLeiosCost(each, 5))
			caster := &forgerTestLeiosCaster{}
			forger := leiosCapacityForger(txs,
				&dijkstra.DijkstraProtocolParameters{
					MaxEndorserBlockTxsSize:          260000,
					MaxEndorserBlockReferencesSize:   40000,
					MaxEndorserBlockExUnits:          tc.units,
					MaxRefScriptSizePerEndorserBlock: tc.refMax,
				},
				caster,
			)
			forger.leiosValidator = validator
			announcement, err := forger.checkAndForgeLeiosEB(
				context.Background(), 10, leiosSelectionBasis{},
			)
			require.NoError(t, err)
			require.NotNil(t, announcement)
			require.Equal(t, tc.want, len(caster.txBodies))
		})
	}
}
