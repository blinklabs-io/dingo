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
	"math"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

func scaleTestPParams() *dijkstra.DijkstraProtocolParameters {
	return &dijkstra.DijkstraProtocolParameters{
		MaxEndorserBlockTxsSize:        1000,
		MaxEndorserBlockReferencesSize: 40005,
		MaxEndorserBlockExUnits: lcommon.ExUnits{
			Memory: 1001,
			Steps:  777,
		},
		MaxRefScriptSizePerEndorserBlock: 999,
	}
}

// Every protocol bound shrinks by the scale and rounds down. The manifest
// bound scales what is left after the widest map head, which the reference
// keeps out of the scaled capacity. The reference converts the scale to a
// rational exactly, so a scale that is not representable (0.7) must not round
// the product up to a whole number.
func TestEffectiveLeiosEBCapsScalesProtocolBounds(t *testing.T) {
	t.Parallel()
	operator := leiosEBCaps{maxRefs: 20000}

	caps, ok := effectiveLeiosEBCapsScaled(operator, scaleTestPParams(), 0.5)
	require.True(t, ok)
	require.Equal(t, leiosEBCaps{
		maxRefs:           20000,
		maxBytes:          500,
		maxManifestBytes:  5 + 20000,
		aggregateBounded:  true,
		maxExUnits:        lcommon.ExUnits{Memory: 500, Steps: 388},
		maxRefScriptBytes: 499,
	}, caps)

	pparams := scaleTestPParams()
	pparams.MaxEndorserBlockTxsSize = 100
	caps, ok = effectiveLeiosEBCapsScaled(operator, pparams, 0.7)
	require.True(t, ok)
	require.Equal(t, uint64(70-1), caps.maxBytes,
		"0.7 is a hair under 7/10, so 100 of it is 69 and a fraction")
}

// The operator's caps are an extra bound, not part of the protocol capacity,
// so the scale shrinks only the protocol's side of the minimum.
func TestEffectiveLeiosEBCapsScaleLeavesOperatorCaps(t *testing.T) {
	t.Parallel()
	pparams := scaleTestPParams()

	caps, ok := effectiveLeiosEBCapsScaled(
		leiosEBCaps{maxRefs: 7, maxBytes: 300, maxManifestBytes: 9},
		pparams,
		0.5,
	)
	require.True(t, ok)
	require.Equal(
		t,
		uint64(7),
		caps.maxRefs,
		"references have no protocol bound",
	)
	require.Equal(t, uint64(300), caps.maxBytes, "300 is below the scaled 500")
	require.Equal(t, uint64(9), caps.maxManifestBytes)

	caps, ok = effectiveLeiosEBCapsScaled(
		leiosEBCaps{maxBytes: 600},
		pparams,
		0.5,
	)
	require.True(t, ok)
	require.Equal(t, uint64(500), caps.maxBytes, "600 is above the scaled 500")
}

// A scale of one is the protocol capacity itself, and values outside [0, 1]
// cannot grow it.
func TestEffectiveLeiosEBCapsScaleBoundaries(t *testing.T) {
	t.Parallel()
	pparams := scaleTestPParams()
	operator := leiosEBCaps{maxRefs: 3}
	full, ok := effectiveLeiosEBCaps(operator, pparams)
	require.True(t, ok)

	for _, scale := range []float64{1, 1.5, math.Inf(1), math.NaN()} {
		caps, ok := effectiveLeiosEBCapsScaled(operator, pparams, scale)
		require.True(t, ok, "scale %v", scale)
		require.Equal(t, full, caps, "scale %v", scale)
	}

	// The smallest whole capacity: one byte still scales to nothing at
	// half, and a bound of nothing admits no endorser block.
	tiny := scaleTestPParams()
	tiny.MaxEndorserBlockTxsSize = 1
	caps, ok := effectiveLeiosEBCapsScaled(operator, tiny, 0.5)
	require.False(t, ok)
	require.Equal(t, leiosEBCaps{}, caps)
	_, ok = effectiveLeiosEBCapsScaled(operator, pparams, 0)
	require.False(t, ok)
	_, ok = effectiveLeiosEBCapsScaled(operator, pparams, -1)
	require.False(t, ok)

	// A zero aggregate bound stays a bound that admits only free
	// transactions, however it scales.
	zero := scaleTestPParams()
	zero.MaxEndorserBlockExUnits = lcommon.ExUnits{}
	zero.MaxRefScriptSizePerEndorserBlock = 0
	caps, ok = effectiveLeiosEBCapsScaled(operator, zero, 0.5)
	require.True(t, ok)
	require.True(t, caps.aggregateBounded)
	require.Equal(t, lcommon.ExUnits{}, caps.maxExUnits)
	require.Zero(t, caps.maxRefScriptBytes)

	// A manifest bound within the framing has no references to scale.
	narrow := scaleTestPParams()
	narrow.MaxEndorserBlockReferencesSize = 5
	caps, ok = effectiveLeiosEBCapsScaled(operator, narrow, 0.5)
	require.True(t, ok)
	require.Equal(t, uint64(5), caps.maxManifestBytes)
}

// The scale the forger reads at the slot bounds the endorser block it
// announces, and a later scale applies to the next one.
func TestCheckAndForgeLeiosEBAppliesCapacityScale(t *testing.T) {
	t.Parallel()
	txs := leiosCapacityCandidates(t, 10, 0)
	txSize := len(blockTxFromMempool(t, txs[0].Cbor))
	pparams := &dijkstra.DijkstraProtocolParameters{
		MaxEndorserBlockTxsSize: uint32(
			8 * txSize,
		), // #nosec G115 -- small fixture
		MaxEndorserBlockReferencesSize: 40000,
	}
	scale := 1.0
	caster := &forgerTestLeiosCaster{}
	forger := leiosCapacityForger(txs, pparams, caster)
	forger.ebCapacityScale = func() float64 { return scale }

	_, err := forger.checkAndForgeLeiosEB(context.Background(), 10,
		leiosSelectionBasis{})
	require.NoError(t, err)
	require.Len(t, caster.txBodies, 8)

	scale = 0.5
	_, err = forger.checkAndForgeLeiosEB(context.Background(), 11,
		leiosSelectionBasis{})
	require.NoError(t, err)
	require.Len(t, caster.txBodies, 4)

	scale = 0.25
	_, err = forger.checkAndForgeLeiosEB(context.Background(), 12,
		leiosSelectionBasis{})
	require.NoError(t, err)
	require.Len(t, caster.txBodies, 2)
}

// The scaled execution-unit and reference-script bounds apply to the
// endorser block a forger selects, each on its own: a sum at the scaled bound
// fits and one more transaction does not. Half of an odd protocol bound
// rounds down, so the bound one above twice the sum fits it exactly and the
// bound twice the sum minus one does not.
func TestCheckAndForgeLeiosEBAppliesScaleToAggregateBounds(t *testing.T) {
	t.Parallel()
	each := leiosAggregateCost{memory: 10, steps: 100, refScriptBytes: 1000}
	const unbounded = 1 << 30
	for _, tc := range []struct {
		name   string
		mem    int64
		steps  int64
		ref    uint32
		wantTx int
	}{
		{"memory at the scaled bound", 81, unbounded, unbounded, 4},
		{"memory one under the scaled bound", 79, unbounded, unbounded, 3},
		{"steps at the scaled bound", unbounded, 801, unbounded, 4},
		{"steps one under the scaled bound", unbounded, 799, unbounded, 3},
		{"reference scripts at the scaled bound", unbounded, unbounded, 8001, 4},
		{
			"reference scripts one under the scaled bound",
			unbounded, unbounded, 7999, 3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			txs, validator := leiosAggregateCandidates(
				t, repeatLeiosCost(each, 5),
			)
			caster := &forgerTestLeiosCaster{}
			forger := leiosCapacityForger(txs,
				&dijkstra.DijkstraProtocolParameters{
					MaxEndorserBlockTxsSize:        1 << 20,
					MaxEndorserBlockReferencesSize: 40000,
					MaxEndorserBlockExUnits: lcommon.ExUnits{
						Memory: tc.mem,
						Steps:  tc.steps,
					},
					MaxRefScriptSizePerEndorserBlock: tc.ref,
				},
				caster,
			)
			forger.leiosValidator = validator

			forger.ebCapacityScale = func() float64 { return 1 }
			_, err := forger.checkAndForgeLeiosEB(context.Background(), 10,
				leiosSelectionBasis{})
			require.NoError(t, err)
			require.Len(t, caster.txBodies, 5,
				"the whole protocol capacity holds every transaction")

			forger.ebCapacityScale = func() float64 { return 0.5 }
			_, err = forger.checkAndForgeLeiosEB(context.Background(), 11,
				leiosSelectionBasis{})
			require.NoError(t, err)
			require.Len(t, caster.txBodies, tc.wantTx)
		})
	}
}
