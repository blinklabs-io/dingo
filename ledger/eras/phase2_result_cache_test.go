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

package eras

import (
	"errors"
	"math/big"
	"slices"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// cachingPreprodLedgerState is a preprod ledger state that provides a
// Phase2ResultCache, as ledger.LedgerView does.
type cachingPreprodLedgerState struct {
	preprodLedgerState
	cache *Phase2ResultCache
}

func (s cachingPreprodLedgerState) Phase2ResultCache() *Phase2ResultCache {
	return s.cache
}

// shiftedClockLedgerState converts slots to times a fixed offset later, as a
// different era history would.
type shiftedClockLedgerState struct {
	preprodLedgerState
}

func (s shiftedClockLedgerState) SlotToTime(slot uint64) (time.Time, error) {
	t, err := s.preprodLedgerState.SlotToTime(slot)
	return t.Add(time.Second), err
}

// preprodValidationProtocolParams extends the fixture parameters, which carry
// only what script evaluation reads, with preprod's values for the phase-1
// rules ValidateTxConway runs first.
func preprodValidationProtocolParams(
	t testing.TB,
) *conway.ConwayProtocolParameters {
	t.Helper()
	pp := preprodFixtureProtocolParams(t)
	pp.MinFeeA = 44
	pp.MinFeeB = 155381
	pp.MaxBlockBodySize = 90112
	pp.MaxTxSize = 16384
	pp.MaxBlockHeaderSize = 1100
	pp.KeyDeposit = 2000000
	pp.PoolDeposit = 500000000
	pp.MaxEpoch = 18
	pp.AdaPerUtxoByte = 4310
	pp.ExecutionCosts = lcommon.ExUnitPrice{
		MemPrice:  &cbor.Rat{Rat: big.NewRat(577, 10000)},
		StepPrice: &cbor.Rat{Rat: big.NewRat(721, 10000000)},
	}
	pp.MaxBlockExUnits = lcommon.ExUnits{
		Memory: 62000000,
		Steps:  20000000000,
	}
	pp.MaxValueSize = 5000
	pp.CollateralPercentage = 150
	pp.MaxCollateralInputs = 3
	pp.GovActionDeposit = 100000000000
	pp.DRepDeposit = 500000000
	pp.MinFeeRefScriptCostPerByte = &cbor.Rat{Rat: big.NewRat(15, 1)}
	return pp
}

type preprodPhase2Case struct {
	tx     lcommon.Transaction
	slot   uint64
	ls     preprodLedgerState
	inputs []lcommon.Utxo
}

func loadPreprodPhase2Case(
	t testing.TB,
	fixture preprodPlutusFixture,
) preprodPhase2Case {
	t.Helper()
	blk, err := gledger.NewBlockFromCbor(
		gledger.BlockTypeConway,
		readErasFixture(t, fixture.blockFile),
	)
	require.NoError(t, err)
	var tx lcommon.Transaction
	for _, candidate := range blk.Transactions() {
		if candidate.Hash().String() == fixture.txId {
			tx = candidate
		}
	}
	require.NotNil(t, tx)
	var inputTxBytes [][]byte
	_, err = cbor.Decode(readErasFixture(t, fixture.inputsFile), &inputTxBytes)
	require.NoError(t, err)
	ls := preprodLedgerState{mockLedgerState: newMockLedgerState()}
	ls.networkId = uint(lcommon.AddressNetworkTestnet)
	for _, raw := range inputTxBytes {
		inputTx, err := conway.NewConwayTransactionFromCbor(raw)
		require.NoError(t, err)
		for idx, output := range inputTx.Outputs() {
			ls.addUtxo(
				shelley.MustNewShelleyTransactionInput(
					inputTx.Hash().String(),
					idx,
				),
				output,
			)
		}
	}
	var inputs []lcommon.Utxo
	for _, input := range slices.Concat(tx.Inputs(), tx.ReferenceInputs()) {
		utxo, err := ls.UtxoById(input)
		require.NoError(t, err)
		inputs = append(inputs, utxo)
	}
	return preprodPhase2Case{
		tx:     tx,
		slot:   blk.SlotNumber(),
		ls:     ls,
		inputs: inputs,
	}
}

// countConwayPhase2 replaces validateConwayPhase2 with a wrapper that counts
// its calls, for the duration of the test. Tests that use it must not run in
// parallel.
func countConwayPhase2(t *testing.T, result func() error) *int {
	t.Helper()
	calls := 0
	original := validateConwayPhase2
	validateConwayPhase2 = func(
		tx lcommon.Transaction,
		ls lcommon.LedgerState,
		pp *conway.ConwayProtocolParameters,
		plutusCtx conwayPlutusValidationContext,
		validateRequiredRedeemers bool,
	) error {
		calls++
		if result != nil {
			return result()
		}
		return original(tx, ls, pp, plutusCtx, validateRequiredRedeemers)
	}
	t.Cleanup(func() { validateConwayPhase2 = original })
	return &calls
}

func TestPhase2ResultCacheSkipsRepeatedEvaluation(t *testing.T) {
	pp := preprodValidationProtocolParams(t)
	for _, fixture := range preprodPlutusFixtures {
		t.Run(fixture.name, func(t *testing.T) {
			c := loadPreprodPhase2Case(t, fixture)
			calls := countConwayPhase2(t, nil)
			ls := cachingPreprodLedgerState{
				preprodLedgerState: c.ls,
				cache:              NewPhase2ResultCache(),
			}
			for range 3 {
				require.NoError(t, ValidateTxConway(c.tx, c.slot, ls, pp))
			}
			require.Equal(t, 1, *calls, "scripts must run once")

			// Without a cache every validation runs the scripts.
			*calls = 0
			for range 2 {
				require.NoError(t, ValidateTxConway(c.tx, c.slot, c.ls, pp))
			}
			require.Equal(t, 2, *calls)
		})
	}
}

func TestPhase2ResultCacheDoesNotCacheFailure(t *testing.T) {
	pp := preprodValidationProtocolParams(t)
	c := loadPreprodPhase2Case(t, preprodPlutusFixtures[0])
	scriptErr := errors.New("script failed")
	calls := countConwayPhase2(t, func() error { return scriptErr })
	ls := cachingPreprodLedgerState{
		preprodLedgerState: c.ls,
		cache:              NewPhase2ResultCache(),
	}
	for range 2 {
		require.ErrorIs(t, ValidateTxConway(c.tx, c.slot, ls, pp), scriptErr)
	}
	require.Equal(t, 2, *calls, "a failure must be re-evaluated")
}

func TestConwayPhase2ResultKeyCoversEvaluationInputs(t *testing.T) {
	pp := preprodFixtureProtocolParams(t)
	c := loadPreprodPhase2Case(t, preprodPlutusFixtures[0])
	key := func(
		ls lcommon.LedgerState,
		pp *conway.ConwayProtocolParameters,
		inputs []lcommon.Utxo,
		synthetic bool,
	) phase2ResultKey {
		t.Helper()
		k, ok := conwayPhase2ResultKey(c.tx, ls, pp, inputs, synthetic)
		require.True(t, ok)
		return k
	}
	base := key(c.ls, pp, c.inputs, false)
	require.Equal(t, base, key(c.ls, pp, c.inputs, false), "key must be stable")

	clonePP := func() *conway.ConwayProtocolParameters {
		p := *pp
		p.CostModels = make(map[uint][]int64, len(pp.CostModels))
		for language, params := range pp.CostModels {
			p.CostModels[language] = slices.Clone(params)
		}
		return &p
	}
	otherOutput := c.inputs[1].Output
	changedInputs := make([]lcommon.Utxo, len(c.inputs))
	copy(changedInputs, c.inputs)
	changedInputs[0].Output = otherOutput

	variants := map[string]phase2ResultKey{
		"synthetic V2 cost model": key(c.ls, pp, c.inputs, true),
		"resolved output":         key(c.ls, pp, changedInputs, false),
		"fewer inputs":            key(c.ls, pp, c.inputs[1:], false),
		"slot to time": key(
			shiftedClockLedgerState{c.ls}, pp, c.inputs, false,
		),
		"protocol major": key(c.ls, func() *conway.ConwayProtocolParameters {
			p := clonePP()
			p.ProtocolVersion.Major++
			return p
		}(), c.inputs, false),
		"protocol minor": key(c.ls, func() *conway.ConwayProtocolParameters {
			p := clonePP()
			p.ProtocolVersion.Minor++
			return p
		}(), c.inputs, false),
		"max tx execution units": key(c.ls, func() *conway.ConwayProtocolParameters {
			p := clonePP()
			p.MaxTxExUnits.Steps++
			return p
		}(), c.inputs, false),
		"cost model parameter": key(c.ls, func() *conway.ConwayProtocolParameters {
			p := clonePP()
			p.CostModels[2][0]++
			return p
		}(), c.inputs, false),
		"cost model length": key(c.ls, func() *conway.ConwayProtocolParameters {
			p := clonePP()
			p.CostModels[2] = append(p.CostModels[2], 0)
			return p
		}(), c.inputs, false),
		"another language": key(c.ls, func() *conway.ConwayProtocolParameters {
			p := clonePP()
			p.CostModels[1] = slices.Clone(p.CostModels[2])
			return p
		}(), c.inputs, false),
	}
	for name, k := range variants {
		require.NotEqual(t, base, k, "%s must change the key", name)
	}

	other := loadPreprodPhase2Case(t, preprodPlutusFixtures[1])
	otherKey, ok := conwayPhase2ResultKey(other.tx, c.ls, pp, c.inputs, false)
	require.True(t, ok)
	require.NotEqual(t, base, otherKey, "the transaction must change the key")
}

func TestPhase2ResultCacheKeepsRecentKeys(t *testing.T) {
	c := NewPhase2ResultCache()
	keyOf := func(i int) phase2ResultKey {
		var k phase2ResultKey
		k[0], k[1], k[2] = byte(i), byte(i>>8), byte(i>>16)
		return k
	}
	// Fill one generation, then keep key 0 in use while filling another.
	for i := range phase2ResultCacheGeneration {
		c.add(keyOf(i))
	}
	for i := phase2ResultCacheGeneration; i < 2*phase2ResultCacheGeneration-1; i++ {
		c.add(keyOf(i))
		if i == phase2ResultCacheGeneration {
			require.True(t, c.contains(keyOf(0)))
		}
	}
	// Admitting one more key starts a third generation and drops the first,
	// except key 0, which the lookup moved forward.
	c.add(keyOf(2 * phase2ResultCacheGeneration))
	require.True(t, c.contains(keyOf(0)))
	require.False(t, c.contains(keyOf(1)))
	require.True(t, c.contains(keyOf(2*phase2ResultCacheGeneration-2)))
	require.True(t, c.contains(keyOf(2*phase2ResultCacheGeneration)))
}

// BenchmarkValidateTxConwayPhase2Cache measures validating a transaction the
// node has already validated, with and without the phase-2 result cache.
func BenchmarkValidateTxConwayPhase2Cache(b *testing.B) {
	pp := preprodValidationProtocolParams(b)
	for _, fixture := range preprodPlutusFixtures {
		c := loadPreprodPhase2Case(b, fixture)
		for _, cached := range []bool{false, true} {
			name := fixture.name + "/uncached"
			var ls lcommon.LedgerState = c.ls
			if cached {
				name = fixture.name + "/cached"
				ls = cachingPreprodLedgerState{
					preprodLedgerState: c.ls,
					cache:              NewPhase2ResultCache(),
				}
			}
			b.Run(name, func(b *testing.B) {
				require.NoError(b, ValidateTxConway(c.tx, c.slot, ls, pp))
				b.ReportAllocs()
				for b.Loop() {
					if err := ValidateTxConway(c.tx, c.slot, ls, pp); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
