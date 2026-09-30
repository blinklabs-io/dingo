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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package database

import (
	"maps"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func babbageGuardPParams(major uint) *babbage.BabbageProtocolParameters {
	return &babbage.BabbageProtocolParameters{
		MinFeeA:            44,
		MinFeeB:            155381,
		MaxBlockBodySize:   90112,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
		ProtocolMajor:      major,
		CostModels: map[uint][]int64{
			0: make([]int64, 166),
			1: make([]int64, 175),
		},
		ExecutionCosts: lcommon.ExUnitPrice{
			MemPrice:  &cbor.Rat{Rat: big.NewRat(577, 10000)},
			StepPrice: &cbor.Rat{Rat: big.NewRat(721, 10000000)},
		},
		MaxTxExUnits: lcommon.ExUnits{Memory: 14000000, Steps: 10000000000},
	}
}

func cloneBabbageGuardPParams(
	pp *babbage.BabbageProtocolParameters,
) *babbage.BabbageProtocolParameters {
	c := *pp
	c.CostModels = make(map[uint][]int64, len(pp.CostModels))
	for k, v := range pp.CostModels {
		c.CostModels[k] = append([]int64(nil), v...)
	}
	return &c
}

func babbageGuardDecode(data []byte) (any, error) {
	var update babbage.BabbageProtocolParameterUpdate
	_, err := cbor.Decode(data, &update)
	return update, err
}

func babbageGuardApply(
	current lcommon.ProtocolParameters,
	update any,
) (lcommon.ProtocolParameters, error) {
	pp := current.(*babbage.BabbageProtocolParameters)
	u := update.(babbage.BabbageProtocolParameterUpdate)
	pp.Update(&u)
	return pp, nil
}

func babbageGuardClone(
	current lcommon.ProtocolParameters,
) (lcommon.ProtocolParameters, error) {
	return cloneBabbageGuardPParams(
		current.(*babbage.BabbageProtocolParameters),
	), nil
}

type babbageGuardOutcome struct {
	mode   string
	result *babbage.BabbageProtocolParameters
	input  *babbage.BabbageProtocolParameters
}

// runBabbageGuardEnactment stores one proposal from a single genesis key with
// quorum 1 and runs the boundary into epoch 4 through each enactment entry
// point. It requires no error: a refused update must skip enactment, not
// halt the boundary.
func runBabbageGuardEnactment(
	t *testing.T,
	proposal []byte,
	current *babbage.BabbageProtocolParameters,
) []babbageGuardOutcome {
	t.Helper()
	var outcomes []babbageGuardOutcome
	for _, mode := range []string{"compute", "forecast", "apply"} {
		db, err := newTestDatabase(t, &Config{DataDir: ""})
		require.NoError(t, err)
		txn := db.Transaction(true)
		for epoch, start := range map[uint64]uint64{
			classicTestSubmissionEpoch - 1: classicTestEpochStart - 100,
			classicTestSubmissionEpoch:     classicTestEpochStart,
		} {
			require.NoError(t, db.SetEpoch(
				start, epoch, nil, nil, nil, nil, 1, 1, 100, txn,
			))
		}
		require.NoError(t, db.SetPParamUpdate(
			[]byte{1}, proposal, classicTestEpochStart, classicTestSubmissionEpoch, txn,
		))
		input := cloneBabbageGuardPParams(current)
		var result lcommon.ProtocolParameters
		switch mode {
		case "compute":
			result, _, err = db.ComputeAndApplyPParamUpdates(
				classicTestEpochStart+100, classicTestEnactEpoch, 1, 1,
				input, babbageGuardDecode, babbageGuardApply, nil, txn,
			)
		case "forecast":
			result, err = db.ForecastPParamUpdates(
				classicTestEnactEpoch, 1, input,
				babbageGuardDecode, babbageGuardApply, babbageGuardClone, txn,
			)
		case "apply":
			result = input
			err = db.ApplyPParamUpdates(
				classicTestEpochStart+100, classicTestEnactEpoch, 1, 1,
				&result, babbageGuardDecode, babbageGuardApply, txn,
			)
		}
		require.NoError(t, err, mode)
		require.NoError(t, txn.Rollback())
		txn.Release()
		require.NoError(t, db.Close())
		outcomes = append(outcomes, babbageGuardOutcome{
			mode:   mode,
			result: result.(*babbage.BabbageProtocolParameters),
			input:  input,
		})
	}
	return outcomes
}

func rawCbor(t *testing.T, hexBytes ...byte) cbor.RawMessage {
	t.Helper()
	return cbor.RawMessage(hexBytes)
}

func requireGuardRefused(
	t *testing.T,
	proposal []byte,
	current *babbage.BabbageProtocolParameters,
) {
	t.Helper()
	for _, o := range runBabbageGuardEnactment(t, proposal, current) {
		require.Equal(t, current, o.result, o.mode+": parameters in effect changed")
		require.Equal(t, current, o.input, o.mode+": input parameters mutated")
	}
}

// An update whose values are outside the reference domain must not enact, even
// when it is already stored and reaches quorum, and must leave the parameters
// in effect untouched (the era update function mutates them in place).
func TestClassicPParamEnactmentRefusesOutOfDomainUpdate(t *testing.T) {
	t.Parallel()
	// cbor rational tag 30 followed by a two-element array.
	tag := []byte{0xd8, 0x1e, 0x82}
	rat := func(num []byte, den byte) cbor.RawMessage {
		return cbor.RawMessage(append(append(append([]byte{}, tag...), num...), den))
	}
	price := func(mem cbor.RawMessage) cbor.RawMessage {
		step := rat([]byte{0x01}, 0x01)
		out := []byte{0x82}
		out = append(out, mem...)
		out = append(out, step...)
		return out
	}
	for _, tc := range []struct {
		name   string
		fields map[uint64]any
	}{
		{"unit interval above one", map[uint64]any{10: rat([]byte{0x03}, 0x02)}},
		{
			"negative execution price",
			map[uint64]any{19: price(rat([]byte{0x39, 0x02, 0x40}, 0x0a))},
		},
		{
			"negative execution-unit limit",
			map[uint64]any{20: rawCbor(t, 0x82, 0x20, 0x05)},
		},
		{
			"integer beyond its width",
			map[uint64]any{3: uint64(1) << 32},
		},
		{
			"numerator beyond 64 bits",
			map[uint64]any{19: price(rat(
				[]byte{0xc2, 0x49, 1, 0, 0, 0, 0, 0, 0, 0, 0}, 0x01,
			))},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireGuardRefused(
				t, classicUpdate(t, tc.fields), babbageGuardPParams(8),
			)
		})
	}

	t.Run("interval endpoint still enacts", func(t *testing.T) {
		t.Parallel()
		current := babbageGuardPParams(8)
		for _, o := range runBabbageGuardEnactment(t, classicUpdate(t, map[uint64]any{
			10: rat([]byte{0x01}, 0x01),
		}), current) {
			require.NotNil(t, o.result.Rho, o.mode)
			require.Equal(t, int64(1), o.result.Rho.Num().Int64(), o.mode)
		}
	})
}

func cost(n int, v int64) []int64 {
	m := make([]int64, n)
	for i := range m {
		m[i] = v
	}
	return m
}

// Before protocol version 9 a cost-model update must name a known language
// with exactly its parameter count, or it must not enact.
func TestClassicPParamEnactmentRefusesMalformedCostModel(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		models map[uint][]int64
	}{
		{"unknown language", map[uint][]int64{9: cost(10, 1)}},
		{"short model", map[uint][]int64{0: cost(165, 1)}},
		{"over-long model", map[uint][]int64{1: cost(176, 1)}},
		{
			"malformed entry beside a valid one",
			map[uint][]int64{0: cost(166, 1), 1: cost(3, 1)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireGuardRefused(
				t,
				classicUpdate(t, map[uint64]any{18: tc.models}),
				babbageGuardPParams(8),
			)
		})
	}

	t.Run("partial update replaces one language", func(t *testing.T) {
		t.Parallel()
		current := babbageGuardPParams(8)
		for _, o := range runBabbageGuardEnactment(t, classicUpdate(t, map[uint64]any{
			18: map[uint][]int64{0: cost(166, 7)},
		}), current) {
			require.Equal(t, cost(166, 7), o.result.CostModels[0], o.mode)
			require.Equal(t, current.CostModels[1], o.result.CostModels[1], o.mode)
		}
	})

	t.Run("protocol version 9 boundary is not strict", func(t *testing.T) {
		t.Parallel()
		current := babbageGuardPParams(9)
		for _, o := range runBabbageGuardEnactment(t, classicUpdate(t, map[uint64]any{
			18: map[uint][]int64{0: cost(165, 7)},
		}), current) {
			want := maps.Clone(current.CostModels)
			want[0] = cost(165, 7)
			require.Equal(t, want, o.result.CostModels, o.mode)
		}
	})
}
