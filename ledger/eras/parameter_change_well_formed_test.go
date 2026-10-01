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
	"fmt"
	"math"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

// ParameterChange update keys (Conway CDDL, shared by Dijkstra).
const (
	ppuKeyMaxBlockBodySize = 2
	ppuKeyMaxTxSize        = 3
	ppuKeyMaxBHSize        = 4
	ppuKeyPoolDeposit      = 6
	ppuKeyMaxEpoch         = 7
	ppuKeyNOpt             = 8
	ppuKeyAdaPerUtxoByte   = 17
	ppuKeyCostModels       = 18
	ppuKeyMaxValueSize     = 22
	ppuKeyCollateralPct    = 23
	ppuKeyMaxCollInputs    = 24
	ppuKeyMinCommittee     = 27
	ppuKeyCommitteeTerm    = 28
	ppuKeyGovActionPeriod  = 29
	ppuKeyGovActionDeposit = 30
	ppuKeyDRepDeposit      = 31
	ppuKeyDRepInactivity   = 32
	ppuKeyRefScriptMult    = 37
	ppuKeyMinPoolMargin    = 39
	ppuKeyLeiosQuorum      = 44
	ppuKeyMaxEBExUnits     = 47
)

func ppuRat(num, den int64) cbor.Tag {
	return cbor.Tag{Number: 30, Content: []any{num, den}}
}

// parameterChangeTxCbor builds a transaction carrying one ParameterChange
// proposal with update map ppu. The transaction is otherwise minimal: these
// tests assert which rule rejects the proposal, not that the whole
// transaction is valid.
func parameterChangeTxCbor(t *testing.T, ppu map[uint]any) []byte {
	t.Helper()
	inputHash := make([]byte, 32)
	inputHash[0] = 0xaa
	addr := make([]byte, 29)
	addr[0] = 0x60
	rewardAccount := make([]byte, 29)
	rewardAccount[0] = 0xe0
	body := map[uint]any{
		0: cbor.Tag{
			Number:  258,
			Content: []any{[]any{inputHash, uint64(0)}},
		},
		1: []any{[]any{addr, uint64(1_000_000)}},
		2: uint64(200_000),
		20: []any{
			[]any{
				uint64(1_000_000_000),
				rewardAccount,
				[]any{
					uint64(lcommon.GovActionTypeParameterChange),
					nil,
					ppu,
					nil,
				},
				[]any{"https://example.invalid/a", make([]byte, 32)},
			},
		},
	}
	txCbor, err := cbor.Encode([]any{body, map[uint]any{}, true, nil})
	require.NoError(t, err)
	return txCbor
}

// validateParameterChange runs the production validator for era over a
// transaction carrying ppu. A decode failure is reported with a "decode:"
// prefix: the reference also rejects an undecodable update.
func validateParameterChange(
	t *testing.T,
	era string,
	major uint,
	ppu map[uint]any,
) error {
	t.Helper()
	txCbor := parameterChangeTxCbor(t, ppu)
	pp := conwayDivergencePparams()
	pp.ProtocolVersion.Major = major
	switch era {
	case "Conway":
		tx, err := conway.NewConwayTransactionFromCbor(txCbor)
		if err != nil {
			return fmt.Errorf("decode: %w", err)
		}
		return ValidateTxConway(tx, 0, newMockLedgerState(), pp)
	case "Dijkstra":
		tx, err := gdijkstra.NewDijkstraTransactionFromCbor(txCbor)
		if err != nil {
			return fmt.Errorf("decode: %w", err)
		}
		return ValidateTxDijkstra(
			tx,
			0,
			newMockLedgerState(),
			&gdijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: *pp,
			},
		)
	}
	t.Fatalf("unknown era %q", era)
	return nil
}

// requireRejectedBy asserts err names the rule text want, so a rejection by
// an unrelated rule (bad inputs, fee) cannot satisfy the test.
func requireRejectedBy(t *testing.T, err error, want string) {
	t.Helper()
	require.Error(t, err)
	require.ErrorContains(t, err, want)
}

// requireNotRejectedBy asserts err, if any, does not mention any of the
// parameter-change diagnostics; these minimal transactions still fail
// unrelated rules.
func requireNotRejectedBy(t *testing.T, err error, unwanted ...string) {
	t.Helper()
	if err == nil {
		return
	}
	for _, s := range unwanted {
		require.NotContains(t, err.Error(), s)
	}
}

// TestValidateTxParameterChangeZeroFields covers dingo#4438: six
// unconditional zero-valued fields, and the PV-gated AdaPerUtxoByte (PV10+)
// and NOpt (PV11+) boundaries.
func TestValidateTxParameterChangeZeroFields(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		major      uint
		ppu        map[uint]any
		diagnostic string
		reject     bool
		conwayOnly bool // the case is below Dijkstra's PV12 floor
	}{
		{
			"CollateralPercentage",
			9,
			map[uint]any{ppuKeyCollateralPct: 0},
			"collateralPercentage",
			true,
			false,
		},
		{
			"CommitteeTermLimit",
			9,
			map[uint]any{ppuKeyCommitteeTerm: 0},
			"committeeMaxTermLength",
			true,
			false,
		},
		{
			"GovActionValidityPeriod",
			9,
			map[uint]any{ppuKeyGovActionPeriod: 0},
			"govActionLifetime",
			true,
			false,
		},
		{
			"PoolDeposit",
			9,
			map[uint]any{ppuKeyPoolDeposit: 0},
			"poolDeposit",
			true,
			false,
		},
		{
			"GovActionDepositPV11",
			11,
			map[uint]any{ppuKeyGovActionDeposit: 0},
			"govActionDeposit",
			true,
			false,
		},
		{
			"DRepDeposit",
			9,
			map[uint]any{ppuKeyDRepDeposit: 0},
			"drepDeposit",
			true,
			false,
		},
		{
			"AdaPerUtxoBytePV10",
			10,
			map[uint]any{ppuKeyAdaPerUtxoByte: 0},
			"coinsPerUTxOByte",
			true,
			false,
		},
		{
			"AdaPerUtxoBytePV11",
			11,
			map[uint]any{ppuKeyAdaPerUtxoByte: 0},
			"coinsPerUTxOByte",
			true,
			false,
		},
		{
			"NOptPV11",
			11,
			map[uint]any{ppuKeyNOpt: 0},
			"nOptimalPoolCount",
			true,
			false,
		},
		{
			"AdaPerUtxoBytePV9Allowed",
			9,
			map[uint]any{ppuKeyAdaPerUtxoByte: 0},
			"coinsPerUTxOByte",
			false,
			true,
		},
		{
			"NOptPV9Allowed",
			9,
			map[uint]any{ppuKeyNOpt: 0},
			"nOptimalPoolCount",
			false,
			true,
		},
		{
			"NOptPV10Allowed",
			10,
			map[uint]any{ppuKeyNOpt: 0},
			"nOptimalPoolCount",
			false,
			true,
		},
		{
			"CollateralPercentageNonzero",
			9,
			map[uint]any{ppuKeyCollateralPct: 1},
			"collateralPercentage",
			false,
			false,
		},
		{
			"GovActionDepositNonzero",
			11,
			map[uint]any{ppuKeyGovActionDeposit: 1},
			"govActionDeposit",
			false,
			false,
		},
		{
			"AdaPerUtxoByteNonzero",
			11,
			map[uint]any{ppuKeyAdaPerUtxoByte: 1},
			"coinsPerUTxOByte",
			false,
			false,
		},
		{
			"NOptNonzero",
			11,
			map[uint]any{ppuKeyNOpt: 1},
			"nOptimalPoolCount",
			false,
			false,
		},
	}
	for _, era := range []string{"Conway", "Dijkstra"} {
		for _, tc := range cases {
			major := tc.major
			if era == "Dijkstra" {
				if tc.conwayOnly {
					continue
				}
				major = gdijkstra.MinProtocolVersionDijkstra
			}
			t.Run(era+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				err := validateParameterChange(t, era, major, tc.ppu)
				if tc.reject {
					requireRejectedBy(t, err, tc.diagnostic)
				} else {
					requireNotRejectedBy(t, err, tc.diagnostic, "decode:")
				}
			})
		}
	}
}

// TestValidateTxParameterChangeIntegerWidths covers dingo#4478: the exact
// maximum of each .size 2 and .size 4 field is accepted, maximum+1 rejected.
func TestValidateTxParameterChangeIntegerWidths(t *testing.T) {
	t.Parallel()
	fields := []struct {
		name string
		key  uint
		max  uint64
	}{
		{"maxBlockBodySize", ppuKeyMaxBlockBodySize, math.MaxUint32},
		{"maxTxSize", ppuKeyMaxTxSize, math.MaxUint32},
		{"maxEpoch", ppuKeyMaxEpoch, math.MaxUint32},
		{"maxValueSize", ppuKeyMaxValueSize, math.MaxUint32},
		{"committeeTermLimit", ppuKeyCommitteeTerm, math.MaxUint32},
		{"govActionValidityPeriod", ppuKeyGovActionPeriod, math.MaxUint32},
		{"dRepInactivityPeriod", ppuKeyDRepInactivity, math.MaxUint32},
		{"maxBlockHeaderSize", ppuKeyMaxBHSize, math.MaxUint16},
		{"nOpt", ppuKeyNOpt, math.MaxUint16},
		{"collateralPercentage", ppuKeyCollateralPct, math.MaxUint16},
		{"maxCollateralInputs", ppuKeyMaxCollInputs, math.MaxUint16},
		{"minCommitteeSize", ppuKeyMinCommittee, math.MaxUint16},
	}
	for _, era := range []string{"Conway", "Dijkstra"} {
		for _, f := range fields {
			t.Run(era+"/"+f.name+"/max", func(t *testing.T) {
				t.Parallel()
				err := validateParameterChange(
					t, era, 11, map[uint]any{f.key: f.max},
				)
				requireNotRejectedBy(t, err, "must fit Word", "decode:")
			})
			t.Run(era+"/"+f.name+"/max+1", func(t *testing.T) {
				t.Parallel()
				err := validateParameterChange(
					t, era, 11, map[uint]any{f.key: f.max + 1},
				)
				require.Error(t, err)
				require.ErrorContains(t, err, "must fit Word")
			})
		}
	}
}

// TestValidateTxParameterChangeDijkstraDomains covers dingo#4596: the
// Dijkstra-only tags and the inherited Conway tags Dijkstra must not bypass.
func TestValidateTxParameterChangeDijkstraDomains(t *testing.T) {
	t.Parallel()
	exUnits := func(mem, steps int64) []any { return []any{mem, steps} }
	cases := []struct {
		name       string
		ppu        map[uint]any
		diagnostic string
		reject     bool
	}{
		{
			"tag37-zero",
			map[uint]any{ppuKeyRefScriptMult: ppuRat(0, 1)},
			"refScriptCostMultiplier",
			true,
		},
		{
			"tag39-above-one",
			map[uint]any{ppuKeyMinPoolMargin: ppuRat(2, 1)},
			"minPoolMargin",
			true,
		},
		{
			"tag44-above-one",
			map[uint]any{ppuKeyLeiosQuorum: ppuRat(2, 1)},
			"leiosQuorumStakeThreshold",
			true,
		},
		{
			"tag47-negative",
			map[uint]any{ppuKeyMaxEBExUnits: exUnits(-1, 0)},
			"cannot unmarshal negative integer",
			true,
		},
		{
			"tag9-negative",
			map[uint]any{9: ppuRat(-1, 1)},
			"tag 9: rational numerator must be in Word64",
			true,
		},
		{
			"tag10-above-one",
			map[uint]any{10: ppuRat(2, 1)},
			"rho: must be in [0,1]",
			true,
		},
		{
			"tag11-above-one",
			map[uint]any{11: ppuRat(2, 1)},
			"tau: must be in [0,1]",
			true,
		},
		{
			"tag19-negative",
			map[uint]any{19: []any{ppuRat(-1, 1), ppuRat(1, 1)}},
			"tag 19: rational at array index 0: rational numerator must be in Word64",
			true,
		},
		{
			"tag20-negative",
			map[uint]any{20: exUnits(-1, 0)},
			"cannot unmarshal negative integer",
			true,
		},
		{
			"tag21-negative",
			map[uint]any{21: exUnits(0, -1)},
			"cannot unmarshal negative integer",
			true,
		},
		{
			"tag25-above-one",
			map[uint]any{
				25: []any{
					ppuRat(2, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
				},
			},
			"poolVotingThresholds",
			true,
		},
		{
			"tag26-above-one",
			map[uint]any{
				26: []any{
					ppuRat(2, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
				},
			},
			"drepVotingThresholds",
			true,
		},
		{
			"tag33-negative",
			map[uint]any{33: ppuRat(-1, 1)},
			"tag 33: rational numerator must be in Word64",
			true,
		},
		{
			"valid-field-with-null-tag39",
			map[uint]any{ppuKeyMaxTxSize: 16384, ppuKeyMinPoolMargin: nil},
			"tag 39",
			true,
		},
		{
			"tag39-zero",
			map[uint]any{ppuKeyMinPoolMargin: ppuRat(0, 1)},
			"minPoolMargin",
			false,
		},
		{
			"tag39-one",
			map[uint]any{ppuKeyMinPoolMargin: ppuRat(1, 1)},
			"minPoolMargin",
			false,
		},
		{
			"tag44-zero",
			map[uint]any{ppuKeyLeiosQuorum: ppuRat(0, 1)},
			"leiosQuorumStakeThreshold",
			false,
		},
		{
			"tag44-one",
			map[uint]any{ppuKeyLeiosQuorum: ppuRat(1, 1)},
			"leiosQuorumStakeThreshold",
			false,
		},
		{
			"tag47-zero",
			map[uint]any{ppuKeyMaxEBExUnits: exUnits(0, 0)},
			"maxEndorserBlockExUnits",
			false,
		},
		{
			"tag47-maxint64",
			map[uint]any{
				ppuKeyMaxEBExUnits: exUnits(math.MaxInt64, math.MaxInt64),
			},
			"maxEndorserBlockExUnits",
			false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateParameterChange(
				t, "Dijkstra", gdijkstra.MinProtocolVersionDijkstra, tc.ppu,
			)
			if tc.reject {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.diagnostic)
			} else {
				requireNotRejectedBy(t, err, tc.diagnostic, "decode:")
			}
		})
	}
}

// TestValidateTxParameterChangeCostModelLanguageIDWidth covers dingo#4607:
// a cost-model language ID above Word8 is rejected, 255 stays an accepted
// unknown future language.
func TestValidateTxParameterChangeCostModelLanguageIDWidth(t *testing.T) {
	t.Parallel()
	for _, era := range []string{"Conway", "Dijkstra"} {
		t.Run(era+"/256", func(t *testing.T) {
			t.Parallel()
			err := validateParameterChange(t, era, 11, map[uint]any{
				ppuKeyCostModels: map[uint]any{256: []int64{1, 2, 3}},
			})
			require.Error(t, err)
			require.ErrorContains(t, err, "exceeds Word8 maximum 255")
		})
		t.Run(era+"/255", func(t *testing.T) {
			t.Parallel()
			err := validateParameterChange(t, era, 11, map[uint]any{
				ppuKeyCostModels: map[uint]any{255: []int64{1, 2, 3}},
			})
			requireNotRejectedBy(t, err, "costModels", "language", "decode:")
		})
	}
}
