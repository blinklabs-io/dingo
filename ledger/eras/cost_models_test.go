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
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/stretchr/testify/require"
)

// TestRequiredCostModelFailsClosedOnMissingEntry is a regression test for
// issue #3528: a missing Plutus cost model must return a configuration
// error rather than silently reaching script evaluation. Before
// requiredCostModel existed, ledger/eras/{alonzo,babbage,conway}.go indexed
// pp.CostModels[key] directly; a missing key returns Go's zero value (a nil
// slice) rather than an error, and plutigo's costModelFromList treats a
// nil/short cost-model slice as "use the built-in default cost model"
// instead of failing -- silently evaluating scripts under the wrong,
// not-this-network's cost parameters.
func TestRequiredCostModelFailsClosedOnMissingEntry(t *testing.T) {
	t.Run("missing key returns a configuration error", func(t *testing.T) {
		_, err := requiredCostModel(nil, 2, "PlutusV3")
		require.Error(t, err)
		require.ErrorContains(t, err, "missing PlutusV3 cost model")
	})
	t.Run(
		"present but empty model returns a configuration error",
		func(t *testing.T) {
			_, err := requiredCostModel(
				map[uint][]int64{1: {}},
				1,
				"PlutusV2",
			)
			require.Error(t, err)
			require.ErrorContains(t, err, "missing PlutusV2 cost model")
		},
	)
	t.Run(
		"present, short-but-real-for-its-era model is accepted",
		func(t *testing.T) {
			// Regression test for a human-review finding: requiredCostModel
			// must NOT reject a cost model just because it is shorter than
			// plutigo's current (protocol-version-11) parameter table
			// (lang.GetParamNamesForVersion). The parameter count has grown
			// across hard forks, and a shorter list can be the correct,
			// complete shape for an earlier protocol version. Three entries
			// is nowhere near a complete model for any real era, but it is
			// non-empty, which is all this function can safely require --
			// see requiredCostModel's doc comment for why a length floor
			// against the current table is unsafe (it rejects the real
			// mainnet/preprod/preview Alonzo genesis's 166-entry PlutusV1
			// model, proven by TestRequiredCostModelAcceptsRealAlonzoGenesisModel
			// below).
			model := []int64{1, 2, 3}
			got, err := requiredCostModel(
				map[uint][]int64{1: model},
				1,
				"PlutusV2",
			)
			require.NoError(t, err)
			require.Equal(t, model, got)
		},
	)
	t.Run("present, complete model returns it unchanged", func(t *testing.T) {
		model := syntheticFullCostModel(t, lang.LanguageVersionV2)
		got, err := requiredCostModel(
			map[uint][]int64{1: model},
			1,
			"PlutusV2",
		)
		require.NoError(t, err)
		require.Equal(t, model, got)
	})
}

// TestRequiredCostModelAcceptsRealAlonzoGenesisModel is a regression test
// for a human-review finding: requiredCostModel's earlier length floor
// (checked against plutigo's current, protocol-version-11 parameter table)
// rejected the PlutusV1 cost model this repository's own shipped mainnet
// genesis carries, stalling replay from genesis at the first Alonzo-era
// Plutus transaction. Decodes the real genesis file through
// AlonzoProtocolParameters.UpdateFromGenesis -- the actual production
// loading path -- rather than a synthetic fixture, so this can't pass by
// accident the way a hand-padded fixture could.
func TestRequiredCostModelAcceptsRealAlonzoGenesisModel(t *testing.T) {
	genesis, err := alonzo.NewAlonzoGenesisFromFile(
		"../../config/cardano/mainnet/alonzo-genesis.json",
	)
	require.NoError(t, err)

	pp := &alonzo.AlonzoProtocolParameters{}
	require.NoError(t, pp.UpdateFromGenesis(&genesis))

	model, ok := pp.CostModels[0]
	require.True(t, ok, "genesis must populate a PlutusV1 cost model")
	// The real, historically-correct Alonzo PlutusV1 parameter count --
	// far short of plutigo's current 332-parameter table.
	require.Len(t, model, 166)

	got, err := requiredCostModel(pp.CostModels, 0, "PlutusV1")
	require.NoError(
		t,
		err,
		"a real, complete-for-its-era genesis cost model must be accepted",
	)
	require.Equal(t, model, got)
}

// syntheticFullCostModel returns a cost model of exactly the length
// requiredCostModel requires for the given version, with every parameter
// set to the same placeholder value. It exists so tests across this package
// that need a complete (non-fallback) cost model to satisfy
// requiredCostModel's length check -- but aren't about cost-model realism,
// only about exercising phase-2 evaluation, budget comparison, or error
// classification -- don't have to hand-maintain a 332/350-entry literal
// pinned to a real network. Tests that assert specific CPU/memory numbers
// must treat those numbers as tied to this specific placeholder value, not
// to any real network's cost model.
func syntheticFullCostModel(
	t testing.TB,
	version lang.LanguageVersion,
) []int64 {
	t.Helper()
	names := lang.GetParamNamesForVersion(version)
	model := make([]int64, len(names))
	for i := range model {
		model[i] = 100
	}
	return model
}

// defaultMachineCostMachineCosts pins plutigo's cek.DefaultMachineCosts
// (cpu, mem), keyed by the "cek<Name>Cost" parameter prefix
// MachineCosts.update matches on. plutigo has no public API to read
// DefaultMachineCosts, so these are copied from cek/cost_model_machine.go's
// literal; a future plutigo bump that changes them would need this updated
// too, same as TestCostModelParameterTablesMatchLiveNetworks already
// tracks plutigo's parameter-table shape.
var defaultMachineCostMachineCosts = map[string][2]int64{
	"cekStartupCost": {100, 100},
	"cekVarCost":     {16000, 100},
	"cekConstCost":   {16000, 100},
	"cekLamCost":     {16000, 100},
	"cekDelayCost":   {16000, 100},
	"cekForceCost":   {16000, 100},
	"cekApplyCost":   {16000, 100},
	"cekBuiltinCost": {16000, 100},
	"cekConstrCost":  {16000, 100},
	"cekCaseCost":    {16000, 100},
}

// defaultMachineCostModel returns a complete cost model for the given
// version that reproduces plutigo's real DefaultMachineCosts for every CEK
// machine-step parameter, and a placeholder for every builtin-function cost
// parameter. A test whose script never invokes an actual Plutus builtin
// function (only constants/lambdas/application, no e.g. addInteger) gets
// the exact same evaluated cost plutigo's empty-cost-model fallback used to
// silently produce -- matching known-good, externally-verified reference
// numbers -- while still supplying requiredCostModel a complete,
// non-fallback-triggering list. A script that does invoke a builtin
// function needs its own model with real values for that builtin, since
// this helper's builtin-cost entries are placeholders.
func defaultMachineCostModel(
	t testing.TB,
	version lang.LanguageVersion,
) []int64 {
	t.Helper()
	names := lang.GetParamNamesForVersion(version)
	model := make([]int64, len(names))
	for i, name := range names {
		prefix, suffix, ok := strings.Cut(name, "-")
		if costs, isMachineCost := defaultMachineCostMachineCosts[prefix]; ok &&
			isMachineCost {
			switch suffix {
			case "exBudgetCPU":
				model[i] = costs[0]
				continue
			case "exBudgetMemory":
				model[i] = costs[1]
				continue
			}
		}
		// Builtin-function cost parameter: callers use scripts that never
		// invoke an actual builtin, so this value doesn't affect the
		// evaluated cost -- any valid placeholder works.
		model[i] = 100
	}
	return model
}

// TestCostModelParameterTablesMatchLiveNetworks pins plutigo's cost model
// parameter tables to the cost model lengths mainnet, preprod and preview all
// publish at protocol version 11. The three networks carry byte identical
// models, checked against Koios `epoch_params` for mainnet epoch 652, preprod
// 310 and preview 1404.
//
// The evaluator maps the on-chain cost model onto these tables positionally:
// cek.costModelFromList walks the parameter names and assigns data[i] to the
// name at index i, breaking out at i >= len(data). It does not error when the
// on-chain model is longer than the table, it silently ignores the excess. So
// a table shorter than what the chain publishes is not a compile failure or a
// runtime error, it is wrong ExUnits on the mainnet script path
// (ledger/eras/{alonzo,babbage,conway}.go pass pp.CostModels[...] straight
// into cek.NewEvalContext).
//
// plutigo v0.4.0 had 328/328/346 against these 332/332/350, and the shortfall
// was not a clean truncation: valueData and unValueData changed shape rather
// than gaining appended entries, so nine tail values landed on five
// differently shaped names before the last four were dropped. This test exists
// so the next upstream parameter addition fails here instead of silently
// mispricing builtins.
func TestCostModelParameterTablesMatchLiveNetworks(t *testing.T) {
	testCases := []struct {
		name    string
		version lang.LanguageVersion
		want    int
	}{
		{name: "PlutusV1", version: lang.LanguageVersionV1, want: 332},
		{name: "PlutusV2", version: lang.LanguageVersionV2, want: 332},
		{name: "PlutusV3", version: lang.LanguageVersionV3, want: 350},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Len(
				t,
				lang.GetParamNamesForVersion(testCase.version),
				testCase.want,
			)
		})
	}
}

// TestCostModelParameterTablesPriceValueDataBuiltins checks the specific
// parameters whose shape changed in plutigo v0.5.0. valueData's cpu and memory
// costs each split into an intercept and a slope, and unValueData's cpu went
// from intercept/slope to a three coefficient form while its memory gained a
// slope. Asserting the names, not just the count, catches a table that is the
// right length but carries the older single-argument forms.
func TestCostModelParameterTablesPriceValueDataBuiltins(t *testing.T) {
	required := []string{
		"valueData-cpu-arguments-intercept",
		"valueData-cpu-arguments-slope",
		"valueData-memory-arguments-intercept",
		"valueData-memory-arguments-slope",
		"unValueData-cpu-arguments-c0",
		"unValueData-cpu-arguments-c1",
		"unValueData-cpu-arguments-c2",
		"unValueData-memory-arguments-intercept",
		"unValueData-memory-arguments-slope",
	}
	versions := map[string]lang.LanguageVersion{
		"PlutusV1": lang.LanguageVersionV1,
		"PlutusV2": lang.LanguageVersionV2,
		"PlutusV3": lang.LanguageVersionV3,
	}
	for name, version := range versions {
		t.Run(name, func(t *testing.T) {
			params := lang.GetParamNamesForVersion(version)
			present := make(map[string]struct{}, len(params))
			for _, param := range params {
				present[param] = struct{}{}
			}
			for _, want := range required {
				require.Contains(t, present, want)
			}
		})
	}
}
