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
	"testing"

	"github.com/blinklabs-io/plutigo/lang"
	"github.com/stretchr/testify/require"
)

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
