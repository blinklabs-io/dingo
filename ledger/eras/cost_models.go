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

import "fmt"

// requiredCostModel returns the cost model for a Plutus language version, or
// an error when the protocol parameters don't carry one at all. A plain map
// index (pp.CostModels[key]) returns a nil slice for a missing entry, and
// plutigo's costModelFromList treats a nil or empty cost-model slice as "use
// the built-in default cost model" instead of failing -- evaluating a
// script under the wrong (default, not this network's configured) cost
// parameters instead of refusing to evaluate it at all. Callers must use
// this instead of indexing CostModels directly.
//
// This deliberately does NOT reject a present-but-short list against
// plutigo's current (protocol-version-11) parameter-table length
// (lang.GetParamNamesForVersion): the cost-model parameter count has grown
// across hard forks, and a shorter list is the CORRECT, complete shape for
// an earlier protocol version, not a truncated one. Real shipped genesis
// data proves this: config/cardano/{mainnet,preprod,preview}/alonzo-genesis.json
// each carry a 166-entry PlutusV1 model (Alonzo's actual param count before
// later hard forks added more), and babbage.go's DefaultPlutusV2CostModel
// (the real canonical mainnet value used before an on-chain V2 model
// lands) is 175 entries -- both legitimately far short of the current
// 332-parameter table, and both must still validate to keep replaying
// mainnet/testnet history from genesis (a length floor against the current
// table rejected both and stalled sync at the first Alonzo/Babbage Plutus
// transaction). Matches the equivalent presence check gouroboros' own
// UtxoValidateCostModelsPresent rule already performs
// (!ok || len(model) == 0) elsewhere in the validation pipeline.
func requiredCostModel(
	costModels map[uint][]int64,
	key uint,
	versionName string,
) ([]int64, error) {
	model, ok := costModels[key]
	if !ok || len(model) == 0 {
		return nil, fmt.Errorf(
			"missing %s cost model in protocol parameters",
			versionName,
		)
	}
	return model, nil
}

// cloneCostModels gives hard-fork parameter conversion ownership of both the
// map and its slice values. The upstream UpgradePParams helpers copy structs,
// so without this step a conversion can mutate the previous era's parameters.
func cloneCostModels(values map[uint][]int64) map[uint][]int64 {
	if values == nil {
		return nil
	}
	ret := make(map[uint][]int64, len(values))
	for language, model := range values {
		ret[language] = append([]int64(nil), model...)
	}
	return ret
}
