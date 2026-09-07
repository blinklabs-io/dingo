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
// an error when the protocol parameters don't carry one. A plain map index
// (pp.CostModels[key]) returns a nil slice for a missing entry, and
// plutigo's costModelFromList silently treats a nil/short cost-model slice
// as "use the built-in default cost model" instead of failing -- evaluating
// a script under the wrong (default, not this network's configured) cost
// parameters instead of refusing to evaluate it at all. Callers must use
// this instead of indexing CostModels directly.
func requiredCostModel(
	costModels map[uint][]int64,
	key uint,
	versionName string,
) ([]int64, error) {
	model, ok := costModels[key]
	if !ok {
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
