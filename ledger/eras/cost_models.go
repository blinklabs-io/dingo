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
