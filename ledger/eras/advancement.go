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

// IsValidEraAdvancement reports whether a block tagged with nextEraId
// can legitimately advance the ledger from currentEraId. Valid cases:
//
//   - Same era (in-era epoch rollover): nextEraId == currentEraId.
//   - One step forward: nextEraId is the era immediately following
//     currentEraId in the ordered Eras slice.
//
// All other cases — multi-step forward, any backwards step, and
// advancement to or from an era that is not present in the Eras slice
// — are rejected. Each era boundary on a healthy chain is crossed by a
// specific block from that era, so multi-step advancement implies a
// non-conforming block, a malformed snapshot, or a chain-selection bug;
// silently catching up by iterating each era's HardForkFunc would skip
// per-era epoch-based events that should have fired during the omitted
// span.
//
// Era IDs are not assumed to be contiguous integers; the check resolves
// each ID to its position in Eras and compares positions.
func IsValidEraAdvancement(currentEraId, nextEraId uint) bool {
	return IsValidEraAdvancementIn(Eras, currentEraId, nextEraId)
}

func IsValidEraAdvancementIn(
	eraList []EraDesc,
	currentEraId, nextEraId uint,
) bool {
	currentIdx := indexOfEraIn(eraList, currentEraId)
	if currentIdx < 0 {
		return false
	}
	if nextEraId == currentEraId {
		return true
	}
	nextIdx := indexOfEraIn(eraList, nextEraId)
	if nextIdx < 0 {
		return false
	}
	return nextIdx == currentIdx+1
}

func indexOfEraIn(eraList []EraDesc, eraId uint) int {
	for i := range eraList {
		if eraList[i].Id == eraId {
			return i
		}
	}
	return -1
}
