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

package ledger

// BlockProcessingSlot returns the highest slot successfully processed inside
// a block-apply transaction during this LedgerState's lifetime. It is a local
// work signal, not a committed tip or a validation/readiness verdict. It never
// decreases, so retrying the same failed batch cannot report fresh progress.
func (ls *LedgerState) BlockProcessingSlot() uint64 {
	return ls.processedBlockSlot.Load()
}

func (ls *LedgerState) recordProcessedBlockSlot(slot uint64) {
	for previous := ls.processedBlockSlot.Load(); slot > previous; previous = ls.processedBlockSlot.Load() {
		if ls.processedBlockSlot.CompareAndSwap(previous, slot) {
			return
		}
	}
}
