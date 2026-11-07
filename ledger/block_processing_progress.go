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
// a block-apply transaction and whether any block has completed during this
// LedgerState's lifetime. It is a local work signal, not a committed tip or a
// validation/readiness verdict. The slot never decreases, so retrying the same
// failed batch cannot report fresh progress. The boolean distinguishes slot zero
// from a ledger that has not processed a block.
func (ls *LedgerState) BlockProcessingSlot() (uint64, bool) {
	if !ls.processedBlockSeen.Load() {
		return 0, false
	}
	return ls.processedBlockSlot.Load(), true
}

func (ls *LedgerState) recordProcessedBlockSlot(slot uint64) {
	for previous := ls.processedBlockSlot.Load(); slot > previous; previous = ls.processedBlockSlot.Load() {
		if ls.processedBlockSlot.CompareAndSwap(previous, slot) {
			break
		}
	}
	ls.processedBlockSeen.Store(true)
}
