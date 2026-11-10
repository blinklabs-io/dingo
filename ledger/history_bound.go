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

// OldestRetainedSlot returns the slot of the oldest block whose body is still
// stored locally, as last computed by the history-expiry pruner. ok is false
// when that is not known. It reads only memory.
func (ls *LedgerState) OldestRetainedSlot() (slot uint64, ok bool) {
	return ls.db.OldestRetainedSlot()
}
