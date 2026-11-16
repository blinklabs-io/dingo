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

package utxoset

// SetAvailableDiskForTest replaces the free-space probe of checkpoint writes.
func (s *Set) SetAvailableDiskForTest(fn func(path string) (uint64, error)) {
	s.availableDisk = fn
}

// PruneJournalForTest prunes the rollback journal below slot now.
func (s *Set) PruneJournalForTest(slot uint64) error {
	return s.store.PruneJournal(slot)
}

// SetAfterSwapCheckpointForTest runs fn after a re-import has written the
// imported set's checkpoint and before it restores it.
func (s *Set) SetAfterSwapCheckpointForTest(fn func()) {
	s.afterSwapCheckpoint = fn
}

// CheckpointWriteBusyForTest reports whether a checkpoint write of this set is
// in progress, as another writer sees it.
func (s *Set) CheckpointWriteBusyForTest() bool {
	if s.ckptWrite.TryLock() {
		s.ckptWrite.Unlock()
		return false
	}
	return true
}
