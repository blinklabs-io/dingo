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

import (
	"sync"
	"testing"
)

func TestChainsyncValidationStateConcurrentAccess(t *testing.T) {
	ls := &LedgerState{
		chainsyncState:    SyncingChainsyncState,
		validationEnabled: true,
		mithrilLedgerSlot: 10,
	}
	ls.publishSnapshotsLocked()

	const iterations = 1000

	var wg sync.WaitGroup
	for worker := 0; worker < 2; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for slot := 0; slot < iterations; slot++ {
				_ = ls.shouldVerifyChainsyncHeaderCrypto(uint64(slot))
				_, _ = ls.validationStateSnapshot()
				_ = ls.mithrilLedgerSlotSnapshot()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			ls.Lock()
			ls.validationEnabled = i%2 == 0
			ls.mithrilLedgerSlot = uint64(i % 128)
			ls.Unlock()

			if i%3 == 0 {
				ls.setChainsyncState(RollbackChainsyncState)
			} else {
				ls.setChainsyncState(SyncingChainsyncState)
			}
			ls.setChainsyncStateIf(
				RollbackChainsyncState,
				SyncingChainsyncState,
			)
		}
	}()

	wg.Wait()
}
