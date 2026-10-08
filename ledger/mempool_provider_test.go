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

	"github.com/stretchr/testify/require"
)

type stubMempoolProvider struct{}

func (stubMempoolProvider) Transactions() []PendingTransaction { return nil }

func (stubMempoolProvider) RemoveTxsByHash([]string) {}

// SetMempool is a startup call, but the forger reads the provider on its own
// goroutine, so a late or repeated call must not race those reads. The race
// detector is the assertion: it fails this test when the provider is read or
// written without synchronization.
func TestSetMempoolLateCallDoesNotRaceReaders(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	require.Nil(t, ls.mempoolProvider())

	const iterations = 1000
	var wg sync.WaitGroup
	wg.Go(func() {
		for range iterations {
			ls.SetMempool(stubMempoolProvider{})
		}
	})
	wg.Go(func() {
		for range iterations {
			_ = ls.mempoolProvider()
		}
	})
	wg.Wait()

	require.Equal(t, stubMempoolProvider{}, ls.mempoolProvider())
	ls.SetMempool(nil)
	require.Nil(t, ls.mempoolProvider())
}
