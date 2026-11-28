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

package chain

import (
	"testing"

	sdkchain "github.com/blinklabs-io/dingo/sdk/chain"
	"github.com/stretchr/testify/require"
)

type testLedgerRole struct {
	tip sdkchain.Tip
	k   int
}

func (r testLedgerRole) SecurityParam() int         { return r.k }
func (r testLedgerRole) Tip() sdkchain.Tip          { return r.tip }
func (testLedgerRole) ValidateTx(sdkchain.Tx) error { return nil }

func TestChainManagerUsesLedgerRole(t *testing.T) {
	t.Parallel()

	want := sdkchain.Tip{
		Point:       sdkchain.Point{Slot: 42, Hash: []byte("tip")},
		BlockNumber: 21,
	}
	manager := &ChainManager{}
	require.NoError(t, manager.SetLedgerRole(testLedgerRole{k: 7, tip: want}))
	require.Equal(t, 7, manager.SecurityParam())
	got, ok := manager.LedgerTip()
	require.True(t, ok)
	require.Equal(t, want, got)

	require.NoError(t, manager.SetLedger(testLedgerRole{k: 9}))
	require.Equal(t, 9, manager.SecurityParam())
	_, ok = manager.LedgerTip()
	require.False(t, ok)
}
