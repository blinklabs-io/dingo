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

package txpump

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeUTxO(hash string, idx uint32, amount uint64) UTxO {
	return UTxO{TxHash: hash, Index: idx, Amount: amount}
}

func TestWallet_EmptyBalance(t *testing.T) {
	w := NewWallet()
	assert.Equal(t, uint64(0), w.Balance())
	assert.Equal(t, 0, w.Len())
}

func TestWallet_AddAndBalance(t *testing.T) {
	w := NewWallet()
	w.Add(makeUTxO("aaaa", 0, 1_000_000))
	w.Add(makeUTxO("bbbb", 0, 2_000_000), makeUTxO("cccc", 1, 500_000))
	assert.Equal(t, uint64(3_500_000), w.Balance())
	assert.Equal(t, 3, w.Len())
}

func TestWallet_SelectCoins_LargestFirst(t *testing.T) {
	w := NewWallet()
	w.Add(
		makeUTxO("small", 0, 100_000),
		makeUTxO("large", 0, 5_000_000),
		makeUTxO("medium", 0, 1_000_000),
	)

	selected, change, err := w.SelectCoins(2_000_000)
	require.NoError(t, err)

	// Largest-first: the 5 ADA coin covers the target alone.
	require.Len(t, selected, 1)
	assert.Equal(t, "large", selected[0].TxHash)
	assert.Equal(t, uint64(3_000_000), change)

	// Selected UTxO should no longer be in the wallet.
	assert.Equal(t, 2, w.Len())
	assert.Equal(t, uint64(1_100_000), w.Balance())
}

func TestWallet_SelectCoins_MultipleInputsNeeded(t *testing.T) {
	w := NewWallet()
	w.Add(
		makeUTxO("a", 0, 1_000_000),
		makeUTxO("b", 0, 1_000_000),
		makeUTxO("c", 0, 1_000_000),
	)

	selected, change, err := w.SelectCoins(2_500_000)
	require.NoError(t, err)
	require.Len(t, selected, 3)
	assert.Equal(t, uint64(500_000), change)
	assert.Equal(t, 0, w.Len())
}

func TestWallet_SelectCoins_ExactAmount(t *testing.T) {
	w := NewWallet()
	w.Add(makeUTxO("exact", 0, 3_000_000))

	selected, change, err := w.SelectCoins(3_000_000)
	require.NoError(t, err)
	require.Len(t, selected, 1)
	assert.Equal(t, uint64(0), change)
}

func TestWallet_SelectCoins_InsufficientFunds(t *testing.T) {
	w := NewWallet()
	w.Add(makeUTxO("small", 0, 500_000))

	_, _, err := w.SelectCoins(1_000_000)
	require.ErrorIs(t, err, ErrInsufficientFunds)
	// Wallet should be unchanged after failure.
	assert.Equal(t, 1, w.Len())
}

func TestWallet_SelectCoins_EmptyWallet(t *testing.T) {
	w := NewWallet()
	_, _, err := w.SelectCoins(1)
	require.ErrorIs(t, err, ErrInsufficientFunds)
}

func TestWallet_ReturnUTxOs(t *testing.T) {
	w := NewWallet()
	w.Add(makeUTxO("a", 0, 5_000_000))

	selected, _, err := w.SelectCoins(1_000_000)
	require.NoError(t, err)
	assert.Equal(t, 0, w.Len())

	w.ReturnUTxOs(selected)
	assert.Equal(t, 1, w.Len())
	assert.Equal(t, uint64(5_000_000), w.Balance())
}

func TestWallet_ConcurrentAccess(t *testing.T) {
	// Verify that concurrent Add + Balance + SelectCoins calls don't race.
	w := NewWallet()
	for i := 0; i < 50; i++ {
		w.Add(makeUTxO("x", uint32(i), 1_000_000)) //nolint:gosec // test data
	}

	selectDone := make(chan struct{})
	addDone := make(chan struct{})
	go func() {
		defer close(selectDone)
		for i := 0; i < 20; i++ {
			if _, _, err := w.SelectCoins(500_000); err != nil {
				t.Errorf("SelectCoins(500_000): %v", err)
				return
			}
		}
	}()
	go func() {
		defer close(addDone)
		for i := 50; i < 70; i++ {
			w.Add(makeUTxO("y", uint32(i), 500_000)) //nolint:gosec // test data
		}
	}()
	for i := 0; i < 20; i++ {
		_ = w.Balance()
	}
	<-selectDone
	<-addDone
}
