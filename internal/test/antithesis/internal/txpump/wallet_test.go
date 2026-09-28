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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeUTxO(hash string, idx uint32, amount uint64) UTxO {
	return UTxO{TxHash: hash, Index: idx, Amount: amount}
}

// TestWallet_AddAfterQuarantinesSubmittedOutputs verifies that a submitted
// output is excluded from balance and coin selection until its confirmation
// delay expires, then becomes spendable at the boundary.
func TestWallet_AddAfterQuarantinesSubmittedOutputs(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	w := NewWallet()
	w.now = func() time.Time { return now }
	w.AddAfter(15*time.Second, makeUTxO("pending", 0, 5_000_000))

	require.Zero(t, w.Len())
	require.Zero(t, w.Balance())
	_, _, err := w.SelectCoins(1_000_000)
	require.ErrorIs(t, err, ErrInsufficientFunds)

	now = now.Add(15 * time.Second)
	require.Equal(t, 1, w.Len())
	require.Equal(t, uint64(5_000_000), w.Balance())
	selected, _, err := w.SelectCoins(1_000_000)
	require.NoError(t, err)
	require.Equal(t, "pending", selected[0].TxHash)
}

func TestWallet_ReconcileRemovesRolledBackOutput(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	w := NewWallet()
	w.now = func() time.Time { return now }
	w.AddAfter(time.Second, makeUTxO("parent", 0, 5_000_000))
	now = now.Add(time.Second)
	w.ReconcileSnapshot([]UTxO{makeUTxO("parent", 0, 5_000_000)}, nil)
	require.Equal(t, 1, w.Len())
	w.ReconcileSnapshot(nil, nil)
	require.Zero(t, w.Len())
	_, _, err := w.SelectCoins(1_000_000)
	require.ErrorIs(t, err, ErrInsufficientFunds)
}

func TestWallet_ReconcileRestoresFundingOutput(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	w := NewWallet()
	w.now = func() time.Time { return now }
	w.Add(makeUTxO("funding", 0, 5_000_000))
	w.AddAfter(time.Second, makeUTxO("parent", 0, 2_000_000))
	selected, _, err := w.SelectCoins(1_000_000)
	require.NoError(t, err)
	require.Len(t, selected, 1)
	require.Zero(t, w.Len())

	now = now.Add(time.Second)
	// The parent is absent after rollback, while the original funding output
	// remains in the acquired snapshot and is restored for continued payment.
	w.ReconcileSnapshot([]UTxO{makeUTxO("funding", 0, 5_000_000)}, nil)
	require.Equal(t, 1, w.Len())
}

func TestWalletPendingChildNotReselected(t *testing.T) {
	w := NewWallet()
	source := makeUTxO("source", 0, 10_000_000)
	w.Add(source)
	selected, _, err := w.SelectCoins(2_000_000)
	require.NoError(t, err)
	w.Reserve("parent", selected, []UTxO{makeUTxO("parent", 0, 8_000_000)}, 0)
	w.Reserve(
		"child",
		[]UTxO{makeUTxO("parent", 0, 8_000_000)},
		[]UTxO{makeUTxO("child", 0, 7_000_000)},
		0,
	)
	w.ReconcileSnapshot(
		[]UTxO{makeUTxO("parent", 0, 8_000_000)},
		map[string]bool{"parent": true, "child": true},
	)
	_, _, err = w.SelectCoins(1)
	require.ErrorIs(
		t,
		err,
		ErrInsufficientFunds,
		"pending child output must not be selected",
	)
}

func TestWalletRollbackRestoresFundingForNextPayment(t *testing.T) {
	w := NewWallet()
	source := makeUTxO("funding", 0, 5_000_000)
	w.Add(source)
	inputs, _, err := w.SelectCoins(1_000_000)
	require.NoError(t, err)
	w.Reserve("tx", inputs, []UTxO{makeUTxO("tx", 0, 4_000_000)}, 0)
	w.ReconcileSnapshot([]UTxO{source}, map[string]bool{"tx": false})
	require.Len(t, w.PendingIDs(), 1, "one absent observation is ambiguous")
	_, _, err = w.SelectCoins(1_000_000)
	require.ErrorIs(
		t,
		err,
		ErrInsufficientFunds,
		"ambiguous input remains reserved",
	)
	w.ReconcileSnapshot([]UTxO{source}, map[string]bool{"tx": false})
	require.Empty(
		t,
		w.PendingIDs(),
		"rejected records retire after the grace observation",
	)
	selected, _, err := w.SelectCoins(1_000_000)
	require.NoError(t, err)
	require.Equal(t, source.TxHash, selected[0].TxHash)
}

func TestWalletAbsenceGraceResetsAfterMempoolReappearance(t *testing.T) {
	w := NewWallet()
	source := makeUTxO("funding", 0, 5_000_000)
	w.Add(source)
	inputs, _, err := w.SelectCoins(1_000_000)
	require.NoError(t, err)
	w.Reserve("tx", inputs, nil, 0)

	w.ReconcileSnapshot([]UTxO{source}, map[string]bool{"tx": false})
	require.Len(t, w.PendingIDs(), 1)
	w.ReconcileSnapshot([]UTxO{source}, map[string]bool{"tx": true})
	w.ReconcileSnapshot([]UTxO{source}, map[string]bool{"tx": false})
	require.Len(t, w.PendingIDs(), 1,
		"each transition from present to absent gets a fresh grace observation")
	_, _, err = w.SelectCoins(1)
	require.ErrorIs(t, err, ErrInsufficientFunds,
		"the reappearing transaction's source input must remain reserved")

	w.ReconcileSnapshot([]UTxO{source}, map[string]bool{"tx": false})
	require.Empty(t, w.PendingIDs())
	selected, _, err := w.SelectCoins(1)
	require.NoError(t, err)
	require.Equal(t, source.TxHash, selected[0].TxHash)
}

func TestWalletRollbackRestoresOnlyInputsPresentInSnapshot(t *testing.T) {
	w := NewWallet()
	first := makeUTxO("first", 0, 3_000_000)
	second := makeUTxO("second", 0, 4_000_000)
	w.Add(first, second)
	inputs, _, err := w.SelectCoins(6_000_000)
	require.NoError(t, err)
	w.Reserve("tx", inputs, nil, 0)
	w.ReconcileSnapshot([]UTxO{first}, map[string]bool{"tx": false})
	w.ReconcileSnapshot([]UTxO{first}, map[string]bool{"tx": false})
	selected, _, err := w.SelectCoins(1_000_000)
	require.NoError(t, err)
	require.Len(t, selected, 1)
	require.Equal(t, first.TxHash, selected[0].TxHash)
	_, _, err = w.SelectCoins(1_000_000)
	require.ErrorIs(
		t,
		err,
		ErrInsufficientFunds,
		"missing rollback input must remain spent",
	)
}

func TestWalletRollbackRevivesSeededInputWithSnapshotAddress(t *testing.T) {
	key := &UTxOKey{Address: []byte{1, 2, 3}}
	w := NewWallet()
	source := makeUTxO("funding", 0, 5_000_000)
	source.SigningKey = key
	w.Add(source)
	inputs, _, err := w.SelectCoins(1_000_000)
	require.NoError(t, err)
	w.RecordAccepted("parent", inputs, []UTxO{{
		TxHash: "parent", Index: 0, Amount: 4_000_000, SigningKey: key,
	}}, 0)
	require.Zero(t, w.Len(), "acceptance alone must not expose parent outputs")

	// The child is authoritative and retires the pending parent record.
	w.ReconcileSnapshot(
		[]UTxO{{TxHash: "parent", Index: 0, Amount: 4_000_000}},
		nil,
	)
	require.Equal(
		t,
		uint64(4_000_000),
		w.Balance(),
		"confirmed parent remains usable",
	)
	// A rollback revives the seeded input, but it has no prior wallet or
	// pending record. The authoritative snapshot carries only its address.
	w.ReconcileSnapshot(
		[]UTxO{
			{
				TxHash:  source.TxHash,
				Index:   source.Index,
				Amount:  source.Amount,
				address: key.Address,
			},
		},
		nil,
	)
	selected, _, err := w.SelectCoins(1_000_000)
	require.NoError(t, err)
	require.Equal(t, source.TxHash, selected[0].TxHash)
	require.Same(t, key, selected[0].SigningKey)
}

func TestWalletConfirmedParentOutputSurvivesPendingControl(t *testing.T) {
	w := NewWallet()
	parentOut := makeUTxO("parent", 0, 4_000_000)
	w.Reserve(
		"parent",
		[]UTxO{makeUTxO("funding", 0, 5_000_000)},
		[]UTxO{parentOut},
		0,
	)
	w.ReconcileSnapshot([]UTxO{parentOut}, map[string]bool{"parent": false})
	selected, _, err := w.SelectCoins(1_000_000)
	require.NoError(t, err)
	require.Equal(t, parentOut.TxHash, selected[0].TxHash)
}

func TestWalletSnapshotPreservesOutputPacing(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	w := NewWallet()
	w.now = func() time.Time { return now }
	source := makeUTxO("source", 0, 5_000_000)
	w.Add(source)
	inputs, _, err := w.SelectCoins(1_000_000)
	require.NoError(t, err)
	w.Reserve("tx", inputs, []UTxO{makeUTxO("tx", 0, 4_000_000)}, time.Minute)
	w.ReconcileSnapshot(
		[]UTxO{makeUTxO("tx", 0, 4_000_000)},
		map[string]bool{"tx": false},
	)
	_, _, err = w.SelectCoins(1_000_000)
	require.ErrorIs(
		t,
		err,
		ErrInsufficientFunds,
		"chain visibility must not remove output pacing",
	)
	// A later snapshot arrives after the pending record has been retired. The
	// deadline remains attached to the current output, not historical state.
	w.ReconcileSnapshot([]UTxO{makeUTxO("tx", 0, 4_000_000)}, nil)
	_, _, err = w.SelectCoins(1_000_000)
	require.ErrorIs(
		t,
		err,
		ErrInsufficientFunds,
		"later snapshots must preserve pacing",
	)
	now = now.Add(time.Minute)
	selected, _, err := w.SelectCoins(1_000_000)
	require.NoError(t, err)
	require.Equal(t, "tx", selected[0].TxHash)
}

func TestWalletRecordAcceptedUnsignedUsesDelayedOutputs(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	w := NewWallet()
	w.now = func() time.Time { return now }
	w.Add(makeUTxO("funding", 0, 5_000_000))
	inputs, _, err := w.SelectCoins(1_000_000)
	require.NoError(t, err)
	w.RecordAccepted(
		"tx",
		inputs,
		[]UTxO{makeUTxO("tx", 0, 4_000_000)},
		time.Minute,
	)
	require.Empty(t, w.PendingIDs())
	require.Equal(t, 0, w.Len())
	now = now.Add(time.Minute)
	require.Equal(t, 1, w.Len())
}

func TestWalletRecordAcceptedKeyedZeroChangeReservesInputs(t *testing.T) {
	key := &UTxOKey{Address: []byte{1, 2, 3}}
	w := NewWallet()
	source := makeUTxO("funding", 0, 5_000_000)
	source.SigningKey = key
	w.Add(source)
	inputs, _, err := w.SelectCoins(5_000_000)
	require.NoError(t, err)
	w.RecordAccepted("tx", inputs, nil, time.Minute)
	require.Len(t, w.PendingIDs(), 1)
	w.ReconcileSnapshot([]UTxO{source}, map[string]bool{"tx": true})
	_, _, err = w.SelectCoins(1)
	require.ErrorIs(t, err, ErrInsufficientFunds)
}

// TestWallet_ZeroValueRemainsUsable verifies that adding the injectable clock
// does not break callers that construct Wallet using its zero value.
func TestWallet_ZeroValueRemainsUsable(t *testing.T) {
	var w Wallet
	w.Add(makeUTxO("available", 0, 2_000_000))
	w.AddAfter(time.Hour, makeUTxO("pending", 0, 3_000_000))

	require.Equal(t, 1, w.Len())
	require.Equal(t, uint64(2_000_000), w.Balance())
	selected, change, err := w.SelectCoins(1_000_000)
	require.NoError(t, err)
	require.Equal(t, uint64(1_000_000), change)
	require.Equal(t, "available", selected[0].TxHash)
	require.Zero(t, w.Len())
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
