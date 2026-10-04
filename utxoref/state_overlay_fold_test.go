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

package utxoref_test

import (
	"bytes"
	"errors"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"testing"

	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

var foldStakeKey = bytes.Repeat([]byte{0xa1}, 28)

func foldWithdrawalCbor(t *testing.T, seed byte) []byte {
	t.Helper()
	body := map[uint]any{
		0: cbor.Tag{
			Number: 258,
			Content: []any{
				[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)},
			},
		},
		1: []any{[]any{
			append([]byte{0x61}, make([]byte, 28)...),
			uint64(1_000_000),
		}},
		2: uint64(200_000),
		5: map[cbor.ByteString]uint64{
			cbor.NewByteString(append([]byte{0xe1}, foldStakeKey...)): 0,
		},
	}
	txCbor, err := cbor.Encode([]any{body, map[uint]any{}, true, nil})
	require.NoError(t, err)
	_, err = conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(t, err)
	return txCbor
}

// countingBase counts reward balance reads and answers UtxoById with a
// sentinel, so a test can tell which layer served a lookup.
type countingBase struct {
	lcommon.LedgerState
	balanceReads int
	registered   bool
}

var errBaseUtxo = errors.New("served by the base")

func (b *countingBase) RewardAccountBalance(
	cred lcommon.Credential,
) (*uint64, error) {
	b.balanceReads++
	return b.LedgerState.RewardAccountBalance(cred)
}

func (b *countingBase) UtxoById(lcommon.TransactionInput) (lcommon.Utxo, error) {
	return lcommon.Utxo{}, errBaseUtxo
}

func (b *countingBase) IsStakeCredentialRegistered(lcommon.Credential) bool {
	return b.registered
}

func newCountingBase() *countingBase {
	var stake lcommon.Blake2b224
	copy(stake[:], foldStakeKey)
	return &countingBase{
		LedgerState: mockledger.NewLedgerStateBuilder().
			WithRewardAccountBalance(stake, 100).
			Build(),
	}
}

func TestStateOverlayViewFoldsEachTransactionOnce(t *testing.T) {
	t.Parallel()
	base := newCountingBase()
	overlay := utxoref.NewStateOverlay()
	for _, seed := range []byte{0x01, 0x02, 0x03} {
		overlay.ApplyEncoded(
			uint(conway.EraIdConway),
			foldWithdrawalCbor(t, seed),
		)
	}
	_, err := overlay.View(base, nil, ocommon.Point{})
	require.NoError(t, err)
	require.Equal(t, 3, base.balanceReads)

	overlay.ApplyEncoded(uint(conway.EraIdConway), foldWithdrawalCbor(t, 0x04))
	base.balanceReads = 0
	_, err = overlay.View(base, nil, ocommon.Point{})
	require.NoError(t, err)
	require.Equal(
		t,
		1,
		base.balanceReads,
		"a second View must apply only the transaction recorded since the first",
	)

	base.balanceReads = 0
	_, err = overlay.View(base, nil, ocommon.Point{})
	require.NoError(t, err)
	require.Zero(t, base.balanceReads, "nothing new to apply")
	require.Equal(t, 4, overlay.Len())
}

func TestStateOverlayViewReadsThroughTheLatestBase(t *testing.T) {
	t.Parallel()
	first := newCountingBase()
	overlay := utxoref.NewStateOverlay()
	overlay.ApplyEncoded(uint(conway.EraIdConway), foldWithdrawalCbor(t, 0x01))
	view, err := overlay.View(first, nil, ocommon.Point{})
	require.NoError(t, err)
	require.False(t, view.IsStakeCredentialRegistered(overlayCredential))

	second := newCountingBase()
	second.registered = true
	view, err = overlay.View(second, nil, ocommon.Point{})
	require.NoError(t, err)
	require.True(
		t,
		view.IsStakeCredentialRegistered(overlayCredential),
		"the folded state must read from the base of the latest View",
	)
}

func TestStateOverlayViewLeavesUtxoLookupsToTheBase(t *testing.T) {
	t.Parallel()
	base := newCountingBase()
	out, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr_test1qpe6s9amgfwtu9u6lqj998vke6uncswr4dg88qqft5d7f67kfjf77qy57hqhnefcqyy7hmhsygj9j38rj984hn9r57fswc4wg0").
		WithLovelace(5).Build()
	require.NoError(t, err)
	stored := overlayTx(0x0b)
	stored.WithOutputs(out)
	stored.WithWithdrawals(
		map[*lcommon.Address]uint64{mustRewardAddress(t): 1},
	)
	overlay := utxoref.NewStateOverlay()
	overlay.Apply(stored)
	require.Equal(t, 1, overlay.Len())

	view, err := overlay.View(base, nil, ocommon.Point{})
	require.NoError(t, err)
	created := stored.Produced()[0]
	_, err = view.UtxoById(created.Id)
	require.ErrorIs(
		t,
		err,
		errBaseUtxo,
		"an output of a stored transaction is the mempool maps' to answer",
	)
}

func TestStateOverlayViewFailsOnUndecodableTransaction(t *testing.T) {
	t.Parallel()
	overlay := utxoref.NewStateOverlay()
	overlay.ApplyEncoded(uint(conway.EraIdConway), []byte{0xff})
	for range 2 {
		_, err := overlay.View(newCountingBase(), nil, ocommon.Point{})
		require.ErrorContains(t, err, "decode pending transaction")
	}
}

func mustRewardAddress(t *testing.T) *lcommon.Address {
	t.Helper()
	addr, err := lcommon.NewAddressFromBytes(
		append([]byte{0xe1}, foldStakeKey...),
	)
	require.NoError(t, err)
	return &addr
}

func TestStateOverlayViewRefoldsWhenTipChanges(t *testing.T) {
	t.Parallel()
	var stake lcommon.Blake2b224
	copy(stake[:], foldStakeKey)
	cred := lcommon.Credential{CredType: lcommon.CredentialTypeAddrKeyHash}
	cred.Credential = stake
	balanceOf := func(t *testing.T, view lcommon.LedgerState) uint64 {
		t.Helper()
		balance, err := view.RewardAccountBalance(cred)
		require.NoError(t, err)
		require.NotNil(t, balance)
		return *balance
	}
	// A zero withdrawal leaves the balance as the base reports it, so the
	// folded state caches whatever base it was folded over.
	overlay := utxoref.NewStateOverlay()
	overlay.ApplyEncoded(uint(conway.EraIdConway), foldWithdrawalCbor(t, 0x01))
	tipA := ocommon.Point{Slot: 10, Hash: []byte{0x0a}}
	tipB := ocommon.Point{Slot: 11, Hash: []byte{0x0b}}

	view, err := overlay.View(newCountingBase(), nil, tipA)
	require.NoError(t, err)
	require.Equal(t, uint64(100), balanceOf(t, view))

	drained := &countingBase{
		LedgerState: mockledger.NewLedgerStateBuilder().
			WithRewardAccountBalance(stake, 0).
			Build(),
	}
	view, err = overlay.View(drained, nil, tipB)
	require.NoError(t, err)
	require.Zero(
		t,
		balanceOf(t, view),
		"a View over a new tip must not return state folded over the old one",
	)

	drained.balanceReads = 0
	_, err = overlay.View(drained, nil, tipB)
	require.NoError(t, err)
	require.Zero(t, drained.balanceReads, "an unchanged tip keeps the fold")

	view, err = overlay.View(newCountingBase(), nil, tipA)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(100),
		balanceOf(t, view),
		"returning to an earlier tip refolds as well",
	)
}
