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
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/utxoref"
)

func testRewardAccount(
	t *testing.T,
) (lcommon.Address, lcommon.Credential) {
	t.Helper()
	key := bytes.Repeat([]byte{0xa1}, lcommon.AddressHashSize)
	address, err := lcommon.NewAddressFromBytes(append([]byte{0xe1}, key...))
	require.NoError(t, err)
	credential, ok := address.StakeCredential()
	require.True(t, ok)
	return address, credential
}

// accountLevel is the reward-account activity of one batch body.
type accountLevel struct {
	withdrawal uint64
	deposit    uint64
}

// dijkstraBatchAccounts encodes a batch whose child bodies and enclosing body
// each withdraw from and credit the reward account as described.
func dijkstraBatchAccounts(
	t *testing.T,
	address lcommon.Address,
	children []accountLevel,
	top accountLevel,
) lcommon.Transaction {
	t.Helper()
	addressBytes, err := address.Bytes()
	require.NoError(t, err)
	level := func(body map[uint]any, l accountLevel) map[uint]any {
		amounts := func(amount uint64) map[cbor.ByteString]uint64 {
			return map[cbor.ByteString]uint64{
				cbor.NewByteString(addressBytes): amount,
			}
		}
		if l.withdrawal > 0 {
			body[5] = amounts(l.withdrawal)
		}
		if l.deposit > 0 {
			body[25] = amounts(l.deposit)
		}
		return body
	}
	subTransactions := make([]cbor.RawMessage, 0, len(children))
	for _, child := range children {
		subBody, err := cbor.Encode(
			level(map[uint]any{0: []any{}, 1: []any{}}, child),
		)
		require.NoError(t, err)
		subTransaction, err := cbor.Encode([]any{
			cbor.RawMessage(subBody), map[uint]any{}, nil,
		})
		require.NoError(t, err)
		subTransactions = append(subTransactions, subTransaction)
	}
	topBody := level(map[uint]any{0: []any{}, 1: []any{}, 2: uint64(0)}, top)
	if len(subTransactions) > 0 {
		topBody[23] = cbor.NewSetType(subTransactions, true)
	}
	body, err := cbor.Encode(topBody)
	require.NoError(t, err)
	txCbor, err := cbor.Encode(
		[]any{cbor.RawMessage(body), map[uint]any{}, true, nil},
	)
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(gledger.TxTypeDijkstra, txCbor)
	require.NoError(t, err)
	return tx
}

func TestAccountEffectsOrdersChildrenBeforeEnclosingBody(t *testing.T) {
	t.Parallel()
	address, credential := testRewardAccount(t)
	tx := dijkstraBatchAccounts(
		t,
		address,
		[]accountLevel{{withdrawal: 40}},
		accountLevel{withdrawal: 60},
	)

	require.Equal(t, []utxoref.AccountEffect{
		{Credential: credential, Kind: utxoref.AccountWithdrawal, Amount: 40},
		{Credential: credential, Kind: utxoref.AccountWithdrawal, Amount: 60},
	}, utxoref.AccountEffects(tx))

	overlay := utxoref.NewAccountOverlay()
	overlay.Apply(utxoref.AccountEffects(tx))
	require.Equal(t, uint64(50), overlay.Balance(credential, 150))
}

func TestAccountEffectsThreadsEveryChildBeforeEnclosingBody(t *testing.T) {
	t.Parallel()
	address, credential := testRewardAccount(t)
	tx := dijkstraBatchAccounts(
		t,
		address,
		[]accountLevel{
			{withdrawal: 30},
			{withdrawal: 20, deposit: 5},
		},
		accountLevel{withdrawal: 40, deposit: 3},
	)

	effects := utxoref.AccountEffects(tx)
	require.Equal(t, []utxoref.AccountEffect{
		{Credential: credential, Kind: utxoref.AccountWithdrawal, Amount: 30},
		{Credential: credential, Kind: utxoref.AccountWithdrawal, Amount: 20},
		{Credential: credential, Kind: utxoref.AccountDirectDeposit, Amount: 5},
		{Credential: credential, Kind: utxoref.AccountWithdrawal, Amount: 40},
		{Credential: credential, Kind: utxoref.AccountDirectDeposit, Amount: 3},
	}, effects)

	overlay := utxoref.NewAccountOverlay()
	overlay.Apply(effects)
	require.Equal(t, uint64(18), overlay.Balance(credential, 100))
}

func TestAccountEffectsIgnoresPhase2InvalidTransaction(t *testing.T) {
	t.Parallel()
	address, _ := testRewardAccount(t)
	tx := mockledger.NewTransactionBuilder().WithWithdrawals(
		map[*lcommon.Address]uint64{&address: 10},
	)
	require.NotEmpty(t, utxoref.AccountEffects(tx))

	tx.WithValid(false)
	require.Empty(t, utxoref.AccountEffects(tx))
}

func TestAccountEffectsRecordsWithdrawalsBeforeCertificates(t *testing.T) {
	t.Parallel()
	address, credential := testRewardAccount(t)
	tx := mockledger.NewTransactionBuilder().
		WithWithdrawals(map[*lcommon.Address]uint64{&address: 10}).
		WithCertificates(
			&lcommon.StakeDeregistrationCertificate{
				StakeCredential: credential,
			},
			&lcommon.StakeRegistrationCertificate{
				StakeCredential: credential,
			},
		)

	require.Equal(t, []utxoref.AccountEffect{
		{Credential: credential, Kind: utxoref.AccountWithdrawal, Amount: 10},
		{Credential: credential, Kind: utxoref.AccountDeregistration},
		{Credential: credential, Kind: utxoref.AccountRegistration},
	}, utxoref.AccountEffects(tx))
}

func TestAccountOverlayTracksWithdrawalsAndRegistration(t *testing.T) {
	t.Parallel()
	_, credential := testRewardAccount(t)
	other := lcommon.Credential{
		CredType:   credential.CredType,
		Credential: lcommon.CredentialHash{0x01},
	}
	overlay := utxoref.NewAccountOverlay()

	_, decided := overlay.Registration(credential)
	require.False(t, decided, "an untouched account defers to stored state")

	overlay.Apply([]utxoref.AccountEffect{
		{Credential: credential, Kind: utxoref.AccountWithdrawal, Amount: 30},
		{Credential: credential, Kind: utxoref.AccountWithdrawal, Amount: 20},
	})
	require.Equal(t, uint64(50), overlay.Balance(credential, 100))
	require.Equal(t, uint64(100), overlay.Balance(other, 100))
	require.Zero(t, overlay.Balance(credential, 10), "saturates at zero")
	_, decided = overlay.Registration(credential)
	require.False(t, decided, "a withdrawal does not decide registration")

	overlay.Apply([]utxoref.AccountEffect{
		{Credential: credential, Kind: utxoref.AccountDeregistration},
	})
	registered, decided := overlay.Registration(credential)
	require.True(t, decided)
	require.False(t, registered)
	require.Equal(t, uint64(100), overlay.Balance(credential, 100))

	overlay.Apply([]utxoref.AccountEffect{
		{Credential: credential, Kind: utxoref.AccountDirectDeposit, Amount: 7},
		{Credential: credential, Kind: utxoref.AccountRegistration},
	})
	registered, decided = overlay.Registration(credential)
	require.True(t, decided)
	require.True(t, registered)
}

func TestNilAccountOverlayHoldsNoEffects(t *testing.T) {
	t.Parallel()
	_, credential := testRewardAccount(t)
	var overlay *utxoref.AccountOverlay
	_, decided := overlay.Registration(credential)
	require.False(t, decided)
	require.Equal(t, uint64(9), overlay.Balance(credential, 9))
}
