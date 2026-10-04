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

	"github.com/blinklabs-io/dingo/utxoref"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

var overlayCredential = lcommon.Credential{
	CredType:   lcommon.CredentialTypeAddrKeyHash,
	Credential: lcommon.CredentialHash{0xd1},
}

func overlayTx(seed byte) *mockledger.MockTransaction {
	tx := mockledger.NewTransactionBuilder().WithType(0)
	tx.WithId(bytes.Repeat([]byte{seed}, 32))
	return tx
}

func registrationTx(seed byte, deposit int64) lcommon.Transaction {
	return overlayTx(seed).WithCertificates(&lcommon.RegistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeRegistration),
		StakeCredential: overlayCredential,
		Amount:          deposit,
	})
}

func invalidCertificateTx(seed byte) lcommon.Transaction {
	tx := overlayTx(seed)
	tx.WithValid(false)
	return tx.WithCertificates(&lcommon.RegistrationCertificate{
		StakeCredential: overlayCredential,
	})
}

func TestChangesState(t *testing.T) {
	t.Parallel()
	address, err := lcommon.NewAddressFromBytes(
		append([]byte{0xe1}, bytes.Repeat([]byte{0xd1}, 28)...),
	)
	require.NoError(t, err)
	tests := []struct {
		name string
		tx   lcommon.Transaction
		want bool
	}{
		{"nil transaction", nil, false},
		{"utxo-only transaction", overlayTx(0x01), false},
		{"certificate", registrationTx(0x02, 1), true},
		{
			"withdrawal",
			overlayTx(0x03).WithWithdrawals(
				map[*lcommon.Address]uint64{&address: 1},
			),
			true,
		},
		{
			"phase-2-invalid certificate",
			invalidCertificateTx(0x04),
			false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.want, utxoref.ChangesState(test.tx))
		})
	}
}

func TestStateOverlayIgnoresTransactionsThatOnlyChangeUtxos(t *testing.T) {
	t.Parallel()
	overlay := utxoref.NewStateOverlay()
	overlay.Apply(overlayTx(0x01))
	require.Zero(t, overlay.Len())
	overlay.Apply(registrationTx(0x02, 1))
	require.Equal(t, 1, overlay.Len())
}

func TestStateOverlayViewWithoutTransactionsReturnsBase(t *testing.T) {
	t.Parallel()
	base := mockledger.NewLedgerStateBuilder().Build()
	var nilOverlay *utxoref.StateOverlay
	for _, overlay := range []*utxoref.StateOverlay{
		nilOverlay,
		utxoref.NewStateOverlay(),
	} {
		view, err := overlay.View(base, nil)
		require.NoError(t, err)
		require.Same(t, base, view)
	}
}

func TestStateOverlayViewRecordsRegistrationDeposit(t *testing.T) {
	t.Parallel()
	const deposit = 2_000_000
	base := mockledger.NewLedgerStateBuilder().Build()
	overlay := utxoref.NewStateOverlay()
	overlay.Apply(registrationTx(0x01, deposit))

	view, err := overlay.View(base, nil)
	require.NoError(t, err)
	require.True(t, view.IsStakeCredentialRegistered(overlayCredential))
	depositState, ok := lcommon.StakeCredentialDepositStateFor(view)
	require.True(t, ok)
	held, err := depositState.StakeCredentialDeposit(overlayCredential)
	require.NoError(t, err)
	require.NotNil(t, held, "a pending registration holds its deposit")
	require.Equal(t, uint64(deposit), *held)
}

func TestStateOverlayViewAppliesTransactionsInOrder(t *testing.T) {
	t.Parallel()
	base := mockledger.NewLedgerStateBuilder().Build()
	deregister := overlayTx(0x02).WithCertificates(
		&lcommon.DeregistrationCertificate{
			CertType:        uint(lcommon.CertificateTypeDeregistration),
			StakeCredential: overlayCredential,
			Amount:          1,
		},
	)

	overlay := utxoref.NewStateOverlay()
	overlay.Apply(registrationTx(0x01, 1))
	overlay.Apply(deregister)
	view, err := overlay.View(base, nil)
	require.NoError(t, err)
	require.False(t, view.IsStakeCredentialRegistered(overlayCredential))

	reversed := utxoref.NewStateOverlay()
	reversed.Apply(deregister)
	reversed.Apply(registrationTx(0x01, 1))
	view, err = reversed.View(base, nil)
	require.NoError(t, err)
	require.True(t, view.IsStakeCredentialRegistered(overlayCredential))
}
