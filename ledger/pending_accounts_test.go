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
	"bytes"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/utxoref"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// pendingAccountsFixture is a ledger whose only transaction rule is the
// reward-withdrawal rule, over one registered account with a stored balance.
type pendingAccountsFixture struct {
	ls         *LedgerState
	credential lcommon.Credential
	address    lcommon.Address
}

func newPendingAccountsFixture(
	t *testing.T,
	balance uint64,
) *pendingAccountsFixture {
	t.Helper()
	return newPendingAccountsFixtureWithRule(
		t,
		balance,
		shelley.UtxoValidateWithdrawals,
		&shelley.ShelleyProtocolParameters{},
	)
}

func newPendingAccountsFixtureWithRule(
	t *testing.T,
	balance uint64,
	rule lcommon.UtxoValidationRuleFunc,
	pparams lcommon.ProtocolParameters,
) *pendingAccountsFixture {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	key := bytes.Repeat([]byte{0xa1}, lcommon.AddressHashSize)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: key,
		Reward:     types.Uint64(balance),
		Active:     true,
	}))
	address, err := lcommon.NewAddressFromBytes(append([]byte{0xe1}, key...))
	require.NoError(t, err)
	credential, ok := address.StakeCredential()
	require.True(t, ok)

	era := eras.EraDesc{
		Id:   eras.ConwayEraDesc.Id,
		Name: "withdrawal-rule",
		ValidateTxFunc: func(
			tx lcommon.Transaction,
			slot uint64,
			state lcommon.LedgerState,
			pparams lcommon.ProtocolParameters,
		) error {
			return rule(tx, slot, state, pparams)
		},
	}
	tip := ochainsync.Tip{
		Point: ocommon.Point{Slot: 1, Hash: bytes.Repeat([]byte{0xf1}, 32)},
	}
	require.NoError(t, db.SetTip(tip, nil))
	ls := &LedgerState{
		db:         db,
		activeEras: []eras.EraDesc{era},
		currentEra: era,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1,
			LengthInSlots: 1_000,
			EraId:         era.Id,
		},
		currentPParams:    pparams,
		currentTip:        tip,
		validationEnabled: true,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.publishSnapshotsLocked()
	return &pendingAccountsFixture{
		ls:         ls,
		credential: credential,
		address:    address,
	}
}

func (f *pendingAccountsFixture) withdrawalTx(
	seed byte,
	amount uint64,
) lcommon.Transaction {
	tx := mockledger.NewTransactionBuilder().WithType(int(f.ls.currentEra.Id))
	tx.WithId(bytes.Repeat([]byte{seed}, 32))
	return tx.WithWithdrawals(
		map[*lcommon.Address]uint64{&f.address: amount},
	)
}

func (f *pendingAccountsFixture) certificateTx(
	seed byte,
	certs ...lcommon.Certificate,
) lcommon.Transaction {
	tx := mockledger.NewTransactionBuilder().WithType(int(f.ls.currentEra.Id))
	tx.WithId(bytes.Repeat([]byte{seed}, 32))
	return tx.WithCertificates(certs...)
}

func (f *pendingAccountsFixture) overlayAfter(
	txs ...lcommon.Transaction,
) *utxoref.StateOverlay {
	overlay := utxoref.NewStateOverlay()
	for _, tx := range txs {
		overlay.Apply(tx)
	}
	return overlay
}

func TestValidateTxWithOverlayAccountsForPendingWithdrawals(t *testing.T) {
	t.Parallel()
	f := newPendingAccountsFixture(t, 100)
	first := f.withdrawalTx(0x01, 100)
	second := f.withdrawalTx(0x02, 100)

	require.NoError(t, f.ls.ValidateTxWithOverlay(first, nil, nil, nil))
	require.NoError(
		t,
		f.ls.ValidateTxWithOverlay(second, nil, nil, nil),
		"without the pending overlay the stored balance still looks intact",
	)
	var incorrect shelley.IncorrectWithdrawalAmountError
	require.ErrorAs(
		t,
		f.ls.ValidateTxWithOverlay(second, nil, nil, f.overlayAfter(first)),
		&incorrect,
	)
	require.Zero(t, incorrect.Balance)
}

// directDepositTx is a transaction whose single effect level credits direct
// deposits, which the mock transaction builder cannot express.
type directDepositTx struct {
	lcommon.Transaction
	deposits []lcommon.DirectDeposit
}

func (d directDepositTx) LedgerEffectLevels() ([]lcommon.LedgerEffectLevel, error) {
	return []lcommon.LedgerEffectLevel{{
		Id:             d.Hash(),
		Body:           d.Transaction,
		DirectDeposits: d.deposits,
	}}, nil
}

func TestValidateTxWithOverlayAccountsForPendingDirectDeposit(t *testing.T) {
	t.Parallel()
	f := newPendingAccountsFixture(t, 100)
	deposit := directDepositTx{
		Transaction: f.certificateTx(0x03),
		deposits: []lcommon.DirectDeposit{
			{Credential: f.credential, Amount: 50},
		},
	}
	pending := f.overlayAfter(deposit)

	var incorrect shelley.IncorrectWithdrawalAmountError
	require.ErrorAs(
		t,
		f.ls.ValidateTxWithOverlay(f.withdrawalTx(0x01, 100), nil, nil, pending),
		&incorrect,
		"the stored balance no longer drains the account",
	)
	require.Equal(t, uint64(150), incorrect.Balance)
	require.NoError(
		t,
		f.ls.ValidateTxWithOverlay(f.withdrawalTx(0x02, 150), nil, nil, pending),
	)
}

func TestTxValidationSessionAccountsForPendingWithdrawals(t *testing.T) {
	t.Parallel()
	f := newPendingAccountsFixture(t, 100)
	first := f.withdrawalTx(0x01, 100)
	second := f.withdrawalTx(0x02, 100)

	err := f.ls.WithTxValidationSession(func(
		validate func(
			gledger.Transaction,
			map[utxoref.Key]struct{},
			map[utxoref.Key]lcommon.Utxo,
			*utxoref.StateOverlay,
		) error,
		_ func() bool,
	) error {
		overlay := utxoref.NewStateOverlay()
		require.NoError(t, validate(first, nil, nil, overlay))
		overlay.Apply(first)
		var incorrect shelley.IncorrectWithdrawalAmountError
		require.ErrorAs(t, validate(second, nil, nil, overlay), &incorrect)
		return nil
	})
	require.NoError(t, err)
}

func TestValidateTxWithOverlayAccountsForPendingRegistration(t *testing.T) {
	t.Parallel()
	f := newPendingAccountsFixture(t, 0)
	tx := f.withdrawalTx(0x01, 0)
	deregister := f.certificateTx(0x02, &lcommon.StakeDeregistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeDeregistration),
		StakeCredential: f.credential,
	})
	register := f.certificateTx(0x03, &lcommon.RegistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeRegistration),
		StakeCredential: f.credential,
	})

	var unregistered shelley.WithdrawalFromUnregisteredRewardAccountError
	require.ErrorAs(
		t,
		f.ls.ValidateTxWithOverlay(tx, nil, nil, f.overlayAfter(deregister)),
		&unregistered,
	)
	require.NoError(
		t,
		f.ls.ValidateTxWithOverlay(
			tx, nil, nil, f.overlayAfter(deregister, register),
		),
		"a pending re-registration leaves an empty registered account",
	)
}

type pendingAccountsBlock struct {
	*stubValidateBlock
	txs []lcommon.Transaction
}

func (b pendingAccountsBlock) Transactions() []lcommon.Transaction {
	return b.txs
}

func TestValidateForgedTxsAccountsForEarlierWithdrawals(t *testing.T) {
	t.Parallel()
	f := newPendingAccountsFixture(t, 100)
	block := pendingAccountsBlock{
		stubValidateBlock: &stubValidateBlock{slot: 10},
		txs: []lcommon.Transaction{
			f.withdrawalTx(0x01, 100),
			f.withdrawalTx(0x02, 100),
		},
	}

	err := f.ls.validateForgedTxs(block)
	var incorrect shelley.IncorrectWithdrawalAmountError
	require.ErrorAs(
		t,
		err,
		&incorrect,
		"the second withdrawal of a drained account must fail block validation",
	)
	require.Contains(t, err.Error(), "in forged block at slot 10")

	block.txs = block.txs[:1]
	require.NoError(t, f.ls.validateForgedTxs(block))
}

const pendingKeyDeposit = 2_000_000

// TestValidateTxWithOverlayDeregistersPendingRegistration runs the Conway
// certificate-deposit rule, which reads the deposit held for a registered
// credential, against a registration that is still pending.
func TestValidateTxWithOverlayDeregistersPendingRegistration(t *testing.T) {
	t.Parallel()
	f := newPendingAccountsFixtureWithRule(
		t,
		0,
		conway.UtxoValidateCertificateDeposits,
		&conway.ConwayProtocolParameters{
			KeyDeposit:  pendingKeyDeposit,
			DRepDeposit: 500_000_000,
		},
	)
	fresh := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.CredentialHash{0xc7},
	}
	register := f.certificateTx(0x01, &lcommon.RegistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeRegistration),
		StakeCredential: fresh,
		Amount:          pendingKeyDeposit,
	})
	deregister := func(amount int64) lcommon.Transaction {
		return f.certificateTx(0x02, &lcommon.DeregistrationCertificate{
			CertType:        uint(lcommon.CertificateTypeDeregistration),
			StakeCredential: fresh,
			Amount:          amount,
		})
	}
	pending := f.overlayAfter(register)

	require.NoError(
		t,
		f.ls.ValidateTxWithOverlay(deregister(pendingKeyDeposit), nil, nil, pending),
		"a credential registered by a pending transaction holds that deposit",
	)
	var mismatch conway.CertificateRefundIncorrectError
	require.ErrorAs(
		t,
		f.ls.ValidateTxWithOverlay(deregister(pendingKeyDeposit+1), nil, nil, pending),
		&mismatch,
		"a refund that differs from the pending deposit is rejected",
	)
}

func TestValidateForgedTxsAcceptsDeregistrationOfEarlierRegistration(
	t *testing.T,
) {
	t.Parallel()
	f := newPendingAccountsFixtureWithRule(
		t,
		0,
		conway.UtxoValidateCertificateDeposits,
		&conway.ConwayProtocolParameters{
			KeyDeposit:  pendingKeyDeposit,
			DRepDeposit: 500_000_000,
		},
	)
	fresh := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.CredentialHash{0xc8},
	}
	block := pendingAccountsBlock{
		stubValidateBlock: &stubValidateBlock{slot: 10},
		txs: []lcommon.Transaction{
			f.certificateTx(0x01, &lcommon.RegistrationCertificate{
				CertType:        uint(lcommon.CertificateTypeRegistration),
				StakeCredential: fresh,
				Amount:          pendingKeyDeposit,
			}),
			f.certificateTx(0x02, &lcommon.DeregistrationCertificate{
				CertType:        uint(lcommon.CertificateTypeDeregistration),
				StakeCredential: fresh,
				Amount:          pendingKeyDeposit,
			}),
		},
	}
	require.NoError(t, f.ls.validateForgedTxs(block))

	block.txs = block.txs[1:]
	require.Error(
		t,
		f.ls.validateForgedTxs(block),
		"deregistering a credential no transaction registered must fail",
	)
}

func TestValidateTxWithOverlayRejectsSpendOfStoredOutputAlreadySpent(
	t *testing.T,
) {
	t.Parallel()
	f := newPendingAccountsFixtureWithRule(
		t, 100, shelley.UtxoValidateBadInputsUtxo,
		&shelley.ShelleyProtocolParameters{},
	)
	out, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(f.address.String()).WithLovelace(5).Build()
	require.NoError(t, err)
	// Stored tx: changes account state, so the overlay records it, and
	// creates the output the others spend.
	stored := mockledger.NewTransactionBuilder().WithType(int(f.ls.currentEra.Id))
	stored.WithId(bytes.Repeat([]byte{0x0b}, 32))
	stored.WithOutputs(out)
	stored.WithWithdrawals(map[*lcommon.Address]uint64{&f.address: 10})
	var storedTx lcommon.Transaction = stored
	require.True(t, utxoref.ChangesState(storedTx))
	produced := storedTx.Produced()[0]
	key := utxoref.ForUtxo(produced)

	// The mempool's UTxO maps already hold a UTxO-only pending tx that
	// spent the stored tx's output.
	consumed := map[utxoref.Key]struct{}{key: {}}
	created := map[utxoref.Key]lcommon.Utxo{key: produced}
	overlay := utxoref.NewStateOverlay()
	overlay.Apply(storedTx)

	spender := mockledger.NewTransactionBuilder().WithType(int(f.ls.currentEra.Id))
	spender.WithId(bytes.Repeat([]byte{0x0d}, 32))
	spender.WithInputs(produced.Id)
	var spenderTx lcommon.Transaction = spender

	require.Error(
		t,
		f.ls.ValidateTxWithOverlay(spenderTx, consumed, created, nil),
		"control: without the state overlay the double spend is rejected",
	)
	require.Error(
		t,
		f.ls.ValidateTxWithOverlay(spenderTx, consumed, created, overlay),
		"a second spend of a stored tx's output must be rejected",
	)
}
