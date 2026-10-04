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
			return shelley.UtxoValidateWithdrawals(tx, slot, state, pparams)
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
		currentPParams:    &shelley.ShelleyProtocolParameters{},
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

func (f *pendingAccountsFixture) overlayAfter(
	txs ...lcommon.Transaction,
) *utxoref.AccountOverlay {
	overlay := utxoref.NewAccountOverlay()
	for _, tx := range txs {
		overlay.Apply(utxoref.AccountEffects(tx))
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

func TestValidateTxWithOverlayAccountsForPendingDirectDeposit(t *testing.T) {
	t.Parallel()
	f := newPendingAccountsFixture(t, 100)
	pending := utxoref.NewAccountOverlay()
	pending.Apply([]utxoref.AccountEffect{{
		Credential: f.credential,
		Kind:       utxoref.AccountDirectDeposit,
		Amount:     50,
	}})

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
			*utxoref.AccountOverlay,
		) error,
		_ func() bool,
	) error {
		overlay := utxoref.NewAccountOverlay()
		require.NoError(t, validate(first, nil, nil, overlay))
		overlay.Apply(utxoref.AccountEffects(first))
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

	deregistered := utxoref.NewAccountOverlay()
	deregistered.Apply([]utxoref.AccountEffect{{
		Credential: f.credential,
		Kind:       utxoref.AccountDeregistration,
	}})
	var unregistered shelley.WithdrawalFromUnregisteredRewardAccountError
	require.ErrorAs(
		t,
		f.ls.ValidateTxWithOverlay(tx, nil, nil, deregistered),
		&unregistered,
	)

	reregistered := utxoref.NewAccountOverlay()
	reregistered.Apply([]utxoref.AccountEffect{
		{Credential: f.credential, Kind: utxoref.AccountDeregistration},
		{Credential: f.credential, Kind: utxoref.AccountRegistration},
	})
	require.NoError(
		t,
		f.ls.ValidateTxWithOverlay(tx, nil, nil, reregistered),
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
