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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func TestConwayWithdrawalDRepGateCredentialBoundary(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	keyBytes := bytes.Repeat([]byte{0x42}, lcommon.Blake2b224Size)
	keyHash := lcommon.NewBlake2b224(keyBytes)
	keyRewardAddr, err := lcommon.NewAddressFromBytes(
		append([]byte{0xE1}, keyBytes...),
	)
	require.NoError(t, err)

	scriptRewardAddr, err := lcommon.NewAddress(
		"stake17xt4n07cnlafzefqvne69mmxmnzu2t9gtd27jw9d9yvc7uscsd3d3",
	)
	require.NoError(t, err)
	scriptCredential, ok := scriptRewardAddr.StakeCredential()
	require.True(t, ok)
	require.Equal(
		t,
		uint(lcommon.CredentialTypeScriptHash),
		scriptCredential.CredType,
	)

	for _, account := range []*models.Account{
		{
			StakingKey:    keyHash.Bytes(),
			CredentialTag: 0,
			Reward:        1_000_000,
			Active:        true,
		},
		{
			StakingKey:    scriptCredential.Credential.Bytes(),
			CredentialTag: 1,
			Reward:        1_000_000,
			Active:        true,
		},
	} {
		require.NoError(t, db.CreateAccount(nil, account))
	}

	ls := &LedgerState{db: db}
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionPlomin,
		},
	}
	validate := func(t *testing.T, withdrawals map[*lcommon.Address]uint64) error {
		t.Helper()
		tx := mockledger.NewTransactionBuilder()
		tx.WithValid(true)
		tx.WithWithdrawals(withdrawals)
		txn := db.Transaction(false)
		var validationErr error
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			validationErr = conway.UtxoValidateWithdrawals(
				tx,
				0,
				&LedgerView{txn: txn, ls: ls},
				pp,
			)
			return nil
		}))
		return validationErr
	}

	t.Run("script hash is exempt", func(t *testing.T) {
		require.NoError(t, validate(t, map[*lcommon.Address]uint64{
			&scriptRewardAddr: 1_000_000,
		}))
	})

	t.Run("key hash remains gated", func(t *testing.T) {
		err := validate(t, map[*lcommon.Address]uint64{
			&keyRewardAddr: 1_000_000,
		})
		var target conway.WithdrawalNotDelegatedToDRepError
		require.ErrorAs(t, err, &target)
		require.Equal(t, keyRewardAddr, target.RewardAddress)
	})
}
