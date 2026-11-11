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

package sqlstore

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func TestDrepDeregistrationUsesCurrentPV10DelegationWithStaleReverseIndex(
	t *testing.T,
) {
	t.Parallel()
	for _, major := range []uint64{0, 9, 10, 11, 12} {
		t.Run(fmt.Sprintf("PV%d", major), func(t *testing.T) {
			t.Parallel()
			store := newMigratedSQLiteStore(t)
			oldDrep := bytes.Repeat([]byte{0xa1}, 28)
			currentDrep := bytes.Repeat([]byte{0xa2}, 28)
			stale := bytes.Repeat([]byte{0xa3}, 28)
			missing := bytes.Repeat([]byte{0xa4}, 28)
			inactive := bytes.Repeat([]byte{0xa5}, 28)
			scriptDelegate := bytes.Repeat([]byte{0xa6}, 28)
			for _, account := range []*models.Account{
				{StakingKey: stale, Drep: currentDrep, AddedSlot: 100, Active: true},
				{StakingKey: missing, Drep: oldDrep, AddedSlot: 100, Active: true},
				{StakingKey: inactive, AddedSlot: 100, Active: false},
				{StakingKey: scriptDelegate, Drep: oldDrep, DrepType: models.DrepTypeScriptHash, AddedSlot: 100, Active: true},
			} {
				require.NoError(t, store.ImportAccount(account, nil))
			}
			require.NoError(t, store.ImportDrep(&models.Drep{
				Credential: oldDrep, AddedSlot: 100, Active: true,
				Delegators: []models.StakeCredentialRef{
					{Key: stale},
					{Key: inactive},
				},
			}, &models.RegistrationDrep{DrepCredential: oldDrep, AddedSlot: 100, DepositAmount: 500}, nil))
			require.NoError(t, store.ImportDrep(&models.Drep{
				Credential: currentDrep, AddedSlot: 100, Active: true,
				Delegators: []models.StakeCredentialRef{{Key: stale}},
			}, &models.RegistrationDrep{DrepCredential: currentDrep, AddedSlot: 100, DepositAmount: 500}, nil))

			tx := mockledger.NewTransactionBuilder().
				WithCertificates(&common.DeregistrationDrepCertificate{
					CertType: uint(common.CertificateTypeDeregistrationDrep),
					DrepCredential: common.Credential{
						CredType:   0,
						Credential: common.NewBlake2b224(oldDrep),
					},
					Amount: 500,
				})
			tx.WithId(bytes.Repeat([]byte{0xa7}, 32))
			tx.WithValid(true)
			require.NoError(
				t,
				store.SetTransaction(
					tx,
					ocommon.Point{Slot: 200, Hash: tx.Hash().Bytes()},
					0,
					nil,
					false,
					nil,
					major,
				),
			)
			staleAccount, err := store.GetAccountByCredential(
				0,
				stale,
				true,
				nil,
			)
			require.NoError(t, err)
			missingAccount, err := store.GetAccountByCredential(
				0,
				missing,
				true,
				nil,
			)
			require.NoError(t, err)
			inactiveAccount, err := store.GetAccountByCredential(
				0,
				inactive,
				true,
				nil,
			)
			require.NoError(t, err)
			scriptAccount, err := store.GetAccountByCredential(
				0,
				scriptDelegate,
				true,
				nil,
			)
			require.NoError(t, err)
			if major < 10 {
				require.Empty(
					t,
					staleAccount.Drep,
					"PV9 uses the historical reverse membership",
				)
				require.Equal(t, oldDrep, missingAccount.Drep)
				require.Equal(t, uint64(200), inactiveAccount.AddedSlot)
			} else {
				require.Equal(t, currentDrep, staleAccount.Drep)
				require.Equal(t, uint64(100), staleAccount.AddedSlot)
				require.Empty(t, missingAccount.Drep, "a missing reverse row must not preserve a deregistered delegate")
				require.Equal(t, uint64(200), missingAccount.AddedSlot)
				require.Equal(t, uint64(100), inactiveAccount.AddedSlot)
			}
			require.Equal(t, oldDrep, scriptAccount.Drep)
			require.Equal(t, models.DrepTypeScriptHash, scriptAccount.DrepType)
			require.Equal(t, uint64(100), scriptAccount.AddedSlot)
			db, ctx, err := store.dbFromTxn(nil)
			require.NoError(t, err)
			var cleared [][]byte
			rows, err := db.QueryContext(
				ctx,
				"SELECT staking_key FROM account_drep_clear WHERE added_slot = 200",
			)
			require.NoError(t, err)
			for rows.Next() {
				var key []byte
				require.NoError(t, rows.Scan(&key))
				cleared = append(cleared, key)
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			if major < 10 {
				require.ElementsMatch(t, [][]byte{stale, inactive}, cleared)
			} else {
				require.Equal(t, [][]byte{missing}, cleared)
			}
		})
	}
}
