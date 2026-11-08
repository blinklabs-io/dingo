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

package node

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func TestBackfillRestoresPV10ReverseDelegationsAtSnapshotAnchor(t *testing.T) {
	t.Parallel()
	for _, major := range []uint{9, 10, 11} {
		t.Run(fmt.Sprintf("PV%d", major), func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			store := db.Metadata()
			ctx := context.Background()
			old := bytes.Repeat([]byte{0xb1}, 28)
			current := bytes.Repeat([]byte{0xb2}, 28)
			stake := bytes.Repeat([]byte{0xb3}, 28)
			require.NoError(
				t,
				store.ImportDrep(
					&models.Drep{Credential: old, AddedSlot: 100, Active: true},
					&models.RegistrationDrep{
						DrepCredential: old,
						AddedSlot:      100,
						DepositAmount:  500,
					},
					nil,
				),
			)
			require.NoError(
				t,
				store.ImportDrep(
					&models.Drep{
						Credential: current,
						AddedSlot:  100,
						Active:     true,
						Delegators: []models.StakeCredentialRef{{Key: stake}},
					},
					&models.RegistrationDrep{
						DrepCredential: current,
						AddedSlot:      100,
						DepositAmount:  500,
					},
					nil,
				),
			)
			require.NoError(
				t,
				store.ImportAccount(
					&models.Account{
						StakingKey: stake,
						Drep:       current,
						AddedSlot:  100,
						Reward:     700,
						Active:     true,
					},
					nil,
				),
			)
			vote := func(slot uint64, id byte, drep []byte) {
				tx := mockledger.NewTransactionBuilder().
					WithCertificates(&common.VoteDelegationCertificate{CertType: uint(common.CertificateTypeVoteDelegation), StakeCredential: common.Credential{CredType: 0, Credential: common.NewBlake2b224(stake)}, Drep: common.Drep{Type: common.DrepTypeAddrKeyHash, Credential: drep}})
				tx.WithId(bytes.Repeat([]byte{id}, 32))
				tx.WithValid(true)
				require.NoError(
					t,
					store.SetTransaction(
						tx,
						ocommon.Point{Slot: slot, Hash: tx.Hash().Bytes()},
						0,
						nil,
						false,
						nil,
						9,
					),
				)
			}
			vote(50, 0xb4, old)
			vote(60, 0xb5, current)
			_, before, err := store.GetDRepDelegatorStakes(
				0,
				old,
				nil,
				100,
				0,
				"asc",
				nil,
			)
			require.NoError(t, err)
			require.Equal(t, 1, before)
			b := &Backfill{
				db:           db,
				currentEraId: conway.EraIdConway,
				currentPParams: &conway.ConwayProtocolParameters{
					ProtocolVersion: common.ProtocolParametersProtocolVersion{
						Major: major,
					},
				},
			}
			_, err = b.restoreImportedAccountsAtAnchor(ctx, 100)
			require.NoError(t, err)
			_, after, err := store.GetDRepDelegatorStakes(
				0,
				old,
				nil,
				100,
				0,
				"asc",
				nil,
			)
			require.NoError(t, err)
			if major >= 10 {
				require.Zero(t, after)
			} else {
				require.Equal(t, 1, after)
			}
			tx := mockledger.NewTransactionBuilder().
				WithCertificates(&common.DeregistrationDrepCertificate{CertType: uint(common.CertificateTypeDeregistrationDrep), DrepCredential: common.Credential{CredType: 0, Credential: common.NewBlake2b224(old)}, Amount: 500})
			tx.WithId(bytes.Repeat([]byte{0xb6}, 32))
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
					uint64(major),
				),
			)
			account, err := db.GetAccountByCredential(ctx, 0, stake, true, nil)
			require.NoError(t, err)
			if major >= 10 {
				require.Equal(t, current, account.Drep)
			} else {
				// PV9 deliberately consumes the historical reverse set;
				// its pre-Plomin behavior must remain unchanged.
				require.Empty(t, account.Drep)
			}
			require.Equal(t, uint64(700), uint64(account.Reward))
			require.True(t, account.Active)
		})
	}
}

func TestBackfillDRepReconciliationRejectsDanglingSnapshotAtomically(
	t *testing.T,
) {
	t.Parallel()
	db := newTestDB(t)
	store := db.Metadata()
	ctx := context.Background()
	old := bytes.Repeat([]byte{0xd1}, 28)
	current := bytes.Repeat([]byte{0xd2}, 28)
	stake := bytes.Repeat([]byte{0xd3}, 28)
	require.NoError(
		t,
		store.CreateDrep(
			nil,
			&models.Drep{Credential: old, AddedSlot: 1, Active: true},
		),
	)
	require.NoError(
		t,
		store.ImportAccount(
			&models.Account{
				StakingKey: stake,
				Drep:       current,
				AddedSlot:  100,
				Reward:     700,
				Active:     true,
			},
			nil,
		),
	)
	tx := mockledger.NewTransactionBuilder().
		WithCertificates(&common.VoteDelegationCertificate{CertType: uint(common.CertificateTypeVoteDelegation), StakeCredential: common.Credential{CredType: 0, Credential: common.NewBlake2b224(stake)}, Drep: common.Drep{Type: common.DrepTypeAddrKeyHash, Credential: old}})
	tx.WithId(bytes.Repeat([]byte{0xd4}, 32))
	tx.WithValid(true)
	require.NoError(
		t,
		store.SetTransaction(
			tx,
			ocommon.Point{Slot: 50, Hash: tx.Hash().Bytes()},
			0,
			nil,
			false,
			nil,
			9,
		),
	)
	b := &Backfill{
		db:           db,
		currentEraId: conway.EraIdConway,
		currentPParams: &conway.ConwayProtocolParameters{
			ProtocolVersion: common.ProtocolParametersProtocolVersion{
				Major: 10,
			},
		},
	}
	_, err := b.restoreImportedAccountsAtAnchor(ctx, 100)
	require.ErrorContains(t, err, "dangling DRep delegations")
	account, err := db.GetAccountByCredential(ctx, 0, stake, true, nil)
	require.NoError(t, err)
	require.Equal(t, old, account.Drep)
	require.Equal(t, uint64(50), account.AddedSlot)
	require.Equal(t, uint64(700), uint64(account.Reward))
	_, active, err := store.GetDRepDelegatorStakes(
		0,
		old,
		nil,
		100,
		0,
		"asc",
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 1, active)
}

func TestBackfillDRepReconciliationRejectsUnknownProtocolAtomically(
	t *testing.T,
) {
	t.Parallel()
	db := newTestDB(t)
	store := db.Metadata()
	ctx := context.Background()
	old := bytes.Repeat([]byte{0xd1}, 28)
	current := bytes.Repeat([]byte{0xd2}, 28)
	stake := bytes.Repeat([]byte{0xd3}, 28)
	require.NoError(
		t,
		store.CreateDrep(
			nil,
			&models.Drep{Credential: old, AddedSlot: 1, Active: true},
		),
	)
	require.NoError(
		t,
		store.ImportAccount(
			&models.Account{
				StakingKey: stake,
				Drep:       current,
				AddedSlot:  100,
				Reward:     700,
				Active:     true,
			},
			nil,
		),
	)
	tx := mockledger.NewTransactionBuilder().
		WithCertificates(&common.VoteDelegationCertificate{CertType: uint(common.CertificateTypeVoteDelegation), StakeCredential: common.Credential{CredType: 0, Credential: common.NewBlake2b224(stake)}, Drep: common.Drep{Type: common.DrepTypeAddrKeyHash, Credential: old}})
	tx.WithId(bytes.Repeat([]byte{0xd4}, 32))
	tx.WithValid(true)
	require.NoError(
		t,
		store.SetTransaction(
			tx,
			ocommon.Point{Slot: 50, Hash: tx.Hash().Bytes()},
			0,
			nil,
			false,
			nil,
			9,
		),
	)
	b := &Backfill{
		db:           db,
		currentEraId: conway.EraIdConway,
	}
	_, err := b.restoreImportedAccountsAtAnchor(ctx, 100)
	require.ErrorContains(
		t,
		err,
		"snapshot anchor protocol version is unavailable",
	)
	account, err := db.GetAccountByCredential(ctx, 0, stake, true, nil)
	require.NoError(t, err)
	require.Equal(t, old, account.Drep)
	require.Equal(t, uint64(50), account.AddedSlot)
	require.Equal(t, uint64(700), uint64(account.Reward))
	_, active, err := store.GetDRepDelegatorStakes(
		0,
		old,
		nil,
		100,
		0,
		"asc",
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 1, active)
}
