// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package sqlite

import (
	"bytes"
	"database/sql"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type transactionWriteStore interface {
	CreateAccount(types.Txn, *models.Account) error
	CreateUtxo(types.Txn, *models.Utxo) error
	SetTransaction(
		lcommon.Transaction,
		ocommon.Point,
		uint32,
		map[int]uint64,
		bool,
		types.Txn,
	) error
	GetTransactionByHash([]byte, types.Txn) (*models.Transaction, error)
	GetUtxoIncludingSpent([]byte, uint32, types.Txn) (*models.Utxo, error)
	GetUtxo([]byte, uint32, types.Txn) (*models.Utxo, error)
	GetAccountByCredential(
		uint8,
		[]byte,
		bool,
		types.Txn,
	) (*models.Account, error)
}

type transactionWriteState struct {
	Slot             uint64
	BlockIndex       uint32
	Fee              uint64
	Valid            bool
	InputDeletedSlot uint64
	InputSpentBy     []byte
	OutputAmount     uint64
	AccountReward    uint64
	WithdrawalDeltas int
	WithdrawalProofs int
	Inputs           int
	Outputs          int
}

func TestSharedSQLStoreTransactionWriteParity(t *testing.T) {
	t.Parallel()
	store, raw := newSharedSQLStore(t)
	counts := func() (int, int) {
		var deltas int
		var witnesses int
		require.NoError(t, raw.QueryRow(
			"SELECT COUNT(*) FROM account_reward_delta",
		).Scan(&deltas))
		require.NoError(t, raw.QueryRow(
			"SELECT COUNT(*) FROM account_withdrawal_witness",
		).Scan(&witnesses))
		return deltas, witnesses
	}
	_ = exerciseTransactionWriteStore(t, store, false, counts)
}

// TestSharedSQLStoreWithdrawalWitnessGate covers issue #2919: the
// account_withdrawal_witness insert must be elided when the caller reports
// the delegator-inactivity gate off (skipWithdrawalWitness=true), and written
// when the gate is on -- in both cases the unrelated reward-delta bookkeeping
// (account_reward_delta) must be unaffected.
func TestSharedSQLStoreWithdrawalWitnessGate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                  string
		skipWithdrawalWitness bool
		wantWitnesses         int
	}{
		{name: "gate off elides witness row", skipWithdrawalWitness: true, wantWitnesses: 0},
		{name: "gate on writes witness row", skipWithdrawalWitness: false, wantWitnesses: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, raw := newSharedSQLStore(t)
			counts := func() (int, int) {
				var deltas int
				var witnesses int
				require.NoError(t, raw.QueryRow(
					"SELECT COUNT(*) FROM account_reward_delta",
				).Scan(&deltas))
				require.NoError(t, raw.QueryRow(
					"SELECT COUNT(*) FROM account_withdrawal_witness",
				).Scan(&witnesses))
				return deltas, witnesses
			}
			state := exerciseTransactionWriteStore(
				t, store, tc.skipWithdrawalWitness, counts,
			)
			require.Equal(t, tc.wantWitnesses, state.WithdrawalProofs)
			require.Equal(t, 1, state.WithdrawalDeltas)
		})
	}
}

func TestSharedSQLStoreWithdrawalRejectsExcessiveBalance(t *testing.T) {
	t.Parallel()
	store, raw := newSharedSQLStore(t)
	stakeKey := bytes.Repeat([]byte{0xc1}, lcommon.AddressHashSize)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeKey,
		Reward:     1234,
		Active:     true,
	}))
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeKey,
	)
	require.NoError(t, err)
	transactionHash := lcommon.Blake2b256{0xd1}
	transaction := &mockTransaction{
		hash:        transactionHash,
		isValid:     true,
		withdrawals: map[*lcommon.Address]*big.Int{&address: big.NewInt(1235)},
	}
	err = store.SetTransaction(
		transaction,
		ocommon.Point{Slot: 10, Hash: bytes.Repeat([]byte{0xd2}, 32)},
		0,
		nil,
		false,
		nil,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "reward withdrawal amount 1235 exceeds")

	account, err := store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1234), uint64(account.Reward))
	var deltas int
	require.NoError(t, raw.QueryRow(
		"SELECT COUNT(*) FROM account_reward_delta",
	).Scan(&deltas))
	require.Zero(t, deltas)
	var witnesses int
	require.NoError(t, raw.QueryRow(
		"SELECT COUNT(*) FROM account_withdrawal_witness",
	).Scan(&witnesses))
	require.Zero(t, witnesses)
	stored, err := store.GetTransactionByHash(transactionHash.Bytes(), nil)
	require.NoError(t, err)
	require.Nil(t, stored)
	excessiveHash := lcommon.Blake2b256{0xd3}
	excessive := &mockTransaction{
		hash:        excessiveHash,
		isValid:     true,
		withdrawals: map[*lcommon.Address]*big.Int{&address: big.NewInt(1236)},
	}
	err = store.SetTransaction(
		excessive,
		ocommon.Point{Slot: 11, Hash: bytes.Repeat([]byte{0xd4}, 32)},
		0,
		nil,
		true,
		nil,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "reward withdrawal amount 1236 exceeds")
	account, err = store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1234), uint64(account.Reward))
}

func TestSharedSQLStoreWithdrawalCredentialTagsRemainDistinct(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	stakeKey := bytes.Repeat([]byte{0xe1}, lcommon.AddressHashSize)
	for _, tag := range []uint8{0, 1} {
		require.NoError(t, store.CreateAccount(nil, &models.Account{
			StakingKey:    stakeKey,
			CredentialTag: tag,
			Reward:        17,
			Active:        true,
		}))
	}
	for i, tag := range []uint8{0, 1} {
		addressType := uint8(lcommon.AddressTypeNoneKey)
		if tag == 1 {
			addressType = lcommon.AddressTypeNoneScript
		}
		address, err := lcommon.NewAddressFromParts(
			addressType,
			lcommon.AddressNetworkTestnet,
			nil,
			stakeKey,
		)
		require.NoError(t, err)
		hash := lcommon.Blake2b256{byte(0xe2 + i)}
		transaction := &mockTransaction{
			hash:        hash,
			isValid:     true,
			withdrawals: map[*lcommon.Address]*big.Int{&address: big.NewInt(17)},
		}
		require.NoError(t, store.SetTransaction(
			transaction,
			ocommon.Point{Slot: uint64(20 + i), Hash: bytes.Repeat([]byte{byte(0xe4 + i)}, 32)},
			0,
			nil,
			true,
			nil,
		))
	}
	for tag := uint8(0); tag < 2; tag++ {
		account, err := store.GetAccountByCredential(tag, stakeKey, true, nil)
		require.NoError(t, err)
		require.Zero(t, account.Reward)
	}
}

func TestSharedSQLStoreWithdrawalAllowsPartialBalance(t *testing.T) {
	t.Parallel()
	store, raw := newSharedSQLStore(t)
	stakeKey := bytes.Repeat([]byte{0xf1}, lcommon.AddressHashSize)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeKey,
		Reward:     1234,
		Active:     true,
	}))
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeKey,
	)
	require.NoError(t, err)
	zero := &mockTransaction{
		hash:        lcommon.Blake2b256{0xf0},
		isValid:     true,
		withdrawals: map[*lcommon.Address]*big.Int{&address: big.NewInt(0)},
	}
	require.NoError(t, store.SetTransaction(
		zero,
		ocommon.Point{Slot: 20, Hash: bytes.Repeat([]byte{0xf6}, 32)},
		0,
		nil,
		true,
		nil,
	))
	account, err := store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1234), uint64(account.Reward))
	transactionHash := lcommon.Blake2b256{0xf2}
	transaction := &mockTransaction{
		hash:        transactionHash,
		isValid:     true,
		withdrawals: map[*lcommon.Address]*big.Int{&address: big.NewInt(234)},
	}
	point := ocommon.Point{Slot: 30, Hash: bytes.Repeat([]byte{0xf3}, 32)}
	require.NoError(t, store.SetTransaction(
		transaction, point, 0, nil, true, nil,
	))
	account, err = store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), uint64(account.Reward))

	// Replaying the same transaction must not debit the remaining balance again.
	require.NoError(t, store.SetTransaction(
		transaction, point, 0, nil, true, nil,
	))
	account, err = store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), uint64(account.Reward))

	excessive := &mockTransaction{
		hash:        lcommon.Blake2b256{0xf4},
		isValid:     true,
		withdrawals: map[*lcommon.Address]*big.Int{&address: big.NewInt(1001)},
	}
	err = store.SetTransaction(
		excessive,
		ocommon.Point{Slot: 31, Hash: bytes.Repeat([]byte{0xf5}, 32)},
		0,
		nil,
		true,
		nil,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "exceeds account balance 1000")
	account, err = store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), uint64(account.Reward))

	// Rollback restores the pre-withdrawal balance from the journal.
	require.NoError(t, store.DeleteAccountRewardsAfterSlot(29, nil))
	account, err = store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1234), uint64(account.Reward))
	var deltas int
	require.NoError(t, raw.QueryRow(
		"SELECT COUNT(*) FROM account_reward_delta",
	).Scan(&deltas))
	require.Zero(t, deltas)
}

func TestSharedSQLStoreZeroWithdrawalValidatesAccountAndBalance(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		reward     uint64
		active     bool
		create     bool
		wantError  string
		wantReward uint64
	}{
		{
			name:       "registered nonzero balance",
			reward:     12,
			active:     true,
			create:     true,
			wantReward: 12,
		},
		{
			name:       "registered zero balance",
			active:     true,
			create:     true,
			wantReward: 0,
		},
		{
			name:      "missing account",
			wantError: "account not found",
		},
		{
			name:       "inactive account",
			reward:     12,
			create:     true,
			wantError:  "account not found",
			wantReward: 12,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, _ := newSharedSQLStore(t)
			stakeKey := bytes.Repeat([]byte{0xf7}, lcommon.AddressHashSize)
			if tc.create {
				require.NoError(t, store.CreateAccount(nil, &models.Account{
					StakingKey: stakeKey,
					Reward:     types.Uint64(tc.reward),
					Active:     tc.active,
				}))
			}
			address, err := lcommon.NewAddressFromParts(
				lcommon.AddressTypeNoneKey,
				lcommon.AddressNetworkTestnet,
				nil,
				stakeKey,
			)
			require.NoError(t, err)
			transaction := &mockTransaction{
				hash:        lcommon.Blake2b256{0xf8},
				isValid:     true,
				withdrawals: map[*lcommon.Address]*big.Int{&address: big.NewInt(0)},
			}
			err = store.SetTransaction(
				transaction,
				ocommon.Point{Slot: 40, Hash: bytes.Repeat([]byte{0xf9}, 32)},
				0,
				nil,
				true,
				nil,
			)
			if tc.wantError != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
			}
			if !tc.create {
				return
			}
			account, accountErr := store.GetAccountByCredential(
				0, stakeKey, true, nil,
			)
			require.NoError(t, accountErr)
			require.Equal(t, tc.wantReward, uint64(account.Reward))
		})
	}
}

type certificateWriteState struct {
	CertificateCount int
	UnlinkedCount    int
	AccountActive    bool
	AccountPool      []byte
	AccountDrep      []byte
	AccountDrepType  uint64
	AccountAddedSlot uint64
	AccountCreated   uint64
	DrepActive       bool
	DrepAddedSlot    uint64
	TableCounts      map[string]int
}

func TestSharedSQLStoreCertificateWriteParity(t *testing.T) {
	t.Parallel()
	store, raw := newSharedSQLStore(t)
	_ = exerciseCertificateWriteStore(t, store, raw)
}

func TestSharedSQLStoreStorageModeTransactionParity(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{
		types.StorageModeCore,
		types.StorageModeAPI,
	} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			store, raw, _, err := openSQLStore(
				Config{DataDir: t.TempDir()},
				metadata.ProviderDependencies{StorageMode: mode},
			)
			require.NoError(t, err)
			require.NoError(t, store.Start(t.Context()))
			t.Cleanup(func() {
				require.NoError(t, store.Close())
			})
			exercise := func(
				store transactionWriteStore,
				db *sql.DB,
			) map[string]int {
				tx := newTestWitnessTransaction(
					"shared_sqlstore_storage_mode_" + mode,
				)
				require.NoError(t, store.SetTransaction(
					tx,
					ocommon.Point{
						Slot: 97,
						Hash: bytes.Repeat([]byte{0x3d}, 32),
					},
					0,
					nil,
					false,
					nil,
				))
				ret := map[string]int{}
				for _, table := range []string{
					"transaction",
					"key_witness",
				} {
					var count int
					require.NoError(
						t,
						db.QueryRow(
							`SELECT COUNT(*) FROM "`+table+`"`,
						).Scan(&count),
					)
					ret[table] = count
				}
				return ret
			}
			_ = exercise(store, raw)
		})
	}
}

func exerciseCertificateWriteStore(
	t *testing.T,
	store transactionWriteStore,
	db *sql.DB,
) certificateWriteState {
	t.Helper()
	stakeKey := lcommon.NewBlake2b224(bytes.Repeat([]byte{0x31}, 28))
	poolKey := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0x32}, 28)),
	)
	drepKey := lcommon.NewBlake2b224(bytes.Repeat([]byte{0x33}, 28))
	coldKey := lcommon.NewBlake2b224(bytes.Repeat([]byte{0x34}, 28))
	hotKey := lcommon.NewBlake2b224(bytes.Repeat([]byte{0x35}, 28))
	vrfKey := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x3b}, 32))
	rewardKey := lcommon.NewBlake2b224(bytes.Repeat([]byte{0x3c}, 28))
	credential := lcommon.Credential{
		CredType:   0,
		Credential: stakeKey,
	}
	certificates := []lcommon.Certificate{
		&lcommon.PoolRegistrationCertificate{
			CertType:      uint(lcommon.CertificateTypePoolRegistration),
			Operator:      poolKey,
			VrfKeyHash:    lcommon.VrfKeyHash(vrfKey),
			Pledge:        1_000_000,
			Cost:          340_000_000,
			Margin:        gcbor.Rat{Rat: big.NewRat(1, 100)},
			RewardAccount: lcommon.AddrKeyHash(rewardKey),
			PoolOwners: []lcommon.AddrKeyHash{
				lcommon.AddrKeyHash(stakeKey),
			},
		},
		&lcommon.StakeRegistrationCertificate{
			CertType:        uint(lcommon.CertificateTypeStakeRegistration),
			StakeCredential: credential,
		},
		&lcommon.StakeDelegationCertificate{
			CertType:        uint(lcommon.CertificateTypeStakeDelegation),
			StakeCredential: &credential,
			PoolKeyHash:     poolKey,
		},
		&lcommon.VoteDelegationCertificate{
			CertType:        uint(lcommon.CertificateTypeVoteDelegation),
			StakeCredential: credential,
			Drep: lcommon.Drep{
				Type: lcommon.DrepTypeAbstain,
			},
		},
		&lcommon.RegistrationDrepCertificate{
			CertType: uint(lcommon.CertificateTypeRegistrationDrep),
			DrepCredential: lcommon.Credential{
				CredType:   0,
				Credential: drepKey,
			},
		},
		&lcommon.UpdateDrepCertificate{
			CertType: uint(lcommon.CertificateTypeUpdateDrep),
			DrepCredential: lcommon.Credential{
				CredType:   0,
				Credential: drepKey,
			},
		},
		&lcommon.AuthCommitteeHotCertificate{
			CertType: uint(lcommon.CertificateTypeAuthCommitteeHot),
			ColdCredential: lcommon.Credential{
				CredType:   0,
				Credential: coldKey,
			},
			HotCredential: lcommon.Credential{
				CredType:   0,
				Credential: hotKey,
			},
		},
		&lcommon.ResignCommitteeColdCertificate{
			CertType: uint(lcommon.CertificateTypeResignCommitteeCold),
			ColdCredential: lcommon.Credential{
				CredType:   0,
				Credential: coldKey,
			},
		},
		&lcommon.GenesisKeyDelegationCertificate{
			CertType: uint(
				lcommon.CertificateTypeGenesisKeyDelegation,
			),
			GenesisHash:         bytes.Repeat([]byte{0x36}, 28),
			GenesisDelegateHash: bytes.Repeat([]byte{0x37}, 28),
			VrfKeyHash: lcommon.VrfKeyHash(
				lcommon.NewBlake2b256(bytes.Repeat([]byte{0x38}, 32)),
			),
		},
	}
	transaction := &mockTransaction{
		hash:         lcommon.NewBlake2b256(bytes.Repeat([]byte{0x39}, 32)),
		isValid:      true,
		certificates: certificates,
	}
	point := ocommon.Point{
		Slot: 81,
		Hash: bytes.Repeat([]byte{0x3a}, 32),
	}
	deposits := map[int]uint64{
		0: 500_000_000,
		1: 2_000_000,
		4: 500_000_000,
	}
	require.NoError(t, store.SetTransaction(
		transaction,
		point,
		7,
		deposits,
		false,
		nil,
	))
	require.NoError(t, store.SetTransaction(
		transaction,
		point,
		7,
		deposits,
		false,
		nil,
	))
	state := certificateWriteState{TableCounts: map[string]int{}}
	require.NoError(t, db.QueryRow(`
SELECT COUNT(*), COALESCE(SUM(certificate_id = 0), 0)
FROM certs`).Scan(
		&state.CertificateCount,
		&state.UnlinkedCount,
	))
	require.NoError(t, db.QueryRow(`
SELECT active, pool, drep, drep_type, added_slot, created_slot
FROM account WHERE credential_tag = 0 AND staking_key = ?`,
		stakeKey[:],
	).Scan(
		&state.AccountActive,
		&state.AccountPool,
		&state.AccountDrep,
		&state.AccountDrepType,
		&state.AccountAddedSlot,
		&state.AccountCreated,
	))
	require.NoError(t, db.QueryRow(`
SELECT active, added_slot FROM drep
WHERE credential_tag = 0 AND credential = ?`,
		drepKey[:],
	).Scan(&state.DrepActive, &state.DrepAddedSlot))
	for _, table := range []string{
		"pool_registration",
		"pool_registration_owner",
		"stake_registration",
		"stake_delegation",
		"vote_delegation",
		"registration_drep",
		"update_drep",
		"auth_committee_hot",
		"resign_committee_cold",
		"genesis_delegation",
	} {
		var count int
		require.NoError(
			t,
			db.QueryRow("SELECT COUNT(*) FROM "+table).Scan(&count),
		)
		state.TableCounts[table] = count
	}
	return state
}

func exerciseTransactionWriteStore(
	t *testing.T,
	store transactionWriteStore,
	skipWithdrawalWitness bool,
	counts func() (int, int),
) transactionWriteState {
	t.Helper()
	producerHash := lcommon.Blake2b256{}
	producerHash[0] = 0xa1
	transactionHash := lcommon.Blake2b256{}
	transactionHash[0] = 0xa2
	input := mockTransactionInput{hash: producerHash, index: 0}
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId: producerHash.Bytes(), OutputIdx: 0, Amount: 700,
		AddedSlot: 5,
	}))
	stakeKey := bytes.Repeat([]byte{0xb1}, lcommon.AddressHashSize)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeKey, Reward: 1234, Active: true,
	}))
	withdrawalAddress, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeKey,
	)
	require.NoError(t, err)
	output := &mockTransactionOutput{amount: big.NewInt(600)}
	transaction := &mockTransaction{
		hash:     transactionHash,
		isValid:  true,
		consumed: []lcommon.TransactionInput{input},
		produced: []lcommon.Utxo{{
			Id:     mockTransactionInput{hash: transactionHash, index: 0},
			Output: output,
		}},
		withdrawals: map[*lcommon.Address]*big.Int{
			&withdrawalAddress: big.NewInt(1234),
		},
	}
	point := ocommon.Point{
		Slot: 10,
		Hash: bytes.Repeat([]byte{0xc1}, 32),
	}
	require.NoError(t, store.SetTransaction(
		transaction, point, 3, nil, skipWithdrawalWitness, nil,
	))
	require.NoError(t, store.SetTransaction(
		transaction, point, 3, nil, skipWithdrawalWitness, nil,
	))
	stored, err := store.GetTransactionByHash(transactionHash.Bytes(), nil)
	require.NoError(t, err)
	require.NotNil(t, stored)
	spent, err := store.GetUtxoIncludingSpent(producerHash.Bytes(), 0, nil)
	require.NoError(t, err)
	require.NotNil(t, spent)
	produced, err := store.GetUtxo(transactionHash.Bytes(), 0, nil)
	require.NoError(t, err)
	require.NotNil(t, produced)
	account, err := store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	deltas, witnesses := counts()
	return transactionWriteState{
		Slot:             stored.Slot,
		BlockIndex:       stored.BlockIndex,
		Fee:              uint64(stored.Fee),
		Valid:            stored.Valid,
		InputDeletedSlot: spent.DeletedSlot,
		InputSpentBy:     spent.SpentAtTxId,
		OutputAmount:     uint64(produced.Amount),
		AccountReward:    uint64(account.Reward),
		WithdrawalDeltas: deltas,
		WithdrawalProofs: witnesses,
		Inputs:           len(stored.Inputs),
		Outputs:          len(stored.Outputs),
	}
}
