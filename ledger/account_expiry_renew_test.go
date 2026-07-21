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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// renewTestCred builds a 28-byte credential hash filled with b.
func renewTestCred(b byte) []byte {
	return bytes.Repeat([]byte{b}, 28)
}

// renewTestCredential builds a gouroboros stake Credential with the given tag
// (0 = key hash, 1 = script hash) and a 28-byte hash filled with b.
func renewTestCredential(tag uint, b byte) lcommon.Credential {
	return lcommon.Credential{
		CredType:   tag,
		Credential: lcommon.NewBlake2b224(renewTestCred(b)),
	}
}

// renewTestRewardAddress builds a reward (stake) address for the credential hash
// filled with b. header 0xE1 => key-hash reward address; 0xF1 => script-hash.
func renewTestRewardAddress(t *testing.T, header byte, b byte) *lcommon.Address {
	t.Helper()
	addr, err := lcommon.NewAddressFromBytes(
		append([]byte{header}, renewTestCred(b)...),
	)
	require.NoError(t, err)
	return &addr
}

// refKeys returns the dedup MapKeys of refs so a returned set can be compared
// independent of ordering (withdrawal map iteration order is nondeterministic).
func refKeys(refs []models.StakeCredentialRef) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.MapKey())
	}
	return out
}

func TestWitnessedRewardCredentials(t *testing.T) {
	t.Parallel()

	stakeKey := renewTestCredential(0, 0x01)
	scriptKey := renewTestCredential(1, 0x02)
	poolHash := lcommon.NewBlake2b224(renewTestCred(0x0A))

	tests := []struct {
		name     string
		build    func(t *testing.T, tx *mockledger.MockTransaction)
		wantRefs []models.StakeCredentialRef
	}{
		{
			name: "withdrawal key-hash reward account",
			build: func(t *testing.T, tx *mockledger.MockTransaction) {
				tx.WithWithdrawals(map[*lcommon.Address]uint64{
					renewTestRewardAddress(t, 0xE1, 0xAB): 5_000,
				})
			},
			wantRefs: []models.StakeCredentialRef{
				models.NewStakeCredentialRef(0, renewTestCred(0xAB)),
			},
		},
		{
			name: "withdrawal script-hash reward account",
			build: func(t *testing.T, tx *mockledger.MockTransaction) {
				tx.WithWithdrawals(map[*lcommon.Address]uint64{
					renewTestRewardAddress(t, 0xF1, 0xCD): 1,
				})
			},
			wantRefs: []models.StakeCredentialRef{
				models.NewStakeCredentialRef(1, renewTestCred(0xCD)),
			},
		},
		{
			name: "zero-amount withdrawal still witnesses",
			build: func(t *testing.T, tx *mockledger.MockTransaction) {
				tx.WithWithdrawals(map[*lcommon.Address]uint64{
					renewTestRewardAddress(t, 0xE1, 0xAB): 0,
				})
			},
			wantRefs: []models.StakeCredentialRef{
				models.NewStakeCredentialRef(0, renewTestCred(0xAB)),
			},
		},
		{
			name: "stake registration certificate",
			build: func(t *testing.T, tx *mockledger.MockTransaction) {
				tx.WithCertificates(&lcommon.StakeRegistrationCertificate{
					StakeCredential: stakeKey,
				})
			},
			wantRefs: []models.StakeCredentialRef{
				models.NewStakeCredentialRef(0, renewTestCred(0x01)),
			},
		},
		{
			name: "stake deregistration certificate",
			build: func(t *testing.T, tx *mockledger.MockTransaction) {
				tx.WithCertificates(&lcommon.StakeDeregistrationCertificate{
					StakeCredential: stakeKey,
				})
			},
			wantRefs: []models.StakeCredentialRef{
				models.NewStakeCredentialRef(0, renewTestCred(0x01)),
			},
		},
		{
			name: "stake delegation certificate",
			build: func(t *testing.T, tx *mockledger.MockTransaction) {
				tx.WithCertificates(&lcommon.StakeDelegationCertificate{
					StakeCredential: &stakeKey,
					PoolKeyHash:     poolHash,
				})
			},
			wantRefs: []models.StakeCredentialRef{
				models.NewStakeCredentialRef(0, renewTestCred(0x01)),
			},
		},
		{
			name: "vote delegation certificate",
			build: func(t *testing.T, tx *mockledger.MockTransaction) {
				tx.WithCertificates(&lcommon.VoteDelegationCertificate{
					StakeCredential: stakeKey,
					Drep:            lcommon.Drep{Type: lcommon.DrepTypeAbstain},
				})
			},
			wantRefs: []models.StakeCredentialRef{
				models.NewStakeCredentialRef(0, renewTestCred(0x01)),
			},
		},
		{
			name: "stake vote delegation certificate",
			build: func(t *testing.T, tx *mockledger.MockTransaction) {
				tx.WithCertificates(&lcommon.StakeVoteDelegationCertificate{
					StakeCredential: stakeKey,
					PoolKeyHash:     poolHash,
					Drep:            lcommon.Drep{Type: lcommon.DrepTypeAbstain},
				})
			},
			wantRefs: []models.StakeCredentialRef{
				models.NewStakeCredentialRef(0, renewTestCred(0x01)),
			},
		},
		{
			name: "registration certificate (conway)",
			build: func(t *testing.T, tx *mockledger.MockTransaction) {
				tx.WithCertificates(&lcommon.RegistrationCertificate{
					StakeCredential: scriptKey,
				})
			},
			wantRefs: []models.StakeCredentialRef{
				models.NewStakeCredentialRef(1, renewTestCred(0x02)),
			},
		},
		{
			name: "deregistration certificate (conway)",
			build: func(t *testing.T, tx *mockledger.MockTransaction) {
				tx.WithCertificates(&lcommon.DeregistrationCertificate{
					StakeCredential: scriptKey,
				})
			},
			wantRefs: []models.StakeCredentialRef{
				models.NewStakeCredentialRef(1, renewTestCred(0x02)),
			},
		},
		{
			name: "stake registration+delegation certificate",
			build: func(t *testing.T, tx *mockledger.MockTransaction) {
				tx.WithCertificates(&lcommon.StakeRegistrationDelegationCertificate{
					StakeCredential: stakeKey,
					PoolKeyHash:     poolHash,
				})
			},
			wantRefs: []models.StakeCredentialRef{
				models.NewStakeCredentialRef(0, renewTestCred(0x01)),
			},
		},
		{
			name: "stake vote registration+delegation certificate",
			build: func(t *testing.T, tx *mockledger.MockTransaction) {
				tx.WithCertificates(&lcommon.StakeVoteRegistrationDelegationCertificate{
					StakeCredential: stakeKey,
					PoolKeyHash:     poolHash,
					Drep:            lcommon.Drep{Type: lcommon.DrepTypeAbstain},
				})
			},
			wantRefs: []models.StakeCredentialRef{
				models.NewStakeCredentialRef(0, renewTestCred(0x01)),
			},
		},
		{
			name: "vote registration+delegation certificate",
			build: func(t *testing.T, tx *mockledger.MockTransaction) {
				tx.WithCertificates(&lcommon.VoteRegistrationDelegationCertificate{
					StakeCredential: stakeKey,
					Drep:            lcommon.Drep{Type: lcommon.DrepTypeAbstain},
				})
			},
			wantRefs: []models.StakeCredentialRef{
				models.NewStakeCredentialRef(0, renewTestCred(0x01)),
			},
		},
		{
			name: "withdrawal plus stake delegation (brief example)",
			build: func(t *testing.T, tx *mockledger.MockTransaction) {
				tx.WithWithdrawals(map[*lcommon.Address]uint64{
					renewTestRewardAddress(t, 0xE1, 0xAB): 5_000,
				})
				tx.WithCertificates(&lcommon.StakeDelegationCertificate{
					StakeCredential: &stakeKey,
					PoolKeyHash:     poolHash,
				})
			},
			wantRefs: []models.StakeCredentialRef{
				models.NewStakeCredentialRef(0, renewTestCred(0xAB)),
				models.NewStakeCredentialRef(0, renewTestCred(0x01)),
			},
		},
		{
			name: "duplicate credentials are collapsed",
			build: func(t *testing.T, tx *mockledger.MockTransaction) {
				tx.WithCertificates(
					&lcommon.StakeRegistrationCertificate{StakeCredential: stakeKey},
					&lcommon.StakeDelegationCertificate{
						StakeCredential: &stakeKey,
						PoolKeyHash:     poolHash,
					},
				)
			},
			wantRefs: []models.StakeCredentialRef{
				models.NewStakeCredentialRef(0, renewTestCred(0x01)),
			},
		},
		{
			name: "non-witnessing certificate contributes nothing",
			build: func(t *testing.T, tx *mockledger.MockTransaction) {
				tx.WithCertificates(&lcommon.PoolRetirementCertificate{
					PoolKeyHash: poolHash,
					Epoch:       5,
				})
			},
			wantRefs: nil,
		},
		{
			name: "payment only (no certs or withdrawals)",
			build: func(t *testing.T, tx *mockledger.MockTransaction) {
				// nothing witnessed
			},
			wantRefs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tx := mockledger.NewTransactionBuilder()
			tx.WithValid(true)
			tt.build(t, tx)

			got := witnessedRewardCredentials(tx)
			require.ElementsMatch(t, refKeys(tt.wantRefs), refKeys(got))
		})
	}
}

// newRenewTestLedger builds a DB-backed LedgerState with the CIP-0163
// delegator-inactivity gate configured. It uses the same sqlite/badger
// in-memory setup as the other ledger tests. renewWitnessedAccountExpirations
// only touches ls.config and ls.db, so no era/protocol-parameter state is
// required.
func newRenewTestLedger(
	t *testing.T,
	enabled bool,
	inactivity uint64,
) (*LedgerState, *database.Database) {
	t.Helper()
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() }) //nolint:errcheck
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger:                     slog.New(slog.NewTextHandler(io.Discard, nil)),
			DelegatorInactivityEnabled: enabled,
			DelegatorInactivity:        inactivity,
		},
	}
	return ls, db
}

// runRenew applies renewWitnessedAccountExpirations inside a real write
// transaction, mirroring how the block-application path calls it.
func runRenew(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	currentEpoch uint64,
	txs ...lcommon.Transaction,
) {
	t.Helper()
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.renewWitnessedAccountExpirations(txn, currentEpoch, txs)
	}))
}

// stakeDelegationTx builds a valid transaction that witnesses cred via a stake
// delegation certificate.
func stakeDelegationTx(cred []byte) lcommon.Transaction {
	stakeCred := lcommon.Credential{
		CredType:   0,
		Credential: lcommon.NewBlake2b224(cred),
	}
	tx := mockledger.NewTransactionBuilder()
	tx.WithValid(true)
	tx.WithCertificates(&lcommon.StakeDelegationCertificate{
		StakeCredential: &stakeCred,
		PoolKeyHash:     lcommon.NewBlake2b224(renewTestCred(0x0A)),
	})
	return tx
}

// TestRenewWitnessedAccountExpirationsGateOn verifies that, with the gate on,
// applying a block containing a stake delegation for a registered credential in
// epoch E sets the account's ExpirationEpoch to E + DelegatorInactivity.
func TestRenewWitnessedAccountExpirationsGateOn(t *testing.T) {
	const (
		epoch      = uint64(42)
		inactivity = uint64(90)
	)
	ls, db := newRenewTestLedger(t, true, inactivity)

	cred := renewTestCred(0x01)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    cred,
		CredentialTag: 0,
		Active:        true,
	}))

	runRenew(t, ls, db, epoch, stakeDelegationTx(cred))

	acct, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	require.Equal(t, epoch+inactivity, acct.ExpirationEpoch)
}

// TestRenewWitnessedAccountExpirationsGateOff verifies that, with the gate off,
// witnessing the same credential leaves ExpirationEpoch at its unset value (0).
func TestRenewWitnessedAccountExpirationsGateOff(t *testing.T) {
	const epoch = uint64(42)
	ls, db := newRenewTestLedger(t, false, 90)

	cred := renewTestCred(0x01)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    cred,
		CredentialTag: 0,
		Active:        true,
	}))

	runRenew(t, ls, db, epoch, stakeDelegationTx(cred))

	acct, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), acct.ExpirationEpoch)
}

// TestRenewWitnessedAccountExpirationsWithdrawal verifies that a reward
// withdrawal renews the withdrawn account's expiration (gate on).
func TestRenewWitnessedAccountExpirationsWithdrawal(t *testing.T) {
	const (
		epoch      = uint64(10)
		inactivity = uint64(90)
	)
	ls, db := newRenewTestLedger(t, true, inactivity)

	// A key-hash reward address (header 0xE1) over the 0xAB..AB credential.
	rewardAddr := renewTestRewardAddress(t, 0xE1, 0xAB)
	cred := renewTestCred(0xAB)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    cred,
		CredentialTag: 0,
		Active:        true,
	}))

	tx := mockledger.NewTransactionBuilder()
	tx.WithValid(true)
	tx.WithWithdrawals(map[*lcommon.Address]uint64{rewardAddr: 1_000})

	runRenew(t, ls, db, epoch, tx)

	acct, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	require.Equal(t, epoch+inactivity, acct.ExpirationEpoch)
}

// TestRenewWitnessedAccountExpirationsMissingRowIgnored verifies that
// witnessing a credential with no account row is a no-op: no error and no row
// is created (RenewAccountExpirations only touches existing rows).
func TestRenewWitnessedAccountExpirationsMissingRowIgnored(t *testing.T) {
	ls, db := newRenewTestLedger(t, true, 90)

	cred := renewTestCred(0x07)
	runRenew(t, ls, db, 5, stakeDelegationTx(cred))

	_, err := db.GetAccountByCredential(0, cred, true, nil)
	require.ErrorIs(t, err, models.ErrAccountNotFound)
}

// TestRenewWitnessedAccountExpirationsSkipsInvalidTx verifies that a
// phase-2-invalid transaction witnesses nothing (its certificates and
// withdrawals are not applied by the ledger), so it does not renew.
func TestRenewWitnessedAccountExpirationsSkipsInvalidTx(t *testing.T) {
	ls, db := newRenewTestLedger(t, true, 90)

	cred := renewTestCred(0x01)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    cred,
		CredentialTag: 0,
		Active:        true,
	}))

	stakeCred := lcommon.Credential{
		CredType:   0,
		Credential: lcommon.NewBlake2b224(cred),
	}
	tx := mockledger.NewTransactionBuilder()
	tx.WithValid(false) // phase-2 failed: no certs/withdrawals applied
	tx.WithCertificates(&lcommon.StakeDelegationCertificate{
		StakeCredential: &stakeCred,
		PoolKeyHash:     lcommon.NewBlake2b224(renewTestCred(0x0A)),
	})

	runRenew(t, ls, db, 5, tx)

	acct, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), acct.ExpirationEpoch)
}
