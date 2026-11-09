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

package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

func TestSharedSQLStoreAccountParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	activeKey := bytes.Repeat([]byte{0x11}, 28)
	inactiveKey := bytes.Repeat([]byte{0x22}, 28)
	require.NoError(t, store.CreateAccount(
		nil,
		&models.Account{
			StakingKey: activeKey, CredentialTag: 0,
			Pool: []byte("pool-a"), AddedSlot: 10, CreatedSlot: 5,
			CertificateID: 2, Reward: 30, Active: true,
			ExpirationEpoch: 100,
		},
	))
	require.NoError(t, store.CreateAccount(
		nil,
		&models.Account{
			StakingKey: inactiveKey, CredentialTag: 1,
			AddedSlot: 20, CreatedSlot: 15, Reward: 40,
		},
	))
	require.NoError(t, store.ImportAccount(
		&models.Account{
			StakingKey: activeKey, CredentialTag: 0,
			Pool: []byte("pool-b"), Drep: []byte("drep-a"),
			AddedSlot: 999, CreatedSlot: 999, CertificateID: 999,
			Reward: 50, DrepType: 1, Active: true,
			ExpirationEpoch: 999,
		},
		nil,
	))

	refs := []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, activeKey),
		models.NewStakeCredentialRef(1, inactiveKey),
		models.NewStakeCredentialRef(0, []byte("missing")),
	}

	var err error
	gotActive, err := store.GetAccountByCredential(0, activeKey, false, nil)
	require.NoError(t, err)
	gotInactiveHidden, err := store.GetAccountByCredential(
		1,
		inactiveKey,
		false,
		nil,
	)
	require.NoError(t, err)
	gotInactive, err := store.GetAccountByCredential(
		1,
		inactiveKey,
		true,
		nil,
	)
	require.NoError(t, err)
	gotActiveBatch, err := store.GetAccountsByCredential(refs, false, nil)
	require.NoError(t, err)
	gotAllBatch, err := store.GetAccountsByCredential(refs, true, nil)
	require.NoError(t, err)
	require.NoError(t, store.RenewAccountExpirations(refs, 55, nil))
	gotRenewed, err := store.GetAccountByCredential(0, activeKey, true, nil)
	require.NoError(t, err)
	gotActiveRefs, err := store.GetActiveAccountCredentials(nil)
	require.NoError(t, err)
	gotStamped, err := store.StampAllActiveAccountExpirations(77, nil)
	require.NoError(t, err)
	_, err = store.AccountInactivityActivationMembership(
		refs,
		nil,
	)
	require.NoError(t, err)
	_, err = store.ResetAccountExpirationActivation(nil)
	require.NoError(t, err)
	_, err = store.GetAccountByCredential(0, activeKey, true, nil)
	require.NoError(t, err)
	require.NoError(t, store.AddAccountRewardByCredential(
		0, activeKey, 10, 80, []byte("credit"), nil,
	))
	require.NoError(t, store.AddAccountRewardByCredential(
		0, activeKey, 10, 80, []byte("credit"), nil,
	))
	gotAfterCredit, err := store.GetAccountByCredential(0, activeKey, true, nil)
	require.NoError(t, err)
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, activeKey, 60, 90, []byte("withdraw"), nil,
	))
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, activeKey, 60, 90, []byte("withdraw"), nil,
	))
	gotAfterWithdrawal, err := store.GetAccountByCredential(
		0, activeKey, true, nil,
	)
	require.NoError(t, err)
	require.NoError(t, store.DeleteAccountRewardsAfterSlot(85, nil))
	_, err = store.GetAccountByCredential(
		0, activeKey, true, nil,
	)
	require.NoError(t, err)
	require.NoError(t, store.DeleteAccountRewardsAfterSlot(75, nil))
	gotAfterCreditRollback, err := store.GetAccountByCredential(
		0, activeKey, true, nil,
	)
	require.NoError(t, err)
	_, err = store.GetAccountSumsByCredential(0, activeKey, nil)
	require.NoError(t, err)
	require.NotNil(t, gotAfterCreditRollback)
	require.True(t, gotAfterCreditRollback.Active)
	require.NoError(t, store.DeactivateAccounts(
		nil,
		[]models.StakeCredentialRef{
			models.NewStakeCredentialRef(0, activeKey),
		},
		1_000,
	))
	gotDeactivated, err := store.GetAccountByCredential(
		0,
		activeKey,
		true,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, gotActive)
	require.Nil(t, gotInactiveHidden)
	require.NotNil(t, gotInactive)
	require.Len(t, gotActiveBatch, 1)
	require.Len(t, gotAllBatch, 2)
	require.NotNil(t, gotRenewed)
	require.Equal(t, uint64(55), gotRenewed.ExpirationEpoch)
	require.Equal(t, []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x11}, 28)),
	}, gotActiveRefs)
	require.Equal(t, int64(1), gotStamped)
	require.NotNil(t, gotAfterCredit)
	require.Equal(t, uint64(60), uint64(gotAfterCredit.Reward))
	require.NotNil(t, gotAfterWithdrawal)
	require.Zero(t, gotAfterWithdrawal.Reward)
	require.NotNil(t, gotAfterCreditRollback)
	require.Equal(t, uint64(50), uint64(gotAfterCreditRollback.Reward))
	require.NotNil(t, gotDeactivated)
	require.False(t, gotDeactivated.Active)
}

// Two openers racing to create the same metadata database must both get
// through, because the loser waits out the winner's journal_mode conversion
// instead of failing on it. Before the fix this surfaced as
// "ping write database: database is locked (5) (SQLITE_BUSY)" from whichever
// opener lost, which is what made TestPhase1ConcurrentFirstOpenOneWinnerOne-
// Mismatch flaky in CI: the loser died before it could reach the node
// settings comparison the test was actually asserting on.
func TestConcurrentFirstOpenDoesNotFailOnLockedDatabase(t *testing.T) {
	const openers = 8
	dataDir := t.TempDir()

	var wg sync.WaitGroup
	errs := make([]error, openers)
	wg.Add(openers)
	for i := range openers {
		go func() {
			defer wg.Done()
			store, err := NewSQLStore(
				Config{DataDir: dataDir},
				metadata.ProviderDependencies{},
			)
			if err != nil {
				errs[i] = err
				return
			}
			defer func() {
				_ = store.Close()
			}()
			// Start, not construction: constructing only builds the pools,
			// and the WAL conversion is deliberately deferred to Start so
			// that constructing a store does not materialise the database
			// file. Start is also where the original failure surfaced, as
			// "ping write database: database is locked".
			errs[i] = store.Start(t.Context())
		}()
	}
	wg.Wait()

	for i, err := range errs {
		require.NoErrorf(t, err, "opener %d failed to start", i)
	}
}

func TestSharedStoreDeferredIndexLifecycle(t *testing.T) {
	t.Parallel()
	store, writeDB, _, err := openSQLStore(
		Config{},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	require.True(t, sqliteIndexExists(t, writeDB, "idx_utxo_payment_key"))
	require.True(t, sqliteIndexExists(t, writeDB, "idx_datum_added_slot"))

	require.NoError(t, store.DropDeferredIndexes())
	pending, err := store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(t, pending)
	require.False(t, sqliteIndexExists(t, writeDB, "idx_utxo_payment_key"))
	require.False(t, sqliteIndexExists(t, writeDB, "idx_datum_added_slot"))

	require.NoError(t, store.BuildCriticalDeferredIndexes())
	require.True(t, sqliteIndexExists(t, writeDB, "idx_utxo_payment_key"))
	require.False(t, sqliteIndexExists(t, writeDB, "idx_datum_added_slot"))
	pending, err = store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(t, pending)

	require.NoError(t, store.BuildDeferredIndexes())
	require.True(t, sqliteIndexExists(t, writeDB, "idx_datum_added_slot"))
	pending, err = store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.False(t, pending)
}

func sqliteIndexExists(t *testing.T, db interface {
	QueryRow(string, ...any) *sql.Row
}, name string) bool {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?",
		name,
	).Scan(&count))
	return count == 1
}

func TestSharedSQLStoreDrepParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	createdCredential := bytes.Repeat([]byte{0x41}, 28)
	importedCredential := bytes.Repeat([]byte{0x42}, 28)
	missingCredential := bytes.Repeat([]byte{0x43}, 28)
	// A registration row shaped the way ledger-state import and genesis
	// seeding write it: a real deposit but no certificate, so
	// certificate_id lands at 0. This is the case that motivates the
	// deposit queries existing separately from
	// GetDrepLastRegistrationSlot, whose certificate_id filter drops
	// exactly these rows. Copying that filter across is the natural
	// mistake, and it would show up here as a refund of 0 rather than as
	// a missing method.
	importOnlyCredential := bytes.Repeat([]byte{0x44}, 28)
	// Two registration rows for one credential, so the latest-row rule is
	// exercised rather than assumed from a single row.
	rereggedCredential := bytes.Repeat([]byte{0x45}, 28)
	// Registered, with a real deposit, but no longer active. Readable
	// through the singular form and excluded from the batched one, which
	// is scoped to the active set its callers list.
	inactiveCredential := bytes.Repeat([]byte{0x46}, 28)

	created := &models.Drep{
		Credential: createdCredential, AddedSlot: 10,
		AnchorURL: "created", AnchorHash: []byte("created-hash"),
	}
	require.NoError(t, store.CreateDrep(nil, created))
	require.NoError(t, store.SetDrep(
		0,
		createdCredential,
		12,
		"inactive",
		[]byte("inactive-hash"),
		false,
		nil,
	))
	require.NoError(t, store.InsertDrepIfAbsent(
		0,
		createdCredential,
		99,
		"ignored",
		[]byte("ignored"),
		true,
		nil,
	))

	imported := &models.Drep{
		CredentialTag: 1, Credential: importedCredential,
		AddedSlot: 20, AnchorURL: "imported",
		AnchorHash: []byte("imported-hash"), Active: true,
	}
	registration := &models.RegistrationDrep{
		CredentialTag: 1, DrepCredential: importedCredential,
		AddedSlot: 21, CertificateID: 7, AnchorURL: "registered",
		AnchorHash: []byte("registration-hash"), DepositAmount: 500,
	}
	require.NoError(t, store.ImportDrep(imported, registration, nil))
	require.NoError(t, store.ImportDrep(
		&models.Drep{
			Credential: importOnlyCredential, AddedSlot: 22,
			AnchorURL: "import-only", Active: true,
		},
		&models.RegistrationDrep{
			DrepCredential: importOnlyCredential,
			AddedSlot:      22,
			AnchorURL:      "import-only",
			DepositAmount:  500000000,
		},
		nil,
	))
	require.NoError(t, store.ImportDrep(
		&models.Drep{
			Credential: rereggedCredential, AddedSlot: 23,
			Active: true,
		},
		&models.RegistrationDrep{
			DrepCredential: rereggedCredential,
			AddedSlot:      23,
			DepositAmount:  400000000,
		},
		nil,
	))
	require.NoError(t, store.ImportDrep(
		&models.Drep{
			Credential: rereggedCredential, AddedSlot: 44,
			Active: true,
		},
		&models.RegistrationDrep{
			DrepCredential: rereggedCredential,
			AddedSlot:      44,
			CertificateID:  11,
			DepositAmount:  300000000,
		},
		nil,
	))
	require.NoError(t, store.ImportDrep(
		&models.Drep{
			Credential: inactiveCredential, AddedSlot: 24,
			Active: false,
		},
		&models.RegistrationDrep{
			DrepCredential: inactiveCredential,
			AddedSlot:      24,
			CertificateID:  12,
			DepositAmount:  200000000,
		},
		nil,
	))
	require.NoError(t, store.UpdateDRepActivity(
		1,
		importedCredential,
		30,
		5,
		300,
		nil,
	))
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: bytes.Repeat([]byte{0x51}, 28),
		Pool:       []byte("pool-a"), Drep: importedCredential,
		DrepType: 1, Active: true, Reward: 100,
	}))
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey:    bytes.Repeat([]byte{0x50}, 28),
		CredentialTag: 1, Drep: importedCredential,
		DrepType: 1, Active: true, Reward: 200,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       bytes.Repeat([]byte{0x61}, 32),
		StakingKey: bytes.Repeat([]byte{0x51}, 28),
		Amount:     400,
	}))
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: bytes.Repeat([]byte{0x52}, 28),
		DrepType:   models.DrepTypeAlwaysAbstain,
		Active:     true, Reward: 30, AddedSlot: 22,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       bytes.Repeat([]byte{0x62}, 32),
		StakingKey: bytes.Repeat([]byte{0x52}, 28),
		Amount:     20,
	}))

	var err error
	gotCreated, err := store.GetDrep(createdCredential, true, nil)
	require.NoError(t, err)
	gotImported, err := store.GetDrepByCredential(
		1,
		importedCredential,
		true,
		nil,
	)
	require.NoError(t, err)
	gotInactiveHidden, err := store.GetDrepByCredential(
		0,
		createdCredential,
		false,
		nil,
	)
	require.NoError(t, err)
	gotInactive, err := store.GetDrepByCredential(
		0,
		createdCredential,
		true,
		nil,
	)
	require.NoError(t, err)
	gotActive, err := store.GetActiveDreps(nil)
	require.NoError(t, err)
	gotDelegators, err := store.GetDRepDelegators(
		1,
		importedCredential,
		nil,
	)
	require.NoError(t, err)
	_, err = store.GetExpiredDReps(35, nil)
	require.NoError(t, err)
	_, err = store.GetDrepLastRegistrationSlot(
		1,
		importedCredential,
		nil,
	)
	require.NoError(t, err)
	_, err = store.GetDrepLastRegistrationSlot(
		0,
		missingCredential,
		nil,
	)
	require.NoError(t, err)

	// The deposit queries, which deregistration-refund validation reads.
	// The import-shaped row is the load-bearing assertion: it carries no
	// certificate, so a certificate_id filter here would return 0 and a
	// refund would be validated against the wrong amount.
	gotImportedDeposit, err := store.GetDrepLastRegistrationDeposit(
		0,
		importOnlyCredential,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, gotImportedDeposit)
	require.Equal(t, uint64(500000000), *gotImportedDeposit)
	// A row that does carry a certificate must still be found, so the
	// query is not merely inverting the filter.
	gotCertifiedDeposit, err := store.GetDrepLastRegistrationDeposit(
		1,
		importedCredential,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, gotCertifiedDeposit)
	require.Equal(t, uint64(500), *gotCertifiedDeposit)
	// Two registration rows for one credential: the later one wins, so an
	// earlier import placeholder cannot shadow a real re-registration.
	gotLatestDeposit, err := store.GetDrepLastRegistrationDeposit(
		0,
		rereggedCredential,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, gotLatestDeposit)
	require.Equal(t, uint64(300000000), *gotLatestDeposit)
	// A registered but inactive credential is still readable through the
	// singular form, which does not consult drep.active.
	gotInactiveDeposit, err := store.GetDrepLastRegistrationDeposit(
		0,
		inactiveCredential,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, gotInactiveDeposit)
	require.Equal(t, uint64(200000000), *gotInactiveDeposit)
	// No registration history at all reports 0 rather than erroring.
	gotMissingDeposit, err := store.GetDrepLastRegistrationDeposit(
		0,
		missingCredential,
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, gotMissingDeposit)

	// The batched form must agree with the singular one on both rows.
	// This is the only thing that executes its derived-table join.
	gotDeposits, err := store.GetDrepLastRegistrationDeposits(nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(500000000),
		gotDeposits[models.DrepDepositKey(0, importOnlyCredential)],
	)
	require.Equal(
		t,
		uint64(500),
		gotDeposits[models.DrepDepositKey(1, importedCredential)],
	)
	require.NotContains(
		t,
		gotDeposits,
		models.DrepDepositKey(0, missingCredential),
	)
	// The batched form is scoped to the active set, so the exact map is
	// what pins that down: the inactive credential is excluded even though
	// it has a registration row with a real deposit, and the created
	// credential is excluded because it has none.
	require.Equal(
		t,
		map[string]uint64{
			models.DrepDepositKey(1, importedCredential):   500,
			models.DrepDepositKey(0, importOnlyCredential): 500000000,
			models.DrepDepositKey(0, rereggedCredential):   300000000,
		},
		gotDeposits,
	)
	err = store.UpdateDRepActivity(0, missingCredential, 1, 1, 10, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, models.ErrDrepActivityNotUpdated))
	_ = err.Error()
	_, err = store.GetDRepVotingPower(
		1,
		importedCredential,
		0,
		nil,
	)
	require.NoError(t, err)
	gotVotingPowerBatch, err := store.GetDRepVotingPowerBatch(
		[]models.StakeCredentialRef{
			models.NewStakeCredentialRef(1, importedCredential),
			models.NewStakeCredentialRef(0, missingCredential),
		},
		0,
		nil,
	)
	require.NoError(t, err)
	_, err = store.GetDRepVotingPowerByType(
		[]uint64{
			models.DrepTypeAlwaysAbstain,
			models.DrepTypeAlwaysNoConfidence,
		},
		0,
		nil,
	)
	require.NoError(t, err)
	gotDreps, err := store.GetDreps(nil)
	require.NoError(t, err)
	_, err = store.GetPredefinedDrepFirstSeenSlots(nil)
	require.NoError(t, err)
	gotLiveStake, err := store.GetLiveStakeInputsForPools(
		[][]byte{[]byte("pool-a"), []byte("pool-a")},
		0,
		nil,
	)
	require.NoError(t, err)
	_, err = store.RewardLiveStakeNeedsBackfill(nil)
	require.NoError(t, err)
	require.NoError(t, store.RebuildRewardLiveStake(88, nil))
	gotLiveStakeAfterRebuild, err := store.GetLiveStakeInputsForPools(
		[][]byte{[]byte("pool-a")},
		0,
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, store.DeactivateDreps(
		nil,
		[]models.StakeCredentialRef{
			models.NewStakeCredentialRef(1, importedCredential),
		},
	))
	_, err = store.ClearDanglingDRepDelegations(99, nil)
	require.NoError(t, err)
	_, err = store.GetDrepByCredential(
		1,
		importedCredential,
		true,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, gotCreated)
	require.Equal(t, uint64(12), gotCreated.AddedSlot)
	require.Equal(t, "inactive", gotCreated.AnchorURL)
	require.NotNil(t, gotImported)
	require.Equal(t, uint64(30), gotImported.LastActivityEpoch)
	require.Equal(t, uint64(35), gotImported.ExpiryEpoch)
	require.Nil(t, gotInactiveHidden)
	require.NotNil(t, gotInactive)
	require.False(t, gotInactive.Active)
	require.Len(t, gotActive, 3)
	activeDrepCredentials := make([]string, len(gotActive))
	for i, drep := range gotActive {
		activeDrepCredentials[i] = models.DrepDepositKey(
			drep.CredentialTag, drep.Credential,
		)
	}
	require.ElementsMatch(t, []string{
		models.DrepDepositKey(1, bytes.Repeat([]byte{0x42}, 28)),
		models.DrepDepositKey(0, bytes.Repeat([]byte{0x44}, 28)),
		models.DrepDepositKey(0, bytes.Repeat([]byte{0x45}, 28)),
	}, activeDrepCredentials)
	require.Len(t, gotDelegators, 2)
	require.ElementsMatch(t, []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x51}, 28)),
		models.NewStakeCredentialRef(1, bytes.Repeat([]byte{0x50}, 28)),
	}, gotDelegators)
	firstSeenSlots := map[string]uint64{
		models.DrepDepositKey(0, bytes.Repeat([]byte{0x41}, 28)): 12,
		models.DrepDepositKey(1, bytes.Repeat([]byte{0x42}, 28)): 21,
		models.DrepDepositKey(0, bytes.Repeat([]byte{0x44}, 28)): 22,
		models.DrepDepositKey(0, bytes.Repeat([]byte{0x45}, 28)): 23,
		models.DrepDepositKey(0, bytes.Repeat([]byte{0x46}, 28)): 24,
	}
	lastRegistrationSlots := map[string]uint64{
		models.DrepDepositKey(0, bytes.Repeat([]byte{0x41}, 28)): 0,
		models.DrepDepositKey(1, bytes.Repeat([]byte{0x42}, 28)): 21,
		models.DrepDepositKey(0, bytes.Repeat([]byte{0x44}, 28)): 0,
		models.DrepDepositKey(0, bytes.Repeat([]byte{0x45}, 28)): 44,
		models.DrepDepositKey(0, bytes.Repeat([]byte{0x46}, 28)): 24,
	}
	gotDrepSlots := make(map[string]uint64, len(gotDreps))
	gotDrepLastRegistrationSlots := make(map[string]uint64, len(gotDreps))
	for _, drep := range gotDreps {
		key := models.DrepDepositKey(drep.CredentialTag, drep.Credential)
		gotDrepSlots[key] = drep.FirstSeenSlot
		gotDrepLastRegistrationSlots[key] = drep.LastRegistrationSlot
	}
	require.Equal(t, firstSeenSlots, gotDrepSlots)
	require.Equal(t, lastRegistrationSlots, gotDrepLastRegistrationSlots)
	require.Len(t, gotDeposits, 3)
	require.Equal(t, map[string]uint64{
		models.DrepDepositKey(1, bytes.Repeat([]byte{0x42}, 28)): 700,
	}, gotVotingPowerBatch)
	wantLiveStake := []*models.RewardStakeInput{{
		PoolKeyHash:   []byte("pool-a"),
		StakingKey:    bytes.Repeat([]byte{0x51}, 28),
		CredentialTag: 0,
		Stake:         types.Uint64(500),
		Registered:    true,
	}}
	require.Equal(t, wantLiveStake, gotLiveStake)
	require.Equal(t, wantLiveStake, gotLiveStakeAfterRebuild)
}

// cascadeForeignKey is one ON DELETE CASCADE foreign key as the live schema
// declares it.
type cascadeForeignKey struct {
	table  string
	column string
	parent string
}

func (fk cascadeForeignKey) String() string {
	return fmt.Sprintf("%s.%s -> %s", fk.table, fk.column, fk.parent)
}

// newCoreModeSQLStore opens a core-mode store on its own data directory. Core
// is the mode a block producer or relay runs, and the mode the rollback sweep
// that motivates these tests runs in.
func newCoreModeSQLStore(t *testing.T) (*sqlstore.Store, *sql.DB) {
	t.Helper()
	store, writeDB, _, err := openSQLStore(
		Config{},
		metadata.ProviderDependencies{StorageMode: types.StorageModeCore},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() {
		// Logged rather than asserted: require calls FailNow, which stops
		// the remaining cleanup callbacks and leaks the t.TempDir removal
		// registered before this one.
		if err := store.Close(); err != nil {
			t.Logf("closing store: %v", err)
		}
	})
	return store, writeDB
}

// cascadeForeignKeys reads every ON DELETE CASCADE foreign key out of the
// schema the migrations produced, rather than restating a list that a later
// migration could add to without anyone revisiting this file.
func cascadeForeignKeys(t *testing.T, db *sql.DB) []cascadeForeignKey {
	t.Helper()
	rows, err := db.Query(`
SELECT m.name, fk."from", fk."table"
FROM sqlite_master m
JOIN pragma_foreign_key_list(m.name) fk
WHERE m.type = 'table' AND fk.on_delete = 'CASCADE'
ORDER BY m.name, fk."from"`)
	require.NoError(t, err)
	defer rows.Close()
	var out []cascadeForeignKey
	for rows.Next() {
		var fk cascadeForeignKey
		require.NoError(t, rows.Scan(&fk.table, &fk.column, &fk.parent))
		out = append(out, fk)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(
		t,
		out,
		"the schema must declare cascading foreign keys for this to test "+
			"anything",
	)
	return out
}

// coveringIndexes names the indexes whose leftmost column is column, which are
// the ones SQLite can use to answer the cascade's lookup. An index that
// mentions the column in any later position cannot.
func coveringIndexes(
	t *testing.T,
	db *sql.DB,
	table, column string,
) []string {
	t.Helper()
	rows, err := db.Query(`
SELECT il.name
FROM pragma_index_list(?) il
JOIN pragma_index_info(il.name) ii ON ii.seqno = 0
WHERE ii.name = ?`, table, column)
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		out = append(out, name)
	}
	require.NoError(t, rows.Err())
	return out
}

// cascadeChildLookupIsIndexed reports whether SQLite can find a cascading
// child row without scanning the table. An INTEGER PRIMARY KEY is the rowid
// itself, so it does not appear in pragma_index_list even though it is already
// the most direct lookup path.
func cascadeChildLookupIsIndexed(
	t *testing.T,
	db *sql.DB,
	table, column string,
) bool {
	t.Helper()
	if len(coveringIndexes(t, db, table, column)) > 0 {
		return true
	}

	var columnType string
	var primaryKey int
	err := db.QueryRow(
		`SELECT type, pk FROM pragma_table_info(?) WHERE name = ?`,
		table,
		column,
	).Scan(&columnType, &primaryKey)
	if err != nil {
		require.NoError(t, err)
		return false
	}
	return primaryKey == 1 && strings.EqualFold(columnType, "INTEGER")
}

// requireCascadeChildColumnsIndexed asserts that every cascading foreign key
// can be enforced by an index lookup rather than a scan of the child table.
func requireCascadeChildColumnsIndexed(
	t *testing.T,
	db *sql.DB,
	fks []cascadeForeignKey,
	when string,
) {
	t.Helper()
	for _, fk := range fks {
		require.True(
			t,
			cascadeChildLookupIsIndexed(t, db, fk.table, fk.column),
			"%s: %s is ON DELETE CASCADE with no index on the child "+
				"column, so deleting one parent row scans all of %s",
			when,
			fk,
			fk.table,
		)
	}
}

// TestCascadeChildColumnsIndexedAfterCriticalRebuild is the manifest
// classification this file exists for.
//
// mithril/sync.go rebuilds only the critical subset of the deferred-index
// manifest before it marks the database ready, and the rollback sweep the node
// runs from its first reconciliation onward deletes "transaction" rows whose
// children cascade. A cascading child column left without an index at that
// point turns each deleted parent row into a full scan of the child table:
// measured on a preview relay, one 1,001-transaction rollback against a 3.2M
// row utxo table took 556s with idx_utxo_transaction_id absent and 0.095s with
// it present.
//
// The invariant is asserted over the schema's own foreign keys rather than
// over the one index that motivated it, so a new cascading foreign key whose
// child index is classified lazy fails here instead of in production.
func TestCascadeChildColumnsIndexedAfterCriticalRebuild(t *testing.T) {
	t.Parallel()
	store, db := newCoreModeSQLStore(t)
	fks := cascadeForeignKeys(t, db)
	require.Contains(
		t,
		fks,
		cascadeForeignKey{
			table:  "utxo",
			column: "transaction_id",
			parent: "transaction",
		},
		"the rollback's DELETE FROM \"transaction\" must still cascade "+
			"into utxo for this test to cover the sweep it describes",
	)
	requireCascadeChildColumnsIndexed(t, db, fks, "before deferring indexes")

	// The Mithril bootstrap sequence: drop the manifest for the bulk load,
	// then rebuild only the critical subset before the database is marked
	// ready. The lazy remainder is finished by later maintenance, and on a
	// database whose pending marker an older sync's blanket clear wiped,
	// never — so whatever the rollback path needs has to be in this
	// subset.
	require.NoError(t, store.DropDeferredIndexes())
	require.NoError(t, store.BuildCriticalDeferredIndexes())

	requireCascadeChildColumnsIndexed(
		t,
		db,
		fks,
		"after BuildCriticalDeferredIndexes",
	)
}

// consumeTx builds a transaction with the given hash byte that consumes the one
// producer input and produces a single output, mirroring a Leios endorser-block
// transaction.
func consumeTx(hashByte byte, input mockTransactionInput) *mockTransaction {
	h := lcommon.Blake2b256{}
	h[0] = hashByte
	return &mockTransaction{
		hash:     h,
		isValid:  true,
		consumed: []lcommon.TransactionInput{input},
		produced: []lcommon.Utxo{{
			Id:     mockTransactionInput{hash: h, index: 0},
			Output: &mockTransactionOutput{amount: big.NewInt(600)},
		}},
	}
}

// TestSharedSQLStoreLeiosClosureTolerateDoubleConsume covers the cross-EB
// double-consume that wedged the Musashi ledger pipeline: two certified
// endorser-block transactions name the same input across blocks. The reference
// ledger's applyLeiosClosure (ValidateNone) folds the closure without
// re-validation, so the second consume of an already-spent input is a no-op.
// SetTransaction (ranking-block path) must still reject it as a double-spend,
// while SetTransactionLeiosClosure must tolerate it and still write the second
// transaction's produced output.
func TestSharedSQLStoreLeiosClosureTolerateDoubleConsume(t *testing.T) {
	t.Parallel()

	producerHash := lcommon.Blake2b256{}
	producerHash[0] = 0xa1
	input := mockTransactionInput{hash: producerHash, index: 0}

	// newStoreWithSpentInput returns a store whose producer input has already
	// been spent by an earlier certified transaction (txA).
	newStoreWithSpentInput := func(t *testing.T) (*sqlstore.Store, *mockTransaction) {
		t.Helper()
		store, _ := newSharedSQLStore(t)
		require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
			TxId: producerHash.Bytes(), OutputIdx: 0, Amount: 700, AddedSlot: 5,
		}))
		txA := consumeTx(0xa2, input)
		pointA := ocommon.Point{Slot: 10, Hash: bytes.Repeat([]byte{0xc1}, 32)}
		require.NoError(t, store.SetTransaction(txA, pointA, 0, nil, true, nil, 0))
		return store, txA
	}

	// Ranking-block path: the second consume of the already-spent input must
	// fail with ErrUtxoConflict.
	t.Run("ranking block rejects double consume", func(t *testing.T) {
		t.Parallel()
		store, _ := newStoreWithSpentInput(t)
		txB := consumeTx(0xb2, input)
		pointB := ocommon.Point{Slot: 20, Hash: bytes.Repeat([]byte{0xc2}, 32)}
		err := store.SetTransaction(txB, pointB, 0, nil, true, nil, 0)
		require.Error(t, err)
		require.ErrorIs(t, err, types.ErrUtxoConflict)
	})

	// Leios closure path: the second consume is a no-op, the call succeeds, the
	// producer input stays spent by the first transaction, and the second
	// transaction's produced output is written.
	t.Run("leios closure tolerates double consume", func(t *testing.T) {
		t.Parallel()
		store, txA := newStoreWithSpentInput(t)
		txB := consumeTx(0xb3, input)
		pointB := ocommon.Point{Slot: 20, Hash: bytes.Repeat([]byte{0xc3}, 32)}
		require.NoError(
			t,
			store.SetTransactionLeiosClosure(txB, pointB, 0, nil, true, nil, 0),
		)

		// Producer input remains spent by the first (earlier certified) tx.
		spent, err := store.GetUtxoIncludingSpent(producerHash.Bytes(), 0, nil)
		require.NoError(t, err)
		require.NotNil(t, spent)
		require.True(
			t,
			bytes.Equal(txA.hash.Bytes(), spent.SpentAtTxId[:]),
			"producer input must remain spent by the first certified tx",
		)

		// The second transaction and its produced output are recorded.
		stored, err := store.GetTransactionByHash(txB.hash.Bytes(), nil)
		require.NoError(t, err)
		require.NotNil(t, stored)
		producedB, err := store.GetUtxo(txB.hash.Bytes(), 0, nil)
		require.NoError(t, err)
		require.NotNil(t, producedB)
		require.Equal(t, uint64(600), uint64(producedB.Amount))
	})
}

func normalizeOffchainTimes(docs []models.OffchainMetadata) {
	for i := range docs {
		docs[i].CreatedAt = time.Time{}
		docs[i].UpdatedAt = time.Time{}
	}
}

func normalizeOffchainDocument(doc *models.OffchainMetadata) {
	if doc == nil {
		return
	}
	doc.CreatedAt = time.Time{}
	doc.UpdatedAt = time.Time{}
}

func TestSharedSQLStoreRewardStateParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	blocks := uint64(4)
	totalBlocks := uint64(10)

	require.NoError(t, store.SaveRewardAdaPots(
		&models.RewardAdaPots{
			Epoch:        5,
			Treasury:     1,
			Reserves:     2,
			Fees:         3,
			Rewards:      4,
			CapturedSlot: 50,
		},
		nil,
	))
	require.NoError(t, store.SaveRewardAdaPots(
		&models.RewardAdaPots{
			Epoch:        5,
			Treasury:     10,
			Reserves:     20,
			Fees:         30,
			Rewards:      40,
			CapturedSlot: 55,
		},
		nil,
	))
	require.NoError(t, store.SaveRewardSnapshot(
		&models.RewardSnapshot{
			Epoch:            5,
			SnapshotType:     "mark",
			TotalActiveStake: 100,
			TotalPoolCount:   1,
			TotalDelegators:  2,
			CapturedSlot:     50,
			BoundarySlot:     49,
			EpochNonce:       []byte("authoritative"),
			ProtocolVersion:  10,
			Authoritative:    true,
		},
		nil,
	))
	authoritativeClaim, err := store.ClaimFallbackRewardSnapshot(
		&models.RewardSnapshot{
			Epoch:        5,
			SnapshotType: "mark",
		},
		nil,
	)
	require.NoError(t, err)
	fallbackClaim, err := store.ClaimFallbackRewardSnapshot(
		&models.RewardSnapshot{
			Epoch:            6,
			SnapshotType:     "mark",
			TotalActiveStake: 200,
			TotalPoolCount:   2,
			TotalDelegators:  3,
			CapturedSlot:     60,
			BoundarySlot:     59,
			EpochNonce:       []byte("fallback-one"),
			ProtocolVersion:  10,
		},
		nil,
	)
	require.NoError(t, err)
	fallbackClaim, err = store.ClaimFallbackRewardSnapshot(
		&models.RewardSnapshot{
			Epoch:            6,
			SnapshotType:     "mark",
			TotalActiveStake: 250,
			TotalPoolCount:   3,
			TotalDelegators:  4,
			CapturedSlot:     61,
			BoundarySlot:     60,
			EpochNonce:       []byte("fallback-two"),
			ProtocolVersion:  11,
		},
		nil,
	)
	require.NoError(t, err)

	guardTxn := store.Transaction(t.Context())
	guardCreated, guardID, err := store.ClaimFallbackRewardSnapshotGuard(
		7,
		"mark",
		guardTxn,
	)
	require.NoError(t, err)
	require.NoError(t, store.ReleaseFallbackRewardSnapshotGuard(
		guardID,
		guardTxn,
	))
	require.NoError(t, guardTxn.Commit())

	require.NoError(t, store.SaveRewardSnapshot(
		&models.RewardSnapshot{
			Epoch:        8,
			SnapshotType: "mark",
		},
		nil,
	))
	provisionalTxn := store.Transaction(t.Context())
	provisionalGuard, _, err :=
		store.ClaimFallbackRewardSnapshotGuard(
			8,
			"mark",
			provisionalTxn,
		)
	require.NoError(t, err)
	require.NoError(t, provisionalTxn.Commit())

	authoritativeTxn := store.Transaction(t.Context())
	authoritativeGuard, _, err := store.ClaimFallbackRewardSnapshotGuard(
		5,
		"mark",
		authoritativeTxn,
	)
	require.NoError(t, err)
	require.NoError(t, authoritativeTxn.Commit())

	require.NoError(t, store.SaveRewardPoolInputs(
		[]*models.RewardPoolInput{
			{
				Margin:                     &types.Rat{Rat: big.NewRat(1, 5)},
				PoolKeyHash:                []byte("pool-a"),
				RewardAccount:              []byte("reward-a"),
				BlocksProduced:             &blocks,
				TotalBlocksInEpoch:         &totalBlocks,
				Epoch:                      5,
				Pledge:                     10,
				DelegatedStake:             100,
				OwnerStake:                 20,
				Cost:                       3,
				DelegatorCount:             2,
				RewardAccountCredentialTag: 1,
				CapturedSlot:               50,
				BoundarySlot:               49,
			},
		},
		nil,
	))
	require.NoError(t, store.SaveRewardStakeInputs(
		[]*models.RewardStakeInput{
			{
				PoolKeyHash:   []byte("pool-a"),
				StakingKey:    []byte("stake-a"),
				Epoch:         5,
				CredentialTag: 1,
				Stake:         75,
				Owner:         true,
				Registered:    true,
				CapturedSlot:  50,
				BoundarySlot:  49,
			},
			{
				PoolKeyHash:  []byte("pool-old"),
				StakingKey:   []byte("stake-old"),
				Epoch:        1,
				Stake:        1,
				CapturedSlot: 10,
				BoundarySlot: 9,
			},
		},
		nil,
	))
	require.NoError(t, store.SaveRewardPoolOutputs(
		[]*models.RewardPoolOutput{
			{
				ApparentPerformance: &types.Rat{Rat: big.NewRat(3, 4)},
				PoolKeyHash:         []byte("pool-a"),
				Epoch:               5,
				OptimalReward:       100,
				TotalReward:         90,
				LeaderReward:        10,
				LeaderRewardDeficit: 7,
				MemberRewardTotal:   80,
				OwnerStake:          20,
				Undistributed:       5,
				Unspendable:         1,
				CapturedSlot:        50,
				BoundarySlot:        49,
			},
		},
		nil,
	))
	require.NoError(t, store.SaveRewardAccountOutputs(
		[]*models.RewardAccountOutput{
			{
				StakingKey:    []byte("stake-a"),
				PoolKeyHash:   []byte("pool-a"),
				RewardType:    "member",
				Epoch:         5,
				CredentialTag: 1,
				Amount:        80,
				Spendable:     true,
				CapturedSlot:  50,
				BoundarySlot:  49,
			},
			{
				StakingKey:   []byte("stake-old"),
				PoolKeyHash:  []byte("pool-old"),
				RewardType:   "member",
				Epoch:        1,
				Amount:       1,
				CapturedSlot: 10,
				BoundarySlot: 9,
			},
		},
		nil,
	))

	gotAuthoritativeClaim := authoritativeClaim
	gotAuthoritativeGuard := authoritativeGuard
	gotFallbackClaim := fallbackClaim
	gotGuardCreated := guardCreated
	gotProvisionalGuard := provisionalGuard
	gotPots, err := store.GetRewardAdaPots(5, nil)
	require.NoError(t, err)
	gotFallback, err := store.GetRewardSnapshot(6, "mark", nil)
	require.NoError(t, err)
	gotGuardRemoved, err := store.GetRewardSnapshot(7, "mark", nil)
	require.NoError(t, err)
	gotPoolInputs, err := store.GetRewardPoolInputs(5, nil)
	require.NoError(t, err)
	gotStakeInputs, err := store.GetRewardStakeInputs(5, nil)
	require.NoError(t, err)
	gotPoolOutputs, err := store.GetRewardPoolOutputs(5, nil)
	require.NoError(t, err)
	gotAccountOutputs, err := store.GetRewardAccountOutputs(5, nil)
	require.NoError(t, err)

	require.NoError(t, store.DeleteRewardStateBeforeEpoch(
		context.Background(), 5, nil,
	))
	_, err = store.GetRewardStakeInputs(1, nil)
	require.NoError(t, err)
	_, err = store.GetRewardStakeInputs(5, nil)
	require.NoError(t, err)
	require.NoError(t, store.DeleteRewardStateAfterSlot(49, nil))
	gotRolledBackPoolOutputs, err := store.GetRewardPoolOutputs(5, nil)
	require.NoError(t, err)
	gotRolledBackAccountOutput, err := store.GetRewardAccountOutputs(5, nil)
	require.NoError(t, err)
	require.NotNil(t, gotPots)
	require.Equal(t, uint64(10), uint64(gotPots.Treasury))
	require.Equal(t, uint64(20), uint64(gotPots.Reserves))
	require.Equal(t, uint64(30), uint64(gotPots.Fees))
	require.Equal(t, uint64(40), uint64(gotPots.Rewards))
	require.False(t, gotAuthoritativeClaim)
	require.True(t, gotFallbackClaim)
	require.NotNil(t, gotFallback)
	require.Equal(t, uint64(250), uint64(gotFallback.TotalActiveStake))
	require.Equal(t, uint64(60), gotFallback.BoundarySlot)
	require.True(t, gotGuardCreated)
	require.Nil(t, gotGuardRemoved)
	require.True(t, gotProvisionalGuard)
	require.False(t, gotAuthoritativeGuard)
	require.Len(t, gotPoolInputs, 1)
	require.Equal(t, []byte("pool-a"), gotPoolInputs[0].PoolKeyHash)
	require.Equal(t, types.Uint64(100), gotPoolInputs[0].DelegatedStake)
	require.Len(t, gotStakeInputs, 1)
	require.Equal(t, []byte("stake-a"), gotStakeInputs[0].StakingKey)
	require.Equal(t, types.Uint64(75), gotStakeInputs[0].Stake)
	require.Len(t, gotPoolOutputs, 1)
	require.Equal(t, types.Uint64(90), gotPoolOutputs[0].TotalReward)
	require.Equal(t, types.Uint64(10), gotPoolOutputs[0].LeaderReward)
	require.Equal(t, types.Uint64(7), gotPoolOutputs[0].LeaderRewardDeficit)
	require.Len(t, gotAccountOutputs, 1)
	require.Equal(t, uint64(80), uint64(gotAccountOutputs[0].Amount))
	require.Empty(t, gotRolledBackPoolOutputs)
	require.Empty(t, gotRolledBackAccountOutput)
}

func newSharedSQLStore(
	t *testing.T,
) (*sqlstore.Store, *sql.DB) {
	t.Helper()
	store, writeDB, _, err := openSQLStore(
		Config{},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store, writeDB
}

// smallLimitDialect shrinks the bound-parameter budget so a handful of rows
// spans several chunks.
type smallLimitDialect struct {
	sqlstore.Dialect
	limit int
}

func (d smallLimitDialect) ParameterLimit() int { return d.limit }

func newSharedSQLStoreWithParameterLimit(
	t *testing.T,
	limit int,
) (*sqlstore.Store, *sql.DB) {
	t.Helper()
	store, writeDB, _, err := openSQLStoreWithDialect(
		Config{},
		metadata.ProviderDependencies{},
		smallLimitDialect{Dialect: sqlstore.SQLiteDialect(), limit: limit},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store, writeDB
}

func TestSQLiteVacuumMaintenanceIsOptInAndConfigurable(t *testing.T) {
	t.Parallel()
	db, err := sqlstore.OpenDB(
		"sqlite",
		fmt.Sprintf(
			"file:sqlite_vacuum_config_%d?mode=memory&cache=shared",
			sharedMemoryDBSequence.Add(1),
		),
		"sqlite",
		false,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	maintenance, interval, err := sqliteVacuum(db, 0)
	require.NoError(t, err)
	require.Nil(t, maintenance)
	require.Zero(t, interval)

	maintenance, interval, err = sqliteVacuum(db, 30)
	require.NoError(t, err)
	require.NotNil(t, maintenance)
	require.Equal(t, 30*time.Second, interval)
	require.NoError(t, maintenance(context.Background()))
}

func TestOpenSQLStoreRejectsVacuumIntervalOverflow(t *testing.T) {
	t.Parallel()
	_, _, _, err := openSQLStore(
		Config{VacuumIntervalSeconds: maxVacuumIntervalSeconds + 1},
		metadata.ProviderDependencies{},
	)
	require.ErrorContains(t, err, "vacuumIntervalSeconds exceeds maximum")
}

// diskSizeUntilComplete calls store.DiskSize until one call completes or
// deadline passes, returning the last result. Safe to call off the test
// goroutine: it never touches *testing.T.
//
// DiskSize bounds its own PRAGMA reads at sqliteDiskSizeQueryTimeout and
// reports a deadline rather than stalling a Prometheus scrape behind a slow
// connection. That budget is a production guarantee, not a test assumption,
// and it can legitimately expire while this package runs its tests in
// parallel under -race, each with its own SQLite database: one such run
// returned "SQLite page count: context deadline exceeded". Retrying keeps
// the assertion that DiskSize works without also asserting that five
// seconds is always enough on a loaded machine; a DiskSize that is wedged
// rather than slow never completes on any attempt.
func diskSizeUntilComplete(
	store *sqlstore.Store,
	deadline time.Time,
) (int64, error) {
	// Backoff, not synchronization: an error DiskSize returns immediately
	// -- a closed or missing database -- would otherwise spin hot until
	// the deadline.
	const retryPause = 50 * time.Millisecond
	for {
		size, err := store.DiskSize()
		if err == nil || !time.Now().Before(deadline) {
			return size, err
		}
		time.Sleep(retryPause)
	}
}

// requireDiskSize is diskSizeUntilComplete on the test goroutine, requiring
// that some call completes and reports a positive size.
func requireDiskSize(t *testing.T, store *sqlstore.Store) int64 {
	t.Helper()
	size, err := diskSizeUntilComplete(
		store,
		time.Now().Add(testutil.AsyncWait),
	)
	require.NoError(t, err, "no DiskSize call completed")
	require.Positive(t, size)
	return size
}

// TestDiskSizeDoesNotBlockOnBusyWriteConnection is the regression test for
// sqliteDiskSize querying through the write pool: writeDB has
// SetMaxOpenConns(1), so an open write transaction holds that pool's only
// connection until it commits or rolls back. DiskSize() (wired to
// dingo_database_sql_disk_bytes, scraped by Prometheus) must not share that
// pool, or a live write transaction stalls every scrape behind it.
// sqliteDiskSize now queries readDB, an independently sized pool, so this
// passes with an open write transaction held for the whole call.
func TestDiskSizeDoesNotBlockOnBusyWriteConnection(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	store, _, _, err := openSQLStore(
		Config{},
		metadata.ProviderDependencies{DataDir: dataDir},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	txn := store.Transaction(context.Background())
	t.Cleanup(func() {
		_ = txn.Rollback()
	})

	done := make(chan struct{})
	var (
		size        int64
		diskSizeErr error
	)
	go func() {
		defer close(done)
		size, diskSizeErr = diskSizeUntilComplete(
			store,
			time.Now().Add(testutil.AsyncWait),
		)
	}()

	// A failure deadline, not a latency budget. The property is that
	// DiskSize does not queue behind the write pool's only connection,
	// and a DiskSize that did queue behind it would not return late --
	// it would not return at all, because nothing rolls the transaction
	// back until cleanup. A generous deadline therefore costs a passing
	// run nothing (RequireReceive returns the instant DiskSize does) and
	// still catches the regression; a 2s one only added a second failure
	// mode, because opening a connection and reading two PRAGMAs takes
	// well over two seconds on a machine running the rest of this package
	// under -race in parallel.
	testutil.RequireReceive(
		t,
		done,
		testutil.AsyncWait,
		"DiskSize blocked behind the write transaction's sole connection",
	)
	require.NoError(
		t, diskSizeErr,
		"no DiskSize call completed while the write transaction held the "+
			"write pool's only connection",
	)
	require.Positive(t, size)
}

// TestDiskSizeDoesNotLeaveReadDBConnectionOpen is the regression test for
// sqliteDiskSize pinning an idle connection open in readDB's shared pool.
// Live symptom this is associated with: perf's containers logged
// checkpointWAL's "a reader is still holding an old snapshot" warning every
// ~2 minutes, for as long as the process ran, after sqliteDiskSize started
// reading through readDB instead of writeDB; vanilla (never moved off
// writeDB) never logged it once. Reproduced directly against a live
// affected container: an external, independently-opened `sqlite3
// metadata.sqlite "PRAGMA wal_checkpoint(TRUNCATE)"` returned the same
// busy=1 result dingo's own checkpointWAL was logging, and moving
// sqliteDiskSize off readDB onto its own dedicated connection made the
// warnings stop and the on-disk -wal file shrink.
//
// This test only proves the narrower, mechanical fact its name says: before
// the fix, one DiskSize() call left a connection sitting in readDB's pool
// that had not been there before and that nothing ever closes on its own
// (neither pool sets SetConnMaxIdleTime or SetConnMaxLifetime); after the
// fix, sqliteDiskSize opens and closes its own dedicated connection per
// call, exactly like checkpointWAL already does, so readDB is never touched
// and never gains one. It does not by itself show that an idle, otherwise
// unused connection blocks wal_checkpoint(TRUNCATE): SQLite documents that
// only an active writer or a reader on an old snapshot does. The production
// mechanism connecting "DiskSize on readDB" to the persistent busy=1 symptom
// is not isolated by this test; the dedicated-connection fix is justified by
// the production before/after observation and by matching checkpointWAL's own
// existing design, not by a claim that mere pool attachment is sufficient to
// block a checkpoint.
func TestDiskSizeDoesNotLeaveReadDBConnectionOpen(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	store, _, readDB, err := openSQLStore(
		Config{},
		metadata.ProviderDependencies{DataDir: dataDir},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	// Force-evict whatever idle connection migrations left behind during
	// Start, so the assertion below reflects DiskSize's own effect rather
	// than incidental startup activity.
	readDB.SetMaxIdleConns(0)
	readDB.SetMaxIdleConns(DefaultMaxConnections)
	require.Zero(
		t,
		readDB.Stats().OpenConnections,
		"test setup: expected a clean readDB baseline before DiskSize",
	)

	// A completed DiskSize() call, mirroring a Prometheus scrape of
	// dingo_database_sql_disk_bytes (see metrics.go). An attempt that hits
	// DiskSize's own query deadline is a scrape too and must leave readDB
	// just as untouched, so retrying until one completes does not weaken
	// the assertion below.
	requireDiskSize(t, store)

	require.Zerof(
		t,
		readDB.Stats().OpenConnections,
		"DiskSize() left %d connection(s) open in readDB's shared pool; "+
			"the pre-fix code path left one behind here indefinitely "+
			"(see this test's doc comment for what that was and was not "+
			"shown to cause)",
		readDB.Stats().OpenConnections,
	)
}

var testSyncedAt = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

const (
	testSubjectNut  = "00000002df633853f6a47465c9496721d2d5b1291b8398016c0e87ae6e7574636f696e"
	testSubjectDjed = "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61446a65644d6963726f555344"
)

func TestSharedSQLStoreTokenRegistryUpsertReplacesProperties(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	ctx := t.Context()

	_, err := store.UpsertTokenRegistryEntries(
		ctx,
		[]models.TokenRegistryEntry{{
			Subject:  testSubjectNut,
			Name:     "old name",
			Ticker:   "OLD",
			Decimals: new(2),
		}},
		testSyncedAt,
		nil,
	)
	require.NoError(t, err)

	// A later sync is authoritative: a property dropped upstream must be
	// cleared here too, otherwise the node keeps serving a ticker or a
	// decimals value the registry has since removed.
	_, err = store.UpsertTokenRegistryEntries(
		ctx,
		[]models.TokenRegistryEntry{{
			Subject: testSubjectNut,
			Name:    "new name",
		}},
		testSyncedAt.Add(time.Hour),
		nil,
	)
	require.NoError(t, err)

	entry, err := store.GetTokenRegistryEntry(testSubjectNut, nil)
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, "new name", entry.Name)
	require.Empty(t, entry.Ticker)
	require.Nil(t, entry.Decimals)
}

func TestSharedSQLStoreTokenRegistryLookupNormalizesSubject(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	ctx := t.Context()

	_, err := store.UpsertTokenRegistryEntries(
		ctx,
		[]models.TokenRegistryEntry{{
			Subject: testSubjectNut,
			Name:    "nutcoin",
		}},
		testSyncedAt,
		nil,
	)
	require.NoError(t, err)

	entry, err := store.GetTokenRegistryEntry(
		strings.ToUpper(testSubjectNut),
		nil,
	)

	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, "nutcoin", entry.Name)
}

// TestSharedSQLStoreTokenRegistryPrune covers the reconciliation half of a
// snapshot: subjects the upstream registry has dropped must stop being served,
// which an upsert-only sync cannot achieve on its own.
func TestSharedSQLStoreTokenRegistryPrune(t *testing.T) {
	t.Parallel()
	var store *sqlstore.Store
	sqlStore, _ := newSharedSQLStore(t)
	store = sqlStore
	ctx := t.Context()

	firstSync := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	_, err := store.UpsertTokenRegistryEntries(
		ctx,
		[]models.TokenRegistryEntry{
			{Subject: testSubjectNut, Name: "nutcoin"},
			{Subject: testSubjectDjed, Name: "Djed USD"},
		},
		firstSync,
		nil,
	)
	require.NoError(t, err)

	// A later snapshot carries only one of the two subjects.
	secondSync := firstSync.Add(time.Hour)
	_, err = store.UpsertTokenRegistryEntries(
		ctx,
		[]models.TokenRegistryEntry{
			{Subject: testSubjectNut, Name: "nutcoin"},
		},
		secondSync,
		nil,
	)
	require.NoError(t, err)

	pruned, err := store.PruneTokenRegistryEntriesBefore(ctx, secondSync, nil)

	require.NoError(t, err)
	require.Equal(t, 1, pruned)
	survivor, err := store.GetTokenRegistryEntry(testSubjectNut, nil)
	require.NoError(t, err)
	require.NotNil(t, survivor)
	dropped, err := store.GetTokenRegistryEntry(testSubjectDjed, nil)
	require.NoError(t, err)
	require.Nil(t, dropped, "a subject absent from the snapshot must be gone")
}

func TestSharedSQLStoreTokenRegistryPruneKeepsCurrentSnapshot(t *testing.T) {
	t.Parallel()
	var store *sqlstore.Store
	sqlStore, _ := newSharedSQLStore(t)
	store = sqlStore
	ctx := t.Context()

	syncedAt := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	_, err := store.UpsertTokenRegistryEntries(
		ctx,
		[]models.TokenRegistryEntry{{Subject: testSubjectNut, Name: "nutcoin"}},
		syncedAt,
		nil,
	)
	require.NoError(t, err)

	// The cutoff is the snapshot's own stamp, so rows written by that
	// snapshot are at the boundary and must survive it.
	pruned, err := store.PruneTokenRegistryEntriesBefore(ctx, syncedAt, nil)

	require.NoError(t, err)
	require.Zero(t, pruned)
	entry, err := store.GetTokenRegistryEntry(testSubjectNut, nil)
	require.NoError(t, err)
	require.NotNil(t, entry)
}

func TestSharedSQLStoreTokenRegistrySnapshotTransaction(t *testing.T) {
	t.Parallel()
	var store *sqlstore.Store
	sqlStore, _ := newSharedSQLStore(t)
	store = sqlStore
	ctx := t.Context()
	firstSync := testSyncedAt
	_, err := store.UpsertTokenRegistryEntries(
		ctx,
		[]models.TokenRegistryEntry{
			{Subject: testSubjectNut, Name: "old nutcoin"},
			{Subject: testSubjectDjed, Name: "old Djed"},
		},
		firstSync,
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, store.SetSyncState("token-registry-test", "old", nil))

	apply := func(txn types.Txn) {
		t.Helper()
		secondSync := firstSync.Add(time.Hour)
		_, applyErr := store.UpsertTokenRegistryEntries(
			ctx,
			[]models.TokenRegistryEntry{{
				Subject: testSubjectNut,
				Name:    "new nutcoin",
			}},
			secondSync,
			txn,
		)
		require.NoError(t, applyErr)
		_, applyErr = store.PruneTokenRegistryEntriesBefore(
			ctx,
			secondSync,
			txn,
		)
		require.NoError(t, applyErr)
		require.NoError(
			t,
			store.SetSyncState("token-registry-test", "new", txn),
		)
	}

	txn := store.Transaction(ctx)
	apply(txn)
	require.NoError(t, txn.Rollback())
	nut, err := store.GetTokenRegistryEntry(testSubjectNut, nil)
	require.NoError(t, err)
	require.Equal(t, "old nutcoin", nut.Name)
	djed, err := store.GetTokenRegistryEntry(testSubjectDjed, nil)
	require.NoError(t, err)
	require.NotNil(t, djed)
	state, err := store.GetSyncState("token-registry-test", nil)
	require.NoError(t, err)
	require.Equal(t, "old", state)

	txn = store.Transaction(ctx)
	apply(txn)
	require.NoError(t, txn.Commit())
	nut, err = store.GetTokenRegistryEntry(testSubjectNut, nil)
	require.NoError(t, err)
	require.Equal(t, "new nutcoin", nut.Name)
	djed, err = store.GetTokenRegistryEntry(testSubjectDjed, nil)
	require.NoError(t, err)
	require.Nil(t, djed)
	state, err = store.GetSyncState("token-registry-test", nil)
	require.NoError(t, err)
	require.Equal(t, "new", state)
}

// transactionWitnessCleanupIndexes names the index that must answer
// TransactionWitnessCleanupSQL for each witness table.
//
// Named here rather than in the store: nothing in the statement mentions an
// index, since which one answers it is the planner's choice, and that choice
// is what these tests check.
var transactionWitnessCleanupIndexes = map[string]string{
	"key_witness":     "idx_key_witness_transaction_id",
	"witness_scripts": "idx_witness_scripts_transaction_id",
	"redeemer":        "idx_redeemer_transaction_id",
	"plutus_data":     "idx_plutus_data_transaction_id",
}

// newAPIModeSQLStore opens an API-mode store on its own data directory. Only
// API mode writes the witness tables at all, so it is the only mode where the
// cleanup deletes run.
func newAPIModeSQLStore(t *testing.T) (*sqlstore.Store, *sql.DB) {
	t.Helper()
	store, writeDB, _, err := openSQLStore(
		Config{},
		metadata.ProviderDependencies{StorageMode: types.StorageModeAPI},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() {
		// Logged rather than asserted: require calls FailNow, which stops
		// the remaining cleanup callbacks and leaks the t.TempDir removal
		// registered before this one.
		if err := store.Close(); err != nil {
			t.Logf("closing store: %v", err)
		}
	})
	return store, writeDB
}

// queryPlan returns the planner's description of stmt, one node per line.
//
// The statement carries a bound parameter, so a value is supplied even though
// EXPLAIN QUERY PLAN never runs the DELETE it describes.
func queryPlan(t *testing.T, db *sql.DB, stmt string, args ...any) string {
	t.Helper()
	rows, err := db.Query("EXPLAIN QUERY PLAN "+stmt, args...)
	require.NoError(t, err)
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
		plan.WriteString(detail)
		plan.WriteString("\n")
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, plan.String(), "the planner must describe %q", stmt)
	return plan.String()
}

// seedWitnessRows adds one transaction, and one row in each witness table, for
// every slot in [from, to).
//
// Written as set-based SQL rather than through SetTransaction so the planner
// tests can prepare representative witness tables without spending the whole
// test budget on inserts. The columns and foreign keys are the ones the store
// writes; slot doubles as the seed ordinal so each call can extend the prior
// range.
func seedWitnessRows(t *testing.T, db *sql.DB, from, to int) {
	t.Helper()
	_, err := db.Exec(`
WITH RECURSIVE seq(n) AS (
    SELECT ?
    UNION ALL
    SELECT n + 1 FROM seq WHERE n + 1 < ?
)
INSERT INTO "transaction" (
    hash, block_hash, slot, type, fee, collateral_fee, ttl, block_index, valid
)
SELECT CAST(n AS BLOB), CAST(n AS BLOB), n, 0, '0', '0', '0', 0, TRUE
FROM seq`, from, to)
	require.NoError(t, err)
	for _, statement := range []string{
		`INSERT INTO key_witness (vkey, signature, transaction_id, type)
SELECT hash, hash, id, 0 FROM "transaction" WHERE slot >= ? AND slot < ?`,
		`INSERT INTO witness_scripts (script_hash, transaction_id, type)
SELECT hash, id, 0 FROM "transaction" WHERE slot >= ? AND slot < ?`,
		`INSERT INTO redeemer (
    data, transaction_id, ex_units_memory, ex_units_cpu, "index", tag
)
SELECT hash, id, 0, 0, 0, 0 FROM "transaction" WHERE slot >= ? AND slot < ?`,
		`INSERT INTO plutus_data (data, transaction_id)
SELECT hash, id FROM "transaction" WHERE slot >= ? AND slot < ?`,
	} {
		_, err := db.Exec(statement, from, to)
		require.NoError(t, err)
	}
	// Mirror node.RunPlannerStats, which Mithril runs immediately before
	// backfill: the plan asserted below has to be the plan the planner picks
	// with statistics present, not the one it picks in their absence.
	_, err = db.Exec("ANALYZE")
	require.NoError(t, err)
}

// requireWitnessCleanupIndexed asserts that every witness-table idempotency
// delete resolves transaction_id through its index, quoting the plan and the
// caller's stage description when it does not.
func requireWitnessCleanupIndexed(t *testing.T, db *sql.DB, when string) {
	t.Helper()
	for _, table := range sqlstore.TransactionWitnessTables() {
		index, ok := transactionWitnessCleanupIndexes[table]
		require.True(
			t,
			ok,
			"table %q has no expected cleanup index; a new witness "+
				"table needs its transaction_id index classified for "+
				"bulk load",
			table,
		)
		plan := queryPlan(
			t,
			db,
			sqlstore.TransactionWitnessCleanupSQL(table),
			1,
		)
		// SQLite reports a covering index as "USING COVERING INDEX",
		// so the index name and the equality it resolves are matched
		// rather than one literal spelling of the whole node.
		require.Contains(
			t,
			plan,
			"SEARCH "+table+" USING",
			"%s: the %s idempotency delete must be an indexed search:\n%s",
			when, table, plan,
		)
		require.Contains(
			t,
			plan,
			"INDEX "+index+" (transaction_id=?)",
			"%s: the %s idempotency delete must resolve transaction_id "+
				"through %s:\n%s",
			when, table, index, plan,
		)
		require.NotContains(
			t,
			plan,
			"SCAN "+table,
			"%s: the %s idempotency delete must not scan the table:\n%s",
			when, table, plan,
		)
	}
}

// preChangeDeferredWitnessIndexes names the witness transaction_id indexes a
// binary shipped before still carried in its deferred-index
// manifest, and therefore dropped at the start of every bulk-load cycle.
var preChangeDeferredWitnessIndexes = []string{
	"idx_key_witness_transaction_id",
	"idx_witness_scripts_transaction_id",
	"idx_redeemer_transaction_id",
}

// seedPreChangeDeferredCycle leaves the store in the state a binary whose
// manifest still deferred these three indexes leaves on disk when its cycle is
// interrupted: the indexes dropped, and the durable recovery marker still set.
//
// Dropping the indexes directly is the whole point. The schema migration that
// created them is already recorded complete, so its
// CREATE INDEX IF NOT EXISTS never runs again, and a manifest that no longer
// names them cannot rebuild them either.
func seedPreChangeDeferredCycle(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, index := range preChangeDeferredWitnessIndexes {
		_, err := db.Exec("DROP INDEX IF EXISTS " + index)
		require.NoError(t, err)
		require.False(
			t,
			sqliteIndexExists(t, db, index),
			"%s must be absent for this to test the upgrade path",
			index,
		)
	}
	_, err := db.Exec(
		`INSERT INTO sync_state (sync_key, value) VALUES (?, ?)
		 ON CONFLICT (sync_key) DO UPDATE SET value = excluded.value`,
		deferred.SyncStateKey,
		deferred.SyncStateValue,
	)
	require.NoError(t, err)
}

// TestRetainedIndexesRepairPreChangeDeferredCycle covers the upgrade path for
// databases created before the witness indexes left the manifest.
//
// Taking the three witness transaction_id indexes out of the manifest fixes
// databases the fixed binary bootstraps itself, but not one already on disk.
// A binary whose manifest still held them dropped them before backfill and
// rebuilds them only in the full rebuild, and own reporter ran that
// backfill for hours across restarts, so an interrupted cycle is the expected
// state rather than a corner case. On such a database the newer manifest can
// no longer name the indexes to rebuild them and migration v1 is recorded
// complete, so without the repair the full scans this fix removes become
// permanent.
//
// Both entry points a restarted node takes are covered: another bulk-load
// cycle, and the pending-marker repair a plain serve runs.
func TestRetainedIndexesRepairPreChangeDeferredCycle(t *testing.T) {
	t.Parallel()
	for name, repair := range map[string]func(*sqlstore.Store) error{
		"next bulk-load cycle": (*sqlstore.Store).DropDeferredIndexes,
		"pending-marker repair": (*sqlstore.Store).
			BuildDeferredIndexes,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store, db := newAPIModeSQLStore(t)
			seedWitnessRows(t, db, 0, 5)
			seedPreChangeDeferredCycle(t, db)

			require.NoError(t, repair(store))

			for _, index := range preChangeDeferredWitnessIndexes {
				require.True(
					t,
					sqliteIndexExists(t, db, index),
					"%s must be restored: nothing else recreates an index "+
						"the manifest no longer names",
					index,
				)
			}
			requireWitnessCleanupIndexed(t, db, "after "+name)
		})
	}
}

// TestBuildDeferredIndexesKeepsMarkerUntilRetainedIndexesExist pins the
// ordering the repair depends on: the durable marker asserts that every index
// an older manifest may have dropped is back, so it may not be cleared while
// one of them is still missing.
func TestBuildDeferredIndexesKeepsMarkerUntilRetainedIndexesExist(
	t *testing.T,
) {
	t.Parallel()
	store, db := newAPIModeSQLStore(t)
	seedPreChangeDeferredCycle(t, db)

	pending, err := store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(t, pending, "the seeded cycle must look interrupted")

	require.NoError(t, store.BuildCriticalDeferredIndexes())
	pending, err = store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(
		t,
		pending,
		"the critical rebuild must leave the marker for the full rebuild",
	)

	require.NoError(t, store.BuildDeferredIndexes())
	for _, index := range preChangeDeferredWitnessIndexes {
		require.True(t, sqliteIndexExists(t, db, index))
	}
	pending, err = store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.False(
		t,
		pending,
		"the marker must clear once the full manifest and the retained "+
			"indexes are present",
	)
}

type mockTransaction struct {
	certificates []lcommon.Certificate
	hash         lcommon.Blake2b256
	isValid      bool
	metadata     lcommon.TransactionMetadatum
	produced     []lcommon.Utxo
	inputs       []lcommon.TransactionInput
	consumed     []lcommon.TransactionInput
	collateral   []lcommon.TransactionInput
	refInputs    []lcommon.TransactionInput
	outputs      []lcommon.TransactionOutput
	collReturn   lcommon.TransactionOutput
	withdrawals  map[*lcommon.Address]*big.Int
	mint         *lcommon.MultiAsset[lcommon.MultiAssetTypeMint]
}

func (m *mockTransaction) Hash() lcommon.Blake2b256 { return m.hash }
func (m *mockTransaction) Id() lcommon.Blake2b256   { return m.hash }
func (m *mockTransaction) Type() int                { return 0 }
func (m *mockTransaction) Fee() *big.Int            { return big.NewInt(1000) }
func (m *mockTransaction) TTL() uint64              { return 1000000 }
func (m *mockTransaction) IsValid() bool            { return m.isValid }
func (m *mockTransaction) Metadata() lcommon.TransactionMetadatum {
	return m.metadata
}
func (m *mockTransaction) AuxiliaryData() lcommon.AuxiliaryData { return nil }
func (m *mockTransaction) RawAuxiliaryData() []byte             { return nil }
func (m *mockTransaction) CollateralReturn() lcommon.TransactionOutput {
	return m.collReturn
}
func (m *mockTransaction) Produced() []lcommon.Utxo { return m.produced }
func (m *mockTransaction) Outputs() []lcommon.TransactionOutput {
	return m.outputs
}

func (m *mockTransaction) Inputs() []lcommon.TransactionInput { return m.inputs }
func (m *mockTransaction) Collateral() []lcommon.TransactionInput {
	return m.collateral
}
func (m *mockTransaction) Certificates() []lcommon.Certificate {
	return m.certificates
}
func (m *mockTransaction) ProtocolParameterUpdates() (
	uint64,
	map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
) {
	return 0, nil
}

func (m *mockTransaction) AssetMint() *lcommon.MultiAsset[lcommon.MultiAssetTypeMint] {
	return m.mint
}
func (m *mockTransaction) AuxDataHash() *lcommon.Blake2b256 { return nil }

func (m *mockTransaction) Cbor() []byte { return []byte("mock_cbor") }
func (m *mockTransaction) Consumed() []lcommon.TransactionInput {
	return m.consumed
}

func (m *mockTransaction) Witnesses() lcommon.TransactionWitnessSet { return nil }
func (m *mockTransaction) ValidityIntervalStart() uint64            { return 0 }
func (m *mockTransaction) ReferenceInputs() []lcommon.TransactionInput {
	return m.refInputs
}
func (m *mockTransaction) TotalCollateral() *big.Int {
	return big.NewInt(0)
}
func (m *mockTransaction) Withdrawals() map[*lcommon.Address]*big.Int {
	return m.withdrawals
}
func (m *mockTransaction) RequiredSigners() []lcommon.Blake2b224 { return nil }
func (m *mockTransaction) ScriptDataHash() *lcommon.Blake2b256   { return nil }
func (m *mockTransaction) VotingProcedures() lcommon.VotingProcedures {
	return lcommon.VotingProcedures{}
}
func (m *mockTransaction) ProposalProcedures() []lcommon.ProposalProcedure {
	return nil
}

func (m *mockTransaction) CurrentTreasuryValue() *big.Int { return big.NewInt(0) }

func (m *mockTransaction) Donation() *big.Int            { return big.NewInt(0) }
func (m *mockTransaction) Utxorpc() (*cardano.Tx, error) { return nil, nil }
func (m *mockTransaction) LeiosHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

type mockTransactionInput struct {
	hash  lcommon.Blake2b256
	index uint32
}

func (m mockTransactionInput) Id() lcommon.Blake2b256 { return m.hash }
func (m mockTransactionInput) Index() uint32          { return m.index }
func (m mockTransactionInput) String() string         { return m.hash.String() }
func (m mockTransactionInput) Utxorpc() (*cardano.TxInput, error) {
	return nil, nil
}
func (m mockTransactionInput) ToPlutusData() data.PlutusData { return nil }

type mockTransactionOutput struct {
	amount *big.Int
	// address defaults to the zero Address, which carries neither a payment
	// nor a staking credential. Set it to exercise the address-derived
	// columns of the produced utxo row -- payment/staking credential,
	// script-locked classification, and the pointer position of a pointer
	// address.
	address lcommon.Address
}

func (m *mockTransactionOutput) Address() lcommon.Address { return m.address }
func (m *mockTransactionOutput) Amount() *big.Int         { return m.amount }

func (m *mockTransactionOutput) Assets() *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput] {
	return nil
}
func (m *mockTransactionOutput) Datum() *lcommon.Datum          { return nil }
func (m *mockTransactionOutput) DatumHash() *lcommon.Blake2b256 { return nil }
func (m *mockTransactionOutput) Cbor() []byte                   { return nil }
func (m *mockTransactionOutput) Utxorpc() (*cardano.TxOutput, error) {
	return nil, nil
}
func (m *mockTransactionOutput) ScriptRef() lcommon.Script     { return nil }
func (m *mockTransactionOutput) ToPlutusData() data.PlutusData { return nil }
func (m *mockTransactionOutput) String() string                { return "" }

func newTestWitnessTransaction(hashSeed string) *ledger.MockTransaction {
	ws := ledger.NewMockTransactionWitnessSet().
		WithVkeyWitnesses(lcommon.VkeyWitness{
			Vkey:      make([]byte, 32),
			Signature: make([]byte, 64),
		})
	tx := ledger.NewTransactionBuilder().WithWitnesses(ws)
	tx.WithId([]byte(hashSeed))
	return tx
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

func requireTransactionWriteAccount(
	t *testing.T,
	store *sqlstore.Store,
	credentialTag uint8,
	stakingKey []byte,
) *models.Account {
	t.Helper()
	account, err := store.GetAccountByCredential(
		credentialTag, stakingKey, true, nil,
	)
	require.NoError(t, err)
	if account == nil {
		t.Fatal("account lookup returned nil without an error")
	}
	return account
}

func TestSharedSQLStoreTransactionWriteParity(t *testing.T) {
	t.Parallel()
	store, raw := newSharedSQLStore(t)
	counts := func() (int, int) {
		var deltas int
		var witnesses int
		require.NoError(t, raw.QueryRow(
			"SELECT (SELECT COUNT(*) FROM reward_withdrawal) + (SELECT COUNT(*) FROM reward_event_credit) + (SELECT COUNT(*) FROM reward_credit)",
		).Scan(&deltas))
		require.NoError(t, raw.QueryRow(
			"SELECT COUNT(*) FROM account_withdrawal_witness",
		).Scan(&witnesses))
		return deltas, witnesses
	}
	state := exerciseTransactionWriteStore(t, store, false, counts)
	require.Equal(t, uint64(10), state.Slot)
	require.Equal(t, uint32(3), state.BlockIndex)
	require.True(t, state.Valid)
	require.Equal(t, uint64(10), state.InputDeletedSlot)
	require.Len(t, state.InputSpentBy, 32)
	require.Equal(t, byte(0xa2), state.InputSpentBy[0])
	require.Equal(t, uint64(600), state.OutputAmount)
	require.Zero(t, state.AccountReward)
	require.Equal(t, 1, state.WithdrawalDeltas)
	require.Equal(t, 1, state.WithdrawalProofs)
	require.Equal(t, 1, state.Inputs)
	require.Equal(t, 1, state.Outputs)
}

func TestSharedSQLStoreTransactionMetadataCollisionIsNullable(t *testing.T) {
	t.Parallel()
	store, raw := func() (*sqlstore.Store, *sql.DB) {
		store, writeDB, _, err := openSQLStore(
			Config{},
			metadata.ProviderDependencies{StorageMode: types.StorageModeAPI},
		)
		require.NoError(t, err)
		require.NoError(t, store.Start(t.Context()))
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		return store, writeDB
	}()
	txHash := lcommon.Blake2b256{0xe7}
	metadataValue := lcommon.MetaMap{Pairs: []lcommon.MetaPair{{
		Key: lcommon.MetaInt{Value: big.NewInt(721)},
		Value: lcommon.MetaMap{Pairs: []lcommon.MetaPair{
			{Key: lcommon.MetaInt{Value: big.NewInt(1)}, Value: lcommon.MetaText{Value: "integer"}},
			{Key: lcommon.MetaText{Value: "1"}, Value: lcommon.MetaText{Value: "text"}},
		}},
	}}}
	require.NoError(t, store.SetTransaction(
		&mockTransaction{hash: txHash, metadata: metadataValue},
		ocommon.Point{Slot: 7, Hash: bytes.Repeat([]byte{0xe8}, 32)}, 0, nil, false, nil, 0,
	))
	var jsonValue sql.NullString
	var cborValue []byte
	require.NoError(t, raw.QueryRow(`
SELECT l.json_value, l.cbor_value
FROM transaction_metadata_label AS l
JOIN "transaction" AS tx ON tx.id = l.transaction_id
WHERE tx.hash = ? AND l.label = ?`, txHash.Bytes(), "721").Scan(&jsonValue, &cborValue))
	require.False(t, jsonValue.Valid)
	require.Equal(t, "a20167696e746567657261316474657874", hex.EncodeToString(cborValue))
}

// TestSharedSQLStoreWithdrawalWitnessGate covers: the
// account_withdrawal_witness insert must be elided when the caller reports
// the delegator-inactivity gate off (skipWithdrawalWitness=true), and written
// when the gate is on -- in both cases the unrelated reward-delta bookkeeping
// (the reward journal) must be unaffected.
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
					"SELECT (SELECT COUNT(*) FROM reward_withdrawal) + (SELECT COUNT(*) FROM reward_event_credit) + (SELECT COUNT(*) FROM reward_credit)",
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
		nil, 0,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "reward withdrawal amount 1235 exceeds")

	account := requireTransactionWriteAccount(t, store, 0, stakeKey)
	require.Equal(t, uint64(1234), uint64(account.Reward))
	var deltas int
	require.NoError(t, raw.QueryRow(
		"SELECT (SELECT COUNT(*) FROM reward_withdrawal) + (SELECT COUNT(*) FROM reward_event_credit) + (SELECT COUNT(*) FROM reward_credit)",
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
		nil, 0,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "reward withdrawal amount 1236 exceeds")
	account = requireTransactionWriteAccount(t, store, 0, stakeKey)
	require.Equal(t, uint64(1234), uint64(account.Reward))

	// Historical API backfill must retain the live snapshot balance while
	// recording a withdrawal whose amount reflects an earlier chain state.
	backfillHash := lcommon.Blake2b256{0xd5}
	backfillTx := &mockTransaction{
		hash:        backfillHash,
		isValid:     true,
		withdrawals: map[*lcommon.Address]*big.Int{&address: big.NewInt(1235)},
	}
	require.NoError(t, store.SetTransactionBatchedHistorical(
		backfillTx,
		ocommon.Point{Slot: 12, Hash: bytes.Repeat([]byte{0xd6}, 32)},
		0,
		nil,
		true,
		true,
		store.NewBatchAccumulator(),
		nil, 0,
	))
	account = requireTransactionWriteAccount(t, store, 0, stakeKey)
	require.Equal(t, uint64(1234), uint64(account.Reward))
	require.NoError(t, raw.QueryRow(
		"SELECT COUNT(*) FROM reward_withdrawal w JOIN \"transaction\" t ON t.id = w.tx_id WHERE t.hash = ?",
		backfillHash.Bytes(),
	).Scan(&deltas))
	require.Equal(t, 1, deltas)
}

// TestSharedSQLStoreHistoricalBackfillWithdrawalMissingAccount covers a
// replay case: a canonical withdrawal replayed during API-mode Mithril historical
// backfill can find an inactive account after deregistration. That row is
// still the historical account and its reward must be preserved. A credential
// with no account row is an invariant failure and must abort the backfill.
//
// An inactive row can arise from backfill's own certificate replay
// (applyTransactionCertificates runs whether or not historicalBackfill is
// set) between a historical deregistration and a later re-registration for
// the same credential. Deregistration's account upsert never clears
// `reward`, so such a row still holds the credential's real balance: it must
// be journaled rather than discarded as 0, and the row itself neither
// mutated nor reactivated.
//
// Live ingestion must still require an active account (`SetTransaction`,
// historicalBackfill=false) in both cases.
func TestSharedSQLStoreHistoricalBackfillWithdrawalMissingAccount(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		createInactive bool
		inactiveReward uint64
	}{
		{
			name: "no account row at all",
		},
		{
			name:           "account row present but inactive with a real balance",
			createInactive: true,
			inactiveReward: 777,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, raw := newSharedSQLStore(t)
			stakeKey := bytes.Repeat([]byte{0xa1}, lcommon.AddressHashSize)
			if tc.createInactive {
				require.NoError(t, store.CreateAccount(nil, &models.Account{
					StakingKey: stakeKey,
					Reward:     types.Uint64(tc.inactiveReward),
					Active:     false,
				}))
			}
			address, err := lcommon.NewAddressFromParts(
				lcommon.AddressTypeNoneKey,
				lcommon.AddressNetworkTestnet,
				nil,
				stakeKey,
			)
			require.NoError(t, err)

			// Live ingestion of the same withdrawal must still fail: an
			// active account is required outside historical backfill.
			liveHash := lcommon.Blake2b256{0xa2}
			liveTx := &mockTransaction{
				hash:        liveHash,
				isValid:     true,
				withdrawals: map[*lcommon.Address]*big.Int{&address: big.NewInt(500)},
			}
			err = store.SetTransaction(
				liveTx,
				ocommon.Point{Slot: 50, Hash: bytes.Repeat([]byte{0xa3}, 32)},
				0,
				nil,
				true,
				nil, 0,
			)
			require.Error(t, err)
			require.ErrorContains(t, err, "account not found")

			// Historical backfill of the same shape must succeed, record the
			// withdrawal, and neither create nor reactivate an account row.
			backfillHash := lcommon.Blake2b256{0xa4}
			backfillTx := &mockTransaction{
				hash:        backfillHash,
				isValid:     true,
				withdrawals: map[*lcommon.Address]*big.Int{&address: big.NewInt(500)},
			}
			backfillErr := store.SetTransactionBatchedHistorical(
				backfillTx,
				ocommon.Point{Slot: 51, Hash: bytes.Repeat([]byte{0xa5}, 32)},
				0,
				nil,
				true,
				true,
				store.NewBatchAccumulator(),
				nil, 0,
			)
			if !tc.createInactive {
				// A credential with no account row at all is an invariant
				// failure: the run must abort, and must leave behind
				// neither a journal row nor an invented account.
				require.ErrorContains(t, backfillErr, "account not found")
				var journaled int
				require.NoError(t, raw.QueryRow(
					"SELECT COUNT(*) FROM reward_withdrawal w "+
						"JOIN \"transaction\" t ON t.id = w.tx_id WHERE t.hash = ?",
					backfillHash.Bytes(),
				).Scan(&journaled))
				require.Zero(t, journaled)
				missing, err := store.GetAccountByCredential(
					0, stakeKey, true, nil,
				)
				require.NoError(t, err)
				require.Nil(t, missing)
				return
			}
			require.NoError(t, backfillErr)

			account, err := store.GetAccountByCredential(0, stakeKey, true, nil)
			require.NoError(t, err)
			require.NotNil(t, account)
			require.False(t, account.Active)
			require.Equal(t, tc.inactiveReward, uint64(account.Reward))

			var deltas int
			var previousReward string
			require.NoError(t, raw.QueryRow(
				"SELECT COUNT(*), COALESCE(MAX(w.previous_reward), '') FROM reward_withdrawal w JOIN \"transaction\" t ON t.id = w.tx_id WHERE t.hash = ?",
				backfillHash.Bytes(),
			).Scan(&deltas, &previousReward))
			require.Equal(t, 1, deltas)
			require.Equal(t, "777", previousReward)

			// Replaying the same backfill transaction must not duplicate the
			// journal row.
			require.NoError(t, store.SetTransactionBatchedHistorical(
				backfillTx,
				ocommon.Point{Slot: 51, Hash: bytes.Repeat([]byte{0xa5}, 32)},
				0,
				nil,
				true,
				true,
				store.NewBatchAccumulator(),
				nil, 0,
			))
			require.NoError(t, raw.QueryRow(
				"SELECT COUNT(*) FROM reward_withdrawal w JOIN \"transaction\" t ON t.id = w.tx_id WHERE t.hash = ?",
				backfillHash.Bytes(),
			).Scan(&deltas))
			require.Equal(t, 1, deltas)
		})
	}
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
			hash:    hash,
			isValid: true,
			withdrawals: map[*lcommon.Address]*big.Int{
				&address: big.NewInt(17),
			},
		}
		require.NoError(t, store.SetTransaction(
			transaction,
			ocommon.Point{
				Slot: uint64(20 + i),
				Hash: bytes.Repeat([]byte{byte(0xe4 + i)}, 32),
			},
			0,
			nil,
			true,
			nil, 0,
		))
	}
	for tag := range uint8(2) {
		account := requireTransactionWriteAccount(t, store, tag, stakeKey)
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
		nil, 0,
	))
	account := requireTransactionWriteAccount(t, store, 0, stakeKey)
	require.Equal(t, uint64(1234), uint64(account.Reward))
	transactionHash := lcommon.Blake2b256{0xf2}
	transaction := &mockTransaction{
		hash:        transactionHash,
		isValid:     true,
		withdrawals: map[*lcommon.Address]*big.Int{&address: big.NewInt(234)},
	}
	point := ocommon.Point{Slot: 30, Hash: bytes.Repeat([]byte{0xf3}, 32)}
	require.NoError(t, store.SetTransaction(
		transaction, point, 0, nil, true, nil, 0,
	))
	account = requireTransactionWriteAccount(t, store, 0, stakeKey)
	require.Equal(t, uint64(1000), uint64(account.Reward))

	// Replaying the same transaction must not debit the remaining balance again.
	require.NoError(t, store.SetTransaction(
		transaction, point, 0, nil, true, nil, 0,
	))
	account = requireTransactionWriteAccount(t, store, 0, stakeKey)
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
		nil, 0,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "exceeds account balance 1000")
	account = requireTransactionWriteAccount(t, store, 0, stakeKey)
	require.Equal(t, uint64(1000), uint64(account.Reward))

	// Rollback restores the pre-withdrawal balance from the journal.
	require.NoError(t, store.DeleteAccountRewardsAfterSlot(29, nil))
	account = requireTransactionWriteAccount(t, store, 0, stakeKey)
	require.Equal(t, uint64(1234), uint64(account.Reward))
	var deltas int
	require.NoError(t, raw.QueryRow(
		"SELECT (SELECT COUNT(*) FROM reward_withdrawal) + (SELECT COUNT(*) FROM reward_event_credit) + (SELECT COUNT(*) FROM reward_credit)",
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
			t.Parallel()
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
				hash:    lcommon.Blake2b256{0xf8},
				isValid: true,
				withdrawals: map[*lcommon.Address]*big.Int{
					&address: big.NewInt(0),
				},
			}
			err = store.SetTransaction(
				transaction,
				ocommon.Point{Slot: 40, Hash: bytes.Repeat([]byte{0xf9}, 32)},
				0,
				nil,
				true,
				nil, 0,
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
			account := requireTransactionWriteAccount(t, store, 0, stakeKey)
			require.Equal(t, tc.wantReward, uint64(account.Reward))
		})
	}
}

func TestSharedSQLStoreCertificateWriteParity(t *testing.T) {
	t.Parallel()
	store, raw := newSharedSQLStore(t)

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
		10,
	))
	require.NoError(t, store.SetTransaction(
		transaction,
		point,
		7,
		deposits,
		false,
		nil,
		10,
	))
	var (
		gotAccountActive    bool
		gotAccountAddedSlot uint64
		gotAccountCreated   uint64
		gotAccountDrep      []byte
		gotAccountDrepType  uint64
		gotAccountPool      []byte
		gotCertificateCount int
		gotDrepActive       bool
		gotDrepAddedSlot    uint64

		gotUnlinkedCount int
	)
	gotTableCounts := map[string]int{}
	require.NoError(t, raw.QueryRow(`
SELECT COUNT(*), COALESCE(SUM(certificate_id = 0), 0)
FROM certs`).Scan(
		&gotCertificateCount,
		&gotUnlinkedCount,
	))
	require.NoError(t, raw.QueryRow(`
SELECT active, pool, drep, drep_type, added_slot, created_slot
FROM account WHERE credential_tag = 0 AND staking_key = ?`,
		stakeKey[:],
	).Scan(
		&gotAccountActive,
		&gotAccountPool,
		&gotAccountDrep,
		&gotAccountDrepType,
		&gotAccountAddedSlot,
		&gotAccountCreated,
	))
	require.NoError(t, raw.QueryRow(`
SELECT active, added_slot FROM drep
WHERE credential_tag = 0 AND credential = ?`,
		drepKey[:],
	).Scan(&gotDrepActive, &gotDrepAddedSlot))
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
			raw.QueryRow("SELECT COUNT(*) FROM "+table).Scan(&count),
		)
		gotTableCounts[table] = count
	}
	require.Equal(t, 9, gotCertificateCount)
	require.True(t, gotAccountActive)
	require.Equal(t, uint64(81), gotAccountAddedSlot)
	require.Equal(t, uint64(81), gotAccountCreated)
	require.True(t, gotDrepActive)
	require.Equal(t, uint64(81), gotDrepAddedSlot)
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
		require.Equal(t, 1, gotTableCounts[table], table)
	}
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
				Config{},
				metadata.ProviderDependencies{StorageMode: mode},
			)
			require.NoError(t, err)
			require.NoError(t, store.Start(t.Context()))
			t.Cleanup(func() {
				require.NoError(t, store.Close())
			})
			exercise := func(
				store *sqlstore.Store,
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
					nil, 0,
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
			counts := exercise(store, raw)
			require.Equal(t, 1, counts["transaction"])
			wantWitnesses := 0
			if mode == types.StorageModeAPI {
				wantWitnesses = 1
			}
			require.Equal(t, wantWitnesses, counts["key_witness"])
		})
	}
}

func exerciseTransactionWriteStore(
	t *testing.T,
	store *sqlstore.Store,
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
		transaction, point, 3, nil, skipWithdrawalWitness, nil, 0,
	))
	require.NoError(t, store.SetTransaction(
		transaction, point, 3, nil, skipWithdrawalWitness, nil, 0,
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
	account := requireTransactionWriteAccount(t, store, 0, stakeKey)
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

func TestSharedSQLStoreUtxoMutationParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	stakeKey := bytes.Repeat([]byte{0x71}, 28)
	pool := bytes.Repeat([]byte{0x72}, 28)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeKey,
		Pool:       pool, Reward: 5, Active: true, AddedSlot: 1,
	}))
	hashes := [][]byte{
		bytes.Repeat([]byte{0x81}, 32),
		bytes.Repeat([]byte{0x82}, 32),
		bytes.Repeat([]byte{0x83}, 32),
		bytes.Repeat([]byte{0x84}, 32),
		bytes.Repeat([]byte{0x85}, 32),
		bytes.Repeat([]byte{0x86}, 32),
	}
	for i := range hashes {
		if i == len(hashes)-1 {
			break
		}
		require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
			TxId: hashes[i], OutputIdx: uint32(i),
			StakingKey: stakeKey, Amount: types.Uint64(10 + i),
			AddedSlot: uint64(10 + i),
		}))
	}
	imported := models.Utxo{
		TxId: hashes[5], OutputIdx: 5, StakingKey: stakeKey,
		Address: []byte("imported-address"), Amount: 25, AddedSlot: 15,
		Assets: []models.Asset{{
			Name: []byte("asset"), PolicyId: []byte("policy"), Amount: 2,
		}},
	}
	require.NoError(t, store.ImportUtxos([]models.Utxo{imported}, nil))
	require.NoError(t, store.ImportUtxos([]models.Utxo{imported}, nil))
	require.NoError(t, store.MarkUtxosDeletedAtSlot(
		nil,
		[]types.UtxoKey{{TxId: hashes[0], OutputIdx: 0}},
		20,
	))

	var err error
	gotMarked, err := store.GetUtxoIncludingSpent(hashes[0], 0, nil)
	require.NoError(t, err)
	require.NoError(t, store.SetUtxosNotDeletedAfterSlot(19, nil))
	gotRestored, err := store.GetUtxoIncludingSpent(hashes[0], 0, nil)
	require.NoError(t, err)
	require.NoError(t, store.DeleteUtxo(
		models.UtxoId{Hash: hashes[1], Idx: 1},
		nil,
	))
	gotDeletedOne, err := store.GetUtxoIncludingSpent(hashes[1], 1, nil)
	require.NoError(t, err)
	require.NoError(t, store.DeleteUtxos(
		[]models.UtxoId{
			{Hash: hashes[2], Idx: 2},
			{Hash: []byte("missing"), Idx: 9},
		},
		nil,
	))
	gotDeletedBatch, err := store.GetUtxoIncludingSpent(hashes[2], 2, nil)
	require.NoError(t, err)
	require.NoError(t, store.DeleteUtxosAfterSlot(13, nil))
	gotDeletedAfterSlot, err := store.GetUtxoIncludingSpent(
		hashes[4],
		4,
		nil,
	)
	require.NoError(t, err)
	gotLiveStake, err := store.GetLiveStakeInputsForPools(
		[][]byte{pool},
		0,
		nil,
	)
	require.NoError(t, err)
	gotImported, err := store.GetUtxoIncludingSpent(hashes[5], 5, nil)
	require.NoError(t, err)
	require.NotNil(t, gotMarked)
	require.Equal(t, uint64(20), uint64(gotMarked.DeletedSlot))
	require.NotNil(t, gotRestored)
	require.Zero(t, gotRestored.DeletedSlot)
	require.Nil(t, gotDeletedOne)
	require.Nil(t, gotDeletedBatch)
	require.Nil(t, gotDeletedAfterSlot)
	require.Len(t, gotLiveStake, 1)
	require.Equal(t, bytes.Repeat([]byte{0x71}, 28), gotLiveStake[0].StakingKey)
	require.Equal(
		t,
		bytes.Repeat([]byte{0x72}, 28),
		gotLiveStake[0].PoolKeyHash,
	)
	require.Equal(t, types.Uint64(28), gotLiveStake[0].Stake)
	require.Nil(t, gotImported)
}

// TestGetUtxosByAddressEmptyPatterns proves an empty patterns slice returns
// (nil, nil) rather than models.ErrEmptyUtxoAddressPattern, matching the
// coordinated Database.UtxosByAddress's empty-input handling for the same
// slice-based API.
func TestGetUtxosByAddressEmptyPatterns(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	got, err := store.GetUtxosByAddress(nil, 100, nil)
	require.NoError(t, err)
	require.Nil(t, got)
}

// TestSharedSQLStorePoolRetirementAndDepositAtSlot reads a pool's pending
// retirement and held deposit as of a slot: a retirement announced later is
// not reported, and a later registration neither cancels an earlier
// retirement nor replaces the deposit held until it is added.
func TestSharedSQLStorePoolRetirementAndDepositAtSlot(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	poolBytes := bytes.Repeat([]byte{0xA1}, 28)
	poolKeyHash := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolBytes))
	vrf := bytes.Repeat([]byte{0xA2}, 32)
	register := func(slot, deposit uint64) {
		t.Helper()
		require.NoError(t, store.ImportPool(
			&models.Pool{PoolKeyHash: poolBytes, VrfKeyHash: vrf},
			&models.PoolRegistration{
				PoolKeyHash:   poolBytes,
				VrfKeyHash:    vrf,
				AddedSlot:     slot,
				DepositAmount: types.Uint64(deposit),
			},
			nil,
		))
	}
	register(10, 100)
	require.NoError(t, store.RetirePools(nil, [][]byte{poolBytes}, 5, 30))
	register(40, 200)

	retiring := func(slot uint64) []models.PoolRetiringRow {
		t.Helper()
		rows, err := store.GetRetiringPoolsAtSlot(2, slot, nil)
		require.NoError(t, err)
		return rows
	}
	require.Empty(t, retiring(20), "retirement announced at slot 30")
	require.Equal(
		t,
		[]models.PoolRetiringRow{{PoolKeyHash: poolBytes, Epoch: 5}},
		retiring(35),
	)
	require.Empty(t, retiring(40), "the registration at slot 40 cancels it")
	live, err := store.GetRetiringPools(2, nil)
	require.NoError(t, err)
	require.Empty(t, live)

	deposit := func(slot uint64) map[string]uint64 {
		t.Helper()
		held, err := store.GetPoolDepositsHeldAtSlot(
			[]lcommon.PoolKeyHash{poolKeyHash},
			slot,
			nil,
		)
		require.NoError(t, err)
		return held
	}
	require.Empty(t, deposit(5), "not registered yet")
	require.Equal(t, map[string]uint64{string(poolBytes): 100}, deposit(35))
	require.Equal(t, map[string]uint64{string(poolBytes): 200}, deposit(45))
	latest, err := store.GetPoolDepositsHeld(
		[]lcommon.PoolKeyHash{poolKeyHash},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{string(poolBytes): 200}, latest)
}

func TestSharedSQLStoreMidnightParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	txn := store.Transaction(t.Context())
	for _, row := range []*models.MidnightAssetCreate{
		{
			Address: []byte("address-a"), Quantity: 10,
			TxHash: []byte("create-a"), OutputIndex: 0,
			BlockNumber: 10, BlockHash: []byte("block-10"), TxIndex: 1,
			BlockTimestampMs: 1000,
		},
		{
			Address: []byte("address-b"), Quantity: 20,
			TxHash: []byte("create-b"), OutputIndex: 0,
			BlockNumber: 10, BlockHash: []byte("block-10"), TxIndex: 1,
			BlockTimestampMs: 1000,
		},
		{
			Address: []byte("address-c"), Quantity: 30,
			TxHash: []byte("create-c"), OutputIndex: 0,
			BlockNumber: 11, BlockHash: []byte("block-11"), TxIndex: 0,
			BlockTimestampMs: 1100,
		},
	} {
		require.NoError(t, store.CreateMidnightAssetCreate(txn, row))
	}
	require.NoError(t, store.CreateMidnightAssetSpend(
		txn,
		&models.MidnightAssetSpend{
			Address: []byte("address-a"), Quantity: 10,
			SpendingTxHash: []byte("spend-a"),
			UtxoTxHash:     []byte("create-a"), UtxoIndex: 0,
			BlockNumber: 12, BlockHash: []byte("block-12"),
			TxIndex: 0, BlockTimestampMs: 1200,
		},
	))
	for _, row := range []*models.MidnightRegistration{
		{
			FullDatum: []byte("registration-a"), TxHash: []byte("reg-a"),
			OutputIndex: 0, BlockNumber: 10, BlockHash: []byte("block-10"),
			TxIndex: 1, BlockTimestampMs: 1000,
		},
		{
			FullDatum: []byte("registration-b"), TxHash: []byte("reg-b"),
			OutputIndex: 0, BlockNumber: 11, BlockHash: []byte("block-11"),
			TxIndex: 0, BlockTimestampMs: 1100,
		},
	} {
		require.NoError(t, store.CreateMidnightRegistration(txn, row))
	}
	require.NoError(t, store.CreateMidnightDeregistration(
		txn,
		&models.MidnightDeregistration{
			FullDatum: []byte("deregistration-a"),
			TxHash:    []byte("dereg-a"), UtxoTxHash: []byte("reg-a"),
			UtxoIndex: 0, BlockNumber: 12, BlockHash: []byte("block-12"),
			TxIndex: 0, BlockTimestampMs: 1200,
		},
	))
	require.NoError(t, store.InsertMidnightGovernanceDatum(
		txn,
		&models.MidnightGovernanceDatum{
			DatumType: models.MidnightGovernanceDatumTypeCouncil,
			TxHash:    []byte("gov-a"), Datum: []byte("datum-a"),
			BlockNumber: 10,
		},
	))
	require.NoError(t, store.UpsertMidnightAriadneParams(
		txn,
		&models.MidnightAriadneParams{Epoch: 1, Datum: []byte("ariadne-a")},
	))
	require.NoError(t, store.UpsertMidnightAriadneParams(
		txn,
		&models.MidnightAriadneParams{Epoch: 2, Datum: []byte("ariadne-b")},
	))
	require.NoError(t, store.CreateMidnightAriadneRollback(
		txn,
		&models.MidnightAriadneRollback{
			BlockNumber: 12, Epoch: 2, PreviousExists: true,
			PreviousDatum: []byte("ariadne-a"),
		},
	))
	require.NoError(t, store.UpsertMidnightEpochCandidates(
		txn,
		&models.MidnightEpochCandidates{
			Epoch: 2, BlockNumber: 12, CandidatesCbor: []byte("candidates"),
		},
	))
	require.NoError(t, store.InsertMidnightCommitteeCandidateRegistration(
		txn,
		&models.MidnightCommitteeCandidateRegistration{
			TxHash: []byte("candidate-a"), BlockNumber: 12, SlotNumber: 120,
			TxIndex: 1, TxInputsCbor: []byte("inputs"),
		},
	))
	require.NoError(t, txn.Commit())

	var err error
	gotUnspentAssets, err := store.FindUnspentMidnightAssetCreates()
	require.NoError(t, err)
	gotUnspentRegistrations, err := store.FindUnspentMidnightRegistrations()
	require.NoError(t, err)
	gotPage, err := store.FindMidnightAssetCreatesFrom(0, 0, 1, nil)
	require.NoError(t, err)
	gotGovernance, err := store.GetLatestMidnightGovernanceDatum(
		models.MidnightGovernanceDatumTypeCouncil,
		11,
		nil,
	)
	require.NoError(t, err)
	gotLatestAriadne, err := store.GetLatestMidnightAriadneParams(nil)
	require.NoError(t, err)
	gotHistoricalAriadne, err :=
		store.GetMidnightAriadneParamsAtOrBeforeEpoch(1, nil)
	require.NoError(t, err)
	gotRollbacks, err := store.FindMidnightAriadneRollbacksByBlock(nil, 12)
	require.NoError(t, err)
	gotCandidates, err := store.GetMidnightEpochCandidatesByEpoch(2, nil)
	require.NoError(t, err)
	gotRegistrations, err :=
		store.GetMidnightCommitteeCandidateRegistrationsByTxHashes(
			[][]byte{[]byte("candidate-a"), []byte("missing")},
			nil,
		)
	require.NoError(t, err)

	rollback := store.Transaction(t.Context())
	gotDeletedCreates, err := store.DeleteMidnightAssetCreatesByBlock(
		rollback,
		11,
	)
	require.NoError(t, err)
	_, err = store.DeleteMidnightAssetSpendsByBlock(
		rollback,
		12,
	)
	require.NoError(t, err)
	gotDeletedRegistrations, err := store.DeleteMidnightRegistrationsByBlock(
		rollback,
		11,
	)
	require.NoError(t, err)
	_, err =
		store.DeleteMidnightDeregistrationsByBlock(rollback, 12)
	require.NoError(t, err)
	require.NoError(t, rollback.Commit())
	assetTxHashes := make([]string, len(gotUnspentAssets))
	for i, asset := range gotUnspentAssets {
		assetTxHashes[i] = string(asset.TxHash)
	}
	require.ElementsMatch(t, []string{"create-b", "create-c"}, assetTxHashes)
	registrationTxHashes := make([]string, len(gotUnspentRegistrations))
	for i, registration := range gotUnspentRegistrations {
		registrationTxHashes[i] = string(registration.TxHash)
	}
	require.ElementsMatch(t, []string{"reg-b"}, registrationTxHashes)
	pageTxHashes := make([]string, len(gotPage))
	for i, asset := range gotPage {
		pageTxHashes[i] = string(asset.TxHash)
	}
	require.Equal(t, []string{"create-a", "create-b"}, pageTxHashes)
	require.NotNil(t, gotGovernance)
	require.Equal(t, []byte("gov-a"), gotGovernance.TxHash)
	require.Equal(t, []byte("datum-a"), gotGovernance.Datum)
	require.Equal(t, uint64(10), gotGovernance.BlockNumber)
	require.NotNil(t, gotLatestAriadne)
	require.Equal(t, uint64(2), gotLatestAriadne.Epoch)
	require.Equal(t, []byte("ariadne-b"), gotLatestAriadne.Datum)
	require.NotNil(t, gotHistoricalAriadne)
	require.Equal(t, uint64(1), gotHistoricalAriadne.Epoch)
	require.Equal(t, []byte("ariadne-a"), gotHistoricalAriadne.Datum)
	require.Len(t, gotRollbacks, 1)
	require.NotNil(t, gotCandidates)
	require.Equal(t, uint64(2), gotCandidates.Epoch)
	require.Equal(t, uint64(12), gotCandidates.BlockNumber)
	require.Equal(t, []byte("candidates"), gotCandidates.CandidatesCbor)
	require.Len(t, gotRegistrations, 1)
	deletedCreateTxHashes := make([]string, len(gotDeletedCreates))
	for i, create := range gotDeletedCreates {
		deletedCreateTxHashes[i] = string(create.TxHash)
	}
	require.ElementsMatch(t, []string{"create-c"}, deletedCreateTxHashes)
	deletedRegistrationTxHashes := make([]string, len(gotDeletedRegistrations))
	for i, registration := range gotDeletedRegistrations {
		deletedRegistrationTxHashes[i] = string(registration.TxHash)
	}
	require.ElementsMatch(t, []string{"reg-b"}, deletedRegistrationTxHashes)
	// The rollback must also remove the rows, not just report them: deleting
	// the spend at block 12 makes create-a unspent again and the create at
	// block 11 is gone, and the same holds for registrations.
	afterAssets, err := store.FindUnspentMidnightAssetCreates()
	require.NoError(t, err)
	afterAssetTxHashes := make([]string, len(afterAssets))
	for i, asset := range afterAssets {
		afterAssetTxHashes[i] = string(asset.TxHash)
	}
	require.ElementsMatch(
		t,
		[]string{"create-a", "create-b"},
		afterAssetTxHashes,
	)
	afterRegistrations, err := store.FindUnspentMidnightRegistrations()
	require.NoError(t, err)
	afterRegistrationTxHashes := make([]string, len(afterRegistrations))
	for i, registration := range afterRegistrations {
		afterRegistrationTxHashes[i] = string(registration.TxHash)
	}
	require.ElementsMatch(t, []string{"reg-a"}, afterRegistrationTxHashes)
}
