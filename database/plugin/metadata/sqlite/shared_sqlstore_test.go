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
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

type accountStore interface {
	CreateAccount(types.Txn, *models.Account) error
	ImportAccount(*models.Account, types.Txn) error
	GetAccountByCredential(
		uint8,
		[]byte,
		bool,
		types.Txn,
	) (*models.Account, error)
	GetAccountsByCredential(
		[]models.StakeCredentialRef,
		bool,
		types.Txn,
	) (map[string]*models.Account, error)
	RenewAccountExpirations(
		[]models.StakeCredentialRef,
		uint64,
		types.Txn,
	) error
	StampAllActiveAccountExpirations(uint64, types.Txn) (int64, error)
	AccountInactivityActivationMembership(
		[]models.StakeCredentialRef,
		types.Txn,
	) (map[string]struct{}, error)
	ResetAccountExpirationActivation(
		types.Txn,
	) ([]models.StakeCredentialRef, error)
	GetActiveAccountCredentials(
		types.Txn,
	) ([]models.StakeCredentialRef, error)
	DeactivateAccounts(types.Txn, []models.StakeCredentialRef, uint64) error
	AddAccountRewardByCredential(
		uint8,
		[]byte,
		uint64,
		uint64,
		[]byte,
		types.Txn,
	) error
	ApplyAccountRewardWithdrawal(
		uint8,
		[]byte,
		uint64,
		uint64,
		[]byte,
		types.Txn,
	) error
	DeleteAccountRewardsAfterSlot(uint64, types.Txn) error
	GetAccountSumsByCredential(
		uint8,
		[]byte,
		types.Txn,
	) (models.AccountSums, error)
}

type accountState struct {
	active                  *models.Account
	inactiveHidden          *models.Account
	inactive                *models.Account
	activeBatch             map[string]*models.Account
	allBatch                map[string]*models.Account
	renewed                 *models.Account
	activeRefs              []models.StakeCredentialRef
	stamped                 int64
	membership              map[string]struct{}
	resetRefs               []models.StakeCredentialRef
	afterReset              *models.Account
	deactivated             *models.Account
	afterCredit             *models.Account
	afterWithdrawal         *models.Account
	afterWithdrawalRollback *models.Account
	afterCreditRollback     *models.Account
	accountSums             models.AccountSums
}

func TestSharedSQLStoreAccountParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	state := exerciseAccountStore(t, store)
	require.NotNil(t, state.active)
	require.Nil(t, state.inactiveHidden)
	require.NotNil(t, state.inactive)
	require.Len(t, state.activeBatch, 1)
	require.Len(t, state.allBatch, 2)
	require.NotNil(t, state.renewed)
	require.Equal(t, uint64(55), state.renewed.ExpirationEpoch)
	require.Equal(t, []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x11}, 28)),
	}, state.activeRefs)
	require.Equal(t, int64(1), state.stamped)
	require.NotNil(t, state.afterCredit)
	require.Equal(t, uint64(60), uint64(state.afterCredit.Reward))
	require.NotNil(t, state.afterWithdrawal)
	require.Zero(t, state.afterWithdrawal.Reward)
	require.NotNil(t, state.afterCreditRollback)
	require.Equal(t, uint64(50), uint64(state.afterCreditRollback.Reward))
	require.NotNil(t, state.deactivated)
	require.False(t, state.deactivated.Active)
}

func exerciseAccountStore(t *testing.T, store accountStore) accountState {
	t.Helper()
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
	var ret accountState
	var err error
	ret.active, err = store.GetAccountByCredential(0, activeKey, false, nil)
	require.NoError(t, err)
	ret.inactiveHidden, err = store.GetAccountByCredential(
		1,
		inactiveKey,
		false,
		nil,
	)
	require.NoError(t, err)
	ret.inactive, err = store.GetAccountByCredential(
		1,
		inactiveKey,
		true,
		nil,
	)
	require.NoError(t, err)
	ret.activeBatch, err = store.GetAccountsByCredential(refs, false, nil)
	require.NoError(t, err)
	ret.allBatch, err = store.GetAccountsByCredential(refs, true, nil)
	require.NoError(t, err)
	require.NoError(t, store.RenewAccountExpirations(refs, 55, nil))
	ret.renewed, err = store.GetAccountByCredential(0, activeKey, true, nil)
	require.NoError(t, err)
	ret.activeRefs, err = store.GetActiveAccountCredentials(nil)
	require.NoError(t, err)
	ret.stamped, err = store.StampAllActiveAccountExpirations(77, nil)
	require.NoError(t, err)
	ret.membership, err = store.AccountInactivityActivationMembership(
		refs,
		nil,
	)
	require.NoError(t, err)
	ret.resetRefs, err = store.ResetAccountExpirationActivation(nil)
	require.NoError(t, err)
	ret.afterReset, err = store.GetAccountByCredential(0, activeKey, true, nil)
	require.NoError(t, err)
	require.NoError(t, store.AddAccountRewardByCredential(
		0, activeKey, 10, 80, []byte("credit"), nil,
	))
	require.NoError(t, store.AddAccountRewardByCredential(
		0, activeKey, 10, 80, []byte("credit"), nil,
	))
	ret.afterCredit, err = store.GetAccountByCredential(0, activeKey, true, nil)
	require.NoError(t, err)
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, activeKey, 60, 90, []byte("withdraw"), nil,
	))
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, activeKey, 60, 90, []byte("withdraw"), nil,
	))
	ret.afterWithdrawal, err = store.GetAccountByCredential(
		0, activeKey, true, nil,
	)
	require.NoError(t, err)
	require.NoError(t, store.DeleteAccountRewardsAfterSlot(85, nil))
	ret.afterWithdrawalRollback, err = store.GetAccountByCredential(
		0, activeKey, true, nil,
	)
	require.NoError(t, err)
	require.NoError(t, store.DeleteAccountRewardsAfterSlot(75, nil))
	ret.afterCreditRollback, err = store.GetAccountByCredential(
		0, activeKey, true, nil,
	)
	require.NoError(t, err)
	ret.accountSums, err = store.GetAccountSumsByCredential(0, activeKey, nil)
	require.NoError(t, err)
	require.NotNil(t, ret.afterCreditRollback)
	require.True(t, ret.afterCreditRollback.Active)
	require.NoError(t, store.DeactivateAccounts(
		nil,
		[]models.StakeCredentialRef{
			models.NewStakeCredentialRef(0, activeKey),
		},
		1_000,
	))
	ret.deactivated, err = store.GetAccountByCredential(
		0,
		activeKey,
		true,
		nil,
	)
	require.NoError(t, err)
	return ret
}

// modernc.org/sqlite always hoists busy_timeout ahead of the rest of the
// _pragma list regardless of DSN order, so this ordering is defensive rather
// than load-bearing today. It still matters as documentation: any pragma
// listed before busy_timeout in the source DSN would, on a driver without
// that hoisting behavior, run with no busy handler installed and fail
// immediately on contention rather than waiting.
//
// Every pragma after it that touches the database file -- cache_size and
// mmap_size both do -- would then be one that gives up instantly instead of
// waiting out a concurrent writer.
//
// Pin the ordering directly. A concurrency test alone would only fail when
// the race is actually lost, which makes an ordering regression look flaky
// instead of broken.
func TestCommonPragmasSetBusyTimeoutFirst(t *testing.T) {
	pragmas := parsePragmas(t, sqliteCommonPragmas)
	require.NotEmpty(t, pragmas, "no pragmas parsed out of the DSN fragment")
	require.NotContains(t, sqliteCommonPragmas, "journal_mode",
		"journal_mode must not be a per-connection pragma; "+
			"ensureWALJournalMode owns the conversion")
	require.Truef(
		t,
		strings.HasPrefix(pragmas[0], "busy_timeout("),
		"busy_timeout must be the first pragma in the DSN so it is in "+
			"effect before any pragma that touches the database file; got %v",
		pragmas,
	)
}

// The backup connection builds its own DSN rather than reusing the shared
// fragment, and it opens against a database a running node already holds
// open -- so it contends by construction, not just at first open.
func TestBackupDSNSetsBusyTimeoutFirst(t *testing.T) {
	dsn := backupSourceDSN("/tmp/example/metadata.sqlite")
	pragmas := parsePragmas(t, dsn)
	require.NotEmpty(t, pragmas, "no pragmas parsed out of the backup DSN")
	require.Truef(
		t,
		strings.HasPrefix(pragmas[0], "busy_timeout("),
		"busy_timeout must be the first pragma in the backup DSN; got %v",
		pragmas,
	)
}

// parsePragmas pulls the _pragma values out of a DSN (or DSN fragment) in the
// order the driver will execute them.
func parsePragmas(t *testing.T, dsn string) []string {
	t.Helper()
	var out []string
	for part := range strings.SplitSeq(dsn, "&") {
		if v, ok := strings.CutPrefix(part, "_pragma="); ok {
			out = append(out, v)
		}
	}
	return out
}

// The ordering test above pins the cause; this pins the symptom it produced.
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

type drepStore interface {
	CreateDrep(types.Txn, *models.Drep) error
	ImportDrep(*models.Drep, *models.RegistrationDrep, types.Txn) error
	GetDrep([]byte, bool, types.Txn) (*models.Drep, error)
	GetDrepByCredential(
		uint8,
		[]byte,
		bool,
		types.Txn,
	) (*models.Drep, error)
	GetActiveDreps(types.Txn) ([]*models.Drep, error)
	SetDrep(uint8, []byte, uint64, string, []byte, bool, types.Txn) error
	InsertDrepIfAbsent(
		uint8,
		[]byte,
		uint64,
		string,
		[]byte,
		bool,
		types.Txn,
	) error
	CreateAccount(types.Txn, *models.Account) error
	CreateUtxo(types.Txn, *models.Utxo) error
	GetDRepDelegators(
		uint8,
		[]byte,
		types.Txn,
	) ([]models.StakeCredentialRef, error)
	UpdateDRepActivity(uint8, []byte, uint64, uint64, types.Txn) error
	GetExpiredDReps(uint64, types.Txn) ([]*models.Drep, error)
	GetDrepLastRegistrationSlot(uint8, []byte, types.Txn) (uint64, error)
	GetDrepLastRegistrationDeposit(uint8, []byte, types.Txn) (*uint64, error)
	GetDrepLastRegistrationDeposits(types.Txn) (map[string]uint64, error)
	GetDRepVotingPower(uint8, []byte, uint64, types.Txn) (uint64, error)
	GetDRepVotingPowerBatch(
		[]models.StakeCredentialRef,
		uint64,
		types.Txn,
	) (map[string]uint64, error)
	GetDRepVotingPowerByType(
		[]uint64,
		uint64,
		types.Txn,
	) (map[uint64]uint64, error)
	GetDreps(types.Txn) ([]models.DrepListRow, error)
	GetPredefinedDrepFirstSeenSlots(types.Txn) (map[uint64]uint64, error)
	DeactivateDreps(types.Txn, []models.StakeCredentialRef) error
	ClearDanglingDRepDelegations(uint64, types.Txn) (int, error)
	GetLiveStakeInputsForPools(
		[][]byte,
		uint64,
		types.Txn,
	) ([]*models.RewardStakeInput, error)
	RewardLiveStakeNeedsBackfill(types.Txn) (bool, error)
	RebuildRewardLiveStake(uint64, types.Txn) error
}

type drepState struct {
	Created                 *models.Drep
	Imported                *models.Drep
	InactiveHidden          *models.Drep
	Inactive                *models.Drep
	Active                  []*models.Drep
	Delegators              []models.StakeCredentialRef
	Expired                 []*models.Drep
	LastRegistrationSlot    uint64
	MissingRegistrationSlot uint64
	CertifiedDeposit        *uint64
	ImportedDeposit         *uint64
	LatestDeposit           *uint64
	InactiveDeposit         *uint64
	MissingDeposit          *uint64
	Deposits                map[string]uint64
	MissingActivityError    string
	VotingPower             uint64
	VotingPowerBatch        map[string]uint64
	VotingPowerByType       map[uint64]uint64
	Dreps                   []models.DrepListRow
	PredefinedFirstSeen     map[uint64]uint64
	DanglingCleared         int
	Deactivated             *models.Drep
	LiveStake               []*models.RewardStakeInput
	LiveStakeNeedsBackfill  bool
	LiveStakeAfterRebuild   []*models.RewardStakeInput
}

func TestSharedSQLStoreDrepParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	state := exerciseDrepStore(t, store)
	require.NotNil(t, state.Created)
	require.Equal(t, uint64(12), state.Created.AddedSlot)
	require.Equal(t, "inactive", state.Created.AnchorURL)
	require.NotNil(t, state.Imported)
	require.Equal(t, uint64(30), state.Imported.LastActivityEpoch)
	require.Equal(t, uint64(35), state.Imported.ExpiryEpoch)
	require.Nil(t, state.InactiveHidden)
	require.NotNil(t, state.Inactive)
	require.False(t, state.Inactive.Active)
	require.Len(t, state.Active, 3)
	activeDrepCredentials := make([]string, len(state.Active))
	for i, drep := range state.Active {
		activeDrepCredentials[i] = models.DrepDepositKey(
			drep.CredentialTag, drep.Credential,
		)
	}
	require.ElementsMatch(t, []string{
		models.DrepDepositKey(1, bytes.Repeat([]byte{0x42}, 28)),
		models.DrepDepositKey(0, bytes.Repeat([]byte{0x44}, 28)),
		models.DrepDepositKey(0, bytes.Repeat([]byte{0x45}, 28)),
	}, activeDrepCredentials)
	require.Len(t, state.Delegators, 2)
	require.ElementsMatch(t, []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x51}, 28)),
		models.NewStakeCredentialRef(1, bytes.Repeat([]byte{0x50}, 28)),
	}, state.Delegators)
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
	gotDrepSlots := make(map[string]uint64, len(state.Dreps))
	gotDrepLastRegistrationSlots := make(map[string]uint64, len(state.Dreps))
	for _, drep := range state.Dreps {
		key := models.DrepDepositKey(drep.CredentialTag, drep.Credential)
		gotDrepSlots[key] = drep.FirstSeenSlot
		gotDrepLastRegistrationSlots[key] = drep.LastRegistrationSlot
	}
	require.Equal(t, firstSeenSlots, gotDrepSlots)
	require.Equal(t, lastRegistrationSlots, gotDrepLastRegistrationSlots)
	require.Len(t, state.Deposits, 3)
	require.Equal(t, map[string]uint64{
		models.DrepDepositKey(1, bytes.Repeat([]byte{0x42}, 28)): 700,
	}, state.VotingPowerBatch)
	wantLiveStake := []*models.RewardStakeInput{{
		PoolKeyHash:   []byte("pool-a"),
		StakingKey:    bytes.Repeat([]byte{0x51}, 28),
		CredentialTag: 0,
		Stake:         types.Uint64(500),
		Registered:    true,
	}}
	require.Equal(t, wantLiveStake, state.LiveStake)
	require.Equal(t, wantLiveStake, state.LiveStakeAfterRebuild)
}

func exerciseDrepStore(t *testing.T, store drepStore) drepState {
	t.Helper()
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

	var ret drepState
	var err error
	ret.Created, err = store.GetDrep(createdCredential, true, nil)
	require.NoError(t, err)
	ret.Imported, err = store.GetDrepByCredential(
		1,
		importedCredential,
		true,
		nil,
	)
	require.NoError(t, err)
	ret.InactiveHidden, err = store.GetDrepByCredential(
		0,
		createdCredential,
		false,
		nil,
	)
	require.NoError(t, err)
	ret.Inactive, err = store.GetDrepByCredential(
		0,
		createdCredential,
		true,
		nil,
	)
	require.NoError(t, err)
	ret.Active, err = store.GetActiveDreps(nil)
	require.NoError(t, err)
	ret.Delegators, err = store.GetDRepDelegators(
		1,
		importedCredential,
		nil,
	)
	require.NoError(t, err)
	ret.Expired, err = store.GetExpiredDReps(35, nil)
	require.NoError(t, err)
	ret.LastRegistrationSlot, err = store.GetDrepLastRegistrationSlot(
		1,
		importedCredential,
		nil,
	)
	require.NoError(t, err)
	ret.MissingRegistrationSlot, err = store.GetDrepLastRegistrationSlot(
		0,
		missingCredential,
		nil,
	)
	require.NoError(t, err)

	// The deposit queries, which deregistration-refund validation reads.
	// The import-shaped row is the load-bearing assertion: it carries no
	// certificate, so a certificate_id filter here would return 0 and a
	// refund would be validated against the wrong amount.
	ret.ImportedDeposit, err = store.GetDrepLastRegistrationDeposit(
		0,
		importOnlyCredential,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, ret.ImportedDeposit)
	require.Equal(t, uint64(500000000), *ret.ImportedDeposit)
	// A row that does carry a certificate must still be found, so the
	// query is not merely inverting the filter.
	ret.CertifiedDeposit, err = store.GetDrepLastRegistrationDeposit(
		1,
		importedCredential,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, ret.CertifiedDeposit)
	require.Equal(t, uint64(500), *ret.CertifiedDeposit)
	// Two registration rows for one credential: the later one wins, so an
	// earlier import placeholder cannot shadow a real re-registration.
	ret.LatestDeposit, err = store.GetDrepLastRegistrationDeposit(
		0,
		rereggedCredential,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, ret.LatestDeposit)
	require.Equal(t, uint64(300000000), *ret.LatestDeposit)
	// A registered but inactive credential is still readable through the
	// singular form, which does not consult drep.active.
	ret.InactiveDeposit, err = store.GetDrepLastRegistrationDeposit(
		0,
		inactiveCredential,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, ret.InactiveDeposit)
	require.Equal(t, uint64(200000000), *ret.InactiveDeposit)
	// No registration history at all reports 0 rather than erroring.
	ret.MissingDeposit, err = store.GetDrepLastRegistrationDeposit(
		0,
		missingCredential,
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, ret.MissingDeposit)

	// The batched form must agree with the singular one on both rows.
	// This is the only thing that executes its derived-table join.
	ret.Deposits, err = store.GetDrepLastRegistrationDeposits(nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(500000000),
		ret.Deposits[models.DrepDepositKey(0, importOnlyCredential)],
	)
	require.Equal(
		t,
		uint64(500),
		ret.Deposits[models.DrepDepositKey(1, importedCredential)],
	)
	require.NotContains(
		t,
		ret.Deposits,
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
		ret.Deposits,
	)
	err = store.UpdateDRepActivity(0, missingCredential, 1, 1, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, models.ErrDrepActivityNotUpdated))
	ret.MissingActivityError = err.Error()
	ret.VotingPower, err = store.GetDRepVotingPower(
		1,
		importedCredential,
		0,
		nil,
	)
	require.NoError(t, err)
	ret.VotingPowerBatch, err = store.GetDRepVotingPowerBatch(
		[]models.StakeCredentialRef{
			models.NewStakeCredentialRef(1, importedCredential),
			models.NewStakeCredentialRef(0, missingCredential),
		},
		0,
		nil,
	)
	require.NoError(t, err)
	ret.VotingPowerByType, err = store.GetDRepVotingPowerByType(
		[]uint64{
			models.DrepTypeAlwaysAbstain,
			models.DrepTypeAlwaysNoConfidence,
		},
		0,
		nil,
	)
	require.NoError(t, err)
	ret.Dreps, err = store.GetDreps(nil)
	require.NoError(t, err)
	ret.PredefinedFirstSeen, err = store.GetPredefinedDrepFirstSeenSlots(nil)
	require.NoError(t, err)
	ret.LiveStake, err = store.GetLiveStakeInputsForPools(
		[][]byte{[]byte("pool-a"), []byte("pool-a")},
		0,
		nil,
	)
	require.NoError(t, err)
	ret.LiveStakeNeedsBackfill, err = store.RewardLiveStakeNeedsBackfill(nil)
	require.NoError(t, err)
	require.NoError(t, store.RebuildRewardLiveStake(88, nil))
	ret.LiveStakeAfterRebuild, err = store.GetLiveStakeInputsForPools(
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
	ret.DanglingCleared, err = store.ClearDanglingDRepDelegations(99, nil)
	require.NoError(t, err)
	ret.Deactivated, err = store.GetDrepByCredential(
		1,
		importedCredential,
		true,
		nil,
	)
	require.NoError(t, err)
	return ret
}

// utxoCascadeDeleteSQL is the statement SQLite runs to enforce
// fk_transaction_outputs, once for every "transaction" row the rollback
// deletes.
//
// It appears nowhere in the store: ON DELETE CASCADE is an action the engine
// takes, not a statement the code issues. It is spelled out here because that
// action is the whole cost of a deep rollback and because it is invisible from
// the parent statement -- EXPLAIN QUERY PLAN of
// DELETE FROM "transaction" WHERE slot > ? reports only the indexed search
// over "transaction" and says nothing about the child table it cascades into.
const utxoCascadeDeleteSQL = `DELETE FROM utxo WHERE transaction_id = ?`

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
		Config{DataDir: t.TempDir()},
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

// seedUtxoRows adds count transactions, each owning two utxo rows, and runs
// ANALYZE so the planner assertions below see the statistics a live database
// has.
//
// Written as set-based SQL rather than through SetTransaction because the
// planner only needs representative table shapes, not faithful transaction
// contents.
func seedUtxoRows(t *testing.T, db *sql.DB, count int) {
	t.Helper()
	_, err := db.Exec(`
WITH RECURSIVE seq(n) AS (
    SELECT 0
    UNION ALL
    SELECT n + 1 FROM seq WHERE n + 1 < ?
)
INSERT INTO "transaction" (
    hash, block_hash, slot, type, fee, collateral_fee, ttl, block_index, valid
)
SELECT CAST(n AS BLOB), CAST(n AS BLOB), n, 0, '0', '0', '0', 0, TRUE
FROM seq`, count)
	require.NoError(t, err)
	for _, outputIdx := range []int{0, 1} {
		_, err := db.Exec(`
INSERT INTO utxo (
    transaction_id, tx_id, output_idx, payment_key, staking_key,
    credential_tag, amount, added_slot, deleted_slot
)
SELECT id, hash, ?, hash, hash, 0, '1000000', slot, 0
FROM "transaction"`, outputIdx)
		require.NoError(t, err)
	}
	_, err = db.Exec("ANALYZE")
	require.NoError(t, err)
}

// TestUtxoCascadeDeleteIndexedAfterCriticalRebuild pins the plan of the
// cascade itself.
//
// The negative case is asserted first: with the manifest dropped, the cascade
// scans utxo. Without it, a rebuild that restored nothing would still satisfy
// the positive assertion if the index had never been missing.
func TestUtxoCascadeDeleteIndexedAfterCriticalRebuild(t *testing.T) {
	t.Parallel()
	store, db := newCoreModeSQLStore(t)
	seedUtxoRows(t, db, 2000)

	require.NoError(t, store.DropDeferredIndexes())
	plan := queryPlan(t, db, utxoCascadeDeleteSQL, 1)
	require.Contains(
		t,
		plan,
		"SCAN utxo",
		"the dropped manifest must leave the cascade unindexed for this "+
			"test to have teeth:\n%s",
		plan,
	)

	require.NoError(t, store.BuildCriticalDeferredIndexes())
	plan = queryPlan(t, db, utxoCascadeDeleteSQL, 1)
	require.Contains(
		t,
		plan,
		"SEARCH utxo USING",
		"the rollback cascade must be an indexed search after the "+
			"critical rebuild:\n%s",
		plan,
	)
	require.Contains(
		t,
		plan,
		"INDEX idx_utxo_transaction_id (transaction_id=?)",
		"the cascade must resolve transaction_id through "+
			"idx_utxo_transaction_id:\n%s",
		plan,
	)
	require.NotContains(
		t,
		plan,
		"SCAN utxo",
		"the cascade must not scan utxo:\n%s",
		plan,
	)
}

// TestRollbackDeleteCascadesIntoUtxo grounds the plan assertion above: the
// cascade is on the rollback's path, so the index the manifest classifies is
// the one the rollback sweep depends on.
//
// Reproduces database/plugin/metadata/sqlstore/transaction_write.go's
// DELETE FROM "transaction" WHERE slot > ?, on a connection carrying the
// foreign_keys(1) pragma the store opens every connection with.
func TestRollbackDeleteCascadesIntoUtxo(t *testing.T) {
	t.Parallel()
	_, db := newCoreModeSQLStore(t)
	seedUtxoRows(t, db, 200)

	var enforced int
	require.NoError(
		t,
		db.QueryRow("PRAGMA foreign_keys").Scan(&enforced),
	)
	require.Equal(
		t,
		1,
		enforced,
		"the store opens every connection with foreign_keys(1); without "+
			"it the cascade this file measures would not run at all",
	)

	var before int
	require.NoError(
		t,
		db.QueryRow("SELECT COUNT(*) FROM utxo WHERE added_slot > 99").
			Scan(&before),
	)
	require.Positive(t, before)

	_, err := db.Exec(`DELETE FROM "transaction" WHERE slot > ?`, 99)
	require.NoError(t, err)

	var after int
	require.NoError(
		t,
		db.QueryRow("SELECT COUNT(*) FROM utxo WHERE added_slot > 99").
			Scan(&after),
	)
	require.Zero(
		t,
		after,
		"deleting the parent transactions must cascade into utxo",
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
		require.NoError(t, store.SetTransaction(txA, pointA, 0, nil, true, nil))
		return store, txA
	}

	// Ranking-block path: the second consume of the already-spent input must
	// fail with ErrUtxoConflict.
	t.Run("ranking block rejects double consume", func(t *testing.T) {
		t.Parallel()
		store, _ := newStoreWithSpentInput(t)
		txB := consumeTx(0xb2, input)
		pointB := ocommon.Point{Slot: 20, Hash: bytes.Repeat([]byte{0xc2}, 32)}
		err := store.SetTransaction(txB, pointB, 0, nil, true, nil)
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
			store.SetTransactionLeiosClosure(txB, pointB, 0, nil, true, nil),
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

type midnightStore interface {
	Transaction(ctx context.Context) types.Txn
	CreateMidnightAssetCreate(types.Txn, *models.MidnightAssetCreate) error
	CreateMidnightAssetSpend(types.Txn, *models.MidnightAssetSpend) error
	CreateMidnightRegistration(types.Txn, *models.MidnightRegistration) error
	CreateMidnightDeregistration(
		types.Txn,
		*models.MidnightDeregistration,
	) error
	FindUnspentMidnightAssetCreates() ([]models.MidnightAssetCreate, error)
	FindUnspentMidnightRegistrations() ([]models.MidnightRegistration, error)
	FindMidnightAssetCreatesFrom(
		uint64,
		uint32,
		int,
		types.Txn,
	) ([]models.MidnightAssetCreate, error)
	DeleteMidnightAssetCreatesByBlock(
		types.Txn,
		uint64,
	) ([]models.MidnightAssetCreate, error)
	DeleteMidnightAssetSpendsByBlock(
		types.Txn,
		uint64,
	) ([]models.MidnightAssetSpend, error)
	DeleteMidnightRegistrationsByBlock(
		types.Txn,
		uint64,
	) ([]models.MidnightRegistration, error)
	DeleteMidnightDeregistrationsByBlock(
		types.Txn,
		uint64,
	) ([]models.MidnightDeregistration, error)
	InsertMidnightGovernanceDatum(
		types.Txn,
		*models.MidnightGovernanceDatum,
	) error
	GetLatestMidnightGovernanceDatum(
		string,
		uint64,
		types.Txn,
	) (*models.MidnightGovernanceDatum, error)
	UpsertMidnightAriadneParams(
		types.Txn,
		*models.MidnightAriadneParams,
	) error
	GetLatestMidnightAriadneParams(
		types.Txn,
	) (*models.MidnightAriadneParams, error)
	GetMidnightAriadneParamsAtOrBeforeEpoch(
		uint64,
		types.Txn,
	) (*models.MidnightAriadneParams, error)
	CreateMidnightAriadneRollback(
		types.Txn,
		*models.MidnightAriadneRollback,
	) error
	FindMidnightAriadneRollbacksByBlock(
		types.Txn,
		uint64,
	) ([]models.MidnightAriadneRollback, error)
	UpsertMidnightEpochCandidates(
		types.Txn,
		*models.MidnightEpochCandidates,
	) error
	GetMidnightEpochCandidatesByEpoch(
		uint64,
		types.Txn,
	) (*models.MidnightEpochCandidates, error)
	InsertMidnightCommitteeCandidateRegistration(
		types.Txn,
		*models.MidnightCommitteeCandidateRegistration,
	) error
	GetMidnightCommitteeCandidateRegistrationsByTxHashes(
		[][]byte,
		types.Txn,
	) ([]models.MidnightCommitteeCandidateRegistration, error)
}

type midnightState struct {
	unspentAssets          []models.MidnightAssetCreate
	unspentRegistrations   []models.MidnightRegistration
	page                   []models.MidnightAssetCreate
	deletedCreates         []models.MidnightAssetCreate
	deletedSpends          []models.MidnightAssetSpend
	deletedRegistrations   []models.MidnightRegistration
	deletedDeregistrations []models.MidnightDeregistration
	governance             *models.MidnightGovernanceDatum
	latestAriadne          *models.MidnightAriadneParams
	historicalAriadne      *models.MidnightAriadneParams
	rollbacks              []models.MidnightAriadneRollback
	candidates             *models.MidnightEpochCandidates
	registrations          []models.MidnightCommitteeCandidateRegistration
}

func TestSharedSQLStoreMidnightParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	state := exerciseMidnightStore(t, store)
	assetTxHashes := make([]string, len(state.unspentAssets))
	for i, asset := range state.unspentAssets {
		assetTxHashes[i] = string(asset.TxHash)
	}
	require.ElementsMatch(t, []string{"create-b", "create-c"}, assetTxHashes)
	registrationTxHashes := make([]string, len(state.unspentRegistrations))
	for i, registration := range state.unspentRegistrations {
		registrationTxHashes[i] = string(registration.TxHash)
	}
	require.ElementsMatch(t, []string{"reg-b"}, registrationTxHashes)
	pageTxHashes := make([]string, len(state.page))
	for i, asset := range state.page {
		pageTxHashes[i] = string(asset.TxHash)
	}
	require.Equal(t, []string{"create-a", "create-b"}, pageTxHashes)
	require.NotNil(t, state.governance)
	require.Equal(t, []byte("gov-a"), state.governance.TxHash)
	require.Equal(t, []byte("datum-a"), state.governance.Datum)
	require.Equal(t, uint64(10), state.governance.BlockNumber)
	require.NotNil(t, state.latestAriadne)
	require.Equal(t, uint64(2), state.latestAriadne.Epoch)
	require.Equal(t, []byte("ariadne-b"), state.latestAriadne.Datum)
	require.NotNil(t, state.historicalAriadne)
	require.Equal(t, uint64(1), state.historicalAriadne.Epoch)
	require.Equal(t, []byte("ariadne-a"), state.historicalAriadne.Datum)
	require.Len(t, state.rollbacks, 1)
	require.NotNil(t, state.candidates)
	require.Equal(t, uint64(2), state.candidates.Epoch)
	require.Equal(t, uint64(12), state.candidates.BlockNumber)
	require.Equal(t, []byte("candidates"), state.candidates.CandidatesCbor)
	require.Len(t, state.registrations, 1)
	deletedCreateTxHashes := make([]string, len(state.deletedCreates))
	for i, create := range state.deletedCreates {
		deletedCreateTxHashes[i] = string(create.TxHash)
	}
	require.ElementsMatch(t, []string{"create-c"}, deletedCreateTxHashes)
	deletedRegistrationTxHashes := make([]string, len(state.deletedRegistrations))
	for i, registration := range state.deletedRegistrations {
		deletedRegistrationTxHashes[i] = string(registration.TxHash)
	}
	require.ElementsMatch(t, []string{"reg-b"}, deletedRegistrationTxHashes)
}

func exerciseMidnightStore(t *testing.T, store midnightStore) midnightState {
	t.Helper()
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

	var ret midnightState
	var err error
	ret.unspentAssets, err = store.FindUnspentMidnightAssetCreates()
	require.NoError(t, err)
	ret.unspentRegistrations, err = store.FindUnspentMidnightRegistrations()
	require.NoError(t, err)
	ret.page, err = store.FindMidnightAssetCreatesFrom(0, 0, 1, nil)
	require.NoError(t, err)
	ret.governance, err = store.GetLatestMidnightGovernanceDatum(
		models.MidnightGovernanceDatumTypeCouncil,
		11,
		nil,
	)
	require.NoError(t, err)
	ret.latestAriadne, err = store.GetLatestMidnightAriadneParams(nil)
	require.NoError(t, err)
	ret.historicalAriadne, err =
		store.GetMidnightAriadneParamsAtOrBeforeEpoch(1, nil)
	require.NoError(t, err)
	ret.rollbacks, err = store.FindMidnightAriadneRollbacksByBlock(nil, 12)
	require.NoError(t, err)
	ret.candidates, err = store.GetMidnightEpochCandidatesByEpoch(2, nil)
	require.NoError(t, err)
	ret.registrations, err =
		store.GetMidnightCommitteeCandidateRegistrationsByTxHashes(
			[][]byte{[]byte("candidate-a"), []byte("missing")},
			nil,
		)
	require.NoError(t, err)

	rollback := store.Transaction(t.Context())
	ret.deletedCreates, err = store.DeleteMidnightAssetCreatesByBlock(
		rollback,
		11,
	)
	require.NoError(t, err)
	ret.deletedSpends, err = store.DeleteMidnightAssetSpendsByBlock(
		rollback,
		12,
	)
	require.NoError(t, err)
	ret.deletedRegistrations, err = store.DeleteMidnightRegistrationsByBlock(
		rollback,
		11,
	)
	require.NoError(t, err)
	ret.deletedDeregistrations, err =
		store.DeleteMidnightDeregistrationsByBlock(rollback, 12)
	require.NoError(t, err)
	require.NoError(t, rollback.Commit())
	return ret
}

type offchainStore interface {
	SetConstitution(*models.Constitution, types.Txn) error
	EnsureOffchainMetadataPointers(
		context.Context,
		time.Time,
		types.Txn,
	) (int, error)
	GetOffchainMetadataFetchBatch(
		context.Context,
		int,
		time.Time,
		types.Txn,
	) ([]models.OffchainMetadata, error)
	SetOffchainMetadataFetchResult(
		context.Context,
		*models.OffchainMetadata,
		types.Txn,
	) error
	GetOffchainMetadata(
		string,
		string,
		[]byte,
		types.Txn,
	) (*models.OffchainMetadata, error)
}

type offchainState struct {
	created      int
	createdAgain int
	firstBatch   []models.OffchainMetadata
	secondBatch  []models.OffchainMetadata
	fetched      *models.OffchainMetadata
}

func TestSharedSQLStoreOffchainParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	state := exerciseOffchainStore(t, store)
	require.Equal(t, 1, state.created)
	require.Zero(t, state.createdAgain)
	require.Len(t, state.firstBatch, 1)
	require.Empty(t, state.secondBatch)
	require.NotNil(t, state.fetched)
	require.Equal(t, models.OffchainMetadataStatusFetched, state.fetched.Status)
	require.Equal(t, []byte(`{"name":"constitution"}`), state.fetched.Content)
}

func exerciseOffchainStore(t *testing.T, store offchainStore) offchainState {
	t.Helper()
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	fetchedAt := now.Add(time.Minute)
	nextFetch := now.Add(time.Hour)
	hash := bytes.Repeat([]byte{0x42}, 32)
	const url = "https://metadata.example.test/constitution.json"
	require.NoError(t, store.SetConstitution(
		&models.Constitution{
			AnchorURL:  "  " + url + " ",
			AnchorHash: hash,
			AddedSlot:  10,
		},
		nil,
	))
	var ret offchainState
	var err error
	ret.created, err = store.EnsureOffchainMetadataPointers(
		t.Context(),
		now,
		nil,
	)
	require.NoError(t, err)
	ret.createdAgain, err = store.EnsureOffchainMetadataPointers(
		t.Context(),
		now,
		nil,
	)
	require.NoError(t, err)
	ret.firstBatch, err = store.GetOffchainMetadataFetchBatch(
		t.Context(),
		10,
		now,
		nil,
	)
	require.NoError(t, err)
	ret.secondBatch, err = store.GetOffchainMetadataFetchBatch(
		t.Context(),
		10,
		now,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, ret.firstBatch, 1)
	require.Equal(t, 1, ret.created)
	require.Zero(t, ret.createdAgain)
	require.Empty(t, ret.secondBatch)
	doc := ret.firstBatch[0]
	doc.Status = models.OffchainMetadataStatusFetched
	doc.ContentType = "application/json"
	doc.BodyHash = bytes.Repeat([]byte{0x24}, 32)
	doc.Content = []byte(`{"name":"constitution"}`)
	doc.FetchedAt = &fetchedAt
	doc.NextFetchAfter = &nextFetch
	doc.FetchAttempts = 1
	doc.LastHTTPStatus = 200
	require.NoError(t, store.SetOffchainMetadataFetchResult(
		t.Context(),
		&doc,
		nil,
	))
	ret.fetched, err = store.GetOffchainMetadata(
		models.OffchainMetadataSourceConstitution,
		url,
		hash,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, ret.fetched)
	require.Equal(t, models.OffchainMetadataStatusFetched, ret.fetched.Status)
	require.Equal(t, url, ret.fetched.URL)
	require.Equal(
		t,
		models.OffchainMetadataSourceConstitution,
		ret.fetched.SourceType,
	)
	require.Equal(t, "application/json", ret.fetched.ContentType)
	require.Equal(t, hash, ret.fetched.Hash)
	require.Equal(t, bytes.Repeat([]byte{0x24}, 32), ret.fetched.BodyHash)
	require.Equal(t, []byte(`{"name":"constitution"}`), ret.fetched.Content)
	require.Equal(t, uint(1), ret.fetched.FetchAttempts)
	require.Equal(t, uint(200), ret.fetched.LastHTTPStatus)
	normalizeOffchainTimes(ret.firstBatch)
	normalizeOffchainDocument(ret.fetched)
	return ret
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

type operationalStore interface {
	Transaction(ctx context.Context) types.Txn
	GetTip(types.Txn) (ochainsync.Tip, error)
	SetTip(ochainsync.Tip, types.Txn) error
	SetNetworkState(uint64, uint64, uint64, types.Txn) error
	GetNetworkState(types.Txn) (*models.NetworkState, error)
	DeleteNetworkStateAfterSlot(uint64, types.Txn) error
	GetSyncState(string, types.Txn) (string, error)
	SetSyncState(string, string, types.Txn) error
	DeleteSyncState(string, types.Txn) error
	SetEpoch(
		uint64,
		uint64,
		[]byte,
		[]byte,
		[]byte,
		[]byte,
		uint,
		uint,
		uint,
		types.Txn,
	) error
	GetEpoch(uint64, types.Txn) (*models.Epoch, error)
	GetEpochs(types.Txn) ([]models.Epoch, error)
	GetEpochsByEra(uint, types.Txn) ([]models.Epoch, error)
	GetEpochBySlot(uint64, types.Txn) (*models.Epoch, error)
	DeleteEpochsAfterSlot(uint64, types.Txn) error
	SetBlockNonce([]byte, uint64, []byte, bool, types.Txn) error
	GetBlockNonce(ocommon.Point, types.Txn) ([]byte, error)
	GetBlockNoncesInSlotRange(
		uint64,
		uint64,
		types.Txn,
	) ([]models.BlockNonce, error)
	GetLastBlockNonceInRange(uint64, uint64, types.Txn) ([]byte, error)
	DeleteBlockNoncesBeforeSlotWithoutCheckpoints(uint64, types.Txn) error
	DeleteBlockNoncesAfterPoint(ocommon.Point, types.Txn) error
	SetDatum(lcommon.Blake2b256, []byte, uint64, types.Txn) error
	GetDatum(lcommon.Blake2b256, types.Txn) (*models.Datum, error)
	SetPParams([]byte, uint64, uint64, uint, types.Txn) error
	GetPParams(uint64, uint, types.Txn) ([]models.PParams, error)
	SetPParamUpdate([]byte, []byte, uint64, uint64, types.Txn) error
	GetPParamUpdates(uint64, types.Txn) ([]models.PParamUpdate, error)
	DeletePParamsAfterSlot(uint64, types.Txn) error
	DeletePParamUpdatesAfterSlot(uint64, types.Txn) error
	AddNetworkDonation(uint64, uint64, uint64, types.Txn) error
	SumNetworkDonationsForEpoch(uint64, types.Txn) (uint64, error)
	DeleteNetworkDonationsAfterSlot(uint64, types.Txn) error
	GetImportCheckpoint(
		string,
		types.Txn,
	) (*models.ImportCheckpoint, error)
	SetImportCheckpoint(*models.ImportCheckpoint, types.Txn) error
	GetBackfillCheckpoint(string, types.Txn) (*models.BackfillCheckpoint, error)
	SetBackfillCheckpoint(*models.BackfillCheckpoint, types.Txn) error
	GetConstitution(types.Txn) (*models.Constitution, error)
	SetConstitution(*models.Constitution, types.Txn) error
	DeleteConstitutionsAfterSlot(uint64, types.Txn) error
	SetCommitteeMembers([]*models.CommitteeMember, types.Txn) error
	SetCommitteeQuorum(*types.Rat, uint64, types.Txn) error
	ClearCommitteeQuorum(uint64, types.Txn) error
	GetCommitteeQuorum(types.Txn) (*types.Rat, error)
	GetCommitteeMembers(types.Txn) ([]*models.CommitteeMember, error)
	GetCommitteeMembersIncludeDeleted(
		types.Txn,
	) ([]*models.CommitteeMember, error)
	SoftDeleteCommitteeMembers(
		[]models.CommitteeCredential,
		uint64,
		types.Txn,
	) error
	DeleteCommitteeMembersAfterSlot(uint64, types.Txn) error
}

type operationalSnapshot struct {
	tip                  ochainsync.Tip
	network              *models.NetworkState
	syncValue            string
	deletedSync          string
	epoch                *models.Epoch
	epochs               []models.Epoch
	epochsByEra          []models.Epoch
	epochBySlot          *models.Epoch
	nonce                []byte
	lastNonce            []byte
	nonces               []models.BlockNonce
	datum                *models.Datum
	pparams              []models.PParams
	pparamUpdates        []models.PParamUpdate
	donations            uint64
	importCheckpoint     *models.ImportCheckpoint
	backfillCheckpoint   *models.BackfillCheckpoint
	constitution         *models.Constitution
	committeeQuorum      *types.Rat
	committeeMembers     []*models.CommitteeMember
	allCommitteeMembers  []*models.CommitteeMember
	networkRollback      *models.NetworkState
	epochsRollback       []models.Epoch
	noncesRollback       []models.BlockNonce
	pparamsRollback      []models.PParams
	updatesRollback      []models.PParamUpdate
	donationsRollback    uint64
	constitutionRollback *models.Constitution
	quorumRollback       *types.Rat
	membersRollback      []*models.CommitteeMember
}

func TestSharedSQLStoreOperationalParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	snapshot := exerciseOperationalStore(t, store)
	require.Equal(t, uint64(7), snapshot.tip.BlockNumber)
	require.Equal(t, ^uint64(0), uint64(snapshot.network.Treasury))
	require.Equal(t, "value", snapshot.syncValue)
	require.Empty(t, snapshot.deletedSync)
	require.NotNil(t, snapshot.epoch)
	require.Equal(t, uint64(1), snapshot.epoch.EpochId)
	require.Equal(t, uint64(100), snapshot.epoch.StartSlot)
	require.Equal(t, []byte("nonce-1"), snapshot.epoch.Nonce)
	require.Equal(t, []byte("evolving-1"), snapshot.epoch.EvolvingNonce)
	require.Equal(t, []byte("candidate-1"), snapshot.epoch.CandidateNonce)
	require.Equal(t, []byte("lab-1"), snapshot.epoch.LastEpochBlockNonce)
	require.Equal(t, uint(2), snapshot.epoch.EraId)
	require.Equal(t, uint(2), snapshot.epoch.SlotLength)
	require.Equal(t, uint(200), snapshot.epoch.LengthInSlots)
	require.Len(t, snapshot.epochs, 2)
	require.Len(t, snapshot.nonces, 3)
	require.Equal(t, []byte("nonce-1-updated"), snapshot.nonce)
	require.Equal(t, uint64(16), snapshot.donations)
	require.NotNil(t, snapshot.datum)
	require.Equal(t, []byte("datum"), snapshot.datum.RawDatum)
	require.Equal(t, uint64(42), snapshot.datum.AddedSlot)
	require.Len(t, snapshot.pparams, 1)
	require.Len(t, snapshot.pparamUpdates, 2)
	require.Nil(t, snapshot.committeeQuorum)
	require.Len(t, snapshot.committeeMembers, 1)
	require.Len(t, snapshot.allCommitteeMembers, 2)
	require.NotNil(t, snapshot.networkRollback)
	require.Equal(t, uint64(10), uint64(snapshot.networkRollback.Treasury))
	require.Equal(t, uint64(20), uint64(snapshot.networkRollback.Reserves))
	require.Equal(t, uint64(5), snapshot.networkRollback.Slot)
	require.Len(t, snapshot.epochsRollback, 1)
	require.Equal(t, uint64(7), snapshot.donationsRollback)
}

func exerciseOperationalStore(
	t *testing.T,
	store operationalStore,
) operationalSnapshot {
	t.Helper()
	var datumHash lcommon.Blake2b256
	copy(datumHash[:], []byte("datum-hash"))
	hash1 := []byte("block-one")
	hash2 := []byte("block-two")
	hash3 := []byte("block-three")
	startedAt := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	updatedAt := startedAt.Add(time.Minute)
	deletedAt := uint64(30)

	txn := store.Transaction(t.Context())
	require.NoError(t, store.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 42, Hash: []byte("tip-hash")},
		BlockNumber: 7,
	}, txn))
	require.NoError(t, store.SetNetworkState(10, 20, 5, txn))
	require.NoError(t, store.SetNetworkState(^uint64(0), 30, 10, txn))
	require.NoError(t, store.SetSyncState("keep", "value", txn))
	require.NoError(t, store.SetSyncState("delete", "value", txn))
	require.NoError(t, store.DeleteSyncState("delete", txn))
	require.NoError(t, store.SetEpoch(
		0,
		0,
		[]byte("nonce-0"),
		[]byte("evolving-0"),
		[]byte("candidate-0"),
		nil,
		1,
		1,
		100,
		txn,
	))
	require.NoError(t, store.SetEpoch(
		100,
		1,
		[]byte("nonce-1"),
		[]byte("evolving-1"),
		[]byte("candidate-1"),
		[]byte("lab-1"),
		2,
		2,
		200,
		txn,
	))
	require.NoError(t, store.SetBlockNonce(
		hash1,
		1,
		[]byte("nonce-1"),
		false,
		txn,
	))
	require.NoError(t, store.SetBlockNonce(
		hash1,
		1,
		[]byte("nonce-1-updated"),
		true,
		txn,
	))
	require.NoError(t, store.SetBlockNonce(
		hash2,
		2,
		[]byte("nonce-2"),
		false,
		txn,
	))
	require.NoError(t, store.SetBlockNonce(
		hash3,
		3,
		[]byte("nonce-3"),
		false,
		txn,
	))
	require.NoError(t, store.SetDatum(
		datumHash,
		[]byte("datum"),
		42,
		txn,
	))
	require.NoError(t, store.SetPParams(
		[]byte("params-1"),
		20,
		1,
		2,
		txn,
	))
	require.NoError(t, store.SetPParams(
		[]byte("params-2"),
		30,
		2,
		2,
		txn,
	))
	require.NoError(t, store.SetPParamUpdate(
		[]byte("genesis"),
		[]byte("update-1"),
		25,
		1,
		txn,
	))
	require.NoError(t, store.SetPParamUpdate(
		[]byte("genesis"),
		[]byte("update-2"),
		35,
		2,
		txn,
	))
	require.NoError(t, store.AddNetworkDonation(20, 3, 7, txn))
	require.NoError(t, store.AddNetworkDonation(21, 3, 8, txn))
	require.NoError(t, store.AddNetworkDonation(21, 3, 9, txn))
	require.NoError(t, store.SetImportCheckpoint(
		&models.ImportCheckpoint{
			ImportKey: "snapshot:42",
			Phase:     models.ImportPhaseUTxO,
		},
		txn,
	))
	require.NoError(t, store.SetImportCheckpoint(
		&models.ImportCheckpoint{
			ImportKey: "snapshot:42",
			Phase:     models.ImportPhasePParams,
		},
		txn,
	))
	require.NoError(t, store.SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:      "metadata",
			LastSlot:   10,
			TotalSlots: 100,
			StartedAt:  startedAt,
			UpdatedAt:  startedAt,
		},
		txn,
	))
	require.NoError(t, store.SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:      "metadata",
			LastSlot:   20,
			TotalSlots: 100,
			StartedAt:  startedAt.Add(time.Hour),
			UpdatedAt:  updatedAt,
			Completed:  true,
		},
		txn,
	))
	require.NoError(t, store.SetConstitution(
		&models.Constitution{
			AnchorURL:  "https://example.test/one",
			AnchorHash: []byte("constitution-one"),
			PolicyHash: []byte("policy-one"),
			AddedSlot:  10,
		},
		txn,
	))
	require.NoError(t, store.SetConstitution(
		&models.Constitution{
			AnchorURL:   "https://example.test/two",
			AnchorHash:  []byte("constitution-two"),
			PolicyHash:  []byte("policy-two"),
			AddedSlot:   20,
			DeletedSlot: &deletedAt,
		},
		txn,
	))
	require.NoError(t, store.SetCommitteeMembers(
		[]*models.CommitteeMember{
			{
				ColdCredHash: []byte("committee-one"),
				ExpiresEpoch: 100,
				AddedSlot:    10,
			},
			{
				ColdCredHash: []byte("committee-two"),
				ExpiresEpoch: 200,
				AddedSlot:    20,
			},
		},
		txn,
	))
	require.NoError(t, store.SetCommitteeQuorum(
		&types.Rat{Rat: big.NewRat(2, 3)},
		40,
		txn,
	))
	require.NoError(t, store.ClearCommitteeQuorum(50, txn))
	require.NoError(t, store.SoftDeleteCommitteeMembers(
		[]models.CommitteeCredential{{
			Credential: []byte("committee-two"),
		}},
		60,
		txn,
	))
	require.NoError(t, txn.Commit())

	var snapshot operationalSnapshot
	var err error
	snapshot.tip, err = store.GetTip(nil)
	require.NoError(t, err)
	snapshot.network, err = store.GetNetworkState(nil)
	require.NoError(t, err)
	snapshot.syncValue, err = store.GetSyncState("keep", nil)
	require.NoError(t, err)
	snapshot.deletedSync, err = store.GetSyncState("delete", nil)
	require.NoError(t, err)
	snapshot.epoch, err = store.GetEpoch(1, nil)
	require.NoError(t, err)
	snapshot.epochs, err = store.GetEpochs(nil)
	require.NoError(t, err)
	snapshot.epochsByEra, err = store.GetEpochsByEra(2, nil)
	require.NoError(t, err)
	snapshot.epochBySlot, err = store.GetEpochBySlot(150, nil)
	require.NoError(t, err)
	snapshot.nonce, err = store.GetBlockNonce(
		ocommon.Point{Slot: 1, Hash: hash1},
		nil,
	)
	require.NoError(t, err)
	snapshot.lastNonce, err = store.GetLastBlockNonceInRange(0, 4, nil)
	require.NoError(t, err)
	snapshot.nonces, err = store.GetBlockNoncesInSlotRange(0, 4, nil)
	require.NoError(t, err)
	snapshot.datum, err = store.GetDatum(datumHash, nil)
	require.NoError(t, err)
	snapshot.pparams, err = store.GetPParams(2, 2, nil)
	require.NoError(t, err)
	snapshot.pparamUpdates, err = store.GetPParamUpdates(2, nil)
	require.NoError(t, err)
	snapshot.donations, err = store.SumNetworkDonationsForEpoch(3, nil)
	require.NoError(t, err)
	snapshot.importCheckpoint, err = store.GetImportCheckpoint(
		"snapshot:42",
		nil,
	)
	require.NoError(t, err)
	snapshot.backfillCheckpoint, err = store.GetBackfillCheckpoint(
		"metadata",
		nil,
	)
	require.NoError(t, err)
	snapshot.constitution, err = store.GetConstitution(nil)
	require.NoError(t, err)
	snapshot.committeeQuorum, err = store.GetCommitteeQuorum(nil)
	require.NoError(t, err)
	snapshot.committeeMembers, err = store.GetCommitteeMembers(nil)
	require.NoError(t, err)
	snapshot.allCommitteeMembers, err = store.GetCommitteeMembersIncludeDeleted(
		nil,
	)
	require.NoError(t, err)

	rollbackTxn := store.Transaction(t.Context())
	require.NoError(t, store.DeleteNetworkStateAfterSlot(5, rollbackTxn))
	require.NoError(t, store.DeleteEpochsAfterSlot(50, rollbackTxn))
	require.NoError(t, store.DeleteBlockNoncesBeforeSlotWithoutCheckpoints(
		3,
		rollbackTxn,
	))
	require.NoError(t, store.DeleteBlockNoncesAfterPoint(
		ocommon.Point{Slot: 1, Hash: hash1},
		rollbackTxn,
	))
	require.NoError(t, store.DeletePParamsAfterSlot(25, rollbackTxn))
	require.NoError(t, store.DeletePParamUpdatesAfterSlot(30, rollbackTxn))
	require.NoError(t, store.DeleteNetworkDonationsAfterSlot(20, rollbackTxn))
	require.NoError(t, store.DeleteConstitutionsAfterSlot(15, rollbackTxn))
	require.NoError(t, store.DeleteCommitteeMembersAfterSlot(40, rollbackTxn))
	require.NoError(t, rollbackTxn.Commit())
	snapshot.networkRollback, err = store.GetNetworkState(nil)
	require.NoError(t, err)
	snapshot.epochsRollback, err = store.GetEpochs(nil)
	require.NoError(t, err)
	snapshot.noncesRollback, err = store.GetBlockNoncesInSlotRange(0, 4, nil)
	require.NoError(t, err)
	snapshot.pparamsRollback, err = store.GetPParams(2, 2, nil)
	require.NoError(t, err)
	snapshot.updatesRollback, err = store.GetPParamUpdates(2, nil)
	require.NoError(t, err)
	snapshot.donationsRollback, err = store.SumNetworkDonationsForEpoch(3, nil)
	require.NoError(t, err)
	snapshot.constitutionRollback, err = store.GetConstitution(nil)
	require.NoError(t, err)
	snapshot.quorumRollback, err = store.GetCommitteeQuorum(nil)
	require.NoError(t, err)
	snapshot.membersRollback, err = store.GetCommitteeMembers(nil)
	require.NoError(t, err)
	return snapshot
}

type poolStore interface {
	ImportPool(*models.Pool, *models.PoolRegistration, types.Txn) error
	GetPool(lcommon.PoolKeyHash, bool, types.Txn) (*models.Pool, error)
	GetPoolByVrfKeyHash([]byte, uint64, types.Txn) (*models.Pool, error)
	GetPools([]lcommon.PoolKeyHash, types.Txn) ([]models.Pool, error)
	UpdatePoolOpCertSequence(
		lcommon.PoolKeyHash,
		uint64,
		uint64,
		types.Txn,
	) error
	LatestPoolOpCertSequence(
		lcommon.PoolKeyHash,
		types.Txn,
	) (uint64, bool, error)
	LatestPoolOpCertSequenceAtOrBefore(
		lcommon.PoolKeyHash,
		uint64,
		types.Txn,
	) (uint64, bool, error)
	GetPoolBlockIssuersInSlotRange(
		uint64,
		uint64,
		types.Txn,
	) ([]models.PoolOpCertSequence, error)
	CountPoolBlocksInSlotRange(
		[]lcommon.PoolKeyHash,
		uint64,
		uint64,
		types.Txn,
	) (map[string]uint64, uint64, error)
	SetEpoch(
		uint64,
		uint64,
		[]byte,
		[]byte,
		[]byte,
		[]byte,
		uint,
		uint,
		uint,
		types.Txn,
	) error
	SetTip(ochainsync.Tip, types.Txn) error
	GetActivePoolKeyHashes(types.Txn) ([][]byte, error)
	GetActivePoolKeyHashesAtSlot(uint64, types.Txn) ([][]byte, error)
	RetirePools(types.Txn, [][]byte, uint64, uint64) error
	GetRetiringPools(uint64, types.Txn) ([]models.PoolRetiringRow, error)
	CreateAccount(types.Txn, *models.Account) error
	CreateUtxo(types.Txn, *models.Utxo) error
	GetStakeByPool([]byte, types.Txn) (uint64, uint64, error)
	GetStakeByPools(
		[][]byte,
		types.Txn,
	) (map[string]uint64, map[string]uint64, error)
}

type poolState struct {
	Pool                  *models.Pool
	ByVRF                 *models.Pool
	Pools                 []models.Pool
	Missing               *models.Pool
	Sequence              uint64
	SequenceSet           bool
	HistoricalSequence    uint64
	HistoricalSequenceSet bool
	Issuers               []models.PoolOpCertSequence
	Counts                map[string]uint64
	Total                 uint64
	Active                [][]byte
	ActiveAtSlot          [][]byte
	Retiring              []models.PoolRetiringRow
	Stake                 uint64
	Delegators            uint64
	StakeMap              map[string]uint64
	DelegatorMap          map[string]uint64
}

func TestSharedSQLStorePoolParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	state := exercisePoolStore(t, store)
	poolKeyHash := bytes.Repeat([]byte{0x91}, 28)
	require.NotNil(t, state.Pool)
	require.Equal(t, poolKeyHash, state.Pool.PoolKeyHash)
	require.NotNil(t, state.ByVRF)
	require.Equal(t, poolKeyHash, state.ByVRF.PoolKeyHash)
	require.Equal(t, 1, len(state.Pools))
	require.Equal(t, bytes.Repeat([]byte{0x91}, 28), state.Pools[0].PoolKeyHash)
	require.Nil(t, state.Missing)
	require.Equal(t, uint64(3), state.Sequence)
	require.True(t, state.SequenceSet)
	require.Equal(t, uint64(2), state.HistoricalSequence)
	require.True(t, state.HistoricalSequenceSet)
	require.Equal(t, []models.PoolOpCertSequence{
		{PoolKeyHash: bytes.Repeat([]byte{0x91}, 28), ID: 1, Sequence: 2, Slot: 20},
		{PoolKeyHash: bytes.Repeat([]byte{0x91}, 28), ID: 2, Sequence: 3, Slot: 21},
	}, state.Issuers)
	require.Equal(t, uint64(2), state.Total)
	require.Equal(t, map[string]uint64{
		string(bytes.Repeat([]byte{0x91}, 28)): 2,
	}, state.Counts)
	require.Len(t, state.Active, 1)
	require.Equal(t, bytes.Repeat([]byte{0x91}, 28), state.Active[0])
	require.Len(t, state.ActiveAtSlot, 1)
	require.Equal(t, bytes.Repeat([]byte{0x91}, 28), state.ActiveAtSlot[0])
	require.Equal(t, uint64(700), state.Stake)
	require.Equal(t, uint64(1), state.Delegators)
	require.Equal(t, map[string]uint64{
		string(bytes.Repeat([]byte{0x91}, 28)): 700,
	}, state.StakeMap)
	require.Equal(t, map[string]uint64{
		string(bytes.Repeat([]byte{0x91}, 28)): 1,
	}, state.DelegatorMap)
	require.Equal(t, []models.PoolRetiringRow{{
		PoolKeyHash: bytes.Repeat([]byte{0x91}, 28), Epoch: 5,
	}}, state.Retiring)
}

func exercisePoolStore(t *testing.T, store poolStore) poolState {
	t.Helper()
	poolBytes := bytes.Repeat([]byte{0x91}, 28)
	poolKeyHash := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(poolBytes),
	)
	vrf := bytes.Repeat([]byte{0x92}, 32)
	ipv4 := net.IPv4(127, 0, 0, 1)
	pool := &models.Pool{
		PoolKeyHash: poolBytes, VrfKeyHash: vrf,
		RewardAccount:              bytes.Repeat([]byte{0x93}, 28),
		RewardAccountCredentialTag: 1,
		Pledge:                     1000, Cost: 50,
		Margin: &types.Rat{Rat: big.NewRat(1, 10)},
	}
	registration := &models.PoolRegistration{
		PoolKeyHash: poolBytes, VrfKeyHash: vrf,
		RewardAccount:              pool.RewardAccount,
		RewardAccountCredentialTag: 1,
		Pledge:                     1000, Cost: 50, AddedSlot: 10,
		DepositAmount: 500,
		Margin:        &types.Rat{Rat: big.NewRat(1, 10)},
		MetadataUrl:   "https://pool.example",
		MetadataHash:  []byte("metadata"),
		Owners: []models.PoolRegistrationOwner{{
			KeyHash: bytes.Repeat([]byte{0x94}, 28),
		}},
		Relays: []models.PoolRegistrationRelay{{
			Ipv4: &ipv4, Port: 3001,
		}},
	}
	require.NoError(t, store.ImportPool(pool, registration, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(
		poolKeyHash,
		2,
		20,
		nil,
	))
	require.NoError(t, store.UpdatePoolOpCertSequence(
		poolKeyHash,
		3,
		21,
		nil,
	))
	require.NoError(t, store.SetEpoch(
		2, 0, nil, nil, nil, nil, 0, 1, 100, nil,
	))
	require.NoError(t, store.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 25, Hash: []byte("tip")},
		BlockNumber: 1,
	}, nil))
	stakeKey := bytes.Repeat([]byte{0x95}, 28)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeKey, Pool: poolBytes, Active: true,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       bytes.Repeat([]byte{0x96}, 32),
		StakingKey: stakeKey, Amount: 700, AddedSlot: 15,
	}))
	var ret poolState
	var err error
	ret.Pool, err = store.GetPool(poolKeyHash, true, nil)
	require.NoError(t, err)
	ret.ByVRF, err = store.GetPoolByVrfKeyHash(vrf, 2, nil)
	require.NoError(t, err)
	ret.Pools, err = store.GetPools([]lcommon.PoolKeyHash{poolKeyHash}, nil)
	require.NoError(t, err)
	ret.Missing, err = store.GetPool(
		lcommon.PoolKeyHash(
			lcommon.NewBlake2b224(bytes.Repeat([]byte{0xff}, 28)),
		),
		true,
		nil,
	)
	require.NoError(t, err)
	ret.Sequence, ret.SequenceSet, err = store.LatestPoolOpCertSequence(
		poolKeyHash,
		nil,
	)
	require.NoError(t, err)
	ret.HistoricalSequence, ret.HistoricalSequenceSet, err =
		store.LatestPoolOpCertSequenceAtOrBefore(poolKeyHash, 20, nil)
	require.NoError(t, err)
	ret.Issuers, err = store.GetPoolBlockIssuersInSlotRange(20, 21, nil)
	require.NoError(t, err)
	ret.Counts, ret.Total, err = store.CountPoolBlocksInSlotRange(
		[]lcommon.PoolKeyHash{poolKeyHash},
		20,
		21,
		nil,
	)
	require.NoError(t, err)
	ret.Active, err = store.GetActivePoolKeyHashes(nil)
	require.NoError(t, err)
	ret.ActiveAtSlot, err = store.GetActivePoolKeyHashesAtSlot(25, nil)
	require.NoError(t, err)
	ret.Stake, ret.Delegators, err = store.GetStakeByPool(poolBytes, nil)
	require.NoError(t, err)
	ret.StakeMap, ret.DelegatorMap, err = store.GetStakeByPools(
		[][]byte{poolBytes},
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, store.RetirePools(
		nil,
		[][]byte{poolBytes},
		5,
		30,
	))
	ret.Retiring, err = store.GetRetiringPools(2, nil)
	require.NoError(t, err)
	return ret
}

type rewardStore interface {
	Transaction(ctx context.Context) types.Txn
	SaveRewardAdaPots(*models.RewardAdaPots, types.Txn) error
	GetRewardAdaPots(uint64, types.Txn) (*models.RewardAdaPots, error)
	SaveRewardSnapshot(*models.RewardSnapshot, types.Txn) error
	ClaimFallbackRewardSnapshot(
		*models.RewardSnapshot,
		types.Txn,
	) (bool, error)
	ClaimFallbackRewardSnapshotGuard(
		uint64,
		string,
		types.Txn,
	) (bool, uint, error)
	ReleaseFallbackRewardSnapshotGuard(uint, types.Txn) error
	GetRewardSnapshot(
		uint64,
		string,
		types.Txn,
	) (*models.RewardSnapshot, error)
	SaveRewardPoolInputs([]*models.RewardPoolInput, types.Txn) error
	GetRewardPoolInputs(
		uint64,
		types.Txn,
	) ([]*models.RewardPoolInput, error)
	SaveRewardStakeInputs([]*models.RewardStakeInput, types.Txn) error
	GetRewardStakeInputs(
		uint64,
		types.Txn,
	) ([]*models.RewardStakeInput, error)
	DeleteRewardInputsForEpoch(uint64, types.Txn) error
	SaveRewardPoolOutputs([]*models.RewardPoolOutput, types.Txn) error
	GetRewardPoolOutputs(
		uint64,
		types.Txn,
	) ([]*models.RewardPoolOutput, error)
	SaveRewardAccountOutputs([]*models.RewardAccountOutput, types.Txn) error
	GetRewardAccountOutputs(
		uint64,
		types.Txn,
	) ([]*models.RewardAccountOutput, error)
	DeleteRewardOutputsForEpoch(uint64, types.Txn) error
	DeleteRewardStateAfterSlot(uint64, types.Txn) error
	DeleteRewardStateBeforeEpoch(uint64, types.Txn) error
}

type rewardState struct {
	pots                    *models.RewardAdaPots
	authoritativeClaim      bool
	fallbackClaim           bool
	fallback                *models.RewardSnapshot
	guardCreated            bool
	guardRemoved            *models.RewardSnapshot
	provisionalGuard        bool
	provisionalGuardID      uint
	authoritativeGuard      bool
	poolInputs              []*models.RewardPoolInput
	stakeInputs             []*models.RewardStakeInput
	poolOutputs             []*models.RewardPoolOutput
	accountOutputs          []*models.RewardAccountOutput
	oldStakeInputs          []*models.RewardStakeInput
	recentStakeInputs       []*models.RewardStakeInput
	rolledBackPoolOutputs   []*models.RewardPoolOutput
	rolledBackAccountOutput []*models.RewardAccountOutput
}

func TestSharedSQLStoreRewardStateParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	state := exerciseRewardStore(t, store)
	require.NotNil(t, state.pots)
	require.Equal(t, uint64(10), uint64(state.pots.Treasury))
	require.Equal(t, uint64(20), uint64(state.pots.Reserves))
	require.Equal(t, uint64(30), uint64(state.pots.Fees))
	require.Equal(t, uint64(40), uint64(state.pots.Rewards))
	require.False(t, state.authoritativeClaim)
	require.True(t, state.fallbackClaim)
	require.NotNil(t, state.fallback)
	require.Equal(t, uint64(250), uint64(state.fallback.TotalActiveStake))
	require.Equal(t, uint64(60), state.fallback.BoundarySlot)
	require.True(t, state.guardCreated)
	require.Nil(t, state.guardRemoved)
	require.True(t, state.provisionalGuard)
	require.False(t, state.authoritativeGuard)
	require.Len(t, state.poolInputs, 1)
	require.Equal(t, []byte("pool-a"), state.poolInputs[0].PoolKeyHash)
	require.Equal(t, types.Uint64(100), state.poolInputs[0].DelegatedStake)
	require.Len(t, state.stakeInputs, 1)
	require.Equal(t, []byte("stake-a"), state.stakeInputs[0].StakingKey)
	require.Equal(t, types.Uint64(75), state.stakeInputs[0].Stake)
	require.Len(t, state.poolOutputs, 1)
	require.Equal(t, types.Uint64(90), state.poolOutputs[0].TotalReward)
	require.Equal(t, types.Uint64(10), state.poolOutputs[0].LeaderReward)
	require.Equal(t, types.Uint64(7), state.poolOutputs[0].LeaderRewardDeficit)
	require.Len(t, state.accountOutputs, 1)
	require.Equal(t, uint64(80), uint64(state.accountOutputs[0].Amount))
	require.Empty(t, state.rolledBackPoolOutputs)
	require.Empty(t, state.rolledBackAccountOutput)
}

func exerciseRewardStore(t *testing.T, store rewardStore) rewardState {
	t.Helper()
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
	provisionalGuard, provisionalGuardID, err :=
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

	ret := rewardState{
		authoritativeClaim: authoritativeClaim,
		fallbackClaim:      fallbackClaim,
		guardCreated:       guardCreated,
		provisionalGuard:   provisionalGuard,
		provisionalGuardID: provisionalGuardID,
		authoritativeGuard: authoritativeGuard,
	}
	ret.pots, err = store.GetRewardAdaPots(5, nil)
	require.NoError(t, err)
	ret.fallback, err = store.GetRewardSnapshot(6, "mark", nil)
	require.NoError(t, err)
	ret.guardRemoved, err = store.GetRewardSnapshot(7, "mark", nil)
	require.NoError(t, err)
	ret.poolInputs, err = store.GetRewardPoolInputs(5, nil)
	require.NoError(t, err)
	ret.stakeInputs, err = store.GetRewardStakeInputs(5, nil)
	require.NoError(t, err)
	ret.poolOutputs, err = store.GetRewardPoolOutputs(5, nil)
	require.NoError(t, err)
	ret.accountOutputs, err = store.GetRewardAccountOutputs(5, nil)
	require.NoError(t, err)

	require.NoError(t, store.DeleteRewardStateBeforeEpoch(5, nil))
	ret.oldStakeInputs, err = store.GetRewardStakeInputs(1, nil)
	require.NoError(t, err)
	ret.recentStakeInputs, err = store.GetRewardStakeInputs(5, nil)
	require.NoError(t, err)
	require.NoError(t, store.DeleteRewardStateAfterSlot(49, nil))
	ret.rolledBackPoolOutputs, err = store.GetRewardPoolOutputs(5, nil)
	require.NoError(t, err)
	ret.rolledBackAccountOutput, err = store.GetRewardAccountOutputs(5, nil)
	require.NoError(t, err)
	return ret
}

type snapshotStore interface {
	Transaction(ctx context.Context) types.Txn
	SavePoolStakeSnapshot(*models.PoolStakeSnapshot, types.Txn) error
	SavePoolStakeSnapshots([]*models.PoolStakeSnapshot, types.Txn) error
	GetPoolStakeSnapshot(
		uint64,
		string,
		[]byte,
		types.Txn,
	) (*models.PoolStakeSnapshot, error)
	GetPoolStakeSnapshotsByEpoch(
		uint64,
		string,
		types.Txn,
	) ([]*models.PoolStakeSnapshot, error)
	GetTotalActiveStake(uint64, string, types.Txn) (uint64, error)
	SaveEpochSummary(*models.EpochSummary, types.Txn) error
	GetEpochSummary(uint64, types.Txn) (*models.EpochSummary, error)
	GetLatestEpochSummary(types.Txn) (*models.EpochSummary, error)
	DeletePoolStakeSnapshotsForEpoch(uint64, string, types.Txn) error
	DeletePoolStakeSnapshotsAfterEpoch(uint64, types.Txn) error
	DeletePoolStakeSnapshotsBeforeEpoch(uint64, types.Txn) error
	DeleteEpochSummariesAfterEpoch(uint64, types.Txn) error
}

type snapshotState struct {
	pool             *models.PoolStakeSnapshot
	pools            []*models.PoolStakeSnapshot
	totalBeforeReady uint64
	totalAfterReady  uint64
	summary          *models.EpochSummary
	latest           *models.EpochSummary
	remaining        []*models.PoolStakeSnapshot
}

func TestSharedSQLStoreSnapshotParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	state := exerciseSnapshotStore(t, store)
	require.NotNil(t, state.pool)
	require.Equal(t, types.Uint64(20), state.pool.TotalStake)
	require.Len(t, state.pools, 2)
	snapshotRows := make(map[string]models.PoolStakeSnapshot, len(state.pools))
	for _, snapshot := range state.pools {
		snapshotRows[string(snapshot.PoolKeyHash)] = *snapshot
	}
	poolA := snapshotRows["pool-a"]
	require.Equal(t, uint64(1), poolA.Epoch)
	require.Equal(t, models.PoolStakeSnapshotTypeMark, poolA.SnapshotType)
	require.Equal(t, types.Uint64(20), poolA.TotalStake)
	require.Equal(t, types.Uint64(999), poolA.StakeDenominator)
	require.Equal(t, uint64(2), poolA.DelegatorCount)
	require.Equal(t, uint64(20), poolA.CapturedSlot)
	require.Equal(t, uint8(2), poolA.RewardAccountAutoVote)
	require.True(t, poolA.RewardAccountAutoVoteResolved)
	poolB := snapshotRows["pool-b"]
	require.Equal(t, uint64(1), poolB.Epoch)
	require.Equal(t, models.PoolStakeSnapshotTypeMark, poolB.SnapshotType)
	require.Equal(t, types.Uint64(30), poolB.TotalStake)
	require.Equal(t, types.Uint64(200), poolB.StakeDenominator)
	require.Equal(t, uint64(3), poolB.DelegatorCount)
	require.Equal(t, uint64(20), poolB.CapturedSlot)
	require.Equal(t, uint64(50), state.totalBeforeReady)
	require.Equal(t, uint64(888), state.totalAfterReady)
	require.NotNil(t, state.summary)
	require.Equal(t, types.Uint64(888), state.summary.TotalActiveStake)
	require.Equal(t, uint64(22), state.summary.BoundarySlot)
	require.NotNil(t, state.latest)
	require.Equal(t, types.Uint64(888), state.latest.TotalActiveStake)
	require.Equal(t, uint64(22), state.latest.BoundarySlot)
	remainingStakeByPool := make(map[string]types.Uint64, len(state.remaining))
	for _, snapshot := range state.remaining {
		remainingStakeByPool[string(snapshot.PoolKeyHash)] = snapshot.TotalStake
	}
	require.Equal(t, map[string]types.Uint64{
		"pool-a": types.Uint64(20),
		"pool-b": types.Uint64(30),
	}, remainingStakeByPool)
}

func exerciseSnapshotStore(t *testing.T, store snapshotStore) snapshotState {
	t.Helper()
	poolA := []byte("pool-a")
	poolB := []byte("pool-b")

	txn := store.Transaction(t.Context())
	require.NoError(t, store.SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:            1,
			SnapshotType:     models.PoolStakeSnapshotTypeMark,
			PoolKeyHash:      poolA,
			TotalStake:       10,
			StakeDenominator: 100,
			DelegatorCount:   1,
			CapturedSlot:     10,
		},
		txn,
	))
	require.NoError(t, store.SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			{
				Epoch:                         1,
				SnapshotType:                  models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:                   poolA,
				TotalStake:                    20,
				StakeDenominator:              999,
				DelegatorCount:                2,
				CapturedSlot:                  20,
				RewardAccountAutoVote:         2,
				RewardAccountAutoVoteResolved: true,
			},
			{
				Epoch:            1,
				SnapshotType:     models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:      poolB,
				TotalStake:       30,
				StakeDenominator: 200,
				DelegatorCount:   3,
				CapturedSlot:     20,
			},
			{
				Epoch:        0,
				SnapshotType: models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:  poolA,
				TotalStake:   5,
			},
			{
				Epoch:        2,
				SnapshotType: models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:  poolA,
				TotalStake:   40,
			},
			{
				Epoch:        1,
				SnapshotType: models.PoolStakeSnapshotTypeGo,
				PoolKeyHash:  poolA,
				TotalStake:   15,
			},
		},
		txn,
	))
	require.NoError(t, store.SaveEpochSummary(
		&models.EpochSummary{
			Epoch:            1,
			TotalActiveStake: 999,
			TotalPoolCount:   2,
			TotalDelegators:  5,
			EpochNonce:       []byte("nonce-one"),
			BoundarySlot:     20,
		},
		txn,
	))
	require.NoError(t, txn.Commit())

	var ret snapshotState
	var err error
	ret.pool, err = store.GetPoolStakeSnapshot(
		1,
		models.PoolStakeSnapshotTypeMark,
		poolA,
		nil,
	)
	require.NoError(t, err)
	ret.pools, err = store.GetPoolStakeSnapshotsByEpoch(
		1,
		models.PoolStakeSnapshotTypeMark,
		nil,
	)
	require.NoError(t, err)
	ret.totalBeforeReady, err = store.GetTotalActiveStake(
		1,
		models.PoolStakeSnapshotTypeMark,
		nil,
	)
	require.NoError(t, err)

	require.NoError(t, store.SaveEpochSummary(
		&models.EpochSummary{
			Epoch:            1,
			TotalActiveStake: 777,
			TotalPoolCount:   2,
			TotalDelegators:  5,
			EpochNonce:       []byte("nonce-two"),
			BoundarySlot:     21,
			SnapshotReady:    true,
		},
		nil,
	))
	require.NoError(t, store.SaveEpochSummary(
		&models.EpochSummary{
			Epoch:            1,
			TotalActiveStake: 888,
			TotalPoolCount:   2,
			TotalDelegators:  5,
			EpochNonce:       []byte("nonce-three"),
			BoundarySlot:     22,
		},
		nil,
	))
	require.NoError(t, store.SaveEpochSummary(
		&models.EpochSummary{
			Epoch:            2,
			TotalActiveStake: 40,
			TotalPoolCount:   1,
			TotalDelegators:  1,
			BoundarySlot:     30,
			SnapshotReady:    true,
		},
		nil,
	))
	ret.totalAfterReady, err = store.GetTotalActiveStake(
		1,
		models.PoolStakeSnapshotTypeMark,
		nil,
	)
	require.NoError(t, err)
	ret.summary, err = store.GetEpochSummary(1, nil)
	require.NoError(t, err)

	rollback := store.Transaction(t.Context())
	require.NoError(t, store.DeletePoolStakeSnapshotsForEpoch(
		1,
		models.PoolStakeSnapshotTypeGo,
		rollback,
	))
	require.NoError(t, store.DeletePoolStakeSnapshotsAfterEpoch(1, rollback))
	require.NoError(t, store.DeletePoolStakeSnapshotsBeforeEpoch(1, rollback))
	require.NoError(t, store.DeleteEpochSummariesAfterEpoch(1, rollback))
	require.NoError(t, rollback.Commit())
	ret.remaining, err = store.GetPoolStakeSnapshotsByEpoch(
		1,
		models.PoolStakeSnapshotTypeMark,
		nil,
	)
	require.NoError(t, err)
	ret.latest, err = store.GetLatestEpochSummary(nil)
	require.NoError(t, err)
	return ret
}

func newSharedSQLStore(
	t *testing.T,
) (*sqlstore.Store, *sql.DB) {
	t.Helper()
	store, writeDB, _, err := openSQLStore(
		Config{DataDir: t.TempDir()},
		metadata.ProviderDependencies{},
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

func TestOpenSharedSQLStoreFilePoolsAndWAL(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	store, writeDB, readDB, err := openSQLStore(
		Config{MaxConnections: 3},
		metadata.ProviderDependencies{DataDir: dataDir},
	)
	require.NoError(t, err)
	require.NotSame(t, writeDB, readDB)
	require.Equal(t, 1, writeDB.Stats().MaxOpenConnections)
	require.Equal(t, 3, readDB.Stats().MaxOpenConnections)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	var journalMode string
	require.NoError(t, writeDB.QueryRow(
		"PRAGMA journal_mode",
	).Scan(&journalMode))
	require.Equal(t, "wal", journalMode)

	var migrationCount int
	require.NoError(t, readDB.QueryRow(
		"SELECT COUNT(*) FROM schema_migrations WHERE phase = 'complete'",
	).Scan(&migrationCount))
	// A fresh database runs every registered migration, so the count is taken
	// from the registry rather than written out: what is under test here is
	// that startup completed all of them, not how many there currently are.
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Equal(t, len(registry), migrationCount)

	requireDiskSize(t, store)
	require.FileExists(t, filepath.Join(dataDir, "metadata.sqlite"))
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
// unused connection blocks wal_checkpoint(TRUNCATE) -- see
// TestWALCheckpointTruncateIdleConnectionDoesNotBlock below, which tests
// that specific claim directly and finds it false: a connection that runs
// this same query pattern and is then left idle in the pool does not block
// a subsequent TRUNCATE against real WAL content, while an actual held read
// transaction does. The production mechanism connecting "DiskSize on
// readDB" to the persistent busy=1 symptom is not fully isolated by either
// test; the dedicated-connection fix is justified by the production
// before/after observation and by matching checkpointWAL's own existing
// design, not by a claim that mere pool attachment is sufficient to block a
// checkpoint.
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

// TestWALCheckpointTruncateIdleConnectionDoesNotBlock settles, by direct
// reproduction against dingo's actual readDB DSN and pragmas, the claim
// disputed in review of the sqliteDiskSize fix above: does a connection that
// ran exactly sqliteDiskSize's old query pattern (two independent
// QueryRowContext(...).Scan(...) calls) and is then left idle in readDB's
// pool -- never closed -- block a separate connection's PRAGMA
// wal_checkpoint(TRUNCATE)?
//
// SQLite's own documentation for wal_checkpoint says RESTART and TRUNCATE
// block only on an active writer or a reader still using an old snapshot,
// not on a connection that is merely idle. This test confirms that is also
// true here: case "idle_finalized_connection" runs the old two-PRAGMA-read
// pattern, confirms (via OpenConnections) that a connection really is
// pinned in the pool afterward, and still observes busy=0 and a full
// truncation to zero bytes against real, substantial WAL content -- the
// same as the "no_other_connection" baseline. Case
// "held_read_transaction" is the positive control: an explicit,
// uncommitted read transaction on the same readDB pool does reproduce
// busy=1, proving this test can detect a real blocker when one exists.
//
// Each case also cross-checks internal consistency: a checkpoint's own
// reported log/checkpointed frame counts can legitimately read 0 with a
// large physical -wal file still on disk, when wal_autocheckpoint's PASSIVE
// checkpoints already backfilled every frame between writes (PASSIVE never
// truncates -- see checkpointInterval's doc comment above) -- so the
// assertions key off busy and the physical file size actually dropping to
// zero, not off log/checkpointed being nonzero.
func TestWALCheckpointTruncateIdleConnectionDoesNotBlock(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	store, writeDB, readDB, err := openSQLStore(
		Config{DataDir: dataDir},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	_, err = writeDB.Exec(
		`CREATE TABLE wal_checkpoint_repro (id INTEGER PRIMARY KEY, data BLOB)`,
	)
	require.NoError(t, err)
	blob := make([]byte, 4096)

	databasePath := filepath.Join(dataDir, "metadata.sqlite")
	databaseURI := sqliteFileURI(databasePath)
	walPath := databasePath + "-wal"

	// growWAL commits enough rows, one autocommit INSERT at a time (like
	// dingo's own chain-sync writes), to leave substantial physical WAL
	// content regardless of wal_autocheckpoint's PASSIVE backfilling.
	growWAL := func() {
		for range 500 {
			_, err := writeDB.Exec(
				`INSERT INTO wal_checkpoint_repro (data) VALUES (?)`,
				blob,
			)
			require.NoError(t, err)
		}
	}
	walSize := func() int64 {
		info, err := os.Stat(walPath)
		if err != nil {
			return 0
		}
		return info.Size()
	}

	// checkpoint opens a dedicated connection and issues exactly one
	// PRAGMA wal_checkpoint(TRUNCATE), mirroring checkpointWAL itself.
	checkpoint := func() (busy, log, checkpointed int) {
		t.Helper()
		db, err := sql.Open(
			"sqlite",
			fmt.Sprintf(
				"%s?_pragma=busy_timeout(%d)&_pragma=synchronous(OFF)",
				databaseURI,
				250,
			),
		)
		require.NoError(t, err)
		defer func() { require.NoError(t, db.Close()) }()
		db.SetMaxOpenConns(1)
		row := db.QueryRowContext(
			context.Background(),
			"PRAGMA wal_checkpoint(TRUNCATE)",
		)
		require.NoError(t, row.Scan(&busy, &log, &checkpointed))
		return busy, log, checkpointed
	}

	t.Run("no_other_connection", func(t *testing.T) {
		growWAL()
		require.Positive(t, walSize(), "need real WAL content to checkpoint")
		busy, _, _ := checkpoint()
		require.Zero(t, busy, "nothing else attached: TRUNCATE must not be busy")
		require.Zero(t, walSize(), "TRUNCATE must truncate the -wal file to 0 bytes")
	})

	t.Run("idle_finalized_connection", func(t *testing.T) {
		growWAL()
		require.Positive(t, walSize(), "need real WAL content to checkpoint")

		// sqliteDiskSize's old query pattern, run directly against readDB.
		var pageCount, pageSize int64
		require.NoError(
			t,
			readDB.QueryRowContext(context.Background(), "PRAGMA page_count").
				Scan(&pageCount),
		)
		require.NoError(
			t,
			readDB.QueryRowContext(context.Background(), "PRAGMA page_size").
				Scan(&pageSize),
		)
		require.Positive(
			t,
			readDB.Stats().OpenConnections,
			"test setup: the query above should leave a connection pinned in the pool",
		)

		busy, _, _ := checkpoint()
		require.Zerof(
			t,
			busy,
			"an idle, already-finalized connection (readDB.OpenConnections=%d) "+
				"must not block TRUNCATE",
			readDB.Stats().OpenConnections,
		)
		require.Zero(t, walSize(), "TRUNCATE must still truncate the -wal file to 0 bytes")
	})

	t.Run("held_read_transaction_positive_control", func(t *testing.T) {
		growWAL()
		before := walSize()
		require.Positive(t, before, "need real WAL content to checkpoint")

		tx, err := readDB.BeginTx(
			context.Background(),
			&sql.TxOptions{ReadOnly: true},
		)
		require.NoError(t, err)
		var count int64
		require.NoError(
			t,
			tx.QueryRowContext(
				context.Background(),
				"SELECT count(*) FROM wal_checkpoint_repro",
			).Scan(&count),
		)

		busy, _, _ := checkpoint()
		require.NotZero(
			t,
			busy,
			"a real, held, uncommitted read transaction must block TRUNCATE "+
				"(control: proves this test can detect a real blocker)",
		)
		require.Equal(
			t,
			before,
			walSize(),
			"a busy TRUNCATE must not have truncated the -wal file",
		)
		require.NoError(t, tx.Rollback())
	})
}

// TestOpenSharedSQLStoreWALPragmas pins the checkpoint threshold and retained
// WAL limit. Both pragmas are connection-local, so each pool must set them.
func TestOpenSharedSQLStoreWALPragmas(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	store, writeDB, readDB, err := openSQLStore(
		Config{},
		metadata.ProviderDependencies{DataDir: dataDir},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	for name, db := range map[string]*sql.DB{"writeDB": writeDB, "readDB": readDB} {
		var pages int
		require.NoError(t, db.QueryRow(
			"PRAGMA wal_autocheckpoint",
		).Scan(&pages))
		require.Equalf(t, 10000, pages, "%s wal_autocheckpoint", name)

		var bytes int
		require.NoError(t, db.QueryRow(
			"PRAGMA journal_size_limit",
		).Scan(&bytes))
		require.Equalf(t, 67108864, bytes, "%s journal_size_limit", name)
	}
}

func TestWALResetHonorsJournalSizeLimit(t *testing.T) {
	t.Parallel()
	const journalSizeLimit = 64 << 20
	dataDir := t.TempDir()
	store, writeDB, _, err := openSQLStore(
		Config{},
		metadata.ProviderDependencies{DataDir: dataDir},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	ctx := context.Background()
	conn, err := writeDB.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})
	var limit int64
	require.NoError(
		t,
		conn.QueryRowContext(ctx, "PRAGMA journal_size_limit").Scan(&limit),
	)
	require.Equal(t, int64(journalSizeLimit), limit)
	_, err = conn.ExecContext(ctx, "PRAGMA wal_autocheckpoint=0")
	require.NoError(t, err)
	_, err = conn.ExecContext(
		ctx,
		`CREATE TABLE wal_size_limit (id INTEGER PRIMARY KEY, data BLOB)`,
	)
	require.NoError(t, err)

	// One transaction ensures the active WAL grows beyond the limit before a
	// reset can occur, leaving room for SQLite page and record overhead.
	blob := bytes.Repeat([]byte{0xA5}, 1<<20)
	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	for range 66 {
		_, err = tx.ExecContext(
			ctx,
			`INSERT INTO wal_size_limit (data) VALUES (?)`,
			blob,
		)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())

	walPath := filepath.Join(dataDir, "metadata.sqlite-wal")
	walInfo, err := os.Stat(walPath)
	require.NoError(t, err)
	require.Greater(t, walInfo.Size(), int64(journalSizeLimit))

	var busy, logFrames, checkpointedFrames int
	require.NoError(
		t,
		conn.QueryRowContext(ctx, "PRAGMA wal_checkpoint(RESTART)").Scan(
			&busy,
			&logFrames,
			&checkpointedFrames,
		),
	)
	require.Zero(t, busy)
	require.Equal(t, logFrames, checkpointedFrames)
	// RESTART checkpoints and makes the next writer restart the WAL from the
	// beginning. That subsequent commit performs the reset and applies the
	// configured journal size limit to the oversized file.
	_, err = conn.ExecContext(
		ctx,
		`INSERT INTO wal_size_limit (data) VALUES (?)`,
		[]byte("reset"),
	)
	require.NoError(t, err)

	walInfo, err = os.Stat(walPath)
	require.NoError(t, err)
	require.LessOrEqual(t, walInfo.Size(), int64(journalSizeLimit))
}

func TestOpenSharedSQLStoreMemoryIsolation(t *testing.T) {
	t.Parallel()
	first, firstDB, _, err := openSQLStore(
		Config{},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, first.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, first.Close())
	})
	second, secondDB, _, err := openSQLStore(
		Config{},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, second.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, second.Close())
	})

	_, err = firstDB.Exec("CREATE TABLE isolation_marker (id INTEGER)")
	require.NoError(t, err)
	var count int
	require.NoError(t, secondDB.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master "+
			"WHERE type = 'table' AND name = 'isolation_marker'",
	).Scan(&count))
	require.Zero(t, count)
}

type tokenRegistryStore interface {
	UpsertTokenRegistryEntries(
		context.Context,
		[]models.TokenRegistryEntry,
		time.Time,
		types.Txn,
	) (int, error)
	GetTokenRegistryEntry(
		string,
		types.Txn,
	) (*models.TokenRegistryEntry, error)
}

var testSyncedAt = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

const (
	testSubjectNut  = "00000002df633853f6a47465c9496721d2d5b1291b8398016c0e87ae6e7574636f696e"
	testSubjectDjed = "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61446a65644d6963726f555344"
)

func TestSharedSQLStoreTokenRegistryRoundTrip(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	exerciseTokenRegistryStore(t, store)
}

func exerciseTokenRegistryStore(t *testing.T, store tokenRegistryStore) {
	t.Helper()
	ctx := t.Context()

	written, err := store.UpsertTokenRegistryEntries(
		ctx,
		[]models.TokenRegistryEntry{
			{
				Subject:     testSubjectNut,
				Name:        "nutcoin",
				Ticker:      "NUT",
				Description: "The legendary Nutcoin.",
				URL:         "https://fivebinaries.com/nutcoin",
				Logo:        "iVBORw0KGgo=",
			},
			{
				Subject:  testSubjectDjed,
				Name:     "Djed USD",
				Ticker:   "DJED",
				Decimals: new(6),
			},
		},
		testSyncedAt,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 2, written)

	nut, err := store.GetTokenRegistryEntry(testSubjectNut, nil)
	require.NoError(t, err)
	require.NotNil(t, nut)
	require.Equal(t, testSubjectNut, nut.Subject)
	require.Equal(t, "nutcoin", nut.Name)
	require.Equal(t, "NUT", nut.Ticker)
	require.Equal(t, "The legendary Nutcoin.", nut.Description)
	require.Equal(t, "https://fivebinaries.com/nutcoin", nut.URL)
	require.Equal(t, "iVBORw0KGgo=", nut.Logo)
	require.Nil(t, nut.Decimals, "absent decimals must not read back as zero")

	djed, err := store.GetTokenRegistryEntry(testSubjectDjed, nil)
	require.NoError(t, err)
	require.NotNil(t, djed)
	require.NotNil(t, djed.Decimals)
	require.Equal(t, 6, *djed.Decimals)
	require.Empty(t, djed.Logo)
}

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

func TestSharedSQLStoreTokenRegistryMissingSubject(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	entry, err := store.GetTokenRegistryEntry(testSubjectNut, nil)

	require.NoError(t, err, "an unknown subject is absence, not an error")
	require.Nil(t, entry)
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

func TestSharedSQLStoreTokenRegistryUpsertEmptyBatch(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	written, err := store.UpsertTokenRegistryEntries(
		t.Context(),
		nil,
		testSyncedAt,
		nil,
	)

	require.NoError(t, err)
	require.Zero(t, written)
}

func TestSharedSQLStoreTokenRegistryRejectsBlankSubject(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	_, err := store.UpsertTokenRegistryEntries(
		t.Context(),
		[]models.TokenRegistryEntry{{Name: "no subject"}},
		testSyncedAt,
		nil,
	)

	require.Error(t, err)
}

type tokenRegistryPruneStore interface {
	tokenRegistryStore
	PruneTokenRegistryEntriesBefore(
		context.Context,
		time.Time,
		types.Txn,
	) (int, error)
}

type transactionalTokenRegistryStore interface {
	tokenRegistryPruneStore
	Transaction(context.Context) types.Txn
	GetSyncState(string, types.Txn) (string, error)
	SetSyncState(string, string, types.Txn) error
}

// TestSharedSQLStoreTokenRegistryPrune covers the reconciliation half of a
// snapshot: subjects the upstream registry has dropped must stop being served,
// which an upsert-only sync cannot achieve on its own.
func TestSharedSQLStoreTokenRegistryPrune(t *testing.T) {
	t.Parallel()
	var store tokenRegistryPruneStore
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
	var store tokenRegistryPruneStore
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
	var store transactionalTokenRegistryStore
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
		Config{DataDir: t.TempDir()},
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

// TestTransactionWitnessCleanupStaysIndexedAfterDeferredIndexDrop covers
// witness cleanup after a deferred index drop.
//
// Mithril drops the deferred-index manifest before API-mode historical
// backfill, and backfill then calls SetTransaction for every transaction it
// replays. Each of those calls clears the four witness tables by
// transaction_id first. With the manifest deferring the transaction_id index on
// three of those tables, each delete became a full scan of a table that grows
// with every transaction written, so per-transaction cost rose with the row
// count already present: measured on preview, backfill fell from 3311 to 9
// blocks/sec and its own ETA climbed from 30m to 177h.
//
// The plan is asserted rather than the index's existence: an index the planner
// does not choose is a write cost with no read benefit, and it is EXPLAINed
// from the store's own exported statement so the plan pinned here is the plan
// of the delete that actually runs.
func TestTransactionWitnessCleanupStaysIndexedAfterDeferredIndexDrop(
	t *testing.T,
) {
	t.Parallel()
	store, db := newAPIModeSQLStore(t)
	seedWitnessRows(t, db, 0, 2000)

	requireWitnessCleanupIndexed(t, db, "before deferring indexes")
	require.NoError(t, store.DropDeferredIndexes())
	requireWitnessCleanupIndexed(t, db, "after DropDeferredIndexes")
}

// TestRetainedIndexesResidentAfterCriticalRebuild covers the repair path a
// node takes when a prior bulk-load cycle was interrupted.
//
// serve calls RepairCriticalDeferredIndexes before it clears sync_status, and
// Mithril sync calls BuildCritical before it clears its own. Both return with
// the store about to accept API writes, and neither waits for the lazy
// remainder that background maintenance finishes later. A database an older
// binary's manifest had dropped a retained transaction_id index from therefore
// has to be repaired by the critical rebuild too: otherwise every
// SetTransaction between that point and the full rebuild clears its witness
// tables with the full scan the retained set exists to prevent.
//
// The state under test is that database, reproduced by dropping the retained
// set out from under a pending marker.
func TestRetainedIndexesResidentAfterCriticalRebuild(t *testing.T) {
	t.Parallel()
	store, db := newAPIModeSQLStore(t)
	seedWitnessRows(t, db, 0, 2000)

	require.NoError(t, store.DropDeferredIndexes())
	dropRetainedIndexes(t, db)
	requireWitnessCleanupScans(t, db)

	require.NoError(t, store.BuildCriticalDeferredIndexes())
	requireRetainedIndexesResident(t, db, "after BuildCriticalDeferredIndexes")
	requireWitnessCleanupIndexed(t, db, "after BuildCriticalDeferredIndexes")

	// The critical rebuild owns only the critical subset, so the marker has
	// to survive it for background maintenance to finish the rest.
	pending, err := store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(
		t,
		pending,
		"BuildCriticalDeferredIndexes must leave the marker for the lazy "+
			"remainder",
	)
}

// dropRetainedIndexes removes every deferred.Retained index, reproducing what
// a binary whose manifest still deferred them leaves on disk.
//
// The names are manifest constants, not input, and SQLite takes no bound
// parameter in DDL.
func dropRetainedIndexes(t *testing.T, db *sql.DB) {
	t.Helper()
	require.NotEmpty(t, deferred.Retained)
	for _, index := range deferred.Retained {
		_, err := db.Exec("DROP INDEX IF EXISTS " + index.Name)
		require.NoError(t, err, "dropping retained index %s", index.Name)
	}
}

// requireRetainedIndexesResident asserts that every deferred.Retained index is
// present in the schema.
func requireRetainedIndexesResident(t *testing.T, db *sql.DB, when string) {
	t.Helper()
	for _, index := range deferred.Retained {
		var found int
		require.NoError(t, db.QueryRow(
			`SELECT COUNT(*) FROM sqlite_master
WHERE type = 'index' AND name = ?`,
			index.Name,
		).Scan(&found))
		require.Equal(
			t,
			1,
			found,
			"%s: retained index %s must be resident whenever the store "+
				"can serve writes",
			when,
			index.Name,
		)
	}
}

// requireWitnessCleanupScans asserts the negative case the repair has to fix:
// with the retained set absent, the idempotency deletes are full scans. Without
// it a rebuild that restored nothing would still pass the assertions above if
// the indexes had never been missing.
func requireWitnessCleanupScans(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, table := range sqlstore.TransactionWitnessTables() {
		plan := queryPlan(
			t,
			db,
			sqlstore.TransactionWitnessCleanupSQL(table),
			1,
		)
		require.Contains(
			t,
			plan,
			"SCAN "+table,
			"the simulated interrupted cycle must leave the %s "+
				"idempotency delete unindexed:\n%s",
			table,
			plan,
		)
	}
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
			seedWitnessRows(t, db, 0, 500)
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

type transactionReadStore interface {
	GetTransactionByHash([]byte, types.Txn) (*models.Transaction, error)
	GetTransactionSlotByHash([]byte, types.Txn) (uint64, bool, error)
	GetTransactionIDByHash([]byte, types.Txn) (uint, bool, error)
	GetTransactionMetadataByHash([]byte, types.Txn) ([]byte, error)
	SumTransactionFeesInSlotRange(uint64, uint64, types.Txn) (uint64, error)
	GetTransactionsByBlockHash([]byte, types.Txn) ([]models.Transaction, error)
	GetTransactionsByHashes([][]byte, types.Txn) ([]models.Transaction, error)
	GetTransactionHashesAfterSlot(uint64, types.Txn) ([][]byte, error)
	GetTransactionsByAddress(
		[]byte,
		uint8,
		[]byte,
		int,
		int,
		string,
		types.Txn,
	) ([]models.Transaction, error)
	CountTransactionsByAddress(
		[]byte,
		uint8,
		[]byte,
		types.Txn,
	) (int, error)
	CountTransactionsByPaymentCred([]byte, types.Txn) (int, error)
	GetTransactionsByMetadataLabel(
		uint64,
		int,
		int,
		bool,
		types.Txn,
	) ([]models.Transaction, error)
	CountTransactionsByMetadataLabel(uint64, types.Txn) (int, error)
	GetAddressesByCredential(
		uint8,
		[]byte,
		int,
		int,
		string,
		types.Txn,
	) ([]models.AddressTransaction, error)
	CountAddressesByCredential(uint8, []byte, types.Txn) (int, error)
	DeleteAddressTransactionsAfterSlot(uint64, types.Txn) error
	DeleteTransactionMetadataLabelsAfterSlot(uint64, types.Txn) error
}

type transactionReadState struct {
	ByHash            *models.Transaction
	Missing           *models.Transaction
	Slot              uint64
	SlotFound         bool
	ID                uint
	IDFound           bool
	Metadata          []byte
	FeeSum            uint64
	ByBlock           []models.Transaction
	ByHashes          []models.Transaction
	HashesAfter       [][]byte
	ByAddress         []models.Transaction
	AddressCount      int
	PaymentCount      int
	ByLabel           []models.Transaction
	LabelCount        int
	Addresses         []models.AddressTransaction
	AddressesCount    int
	AddressCountAfter int
	LabelCountAfter   int
}

func TestSharedSQLStoreTransactionReadParity(t *testing.T) {
	t.Parallel()
	store, raw := newSharedSQLStore(t)

	seedTransactionReadFixture := func(exec func(string, ...any) error) {
		t.Helper()
		require.NoError(t, exec(`
INSERT INTO "transaction" (
    id, hash, block_hash, metadata, slot, type, fee, collateral_fee,
    ttl, block_index, valid
) VALUES
    (1, ?, ?, ?, 10, 1, '5', '0', '20', 0, TRUE),
    (2, ?, ?, NULL, 11, 2, '9', '7', '21', 1, FALSE),
    (3, ?, ?, ?, 12, 3, '4', '0', '22', 0, TRUE)`,
			[]byte("tx-a"), []byte("block-a"), []byte("meta-a"),
			[]byte("tx-b"), []byte("block-a"),
			[]byte("tx-c"), []byte("block-b"), []byte("meta-c"),
		))
		require.NoError(t, exec(`
INSERT INTO address_transaction (
    id, payment_key, staking_key, credential_tag, transaction_id, slot, tx_index
) VALUES
    (1, ?, ?, 0, 1, 10, 0),
    (2, ?, ?, 0, 2, 11, 1),
    (3, ?, ?, 1, 3, 12, 0)`,
			[]byte("pay-a"), []byte("stake-a"),
			[]byte("pay-a"), []byte("stake-a"),
			[]byte("pay-b"), []byte("stake-a"),
		))
		require.NoError(t, exec(`
INSERT INTO transaction_metadata_label (
    id, transaction_id, label, slot, cbor_value, json_value
) VALUES
    (1, 1, '42', 10, X'01', '{}'),
    (2, 2, '42', 11, X'02', '{}'),
    (3, 3, '99', 12, X'03', '{}')`))
	}
	seedTransactionReadFixture(func(query string, args ...any) error {
		_, err := raw.Exec(query, args...)
		return err
	})

	state := exerciseTransactionReadStore(t, store)
	require.NotNil(t, state.ByHash)
	require.Equal(t, []byte("tx-a"), state.ByHash.Hash)
	require.Nil(t, state.Missing)
	require.Equal(t, uint64(11), state.Slot)
	require.True(t, state.SlotFound)
	require.Equal(t, uint(3), state.ID)
	require.True(t, state.IDFound)
	require.Equal(t, []byte("meta-a"), state.Metadata)
	require.Equal(t, uint64(16), state.FeeSum)
	require.Len(t, state.ByBlock, 2)
	require.Len(t, state.ByHashes, 2)
	require.Len(t, state.HashesAfter, 2)
	require.Len(t, state.ByAddress, 1)
	require.Equal(t, 2, state.AddressCount)
	require.Equal(t, 2, state.PaymentCount)
	require.Len(t, state.ByLabel, 2)
	require.Equal(t, 2, state.LabelCount)
	require.Len(t, state.Addresses, 1)
	require.Equal(t, 1, state.AddressesCount)
	require.Equal(t, 1, state.AddressCountAfter)
	require.Equal(t, 1, state.LabelCountAfter)
}

func exerciseTransactionReadStore(
	t *testing.T,
	store transactionReadStore,
) transactionReadState {
	t.Helper()
	var ret transactionReadState
	var err error
	ret.ByHash, err = store.GetTransactionByHash([]byte("tx-a"), nil)
	require.NoError(t, err)
	ret.Missing, err = store.GetTransactionByHash([]byte("missing"), nil)
	require.NoError(t, err)
	ret.Slot, ret.SlotFound, err = store.GetTransactionSlotByHash(
		[]byte("tx-b"),
		nil,
	)
	require.NoError(t, err)
	ret.ID, ret.IDFound, err = store.GetTransactionIDByHash(
		[]byte("tx-c"),
		nil,
	)
	require.NoError(t, err)
	ret.Metadata, err = store.GetTransactionMetadataByHash(
		[]byte("tx-a"),
		nil,
	)
	require.NoError(t, err)
	ret.FeeSum, err = store.SumTransactionFeesInSlotRange(10, 12, nil)
	require.NoError(t, err)
	ret.ByBlock, err = store.GetTransactionsByBlockHash(
		[]byte("block-a"),
		nil,
	)
	require.NoError(t, err)
	ret.ByHashes, err = store.GetTransactionsByHashes(
		[][]byte{[]byte("tx-c"), []byte("tx-a")},
		nil,
	)
	require.NoError(t, err)
	ret.HashesAfter, err = store.GetTransactionHashesAfterSlot(10, nil)
	require.NoError(t, err)
	ret.ByAddress, err = store.GetTransactionsByAddress(
		[]byte("pay-a"),
		0,
		[]byte("stake-a"),
		1,
		0,
		"desc",
		nil,
	)
	require.NoError(t, err)
	ret.AddressCount, err = store.CountTransactionsByAddress(
		[]byte("pay-a"),
		0,
		[]byte("stake-a"),
		nil,
	)
	require.NoError(t, err)
	ret.PaymentCount, err = store.CountTransactionsByPaymentCred(
		[]byte("pay-a"),
		nil,
	)
	require.NoError(t, err)
	ret.ByLabel, err = store.GetTransactionsByMetadataLabel(
		42,
		5,
		0,
		true,
		nil,
	)
	require.NoError(t, err)
	ret.LabelCount, err = store.CountTransactionsByMetadataLabel(42, nil)
	require.NoError(t, err)
	ret.Addresses, err = store.GetAddressesByCredential(
		0,
		[]byte("stake-a"),
		10,
		0,
		"asc",
		nil,
	)
	require.NoError(t, err)
	ret.AddressesCount, err = store.CountAddressesByCredential(
		0,
		[]byte("stake-a"),
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, store.DeleteAddressTransactionsAfterSlot(10, nil))
	require.NoError(
		t,
		store.DeleteTransactionMetadataLabelsAfterSlot(10, nil),
	)
	ret.AddressCountAfter, err = store.CountTransactionsByPaymentCred(
		[]byte("pay-a"),
		nil,
	)
	require.NoError(t, err)
	ret.LabelCountAfter, err = store.CountTransactionsByMetadataLabel(42, nil)
	require.NoError(t, err)
	return ret
}

type transactionWriteStore interface {
	NewBatchAccumulator() types.MetadataBatchAccumulator
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
	SetTransactionBatchedHistorical(
		lcommon.Transaction,
		ocommon.Point,
		uint32,
		map[int]uint64,
		bool,
		bool,
		types.MetadataBatchAccumulator,
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

func requireTransactionWriteAccount(
	t *testing.T,
	store transactionWriteStore,
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
			"SELECT COUNT(*) FROM account_reward_delta",
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
			Config{DataDir: t.TempDir()},
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
		ocommon.Point{Slot: 7, Hash: bytes.Repeat([]byte{0xe8}, 32)}, 0, nil, false, nil,
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

	account := requireTransactionWriteAccount(t, store, 0, stakeKey)
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
		nil,
	))
	account = requireTransactionWriteAccount(t, store, 0, stakeKey)
	require.Equal(t, uint64(1234), uint64(account.Reward))
	require.NoError(t, raw.QueryRow(
		"SELECT COUNT(*) FROM account_reward_delta WHERE tx_hash = ?",
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
				nil,
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
				nil,
			)
			if !tc.createInactive {
				// A credential with no account row at all is an invariant
				// failure: the run must abort, and must leave behind
				// neither a journal row nor an invented account.
				require.ErrorContains(t, backfillErr, "account not found")
				var journaled int
				require.NoError(t, raw.QueryRow(
					"SELECT COUNT(*) FROM account_reward_delta "+
						"WHERE tx_hash = ?",
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
				"SELECT COUNT(*), previous_reward FROM account_reward_delta WHERE tx_hash = ?",
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
				nil,
			))
			require.NoError(t, raw.QueryRow(
				"SELECT COUNT(*) FROM account_reward_delta WHERE tx_hash = ?",
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
			nil,
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
		nil,
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
		transaction, point, 0, nil, true, nil,
	))
	account = requireTransactionWriteAccount(t, store, 0, stakeKey)
	require.Equal(t, uint64(1000), uint64(account.Reward))

	// Replaying the same transaction must not debit the remaining balance again.
	require.NoError(t, store.SetTransaction(
		transaction, point, 0, nil, true, nil,
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
		nil,
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
			account := requireTransactionWriteAccount(t, store, 0, stakeKey)
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
	state := exerciseCertificateWriteStore(t, store, raw)
	require.Equal(t, 9, state.CertificateCount)
	require.True(t, state.AccountActive)
	require.Equal(t, uint64(81), state.AccountAddedSlot)
	require.Equal(t, uint64(81), state.AccountCreated)
	require.True(t, state.DrepActive)
	require.Equal(t, uint64(81), state.DrepAddedSlot)
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
		require.Equal(t, 1, state.TableCounts[table], table)
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

type utxoMutationStore interface {
	CreateAccount(types.Txn, *models.Account) error
	CreateUtxo(types.Txn, *models.Utxo) error
	GetUtxoIncludingSpent(
		[]byte,
		uint32,
		types.Txn,
	) (*models.Utxo, error)
	DeleteUtxo(models.UtxoId, types.Txn) error
	DeleteUtxos([]models.UtxoId, types.Txn) error
	DeleteUtxosAfterSlot(uint64, types.Txn) error
	MarkUtxosDeletedAtSlot(types.Txn, []types.UtxoKey, uint64) error
	SetUtxosNotDeletedAfterSlot(uint64, types.Txn) error
	ImportUtxos([]models.Utxo, types.Txn) error
	GetLiveStakeInputsForPools(
		[][]byte,
		uint64,
		types.Txn,
	) ([]*models.RewardStakeInput, error)
}

type utxoMutationState struct {
	Marked           *models.Utxo
	Restored         *models.Utxo
	DeletedOne       *models.Utxo
	DeletedBatch     *models.Utxo
	DeletedAfterSlot *models.Utxo
	LiveStake        []*models.RewardStakeInput
	Imported         *models.Utxo
}

func TestSharedSQLStoreUtxoMutationParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	state := exerciseUtxoMutationStore(t, store)
	require.NotNil(t, state.Marked)
	require.Equal(t, uint64(20), uint64(state.Marked.DeletedSlot))
	require.NotNil(t, state.Restored)
	require.Zero(t, state.Restored.DeletedSlot)
	require.Nil(t, state.DeletedOne)
	require.Nil(t, state.DeletedBatch)
	require.Nil(t, state.DeletedAfterSlot)
	require.Len(t, state.LiveStake, 1)
	require.Equal(t, bytes.Repeat([]byte{0x71}, 28), state.LiveStake[0].StakingKey)
	require.Equal(t, bytes.Repeat([]byte{0x72}, 28), state.LiveStake[0].PoolKeyHash)
	require.Equal(t, types.Uint64(28), state.LiveStake[0].Stake)
	require.Nil(t, state.Imported)
}

func exerciseUtxoMutationStore(
	t *testing.T,
	store utxoMutationStore,
) utxoMutationState {
	t.Helper()
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
		Amount: 25, AddedSlot: 15,
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
	var ret utxoMutationState
	var err error
	ret.Marked, err = store.GetUtxoIncludingSpent(hashes[0], 0, nil)
	require.NoError(t, err)
	require.NoError(t, store.SetUtxosNotDeletedAfterSlot(19, nil))
	ret.Restored, err = store.GetUtxoIncludingSpent(hashes[0], 0, nil)
	require.NoError(t, err)
	require.NoError(t, store.DeleteUtxo(
		models.UtxoId{Hash: hashes[1], Idx: 1},
		nil,
	))
	ret.DeletedOne, err = store.GetUtxoIncludingSpent(hashes[1], 1, nil)
	require.NoError(t, err)
	require.NoError(t, store.DeleteUtxos(
		[]models.UtxoId{
			{Hash: hashes[2], Idx: 2},
			{Hash: []byte("missing"), Idx: 9},
		},
		nil,
	))
	ret.DeletedBatch, err = store.GetUtxoIncludingSpent(hashes[2], 2, nil)
	require.NoError(t, err)
	require.NoError(t, store.DeleteUtxosAfterSlot(13, nil))
	ret.DeletedAfterSlot, err = store.GetUtxoIncludingSpent(
		hashes[4],
		4,
		nil,
	)
	require.NoError(t, err)
	ret.LiveStake, err = store.GetLiveStakeInputsForPools(
		[][]byte{pool},
		0,
		nil,
	)
	require.NoError(t, err)
	ret.Imported, err = store.GetUtxoIncludingSpent(hashes[5], 5, nil)
	require.NoError(t, err)
	return ret
}

type utxoReadStore interface {
	CreateUtxo(types.Txn, *models.Utxo) error
	GetUtxo([]byte, uint32, types.Txn) (*models.Utxo, error)
	GetUtxoIncludingSpent([]byte, uint32, types.Txn) (*models.Utxo, error)
	GetUtxosAddedAfterSlot(uint64, types.Txn) ([]models.Utxo, error)
	GetLiveUtxosBySlot(uint64, types.Txn) ([]models.UtxoId, error)
	GetUtxosBySlot(uint64, types.Txn) ([]models.UtxoId, error)
	GetUtxosDeletedBeforeSlot(
		uint64,
		int,
		types.Txn,
	) ([]models.Utxo, error)
	GetUtxosByAddress(
		[]models.UtxoAddressPattern,
		int,
		types.Txn,
	) ([]models.Utxo, error)
	GetUtxosByAddressAtSlot(
		models.UtxoAddressPattern,
		uint64,
		types.Txn,
	) ([]models.Utxo, error)
	GetControlledAmountByCredential(uint8, []byte, types.Txn) (uint64, error)
	GetScriptLockedSupply(types.Txn) (uint64, error)
	GetUtxosByAssets([]byte, []byte, types.Txn) ([]models.Utxo, error)
	IterateLiveUtxos(types.Txn, func(*models.Utxo) error) error
}

type utxoReadState struct {
	live            *models.Utxo
	spentLiveLookup *models.Utxo
	spent           *models.Utxo
	added           []models.Utxo
	liveAtSlot      []models.UtxoId
	allAtSlot       []models.UtxoId
	deleted         []models.Utxo
	byAddress       []models.Utxo
	byAddressAtSlot []models.Utxo
	controlled      uint64
	scriptLocked    uint64
	byAsset         []models.Utxo
	iterated        []models.Utxo
}

func TestSharedSQLStoreUtxoReadParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	state := exerciseUtxoReadStore(t, store)
	require.NotNil(t, state.live)
	require.Equal(t, []byte("tx-live"), state.live.TxId)
	require.Equal(t, uint64(10), state.live.AddedSlot)
	require.Equal(t, uint64(100), uint64(state.live.Amount))
	require.Nil(t, state.spentLiveLookup)
	require.NotNil(t, state.spent)
	require.Equal(t, []byte("tx-spent"), state.spent.TxId)
	require.Equal(t, uint64(20), uint64(state.spent.DeletedSlot))
	require.Equal(t, uint64(50), uint64(state.spent.Amount))
	require.Len(t, state.added, 1)
	require.Equal(t, []byte("tx-script"), state.added[0].TxId)
	require.Equal(t, []models.UtxoId{{Hash: []byte("tx-live"), Idx: 0}}, state.liveAtSlot)
	require.ElementsMatch(t, []models.UtxoId{
		{Hash: []byte("tx-live"), Idx: 0},
		{Hash: []byte("tx-spent"), Idx: 1},
	}, state.allAtSlot)
	require.Len(t, state.deleted, 1)
	require.Equal(t, []byte("tx-spent"), state.deleted[0].TxId)
	require.Len(t, state.byAddress, 1)
	require.Equal(t, []byte("tx-live"), state.byAddress[0].TxId)
	require.Equal(t, uint64(100), state.controlled)
	require.Equal(t, uint64(70), state.scriptLocked)
	require.Len(t, state.byAsset, 1)
	require.Equal(t, []byte("tx-live"), state.byAsset[0].TxId)
	iteratedAmounts := make(map[string]types.Uint64, len(state.iterated))
	for _, utxo := range state.iterated {
		iteratedAmounts[string(utxo.TxId)] = utxo.Amount
	}
	require.Equal(t, map[string]types.Uint64{
		"tx-live":   types.Uint64(100),
		"tx-script": types.Uint64(70),
	}, iteratedAmounts)
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

func exerciseUtxoReadStore(t *testing.T, store utxoReadStore) utxoReadState {
	t.Helper()
	paymentKey := bytes.Repeat([]byte{0x11}, 28)
	stakingKey := bytes.Repeat([]byte{0x22}, 28)
	policyID := bytes.Repeat([]byte{0x33}, 28)
	assetName := []byte("asset")
	for _, utxo := range []*models.Utxo{
		{
			TxId:          []byte("tx-live"),
			OutputIdx:     0,
			PaymentKey:    paymentKey,
			StakingKey:    stakingKey,
			CredentialTag: 1,
			AddedSlot:     10,
			Amount:        100,
			Assets: []models.Asset{{
				Name:     assetName,
				PolicyId: policyID, Fingerprint: []byte("fingerprint"),
				Amount: 5,
			}},
		},
		{
			TxId:        []byte("tx-spent"),
			OutputIdx:   1,
			PaymentKey:  paymentKey,
			StakingKey:  stakingKey,
			AddedSlot:   10,
			DeletedSlot: 20,
			Amount:      50,
		},
		{
			TxId:          []byte("tx-script"),
			OutputIdx:     0,
			PaymentKey:    bytes.Repeat([]byte{0x44}, 28),
			AddedSlot:     30,
			Amount:        70,
			PaymentScript: true,
		},
	} {
		require.NoError(t, store.CreateUtxo(nil, utxo))
	}

	var ret utxoReadState
	var err error
	ret.live, err = store.GetUtxo([]byte("tx-live"), 0, nil)
	require.NoError(t, err)
	ret.spentLiveLookup, err = store.GetUtxo([]byte("tx-spent"), 1, nil)
	require.NoError(t, err)
	ret.spent, err = store.GetUtxoIncludingSpent(
		[]byte("tx-spent"),
		1,
		nil,
	)
	require.NoError(t, err)
	ret.added, err = store.GetUtxosAddedAfterSlot(15, nil)
	require.NoError(t, err)
	ret.liveAtSlot, err = store.GetLiveUtxosBySlot(10, nil)
	require.NoError(t, err)
	ret.allAtSlot, err = store.GetUtxosBySlot(10, nil)
	require.NoError(t, err)
	ret.deleted, err = store.GetUtxosDeletedBeforeSlot(25, 1, nil)
	require.NoError(t, err)
	pattern := models.UtxoAddressPattern{PaymentPart: paymentKey}
	ret.byAddress, err = store.GetUtxosByAddress(
		[]models.UtxoAddressPattern{pattern},
		100,
		nil,
	)
	require.NoError(t, err)
	ret.byAddressAtSlot, err = store.GetUtxosByAddressAtSlot(
		pattern,
		15,
		nil,
	)
	require.NoError(t, err)
	ret.controlled, err = store.GetControlledAmountByCredential(
		1,
		stakingKey,
		nil,
	)
	require.NoError(t, err)
	ret.scriptLocked, err = store.GetScriptLockedSupply(nil)
	require.NoError(t, err)
	ret.byAsset, err = store.GetUtxosByAssets(policyID, assetName, nil)
	require.NoError(t, err)
	require.NoError(t, store.IterateLiveUtxos(
		nil,
		func(utxo *models.Utxo) error {
			ret.iterated = append(ret.iterated, *utxo)
			return nil
		},
	))
	return ret
}
