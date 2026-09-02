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
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	_ "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testStoreSequence atomic.Uint64

func newTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB: db,
		Dialect: SQLiteDialect(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store
}

func TestTransactionSavepointAndCommit(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	_, err := store.writeDB.Exec(
		"CREATE TABLE item (id INTEGER PRIMARY KEY, value TEXT)",
	)
	require.NoError(t, err)
	transaction := store.Transaction(t.Context())
	savepointer, ok := transaction.(interface {
		SavePoint(string) error
		RollbackTo(string) error
	})
	require.True(t, ok)
	queryer, _, err := store.dbFromTxn(transaction)
	require.NoError(t, err)
	_, err = queryer.ExecContext(
		context.Background(),
		"INSERT INTO item (id, value) VALUES (1, 'kept')",
	)
	require.NoError(t, err)
	require.NoError(t, savepointer.SavePoint("after_first"))
	_, err = queryer.ExecContext(
		context.Background(),
		"INSERT INTO item (id, value) VALUES (2, 'rolled-back')",
	)
	require.NoError(t, err)
	require.NoError(t, savepointer.RollbackTo("after_first"))
	require.NoError(t, transaction.Commit())
	require.NoError(t, transaction.Commit())

	var count int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM item",
	).Scan(&count))
	require.Equal(t, 1, count)
}

func TestSQLiteBulkModeKeepsPlannerAndWritersAvailable(t *testing.T) {
	store := newTestStore(t)
	store.writeDB.SetMaxOpenConns(1)
	require.NoError(t, store.SetBulkLoadPragmas())
	require.NoError(t, store.UpdatePlannerStats())

	first := store.Transaction(t.Context())
	secondStarted := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		second := store.Transaction(t.Context())
		close(secondStarted)
		secondDone <- second.Commit()
	}()

	select {
	case <-secondStarted:
		t.Fatal("second writer bypassed SQLite pool serialization")
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, first.Commit())
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second writer remained blocked after first commit")
	}
	require.NoError(t, <-secondDone)
	require.NoError(t, store.RestoreNormalPragmas())
}

func TestSumUint64RowsPreservesFullRange(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	_, err := store.writeDB.Exec("CREATE TABLE amounts (amount TEXT NOT NULL)")
	require.NoError(t, err)
	_, err = store.writeDB.Exec(
		"INSERT INTO amounts (amount) VALUES (?)",
		"18446744073709551615",
	)
	require.NoError(t, err)
	db, ctx, err := store.readDBFromTxn(nil)
	require.NoError(t, err)
	value, err := sumUint64Rows(ctx, db, "SELECT amount FROM amounts")
	require.NoError(t, err)
	require.Equal(t, ^uint64(0), value)
}

func TestSumNetworkDonationsReadsSQLiteIntegerAmounts(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	_, err := store.writeDB.Exec(`
CREATE TABLE network_donation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slot INTEGER NOT NULL,
    epoch INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    UNIQUE (slot)
)`)
	require.NoError(t, err)
	require.NoError(t, store.AddNetworkDonation(7, 3, 123, nil))
	total, err := store.SumNetworkDonationsForEpoch(3, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(123), total)
}

func TestCreateUtxoBindsEmptyNullableHashesAsSQLNull(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	utxo := &models.Utxo{
		TxId:      []byte("nullable-utxo"),
		Amount:    1,
		AddedSlot: 1,
	}
	require.NoError(t, store.CreateUtxo(nil, utxo))
	var spent, referenced, collateral any
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT spent_at_tx_id, referenced_by_tx_id, collateral_by_tx_id FROM utxo WHERE id = ?",
		utxo.ID,
	).Scan(&spent, &referenced, &collateral))
	require.Nil(t, spent)
	require.Nil(t, referenced)
	require.Nil(t, collateral)
}

func TestGetStakeByPoolsUsesLiveCredentialAggregate(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	_, err := store.writeDB.Exec(`
CREATE TABLE account (
 credential_tag INTEGER, staking_key BLOB, pool BLOB, active BOOLEAN
);
CREATE TABLE reward_live_stake (
 credential_tag INTEGER, staking_key BLOB, utxo_stake TEXT,
 calculation_version INTEGER
)`)
	require.NoError(t, err)
	pool := []byte{0x01, 0x02}
	key := []byte{0x03, 0x04}
	_, err = store.writeDB.Exec(
		"INSERT INTO account (credential_tag, staking_key, pool, active) VALUES (0, ?, ?, TRUE)",
		key,
		pool,
	)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(
		"INSERT INTO reward_live_stake (credential_tag, staking_key, utxo_stake, calculation_version) VALUES (0, ?, ?, ?)",
		key,
		"9",
		models.RewardStakeCalculationVersion,
	)
	require.NoError(t, err)
	stakes, delegators, err := store.GetStakeByPools([][]byte{pool}, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(9), stakes[string(pool)])
	require.Equal(t, uint64(1), delegators[string(pool)])
}

// TestGetAccountsByCredentialDeduplicatesRepeatedRefs guards against a
// review finding on PR #3782: the derived-table UNION ALL join emits one row
// per v-row, so a caller passing the same (credential_tag, staking_key) ref
// twice would otherwise join against the same account row twice. Harmless
// for this map-shaped result on its own, but wasted derived-table rows and
// chunk capacity -- GetAccountsByCredential deduplicates refs before
// querying to avoid it.
func TestGetAccountsByCredentialDeduplicatesRepeatedRefs(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	key := bytes.Repeat([]byte{0x07}, 28)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey:    key,
		CredentialTag: 0,
		Active:        true,
	}))

	ref := models.NewStakeCredentialRef(0, key)
	result, err := store.GetAccountsByCredential(
		[]models.StakeCredentialRef{ref, ref, ref},
		false,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Contains(t, result, ref.MapKey())
}

// TestDedupeStakeCredentialRefsDropsRepeats asserts the deduplication
// operation directly: GetAccountsByCredential's map-shaped result stays
// correct with or without deduplication (a duplicate row just overwrites the
// same map key with identical data), so a test that only calls
// GetAccountsByCredential cannot distinguish "deduplicated before querying"
// from "queried with duplicates, then map assignment hid it" -- see cubic's
// review on PR #3782 for exactly this gap in an earlier version of this test.
func TestDedupeStakeCredentialRefsDropsRepeats(t *testing.T) {
	t.Parallel()
	keyA := bytes.Repeat([]byte{0x07}, 28)
	keyB := bytes.Repeat([]byte{0x08}, 28)
	refA := models.NewStakeCredentialRef(0, keyA)
	refB := models.NewStakeCredentialRef(1, keyB)

	deduped := dedupeStakeCredentialRefs(
		[]models.StakeCredentialRef{refA, refB, refA, refA, refB},
	)

	require.Equal(t, []models.StakeCredentialRef{refA, refB}, deduped)
}

func TestRebuildRewardLiveStakeBatchesCredentials(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	const count = 200
	for index := range count {
		account := &models.Account{
			StakingKey:    []byte{0x42, byte(index >> 8), byte(index)},
			CredentialTag: 0,
			Pool:          []byte{0x50, byte(index % 3)},
			AddedSlot:     uint64(index + 1),
			CreatedSlot:   uint64(index + 1),
			Active:        true,
		}
		require.NoError(t, store.ImportAccount(account, nil))
	}
	require.NoError(t, store.RebuildRewardLiveStake(1000, nil))
	var rows int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM reward_live_stake",
	).Scan(&rows))
	require.Equal(t, count, rows)
	var version int64
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT calculation_version FROM reward_live_stake LIMIT 1",
	).Scan(&version))
	require.Equal(t, int64(models.RewardStakeCalculationVersion), version)
}

func TestGetPoolsBatchesAssociations(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	poolKeys := make([]lcommon.PoolKeyHash, 3)
	for index := range 3 {
		poolKey := lcommon.PoolKeyHash{}
		poolKey[0], poolKey[1] = 0x60, byte(index)
		poolKeys[index] = poolKey
		pool := &models.Pool{PoolKeyHash: poolKey.Bytes()}
		registration := &models.PoolRegistration{
			PoolKeyHash: poolKey.Bytes(),
			AddedSlot:   uint64(index + 1),
		}
		require.NoError(t, store.ImportPool(pool, registration, nil))
	}
	got, err := store.GetPools(poolKeys, nil)
	require.NoError(t, err)
	require.Len(t, got, len(poolKeys))
	for _, pool := range got {
		require.Len(t, pool.Registration, 1)
		require.Empty(t, pool.Retirement)
	}
}

func TestImportPoolRegistrationFirstWriteWins(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	poolKey := lcommon.PoolKeyHash{}
	poolKey[0] = 0x70
	firstPool := &models.Pool{
		PoolKeyHash: poolKey.Bytes(),
		Pledge:      1,
	}
	firstRegistration := &models.PoolRegistration{
		PoolKeyHash: poolKey.Bytes(),
		MetadataUrl: "first",
		AddedSlot:   7,
		Pledge:      1,
	}
	require.NoError(t, store.ImportPool(firstPool, firstRegistration, nil))
	secondPool := &models.Pool{
		PoolKeyHash: poolKey.Bytes(),
		Pledge:      9,
	}
	secondRegistration := &models.PoolRegistration{
		PoolKeyHash: poolKey.Bytes(),
		MetadataUrl: "second",
		AddedSlot:   firstRegistration.AddedSlot,
		Pledge:      9,
	}
	require.NoError(t, store.ImportPool(secondPool, secondRegistration, nil))
	require.Equal(t, firstRegistration.ID, secondRegistration.ID)
	loaded, err := store.GetPools([]lcommon.PoolKeyHash{poolKey}, nil)
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	require.Len(t, loaded[0].Registration, 1)
	require.Equal(t, "first", loaded[0].Registration[0].MetadataUrl)
	require.Equal(t, types.Uint64(1), loaded[0].Registration[0].Pledge)
}

// TestImportPoolPersistsLeiosKeyRoundTrip covers ImportPool/GetPools for the
// registered Dijkstra/Leios BLS key columns, including that a later import
// with no key clears a previously stored one rather than leaving it stale.
func TestImportPoolPersistsLeiosKeyRoundTrip(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	poolKey := lcommon.PoolKeyHash{}
	poolKey[0] = 0x71
	pub := bytes.Repeat([]byte{0xAB}, 96)
	proof := bytes.Repeat([]byte{0xCD}, 48)
	pool := &models.Pool{
		PoolKeyHash:             poolKey.Bytes(),
		LeiosKeyPublic:          pub,
		LeiosKeyPossessionProof: proof,
	}
	registration := &models.PoolRegistration{
		PoolKeyHash:             poolKey.Bytes(),
		AddedSlot:               1,
		LeiosKeyPublic:          pub,
		LeiosKeyPossessionProof: proof,
	}
	require.NoError(t, store.ImportPool(pool, registration, nil))

	loaded, err := store.GetPools([]lcommon.PoolKeyHash{poolKey}, nil)
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	assert.Equal(t, pub, loaded[0].LeiosKeyPublic)
	assert.Equal(t, proof, loaded[0].LeiosKeyPossessionProof)

	// A registration with no leios_key (e.g. a rotation to no key, or a
	// key that failed proof-of-possession upstream) must clear the
	// previously stored key rather than leaving it stale.
	rotated := &models.Pool{PoolKeyHash: poolKey.Bytes()}
	rotatedRegistration := &models.PoolRegistration{
		PoolKeyHash: poolKey.Bytes(),
		AddedSlot:   2,
	}
	require.NoError(t, store.ImportPool(rotated, rotatedRegistration, nil))
	loaded, err = store.GetPools([]lcommon.PoolKeyHash{poolKey}, nil)
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	assert.Empty(t, loaded[0].LeiosKeyPublic)
	assert.Empty(t, loaded[0].LeiosKeyPossessionProof)
}

func TestTransactionRejectsUnsafeSavepoint(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	transaction := store.Transaction(t.Context())
	savepointer := transaction.(interface {
		SavePoint(string) error
	})
	require.Error(t, savepointer.SavePoint(`bad"; DROP TABLE item`))
	require.NoError(t, transaction.Rollback())
}

func TestStoreRejectsForeignTransaction(t *testing.T) {
	t.Parallel()
	first := newTestStore(t)
	second := newTestStore(t)
	transaction := first.Transaction(t.Context())
	_, _, err := second.dbFromTxn(transaction)
	require.ErrorContains(t, err, "another store")
	require.NoError(t, transaction.Rollback())
}

func TestStoreReadinessGatesTransactions(t *testing.T) {
	t.Parallel()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	store, err := New(Config{WriteDB: db, Dialect: SQLiteDialect()})
	require.NoError(t, err)
	transaction := store.Transaction(t.Context())
	require.ErrorContains(t, transaction.Commit(), "not ready")
	require.NoError(t, store.Start(context.Background()))
	require.True(t, store.Ready())
	require.NoError(t, store.Close())
	require.False(t, store.Ready())
}

func TestStoreMigrationFailurePreventsReadiness(t *testing.T) {
	t.Parallel()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		MigrationLocker: migrations.NewProcessLocker(),
		Migrations: []migrations.Migration{{
			Version:          1,
			Name:             "broken",
			BackfillRevision: "test",
			SQL: map[string]migrations.SQL{
				"sqlite": {
					Expand: []string{"invalid sql"},
				},
			},
		}},
	})
	require.NoError(t, err)
	err = store.Start(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "failed in expand")
	require.False(t, store.Ready())
	require.NoError(t, store.Close())
}

func TestStoreMaintenanceLifecycle(t *testing.T) {
	t.Parallel()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	started := make(chan struct{})
	var calls atomic.Uint32
	store, err := New(Config{
		WriteDB: db,
		Dialect: SQLiteDialect(),
		Maintenance: func(ctx context.Context) error {
			if calls.Add(1) == 1 {
				close(started)
			}
			<-ctx.Done()
			return ctx.Err()
		},
		MaintenanceInterval: time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("maintenance did not start")
	}
	require.NoError(t, store.Close())
	require.Equal(t, uint32(1), calls.Load())
}

func TestStoreStartsForPostgresDialect(t *testing.T) {
	t.Parallel()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	store, err := New(Config{WriteDB: db, Dialect: PostgresDialect()})
	require.NoError(t, err)
	err = store.Start(context.Background())
	require.NoError(t, err)
	require.True(t, store.Ready())
	require.NoError(t, store.Close())
}
