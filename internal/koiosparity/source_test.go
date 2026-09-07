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

package koiosparity

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// newTestDatabaseSourceDB creates a real, in-process *database.Database
// (Badger blob + SQLite metadata, matching ledger/snapshot's own test
// pattern) for exercising DatabaseSource against the same storage stack a
// live node uses, rather than a bare raw connection.
func newTestDatabaseSourceDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	return db
}

// sourceSQLDB reaches into the metadata store's underlying SQLite file so
// the test can seed rows directly via raw SQL, mirroring
// ledger/snapshot/calculator_test.go's snapshotSQLDB helper. Post-#3054 (the
// metadata store rewrite to sqlc-generated sqlstore), the metadata store
// no longer exposes an ORM handle, so this reaches through
// dbtest.RawSQLiteMetadata instead and wraps it in the same testDB seeding
// helper dingo_db_test.go uses. The name is kept (rather than renamed to
// sourceSQLDB) to minimize churn in call sites that still read naturally as
// "the test's handle for seeding the source's DB".
func sourceSQLDB(t *testing.T, db *database.Database) *testDB {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	return &testDB{db: raw}
}

func TestNewDatabaseSourceRejectsNilDatabase(t *testing.T) {
	_, err := NewDatabaseSource(nil)
	require.Error(t, err)
}

// TestDatabaseSourceGetEpochData exercises commit visibility end to end: it
// commits epoch_summary/reward_ada_pots through the same *database.Database
// a live node would write through, then reads them back via DatabaseSource
// (a separate read-only transaction against the same live database, not a
// second connection) and confirms every field lands exactly as committed.
func TestDatabaseSourceGetEpochData(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	sqlDB := sourceSQLDB(t, db)

	require.NoError(t, sqlDB.Create(&models.EpochSummary{
		Epoch:            5,
		TotalActiveStake: types.Uint64(123_456_789),
		BoundarySlot:     4_320_000,
		SnapshotReady:    true,
	}).Error)
	require.NoError(t, sqlDB.Create(&models.RewardAdaPots{
		Epoch:    5,
		Treasury: types.Uint64(1_000),
		Reserves: types.Uint64(2_000),
		Fees:     types.Uint64(3_000),
		Rewards:  types.Uint64(4_000),
	}).Error)

	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	data, err := source.GetEpochData(context.Background(), 5)
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Equal(t, "123456789", data.TotalActiveStake)
	require.True(t, data.RewardAdaPotsPresent)
	require.Equal(t, "1000", data.Treasury)
	require.Equal(t, "2000", data.Reserves)
	require.Equal(t, "3000", data.Fees)
	require.Equal(t, "4000", data.TotalRewards)
	require.Equal(t, uint64(4_320_000), data.BoundarySlot)
}

// TestDatabaseSourceGetEpochDataMissingOrNotReady covers both "no row at
// all" and "row exists but SnapshotReady is still false" (an in-progress
// write Dingo will repair later) -- both must read back as (nil, nil), never
// an error and never a spurious zero-value comparison.
func TestDatabaseSourceGetEpochDataMissingOrNotReady(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	sqlDB := sourceSQLDB(t, db)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	data, err := source.GetEpochData(context.Background(), 9)
	require.NoError(t, err)
	require.Nil(t, data)

	require.NoError(t, sqlDB.Create(&models.EpochSummary{
		Epoch:         9,
		SnapshotReady: false,
	}).Error)
	data, err = source.GetEpochData(context.Background(), 9)
	require.NoError(t, err)
	require.Nil(t, data)
}

// TestDatabaseSourceGetEpochDataRewardAdaPotsAbsent guards the
// RewardAdaPotsPresent distinction documented on DingoEpochData: a
// bootstrap-imported epoch can have epoch_summary.SnapshotReady=true with no
// reward_ada_pots row at all, which must surface as RewardAdaPotsPresent ==
// false (a real dingo_db_missing mismatch upstream), not as legitimately
// empty/zero pots.
func TestDatabaseSourceGetEpochDataRewardAdaPotsAbsent(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	sqlDB := sourceSQLDB(t, db)
	require.NoError(t, sqlDB.Create(&models.EpochSummary{
		Epoch:         3,
		SnapshotReady: true,
	}).Error)

	source, err := NewDatabaseSource(db)
	require.NoError(t, err)
	data, err := source.GetEpochData(context.Background(), 3)
	require.NoError(t, err)
	require.NotNil(t, data)
	require.False(t, data.RewardAdaPotsPresent)
	require.Empty(t, data.Fees)
	require.Empty(t, data.Treasury)
}

// TestDatabaseSourceGetPoolEpochDataMap mirrors DingoDB's own
// GetPoolEpochDataMap semantics: DelegatedStake/DelegatorCount/FixedCost/
// Margin come from stakeEpoch, BlocksProduced from paramEpoch, and
// MemberRewardTotal from stakeEpoch's reward_pool_output -- each field
// group's *Present flag reflects only whether its own row existed.
func TestDatabaseSourceGetPoolEpochDataMap(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	sqlDB := sourceSQLDB(t, db)
	poolKeyHash := []byte("POOLKEYHASH-28-BYTES-LONG!!!")
	require.Len(t, poolKeyHash, 28)

	require.NoError(t, sqlDB.Create(&models.RewardPoolInput{
		Epoch:          10,
		PoolKeyHash:    poolKeyHash,
		DelegatedStake: types.Uint64(500_000),
		DelegatorCount: 3,
		Cost:           types.Uint64(340_000_000),
	}).Error)
	blocksProduced := uint64(7)
	// The param-epoch row owns blocks_produced only; its cost belongs to a
	// later epoch and must not surface (dingo #3484).
	require.NoError(t, sqlDB.Create(&models.RewardPoolInput{
		Epoch:          12,
		PoolKeyHash:    poolKeyHash,
		BlocksProduced: &blocksProduced,
		Cost:           types.Uint64(999_000_000),
	}).Error)
	require.NoError(t, sqlDB.Create(&models.RewardPoolOutput{
		Epoch:             10,
		PoolKeyHash:       poolKeyHash,
		MemberRewardTotal: types.Uint64(999_999),
	}).Error)

	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	m, err := source.GetPoolEpochDataMap(context.Background(), 10, 12)
	require.NoError(t, err)
	require.Len(t, m, 1)

	var key string
	for k := range m {
		key = k
	}
	entry := m[key]
	require.NotNil(t, entry)
	require.True(t, entry.StakePresent)
	require.Equal(t, "500000", entry.DelegatedStake)
	require.Equal(t, uint64(3), entry.DelegatorCount)
	require.True(t, entry.ParamsPresent)
	require.Equal(t, uint64(7), entry.BlocksProduced)
	require.Equal(t, "340000000", entry.FixedCost)
	require.True(t, entry.MemberRewardPresent)
	require.Equal(t, "999999", entry.MemberRewardTotal)
}

// TestDatabaseSourceGetPoolEpochDataMapPartialPresence covers the case where
// only stakeEpoch's RewardPoolInput row exists for a pool (no paramEpoch row,
// no RewardPoolOutput row yet) -- e.g. a pool captured at the stake snapshot
// whose reward_pool_output hasn't been computed and written yet. Per
// dingo_db.go's doc comment (~lines 89-134) and the fallback stub-
// construction logic in source.go (~lines 211-241), each field group's
// *Present flag must reflect only whether its own row actually exists, not
// whether any row exists for the pool at all.
func TestDatabaseSourceGetPoolEpochDataMapPartialPresence(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	sqlDB := sourceSQLDB(t, db)
	poolKeyHash := []byte("POOLKEYHASH-28-BYTES-LONG!!!")
	require.Len(t, poolKeyHash, 28)

	require.NoError(t, sqlDB.Create(&models.RewardPoolInput{
		Epoch:          10,
		PoolKeyHash:    poolKeyHash,
		DelegatedStake: types.Uint64(500_000),
		DelegatorCount: 3,
	}).Error)

	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	m, err := source.GetPoolEpochDataMap(context.Background(), 10, 12)
	require.NoError(t, err)
	require.Len(t, m, 1)

	var key string
	for k := range m {
		key = k
	}
	entry := m[key]
	require.NotNil(t, entry)
	require.True(t, entry.StakePresent)
	require.Equal(t, "500000", entry.DelegatedStake)
	require.Equal(t, uint64(3), entry.DelegatorCount)
	require.False(t, entry.ParamsPresent)
	require.False(t, entry.MemberRewardPresent)
}

func TestDatabaseSourceGetLatestEpoch(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	_, err = source.GetLatestEpoch(context.Background())
	require.Error(t, err, "no epoch_summary rows yet")

	sqlDB := sourceSQLDB(t, db)
	require.NoError(t, sqlDB.Create(&models.EpochSummary{Epoch: 2}).Error)
	require.NoError(t, sqlDB.Create(&models.EpochSummary{Epoch: 7}).Error)
	require.NoError(t, sqlDB.Create(&models.EpochSummary{Epoch: 4}).Error)

	latest, err := source.GetLatestEpoch(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(7), latest)
}

func TestDatabaseSourceGetRewardAccountOutputs(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	sqlDB := sourceSQLDB(t, db)
	stakingKey := []byte("STAKING-KEY-28-BYTES-LONG!!!")
	require.Len(t, stakingKey, 28)
	poolKeyHash := []byte("POOLKEYHASH-28-BYTES-LONG!!!")
	require.NoError(t, sqlDB.Create(&models.RewardAccountOutput{
		Epoch:       6,
		StakingKey:  stakingKey,
		PoolKeyHash: poolKeyHash,
		RewardType:  "member",
		Amount:      types.Uint64(42),
		Spendable:   true,
	}).Error)

	source, err := NewDatabaseSource(db)
	require.NoError(t, err)
	rows, err := source.GetRewardAccountOutputs(context.Background(), 6)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, types.Uint64(42), rows[0].Amount)
	require.True(t, rows[0].Spendable)
}

// TestDatabaseSourceCoreModePruningTiming demonstrates the retention-window
// property DatabaseSource's doc comment relies on for reward_account_output
// (the table #3097's per-account parity check will read): it remains fully
// readable through DatabaseSource until something actually deletes it
// (ledger/snapshot/rotation.go's cleanupOldSnapshots calls
// MetadataStore.DeleteRewardStateBeforeEpoch on a rolling window in core
// storage mode; API mode retains it without bound instead — see
// rotation.go's doc comment). Before that deletion, the data is present;
// simulating exactly the deletion cleanupOldSnapshots performs makes the
// epoch read back as "not present" afterward — the same signal
// GetRewardAccountOutputs already uses for "not yet computed", not an
// error. This is why the in-process observer must process a newly closed
// epoch promptly (within that multi-epoch window) rather than relying on
// some same-transaction race with the write that produced the data: reading
// late is indistinguishable from reading an epoch that was simply never
// computed.
func TestDatabaseSourceCoreModePruningTiming(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	sqlDB := sourceSQLDB(t, db)
	poolKeyHash := []byte("POOLKEYHASH-28-BYTES-LONG!!!")
	stakingKey := []byte("STAKING-KEY-28-BYTES-LONG!!!")

	const epoch = 20
	require.NoError(t, sqlDB.Create(&models.RewardAccountOutput{
		Epoch:       epoch,
		StakingKey:  stakingKey,
		PoolKeyHash: poolKeyHash,
		RewardType:  "member",
		Amount:      types.Uint64(1),
		Spendable:   true,
	}).Error)

	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	// Before pruning: fully readable.
	accounts, err := source.GetRewardAccountOutputs(context.Background(), epoch)
	require.NoError(t, err)
	require.Len(t, accounts, 1)

	// Simulate the same core-mode retention cleanup
	// ledger/snapshot/rotation.go's cleanupOldSnapshots performs once epoch
	// falls out of the rolling window (deleteBeforeEpoch = currentEpoch-3).
	txn := db.Transaction(true)
	require.NoError(
		t,
		db.Metadata().DeleteRewardStateBeforeEpoch(epoch+1, txn.Metadata()),
	)
	require.NoError(t, txn.Commit())

	// After pruning: reads back as "not present", not an error -- the same
	// signal as an epoch whose reward calculation never ran.
	accounts, err = source.GetRewardAccountOutputs(context.Background(), epoch)
	require.NoError(t, err)
	require.Empty(t, accounts)
}

// TestDatabaseSourceGetPoolEpochDataMapTracksChangingPoolParams is the
// in-process counterpart of dingo_db_test.go's
// TestGetPoolEpochDataMapTracksChangingPoolParams. Both implementations of
// RewardParitySource must resolve the same field-to-epoch mapping, and only
// this one runs inside the node — the standalone CLI reads SQLite directly.
//
// The two drifted once: dingo #3484 was fixed in DingoDB while
// DatabaseSource kept reading Margin/FixedCost from the param epoch, so the
// unit tests passed while a live preview replay still failed at epoch 13.
func TestDatabaseSourceGetPoolEpochDataMapTracksChangingPoolParams(
	t *testing.T,
) {
	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	const (
		koiosEpoch = uint64(13)
		stakeEpoch = koiosEpoch - 1
		paramEpoch = koiosEpoch + 1
	)
	poolHash := testPoolKeyHash(t, 0x11)
	blocksAtParamEpoch := uint64(10)
	meta := db.Metadata()

	// Stake epoch (12): the parameters in force for Koios epoch 13.
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{{
		Epoch:          stakeEpoch,
		PoolKeyHash:    poolHash,
		DelegatedStake: types.Uint64(5_000_000),
		DelegatorCount: 1,
		Cost:           types.Uint64(411_000_000),
		Margin:         &types.Rat{Rat: big.NewRat(1, 20)},
	}}, nil))

	// Param epoch (14): owns blocks_produced for epoch 13. Its own cost and
	// margin belong to a later epoch and must not surface.
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{{
		Epoch:          paramEpoch,
		PoolKeyHash:    poolHash,
		Cost:           types.Uint64(412_000_000),
		Margin:         &types.Rat{Rat: big.NewRat(1, 25)},
		BlocksProduced: &blocksAtParamEpoch,
	}}, nil))

	m, err := source.GetPoolEpochDataMap(
		context.Background(),
		stakeEpoch,
		paramEpoch,
	)
	require.NoError(t, err)

	data, ok := m[hex.EncodeToString(poolHash)]
	require.True(t, ok)
	require.Equal(
		t,
		"411000000",
		data.FixedCost,
		"the cost in force for epoch %d is on the stake-epoch row",
		koiosEpoch,
	)
	require.Equal(
		t,
		"1/20",
		data.Margin,
		"the margin in force for epoch %d is on the stake-epoch row",
		koiosEpoch,
	)
	require.Equal(
		t,
		blocksAtParamEpoch,
		data.BlocksProduced,
		"blocks_produced still comes from the param epoch",
	)
}

// TestDatabaseSourceReportsRewardsPending covers the in-process source half
// of dingo #3852. The live observer reads the same committed tip and reward
// output boundary as the standalone checker; before the boundary, a
// spendable-sum difference is provisional, while at the boundary it is real.
func TestDatabaseSourceReportsRewardsPending(t *testing.T) {
	const (
		stakeEpoch   = uint64(9)
		paramEpoch   = uint64(10)
		boundarySlot = uint64(1_000_000)
	)
	db := newTestDatabaseSourceDB(t)
	sqlDB := sourceSQLDB(t, db)
	poolHash := testPoolKeyHash(t, 0x42)

	require.NoError(t, sqlDB.Exec(
		`INSERT INTO reward_pool_input
		 (pool_key_hash, epoch, pledge, delegated_stake, owner_stake,
		  cost, delegator_count, captured_slot, boundary_slot)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		poolHash, stakeEpoch, "0", "1000", "0", "0", 1, 0, 0).Error)
	require.NoError(t, sqlDB.Exec(
		`INSERT INTO reward_pool_output
			 (pool_key_hash, epoch, optimal_reward, total_reward, leader_reward,
			  member_reward_total, owner_stake, undistributed, unspendable,
			  captured_slot, boundary_slot)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		poolHash, stakeEpoch, "0", "0", "0", "4006269", "0", "0", "1857",
		0, boundarySlot,
	).Error)
	require.NoError(t, sqlDB.Exec(
		`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
		[]byte{0x01}, boundarySlot-1, 1,
	).Error)

	source, err := NewDatabaseSource(db)
	require.NoError(t, err)
	dataMap, err := source.GetPoolEpochDataMap(
		context.Background(), stakeEpoch, paramEpoch,
	)
	require.NoError(t, err)
	data, ok := dataMap[hex.EncodeToString(poolHash)]
	require.True(t, ok, "pool missing from the map")
	require.True(t, data.RewardsPending,
		"before the boundary, the spendable flags remain provisional")

	require.NoError(t, sqlDB.Exec(
		`UPDATE tip SET slot = ?`, boundarySlot,
	).Error)
	dataMap, err = source.GetPoolEpochDataMap(
		context.Background(), stakeEpoch, paramEpoch,
	)
	require.NoError(t, err)
	data, ok = dataMap[hex.EncodeToString(poolHash)]
	require.True(t, ok, "pool missing from the map after tip update")
	require.False(t, data.RewardsPending,
		"at the boundary, the spendable flags are final")
}

// TestDatabaseSourceGetPoolsRetiredByEpoch proves the in-process source
// resolves departure the same way DingoDB's raw-SQL twin does, including the
// case that makes "a retirement certificate exists" the wrong predicate: a
// registration filed after the retirement puts the pool back. Both halves are
// asserted from one seeding, so a query that simply returned every pool with
// a retirement row would fail on the re-registered pool.
func TestDatabaseSourceGetPoolsRetiredByEpoch(t *testing.T) {
	const (
		queryEpoch   = uint64(7)
		boundarySlot = uint64(1_000)
	)
	db := newTestDatabaseSourceDB(t)
	sqlDB := sourceSQLDB(t, db)

	departed := testPoolKeyHash(t, 0x21)
	reregistered := testPoolKeyHash(t, 0x22)
	retiringLater := testPoolKeyHash(t, 0x23)

	seed := func(keyHash []byte, regSlots []uint64, retSlot, retEpoch uint64) {
		t.Helper()
		raw, err := sqlDB.DB()
		require.NoError(t, err)
		res, err := raw.Exec(
			`INSERT INTO pool (pool_key_hash) VALUES (?)`,
			keyHash,
		)
		require.NoError(t, err)
		poolID, err := res.LastInsertId()
		require.NoError(t, err)
		for _, slot := range regSlots {
			require.NoError(t, sqlDB.Exec(`
INSERT INTO pool_registration (pool_id, pool_key_hash, added_slot)
VALUES (?, ?, ?)`, poolID, keyHash, slot).Error)
		}
		require.NoError(t, sqlDB.Exec(`
INSERT INTO pool_retirement (pool_id, pool_key_hash, epoch, added_slot)
VALUES (?, ?, ?, ?)`, poolID, keyHash, retEpoch, retSlot).Error)
	}
	seed(departed, []uint64{100}, 200, queryEpoch-2)
	seed(reregistered, []uint64{100, 300}, 200, queryEpoch-2)
	seed(retiringLater, []uint64{100}, 200, queryEpoch+1)

	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	retired, err := source.GetPoolsRetiredByEpoch(
		context.Background(),
		queryEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		map[string]struct{}{hex.EncodeToString(departed): {}},
		retired,
	)
}

// retiredParityCert is one seeded certificate. blockIndex is the
// transaction's index within its block and certIndex the certificate's index
// within that transaction, which together break added_slot ties exactly the
// way a registration and a retirement filed in one block do on chain.
//
// reconcile writes the row the way Store.RetirePools does — certificate_id = 0
// with no certs/transaction row behind it — so such a row has no
// block_index/cert_index of its own and is ordered by the synthetic_ret key
// instead. Retirements only.
type retiredParityCert struct {
	slot       uint64
	blockIndex uint64
	certIndex  uint64
	epoch      uint64 // retirements only
	reconcile  bool   // retirements only
}

// retiredParitySeeder seeds pool certificate histories into the real,
// migrated metadata schema. Every certificate-backed registration and
// retirement gets a transaction/certs row so added_slot ties are broken on
// block_index then cert_index, which is the only way to exercise the same-slot
// half of the cancellation rule.
type retiredParitySeeder struct {
	t   *testing.T
	raw *sql.DB
	seq uint64
}

func (s *retiredParitySeeder) cert(slot, blockIndex, certIndex uint64) int64 {
	s.t.Helper()
	s.seq++
	hash := make([]byte, 32)
	binary.BigEndian.PutUint64(hash, s.seq)
	res, err := s.raw.Exec(
		`INSERT INTO "transaction" (hash, slot, block_index) VALUES (?, ?, ?)`,
		hash, slot, blockIndex,
	)
	require.NoError(s.t, err)
	txID, err := res.LastInsertId()
	require.NoError(s.t, err)
	res, err = s.raw.Exec(
		`INSERT INTO certs (transaction_id, slot, cert_index) VALUES (?, ?, ?)`,
		txID, slot, certIndex,
	)
	require.NoError(s.t, err)
	certID, err := res.LastInsertId()
	require.NoError(s.t, err)
	return certID
}

// seed writes one pool with the given registration and retirement history.
func (s *retiredParitySeeder) seed(
	keyHash []byte,
	regs []retiredParityCert,
	rets []retiredParityCert,
) {
	s.t.Helper()
	res, err := s.raw.Exec(
		`INSERT INTO pool (pool_key_hash) VALUES (?)`,
		keyHash,
	)
	require.NoError(s.t, err)
	poolID, err := res.LastInsertId()
	require.NoError(s.t, err)
	for _, reg := range regs {
		certID := s.cert(reg.slot, reg.blockIndex, reg.certIndex)
		_, err := s.raw.Exec(`
INSERT INTO pool_registration (
    pool_id, pool_key_hash, certificate_id, added_slot, deposit_amount
) VALUES (?, ?, ?, ?, '500')`,
			poolID, keyHash, certID, reg.slot,
		)
		require.NoError(s.t, err)
	}
	for _, ret := range rets {
		var certID int64
		if !ret.reconcile {
			certID = s.cert(ret.slot, ret.blockIndex, ret.certIndex)
		}
		_, err := s.raw.Exec(`
INSERT INTO pool_retirement (
    pool_id, pool_key_hash, certificate_id, epoch, added_slot
) VALUES (?, ?, ?, ?, ?)`,
			poolID, keyHash, certID, ret.epoch, ret.slot,
		)
		require.NoError(s.t, err)
	}
}

// TestGetPoolsRetiredByEpochImplementationsAgree runs both RewardParitySource
// implementations against one physical metadata.sqlite on the real migrated
// schema: DatabaseSource through MetadataStore.GetPoolKeyHashesRetiredByEpoch,
// and DingoDB through its own copy of that SQL on a read-only connection to
// the same file.
//
// Two things are asserted, and both are needed. Equality between the
// implementations is what pins them against drift — nothing else in the tree
// requires DingoDB's hand-written SQL to keep matching the store query, and
// the end-to-end checks exercise only DingoDB while
// TestDatabaseSourceGetPoolsRetiredByEpoch exercises only the store. Equality
// against an explicit expected set is what stops two identically-broken
// implementations from agreeing with each other and passing.
//
// The fixture is built so that every clause of the predicate is load-bearing
// for at least one pool: the `<=` comparison, the `added_slot < boundarySlot`
// visibility cut, the cancellation guard, both directions of the same-slot
// block_index and cert_index tie-breaks, latest_ret's own cert_index ordering,
// and the synthetic_ret key that ranks a reconcile retirement
// (`certificate_id = 0`) ahead of certificate-backed rows in its own slot and
// exempts it from cancellation. Neutralising any one of them in either
// implementation fails this test.
//
// The reconcile pools matter because the two implementations diverged on
// exactly them: DingoDB carried synthetic_ret while the store query did not,
// so a node bootstrapped from a ledger-state snapshot — where ImportPool and
// RetirePools write a registration and a certificate_id = 0 retirement in one
// slot — would have the standalone CLI and the in-process observer classify
// the same pool differently.
func TestGetPoolsRetiredByEpochImplementationsAgree(t *testing.T) {
	const (
		queryEpoch   = uint64(7)
		boundarySlot = uint64(1_000)
	)
	dir := t.TempDir()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: dir})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	seeder := &retiredParitySeeder{t: t, raw: raw}

	var (
		departedEarlier      = testPoolKeyHash(t, 0x31)
		departedAtEpoch      = testPoolKeyHash(t, 0x32)
		retiringLater        = testPoolKeyHash(t, 0x33)
		reregistered         = testPoolKeyHash(t, 0x34)
		reregisteredSameSlot = testPoolKeyHash(t, 0x35)
		retiredSameSlotAfter = testPoolKeyHash(t, 0x36)
		retiredAfterBoundary = testPoolKeyHash(t, 0x37)
		neverRetired         = testPoolKeyHash(t, 0x38)
		reregisteredAtBound  = testPoolKeyHash(t, 0x39)
		retiredLaterTx       = testPoolKeyHash(t, 0x3A)
		reregisteredLaterTx  = testPoolKeyHash(t, 0x3B)
		retiredTwiceSameSlot = testPoolKeyHash(t, 0x3C)
		reconcileSameSlot    = testPoolKeyHash(t, 0x3D)
		reconcileOverCertRet = testPoolKeyHash(t, 0x3E)
	)
	// Retired several epochs ago and still departed — the `<=` clause.
	seeder.seed(
		departedEarlier,
		[]retiredParityCert{{slot: 100}},
		[]retiredParityCert{{slot: 200, epoch: 5}},
	)
	seeder.seed(
		departedAtEpoch,
		[]retiredParityCert{{slot: 100}},
		[]retiredParityCert{{slot: 200, epoch: queryEpoch}},
	)
	seeder.seed(
		retiringLater,
		[]retiredParityCert{{slot: 100}},
		[]retiredParityCert{{slot: 200, epoch: 9}},
	)
	// The cancellation guard: a later registration puts the pool back.
	seeder.seed(
		reregistered,
		[]retiredParityCert{{slot: 100}, {slot: 300}},
		[]retiredParityCert{{slot: 200, epoch: 5}},
	)
	// Same slot and same transaction, registration ordered after the
	// retirement by cert_index.
	seeder.seed(
		reregisteredSameSlot,
		[]retiredParityCert{{slot: 100}, {slot: 200, certIndex: 2}},
		[]retiredParityCert{{slot: 200, certIndex: 1, epoch: 5}},
	)
	// Same slot and same transaction, retirement ordered after the
	// registration.
	seeder.seed(
		retiredSameSlotAfter,
		[]retiredParityCert{{slot: 100}, {slot: 200, certIndex: 1}},
		[]retiredParityCert{{slot: 200, certIndex: 2, epoch: 5}},
	)
	// Not yet visible at the boundary.
	seeder.seed(
		retiredAfterBoundary,
		[]retiredParityCert{{slot: 100}},
		[]retiredParityCert{{slot: boundarySlot, epoch: 5}},
	)
	seeder.seed(neverRetired, []retiredParityCert{{slot: 100}}, nil)
	// The re-registration lands exactly on the boundary slot, so it is not
	// yet visible and cannot cancel: the pool is still departed. This is the
	// only pool for which the registration-side `added_slot < boundarySlot`
	// cut is load-bearing — every other cancellation here is decided by the
	// ordering guard instead.
	seeder.seed(
		reregisteredAtBound,
		[]retiredParityCert{{slot: 100}, {slot: boundarySlot}},
		[]retiredParityCert{{slot: 200, epoch: 5}},
	)
	// Same slot, different transactions: the retirement is in the later
	// transaction of the block, so it stands. cert_index cannot decide this
	// pair — both are 0 — so only the block_index comparison can.
	seeder.seed(
		retiredLaterTx,
		[]retiredParityCert{{slot: 100}, {slot: 200, blockIndex: 1}},
		[]retiredParityCert{{slot: 200, blockIndex: 2, epoch: 5}},
	)
	// The mirror image: the registration is in the later transaction, so it
	// cancels. Together these two pin both directions of the block_index
	// comparison, which no other pool here exercises.
	seeder.seed(
		reregisteredLaterTx,
		[]retiredParityCert{{slot: 100}, {slot: 200, blockIndex: 2}},
		[]retiredParityCert{{slot: 200, blockIndex: 1, epoch: 5}},
	)
	// Two retirement certificates in one transaction naming different
	// effective epochs. latest_ret must pick the higher cert_index, so the
	// pool is departed by 5 rather than still retiring at 9. The epoch-9 row
	// is seeded first so insertion order disagrees with cert_index order,
	// which is what makes latest_ret's ORDER BY key load-bearing rather than
	// incidentally satisfied by the scan order.
	seeder.seed(
		retiredTwiceSameSlot,
		[]retiredParityCert{{slot: 100}},
		[]retiredParityCert{
			{slot: 200, certIndex: 1, epoch: 9},
			{slot: 200, certIndex: 2, epoch: 5},
		},
	)
	// A reconcile retirement (certificate_id = 0) in a certificate-backed
	// registration's own slot. It has no certs row, so both its indices are
	// zero and it would lose the tie-break without the synthetic_ret
	// exemption. This is the shape ledgerstate's snapshot import writes, so
	// it is on every node bootstrapped from a ledger-state snapshot.
	seeder.seed(
		reconcileSameSlot,
		[]retiredParityCert{{slot: 100}, {slot: 200, certIndex: 1}},
		[]retiredParityCert{{slot: 200, epoch: 5, reconcile: true}},
	)
	// A reconcile retirement sharing a slot with a certificate-backed
	// retirement effective after the queried epoch. The reconcile row is the
	// ledger state's answer and must win the ROW_NUMBER ordering despite its
	// zero indices.
	seeder.seed(
		reconcileOverCertRet,
		[]retiredParityCert{{slot: 100}},
		[]retiredParityCert{
			{slot: 200, certIndex: 3, epoch: 9},
			{slot: 200, epoch: 5, reconcile: true},
		},
	)

	want := map[string]struct{}{
		hex.EncodeToString(departedEarlier):      {},
		hex.EncodeToString(departedAtEpoch):      {},
		hex.EncodeToString(retiredSameSlotAfter): {},
		hex.EncodeToString(reregisteredAtBound):  {},
		hex.EncodeToString(retiredLaterTx):       {},
		hex.EncodeToString(retiredTwiceSameSlot): {},
		hex.EncodeToString(reconcileSameSlot):    {},
		hex.EncodeToString(reconcileOverCertRet): {},
	}

	source, err := NewDatabaseSource(db)
	require.NoError(t, err)
	fromStore, err := source.GetPoolsRetiredByEpoch(
		context.Background(),
		queryEpoch,
		boundarySlot,
	)
	require.NoError(t, err)

	dingoDB, err := OpenDingoDB(DingoDBConfig{Plugin: "sqlite", DataDir: dir})
	require.NoError(t, err)
	defer dingoDB.Close() //nolint:errcheck
	fromDingoDB, err := dingoDB.GetPoolsRetiredByEpoch(
		context.Background(),
		queryEpoch,
		boundarySlot,
	)
	require.NoError(t, err)

	require.Equal(
		t,
		want,
		fromStore,
		"DatabaseSource/MetadataStore resolved the wrong departure set",
	)
	require.Equal(
		t,
		want,
		fromDingoDB,
		"DingoDB resolved the wrong departure set",
	)
	require.Equal(
		t,
		fromStore,
		fromDingoDB,
		"the two RewardParitySource implementations must not drift",
	)
}
