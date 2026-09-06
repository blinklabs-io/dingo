package koiosparity

import (
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/blinklabs-io/dingo/database/models"
	_ "github.com/glebarez/go-sqlite"
)

type testDB struct{ db *sql.DB }
type testResult struct{ Error error }

func (d *testDB) DB() (*sql.DB, error) { return d.db, nil }
func (d *testDB) Close() error         { return d.db.Close() }

func (d *testDB) Create(value any) testResult {
	var query string
	var args []any
	switch v := value.(type) {
	case *models.EpochSummary:
		query = `INSERT INTO epoch_summary (epoch,total_active_stake,total_pool_count,total_delegators,epoch_nonce,boundary_slot,snapshot_ready) VALUES (?,?,?,?,?,?,?)`
		args = []any{v.Epoch, v.TotalActiveStake, v.TotalPoolCount, v.TotalDelegators, v.EpochNonce, v.BoundarySlot, v.SnapshotReady}
	case *models.RewardAdaPots:
		query = `INSERT INTO reward_ada_pots (epoch,treasury,reserves,fees,rewards,captured_slot) VALUES (?,?,?,?,?,?)`
		args = []any{v.Epoch, v.Treasury, v.Reserves, v.Fees, v.Rewards, v.CapturedSlot}
	case *models.RewardPoolInput:
		query = `INSERT INTO reward_pool_input (margin,pool_key_hash,reward_account,blocks_produced,total_blocks_in_epoch,epoch,pledge,delegated_stake,owner_stake,cost,delegator_count,reward_account_credential_tag,captured_slot,boundary_slot) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
		args = []any{v.Margin, v.PoolKeyHash, v.RewardAccount, v.BlocksProduced, v.TotalBlocksInEpoch, v.Epoch, v.Pledge, v.DelegatedStake, v.OwnerStake, v.Cost, v.DelegatorCount, v.RewardAccountCredentialTag, v.CapturedSlot, v.BoundarySlot}
	case *models.RewardPoolOutput:
		query = `INSERT INTO reward_pool_output (apparent_performance,pool_key_hash,epoch,optimal_reward,total_reward,leader_reward,member_reward_total,owner_stake,undistributed,unspendable,captured_slot,boundary_slot) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`
		args = []any{v.ApparentPerformance, v.PoolKeyHash, v.Epoch, v.OptimalReward, v.TotalReward, v.LeaderReward, v.MemberRewardTotal, v.OwnerStake, v.Undistributed, v.Unspendable, v.CapturedSlot, v.BoundarySlot}
	case *models.RewardAccountOutput:
		query = `INSERT INTO reward_account_output (staking_key,pool_key_hash,reward_type,epoch,credential_tag,amount,spendable,guarded,captured_slot,boundary_slot) VALUES (?,?,?,?,?,?,?,?,?,?)`
		args = []any{v.StakingKey, v.PoolKeyHash, v.RewardType, v.Epoch, v.CredentialTag, v.Amount, v.Spendable, v.Guarded, v.CapturedSlot, v.BoundarySlot}
	case *models.PoolStakeSnapshot:
		query = `INSERT INTO pool_stake_snapshot (epoch,snapshot_type,pool_key_hash,total_stake,stake_denominator,delegator_count,captured_slot) VALUES (?,?,?,?,?,?,?)`
		args = []any{v.Epoch, v.SnapshotType, v.PoolKeyHash, v.TotalStake, v.StakeDenominator, v.DelegatorCount, v.CapturedSlot}
	default:
		return testResult{Error: fmt.Errorf("unsupported test row %T", value)}
	}
	_, err := d.db.Exec(query, args...)
	return testResult{Error: err}
}

// Exec runs an arbitrary write statement against the underlying database,
// for seeding/mutation patterns testDB.Create's fixed type switch doesn't
// cover (e.g. a targeted UPDATE simulating a rollback+replay of a single
// column).
func (d *testDB) Exec(query string, args ...any) testResult {
	_, err := d.db.Exec(query, args...)
	return testResult{Error: err}
}

func openTestSQLDB(t testingT, dir string, includePools bool) *testDB {
	t.Helper()
	path := filepath.Join(dir, "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	for _, stmt := range testSchema(includePools) {
		if _, err := db.Exec(stmt); err != nil {
			_ = db.Close()
			t.Fatalf("create sqlite schema: %v", err)
		}
	}
	t.Cleanup(func() { _ = db.Close() })
	return &testDB{db: db}
}

type testingT interface {
	Helper()
	Fatalf(string, ...any)
	Cleanup(func())
}

func testSchema(includePools bool) []string {
	ret := []string{
		`CREATE TABLE pool_stake_snapshot (id INTEGER PRIMARY KEY AUTOINCREMENT, epoch INTEGER NOT NULL, snapshot_type TEXT NOT NULL, pool_key_hash BLOB NOT NULL, total_stake TEXT NOT NULL, stake_denominator TEXT NOT NULL DEFAULT '0', delegator_count INTEGER NOT NULL DEFAULT 0, captured_slot INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE epoch_summary (id INTEGER PRIMARY KEY AUTOINCREMENT, epoch INTEGER NOT NULL UNIQUE, total_active_stake TEXT NOT NULL, total_pool_count INTEGER NOT NULL DEFAULT 0, total_delegators INTEGER NOT NULL DEFAULT 0, epoch_nonce BLOB, boundary_slot INTEGER NOT NULL DEFAULT 0, snapshot_ready NUMERIC NOT NULL DEFAULT 0)`,
		`CREATE TABLE reward_ada_pots (id INTEGER PRIMARY KEY AUTOINCREMENT, epoch INTEGER NOT NULL UNIQUE, treasury TEXT NOT NULL, reserves TEXT NOT NULL, fees TEXT NOT NULL, rewards TEXT NOT NULL, captured_slot INTEGER NOT NULL DEFAULT 0)`,
		// reward_account_output is created unconditionally (not gated behind
		// includePools) — it's a cheap CREATE TABLE and is independent of the
		// per-pool reward tables; TestDingoDBGetRewardAccountOutputs seeds no
		// pool rows at all, so gating this behind includePools would force
		// that test to opt into unrelated schema it doesn't use.
		// epoch and pparams mirror Dingo's real metadata schema (column
		// names and types copied from a synced preview metadata.sqlite) so
		// GetProtocolParams is exercised against the same shape it reads in
		// production, including the nullable integer columns.
		`CREATE TABLE epoch (nonce BLOB, evolving_nonce BLOB, candidate_nonce BLOB, last_epoch_block_nonce BLOB, id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_id INTEGER, start_slot INTEGER, era_id INTEGER, slot_length INTEGER, length_in_slots INTEGER)`,
		`CREATE UNIQUE INDEX idx_epoch_epoch_id ON epoch(epoch_id)`,
		`CREATE TABLE pparams (cbor BLOB, id INTEGER PRIMARY KEY AUTOINCREMENT, added_slot INTEGER, epoch INTEGER, era_id INTEGER)`,
		`CREATE TABLE reward_account_output (staking_key BLOB NOT NULL, pool_key_hash BLOB NOT NULL, reward_type TEXT NOT NULL, id INTEGER PRIMARY KEY, epoch INTEGER NOT NULL, credential_tag INTEGER NOT NULL DEFAULT 0, amount TEXT NOT NULL, spendable BOOLEAN NOT NULL, guarded BOOLEAN NOT NULL DEFAULT FALSE, captured_slot INTEGER NOT NULL, boundary_slot INTEGER NOT NULL, UNIQUE (epoch, credential_tag, staking_key, pool_key_hash, reward_type))`,
		// The pool certificate tables back DingoDB.GetPoolsRetiredByEpoch.
		// Created unconditionally for the same reason as
		// reward_account_output: they are cheap, independent of the reward
		// tables, and a checkEpoch run reads them for every epoch whether
		// or not the test seeds pool reward rows.
		`CREATE TABLE pool (id INTEGER PRIMARY KEY AUTOINCREMENT, pool_key_hash BLOB NOT NULL UNIQUE)`,
		`CREATE TABLE pool_registration (id INTEGER PRIMARY KEY AUTOINCREMENT, pool_id INTEGER NOT NULL, pool_key_hash BLOB NOT NULL, certificate_id INTEGER, added_slot INTEGER NOT NULL)`,
		`CREATE TABLE pool_retirement (id INTEGER PRIMARY KEY AUTOINCREMENT, pool_id INTEGER NOT NULL, pool_key_hash BLOB NOT NULL, certificate_id INTEGER, epoch INTEGER NOT NULL, added_slot INTEGER NOT NULL)`,
		`CREATE TABLE "transaction" (id INTEGER PRIMARY KEY AUTOINCREMENT, hash BLOB, slot INTEGER, block_index INTEGER)`,
		`CREATE TABLE certs (id INTEGER PRIMARY KEY AUTOINCREMENT, transaction_id INTEGER, slot INTEGER, cert_index INTEGER)`,
	}
	if includePools {
		ret = append(
			ret,
			`CREATE TABLE reward_pool_input (margin TEXT, pool_key_hash BLOB NOT NULL, reward_account BLOB, blocks_produced INTEGER, total_blocks_in_epoch INTEGER, id INTEGER PRIMARY KEY AUTOINCREMENT, epoch INTEGER NOT NULL, pledge TEXT NOT NULL DEFAULT '0', delegated_stake TEXT NOT NULL DEFAULT '0', owner_stake TEXT NOT NULL DEFAULT '0', cost TEXT NOT NULL DEFAULT '0', delegator_count INTEGER NOT NULL DEFAULT 0, reward_account_credential_tag INTEGER NOT NULL DEFAULT 0, captured_slot INTEGER NOT NULL DEFAULT 0, boundary_slot INTEGER NOT NULL DEFAULT 0)`,
			`CREATE TABLE epoch (id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_id INTEGER, start_slot INTEGER, length_in_slots INTEGER)`,
			`CREATE TABLE tip (hash BLOB, id INTEGER PRIMARY KEY AUTOINCREMENT, slot INTEGER, block_number INTEGER)`,
			`CREATE TABLE reward_pool_output (apparent_performance TEXT, pool_key_hash BLOB NOT NULL, id INTEGER PRIMARY KEY AUTOINCREMENT, epoch INTEGER NOT NULL, optimal_reward TEXT NOT NULL DEFAULT '0', total_reward TEXT NOT NULL DEFAULT '0', leader_reward TEXT NOT NULL DEFAULT '0', member_reward_total TEXT NOT NULL DEFAULT '0', owner_stake TEXT NOT NULL DEFAULT '0', undistributed TEXT NOT NULL DEFAULT '0', unspendable TEXT NOT NULL DEFAULT '0', captured_slot INTEGER NOT NULL DEFAULT 0, boundary_slot INTEGER NOT NULL DEFAULT 0)`,
		)
	}
	return ret
}
