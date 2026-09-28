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

//go:build dingo_extra_plugins

package ledger

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/mysql"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/postgres"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rewardRaceBackend opens a metadata backend plus a raw connection that can
// seed certificate history the database API only writes from real blocks.
type rewardRaceBackend struct {
	name string
	open func(t *testing.T) (*database.Database, *sql.DB, string)
}

// A rollback's truncation and a precompute write are separate transactions.
// SQLite's single write connection orders them; Postgres and MySQL run them
// concurrently, so the ledger itself must keep a write that passed the
// rollback guard from committing after the truncation that should delete it.
func TestRewardPrecomputeWriteCannotOutliveConcurrentRollback(
	t *testing.T,
) {
	t.Parallel()

	backends := []rewardRaceBackend{
		{name: "sqlite", open: openSQLiteRewardRaceBackend},
		{name: "postgres", open: openPostgresRewardRaceBackend},
		{name: "mysql", open: openMySQLRewardRaceBackend},
	}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			t.Parallel()
			db, raw, placeholder := backend.open(t)
			testRewardPrecomputeWriteRacesRollback(t, db, raw, placeholder)
		})
	}
}

func testRewardPrecomputeWriteRacesRollback(
	t *testing.T,
	db *database.Database,
	raw *sql.DB,
	dialect string,
) {
	seedRewardPrecomputeTimingInputs(t, db, 6)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))
	nonce := testHashBytes("reward-epoch")
	require.NoError(t, db.SetEpoch(
		200, 3, nonce, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1, 1_000, nil,
	))
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newRewardCalculationTestNodeConfig(t),
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	epoch, err := db.Metadata().GetEpoch(3, nil)
	require.NoError(t, err)
	ls.currentEpoch = *epoch
	ls.currentEra = eras.ShelleyEraDesc
	cutoff, err := ls.rewardPrefilterSlot(db.Metadata(), nil, 3)
	require.NoError(t, err)
	member := rewardCalcHash(0x6a)
	// The abandoned chain deregisters member after the rollback point and
	// before the RUPD slot, so its precompute excludes member.
	seedRewardRaceStakeCert(
		t, raw, dialect, 21, member, 150,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	seedRewardRaceStakeCert(
		t, raw, dialect, 22, member, cutoff-5,
		uint(lcommon.CertificateTypeStakeDeregistration),
	)
	ancestor := chain.RawBlock{
		Slot: cutoff - 10, Hash: testHashBytes("race-ancestor"),
		BlockNumber: 1, Type: 1, Cbor: []byte{0x80},
	}
	abandoned := chain.RawBlock{
		Slot: cutoff + 1, Hash: testHashBytes("race-abandoned"),
		PrevHash:    ancestor.Hash,
		BlockNumber: 2, Type: 1, Cbor: []byte{0x80},
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(
		[]chain.RawBlock{ancestor, abandoned},
	))
	for _, block := range []chain.RawBlock{ancestor, abandoned} {
		require.NoError(t, db.SetBlockNonce(
			block.Hash, block.Slot, nonce, true, nil,
		))
	}
	ls.currentTip = ochainsync.Tip{
		Point:       ocommon.NewPoint(abandoned.Slot, abandoned.Hash),
		BlockNumber: abandoned.BlockNumber,
	}
	require.NoError(t, db.SetTip(ls.currentTip, nil))

	rollbackDone := make(chan error, 1)
	var hookCalls atomic.Int32
	ls.rewardPrecomputeBeforeSaveHook = func() {
		if hookCalls.Add(1) > 1 {
			return
		}
		go func() {
			rollbackDone <- ls.rollbackWithBlocks(
				ocommon.NewPoint(ancestor.Slot, ancestor.Hash), nil, false,
			)
		}()
		// The guard has passed and the outputs are not written yet. A
		// rollback that finished now would have truncated before this
		// write's rows exist.
		assert.Never(t, func() bool { return len(rollbackDone) > 0 },
			2*time.Second, 10*time.Millisecond,
			"a rollback completed inside a precompute write that passed "+
				"its guard")
	}
	require.NoError(t, ls.precomputeStakeRewardsAfterEpochTransition(
		event.EpochTransitionEvent{
			NewEpoch:     3,
			BoundarySlot: abandoned.Slot,
			EpochNonce:   nonce,
		},
	))
	require.Equal(t, int32(1), hookCalls.Load(),
		"the abandoned-chain write reached the seam")
	require.NoError(t, testutil.RequireReceive(
		t, rollbackDone, 30*time.Second, "rollback did not finish",
	))
	ls.rewardPrecomputeWG.Wait()

	outputs, err := db.Metadata().GetRewardAccountOutputs(1, nil)
	require.NoError(t, err)
	assert.Empty(t, outputs,
		"an output from the abandoned chain survived the rollback")

	replacement := ocommon.NewPoint(
		cutoff+1, testHashBytes("race-replacement"),
	)
	ls.Lock()
	ls.currentTip = ochainsync.Tip{Point: replacement, BlockNumber: 2}
	ls.Unlock()
	ls.maybeQueueStakeRewardPrecomputeRetry(replacement.Slot)
	ls.rewardPrecomputeWG.Wait()

	var wantMember uint64
	readTxn := db.Transaction(false)
	require.NoError(t, readTxn.Do(func(txn *database.Txn) error {
		want, ok, err := ls.calculateStakeRewardApplication(
			txn, 4, replacement.Slot, 1_200, false,
		)
		require.NoError(t, err)
		require.True(t, ok)
		for _, output := range want.accountOutputs {
			if string(output.StakingKey) == string(member) {
				wantMember += uint64(output.Amount)
			}
		}
		return nil
	}))
	require.NotZero(t, wantMember,
		"control: the surviving chain pays member")
	writeTxn := db.Transaction(true)
	require.NoError(t, writeTxn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, 4, 1_200)
	}))
	account, err := db.GetAccountByCredential(0, member, true, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	require.Equal(t, wantMember, uint64(account.Reward),
		"the boundary must credit member from the surviving chain")
}

func seedRewardRaceStakeCert(
	t *testing.T,
	raw *sql.DB,
	dialect string,
	id uint,
	stakingKey []byte,
	slot uint64,
	certType uint,
) {
	t.Helper()
	table := `"transaction"`
	bind := func(n int) string { return fmt.Sprintf("$%d", n) }
	if dialect == "mysql" {
		table = "`transaction`"
		bind = func(int) string { return "?" }
	} else if dialect == "sqlite" {
		bind = func(int) string { return "?" }
	}
	hash := make([]byte, 32)
	binary.BigEndian.PutUint64(hash[24:], uint64(id))
	_, err := raw.Exec(fmt.Sprintf(
		"INSERT INTO %s (id, hash, slot, block_index) VALUES (%s, %s, %s, 0)",
		table, bind(1), bind(2), bind(3),
	), id, hash, slot)
	require.NoError(t, err)
	_, err = raw.Exec(fmt.Sprintf(
		"INSERT INTO certs (id, transaction_id, cert_index, slot, cert_type) "+
			"VALUES (%s, %s, 0, %s, %s)",
		bind(1), bind(2), bind(3), bind(4),
	), id, id, slot, certType)
	require.NoError(t, err)
	certTable := "stake_registration"
	if certType == uint(lcommon.CertificateTypeStakeDeregistration) {
		certTable = "stake_deregistration"
	}
	_, err = raw.Exec(fmt.Sprintf(
		"INSERT INTO %s "+
			"(id, staking_key, credential_tag, certificate_id, added_slot) "+
			"VALUES (%s, %s, 0, %s, %s)",
		certTable, bind(1), bind(2), bind(3), bind(4),
	), id, stakingKey, id, slot)
	require.NoError(t, err)
}

func openSQLiteRewardRaceBackend(
	t *testing.T,
) (*database.Database, *sql.DB, string) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	return db, raw, "sqlite"
}

func openPostgresRewardRaceBackend(
	t *testing.T,
) (*database.Database, *sql.DB, string) {
	t.Helper()
	if os.Getenv("POSTGRES_PASSWORD") == "" &&
		os.Getenv("POSTGRES_DSN") == "" {
		t.Skip(
			"postgres not configured (set POSTGRES_PASSWORD or POSTGRES_DSN)",
		)
	}
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "host=" + storagetest.EscapeLibpqValue(
			envOr("POSTGRES_HOST", "localhost"),
		) +
			" port=" + storagetest.EscapeLibpqValue(
			envOr("POSTGRES_PORT", "5432"),
		) +
			" user=" + storagetest.EscapeLibpqValue(
			envOr("POSTGRES_USER", "postgres"),
		) +
			" password=" + storagetest.EscapeLibpqValue(
			os.Getenv("POSTGRES_PASSWORD"),
		) +
			" dbname=" + storagetest.EscapeLibpqValue(
			envOr("POSTGRES_DATABASE", "dingo_test"),
		) +
			" sslmode=" + storagetest.EscapeLibpqValue(
			envOr("POSTGRES_SSLMODE", "disable"),
		)
	}
	schema := fmt.Sprintf("reward_race_%d", time.Now().UnixNano())
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(t.Context()))
	_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
		_ = admin.Close()
	})
	scoped := storagetest.PostgresDSNWithSearchPath(dsn, schema)
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{
			DataDir: filepath.Join(t.TempDir(), "blob"),
		},
		Metadata: dbtest.StorageProvider{
			Name:     "postgres",
			Config:   map[string]any{"dsn": scoped},
			Register: postgres.RegisterProvider,
		},
	})
	require.NoError(t, err)
	raw, err := sql.Open("pgx", scoped)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	return db, raw, "postgres"
}

func openMySQLRewardRaceBackend(
	t *testing.T,
) (*database.Database, *sql.DB, string) {
	t.Helper()
	if os.Getenv("MYSQL_ROOT_PASSWORD") == "" &&
		os.Getenv("MYSQL_DSN") == "" {
		t.Skip("mysql not configured (set MYSQL_ROOT_PASSWORD or MYSQL_DSN)")
	}
	rootDSN := os.Getenv("MYSQL_DSN")
	if rootDSN == "" {
		cfg := mysqldriver.Config{
			User:   "root",
			Passwd: os.Getenv("MYSQL_ROOT_PASSWORD"),
			Net:    "tcp",
			Addr: envOr("MYSQL_HOST", "localhost") + ":" +
				envOr("MYSQL_PORT", "3306"),
			ParseTime:            true,
			AllowNativePasswords: true,
		}
		rootDSN = cfg.FormatDSN()
	}
	dbName := fmt.Sprintf("reward_race_%d", time.Now().UnixNano())
	admin, err := sql.Open("mysql", rootDSN)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(t.Context()))
	_, err = admin.Exec("CREATE DATABASE `" + dbName + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE `" + dbName + "`")
		_ = admin.Close()
	})
	parsed, err := mysqldriver.ParseDSN(rootDSN)
	require.NoError(t, err)
	parsed.DBName = dbName
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{
			DataDir: filepath.Join(t.TempDir(), "blob"),
		},
		Metadata: dbtest.StorageProvider{
			Name:     "mysql",
			Config:   map[string]any{"dsn": parsed.FormatDSN()},
			Register: mysql.RegisterProvider,
		},
	})
	require.NoError(t, err)
	raw, err := sql.Open("mysql", parsed.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	return db, raw, "mysql"
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
