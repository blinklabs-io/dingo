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
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/drepquery"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetLatestBlockNonce(t *testing.T) {
	store, _ := newSharedSQLStore(t)

	row, ok, err := store.GetLatestBlockNonce(nil)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, models.BlockNonce{}, row)

	require.NoError(t, store.SetBlockNonce(
		[]byte{0x01}, 10, []byte{0x0a}, false, nil,
	))
	require.NoError(t, store.SetBlockNonce(
		[]byte{0xff}, 20, []byte{0x14}, false, nil,
	))

	row, ok, err = store.GetLatestBlockNonce(nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(20), row.Slot)
	require.Equal(t, []byte{0xff}, row.Hash)
}

func TestGetLatestBlockNonceUsesApplicationOrderForSameSlot(t *testing.T) {
	store, _ := newSharedSQLStore(t)

	const slot = uint64(20)
	firstHash := []byte{0xff}
	secondHash := []byte{0x01}
	firstNonce := []byte("nonce-first")
	secondNonce := []byte("nonce-second")

	// The later application has the lower hash. Hash ordering must not make the
	// earlier row look like the durable floor after a same-slot fork race.
	require.NoError(t, store.SetBlockNonce(
		firstHash, slot, firstNonce, false, nil,
	))
	require.NoError(t, store.SetBlockNonce(
		secondHash, slot, secondNonce, false, nil,
	))

	row, ok, err := store.GetLatestBlockNonce(nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, secondHash, row.Hash)
	require.Equal(t, secondNonce, row.Nonce)
}

// TestCheckpointWALTruncatesFile proves checkpointWAL's central claim: a
// PASSIVE checkpoint (what wal_autocheckpoint invokes automatically after
// every commit) never shrinks the -wal file's on-disk size even when it
// fully succeeds, but PRAGMA wal_checkpoint(TRUNCATE) does. Without this,
// dingo_database_sql_wal_bytes -- a plain os.Stat of that file, see
// metrics.go -- can never show a single decrease no matter how well
// checkpointing is otherwise working underneath.
func TestCheckpointWALTruncatesFile(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	store, writeDB, _, err := openSQLStore(
		Config{DataDir: dataDir},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	_, err = writeDB.ExecContext(
		t.Context(),
		"CREATE TABLE checkpoint_probe (n INTEGER)",
	)
	require.NoError(t, err)
	// wal_autocheckpoint's own threshold is 10000 pages (~40MB); stay well
	// under it so this test observes checkpointWAL's own effect rather than
	// the automatic pragma firing mid-loop.
	for i := range 200 {
		_, err := writeDB.ExecContext(
			t.Context(),
			"INSERT INTO checkpoint_probe (n) VALUES (?)",
			i,
		)
		require.NoError(t, err)
	}

	walPath := filepath.Join(dataDir, "metadata.sqlite-wal")
	before, err := os.Stat(walPath)
	require.NoError(t, err)
	require.Positive(
		t,
		before.Size(),
		"WAL file should hold uncheckpointed frames before the forced checkpoint",
	)

	databaseURI := sqliteFileURI(filepath.Join(dataDir, "metadata.sqlite"))
	require.NoError(
		t,
		checkpointWAL(databaseURI, slog.Default())(t.Context()),
	)

	after, err := os.Stat(walPath)
	require.NoError(t, err)
	require.Zero(
		t, after.Size(),
		"a TRUNCATE checkpoint should shrink the WAL file to zero bytes",
	)
}

// TestCheckpointWALDoesNotBlockWriteBehindReaderSnapshot is the regression
// test for the writeDB-based design checkpointWAL used before: issuing
// PRAGMA wal_checkpoint(TRUNCATE) against writeDB (SetMaxOpenConns(1))
// blocked that sole connection -- and therefore every other write -- for up
// to the full busy_timeout(30000) whenever a readDB snapshot was open, since
// the checkpoint's busy handler has to wait out that snapshot before it can
// truncate. Measured against that design: a checkpoint attempt with one open
// readDB snapshot took 30.04s, blocked a concurrent writeDB insert for
// 29.99s of that, and still finished with busy=1 (no truncation).
// checkpointWAL now issues the pragma from a dedicated connection with a
// short busy_timeout instead, so it fails fast on the same busy=1 outcome
// and a concurrent writeDB write is never blocked by it.
//
// Not t.Parallel: every assertion below is a wall-clock duration, which is a
// process-wide measurement in the same sense as testing.AllocsPerRun or
// runtime.NumGoroutine -- what it reports depends on whatever else is running
// in the process, not only on the code under test. Run in parallel with the
// rest of this package it measured the runner's load as much as the
// checkpoint, and failed on a loaded CI runner while the behaviour it guards
// was intact.
func TestCheckpointWALDoesNotBlockWriteBehindReaderSnapshot(t *testing.T) {
	dataDir := t.TempDir()
	store, writeDB, readDB, err := openSQLStore(
		Config{DataDir: dataDir},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	_, err = writeDB.ExecContext(
		t.Context(),
		"CREATE TABLE checkpoint_probe (n INTEGER)",
	)
	require.NoError(t, err)
	_, err = writeDB.ExecContext(
		t.Context(),
		"INSERT INTO checkpoint_probe (n) VALUES (0)",
	)
	require.NoError(t, err)

	// Hold a readDB snapshot open so a TRUNCATE checkpoint can never
	// complete (busy=1): this is the condition under which the old
	// writeDB-based design blocked concurrent writes for up to
	// busy_timeout(30000).
	readTx, err := readDB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = readTx.Rollback()
	})
	// The design this guards blocked for the full busy_timeout(30000); a
	// healthy dedicated-connection attempt gives up after
	// checkpointBusyTimeout (250ms). Bound the gap well below 30s rather
	// than just above 250ms: these are wall-clock measurements on shared
	// runners, and a bound close to the healthy time fails on load rather
	// than on the defect it exists to catch.
	const maxUnblocked = 10 * time.Second

	var probe int
	require.NoError(
		t,
		readTx.QueryRowContext(
			t.Context(),
			"SELECT n FROM checkpoint_probe LIMIT 1",
		).Scan(&probe),
	)

	type writeResult struct {
		duration time.Duration
		err      error
	}
	writeResultCh := make(chan writeResult, 1)
	go func() {
		started := time.Now()
		_, execErr := writeDB.ExecContext(
			t.Context(),
			"INSERT INTO checkpoint_probe (n) VALUES (1)",
		)
		writeResultCh <- writeResult{
			duration: time.Since(started),
			err:      execErr,
		}
	}()

	var logBuf bytes.Buffer
	checkpointLogger := slog.New(slog.NewTextHandler(&logBuf, nil))

	databaseURI := sqliteFileURI(filepath.Join(dataDir, "metadata.sqlite"))
	checkpointStarted := time.Now()
	require.NoError(
		t,
		checkpointWAL(databaseURI, checkpointLogger)(t.Context()),
	)
	checkpointDuration := time.Since(checkpointStarted)
	require.Less(
		t, checkpointDuration, maxUnblocked,
		"a dedicated-connection checkpoint attempt should fail fast on a "+
			"blocked truncate, not wait out busy_timeout(30000)",
	)
	require.Contains(
		t, logBuf.String(), "could not fully complete",
		"the open reader snapshot should make the truncate impossible, "+
			"reproducing the busy=1 condition the old design blocked on",
	)

	result := testutil.RequireReceive(
		t, writeResultCh, maxUnblocked,
		"concurrent writeDB insert must not be blocked behind the "+
			"checkpoint attempt",
	)
	require.NoError(t, result.err)
	require.Less(
		t, result.duration, maxUnblocked,
		"a concurrent write must not be blocked behind the checkpoint attempt",
	)
}

func TestMetadataStoreConformance(t *testing.T) {
	storagetest.RunMetadataStoreConformance(
		t,
		func(t *testing.T) metadata.MetadataStore {
			t.Helper()
			store, _, _, err := openSQLStore(
				Config{DataDir: t.TempDir()},
				metadata.ProviderDependencies{},
			)
			require.NoError(t, err)
			require.NoError(t, store.Start(t.Context()))
			t.Cleanup(func() {
				require.NoError(t, store.Close())
			})
			return store
		},
	)
}

func TestMetadataStoreResourceCleanup(t *testing.T) {
	storagetest.AssertNoGoroutineLeak(t, func(t *testing.T) {
		store, _, _, err := openSQLStore(
			Config{DataDir: t.TempDir()},
			metadata.ProviderDependencies{},
		)
		require.NoError(t, err)
		require.NoError(t, store.Start(t.Context()))
		txn := store.Transaction(t.Context())
		require.NoError(t, store.SetCommitTimestamp(1, txn))
		require.NoError(t, txn.Commit())
		require.NoError(t, store.Close())
	})
}

// oldVotingPowerByTypeSQL is the pre-fix shape of
// drepquery.VotingPowerByTypeSQL for sqlite: it scans every live utxo row and
// runs a correlated EXISTS subquery against account per row, instead of
// starting from the small set of drep-delegated accounts and joining outward
// to utxo. Kept here, side by side with the fixed query, so the tests below
// hold both shapes against the same fixture permanently rather than only at
// the moment of the fix. IN (?,?) is inlined for the fixed two-element
// AlwaysAbstain/AlwaysNoConfidence collection this test always passes,
// mirroring what expandDrepCollectionQuery would produce for that count.
const oldVotingPowerByTypeSQL = `
	SELECT a.drep_type AS drep_type,
		   COALESCE(SUM(
			   COALESCE(u.utxo_sum, 0)
			   + COALESCE(CAST(a.reward AS INTEGER), 0)
		   ), 0) AS stake
	FROM account a
	LEFT JOIN (
		SELECT credential_tag, staking_key,
			   COALESCE(SUM(CAST(amount AS INTEGER)), 0) AS utxo_sum
		FROM utxo
		WHERE deleted_slot = 0
		  AND EXISTS (
			  SELECT 1 FROM account ax
			  WHERE ax.credential_tag = utxo.credential_tag
			    AND ax.staking_key = utxo.staking_key
			    AND ax.active = 1 AND ax.drep_type IN (?,?)
		  )
		GROUP BY credential_tag, staking_key
	) u ON u.credential_tag = a.credential_tag
		AND u.staking_key = a.staking_key
	WHERE a.active = 1 AND a.drep_type IN (?,?)
	GROUP BY a.drep_type
`

// newVotingPowerByTypeSQL returns the current (fixed) query, with its
// collection placeholder expanded exactly as
// sqlstore.expandDrepCollectionQuery would for a two-element drep_type list.
// That helper is unexported in package sqlstore, so the expansion is
// reproduced here rather than imported.
func newVotingPowerByTypeSQL(t *testing.T, expiryEpoch uint64) string {
	t.Helper()
	query := drepquery.VotingPowerByTypeSQL("sqlite", expiryEpoch)
	expanded := strings.Replace(query, "IN ?", "IN (?,?)", 2)
	require.NotEqual(
		t,
		query,
		expanded,
		"expected two IN ? placeholders to expand",
	)
	return expanded
}

// runVotingPowerByType executes a two-element AlwaysAbstain/
// AlwaysNoConfidence query built by either oldVotingPowerByTypeSQL or
// newVotingPowerByTypeSQL and returns the resulting drep_type -> stake map.
// Both queries bind [type1, type2, type1, type2] with no expiry epoch, or
// [expiry, type1, type2, expiry, type1, type2] with one.
func runVotingPowerByType(
	t *testing.T,
	db *sql.DB,
	query string,
	expiryEpoch uint64,
	drepType1, drepType2 uint64,
) map[uint64]uint64 {
	t.Helper()
	args := []any{}
	if expiryEpoch > 0 {
		args = append(args, expiryEpoch)
	}
	args = append(args, drepType1, drepType2)
	if expiryEpoch > 0 {
		args = append(args, expiryEpoch)
	}
	args = append(args, drepType1, drepType2)
	rows, err := db.QueryContext(context.Background(), query, args...)
	require.NoError(t, err)
	defer rows.Close()
	ret := map[uint64]uint64{}
	for rows.Next() {
		var drepType int64
		var stake int64
		require.NoError(t, rows.Scan(&drepType, &stake))
		ret[uint64(drepType)] = uint64(stake)
	}
	require.NoError(t, rows.Err())
	return ret
}

// seedVotingPowerByTypeFixture creates a mix of accounts and utxos designed
// to exercise every exclusion the account-first rewrite must preserve:
//   - two AlwaysAbstain accounts (one with two live utxos, to prove the
//     inner SUM/GROUP BY still aggregates per credential) and one
//     AlwaysNoConfidence account, all counted;
//   - a deleted utxo on an otherwise-counted account, excluded by
//     deleted_slot;
//   - an inactive AlwaysAbstain account, excluded entirely (reward and
//     utxo);
//   - a script-credential (drep_type=1) account with its own utxo, excluded
//     because it is not one of the requested predefined types -- this is
//     the case that would silently reappear if the rewritten inner query
//     ever dropped its ax.drep_type filter;
//   - an orphan utxo with no matching account row at all, excluded because
//     the account-first join never reaches it;
//   - an AlwaysAbstain account with a nonzero ExpirationEpoch, used by the
//     boundary test below.
func seedVotingPowerByTypeFixture(t *testing.T, store sharedDrepStore) {
	t.Helper()
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: []byte("abstain-one-staking-key-28by"),
		DrepType:   models.DrepTypeAlwaysAbstain,
		Active:     true, Reward: 1000,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       []byte("abstain-one-live-utxo-tx-32bytes"),
		StakingKey: []byte("abstain-one-staking-key-28by"),
		Amount:     500,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       []byte("abstain-one-live-utxo-2-tx-32byt"),
		StakingKey: []byte("abstain-one-staking-key-28by"),
		Amount:     700,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:        []byte("abstain-one-deleted-utxo-tx-32by"),
		StakingKey:  []byte("abstain-one-staking-key-28by"),
		Amount:      9_999_999,
		DeletedSlot: 999,
	}))

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey:    []byte("abstain-two-staking-key-28byt"),
		CredentialTag: 1,
		DrepType:      models.DrepTypeAlwaysAbstain,
		Active:        true, Reward: 50,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:          []byte("abstain-two-live-utxo-tx-32byte"),
		StakingKey:    []byte("abstain-two-staking-key-28byt"),
		CredentialTag: 1,
		Amount:        300,
	}))

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: []byte("no-confidence-staking-key-28b"),
		DrepType:   models.DrepTypeAlwaysNoConfidence,
		Active:     true, Reward: 20,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       []byte("no-confidence-live-utxo-tx-32byt"),
		StakingKey: []byte("no-confidence-staking-key-28b"),
		Amount:     80,
	}))

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: []byte("inactive-abstain-staking-key-2"),
		DrepType:   models.DrepTypeAlwaysAbstain,
		Active:     false, Reward: 999,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       []byte("inactive-abstain-utxo-tx-32byte"),
		StakingKey: []byte("inactive-abstain-staking-key-2"),
		Amount:     444,
	}))

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: []byte("script-drep-staking-key-28byt"),
		DrepType:   1, // credential-backed (script hash), not predefined
		Active:     true, Reward: 10,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       []byte("script-drep-utxo-tx-32bytes-here"),
		StakingKey: []byte("script-drep-staking-key-28byt"),
		Amount:     20,
	}))

	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       []byte("orphan-utxo-no-account-tx-32byte"),
		StakingKey: []byte("orphan-staking-key-no-account"),
		Amount:     777,
	}))

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey:      []byte("expiring-abstain-staking-key1"),
		DrepType:        models.DrepTypeAlwaysAbstain,
		Active:          true,
		Reward:          5,
		ExpirationEpoch: 100,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       []byte("expiring-abstain-utxo-tx-32byte"),
		StakingKey: []byte("expiring-abstain-staking-key1"),
		Amount:     15,
	}))
}

// sharedDrepStore is the subset of *sqlstore.Store this file exercises
// directly, without going through the full drepStore interface in
// shared_sqlstore_test.go.
type sharedDrepStore interface {
	CreateAccount(types.Txn, *models.Account) error
	CreateUtxo(types.Txn, *models.Utxo) error
	GetDRepVotingPowerByType(
		[]uint64,
		uint64,
		types.Txn,
	) (map[uint64]uint64, error)
}

func TestDRepVotingPowerByTypeUtxoJoinRewriteMatchesReference(t *testing.T) {
	t.Parallel()
	store, writeDB := newSharedSQLStore(t)
	seedVotingPowerByTypeFixture(t, store)

	// Independently hand-computed from the fixture: two live utxos plus the
	// reward on the first AlwaysAbstain account, one live utxo plus reward
	// on the second, plus the ExpirationEpoch=100 account (included because
	// expiryEpoch=0 applies no filter at all). The deleted utxo, the
	// inactive account, the script-credential account, and the orphan utxo
	// each contribute nothing.
	wantAbstain := uint64(1000+500+700) + uint64(50+300) + uint64(5+15)
	wantNoConfidence := uint64(20 + 80)
	want := map[uint64]uint64{
		models.DrepTypeAlwaysAbstain:      wantAbstain,
		models.DrepTypeAlwaysNoConfidence: wantNoConfidence,
	}

	oldResult := runVotingPowerByType(
		t,
		writeDB,
		oldVotingPowerByTypeSQL,
		0,
		models.DrepTypeAlwaysAbstain,
		models.DrepTypeAlwaysNoConfidence,
	)
	require.Equal(t, want, oldResult, "pre-fix correlated-EXISTS query")

	newResult := runVotingPowerByType(
		t,
		writeDB,
		newVotingPowerByTypeSQL(t, 0),
		0,
		models.DrepTypeAlwaysAbstain,
		models.DrepTypeAlwaysNoConfidence,
	)
	require.Equal(t, want, newResult, "fixed account-first join query")

	// The production entry point (GetDRepVotingPowerByType) must agree too.
	prodResult, err := store.GetDRepVotingPowerByType(
		[]uint64{
			models.DrepTypeAlwaysAbstain,
			models.DrepTypeAlwaysNoConfidence,
		},
		0,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, want, prodResult)
}

// TestDRepVotingPowerByTypeExpiryBoundary drives the CIP-0163 expiry filter
// through its exact boundary: expiration_epoch = 100 must still count an
// account queried as of epoch 100, and must exclude it as of epoch 101. This
// filter and its boundary carry over unchanged from the pre-fix query, but
// the rewrite touches the clause's surrounding structure, so the boundary is
// exercised directly against the production entry point.
func TestDRepVotingPowerByTypeExpiryBoundary(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	seedVotingPowerByTypeFixture(t, store)

	const stakeWithoutExpiring = uint64(1000+500+700) + uint64(50+300)
	const stakeWithExpiring = stakeWithoutExpiring + uint64(5+15)

	atExpiry, err := store.GetDRepVotingPowerByType(
		[]uint64{models.DrepTypeAlwaysAbstain},
		100,
		nil,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		stakeWithExpiring,
		atExpiry[models.DrepTypeAlwaysAbstain],
		"expiration_epoch = queried epoch must still count",
	)

	afterExpiry, err := store.GetDRepVotingPowerByType(
		[]uint64{models.DrepTypeAlwaysAbstain},
		101,
		nil,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		stakeWithoutExpiring,
		afterExpiry[models.DrepTypeAlwaysAbstain],
		"expiration_epoch < queried epoch must be excluded",
	)
}

// TestDRepVotingPowerByTypeQueryPlanDrivesFromAccountNotUtxo is the
// performance regression check. At this test's
// row counts sqlite flattens the pre-fix query's EXISTS into an indexed
// semi-join rather than literally naming it a "CORRELATED SCALAR SUBQUERY"
// in EXPLAIN QUERY PLAN output (that wording is what the issue's comment
// observed at production scale, 2.86M live utxo rows), but the pre-fix
// query's FROM clause is still `utxo`, so its inner subquery's driving loop
// is forced to scan every live utxo row regardless of scale, then probe
// account per row. The fixed query's FROM clause is `account`, so its
// driving loop scans only the requested drep_type's accounts and probes
// UTxO per row -- the account-first shape. That FROM-clause
// difference, not a cardinality estimate, is what fixes the driving table,
// so this assertion holds at any data size. Reverting the production fix
// makes this test fail: drepquery.VotingPowerByTypeSQL would then also plan
// a full live-utxo scan as its driving loop.
func TestDRepVotingPowerByTypeQueryPlanDrivesFromAccountNotUtxo(t *testing.T) {
	t.Parallel()
	store, writeDB := newSharedSQLStore(t)
	seedVotingPowerByTypeFixture(t, store)

	const utxoDrivenScan = "SEARCH utxo USING COVERING INDEX " +
		"idx_utxo_deleted_staking_amount (deleted_slot=?)"
	const accountDrivenScan = "SEARCH ax USING COVERING INDEX " +
		"idx_account_drep_type_active_staking_key (drep_type=? AND active=?)"

	oldPlan := explainQueryPlan(
		t,
		writeDB,
		oldVotingPowerByTypeSQL,
		0,
		models.DrepTypeAlwaysAbstain,
		models.DrepTypeAlwaysNoConfidence,
	)
	require.Contains(
		t,
		oldPlan,
		utxoDrivenScan,
		"pre-fix query plan should drive its inner subquery off every "+
			"live utxo row: %s",
		oldPlan,
	)

	newPlan := explainQueryPlan(
		t,
		writeDB,
		newVotingPowerByTypeSQL(t, 0),
		0,
		models.DrepTypeAlwaysAbstain,
		models.DrepTypeAlwaysNoConfidence,
	)
	require.NotContains(
		t,
		newPlan,
		utxoDrivenScan,
		"fixed query plan must not scan every live utxo row: %s",
		newPlan,
	)
	require.Contains(
		t,
		newPlan,
		accountDrivenScan,
		"fixed query plan should drive its inner subquery off the "+
			"drep_type-filtered account set instead: %s",
		newPlan,
	)
}

func explainQueryPlan(
	t *testing.T,
	db *sql.DB,
	query string,
	expiryEpoch uint64,
	drepType1, drepType2 uint64,
) string {
	t.Helper()
	args := []any{}
	if expiryEpoch > 0 {
		args = append(args, expiryEpoch)
	}
	args = append(args, drepType1, drepType2)
	if expiryEpoch > 0 {
		args = append(args, expiryEpoch)
	}
	args = append(args, drepType1, drepType2)
	rows, err := db.QueryContext(
		context.Background(),
		"EXPLAIN QUERY PLAN "+query,
		args...,
	)
	require.NoError(t, err)
	defer rows.Close()
	cols, err := rows.Columns()
	require.NoError(t, err)
	var plan strings.Builder
	for rows.Next() {
		dest := make([]any, len(cols))
		scanBuf := make([]sql.NullString, len(cols))
		for i := range dest {
			dest[i] = &scanBuf[i]
		}
		require.NoError(t, rows.Scan(dest...))
		for _, c := range scanBuf {
			plan.WriteString(c.String)
			plan.WriteString(" ")
		}
		plan.WriteString("\n")
	}
	require.NoError(t, rows.Err())
	return plan.String()
}

// gapDepositFixture drives one pool through register / re-register / retire so
// the deposit POOLREAP refunds can be read back from the certificate rows.
type gapDepositFixture struct {
	store      *sqlstore.Store
	poolKey    []byte
	vrfKey     []byte
	rewardKey  []byte
	stakeKey   []byte
	nextTxByte byte
}

func newGapDepositFixture(t *testing.T) *gapDepositFixture {
	t.Helper()
	store, _ := newSharedSQLStore(t)
	return &gapDepositFixture{
		store:      store,
		poolKey:    bytes.Repeat([]byte{0xA1}, 28),
		vrfKey:     bytes.Repeat([]byte{0xA2}, 32),
		rewardKey:  bytes.Repeat([]byte{0xA3}, 28),
		stakeKey:   bytes.Repeat([]byte{0xA4}, 28),
		nextTxByte: 1,
	}
}

func (f *gapDepositFixture) hash() lcommon.Blake2b256 {
	var h lcommon.Blake2b256
	h[0] = f.nextTxByte
	f.nextTxByte++
	return h
}

func (f *gapDepositFixture) registration() lcommon.Certificate {
	return &lcommon.PoolRegistrationCertificate{
		CertType:      uint(lcommon.CertificateTypePoolRegistration),
		Operator:      lcommon.PoolKeyHash(f.poolKey),
		VrfKeyHash:    lcommon.VrfKeyHash(f.vrfKey),
		Pledge:        1_000_000,
		Cost:          340_000_000,
		Margin:        gcbor.Rat{Rat: big.NewRat(1, 100)},
		RewardAccount: lcommon.AddrKeyHash(f.rewardKey),
		PoolOwners: []lcommon.AddrKeyHash{
			lcommon.AddrKeyHash(f.stakeKey),
		},
	}
}

func (f *gapDepositFixture) retirement(epoch uint64) lcommon.Certificate {
	return &lcommon.PoolRetirementCertificate{
		CertType:    uint(lcommon.CertificateTypePoolRetirement),
		PoolKeyHash: lcommon.PoolKeyHash(f.poolKey),
		Epoch:       epoch,
	}
}

// applyLive writes a transaction through the normal block-apply path, which
// always carries calculated deposits.
func (f *gapDepositFixture) applyLive(
	t *testing.T,
	slot uint64,
	certificates []lcommon.Certificate,
	deposits map[int]uint64,
) {
	t.Helper()
	require.NoError(t, f.store.SetTransaction(
		&mockTransaction{
			hash:         f.hash(),
			isValid:      true,
			certificates: certificates,
		},
		ocommon.Point{Slot: slot, Hash: bytes.Repeat([]byte{0xb1}, 32)},
		0,
		deposits,
		false,
		nil,
	))
}

// applyGap writes a transaction through the Mithril gap path. deposits is what
// mithril's gapCertDeposits derives from the gap block's era and the epoch's
// protocol parameters.
func (f *gapDepositFixture) applyGap(
	t *testing.T,
	slot uint64,
	certificates []lcommon.Certificate,
	deposits map[int]uint64,
) {
	t.Helper()
	require.NoError(t, f.store.SetGapBlockTransaction(
		&mockTransaction{
			hash:         f.hash(),
			isValid:      true,
			certificates: certificates,
		},
		ocommon.Point{Slot: slot, Hash: bytes.Repeat([]byte{0xb2}, 32)},
		0,
		deposits,
		nil,
	))
}

// TestGapBlockPoolRegistrationRefundsItsDeposit pins the refund POOLREAP pays a
// pool whose registration was ingested from a Mithril gap block.
//
// GetPoolsRetiringAtEpoch takes the retiring pool's latest pool_registration
// row and applyPoolRetirements credits that row's deposit_held as the refund.
// A gap block replays from raw CBOR with no ledger delta, so nothing upstream
// calculates its deposits; mithril's gapCertDeposits derives them from the
// block's era and the epoch's protocol parameters and passes them in here.
//
// Without them the gap registration records no deposit, which parseNullUint64
// reads back as 0 -- indistinguishable from a pool that genuinely paid nothing
// -- and the operator's real deposit is never refunded.
//
// The gap block carries the pool's *first* registration. A gap re-registration
// of a still-registered pool cannot exercise the supplied amount at all:
// poolRegistrationDepositHeld returns the earlier row's held deposit and
// ignores the charged amount, because re-registering a live pool pays no new
// deposit. Such a case passes whether or not the gap path forwards deposits.
func TestGapBlockPoolRegistrationRefundsItsDeposit(t *testing.T) {
	t.Parallel()
	const deposit = uint64(500_000_000)

	for _, test := range []struct {
		name string
		// gap reports whether the registration arrives through the Mithril
		// gap path rather than the live block-apply path.
		gap bool
	}{
		// Control: the live path always carries calculated deposits. This is
		// the refund the same pool must still receive when the gap path
		// supplies them instead.
		{name: "live registration", gap: false},
		{name: "gap block registration", gap: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newGapDepositFixture(t)
			certificates := []lcommon.Certificate{f.registration()}
			deposits := map[int]uint64{0: deposit}
			if test.gap {
				f.applyGap(t, 100, certificates, deposits)
			} else {
				f.applyLive(t, 100, certificates, deposits)
			}
			f.applyLive(
				t, 300,
				[]lcommon.Certificate{f.retirement(5)},
				map[int]uint64{},
			)

			refunds, err := f.store.GetPoolsRetiringAtEpoch(5, 400, nil)
			require.NoError(t, err)
			require.Len(t, refunds, 1,
				"the pool must be found retiring at epoch 5")
			require.Equal(t, f.poolKey, refunds[0].PoolKeyHash)
			require.Equal(t,
				deposit,
				uint64(refunds[0].DepositHeld),
				"the refund must be the deposit the pool actually paid",
			)
		})
	}
}

// TestGetPoolsChunksBeyondParameterLimit covers a pool set spanning more than
// one chunk of GetPools' IN list.
//
// What this pins is the merge across the boundary, not the driver's reaction
// to a long statement: it asserts every requested pool comes back exactly
// once, which an off-by-one in the chunk arithmetic would break by dropping or
// duplicating the pools either side of the split. It deliberately does not
// assert that an unchunked read would fail -- the store contracts to a
// conservative 999 parameters for SQLite while the driver itself accepts
// 32766, so a set this size would be accepted either way and a test resting on
// rejection would pass whether or not the chunking existed.
func TestGetPoolsChunksBeyondParameterLimit(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	// Past the store's 999-parameter chunk by enough that the split lands
	// mid-request rather than exactly on the end.
	const poolCount = 1200
	hashes := make([]lcommon.PoolKeyHash, 0, poolCount)
	for i := range poolCount {
		raw := make([]byte, 28)
		binary.BigEndian.PutUint32(raw, uint32(i)+1)
		// NewBlake2b224 copies 28 bytes into the fixed-width type; despite the
		// name it does not hash them, so the key requested below is the same
		// key stored as pool_key_hash. Asserted rather than assumed, because
		// the alternative reading makes every row silently fail to match and
		// this test would then be reporting the wrong thing.
		pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(raw))
		require.Equal(t, raw, pkh.Bytes())
		hashes = append(hashes, pkh)

		vrf := make([]byte, 32)
		binary.BigEndian.PutUint32(vrf, uint32(i)+1)
		require.NoError(t, store.ImportPool(
			&models.Pool{
				PoolKeyHash:   raw,
				VrfKeyHash:    vrf,
				RewardAccount: raw,
			},
			&models.PoolRegistration{
				PoolKeyHash:   raw,
				VrfKeyHash:    vrf,
				RewardAccount: raw,
				AddedSlot:     uint64(i),
			},
			nil,
		))
	}

	pools, err := store.GetPools(hashes, nil)
	require.NoError(t, err)
	require.Len(t, pools, poolCount,
		"every requested pool must survive the chunk boundary")

	seen := make(map[string]struct{}, len(pools))
	for _, pool := range pools {
		seen[string(pool.PoolKeyHash)] = struct{}{}
	}
	require.Len(t, seen, poolCount, "no pool may be returned twice")
	// Spot-check either side of the boundary rather than all 1200.
	for _, i := range []int{0, 998, 999, 1000, poolCount - 1} {
		assert.Contains(t, seen, string(hashes[i].Bytes()),
			"pool %d must be present across the chunk boundary", i)
	}
}

// TestGetPoolsDeduplicatesRepeatedHashesAcrossChunks covers a hash named more
// than once in the same request.
//
// A single `IN (...)` has set semantics: listing a value twice still matches
// its row once. Chunking silently dropped that, because a hash landing in two
// different chunks matches in both and the results are concatenated. The
// caller then sees the same pool twice from a request that, unchunked, would
// have returned it once.
//
// The duplicate is worse than a repeat. loadPoolsAssociations keys its pool-ID
// map by ID and so retains only the last index for a repeated pool, leaving
// the earlier copy with its registrations and retirements empty -- a pool that
// reads as never registered. Anything deciding on len(pool.Registration), as
// registeredPoolVrfKeyHash does, gets a different answer depending on which
// copy it happens to look at.
//
// The repeat is placed at both ends of the request so the two occurrences fall
// in different chunks; within one chunk SQL would have collapsed them anyway,
// which is exactly the behaviour being restored.
func TestGetPoolsDeduplicatesRepeatedHashesAcrossChunks(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	const poolCount = 1200
	hashes := make([]lcommon.PoolKeyHash, 0, poolCount+1)
	for i := range poolCount {
		raw := make([]byte, 28)
		binary.BigEndian.PutUint32(raw, uint32(i)+1)
		pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(raw))
		hashes = append(hashes, pkh)

		vrf := make([]byte, 32)
		binary.BigEndian.PutUint32(vrf, uint32(i)+1)
		require.NoError(t, store.ImportPool(
			&models.Pool{
				PoolKeyHash:   raw,
				VrfKeyHash:    vrf,
				RewardAccount: raw,
			},
			&models.PoolRegistration{
				PoolKeyHash:   raw,
				VrfKeyHash:    vrf,
				RewardAccount: raw,
				AddedSlot:     uint64(i),
			},
			nil,
		))
	}
	// The first pool again, at the end, so its two mentions straddle the
	// 999-parameter split.
	repeated := hashes[0]
	hashes = append(hashes, repeated)

	pools, err := store.GetPools(hashes, nil)
	require.NoError(t, err)

	counts := make(map[string]int, len(pools))
	for _, pool := range pools {
		counts[string(pool.PoolKeyHash)]++
	}
	assert.Equal(t, 1, counts[string(repeated.Bytes())],
		"a hash named twice must still match its row once, as a single IN "+
			"list would have done")
	assert.Equal(t, poolCount, len(pools),
		"the result is one row per distinct pool requested, not per mention")

	// The copy that survives must be the hydrated one. Under the duplicate
	// this assertion is what fails first in practice: one of the two copies
	// carries no registration at all.
	for _, pool := range pools {
		if string(pool.PoolKeyHash) != string(repeated.Bytes()) {
			continue
		}
		assert.NotEmpty(t, pool.Registration,
			"the returned pool must carry its registrations; an unhydrated "+
				"duplicate reads as a pool that was never registered")
	}
}

// mithrilTrustBoundarySyncKey mirrors database.mithrilLedgerSlotSyncKey. The
// database package keeps its own unexported copy for the same reason this
// test does: nothing lower than it may import the ledger that writes the key.
const mithrilTrustBoundarySyncKey = "mithril_ledger_slot"

// TestCountPoolBlocksInSlotRangeExcludesMithrilImportedCounters covers the
// two row kinds pool_opcert_sequence carries. A block-apply writes one row per
// block minted, but a Mithril restore also writes one row per pool in the
// certified HeaderState counter map, all at the snapshot's anchor slot
// (ledgerstate.importOpCertCounters). Those rows are counters, not blocks: the
// node never applied a block for them, and the anchor slot cannot hold more
// than one block in any case.
//
// Reward performance (ledger/reward_calculation.go),
// reward_pool_input seeding (ledger/snapshot/rotation.go) and Blockfrost's
// blocks_minted all count these rows as minted blocks, so a bootstrapped node
// credits every pool holding a certified counter with a block it never made
// and inflates the epoch denominator by the size of the pool set.
func TestCountPoolBlocksInSlotRangeExcludesMithrilImportedCounters(
	t *testing.T,
) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	const boundarySlot = 1000
	require.NoError(t, store.SetSyncState(
		mithrilTrustBoundarySyncKey, "1000", nil,
	))

	pkhA := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xA1}, 28)),
	)
	pkhB := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xB2}, 28)),
	)
	pkhC := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xC3}, 28)),
	)

	// The certified counter map imported at the anchor: three pools, no
	// blocks applied for any of them.
	require.NoError(
		t,
		store.UpdatePoolOpCertSequence(pkhA, 5, boundarySlot, nil),
	)
	require.NoError(
		t,
		store.UpdatePoolOpCertSequence(pkhB, 7, boundarySlot, nil),
	)
	require.NoError(
		t,
		store.UpdatePoolOpCertSequence(pkhC, 2, boundarySlot, nil),
	)

	// One block the node actually applied after the boundary.
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhA, 5, 1200, nil))

	pools := []lcommon.PoolKeyHash{pkhA, pkhB, pkhC}
	counts, total, err := store.CountPoolBlocksInSlotRange(
		pools, 900, 1300, nil,
	)
	require.NoError(t, err)

	assert.Equal(t, map[string]uint64{
		string(pkhA.Bytes()): 1,
		string(pkhB.Bytes()): 0,
		string(pkhC.Bytes()): 0,
	}, counts, "only the post-boundary block counts as minted")
	assert.Equal(t, uint64(1), total,
		"epoch denominator must not include imported counter rows")
}

// TestGetPoolBlockIssuersInSlotRangeExcludesMithrilImportedCounters covers the
// ordered-row path the decentralization-aware reward count uses
// (ledger.rewardBlockCountsExcludingOverlaySlots), which reads the same table
// directly and so needs the same discrimination.
func TestGetPoolBlockIssuersInSlotRangeExcludesMithrilImportedCounters(
	t *testing.T,
) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	const boundarySlot = 1000
	require.NoError(t, store.SetSyncState(
		mithrilTrustBoundarySyncKey, "1000", nil,
	))

	pkhA := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xA1}, 28)),
	)
	pkhB := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xB2}, 28)),
	)

	require.NoError(
		t,
		store.UpdatePoolOpCertSequence(pkhA, 5, boundarySlot, nil),
	)
	require.NoError(
		t,
		store.UpdatePoolOpCertSequence(pkhB, 7, boundarySlot, nil),
	)
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhB, 7, 1100, nil))

	rows, err := store.GetPoolBlockIssuersInSlotRange(900, 1300, nil)
	require.NoError(t, err)

	require.Len(t, rows, 1, "only the post-boundary block is an issued block")
	assert.Equal(t, uint64(1100), rows[0].Slot)
	assert.Equal(t, pkhB.Bytes(), []byte(rows[0].PoolKeyHash))
}

// TestPoolBlockCountsWithoutMithrilBoundaryCountEveryRow pins the genesis-sync
// case: with no boundary recorded every row in the table is a block the node
// applied, including rows at low slots, so the filter must not narrow them.
func TestPoolBlockCountsWithoutMithrilBoundaryCountEveryRow(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	pkhA := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xA1}, 28)),
	)

	require.NoError(t, store.UpdatePoolOpCertSequence(pkhA, 1, 10, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhA, 1, 20, nil))

	counts, total, err := store.CountPoolBlocksInSlotRange(
		[]lcommon.PoolKeyHash{pkhA}, 0, 100, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), counts[string(pkhA.Bytes())])
	assert.Equal(t, uint64(2), total)

	rows, err := store.GetPoolBlockIssuersInSlotRange(0, 100, nil)
	require.NoError(t, err)
	assert.Len(t, rows, 2)
}

// TestPoolBlockCountsRejectMalformedMithrilBoundary covers a sync_state row
// that exists and holds nothing. sqlc's GetSyncState reports an absent key as
// sql.ErrNoRows, so an empty string is a malformed boundary rather than the
// absence of one; treating it as absent would silently re-admit every imported
// counter row as a minted block at exactly the moment the boundary could not
// be trusted.
func TestPoolBlockCountsRejectMalformedMithrilBoundary(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"", "not-a-slot"} {
		store, _ := newSharedSQLStore(t)
		require.NoError(t, store.SetSyncState(
			mithrilTrustBoundarySyncKey, value, nil,
		))
		pkh := lcommon.PoolKeyHash(
			lcommon.NewBlake2b224(bytes.Repeat([]byte{0xA1}, 28)),
		)
		require.NoError(t, store.UpdatePoolOpCertSequence(pkh, 1, 1000, nil))

		_, _, err := store.CountPoolBlocksInSlotRange(
			[]lcommon.PoolKeyHash{pkh}, 0, 2000, nil,
		)
		require.Error(
			t,
			err,
			"boundary %q must not be treated as absent",
			value,
		)
		assert.Contains(t, err.Error(), "Mithril trust boundary")

		_, err = store.GetPoolBlockIssuersInSlotRange(0, 2000, nil)
		require.Error(
			t,
			err,
			"boundary %q must not be treated as absent",
			value,
		)
	}
}

// poolOpCertSequenceIndex is the index migration v2 declares for the counter
// aggregate. Named here rather than in the store: nothing in the query
// mentions it, since which index answers a statement is the planner's choice,
// and that choice is exactly what this test checks.
const poolOpCertSequenceIndex = "idx_pool_opcert_sequence_pool_sequence"

// TestLatestPoolOpCertSequencesReadsIndexOnly pins the read plan of the
// op-cert counter aggregate.
//
// pool_opcert_sequence takes a row per block minted and is never pruned, so on
// a synced mainnet database this aggregate covers millions of rows to produce
// one entry per pool that has ever minted -- a few thousand. There is no slot
// bound available to narrow it: every row the table holds is at or below the
// tip, so restricting to the tip would exclude nothing. What keeps it off the
// table itself is an index carrying both columns it reads, which lets the
// aggregate run without touching a single row.
//
// What this does NOT claim is that the aggregate stops being linear in the
// table. SQLite has no loose index scan, so the plan below is a full scan of
// the index -- every entry visited, none of the rows. MySQL 8 can skip through
// the same index a group at a time; SQLite reads it end to end. The win pinned
// here is dropping the row fetches, not dropping the scan, and it is worth
// having because this is a one-shot query behind `leadership-schedule` rather
// than something on a hot path.
//
// The plan is asserted rather than the index's mere existence: an index no
// planner chooses is a write cost with no read benefit. It is EXPLAINed from
// the store's own exported statement rather than a copy of it, so the plan
// pinned here is the plan of the query that actually runs.
func TestLatestPoolOpCertSequencesReadsIndexOnly(t *testing.T) {
	t.Parallel()
	store, db := newSharedSQLStore(t)

	// Rows for two pools, so the group-by has something to fold.
	pkhA := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xA1}, 28)),
	)
	pkhB := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xB2}, 28)),
	)
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhA, 2, 10, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhA, 9, 20, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhB, 1, 15, nil))

	rows, err := db.Query(
		"EXPLAIN QUERY PLAN " + sqlstore.LatestPoolOpCertSequencesSQL,
	)
	require.NoError(t, err)
	defer rows.Close()

	var details string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
		details += detail + "\n"
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, details, "the planner must describe the aggregate")

	assert.Contains(t, details, "COVERING INDEX "+poolOpCertSequenceIndex,
		"the counter aggregate must read the index alone, not the table:\n%s",
		details,
	)
	// "COVERING INDEX" alone would still be satisfied by a plan that fell back
	// to sorting for the GROUP BY, which is the cost the index exists to avoid:
	// the entries already arrive grouped by pool_key_hash and ascending in
	// sequence, so the aggregate folds them as it goes.
	assert.NotContains(t, details, "USE TEMP B-TREE",
		"the index's column order must supply the GROUP BY, so no sort is "+
			"materialised:\n%s",
		details,
	)
}

// TestLatestPoolOpCertSequencesAtOrBefore pins both the values the counters
// at a slot take and the indexes each of the read's two statements uses: the
// slot index finds the pools with a later row, and the (pool, slot) index
// recomputes each of those, so neither reads the table end to end.
func TestLatestPoolOpCertSequencesAtOrBefore(t *testing.T) {
	t.Parallel()
	store, db := newSharedSQLStore(t)

	pool := func(b byte) lcommon.PoolKeyHash {
		return lcommon.PoolKeyHash(
			lcommon.NewBlake2b224(bytes.Repeat([]byte{b}, 28)),
		)
	}
	early, both, late := pool(0xA1), pool(0xB2), pool(0xC3)
	require.NoError(t, store.UpdatePoolOpCertSequence(early, 2, 10, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(both, 1, 15, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(both, 9, 25, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(late, 4, 30, nil))

	key := func(p lcommon.PoolKeyHash) string { return string(p[:]) }
	for _, tc := range []struct {
		slot uint64
		want map[string]uint64
	}{
		{5, map[string]uint64{}},
		{12, map[string]uint64{key(early): 2}},
		{20, map[string]uint64{key(early): 2, key(both): 1}},
		{30, map[string]uint64{key(early): 2, key(both): 9, key(late): 4}},
	} {
		got, err := store.LatestPoolOpCertSequencesAtOrBefore(tc.slot, nil)
		require.NoError(t, err)
		assert.Equal(t, tc.want, got, "slot %d", tc.slot)
	}

	for _, tc := range []struct {
		statement string
		args      []any
		index     string
	}{
		{
			sqlstore.PoolOpCertSequencesChangedAfterSQL,
			[]any{12},
			"idx_pool_opcert_sequence_slot",
		},
		{
			sqlstore.PoolOpCertSequenceAtOrBeforeSQL,
			[]any{key(both), 20},
			"idx_pool_opcert_sequence_pool_slot",
		},
	} {
		rows, err := db.Query("EXPLAIN QUERY PLAN "+tc.statement, tc.args...)
		require.NoError(t, err)
		var details string
		for rows.Next() {
			var id, parent, notUsed int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
			details += detail + "\n"
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		assert.Contains(t, details, tc.index,
			"the statement must read through its index:\n%s", details)
		assert.NotContains(t, details, "SCAN pool_opcert_sequence\n",
			"the statement must not scan the table:\n%s", details)
	}
}

// TestLatestPoolOpCertSequences covers the bulk read backing the
// GetChainDepState query's operational-certificate counters.
//
// The per-pool accessor answers the same question one pool at a time; this one
// has to agree with it for every pool at once, and has to reduce each pool's
// rows to the highest sequence rather than the newest, since the chain
// enforces the highest issue number it has accepted.
func TestLatestPoolOpCertSequences(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	poolA := bytes.Repeat([]byte{0xA1}, 28)
	poolB := bytes.Repeat([]byte{0xB2}, 28)
	pkhA := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolA))
	pkhB := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolB))

	// Pool A rotates certificates, with the highest issue number in the
	// middle: a query returning the newest row would report 4, not 9.
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhA, 2, 10, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhA, 9, 20, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhA, 4, 30, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhB, 1, 15, nil))

	sequences, err := store.LatestPoolOpCertSequences(nil)
	require.NoError(t, err)

	assert.Equal(t, map[string]uint64{
		string(pkhA.Bytes()): 9,
		string(pkhB.Bytes()): 1,
	}, sequences)

	// The two accessors must not be able to disagree.
	for _, pkh := range []lcommon.PoolKeyHash{pkhA, pkhB} {
		single, found, err := store.LatestPoolOpCertSequence(pkh, nil)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, single, sequences[string(pkh.Bytes())],
			"bulk and per-pool reads must agree for pool %x", pkh.Bytes())
	}
}

// TestLatestPoolOpCertSequencesEmpty covers a chain on which no pool has
// issued a block. The caller builds a CBOR map from the result, and the node
// emits an empty map there rather than null, so an empty read is not an error.
func TestLatestPoolOpCertSequencesEmpty(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	sequences, err := store.LatestPoolOpCertSequences(nil)
	require.NoError(t, err)
	assert.Empty(t, sequences)
}

func TestLatestPoolOpCertSequenceAfter(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	poolKeyHash := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xC3}, 28)),
	)
	require.NoError(t, store.UpdatePoolOpCertSequence(poolKeyHash, 1, 100, nil))
	require.NoError(
		t,
		store.UpdatePoolOpCertSequence(poolKeyHash, 490, 101, nil),
	)
	require.NoError(
		t,
		store.UpdatePoolOpCertSequence(poolKeyHash, 491, 102, nil),
	)

	sequence, found, err := store.LatestPoolOpCertSequenceAfter(
		poolKeyHash,
		100,
		nil,
	)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, uint64(491), sequence)

	sequence, found, err = store.LatestPoolOpCertSequenceAfter(
		poolKeyHash,
		102,
		nil,
	)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Zero(t, sequence)
}

func TestLatestPoolOpCertSequenceAtOrBefore(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	pool := bytes.Repeat([]byte{0xC3}, 28)
	pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(pool))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkh, 2, 10, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkh, 9, 20, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkh, 4, 30, nil))

	sequence, found, err := store.LatestPoolOpCertSequenceAtOrBefore(
		pkh,
		9,
		nil,
	)
	require.NoError(t, err)
	require.False(t, found)
	require.Zero(t, sequence)

	sequence, found, err = store.LatestPoolOpCertSequenceAtOrBefore(
		pkh,
		20,
		nil,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(9), sequence)

	sequence, found, err = store.LatestPoolOpCertSequenceAtOrBefore(
		pkh,
		30,
		nil,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(9), sequence,
		"the highest accepted counter, not the newest row, is authoritative")
}

// The account that wedged a from-genesis Preview replay at epoch 111, and the
// pointer address it held most of its funds at. The pointer tail 81b3bb010100
// decodes to (2940289, 1, 0) -- the position of the stake registration
// certificate that registered the credential.
const (
	wedgePointerAddress = "addr_test1gzgj6rad2h398mvgv59zcnrrq0x2adcftl6647ukcp7masupkwaszqgqjupejx"
	wedgePointerSlot    = 2_940_289
	wedgePointerTxIndex = 1
	wedgeCertIndex      = 0
)

// pointerVarNat encodes one component of a pointer address's tail: a
// big-endian base-128 natural whose non-final bytes carry the high bit.
func pointerVarNat(value uint64) []byte {
	ret := []byte{byte(value & 0x7f)}
	for value >>= 7; value > 0; value >>= 7 {
		ret = append([]byte{byte(value&0x7f) | 0x80}, ret...)
	}
	return ret
}

// newPointerAddress builds a testnet type-4 (pointer, key payment) address
// naming the certificate at (slot, txIndex, certIndex).
func newPointerAddress(
	t *testing.T,
	paymentKey []byte,
	slot, txIndex, certIndex uint64,
) lcommon.Address {
	t.Helper()
	raw := []byte{0x40}
	raw = append(raw, paymentKey...)
	raw = append(raw, pointerVarNat(slot)...)
	raw = append(raw, pointerVarNat(txIndex)...)
	raw = append(raw, pointerVarNat(certIndex)...)
	addr, err := lcommon.NewAddressFromBytes(raw)
	require.NoError(t, err)
	require.IsType(t, lcommon.AddressPayloadPointer{}, addr.StakingPayload(),
		"fixture must be a pointer address")
	return addr
}

type pointerStakeFixture struct {
	store    *sqlstore.Store
	db       *sql.DB
	pool     []byte
	stakeKey lcommon.CredentialHash
	nextTx   byte
}

func newPointerStakeFixture(t *testing.T) *pointerStakeFixture {
	t.Helper()
	store, db := newSharedSQLStore(t)
	return &pointerStakeFixture{
		store: store,
		db:    db,
		pool:  bytes.Repeat([]byte{0xF1}, 28),
		stakeKey: lcommon.NewBlake2b224(
			bytes.Repeat([]byte{0x31}, lcommon.AddressHashSize),
		),
		nextTx: 1,
	}
}

// setEra records the epoch containing every slot the fixture uses. The stake
// query resolves the era from this row; the length is deliberately wide enough
// to cover the whole fixture.
func (f *pointerStakeFixture) setEra(t *testing.T, eraID uint) {
	t.Helper()
	require.NoError(t, f.store.SetEpoch(
		0, 0, nil, nil, nil, nil, eraID, 1, 1_000_000, nil,
	))
}

// apply writes one transaction as block index blockIndex of the block at slot,
// with the given certificates and produced outputs.
func (f *pointerStakeFixture) apply(
	t *testing.T,
	slot uint64,
	blockIndex uint32,
	certificates []lcommon.Certificate,
	outputs ...lcommon.TransactionOutput,
) {
	t.Helper()
	hash := lcommon.Blake2b256{}
	hash[0] = f.nextTx
	f.nextTx++
	produced := make([]lcommon.Utxo, 0, len(outputs))
	for i, output := range outputs {
		produced = append(produced, lcommon.Utxo{
			Id: mockTransactionInput{
				hash:  hash,
				index: uint32(i), //nolint:gosec // small test index
			},
			Output: output,
		})
	}
	deposits := make(map[int]uint64, len(certificates))
	for i := range certificates {
		deposits[i] = 0
	}
	require.NoError(t, f.store.SetTransaction(
		&mockTransaction{
			hash:         hash,
			isValid:      true,
			certificates: certificates,
			produced:     produced,
		},
		ocommon.Point{Slot: slot, Hash: bytes.Repeat([]byte{0xc1}, 32)},
		blockIndex,
		deposits,
		false,
		nil,
	))
}

// applyGap writes one transaction through the Mithril gap path, which has no
// consumed-input state. deposits is what mithril's gapCertDeposits derives from
// the epoch's protocol parameters; a certificate absent from it records NULL.
func (f *pointerStakeFixture) applyGap(
	t *testing.T,
	slot uint64,
	blockIndex uint32,
	certificates []lcommon.Certificate,
	deposits map[int]uint64,
) {
	t.Helper()
	hash := lcommon.Blake2b256{}
	hash[0] = f.nextTx
	f.nextTx++
	require.NoError(t, f.store.SetGapBlockTransaction(
		&mockTransaction{
			hash:         hash,
			isValid:      true,
			certificates: certificates,
		},
		ocommon.Point{Slot: slot, Hash: bytes.Repeat([]byte{0xc3}, 32)},
		blockIndex,
		deposits,
		nil,
	))
}

// spend consumes an input at slot, marking the output it names deleted.
func (f *pointerStakeFixture) spend(
	t *testing.T,
	slot uint64,
	input lcommon.TransactionInput,
) {
	t.Helper()
	hash := lcommon.Blake2b256{}
	hash[0] = f.nextTx
	f.nextTx++
	require.NoError(t, f.store.SetTransaction(
		&mockTransaction{
			hash:     hash,
			isValid:  true,
			consumed: []lcommon.TransactionInput{input},
		},
		ocommon.Point{Slot: slot, Hash: bytes.Repeat([]byte{0xc2}, 32)},
		0,
		nil,
		false,
		nil,
	))
}

func (f *pointerStakeFixture) register() lcommon.Certificate {
	return &lcommon.StakeRegistrationCertificate{
		CertType: uint(lcommon.CertificateTypeStakeRegistration),
		StakeCredential: lcommon.Credential{
			CredType: 0, Credential: f.stakeKey,
		},
	}
}

func (f *pointerStakeFixture) deregister() lcommon.Certificate {
	return &lcommon.StakeDeregistrationCertificate{
		CertType: uint(lcommon.CertificateTypeStakeDeregistration),
		StakeCredential: lcommon.Credential{
			CredType: 0, Credential: f.stakeKey,
		},
	}
}

func (f *pointerStakeFixture) delegate() lcommon.Certificate {
	credential := lcommon.Credential{CredType: 0, Credential: f.stakeKey}
	return &lcommon.StakeDelegationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeDelegation),
		StakeCredential: &credential,
		PoolKeyHash:     lcommon.PoolKeyHash(f.pool),
	}
}

func (f *pointerStakeFixture) output(
	amount int64,
	address lcommon.Address,
) lcommon.TransactionOutput {
	return &mockTransactionOutput{
		amount:  big.NewInt(amount),
		address: address,
	}
}

func (f *pointerStakeFixture) baseAddress(t *testing.T) lcommon.Address {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x21}, lcommon.AddressHashSize),
		f.stakeKey.Bytes(),
	)
	require.NoError(t, err)
	return addr
}

func (f *pointerStakeFixture) stakeAt(t *testing.T, slot uint64) uint64 {
	t.Helper()
	stakes, _, err := f.store.GetStakeByPoolsAtSlot(
		[][]byte{f.pool}, slot, 0, 0, nil,
	)
	require.NoError(t, err)
	return stakes[string(f.pool)]
}

// TestPointerAddressStakeReachesItsCredential is the regression,
// driven end to end through Store.SetTransaction and the historical stake
// query rather than against the resolver in isolation.
//
// A pointer address carries the position of a stake registration certificate
// instead of a credential, so the produced utxo row has no staking_key and the
// output's value never reached the stake distribution -- understating the
// producing pool's stake until the node rejected a canonical block.
func TestPointerAddressStakeReachesItsCredential(t *testing.T) {
	t.Parallel()
	f := newPointerStakeFixture(t)
	f.setEra(t, babbage.EraIdBabbage)
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)

	// The registration this pointer names, at (100, 0, 0), plus the
	// delegation that puts the credential under the pool.
	f.apply(t, 100, 0, []lcommon.Certificate{f.register(), f.delegate()})
	// A base-address output for the same credential, so the assertion
	// isolates the pointer's contribution rather than the whole query.
	f.apply(t, 150, 0, nil, f.output(700, f.baseAddress(t)))
	f.apply(t, 200, 0, nil,
		f.output(600, newPointerAddress(t, paymentKey, 100, 0, 0)))

	require.Equal(t, uint64(1_300), f.stakeAt(t, 300),
		"stake held at a pointer address must reach the credential the "+
			"pointer designates")
}

// TestPointerAddressStakeRejectsMismatchedPositions pins each of the three
// pointer components. A resolution that ignored any one of them would credit a
// pool with stake the ledger does not.
func TestPointerAddressStakeRejectsMismatchedPositions(t *testing.T) {
	t.Parallel()
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)
	for _, tc := range []struct {
		name                     string
		slot, txIndex, certIndex uint64
		want                     uint64
	}{
		{name: "the named position resolves", slot: 100, want: 1_300},
		{name: "a different slot does not", slot: 101, want: 700},
		{
			name: "a different transaction index does not",
			slot: 100, txIndex: 1, want: 700,
		},
		{
			name: "a different certificate index does not",
			slot: 100, certIndex: 1, want: 700,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newPointerStakeFixture(t)
			f.setEra(t, babbage.EraIdBabbage)
			// Registration at (100, 0, 0); the delegation follows it in the
			// same transaction, at certificate index 1.
			f.apply(
				t, 100, 0,
				[]lcommon.Certificate{f.register(), f.delegate()},
			)
			f.apply(t, 150, 0, nil, f.output(700, f.baseAddress(t)))
			f.apply(t, 200, 0, nil, f.output(600, newPointerAddress(
				t, paymentKey, tc.slot, tc.txIndex, tc.certIndex,
			)))
			require.Equal(t, tc.want, f.stakeAt(t, 300))
		})
	}
}

// TestPointerAddressStakeIndexesEveryCertificate covers the highest-value
// invariant in the resolution: a pointer's third component counts every
// certificate in the transaction, not only the registrations. The reference
// mints the Ptr with CertIx (length gamma), gamma being all certificates
// processed so far.
//
// The registration here is the second certificate of its transaction, so it
// sits at index 1 and index 0 holds an unrelated delegation.
func TestPointerAddressStakeIndexesEveryCertificate(t *testing.T) {
	t.Parallel()
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)
	otherKey := bytes.Repeat([]byte{0x41}, lcommon.AddressHashSize)
	for _, tc := range []struct {
		name      string
		certIndex uint64
		want      uint64
	}{
		{
			name:      "the registration is at the index counting all certificates",
			certIndex: 1,
			want:      1_300,
		},
		{
			name:      "the index of the preceding delegation resolves to nothing",
			certIndex: 0,
			want:      700,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newPointerStakeFixture(t)
			f.setEra(t, babbage.EraIdBabbage)
			otherCredential := lcommon.Credential{
				CredType:   0,
				Credential: lcommon.NewBlake2b224(otherKey),
			}
			// Certificate 0 is another account's delegation; the registration
			// under test is certificate 1.
			f.apply(t, 100, 0, []lcommon.Certificate{
				&lcommon.StakeDelegationCertificate{
					CertType: uint(
						lcommon.CertificateTypeStakeDelegation,
					),
					StakeCredential: &otherCredential,
					PoolKeyHash:     lcommon.PoolKeyHash(f.pool),
				},
				f.register(),
			})
			// The delegation of the credential under test follows, so its
			// position is after the registration's.
			f.apply(t, 120, 0, []lcommon.Certificate{f.delegate()})
			f.apply(t, 150, 0, nil, f.output(700, f.baseAddress(t)))
			f.apply(t, 200, 0, nil, f.output(600, newPointerAddress(
				t, paymentKey, 100, 0, tc.certIndex,
			)))
			require.Equal(t, tc.want, f.stakeAt(t, 300))
		})
	}
}

// TestPointerAddressStakeIsEraGated covers the Conway divergence. Shelley
// through Babbage carry sisPtrStake alongside the credential map, so pointer
// stake counts. ConwayInstantStake has no pointer map at all and the
// Babbage->Conway translation drops saPtrs, so pointer stake stops counting for
// every such output -- including one produced long before the fork, which is
// the case every network is in today.
func TestPointerAddressStakeIsEraGated(t *testing.T) {
	t.Parallel()
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)
	for _, tc := range []struct {
		name  string
		eraID uint
		want  uint64
	}{
		{name: "babbage counts pointer stake", eraID: babbage.EraIdBabbage, want: 1_300},
		{name: "conway does not", eraID: conway.EraIdConway, want: 700},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newPointerStakeFixture(t)
			f.setEra(t, tc.eraID)
			f.apply(
				t, 100, 0,
				[]lcommon.Certificate{f.register(), f.delegate()},
			)
			f.apply(t, 150, 0, nil, f.output(700, f.baseAddress(t)))
			f.apply(t, 200, 0, nil, f.output(600, newPointerAddress(
				t, paymentKey, 100, 0, 0,
			)))
			require.Equal(t, tc.want, f.stakeAt(t, 300))
		})
	}
}

// TestPointerAddressStakeEraGateUsesTheIncomingEpochAtTheBoundary pins the
// era-cutover finding left open on the PR: cardano-ledger's hard-fork
// combinator translates the ledger state into the incoming era in
// extendToSlot, and SNAP runs inside TICK for the incoming epoch's first
// slot -- after that translation. So the mark snapshot at a Babbage->Conway
// boundary is produced under ConwayInstantStake, which drops sisPtrStake,
// even though the evaluated slot (boundarySlot-1, the outgoing epoch's last
// slot) is still Babbage.
//
// GetEpochBoundaryStakeByPools' contract is boundarySlot == snapshotSlot+1
// (see GetEpochBoundaryStakeByPools and boundaryRewardSlot), so this pins the
// era gate against the incoming epoch's era, not the outgoing one, in both
// directions: a boundary that stays in Babbage must still count the pointer,
// and one that crosses into Conway must not.
func TestPointerAddressStakeEraGateUsesTheIncomingEpochAtTheBoundary(
	t *testing.T,
) {
	t.Parallel()
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)
	for _, tc := range []struct {
		name          string
		incomingEraID uint
		want          uint64
	}{
		{
			name:          "the incoming epoch is still Babbage",
			incomingEraID: babbage.EraIdBabbage,
			want:          1_300,
		},
		{
			name:          "the incoming epoch is Conway",
			incomingEraID: conway.EraIdConway,
			want:          700,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newPointerStakeFixture(t)
			// The outgoing epoch (10, Babbage) covers every slot the fixture
			// writes to; the incoming epoch (11) starts exactly at the
			// boundary slot and carries the era under test.
			require.NoError(t, f.store.SetEpoch(
				0, 10, nil, nil, nil, nil, babbage.EraIdBabbage, 1, 300, nil,
			))
			require.NoError(t, f.store.SetEpoch(
				300, 11, nil, nil, nil, nil, tc.incomingEraID, 1, 1_000_000,
				nil,
			))

			f.apply(
				t, 100, 0,
				[]lcommon.Certificate{f.register(), f.delegate()},
			)
			f.apply(t, 150, 0, nil, f.output(700, f.baseAddress(t)))
			f.apply(t, 200, 0, nil, f.output(600, newPointerAddress(
				t, paymentKey, 100, 0, 0,
			)))

			// A plain "stake at slot" query for the outgoing epoch's last
			// slot is unaffected by the boundary gate: it resolves the era
			// at that slot directly, and that slot is still Babbage either
			// way.
			plainStakes, _, err := f.store.GetStakeByPoolsAtSlot(
				[][]byte{f.pool}, 299, 0, 0, nil,
			)
			require.NoError(t, err)
			require.Equal(t, uint64(1_300), plainStakes[string(f.pool)],
				"a plain historical query at the outgoing epoch's last slot "+
					"must still see Babbage, regardless of the incoming era")

			boundaryStakes, _, err := f.store.GetEpochBoundaryStakeByPools(
				[][]byte{f.pool}, 299, 300, 0, 0, nil,
			)
			require.NoError(t, err)
			require.Equal(t, tc.want, boundaryStakes[string(f.pool)],
				"the epoch-boundary query must resolve the era the mark "+
					"snapshot is actually produced under: the incoming epoch")
		})
	}
}

// TestPointerAddressStakeCountsAForwardPointer covers a pointer that names a
// position no certificate occupies yet. Nothing in any era validates an
// address's pointer payload, so an output may be produced first; the reference
// resolves the Ptr at snapshot time, so the stake starts counting once the
// registration lands.
func TestPointerAddressStakeCountsAForwardPointer(t *testing.T) {
	t.Parallel()
	f := newPointerStakeFixture(t)
	f.setEra(t, babbage.EraIdBabbage)
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)

	// The output comes first, naming a position that does not exist yet.
	f.apply(t, 100, 0, nil,
		f.output(600, newPointerAddress(t, paymentKey, 200, 0, 0)))
	require.Equal(t, uint64(0), f.stakeAt(t, 150),
		"a pointer naming no certificate confers no stake")

	// The registration then lands at exactly that position.
	f.apply(t, 200, 0, []lcommon.Certificate{f.register(), f.delegate()})
	require.Equal(t, uint64(600), f.stakeAt(t, 300),
		"the pointer must resolve once its certificate is on chain")
}

// TestPointerAddressStakeStopsWhenTheOutputIsSpent pins the liveness bound on
// the pointer branch. Stake is held by the output, not by the pointer, so an
// output spent before the evaluated slot confers none -- and one spent after it
// still does.
func TestPointerAddressStakeStopsWhenTheOutputIsSpent(t *testing.T) {
	t.Parallel()
	f := newPointerStakeFixture(t)
	f.setEra(t, babbage.EraIdBabbage)
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)

	f.apply(t, 100, 0, []lcommon.Certificate{f.register(), f.delegate()})
	pointerTx := f.nextTx
	f.apply(t, 150, 0, nil,
		f.output(600, newPointerAddress(t, paymentKey, 100, 0, 0)))

	spentHash := lcommon.Blake2b256{}
	spentHash[0] = pointerTx
	f.spend(t, 200, mockTransactionInput{hash: spentHash, index: 0})

	require.Equal(t, uint64(600), f.stakeAt(t, 175),
		"the output was still live at this slot")
	require.Equal(t, uint64(0), f.stakeAt(t, 250),
		"a spent pointer output confers no stake")
}

// TestPointerAddressStakeStopsAtDeregistration covers removePtr: de-registering
// the credential deletes the Ptr, so the address is permanently dangling. A
// later re-registration mints a Ptr at a new position, which the old address
// does not name, so it must not revive the old one.
func TestPointerAddressStakeStopsAtDeregistration(t *testing.T) {
	t.Parallel()
	f := newPointerStakeFixture(t)
	f.setEra(t, babbage.EraIdBabbage)
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)

	f.apply(t, 100, 0, []lcommon.Certificate{f.register(), f.delegate()})
	f.apply(t, 150, 0, nil,
		f.output(600, newPointerAddress(t, paymentKey, 100, 0, 0)))
	require.Equal(t, uint64(600), f.stakeAt(t, 175),
		"the pointer resolves while its registration stands")

	f.apply(t, 200, 0, []lcommon.Certificate{f.deregister()})
	require.Equal(t, uint64(0), f.stakeAt(t, 225),
		"a de-registered credential holds no stake at all")

	// Re-registered at a new position, so the credential is delegated again --
	// but the address names the old, removed Ptr.
	f.apply(t, 250, 0, []lcommon.Certificate{f.register(), f.delegate()})
	f.apply(t, 260, 0, nil, f.output(700, f.baseAddress(t)))
	require.Equal(t, uint64(700), f.stakeAt(t, 300),
		"a re-registration mints a new Ptr; the old address stays dangling")
}

// TestGetPointerStakeInputsForPoolsStopsAtDeregistration covers the live
// snapshot overlay (GetPointerStakeInputsForPools) directly, rather than only
// through GetStakeByPoolsAtSlot's embedded CTE. It shares removePtr with the
// historical query via the same pointerResolutionSQL: a de-registered
// credential's old pointer stays dangling even after a re-registration
// elsewhere.
func TestGetPointerStakeInputsForPoolsStopsAtDeregistration(t *testing.T) {
	t.Parallel()
	f := newPointerStakeFixture(t)
	f.setEra(t, babbage.EraIdBabbage)
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)

	f.apply(t, 100, 0, []lcommon.Certificate{f.register(), f.delegate()})
	f.apply(t, 150, 0, nil,
		f.output(600, newPointerAddress(t, paymentKey, 100, 0, 0)))

	inputs, err := f.store.GetPointerStakeInputsForPools(
		[][]byte{f.pool}, 175, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Len(
		t,
		inputs,
		1,
		"the pointer resolves while its registration stands",
	)
	require.Equal(t, uint64(600), uint64(inputs[0].Stake))

	f.apply(t, 200, 0, []lcommon.Certificate{f.deregister()})
	inputs, err = f.store.GetPointerStakeInputsForPools(
		[][]byte{f.pool}, 225, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Empty(t, inputs, "a de-registered credential holds no stake at all")

	// Re-registered at a new position; the address still names the old,
	// removed Ptr.
	f.apply(t, 250, 0, []lcommon.Certificate{f.register(), f.delegate()})
	inputs, err = f.store.GetPointerStakeInputsForPools(
		[][]byte{f.pool}, 300, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Empty(
		t,
		inputs,
		"a re-registration mints a new Ptr; the old address stays dangling",
	)
}

// TestPointerAddressStakeResolvesTheWedgeAddress runs the real bech32 address
// from the Preview wedge, so the fixture is the on-chain encoding rather than
// this test's own pointer writer.
func TestPointerAddressStakeResolvesTheWedgeAddress(t *testing.T) {
	t.Parallel()
	f := newPointerStakeFixture(t)
	f.setEra(t, babbage.EraIdBabbage)
	addr, err := lcommon.NewAddress(wedgePointerAddress)
	require.NoError(t, err)
	pointer, ok := addr.StakingPayload().(lcommon.AddressPayloadPointer)
	require.True(t, ok)
	require.Equal(t, uint64(wedgePointerSlot), pointer.Slot)
	require.Equal(t, uint64(wedgePointerTxIndex), pointer.TxIndex)
	require.Equal(t, uint64(wedgeCertIndex), pointer.CertIndex)

	// The registration at (2940289, 1, 0): transaction index 1 of that block.
	f.apply(
		t, wedgePointerSlot, wedgePointerTxIndex,
		[]lcommon.Certificate{f.register(), f.delegate()},
	)
	f.apply(t, wedgePointerSlot+1, 0, nil, f.output(35_553_515_656, addr))

	require.Equal(t, uint64(35_553_515_656), f.stakeAt(t, wedgePointerSlot+10))
}

// TestPointerAddressStakeResolvesAnInGapRegistration covers the case both
// review bots raised against resolving at ingest: a pointer whose registration
// certificate lands inside a Mithril gap block. Gap ingestion supplies no
// deposits and no input state, and until recently wrote no certificate rows at
// all, so nothing was available to resolve against at the moment the output was
// applied.
//
// Resolving at the slot being evaluated removes the ordering requirement
// entirely -- the registration only has to be in the database by the time
// stake is computed, not by the time the output is written.
func TestPointerAddressStakeResolvesAnInGapRegistration(t *testing.T) {
	t.Parallel()
	f := newPointerStakeFixture(t)
	f.setEra(t, babbage.EraIdBabbage)
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)

	// Both the registration and the delegation arrive through the gap path.
	f.applyGap(
		t, 100, 0,
		[]lcommon.Certificate{f.register(), f.delegate()},
		map[int]uint64{0: 2_000_000},
	)
	f.apply(t, 200, 0, nil,
		f.output(600, newPointerAddress(t, paymentKey, 100, 0, 0)))

	require.Equal(t, uint64(600), f.stakeAt(t, 300),
		"a pointer naming a registration ingested through a gap block must "+
			"still resolve")
}

// TestPointerAddressStakeToleratesAnUnrepresentablePosition covers the
// availability hazard in recording the position: nothing validates a pointer
// payload, and gouroboros decodes each component with an unbounded
// shift-accumulate loop, so a spendable output can name a position above
// int64. The columns holding it are signed, so the value cannot be stored.
//
// The block still has to apply. Failing the write would stall ingestion of a
// block the network accepted -- the same class of failure exists to
// avoid -- and the position names no certificate in any case, so the output is
// simply left unattributed, as a pointer to an unoccupied position is.
func TestPointerAddressStakeToleratesAnUnrepresentablePosition(t *testing.T) {
	t.Parallel()
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)
	for _, tc := range []struct {
		name                     string
		slot, txIndex, certIndex uint64
	}{
		{name: "slot", slot: math.MaxInt64 + 1},
		{name: "transaction index", slot: 100, txIndex: math.MaxInt64 + 1},
		{name: "certificate index", slot: 100, certIndex: math.MaxInt64 + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newPointerStakeFixture(t)
			f.setEra(t, babbage.EraIdBabbage)
			f.apply(
				t, 100, 0,
				[]lcommon.Certificate{f.register(), f.delegate()},
			)
			f.apply(t, 150, 0, nil, f.output(700, f.baseAddress(t)))
			// f.apply requires SetTransaction to succeed, so the block
			// applying at all is the first half of the assertion.
			f.apply(t, 200, 0, nil, f.output(600, newPointerAddress(
				t, paymentKey, tc.slot, tc.txIndex, tc.certIndex,
			)))
			require.Equal(t, uint64(700), f.stakeAt(t, 300),
				"an unrepresentable pointer position confers no stake")
		})
	}
}

// createUtxo writes one output through Store.CreateUtxo, the direct-write path
// snapshot import and the conformance harness use, rather than through a block
// apply. It performs the same models.UtxoLedgerToModel conversion the block
// path does, so anything the conversion produces has to survive this write too.
func (f *pointerStakeFixture) createUtxo(
	t *testing.T,
	slot uint64,
	output lcommon.TransactionOutput,
) {
	t.Helper()
	hash := lcommon.Blake2b256{}
	hash[0] = f.nextTx
	f.nextTx++
	model, err := models.UtxoLedgerToModel(
		lcommon.Utxo{
			Id:     mockTransactionInput{hash: hash, index: 0},
			Output: output,
		},
		slot,
	)
	require.NoError(t, err)
	require.NoError(t, f.store.CreateUtxo(nil, &model))
}

// TestPointerAddressStakeSurvivesCreateUtxo pins the pointer position across
// Store.CreateUtxo, not only across the block-apply path.
//
// A pointer address's stake reference is a position rather than a credential,
// so it is persisted in utxo_pointer rather than in a utxo column.
// UtxoLedgerToModel sets it for every caller, but only the block-apply path
// wrote it: CreateUtxo built its row from createUtxoParams, which has no
// pointer field, and dropped the position silently. Snapshot import and
// internal/test/conformance both write produced outputs this way, so a pointer
// output written through them conferred no stake -- and the conformance
// harness, which states that it reuses the production conversion, could not
// observe pointer stake at all.
func TestPointerAddressStakeSurvivesCreateUtxo(t *testing.T) {
	t.Parallel()
	f := newPointerStakeFixture(t)
	f.setEra(t, babbage.EraIdBabbage)
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)

	// The registration this pointer names, at (100, 0, 0), and the
	// delegation putting the credential under the pool.
	f.apply(t, 100, 0, []lcommon.Certificate{f.register(), f.delegate()})
	// A base-address output written the same way, so the assertion
	// isolates the pointer's contribution rather than CreateUtxo as such.
	f.createUtxo(t, 150, f.output(700, f.baseAddress(t)))
	f.createUtxo(t, 200,
		f.output(600, newPointerAddress(t, paymentKey, 100, 0, 0)))

	require.Equal(t, uint64(1_300), f.stakeAt(t, 300),
		"a pointer output written through CreateUtxo must reach the "+
			"credential its position designates")
}

// TestPointerRowsAreRemovedByRollback pins the half of the pointer's lifecycle
// that only rollback exercises. The position lives in utxo_pointer rather than
// on the utxo row, and nothing deletes it explicitly: it goes with its utxo
// through the migration's ON DELETE CASCADE, which SQLite honours only because
// the connection sets foreign_keys(1).
//
// So a dropped pragma, or a dialect translation that loses the constraint,
// would orphan the row rather than fail loudly — and the rolled-back output
// would keep conferring stake on the credential its position names, which is
// exactly the attribution error this table exists to get right.
func TestPointerRowsAreRemovedByRollback(t *testing.T) {
	t.Parallel()
	f := newPointerStakeFixture(t)
	f.setEra(t, babbage.EraIdBabbage)
	paymentKey := bytes.Repeat([]byte{0x77}, lcommon.AddressHashSize)

	f.apply(t, 100, 0, []lcommon.Certificate{f.register(), f.delegate()})
	f.apply(t, 200, 0, nil,
		f.output(600, newPointerAddress(t, paymentKey, 100, 0, 0)))

	require.Equal(t, uint64(600), f.stakeAt(t, 300),
		"precondition: the pointer output reaches its credential")
	require.Equal(t, 1, f.pointerRowCount(t),
		"precondition: the position was persisted")

	require.NoError(t, f.store.DeleteTransactionsAfterSlot(150, nil))

	require.Zero(t, f.pointerRowCount(t),
		"a rolled-back output must not leave its pointer position behind")
	require.Zero(t, f.stakeAt(t, 300),
		"a rolled-back pointer output must stop conferring stake")
}

// pointerRowCount reports how many pointer positions the store currently holds.
func (f *pointerStakeFixture) pointerRowCount(t *testing.T) int {
	t.Helper()
	var n int
	require.NoError(t, f.db.QueryRow(
		"SELECT COUNT(*) FROM utxo_pointer",
	).Scan(&n))
	return n
}

// TestGetPointerStakeInputsForPoolsAppliesTheLiveInactivityGate pins the
// pointer overlay to the same CIP-0163 gate GetLiveStakeInputsForPools applies
// to the base-address side of the very same query.
//
// The snapshot path's live route adds the overlay's rows onto
// GetLiveStakeInputsForPools's, so the two must agree about which credentials
// are inactive. GetLiveStakeInputsForPools reads the mutable
// account.expiration_epoch column, not the historical fallback's witness
// reconstruction, and the overlay has to read the same column or a credential
// would contribute its pointer stake to a snapshot that excluded its
// base-address stake.
func TestGetPointerStakeInputsForPoolsAppliesTheLiveInactivityGate(
	t *testing.T,
) {
	t.Parallel()
	f := newPointerStakeFixture(t)
	f.setEra(t, babbage.EraIdBabbage)
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)

	f.apply(t, 100, 0, []lcommon.Certificate{f.register(), f.delegate()})
	f.apply(t, 150, 0, nil, f.output(700, f.baseAddress(t)))
	f.apply(t, 200, 0, nil,
		f.output(600, newPointerAddress(t, paymentKey, 100, 0, 0)))

	// The credential's last witness leaves it expiring at epoch 5.
	_, err := f.db.Exec(
		"UPDATE account SET expiration_epoch = 5 WHERE staking_key = ?",
		f.stakeKey.Bytes(),
	)
	require.NoError(t, err)

	pointerStake := func(expiryEpoch uint64) uint64 {
		t.Helper()
		inputs, err := f.store.GetPointerStakeInputsForPools(
			[][]byte{f.pool}, 300, 0, expiryEpoch, nil,
		)
		require.NoError(t, err)
		var total uint64
		for _, input := range inputs {
			total += uint64(input.Stake)
		}
		return total
	}
	liveStake := func(expiryEpoch uint64) uint64 {
		t.Helper()
		inputs, err := f.store.GetLiveStakeInputsForPools(
			[][]byte{f.pool}, expiryEpoch, nil,
		)
		require.NoError(t, err)
		var total uint64
		for _, input := range inputs {
			total += uint64(input.Stake)
		}
		return total
	}

	require.Equal(t, uint64(600), pointerStake(0),
		"with the gate off the overlay reports the pointer stake")
	require.Equal(t, uint64(700), liveStake(0),
		"with the gate off the live aggregate reports the base stake")

	// A snapshot for an epoch the account is still active in: both sides
	// report.
	require.Equal(t, uint64(600), pointerStake(5))
	require.Equal(t, uint64(700), liveStake(5))

	// A snapshot for an epoch past the expiration: both sides must drop the
	// credential, or the two halves of one live-route snapshot disagree.
	require.Equal(t, uint64(0), liveStake(6),
		"the live aggregate drops an expired credential")
	require.Equal(t, uint64(0), pointerStake(6),
		"the overlay must drop the same credential the live aggregate did")
}

// newScriptPointerAddress builds a testnet type-5 (pointer, script payment)
// address naming the certificate at (slot, txIndex, certIndex). Type 5 differs
// from type 4 only in the payment credential's kind, and the staking half is
// the same pointer payload, so the resolution must not depend on which of the
// two an output used.
func newScriptPointerAddress(
	t *testing.T,
	scriptHash []byte,
	slot, txIndex, certIndex uint64,
) lcommon.Address {
	t.Helper()
	raw := []byte{0x50}
	raw = append(raw, scriptHash...)
	raw = append(raw, pointerVarNat(slot)...)
	raw = append(raw, pointerVarNat(txIndex)...)
	raw = append(raw, pointerVarNat(certIndex)...)
	addr, err := lcommon.NewAddressFromBytes(raw)
	require.NoError(t, err)
	require.IsType(t, lcommon.AddressPayloadPointer{}, addr.StakingPayload(),
		"fixture must be a pointer address")
	require.Equal(t, uint8(lcommon.AddressTypeScriptPointer), addr.Type(),
		"fixture must be a type-5 pointer address")
	return addr
}

// TestPointerAddressStakeResolvesAScriptPaymentPointer covers the type-5
// pointer address. Both pointer address types reach the same
// StakingPayload() branch in UtxoLedgerToModel, so an output paying a script
// must record and resolve its pointer exactly as a key-payment one does.
func TestPointerAddressStakeResolvesAScriptPaymentPointer(t *testing.T) {
	t.Parallel()
	f := newPointerStakeFixture(t)
	f.setEra(t, babbage.EraIdBabbage)
	scriptHash := bytes.Repeat([]byte{0x33}, lcommon.AddressHashSize)

	f.apply(t, 100, 0, []lcommon.Certificate{f.register(), f.delegate()})
	f.apply(t, 200, 0, nil, f.output(600, newScriptPointerAddress(
		t, scriptHash, 100, 0, 0,
	)))

	require.Equal(t, uint64(600), f.stakeAt(t, 300),
		"a type-5 pointer address resolves to the same credential a type-4 "+
			"one naming the same position does")
}

// TestPointerAddressStakeIsNotCountedWithoutAnEraRow covers pointerStakeCounted's
// fail-safe: a database with no epoch row covering the evaluated slot has an
// unresolvable era, and pointer stake is not counted there. That understates a
// pool rather than inflating it and the shared active-stake denominator, which
// is the direction that tightens the leader threshold rather than loosening it.
func TestPointerAddressStakeIsNotCountedWithoutAnEraRow(t *testing.T) {
	t.Parallel()
	f := newPointerStakeFixture(t)
	// Deliberately no setEra: the epoch table stays empty.
	paymentKey := bytes.Repeat([]byte{0x22}, lcommon.AddressHashSize)

	f.apply(t, 100, 0, []lcommon.Certificate{f.register(), f.delegate()})
	f.apply(t, 150, 0, nil, f.output(700, f.baseAddress(t)))
	f.apply(t, 200, 0, nil,
		f.output(600, newPointerAddress(t, paymentKey, 100, 0, 0)))

	require.Equal(t, uint64(700), f.stakeAt(t, 300),
		"an unresolvable era must not count pointer stake")

	inputs, err := f.store.GetPointerStakeInputsForPools(
		[][]byte{f.pool}, 300, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Empty(t, inputs,
		"the live overlay applies the same fail-safe as the historical CTE")
}

// seedPoolRegistration inserts one pool_registration row for the pool,
// creating the pool row on first use.
func seedPoolRegistration(
	t *testing.T,
	raw *sql.DB,
	poolKeyHash, vrfKeyHash []byte,
	addedSlot uint64,
) {
	t.Helper()
	var poolID int64
	err := raw.QueryRow(
		`SELECT id FROM pool WHERE pool_key_hash = ?`, poolKeyHash,
	).Scan(&poolID)
	if err != nil {
		res, ierr := raw.Exec(
			`INSERT INTO pool (pool_key_hash, vrf_key_hash) VALUES (?, ?)`,
			poolKeyHash, vrfKeyHash,
		)
		require.NoError(t, ierr)
		poolID, ierr = res.LastInsertId()
		require.NoError(t, ierr)
	} else {
		_, uerr := raw.Exec(
			`UPDATE pool SET vrf_key_hash = ? WHERE id = ?`,
			vrfKeyHash, poolID,
		)
		require.NoError(t, uerr)
	}
	_, err = raw.Exec(`
INSERT INTO pool_registration (pool_id, pool_key_hash, vrf_key_hash, added_slot)
VALUES (?, ?, ?, ?)`,
		poolID, poolKeyHash, vrfKeyHash, addedSlot,
	)
	require.NoError(t, err)
}

// TestGetPoolVrfKeyHashAtSlotFollowsRotation is the regression,
// built from the rotation that wedged a Preview replay at epoch 38.
//
// The pool ran on one VRF key from slot 1014930, rotated to a second at slot
// 3279920, and produced a block at slot 3362555. The snapshot that elected that
// block was captured at slot 3196799 — before the rotation — so the header
// legitimately carries the old key. Reading the pool's current registration
// yields the new key and rejects a canonical block.
func TestGetPoolVrfKeyHashAtSlotFollowsRotation(t *testing.T) {
	t.Parallel()
	store, raw := newSharedSQLStore(t)

	pool := bytes.Repeat([]byte{0x11}, 28)
	oldKey := bytes.Repeat([]byte{0xB5}, 32)
	newKey := bytes.Repeat([]byte{0xFA}, 32)

	seedPoolRegistration(t, raw, pool, oldKey, 1_014_930)
	seedPoolRegistration(t, raw, pool, oldKey, 2_479_516)
	seedPoolRegistration(t, raw, pool, newKey, 3_279_920)

	// At the electing snapshot's capture, the old key was in force.
	got, ok, err := store.GetPoolVrfKeyHashAtSlot(pool, 3_196_799, nil)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, oldKey, got,
		"a rotation after the capture must not change the electing key")

	// At and after the rotation, the new key is in force.
	got, ok, err = store.GetPoolVrfKeyHashAtSlot(pool, 3_279_920, nil)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, newKey, got)

	got, ok, err = store.GetPoolVrfKeyHashAtSlot(pool, 3_362_555, nil)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, newKey, got)
}

// TestGetPoolVrfKeyHashAtSlotBeforeFirstRegistration pins the absent case:
// before the pool ever registered there is no key, which is a different answer
// from a registration carrying no key.
func TestGetPoolVrfKeyHashAtSlotBeforeFirstRegistration(t *testing.T) {
	t.Parallel()
	store, raw := newSharedSQLStore(t)

	pool := bytes.Repeat([]byte{0x22}, 28)
	seedPoolRegistration(t, raw, pool, bytes.Repeat([]byte{0xAA}, 32), 5_000)

	got, ok, err := store.GetPoolVrfKeyHashAtSlot(pool, 4_999, nil)
	require.NoError(t, err)
	assert.False(t, ok, "no registration exists at or before this slot")
	assert.Nil(t, got)

	// An unknown pool is likewise absent rather than an error.
	got, ok, err = store.GetPoolVrfKeyHashAtSlot(
		bytes.Repeat([]byte{0x33}, 28), 10_000, nil,
	)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, got)
}

// TestGetPoolEarliestVrfKeyHashAtSlotResolvesTheFirstRegistration covers the
// ascending sibling, which answers a different question from
// GetPoolVrfKeyHashAtSlot: not "which registration was in force at this slot"
// but "which one did the pool first make at or before it".
//
// That is what cardano-ledger's psStakePools holds for a pool whose first
// registration lands inside the epoch a snapshot was captured in. The POOL rule
// inserts a first registration immediately and defers only a re-registration
// through psFutureStakePoolParams, so a re-registration made in the same epoch
// is not the key the snapshot carries — and resolving the latest instead of the
// earliest would pick exactly that deferred key.
func TestGetPoolEarliestVrfKeyHashAtSlotResolvesTheFirstRegistration(
	t *testing.T,
) {
	t.Parallel()
	store, raw := newSharedSQLStore(t)

	pool := bytes.Repeat([]byte{0x44}, 28)
	firstKey := bytes.Repeat([]byte{0xC3}, 32)
	secondKey := bytes.Repeat([]byte{0xB5}, 32)
	thirdKey := bytes.Repeat([]byte{0xFA}, 32)

	seedPoolRegistration(t, raw, pool, firstKey, 3_150_000)
	seedPoolRegistration(t, raw, pool, secondKey, 3_160_000)
	seedPoolRegistration(t, raw, pool, thirdKey, 3_290_000)

	// Capture slot 3196799 sees the first two registrations. The snapshot
	// carries the first, because the second was deferred past SNAP.
	got, ok, err := store.GetPoolEarliestVrfKeyHashAtSlot(pool, 3_196_799, nil)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, firstKey, got,
		"the earliest registration at or before the capture is the one "+
			"psStakePools holds")
	assert.NotEqual(t, secondKey, got,
		"a re-registration in the same epoch is deferred past SNAP")

	// Widening the slot must not change the answer: the question is anchored
	// at the pool's first registration, not at the slot.
	got, ok, err = store.GetPoolEarliestVrfKeyHashAtSlot(pool, 3_290_000, nil)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, firstKey, got)

	// Before the pool ever registered there is no key, matching the
	// descending sibling's absent case.
	got, ok, err = store.GetPoolEarliestVrfKeyHashAtSlot(pool, 3_149_999, nil)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, got)
}

// Two registrations for one pool at the same slot are not representable:
// pool_registration is UNIQUE on (pool_id, added_slot). The query still orders
// by block and certificate index after added_slot so it cannot disagree with
// GetActivePoolKeyHashesAtSlot, but that tie-break is unreachable here and so
// is not covered by a test that would have to violate the constraint to exist.

func TestRatificationHistorySurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	first, err := NewSQLStore(
		Config{DataDir: dataDir},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close() })
	require.NoError(t, first.Start(t.Context()))

	ratifiedEpoch := uint64(5)
	ratifiedSlot := uint64(550)
	proposal := &models.GovernanceProposal{
		TxHash:        []byte("restart-ratification-history"),
		ActionIndex:   0,
		ActionType:    6,
		ProposedEpoch: 1,
		ExpiresEpoch:  100,
		RatifiedEpoch: &ratifiedEpoch,
		RatifiedSlot:  &ratifiedSlot,
		AnchorURL:     "https://example.invalid/governance",
		AnchorHash:    []byte("restart-governance-anchor"),
		ReturnAddress: []byte("restart-return-address"),
		GovActionCbor: []byte{0x80},
		AddedSlot:     500,
	}
	write := first.Transaction(t.Context())
	require.NoError(t, first.SetGovernanceProposal(proposal, write))
	require.NoError(t, first.ClearGovernanceProposalRatification(
		proposal.TxHash,
		proposal.ActionIndex,
		600,
		write,
	))
	require.NoError(t, write.Commit())
	require.NoError(t, first.Close())

	second, err := NewSQLStore(
		Config{DataDir: dataDir},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	require.NoError(t, second.Start(t.Context()))

	read := second.ReadTransaction(t.Context())
	got, err := second.GetGovernanceProposal(
		proposal.TxHash,
		proposal.ActionIndex,
		read,
	)
	require.NoError(t, err)
	require.NoError(t, read.Rollback())
	require.NotNil(t, got)
	require.Nil(t, got.RatifiedEpoch)
	require.Nil(t, got.RatifiedSlot)

	rollback := second.Transaction(t.Context())
	require.NoError(t, second.DeleteGovernanceProposalsAfterSlot(599, rollback))
	require.NoError(t, rollback.Commit())

	read = second.ReadTransaction(t.Context())
	got, err = second.GetGovernanceProposal(
		proposal.TxHash,
		proposal.ActionIndex,
		read,
	)
	require.NoError(t, err)
	require.NoError(t, read.Rollback())
	require.NotNil(t, got)
	require.NotNil(t, got.RatifiedEpoch)
	require.NotNil(t, got.RatifiedSlot)
	require.Equal(t, uint64(5), *got.RatifiedEpoch)
	require.Equal(t, uint64(550), *got.RatifiedSlot)
}

// snapshotPoolKeyHash builds a distinct 28-byte pool key hash for index i.
func snapshotPoolKeyHash(i int) []byte {
	hash := make([]byte, 28)
	binary.BigEndian.PutUint32(hash, uint32(i)+1)
	return hash
}

// seedMarkSnapshots writes `count` mark-snapshot rows, pool i holding stake
// (i+1)*1000, and returns their key hashes in order.
func seedMarkSnapshots(
	t *testing.T,
	store interface {
		SavePoolStakeSnapshots([]*models.PoolStakeSnapshot, types.Txn) error
	},
	epoch uint64,
	count int,
) [][]byte {
	t.Helper()
	snapshots := make([]*models.PoolStakeSnapshot, 0, count)
	hashes := make([][]byte, 0, count)
	for i := range count {
		hash := snapshotPoolKeyHash(i)
		hashes = append(hashes, hash)
		snapshots = append(snapshots, &models.PoolStakeSnapshot{
			Epoch:        epoch,
			SnapshotType: "mark",
			PoolKeyHash:  hash,
			TotalStake:   types.Uint64(uint64(i+1) * 1000),
		})
	}
	require.NoError(t, store.SavePoolStakeSnapshots(snapshots, nil))
	return hashes
}

// TestGetPoolStakeSnapshotsForPoolsReturnsOnlyRequested covers the bounded
// read behind a GetPoolDistr2 pool filter.
//
// A pool the snapshot has no row for comes back absent rather than at zero
// stake: the two are different answers, and the caller distinguishes them.
func TestGetPoolStakeSnapshotsForPoolsReturnsOnlyRequested(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	const epoch = 7
	hashes := seedMarkSnapshots(t, store, epoch, 4)
	absent := snapshotPoolKeyHash(99)

	got, err := store.GetPoolStakeSnapshotsForPools(
		epoch,
		"mark",
		[][]byte{hashes[1], hashes[3], absent},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, got, 2, "only the pools the snapshot holds are returned")

	byPool := map[string]uint64{}
	for _, snapshot := range got {
		byPool[string(snapshot.PoolKeyHash)] = uint64(snapshot.TotalStake)
	}
	assert.Equal(t, uint64(2000), byPool[string(hashes[1])])
	assert.Equal(t, uint64(4000), byPool[string(hashes[3])])
	assert.NotContains(t, byPool, string(absent))

	// An empty filter is not a request for everything.
	empty, err := store.GetPoolStakeSnapshotsForPools(epoch, "mark", nil, nil)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

// TestGetPoolStakeSnapshotsForPoolsChunksBeyondParameterLimit covers a filter
// spanning more than one chunk.
//
// The store contracts to 999 parameters per statement on SQLite and spends two
// before the first pool key hash, so a filter naming more than 997 pools is
// split. What this pins is the merge across that split: every requested pool
// comes back exactly once, where an off-by-one would drop or duplicate the
// pools either side of it. It does not rest on the driver rejecting a longer
// statement -- SQLite itself accepts 32766 bound parameters, so a set this
// size would be accepted chunked or not.
func TestGetPoolStakeSnapshotsForPoolsChunksBeyondParameterLimit(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	const epoch = 3
	// Comfortably past the 997 that fit in one statement, so the split lands
	// mid-request rather than exactly at the end.
	const poolCount = 1500
	hashes := seedMarkSnapshots(t, store, epoch, poolCount)

	got, err := store.GetPoolStakeSnapshotsForPools(
		epoch,
		"mark",
		hashes,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, got, poolCount,
		"every requested pool must survive the chunk boundary")

	byPool := make(map[string]uint64, len(got))
	for _, snapshot := range got {
		byPool[string(snapshot.PoolKeyHash)] = uint64(snapshot.TotalStake)
	}
	require.Len(t, byPool, poolCount, "no pool may be returned twice")
	// Spot-check either side of the boundary rather than all 1500.
	for _, i := range []int{0, 996, 997, 998, poolCount - 1} {
		assert.Equal(t, uint64(i+1)*1000, byPool[string(hashes[i])],
			"pool %d must carry its own stake", i)
	}
}

// TestGetPoolStakeSnapshotsForPoolsDeduplicatesRepeatedHashes covers a pool
// named more than once in the same filter.
//
// This list is not the node's to choose: it arrives from the wire as the pool
// filter on GetPoolDistr2, and PoolFilter hands back the client's items
// verbatim without collapsing repeats. A client may therefore name a pool
// twice, and nothing upstream stops it.
//
// A single `IN (...)` would match that pool's row once regardless. Chunking
// breaks that when the two mentions fall either side of a split, since each
// chunk matches independently and the results are concatenated. The store's
// contract is one row per distinct pool it holds, so the repeats collapse
// before the list is chunked rather than after.
func TestGetPoolStakeSnapshotsForPoolsDeduplicatesRepeatedHashes(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	const epoch = 4
	const poolCount = 1500
	hashes := seedMarkSnapshots(t, store, epoch, poolCount)

	// The first pool named again at the end, so its two mentions straddle the
	// 997-parameter split. Within one chunk SQL collapses them anyway, which
	// is the behaviour being restored.
	requested := append(append([][]byte{}, hashes...), hashes[0])

	got, err := store.GetPoolStakeSnapshotsForPools(
		epoch,
		"mark",
		requested,
		nil,
	)
	require.NoError(t, err)

	counts := make(map[string]int, len(got))
	for _, snapshot := range got {
		counts[string(snapshot.PoolKeyHash)]++
	}
	assert.Equal(t, 1, counts[string(hashes[0])],
		"a pool named twice must still yield one row, as a single IN list "+
			"would have done")
	assert.Equal(t, poolCount, len(got),
		"the result is one row per distinct pool held, not per mention")
}

type sqliteTestResult struct{ Error error }

// sqliteTestDB is a deliberately tiny raw-SQL fixture facade. It keeps these
// focused historical-stake tests readable while ensuring they exercise the
// same database/sql schema as production.
type sqliteTestDB struct{ db *sql.DB }

func setupStakeSnapshotTestStore(
	t *testing.T,
) (*sqlstore.Store, *sqliteTestDB) {
	t.Helper()
	store, db, _, err := openSQLStore(
		Config{DataDir: t.TempDir()},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	return store, &sqliteTestDB{db: db}
}

func (d *sqliteTestDB) Create(value any) sqliteTestResult {
	var (
		result sql.Result
		err    error
	)
	switch v := value.(type) {
	case *models.Account:
		result, err = d.db.Exec(`INSERT INTO account
            (staking_key, credential_tag, pool, drep, added_slot, created_slot, reward, active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, v.StakingKey, v.CredentialTag, v.Pool,
			v.Drep, v.AddedSlot, v.CreatedSlot, fmt.Sprint(uint64(v.Reward)), v.Active)
	case *models.Utxo:
		result, err = d.db.Exec(`INSERT INTO utxo
			(tx_id, output_idx, staking_key, credential_tag, amount, added_slot, deleted_slot)
			VALUES (?, ?, ?, ?, ?, ?, 0)`, v.TxId, v.OutputIdx, v.StakingKey, v.CredentialTag,
			fmt.Sprint(uint64(v.Amount)), v.AddedSlot)
	case *models.AccountRewardDelta:
		result, err = d.db.Exec(`INSERT INTO account_reward_delta
            (staking_key, credential_tag, tx_hash, amount, previous_reward, added_slot, withdrawal, post_snapshot)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, v.StakingKey, v.CredentialTag, v.TxHash,
			fmt.Sprint(uint64(v.Amount)), fmt.Sprint(uint64(v.PreviousReward)), v.AddedSlot,
			v.Withdrawal, v.PostSnapshot)
	case *models.Certificate:
		result, err = d.db.Exec(`INSERT INTO certs
            (transaction_id, certificate_id, slot, cert_index, cert_type)
            VALUES (?, ?, ?, ?, ?)`, v.TransactionID, v.CertificateID, v.Slot, v.CertIndex, v.CertType)
	case *models.StakeDelegation:
		result, err = d.db.Exec(`INSERT INTO stake_delegation
            (staking_key, credential_tag, pool_key_hash, certificate_id, added_slot)
            VALUES (?, ?, ?, ?, ?)`, v.StakingKey, v.CredentialTag, v.PoolKeyHash,
			v.CertificateID, v.AddedSlot)
	case *models.VoteDelegation:
		result, err = d.db.Exec(`INSERT INTO vote_delegation
            (staking_key, credential_tag, drep, drep_type, certificate_id, added_slot)
            VALUES (?, ?, ?, ?, ?, ?)`, v.StakingKey, v.CredentialTag, v.Drep,
			v.DrepType, v.CertificateID, v.AddedSlot)
	default:
		err = fmt.Errorf("unsupported sqlite test model %T", value)
	}
	if err == nil && result != nil {
		if id, idErr := result.LastInsertId(); idErr == nil {
			switch v := value.(type) {
			case *models.Certificate:
				v.ID = uint(id)
			case *models.StakeDelegation:
				v.ID = uint(id)
			case *models.VoteDelegation:
				v.ID = uint(id)
			}
		}
	}
	return sqliteTestResult{Error: err}
}

type sqliteTestQuery struct {
	db   *sqliteTestDB
	args []any
}

func (d *sqliteTestDB) Model(
	any,
) *sqliteTestQuery {
	return &sqliteTestQuery{db: d}
}

func (q *sqliteTestQuery) Where(_ string, args ...any) *sqliteTestQuery {
	q.args = args
	return q
}

func (q *sqliteTestQuery) Updates(values map[string]any) sqliteTestResult {
	_, err := q.db.db.Exec(
		"UPDATE account SET drep = ?, added_slot = ? WHERE credential_tag = ? AND staking_key = ?",
		values["drep"],
		values["added_slot"],
		q.args[0],
		q.args[1],
	)
	return sqliteTestResult{Error: err}
}

func createTestTransaction(db *sqliteTestDB, txID uint, slot uint64) error {
	_, err := db.db.Exec(
		`INSERT INTO "transaction" (id, hash, slot, block_index, valid)
        VALUES (?, ?, ?, 0, 1)`,
		txID,
		[]byte(fmt.Sprintf("tx-%d", txID)),
		slot,
	)
	return err
}

// seedStakeDelegationCert writes the certs + transaction rows the historical
// stake CTE joins against, plus the typed certificate row itself.
func seedStakeDelegationCert(
	t *testing.T,
	db *sqliteTestDB,
	txID uint,
	slot uint64,
	stakeKey []byte,
	poolKeyHash []byte,
) {
	t.Helper()
	require.NoError(t, createTestTransaction(db, txID, slot))
	cert := models.Certificate{
		TransactionID: txID,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeStakeDelegation),
		Slot:          slot,
	}
	require.NoError(t, db.Create(&cert).Error)
	require.NoError(t, db.Create(&models.StakeDelegation{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		PoolKeyHash:   poolKeyHash,
		AddedSlot:     slot,
		CertificateID: cert.ID,
	}).Error)
}

// seedVoteDelegationCert writes a DRep-only vote-delegation certificate and
// bumps the mutable account.added_slot exactly the way the sqlite certificate
// processor does (see the VoteDelegationCertificate case in transaction.go).
// vote_delegation is neither a stake-delegation nor a registration source, so
// the account-derived fallback rows in the historical CTE must not treat the
// bumped added_slot as registration/delegation evidence.
func seedVoteDelegationCert(
	t *testing.T,
	db *sqliteTestDB,
	txID uint,
	slot uint64,
	stakeKey []byte,
	drep []byte,
) {
	t.Helper()
	require.NoError(t, createTestTransaction(db, txID, slot))
	cert := models.Certificate{
		TransactionID: txID,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeVoteDelegation),
		Slot:          slot,
	}
	require.NoError(t, db.Create(&cert).Error)
	require.NoError(t, db.Create(&models.VoteDelegation{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Drep:          drep,
		AddedSlot:     slot,
		CertificateID: cert.ID,
	}).Error)
	require.NoError(t, db.Model(&models.Account{}).
		Where("credential_tag = ? AND staking_key = ?", 0, stakeKey).
		Updates(map[string]any{
			"drep":       drep,
			"added_slot": slot,
		}).Error)
}

// TestGetStakeByPoolsAtSlotKeepsCredentialAfterVoteDelegation covers the
// dropped-credential defect in the historical stake CTE's account fallback. A
// Mithril-imported (or Shelley-genesis-staked) credential has no local
// registration certificate, so its registration state is synthesized from the
// live account row. A DRep-only vote delegation bumps the mutable
// account.added_slot past the credential's stake-delegation certificate, which
// used to make latest_delegation.added_slot > latest_registration.added_slot
// false and drop the whole credential — and all of its stake — out of
// active_delegation.
func TestGetStakeByPoolsAtSlotKeepsCredentialAfterVoteDelegation(t *testing.T) {
	store, db := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck
	pool := bytes.Repeat([]byte{0xF1}, 28)
	stakeKey := bytes.Repeat([]byte{0x31}, 28)
	drep := bytes.Repeat([]byte{0x71}, 28)

	// Imported account: live row only, no registration certificate history.
	require.NoError(t, db.Create(&models.Account{
		StakingKey:  stakeKey,
		Pool:        pool,
		Active:      true,
		AddedSlot:   10,
		CreatedSlot: 10,
	}).Error)
	require.NoError(t, db.Create(&models.Utxo{
		TxId:       bytes.Repeat([]byte{0x61}, 32),
		OutputIdx:  0,
		StakingKey: stakeKey,
		Amount:     1_000,
		AddedSlot:  20,
	}).Error)
	// On-chain stake delegation at slot 100 (real certificate history).
	seedStakeDelegationCert(t, db, 9001, 100, stakeKey, pool)
	// DRep-only vote delegation at slot 150 bumps account.added_slot to 150.
	seedVoteDelegationCert(t, db, 9002, 150, stakeKey, drep)
	stakes, delegators, err := store.GetStakeByPoolsAtSlot(
		[][]byte{pool}, 200, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(1_000), stakes[string(pool)],
		"a DRep-only vote delegation must not drop the credential's stake")
	require.Equal(t, uint64(1), delegators[string(pool)],
		"a DRep-only vote delegation must not drop the credential")
}

// TestGetStakeByPoolsAtSlotKeepsCredentialMutatedAfterSlot covers the second
// face of the same defect: the account fallback's visibility gate. A credential
// whose account row is mutated after the requested slot (here by a DRep-only
// vote delegation) demonstrably existed at that slot — account.created_slot
// proves it — so the mutation must not hide it from a historical
// reconstruction. This is the shape the epoch-boundary fallback capture hits,
// because it reconstructs the boundary after live account state has already
// advanced past it.
func TestGetStakeByPoolsAtSlotKeepsCredentialMutatedAfterSlot(t *testing.T) {
	store, db := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck
	pool := bytes.Repeat([]byte{0xF2}, 28)
	stakeKey := bytes.Repeat([]byte{0x32}, 28)
	drep := bytes.Repeat([]byte{0x72}, 28)

	require.NoError(t, db.Create(&models.Account{
		StakingKey:  stakeKey,
		Pool:        pool,
		Active:      true,
		AddedSlot:   10,
		CreatedSlot: 10,
	}).Error)
	require.NoError(t, db.Create(&models.Utxo{
		TxId:       bytes.Repeat([]byte{0x62}, 32),
		OutputIdx:  0,
		StakingKey: stakeKey,
		Amount:     2_000,
		AddedSlot:  20,
	}).Error)
	seedStakeDelegationCert(t, db, 9101, 100, stakeKey, pool)
	// Mutation lands after the reconstruction slot.
	seedVoteDelegationCert(t, db, 9102, 250, stakeKey, drep)

	stakes, delegators, err := store.GetStakeByPoolsAtSlot(
		[][]byte{pool}, 200, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(2_000), stakes[string(pool)],
		"an account mutation after the slot must not hide the credential")
	require.Equal(t, uint64(1), delegators[string(pool)],
		"an account mutation after the slot must not hide the credential")
}

// TestGetStakeByPoolsAtSlotFloorsNegativeHistoricalReward covers the
// total_stake unsigned wrap. The historical reward reconstruction subtracts
// every later credit from the live balance; when the journal retains more
// credit than the live balance can account for (a pruned or imported journal),
// the intermediate goes negative and used to be scanned straight into a uint64,
// turning a tiny stake into a near-2^64 one.
func TestGetStakeByPoolsAtSlotFloorsNegativeHistoricalReward(t *testing.T) {
	store, db := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck
	pool := bytes.Repeat([]byte{0xF3}, 28)
	stakeKey := bytes.Repeat([]byte{0x33}, 28)

	require.NoError(t, db.Create(&models.Account{
		StakingKey:  stakeKey,
		Pool:        pool,
		Active:      true,
		AddedSlot:   10,
		CreatedSlot: 10,
		Reward:      types.Uint64(10),
	}).Error)
	require.NoError(t, db.Create(&models.Utxo{
		TxId:       bytes.Repeat([]byte{0x63}, 32),
		OutputIdx:  0,
		StakingKey: stakeKey,
		Amount:     5,
		AddedSlot:  20,
	}).Error)
	// A journal credit larger than the live balance: reconstructing slot 100
	// yields 10 - 100 = -90 before flooring.
	require.NoError(t, db.Create(&models.AccountRewardDelta{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		TxHash:        bytes.Repeat([]byte{0x91}, 32),
		Amount:        types.Uint64(100),
		AddedSlot:     200,
		Withdrawal:    false,
	}).Error)

	stakes, delegators, err := store.GetStakeByPoolsAtSlot(
		[][]byte{pool}, 100, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(5), stakes[string(pool)],
		"a negative reconstructed reward must floor at zero, not wrap")
	require.Equal(t, uint64(1), delegators[string(pool)])
}

// TestEpochBoundaryStakeRetainsBoundaryRewardUpdate covers the whole-epoch
// divergence between the two mark-snapshot capture paths.
//
// cardano-ledger applies the delayed reward update before SNAP, so a mark
// snapshot includes that epoch's rewards; the authoritative capture, reading the
// live aggregate at the SNAP point, does too. dingo records the update at the
// boundary slot — one past the snapshot slot — so the plain
// "subtract everything after slot" reconstruction used by the fallback capture
// removed a whole epoch of rewards from every delegator.
//
// The epoch-boundary query must retain that credit and still exclude what
// cardano-ledger applies after SNAP (POOLREAP refunds, MIR, treasury
// withdrawals, proposal refunds), plus anything past the boundary. The plain
// "stake at slot" query must be unchanged.
func TestEpochBoundaryStakeRetainsBoundaryRewardUpdate(t *testing.T) {
	store, db := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck
	pool := bytes.Repeat([]byte{0xF4}, 28)
	stakeKey := bytes.Repeat([]byte{0x34}, 28)

	const (
		snapshotSlot = uint64(199)
		boundarySlot = uint64(200)
	)

	require.NoError(t, db.Create(&models.Account{
		StakingKey:  stakeKey,
		Pool:        pool,
		Active:      true,
		AddedSlot:   10,
		CreatedSlot: 10,
	}).Error)
	require.NoError(t, db.Create(&models.Utxo{
		TxId:       bytes.Repeat([]byte{0x64}, 32),
		OutputIdx:  0,
		StakingKey: stakeKey,
		Amount:     100,
		AddedSlot:  10,
	}).Error)

	// Pre-SNAP: the delayed reward update, applied at the boundary slot.
	require.NoError(t, store.AddAccountRewardByCredential(
		0, stakeKey, 50, boundarySlot,
		bytes.Repeat([]byte{0xa1}, 32), nil,
	))
	// Post-SNAP: a boundary credit cardano-ledger applies after SNAP.
	require.NoError(t, store.AddPostSnapshotAccountRewardByCredential(
		0, stakeKey, 7, boundarySlot,
		bytes.Repeat([]byte{0xa2}, 32), nil,
	))
	// Well past the boundary: never part of this snapshot.
	require.NoError(t, store.AddAccountRewardByCredential(
		0, stakeKey, 3, 300,
		bytes.Repeat([]byte{0xa3}, 32), nil,
	))

	stakes, delegators, err := store.GetStakeByPoolsAtSlot(
		[][]byte{pool}, snapshotSlot, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(100), stakes[string(pool)],
		"a plain stake-at-slot query must exclude every later credit")
	require.Equal(t, uint64(1), delegators[string(pool)])

	stakes, delegators, err = store.GetEpochBoundaryStakeByPools(
		[][]byte{pool}, snapshotSlot, boundarySlot, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(150),
		stakes[string(pool)],
		"the boundary query must retain the pre-SNAP reward update and drop the rest",
	)
	require.Equal(t, uint64(1), delegators[string(pool)])

	inputs, err := store.GetEpochBoundaryRewardStakeInputsForPools(
		[][]byte{pool}, snapshotSlot, boundarySlot, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, uint64(150), uint64(inputs[0].Stake),
		"the reward basis must agree with the leader-election pool total")
}

// TestEpochBoundaryStakeHandlesBoundaryWithdrawal covers the interaction between
// the retained boundary reward update and a withdrawal in the boundary block.
// The boundary block is applied after the rollover, so its withdrawal is
// post-boundary: its recorded previous balance already includes the reward
// update, and reconstruction must recover that balance rather than the cleared
// one.
func TestEpochBoundaryStakeHandlesBoundaryWithdrawal(t *testing.T) {
	store, db := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck
	pool := bytes.Repeat([]byte{0xF5}, 28)
	stakeKey := bytes.Repeat([]byte{0x35}, 28)

	const (
		snapshotSlot = uint64(199)
		boundarySlot = uint64(200)
	)

	require.NoError(t, db.Create(&models.Account{
		StakingKey:  stakeKey,
		Pool:        pool,
		Active:      true,
		AddedSlot:   10,
		CreatedSlot: 10,
	}).Error)
	require.NoError(t, db.Create(&models.Utxo{
		TxId:       bytes.Repeat([]byte{0x65}, 32),
		OutputIdx:  0,
		StakingKey: stakeKey,
		Amount:     100,
		AddedSlot:  10,
	}).Error)

	require.NoError(t, store.AddAccountRewardByCredential(
		0, stakeKey, 50, boundarySlot,
		bytes.Repeat([]byte{0xb1}, 32), nil,
	))
	require.NoError(t, store.AddPostSnapshotAccountRewardByCredential(
		0, stakeKey, 7, boundarySlot,
		bytes.Repeat([]byte{0xb2}, 32), nil,
	))
	// A withdrawal in the boundary block clears the whole balance.
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, stakeKey, 57, boundarySlot,
		bytes.Repeat([]byte{0xb3}, 32), nil,
	))

	stakes, _, err := store.GetEpochBoundaryStakeByPools(
		[][]byte{pool}, snapshotSlot, boundarySlot, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(150), stakes[string(pool)],
		"a boundary-block withdrawal must not erase the retained reward update")
}

// TestEpochBoundaryStakeIncludesPreSnapshotCreditsOnly pins which epoch-boundary
// reward-account credits a mark snapshot contains, for every kind dingo applies
// at a boundary. The reference sequence is NEWEPOCH = applyRUpd, MIR, EPOCH and
// EPOCH = SNAP, POOLREAP, ratification/enactment, so:
//
//   - the delayed reward update and MIR credits precede SNAP and are INCLUDED,
//   - POOLREAP deposit refunds, enacted treasury withdrawals and
//     proposal-deposit refunds follow SNAP and are EXCLUDED.
//
// Each credit is written through the same store method its ledger rule uses, so
// this pins the include/exclude split rather than just re-asserting the flag.
// ledger.TestBoundaryCreditVisibility_* pin that each rule reaches the right
// method.
func TestEpochBoundaryStakeIncludesPreSnapshotCreditsOnly(t *testing.T) {
	store, db := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck
	pool := bytes.Repeat([]byte{0xF6}, 28)
	stakeKey := bytes.Repeat([]byte{0x36}, 28)

	const (
		snapshotSlot = uint64(199)
		boundarySlot = uint64(200)
	)

	require.NoError(t, db.Create(&models.Account{
		StakingKey:  stakeKey,
		Pool:        pool,
		Active:      true,
		AddedSlot:   10,
		CreatedSlot: 10,
	}).Error)
	require.NoError(t, db.Create(&models.Utxo{
		TxId:       bytes.Repeat([]byte{0x66}, 32),
		OutputIdx:  0,
		StakingKey: stakeKey,
		Amount:     1_000,
		AddedSlot:  10,
	}).Error)

	// Pre-SNAP: delayed reward update (ledger.applyStakeRewards) and MIR
	// (governance.CreditRegisteredRewardAccountBeforeSnapshot).
	for i, amount := range []uint64{50, 3} {
		require.NoError(t, store.AddAccountRewardByCredential(
			0, stakeKey, amount, boundarySlot,
			bytes.Repeat([]byte{byte(0xe0 + i)}, 32), nil,
		))
	}
	// Post-SNAP: POOLREAP refund, treasury withdrawal, proposal-deposit refund
	// (all governance.CreditRegisteredRewardAccountAfterSnapshot).
	for i, amount := range []uint64{7, 11, 13} {
		require.NoError(t, store.AddPostSnapshotAccountRewardByCredential(
			0, stakeKey, amount, boundarySlot,
			bytes.Repeat([]byte{byte(0xf0 + i)}, 32), nil,
		))
	}

	stakes, _, err := store.GetEpochBoundaryStakeByPools(
		[][]byte{pool}, snapshotSlot, boundarySlot, 0, 0, nil,
	)
	require.NoError(t, err)
	// 1000 utxo + 50 reward update + 3 MIR; the 7 + 11 + 13 post-SNAP credits
	// are excluded.
	require.Equal(t, uint64(1_053), stakes[string(pool)],
		"only pre-SNAP boundary credits belong in the mark snapshot")

	inputs, err := store.GetEpochBoundaryRewardStakeInputsForPools(
		[][]byte{pool}, snapshotSlot, boundarySlot, 0, 0, nil,
	)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, uint64(1_053), uint64(inputs[0].Stake),
		"the reward basis must apply the same include/exclude split")
}
