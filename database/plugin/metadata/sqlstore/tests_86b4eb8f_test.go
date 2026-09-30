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
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// TestTransactionAlreadyCanceledContextFailsImmediately guards the simplest
// case: a ctx canceled before Transaction/ReadTransaction is even called
// must fail the begin outright rather than opening a transaction nothing
// can ever commit. Mirrors migrations/runner_test.go's
// TestProcessLockerCancellation shape: an already-canceled context is a
// deterministic guarantee, not a timing race.
func TestTransactionAlreadyCanceledContextFailsImmediately(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	writeErr := store.Transaction(ctx).Commit()
	require.ErrorIs(t, writeErr, context.Canceled)

	readErr := store.ReadTransaction(ctx).Commit()
	require.ErrorIs(t, readErr, context.Canceled)
}

// TestTransactionContextDeadlineAbortsBlockedBegin guards the core promise
// of threading a caller's ctx into Transaction: a caller waiting for a
// connection (here, SQLite's single-writer pool held by another
// transaction) is unblocked by its own deadline instead of waiting out
// whoever is holding the connection. Adapts
// TestSQLiteBulkModeKeepsPlannerAndWritersAvailable's blocking shape
// (store_test.go), swapping the blocking cause's resolution from "the
// holder commits" to "the waiter's own deadline fires".
func TestTransactionContextDeadlineAbortsBlockedBegin(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	store.writeDB.SetMaxOpenConns(1)

	holder := store.Transaction(t.Context())
	t.Cleanup(func() { _ = holder.Rollback() })

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	waiter := store.Transaction(ctx)
	err := waiter.Commit()
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(
		t, elapsed, 2*time.Second,
		"the waiter must return on its own deadline, not wait for the holder",
	)

	// The holder is unaffected by the waiter's unrelated deadline.
	require.NoError(t, holder.Commit())
}

// TestTransactionContextCancellationRollsBackWrites guards "preserve
// transaction rollback on cancellation": a write issued through a
// Transaction(ctx) must not survive once ctx is canceled mid-transaction,
// and the connection it held must be released back to the pool rather than
// leaked.
//
// This intentionally does not pin writeDB to a single connection: doing so
// with SQLite's mode=memory&cache=shared DSN interacts badly with
// database/sql discarding (rather than idling) a connection whose in-flight
// statement failed from ctx cancellation -- a brief window with zero live
// connections destroys the shared in-memory database out from under the
// test, which is a SQLite test-fixture artifact, not the behavior under
// test. Connection release is instead asserted directly against pool
// stats.
func TestTransactionContextCancellationRollsBackWrites(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)

	// database/sql discards (rather than idles) a pooled connection whose
	// in-flight statement failed from ctx cancellation, and reopens a fresh
	// one lazily on next use. For SQLite's mode=memory&cache=shared DSN, a
	// window with zero live connections destroys the shared in-memory
	// database along with it. Hold one extra, otherwise-unused connection
	// open for the test's duration so the schema survives that window.
	keepAlive, err := store.writeDB.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = keepAlive.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	txn := store.Transaction(ctx)
	require.NoError(t, store.SetCommitTimestamp(42, txn))

	cancel()

	// database/sql rolls back a Tx once the ctx supplied to BeginTx is
	// canceled, per BeginTx's documented contract -- but that happens on an
	// internal watcher goroutine, not synchronously with cancel(), so poll
	// rather than assert immediately. (In practice this also fails on the
	// first attempt regardless of that goroutine's timing: dbFromTxn hands
	// this statement the transaction's own now-canceled ctx directly.)
	require.Eventually(t, func() bool {
		return store.SetCommitTimestamp(43, txn) != nil
	}, 2*time.Second, 5*time.Millisecond,
		"transaction must stop accepting writes once its ctx is canceled")

	require.Error(t, txn.Commit())

	// The connection the aborted transaction held must come back to the
	// pool rather than being leaked: only the keepAlive connection above
	// should remain checked out.
	require.Eventually(t, func() bool {
		return store.writeDB.Stats().InUse <= 1
	}, 2*time.Second, 5*time.Millisecond,
		"canceled transaction's connection must be released back to the pool")

	// Neither the successful first write nor anything else from the
	// canceled transaction may be durably visible.
	persisted, err := store.GetCommitTimestamp()
	require.NoError(t, err)
	require.Zero(t, persisted)
}

// TestCommitteeCertificateRejectsUnsupportedCredentialTag proves committee
// certificates validate their credential tags before writing, like every
// other credential-backed certificate path.
//
// Credential.CredType is decoded from CBOR without a range check. Storing it
// raw would write a tag no validated uint8 writer can ever match, so the
// member would silently drop out of the active committee, or the row would
// fail to scan back into the uint8 model field. The tag conversion runs before
// any statement, so a nil queryer is never reached on the rejection path.
func TestCommitteeCertificateRejectsUnsupportedCredentialTag(t *testing.T) {
	t.Parallel()

	var hash lcommon.Blake2b224
	hash[0] = 0xe1
	const unsupportedTag = uint(7)

	store := &Store{}
	for _, test := range []struct {
		name        string
		certificate lcommon.Certificate
	}{
		{
			name: "auth committee hot unsupported cold tag",
			certificate: &lcommon.AuthCommitteeHotCertificate{
				CertType: uint(lcommon.CertificateTypeAuthCommitteeHot),
				ColdCredential: lcommon.Credential{
					CredType:   unsupportedTag,
					Credential: hash,
				},
				HotCredential: lcommon.Credential{
					CredType:   lcommon.CredentialTypeAddrKeyHash,
					Credential: hash,
				},
			},
		},
		{
			name: "auth committee hot unsupported hot tag",
			certificate: &lcommon.AuthCommitteeHotCertificate{
				CertType: uint(lcommon.CertificateTypeAuthCommitteeHot),
				ColdCredential: lcommon.Credential{
					CredType:   lcommon.CredentialTypeAddrKeyHash,
					Credential: hash,
				},
				HotCredential: lcommon.Credential{
					CredType:   unsupportedTag,
					Credential: hash,
				},
			},
		},
		{
			name: "resign committee cold unsupported cold tag",
			certificate: &lcommon.ResignCommitteeColdCertificate{
				CertType: uint(lcommon.CertificateTypeResignCommitteeCold),
				ColdCredential: lcommon.Credential{
					CredType:   unsupportedTag,
					Credential: hash,
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := store.applySpecializedCertificate(
				context.Background(),
				nil,
				test.certificate,
				1,
				100,
				0,
				0,
				nil,
			)
			require.ErrorContains(t, err, "unsupported stake credential tag")
		})
	}
}

func TestGetCommitteeHotAuthorizationsSinceSQLite(t *testing.T) {
	t.Parallel()
	exerciseCommitteeHotAuthorizationsSince(t, newManagementTestStore(t))
}

// exerciseCommitteeHotAuthorizationsSince checks that the window filter keeps
// exactly each cold credential's latest authorization when that row is in the
// window, including the slot tie broken by certificate_id and a script
// credential sharing a key credential's hash.
func exerciseCommitteeHotAuthorizationsSince(t *testing.T, store *Store) {
	t.Helper()
	const (
		keyTag    = uint8(lcommon.CredentialTypeAddrKeyHash)
		scriptTag = uint8(lcommon.CredentialTypeScriptHash)
	)
	seed := func(coldTag uint8, cold byte, hot byte, certificateID, slot uint64) {
		_, err := store.writeDB.Exec(
			store.dialect.Rebind(`
INSERT INTO auth_committee_hot (
    cold_credential_tag, cold_credential, hot_credential_tag,
    host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?, ?, ?)`),
			coldTag, credentialHash(cold), keyTag, credentialHash(hot),
			certificateID, slot,
		)
		require.NoError(t, err)
	}
	// A key cold credential re-authorized inside the window.
	seed(keyTag, 0xa1, 0x01, 1, 10)
	seed(keyTag, 0xa1, 0x02, 2, 200)
	// A key cold credential whose only authorization predates the window.
	seed(keyTag, 0xb1, 0x03, 3, 50)
	// A script cold credential sharing 0xa1's hash, with two authorizations
	// in one slot.
	seed(scriptTag, 0xa1, 0x04, 4, 150)
	seed(scriptTag, 0xa1, 0x05, 5, 150)
	// A key cold credential authorized exactly at the window start.
	seed(keyTag, 0xc1, 0x06, 6, 100)

	collect := func(minSlot uint64) map[string]string {
		rows, err := store.GetCommitteeHotAuthorizationsSince(minSlot, nil)
		require.NoError(t, err)
		ret := make(map[string]string, len(rows))
		for _, row := range rows {
			key := fmt.Sprintf(
				"%d:%s",
				row.ColdCredentialTag,
				hex.EncodeToString(row.ColdCredential),
			)
			require.NotContains(t, ret, key, "one row per cold credential")
			ret[key] = fmt.Sprintf(
				"%s@%d",
				hex.EncodeToString(row.HotCredential[:1]),
				row.AddedSlot,
			)
		}
		return ret
	}
	cold := func(tag uint8, seed byte) string {
		return fmt.Sprintf(
			"%d:%s",
			tag,
			hex.EncodeToString(credentialHash(seed)),
		)
	}

	require.Equal(t, map[string]string{
		cold(keyTag, 0xa1):    "02@200",
		cold(scriptTag, 0xa1): "05@150",
		cold(keyTag, 0xc1):    "06@100",
	}, collect(100))
	require.Equal(t, map[string]string{
		cold(keyTag, 0xa1):    "02@200",
		cold(keyTag, 0xb1):    "03@50",
		cold(scriptTag, 0xa1): "05@150",
		cold(keyTag, 0xc1):    "06@100",
	}, collect(0))
	require.Empty(t, collect(201))
}

func TestCommitteeQuorumZeroDiffersFromClear(t *testing.T) {
	t.Parallel()

	store := newManagementTestStore(t)
	require.NoError(t, store.SetCommitteeQuorum(
		&types.Rat{Rat: big.NewRat(0, 1)}, 10, nil,
	))
	quorum, err := store.GetCommitteeQuorum(nil)
	require.NoError(t, err)
	require.NotNil(t, quorum)
	require.Zero(t, quorum.Sign())

	require.NoError(t, store.ClearCommitteeQuorum(20, nil))
	quorum, err = store.GetCommitteeQuorum(nil)
	require.NoError(t, err)
	require.Nil(t, quorum)

	require.NoError(t, store.SetCommitteeQuorum(
		&types.Rat{Rat: big.NewRat(1, 2)}, 30, nil,
	))
	require.NoError(t, store.SetCommitteeQuorum(
		&types.Rat{Rat: big.NewRat(0, 1)}, 40, nil,
	))
	require.NoError(t, store.DeleteCommitteeMembersAfterSlot(30, nil))
	quorum, err = store.GetCommitteeQuorum(nil)
	require.NoError(t, err)
	require.NotNil(t, quorum)
	require.Equal(t, big.NewRat(1, 2), quorum.Rat)
}

// utxoForInsertCacheTest builds a minimal, valid models.Utxo for exercising
// insertUtxoModel directly: distinct txSeed/outputIdx pairs target distinct
// rows, and the same pair can be reused deliberately to exercise the ON
// CONFLICT DO NOTHING branch.
func utxoForInsertCacheTest(
	txSeed byte,
	outputIdx uint32,
	amount uint64,
) *models.Utxo {
	txID := make([]byte, 32)
	txID[31] = txSeed
	paymentKey := bytes.Repeat([]byte{txSeed}, lcommon.AddressHashSize)
	return &models.Utxo{
		TxId:       txID,
		OutputIdx:  outputIdx,
		PaymentKey: paymentKey,
		AddedSlot:  1,
		Amount:     types.Uint64(amount),
	}
}

// insertUtxoInTxn runs insertUtxoModel inside its own write transaction.
// This is simpler than production, not representative of it: production
// applies many outputs within one shared write transaction --
// LedgerDeltaBatch.apply (ledger/delta.go) applies a whole block batch under
// the single txn ledger/state.go passes it, and genesis import
// (ledger/chainsync.go) inserts the entire Byron and Shelley UTxO set inside
// one txn.Do -- so a caller here that wants to observe the per-transaction
// Tx-scoped statement retention this cache introduces must call
// insertUtxoModel repeatedly against one shared transaction instead; see
// TestInsertUtxoModelBoundsTxScopedStatementRetentionInOneTransaction.
func insertUtxoInTxn(
	t *testing.T,
	store *Store,
	utxo *models.Utxo,
	ignoreConflict bool,
) {
	t.Helper()
	err := store.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			return store.insertUtxoModel(ctx, db, utxo, ignoreConflict)
		},
	)
	require.NoError(t, err)
}

// TestInsertUtxoModelReusesCachedStatementAcrossTransactions proves
// insertUtxoModel's move onto the hot-statement cache (queryRowCached) does
// not change its behavior: distinct inserts still get distinct ids, a
// repeated (tx_id, output_idx) under ignoreConflict still resolves to the
// existing row's id via the ON CONFLICT DO NOTHING + fallback SELECT branch,
// and the stored row round-trips correctly through GetUtxo -- while the
// cached *sql.Stmt for insertUtxoQueryIgnoreConflict is the same object
// across independent write transactions. This exercises independent
// transactions for simplicity; it is not the production access pattern --
// see insertUtxoInTxn's doc comment, and
// TestInsertUtxoModelBoundsTxScopedStatementRetentionInOneTransaction for the
// real multi-output-per-transaction shape.
func TestInsertUtxoModelReusesCachedStatementAcrossTransactions(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	first := utxoForInsertCacheTest(1, 0, 5_000_000)
	insertUtxoInTxn(t, store, first, true)
	require.NotZero(t, first.ID)

	store.stmtMu.Lock()
	cachedBefore := store.stmts[insertUtxoQueryIgnoreConflict]
	store.stmtMu.Unlock()
	require.NotNil(
		t,
		cachedBefore,
		"expected insertUtxoQueryIgnoreConflict to be cached on SQLite",
	)

	second := utxoForInsertCacheTest(2, 0, 7)
	insertUtxoInTxn(t, store, second, true)
	require.NotZero(t, second.ID)
	require.NotEqual(t, first.ID, second.ID)

	store.stmtMu.Lock()
	cachedAfter := store.stmts[insertUtxoQueryIgnoreConflict]
	store.stmtMu.Unlock()
	require.Same(
		t,
		cachedBefore,
		cachedAfter,
		"expected the same cached *sql.Stmt across independent write transactions",
	)

	// Re-inserting the same (tx_id, output_idx) under ignoreConflict must
	// take the ON CONFLICT DO NOTHING branch and resolve to the existing
	// row's id via the fallback SELECT, not error and not create a second
	// row -- exactly like before this query went through the cache.
	dup := utxoForInsertCacheTest(1, 0, 999)
	insertUtxoInTxn(t, store, dup, true)
	require.Equal(
		t,
		first.ID,
		dup.ID,
		"expected ON CONFLICT DO NOTHING to resolve to the existing row's id",
	)

	// Round-trip through the public read path: the row the cached statement
	// wrote back is a correct, complete row, not just "an insert succeeded".
	got, err := store.GetUtxo(first.TxId, first.OutputIdx, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, types.Uint64(5_000_000), got.Amount)

	got2, err := store.GetUtxo(second.TxId, second.OutputIdx, nil)
	require.NoError(t, err)
	require.NotNil(t, got2)
	require.Equal(t, types.Uint64(7), got2.Amount)
}

// TestImportUtxosReusesCachedStatementAcrossTransactions proves the snapshot
// importer uses the same cached insert as the ordinary UTxO path. It also
// exercises the ON CONFLICT DO NOTHING + fallback lookup that assigns the
// existing row ID, preserving idempotent imports.
func TestImportUtxosReusesCachedStatementAcrossTransactions(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	first := *utxoForInsertCacheTest(21, 0, 5_000_000)
	require.NoError(t, store.ImportUtxos([]models.Utxo{first}, nil))
	gotFirst, err := store.GetUtxo(first.TxId, first.OutputIdx, nil)
	require.NoError(t, err)
	require.NotNil(t, gotFirst)

	store.stmtMu.Lock()
	cachedBefore := store.stmts[insertUtxoQueryIgnoreConflict]
	store.stmtMu.Unlock()
	require.NotNil(
		t,
		cachedBefore,
		"expected importer insert to use the cached statement on SQLite",
	)

	second := *utxoForInsertCacheTest(22, 0, 7)
	require.NoError(t, store.ImportUtxos([]models.Utxo{second}, nil))
	gotSecond, err := store.GetUtxo(second.TxId, second.OutputIdx, nil)
	require.NoError(t, err)
	require.NotNil(t, gotSecond)
	require.NotEqual(t, gotFirst.ID, gotSecond.ID)

	store.stmtMu.Lock()
	cachedAfter := store.stmts[insertUtxoQueryIgnoreConflict]
	store.stmtMu.Unlock()
	require.Same(
		t,
		cachedBefore,
		cachedAfter,
		"expected the importer to reuse the same cached statement",
	)

	duplicate := first
	duplicate.Amount = 999
	require.NoError(t, store.ImportUtxos([]models.Utxo{duplicate}, nil))
	gotDuplicate, err := store.GetUtxo(first.TxId, first.OutputIdx, nil)
	require.NoError(t, err)
	require.NotNil(t, gotDuplicate)
	require.Equal(t, gotFirst.ID, gotDuplicate.ID)
	require.Equal(
		t,
		types.Uint64(5_000_000),
		gotDuplicate.Amount,
		"conflicting import must preserve the existing UTxO row",
	)
}

func TestImportUtxosDeferredRewardLiveStakeRefreshRebuildsCorrectly(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	utxo := *utxoForInsertCacheTest(23, 0, 5_000_000)
	utxo.CredentialTag = 0
	utxo.StakingKey = bytes.Repeat([]byte{0x23}, lcommon.AddressHashSize)

	require.NoError(t, store.ImportUtxosDeferredRewardLiveStakeRefresh(
		[]models.Utxo{utxo},
		nil,
	))
	var aggregateCount int
	require.NoError(t, store.writeDB.QueryRowContext(
		context.Background(),
		"SELECT COUNT(*) FROM reward_live_stake",
	).Scan(&aggregateCount))
	require.Zero(t, aggregateCount,
		"deferred import must not refresh the aggregate per batch")

	require.NoError(t, store.RebuildRewardLiveStake(utxo.AddedSlot, nil))
	var utxoStake string
	require.NoError(t, store.writeDB.QueryRowContext(
		context.Background(),
		"SELECT utxo_stake FROM reward_live_stake WHERE credential_tag = 0 AND staking_key = ?",
		utxo.StakingKey,
	).Scan(&utxoStake))
	require.Equal(t, "5000000", utxoStake)
}

// TestImportUtxosBoundsTxScopedStatementRetentionInOneTransaction exercises
// the production shape: one import batch keeps a write transaction open while
// it inserts many outputs. Reusing one Tx-scoped derivative keeps database/sql
// from retaining one prepared statement per imported output.
func TestImportUtxosBoundsTxScopedStatementRetentionInOneTransaction(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()

	const outputCount = 2_000
	utxos := make([]models.Utxo, outputCount)
	for i := range outputCount {
		utxos[i] = *utxoForBenchmarkIteration(uint64(i + 1))
	}

	txn := store.Transaction(ctx)
	sqlTransaction, ok := txn.(*sqlTxn)
	require.True(t, ok)
	require.NoError(t, sqlTransaction.beginErr)
	require.NoError(t, store.ImportUtxos(utxos, txn))

	retained := retainedTxStmtCount(t, sqlTransaction.tx)
	require.NoError(t, txn.Commit())
	require.LessOrEqual(
		t,
		retained,
		1,
		"expected one Tx-scoped importer statement for %d outputs, got %d",
		outputCount,
		retained,
	)
}

// TestInsertUtxoModelCachesAssetIDLookup proves getAssetIDQuery -- the other
// query insertUtxoModel now routes through the hot-statement cache -- is
// populated, reused across independent write transactions, and still
// resolves each asset's id correctly.
func TestInsertUtxoModelCachesAssetIDLookup(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	utxo := utxoForInsertCacheTest(10, 0, 2_000_000)
	utxo.Assets = []models.Asset{
		{
			Name:        []byte("token"),
			PolicyId:    bytes.Repeat([]byte{0xAA}, 28),
			Fingerprint: []byte("asset1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			Amount:      types.Uint64(42),
		},
	}
	insertUtxoInTxn(t, store, utxo, true)
	require.NotZero(t, utxo.Assets[0].ID)

	store.stmtMu.Lock()
	cachedBefore := store.stmts[getAssetIDQuery]
	store.stmtMu.Unlock()
	require.NotNil(
		t,
		cachedBefore,
		"expected getAssetIDQuery to be cached on SQLite",
	)

	utxo2 := utxoForInsertCacheTest(11, 0, 3_000_000)
	utxo2.Assets = []models.Asset{
		{
			Name:        []byte("token2"),
			PolicyId:    bytes.Repeat([]byte{0xBB}, 28),
			Fingerprint: []byte("asset1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
			Amount:      types.Uint64(7),
		},
	}
	insertUtxoInTxn(t, store, utxo2, true)
	require.NotZero(t, utxo2.Assets[0].ID)
	require.NotEqual(t, utxo.Assets[0].ID, utxo2.Assets[0].ID)

	store.stmtMu.Lock()
	cachedAfter := store.stmts[getAssetIDQuery]
	store.stmtMu.Unlock()
	require.Same(
		t,
		cachedBefore,
		cachedAfter,
		"expected the same cached *sql.Stmt across independent write transactions",
	)
}

// TestInsertUtxoModelBoundsTxScopedStatementRetentionInOneTransaction is the
// regression test for the retention bug raised against this PR: production
// applies many outputs within one shared write transaction (a whole block
// batch via LedgerDeltaBatch.apply, or the entire genesis UTxO set in one
// txn.Do), not one transaction per output, so insertUtxoModel's INSERT and
// asset-id lookup are each consulted many times against the same *sql.Tx.
// Both route through queryRowCached, which derives a Tx-scoped *sql.Stmt via
// stmtForQueryer/txScopedStmt (prepared_stmt.go). Before
// perf/reward-live-stake-touch-cache's fix (00bedb10), txScopedStmt called
// (*sql.Tx).StmtContext on every invocation and database/sql retained every
// result until commit or rollback, so retention was linear in outputs
// inserted per transaction: 20000 outputs, each carrying one asset, would
// have retained 20000 Tx-scoped statements. txScopedStmt now derives the
// Tx-scoped statement once per (tx, cached) pair and reuses it, so this
// asserts retention stays bounded regardless of output count.
func TestInsertUtxoModelBoundsTxScopedStatementRetentionInOneTransaction(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()

	const outputCount = 20_000

	txn := store.Transaction(ctx)
	sqlTransaction, ok := txn.(*sqlTxn)
	require.True(t, ok)
	require.NoError(t, sqlTransaction.beginErr)

	require.NoError(t, store.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			for i := range uint32(outputCount) {
				utxo := utxoForInsertCacheTest(1, i, 1_000_000+uint64(i))
				utxo.Assets = []models.Asset{
					{
						Name:     []byte("token"),
						PolicyId: bytes.Repeat([]byte{0xCC}, 28),
						Fingerprint: []byte(
							"asset1cccccccccccccccccccccccccccccccccccccccc",
						),
						Amount: types.Uint64(1),
					},
				}
				if err := store.insertUtxoModel(ctx, db, utxo, true); err != nil {
					return err
				}
			}
			return nil
		},
	))

	retained := retainedTxStmtCount(t, sqlTransaction.tx)
	require.NoError(t, txn.Commit())

	// insertUtxoModel here consults exactly three distinct cached queries --
	// insertUtxoQueryIgnoreConflict, importAssetQuery, and getAssetIDQuery --
	// so 3 is the exact bound, not just an upper one.
	require.LessOrEqual(
		t,
		retained,
		3,
		"expected bounded Tx-scoped statement retention for %d outputs, got %d",
		outputCount,
		retained,
	)
}

// TestCacheableForDialect is a table-driven unit test of the pure predicate
// prepareHotStatements now consults before caching a hot statement. The only
// case that must come back false is a RETURNING-id query on MySQL (see
// insertUtxoQuery's doc comment); every other dialect/query combination,
// including a RETURNING-id query on PostgreSQL (which supports RETURNING
// natively) and a non-RETURNING query on MySQL, must come back true.
func TestCacheableForDialect(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		dialect string
		query   string
		want    bool
	}{
		{
			"sqlite insert with returning",
			"sqlite", insertUtxoQueryIgnoreConflict, true,
		},
		{
			"sqlite insert without conflict clause",
			"sqlite", insertUtxoQuery, true,
		},
		{
			"postgres insert with returning",
			"postgres", insertUtxoQueryIgnoreConflict, true,
		},
		{
			"mysql insert with returning (ignore conflict)",
			"mysql", insertUtxoQueryIgnoreConflict, false,
		},
		{
			"mysql insert with returning (no conflict clause)",
			"mysql", insertUtxoQuery, false,
		},
		{
			"mysql plain select, no returning",
			"mysql", getAssetIDQuery, true,
		},
		{
			"mysql reward account select, no returning",
			"mysql", rewardLiveStakeAccountQuery, true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(
				t,
				tc.want,
				cacheableForDialect(tc.dialect, tc.query),
			)
		})
	}
}

// TestPrepareHotStatementsSkipsReturningIDQueryOnMySQL is the end-to-end
// counterpart to TestCacheableForDialect: it proves prepareHotStatements
// itself actually leaves insertUtxoQuery/insertUtxoQueryIgnoreConflict
// uncached against a MySQL-dialect Store, while a non-RETURNING hot
// statement (getAssetIDQuery) still gets cached -- i.e. the skip is scoped
// to the unsafe dialect+query-shape combination, not a blanket "MySQL never
// caches anything".
//
// This borrows the real migrated schema a SQLite store already built
// (newMigratedSQLiteStore) rather than running Start's migration runner
// under a MySQL-labeled dialect: migrations.SQLiteRegistry() is SQLite DDL,
// which the runner would try to execute as-is regardless of s.dialect, so
// mixing a real MySQL Start with a SQLite migration registry would fail for
// a reason unrelated to the thing this test checks. prepareHotStatements
// itself only needs an existing schema and a writeDB to call PrepareContext
// against, so a second, minimal Store value sharing the same *sql.DB (with
// dialect swapped to MySQL) exercises exactly the code path under test.
func TestPrepareHotStatementsSkipsReturningIDQueryOnMySQL(t *testing.T) {
	t.Parallel()
	sqliteStore := newMigratedSQLiteStore(t)

	mysqlStore := &Store{
		writeDB: sqliteStore.writeDB,
		dialect: MySQLDialect(),
		logger:  slog.Default(),
	}
	t.Cleanup(mysqlStore.closePreparedStatements)
	mysqlStore.prepareHotStatements(context.Background())

	_, ok := mysqlStore.lookupCachedStmt(insertUtxoQuery)
	require.False(t, ok, "expected insertUtxoQuery to be uncached on MySQL")

	_, ok = mysqlStore.lookupCachedStmt(insertUtxoQueryIgnoreConflict)
	require.False(
		t,
		ok,
		"expected insertUtxoQueryIgnoreConflict to be uncached on MySQL",
	)

	_, ok = mysqlStore.lookupCachedStmt(getAssetIDQuery)
	require.True(
		t,
		ok,
		"expected a non-RETURNING hot statement to still be cached on MySQL",
	)
}

// BenchmarkInsertUtxoModel is the before/after timing counterpart:
// legacyInsertUtxo reproduces insertUtxoModel's pre-cache one-shot
// QueryRowContext call (the exact query text and argument order that used
// to be inlined directly in insertUtxoModel), run against the same migrated
// schema and connection insertUtxoModel itself uses via the hot-statement
// cache.
func legacyInsertUtxo(
	ctx context.Context,
	db queryer,
	utxo *models.Utxo,
) (int64, error) {
	params, err := createUtxoParams(utxo)
	if err != nil {
		return 0, err
	}
	var id int64
	err = db.QueryRowContext(ctx, insertUtxoQueryIgnoreConflict,
		params.TransactionID,
		params.CollateralReturnForTxID,
		params.TxID,
		params.PaymentKey,
		params.StakingKey,
		params.CredentialTag,
		params.DatumHash,
		nullBytes(params.SpentAtTxID),
		nullBytes(params.ReferencedByTxID),
		nullBytes(params.CollateralByTxID),
		params.AddedSlot,
		params.DeletedSlot,
		params.Amount,
		params.OutputIdx,
		params.PaymentScript,
	).Scan(&id)
	return id, err
}

// utxoForBenchmarkIteration builds a UTxO with a unique tx_id per i, so each
// benchmark iteration inserts a genuinely new row (the realistic sync
// workload) instead of repeatedly hitting the ON CONFLICT DO NOTHING branch.
func utxoForBenchmarkIteration(i uint64) *models.Utxo {
	txID := make([]byte, 32)
	binary.BigEndian.PutUint64(txID[24:], i)
	return &models.Utxo{
		TxId:       txID,
		OutputIdx:  0,
		PaymentKey: bytes.Repeat([]byte{0x01}, lcommon.AddressHashSize),
		AddedSlot:  1,
		Amount:     types.Uint64(1_000_000 + i),
	}
}

func BenchmarkInsertUtxoModel(b *testing.B) {
	ctx := context.Background()

	b.Run("one_shot_uncached", func(b *testing.B) {
		store := newMigratedSQLiteStore(b)
		i := uint64(0)
		for b.Loop() {
			i++
			utxo := utxoForBenchmarkIteration(i)
			if _, err := legacyInsertUtxo(ctx, store.writeDB, utxo); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("prepared_cache", func(b *testing.B) {
		store := newMigratedSQLiteStore(b)
		i := uint64(0)
		for b.Loop() {
			i++
			utxo := utxoForBenchmarkIteration(i)
			if err := store.insertUtxoModel(ctx, store.writeDB, utxo, true); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// legacyImportUtxoStatement reproduces importUtxos' former generated sqlc
// call. The benchmark keeps both paths in one transaction so the measured
// difference is statement preparation/reuse, not transaction setup.
func legacyImportUtxoStatement(
	ctx context.Context,
	db queryer,
	utxo *models.Utxo,
) (int64, error) {
	params, err := createUtxoParams(utxo)
	if err != nil {
		return 0, err
	}
	return sqlitequery.New(db).CreateUtxoIfAbsent(
		ctx,
		sqlitequery.CreateUtxoIfAbsentParams(params),
	)
}

func cachedImportUtxoStatement(
	ctx context.Context,
	store *Store,
	db queryer,
	utxo *models.Utxo,
) (int64, error) {
	params, err := createUtxoParams(utxo)
	if err != nil {
		return 0, err
	}
	var id int64
	err = store.queryRowCached(ctx, db, insertUtxoQueryIgnoreConflict,
		params.TransactionID,
		params.CollateralReturnForTxID,
		params.TxID,
		params.PaymentKey,
		params.StakingKey,
		params.CredentialTag,
		params.DatumHash,
		nullBytes(params.SpentAtTxID),
		nullBytes(params.ReferencedByTxID),
		nullBytes(params.CollateralByTxID),
		params.AddedSlot,
		params.DeletedSlot,
		params.Amount,
		params.OutputIdx,
		params.PaymentScript,
	).Scan(&id)
	return id, err
}

// BenchmarkImportUtxoStatement measures the exact importer insert before and
// after transaction-scoped prepared-statement reuse. Unique transaction IDs
// keep every iteration on the insert path rather than the conflict fallback.
func BenchmarkImportUtxoStatement(b *testing.B) {
	ctx := context.Background()

	b.Run("one_shot_uncached", func(b *testing.B) {
		store := newMigratedSQLiteStore(b)
		txn := store.Transaction(ctx)
		db, txCtx, err := store.dbFromTxn(txn)
		require.NoError(b, err)
		i := uint64(0)
		b.ResetTimer()
		for b.Loop() {
			i++
			utxo := utxoForBenchmarkIteration(i)
			if _, err := legacyImportUtxoStatement(txCtx, db, utxo); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		require.NoError(b, txn.Commit())
	})

	b.Run("transaction_scoped_cache", func(b *testing.B) {
		store := newMigratedSQLiteStore(b)
		txn := store.Transaction(ctx)
		db, txCtx, err := store.dbFromTxn(txn)
		require.NoError(b, err)
		i := uint64(0)
		b.ResetTimer()
		for b.Loop() {
			i++
			utxo := utxoForBenchmarkIteration(i)
			if _, err := cachedImportUtxoStatement(
				txCtx,
				store,
				db,
				utxo,
			); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		require.NoError(b, txn.Commit())
	})
}

func integrationMigrationLocker(
	dialect string,
	namespace string,
) migrations.Locker {
	// PostgreSQL and MySQL advisory locks are server-scoped. These tests use
	// isolated schemas or databases, so their locks must be isolated too.
	return migrations.NewAdvisoryLocker(
		dialect,
		integrationMigrationLockKey(namespace),
		time.Second,
	)
}

func integrationMigrationLockKey(namespace string) int64 {
	digest := sha256.Sum256([]byte(namespace))
	return int64(binary.BigEndian.Uint64(digest[:8]))
}

func TestIntegrationMigrationLockKey(t *testing.T) {
	t.Parallel()

	first := integrationMigrationLockKey("sqlstore_pool_1")
	require.Equal(
		t,
		first,
		integrationMigrationLockKey("sqlstore_pool_1"),
	)
	require.NotEqual(
		t,
		first,
		integrationMigrationLockKey("sqlstore_pool_2"),
	)
}

var migratedStoreSequence atomic.Uint64

// newMigratedTestStore opens an in-memory SQLite store carrying the checked-in
// schema, so a test exercises the real column types rather than a hand-written
// approximation of them.
func newMigratedTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_mir_%d?mode=memory&cache=shared",
			migratedStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store
}

func mirTestCredential(seed byte) *lcommon.Credential {
	var hash lcommon.Blake2b224
	for i := range hash {
		hash[i] = seed
	}
	return &lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: hash,
	}
}

// TestApplyMIRCertificatePersistsProjectedDeltas proves the persistence path
// writes exactly what the certificate's reward projection returns, for every
// credential, rather than a value it re-derives from the underlying field.
// RewardsAmount is *big.Int on every gouroboros release, so this is the path a
// signed delta travels once the underlying field is widened to delta_coin.
func TestApplyMIRCertificatePersistsProjectedDeltas(t *testing.T) {
	t.Parallel()
	store := newMigratedTestStore(t)

	first := mirTestCredential(0x21)
	second := mirTestCredential(0x22)
	cert := decodeMIRDistributionCertificate(
		t,
		uint(lcommon.MirSourceReserves),
		map[*lcommon.Credential]uint64{
			first:  1_200,
			second: 450,
		},
	)
	_, err := applyMIRCertificate(
		context.Background(),
		newDialectQueryer(store.writeDB, store.dialect.Name()),
		cert,
		0,
		400,
	)
	require.NoError(t, err)

	want := map[string]string{}
	for credential, amount := range cert.Reward.RewardsAmount() {
		want[string(credential.Credential[:])] = amount.String()
	}
	require.Len(t, want, 2)

	effects, err := store.GetMIRCertsInSlotRange(0, 1_000, nil)
	require.NoError(t, err)
	require.Len(t, effects, 1)
	got := map[string]string{}
	for _, reward := range effects[0].Rewards {
		require.NotNil(t, reward.Amount)
		got[string(reward.Credential)] = reward.Amount.String()
	}
	assert.Equal(t, want, got)
}

// TestMIRRewardDeltaColumnRoundTripsSigned proves the reward amount column and
// its encoder and decoder carry a sign. A MIR reward is delta_coin, so a
// negative delta has to read back as the value that was written rather than
// being refused by the coin encoder or rejected by the unsigned parser.
//
// The certificate type gouroboros currently exposes cannot hold a negative
// delta, so this exercises the persistence encoding directly; the end-to-end
// certificate path is covered by
// TestApplyMIRCertificatePersistsProjectedDeltas.
func TestMIRRewardDeltaColumnRoundTripsSigned(t *testing.T) {
	t.Parallel()
	store := newMigratedTestStore(t)

	credential := mirTestCredential(0x25).Credential[:]
	for _, delta := range []*big.Int{
		big.NewInt(-450),
		big.NewInt(1_200),
		new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 70)),
	} {
		encoded, err := signedDecimal("MIR reward delta", delta)
		require.NoError(t, err)
		seedMIRRewardRow(t, store, 0, credential, encoded)
	}

	effects, err := store.GetMIRCertsInSlotRange(0, 1_000, nil)
	require.NoError(t, err)
	require.Len(t, effects, 3)
	got := []string{}
	for _, effect := range effects {
		require.Len(t, effect.Rewards, 1)
		require.NotNil(t, effect.Rewards[0].Amount)
		got = append(got, effect.Rewards[0].Amount.String())
	}
	assert.Equal(
		t,
		[]string{"-450", "1200", "-1180591620717411303424"},
		got,
	)
}

// TestSignedDecimalRejectsMissingDelta pins that a missing delta is reported
// rather than written as zero, so a certificate that cannot be represented
// fails at the boundary that cannot represent it.
func TestSignedDecimalRejectsMissingDelta(t *testing.T) {
	t.Parallel()
	_, err := signedDecimal("MIR reward delta", nil)
	require.ErrorContains(t, err, "MIR reward delta")
}

// decodeMIRDistributionCertificate builds a distribution MIR certificate
// through the CBOR decoder, so the test does not depend on the Go type of the
// reward map.
func decodeMIRDistributionCertificate(
	t *testing.T,
	source uint,
	rewards map[*lcommon.Credential]uint64,
) *lcommon.MoveInstantaneousRewardsCertificate {
	t.Helper()
	encoded, err := cbor.Encode(struct {
		cbor.StructAsArray
		Source  uint
		Rewards map[*lcommon.Credential]uint64
	}{
		Source:  source,
		Rewards: rewards,
	})
	require.NoError(t, err)
	cert := &lcommon.MoveInstantaneousRewardsCertificate{
		CertType: uint(lcommon.CertificateTypeMoveInstantaneousRewards),
	}
	require.NoError(t, cert.Reward.UnmarshalCBOR(encoded))
	return cert
}

// TestGetAccountSumsByCredentialSumsSignedMIRDeltas proves the reserves and
// treasury aggregate reads sum signed values. Summing them through the coin
// helper would reject the negative row outright; ignoring the sign would report
// a total larger than the account ever received.
func TestGetAccountSumsByCredentialSumsSignedMIRDeltas(t *testing.T) {
	t.Parallel()
	store := newMigratedTestStore(t)

	credential := mirTestCredential(0x23).Credential[:]
	seedMIRRewardRow(t, store, 0, credential, "1000")
	seedMIRRewardRow(t, store, 0, credential, "-250")
	seedMIRRewardRow(t, store, 1, credential, "700")
	seedMIRRewardRow(t, store, 1, credential, "-900")

	sums, err := store.GetAccountSumsByCredential(0, credential, nil)
	require.NoError(t, err)
	require.NotNil(t, sums.ReservesSum)
	require.NotNil(t, sums.TreasurySum)
	assert.Equal(t, "750", sums.ReservesSum.String())
	assert.Equal(t, "-200", sums.TreasurySum.String())
}

// TestGetAccountSumsByCredentialWithoutMIRHistory pins the zero value the
// aggregate reads return when there is nothing to sum, so the signed totals are
// never handed to a caller as nil. The empty-credential case never reaches a
// query, so it is the one that depends on the returned value being initialized.
func TestGetAccountSumsByCredentialWithoutMIRHistory(t *testing.T) {
	t.Parallel()
	store := newMigratedTestStore(t)

	for _, test := range []struct {
		name       string
		credential []byte
	}{
		{
			name:       "known credential with no MIR rows",
			credential: mirTestCredential(0x24).Credential[:],
		},
		{
			name:       "empty credential short-circuits the query",
			credential: nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sums, err := store.GetAccountSumsByCredential(
				0,
				test.credential,
				nil,
			)
			require.NoError(t, err)
			require.NotNil(t, sums.ReservesSum)
			require.NotNil(t, sums.TreasurySum)
			assert.Equal(t, "0", sums.ReservesSum.String())
			assert.Equal(t, "0", sums.TreasurySum.String())
		})
	}
}

func seedMIRRewardRow(
	t *testing.T,
	store *Store,
	pot uint,
	credential []byte,
	amount string,
) {
	t.Helper()
	var mirID int64
	require.NoError(t, store.writeDB.QueryRow(`
INSERT INTO move_instantaneous_rewards (pot, certificate_id, added_slot, other_pot)
VALUES (?, 0, 100, '0')
RETURNING id`,
		pot,
	).Scan(&mirID))
	_, err := store.writeDB.Exec(`
INSERT INTO move_instantaneous_rewards_reward (
    credential, credential_tag, amount, mir_id
) VALUES (?, 0, ?, ?)`,
		credential,
		amount,
		mirID,
	)
	require.NoError(t, err)
}

// legacyLatestPoolOpCertSequenceQuery reproduces LatestPoolOpCertSequence's
// pre-fix statement: MAX(sequence) paired with COUNT(*) in the same
// aggregate query so "found" could be read off COUNT(*) > 0. Kept here as
// the comparison point this file benchmarks and correctness-checks the
// MAX-only rewrite against.
const legacyLatestPoolOpCertSequenceQuery = `
SELECT COALESCE(MAX(sequence), 0), COUNT(*)
FROM pool_opcert_sequence
WHERE pool_key_hash = ?`

// seedPoolOpCertSequence inserts n rows for a single pool, one per
// (slot, sequence) pair -- the shape a long-producing pool leaves in
// pool_opcert_sequence after a from-genesis sync on a network with a
// concentrated stake distribution (a handful of pools producing almost
// every block).
func seedPoolOpCertSequence(
	tb testing.TB,
	store *Store,
	poolKeyHash []byte,
	n int,
) {
	tb.Helper()
	tx, err := store.writeDB.Begin()
	require.NoError(tb, err)
	stmt, err := tx.Prepare(
		"INSERT INTO pool_opcert_sequence (pool_key_hash, slot, sequence) " +
			"VALUES (?, ?, ?)",
	)
	require.NoError(tb, err)
	for i := range n {
		_, err := stmt.Exec(poolKeyHash, int64(i)*20, int64(i))
		require.NoError(tb, err)
	}
	require.NoError(tb, stmt.Close())
	require.NoError(tb, tx.Commit())
}

// TestLatestPoolOpCertSequenceNoRowsReturnsNotFound covers the case the
// MAX-only rewrite has to keep exact: a pool with no pool_opcert_sequence
// rows at all. The old query read "found" off COUNT(*) > 0; the new one
// reads it off MAX(sequence) being non-NULL, which UpdatePoolOpCertSequence
// (the table's only writer, always inserting a concrete sequence) makes
// equivalent.
func TestLatestPoolOpCertSequenceNoRowsReturnsNotFound(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	other := make([]byte, 28)
	other[0] = 0x01
	seedPoolOpCertSequence(t, store, other, 5)

	missing := make([]byte, 28)
	missing[0] = 0x02
	sequence, found, err := store.LatestPoolOpCertSequence(
		lcommon.PoolKeyHash(missing),
		nil,
	)
	require.NoError(t, err)
	require.False(t, found)
	require.Zero(t, sequence)
}

// TestLatestPoolOpCertSequenceMatchesLegacyQuery proves the MAX-only rewrite
// returns the same (sequence, found) pair the COUNT(*)-paired legacy query
// did, for both a populated and an absent pool.
func TestLatestPoolOpCertSequenceMatchesLegacyQuery(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	hot := make([]byte, 28)
	hot[0] = 0xAA
	seedPoolOpCertSequence(t, store, hot, 1_000)

	for _, pkh := range [][]byte{hot, bytes.Repeat([]byte{0xFF}, 28)} {
		var legacySeq, legacyCount int64
		require.NoError(t, store.writeDB.QueryRow(
			legacyLatestPoolOpCertSequenceQuery, pkh,
		).Scan(&legacySeq, &legacyCount))

		gotSeq, gotFound, err := store.LatestPoolOpCertSequence(
			lcommon.PoolKeyHash(pkh), nil,
		)
		require.NoError(t, err)
		require.Equal(t, legacyCount > 0, gotFound)
		require.Equal(t, uint64(legacySeq), gotSeq)
	}
}

// BenchmarkLatestPoolOpCertSequence is the timing counterpart: pairing
// MAX(sequence) with COUNT(*) (the legacy form) defeats SQLite's min/max
// optimization on idx_pool_opcert_sequence_pool_sequence, forcing a scan of
// every row recorded for the pool instead of a single index descent to the
// largest one. n scales with how many blocks a single pool has produced by
// the time a from-genesis sync reaches it.
func BenchmarkLatestPoolOpCertSequence(b *testing.B) {
	for _, n := range []int{1_000, 50_000, 200_000} {
		store := newMigratedSQLiteStore(b)
		hot := make([]byte, 28)
		hot[0] = 0xAA
		seedPoolOpCertSequence(b, store, hot, n)
		pkh := lcommon.PoolKeyHash(hot)

		b.Run(fmt.Sprintf("n=%d/legacy_max_and_count", n), func(b *testing.B) {
			for b.Loop() {
				var seq, count int64
				if err := store.writeDB.QueryRow(
					legacyLatestPoolOpCertSequenceQuery, hot,
				).Scan(&seq, &count); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("n=%d/max_only", n), func(b *testing.B) {
			for b.Loop() {
				if _, _, err := store.LatestPoolOpCertSequence(pkh, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// testPoolOpCertSequencesExistAtSlot checks the exact-slot probe the ledger
// reads at a Mithril trust boundary: rows at neighbouring slots, from any
// pool, must not count, and one row at the slot must.
func testPoolOpCertSequencesExistAtSlot(t *testing.T, store *Store) {
	t.Helper()
	const boundary = uint64(100)
	exists, err := store.PoolOpCertSequencesExistAtSlot(boundary, nil)
	require.NoError(t, err)
	require.False(t, exists, "an empty table has no row at any slot")

	poolA := lcommon.PoolKeyHash(lcommon.NewBlake2b224([]byte("opcert-slot-a")))
	poolB := lcommon.PoolKeyHash(lcommon.NewBlake2b224([]byte("opcert-slot-b")))
	require.NoError(t, store.UpdatePoolOpCertSequence(poolA, 3, boundary-1, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(poolB, 4, boundary+1, nil))
	exists, err = store.PoolOpCertSequencesExistAtSlot(boundary, nil)
	require.NoError(t, err)
	require.False(t, exists, "rows either side of the slot must not count")

	require.NoError(t, store.UpdatePoolOpCertSequence(poolB, 2, boundary, nil))
	exists, err = store.PoolOpCertSequencesExistAtSlot(boundary, nil)
	require.NoError(t, err)
	require.True(t, exists)

	txn := store.Transaction(t.Context())
	defer func() { _ = txn.Rollback() }()
	exists, err = store.PoolOpCertSequencesExistAtSlot(boundary, txn)
	require.NoError(t, err)
	require.True(t, exists, "the probe must read through a caller transaction")
}

func TestSQLitePoolOpCertSequencesExistAtSlot(t *testing.T) {
	t.Parallel()
	testPoolOpCertSequencesExistAtSlot(t, newMigratedTestStore(t))
}

func TestSQLitePParamUpdateOrdering(t *testing.T) {
	t.Parallel()
	testPParamUpdateOrdering(t, newMigratedSQLiteStore(t))
}

// testPParamUpdateOrdering pins the storage contract classic update
// enactment relies on: GetPParamUpdates(e) returns the rows for e and e-1
// with their stored fields, and row IDs follow insertion order, including
// after a rollback deletes the newest rows, so (added slot, ID) is chain
// order for a genesis key's proposals within one slot.
func testPParamUpdateOrdering(t *testing.T, store *Store) {
	t.Helper()
	type row struct {
		genesis byte
		slot    uint64
		epoch   uint64
	}
	insert := func(r row) {
		require.NoError(t, store.SetPParamUpdate(
			[]byte{
				r.genesis,
			},
			[]byte{0xa1, 0x00, r.genesis},
			r.slot,
			r.epoch,
			nil,
		))
	}
	for _, r := range []row{
		{genesis: 4, slot: 200, epoch: 2},
		{genesis: 3, slot: 250, epoch: 3},
		{genesis: 1, slot: 300, epoch: 3},
		{genesis: 2, slot: 305, epoch: 3},
		{genesis: 1, slot: 305, epoch: 3},
		{genesis: 5, slot: 360, epoch: 4},
	} {
		insert(r)
	}
	read := func() []models.PParamUpdate {
		rows, err := store.GetPParamUpdates(3, nil)
		require.NoError(t, err)
		return rows
	}
	byInsertion := func(rows []models.PParamUpdate) []models.PParamUpdate {
		ret := append([]models.PParamUpdate(nil), rows...)
		for i := 1; i < len(ret); i++ {
			for j := i; j > 0 && ret[j].ID < ret[j-1].ID; j-- {
				ret[j], ret[j-1] = ret[j-1], ret[j]
			}
		}
		return ret
	}

	rows := byInsertion(read())
	require.Len(t, rows, 5)
	want := []row{
		{genesis: 4, slot: 200, epoch: 2},
		{genesis: 3, slot: 250, epoch: 3},
		{genesis: 1, slot: 300, epoch: 3},
		{genesis: 2, slot: 305, epoch: 3},
		{genesis: 1, slot: 305, epoch: 3},
	}
	for i, w := range want {
		require.Equal(t, []byte{w.genesis}, rows[i].GenesisHash, "row %d", i)
		require.Equal(
			t,
			[]byte{0xa1, 0x00, w.genesis},
			rows[i].Cbor,
			"row %d",
			i,
		)
		require.Equal(t, w.slot, rows[i].AddedSlot, "row %d", i)
		require.Equal(t, w.epoch, rows[i].Epoch, "row %d", i)
	}

	require.NoError(t, store.DeletePParamUpdatesAfterSlot(300, nil))
	survivors := byInsertion(read())
	require.Len(t, survivors, 3)
	insert(row{genesis: 2, slot: 305, epoch: 3})
	insert(row{genesis: 1, slot: 305, epoch: 3})
	rows = byInsertion(read())
	require.Len(t, rows, 5)
	require.Equal(t, survivors, rows[:3])
	require.Greater(t, rows[3].ID, survivors[2].ID)
	require.Equal(t, []byte{2}, rows[3].GenesisHash)
	require.Equal(t, []byte{1}, rows[4].GenesisHash)
	require.Greater(t, rows[4].ID, rows[3].ID)
}

// seedRewardLiveStakeAccount inserts one registered `account` row directly
// against the real migrated schema, giving refreshRewardLiveStakeAggregate's
// account lookup (rewardLiveStakeAccountQuery) a row to actually match, so
// tests exercise the reward-carrying, registered path rather than always
// hitting sql.ErrNoRows.
func seedRewardLiveStakeAccount(
	tb testing.TB,
	store *Store,
	ref models.StakeCredentialRef,
	pool []byte,
	reward uint64,
) {
	tb.Helper()
	_, err := store.writeDB.Exec(
		`INSERT INTO account (staking_key, credential_tag, pool, reward, active, added_slot)
VALUES (?, ?, ?, ?, TRUE, ?)`,
		ref.Key,
		int64(ref.Tag),
		pool,
		decimalUint64(types.Uint64(reward)),
		int64(1),
	)
	require.NoError(tb, err)
}

// oneShotRefreshRewardLiveStakeAggregate reproduces refreshRewardLiveStakeAggregate
// exactly, except its account lookup and upsert are issued as plain one-shot
// QueryRowContext/ExecContext calls instead of going through Store's cached
// statements. It is the direct before/after benchmark and correctness
// comparison point for the rewardLiveStakeAccountQuery/rewardLiveStakeUpsertQuery
// cache entries, mirroring oneShotSumCredentialUtxoStake.
func oneShotRefreshRewardLiveStakeAggregate(
	ctx context.Context,
	s *Store,
	db queryer,
	ref models.StakeCredentialRef,
	slot uint64,
) error {
	if len(ref.Key) == 0 {
		return nil
	}
	var reward sql.NullString
	var pool []byte
	var active sql.NullBool
	var addedSlot sql.NullInt64
	accountErr := db.QueryRowContext(ctx, rewardLiveStakeAccountQuery,
		ref.Tag, ref.Key,
	).Scan(&reward, &pool, &active, &addedSlot)
	if accountErr != nil && !isNoRows(accountErr) {
		return fmt.Errorf("query reward live stake account: %w", accountErr)
	}
	utxoStake, err := s.sumCredentialUtxoStake(ctx, db, ref)
	if err != nil {
		return fmt.Errorf("sum reward live stake UTxOs: %w", err)
	}
	rewardStake := uint64(0)
	registered := false
	if accountErr == nil {
		registered = active.Bool
		if reward.Valid {
			value, err := parseUint64("reward live stake reward", reward.String)
			if err != nil {
				return err
			}
			rewardStake = value
		}
	}
	total := utxoStake + rewardStake
	if isNoRows(accountErr) && total == 0 {
		_, err := db.ExecContext(ctx,
			`DELETE FROM reward_live_stake WHERE credential_tag = ? AND staking_key = ?`,
			ref.Tag, ref.Key,
		)
		return err
	}
	if !registered {
		pool = nil
	}
	delegationSlot := int64(0)
	if registered && len(pool) > 0 {
		delegationSlot = addedSlot.Int64
	}
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, rewardLiveStakeUpsertQuery,
		ref.Tag,
		ref.Key,
		pool,
		decimalUint64(types.Uint64(utxoStake)),
		decimalUint64(types.Uint64(rewardStake)),
		decimalUint64(types.Uint64(total)),
		registered,
		delegationSlot,
		slotValue,
		models.RewardStakeCalculationVersion,
	)
	if err != nil {
		return fmt.Errorf("upsert reward live stake: %w", err)
	}
	return nil
}

func isNoRows(err error) bool {
	return err == sql.ErrNoRows
}

// readRewardLiveStake fetches the row refreshRewardLiveStakeAggregate wrote,
// for asserting on its computed totals.
func readRewardLiveStake(
	tb testing.TB,
	store *Store,
	ref models.StakeCredentialRef,
) (utxoStake, rewardStake, totalStake uint64, registered bool) {
	tb.Helper()
	var utxo, rwd, tot string
	row := store.writeDB.QueryRow(
		`SELECT utxo_stake, reward_stake, total_stake, registered
FROM reward_live_stake WHERE credential_tag = ? AND staking_key = ?`,
		ref.Tag, ref.Key,
	)
	require.NoError(tb, row.Scan(&utxo, &rwd, &tot, &registered))
	u, err := parseUint64("utxo_stake", utxo)
	require.NoError(tb, err)
	r, err := parseUint64("reward_stake", rwd)
	require.NoError(tb, err)
	t, err := parseUint64("total_stake", tot)
	require.NoError(tb, err)
	return u, r, t, registered
}

// requireNoRewardLiveStakeRow asserts no reward_live_stake row exists for
// ref, the expected outcome of refreshRewardLiveStakeAggregate's early
// DELETE-and-return path (never registered, zero total).
func requireNoRewardLiveStakeRow(
	tb testing.TB,
	store *Store,
	ref models.StakeCredentialRef,
) {
	tb.Helper()
	var exists bool
	err := store.writeDB.QueryRow(
		`SELECT EXISTS (SELECT 1 FROM reward_live_stake WHERE credential_tag = ? AND staking_key = ?)`,
		ref.Tag, ref.Key,
	).Scan(&exists)
	require.NoError(tb, err)
	require.False(tb, exists, "expected no reward_live_stake row")
}

// TestRefreshRewardLiveStakeAggregateCachesAccountAndUpsertStatements proves
// Start eagerly caches both new per-touch queries (not just
// sumCredentialUtxoStakeQuery) and that repeated lookups return the same
// *sql.Stmt. This is the regression check for the caching change itself:
// against the prior version of this function, lookupCachedStmt for either
// query returned ok=false, since neither was in hotStatements.
func TestRefreshRewardLiveStakeAggregateCachesAccountAndUpsertStatements(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	accountStmt, ok := store.lookupCachedStmt(rewardLiveStakeAccountQuery)
	require.True(t, ok, "expected the account lookup to be cached")
	require.NotNil(t, accountStmt)

	upsertStmt, ok := store.lookupCachedStmt(rewardLiveStakeUpsertQuery)
	require.True(t, ok, "expected the upsert to be cached")
	require.NotNil(t, upsertStmt)

	require.NotSame(
		t,
		accountStmt,
		upsertStmt,
		"the two queries must not share a cache slot",
	)
}

// TestRefreshRewardLiveStakeAggregateMatchesOneShot proves the cached path
// and the one-shot path compute identical reward_live_stake rows across an
// unregistered credential (no account row), a registered credential with a
// reward and delegated pool, and a credential whose UTxO stake changes
// across repeated touches -- the same access pattern real block processing
// uses (repeated calls through separate write transactions).
func TestRefreshRewardLiveStakeAggregateMatchesOneShot(t *testing.T) {
	t.Parallel()

	pool := make([]byte, 28)
	pool[0] = 0xAA

	cases := []struct {
		name          string
		seedAccount   bool
		reward        uint64
		utxoAmounts   []uint64
		wantUtxoStake uint64
		wantReward    uint64
	}{
		{
			name:          "no account, no utxos",
			wantUtxoStake: 0,
			wantReward:    0,
		},
		{
			name:          "registered account with reward, no utxos",
			seedAccount:   true,
			reward:        1_000_000,
			wantUtxoStake: 0,
			wantReward:    1_000_000,
		},
		{
			name:          "registered account with reward and utxos",
			seedAccount:   true,
			reward:        2_500_000,
			utxoAmounts:   []uint64{5_000_000, 7},
			wantUtxoStake: 5_000_007,
			wantReward:    2_500_000,
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cachedStore := newMigratedSQLiteStore(t)
			oneShotStore := newMigratedSQLiteStore(t)

			ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(i))
			for _, store := range []*Store{cachedStore, oneShotStore} {
				if tc.seedAccount {
					seedRewardLiveStakeAccount(t, store, ref, pool, tc.reward)
				}
				if len(tc.utxoAmounts) > 0 {
					seedCredentialUtxos(t, store, i, ref, tc.utxoAmounts, nil)
				}
			}

			require.NoError(t, cachedStore.withWriteTransaction(
				nil,
				func(db queryer, ctx context.Context) error {
					return cachedStore.refreshRewardLiveStakeAggregate(ctx, db, ref, 100)
				},
			))
			require.NoError(t, oneShotStore.withWriteTransaction(
				nil,
				func(db queryer, ctx context.Context) error {
					return oneShotRefreshRewardLiveStakeAggregate(ctx, oneShotStore, db, ref, 100)
				},
			))

			if !tc.seedAccount && len(tc.utxoAmounts) == 0 {
				// Both variants take the early DELETE-and-return path (no
				// account, zero total) and never reach the upsert, so no
				// row is ever created for either.
				requireNoRewardLiveStakeRow(t, cachedStore, ref)
				requireNoRewardLiveStakeRow(t, oneShotStore, ref)
				return
			}

			cachedUtxo, cachedReward, cachedTotal, cachedRegistered := readRewardLiveStake(t, cachedStore, ref)
			oneShotUtxo, oneShotReward, oneShotTotal, oneShotRegistered := readRewardLiveStake(t, oneShotStore, ref)

			require.Equal(t, tc.wantUtxoStake, cachedUtxo)
			require.Equal(t, tc.wantReward, cachedReward)
			require.Equal(t, oneShotUtxo, cachedUtxo)
			require.Equal(t, oneShotReward, cachedReward)
			require.Equal(t, oneShotTotal, cachedTotal)
			require.Equal(t, oneShotRegistered, cachedRegistered)
			require.Equal(t, tc.seedAccount, cachedRegistered)
		})
	}
}

// TestRefreshRewardLiveStakeAggregateReusesCachedStatementsAcrossTransactions
// proves both new cached statements are actually reused across independent
// write transactions -- the real access pattern (one write transaction per
// block/UTxO touch) -- the way
// TestSumCredentialUtxoStakeReusesCachedStatementAcrossTransactions already
// proves for sumCredentialUtxoStakeQuery.
func TestRefreshRewardLiveStakeAggregateReusesCachedStatementsAcrossTransactions(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))

	touch := func(slot uint64) {
		err := store.withWriteTransaction(
			nil,
			func(db queryer, ctx context.Context) error {
				return store.refreshRewardLiveStakeAggregate(ctx, db, ref, slot)
			},
		)
		require.NoError(t, err)
	}

	touch(1)

	store.stmtMu.Lock()
	firstAccountStmt := store.stmts[rewardLiveStakeAccountQuery]
	firstUpsertStmt := store.stmts[rewardLiveStakeUpsertQuery]
	store.stmtMu.Unlock()
	require.NotNil(t, firstAccountStmt)
	require.NotNil(t, firstUpsertStmt)

	seedCredentialUtxos(t, store, 1, ref, []uint64{9_000_000}, nil)
	touch(2)

	store.stmtMu.Lock()
	secondAccountStmt := store.stmts[rewardLiveStakeAccountQuery]
	secondUpsertStmt := store.stmts[rewardLiveStakeUpsertQuery]
	store.stmtMu.Unlock()

	require.Same(t, firstAccountStmt, secondAccountStmt,
		"expected the account lookup statement to be reused across transactions")
	require.Same(t, firstUpsertStmt, secondUpsertStmt,
		"expected the upsert statement to be reused across transactions")

	utxoStake, _, _, _ := readRewardLiveStake(t, store, ref)
	require.Equal(t, uint64(9_000_000), utxoStake)
}

// BenchmarkRefreshRewardLiveStakeAggregateAccountAndUpsert is the timing
// counterpart to BenchmarkSumCredentialUtxoStake: it isolates just the two
// newly cached queries (a registered credential with no live UTxOs, so
// sumCredentialUtxoStake's own cost is negligible and constant across both
// variants) to measure the one-shot-vs-cached parse-cost delta in isolation.
func BenchmarkRefreshRewardLiveStakeAggregateAccountAndUpsert(b *testing.B) {
	pool := make([]byte, 28)
	pool[0] = 0xBB

	b.Run("one_shot", func(b *testing.B) {
		store := newMigratedSQLiteStore(b)
		ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
		seedRewardLiveStakeAccount(b, store, ref, pool, 1_000_000)
		for n := 0; b.Loop(); n++ {
			err := store.withWriteTransaction(
				nil,
				func(db queryer, ctx context.Context) error {
					return oneShotRefreshRewardLiveStakeAggregate(
						ctx, store, db, ref, uint64(n+1),
					)
				},
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("prepared_cache", func(b *testing.B) {
		store := newMigratedSQLiteStore(b)
		ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
		seedRewardLiveStakeAccount(b, store, ref, pool, 1_000_000)
		for n := 0; b.Loop(); n++ {
			err := store.withWriteTransaction(
				nil,
				func(db queryer, ctx context.Context) error {
					return store.refreshRewardLiveStakeAggregate(
						ctx, db, ref, uint64(n+1),
					)
				},
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// populateRewardLiveStakeBatchFixture writes stake keys under both
// credential tags, including keys that are byte prefixes of one another, so
// that batch boundaries fall between tags and between a key and its
// extensions. Some keys have an account only, some a live UTxO only, and
// some both; every third account delegates, some more than once. It writes
// only through dialect-translated statements, so it runs on every dialect.
func populateRewardLiveStakeBatchFixture(t testing.TB, store *Store) int {
	t.Helper()
	keys := make([]models.StakeCredentialRef, 0)
	for tag := range uint8(2) {
		for index := range 12 {
			base := []byte{byte(index / 3)}
			for extension := range index % 3 {
				base = append(base, byte(extension))
			}
			keys = append(keys, models.NewStakeCredentialRef(tag, base))
		}
	}
	db := store.instrumentedQueryer(store.writeDB)
	var utxos []models.Utxo
	for index, ref := range keys {
		hasAccount := index%4 != 3
		hasUtxo := index%4 != 0
		pooled := hasAccount && index%3 == 0
		if hasAccount {
			account := &models.Account{
				StakingKey: ref.Key, CredentialTag: ref.Tag,
				AddedSlot: uint64(10 + index), CreatedSlot: uint64(10 + index),
				Reward: types.Uint64(uint64(index)), Active: index%5 != 0,
			}
			if pooled {
				account.Pool = []byte{0xa0, byte(index % 2)}
			}
			require.NoError(t, store.ImportAccount(account, nil))
		}
		if hasUtxo {
			for output := range 1 + index%3 {
				txID := make([]byte, 32)
				txID[0], txID[1], txID[2] = ref.Tag, byte(index), byte(output)
				utxos = append(utxos, models.Utxo{
					TxId: txID, OutputIdx: uint32(output),
					StakingKey: ref.Key, CredentialTag: ref.Tag,
					AddedSlot: uint64(100 + index),
					Amount:    types.Uint64(uint64(1000*index + output)),
				})
			}
		}
		if !pooled {
			continue
		}
		assignments := []struct {
			pool []byte
			slot int
		}{{[]byte{0xa0, byte(index % 2)}, 20 + index}}
		if index%2 == 0 {
			assignments = append(assignments, struct {
				pool []byte
				slot int
			}{[]byte{0xa0, 0x09}, 30 + index})
		}
		for _, assignment := range assignments {
			_, err := db.ExecContext(t.Context(), `
INSERT INTO stake_delegation
    (staking_key, credential_tag, pool_key_hash, certificate_id, added_slot)
VALUES (?, ?, ?, NULL, ?)`,
				ref.Key, ref.Tag, assignment.pool, assignment.slot,
			)
			require.NoError(t, err)
		}
	}
	require.NoError(t, store.ImportUtxos(utxos, nil))
	return len(keys)
}

// testRewardLiveStakeBatchBoundaries proves that splitting the rebuild into
// key-range batches neither drops nor alters a row at any boundary. Both
// rebuilds are idempotent over unchanged inputs, so one store is rebuilt as a
// single batch to establish the expected table and then again at each batch
// size.
func testRewardLiveStakeBatchBoundaries(t *testing.T, store *Store) {
	keys := populateRewardLiveStakeBatchFixture(t, store)
	for _, fromRunningTotals := range []bool{false, true} {
		rebuild := func(batch int) map[string]rewardLiveStakeSnapshotRow {
			store.rewardLiveStakeBatchSize = batch
			if fromRunningTotals {
				require.NoError(
					t,
					store.RebuildRewardLiveStakeFromRunningTotals(500, nil),
				)
			} else {
				require.NoError(t, store.RebuildRewardLiveStake(500, nil))
			}
			return readRewardLiveStakeSnapshot(t, store)
		}
		want := rebuild(keys + 1)
		require.Len(t, want, keys)
		for _, batch := range []int{1, 2, 3, 5, 7, keys - 1, keys} {
			require.Equal(
				t,
				want,
				rebuild(batch),
				"running=%t batch=%d",
				fromRunningTotals,
				batch,
			)
		}
	}
}

func TestRebuildRewardLiveStakeBatchBoundaries(t *testing.T) {
	t.Parallel()
	testRewardLiveStakeBatchBoundaries(t, newMigratedSQLiteStore(t))
}

// readUtxoStake reads a credential's stored reward_live_stake.utxo_stake
// column, the running total refreshRewardLiveStakeAggregateDelta maintains.
func readUtxoStake(
	tb testing.TB,
	store *Store,
	ref models.StakeCredentialRef,
) uint64 {
	tb.Helper()
	var raw string
	err := store.writeDB.QueryRow(`
SELECT utxo_stake FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`,
		int64(ref.Tag), ref.Key,
	).Scan(&raw)
	require.NoError(tb, err)
	value, err := parseUint64("test utxo stake", raw)
	require.NoError(tb, err)
	return value
}

// establishRunningTotal seeds a credential's reward_live_stake row via the
// authoritative full-scan path, the way a credential's real first touch
// would -- refreshRewardLiveStakeAggregateDelta's tests build on this
// baseline rather than starting from an empty table, so they exercise the
// warm incremental path instead of its cold-start fallback.
func establishRunningTotal(
	tb testing.TB,
	store *Store,
	ref models.StakeCredentialRef,
	slot uint64,
) {
	tb.Helper()
	require.NoError(tb, store.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			return store.refreshRewardLiveStakeAggregate(ctx, db, ref, slot)
		},
	))
}

// insertLiveUtxoTx inserts one additional live UTxO row for ref through db
// (the caller's write-transaction handle), so it participates in the same
// transaction as a refreshRewardLiveStakeAggregateDelta call issued
// alongside it -- mirroring how a real gain and its delta application share
// one write transaction. txSeed must be distinct per call within a test to
// avoid tx_id collisions with other rows the test seeds.
func insertLiveUtxoTx(
	ctx context.Context,
	db queryer,
	ref models.StakeCredentialRef,
	txSeed byte,
	amount uint64,
) error {
	txID := make([]byte, 32)
	txID[0] = txSeed
	_, err := db.ExecContext(ctx, `
INSERT INTO utxo (tx_id, output_idx, staking_key, credential_tag, added_slot, deleted_slot, amount)
VALUES (?, 0, ?, ?, 1, 0, ?)`,
		txID, ref.Key, int64(ref.Tag), decimalUint64(types.Uint64(amount)),
	)
	return err
}

// markUtxoDeletedTx marks the one live UTxO of the given amount deleted
// through db, failing if that does not match exactly one row -- a test
// asserting a specific loss delta needs to know precisely which row it
// removed.
func markUtxoDeletedTx(
	ctx context.Context,
	db queryer,
	ref models.StakeCredentialRef,
	amount uint64,
	deletedSlot int64,
) error {
	result, err := db.ExecContext(
		ctx,
		`
UPDATE utxo SET deleted_slot = ?
WHERE credential_tag = ? AND staking_key = ? AND amount = ? AND deleted_slot = 0`,
		deletedSlot,
		int64(ref.Tag),
		ref.Key,
		decimalUint64(types.Uint64(amount)),
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf(
			"expected exactly one live UTxO of amount %d, affected %d",
			amount,
			affected,
		)
	}
	return nil
}

// TestRefreshRewardLiveStakeAggregateDeltaGain proves a single UTxO gain
// applied through the incremental path produces the same stored total a
// fresh authoritative sumCredentialUtxoStake scan would, and that it does so
// without falling back to that scan (the whole point of dingo #4421).
func TestRefreshRewardLiveStakeAggregateDeltaGain(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))

	seedCredentialUtxos(
		t, store, 0, ref,
		[]uint64{1_000_000, 2_000_000, 3_000_000}, nil,
	)
	establishRunningTotal(t, store, ref, 1)
	require.Equal(t, uint64(6_000_000), readUtxoStake(t, store, ref))

	const gain = 500_000
	before := store.sumCredentialUtxoStakeCalls.Load()
	require.NoError(t, store.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			if err := insertLiveUtxoTx(ctx, db, ref, 0x10, gain); err != nil {
				return err
			}
			return store.refreshRewardLiveStakeAggregateDelta(
				ctx, db, ref, 2, gain,
			)
		},
	))
	require.Equal(
		t,
		before,
		store.sumCredentialUtxoStakeCalls.Load(),
		"a warm running total must not fall back to a full scan",
	)

	got := readUtxoStake(t, store, ref)
	want, err := store.sumCredentialUtxoStake(ctx, store.writeDB, ref)
	require.NoError(t, err)
	require.Equal(t, want, got, "must match a fresh authoritative scan")
	require.Equal(t, uint64(6_500_000), got)
}

// TestRefreshRewardLiveStakeAggregateDeltaLoss is
// TestRefreshRewardLiveStakeAggregateDeltaGain's counterpart for a spend.
func TestRefreshRewardLiveStakeAggregateDeltaLoss(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))

	seedCredentialUtxos(
		t, store, 0, ref,
		[]uint64{1_000_000, 2_000_000, 3_000_000}, nil,
	)
	establishRunningTotal(t, store, ref, 1)
	require.Equal(t, uint64(6_000_000), readUtxoStake(t, store, ref))

	before := store.sumCredentialUtxoStakeCalls.Load()
	require.NoError(t, store.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			if err := markUtxoDeletedTx(ctx, db, ref, 2_000_000, 2); err != nil {
				return err
			}
			return store.refreshRewardLiveStakeAggregateDelta(
				ctx, db, ref, 2, -2_000_000,
			)
		},
	))
	require.Equal(
		t,
		before,
		store.sumCredentialUtxoStakeCalls.Load(),
		"a warm running total must not fall back to a full scan",
	)

	got := readUtxoStake(t, store, ref)
	want, err := store.sumCredentialUtxoStake(ctx, store.writeDB, ref)
	require.NoError(t, err)
	require.Equal(t, want, got, "must match a fresh authoritative scan")
	require.Equal(t, uint64(4_000_000), got)
}

// TestRefreshRewardLiveStakeAggregateDeltaRapidSequenceWithinOneBlock applies
// several gains and losses for the same credential inside one write
// transaction -- the shape a block with several transactions touching the
// same address produces, all sharing one *sql.Tx -- and proves the final
// running total matches a fresh authoritative scan of the resulting UTxO
// set, with no fallback scan anywhere in the sequence.
func TestRefreshRewardLiveStakeAggregateDeltaRapidSequenceWithinOneBlock(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))

	// Baseline: A=10,000,000 (survives every step), C=1,000,000 and
	// E=3,000,000 (each spent by one of the steps below).
	seedCredentialUtxos(
		t, store, 0, ref,
		[]uint64{10_000_000, 1_000_000, 3_000_000}, nil,
	)
	establishRunningTotal(t, store, ref, 1)
	require.Equal(t, uint64(14_000_000), readUtxoStake(t, store, ref))

	type step struct {
		delta  int64
		mutate func(db queryer, ctx context.Context) error
	}
	steps := []step{
		{2_000_000, func(db queryer, ctx context.Context) error {
			return insertLiveUtxoTx(ctx, db, ref, 0x20, 2_000_000) // +B
		}},
		{-1_000_000, func(db queryer, ctx context.Context) error {
			return markUtxoDeletedTx(ctx, db, ref, 1_000_000, 2) // -C
		}},
		{5_000_000, func(db queryer, ctx context.Context) error {
			return insertLiveUtxoTx(ctx, db, ref, 0x21, 5_000_000) // +D
		}},
		{-3_000_000, func(db queryer, ctx context.Context) error {
			return markUtxoDeletedTx(ctx, db, ref, 3_000_000, 2) // -E
		}},
		{1_000_000, func(db queryer, ctx context.Context) error {
			return insertLiveUtxoTx(ctx, db, ref, 0x22, 1_000_000) // +F
		}},
	}

	before := store.sumCredentialUtxoStakeCalls.Load()
	require.NoError(t, store.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			for _, st := range steps {
				if err := st.mutate(db, ctx); err != nil {
					return err
				}
				if err := store.refreshRewardLiveStakeAggregateDelta(
					ctx, db, ref, 2, st.delta,
				); err != nil {
					return err
				}
			}
			return nil
		},
	))
	require.Equal(
		t,
		before,
		store.sumCredentialUtxoStakeCalls.Load(),
		"a warm running total must never fall back to a full scan mid-sequence",
	)

	got := readUtxoStake(t, store, ref)
	want, err := store.sumCredentialUtxoStake(ctx, store.writeDB, ref)
	require.NoError(t, err)
	require.Equal(t, want, got, "must match a fresh authoritative scan")
	// 10,000,000 (A) + 2,000,000 (B) + 5,000,000 (D) + 1,000,000 (F); C and E
	// were spent along the way.
	require.Equal(t, uint64(18_000_000), got)
}

// TestRefreshRewardLiveStakeAggregateDeltaAcrossMultipleTransactions proves
// the running total is correctly persisted and re-read across separate,
// independently committed write transactions -- the shape several blocks
// touching the same credential over time produce -- rather than depending on
// any in-memory state carried between calls.
func TestRefreshRewardLiveStakeAggregateDeltaAcrossMultipleTransactions(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))

	seedCredentialUtxos(t, store, 0, ref, []uint64{10_000_000}, nil)
	establishRunningTotal(t, store, ref, 1)
	require.Equal(t, uint64(10_000_000), readUtxoStake(t, store, ref))

	type step struct {
		delta  int64
		mutate func(db queryer, ctx context.Context) error
	}
	steps := []step{
		{2_000_000, func(db queryer, ctx context.Context) error {
			return insertLiveUtxoTx(ctx, db, ref, 0x30, 2_000_000)
		}},
		{-2_000_000, func(db queryer, ctx context.Context) error {
			return markUtxoDeletedTx(ctx, db, ref, 2_000_000, 3)
		}},
		{7_000_000, func(db queryer, ctx context.Context) error {
			return insertLiveUtxoTx(ctx, db, ref, 0x31, 7_000_000)
		}},
	}

	before := store.sumCredentialUtxoStakeCalls.Load()
	for i, st := range steps {
		slot := uint64(2 + i)
		require.NoError(t, store.withWriteTransaction(
			nil,
			func(db queryer, ctx context.Context) error {
				if err := st.mutate(db, ctx); err != nil {
					return err
				}
				return store.refreshRewardLiveStakeAggregateDelta(
					ctx, db, ref, slot, st.delta,
				)
			},
		))
	}
	require.Equal(
		t,
		before,
		store.sumCredentialUtxoStakeCalls.Load(),
		"a warm running total must never fall back to a full scan across "+
			"separate transactions",
	)

	got := readUtxoStake(t, store, ref)
	want, err := store.sumCredentialUtxoStake(ctx, store.writeDB, ref)
	require.NoError(t, err)
	require.Equal(t, want, got, "must match a fresh authoritative scan")
	require.Equal(t, uint64(17_000_000), got)
}

// TestSetTransactionIncrementalDeltaMatchesFullScan drives the production
// entry point (SetTransaction, not the internal helper directly) for a
// credential whose running total is already warm -- established the way an
// earlier block's touch would -- so this transaction's own certificate,
// consumed input, and produced output all take
// refreshRewardLiveStakeAggregateDelta's incremental path together, exactly
// as setTransactionWithAccumulator wires them. It proves the result matches
// a fresh authoritative scan and that no full scan ran.
func TestSetTransactionIncrementalDeltaMatchesFullScan(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	fx := buildSharedCredentialTx(t, 0x07)
	seedConsumedUtxo(t, store, fx)
	establishRunningTotal(t, store, fx.ref, 1)
	require.Equal(
		t,
		fx.consumedAmount,
		readUtxoStake(t, store, fx.ref),
		"baseline must include the UTxO this transaction is about to spend",
	)

	before := store.sumCredentialUtxoStakeCalls.Load()
	require.NoError(t, store.SetTransaction(
		fx.tx, fx.point, 0, fx.certDeposits, false, nil,
	))
	require.Equal(
		t,
		before,
		store.sumCredentialUtxoStakeCalls.Load(),
		"a warm running total must not fall back to a full scan through "+
			"SetTransaction",
	)

	got := readUtxoStake(t, store, fx.ref)
	want, err := store.sumCredentialUtxoStake(ctx, store.writeDB, fx.ref)
	require.NoError(t, err)
	require.Equal(t, want, got, "must match a fresh authoritative scan")
	require.Equal(t, fx.producedAmount, got)
}

// TestApplyUtxoStakeDeltaOverflowAndUnderflow covers applyUtxoStakeDelta's
// negative cases directly: a delta whose magnitude the current total cannot
// absorb must fail closed rather than wrap silently, in both directions.
func TestApplyUtxoStakeDeltaOverflowAndUnderflow(t *testing.T) {
	t.Parallel()
	ref := models.NewStakeCredentialRef(0, []byte("credential"))

	t.Run("ordinary increase", func(t *testing.T) {
		t.Parallel()
		got, err := applyUtxoStakeDelta(100, 50, ref)
		require.NoError(t, err)
		require.Equal(t, uint64(150), got)
	})

	t.Run("ordinary decrease", func(t *testing.T) {
		t.Parallel()
		got, err := applyUtxoStakeDelta(100, -40, ref)
		require.NoError(t, err)
		require.Equal(t, uint64(60), got)
	})

	t.Run("decrease to exactly zero", func(t *testing.T) {
		t.Parallel()
		got, err := applyUtxoStakeDelta(100, -100, ref)
		require.NoError(t, err)
		require.Equal(t, uint64(0), got)
	})

	t.Run("underflow", func(t *testing.T) {
		t.Parallel()
		_, err := applyUtxoStakeDelta(100, -101, ref)
		require.Error(t, err)
		require.ErrorContains(t, err, "underflow")
	})

	t.Run("overflow", func(t *testing.T) {
		t.Parallel()
		_, err := applyUtxoStakeDelta(^uint64(0), 1, ref)
		require.Error(t, err)
		require.ErrorContains(t, err, "overflow")
	})
}

// TestMergeStakeCredentialDeltasSumsPerCredential proves
// mergeStakeCredentialDeltas adds every occurrence's delta for a credential
// (unlike mergeStakeCredentialRefs, which keeps only the first), and that
// order follows first occurrence.
func TestMergeStakeCredentialDeltasSumsPerCredential(t *testing.T) {
	t.Parallel()

	a := models.NewStakeCredentialRef(0, []byte("credential-a"))
	b := models.NewStakeCredentialRef(0, []byte("credential-b"))

	t.Run("all empty", func(t *testing.T) {
		t.Parallel()
		got := mergeStakeCredentialDeltas(nil, []stakeCredentialDelta{}, nil)
		require.Empty(t, got)
	})

	t.Run(
		"sums overlapping deltas across and within slices",
		func(t *testing.T) {
			t.Parallel()
			got := mergeStakeCredentialDeltas(
				[]stakeCredentialDelta{{ref: a, delta: 5}, {ref: b, delta: -2}},
				[]stakeCredentialDelta{{ref: a, delta: -3}},
				[]stakeCredentialDelta{{ref: a, delta: 10}, {ref: b, delta: 1}},
			)
			byKey := make(map[string]int64, len(got))
			for _, d := range got {
				byKey[d.ref.MapKey()] = d.delta
			}
			require.Equal(t, int64(12), byKey[a.MapKey()]) // 5 - 3 + 10
			require.Equal(t, int64(-1), byKey[b.MapKey()]) // -2 + 1
			require.Len(t, got, 2)
		},
	)
}

// TestQueryUtxoStakeConsumedDeltasNegatesAmounts proves
// queryUtxoStakeConsumedDeltas reports the exact negative delta for each
// spent input's credential, grouping and summing when several consumed
// inputs share a credential.
func TestQueryUtxoStakeConsumedDeltasNegatesAmounts(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	refA := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
	refB := models.NewStakeCredentialRef(0, credentialKeyForIndex(1))

	seedCredentialUtxos(t, store, 0, refA, []uint64{4_000_000, 6_000_000}, nil)
	seedCredentialUtxos(t, store, 1, refB, []uint64{9_000_000}, nil)

	ids := []models.UtxoId{
		{Hash: utxoTxIDForGroup(0, 0), Idx: 0},
		{Hash: utxoTxIDForGroup(0, 1), Idx: 0},
		{Hash: utxoTxIDForGroup(1, 0), Idx: 0},
	}
	deltas, err := queryUtxoStakeConsumedDeltas(ctx, store.writeDB, ids)
	require.NoError(t, err)

	byKey := make(map[string]int64, len(deltas))
	for _, d := range deltas {
		byKey[d.ref.MapKey()] = d.delta
	}
	require.Equal(t, int64(-10_000_000), byKey[refA.MapKey()])
	require.Equal(t, int64(-9_000_000), byKey[refB.MapKey()])
	require.Len(t, deltas, 2)
}

// utxoTxIDForGroup reproduces seedCredentialUtxos' tx_id derivation from
// (group, index) so a test can look its seeded rows back up by UtxoId.
func utxoTxIDForGroup(group, index int) []byte {
	txID := make([]byte, 32)
	txID[0] = byte(group >> 24)
	txID[1] = byte(group >> 16)
	txID[2] = byte(group >> 8)
	txID[3] = byte(group)
	txID[4] = byte(index >> 24)
	txID[5] = byte(index >> 16)
	txID[6] = byte(index >> 8)
	txID[7] = byte(index)
	return txID
}

// TestRewardLiveStakeNeedsBackfillHealsCorruptedRunningTotal is the
// load-bearing test for the whole design (dingo #4421): the incremental
// running total this change introduces trades away sumCredentialUtxoStake's
// self-healing full-scan property, so it depends entirely on
// RewardLiveStakeNeedsBackfill (run at every node startup, before block
// application resumes -- see Node.backfillRewardLiveStake) actually
// detecting a corrupted running total and RebuildRewardLiveStake actually
// correcting it. This proves both halves against a corruption that exactly
// simulates a missed or double-applied delta: the stored total is wrong, and
// nothing about the incremental path itself would ever notice.
func TestRewardLiveStakeNeedsBackfillHealsCorruptedRunningTotal(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))

	seedCredentialUtxos(
		t, store, 0, ref, []uint64{10_000_000, 5_000_000}, nil,
	)
	establishRunningTotal(t, store, ref, 1)
	require.Equal(t, uint64(15_000_000), readUtxoStake(t, store, ref))

	// Simulate a missed/double-applied delta by corrupting only the stored
	// utxo_stake column directly, without touching the utxo table it is
	// supposed to track and without touching total_stake. This is
	// deliberately not a call through any production code path: it stands in
	// for a bug in that path, and leaving total_stake at its previous
	// (correct) value isolates RewardLiveStakeNeedsBackfill's utxo_stake
	// comparison specifically -- total_stake alone would still agree with
	// the authoritative recomputation here (reward_stake is 0 for this
	// unregistered credential), so this corruption is only caught if the
	// utxo_stake check runs.
	const corrupted = "999999999"
	_, err := store.writeDB.ExecContext(ctx, `
UPDATE reward_live_stake SET utxo_stake = ?
WHERE credential_tag = ? AND staking_key = ?`,
		corrupted, int64(ref.Tag), ref.Key,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(999_999_999),
		readUtxoStake(t, store, ref),
		"corruption must actually take hold before reconciliation can be "+
			"asked to fix it",
	)

	authoritative, err := store.sumCredentialUtxoStake(ctx, store.writeDB, ref)
	require.NoError(t, err)
	require.NotEqual(
		t,
		authoritative,
		readUtxoStake(t, store, ref),
		"the corrupted value must actually disagree with the authoritative "+
			"scan, or this test proves nothing",
	)

	needed, err := store.RewardLiveStakeNeedsBackfill(nil)
	require.NoError(t, err)
	require.True(
		t,
		needed,
		"expected the corrupted running total to be detected as drift",
	)

	require.NoError(t, store.RebuildRewardLiveStake(2, nil))

	require.Equal(
		t,
		authoritative,
		readUtxoStake(t, store, ref),
		"expected the corrupted running total healed back to the "+
			"authoritative scan",
	)

	stillNeeded, err := store.RewardLiveStakeNeedsBackfill(nil)
	require.NoError(t, err)
	require.False(
		t,
		stillNeeded,
		"expected no further drift to be reported after the rebuild",
	)
}

// BenchmarkRefreshRewardLiveStakeAggregateDeltaAtScale is the direct
// before/after comparison for dingo #4421: refreshRewardLiveStakeAggregate's
// full sumCredentialUtxoStake rescan against
// refreshRewardLiveStakeAggregateDelta's O(1) running-total update, for a
// credential holding as many live UTxOs as the 20,003-UTxO case the issue
// measured on a live node. Both benchmarks touch the same warmed-up
// credential; only the incremental one is expected to stay flat as n grows.
func BenchmarkRefreshRewardLiveStakeAggregateDeltaAtScale(b *testing.B) {
	for _, n := range []int{100, 1_000, 20_003} {
		store := newMigratedSQLiteStore(b)
		ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
		amounts := make([]uint64, n)
		for i := range amounts {
			amounts[i] = uint64(1_000_000 + i)
		}
		seedCredentialUtxos(b, store, n, ref, amounts, nil)

		b.Run(fmt.Sprintf("n=%d/full_scan", n), func(b *testing.B) {
			for b.Loop() {
				err := store.withWriteTransaction(
					nil,
					func(db queryer, ctx context.Context) error {
						return store.refreshRewardLiveStakeAggregate(
							ctx, db, ref, 1,
						)
					},
				)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("n=%d/incremental_delta", n), func(b *testing.B) {
			establishRunningTotal(b, store, ref, 1)
			for b.Loop() {
				err := store.withWriteTransaction(
					nil,
					func(db queryer, ctx context.Context) error {
						return store.refreshRewardLiveStakeAggregateDelta(
							ctx, db, ref, 1, 0,
						)
					},
				)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestSetTransactionReapplyAppliesNoSecondDelta covers the invariant the
// incremental path rests on: a delta must state the change this write made to
// the utxo table, not the change the transaction describes. Re-applying an
// already-stored transaction mutates nothing -- the produced output collides
// with insertUtxoQueryIgnoreConflict's ON CONFLICT DO NOTHING, and the
// consumed input's UPDATE matches no row because this same transaction
// already spent it -- so the credential's running total must not move.
//
// Counting those no-op mutations again drives the stored value away from the
// authoritative scan in both directions at once (a spurious gain for the
// output, a spurious loss for the input), and a loss larger than the
// credential's recorded total fails applyUtxoStakeDelta's underflow guard,
// which aborts block application rather than merely reporting wrong stake.
func TestSetTransactionReapplyAppliesNoSecondDelta(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	fx := buildSharedCredentialTx(t, 0x41)
	seedConsumedUtxo(t, store, fx)
	establishRunningTotal(t, store, fx.ref, 1)

	require.NoError(t, store.SetTransaction(
		fx.tx, fx.point, 0, fx.certDeposits, false, nil,
	))
	require.Equal(t, fx.producedAmount, readUtxoStake(t, store, fx.ref))

	require.NoError(t, store.SetTransaction(
		fx.tx, fx.point, 0, fx.certDeposits, false, nil,
	))
	want, err := store.sumCredentialUtxoStake(ctx, store.writeDB, fx.ref)
	require.NoError(t, err)
	require.Equal(t, fx.producedAmount, want, "the utxo table must not move")
	require.Equal(
		t,
		want,
		readUtxoStake(t, store, fx.ref),
		"a re-applied transaction must not move the running total",
	)
}

// TestSetTransactionLeiosClosureSkippedInputAppliesNoDelta covers the same
// invariant on the Leios closure path, where an input already spent by a
// *different* certified endorser-block transaction is deliberately a no-op
// (see setTransactionWithAccumulator's tolerateConsumedInputConflict branch).
// The row stays deleted with its original spender, so this write removed no
// live stake and must subtract none.
func TestSetTransactionLeiosClosureSkippedInputAppliesNoDelta(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	fx := buildSharedCredentialTx(t, 0x43)
	seedConsumedUtxo(t, store, fx)

	otherSpender := make([]byte, 32)
	otherSpender[0] = 0xfe
	_, err := store.writeDB.ExecContext(ctx, `
UPDATE utxo SET deleted_slot = 900, spent_at_tx_id = ?
WHERE tx_id = ? AND output_idx = 0`, otherSpender, fx.consumedTxID)
	require.NoError(t, err)

	// A second live UTxO keeps the credential's reward_live_stake row in
	// place, so the write exercises the warm incremental path rather than
	// refreshRewardLiveStakeAggregateDelta's cold-start full-scan fallback.
	const retained = 9_000_000
	otherTx := make([]byte, 32)
	otherTx[0] = 0x77
	require.NoError(t, store.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			return insertLiveUtxoTx(ctx, db, fx.ref, otherTx[0], retained)
		},
	))
	establishRunningTotal(t, store, fx.ref, 1)
	require.Equal(t, uint64(retained), readUtxoStake(t, store, fx.ref))

	require.NoError(t, store.SetTransactionLeiosClosure(
		fx.tx, fx.point, 0, fx.certDeposits, false, nil,
	))
	want, err := store.sumCredentialUtxoStake(ctx, store.writeDB, fx.ref)
	require.NoError(t, err)
	require.Equal(t, uint64(retained+fx.producedAmount), want)
	require.Equal(
		t,
		want,
		readUtxoStake(t, store, fx.ref),
		"an already-spent input must contribute no loss delta",
	)
}

// TestSetGapBlockTransactionReapplyAppliesNoSecondDelta is the same check for
// SetGapBlockTransaction, the other caller on the incremental path. It is the
// likeliest place for a colliding produced output in practice: gap closure
// replays blocks whose outputs a Mithril snapshot import may already have
// created.
func TestSetGapBlockTransactionReapplyAppliesNoSecondDelta(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	fx := buildSharedCredentialTx(t, 0x44)

	require.NoError(t, store.SetGapBlockTransaction(
		fx.tx, fx.point, 0, fx.certDeposits, nil,
	))
	first := readUtxoStake(t, store, fx.ref)
	require.Equal(t, fx.producedAmount, first)

	require.NoError(t, store.SetGapBlockTransaction(
		fx.tx, fx.point, 0, fx.certDeposits, nil,
	))
	want, err := store.sumCredentialUtxoStake(ctx, store.writeDB, fx.ref)
	require.NoError(t, err)
	require.Equal(t, fx.producedAmount, want, "the utxo table must not move")
	require.Equal(
		t,
		want,
		readUtxoStake(t, store, fx.ref),
		"a replayed gap block must not double-count its produced outputs",
	)
}

// insertLatestDelegationRow writes one stake_delegation row together with the
// certs and transaction rows the finalizer's ranking reads block_index and
// cert_index from.
func insertLatestDelegationRow(
	t testing.TB,
	store *Store,
	tag uint8,
	key, pool []byte,
	addedSlot, blockIndex, certIndex int64,
) {
	t.Helper()
	txResult, err := store.writeDB.Exec(
		"INSERT INTO \"transaction\" (slot, block_index) VALUES (?, ?)",
		addedSlot, blockIndex,
	)
	require.NoError(t, err)
	txID, err := txResult.LastInsertId()
	require.NoError(t, err)
	certResult, err := store.writeDB.Exec(
		"INSERT INTO certs (transaction_id, slot, cert_index) VALUES (?, ?, ?)",
		txID, addedSlot, certIndex,
	)
	require.NoError(t, err)
	certID, err := certResult.LastInsertId()
	require.NoError(t, err)
	_, err = store.writeDB.Exec(`
INSERT INTO stake_delegation
    (staking_key, credential_tag, pool_key_hash, certificate_id, added_slot)
VALUES (?, ?, ?, ?, ?)`, key, tag, pool, certID, addedSlot)
	require.NoError(t, err)
}

// snapshotRow returns one key's aggregate row, failing when the rebuild
// wrote none: every assertion below would otherwise hold for the zero value.
func snapshotRow(
	t testing.TB,
	snapshot map[string]rewardLiveStakeSnapshotRow,
	tag uint8,
	key []byte,
) rewardLiveStakeSnapshotRow {
	t.Helper()
	row, ok := snapshot[fmt.Sprintf("%d:%s", tag, key)]
	require.True(t, ok, "no reward live stake row for %d:%x", tag, key)
	return row
}

// TestRebuildRewardLiveStakeLatestDelegationSelection pins the delegation
// columns for the credential shapes the finalizer's latest-assignment
// pruning has to leave alone. The pruning keeps every credential whose
// account row is active with a non-NULL pool and ranks that credential's
// whole history, so a credential whose newest assignment is to some other
// pool must still fall back to the account's own added_slot rather than
// silently promoting the older matching assignment.
func TestRebuildRewardLiveStakeLatestDelegationSelection(t *testing.T) {
	t.Parallel()
	poolA := []byte{0xa0, 0xa1}
	poolB := []byte{0xb0, 0xb1}

	matching := models.NewStakeCredentialRef(0, []byte{0x01})
	superseded := models.NewStakeCredentialRef(0, []byte{0x02})
	inactive := models.NewStakeCredentialRef(0, []byte{0x03})
	noPool := models.NewStakeCredentialRef(0, []byte{0x04})

	store := newMigratedSQLiteStore(t)
	for _, account := range []*models.Account{
		{
			StakingKey: matching.Key, CredentialTag: matching.Tag,
			Pool: poolA, AddedSlot: 10, CreatedSlot: 10, Active: true,
		},
		{
			StakingKey: superseded.Key, CredentialTag: superseded.Tag,
			Pool: poolA, AddedSlot: 20, CreatedSlot: 20, Active: true,
		},
		{
			StakingKey: inactive.Key, CredentialTag: inactive.Tag,
			Pool: poolA, AddedSlot: 30, CreatedSlot: 30, Active: false,
		},
		{
			StakingKey: noPool.Key, CredentialTag: noPool.Tag,
			AddedSlot: 40, CreatedSlot: 40, Active: true,
		},
	} {
		require.NoError(t, store.ImportAccount(account, nil))
	}
	require.NoError(t, store.ImportUtxos([]models.Utxo{
		{
			TxId: bytesForRebuildTest(0x51), StakingKey: matching.Key,
			CredentialTag: matching.Tag, AddedSlot: 11, Amount: types.Uint64(5),
		},
		{
			TxId: bytesForRebuildTest(0x52), StakingKey: superseded.Key,
			CredentialTag: superseded.Tag, AddedSlot: 21, Amount: types.Uint64(6),
		},
		{
			TxId: bytesForRebuildTest(0x53), StakingKey: inactive.Key,
			CredentialTag: inactive.Tag, AddedSlot: 31, Amount: types.Uint64(7),
		},
		{
			TxId: bytesForRebuildTest(0x54), StakingKey: noPool.Key,
			CredentialTag: noPool.Tag, AddedSlot: 41, Amount: types.Uint64(8),
		},
	}, nil))

	insertLatestDelegationRow(t, store, matching.Tag, matching.Key, poolA, 12, 3, 4)
	// superseded delegated to poolA first and to poolB later; the account
	// still names poolA, so the newest assignment does not match it.
	insertLatestDelegationRow(t, store, superseded.Tag, superseded.Key, poolA, 22, 1, 1)
	insertLatestDelegationRow(t, store, superseded.Tag, superseded.Key, poolB, 23, 2, 2)
	insertLatestDelegationRow(t, store, inactive.Tag, inactive.Key, poolA, 32, 5, 6)
	insertLatestDelegationRow(t, store, noPool.Tag, noPool.Key, poolA, 42, 7, 8)

	require.NoError(t, store.RebuildRewardLiveStake(100, nil))
	snapshot := readRewardLiveStakeSnapshot(t, store)

	matched := snapshotRow(t, snapshot, 0, matching.Key)
	require.Equal(t, string(poolA), matched.pool)
	require.Equal(t, int64(12), matched.delegationSlot)
	require.Equal(t, int64(3), matched.delegationBlock)
	require.Equal(t, int64(4), matched.delegationCert)

	// No latest_delegation match, so the account's own added_slot stands and
	// the older poolA assignment at slot 22 is not promoted.
	stale := snapshotRow(t, snapshot, 0, superseded.Key)
	require.Equal(t, string(poolA), stale.pool)
	require.Equal(t, int64(20), stale.delegationSlot)
	require.Equal(t, int64(0), stale.delegationBlock)
	require.Equal(t, int64(0), stale.delegationCert)

	unregistered := snapshotRow(t, snapshot, 0, inactive.Key)
	require.False(t, unregistered.registered)
	require.Empty(t, unregistered.pool)
	require.Equal(t, int64(0), unregistered.delegationSlot)

	undelegated := snapshotRow(t, snapshot, 0, noPool.Key)
	require.True(t, undelegated.registered)
	require.Empty(t, undelegated.pool)
	require.Equal(t, int64(0), undelegated.delegationSlot)
}

// TestRebuildRewardLiveStakeFromRunningTotalsLatestDelegationSelection runs
// the same shapes through the Mithril finalization path, which reaches the
// same ranked query with the running-total join attached.
func TestRebuildRewardLiveStakeFromRunningTotalsLatestDelegationSelection(
	t *testing.T,
) {
	t.Parallel()
	authoritative := newMigratedSQLiteStore(t)
	runningTotals := newMigratedSQLiteStore(t)
	for _, store := range []*Store{authoritative, runningTotals} {
		populateRewardLiveStakeRebuildFixture(t, store)
		insertLatestDelegationRow(
			t, store, 0, []byte{0x10, 0x11}, []byte{0xa0, 0xa1}, 12, 3, 4,
		)
		insertLatestDelegationRow(
			t, store, 0, []byte{0x10, 0x11}, []byte{0xc0, 0xc1}, 13, 4, 5,
		)
		insertLatestDelegationRow(
			t, store, 1, []byte{0x30, 0x31}, []byte{0xb0, 0xb1}, 32, 6, 7,
		)
	}
	require.NoError(t, authoritative.RebuildRewardLiveStake(100, nil))
	require.NoError(
		t,
		runningTotals.RebuildRewardLiveStakeFromRunningTotals(100, nil),
	)
	require.Equal(
		t,
		readRewardLiveStakeSnapshot(t, authoritative),
		readRewardLiveStakeSnapshot(t, runningTotals),
	)
	snapshot := readRewardLiveStakeSnapshot(t, runningTotals)
	require.Equal(t, int64(32), snapshotRow(t, snapshot, 1, []byte{0x30, 0x31}).delegationSlot)
	require.Equal(t, int64(10), snapshotRow(t, snapshot, 0, []byte{0x10, 0x11}).delegationSlot)
}

// seedRewardLiveStakeScaleFixture writes credentials directly through SQL
// rather than the model importers: the finalizer's cost is a property of the
// row populations it reads, and the importers' per-row bookkeeping would
// dominate the setup at these sizes.
//
// delegatedShare is the percentage of credentials that hold an active account
// with a pool. deadAssignments adds stake-assignment rows for credentials
// with no account row at all, which is what a long-lived chain accumulates as
// stake keys deregister: that population grows with the chain's age while the
// live credential population does not, and it is the shape the finalizer's
// ranked query must not spend time on.
func seedRewardLiveStakeScaleFixture(
	tb testing.TB,
	store *Store,
	credentials int,
	assignmentsEach int,
	delegatedShare int,
	deadAssignments int,
) {
	tb.Helper()
	tx, err := store.writeDB.Begin()
	require.NoError(tb, err)
	defer func() { _ = tx.Rollback() }()
	prepare := func(query string) *sql.Stmt {
		stmt, err := tx.Prepare(query)
		require.NoError(tb, err)
		return stmt
	}
	accountStmt := prepare(`
INSERT INTO account
    (staking_key, credential_tag, pool, added_slot, created_slot, reward,
     active)
VALUES (?, 0, ?, ?, ?, ?, ?)`)
	utxoStmt := prepare(`
INSERT INTO utxo
    (tx_id, output_idx, staking_key, credential_tag, added_slot, deleted_slot,
     amount)
VALUES (?, 0, ?, 0, ?, 0, ?)`)
	totalStmt := prepare(`
INSERT INTO reward_live_stake
    (credential_tag, staking_key, utxo_stake, reward_stake, total_stake,
     registered, updated_slot, calculation_version)
VALUES (0, ?, ?, '0', ?, false, 1, 0)`)
	transactionStmt := prepare(`
INSERT INTO "transaction" (id, slot, block_index)
VALUES (?, ?, ?)`)
	certStmt := prepare(`
INSERT INTO certs (id, transaction_id, slot, cert_index)
VALUES (?, ?, ?, ?)`)
	delegationStmt := prepare(`
INSERT INTO stake_delegation
    (staking_key, credential_tag, pool_key_hash, certificate_id, added_slot)
VALUES (?, 0, ?, ?, ?)`)
	registrationDelegationStmt := prepare(`
INSERT INTO stake_registration_delegation
    (staking_key, credential_tag, pool_key_hash, certificate_id, added_slot)
VALUES (?, 0, ?, ?, ?)`)
	voteDelegationStmt := prepare(`
INSERT INTO stake_vote_delegation
    (staking_key, credential_tag, pool_key_hash, certificate_id, added_slot)
VALUES (?, 0, ?, ?, ?)`)
	voteRegistrationDelegationStmt := prepare(`
INSERT INTO stake_vote_registration_delegation
    (staking_key, credential_tag, pool_key_hash, certificate_id, added_slot)
VALUES (?, 0, ?, ?, ?)`)
	delegationStmts := []*sql.Stmt{
		delegationStmt,
		registrationDelegationStmt,
		voteDelegationStmt,
		voteRegistrationDelegationStmt,
	}

	pool := func(index int) []byte {
		buf := make([]byte, 28)
		binary.BigEndian.PutUint64(buf, uint64(index%64))
		return buf
	}
	var eventID int64
	for index := range credentials {
		key := make([]byte, 28)
		binary.BigEndian.PutUint64(key, uint64(index))
		active := index%100 < delegatedShare
		slot := int64(index + 1)
		var accountPool any
		if active {
			accountPool = pool(index)
		}
		_, err := accountStmt.Exec(key, accountPool, slot, slot, "0", active)
		require.NoError(tb, err)
		txID := make([]byte, 32)
		binary.BigEndian.PutUint64(txID, uint64(index))
		_, err = utxoStmt.Exec(txID, key, slot, "1000000")
		require.NoError(tb, err)
		_, err = totalStmt.Exec(key, "1000000", "1000000")
		require.NoError(tb, err)
		for assignment := range assignmentsEach {
			eventID++
			assignmentSlot := slot + int64(assignment)
			assignmentPool := pool(index + assignment)
			if assignment == assignmentsEach-1 {
				assignmentPool = pool(index)
			}
			_, err := transactionStmt.Exec(eventID, assignmentSlot, assignment)
			require.NoError(tb, err)
			_, err = certStmt.Exec(eventID, eventID, assignmentSlot, 0)
			require.NoError(tb, err)
			_, err = delegationStmts[assignment%len(delegationStmts)].Exec(
				key, assignmentPool, eventID, assignmentSlot,
			)
			require.NoError(tb, err)
		}
	}
	for index := range deadAssignments {
		eventID++
		key := make([]byte, 28)
		binary.BigEndian.PutUint64(key, uint64(credentials+index/4))
		assignmentSlot := int64(index + 1)
		_, err := transactionStmt.Exec(eventID, assignmentSlot, 0)
		require.NoError(tb, err)
		_, err = certStmt.Exec(eventID, eventID, assignmentSlot, 0)
		require.NoError(tb, err)
		_, err = delegationStmts[index%len(delegationStmts)].Exec(
			key, pool(index), eventID, assignmentSlot,
		)
		require.NoError(tb, err)
	}
	require.NoError(tb, tx.Commit())
}

func logRewardLiveStakeBatchPlan(b *testing.B, store *Store) {
	b.Helper()
	lastKey := make([]byte, 28)
	binary.BigEndian.PutUint64(lastKey, rewardLiveStakeRebuildBatch-1)
	query, args := rewardLiveStakeCredentialQuery(true, stakeKeyRange{
		hi: &stakeKeyBound{tag: 0, key: lastKey},
	})
	rows, err := store.writeDB.Query("EXPLAIN QUERY PLAN "+query, args...)
	require.NoError(b, err)
	defer rows.Close()
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(b, rows.Scan(&id, &parent, &notUsed, &detail))
		b.Logf("query_plan=%s", detail)
	}
	require.NoError(b, rows.Err())
}

func rewardLiveStakeBatchQuery(
	keys stakeKeyRange,
	forceCredentialIndex bool,
	constrainDelegationRange bool,
	forceAccountFirst bool,
) (string, []any, error) {
	query, args := rewardLiveStakeCredentialQuery(true, keys)
	if constrainDelegationRange {
		accountRange, rangeArgs := keys.predicate(
			"a.credential_tag",
			"a.staking_key",
		)
		searchStart := 0
		for index, alias := range []string{"sd", "srd", "svd", "svrd"} {
			needle := "AND " + accountRange
			position := strings.Index(query[searchStart:], needle)
			if position < 0 {
				return "", nil, fmt.Errorf(
					"find account key range for delegation alias %s",
					alias,
				)
			}
			position += searchStart
			delegationRange, delegationArgs := keys.predicate(
				alias+".credential_tag",
				alias+".staking_key",
			)
			if len(delegationArgs) != len(rangeArgs) {
				return "", nil, fmt.Errorf(
					"delegation range argument count differs for alias %s",
					alias,
				)
			}
			insertAt := position + len(needle)
			query = query[:insertAt] + " AND " + delegationRange + query[insertAt:]
			searchStart = insertAt + len(" AND "+delegationRange)

			// The new placeholders follow this branch's account range in SQL order.
			argPosition := (2*index + 1) * len(rangeArgs)
			if argPosition > len(args) {
				return "", nil, fmt.Errorf(
					"delegation range argument position %d exceeds %d arguments",
					argPosition,
					len(args),
				)
			}
			updatedArgs := make([]any, 0, len(args)+len(delegationArgs))
			updatedArgs = append(updatedArgs, args[:argPosition]...)
			updatedArgs = append(updatedArgs, delegationArgs...)
			updatedArgs = append(updatedArgs, args[argPosition:]...)
			args = updatedArgs
		}
	}
	if forceAccountFirst {
		var err error
		query, err = sqliteRewardLiveStakeAccountFirstQuery(query)
		if err != nil {
			return "", nil, err
		}
	} else if forceCredentialIndex {
		query = strings.ReplaceAll(
			query,
			"FROM account a\n",
			"FROM account a INDEXED BY idx_account_credential\n",
		)
	}
	return query, args, nil
}

func scanRewardLiveStakeBatches(
	ctx context.Context,
	store *Store,
	forceCredentialIndex bool,
	constrainDelegationRange bool,
	forceAccountFirst bool,
) (int64, error) {
	var processed int64
	var lo *stakeKeyBound
	for {
		hi, err := nextRewardLiveStakeBatchEnd(
			ctx,
			store.writeDB,
			lo,
			rewardLiveStakeRebuildBatch,
		)
		if err != nil {
			return 0, err
		}
		query, args, err := rewardLiveStakeBatchQuery(
			stakeKeyRange{lo: lo, hi: hi},
			forceCredentialIndex,
			constrainDelegationRange,
			forceAccountFirst,
		)
		if err != nil {
			return 0, err
		}
		rows, err := store.writeDB.QueryContext(ctx, query, args...)
		if err != nil {
			return 0, err
		}
		for rows.Next() {
			processed++
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return 0, err
		}
		if err := rows.Close(); err != nil {
			return 0, err
		}
		if hi == nil {
			return processed, nil
		}
		lo = hi
	}
}

func logRewardLiveStakeRangeQueryPlan(
	b *testing.B,
	store *Store,
	forceCredentialIndex bool,
	constrainDelegationRange bool,
	forceAccountFirst bool,
) {
	b.Helper()
	lastKey := make([]byte, 28)
	binary.BigEndian.PutUint64(lastKey, rewardLiveStakeRebuildBatch-1)
	query, args, err := rewardLiveStakeBatchQuery(
		stakeKeyRange{hi: &stakeKeyBound{tag: 0, key: lastKey}},
		forceCredentialIndex,
		constrainDelegationRange,
		forceAccountFirst,
	)
	require.NoError(b, err)
	if forceCredentialIndex {
		b.Log("query plan with credential-key index forced")
	} else if constrainDelegationRange {
		b.Log("query plan with delegation key range pushed down")
	} else if forceAccountFirst {
		b.Log("query plan with account-first credential-key index forced")
	} else {
		b.Log("current query plan")
	}
	rows, err := store.writeDB.Query("EXPLAIN QUERY PLAN "+query, args...)
	require.NoError(b, err)
	defer rows.Close()
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(b, rows.Scan(&id, &parent, &notUsed, &detail))
		b.Logf("query_plan=%s", detail)
	}
	require.NoError(b, rows.Err())
}

// BenchmarkRebuildRewardLiveStakeFromRunningTotals measures the Mithril
// bootstrap finalizer over live key histories and deregistered-key history.
func BenchmarkRebuildRewardLiveStakeFromRunningTotals(b *testing.B) {
	for _, scenario := range []struct {
		credentials     int
		assignmentsEach int
		deadAssignments int
	}{
		{credentials: 100_000, assignmentsEach: 3},
		{credentials: 100_000, assignmentsEach: 3, deadAssignments: 1_200_000},
		{credentials: 100_000, assignmentsEach: 20},
	} {
		name := fmt.Sprintf(
			"keys=%d/assignments=%d/dead=%d",
			scenario.credentials,
			scenario.assignmentsEach,
			scenario.deadAssignments,
		)
		b.Run(name, func(b *testing.B) {
			store := newMigratedSQLiteStore(b)
			seedRewardLiveStakeScaleFixture(
				b,
				store,
				scenario.credentials,
				scenario.assignmentsEach,
				80,
				scenario.deadAssignments,
			)
			logRewardLiveStakeBatchPlan(b, store)
			b.ResetTimer()
			for b.Loop() {
				require.NoError(
					b,
					store.RebuildRewardLiveStakeFromRunningTotals(1_000_000, nil),
				)
			}
		})
	}
}

func BenchmarkRewardLiveStakeRangeQuery(b *testing.B) {
	store := newMigratedSQLiteStore(b)
	seedRewardLiveStakeScaleFixture(b, store, 100_000, 20, 80, 0)
	logRewardLiveStakeRangeQueryPlan(b, store, false, false, false)
	logRewardLiveStakeRangeQueryPlan(b, store, true, false, false)
	logRewardLiveStakeRangeQueryPlan(b, store, false, true, false)
	logRewardLiveStakeRangeQueryPlan(b, store, false, false, true)
	currentRows, err := scanRewardLiveStakeBatches(
		context.Background(),
		store,
		false,
		false,
		false,
	)
	require.NoError(b, err)
	credentialIndexRows, err := scanRewardLiveStakeBatches(
		context.Background(),
		store,
		true,
		false,
		false,
	)
	require.NoError(b, err)
	require.Equal(b, currentRows, credentialIndexRows)
	delegationRangeRows, err := scanRewardLiveStakeBatches(
		context.Background(),
		store,
		false,
		true,
		false,
	)
	require.NoError(b, err)
	require.Equal(b, currentRows, delegationRangeRows)
	accountFirstRows, err := scanRewardLiveStakeBatches(
		context.Background(),
		store,
		false,
		false,
		true,
	)
	require.NoError(b, err)
	require.Equal(b, currentRows, accountFirstRows)
	for _, variant := range []struct {
		name                     string
		forceCredentialIndex     bool
		constrainDelegationRange bool
		forceAccountFirst        bool
	}{
		{name: "current"},
		{name: "credential-key-index", forceCredentialIndex: true},
		{name: "delegation-key-range", constrainDelegationRange: true},
		{name: "account-first-credential-key-index", forceAccountFirst: true},
	} {
		b.Run(variant.name, func(b *testing.B) {
			for b.Loop() {
				rows, err := scanRewardLiveStakeBatches(
					context.Background(),
					store,
					variant.forceCredentialIndex,
					variant.constrainDelegationRange,
					variant.forceAccountFirst,
				)
				require.NoError(b, err)
				require.Equal(b, currentRows, rows)
			}
		})
	}
}

type rewardLiveStakeSnapshotRow struct {
	tag, key, pool                                  string
	utxoStake, rewardStake, totalStake              string
	registered                                      bool
	delegationSlot, delegationBlock, delegationCert int64
	updatedSlot, calculationVersion                 int64
}

func populateRewardLiveStakeRebuildFixture(t testing.TB, store *Store) {
	t.Helper()
	delegated := models.NewStakeCredentialRef(0, []byte{0x10, 0x11})
	accountOnly := models.NewStakeCredentialRef(0, []byte{0x20, 0x21})
	scriptDelegated := models.NewStakeCredentialRef(1, []byte{0x30, 0x31})
	for _, account := range []*models.Account{
		{
			StakingKey: delegated.Key, CredentialTag: delegated.Tag,
			Pool: []byte{0xa0, 0xa1}, AddedSlot: 10, CreatedSlot: 10,
			Reward: types.Uint64(100), Active: true,
		},
		{
			StakingKey: accountOnly.Key, CredentialTag: accountOnly.Tag,
			AddedSlot: 20, CreatedSlot: 20, Reward: types.Uint64(7), Active: true,
		},
		{
			StakingKey: scriptDelegated.Key, CredentialTag: scriptDelegated.Tag,
			Pool: []byte{0xb0, 0xb1}, AddedSlot: 30, CreatedSlot: 30,
			Reward: types.Uint64(11), Active: true,
		},
	} {
		require.NoError(t, store.ImportAccount(account, nil))
	}
	utxos := []models.Utxo{
		{
			TxId: bytesForRebuildTest(0x41), StakingKey: delegated.Key,
			CredentialTag: delegated.Tag, AddedSlot: 11, Amount: types.Uint64(50),
		},
		{
			TxId: bytesForRebuildTest(0x42), StakingKey: delegated.Key,
			CredentialTag: delegated.Tag, AddedSlot: 12, Amount: types.Uint64(75),
		},
		{
			TxId: bytesForRebuildTest(0x43), StakingKey: scriptDelegated.Key,
			CredentialTag: scriptDelegated.Tag, AddedSlot: 31, Amount: types.Uint64(9),
		},
	}
	require.NoError(t, store.ImportUtxos(utxos, nil))
}

func readRewardLiveStakeSnapshot(
	t testing.TB,
	store *Store,
) map[string]rewardLiveStakeSnapshotRow {
	t.Helper()
	rows, err := store.writeDB.Query(`
SELECT credential_tag, staking_key, pool_key_hash,
       utxo_stake, reward_stake, total_stake, registered,
       pool_delegation_slot, pool_delegation_block_index,
       pool_delegation_cert_index, updated_slot, calculation_version
FROM reward_live_stake ORDER BY credential_tag, staking_key`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	ret := make(map[string]rewardLiveStakeSnapshotRow)
	for rows.Next() {
		var row rewardLiveStakeSnapshotRow
		var tag int64
		var key, pool []byte
		require.NoError(t, rows.Scan(
			&tag, &key, &pool, &row.utxoStake, &row.rewardStake,
			&row.totalStake, &row.registered, &row.delegationSlot,
			&row.delegationBlock, &row.delegationCert, &row.updatedSlot,
			&row.calculationVersion,
		))
		row.tag = fmt.Sprintf("%d", tag)
		row.key = string(key)
		row.pool = string(pool)
		ret[row.tag+":"+row.key] = row
	}
	require.NoError(t, rows.Err())
	return ret
}

// TestRebuildRewardLiveStakeFromRunningTotalsMatchesAuthoritativeRebuild
// compares every aggregate column on one fixture, including an account with
// no UTxO and key/script credentials delegated to pools.
func TestRebuildRewardLiveStakeFromRunningTotalsMatchesAuthoritativeRebuild(
	t *testing.T,
) {
	t.Parallel()
	authoritative := newMigratedSQLiteStore(t)
	runningTotals := newMigratedSQLiteStore(t)
	populateRewardLiveStakeRebuildFixture(t, authoritative)
	populateRewardLiveStakeRebuildFixture(t, runningTotals)
	_, err := runningTotals.writeDB.Exec(`
INSERT INTO reward_live_stake
    (credential_tag, staking_key, utxo_stake, reward_stake, total_stake,
     registered, updated_slot, calculation_version)
VALUES (0, ?, '999', '999', '1998', false, 1, 0)`, []byte{0xee, 0xef})
	require.NoError(t, err)
	require.NoError(t, authoritative.RebuildRewardLiveStake(100, nil))
	require.NoError(t, runningTotals.RebuildRewardLiveStakeFromRunningTotals(100, nil))
	require.Equal(
		t,
		readRewardLiveStakeSnapshot(t, authoritative),
		readRewardLiveStakeSnapshot(t, runningTotals),
	)
}

// TestRebuildRewardLiveStakeFromRunningTotalsUsesImportedTotals proves the
// API-backfill finalization path does not read the live UTxO table again. The
// deliberately malformed amount would make the authoritative scan fail, but
// the running total established by the importer remains usable.
func TestRebuildRewardLiveStakeFromRunningTotalsUsesImportedTotals(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ref := models.NewStakeCredentialRef(0, []byte{0x01, 0x02})
	reward := types.Uint64(100)
	require.NoError(t, store.ImportAccount(&models.Account{
		StakingKey:    ref.Key,
		CredentialTag: ref.Tag,
		AddedSlot:     10,
		CreatedSlot:   10,
		Reward:        reward,
		Active:        true,
	}, nil))
	accountOnly := models.NewStakeCredentialRef(0, []byte{0x03, 0x04})
	require.NoError(t, store.ImportAccount(&models.Account{
		StakingKey:    accountOnly.Key,
		CredentialTag: accountOnly.Tag,
		AddedSlot:     12,
		CreatedSlot:   12,
		Reward:        types.Uint64(7),
		Active:        true,
	}, nil))
	utxo := models.Utxo{
		TxId:          bytesForRebuildTest(0x11),
		StakingKey:    ref.Key,
		CredentialTag: ref.Tag,
		AddedSlot:     11,
		Amount:        types.Uint64(50),
	}
	require.NoError(t, store.ImportUtxos([]models.Utxo{utxo}, nil))

	var before string
	require.NoError(t, store.writeDB.QueryRow(`
SELECT utxo_stake FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`, ref.Tag, ref.Key).
		Scan(&before))
	require.Equal(t, "50", before)

	_, err := store.writeDB.Exec(
		`UPDATE utxo SET amount = 'not-a-lovelace' WHERE tx_id = ?`,
		utxo.TxId,
	)
	require.NoError(t, err)

	require.NoError(t, store.RebuildRewardLiveStakeFromRunningTotals(100, nil))
	var gotUtxo, gotReward, gotTotal string
	require.NoError(t, store.writeDB.QueryRow(`
SELECT utxo_stake, reward_stake, total_stake
FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`, ref.Tag, ref.Key).
		Scan(&gotUtxo, &gotReward, &gotTotal))
	require.Equal(t, "50", gotUtxo)
	require.Equal(t, "100", gotReward)
	require.Equal(t, "150", gotTotal)
	var accountOnlyTotal string
	require.NoError(t, store.writeDB.QueryRow(`
SELECT total_stake FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`,
		accountOnly.Tag, accountOnly.Key).Scan(&accountOnlyTotal))
	require.Equal(t, "7", accountOnlyTotal)
}

func TestRebuildRewardLiveStakeFromRunningTotalsRejectsMissingUtxoTotal(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ref := models.NewStakeCredentialRef(0, []byte{0x51, 0x52})
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId:          bytesForRebuildTest(0x53),
		StakingKey:    ref.Key,
		CredentialTag: ref.Tag,
		AddedSlot:     11,
		Amount:        types.Uint64(50),
	}}, nil))
	_, err := store.writeDB.Exec(
		`DELETE FROM reward_live_stake WHERE credential_tag = ? AND staking_key = ?`,
		ref.Tag,
		ref.Key,
	)
	require.NoError(t, err)
	err = store.RebuildRewardLiveStakeFromRunningTotals(100, nil)
	require.ErrorContains(t, err, "missing reward live stake running total")
}

func bytesForRebuildTest(seed byte) []byte {
	ret := make([]byte, 32)
	ret[0] = seed
	return ret
}

// BenchmarkRebuildRewardLiveStakeFinalizers compares the two finalization
// paths on a 20,000-live-UTxO fixture. Reproduce with:
//
//	go test ./database/plugin/metadata/sqlstore -run '^$' \
//	  -bench BenchmarkRebuildRewardLiveStakeFinalizers -benchtime=1x -count=1
func BenchmarkRebuildRewardLiveStakeFinalizers(b *testing.B) {
	for _, path := range []struct {
		name string
		fast bool
	}{
		{name: "authoritative"},
		{name: "running_totals", fast: true},
	} {
		b.Run(path.name, func(b *testing.B) {
			store := newMigratedSQLiteStore(b)
			ref := models.NewStakeCredentialRef(0, []byte{1, 2, 3})
			amounts := make([]uint64, 20_000)
			for i := range amounts {
				amounts[i] = uint64(i + 1)
			}
			seedCredentialUtxos(b, store, 99, ref, amounts, nil)
			require.NoError(b, store.ImportAccount(&models.Account{
				StakingKey: ref.Key, CredentialTag: ref.Tag,
				AddedSlot: 1, CreatedSlot: 1, Reward: types.Uint64(100), Active: true,
			}, nil))
			require.NoError(b, store.RebuildRewardLiveStake(100, nil))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var err error
				if path.fast {
					err = store.RebuildRewardLiveStakeFromRunningTotals(100, nil)
				} else {
					err = store.RebuildRewardLiveStake(100, nil)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// legacySumCredentialUtxoStake reproduces refreshRewardLiveStakeAggregate's
// pre-fix approach: fetch every live UTxO amount for the credential and sum
// it in Go via the generic sumUint64Rows helper. Kept here as the
// correctness and benchmark comparison point for sumCredentialUtxoStake's
// single-aggregate rewrite.
func legacySumCredentialUtxoStake(
	ctx context.Context,
	db queryer,
	ref models.StakeCredentialRef,
) (uint64, error) {
	return sumUint64Rows(ctx, db, `
SELECT amount
FROM utxo
WHERE credential_tag = ? AND staking_key = ? AND deleted_slot = 0`,
		ref.Tag, ref.Key)
}

// seedCredentialUtxos inserts one live (or, for entries marked deleted, spent)
// UTxO per amount, all under the same stake credential. A distinct tx_id per
// row satisfies the utxo table's uniqueness expectations without colliding
// with any other seeded credential in the same test.
func seedCredentialUtxos(
	tb testing.TB,
	store *Store,
	group int,
	ref models.StakeCredentialRef,
	amounts []uint64,
	deleted []bool,
) {
	tb.Helper()
	tx, err := store.writeDB.Begin()
	require.NoError(tb, err)
	stmt, err := tx.Prepare(
		"INSERT INTO utxo (tx_id, output_idx, staking_key, credential_tag, " +
			"added_slot, deleted_slot, amount) VALUES (?, ?, ?, ?, ?, ?, ?)",
	)
	require.NoError(tb, err)
	for i, amount := range amounts {
		txID := make([]byte, 32)
		txID[0] = byte(group >> 24)
		txID[1] = byte(group >> 16)
		txID[2] = byte(group >> 8)
		txID[3] = byte(group)
		txID[4] = byte(i >> 24)
		txID[5] = byte(i >> 16)
		txID[6] = byte(i >> 8)
		txID[7] = byte(i)
		deletedSlot := int64(0)
		if deleted != nil && deleted[i] {
			deletedSlot = 100
		}
		_, err := stmt.Exec(
			txID,
			0,
			ref.Key,
			int64(ref.Tag),
			int64(1),
			deletedSlot,
			decimalUint64(types.Uint64(amount)),
		)
		require.NoError(tb, err)
	}
	require.NoError(tb, stmt.Close())
	require.NoError(tb, tx.Commit())
}

// TestSumCredentialUtxoStakeMatchesLegacyRowSum proves the single-aggregate
// rewrite returns exactly what the old per-row Go summation did, across an
// empty credential, a single UTxO, a mix of live and already-spent UTxOs
// (the deleted ones must be excluded from both), and amounts spanning small
// balances up to a total near the real lovelace supply.
func TestSumCredentialUtxoStakeMatchesLegacyRowSum(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()

	cases := []struct {
		name    string
		amounts []uint64
		deleted []bool
	}{
		{name: "no utxos"},
		{name: "single utxo", amounts: []uint64{5_000_000}},
		{
			name:    "many utxos, some spent",
			amounts: []uint64{1, 2, 3, 1_000_000, 45_000_000_000_000_000},
			deleted: []bool{false, true, false, true, false},
		},
		{
			name: "large realistic totals",
			amounts: []uint64{
				44_999_999_000_000_000,
				999_000_000,
				1,
			},
		},
	}

	for i, tc := range cases {
		ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(i))
		if len(tc.amounts) > 0 {
			seedCredentialUtxos(t, store, i, ref, tc.amounts, tc.deleted)
		}

		legacy, err := legacySumCredentialUtxoStake(ctx, store.writeDB, ref)
		require.NoError(t, err, tc.name)
		got, err := store.sumCredentialUtxoStake(ctx, store.writeDB, ref)
		require.NoError(t, err, tc.name)
		require.Equal(t, legacy, got, tc.name)

		var want uint64
		for j, amount := range tc.amounts {
			if tc.deleted != nil && tc.deleted[j] {
				continue
			}
			want += amount
		}
		require.Equal(t, want, got, tc.name)
	}
}

// oneShotSumCredentialUtxoStake reproduces sumCredentialUtxoStake's SQL
// exactly (sumCredentialUtxoStakeQuery, defined in live_stake.go) but issues
// it as a plain QueryRowContext call instead of going through Store's
// cachedStmt -- i.e. it is what sumCredentialUtxoStake looked like before it
// became a Store method backed by the prepared-statement cache. Kept as the
// direct before/after benchmark comparison point for that cache (see
// prepared_stmt.go and BenchmarkSumCredentialUtxoStake).
func oneShotSumCredentialUtxoStake(
	ctx context.Context,
	db queryer,
	ref models.StakeCredentialRef,
) (uint64, error) {
	var total sql.NullInt64
	err := db.QueryRowContext(
		ctx,
		sumCredentialUtxoStakeQuery,
		ref.Tag, ref.Key,
	).Scan(&total)
	if err != nil {
		return 0, err
	}
	if !total.Valid {
		return 0, nil
	}
	if total.Int64 < 0 {
		return 0, fmt.Errorf(
			"negative reward live stake UTxO sum for credential %d:%x",
			ref.Tag,
			ref.Key,
		)
	}
	return uint64(total.Int64), nil
}

func credentialKeyForIndex(i int) []byte {
	key := make([]byte, 28)
	key[0] = byte(i >> 16)
	key[1] = byte(i >> 8)
	key[2] = byte(i)
	return key
}

// BenchmarkSumCredentialUtxoStake is the timing counterpart: a stake
// credential with many live UTxOs (a heavily used address; profiling a
// synced node found one holding 6,794) forces refreshRewardLiveStakeAggregate
// to fetch every one of them into Go and sum there on every touch. n scales
// with how many live UTxOs a credential has accumulated by the time it is
// next touched.
func BenchmarkSumCredentialUtxoStake(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{100, 1_000, 7_000} {
		store := newMigratedSQLiteStore(b)
		ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
		amounts := make([]uint64, n)
		for i := range amounts {
			amounts[i] = uint64(1_000_000 + i)
		}
		seedCredentialUtxos(b, store, n, ref, amounts, nil)

		b.Run(fmt.Sprintf("n=%d/legacy_row_sum", n), func(b *testing.B) {
			for b.Loop() {
				if _, err := legacySumCredentialUtxoStake(ctx, store.writeDB, ref); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("n=%d/sql_aggregate_one_shot", n), func(b *testing.B) {
			for b.Loop() {
				if _, err := oneShotSumCredentialUtxoStake(ctx, store.writeDB, ref); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("n=%d/sql_aggregate_prepared_cache", n), func(b *testing.B) {
			for b.Loop() {
				if _, err := store.sumCredentialUtxoStake(ctx, store.writeDB, ref); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestSumCredentialUtxoStakeReusesCachedStatementAcrossTransactions proves
// the cached statement sumCredentialUtxoStake now uses is actually reused
// across separate write transactions (each its own *sql.Tx, via
// withWriteTransaction's autocommit path), not just within a single one --
// and that results stay correct as a credential goes from zero UTxOs to
// several, using that same cached statement across the change. This is the
// access pattern refreshRewardLiveStakeAggregate's real callers use: one
// write transaction per block/UTxO touch, not one long-lived transaction.
func TestSumCredentialUtxoStakeReusesCachedStatementAcrossTransactions(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))

	sumInTxn := func() uint64 {
		var got uint64
		err := store.withWriteTransaction(
			nil,
			func(db queryer, ctx context.Context) error {
				var err error
				got, err = store.sumCredentialUtxoStake(ctx, db, ref)
				return err
			},
		)
		require.NoError(t, err)
		return got
	}

	// Zero UTxOs: nothing seeded yet.
	require.Equal(t, uint64(0), sumInTxn())

	store.stmtMu.Lock()
	firstStmt := store.stmts[sumCredentialUtxoStakeQuery]
	store.stmtMu.Unlock()
	require.NotNil(t, firstStmt)

	// The credential gains UTxOs; queried again through a brand new write
	// transaction.
	seedCredentialUtxos(t, store, 1, ref, []uint64{5_000_000, 7}, nil)
	require.Equal(t, uint64(5_000_007), sumInTxn())

	// One of them is later spent, through yet another transaction.
	_, err := store.writeDB.ExecContext(ctx,
		"UPDATE utxo SET deleted_slot = 100 WHERE credential_tag = ? AND staking_key = ? AND amount = ?",
		ref.Tag, ref.Key, decimalUint64(types.Uint64(7)),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(5_000_000), sumInTxn())

	store.stmtMu.Lock()
	secondStmt := store.stmts[sumCredentialUtxoStakeQuery]
	entries := len(store.stmts)
	store.stmtMu.Unlock()
	require.Same(
		t,
		firstStmt,
		secondStmt,
		"expected the same cached *sql.Stmt across independent write transactions",
	)
	// hotStatements now has more than just sumCredentialUtxoStakeQuery (see
	// prepared_stmt.go); this asserts no spurious extra entry was created
	// beyond the fixed set Start prepares eagerly, not that the cache holds
	// exactly one statement.
	require.Equal(t, len(hotStatements), entries)
}

// TestSumCredentialUtxoStakeConcurrentReuse drives many goroutines through
// the cached statement concurrently, each against a distinct credential, and
// must be run with -race. It exercises both cachedStmt's first-use race
// (goroutines racing to populate the cache) and repeated concurrent use of
// the same *sql.Stmt once installed -- both documented safe by
// database/sql, but real regressions in that area are exactly the kind of
// bug a scalar-looking cache like this one can hide without a concurrent
// test.
func TestSumCredentialUtxoStakeConcurrentReuse(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()

	const goroutines = 8
	const iterations = 25
	refs := make([]models.StakeCredentialRef, goroutines)
	want := make([]uint64, goroutines)
	for i := range refs {
		refs[i] = models.NewStakeCredentialRef(0, credentialKeyForIndex(i))
		amounts := []uint64{uint64(1_000_000 + i), uint64(2_000_000 + i)}
		seedCredentialUtxos(t, store, i, refs[i], amounts, nil)
		want[i] = amounts[0] + amounts[1]
	}

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*iterations)
	for g := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for range iterations {
				got, err := store.sumCredentialUtxoStake(
					ctx,
					store.writeDB,
					refs[idx],
				)
				if err != nil {
					errCh <- err
					return
				}
				if got != want[idx] {
					errCh <- fmt.Errorf(
						"credential %d: got %d want %d",
						idx,
						got,
						want[idx],
					)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	store.stmtMu.Lock()
	entries := len(store.stmts)
	store.stmtMu.Unlock()
	// hotStatements now has more than just sumCredentialUtxoStakeQuery (see
	// prepared_stmt.go); this asserts concurrent first use created no
	// spurious extra entry beyond the fixed set Start prepares eagerly.
	require.Equal(
		t,
		len(hotStatements),
		entries,
		"expected concurrent first use to converge on the fixed set of cached statements",
	)
}

func TestRewardAccountOutputsExcludeUncreditedRows(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	stakingKey := bytes.Repeat([]byte{0x11}, 28)
	poolKey := bytes.Repeat([]byte{0x22}, 28)
	outputs := []*models.RewardAccountOutput{
		{
			Epoch: 1, StakingKey: stakingKey, PoolKeyHash: poolKey,
			RewardType: "member", Amount: 10, Spendable: true,
		},
		{
			Epoch: 2, StakingKey: stakingKey, PoolKeyHash: poolKey,
			RewardType: "member", Amount: 20, Spendable: false,
		},
		{
			Epoch: 3, StakingKey: stakingKey, PoolKeyHash: poolKey,
			RewardType: "member", Amount: 30, Spendable: true,
			Guarded: true,
		},
	}
	require.NoError(t, store.SaveRewardAccountOutputs(outputs, nil))

	rows, err := store.GetRewardAccountOutputsByCredential(
		0,
		stakingKey,
		100,
		0,
		"asc",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, uint64(1), rows[0].Epoch)
	require.False(t, rows[0].Guarded)

	count, err := store.CountRewardAccountOutputsByCredential(
		0,
		stakingKey,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	all, err := store.GetRewardAccountOutputs(3, nil)
	require.NoError(t, err)
	require.Len(t, all, 1)
	require.True(t, all[0].Guarded)
}

func TestSaveRewardAccountOutputsBatchesAndAssignsIDs(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	outputs := make([]*models.RewardAccountOutput, 250)
	for index := range outputs {
		outputs[index] = &models.RewardAccountOutput{
			Epoch:       uint64(index),
			StakingKey:  []byte{byte(index), 0x11},
			PoolKeyHash: []byte{0x22, byte(index)},
			RewardType:  "member",
			Amount:      types.Uint64(index + 1),
			Spendable:   true,
		}
	}
	require.NoError(t, store.SaveRewardAccountOutputs(outputs, nil))
	for _, output := range outputs {
		require.NotZero(t, output.ID)
	}
	rows, err := store.GetRewardAccountOutputs(249, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	// Replaying the same natural keys updates in place and retains IDs.
	ids := make([]uint, len(outputs))
	for index, output := range outputs {
		ids[index] = output.ID
	}
	require.NoError(t, store.SaveRewardAccountOutputs(outputs, nil))
	for index, output := range outputs {
		require.Equal(t, ids[index], output.ID)
	}
	duplicateA := &models.RewardAccountOutput{
		Epoch: 500, StakingKey: []byte{1}, PoolKeyHash: []byte{2},
		RewardType: "member", Amount: 1, Spendable: true,
	}
	duplicateB := &models.RewardAccountOutput{
		Epoch: 500, StakingKey: []byte{1}, PoolKeyHash: []byte{2},
		RewardType: "member", Amount: 2, Spendable: true,
	}
	require.NoError(
		t,
		store.SaveRewardAccountOutputs(
			[]*models.RewardAccountOutput{duplicateA, duplicateB},
			nil,
		),
	)
	require.Equal(t, duplicateA.ID, duplicateB.ID)
	rows, err = store.GetRewardAccountOutputs(500, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, types.Uint64(2), rows[0].Amount)
}

func TestRewardAccountGuardedQueryUsesIndex(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	rows, err := store.writeDB.Query(`
EXPLAIN QUERY PLAN
SELECT staking_key, pool_key_hash, reward_type, id, epoch, credential_tag,
       amount, spendable, guarded, captured_slot, boundary_slot
FROM reward_account_output
WHERE credential_tag = ? AND staking_key = ?
  AND spendable = TRUE AND guarded = FALSE
ORDER BY epoch ASC, pool_key_hash ASC, reward_type ASC
LIMIT ? OFFSET ?`,
		0,
		bytes.Repeat([]byte{0x11}, 28),
		100,
		0,
	)
	require.NoError(t, err)
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		details = append(details, detail)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, details)
	plan := strings.Join(details, "\n")
	require.Contains(
		t,
		plan,
		"idx_reward_account_output_credential_spendable_guarded",
	)
	require.Contains(t, plan, "guarded=?")
	require.NotContains(t, strings.ToUpper(plan), "SCAN REWARD_ACCOUNT_OUTPUT")
}

func TestStakeCalculationVersionRoundTrip(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	poolKey := bytes.Repeat([]byte{0x33}, 28)
	keyRegistrationEpoch := uint64(7)
	poolSnapshot := &models.PoolStakeSnapshot{
		Epoch:                     10,
		SnapshotType:              models.PoolStakeSnapshotTypeMark,
		PoolKeyHash:               poolKey,
		CalculationVersion:        models.RewardStakeCalculationVersion,
		LeiosKeyPublic:            []byte{1, 2, 3},
		LeiosKeyPossessionProof:   []byte{4, 5, 6},
		LeiosKeyRegistrationEpoch: &keyRegistrationEpoch,
	}
	require.NoError(t, store.SavePoolStakeSnapshot(poolSnapshot, nil))
	gotPool, err := store.GetPoolStakeSnapshot(
		10,
		models.PoolStakeSnapshotTypeMark,
		poolKey,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, gotPool)
	require.Equal(
		t,
		models.RewardStakeCalculationVersion,
		gotPool.CalculationVersion,
	)
	require.Equal(
		t,
		poolSnapshot.LeiosKeyPublic,
		gotPool.LeiosKeyPublic,
	)
	require.Equal(
		t,
		poolSnapshot.LeiosKeyPossessionProof,
		gotPool.LeiosKeyPossessionProof,
	)
	require.Equal(
		t,
		poolSnapshot.LeiosKeyRegistrationEpoch,
		gotPool.LeiosKeyRegistrationEpoch,
	)

	rewardSnapshot := &models.RewardSnapshot{
		Epoch:              10,
		SnapshotType:       models.PoolStakeSnapshotTypeMark,
		CalculationVersion: models.RewardStakeCalculationVersion,
	}
	require.NoError(t, store.SaveRewardSnapshot(rewardSnapshot, nil))
	gotReward, err := store.GetRewardSnapshot(
		10,
		models.PoolStakeSnapshotTypeMark,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, gotReward)
	require.Equal(
		t,
		models.RewardStakeCalculationVersion,
		gotReward.CalculationVersion,
	)
}

// TestStaleConsensusStakeSnapshotsExistFailsClosed covers the fail-closed
// gate itself (dingo #4026 finding 3): every prior test writes the symbolic
// current version, so none of them exercise a literal old
// calculation_version tripping the gate. It also covers finding 2: a
// non-authoritative (fallback) Mark reward_snapshot row must fail the gate
// on its own, not by relying on authoritativeMarkRewardSnapshotExists
// rejecting the version mismatch separately.
func TestStaleConsensusStakeSnapshotsExistFailsClosed(t *testing.T) {
	t.Parallel()

	t.Run("current version only reports not stale", func(t *testing.T) {
		t.Parallel()
		store := newManagementTestStore(t)
		poolKey := bytes.Repeat([]byte{0x44}, 28)
		require.NoError(
			t,
			store.SavePoolStakeSnapshot(&models.PoolStakeSnapshot{
				Epoch:              20,
				SnapshotType:       models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:        poolKey,
				CalculationVersion: models.RewardStakeCalculationVersion,
			}, nil),
		)
		require.NoError(t, store.SaveRewardSnapshot(&models.RewardSnapshot{
			Epoch:              20,
			SnapshotType:       models.PoolStakeSnapshotTypeMark,
			Authoritative:      true,
			CalculationVersion: models.RewardStakeCalculationVersion,
		}, nil))
		stale, err := store.StaleConsensusStakeSnapshotsExist(nil)
		require.NoError(t, err)
		require.False(t, stale)
	})

	t.Run(
		"literal old pool_stake_snapshot version trips the gate",
		func(t *testing.T) {
			t.Parallel()
			store := newManagementTestStore(t)
			poolKey := bytes.Repeat([]byte{0x55}, 28)
			require.NoError(
				t,
				store.SavePoolStakeSnapshot(&models.PoolStakeSnapshot{
					Epoch:              21,
					SnapshotType:       models.PoolStakeSnapshotTypeMark,
					PoolKeyHash:        poolKey,
					CalculationVersion: 1,
				}, nil),
			)
			stale, err := store.StaleConsensusStakeSnapshotsExist(nil)
			require.NoError(t, err)
			require.True(t, stale)
			epochs, err := store.StaleConsensusStakeSnapshotEpochs(nil)
			require.NoError(t, err)
			require.Equal(t, []uint64{21}, epochs)
		},
	)

	t.Run(
		"literal old authoritative reward_snapshot version trips the gate",
		func(t *testing.T) {
			t.Parallel()
			store := newManagementTestStore(t)
			require.NoError(t, store.SaveRewardSnapshot(&models.RewardSnapshot{
				Epoch:              22,
				SnapshotType:       models.PoolStakeSnapshotTypeMark,
				Authoritative:      true,
				CalculationVersion: 1,
			}, nil))
			stale, err := store.StaleConsensusStakeSnapshotsExist(nil)
			require.NoError(t, err)
			require.True(t, stale)
		},
	)

	t.Run(
		"literal old non-authoritative fallback reward_snapshot version trips the gate",
		func(t *testing.T) {
			t.Parallel()
			store := newManagementTestStore(t)
			require.NoError(t, store.SaveRewardSnapshot(&models.RewardSnapshot{
				Epoch:              23,
				SnapshotType:       models.PoolStakeSnapshotTypeMark,
				Authoritative:      false,
				CalculationVersion: 1,
			}, nil))
			stale, err := store.StaleConsensusStakeSnapshotsExist(nil)
			require.NoError(t, err)
			require.True(t, stale)
			epochs, err := store.StaleConsensusStakeSnapshotEpochs(nil)
			require.NoError(t, err)
			require.Equal(t, []uint64{23}, epochs)
		},
	)
}

func TestRewardSeedFailureRoundTripAndRollback(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	require.NoError(t, store.SaveRewardSeedFailure(
		10, "mark", "pool has no reward account", 100, nil,
	))
	reason, err := store.GetRewardSeedFailure(10, "mark", nil)
	require.NoError(t, err)
	require.Equal(t, "pool has no reward account", reason)
	require.NoError(t, store.SaveRewardSeedFailure(
		12, "mark", "pool has no reward account", 50, nil,
	))
	require.NoError(t, store.SaveRewardSeedFailure(
		12, "mark", "pool has no parameters", 200, nil,
	))
	require.NoError(t, store.SaveRewardSeedFailure(
		11, "mark", "missing parameters", 200, nil,
	))
	require.NoError(t, store.DeleteRewardStateAfterSlot(150, nil))
	reason, err = store.GetRewardSeedFailure(10, "mark", nil)
	require.NoError(t, err)
	require.Equal(t, "pool has no reward account", reason)
	reason, err = store.GetRewardSeedFailure(12, "mark", nil)
	require.NoError(t, err)
	require.Equal(t, "pool has no reward account", reason)
	reason, err = store.GetRewardSeedFailure(11, "mark", nil)
	require.NoError(t, err)
	require.Empty(t, reason)
}

// TestImportedBlockCountRollbackRemovesBothTables pins the rollback cleanup for
// the imported block counts. The per-pool rows and the epoch total row are
// deleted by separate statements in DeleteRewardStateAfterSlot, and either one
// surviving alone is worse than both surviving: an epoch total without its rows
// fails the stored-total check on every read, and rows without a total read as
// an epoch nothing was imported for. Rows left above a rollback slot would also
// pair counts from a reverted anchor with the trust boundary of the surviving
// one, which is the disjointness the reward merge assumes.
func TestImportedBlockCountRollbackRemovesBothTables(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	keptPool := bytes.Repeat([]byte{0x01}, 28)
	rolledPool := bytes.Repeat([]byte{0x02}, 28)

	require.NoError(t, store.SaveImportedPoolBlockCounts(
		[]models.ImportedPoolBlockCount{
			{
				PoolKeyHash:    keptPool,
				Epoch:          10,
				BlocksProduced: 7,
				CapturedSlot:   100,
			},
			{
				PoolKeyHash:    rolledPool,
				Epoch:          11,
				BlocksProduced: 9,
				CapturedSlot:   200,
			},
		},
		nil,
	))
	require.NoError(t, store.SaveImportedEpochBlockTotal(10, 7, 100, nil))
	require.NoError(t, store.SaveImportedEpochBlockTotal(11, 9, 200, nil))

	require.NoError(t, store.DeleteRewardStateAfterSlot(150, nil))

	counts, total, known, err := store.GetImportedPoolBlockCounts(10, nil)
	require.NoError(t, err)
	require.True(t, known, "an epoch captured below the rollback slot survives")
	require.Equal(t, uint64(7), total)
	require.Equal(t, map[string]uint64{string(keptPool): 7}, counts)

	counts, total, known, err = store.GetImportedPoolBlockCounts(11, nil)
	require.NoError(t, err,
		"neither table may keep a row captured above the rollback slot")
	require.False(t, known)
	require.Zero(t, total)
	require.Empty(t, counts)

	// Asserted against the table rather than through
	// GetImportedPoolBlockCounts, which answers "unknown" from the missing
	// total row alone and never looks at the per-pool rows. Reading it
	// through the accessor would pass with the per-pool delete removed.
	var rolledRows int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM imported_pool_block_count WHERE epoch = 11",
	).Scan(&rolledRows))
	require.Zero(t, rolledRows,
		"per-pool rows captured above the rollback slot are deleted too")
}

func TestV1Alpha1AddressTransactionIndex(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	var count int
	require.NoError(t, store.writeDB.QueryRow(`
SELECT COUNT(*)
FROM pragma_index_info('idx_addr_tx_stake_position')
WHERE (seqno = 0 AND name = 'credential_tag')
   OR (seqno = 1 AND name = 'staking_key')
   OR (seqno = 2 AND name = 'slot')
   OR (seqno = 3 AND name = 'tx_index')
   OR (seqno = 4 AND name = 'payment_key')`).Scan(&count))
	require.Equal(t, 5, count)
}

// fakePrepareOnlyQueryer implements queryer with a PrepareContext that
// always fails, so insertTransaction returns before touching the *sql.Stmt
// it would otherwise store -- this test only needs to observe the dialect
// flag insertTransaction sets before calling PrepareContext, not to execute
// a real statement.
type fakePrepareOnlyQueryer struct {
	prepareErr error
}

func (fakePrepareOnlyQueryer) ExecContext(
	context.Context,
	string,
	...any,
) (sql.Result, error) {
	return nil, errors.New("fakePrepareOnlyQueryer: ExecContext not implemented")
}

func (fakePrepareOnlyQueryer) QueryContext(
	context.Context,
	string,
	...any,
) (*sql.Rows, error) {
	return nil, errors.New("fakePrepareOnlyQueryer: QueryContext not implemented")
}

func (fakePrepareOnlyQueryer) QueryRowContext(
	context.Context,
	string,
	...any,
) *sql.Row {
	return nil
}

func (f fakePrepareOnlyQueryer) PrepareContext(
	context.Context,
	string,
) (*sql.Stmt, error) {
	return nil, f.prepareErr
}

// TestInsertTransactionDetectsMySQLThroughCountingQueryer is the regression
// test for the type assertion insertTransaction uses to detect a MySQL
// dialect: db.(dialectQueryer) alone misses a dialectQueryer wrapped in
// countingQueryer, which is exactly what every real caller passes once
// Config.PromRegistry is set (see Store.instrumentedQueryer). Without
// unwrapDialectQueryer, a.mysql stays false on a metrics-enabled MySQL
// store, and insertTransaction takes the RETURNING-id QueryRowContext path
// MySQL cannot serve instead of the ExecContext/LastInsertId path this test
// proves gets selected.
func TestInsertTransactionDetectsMySQLThroughCountingQueryer(t *testing.T) {
	t.Parallel()
	prepareErr := errors.New("prepare not needed for this assertion")
	inner := dialectQueryer{
		queryer: fakePrepareOnlyQueryer{prepareErr: prepareErr},
		dialect: "mysql",
	}
	wrapped := countingQueryer{queryer: inner, counter: nil}

	acc := &transactionBatchAccumulator{}
	_, err := acc.insertTransaction(context.Background(), wrapped)
	require.ErrorIs(t, err, prepareErr)
	require.True(
		t,
		acc.mysql,
		"expected insertTransaction to detect the mysql dialect through countingQueryer",
	)
}

func TestTransactionBatchAccumulatorResetClosesStatement(t *testing.T) {
	store := newMigratedSQLiteStore(t)
	txn := store.Transaction(context.Background())
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)

	acc := &transactionBatchAccumulator{}
	oldStmtArgs := []any{
		[]byte{0x01}, []byte{0x02}, nil, 1, 0,
		"0", "0", "0", 0, true,
	}
	_, err = acc.insertTransaction(ctx, db, oldStmtArgs...)
	require.NoError(t, err)
	stmt := acc.transactionInsert
	require.NotNil(t, stmt)

	acc.Reset()
	require.Nil(t, acc.transactionInsert)
	_, err = stmt.ExecContext(ctx, oldStmtArgs...)
	require.Error(t, err, "reset must close statements bound to the batch transaction")

	require.NoError(t, txn.Rollback())
	var count int
	require.NoError(t, store.readDB.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM "transaction" WHERE hash = ?`,
		[]byte{0x01},
	).Scan(&count))
	require.Zero(t, count, "rollback must discard writes made through the accumulator")
}

// feelessTransaction stands in for a transaction body whose Fee is nil, which
// is what TransactionBodyBase returns for any body that does not override it --
// the synthetic transaction carrying imported certificates among them. Fee is
// overridden here only so a single type can cover both the nil and non-nil
// cases; the write-path reproduction lives in ledgerstate.
type feelessTransaction struct {
	lcommon.TransactionBodyBase
	fee *big.Int
}

func (t *feelessTransaction) Type() int     { return 0 }
func (t *feelessTransaction) Cbor() []byte  { return nil }
func (t *feelessTransaction) IsValid() bool { return true }
func (t *feelessTransaction) Fee() *big.Int { return t.fee }
func (t *feelessTransaction) Metadata() lcommon.TransactionMetadatum {
	return nil
}

func (t *feelessTransaction) AuxiliaryData() lcommon.AuxiliaryData { return nil }

func (t *feelessTransaction) Certificates() []lcommon.Certificate { return nil }

func (t *feelessTransaction) Consumed() []lcommon.TransactionInput { return nil }

func (t *feelessTransaction) Produced() []lcommon.Utxo { return nil }

func (t *feelessTransaction) Hash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func (t *feelessTransaction) Id() lcommon.Blake2b256 { return lcommon.Blake2b256{} }

func (t *feelessTransaction) LeiosHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func (t *feelessTransaction) ProtocolParameterUpdates() (uint64, map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate) {
	return 0, nil
}

func (t *feelessTransaction) Witnesses() lcommon.TransactionWitnessSet {
	return nil
}

// TestTransactionFeeTreatsNilAsZero unit-tests the accessor. It does not by
// itself prove the write path is guarded -- reverting the setTransaction call
// site leaves this green -- so the reproduction that exercises
// persistImportedCommitteeCertificates end to end lives in
// ledgerstate/tests_test.go.
func TestTransactionFeeTreatsNilAsZero(t *testing.T) {
	t.Parallel()

	require.NotPanics(t, func() {
		require.Equal(
			t,
			uint64(0),
			uint64(transactionFee(&feelessTransaction{})),
		)
	})

	// A real fee is still recorded unchanged.
	require.Equal(
		t,
		uint64(174301),
		uint64(transactionFee(
			&feelessTransaction{fee: big.NewInt(174301)},
		)),
	)
}

// TestTransactionBodyBaseFeeIsNil pins the upstream behaviour this guard exists
// for. If gouroboros ever returns a zero big.Int instead, the guard becomes
// redundant rather than wrong, but the write path must never assume it.
func TestTransactionBodyBaseFeeIsNil(t *testing.T) {
	t.Parallel()

	var base lcommon.TransactionBodyBase
	require.Nil(t, base.Fee())
}
