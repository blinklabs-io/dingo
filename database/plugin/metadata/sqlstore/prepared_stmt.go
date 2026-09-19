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
	"context"
	"database/sql"
	"time"
)

// hotStatements is the fixed, exhaustive list of query texts Start prepares
// eagerly (see prepareHotStatements) and installs in the cache before the
// store is marked ready. Add a query here only after confirming, the way
// sumCredentialUtxoStakeQuery's doc comment does, that it is worth the
// caching mechanism's overhead and that it is safe to prepare once and
// reuse under this Store's single write connection.
var hotStatements = []string{
	sumCredentialUtxoStakeQuery,
	rewardLiveStakeAccountQuery,
	rewardLiveStakeUpsertQuery,
	rewardLiveStakeUtxoStakeQuery,
	insertUtxoQuery,
	insertUtxoQueryIgnoreConflict,
	importAssetQuery,
	getAssetIDQuery,
}

// cacheableForDialect reports whether query is safe to serve from the
// hot-statement cache when running against dialect. The only unsafe
// combination today is a RETURNING-id query on MySQL: dialectQueryer.
// QueryRowContext (dialect_queryer.go) special-cases exactly that shape and
// never calls QueryRowContext with the translated text at all, instead
// issuing its own ExecContext + LastInsertId (or RowsAffected, for an
// ON CONFLICT DO NOTHING that matched nothing) sequence -- a cached
// *sql.Stmt would sit unused by that path regardless of whether one exists,
// so prepareHotStatements skips creating it. PostgreSQL supports RETURNING
// natively (dialectQueryer.QueryRowContext takes its ordinary path there,
// like SQLite), so this only ever excludes the MySQL+RETURNING pair.
func cacheableForDialect(dialect, query string) bool {
	return dialect != "mysql" || !hasReturningID(query)
}

// prepareHotStatements prepares every entry in hotStatements once against
// s.writeDB and installs the result in the cache, so a later lookupCachedStmt
// for one of these queries is a plain map read rather than a fresh
// PrepareContext call. This exists because modernc.org/sqlite (the SQLite
// driver dingo now uses directly, see the "switch from glebarez/go-sqlite to
// modernc.org/sqlite directly" commit) only takes its cached-statement fast
// path when a caller holds and reuses the same *sql.Stmt: a standalone
// harness in that commit measured one-shot QueryRowContext calls at parity
// with the old driver (~6.1-6.3us/op) and a prepare-once/reuse-many pattern
// at ~2.4x faster (~2.5-3.2us/op). Every sqlc-generated query in this package
// uses the one-shot pattern, so none of them were capturing that win until
// this cache existed.
//
// This has to run here -- synchronously inside Start, before s.ready is set
// -- rather than lazily the first time a caller needs one of these
// statements. writeDB has SetMaxOpenConns(1) in production
// (database/plugin/metadata/sqlite/shared_sqlstore.go), so once any write
// transaction is open it holds the pool's only connection for its own
// lifetime; a lazy PrepareContext issued against s.writeDB while that
// transaction is still open would need a *second* connection from a pool
// that can only ever open one, and would block forever waiting for the very
// transaction it is trying to serve to release a connection it never held in
// the first place -- a self-deadlock. (An earlier version of this cache did
// exactly that and hung every test in database/plugin/metadata/sqlite,
// which wires a real SetMaxOpenConns(1) pool; database/plugin/metadata/
// sqlstore's own tests missed it because none of their harnesses cap
// connections, so the lazy PrepareContext simply opened a second one.)
// Start runs before any caller can obtain a transaction --
// transaction/dbFromTxn refuse until s.ready is true -- so it is the one
// point in the Store's lifecycle guaranteed to have no transaction open yet,
// making it the only safe place to populate this cache from scratch.
//
// A later Reset or RestoreFrom (see closePreparedStatements) empties the
// cache again without repreparing it: both are only ever exercised by
// database/lifecycle/restore.go immediately followed by closing the store
// for good (the real restore flow constructs a brand new *Store, with its
// own fresh Start, for actual subsequent use), so there is no live caller
// left to reprepare for. sumCredentialUtxoStake's uncached fallback path
// covers the remaining case -- a test or future caller that invokes Reset
// without discarding the Store -- correctly, just without the cache's
// benefit, rather than trying to reprepare on the spot and risking the same
// deadlock this function exists to avoid.
//
// A failed PrepareContext here (for example a Store constructed without
// its full migration registry, as several sqlstore unit tests do to
// exercise unrelated mechanics against a bare connection with no utxo
// table) is logged and otherwise ignored rather than failing Start: every
// hot query already has a correct, uncached fallback path for exactly this
// "not cached" case, so a missing cache entry costs performance, never
// correctness. A real production Store always runs the full migration
// registry before this point, so this only degrades a deliberately
// schema-less test harness, never a real deployment.
func (s *Store) prepareHotStatements(ctx context.Context) {
	// instrumentedQueryer, not newDialectQueryer directly: PrepareContext is
	// not counted by countingQueryer (see metrics.go), so this only gains
	// dialect translation here, same as before -- routed through the shared
	// helper purely for consistency with every other call site.
	dialectDB := s.instrumentedQueryer(s.writeDB)
	for _, query := range hotStatements {
		if !cacheableForDialect(s.dialect.Name(), query) {
			// See insertUtxoQuery's doc comment (transaction_write.go): a
			// RETURNING-id query is never safe to cache on MySQL, because
			// dialectQueryer.QueryRowContext's MySQL emulation for it bypasses
			// QueryRowContext (and so any cached *sql.Stmt) entirely. Leave it
			// out of the cache for that dialect+shape combination rather than
			// prepare a statement nothing will ever look up: queryRowCached's
			// existing "not found" fallback already calls db.QueryRowContext
			// directly, which dialectQueryer handles correctly on its own.
			continue
		}
		// Cached for reuse: stmt is stored in s.stmts below and lives for
		// the Store's lifetime, closed by closePreparedStatements on
		// Reset, RestoreFrom, and CloseContext (see that function's doc
		// comment). It is not a one-shot resource, so closing it here
		// would defeat the caching this function exists to provide.
		stmt, err := dialectDB.PrepareContext(ctx, query) //nolint:sqlclosecheck
		if err != nil {
			s.logger.Warn(
				"sqlstore: skipping prepared-statement cache entry",
				"dialect", s.dialect.Name(),
				"error", err,
			)
			continue
		}
		s.stmtMu.Lock()
		if s.stmts == nil {
			s.stmts = make(map[string]*sql.Stmt)
		}
		s.stmts[query] = stmt
		s.stmtMu.Unlock()
	}
}

// lookupCachedStmt returns the statement prepareHotStatements installed for
// query, if any. It never prepares one itself -- see prepareHotStatements
// for why a lazy Prepare against s.writeDB here would risk deadlocking
// against a caller's own open write transaction. A caller must handle the
// !ok case with a plain, uncached call against its own queryer (which needs
// no extra connection and so can never deadlock this way) rather than treat
// a miss as an error: after Reset/RestoreFrom invalidate the cache, absence
// is an expected, correct state, not a bug.
func (s *Store) lookupCachedStmt(query string) (*sql.Stmt, bool) {
	s.stmtMu.Lock()
	defer s.stmtMu.Unlock()
	stmt, ok := s.stmts[query]
	return stmt, ok
}

// closePreparedStatements closes and discards every cached statement. Safe
// to call more than once (a nil map closes nothing) and safe to call while
// another goroutine holds a previously-returned *sql.Stmt or *sql.Tx-scoped
// statement derived from it: database/sql keeps a Stmt usable by any Rows
// or Tx already using it and only releases the underlying driver resource
// once every such use completes.
func (s *Store) closePreparedStatements() {
	s.stmtMu.Lock()
	stmts := s.stmts
	s.stmts = nil
	s.stmtMu.Unlock()
	for _, stmt := range stmts {
		_ = stmt.Close()
	}
}

// stmtForQueryer returns the statement to actually call for db, reusing
// cached's already-compiled statement instead of recompiling one from
// scratch wherever database/sql allows it.
//
//   - *sql.Tx: txScopedStmt hands back a transaction-scoped *sql.Stmt,
//     deriving it with (*sql.Tx).StmtContext on first use and reusing the
//     same derived *sql.Stmt for every later call against the same (tx,
//     cached) pair -- see txScopedStmt for why a fresh StmtContext call per
//     row is itself a retention bug, not just a missed optimization. Per
//     database/sql's own documentation, StmtContext reuses cached's
//     underlying driver statement when tx is running on the same connection
//     cached was prepared against, and transparently re-prepares on tx's
//     connection otherwise -- either way the result is correct, but only the
//     first case captures modernc.org/sqlite's cached-statement fast path.
//     writeDB has SetMaxOpenConns(1) (database/plugin/metadata/sqlite/
//     shared_sqlstore.go), so in dingo's real access pattern there is
//     exactly one live write connection and every write transaction runs on
//     it, making the reuse path the only path actually taken -- confirmed by
//     TestSumCredentialUtxoStakeReusesCachedStatementAcrossTransactions,
//     which asserts pointer identity of the cached *sql.Stmt across
//     independent transactions rather than trusting this comment.
//   - dialectQueryer: unwrap to the handle it wraps and recurse. Dialect
//     translation already happened once, at prepare time
//     (prepareHotStatements uses the same newDialectQueryer(...).
//     PrepareContext used here), so calling through the unwrapped handle is
//     correct and avoids re-translating text that is already
//     dialect-correct.
//   - countingQueryer: unwrap and recurse for the same reason as
//     dialectQueryer. This case is not optional: every db a real caller
//     passes in is wrapped in countingQueryer whenever Config.PromRegistry
//     is set (Store.instrumentedQueryer applies it around every queryer,
//     including the *sql.Tx a write transaction hands out), so without this
//     case the *sql.Tx below it would never match on a real, metrics-
//     enabled Store -- every hot-statement call inside a write transaction
//     would silently fall through to the default branch and call cached
//     directly against the pool it was originally prepared on, instead of
//     the transaction-scoped statement (*sql.Tx).StmtContext returns. With
//     writeDB's SetMaxOpenConns(1), that pool has no connection to hand out
//     while the transaction holds its only one, so a real call would block
//     forever -- exactly the deadlock prepareHotStatements' own comment
//     warns eager (not lazy) preparation exists to avoid, reintroduced here
//     by a different path.
//   - anything else (a bare *sql.DB, or any future queryer implementation):
//     cached was prepared directly against s.writeDB, so it is already the
//     right handle to call with no further translation needed.
func (s *Store) stmtForQueryer(
	ctx context.Context,
	db queryer,
	cached *sql.Stmt,
) *sql.Stmt {
	switch v := db.(type) {
	case *sql.Tx:
		return s.txScopedStmt(ctx, v, cached)
	case dialectQueryer:
		return s.stmtForQueryer(ctx, v.queryer, cached)
	case countingQueryer:
		return s.stmtForQueryer(ctx, v.queryer, cached)
	default:
		return cached
	}
}

// txScopedStmtAfterDerive, when non-nil, runs synchronously inside
// txScopedStmt immediately after tx.StmtContext returns and before the
// derived statement is (re-)inserted into s.txStmts. Production code never
// sets this -- it exists only so a test can force the eviction race window
// between derivation and insertion deterministically instead of relying on
// scheduler timing (see TestTxScopedStmtDoesNotLeakAcrossConcurrentEviction).
var txScopedStmtAfterDerive func()

// txScopedStmt returns the *sql.Stmt tx should use for cached, deriving it
// with (*sql.Tx).StmtContext only on the first call for this (tx, cached)
// pair within the transaction's lifetime and returning the same derived
// *sql.Stmt on every later call.
//
// Without this, a caller that consults the same cached statement many times
// inside one transaction -- refreshRewardLiveStakeRefs loops
// refreshRewardLiveStakeAggregate over every stake ref inside one
// withWriteTransaction, and DeleteUtxos/importUtxos/the genesis paths pass
// whole ref slices the same way -- derives a brand new Tx-scoped *sql.Stmt
// on every row. database/sql appends every such *sql.Stmt to the *sql.Tx's
// own internal list and only releases it at commit or rollback (see
// database/sql's Tx.stmts), so retention was linear in rows times cached
// statements consulted per row: measured in-tree, 2000 refs in one write
// transaction retained 4000 Tx-scoped statements with the reward-live-stake
// cache entries active, and 0 with hotStatements emptied. Deriving once per
// transaction keeps the parse-cost saving prepareHotStatements exists for
// without that retention, because it is the same object being reused --
// exactly the cached, Store-lifetime statement's own reuse story, one layer
// down.
//
// The cache lives in s.txStmts, keyed by tx itself, rather than on a
// wrapper type threaded through the queryer chain (dialectQueryer, the
// only other layer in that chain) deliberately: dbFromTxn and
// withWriteTransaction hand every caller the *sql.Tx itself (unwrapped, or
// wrapped only in dialectQueryer), and at least one caller --
// requireAccountBaselineTransaction -- type-asserts through dialectQueryer
// looking for exactly a *sql.Tx to confirm it is running inside a write
// transaction. Substituting a different concrete type there would silently
// break that check. Keying by the *sql.Tx pointer instead leaves every
// existing type assertion on db's dynamic type untouched.
//
// store.go's sqlTxn.releaseConnection, called from both Commit and
// Rollback, evicts tx's entry from s.txStmts (via evictTxStmts) once tx
// itself is finished, so this cache never outlives the transaction it was
// built for and never grows across transactions: a later transaction gets a
// new *sql.Tx from the driver and therefore a fresh, empty entry here.
//
// This requires tx to be used by exactly one goroutine at a time, start to
// finish: derivation above runs with s.txStmtMu released (StmtContext can
// block on I/O), so a concurrent Commit/Rollback on the SAME *sqlTxn could
// run releaseConnection/evictTxStmts in that window and have this call's own
// insert below recreate the now-evicted entry afterward, retaining a *sql.Stmt
// tied to an already-finished transaction for the Store's lifetime
// (TestTxScopedStmtDoesNotLeakAcrossConcurrentEviction reproduces this
// directly, by calling txScopedStmt and evictTxStmts concurrently against one
// tx). dingo's real write path never creates that window: withWriteTransaction
// and database.Txn.Do both run the caller's callback to completion,
// synchronously, in the same goroutine that then calls Commit/Rollback, and
// the one place in the ledger that fans a single logical read out across
// goroutines against a shared *database.Txn -- queryShelleyUtxoWhole's
// resolve pool (ledger/queries_utxowhole.go) -- deliberately gives each
// worker its own transaction instead of sharing the caller's, exactly to
// avoid this. A caller-supplied txn threaded through several sequential
// domain calls (the common "if txn == nil { txn = ... }" shape used
// throughout database/*.go) stays on one goroutine the same way.
func (s *Store) txScopedStmt(
	ctx context.Context,
	tx *sql.Tx,
	cached *sql.Stmt,
) *sql.Stmt {
	s.txStmtMu.Lock()
	if derived, ok := s.txStmts[tx][cached]; ok {
		s.txStmtMu.Unlock()
		return derived
	}
	s.txStmtMu.Unlock()

	// StmtContext itself can block on I/O (re-preparing on tx's connection
	// when it differs from cached's), so it runs outside the lock. A
	// concurrent caller deriving the same (tx, cached) pair at the same time
	// would each derive their own *sql.Stmt here; the second store below
	// discards the loser rather than leaking it, so the map never disagrees
	// with which one is "the" cached derivative even under that race.
	derived := tx.StmtContext(ctx, cached)
	if txScopedStmtAfterDerive != nil {
		txScopedStmtAfterDerive()
	}

	s.txStmtMu.Lock()
	defer s.txStmtMu.Unlock()
	if existing, ok := s.txStmts[tx][cached]; ok {
		return existing
	}
	if s.txStmts == nil {
		s.txStmts = make(map[*sql.Tx]map[*sql.Stmt]*sql.Stmt)
	}
	if s.txStmts[tx] == nil {
		s.txStmts[tx] = make(map[*sql.Stmt]*sql.Stmt)
	}
	s.txStmts[tx][cached] = derived
	return derived
}

// evictTxStmts discards tx's entry in the per-transaction Tx-scoped
// statement cache, if any. Safe to call with a nil tx (a *sqlTxn that never
// obtained a real *sql.Tx, see store.go's transaction/beginWriteTx error
// paths) and safe to call more than once: both are plain map operations
// that no-op when there is nothing to remove. It does not close any
// statement itself -- database/sql already closes every *sql.Stmt it
// derived from tx once tx commits or rolls back, which by construction has
// already happened by the time a caller (sqlTxn.releaseConnection) invokes
// this.
func (s *Store) evictTxStmts(tx *sql.Tx) {
	s.txStmtMu.Lock()
	delete(s.txStmts, tx)
	s.txStmtMu.Unlock()
}

// queryRowCached and execCached are the shared cache-or-fallback dance
// sumCredentialUtxoStake originally inlined by hand: use the hot-statement
// cache when query has an entry, and fall back to a plain one-shot call
// against db (which needs no extra connection, see prepareHotStatements) when
// it does not. Every hotStatements entry should route through one of these
// two rather than repeating the branch, so the deadlock and
// Reset/RestoreFrom-invalidation reasoning documented above stays in one
// place as the cache gains more entries.
func (s *Store) queryRowCached(
	ctx context.Context,
	db queryer,
	query string,
	args ...any,
) *sql.Row {
	if cached, ok := s.lookupCachedStmt(query); ok {
		// Counted and timed here, not by countingQueryer: stmtForQueryer
		// resolves straight to a *sql.Stmt, bypassing db (and any
		// countingQueryer wrapping it) entirely -- see metrics.go's doc
		// comment on countingQueryer's PrepareContext for why that makes
		// this the right place to count and time a cache hit.
		op, name := classifySQLStatement(query)
		if s.sqlOperations != nil {
			s.sqlOperations.WithLabelValues(op).Inc()
		}
		// stmtForQueryer returns either the shared, Store-lifetime cached
		// statement itself (must not be closed here, see
		// prepareHotStatements) or a *sql.Tx-scoped statement from
		// (*sql.Tx).StmtContext, which database/sql documents as being
		// closed automatically when the transaction commits or rolls
		// back. Either way this call site owns no resource of its own to
		// close, and closing eagerly would be wrong besides: the *sql.Row
		// returned below defers running Scan against it until the caller
		// invokes Scan.
		stmt := s.stmtForQueryer(ctx, db, cached) //nolint:sqlclosecheck
		if s.sqlQueryDuration == nil {
			return stmt.QueryRowContext(ctx, args...)
		}
		start := time.Now()
		row := stmt.QueryRowContext(ctx, args...)
		s.sqlQueryDuration.WithLabelValues(op, name).
			Observe(time.Since(start).Seconds())
		return row
	}
	return db.QueryRowContext(ctx, query, args...)
}

func (s *Store) execCached(
	ctx context.Context,
	db queryer,
	query string,
	args ...any,
) (sql.Result, error) {
	if cached, ok := s.lookupCachedStmt(query); ok {
		op, name := classifySQLStatement(query)
		if s.sqlOperations != nil {
			s.sqlOperations.WithLabelValues(op).Inc()
		}
		// Same reasoning as queryRowCached above: the statement here is
		// either the shared cache entry (never closed by a call site) or
		// a Tx-scoped derivative that database/sql closes on its own when
		// the transaction ends, so there is nothing for this function to
		// close.
		stmt := s.stmtForQueryer(ctx, db, cached) //nolint:sqlclosecheck
		if s.sqlQueryDuration == nil {
			return stmt.ExecContext(ctx, args...)
		}
		start := time.Now()
		result, err := stmt.ExecContext(ctx, args...)
		s.sqlQueryDuration.WithLabelValues(op, name).
			Observe(time.Since(start).Seconds())
		return result, err
	}
	return db.ExecContext(ctx, query, args...)
}
