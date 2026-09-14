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
//   - *sql.Tx: (*sql.Tx).StmtContext hands back a transaction-scoped
//     *sql.Stmt. Per database/sql's own documentation, this reuses cached's
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
func stmtForQueryer(
	ctx context.Context,
	db queryer,
	cached *sql.Stmt,
) *sql.Stmt {
	switch v := db.(type) {
	case *sql.Tx:
		return v.StmtContext(ctx, cached)
	case dialectQueryer:
		return stmtForQueryer(ctx, v.queryer, cached)
	case countingQueryer:
		return stmtForQueryer(ctx, v.queryer, cached)
	default:
		return cached
	}
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
		stmt := stmtForQueryer(ctx, db, cached) //nolint:sqlclosecheck
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
		stmt := stmtForQueryer(ctx, db, cached) //nolint:sqlclosecheck
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
