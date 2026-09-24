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

//nolint:gosec,sqlclosecheck // SQL INTEGER mappings preserve the unsigned domain API; cursors are explicitly closed before dependent queries.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// rewardLiveStakeAccountQuery is refreshRewardLiveStakeAggregate's account
// lookup: a plain SELECT with no RETURNING clause, run once per UTxO a stake
// credential gains or loses -- the same call frequency as
// sumCredentialUtxoStakeQuery, which it is always paired with in that
// function. Like sumCredentialUtxoStakeQuery it is safe to route through
// dialectQueryer.QueryRowContext's ordinary path for every dialect: that
// wrapper only special-cases a query matched by hasReturningID (a trailing
// "RETURNING id"), which this query never has, so translate() plus a direct
// QueryRowContext call is exactly what a cached *sql.Stmt (already
// dialect-translated at prepare time, see prepareHotStatements) reproduces.
const rewardLiveStakeAccountQuery = `
SELECT reward, pool, active, added_slot
FROM account
WHERE credential_tag = ? AND staking_key = ?`

// rewardLiveStakeUpsertQuery is refreshRewardLiveStakeAggregate's other
// per-touch query: the upsert that records the freshly recomputed total.
// It has no RETURNING clause either (the row's id is never read back here),
// so it always goes through ExecContext -- both as a one-shot call and,
// once cached, via a *sql.Stmt returned by stmtForQueryer -- with no
// dialect-specific branch to bypass. dialectQueryer.translate's ON CONFLICT
// rewrite for MySQL happens once, at PrepareContext time, exactly as it does
// for a one-shot ExecContext call today.
const rewardLiveStakeUpsertQuery = `
INSERT INTO reward_live_stake (
    credential_tag, staking_key, pool_key_hash, utxo_stake, reward_stake,
    total_stake, registered, pool_delegation_slot,
    pool_delegation_block_index, pool_delegation_cert_index, updated_slot,
    calculation_version
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
ON CONFLICT (credential_tag, staking_key) DO UPDATE SET
    pool_key_hash = excluded.pool_key_hash,
    utxo_stake = excluded.utxo_stake,
    reward_stake = excluded.reward_stake,
    total_stake = excluded.total_stake,
    registered = excluded.registered,
    pool_delegation_slot = excluded.pool_delegation_slot,
    pool_delegation_block_index = excluded.pool_delegation_block_index,
    pool_delegation_cert_index = excluded.pool_delegation_cert_index,
    updated_slot = excluded.updated_slot,
    calculation_version = excluded.calculation_version`

func (s *Store) refreshRewardLiveStakeAggregate(
	ctx context.Context,
	db queryer,
	ref models.StakeCredentialRef,
	slot uint64,
) error {
	if len(ref.Key) == 0 {
		return nil
	}
	utxoStake, err := s.sumCredentialUtxoStake(ctx, db, ref)
	if err != nil {
		return fmt.Errorf("sum reward live stake UTxOs: %w", err)
	}
	return s.applyRewardLiveStakeAggregate(ctx, db, ref, slot, utxoStake)
}

// rewardLiveStakeUtxoStakeQuery reads the running live-UTxO total
// refreshRewardLiveStakeAggregateDelta trusts instead of recomputing it with
// sumCredentialUtxoStake's full scan. It is the same (credential_tag,
// staking_key) point lookup the upsert's conflict target already indexes
// (idx_reward_live_stake_cred), so this is an indexed single-row read, not a
// scan.
const rewardLiveStakeUtxoStakeQuery = `
SELECT utxo_stake FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`

// currentCredentialUtxoStake reads a credential's currently stored running
// UTxO total. The bool return distinguishes "no row yet" (a brand new
// credential, or one whose row was deleted after its stake dropped to zero --
// see applyRewardLiveStakeAggregate's delete branch) from "row exists with
// stake 0", since only the latter is a trustworthy baseline to apply a delta
// against.
func (s *Store) currentCredentialUtxoStake(
	ctx context.Context,
	db queryer,
	ref models.StakeCredentialRef,
) (uint64, bool, error) {
	var raw sql.NullString
	err := s.queryRowCached(
		ctx, db, rewardLiveStakeUtxoStakeQuery, ref.Tag, ref.Key,
	).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if !raw.Valid || raw.String == "" {
		return 0, false, nil
	}
	value, err := parseUint64("reward live stake UTxO total", raw.String)
	if err != nil {
		return 0, false, err
	}
	return value, true, nil
}

// applyUtxoStakeDelta adjusts a credential's running UTxO total by delta,
// the exact signed lovelace amount one write's UTxO mutations contributed to
// it. It fails closed on underflow (a negative result, which can only mean
// the running total was already wrong -- a delta larger in magnitude than
// the credential is recorded as holding) and on overflow, rather than
// silently wrapping either into a value sumCredentialUtxoStake's own
// overflow check would otherwise have to catch downstream.
func applyUtxoStakeDelta(
	current uint64,
	delta int64,
	ref models.StakeCredentialRef,
) (uint64, error) {
	if delta < 0 {
		dec := uint64(-delta)
		if dec > current {
			return 0, fmt.Errorf(
				"reward live stake UTxO underflow for credential %d:%x (current %d, delta %d)",
				ref.Tag,
				ref.Key,
				current,
				delta,
			)
		}
		return current - dec, nil
	}
	inc := uint64(delta)
	if inc > ^uint64(0)-current {
		return 0, fmt.Errorf(
			"reward live stake UTxO overflow for credential %d:%x (current %d, delta %d)",
			ref.Tag,
			ref.Key,
			current,
			delta,
		)
	}
	return current + inc, nil
}

// refreshRewardLiveStakeAggregateDelta is refreshRewardLiveStakeAggregate's
// incremental counterpart: instead of recomputing a credential's entire
// live-UTxO total from scratch (sumCredentialUtxoStake's O(live UTxOs for
// this credential) scan, dingo #4421), it reads the running total already
// stored in reward_live_stake and adjusts it by delta -- the exact signed
// change this one write's UTxO mutations made to the credential's total, an
// O(1) indexed point lookup plus an in-memory add.
//
// This is safe only for a caller that can state delta exactly: a produced
// output's amount, the negative of a consumed input's amount, or 0 for a
// certificate-only touch that never mutated the utxo table. A caller that
// cannot state delta exactly (a bulk rollback sweep affecting an unknown mix
// of rows, for instance) must keep using refreshRewardLiveStakeAggregate's
// full scan instead -- see setTransactionWithAccumulator and
// SetGapBlockTransaction for the two callers that qualify today, and
// DATABASE.md's "Incremental live-UTxO stake maintenance" section for the
// full design, including why the other callers deliberately do not use this
// path.
//
// The read and the upsert are not one atomic statement, so they rely on the
// same property the full-scan path always has: block application is the only
// writer of a credential's reward_live_stake row and applies one block at a
// time, so no second write transaction can change utxo_stake between them.
// (On SQLite that is reinforced by writeDB's SetMaxOpenConns(1); on Postgres
// and MySQL, whose providers share one pool of up to 100 connections, the
// ledger's single apply loop is the whole of it.) The consequence of breaking
// that property is worse here than on the full-scan path -- a lost update
// there is recomputed from the utxo table on the next touch, while a lost
// update here persists until RewardLiveStakeNeedsBackfill runs -- so a future
// caller that writes this table off the apply loop must make the read and
// write atomic rather than reuse this function as-is.
//
// If no running total is recorded yet for this credential,
// sumCredentialUtxoStake's full scan establishes a fresh, authoritative
// baseline instead of trusting delta against an unknown prior value -- cheap
// here specifically because a credential with no running total has, by
// construction, few live UTxOs at this point (a credential that has been
// touched enough to accumulate many either already has a running total, or
// the touch that dropped it to zero deleted the row and this is its first
// touch since).
func (s *Store) refreshRewardLiveStakeAggregateDelta(
	ctx context.Context,
	db queryer,
	ref models.StakeCredentialRef,
	slot uint64,
	delta int64,
) error {
	if len(ref.Key) == 0 {
		return nil
	}
	current, ok, err := s.currentCredentialUtxoStake(ctx, db, ref)
	if err != nil {
		return fmt.Errorf("read running reward live stake UTxO total: %w", err)
	}
	var utxoStake uint64
	if ok {
		utxoStake, err = applyUtxoStakeDelta(current, delta, ref)
		if err != nil {
			return err
		}
	} else {
		utxoStake, err = s.sumCredentialUtxoStake(ctx, db, ref)
		if err != nil {
			return fmt.Errorf("sum reward live stake UTxOs: %w", err)
		}
	}
	return s.applyRewardLiveStakeAggregate(ctx, db, ref, slot, utxoStake)
}

// applyRewardLiveStakeAggregate is refreshRewardLiveStakeAggregate and
// refreshRewardLiveStakeAggregateDelta's shared tail: given a credential's
// UTxO total (however it was obtained), read its account state and upsert
// the combined reward_live_stake row. Neither caller-specific computation
// above changes any of this logic.
func (s *Store) applyRewardLiveStakeAggregate(
	ctx context.Context,
	db queryer,
	ref models.StakeCredentialRef,
	slot uint64,
	utxoStake uint64,
) error {
	var reward sql.NullString
	var pool []byte
	var active sql.NullBool
	var addedSlot sql.NullInt64
	accountErr := s.queryRowCached(
		ctx, db, rewardLiveStakeAccountQuery, ref.Tag, ref.Key,
	).Scan(&reward, &pool, &active, &addedSlot)
	if accountErr != nil && !errors.Is(accountErr, sql.ErrNoRows) {
		return fmt.Errorf("query reward live stake account: %w", accountErr)
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
	utxoValue := utxoStake
	if utxoValue > ^uint64(0)-rewardStake {
		return fmt.Errorf(
			"reward live stake overflow for credential %d:%x",
			ref.Tag,
			ref.Key,
		)
	}
	total := utxoValue + rewardStake
	if errors.Is(accountErr, sql.ErrNoRows) && total == 0 {
		_, err := db.ExecContext(ctx, `
DELETE FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`,
			ref.Tag,
			ref.Key,
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
	_, err = s.execCached(
		ctx, db, rewardLiveStakeUpsertQuery,
		ref.Tag,
		ref.Key,
		pool,
		decimalUint64(types.Uint64(utxoValue)),
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

// sumCredentialUtxoStake totals a stake credential's live UTxO amounts with
// a single SQL aggregate instead of streaming every matching row into Go and
// summing there (what sumUint64Rows does generically). A heavily used
// address can carry thousands of live UTxOs, and refreshRewardLiveStakeAggregate
// recomputes this total on every UTxO the credential gains or loses, so the
// per-row round trip was scaling with the credential's UTxO count on every
// touch: profiling a synced node found one credential with 6,794 live UTxOs,
// and sumUint64Rows's rows.Next() loop over exactly this query was 20.78% of
// total CPU, almost all of it inside SQLite's row-fetch path rather than the
// small Go-side parse per row.
//
// CAST(amount AS BIGINT) is safe in a way sumUint64Rows's generic decimal-text
// parsing has to avoid: sumUint64Rows exists because some amount domains (for
// example asset.amount, a native-token quantity) span the full uint64 range,
// which SQL SUM as a signed integer cannot represent exactly. A utxo.amount is
// a lovelace value bounded by the total ada supply (~4.5e16), far inside a
// signed 64-bit integer, so summing it in SQL can never overflow the way a
// true full-width uint64 domain could -- this function is deliberately not a
// replacement for sumUint64Rows and must not be reused for a column whose
// domain isn't similarly bounded.
//
// BIGINT rather than INTEGER: SQLite gives any CAST type name containing
// "INT" the same 8-byte integer storage, so "AS INTEGER" and "AS BIGINT"
// behave identically there, and dialect_queryer.go already rewrites "AS
// INTEGER" to MySQL's 64-bit "AS SIGNED" for other queries -- but plain
// PostgreSQL INTEGER is only 32 bits (max ~2147 ADA), which real UTxO totals
// exceed immediately, unlike the existing "AS INTEGER" casts in this package,
// which are all over slot numbers still far below that bound. BIGINT is
// PostgreSQL's native 64-bit type name, and dialect_queryer.go now rewrites
// it to "AS SIGNED" for MySQL the same way it does "AS INTEGER".
//
// The one behavior sumUint64Rows had that this does not reproduce: a
// negative-looking amount string fails sumUint64Rows immediately, at the row
// that holds it, while an aggregate SUM would only be caught here if the
// total itself goes negative. decimalUint64 is the column's only writer and
// never emits a negative representation, so this is a difference on data the
// invariant already rules out, not a live behavior change -- the same
// single-writer argument LatestPoolOpCertSequence relied on for its NULL
// case (commit cee516017).
//
// This is a method (rather than the free function it used to be) so it can
// reach s.lookupCachedStmt: profiling a synced node found this single query
// -- called on every UTxO a stake credential gains or loses, via
// refreshRewardLiveStakeAggregate -- was 17.55% of total process CPU, the
// largest single hotspot found. It was already the single-aggregate rewrite
// described above rather than the row-streaming sumUint64Rows path, so the
// remaining cost was the one-shot QueryRowContext call pattern itself
// recompiling the statement on every invocation; see prepared_stmt.go for
// why caching and reusing one *sql.Stmt here is safe and
// BenchmarkSumCredentialUtxoStake for the measured effect.
const sumCredentialUtxoStakeQuery = `
SELECT SUM(CAST(amount AS BIGINT))
FROM utxo
WHERE credential_tag = ? AND staking_key = ? AND deleted_slot = 0`

func (s *Store) sumCredentialUtxoStake(
	ctx context.Context,
	db queryer,
	ref models.StakeCredentialRef,
) (uint64, error) {
	s.sumCredentialUtxoStakeCalls.Add(1)
	// No cached statement means Start (the only place that populates it) has
	// not run since a Reset/RestoreFrom last invalidated the cache;
	// queryRowCached falls back to a plain one-shot call against db itself in
	// that case rather than trying to populate the cache here -- db already
	// holds whatever connection it needs (see prepareHotStatements for why a
	// fresh PrepareContext against s.writeDB at this point could deadlock
	// against db's own open transaction).
	row := s.queryRowCached(ctx, db, sumCredentialUtxoStakeQuery, ref.Tag, ref.Key)
	var total sql.NullInt64
	err := row.Scan(&total)
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

func (s *Store) RebuildRewardLiveStake(
	slot uint64,
	txn types.Txn,
) error {
	return s.rebuildRewardLiveStake(slot, txn, false)
}

// RebuildRewardLiveStakeFromRunningTotals finalizes the aggregate using the
// running UTxO totals maintained during Mithril ledger-state import. It is
// valid only after that import has completed: historical API replay preserves
// the snapshot's live UTxO set while filling in transaction history.
func (s *Store) RebuildRewardLiveStakeFromRunningTotals(
	slot uint64,
	txn types.Txn,
) error {
	return s.rebuildRewardLiveStake(slot, txn, true)
}

// verifyRewardLiveStakeRunningTotals enforces the running-total path's
// precondition: the ledger-state importer must have recorded a utxo_stake for
// every credential that still holds a live UTxO, because that path never
// rescans the live UTxO set to derive one.
//
// This is one indexed semi-join over the distinct live-UTxO credentials
// rather than a correlated EXISTS evaluated per credential inside the
// finalizer's own SELECT. The per-credential form made the finalizer's cost
// grow with the credential population twice over, and it is a large part of
// why that SELECT could hold its write transaction -- and so the WAL snapshot
// -- far longer than the work required (#4610).
func (s *Store) verifyRewardLiveStakeRunningTotals(
	ctx context.Context,
	db queryer,
) error {
	row := db.QueryRowContext(ctx, `
SELECT live.credential_tag, live.staking_key
FROM (
    SELECT DISTINCT credential_tag, staking_key
    FROM utxo
    WHERE deleted_slot = 0
      AND staking_key IS NOT NULL
      AND LENGTH(staking_key) > 0
) live
WHERE NOT EXISTS (
    SELECT 1 FROM reward_live_stake
    WHERE reward_live_stake.credential_tag = live.credential_tag
      AND reward_live_stake.staking_key = live.staking_key
      AND reward_live_stake.utxo_stake IS NOT NULL
)
LIMIT 1`)
	var tag uint8
	var key []byte
	switch err := row.Scan(&tag, &key); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("verify reward live stake running totals: %w", err)
	}
	return fmt.Errorf(
		"missing reward live stake running total for credential %d:%x",
		tag,
		key,
	)
}

// rewardLiveStakeRebuildBatch is how many stake keys one pass of the rebuild
// ranks, materializes and upserts. A single pass over every key has to hold
// the whole result set in Go before it can write (see
// rebuildRewardLiveStakeRange), which measured 3.2 GB of resident memory at
// 3.6 million keys against a 0.4 GB process baseline, and reports nothing
// until it finishes. Batching bounds both the Go-side rows and SQLite's
// per-statement temp b-trees to one batch, and gives the rebuild a place to
// report progress, while each batch reads only its own index ranges so the
// total work stays linear in the key count (#4610).
const rewardLiveStakeRebuildBatch = 20_000

func (s *Store) rewardLiveStakeBatch() int {
	if s.rewardLiveStakeBatchSize > 0 {
		return s.rewardLiveStakeBatchSize
	}
	return rewardLiveStakeRebuildBatch
}

// rewardLiveStakeProgressInterval throttles the rebuild's Info progress log.
const rewardLiveStakeProgressInterval = 10 * time.Second

type stakeKeyBound struct {
	tag uint8
	key []byte
}

// stakeKeyRange is the interval (lo, hi] over (credential_tag, staking_key)
// in index order; a nil bound is unbounded. Batches are contiguous, so each
// key lands in exactly one batch.
type stakeKeyRange struct {
	lo, hi *stakeKeyBound
}

// predicate returns the range condition over the given columns and the
// arguments for its placeholders. It is a row-value comparison so that every
// dialect can drive it from the (credential_tag, staking_key) index prefix
// the account, utxo and stake-assignment tables already carry.
func (r stakeKeyRange) predicate(tagColumn, keyColumn string) (string, []any) {
	var clauses []string
	var args []any
	row := "(" + tagColumn + ", " + keyColumn + ")"
	if r.lo != nil {
		clauses = append(clauses, row+" > (?, ?)")
		args = append(args, r.lo.tag, r.lo.key)
	}
	if r.hi != nil {
		clauses = append(clauses, row+" <= (?, ?)")
		args = append(args, r.hi.tag, r.hi.key)
	}
	if len(clauses) == 0 {
		return "1 = 1", nil
	}
	return strings.Join(clauses, " AND "), args
}

const liveUtxoStakeKeyFilter = `deleted_slot = 0
      AND staking_key IS NOT NULL
      AND LENGTH(staking_key) > 0`

// rewardLiveStakeKeySource is every stake key the aggregate carries a row
// for: registered accounts and keys that hold a live UTxO without one.
func rewardLiveStakeKeySource(accountRange, utxoRange string) string {
	return `
    SELECT credential_tag, staking_key FROM account
    WHERE ` + accountRange + `
    UNION
    SELECT credential_tag, staking_key FROM utxo
    WHERE ` + liveUtxoStakeKeyFilter + `
      AND ` + utxoRange
}

// countRewardLiveStakeKeys sizes the rebuild so its progress log can report
// an ETA.
func countRewardLiveStakeKeys(ctx context.Context, db queryer) (int64, error) {
	var count int64
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM (`+
		rewardLiveStakeKeySource("1 = 1", "1 = 1")+`
) stake_keys`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count reward live stake keys: %w", err)
	}
	return count, nil
}

// nextRewardLiveStakeBatchEnd returns the last stake key of the batch of
// batchSize keys that starts after lo, or nil when fewer remain and the batch
// runs to the end of the key space.
func nextRewardLiveStakeBatchEnd(
	ctx context.Context,
	db queryer,
	lo *stakeKeyBound,
	batchSize int,
) (*stakeKeyBound, error) {
	after := stakeKeyRange{lo: lo}
	accountRange, accountArgs := after.predicate("credential_tag", "staking_key")
	utxoRange, utxoArgs := after.predicate("credential_tag", "staking_key")
	args := append(append(accountArgs, utxoArgs...), batchSize-1)
	var end stakeKeyBound
	err := db.QueryRowContext(ctx, `
SELECT credential_tag, staking_key FROM (`+
		rewardLiveStakeKeySource(accountRange, utxoRange)+`
) stake_keys
ORDER BY credential_tag, staking_key
LIMIT 1 OFFSET ?`, args...).Scan(&end.tag, &end.key)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find reward live stake batch end: %w", err)
	}
	return &end, nil
}

// rewardLiveStakeCredentialQuery builds the rebuild's SELECT for one key
// range, with the range pushed into every table it reads so each batch scans
// only its own index ranges.
//
// latest_delegation is computed only for keys whose account row is active
// with a non-NULL pool. The outer LEFT JOIN can match no other key, and the
// caller discards the delegation columns for every other key, so this is
// result-preserving; the restriction is per key, not per row, so it cannot
// change which row wins rn = 1 for a key it keeps. It is also what keeps the
// rebuild linear in the live key count: the assignment history of keys that
// deregistered long ago grows with the chain's age, and driving the ranked
// input from the accounts means that history is never read.
func rewardLiveStakeCredentialQuery(
	fromRunningTotals bool,
	keys stakeKeyRange,
) (string, []any) {
	var args []any
	inRange := func(tagColumn, keyColumn string) string {
		clause, clauseArgs := keys.predicate(tagColumn, keyColumn)
		args = append(args, clauseArgs...)
		return clause
	}
	assignments := func(table, alias string) string {
		return `
            SELECT a.credential_tag, a.staking_key,
                   ` + alias + `.pool_key_hash, ` + alias + `.added_slot,
                   COALESCE(tx.block_index, 0) AS block_index,
                   COALESCE(c.cert_index, 0) AS cert_index
            FROM account a
            JOIN ` + table + ` ` + alias + `
              ON ` + alias + `.credential_tag = a.credential_tag
             AND ` + alias + `.staking_key = a.staking_key
            LEFT JOIN certs c ON c.id = ` + alias + `.certificate_id
            LEFT JOIN "transaction" tx ON tx.id = c.transaction_id
            WHERE a.active = TRUE
              AND a.pool IS NOT NULL
              AND ` + inRange("a.credential_tag", "a.staking_key")
	}
	// Placeholders are consumed in document order, so the pieces are built in
	// the order they appear in the statement.
	history := assignments("stake_delegation", "sd") + `
            UNION ALL` + assignments("stake_registration_delegation", "srd") + `
            UNION ALL` + assignments("stake_vote_delegation", "svd") + `
            UNION ALL` + assignments("stake_vote_registration_delegation", "svrd")
	creds := rewardLiveStakeKeySource(
		inRange("credential_tag", "staking_key"),
		inRange("credential_tag", "staking_key"),
	)
	utxoStakeSelect := "NULL"
	runningTotalJoin := ""
	if fromRunningTotals {
		utxoStakeSelect = "reward_live_stake.utxo_stake"
		runningTotalJoin = `
LEFT JOIN reward_live_stake
  ON reward_live_stake.credential_tag = creds.credential_tag
 AND reward_live_stake.staking_key = creds.staking_key`
	}
	return `
WITH latest_delegation AS (
    SELECT credential_tag, staking_key, pool_key_hash, added_slot,
           block_index, cert_index
    FROM (
        SELECT delegation.*,
               ROW_NUMBER() OVER (
                   PARTITION BY credential_tag, staking_key
                   ORDER BY added_slot DESC, block_index DESC, cert_index DESC
               ) AS rn
        FROM (` + history + `
        ) delegation
    ) ranked_delegation
    WHERE rn = 1
)
SELECT creds.credential_tag, creds.staking_key,
       CASE WHEN account.active = TRUE THEN account.pool ELSE NULL END,
       account.reward, account.active, account.added_slot,
       latest_delegation.added_slot,
       latest_delegation.block_index,
       latest_delegation.cert_index,
       ` + utxoStakeSelect + `
FROM (` + creds + `) creds
LEFT JOIN account
  ON account.credential_tag = creds.credential_tag
 AND account.staking_key = creds.staking_key
LEFT JOIN latest_delegation
  ON latest_delegation.credential_tag = account.credential_tag
 AND latest_delegation.staking_key = account.staking_key
 AND latest_delegation.pool_key_hash = account.pool` + runningTotalJoin, args
}

func (s *Store) rebuildRewardLiveStake(
	slot uint64,
	txn types.Txn,
	fromRunningTotals bool,
) error {
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			rebuildStart := time.Now()
			if !fromRunningTotals {
				if _, err := db.ExecContext(
					ctx,
					"DELETE FROM reward_live_stake",
				); err != nil {
					return fmt.Errorf("clear reward live stake: %w", err)
				}
			} else {
				// A fresh Mithril import normally starts with an empty aggregate, but
				// clearing orphan rows keeps the fast path correct if a prior run left
				// aggregate state behind while its sync marker was lost.
				if _, err := db.ExecContext(ctx, `
DELETE FROM reward_live_stake
WHERE NOT EXISTS (
    SELECT 1 FROM account
    WHERE account.credential_tag = reward_live_stake.credential_tag
      AND account.staking_key = reward_live_stake.staking_key
)
AND NOT EXISTS (
    SELECT 1 FROM utxo
    WHERE utxo.deleted_slot = 0
      AND utxo.staking_key IS NOT NULL
      AND LENGTH(utxo.staking_key) > 0
      AND utxo.credential_tag = reward_live_stake.credential_tag
      AND utxo.staking_key = reward_live_stake.staking_key
)`); err != nil {
					return fmt.Errorf("clear orphan reward live stake: %w", err)
				}
				if err := s.verifyRewardLiveStakeRunningTotals(
					ctx,
					db,
				); err != nil {
					return err
				}
			}
			total, err := countRewardLiveStakeKeys(ctx, db)
			if err != nil {
				return err
			}
			s.logger.Info(
				"reward live stake rebuild: started",
				"stake_keys", total,
				"from_running_totals", fromRunningTotals,
			)
			upserter := s.newRewardLiveStakeUpserter(db)
			defer upserter.Close()
			var processed int64
			lastProgress := time.Now()
			var lo *stakeKeyBound
			for {
				if err := ctx.Err(); err != nil {
					return err
				}
				hi, err := nextRewardLiveStakeBatchEnd(
					ctx, db, lo, s.rewardLiveStakeBatch(),
				)
				if err != nil {
					return err
				}
				rows, err := s.rebuildRewardLiveStakeRange(
					ctx,
					db,
					upserter,
					stakeKeyRange{lo: lo, hi: hi},
					fromRunningTotals,
					slotValue,
				)
				if err != nil {
					return err
				}
				processed += int64(rows)
				if hi == nil {
					break
				}
				lo = hi
				if time.Since(lastProgress) >= rewardLiveStakeProgressInterval {
					lastProgress = time.Now()
					elapsed := time.Since(rebuildStart)
					remaining := time.Duration(0)
					if processed > 0 && total > processed {
						remaining = time.Duration(
							float64(elapsed) *
								float64(total-processed) / float64(processed),
						)
					}
					s.logger.Info(
						"reward live stake rebuild: progress",
						"stake_keys_done", processed,
						"stake_keys", total,
						"elapsed", elapsed.Round(time.Second).String(),
						"eta", remaining.Round(time.Second).String(),
					)
				}
			}
			s.logger.Info(
				"reward live stake rebuild: complete",
				"rows", processed,
				"duration", time.Since(rebuildStart).String(),
			)
			return nil
		},
	)
}

type rewardLiveStakeCredential struct {
	tag             uint8
	key, pool       []byte
	reward          sql.NullString
	active          sql.NullBool
	addedSlot       sql.NullInt64
	delegationSlot  sql.NullInt64
	delegationBlock sql.NullInt64
	delegationCert  sql.NullInt64
	utxoStake       sql.NullString
}

// rebuildRewardLiveStakeRange rebuilds the aggregate rows for one key range
// and returns how many it wrote.
func (s *Store) rebuildRewardLiveStakeRange(
	ctx context.Context,
	db queryer,
	upserter *rewardLiveStakeUpserter,
	keys stakeKeyRange,
	fromRunningTotals bool,
	slotValue int64,
) (int, error) {
	var utxoStakes map[string]uint64
	if !fromRunningTotals {
		var err error
		utxoStakes, err = sumRewardLiveStakeUtxos(ctx, db, keys)
		if err != nil {
			return 0, err
		}
	}
	query, args := rewardLiveStakeCredentialQuery(fromRunningTotals, keys)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("load reward live stake credentials: %w", err)
	}
	// Materialize the credential cursor before issuing any upserts. On
	// PostgreSQL a query keeps the transaction's sole connection busy until
	// its rows are closed; attempting the first INSERT while this cursor is
	// open therefore blocks/fails with a connection error.
	credentials := make([]rewardLiveStakeCredential, 0, s.rewardLiveStakeBatch())
	for rows.Next() {
		var credential rewardLiveStakeCredential
		if err := rows.Scan(&credential.tag, &credential.key, &credential.pool,
			&credential.reward, &credential.active, &credential.addedSlot,
			&credential.delegationSlot, &credential.delegationBlock,
			&credential.delegationCert,
			&credential.utxoStake,
		); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan reward live stake credential: %w", err)
		}
		credentials = append(credentials, credential)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("iterate reward live stake credentials: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close reward live stake credentials: %w", err)
	}
	values := make([]rewardLiveStakeRow, 0, len(credentials))
	for _, credential := range credentials {
		tag := credential.tag
		key := credential.key
		pool := credential.pool
		utxoStake := utxoStakes[rewardLiveStakeKey(tag, key)]
		if fromRunningTotals && credential.utxoStake.Valid {
			utxoStake, err = parseUint64(
				"reward live stake running UTxO total",
				credential.utxoStake.String,
			)
			if err != nil {
				return 0, err
			}
		}
		rewardStake := uint64(0)
		if credential.reward.Valid && credential.reward.String != "" {
			rewardStake, err = parseUint64(
				"reward live stake reward",
				credential.reward.String,
			)
			if err != nil {
				return 0, err
			}
		}
		if ^uint64(0)-utxoStake < rewardStake {
			return 0, fmt.Errorf(
				"reward live stake overflow for credential %d:%x",
				tag,
				key,
			)
		}
		total := utxoStake + rewardStake
		registered := credential.active.Valid && credential.active.Bool
		if !registered {
			pool = nil
		}
		delegSlot := int64(0)
		blockIndex := int64(0)
		certIndex := int64(0)
		if registered && len(pool) > 0 {
			if credential.delegationSlot.Valid {
				delegSlot = credential.delegationSlot.Int64
			} else if credential.addedSlot.Valid {
				delegSlot = credential.addedSlot.Int64
			}
			if credential.delegationBlock.Valid {
				blockIndex = credential.delegationBlock.Int64
			}
			if credential.delegationCert.Valid {
				certIndex = credential.delegationCert.Int64
			}
		}
		values = append(values, rewardLiveStakeRow{
			tag:             tag,
			key:             key,
			pool:            pool,
			utxoStake:       utxoStake,
			rewardStake:     rewardStake,
			totalStake:      total,
			registered:      registered,
			delegationSlot:  delegSlot,
			delegationBlock: blockIndex,
			delegationCert:  certIndex,
		})
	}
	if err := upserter.upsert(ctx, values, slotValue); err != nil {
		return 0, err
	}
	return len(values), nil
}

func rewardLiveStakeKey(tag uint8, key []byte) string {
	return string(append([]byte{tag}, key...))
}

// sumRewardLiveStakeUtxos totals the live UTxO stake of every key in the
// range for the authoritative rebuild. Amount arithmetic stays in Go: SQL
// INTEGER is signed and cannot represent every valid lovelace value.
func sumRewardLiveStakeUtxos(
	ctx context.Context,
	db queryer,
	keys stakeKeyRange,
) (map[string]uint64, error) {
	inRange, args := keys.predicate("credential_tag", "staking_key")
	utxoRows, err := db.QueryContext(ctx,
		`SELECT credential_tag, staking_key, amount FROM utxo
WHERE `+liveUtxoStakeKeyFilter+`
  AND `+inRange, args...)
	if err != nil {
		return nil, fmt.Errorf("load reward live stake UTxOs: %w", err)
	}
	defer utxoRows.Close()
	utxoStakes := make(map[string]uint64)
	for utxoRows.Next() {
		var tag uint8
		var key []byte
		var raw sql.NullString
		if err := utxoRows.Scan(&tag, &key, &raw); err != nil {
			return nil, fmt.Errorf("scan reward live stake UTxO: %w", err)
		}
		if !raw.Valid || raw.String == "" {
			continue
		}
		amount, err := parseUint64("reward live stake UTxO amount", raw.String)
		if err != nil {
			return nil, err
		}
		ref := rewardLiveStakeKey(tag, key)
		if ^uint64(0)-utxoStakes[ref] < amount {
			return nil, fmt.Errorf(
				"reward live stake UTxO overflow for credential %d:%x",
				tag,
				key,
			)
		}
		utxoStakes[ref] += amount
	}
	if err := utxoRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reward live stake UTxOs: %w", err)
	}
	if err := utxoRows.Close(); err != nil {
		return nil, fmt.Errorf("close reward live stake UTxOs: %w", err)
	}
	return utxoStakes, nil
}

// rewardLiveStakeRow is the materialized form of one canonical credential
// used by RebuildRewardLiveStake. Keeping this separate from the scan row
// allows the rebuild to issue bounded multi-row upserts instead of one round
// trip per credential.
type rewardLiveStakeRow struct {
	tag                             uint8
	key, pool                       []byte
	utxoStake, rewardStake          uint64
	totalStake                      uint64
	registered                      bool
	delegationSlot, delegationBlock int64
	delegationCert                  int64
}

// rewardLiveStakeUpsertRows is how many rows one rebuild upsert statement
// carries, capped below by the dialect's parameter limit. modernc.org/sqlite
// binds each placeholder by scanning the whole argument list, so a
// statement's bind cost is quadratic in its parameter count: measured over
// 200,000 upserts, 83 rows per statement (SQLite's 999-parameter limit)
// costs about 1.4x what 32 does, and 2,730 costs 20x. A fixed size also lets
// one prepared statement serve every full chunk instead of re-parsing a
// freshly built statement each time, while PostgreSQL and MySQL still
// amortize a round trip over dozens of rows.
const rewardLiveStakeUpsertRows = 32

const rewardLiveStakeUpsertValuesPerRow = 12

func rewardLiveStakeUpsertStatement(rows int) string {
	placeholders := make([]string, rows)
	for index := range placeholders {
		placeholders[index] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	}
	return `INSERT INTO reward_live_stake (credential_tag, staking_key,
 pool_key_hash, utxo_stake, reward_stake, total_stake, registered,
 pool_delegation_slot, pool_delegation_block_index, pool_delegation_cert_index,
 updated_slot, calculation_version)
VALUES ` + strings.Join(placeholders, ", ") + `
ON CONFLICT (credential_tag, staking_key) DO UPDATE SET
 pool_key_hash = excluded.pool_key_hash, utxo_stake = excluded.utxo_stake,
 reward_stake = excluded.reward_stake, total_stake = excluded.total_stake,
 registered = excluded.registered, pool_delegation_slot = excluded.pool_delegation_slot,
 pool_delegation_block_index = excluded.pool_delegation_block_index,
 pool_delegation_cert_index = excluded.pool_delegation_cert_index,
 updated_slot = excluded.updated_slot, calculation_version = excluded.calculation_version`
}

// rewardLiveStakeUpserter writes rebuild rows through one statement prepared
// for the whole rebuild, falling back to a one-off statement only for each
// call's trailing partial chunk.
type rewardLiveStakeUpserter struct {
	db          queryer
	rowsPerStmt int
	full        *sql.Stmt
}

func (s *Store) newRewardLiveStakeUpserter(db queryer) *rewardLiveStakeUpserter {
	return &rewardLiveStakeUpserter{
		db: db,
		rowsPerStmt: max(1, min(
			rewardLiveStakeUpsertRows,
			s.dialect.ParameterLimit()/rewardLiveStakeUpsertValuesPerRow,
		)),
	}
}

func (u *rewardLiveStakeUpserter) upsert(
	ctx context.Context,
	rows []rewardLiveStakeRow,
	updatedSlot int64,
) error {
	for start := 0; start < len(rows); start += u.rowsPerStmt {
		chunk := rows[start:min(start+u.rowsPerStmt, len(rows))]
		args := make([]any, 0, len(chunk)*rewardLiveStakeUpsertValuesPerRow)
		for _, row := range chunk {
			args = append(args,
				row.tag,
				row.key,
				row.pool,
				decimalUint64(types.Uint64(row.utxoStake)),
				decimalUint64(types.Uint64(row.rewardStake)),
				decimalUint64(types.Uint64(row.totalStake)),
				row.registered,
				row.delegationSlot,
				row.delegationBlock,
				row.delegationCert,
				updatedSlot,
				models.RewardStakeCalculationVersion,
			)
		}
		var err error
		if len(chunk) == u.rowsPerStmt {
			if u.full == nil {
				u.full, err = u.db.PrepareContext(
					ctx,
					rewardLiveStakeUpsertStatement(u.rowsPerStmt),
				)
				if err != nil {
					return fmt.Errorf("prepare reward live stake upsert: %w", err)
				}
			}
			_, err = u.full.ExecContext(ctx, args...)
		} else {
			_, err = u.db.ExecContext(
				ctx,
				rewardLiveStakeUpsertStatement(len(chunk)),
				args...,
			)
		}
		if err != nil {
			return fmt.Errorf("populate reward live stake: %w", err)
		}
	}
	return nil
}

func (u *rewardLiveStakeUpserter) Close() error {
	if u.full == nil {
		return nil
	}
	return u.full.Close()
}

func (s *Store) RewardLiveStakeNeedsBackfill(
	txn types.Txn,
) (bool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return false, fmt.Errorf(
			"reward live stake needs backfill: resolve db: %w",
			err,
		)
	}
	type credentialKey struct {
		tag uint8
		key string
	}
	utxoStakes := make(map[credentialKey]uint64)
	utxoRows, err := db.QueryContext(
		ctx,
		`SELECT credential_tag, staking_key, amount FROM utxo WHERE deleted_slot = 0 AND staking_key IS NOT NULL AND LENGTH(staking_key) > 0`,
	)
	if err != nil {
		return false, fmt.Errorf("load reward live stake UTxOs: %w", err)
	}
	for utxoRows.Next() {
		var tag uint8
		var key []byte
		var raw sql.NullString
		if err := utxoRows.Scan(&tag, &key, &raw); err != nil {
			utxoRows.Close()
			return false, err
		}
		if !raw.Valid || raw.String == "" {
			continue
		}
		value, err := parseUint64("reward live stake UTxO amount", raw.String)
		if err != nil {
			utxoRows.Close()
			return false, err
		}
		ref := credentialKey{tag: tag, key: string(key)}
		if ^uint64(0)-utxoStakes[ref] < value {
			utxoRows.Close()
			return false, errors.New("reward live stake UTxO overflow")
		}
		utxoStakes[ref] += value
	}
	if err := utxoRows.Err(); err != nil {
		utxoRows.Close()
		return false, err
	}
	if err := utxoRows.Close(); err != nil {
		return false, err
	}
	rows, err := db.QueryContext(ctx, `
SELECT canonical.credential_tag, canonical.staking_key,
       account.reward, account.active, account.pool,
       reward_live_stake.id, reward_live_stake.calculation_version,
       reward_live_stake.utxo_stake, reward_live_stake.reward_stake,
       reward_live_stake.total_stake, reward_live_stake.registered,
       reward_live_stake.pool_key_hash
FROM (
  SELECT credential_tag, staking_key FROM account WHERE LENGTH(staking_key) > 0
  UNION SELECT credential_tag, staking_key FROM utxo
    WHERE deleted_slot = 0 AND staking_key IS NOT NULL AND LENGTH(staking_key) > 0
) canonical
LEFT JOIN account ON account.credential_tag = canonical.credential_tag
 AND account.staking_key = canonical.staking_key
LEFT JOIN reward_live_stake ON reward_live_stake.credential_tag = canonical.credential_tag
 AND reward_live_stake.staking_key = canonical.staking_key`)
	if err != nil {
		return false, fmt.Errorf("load reward live stake consistency: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tag uint8
		var key, pool, storedPool []byte
		var reward, storedUtxo, storedReward, storedTotal sql.NullString
		var active, registered sql.NullBool
		var id, version sql.NullInt64
		if err := rows.Scan(&tag, &key, &reward, &active, &pool, &id, &version, &storedUtxo, &storedReward, &storedTotal, &registered, &storedPool); err != nil {
			return false, err
		}
		ref := credentialKey{tag: tag, key: string(key)}
		utxoStake := utxoStakes[ref]
		rewardStake := uint64(0)
		if reward.Valid && reward.String != "" {
			rewardStake, err = parseUint64(
				"reward live stake reward",
				reward.String,
			)
			if err != nil {
				return false, err
			}
		}
		if ^uint64(0)-utxoStake < rewardStake {
			return false, errors.New("reward live stake overflow")
		}
		total := utxoStake + rewardStake
		if !id.Valid || !version.Valid ||
			uint64(
				version.Int64,
			) != uint64(
				models.RewardStakeCalculationVersion,
			) ||
			!storedUtxo.Valid ||
			!storedReward.Valid ||
			!storedTotal.Valid {
			return true, nil
		}
		storedUtxoValue, err := parseUint64(
			"stored UTxO stake",
			storedUtxo.String,
		)
		if err != nil {
			return false, err
		}
		storedRewardValue, err := parseUint64(
			"stored reward stake",
			storedReward.String,
		)
		if err != nil {
			return false, err
		}
		storedTotalValue, err := parseUint64(
			"stored total stake",
			storedTotal.String,
		)
		if err != nil {
			return false, err
		}
		if storedUtxoValue != utxoStake || storedRewardValue != rewardStake ||
			storedTotalValue != total ||
			registered.Bool != active.Bool {
			return true, nil
		}
		if active.Bool && len(pool) > 0 && string(pool) != string(storedPool) {
			return true, nil
		}
		if (!active.Bool || len(pool) == 0) && len(storedPool) > 0 {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	var orphan bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM reward_live_stake r WHERE NOT EXISTS (SELECT 1 FROM account a WHERE a.credential_tag = r.credential_tag AND a.staking_key = r.staking_key) AND NOT EXISTS (SELECT 1 FROM utxo u WHERE u.credential_tag = r.credential_tag AND u.staking_key = r.staking_key AND u.deleted_slot = 0))`).Scan(&orphan); err != nil {
		return false, err
	}
	return orphan, nil
}

func (s *Store) StaleConsensusStakeSnapshotsExist(
	txn types.Txn,
) (bool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return false, fmt.Errorf(
			"stale consensus stake snapshots: resolve db: %w",
			err,
		)
	}
	var stale bool
	// The reward_snapshot clause deliberately does not restrict to
	// authoritative rows: a non-authoritative fallback row (written by
	// captureMarkSnapshot) is a real source for reward calculation whenever
	// no authoritative row has been captured yet, so it must fail this gate
	// on its own version rather than rely on authoritativeMarkRewardSnapshotExists
	// separately rejecting a version mismatch when the fallback is consulted
	// (dingo #4026).
	err = db.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1 FROM pool_stake_snapshot
    WHERE snapshot_type IN ('mark', 'set', 'go')
      AND calculation_version <> ?
) OR EXISTS (
    SELECT 1 FROM reward_snapshot
    WHERE snapshot_type = 'mark'
      AND calculation_version <> ?
)`,
		models.RewardStakeCalculationVersion,
		models.RewardStakeCalculationVersion,
	).Scan(&stale)
	if err != nil {
		return false, fmt.Errorf(
			"checking stake snapshot provenance: %w",
			err,
		)
	}
	return stale, nil
}

// StaleConsensusStakeSnapshotEpochs is diagnostics only, for the operator-
// facing error StaleConsensusStakeSnapshotsExist gates on; it is not itself
// part of the fail-closed check.
func (s *Store) StaleConsensusStakeSnapshotEpochs(
	txn types.Txn,
) ([]uint64, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"stale consensus stake snapshot epochs: resolve db: %w",
			err,
		)
	}
	rows, err := db.QueryContext(ctx, `
SELECT epoch FROM pool_stake_snapshot
WHERE snapshot_type IN ('mark', 'set', 'go') AND calculation_version <> ?
UNION
SELECT epoch FROM reward_snapshot
WHERE snapshot_type = 'mark' AND calculation_version <> ?
ORDER BY epoch`,
		models.RewardStakeCalculationVersion,
		models.RewardStakeCalculationVersion,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"listing stale stake snapshot epochs: %w",
			err,
		)
	}
	defer rows.Close()
	var epochs []uint64
	for rows.Next() {
		var epoch uint64
		if err := rows.Scan(&epoch); err != nil {
			return nil, err
		}
		epochs = append(epochs, epoch)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return epochs, nil
}

func (s *Store) GetLiveStakeInputsForPools(
	poolKeyHashes [][]byte,
	expiryEpoch uint64,
	txn types.Txn,
) ([]*models.RewardStakeInput, error) {
	if len(poolKeyHashes) == 0 {
		return nil, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetLiveStakeInputsForPools: resolve db: %w",
			err,
		)
	}
	poolKeyHashes = dedupeByteSlices(poolKeyHashes)
	chunkSize := s.dialect.ParameterLimit()
	if expiryEpoch > 0 {
		chunkSize--
	}
	ret := make([]*models.RewardStakeInput, 0)
	for start := 0; start < len(poolKeyHashes); start += chunkSize {
		end := min(start+chunkSize, len(poolKeyHashes))
		chunk := poolKeyHashes[start:end]
		args := make([]any, 0, len(chunk)+1)
		for i := range chunk {
			args = append(args, chunk[i])
		}
		join := ""
		expiry := ""
		if expiryEpoch > 0 {
			join = `
LEFT JOIN account acct
  ON acct.credential_tag = rls.credential_tag
 AND acct.staking_key = rls.staking_key`
			expiry = `
  AND (acct.expiration_epoch = 0
       OR acct.expiration_epoch >= ?
       OR acct.expiration_epoch IS NULL)`
			args = append(args, expiryEpoch)
		}
		query := `
SELECT rls.pool_key_hash, rls.staking_key, rls.credential_tag,
       rls.total_stake
FROM reward_live_stake rls` + join + `
WHERE rls.pool_key_hash IN (` + bindPlaceholders(len(chunk)) + `)
  AND rls.registered = TRUE` + expiry + `
ORDER BY rls.pool_key_hash ASC, rls.credential_tag ASC,
         rls.staking_key ASC`
		rows, err := db.QueryContext(
			ctx,
			s.dialect.Rebind(query),
			args...,
		)
		if err != nil {
			return nil, fmt.Errorf("GetLiveStakeInputsForPools: %w", err)
		}
		for rows.Next() {
			var item models.RewardStakeInput
			var credentialTag int64
			var stake string
			if err := rows.Scan(
				&item.PoolKeyHash,
				&item.StakingKey,
				&credentialTag,
				&stake,
			); err != nil {
				rows.Close()
				return nil, err
			}
			value, err := parseUint64("live stake", stake)
			if err != nil {
				rows.Close()
				return nil, err
			}
			item.CredentialTag = uint8(credentialTag)
			item.Stake = types.Uint64(value)
			item.Registered = true
			ret = append(ret, &item)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func dedupeByteSlices(values [][]byte) [][]byte {
	if len(values) <= 1 {
		return values
	}
	ret := make([][]byte, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for i := range values {
		key := string(values[i])
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		ret = append(ret, values[i])
	}
	return ret
}

// GetDelegatedPoolKeyHashes returns every pool key hash the live reward stake
// aggregate currently attributes stake to, whether or not that pool is still
// registered.
//
// It exists for the sigma_a denominator. cardano-ledger's ssTotalActiveStake
// sums every registered credential holding a delegation, without consulting
// the stake-pool set (Cardano.Ledger.State.SnapShots.mkSnapShot over
// resolveInstantStake), so a snapshot whose stake is enumerated from the
// active pool set alone silently drops the stake of any credential whose pool
// is absent from it -- which raises sigma_a for every surviving pool and
// under-credits every reward on the node by that stake's share (dingo #4660,
// the same failure #3969 and #4025 fixed on the exclusion side). Unioning this
// set into the one the distribution is fetched for restores the ledger's
// credential-first denominator while leaving which pools earn rewards alone.
//
// The result is deliberately a superset: it applies no registration or expiry
// predicate, because those are applied by the stake fetch this feeds, and a
// pool with no qualifying credential simply contributes no rows there.
func (s *Store) GetDelegatedPoolKeyHashes(
	txn types.Txn,
) ([][]byte, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetDelegatedPoolKeyHashes: resolve db: %w",
			err,
		)
	}
	rows, err := db.QueryContext(ctx, `
SELECT DISTINCT pool_key_hash
FROM reward_live_stake
WHERE pool_key_hash IS NOT NULL AND LENGTH(pool_key_hash) > 0`)
	if err != nil {
		return nil, fmt.Errorf("GetDelegatedPoolKeyHashes: %w", err)
	}
	defer rows.Close()
	ret := [][]byte{}
	for rows.Next() {
		var hash []byte
		if err := rows.Scan(&hash); err != nil {
			return nil, err
		}
		ret = append(ret, hash)
	}
	return ret, rows.Err()
}
