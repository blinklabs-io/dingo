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

// Package leioskeyepoch derives the epoch a pool's BLS key was registered in
// from the registration and retirement rows already persisted for the pool.
package leioskeyepoch

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
)

// Querier is the subset of database/sql the derivation reads through. The
// statements use ? placeholders and the dialect-neutral SQL the sqlstore
// queryer translates.
type Querier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// Position is a pool certificate's position on the chain: the slot it was
// added at, the block index of its transaction, and its index within that
// transaction's certificate sequence.
type Position struct {
	Slot       uint64
	BlockIndex uint64
	CertIndex  uint64
}

func (p Position) before(other Position) bool {
	if p.Slot != other.Slot {
		return p.Slot < other.Slot
	}
	if p.BlockIndex != other.BlockIndex {
		return p.BlockIndex < other.BlockIndex
	}
	return p.CertIndex < other.CertIndex
}

// unsigned converts a SQL INTEGER to the unsigned domain it stores. A negative
// value can only come from a row altered outside normal writes.
func unsigned(value int64) (uint64, error) {
	if value < 0 {
		return 0, fmt.Errorf("negative unsigned SQL value %d", value)
	}
	return uint64(value), nil
}

// GenesisEpoch is the epoch the ledger registers genesis pools' BLS
// keys in: mkStakePoolState (nesEL nes) SNothing, with an initial nesEL of
// epoch 0 (cardano-ledger 9df9f98c, Shelley/Transition.hs:462 initialEpochNo and
// the staking injection using nesELL at lines 543 and 561).
const GenesisEpoch uint64 = 0

// keyState is the BLS key a pool holds and the epoch it was registered in,
// which is the ledger's BlsKeyState. A nil public key is a pool without a key.
// A keyed state with a nil epoch has an epoch the database cannot establish:
// ageUnknown marks a key imported without its age, and its absence a slot the
// epoch table could not place.
type keyState struct {
	public     []byte
	proof      []byte
	epoch      *uint64
	ageUnknown bool
}

func (k keyState) sameKey(public, proof []byte) bool {
	return len(k.public) > 0 &&
		bytes.Equal(k.public, public) &&
		bytes.Equal(k.proof, proof)
}

// registrationRow is one persisted pool_registration row.
type registrationRow struct {
	position          Position
	executionSlot     uint64
	snapshot          bool
	registrationIndex uint64
	stored            keyState
}

// retirementRow is one persisted pool_retirement row.
type retirementRow struct {
	position Position
	epoch    uint64
}

// after reports whether the retirement follows the registration on the chain.
// A registration and a retirement at the same position, which only a snapshot
// import's registration and its tombstone share, resolve to the retirement
// last, as the deposit bookkeeping does.
func (r retirementRow) after(registration registrationRow) bool {
	return !r.position.before(registration.position)
}

// Registration derives the leios_key_registration_epoch and
// leios_key_registration_age_unknown columns for the pool registration
// certificate being written at `at`, which carries the BLS key public/proof.
//
// It follows cardano-ledger's Dijkstra rules (IntersectMBO/cardano-ledger at
// 9df9f98c; paths below are under eras/dijkstra/impl/src/Cardano/Ledger/
// Dijkstra/ unless they name another package). POOL (Rules/Pool.hs:261, the
// not-registered branch) builds a new pool's state with mkStakePoolState
// cEpoch SNothing, so a key registered by a first registration is registered
// in the epoch of the transaction. POOL's re-registration branch only stages
// the parameters in psFutureStakePoolParams, replacing any earlier staged
// ones, and cancels a pending retirement. POOLREAP (Rules/PoolReap.hs:130)
// adopts them at the next epoch boundary through mkStakePoolState e (current
// BLS state) (libs/cardano-ledger-core/src/Cardano/Ledger/State/
// StakePool.hs:288): a key equal to the active one keeps its registration
// epoch, and any other key, including one added to a pool that had none, is
// registered in e, the epoch after the transaction's. BlsKey equality covers
// the public key and the possession proof. A retirement naming epoch e reaps
// the pool at the boundary into e, after the staged parameters are adopted.
// Dijkstra has no NEWEPOCH rule of its own: Era.hs:202 sets EraRule
// "NEWEPOCH" to Conway's, which on an epoch change runs EPOCH
// (eras/conway/impl/src/Cardano/Ledger/Conway/Rules/NewEpoch.hs:153-165), and
// Dijkstra's EPOCH calls POOLREAP (Rules/Epoch.hs:171).
//
// Every keyed row already stores the epoch its key holds once the row is in
// effect, so the result only needs the key state active in the transaction's
// epoch, which is a stored row: the current parameters of the pool's latest
// snapshot import in that epoch when there is one, because an import
// replaces the pool state where it lands, and otherwise the pool's latest
// registration before that epoch, unless a retirement reaped the pool before
// it or the pool was registered afresh within it, when it is the epoch's
// first registration. A constant number of statements finds these rows.
// Reading the latest registration before the epoch is an index seek bounded
// by the closure rows of one block (latestRegistrationExecutedBefore). The
// lookup of the latest snapshot import in the epoch walks the pool's live
// rows added since the epoch started, and the retirement lookups read all of
// the pool's retirements, since no index orders either by position. Nothing is
// kept outside the rows, so a rollback that deletes rows leaves nothing to
// undo. A
// snapshot-imported registration is the state the snapshot carried, with its
// epoch, or without one when the snapshot had none; a key kept from it keeps
// the absence. A snapshot's staged parameters (registration index above
// zero) carry no epoch and are adopted against the snapshot's current
// parameters at the boundary after the import.
//
// Chain validity bounds what the rows can hold: POOL accepts a retirement only
// for a registered pool and only naming a later epoch, so the latest retirement
// after a registration is the one pending, and a pool is not registered afresh
// twice within one epoch.
//
// The returned epoch is nil with ageUnknown set when the registration keeps a
// key whose epoch is unknown, and nil with ageUnknown clear when an epoch
// cannot be placed because the epoch table does not cover a slot involved.
// Zero is never stored for an epoch that is not known.
func Registration(
	ctx context.Context,
	db Querier,
	poolID int64,
	at Position,
	executionSlot uint64,
	public []byte,
	proof []byte,
) (epoch *uint64, ageUnknown bool, err error) {
	current, start, ok, err := epochAtSlot(ctx, db, executionSlot)
	if err != nil || !ok {
		return nil, false, err
	}
	fresh := func() (*uint64, bool, error) {
		value := current
		return &value, false, nil
	}
	latest, ok, err := latestRegistration(ctx, db, poolID, at, nil)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return fresh()
	}
	retirement, ok, err := latestRetirement(ctx, db, poolID, at, nil)
	if err != nil {
		return nil, false, err
	}
	if ok && retirement.after(latest) && retirement.epoch <= current {
		return fresh()
	}

	var active keyState
	var placed bool
	if latest.executionSlot < start {
		active, placed, err = effectiveState(ctx, db, poolID, latest)
	} else {
		active, placed, err = activeWithinEpoch(
			ctx, db, poolID, at, start, current, latest,
		)
	}
	if err != nil || !placed {
		return nil, false, err
	}
	if !active.sameKey(public, proof) {
		value := current + 1
		return &value, false, nil
	}
	if active.epoch == nil {
		return nil, active.ageUnknown, nil
	}
	value := *active.epoch
	return &value, false, nil
}

// activeWithinEpoch resolves the key state active in epoch current when the
// pool already registered in it before `at`: the current parameters of the
// pool's latest snapshot import in the epoch when there is one, because an
// import replaces the pool state where it lands; the latest registration
// before the epoch otherwise, unless there is none or a retirement reaped
// the pool before the epoch, when the epoch's first registration registered
// the pool afresh.
func activeWithinEpoch(
	ctx context.Context,
	db Querier,
	poolID int64,
	at Position,
	start uint64,
	current uint64,
	latest registrationRow,
) (keyState, bool, error) {
	imported, ok, err := latestSnapshotCurrent(ctx, db, poolID, at, start)
	if err != nil {
		return keyState{}, false, err
	}
	if ok {
		return imported.stored, true, nil
	}
	first, ok, err := firstRegistrationFrom(ctx, db, poolID, at, start)
	if err != nil {
		return keyState{}, false, err
	}
	if !ok {
		// latest executed in the epoch, so it is the epoch's first
		// registration when no earlier row qualifies.
		first = latest
	}
	prior, ok, err := latestRegistration(ctx, db, poolID, at, &start)
	if err != nil {
		return keyState{}, false, err
	}
	if !ok {
		return effectiveState(ctx, db, poolID, first)
	}
	retirement, ok, err := latestRetirement(ctx, db, poolID, at, &start)
	if err != nil {
		return keyState{}, false, err
	}
	if ok && retirement.after(prior) && retirement.epoch <= current {
		return effectiveState(ctx, db, poolID, first)
	}
	return effectiveState(ctx, db, poolID, prior)
}

// effectiveState is the key state a registration row leaves the pool in once
// it is in effect. A live row stores it. A snapshot's staged parameters are
// adopted at the boundary after the import against the snapshot's current
// parameters at the same slot, and are the active state themselves when the
// snapshot carried no current parameters.
func effectiveState(
	ctx context.Context,
	db Querier,
	poolID int64,
	row registrationRow,
) (keyState, bool, error) {
	if !row.snapshot || row.registrationIndex == 0 {
		return row.stored, true, nil
	}
	base, ok, err := snapshotCurrent(ctx, db, poolID, row.position.Slot)
	if err != nil || !ok {
		return row.stored, err == nil, err
	}
	if base.sameKey(row.stored.public, row.stored.proof) {
		return base, true, nil
	}
	if len(row.stored.public) == 0 {
		return keyState{}, true, nil
	}
	imported, _, ok, err := epochAtSlot(ctx, db, row.executionSlot)
	if err != nil || !ok {
		return keyState{}, false, err
	}
	adopted := imported + 1
	return keyState{
		public: row.stored.public,
		proof:  row.stored.proof,
		epoch:  &adopted,
	}, true, nil
}

// executionSlotSQL resolves the slot a certificate's transaction executed in:
// its Leios closure slot when it has one, the slot it was added at otherwise.
const executionSlotSQL = `COALESCE((
    SELECT lc.slot FROM leios_transaction_context lc
    JOIN certs lc_cert ON lc_cert.transaction_id = lc.transaction_id
    WHERE lc_cert.id = pr.certificate_id), pr.added_slot)`

// registrationBlockIndex is the block index of a registration's transaction,
// zero for a synthesized row with no certificate.
const registrationBlockIndex = `COALESCE((
    SELECT tx.block_index FROM "transaction" tx WHERE tx.id = c.transaction_id
), 0)`

// registrationSelect lists the columns scanRegistration reads, aliased so a
// statement can wrap the select in a derived table.
const registrationSelect = `
SELECT pr.added_slot AS added_slot, ` + executionSlotSQL + ` AS execution_slot,
       ` + registrationBlockIndex + ` AS block_index,
       COALESCE(c.cert_index, 0) AS cert_index,
       COALESCE(pr.certificate_id, 0) AS certificate_id,
       pr.registration_index AS registration_index,
       pr.leios_key_public AS leios_key_public,
       pr.leios_key_possession_proof AS leios_key_possession_proof,
       pr.leios_key_registration_epoch AS leios_key_registration_epoch,
       pr.leios_key_registration_age_unknown AS leios_key_registration_age_unknown
FROM pool_registration pr
LEFT JOIN certs c ON c.id = pr.certificate_id
WHERE pr.pool_id = ?
`

const registrationColumns = registrationSelect + `  AND pr.added_slot <= ?
  AND `

// latestRegistration reads the pool's latest registration strictly before
// `at`, restricted to those executed before slot executedBefore when it is
// set. Unrestricted, the scan walks the (pool_id, added_slot) index backwards
// from `at` and stops at the first row, which qualifies.
func latestRegistration(
	ctx context.Context,
	db Querier,
	poolID int64,
	at Position,
	executedBefore *uint64,
) (registrationRow, bool, error) {
	if executedBefore != nil {
		return latestRegistrationExecutedBefore(
			ctx, db, poolID, at, *executedBefore,
		)
	}
	predicate, args := positionPredicate("pr", at, registrationBlockIndex)
	query := "-- name: LeiosKeyLatestRegistration" +
		registrationColumns + predicate + registrationOrder
	args = append([]any{poolID, at.Slot}, args...)
	return scanRegistration(db.QueryRowContext(ctx, query, args...))
}

// registrationOrder sorts registrations by chain position, latest first.
const registrationOrder = `
ORDER BY pr.added_slot DESC, ` + registrationBlockIndex + ` DESC,
         COALESCE(c.cert_index, 0) DESC, pr.registration_index DESC,
         pr.id DESC
LIMIT 1`

// latestRegistrationExecutedBefore reads the pool's latest registration
// strictly before `at` that executed before slot start, in work independent of
// how many registrations the pool made in the epoch that starts there.
//
// A transaction executes no later than the slot it was added at, so every row
// added before start qualifies and the latest of them is one seek on the
// (pool_id, added_slot) index. A row added at or after start qualifies only
// when its transaction carries a Leios execution slot before start. Only
// applyUntickedBoundaryClosure writes one (leios_transaction_context is written
// by recordTransactionLedgerContext alone, reached through
// SetTransactionLeiosClosureInContext), for the block that opens an epoch, and
// it executes the block's closure at the slot of the block's parent. Such a
// block is the first on the chain at or after start, so every row of the kind
// is added at one slot, the pool's earliest added at or after start. Those
// rows are read at that slot, so the work is bounded by one block's
// certificates for the pool. The two reads share one statement; the later
// qualifying row wins.
func latestRegistrationExecutedBefore(
	ctx context.Context,
	db Querier,
	poolID int64,
	at Position,
	start uint64,
) (registrationRow, bool, error) {
	// The seek's upper bound is its only range term on added_slot, so the
	// scan starts at the bound rather than at `at`.
	seekBefore := start
	if at.Slot < math.MaxUint64 && at.Slot+1 < seekBefore {
		seekBefore = at.Slot + 1
	}
	predicate, positionArgs := positionPredicate(
		"pr", at, registrationBlockIndex,
	)
	query := `-- name: LeiosKeyLatestRegistrationExecutedBefore
SELECT u.added_slot, u.execution_slot, u.block_index, u.cert_index,
       u.certificate_id, u.registration_index, u.leios_key_public,
       u.leios_key_possession_proof, u.leios_key_registration_epoch,
       u.leios_key_registration_age_unknown
FROM (
    SELECT 0 AS source, s.* FROM (` + registrationSelect + `
      AND pr.added_slot < ?
      AND ` + predicate + registrationOrder + `) s
    UNION ALL
    SELECT 1 AS source, b.* FROM (` + registrationSelect + `
      AND pr.added_slot = (
          SELECT MIN(p0.added_slot) FROM pool_registration p0
          WHERE p0.pool_id = ? AND p0.added_slot >= ?)
      AND ` + executionSlotSQL + ` < ?
      AND ` + predicate + registrationOrder + `) b
) u
ORDER BY u.source DESC
LIMIT 1`
	args := make([]any, 0, 2*len(positionArgs)+6)
	args = append(args, poolID, seekBefore)
	args = append(args, positionArgs...)
	args = append(args, poolID, poolID, start, start)
	args = append(args, positionArgs...)
	return scanRegistration(db.QueryRowContext(ctx, query, args...))
}

// firstRegistrationFrom reads the pool's first registration executed at or
// after slot start and strictly before `at`. A transaction executes no later
// than the slot it was added at, so only rows added from start on qualify.
func firstRegistrationFrom(
	ctx context.Context,
	db Querier,
	poolID int64,
	at Position,
	start uint64,
) (registrationRow, bool, error) {
	predicate, args := positionPredicate("pr", at, registrationBlockIndex)
	query := "-- name: LeiosKeyFirstRegistrationFrom" +
		registrationColumns + predicate + `
  AND pr.added_slot >= ?
  AND ` + executionSlotSQL + ` >= ?
ORDER BY pr.added_slot ASC, ` + registrationBlockIndex + ` ASC,
         COALESCE(c.cert_index, 0) ASC, pr.registration_index ASC,
         pr.id ASC
LIMIT 1`
	args = append([]any{poolID, at.Slot}, args...)
	args = append(args, start, start)
	return scanRegistration(db.QueryRowContext(ctx, query, args...))
}

// latestSnapshotCurrent reads the pool's latest snapshot-imported current
// parameters -- its registration without a certificate at index zero --
// executed at or after slot start and strictly before `at`. A snapshot row's
// execution slot is the slot it was added at, so the added-slot lower bound
// keeps the scan on the (pool_id, added_slot) index.
func latestSnapshotCurrent(
	ctx context.Context,
	db Querier,
	poolID int64,
	at Position,
	start uint64,
) (registrationRow, bool, error) {
	predicate, args := positionPredicate("pr", at, registrationBlockIndex)
	query := "-- name: LeiosKeyLatestSnapshotCurrent" +
		registrationColumns + predicate + `
  AND pr.added_slot >= ?
  AND COALESCE(pr.certificate_id, 0) = 0
  AND pr.registration_index = 0
ORDER BY pr.added_slot DESC, pr.id DESC
LIMIT 1`
	args = append([]any{poolID, at.Slot}, args...)
	args = append(args, start)
	return scanRegistration(db.QueryRowContext(ctx, query, args...))
}

func scanRegistration(row *sql.Row) (registrationRow, bool, error) {
	var (
		addedSlot, exec, blockIdx, certIdx int64
		certificateID, registrationIndex   int64
		public, proof                      []byte
		epoch                              sql.NullInt64
		ageUnknown                         bool
	)
	err := row.Scan(
		&addedSlot, &exec, &blockIdx, &certIdx, &certificateID,
		&registrationIndex, &public, &proof, &epoch, &ageUnknown,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return registrationRow{}, false, nil
	}
	if err != nil {
		return registrationRow{}, false, fmt.Errorf(
			"pool leios key epoch: read registration: %w", err,
		)
	}
	position, err := newPosition(addedSlot, blockIdx, certIdx)
	if err != nil {
		return registrationRow{}, false, err
	}
	executionSlot, err := unsigned(exec)
	if err != nil {
		return registrationRow{}, false, err
	}
	index, err := unsigned(registrationIndex)
	if err != nil {
		return registrationRow{}, false, err
	}
	stored, err := storedKeyState(public, proof, epoch, ageUnknown)
	if err != nil {
		return registrationRow{}, false, err
	}
	return registrationRow{
		position:          position,
		executionSlot:     executionSlot,
		snapshot:          certificateID == 0,
		registrationIndex: index,
		stored:            stored,
	}, true, nil
}

func storedKeyState(
	public, proof []byte,
	epoch sql.NullInt64,
	ageUnknown bool,
) (keyState, error) {
	if len(public) == 0 || len(proof) == 0 {
		return keyState{}, nil
	}
	state := keyState{public: public, proof: proof, ageUnknown: ageUnknown}
	if epoch.Valid {
		value, err := unsigned(epoch.Int64)
		if err != nil {
			return keyState{}, err
		}
		state.epoch = &value
	}
	return state, nil
}

// snapshotCurrent reads the current parameters a snapshot import wrote for the
// pool at addedSlot: its registration without a certificate at index zero.
func snapshotCurrent(
	ctx context.Context,
	db Querier,
	poolID int64,
	addedSlot uint64,
) (keyState, bool, error) {
	var (
		public, proof []byte
		epoch         sql.NullInt64
		ageUnknown    bool
	)
	err := db.QueryRowContext(ctx, `-- name: LeiosKeySnapshotCurrent
SELECT leios_key_public, leios_key_possession_proof,
       leios_key_registration_epoch, leios_key_registration_age_unknown
FROM pool_registration
WHERE pool_id = ?
  AND added_slot = ?
  AND COALESCE(certificate_id, 0) = 0
  AND registration_index = 0
LIMIT 1`,
		poolID, addedSlot,
	).Scan(&public, &proof, &epoch, &ageUnknown)
	if errors.Is(err, sql.ErrNoRows) {
		return keyState{}, false, nil
	}
	if err != nil {
		return keyState{}, false, fmt.Errorf(
			"pool leios key epoch: read snapshot registration: %w", err,
		)
	}
	state, err := storedKeyState(public, proof, epoch, ageUnknown)
	return state, err == nil, err
}

// latestRetirement reads the pool's latest retirement strictly before `at`,
// restricted to those executed before slot executedBefore when it is set.
func latestRetirement(
	ctx context.Context,
	db Querier,
	poolID int64,
	at Position,
	executedBefore *uint64,
) (retirementRow, bool, error) {
	predicate, args := positionPredicate("rt", at, "COALESCE(t.block_index, 0)")
	label := "LeiosKeyLatestRetirement"
	if executedBefore != nil {
		label += "ExecutedBefore"
	}
	query := "-- name: " + label + `
SELECT rt.epoch, rt.added_slot, COALESCE(t.block_index, 0),
       COALESCE(c.cert_index, 0)
FROM pool_retirement rt
LEFT JOIN certs c ON c.id = rt.certificate_id
LEFT JOIN "transaction" t ON t.id = c.transaction_id
WHERE rt.pool_id = ?
  AND rt.added_slot <= ?
  AND ` + predicate
	args = append([]any{poolID, at.Slot}, args...)
	if executedBefore != nil {
		query += `
  AND COALESCE((
      SELECT lc.slot FROM leios_transaction_context lc
      WHERE lc.transaction_id = c.transaction_id), rt.added_slot) < ?`
		args = append(args, *executedBefore)
	}
	query += `
ORDER BY rt.added_slot DESC, COALESCE(t.block_index, 0) DESC,
         COALESCE(c.cert_index, 0) DESC, rt.id DESC
LIMIT 1`
	var epoch, addedSlot, blockIdx, certIdx int64
	err := db.QueryRowContext(ctx, query, args...).Scan(
		&epoch, &addedSlot, &blockIdx, &certIdx,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return retirementRow{}, false, nil
	}
	if err != nil {
		return retirementRow{}, false, fmt.Errorf(
			"pool leios key epoch: read retirement: %w", err,
		)
	}
	position, err := newPosition(addedSlot, blockIdx, certIdx)
	if err != nil {
		return retirementRow{}, false, err
	}
	retireEpoch, err := unsigned(epoch)
	if err != nil {
		return retirementRow{}, false, err
	}
	return retirementRow{position: position, epoch: retireEpoch}, true, nil
}

func newPosition(slot, blockIndex, certIndex int64) (Position, error) {
	var (
		position Position
		err      error
	)
	if position.Slot, err = unsigned(slot); err != nil {
		return Position{}, err
	}
	if position.BlockIndex, err = unsigned(blockIndex); err != nil {
		return Position{}, err
	}
	if position.CertIndex, err = unsigned(certIndex); err != nil {
		return Position{}, err
	}
	return position, nil
}

// positionPredicate matches the pool certificate rows strictly before a
// position, and returns the bind arguments for it. The joined block and
// certificate indexes are NULL for a synthesized row with no certs row, which
// is why both sides COALESCE to zero.
func positionPredicate(
	table string,
	at Position,
	blockIndex string,
) (string, []any) {
	args := []any{
		at.Slot,
		at.Slot, at.BlockIndex,
		at.Slot, at.BlockIndex, at.CertIndex,
	}
	return `(
      ` + table + `.added_slot < ?
      OR (` + table + `.added_slot = ?
          AND ` + blockIndex + ` < ?)
      OR (` + table + `.added_slot = ?
          AND ` + blockIndex + ` = ?
          AND COALESCE(c.cert_index, 0) < ?)
  )`, args
}

// epochAtSlot resolves the persisted epoch containing slot and the slot it
// starts at. It reports resolved=false rather than an error when the epoch
// table cannot place the slot, because a pool registration write must not fail
// on missing epoch bookkeeping.
func epochAtSlot(
	ctx context.Context,
	db Querier,
	slot uint64,
) (epoch uint64, start uint64, resolved bool, err error) {
	var epochID, startSlot, length sql.NullInt64
	err = db.QueryRowContext(ctx, `-- name: LeiosKeyEpochAtSlot
SELECT epoch_id, start_slot, length_in_slots
FROM epoch
WHERE start_slot <= ?
ORDER BY start_slot DESC
LIMIT 1`,
		slot,
	).Scan(&epochID, &startSlot, &length)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, false, nil
	}
	if err != nil {
		return 0, 0, false, fmt.Errorf("epoch at slot %d: %w", slot, err)
	}
	if !epochID.Valid || !startSlot.Valid || !length.Valid {
		return 0, 0, false, nil
	}
	end, err := unsigned(startSlot.Int64 + length.Int64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("epoch at slot %d: %w", slot, err)
	}
	if slot >= end {
		return 0, 0, false, nil
	}
	if epoch, err = unsigned(epochID.Int64); err != nil {
		return 0, 0, false, err
	}
	if start, err = unsigned(startSlot.Int64); err != nil {
		return 0, 0, false, err
	}
	return epoch, start, true, nil
}
