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
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// pool_current_state holds one row per pool with certificate history and
// pool_current_event its events: the part of the history that answers
// GetPoolRegistrationsEffectiveForEpoch for any epoch start at or after the
// pool's horizon_slot: every registration and every
// retirement whose effective slot is at or after horizon_slot, plus the latest
// registration and the latest retirement below it, each under the keys the
// history ranking uses. Haskell's current parameters plus pending
// registration and retirement are the newest entries of the two lists; the
// older ones are what a lookup for an epoch that began before the newest
// events needs.
//
// The tables are derived and maintained, never checked on read.
// refreshPoolCurrentState recomputes a pool's rows from pool_registration and
// pool_retirement, and every statement that adds or removes one of those rows,
// or changes a key they are ranked by (the certificate's transaction context
// or block index), calls it for the pools it touched in the same transaction;
// TestPoolCurrentStateWritersRefresh pins the set of writers. A write to the
// history tables that does not refresh leaves the rows stale and nothing on
// the read path notices.

// poolCurrentHorizonEpochs is how many epochs before the newest one a row's
// horizon lies. A lookup for an epoch start older than the horizon ranks the
// history tables instead.
const poolCurrentHorizonEpochs = 8

const (
	poolCurrentKindRegistration = 0
	poolCurrentKindRetirement   = 1
)

const poolCurrentEffectiveSlotSQL = `COALESCE((SELECT lc.slot FROM leios_transaction_context lc JOIN certs lc_cert ON lc_cert.transaction_id = lc.transaction_id WHERE lc_cert.id = %[1]s.certificate_id), %[1]s.added_slot)`

func poolCurrentEffectiveSlot(alias string) string {
	return fmt.Sprintf(poolCurrentEffectiveSlotSQL, alias)
}

// poolCurrentEvent is one stored registration or retirement.
type poolCurrentEvent struct {
	id                uint
	effectiveSlot     uint64
	addedSlot         uint64
	synthetic         int64
	blockIndex        int64
	certIndex         int64
	registrationIndex int64
	// epoch is the epoch a retirement takes effect in.
	epoch uint64
}

// poolCurrentPool is the stored state of one pool. Both lists are in history
// ranking order, newest first.
type poolCurrentPool struct {
	horizon       uint64
	registrations []poolCurrentEvent
	retirements   []poolCurrentEvent
}

// registrationWins reports whether the registration outranks the retirement
// under the event ordering of the history ranking: effective slot, added slot,
// synthetic flag, block index, certificate index, then a retirement beats a
// registration of equal position.
func (e poolCurrentEvent) registrationWins(ret poolCurrentEvent) bool {
	if e.effectiveSlot != ret.effectiveSlot {
		return e.effectiveSlot > ret.effectiveSlot
	}
	if e.addedSlot != ret.addedSlot {
		return e.addedSlot > ret.addedSlot
	}
	if ret.synthetic != 0 {
		return false
	}
	if e.blockIndex != ret.blockIndex {
		return e.blockIndex > ret.blockIndex
	}
	if e.certIndex != ret.certIndex {
		return e.certIndex > ret.certIndex
	}
	return false
}

// registrationBefore reports whether e is the earlier registration under the
// ordering the history uses for a pool's first registration inside an epoch.
func (e poolCurrentEvent) registrationBefore(other poolCurrentEvent) bool {
	if e.addedSlot != other.addedSlot {
		return e.addedSlot < other.addedSlot
	}
	if e.blockIndex != other.blockIndex {
		return e.blockIndex < other.blockIndex
	}
	if e.certIndex != other.certIndex {
		return e.certIndex < other.certIndex
	}
	if e.registrationIndex != other.registrationIndex {
		return e.registrationIndex < other.registrationIndex
	}
	return e.id < other.id
}

// effective answers the epoch query for the pool. covered is false when the
// epoch starts before the row's horizon, which the stored events cannot
// place. Otherwise the stored lists hold every event at or after epochStart
// and the newest one before it, so the history's selection is made over the
// same candidates: the newest registration before epochStart competes with the
// newest retirement before epochStart (a retirement counts only when its
// epoch is at most endedEpoch); a pool left with no registration, or with the
// retirement ahead, takes its earliest registration in
// [epochStart, snapshotSlot].
func (p poolCurrentPool) effective(
	epochStart, endedEpoch, snapshotSlot uint64,
) (id uint, found bool, covered bool) {
	if epochStart < p.horizon {
		return 0, false, false
	}
	var before *poolCurrentEvent
	for i := range p.registrations {
		if p.registrations[i].effectiveSlot < epochStart {
			before = &p.registrations[i]
			break
		}
	}
	if before != nil {
		var retirement *poolCurrentEvent
		for i := range p.retirements {
			if p.retirements[i].effectiveSlot < epochStart {
				retirement = &p.retirements[i]
				break
			}
		}
		if retirement == nil || retirement.epoch > endedEpoch ||
			before.registrationWins(*retirement) {
			return before.id, true, true
		}
	}
	var first *poolCurrentEvent
	for i := range p.registrations {
		reg := &p.registrations[i]
		if reg.effectiveSlot < epochStart || reg.effectiveSlot > snapshotSlot {
			continue
		}
		if first == nil || reg.registrationBefore(*first) {
			first = reg
		}
	}
	if first == nil {
		return 0, false, true
	}
	return first.id, true, true
}

// PoolCurrentStateDB is the part of *sql.DB and *sql.Tx the refresh uses.
type PoolCurrentStateDB interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// RefreshPoolCurrentState recomputes the stored state of the pools with the
// given key hashes from their certificate history: the step every store
// writer runs after it changes that history. A caller that seeds pool
// certificates, or the keys they are ranked by, with raw SQL on SQLite must
// run it afterwards, as nothing on the read path repairs the rows.
func RefreshPoolCurrentState(
	ctx context.Context,
	db PoolCurrentStateDB,
	hashes [][]byte,
) error {
	return refreshPoolCurrentState(ctx, db, hashes)
}

// scanRows runs a query and hands each row to fn. The rows are closed before
// it returns, so the caller may issue the next statement on the same
// connection, which a server driver requires.
func scanRows(
	ctx context.Context,
	db PoolCurrentStateDB,
	query string,
	args []any,
	fn func(*sql.Rows) error,
) error {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := fn(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// poolCurrentHorizonSlot returns the start slot of the epoch
// poolCurrentHorizonEpochs before the newest one, or zero when the epoch table
// does not reach back that far, which keeps every event.
func poolCurrentHorizonSlot(
	ctx context.Context,
	db PoolCurrentStateDB,
) (int64, error) {
	var slot int64
	err := db.QueryRowContext(ctx, `
SELECT start_slot FROM epoch
WHERE epoch_id <= (SELECT MAX(epoch_id) FROM epoch) - ?
ORDER BY epoch_id DESC LIMIT 1`, poolCurrentHorizonEpochs).Scan(&slot)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("pool current state: horizon: %w", err)
	}
	return slot, nil
}

// refreshPoolCurrentState recomputes the rows of the pools with the given key
// hashes from their certificate history. A pool with no history, or no pool
// row at all, ends with no rows.
func refreshPoolCurrentState(
	ctx context.Context,
	db PoolCurrentStateDB,
	hashes [][]byte,
) error {
	hashes = uniqueHashes(hashes)
	if len(hashes) == 0 {
		return nil
	}
	horizon, err := poolCurrentHorizonSlot(ctx, db)
	if err != nil {
		return err
	}
	for start := 0; start < len(hashes); start += 200 {
		end := min(start+200, len(hashes))
		if err := refreshPoolCurrentStateChunk(
			ctx, db, hashes[start:end], horizon,
		); err != nil {
			return err
		}
	}
	return nil
}

func uniqueHashes(hashes [][]byte) [][]byte {
	seen := make(map[string]struct{}, len(hashes))
	ret := make([][]byte, 0, len(hashes))
	for _, hash := range hashes {
		if len(hash) == 0 {
			continue
		}
		if _, ok := seen[string(hash)]; ok {
			continue
		}
		seen[string(hash)] = struct{}{}
		ret = append(ret, hash)
	}
	return ret
}

// poolCurrentKept appends to kept the events of one pool, newest first, that a
// row with the given horizon stores: all at or after the horizon and the
// newest below it.
func poolCurrentKept(
	events []poolCurrentEvent,
	horizon uint64,
) []poolCurrentEvent {
	for i, event := range events {
		if event.effectiveSlot < horizon {
			return events[:i+1]
		}
	}
	return events
}

func refreshPoolCurrentStateChunk(
	ctx context.Context,
	db PoolCurrentStateDB,
	hashes [][]byte,
	horizon int64,
) error {
	args := make([]any, len(hashes))
	for i, hash := range hashes {
		args[i] = hash
	}
	in := bindPlaceholders(len(hashes))
	poolIDs := map[string]int64{}
	if err := scanRows(ctx, db,
		"SELECT id, pool_key_hash FROM pool WHERE pool_key_hash IN ("+in+")",
		args,
		func(rows *sql.Rows) error {
			var id int64
			var hash []byte
			if err := rows.Scan(&id, &hash); err != nil {
				return err
			}
			poolIDs[string(hash)] = id
			return nil
		},
	); err != nil {
		return fmt.Errorf("pool current state: read pools: %w", err)
	}
	for _, id := range poolIDs {
		for _, table := range []string{
			"pool_current_event", "pool_current_state",
		} {
			if _, err := db.ExecContext(ctx,
				"DELETE FROM "+table+" WHERE pool_id = ?", id,
			); err != nil {
				return fmt.Errorf("pool current state: clear: %w", err)
			}
		}
	}
	if len(poolIDs) == 0 {
		return nil
	}
	horizonSlot, err := checkedUint64(horizon)
	if err != nil {
		return err
	}
	registrations := map[string][]poolCurrentEvent{}
	err = scanRows(ctx, db, `
SELECT pr.pool_key_hash, pr.id,
       `+poolCurrentEffectiveSlot("pr")+` ev_slot,
       pr.added_slot ev_added,
       COALESCE(tx.block_index, 0) ev_block,
       COALESCE(certs.cert_index, 0) ev_cert,
       pr.registration_index ev_index
FROM pool_registration pr
LEFT JOIN certs ON certs.id = pr.certificate_id
LEFT JOIN applied_transaction tx ON tx.tx_id = certs.transaction_id
WHERE pr.pool_key_hash IN (`+in+`)
ORDER BY pr.pool_key_hash, ev_slot DESC, ev_added DESC, ev_block DESC,
         ev_cert DESC, ev_index DESC, pr.id DESC`,
		args, func(rows *sql.Rows) error {
			var hash []byte
			var id, slot, added int64
			var event poolCurrentEvent
			if err := rows.Scan(
				&hash, &id, &slot, &added, &event.blockIndex,
				&event.certIndex, &event.registrationIndex,
			); err != nil {
				return err
			}
			var convErr error
			if event.id, convErr = checkedUint(id); convErr != nil {
				return convErr
			}
			if event.effectiveSlot, convErr = checkedUint64(slot); convErr != nil {
				return convErr
			}
			if event.addedSlot, convErr = checkedUint64(added); convErr != nil {
				return convErr
			}
			registrations[string(hash)] = append(
				registrations[string(hash)],
				event,
			)
			return nil
		})
	if err != nil {
		return fmt.Errorf("pool current state: rank registrations: %w", err)
	}
	retirements := map[string][]poolCurrentEvent{}
	err = scanRows(ctx, db, `
SELECT rt.pool_key_hash, rt.id, rt.epoch,
       `+poolCurrentEffectiveSlot("rt")+` ev_slot,
       rt.added_slot ev_added,
       CASE WHEN rt.certificate_id = 0 THEN 1 ELSE 0 END ev_synthetic,
       COALESCE(tx.block_index, 0) ev_block,
       COALESCE(certs.cert_index, 0) ev_cert
FROM pool_retirement rt
LEFT JOIN certs ON certs.id = rt.certificate_id
LEFT JOIN applied_transaction tx ON tx.tx_id = certs.transaction_id
WHERE rt.pool_key_hash IN (`+in+`)
ORDER BY rt.pool_key_hash, ev_slot DESC, ev_added DESC, ev_synthetic DESC,
         ev_block DESC, ev_cert DESC, rt.id DESC`,
		args, func(rows *sql.Rows) error {
			var hash []byte
			var id, epoch, slot, added int64
			var event poolCurrentEvent
			if err := rows.Scan(
				&hash, &id, &epoch, &slot, &added, &event.synthetic,
				&event.blockIndex, &event.certIndex,
			); err != nil {
				return err
			}
			var convErr error
			if event.id, convErr = checkedUint(id); convErr != nil {
				return convErr
			}
			if event.epoch, convErr = checkedUint64(epoch); convErr != nil {
				return convErr
			}
			if event.effectiveSlot, convErr = checkedUint64(slot); convErr != nil {
				return convErr
			}
			if event.addedSlot, convErr = checkedUint64(added); convErr != nil {
				return convErr
			}
			retirements[string(hash)] = append(retirements[string(hash)], event)
			return nil
		})
	if err != nil {
		return fmt.Errorf("pool current state: rank retirements: %w", err)
	}
	insert := func(
		poolID int64, kind, rank int, event poolCurrentEvent,
	) error {
		if _, err := db.ExecContext(ctx, `
INSERT INTO pool_current_event (
    pool_id, kind, event_rank, event_id, effective_slot, added_slot,
    synthetic, block_index, cert_index, registration_index, epoch
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			poolID, kind, rank, event.id, event.effectiveSlot,
			event.addedSlot, event.synthetic, event.blockIndex,
			event.certIndex, event.registrationIndex, event.epoch,
		); err != nil {
			return fmt.Errorf("pool current state: write: %w", err)
		}
		return nil
	}
	for hash, poolID := range poolIDs {
		if len(registrations[hash]) == 0 && len(retirements[hash]) == 0 {
			continue
		}
		if _, err := db.ExecContext(ctx, `
INSERT INTO pool_current_state (pool_id, horizon_slot) VALUES (?, ?)`,
			poolID, horizon,
		); err != nil {
			return fmt.Errorf("pool current state: write: %w", err)
		}
		for rank, event := range poolCurrentKept(
			registrations[hash], horizonSlot,
		) {
			if err := insert(
				poolID, poolCurrentKindRegistration, rank, event,
			); err != nil {
				return err
			}
		}
		for rank, event := range poolCurrentKept(
			retirements[hash], horizonSlot,
		) {
			if err := insert(
				poolID, poolCurrentKindRetirement, rank, event,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

// poolHashesOfTransactions returns the pools that have a registration or
// retirement certificate in one of the transactions.
func poolHashesOfTransactions(
	ctx context.Context,
	db queryer,
	transactionIDs []int64,
) ([][]byte, error) {
	var ret [][]byte
	for start := 0; start < len(transactionIDs); start += 200 {
		end := min(start+200, len(transactionIDs))
		args := make([]any, 0, 2*(end-start))
		for range 2 {
			for _, id := range transactionIDs[start:end] {
				args = append(args, id)
			}
		}
		in := bindPlaceholders(end - start)
		if err := scanRows(ctx, db, `
SELECT pr.pool_key_hash FROM pool_registration pr
JOIN certs ON certs.id = pr.certificate_id
WHERE certs.transaction_id IN (`+in+`)
UNION
SELECT rt.pool_key_hash FROM pool_retirement rt
JOIN certs ON certs.id = rt.certificate_id
WHERE certs.transaction_id IN (`+in+`)`, args, func(rows *sql.Rows) error {
			var hash []byte
			if err := rows.Scan(&hash); err != nil {
				return err
			}
			ret = append(ret, hash)
			return nil
		}); err != nil {
			return nil, fmt.Errorf(
				"pool current state: transaction pools: %w", err,
			)
		}
	}
	return ret, nil
}

// poolHashesAfterSlot returns the pools with a registration or retirement
// added after slot.
func poolHashesAfterSlot(
	ctx context.Context,
	db queryer,
	slot uint64,
) ([][]byte, error) {
	rows, err := db.QueryContext(ctx, `
SELECT pool_key_hash FROM pool_registration WHERE added_slot > ?
UNION
SELECT pool_key_hash FROM pool_retirement WHERE added_slot > ?`, slot, slot)
	if err != nil {
		return nil, fmt.Errorf("pool current state: pools after slot: %w", err)
	}
	defer rows.Close()
	var ret [][]byte
	for rows.Next() {
		var hash []byte
		if err := rows.Scan(&hash); err != nil {
			return nil, err
		}
		ret = append(ret, hash)
	}
	return ret, rows.Err()
}

// refreshPoolCurrentStateForTransactions recomputes the pools whose
// certificates sit in the transactions.
func refreshPoolCurrentStateForTransactions(
	ctx context.Context,
	db queryer,
	transactionIDs []int64,
) error {
	hashes, err := poolHashesOfTransactions(ctx, db, transactionIDs)
	if err != nil {
		return err
	}
	return refreshPoolCurrentState(ctx, db, hashes)
}

// poolCurrentStateLookupSQL reads the stored events of the listed pools. It
// touches only pool, pool_current_state and pool_current_event, by key.
func poolCurrentStateLookupSQL(pools int) string {
	return `
SELECT pool.pool_key_hash, state.horizon_slot, event.kind, event.event_id,
       event.effective_slot, event.added_slot, event.synthetic,
       event.block_index, event.cert_index, event.registration_index,
       event.epoch
FROM pool
JOIN pool_current_state state ON state.pool_id = pool.id
JOIN pool_current_event event ON event.pool_id = pool.id
WHERE pool.pool_key_hash IN (` + bindPlaceholders(pools) + `)
ORDER BY pool.pool_key_hash, event.kind, event.event_rank`
}

// lookupPoolCurrentStates returns the stored state of the pools, keyed by pool
// key hash. A pool with no history is absent from the map.
func lookupPoolCurrentStates(
	ctx context.Context,
	db queryer,
	pools []lcommon.PoolKeyHash,
) (map[string]*poolCurrentPool, error) {
	args := make([]any, len(pools))
	for i, pool := range pools {
		args[i] = pool.Bytes()
	}
	ret := make(map[string]*poolCurrentPool, len(pools))
	err := scanRows(ctx, db, poolCurrentStateLookupSQL(len(args)), args,
		func(rows *sql.Rows) error {
			var hash []byte
			var kind, id, slot, added, horizon, epoch int64
			var event poolCurrentEvent
			if err := rows.Scan(
				&hash, &horizon, &kind, &id, &slot, &added,
				&event.synthetic, &event.blockIndex, &event.certIndex,
				&event.registrationIndex, &epoch,
			); err != nil {
				return err
			}
			var convErr error
			if event.id, convErr = checkedUint(id); convErr != nil {
				return convErr
			}
			if event.effectiveSlot, convErr = checkedUint64(slot); convErr != nil {
				return convErr
			}
			if event.addedSlot, convErr = checkedUint64(added); convErr != nil {
				return convErr
			}
			if event.epoch, convErr = checkedUint64(epoch); convErr != nil {
				return convErr
			}
			state := ret[string(hash)]
			if state == nil {
				state = &poolCurrentPool{}
				if state.horizon, convErr = checkedUint64(horizon); convErr != nil {
					return convErr
				}
				ret[string(hash)] = state
			}
			if kind == poolCurrentKindRetirement {
				state.retirements = append(state.retirements, event)
			} else {
				state.registrations = append(state.registrations, event)
			}
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("query pool current state: %w", err)
	}
	return ret, nil
}

// GetPoolRegistrationsEffectiveForEpoch answers from pool_current_state. The
// stored rows are exact for any epochStartSlot at or after the pool's horizon.
// A pool whose epochStartSlot precedes its horizon, an epoch more than
// poolCurrentHorizonEpochs behind its newest certificate, is ranked from the
// certificate history tables instead, because a bounded row cannot hold every
// older answer. Reward recovery, repair and import ask about such epochs;
// the epoch-boundary and tip callers ask about the current epoch and do not
// reach that path.
func (s *Store) GetPoolRegistrationsEffectiveForEpoch(
	poolKeyHashes []lcommon.PoolKeyHash,
	epochStartSlot uint64,
	endedEpoch uint64,
	snapshotSlot uint64,
	txn types.Txn,
) ([]models.PoolRegistration, error) {
	if len(poolKeyHashes) == 0 {
		return []models.PoolRegistration{}, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	var registrationIDs []uint
	var fallback []lcommon.PoolKeyHash
	unique := uniquePoolKeyHashes(poolKeyHashes)
	for start := 0; start < len(unique); start += 400 {
		end := min(start+400, len(unique))
		chunk := unique[start:end]
		states, err := lookupPoolCurrentStates(ctx, db, chunk)
		if err != nil {
			return nil, err
		}
		for _, poolKeyHash := range chunk {
			state, ok := states[string(poolKeyHash.Bytes())]
			if !ok {
				continue
			}
			id, found, covered := state.effective(
				epochStartSlot, endedEpoch, snapshotSlot,
			)
			switch {
			case !covered:
				fallback = append(fallback, poolKeyHash)
			case found:
				registrationIDs = append(registrationIDs, id)
			}
		}
	}
	ret, err := loadPoolRegistrationsByID(ctx, db, registrationIDs)
	if err != nil {
		return nil, err
	}
	s.poolCurrentFastPools.Add(int64(len(unique) - len(fallback)))
	s.poolCurrentHistoryPools.Add(int64(len(fallback)))
	if len(fallback) > 0 {
		rest, err := s.getPoolRegistrationsEffectiveForEpochFromHistory(
			fallback, epochStartSlot, endedEpoch, snapshotSlot, txn,
		)
		if err != nil {
			return nil, err
		}
		ret = append(ret, rest...)
	}
	return ret, nil
}

// registrationAtSlot answers the at-slot query for the pool: the first stored
// registration whose effective slot is at or before slot. covered is false
// when slot precedes the row's horizon, where the newest registration at or
// before slot may be one the row dropped.
func (p poolCurrentPool) registrationAtSlot(
	slot uint64,
) (id uint, found bool, covered bool) {
	if slot < p.horizon {
		return 0, false, false
	}
	for i := range p.registrations {
		if p.registrations[i].effectiveSlot <= slot {
			return p.registrations[i].id, true, true
		}
	}
	return 0, false, true
}

// GetPoolRegistrationsAtSlot returns the latest registration for every
// requested pool at or before slot, ranked by effective slot, added slot,
// certificate position and row ID. It answers from pool_current_state for a
// slot at or after the pool's horizon and ranks the certificate history for a
// pool whose horizon is later than slot.
func (s *Store) GetPoolRegistrationsAtSlot(
	poolKeyHashes []lcommon.PoolKeyHash,
	slot uint64,
	txn types.Txn,
) ([]models.PoolRegistration, error) {
	if len(poolKeyHashes) == 0 {
		return []models.PoolRegistration{}, nil
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	unique := uniquePoolKeyHashes(poolKeyHashes)
	var registrationIDs []uint
	var fallback []lcommon.PoolKeyHash
	for start := 0; start < len(unique); start += 400 {
		chunk := unique[start:min(start+400, len(unique))]
		states, err := lookupPoolCurrentStates(ctx, db, chunk)
		if err != nil {
			return nil, err
		}
		for _, poolKeyHash := range chunk {
			state, ok := states[string(poolKeyHash.Bytes())]
			if !ok {
				continue
			}
			id, found, covered := state.registrationAtSlot(slot)
			switch {
			case !covered:
				fallback = append(fallback, poolKeyHash)
			case found:
				registrationIDs = append(registrationIDs, id)
			}
		}
	}
	ret, err := loadPoolRegistrationsByID(ctx, db, registrationIDs)
	if err != nil {
		return nil, err
	}
	if len(fallback) > 0 {
		rest, err := s.getPoolRegistrationsAtSlotFromHistory(
			fallback, slot, txn,
		)
		if err != nil {
			return nil, err
		}
		ret = append(ret, rest...)
	}
	return ret, nil
}

func uniquePoolKeyHashes(
	poolKeyHashes []lcommon.PoolKeyHash,
) []lcommon.PoolKeyHash {
	seen := make(map[string]struct{}, len(poolKeyHashes))
	unique := make([]lcommon.PoolKeyHash, 0, len(poolKeyHashes))
	for _, poolKeyHash := range poolKeyHashes {
		if _, ok := seen[string(poolKeyHash.Bytes())]; ok {
			continue
		}
		seen[string(poolKeyHash.Bytes())] = struct{}{}
		unique = append(unique, poolKeyHash)
	}
	return unique
}

// poolStatusSQL reads a pool row and the newest stored registration and
// retirement of the pool, by key.
const poolStatusSQL = `
SELECT event.kind, event.effective_slot, event.added_slot, event.synthetic,
       event.block_index, event.cert_index, event.epoch
FROM pool
LEFT JOIN pool_current_event event
       ON event.pool_id = pool.id AND event.event_rank = 0
WHERE pool.pool_key_hash = ?`

// GetPoolStatus returns the pool's lifecycle from the newest stored
// registration and retirement, which every row keeps whatever its horizon: no
// certificate history is read. A retirement counts as the pool's newest
// certificate unless a registration outranks it under the event ordering of
// the history ranking. That certificate decides both questions: the pool is
// retired when it is a retirement whose epoch has been reached, and its epoch
// is pending when it has not.
func (s *Store) GetPoolStatus(
	poolKeyHash lcommon.PoolKeyHash,
	txn types.Txn,
) (models.PoolStatus, error) {
	var status models.PoolStatus
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return status, err
	}
	var registration, retirement *poolCurrentEvent
	err = scanRows(ctx, db, poolStatusSQL, []any{poolKeyHash.Bytes()},
		func(rows *sql.Rows) error {
			status.Exists = true
			var kind, slot, added, synthetic, block, cert, epoch sql.NullInt64
			if err := rows.Scan(
				&kind, &slot, &added, &synthetic, &block, &cert, &epoch,
			); err != nil {
				return err
			}
			if !kind.Valid {
				return nil
			}
			event := poolCurrentEvent{
				synthetic:  synthetic.Int64,
				blockIndex: block.Int64,
				certIndex:  cert.Int64,
			}
			var convErr error
			if event.effectiveSlot, convErr = checkedUint64(slot.Int64); convErr != nil {
				return convErr
			}
			if event.addedSlot, convErr = checkedUint64(added.Int64); convErr != nil {
				return convErr
			}
			if event.epoch, convErr = checkedUint64(epoch.Int64); convErr != nil {
				return convErr
			}
			if kind.Int64 == poolCurrentKindRetirement {
				retirement = &event
			} else {
				registration = &event
			}
			return nil
		})
	if err != nil {
		return status, fmt.Errorf("query pool status: %w", err)
	}
	if registration == nil {
		return status, nil
	}
	status.Active = true
	if retirement == nil || registration.registrationWins(*retirement) {
		return status, nil
	}
	epoch := retirement.epoch
	status.RetirementEpoch = &epoch
	current, ok, err := currentEpoch(ctx, db)
	if err != nil {
		return status, err
	}
	if ok && epoch <= current {
		status.Active = false
		status.RetirementEpoch = nil
	}
	return status, nil
}
