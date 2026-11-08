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
	"math"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// script_locked_supply holds the total lovelace of live script-locked UTxOs
// as a running aggregate: one row per slot whose commit changed the total,
// valued at the total after that slot. Row(s) is therefore the sum of every
// delta at or below s, which is what lets a rollback to slot s keep the rows
// at or below s and drop the rest without looking at the UTxOs it removed
// (created outputs leave through ON DELETE CASCADE, invisible to Go).
//
// The table is written only here, in the transaction that changes the utxo
// rows, by the primitives that insert an output or move one from live to
// spent. A caller that bulk-loads live outputs without those primitives
// (snapshot import) calls RebuildScriptLockedSupply when it finishes.
const (
	scriptSupplyLatestQuery = `-- name: GetLatestScriptLockedSupply :one
SELECT slot, amount FROM script_locked_supply ORDER BY slot DESC LIMIT 1`
	scriptSupplyInsertQuery = `-- name: InsertScriptLockedSupply :exec
INSERT INTO script_locked_supply (slot, amount) VALUES (?, ?)`
	scriptSupplySetQuery = `-- name: SetScriptLockedSupply :exec
UPDATE script_locked_supply SET amount = ? WHERE slot = ?`
	scriptSupplyLiveScanQuery = `SELECT amount FROM utxo
WHERE payment_script = TRUE AND deleted_slot = 0`
)

// scriptLockedContribution is the lovelace a live output adds to the
// script-locked total: its amount when its stored payment_script flag is set,
// otherwise zero. A NULL flag reads as key-locked, as the scan always did.
func scriptLockedContribution(
	paymentScript sql.NullBool,
	amount sql.NullString,
) (int64, error) {
	if !paymentScript.Valid || !paymentScript.Bool ||
		!amount.Valid || amount.String == "" {
		return 0, nil
	}
	value, err := parseUint64("script-locked UTxO amount", amount.String)
	if err != nil {
		return 0, err
	}
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("script-locked UTxO amount overflow: %d", value)
	}
	return int64(value), nil
}

// modelScriptLockedContribution is scriptLockedContribution for a row about
// to be inserted; an output created already spent is not live.
func modelScriptLockedContribution(utxo *models.Utxo) (int64, error) {
	if !utxo.PaymentScript || utxo.DeletedSlot != 0 {
		return 0, nil
	}
	amount := uint64(utxo.Amount)
	if amount > math.MaxInt64 {
		return 0, fmt.Errorf("script-locked UTxO amount overflow: %d", amount)
	}
	return int64(amount), nil
}

func checkedSupplyStep(total, delta int64) (int64, error) {
	if (delta > 0 && total > math.MaxInt64-delta) ||
		(delta < 0 && total < -delta) {
		return 0, fmt.Errorf(
			"script-locked supply %d out of range after delta %d",
			total, delta,
		)
	}
	return total + delta, nil
}

// addScriptLockedSupply applies delta at slot. Slots normally arrive in
// ascending order, which costs one read and one write; a slot below the
// newest row (historical replay) also shifts every row above it, because each
// row is a cumulative total.
func (s *Store) addScriptLockedSupply(
	ctx context.Context,
	db queryer,
	slot uint64,
	delta int64,
) error {
	if delta == 0 {
		return nil
	}
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	var latestSlot, latestAmount int64
	err = s.queryRowCached(ctx, db, scriptSupplyLatestQuery).
		Scan(&latestSlot, &latestAmount)
	hasLatest := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read script-locked supply: %w", err)
	}
	switch {
	case !hasLatest || latestSlot < slotValue:
		next, err := checkedSupplyStep(latestAmount, delta)
		if err != nil {
			return err
		}
		_, err = s.execCached(ctx, db, scriptSupplyInsertQuery, slotValue, next)
		return err
	case latestSlot == slotValue:
		next, err := checkedSupplyStep(latestAmount, delta)
		if err != nil {
			return err
		}
		_, err = s.execCached(ctx, db, scriptSupplySetQuery, next, slotValue)
		return err
	}
	return s.addScriptLockedSupplyBelowLatest(ctx, db, slotValue, delta)
}

func (s *Store) addScriptLockedSupplyBelowLatest(
	ctx context.Context,
	db queryer,
	slot int64,
	delta int64,
) error {
	var previous int64
	err := db.QueryRowContext(ctx, s.dialect.Rebind(
		`SELECT amount FROM script_locked_supply
WHERE slot < ? ORDER BY slot DESC LIMIT 1`), slot).Scan(&previous)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	type row struct{ slot, amount int64 }
	var tail []row
	err = func() error {
		rows, err := db.QueryContext(ctx, s.dialect.Rebind(
			`SELECT slot, amount FROM script_locked_supply
WHERE slot >= ? ORDER BY slot`), slot)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.slot, &r.amount); err != nil {
				return err
			}
			tail = append(tail, r)
		}
		return rows.Err()
	}()
	if err != nil {
		return err
	}
	if len(tail) == 0 || tail[0].slot != slot {
		next, err := checkedSupplyStep(previous, delta)
		if err != nil {
			return err
		}
		if _, err := s.execCached(
			ctx, db, scriptSupplyInsertQuery, slot, next,
		); err != nil {
			return err
		}
	}
	for _, r := range tail {
		next, err := checkedSupplyStep(r.amount, delta)
		if err != nil {
			return err
		}
		if _, err := s.execCached(
			ctx, db, scriptSupplySetQuery, next, r.slot,
		); err != nil {
			return err
		}
	}
	return nil
}

// GetScriptLockedSupply returns the lovelace held in live UTxOs whose payment
// credential is a script: the newest aggregate row, or zero when none exists.
func (s *Store) GetScriptLockedSupply(txn types.Txn) (uint64, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	var amount int64
	err = db.QueryRowContext(ctx, scriptSupplyLatestQuery).
		Scan(new(int64), &amount)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if amount < 0 {
		return 0, fmt.Errorf("script-locked supply is negative: %d", amount)
	}
	return uint64(amount), nil
}

// RebuildScriptLockedSupply replaces the aggregate with one row at slot
// holding a full scan of the live script-locked outputs. Bulk loads that
// insert live rows outside the maintained primitives call it when done.
func (s *Store) RebuildScriptLockedSupply(
	slot uint64,
	txn types.Txn,
) error {
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			if _, err := db.ExecContext(
				ctx, "DELETE FROM script_locked_supply",
			); err != nil {
				return err
			}
			return s.seedScriptLockedSupply(ctx, db, slot)
		},
	)
}

func (s *Store) seedScriptLockedSupply(
	ctx context.Context,
	db queryer,
	slot uint64,
) error {
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	total, err := sumUint64Rows(
		ctx,
		db,
		s.dialect.Rebind(scriptSupplyLiveScanQuery),
	)
	if err != nil {
		return fmt.Errorf("scan script-locked supply: %w", err)
	}
	if total > math.MaxInt64 {
		return fmt.Errorf("script-locked supply exceeds int64: %d", total)
	}
	_, err = db.ExecContext(
		ctx, s.dialect.Rebind(scriptSupplyInsertQuery), slotValue, int64(total),
	)
	return err
}

// rollbackScriptLockedSupply drops the rows above slot after a rollback has
// restored the utxo rows to that slot. When that leaves no row at or below
// slot although rows existed above it -- the only row was a seed written at a
// later slot -- the base is gone, so the aggregate is rebuilt by a scan of the
// now-restored rows rather than read as zero.
func (s *Store) rollbackScriptLockedSupply(
	ctx context.Context,
	db queryer,
	slot uint64,
) error {
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	result, err := db.ExecContext(
		ctx,
		s.dialect.Rebind("DELETE FROM script_locked_supply WHERE slot > ?"),
		slotValue,
	)
	if err != nil {
		return err
	}
	removed, err := result.RowsAffected()
	if err != nil || removed == 0 {
		return err
	}
	var remaining int
	if err := db.QueryRowContext(
		ctx, "SELECT COUNT(*) FROM script_locked_supply",
	).Scan(&remaining); err != nil {
		return err
	}
	if remaining > 0 {
		return nil
	}
	return s.seedScriptLockedSupply(ctx, db, slot)
}

// PruneScriptLockedSupplyBefore deletes the rows a rollback can no longer
// reach: everything older than the newest row at or below floor, which stays
// as the base for the slots above it.
func (s *Store) PruneScriptLockedSupplyBefore(
	floor uint64,
	txn types.Txn,
) error {
	floorValue, err := checkedInt64(floor)
	if err != nil {
		return err
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			var base int64
			err := db.QueryRowContext(ctx, s.dialect.Rebind(
				`SELECT slot FROM script_locked_supply
WHERE slot <= ? ORDER BY slot DESC LIMIT 1`), floorValue).Scan(&base)
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, s.dialect.Rebind(
				"DELETE FROM script_locked_supply WHERE slot < ?"), base)
			return err
		},
	)
}
