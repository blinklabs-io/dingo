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
	"fmt"
	"maps"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// spendCacheLimit bounds the outputs the spend cache remembers. Most inputs
// spend recent outputs, so a cache that forgets everything when full still
// serves the bulk of them.
const spendCacheLimit = 1 << 20

// spendCacheEntry is what a spend needs from an output this store inserted:
// its row id, to address the row by primary key, and the columns the spend's
// stake and script-locked supply deltas are computed from. None of them
// changes after insert.
type spendCacheEntry struct {
	id            int64
	tag           int64
	stakingKey    []byte
	amount        sql.NullString
	paymentScript bool
}

// spendCache maps an output reference to the row this store inserted for it.
// It is a hint: a spend through it updates only a row that is still live, and
// a reference whose cached row it did not update takes the by-reference path.
// An insert's entries are staged on its transaction and reach the cache only
// once that transaction commits: a rolled-back SQLite insert frees its id for
// the next insert, and database/sql rolls a transaction back on context
// cancellation without calling the wrapper, so an entry published before
// commit could address a different row. A committed row's id is never reused
// (SQLite AUTOINCREMENT, PostgreSQL sequences), so rollbacks leave the cache
// alone. Reset and RestoreFrom replace the rows themselves, so they empty it.
type spendCache struct {
	entries map[string]spendCacheEntry
}

func (s *Store) spendCacheEnabled() bool {
	return s.liveUtxo == nil && s.dialect.SupportsReturning()
}

// insertedSpendEntries returns the cache entries for the outputs an insert
// created, keyed by reference.
func (s *Store) insertedSpendEntries(
	utxos []*models.Utxo,
	inserted []bool,
) map[string]spendCacheEntry {
	if !s.spendCacheEnabled() {
		return nil
	}
	var out map[string]spendCacheEntry
	for i, u := range utxos {
		if i >= len(inserted) || !inserted[i] || u.ID == 0 ||
			u.DeletedSlot != 0 {
			continue
		}
		if out == nil {
			out = make(map[string]spendCacheEntry, len(utxos))
		}
		out[utxoIdentityKey(u.TxId, u.OutputIdx)] = spendCacheEntry{
			id:         int64(u.ID), //nolint:gosec // row ids are SQL integers
			tag:        int64(u.CredentialTag),
			stakingKey: u.StakingKey,
			amount: sql.NullString{
				String: decimalUint64(u.Amount),
				Valid:  true,
			},
			paymentScript: u.PaymentScript,
		}
	}
	return out
}

// rememberInsertedOutputs records entries written through txn. With no txn
// the write committed on its own, so they are published; otherwise they wait
// on txn until it commits.
func (s *Store) rememberInsertedOutputs(
	txn types.Txn,
	entries map[string]spendCacheEntry,
) {
	if len(entries) == 0 {
		return
	}
	if txn == nil {
		s.publishSpendEntries(entries)
		return
	}
	t, ok := txn.(*sqlTxn)
	if !ok {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished {
		return
	}
	if t.spends == nil || len(t.spends)+len(entries) > spendCacheLimit {
		t.spends = make(map[string]spendCacheEntry, len(entries))
	}
	maps.Copy(t.spends, entries)
}

func (s *Store) publishSpendEntries(entries map[string]spendCacheEntry) {
	if len(entries) == 0 {
		return
	}
	s.spendMu.Lock()
	defer s.spendMu.Unlock()
	if s.spends.entries == nil ||
		len(s.spends.entries)+len(entries) > spendCacheLimit {
		s.spends.entries = make(map[string]spendCacheEntry, 1<<16)
	}
	maps.Copy(s.spends.entries, entries)
}

func (s *Store) forgetSpendCache() {
	s.spendMu.Lock()
	s.spends.entries = nil
	s.spendMu.Unlock()
}

// takeCachedSpends removes and returns the cached rows of refs, keyed by
// reference: those txn inserted and has not committed, then those published.
func (s *Store) takeCachedSpends(
	txn types.Txn,
	refs []models.UtxoId,
) map[string]spendCacheEntry {
	if !s.spendCacheEnabled() {
		return nil
	}
	var out map[string]spendCacheEntry
	take := func(entries map[string]spendCacheEntry) {
		for _, ref := range refs {
			key := utxoIdentityKey(ref.Hash, ref.Idx)
			if e, ok := entries[key]; ok {
				if out == nil {
					out = make(map[string]spendCacheEntry, len(refs))
				}
				out[key] = e
				delete(entries, key)
			}
		}
	}
	if t, ok := txn.(*sqlTxn); ok {
		t.mu.Lock()
		take(t.spends)
		t.mu.Unlock()
	}
	s.spendMu.Lock()
	defer s.spendMu.Unlock()
	take(s.spends.entries)
	return out
}

// spendCachedRows marks the cached rows spent by primary key and reports the
// references whose row it updated, with their stake deltas and the
// script-locked lovelace they took out of the live set. A row already spent
// or deleted is not updated and not reported.
func (s *Store) spendCachedRows(
	ctx context.Context,
	db queryer,
	cached map[string]spendCacheEntry,
	slot uint64,
	transactionID int64,
) (map[string]struct{}, []stakeCredentialDelta, int64, error) {
	byID := make(map[int64]string, len(cached))
	args := make([]any, 0, 2+len(cached))
	args = append(args, slot, transactionID)
	for key, e := range cached {
		byID[e.id] = key
		args = append(args, e.id)
	}
	rows, err := s.queryRowsCached(
		ctx, db, consumeUtxosByIDQuery(len(cached)), args...,
	)
	if err != nil {
		return nil, nil, 0, err
	}
	defer rows.Close()
	updated := make(map[string]struct{}, len(cached))
	var deltas []stakeCredentialDelta
	var scriptLocked int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, nil, 0, err
		}
		key, ok := byID[id]
		if !ok {
			return nil, nil, 0, fmt.Errorf(
				"consume UTxO by id: unexpected row %d",
				id,
			)
		}
		e := cached[key]
		updated[key] = struct{}{}
		locked, err := scriptLockedContribution(
			sql.NullBool{Bool: e.paymentScript, Valid: true}, e.amount,
		)
		if err != nil {
			return nil, nil, 0, err
		}
		scriptLocked, err = checkedSupplyStep(scriptLocked, locked)
		if err != nil {
			return nil, nil, 0, err
		}
		delta, ok, err := consumedUtxoStakeDelta(e.tag, e.stakingKey, e.amount)
		if err != nil {
			return nil, nil, 0, err
		}
		if ok {
			deltas = append(deltas, delta)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, 0, err
	}
	return updated, deltas, scriptLocked, nil
}

// consumeUtxosByIDQuery marks n rows spent by primary key. The texts for
// every batch size are hot statements, so none is parsed per call.
func consumeUtxosByIDQuery(n int) string {
	return "-- name: ConsumeUtxosByID :many\n" +
		"UPDATE utxo SET deleted_slot = ?, spent_at_tx_id = ?\n" +
		"WHERE id IN (" + strings.TrimSuffix(strings.Repeat("?, ", n), ", ") + ")\n" +
		"  AND deleted_slot = 0 AND spent_at_tx_id IS NULL\nRETURNING id"
}
