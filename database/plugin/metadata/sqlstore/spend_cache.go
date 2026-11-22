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
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
)

// spendCacheLimit bounds the outputs the spend cache remembers. Most inputs
// spend recent outputs, so a cache that forgets everything when full still
// serves the bulk of them.
const spendCacheLimit = 1 << 20

// spendCacheEntry is what a spend needs from an output this store inserted:
// its row id, to address the row by primary key, and the columns the spend's
// stake delta is computed from. None of them changes after insert.
type spendCacheEntry struct {
	id         int64
	tag        int64
	stakingKey []byte
	amount     sql.NullString
}

// spendCache maps an output reference to the row this store inserted for it.
// It is a hint: a spend through it updates only a row that is still live, and
// a reference whose cached row it did not update takes the by-reference path.
// SQLite's AUTOINCREMENT never reuses the id of a committed row, but a
// rolled-back insert frees its id, and nothing rewinds sqlite_sequence on
// other paths, so every rollback empties the cache: otherwise a stale id could
// address a different row inserted after the rollback.
type spendCache struct {
	entries map[string]spendCacheEntry
}

func (s *Store) spendCacheEnabled() bool {
	return s.liveUtxo == nil && s.dialect.SupportsReturning()
}

func (s *Store) rememberInsertedOutputs(utxos []*models.Utxo, inserted []bool) {
	if !s.spendCacheEnabled() {
		return
	}
	s.spendMu.Lock()
	defer s.spendMu.Unlock()
	if s.spends.entries == nil || len(s.spends.entries)+len(utxos) > spendCacheLimit {
		s.spends.entries = make(map[string]spendCacheEntry, 1<<16)
	}
	for i, u := range utxos {
		if i >= len(inserted) || !inserted[i] || u.ID == 0 || u.DeletedSlot != 0 {
			continue
		}
		s.spends.entries[utxoIdentityKey(u.TxId, u.OutputIdx)] = spendCacheEntry{
			id:         int64(u.ID), //nolint:gosec // row ids are SQL integers
			tag:        int64(u.CredentialTag),
			stakingKey: u.StakingKey,
			amount:     sql.NullString{String: decimalUint64(u.Amount), Valid: true},
		}
	}
}

func (s *Store) forgetSpendCache() {
	s.spendMu.Lock()
	s.spends.entries = nil
	s.spendMu.Unlock()
}

// takeCachedSpends removes and returns the cached rows of refs, keyed by
// reference.
func (s *Store) takeCachedSpends(refs []models.UtxoId) map[string]spendCacheEntry {
	if !s.spendCacheEnabled() {
		return nil
	}
	s.spendMu.Lock()
	defer s.spendMu.Unlock()
	if len(s.spends.entries) == 0 {
		return nil
	}
	var out map[string]spendCacheEntry
	for _, ref := range refs {
		key := utxoIdentityKey(ref.Hash, ref.Idx)
		if e, ok := s.spends.entries[key]; ok {
			if out == nil {
				out = make(map[string]spendCacheEntry, len(refs))
			}
			out[key] = e
			delete(s.spends.entries, key)
		}
	}
	return out
}

// spendCachedRows marks the cached rows spent by primary key and reports the
// references whose row it updated, with their stake deltas. A row already
// spent or deleted is not updated and not reported.
func (s *Store) spendCachedRows(
	ctx context.Context,
	db queryer,
	cached map[string]spendCacheEntry,
	slot uint64,
	transactionID int64,
) (map[string]struct{}, []stakeCredentialDelta, error) {
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
		return nil, nil, err
	}
	defer rows.Close()
	updated := make(map[string]struct{}, len(cached))
	var deltas []stakeCredentialDelta
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, nil, err
		}
		key, ok := byID[id]
		if !ok {
			return nil, nil, fmt.Errorf("consume UTxO by id: unexpected row %d", id)
		}
		e := cached[key]
		updated[key] = struct{}{}
		delta, ok, err := consumedUtxoStakeDelta(e.tag, e.stakingKey, e.amount)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			deltas = append(deltas, delta)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return updated, deltas, nil
}

// consumeUtxosByIDQuery marks n rows spent by primary key. The texts for
// every batch size are hot statements, so none is parsed per call.
func consumeUtxosByIDQuery(n int) string {
	return "-- name: ConsumeUtxosByID :many\n" +
		"UPDATE utxo SET deleted_slot = ?, spent_at_tx_id = ?\n" +
		"WHERE id IN (" + strings.TrimSuffix(strings.Repeat("?, ", n), ", ") + ")\n" +
		"  AND deleted_slot = 0 AND spent_at_tx_id IS NULL\nRETURNING id"
}
