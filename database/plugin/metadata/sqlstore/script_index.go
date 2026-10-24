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
	"math"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ListScriptHashes implements metadata.MetadataStore.
func (s *Store) ListScriptHashes(
	limit int,
	offset int,
	descending bool,
	txn types.Txn,
) ([][]byte, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	query, args := addLimitOffset(
		`SELECT hash FROM script ORDER BY id `+orderDirection(descending),
		nil,
		limit,
		offset,
	)
	rows, err := db.QueryContext(ctx, s.dialect.Rebind(query), args...)
	if err != nil {
		return nil, fmt.Errorf("list script hashes: %w", err)
	}
	defer rows.Close()
	ret := [][]byte{}
	for rows.Next() {
		var hash []byte
		if err := rows.Scan(&hash); err != nil {
			return nil, fmt.Errorf("scan script hash: %w", err)
		}
		ret = append(ret, hash)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list script hashes: %w", err)
	}
	return ret, nil
}

// CountScripts implements metadata.MetadataStore.
func (s *Store) CountScripts(txn types.Txn) (int, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	var count int
	if err := db.QueryRowContext(
		ctx, `SELECT COUNT(*) FROM script`,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count scripts: %w", err)
	}
	return count, nil
}

// GetScriptRedeemers implements metadata.MetadataStore.
func (s *Store) GetScriptRedeemers(
	hash lcommon.ScriptHash,
	limit int,
	offset int,
	descending bool,
	txn types.Txn,
) ([]models.ScriptRedeemerRef, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	index := s.dialect.QuoteIdentifier("index")
	dir := orderDirection(descending)
	query, args := addLimitOffset(
		`SELECT t.hash, rs.slot, rs.tx_index, rs.`+index+`, rs.tag
FROM redeemer_script AS rs
JOIN "transaction" AS t ON t.id = rs.transaction_id
WHERE rs.script_hash = ?
ORDER BY rs.slot `+dir+`, rs.tx_index `+dir+`, rs.tag `+dir+`, rs.`+index+` `+dir,
		[]any{hash[:]},
		limit,
		offset,
	)
	rows, err := db.QueryContext(ctx, s.dialect.Rebind(query), args...)
	if err != nil {
		return nil, fmt.Errorf("get script redeemers: %w", err)
	}
	defer rows.Close()
	ret := []models.ScriptRedeemerRef{}
	for rows.Next() {
		var (
			ref                          models.ScriptRedeemerRef
			slot, txIndex, redeemerIndex int64
			tag                          int64
		)
		if err := rows.Scan(
			&ref.TxHash, &slot, &txIndex, &redeemerIndex, &tag,
		); err != nil {
			return nil, fmt.Errorf("scan script redeemer: %w", err)
		}
		if ref.Slot, err = checkedUint64(slot); err != nil {
			return nil, fmt.Errorf("script redeemer slot: %w", err)
		}
		if txIndex < 0 || txIndex > math.MaxUint32 ||
			redeemerIndex < 0 || redeemerIndex > math.MaxUint32 {
			return nil, fmt.Errorf(
				"script redeemer position %d/%d out of range",
				txIndex,
				redeemerIndex,
			)
		}
		ref.TxIndex = uint32(txIndex)     // #nosec G115 -- range checked above
		ref.Index = uint32(redeemerIndex) // #nosec G115 -- range checked above
		if ref.Tag, err = checkedUint8(tag); err != nil {
			return nil, fmt.Errorf("script redeemer tag: %w", err)
		}
		ret = append(ret, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("get script redeemers: %w", err)
	}
	return ret, nil
}

// CountScriptRedeemers implements metadata.MetadataStore.
func (s *Store) CountScriptRedeemers(
	hash lcommon.ScriptHash,
	txn types.Txn,
) (int, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	var count int
	if err := db.QueryRowContext(
		ctx,
		s.dialect.Rebind(
			`SELECT COUNT(*) FROM redeemer_script WHERE script_hash = ?`,
		),
		hash[:],
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count script redeemers: %w", err)
	}
	return count, nil
}

const referenceScriptUtxoPredicate = `utxo.deleted_slot = 0 AND utxo.id IN (
    SELECT utxo_id FROM utxo_reference_script WHERE script_hash = ?)`

// GetScriptReferenceUtxos implements metadata.MetadataStore.
func (s *Store) GetScriptReferenceUtxos(
	hash lcommon.ScriptHash,
	limit int,
	offset int,
	descending bool,
	txn types.Txn,
) ([]models.Utxo, error) {
	// queryUtxosWithAssets appends the ordering last, so the window is part
	// of it.
	order := `utxo.id ` + orderDirection(descending)
	if limit > 0 {
		order += fmt.Sprintf(" LIMIT %d", limit)
	} else if offset > 0 {
		order += " LIMIT " + unboundedLimit
	}
	if offset > 0 {
		order += fmt.Sprintf(" OFFSET %d", offset)
	}
	return s.queryUtxosWithAssets(
		txn,
		referenceScriptUtxoPredicate,
		[]any{hash[:]},
		order,
	)
}

// CountScriptReferenceUtxos implements metadata.MetadataStore.
func (s *Store) CountScriptReferenceUtxos(
	hash lcommon.ScriptHash,
	txn types.Txn,
) (int, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return 0, err
	}
	var count int
	if err := db.QueryRowContext(
		ctx,
		s.dialect.Rebind(
			`SELECT COUNT(*) FROM utxo WHERE `+referenceScriptUtxoPredicate,
		),
		hash[:],
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count script reference UTxOs: %w", err)
	}
	return count, nil
}

// persistUtxoReferenceScript records the script an output carries as a
// reference script, in API storage mode only. The script is a property of the
// output, so re-applying the same output converges rather than conflicting.
func (s *Store) persistUtxoReferenceScript(
	ctx context.Context,
	db queryer,
	utxoID uint,
	scriptHash []byte,
) error {
	if s.storageMode != types.StorageModeAPI || len(scriptHash) == 0 {
		return nil
	}
	if _, err := s.execCached(ctx, db, upsertUtxoReferenceScriptQuery,
		sql.NullInt64{Int64: int64(utxoID), Valid: true}, // #nosec G115 -- row ids are positive
		scriptHash,
	); err != nil {
		return fmt.Errorf("record UTxO reference script: %w", err)
	}
	return nil
}

const upsertUtxoReferenceScriptQuery = `-- name: UpsertUtxoReferenceScript :exec
INSERT INTO utxo_reference_script (utxo_id, script_hash) VALUES (?, ?)
ON CONFLICT (utxo_id) DO UPDATE SET script_hash = excluded.script_hash`
