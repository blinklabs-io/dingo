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
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// An output names its producer, and a spent output its spender, by the
// transaction table's row id. A producer that was never applied here, which
// is how an imported or recovered output arrives, has an identity row and no
// applied_transaction row. resolveUtxoIdentities gives every model the ids it
// needs before the insert.

// identityLookupSizes are the hash-list sizes of the identity lookup; a list
// is padded with NULL up to its size.
var identityLookupSizes = [...]int{1, 2, 4, 8, 16, 32, 64}

// maxIdentityLookupHashes is the largest identity lookup list.
const maxIdentityLookupHashes = 64

var identityLookupQueries = func() map[int]string {
	queries := make(map[int]string, len(identityLookupSizes))
	for _, size := range identityLookupSizes {
		queries[size] = "-- name: GetTransactionIdentitiesByHash :many\n" +
			`SELECT id, hash FROM "transaction" WHERE hash IN (` +
			bindPlaceholders(size) + ")"
	}
	return queries
}()

// transactionIdentityIDs returns the row id of each distinct hash in hashes.
// With create set, a hash with no row gets an identity row; without it, a
// hash with no row is absent from the result.
func (s *Store) transactionIdentityIDs(
	ctx context.Context,
	db queryer,
	hashes [][]byte,
	create bool,
) (map[string]int64, error) {
	ids := make(map[string]int64, len(hashes))
	var pending [][]byte
	seen := make(map[string]struct{}, len(hashes))
	for _, hash := range hashes {
		if _, dup := seen[string(hash)]; dup || len(hash) == 0 {
			continue
		}
		seen[string(hash)] = struct{}{}
		pending = append(pending, hash)
	}
	for start := 0; start < len(pending); start += maxIdentityLookupHashes {
		chunk := pending[start:min(start+maxIdentityLookupHashes, len(pending))]
		size := sizeFor(identityLookupSizes[:], len(chunk))
		args := make([]any, 0, size)
		for _, hash := range chunk {
			args = append(args, hash)
		}
		for len(args) < size {
			args = append(args, nil)
		}
		rows, err := s.queryRowsCached(
			ctx, db, identityLookupQueries[size], args...,
		)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var (
				id   int64
				hash []byte
			)
			if err := rows.Scan(&id, &hash); err != nil {
				_ = rows.Close()
				return nil, err
			}
			ids[string(hash)] = id
		}
		rowsErr := rows.Err()
		closeErr := rows.Close()
		if rowsErr != nil {
			return nil, rowsErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	if !create {
		return ids, nil
	}
	ops := s.cachedTransactionRowOps(ctx, db)
	for _, hash := range pending {
		if _, ok := ids[string(hash)]; ok {
			continue
		}
		id, err := insertIdentityRow(s.dialect.Name(), ops, hash)
		if err != nil {
			return nil, err
		}
		ids[string(hash)] = id
	}
	return ids, nil
}

// resolveUtxoIdentities sets TransactionID on every model that names its
// producer only by hash, creating the producer's identity row when it has
// none, and SpentAtTxID on every model that names its spender only by hash.
// A spender must already have a row: a spend recorded for an unknown
// transaction would be unattributable.
func (s *Store) resolveUtxoIdentities(
	ctx context.Context,
	db queryer,
	utxos []*models.Utxo,
) error {
	var producers, spenders [][]byte
	for _, utxo := range utxos {
		if utxo.TransactionID == nil && utxo.CollateralReturnForTxID == nil {
			producers = append(producers, utxo.TxId)
		}
		if utxo.SpentAtTxID == nil && len(utxo.SpentAtTxHash) != 0 {
			spenders = append(spenders, utxo.SpentAtTxHash)
		}
	}
	if len(producers) == 0 && len(spenders) == 0 {
		return nil
	}
	producerIDs, err := s.transactionIdentityIDs(ctx, db, producers, true)
	if err != nil {
		return err
	}
	spenderIDs, err := s.transactionIdentityIDs(ctx, db, spenders, false)
	if err != nil {
		return err
	}
	for _, utxo := range utxos {
		if utxo.TransactionID == nil && utxo.CollateralReturnForTxID == nil {
			id, ok := producerIDs[string(utxo.TxId)]
			if !ok {
				return fmt.Errorf(
					"UTxO %x:%d has no producer transaction hash",
					utxo.TxId, utxo.OutputIdx,
				)
			}
			txID := uint(id)
			utxo.TransactionID = &txID
		}
		if utxo.SpentAtTxID == nil && len(utxo.SpentAtTxHash) != 0 {
			id, ok := spenderIDs[string(utxo.SpentAtTxHash)]
			if !ok {
				return fmt.Errorf(
					"spender transaction %x not found", []byte(utxo.SpentAtTxHash),
				)
			}
			spenderID := uint(id)
			utxo.SpentAtTxID = &spenderID
		}
	}
	return nil
}

// spenderTxID resolves a spender's hash to its transaction row id. An empty
// hash is no spender and resolves to NULL. A hash with no transaction row is
// an error: the spend would otherwise be recorded without its spender.
func (s *Store) spenderTxID(
	ctx context.Context,
	db queryer,
	hash []byte,
) (sql.NullInt64, error) {
	if len(hash) == 0 {
		return sql.NullInt64{}, nil
	}
	var id int64
	err := db.QueryRowContext(ctx, s.dialect.Rebind(
		`SELECT id FROM "transaction" WHERE hash = ?`,
	), hash).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return sql.NullInt64{}, fmt.Errorf(
			"spender transaction %x not found", hash,
		)
	}
	if err != nil {
		return sql.NullInt64{}, err
	}
	return sql.NullInt64{Int64: id, Valid: true}, nil
}

// isUniqueViolation reports whether err is an engine's unique-constraint
// violation. The drivers are not imported: pgx errors carry SQLSTATE 23505,
// modernc SQLite codes 1555 (primary key) and 2067 (unique), and MySQL
// surfaces error 1062 as text through database/sql.
func isUniqueViolation(err error) bool {
	var state interface{ SQLState() string }
	if errors.As(err, &state) && state.SQLState() == "23505" {
		return true
	}
	var code interface{ Code() int }
	if errors.As(err, &code) {
		if c := code.Code(); c == 1555 || c == 2067 {
			return true
		}
	}
	return strings.Contains(err.Error(), "Error 1062")
}

// utxoInsertConflict maps an engine's unique violation on a utxo insert to
// types.ErrUtxoConflict. MySQL's emulated INSERT ... RETURNING discards the
// engine's error (dialectQueryer), so there a failed insert is a conflict
// when the output is stored afterwards.
func (s *Store) utxoInsertConflict(
	ctx context.Context,
	db queryer,
	err error,
	utxo *models.Utxo,
) error {
	if err == nil {
		return nil
	}
	conflict := isUniqueViolation(err)
	if !conflict && s.dialect.Name() == "mysql" {
		var count int
		u := s.utxoSQL()
		if probeErr := db.QueryRowContext(
			ctx, u.countByRef, u.refArgs(utxo.TxId, utxo.OutputIdx)...,
		).Scan(&count); probeErr == nil && count > 0 {
			conflict = true
		}
	}
	if !conflict {
		return err
	}
	return fmt.Errorf(
		"%w: create UTxO %x:%d: output already stored: %w",
		types.ErrUtxoConflict, utxo.TxId, utxo.OutputIdx, err,
	)
}
