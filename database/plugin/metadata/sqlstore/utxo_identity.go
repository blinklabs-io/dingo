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

	"github.com/blinklabs-io/dingo/database/models"
)

// utxoProducedKind reports whether the output names a producing transaction.
func utxoProducedKind(utxo *models.Utxo) bool {
	return utxo.TransactionID != nil || utxo.CollateralReturnForTxID != nil
}

// existingUtxoRefs returns the identity keys of the outputs already stored
// under another kind of identity than the one the insert would conflict on: a
// produced output whose hash an import already stored, or an imported output
// whose producing transaction already wrote it. Those pairs would insert as
// duplicates, since no unique index spans both kinds, so the caller treats
// them as conflicts. It is a no-op away from SQLite, whose full unique index
// sees both kinds.
func (s *Store) existingUtxoRefs(
	ctx context.Context,
	db queryer,
	utxos []*models.Utxo,
) (map[string]struct{}, error) {
	if s.dialect.Name() != "sqlite" || len(utxos) == 0 {
		return nil, nil
	}
	var produced, imported []*models.Utxo
	for _, utxo := range utxos {
		if utxoProducedKind(utxo) {
			produced = append(produced, utxo)
		} else {
			imported = append(imported, utxo)
		}
	}
	found := make(map[string]struct{})
	for start := 0; start < len(produced); start += maxImportedProbeHashes {
		part := produced[start:min(start+maxImportedProbeHashes, len(produced))]
		hashes := distinctUtxoHashes(part)
		size := importedProbeSize(len(hashes))
		for len(hashes) < size {
			// NULL matches no row.
			hashes = append(hashes, nil)
		}
		rows, err := s.queryRowsCached(
			ctx, db, importedProbeQuery(size), hashes...,
		)
		if err != nil {
			return nil, err
		}
		if err := collectUtxoKeys(rows, found); err != nil {
			return nil, err
		}
	}
	for start := 0; start < len(imported); start += maxImportedProbeHashes {
		part := imported[start:min(start+maxImportedProbeHashes, len(imported))]
		hashes := distinctUtxoHashes(part)
		size := importedProbeSize(len(hashes))
		for len(hashes) < size {
			hashes = append(hashes, nil)
		}
		rows, err := db.QueryContext(
			ctx, producedProbeQuery(size), hashes...,
		)
		if err != nil {
			return nil, err
		}
		if err := collectUtxoKeys(rows, found); err != nil {
			return nil, err
		}
	}
	return found, nil
}

// maxImportedProbeHashes bounds the hashes one probe names; probes are padded
// to a power of two so each size is one prepared statement.
const maxImportedProbeHashes = 64

func importedProbeSize(n int) int {
	size := 1
	for size < n {
		size *= 2
	}
	return size
}

// importedProbeQuery finds the imported rows of the given transaction hashes;
// size is one of the sizes of sqliteRefs.
func importedProbeQuery(size int) string {
	return sqliteRefs.importedProbe[size]
}

// producedProbeQuery finds the produced rows of the given transaction hashes;
// size is one of the sizes of sqliteRefs.
func producedProbeQuery(size int) string {
	return sqliteRefs.producedProbe[size]
}

func distinctUtxoHashes(utxos []*models.Utxo) []any {
	seen := make(map[string]struct{}, len(utxos))
	hashes := make([]any, 0, len(utxos))
	for _, utxo := range utxos {
		if _, ok := seen[string(utxo.TxId)]; ok {
			continue
		}
		seen[string(utxo.TxId)] = struct{}{}
		hashes = append(hashes, utxo.TxId)
	}
	return hashes
}

func collectUtxoKeys(rows *sql.Rows, into map[string]struct{}) error {
	defer rows.Close()
	for rows.Next() {
		var (
			hash []byte
			idx  int64
		)
		if err := rows.Scan(&hash, &idx); err != nil {
			return err
		}
		into[utxoIdentityKey(hash, uint32(idx))] = struct{}{} //nolint:gosec
	}
	return rows.Err()
}
