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
	"fmt"
)

var activePayloadQueries = []struct {
	name  string
	query string
	// bySlot is set when the query takes the block's slot and not its hash.
	bySlot bool
	// epoch is set when the query also takes the current epoch.
	epoch bool
}{
	{
		// A datum row names the block that first carried the datum. A live
		// output names it by hash, inline or not, so the block must stay.
		name: "datum named by a live output",
		query: `SELECT EXISTS (SELECT 1 FROM datum d WHERE d.added_slot = ? AND
EXISTS (SELECT 1 FROM utxo u WHERE u.datum_hash = d.hash AND u.deleted_slot = 0))`,
		bySlot: true,
	},
	{
		name: "reference script of a live output",
		query: `SELECT EXISTS (SELECT 1 FROM script sc WHERE sc.created_slot = ? AND
EXISTS (SELECT 1 FROM utxo_reference_script r JOIN utxo u ON u.id = r.utxo_id
WHERE r.script_hash = sc.hash AND u.deleted_slot = 0))`,
		bySlot: true,
	},
	{
		name: "pending protocol parameter update",
		query: `SELECT EXISTS (SELECT 1 FROM pparam_update
WHERE added_slot = ? AND epoch >= ?)`,
		bySlot: true,
		epoch:  true,
	},
	{
		name: "unspent Midnight registration",
		query: `SELECT EXISTS (SELECT 1 FROM midnight_registrations r
WHERE r.block_hash = ? AND NOT EXISTS (SELECT 1 FROM midnight_deregistrations g
WHERE g.utxo_tx_hash = r.tx_hash AND g.utxo_index = r.output_index))`,
	},
	{
		// Bytes 13 to 44 of a stored offset are the hash of the block it names.
		name: "latest Midnight Ariadne parameters",
		query: `SELECT EXISTS (SELECT 1 FROM midnight_ariadne_params p
WHERE p.epoch = (SELECT MAX(epoch) FROM midnight_ariadne_params)
AND SUBSTR(p.datum, 13, 32) = ?)`,
	},
	{
		name: "current Midnight governance datum",
		query: `SELECT EXISTS (SELECT 1 FROM midnight_governance_datums g
WHERE g.block_number = (SELECT MAX(g2.block_number)
FROM midnight_governance_datums g2 WHERE g2.datum_type = g.datum_type)
AND SUBSTR(g.datum, 13, 32) = ?)`,
	},
}

// BlockHoldsActivePayloads reports whether a payload column that is still in
// use names bytes of the block at (slot, hash): a datum or reference script
// that a live output carries or names by hash, a protocol parameter update not
// yet past currentEpoch, an unspent Midnight registration, the latest Midnight
// Ariadne parameters or the current Midnight governance datum of a type. Such a
// block must not be expired.
//
// A classic update proposal targets an epoch and is enacted or discarded at the
// boundary into the next one, so it is pending while its epoch is not before
// currentEpoch (selectClassicPParamUpdate).
func (s *Store) BlockHoldsActivePayloads(
	ctx context.Context,
	slot uint64,
	hash []byte,
	currentEpoch uint64,
) (bool, error) {
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return false, err
	}
	sqlEpoch, err := checkedInt64(currentEpoch)
	if err != nil {
		return false, err
	}
	for _, q := range activePayloadQueries {
		args := []any{hash}
		if q.bySlot {
			args = []any{sqlSlot}
			if q.epoch {
				args = append(args, sqlEpoch)
			}
		}
		var held bool
		if err := s.readDB.QueryRowContext(
			ctx, s.dialect.Rebind(q.query), args...,
		).Scan(&held); err != nil {
			return false, fmt.Errorf("check %s: %w", q.name, err)
		}
		if held {
			return true, nil
		}
	}
	return false, nil
}
