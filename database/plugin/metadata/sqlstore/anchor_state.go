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

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
)

var _ metadata.AnchorStateStore = (*Store)(nil)

// utxoDigestPage bounds the outputs read per statement while digesting.
const utxoDigestPage = 20_000

// WriteImportedAccounts implements metadata.AnchorStateStore.
func (s *Store) WriteImportedAccounts(
	accounts []metadata.ImportedAccount,
	txn types.Txn,
) error {
	if len(accounts) == 0 {
		return nil
	}
	if txn == nil {
		return errors.New(
			"sqlstore: imported accounts are written inside a transaction",
		)
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			for _, a := range accounts {
				drepType, err := checkedInt64(a.DrepType)
				if err != nil {
					return err
				}
				added, err := checkedInt64(a.AddedSlot)
				if err != nil {
					return err
				}
				var deposit any
				if a.DepositAmount != nil {
					deposit = decimalUint64(*a.DepositAmount)
				}
				if _, err := db.ExecContext(ctx, s.dialect.Rebind(`-- name: UpsertAccountImportBaseline :exec
INSERT INTO account_import_baseline (
    credential_tag, staking_key, pool, drep, drep_type, active, added_slot,
    deposit_amount
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (credential_tag, staking_key) DO UPDATE SET
    pool = excluded.pool,
    drep = excluded.drep,
    drep_type = excluded.drep_type,
    active = excluded.active,
    added_slot = excluded.added_slot,
    deposit_amount = excluded.deposit_amount`),
					a.CredentialTag, a.StakingKey, nullBytes(a.Pool),
					nullBytes(a.Drep), drepType, a.Active, added, deposit,
				); err != nil {
					return fmt.Errorf(
						"write imported account baseline: %w",
						err,
					)
				}
				if _, err := db.ExecContext(ctx, s.dialect.Rebind(`-- name: SetImportedAccountReward :exec
UPDATE account SET reward = ? WHERE credential_tag = ? AND staking_key = ?`),
					decimalUint64(a.Reward), a.CredentialTag, a.StakingKey,
				); err != nil {
					return fmt.Errorf("write imported account reward: %w", err)
				}
			}
			return nil
		},
	)
}

// LiveUtxoDigestWithOutputs implements metadata.AnchorStateStore.
func (s *Store) LiveUtxoDigestWithOutputs(
	ctx context.Context,
	atSlot uint64,
	resolve metadata.UtxoOutputResolver,
	txn types.Txn,
) (metadata.UtxoSetDigest, error) {
	return s.liveUtxoDigest(ctx, atSlot, resolve, txn, utxoDigestPage)
}

func (s *Store) liveUtxoDigest(
	ctx context.Context,
	atSlot uint64,
	resolve metadata.UtxoOutputResolver,
	txn types.Txn,
	page int,
) (metadata.UtxoSetDigest, error) {
	slot, err := checkedInt64(atSlot)
	if err != nil {
		return metadata.UtxoSetDigest{}, err
	}
	db, txCtx, err := s.readDBFromTxn(txn) //nolint:contextcheck
	if err != nil {
		return metadata.UtxoSetDigest{}, fmt.Errorf("digest utxo set: %w", err)
	}
	if txn == nil {
		txCtx = ctx
	}
	query := s.dialect.Rebind(`-- name: LiveUtxoPage :many
SELECT t.hash, u.output_idx, u.amount, u.payment_key, u.staking_key,
    u.datum_hash
FROM utxo u
JOIN ` + s.dialect.QuoteIdentifier("transaction") + ` t ON t.id = u.tx_id
WHERE (t.hash > ? OR (t.hash = ? AND u.output_idx > ?))
  AND u.added_slot <= ?
  AND (u.deleted_slot = 0 OR u.deleted_slot > ?)
ORDER BY t.hash, u.output_idx
LIMIT ?`)
	d := metadata.NewUtxoDigester()
	var (
		afterTx        = []byte{}
		afterIdx int64 = -1
	)
	for {
		if err := txCtx.Err(); err != nil {
			return metadata.UtxoSetDigest{}, err
		}
		n, lastTx, lastIdx, err := digestUtxoPage(
			txCtx, db, query, d, resolve, afterTx, afterIdx, slot, page,
		)
		if err != nil {
			return metadata.UtxoSetDigest{}, fmt.Errorf(
				"digest utxo set: %w",
				err,
			)
		}
		if n < page {
			break
		}
		afterTx, afterIdx = lastTx, lastIdx
	}
	return d.Sum(), nil
}

// AppliedTransactionCount implements metadata.AnchorStateStore.
func (s *Store) AppliedTransactionCount(
	ctx context.Context,
	txn types.Txn,
) (uint64, error) {
	db, txCtx, err := s.readDBFromTxn(txn) //nolint:contextcheck
	if err != nil {
		return 0, err
	}
	if txn == nil {
		txCtx = ctx
	}
	var n int64
	if err := db.QueryRowContext(
		txCtx, `SELECT COUNT(*) FROM applied_transaction`,
	).Scan(&n); err != nil {
		return 0, fmt.Errorf("count applied transactions: %w", err)
	}
	return checkedUint64(n)
}

// digestUtxoPage hashes the page of live outputs after (afterTx, afterIdx)
// into h and returns how many it read and the last key. The page is read in
// full before resolve runs, so the cursor is not held open across blob reads.
func digestUtxoPage(
	ctx context.Context,
	db queryer,
	query string,
	d *metadata.UtxoDigester,
	resolve metadata.UtxoOutputResolver,
	afterTx []byte,
	afterIdx int64,
	slot int64,
	page int,
) (n int, lastTx []byte, lastIdx int64, err error) {
	type liveOutput struct {
		txHash, payment, staking, datum []byte
		idx                             int64
		amount                          sql.NullString
	}
	rs, err := db.QueryContext(
		ctx, query, afterTx, afterTx, afterIdx, slot, slot, page,
	)
	if err != nil {
		return 0, nil, 0, err
	}
	defer rs.Close()
	outputs := make([]liveOutput, 0, page)
	for rs.Next() {
		var o liveOutput
		if err := rs.Scan(
			&o.txHash, &o.idx, &o.amount, &o.payment, &o.staking, &o.datum,
		); err != nil {
			return 0, nil, 0, err
		}
		outputs = append(outputs, o)
	}
	if err := rs.Err(); err != nil {
		return 0, nil, 0, err
	}
	for _, o := range outputs {
		var cbor []byte
		if resolve != nil {
			var err error
			cbor, err = resolve(o.txHash, uint32(o.idx)) //nolint:gosec
			if err != nil {
				return 0, nil, 0, fmt.Errorf(
					"resolve output %x#%d: %w", o.txHash, o.idx, err,
				)
			}
		}
		d.Add(
			o.txHash, uint32(o.idx), o.amount.String, //nolint:gosec
			o.payment, o.staking, o.datum, resolve != nil, cbor,
		)
		lastTx, lastIdx = o.txHash, o.idx
		n++
	}
	return n, lastTx, lastIdx, nil
}
