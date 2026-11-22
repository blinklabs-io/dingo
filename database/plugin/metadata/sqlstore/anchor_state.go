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

// ReadImportedAccounts implements metadata.AnchorStateStore.
func (s *Store) ReadImportedAccounts(
	ctx context.Context,
	anchor uint64,
	cursor int64,
	limit int,
	txn types.Txn,
) ([]metadata.ImportedAccount, int64, error) {
	if limit <= 0 {
		return nil, 0, errors.New(
			"sqlstore: imported account read needs a positive limit",
		)
	}
	slot, err := checkedInt64(anchor)
	if err != nil {
		return nil, 0, err
	}
	db, txCtx, err := s.readDBFromTxn(txn) //nolint:contextcheck
	if err != nil {
		return nil, 0, fmt.Errorf("read imported accounts: %w", err)
	}
	if txn == nil {
		txCtx = ctx
	}
	rs, err := db.QueryContext(
		txCtx,
		s.dialect.Rebind(`-- name: ReadImportedAccounts :many
SELECT a.id, b.credential_tag, b.staking_key, b.pool, b.drep, b.drep_type,
    b.active, b.added_slot, b.deposit_amount, a.reward
FROM account_import_baseline b
JOIN account a
  ON a.credential_tag = b.credential_tag AND a.staking_key = b.staking_key
WHERE b.added_slot >= ? AND a.id > ?
ORDER BY a.id
LIMIT ?`),
		slot,
		cursor,
		limit,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("read imported accounts: %w", err)
	}
	defer rs.Close()
	var (
		out  []metadata.ImportedAccount
		last int64
	)
	for rs.Next() {
		var (
			id, drepType, added int64
			tag                 uint8
			active              bool
			pool, drep, key     []byte
			deposit, reward     sql.NullString
		)
		if err := rs.Scan(
			&id, &tag, &key, &pool, &drep, &drepType, &active, &added,
			&deposit, &reward,
		); err != nil {
			return nil, 0, fmt.Errorf("read imported accounts: %w", err)
		}
		acct := metadata.ImportedAccount{
			CredentialTag: tag, StakingKey: key, Pool: pool, Drep: drep,
			Active: active,
		}
		if acct.DrepType, err = checkedUint64(drepType); err != nil {
			return nil, 0, err
		}
		if acct.AddedSlot, err = checkedUint64(added); err != nil {
			return nil, 0, err
		}
		if deposit.Valid && deposit.String != "" {
			d, err := parseUint64("import deposit", deposit.String)
			if err != nil {
				return nil, 0, err
			}
			v := types.Uint64(d)
			acct.DepositAmount = &v
		}
		r, err := parseNullUint64("account reward", reward)
		if err != nil {
			return nil, 0, err
		}
		acct.Reward = types.Uint64(r)
		out = append(out, acct)
		last = id
	}
	if err := rs.Err(); err != nil {
		return nil, 0, err
	}
	if len(out) < limit {
		last = 0
	}
	return out, last, nil
}

// ReadRewardJournalAfter implements metadata.AnchorStateStore.
func (s *Store) ReadRewardJournalAfter(
	ctx context.Context,
	slot uint64,
	cursor int64,
	limit int,
	txn types.Txn,
) ([]metadata.RewardJournalEntry, int64, error) {
	if limit <= 0 {
		return nil, 0, errors.New(
			"sqlstore: reward journal read needs a positive limit",
		)
	}
	after, err := checkedInt64(slot)
	if err != nil {
		return nil, 0, err
	}
	db, txCtx, err := s.readDBFromTxn(txn) //nolint:contextcheck
	if err != nil {
		return nil, 0, fmt.Errorf("read reward journal: %w", err)
	}
	if txn == nil {
		txCtx = ctx
	}
	rs, err := db.QueryContext(
		txCtx,
		s.dialect.Rebind(`-- name: ReadRewardJournalAfter :many
SELECT id, credential_tag, staking_key, amount, withdrawal
FROM account_reward_delta
WHERE added_slot > ? AND id > ?
ORDER BY id
LIMIT ?`),
		after,
		cursor,
		limit,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("read reward journal: %w", err)
	}
	defer rs.Close()
	var (
		out  []metadata.RewardJournalEntry
		last int64
	)
	for rs.Next() {
		var (
			id         int64
			tag        uint8
			key        []byte
			amount     sql.NullString
			withdrawal bool
		)
		if err := rs.Scan(&id, &tag, &key, &amount, &withdrawal); err != nil {
			return nil, 0, fmt.Errorf("read reward journal: %w", err)
		}
		n, err := parseNullUint64("reward journal amount", amount)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, metadata.RewardJournalEntry{
			CredentialTag: tag, StakingKey: key, Amount: n, Withdrawal: withdrawal,
		})
		last = id
	}
	if err := rs.Err(); err != nil {
		return nil, 0, err
	}
	if len(out) < limit {
		last = 0
	}
	return out, last, nil
}

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

// LiveUtxoDigest implements metadata.AnchorStateStore.
func (s *Store) LiveUtxoDigest(
	ctx context.Context,
	atSlot uint64,
	txn types.Txn,
) (metadata.UtxoSetDigest, error) {
	return s.liveUtxoDigest(ctx, atSlot, nil, txn, utxoDigestPage)
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
SELECT t.hash, u.output_idx, u.amount,
    ` + uCredentialHash("payment_key_id") + `,
    ` + uCredentialHash("staking_key_id") + `,
    (SELECT d.hash FROM datum AS d WHERE d.id = u.datum_id)
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

// ReadOpCertCountersAt implements metadata.AnchorStateStore.
func (s *Store) ReadOpCertCountersAt(
	ctx context.Context,
	slot uint64,
	txn types.Txn,
) ([]metadata.OpCertCounter, error) {
	if s.tableDrained("pool_opcert_sequence") {
		return nil, errTableDrained("pool_opcert_sequence")
	}
	slotValue, err := checkedInt64(slot)
	if err != nil {
		return nil, err
	}
	db, txCtx, err := s.readDBFromTxn(txn) //nolint:contextcheck
	if err != nil {
		return nil, err
	}
	if txn == nil {
		txCtx = ctx
	}
	rs, err := db.QueryContext(
		txCtx,
		s.dialect.Rebind(`-- name: ReadOpCertCountersAt :many
SELECT pool_key_hash, sequence FROM pool_opcert_sequence
WHERE slot = ? ORDER BY pool_key_hash`),
		slotValue,
	)
	if err != nil {
		return nil, fmt.Errorf("read op-cert counters at slot %d: %w", slot, err)
	}
	defer rs.Close()
	var out []metadata.OpCertCounter
	for rs.Next() {
		var (
			pool []byte
			seq  int64
		)
		if err := rs.Scan(&pool, &seq); err != nil {
			return nil, fmt.Errorf(
				"read op-cert counters at slot %d: %w", slot, err,
			)
		}
		out = append(out, metadata.OpCertCounter{
			PoolKeyHash: pool, Sequence: decodeOpCertSequence(seq),
		})
	}
	return out, rs.Err()
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
