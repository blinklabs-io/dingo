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

const addressTransactionRebuildBatchSize = 256

// RebuildAddressTransactionIndex upgrades legacy API address rows from the
// canonical transaction inventory captured by migration v46. The index is
// cleared before reconstruction and the source inventory is retained until
// every transaction has been rebuilt, so an interruption fails closed and
// restarts from the beginning.
func (s *Store) RebuildAddressTransactionIndex(
	ctx context.Context,
	rebuild func(context.Context, models.Transaction) ([]models.AddressTransaction, error),
) (retErr error) {
	if s.storageMode != types.StorageModeAPI {
		return nil
	}
	if rebuild == nil {
		return errors.New("address transaction rebuild callback is required")
	}
	var pending bool
	if err := s.readDB.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM address_transaction_rebuild_source)",
	).Scan(&pending); err != nil {
		return fmt.Errorf("check address transaction rebuild state: %w", err)
	}
	if !pending {
		return nil
	}
	var lockConn *sql.Conn
	var err error
	if s.dialect.Name() != "sqlite" {
		lockConn, err = s.readDB.Conn(ctx)
		if err != nil {
			return fmt.Errorf("reserve address transaction rebuild lock: %w", err)
		}
		defer func() { retErr = errors.Join(retErr, lockConn.Close()) }()
	}
	release, err := s.migrationLocker.Acquire(ctx, lockConn)
	if err != nil {
		return fmt.Errorf("acquire address transaction rebuild lock: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, release()) }()
	if err := s.readDB.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM address_transaction_rebuild_source)",
	).Scan(&pending); err != nil {
		return fmt.Errorf("recheck address transaction rebuild state: %w", err)
	}
	if !pending {
		return nil
	}
	if _, err := s.writeDB.ExecContext(ctx, "DELETE FROM address_transaction"); err != nil {
		return fmt.Errorf("clear legacy address transaction index: %w", err)
	}

	var cursor int64
	for {
		transactions, err := s.addressTransactionRebuildPage(ctx, cursor)
		if err != nil {
			return err
		}
		if len(transactions) == 0 {
			break
		}
		rows := &rowBatch{}
		for _, transaction := range transactions {
			if uint64(transaction.ID) > math.MaxInt64 || transaction.Slot > math.MaxInt64 {
				return fmt.Errorf("transaction %d position exceeds SQL integer range", transaction.ID)
			}
			transactionID := int64(transaction.ID)
			transactionSlot := int64(transaction.Slot)
			associations, err := rebuild(ctx, transaction)
			if err != nil {
				return fmt.Errorf(
					"rebuild address history for transaction %x at slot %d: %w",
					transaction.Hash, transaction.Slot, err,
				)
			}
			for _, association := range associations {
				rows.add(addressTransactionShape,
					association.PaymentKey,
					association.PaymentScript,
					association.StakingKey,
					association.CredentialTag,
					transactionID,
					transactionSlot,
					transaction.BlockIndex,
				)
			}
			cursor = transactionID
		}
		tx, err := s.writeDB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin address transaction rebuild batch: %w", err)
		}
		if err := rows.flush(
			ctx,
			newDialectQueryer(tx, s.dialect.Name()),
			s.dialect.ParameterLimit(),
		); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("write address transaction rebuild batch: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit address transaction rebuild batch: %w", err)
		}
	}
	if _, err := s.writeDB.ExecContext(ctx,
		"DELETE FROM address_transaction_rebuild_source",
	); err != nil {
		return fmt.Errorf("complete address transaction rebuild: %w", err)
	}
	return nil
}

func (s *Store) addressTransactionRebuildPage(
	ctx context.Context,
	cursor int64,
) ([]models.Transaction, error) {
	rows, err := s.readDB.QueryContext(ctx, s.dialect.Rebind(`
SELECT source.transaction_id, tx.hash, tx.block_hash, tx.slot, tx.block_index,
       tx.valid
FROM address_transaction_rebuild_source source
LEFT JOIN "transaction" tx ON tx.id = source.transaction_id
WHERE source.transaction_id > ?
ORDER BY source.transaction_id
LIMIT ?`), cursor, addressTransactionRebuildBatchSize)
	if err != nil {
		return nil, fmt.Errorf("read address transaction rebuild page: %w", err)
	}
	defer rows.Close()
	ret := make([]models.Transaction, 0, addressTransactionRebuildBatchSize)
	for rows.Next() {
		var (
			id        int64
			hash      []byte
			blockHash []byte
			slot      sql.NullInt64
			index     sql.NullInt64
			valid     sql.NullBool
		)
		if err := rows.Scan(&id, &hash, &blockHash, &slot, &index, &valid); err != nil {
			return nil, err
		}
		if len(hash) == 0 || len(blockHash) == 0 || !slot.Valid || !index.Valid ||
			id < 0 || slot.Int64 < 0 || index.Int64 < 0 ||
			uint64(index.Int64) > math.MaxUint32 {
			return nil, fmt.Errorf("incomplete transaction metadata for rebuild id %d", id)
		}
		ret = append(ret, models.Transaction{
			ID:         uint(id),
			Hash:       hash,
			BlockHash:  blockHash,
			Slot:       uint64(slot.Int64),
			BlockIndex: uint32(index.Int64),
			Valid:      valid.Bool,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ret, nil
}
