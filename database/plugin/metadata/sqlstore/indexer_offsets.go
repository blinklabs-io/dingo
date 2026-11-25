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
	"database/sql"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
)

var _ metadata.IndexerOffsetStore = (*Store)(nil)

// SetUtxoOffset stores the CBOR entry of output outputIdx of transaction
// txId, replacing any entry it has.
func (s *Store) SetUtxoOffset(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
	data []byte,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("set utxo offset: %w", err)
	}
	if err := s.operationalQueries(db).SetUtxoOffset(
		ctx,
		sqlitequery.SetUtxoOffsetParams{
			TxID: txId, OutputIdx: int64(outputIdx), Data: data,
		},
	); err != nil {
		return fmt.Errorf("set utxo offset: %w", err)
	}
	return nil
}

// GetUtxoOffset returns the CBOR entry of output outputIdx of transaction
// txId, and false when it has none.
func (s *Store) GetUtxoOffset(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
) ([]byte, bool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, false, fmt.Errorf("get utxo offset: %w", err)
	}
	data, err := s.operationalQueries(db).GetUtxoOffset(
		ctx,
		sqlitequery.GetUtxoOffsetParams{TxID: txId, OutputIdx: int64(outputIdx)},
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get utxo offset: %w", err)
	}
	return data, true, nil
}

// DeleteUtxoOffset removes the CBOR entry of output outputIdx of
// transaction txId. Removing an absent entry is not an error.
func (s *Store) DeleteUtxoOffset(
	txn types.Txn,
	txId []byte,
	outputIdx uint32,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("delete utxo offset: %w", err)
	}
	if err := s.operationalQueries(db).DeleteUtxoOffset(
		ctx,
		sqlitequery.DeleteUtxoOffsetParams{
			TxID: txId, OutputIdx: int64(outputIdx),
		},
	); err != nil {
		return fmt.Errorf("delete utxo offset: %w", err)
	}
	return nil
}

// SetTransactionOffset stores the CBOR entry of the transaction with hash
// txHash, replacing any entry it has.
func (s *Store) SetTransactionOffset(
	txn types.Txn,
	txHash []byte,
	data []byte,
) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("set transaction offset: %w", err)
	}
	if err := s.operationalQueries(db).SetTransactionOffset(
		ctx,
		sqlitequery.SetTransactionOffsetParams{Hash: txHash, Data: data},
	); err != nil {
		return fmt.Errorf("set transaction offset: %w", err)
	}
	return nil
}

// GetTransactionOffset returns the CBOR entry of the transaction with hash
// txHash, and false when it has none.
func (s *Store) GetTransactionOffset(
	txn types.Txn,
	txHash []byte,
) ([]byte, bool, error) {
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, false, fmt.Errorf("get transaction offset: %w", err)
	}
	data, err := s.operationalQueries(db).GetTransactionOffset(ctx, txHash)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get transaction offset: %w", err)
	}
	return data, true, nil
}

// DeleteTransactionOffset removes the CBOR entry of the transaction with
// hash txHash. Removing an absent entry is not an error.
func (s *Store) DeleteTransactionOffset(txn types.Txn, txHash []byte) error {
	db, ctx, err := s.dbFromTxn(txn)
	if err != nil {
		return fmt.Errorf("delete transaction offset: %w", err)
	}
	if err := s.operationalQueries(db).DeleteTransactionOffset(
		ctx, txHash,
	); err != nil {
		return fmt.Errorf("delete transaction offset: %w", err)
	}
	return nil
}
