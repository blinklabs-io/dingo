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

package metadata

import "github.com/blinklabs-io/dingo/database/types"

// IndexerOffsetStore is the metadata capability an IndexerDB keeps its UTxO
// and transaction CBOR entries in: a 52-byte CborOffset into a block, or an
// output's raw CBOR once its block has expired. Entries are keyed by
// transaction and output identity. A nil txn runs each call on its own.
type IndexerOffsetStore interface {
	SetUtxoOffset(txn types.Txn, txId []byte, outputIdx uint32, data []byte) error
	// GetUtxoOffset reports false when the output has no entry.
	GetUtxoOffset(
		txn types.Txn,
		txId []byte,
		outputIdx uint32,
	) ([]byte, bool, error)
	DeleteUtxoOffset(txn types.Txn, txId []byte, outputIdx uint32) error
	SetTransactionOffset(txn types.Txn, txHash []byte, data []byte) error
	// GetTransactionOffset reports false when the transaction has no entry.
	GetTransactionOffset(txn types.Txn, txHash []byte) ([]byte, bool, error)
	DeleteTransactionOffset(txn types.Txn, txHash []byte) error
}
