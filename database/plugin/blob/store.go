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

package blob

import (
	"context"
	"fmt"
	"net/url"

	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// BlobStore defines the interface for a blob storage provider.
// All transactional methods (Get, Set, Delete, NewIterator, SetCommitTimestamp)
// require a non-nil types.Txn created by NewTransaction(). Passing nil will
// result in types.ErrNilTxn.
//
// Important: iterators returned by `NewIterator` yield `Item()` values that
// must only be accessed while the transaction used to create the iterator is
// still active. Implementations may validate transaction state at access time
// (for example `ValueCopy` may fail if the transaction has been committed or
// rolled back). Typical usage iterates and accesses item values within the
// same transaction scope.
type BlobStore interface {
	// Transaction management
	Close() error
	NewTransaction(bool) types.Txn

	// KV operations (plugins map these to their internals with transaction context)
	Get(txn types.Txn, key []byte) ([]byte, error)
	Set(txn types.Txn, key, val []byte) error
	Delete(txn types.Txn, key []byte) error
	NewIterator(
		txn types.Txn,
		opts types.BlobIteratorOptions,
	) types.BlobIterator

	// Commit timestamp management
	GetCommitTimestamp() (int64, error)
	// SetCommitTimestamp stores the last commit timestamp; parameter order is
	// (timestamp, txn) to keep the transaction as the final parameter.
	SetCommitTimestamp(int64, types.Txn) error

	// Block operations
	SetBlock(
		txn types.Txn,
		slot uint64,
		hash []byte,
		cbor []byte,
		id uint64,
		blockType uint,
		height uint64,
		prevHash []byte,
	) error
	GetBlock(
		txn types.Txn,
		slot uint64,
		hash []byte,
	) ([]byte, types.BlockMetadata, error)
	DeleteBlock(txn types.Txn, slot uint64, hash []byte, id uint64) error
	GetBlockURL(ctx context.Context, txn types.Txn, point ocommon.Point) (*url.URL, error)
	// UTxO operations
	SetUtxo(txn types.Txn, txId []byte, outputIdx uint32, cbor []byte) error
	GetUtxo(txn types.Txn, txId []byte, outputIdx uint32) ([]byte, error)
	DeleteUtxo(txn types.Txn, txId []byte, outputIdx uint32) error

	// Transaction operations
	SetTx(txn types.Txn, txHash []byte, offsetData []byte) error
	GetTx(txn types.Txn, txHash []byte) ([]byte, error)
	DeleteTx(txn types.Txn, txHash []byte) error
}

// New returns the started blob plugin selected by name
func New(pluginName string) (BlobStore, error) {
	// Get and start the plugin
	p, err := plugin.StartPlugin(plugin.PluginTypeBlob, pluginName)
	if err != nil {
		return nil, err
	}

	// Type assert to BlobStore interface
	blobStore, ok := p.(BlobStore)
	if !ok {
		return nil, fmt.Errorf(
			"plugin '%s' does not implement BlobStore interface",
			pluginName,
		)
	}

	return blobStore, nil
}
