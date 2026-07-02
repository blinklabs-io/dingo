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

package database

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
)

// SetLeiosEBManifest persists the raw Leios endorser-block manifest CBOR
// (received over leios-fetch MsgBlock) to the blob store. The value includes
// the slot as a prefix so it can be recovered on load without a separate index.
// key: "em" + hash(32) → value: slot(8 bytes big-endian) + manifest CBOR.
func (d *Database) SetLeiosEBManifest(slot uint64, hash []byte, manifestRaw []byte) error {
	blob := d.Blob()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}
	txn := d.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return types.ErrNilTxn
	}
	val := make([]byte, 8+len(manifestRaw))
	binary.BigEndian.PutUint64(val[:8], slot)
	copy(val[8:], manifestRaw)
	if err := blob.Set(blobTxn, types.LeiosEBManifestKey(hash), val); err != nil {
		return fmt.Errorf("SetLeiosEBManifest: %w", err)
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf("SetLeiosEBManifest: commit: %w", err)
	}
	return nil
}

// GetLeiosEBManifest retrieves the raw Leios endorser-block manifest CBOR and
// its slot by hash. Returns ErrBlobKeyNotFound when no manifest has been stored
// for this hash.
func (d *Database) GetLeiosEBManifest(hash []byte) (slot uint64, manifestRaw []byte, err error) {
	blob := d.Blob()
	if blob == nil {
		return 0, nil, types.ErrBlobStoreUnavailable
	}
	txn := d.BlobTxn(false)
	defer txn.Rollback() //nolint:errcheck
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return 0, nil, types.ErrNilTxn
	}
	val, err := blob.Get(blobTxn, types.LeiosEBManifestKey(hash))
	if err != nil {
		return 0, nil, err
	}
	if len(val) < 8 {
		return 0, nil, errors.New("GetLeiosEBManifest: stored value too short")
	}
	return binary.BigEndian.Uint64(val[:8]), val[8:], nil
}

// SetLeiosEBTxs persists the complete raw transaction bodies of a Leios
// endorser block to the blob store. txsRaw is the CBOR-in-CBOR wrapped tx list
// from leios-fetch MsgBlockTxs, stored as a CBOR-encoded []cbor.RawMessage.
// Only call this when the transaction cache is complete (all txCount txs).
// key: "et" + hash(32) → value: CBOR-encoded []cbor.RawMessage.
func (d *Database) SetLeiosEBTxs(hash []byte, txsRaw []cbor.RawMessage) error {
	if txsRaw == nil {
		txsRaw = []cbor.RawMessage{}
	}
	blob := d.Blob()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}
	txn := d.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return types.ErrNilTxn
	}
	val, err := cbor.Encode(txsRaw)
	if err != nil {
		return fmt.Errorf("SetLeiosEBTxs: encode txs: %w", err)
	}
	if err := blob.Set(blobTxn, types.LeiosEBTxsKey(hash), val); err != nil {
		return fmt.Errorf("SetLeiosEBTxs: %w", err)
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf("SetLeiosEBTxs: commit: %w", err)
	}
	return nil
}

// GetLeiosEBTxs retrieves the raw transaction bodies for a Leios endorser
// block by hash. Returns ErrBlobKeyNotFound when no txs have been stored for
// this hash. The returned slice is in the same CBOR-in-CBOR wrapped format
// used by the leios-fetch MsgBlockTxs wire message.
func (d *Database) GetLeiosEBTxs(hash []byte) ([]cbor.RawMessage, error) {
	blob := d.Blob()
	if blob == nil {
		return nil, types.ErrBlobStoreUnavailable
	}
	txn := d.BlobTxn(false)
	defer txn.Rollback() //nolint:errcheck
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return nil, types.ErrNilTxn
	}
	val, err := blob.Get(blobTxn, types.LeiosEBTxsKey(hash))
	if err != nil {
		return nil, err
	}
	var txsRaw []cbor.RawMessage
	if _, err := cbor.Decode(val, &txsRaw); err != nil {
		return nil, fmt.Errorf("GetLeiosEBTxs: decode: %w", err)
	}
	return txsRaw, nil
}
