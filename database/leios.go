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
// (received over leios-fetch MsgBlock) to the blob store, keyed by the exact
// (slot, hash) occurrence it was received under.
// key: "em" + hash(32) + slot(8 bytes big-endian) → value: manifest CBOR.
func (d *Database) SetLeiosEBManifest(
	slot uint64,
	hash []byte,
	manifestRaw []byte,
) error {
	txn := d.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck
	blob := txn.BlobStore()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return types.ErrNilTxn
	}
	if err := blob.Set(blobTxn, types.LeiosEBManifestKey(hash, slot), manifestRaw); err != nil {
		return fmt.Errorf("SetLeiosEBManifest: %w", err)
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf("SetLeiosEBManifest: commit: %w", err)
	}
	return nil
}

// GetLeiosEBManifest retrieves the raw Leios endorser-block manifest CBOR for
// the exact (slot, hash) occurrence named. Returns ErrBlobKeyNotFound when no
// manifest has been stored for that occurrence -- including when a manifest
// exists for the same hash under a different slot, since the manifest is
// content-addressed and that is a distinct occurrence (issue #3513 review).
//
// On a miss it also tries the pre-issue-#3513 legacy key (hash only), so
// data persisted by a node running before the key format changed does not
// become silently unreachable after an upgrade: that format could only ever
// hold one occurrence per hash, and its value carries that occurrence's slot
// as an 8-byte big-endian prefix, which must match the requested slot before
// the legacy record is trusted (cubic review).
func (d *Database) GetLeiosEBManifest(
	hash []byte,
	slot uint64,
) (manifestRaw []byte, err error) {
	txn := d.BlobTxn(false)
	defer txn.Rollback() //nolint:errcheck
	blob := txn.BlobStore()
	if blob == nil {
		return nil, types.ErrBlobStoreUnavailable
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return nil, types.ErrNilTxn
	}
	val, err := blob.Get(blobTxn, types.LeiosEBManifestKey(hash, slot))
	if err == nil {
		return val, nil
	}
	if !errors.Is(err, types.ErrBlobKeyNotFound) {
		return nil, err
	}
	legacyVal, legacyErr := blob.Get(
		blobTxn,
		types.LegacyLeiosEBManifestKey(hash),
	)
	if legacyErr != nil {
		// A real failure reading the legacy record (storage, network,
		// auth) must not be hidden behind the exact-key not-found error;
		// only "the legacy record doesn't exist either" collapses to that
		// (cubic review).
		if !errors.Is(legacyErr, types.ErrBlobKeyNotFound) {
			return nil, legacyErr
		}
		return nil, err
	}
	if len(legacyVal) < 8 || binary.BigEndian.Uint64(legacyVal[:8]) != slot {
		return nil, err
	}
	return legacyVal[8:], nil
}

// SetLeiosEBTxs persists the complete raw transaction bodies of a Leios
// endorser block to the blob store, keyed by the exact (slot, hash)
// occurrence. txsRaw is the CBOR-in-CBOR wrapped tx list from leios-fetch
// MsgBlockTxs, stored as a CBOR-encoded []cbor.RawMessage.
// Only call this when the transaction cache is complete (all txCount txs).
// key: "et" + hash(32) + slot(8) → value: CBOR-encoded []cbor.RawMessage.
func (d *Database) SetLeiosEBTxs(
	slot uint64,
	hash []byte,
	txsRaw []cbor.RawMessage,
) error {
	if txsRaw == nil {
		txsRaw = []cbor.RawMessage{}
	}
	txn := d.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck
	blob := txn.BlobStore()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return types.ErrNilTxn
	}
	val, err := cbor.Encode(txsRaw)
	if err != nil {
		return fmt.Errorf("SetLeiosEBTxs: encode txs: %w", err)
	}
	if err := blob.Set(blobTxn, types.LeiosEBTxsKey(hash, slot), val); err != nil {
		return fmt.Errorf("SetLeiosEBTxs: %w", err)
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf("SetLeiosEBTxs: commit: %w", err)
	}
	return nil
}

// GetLeiosEBTxs retrieves the raw transaction bodies for the exact (slot,
// hash) occurrence named. Returns ErrBlobKeyNotFound when no txs have been
// stored for that occurrence. The returned slice is in the same CBOR-in-CBOR
// wrapped format used by the leios-fetch MsgBlockTxs wire message.
//
// On a miss it also tries the pre-issue-#3513 legacy key (hash only, see
// GetLeiosEBManifest), gated on the legacy "em" record's embedded slot
// matching: the legacy format paired one "em" and one "et" record per hash
// (only one occurrence was ever trackable), so once that pairing is
// confirmed to be this occurrence, its "et" value is safe to use too (cubic
// review).
func (d *Database) GetLeiosEBTxs(
	hash []byte,
	slot uint64,
) ([]cbor.RawMessage, error) {
	txn := d.BlobTxn(false)
	defer txn.Rollback() //nolint:errcheck
	blob := txn.BlobStore()
	if blob == nil {
		return nil, types.ErrBlobStoreUnavailable
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return nil, types.ErrNilTxn
	}
	val, err := blob.Get(blobTxn, types.LeiosEBTxsKey(hash, slot))
	if err != nil {
		if !errors.Is(err, types.ErrBlobKeyNotFound) {
			return nil, err
		}
		legacyManifest, legacyErr := blob.Get(
			blobTxn,
			types.LegacyLeiosEBManifestKey(hash),
		)
		if legacyErr != nil {
			// See GetLeiosEBManifest: a real failure reading the legacy
			// manifest must not be hidden behind the exact-key not-found
			// error (cubic review).
			if !errors.Is(legacyErr, types.ErrBlobKeyNotFound) {
				return nil, legacyErr
			}
			return nil, err
		}
		if len(legacyManifest) < 8 ||
			binary.BigEndian.Uint64(legacyManifest[:8]) != slot {
			return nil, err
		}
		val, err = blob.Get(blobTxn, types.LegacyLeiosEBTxsKey(hash))
		if err != nil {
			return nil, err
		}
	}
	var txsRaw []cbor.RawMessage
	if _, err := cbor.Decode(val, &txsRaw); err != nil {
		return nil, fmt.Errorf("GetLeiosEBTxs: decode: %w", err)
	}
	return txsRaw, nil
}

// SetLeiosEB persists an endorser block's manifest and, when txsRaw is non-nil,
// its transaction bodies in a SINGLE blob-store transaction (one commit),
// merging what SetLeiosEBManifest + SetLeiosEBTxs do in two, for the exact
// (slot, hash) occurrence identified by slot and hash. The stored values are
// byte-identical to those setters, so GetLeiosEBManifest / GetLeiosEBTxs and
// the reload path are unchanged. Pass txsRaw==nil to write only the manifest
// (an incomplete endorser block); pass the complete tx set otherwise.
// Note the nil contract differs from SetLeiosEBTxs: SetLeiosEBTxs(nil) writes an
// empty tx list under the "et" key, whereas SetLeiosEB(..., nil) omits the "et"
// key entirely (manifest-only), so the two must not be treated as interchangeable
// nil handlers.
// Used by the asynchronous EB-persistence writer so historical-serving storage
// costs one commit per endorser block off the leios-fetch hot path.
func (d *Database) SetLeiosEB(
	slot uint64,
	hash []byte,
	manifestRaw []byte,
	txsRaw []cbor.RawMessage,
) error {
	txn := d.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck
	blob := txn.BlobStore()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return types.ErrNilTxn
	}
	if err := blob.Set(blobTxn, types.LeiosEBManifestKey(hash, slot), manifestRaw); err != nil {
		return fmt.Errorf("SetLeiosEB: manifest: %w", err)
	}
	if txsRaw != nil {
		txsVal, err := cbor.Encode(txsRaw)
		if err != nil {
			return fmt.Errorf("SetLeiosEB: encode txs: %w", err)
		}
		if err := blob.Set(blobTxn, types.LeiosEBTxsKey(hash, slot), txsVal); err != nil {
			return fmt.Errorf("SetLeiosEB: txs: %w", err)
		}
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf("SetLeiosEB: commit: %w", err)
	}
	return nil
}
