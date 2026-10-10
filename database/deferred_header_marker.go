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
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/types"
)

// Deferred-header markers live in the blob store, not in sync_state. They are
// written from the blockfetch handler, and a metadata write needs the single
// SQLite write connection, which a block-apply transaction holds for its whole
// length (an epoch-rollover snapshot can hold it for minutes). A blob write
// takes no part in that pool, so the handler is never parked behind it. The
// marker still reaches the blob store before the block it guards is added to
// the chain, so the two share one durability domain.

func deferredHeaderMarkerBlobKey(key string) []byte {
	return []byte(types.DeferredHeaderMarkerKeyPrefix + key)
}

// SetDeferredHeaderMarker durably records that the block named by key
// (`<slot>:<hash hex>`) has deferred header checks outstanding.
func (d *Database) SetDeferredHeaderMarker(key string) error {
	return d.SetDeferredHeaderMarkerWithValue(key, []byte{1})
}

// SetDeferredHeaderMarkerWithValue records an opaque marker payload with the
// same durability and isolation as SetDeferredHeaderMarker. The one-byte
// payload used by SetDeferredHeaderMarker remains the legacy unattributed
// encoding.
func (d *Database) SetDeferredHeaderMarkerWithValue(
	key string,
	value []byte,
) error {
	if len(value) == 0 {
		return errors.New("deferred header marker value is empty")
	}
	txn := d.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck
	store := txn.BlobStore()
	if store == nil {
		return types.ErrBlobStoreUnavailable
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return types.ErrNilTxn
	}
	if err := store.Set(
		blobTxn,
		deferredHeaderMarkerBlobKey(key),
		value,
	); err != nil {
		return fmt.Errorf("SetDeferredHeaderMarker(%q): %w", key, err)
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf("SetDeferredHeaderMarker(%q): commit: %w", key, err)
	}
	return nil
}

// GetDeferredHeaderMarkerValue returns a copy of a marker's opaque payload.
// found is false when no marker exists for key.
func (d *Database) GetDeferredHeaderMarkerValue(
	key string,
) (value []byte, found bool, err error) {
	txn := d.BlobTxn(false)
	defer txn.Rollback() //nolint:errcheck
	store := txn.BlobStore()
	if store == nil {
		return nil, false, nil
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return nil, false, types.ErrNilTxn
	}
	value, err = store.Get(blobTxn, deferredHeaderMarkerBlobKey(key))
	if err != nil {
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf(
			"GetDeferredHeaderMarkerValue(%q): %w", key, err,
		)
	}
	return append([]byte(nil), value...), true, nil
}

// HasDeferredHeaderMarker reports whether a marker exists for key. A database
// without a blob store has no markers.
func (d *Database) HasDeferredHeaderMarker(key string) (bool, error) {
	_, found, err := d.GetDeferredHeaderMarkerValue(key)
	return found, err
}

// DeleteDeferredHeaderMarker removes the marker for key. Deleting an absent
// marker is not an error.
func (d *Database) DeleteDeferredHeaderMarker(key string) error {
	txn := d.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck
	store := txn.BlobStore()
	if store == nil {
		return types.ErrBlobStoreUnavailable
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return types.ErrNilTxn
	}
	if err := store.Delete(
		blobTxn,
		deferredHeaderMarkerBlobKey(key),
	); err != nil && !errors.Is(err, types.ErrBlobKeyNotFound) {
		return fmt.Errorf("DeleteDeferredHeaderMarker(%q): %w", key, err)
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf(
			"DeleteDeferredHeaderMarker(%q): commit: %w", key, err,
		)
	}
	return nil
}

// ListDeferredHeaderMarkers returns the key of every persisted marker.
func (d *Database) ListDeferredHeaderMarkers() ([]string, error) {
	markers, err := d.ListDeferredHeaderMarkerValues()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(markers))
	for _, marker := range markers {
		keys = append(keys, marker.Key)
	}
	return keys, nil
}

// DeferredHeaderMarker is one persisted deferred-header marker and its opaque
// payload. The payload is byte{1} for a legacy unattributed marker.
type DeferredHeaderMarker struct {
	Key   string
	Value []byte
}

// ListDeferredHeaderMarkerValues returns every persisted marker and payload in
// one blob-store read transaction.
func (d *Database) ListDeferredHeaderMarkerValues() (
	[]DeferredHeaderMarker,
	error,
) {
	txn := d.BlobTxn(false)
	defer txn.Rollback() //nolint:errcheck
	store := txn.BlobStore()
	if store == nil {
		return nil, nil
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return nil, types.ErrNilTxn
	}
	prefix := []byte(types.DeferredHeaderMarkerKeyPrefix)
	it := store.NewIterator(
		blobTxn,
		types.BlobIteratorOptions{Prefix: prefix},
	)
	if it == nil {
		return nil, types.ErrBlobStoreUnavailable
	}
	defer it.Close()
	var markers []DeferredHeaderMarker
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		if item == nil {
			continue
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return nil, fmt.Errorf(
				"ListDeferredHeaderMarkerValues: copy value: %w", err,
			)
		}
		markers = append(markers, DeferredHeaderMarker{
			Key:   string(item.Key()[len(prefix):]),
			Value: value,
		})
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("ListDeferredHeaderMarkerValues: %w", err)
	}
	return markers, nil
}
