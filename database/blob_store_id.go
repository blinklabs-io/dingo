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
	"github.com/google/uuid"
)

// blobStoreIDKey is the reserved blob key holding this store's identity. The
// value is opaque and is only ever compared for equality.
var blobStoreIDKey = []byte("nodesettings/storeid")

// blobStoreID returns the blob store's identity, minting and persisting one
// on first use.
//
// The identity deliberately is not the store's path or bucket name: mounts
// and container paths move legitimately, so a path comparison produces false
// alarms while still missing the case this guards, which is a metadata store
// paired with a blob store it was never initialised with.
func (d *Database) blobStoreID() (string, error) {
	// One pin for the whole mint: the read, the write, and the Sync that
	// makes the write durable all have to hit the same store, and that
	// store has to stay open until the Sync returns.
	store, releaseBlob := d.PinBlob()
	defer releaseBlob()
	if store == nil {
		return "", types.ErrBlobStoreUnavailable
	}
	readTxn := store.NewTransaction(false)
	defer func() { _ = readTxn.Rollback() }()
	existing, err := store.Get(readTxn, blobStoreIDKey)
	switch {
	case err == nil && len(existing) > 0:
		return string(existing), nil
	case err != nil && !errors.Is(err, types.ErrBlobKeyNotFound):
		return "", fmt.Errorf("read blob store id: %w", err)
	}
	minted := uuid.NewString()
	writeTxn := store.NewTransaction(true)
	if err := store.Set(writeTxn, blobStoreIDKey, []byte(minted)); err != nil {
		_ = writeTxn.Rollback()
		return "", fmt.Errorf("mint blob store id: %w", err)
	}
	if err := writeTxn.Commit(); err != nil {
		_ = writeTxn.Rollback()
		return "", fmt.Errorf("commit blob store id: %w", err)
	}
	// Force this identity write to disk before the caller can hand it to
	// writeGateValues, which latches it into the metadata store as a Frozen
	// gate. Badger is opened with SyncWrites=false (see the doc comment on
	// BlobStoreBadger.Sync), so without this call the commit above only
	// guarantees the key survives a process crash, not an unclean host
	// shutdown -- the same durability gap txn.go's combined commit path
	// closes with an identical Sync call between the blob commit and the
	// metadata commit that depends on it. Losing this specific key after the
	// gate has already latched is permanent: the next startup mints a new
	// id, which can never match the Frozen gate again.
	if err := store.Sync(); err != nil {
		return "", fmt.Errorf("sync blob store id: %w", err)
	}
	return minted, nil
}
