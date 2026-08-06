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
	readTxn := d.Blob().NewTransaction(false)
	defer func() { _ = readTxn.Rollback() }()
	existing, err := d.Blob().Get(readTxn, blobStoreIDKey)
	switch {
	case err == nil && len(existing) > 0:
		return string(existing), nil
	case err != nil && !errors.Is(err, types.ErrBlobKeyNotFound):
		return "", fmt.Errorf("read blob store id: %w", err)
	}
	minted := uuid.NewString()
	writeTxn := d.Blob().NewTransaction(true)
	if err := d.Blob().Set(writeTxn, blobStoreIDKey, []byte(minted)); err != nil {
		_ = writeTxn.Rollback()
		return "", fmt.Errorf("mint blob store id: %w", err)
	}
	if err := writeTxn.Commit(); err != nil {
		_ = writeTxn.Rollback()
		return "", fmt.Errorf("commit blob store id: %w", err)
	}
	return minted, nil
}
