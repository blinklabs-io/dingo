// Copyright 2025 Blink Labs Software
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

package badger

import (
	"math/big"

	"github.com/blinklabs-io/dingo/database/types"
)

const (
	commitTimestampBlobKey = "metadata_commit_timestamp"
)

func (b *BlobStoreBadger) GetCommitTimestamp() (int64, error) {
	txn := b.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck

	val, err := b.Get(txn, []byte(commitTimestampBlobKey))
	if err != nil {
		return 0, err
	}
	return new(big.Int).SetBytes(val).Int64(), nil
}

func (b *BlobStoreBadger) SetCommitTimestamp(
	timestamp int64,
	txn types.Txn,
) error {
	if txn == nil {
		return types.ErrNilTxn
	}
	// Update badger
	tmpTimestamp := new(big.Int).SetInt64(timestamp)
	if err := b.Set(txn, []byte(commitTimestampBlobKey), tmpTimestamp.Bytes()); err != nil {
		return err
	}
	return nil
}
