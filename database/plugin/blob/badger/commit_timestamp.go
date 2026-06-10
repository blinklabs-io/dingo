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
	"encoding/binary"
	"errors"
	"fmt"
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
		// Treat a missing key the same as the metadata stores treat a
		// missing row: no timestamp written yet, return 0. Returning
		// an error here turned a fresh blob into a fatal open failure
		// whenever metadata had any non-zero timestamp.
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to read commit timestamp: %w", err)
	}
	if len(val) == 8 {
		//nolint:gosec // Unix timestamps are always positive and within int64 range.
		return int64(binary.BigEndian.Uint64(val)), nil
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
	var tmpTimestamp [8]byte
	//nolint:gosec // Unix timestamps are always positive.
	timestampU64 := uint64(timestamp)
	binary.BigEndian.PutUint64(tmpTimestamp[:], timestampU64)
	if err := b.Set(txn, []byte(commitTimestampBlobKey), tmpTimestamp[:]); err != nil {
		return fmt.Errorf("failed to write commit timestamp: %w", err)
	}
	return nil
}
