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

package database

import (
	"errors"
	"fmt"
	"math/big"

	badger "github.com/dgraph-io/badger/v4"
)

const (
	commitTimestampBlobKey = "metadata_commit_timestamp"
)

type CommitTimestampError struct {
	MetadataTimestamp int64
	BlobTimestamp     int64
}

func (e CommitTimestampError) Error() string {
	return fmt.Sprintf(
		"commit timestamp mismatch: %d (metadata) != %d (blob)",
		e.MetadataTimestamp,
		e.BlobTimestamp,
	)
}

func (b *BaseDatabase) checkCommitTimestamp() error {
	// Get value from sqlite
	metadataTimestamp, metadataErr := b.Metadata().GetCommitTimestamp()
	if metadataErr != nil {
		return errors.New("failed to get metadata timestamp from plugin")
	}
	// No timestamp in the database
	if metadataTimestamp <= 0 {
		return nil
	}
	// Get value from badger
	err := b.Blob().View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(commitTimestampBlobKey))
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		tmpTimestamp := new(big.Int).SetBytes(val).Int64()
		// Compare values
		if tmpTimestamp != metadataTimestamp {
			return CommitTimestampError{
				MetadataTimestamp: metadataTimestamp,
				BlobTimestamp:     tmpTimestamp,
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (b *BaseDatabase) updateCommitTimestamp(txn *Txn, timestamp int64) error {
	// Update sqlite
	if err := b.Metadata().SetCommitTimestamp(txn.Metadata(), timestamp); err != nil {
		return err
	}
	// Update badger
	tmpTimestamp := new(big.Int).SetInt64(timestamp)
	if err := txn.Blob().Set([]byte(commitTimestampBlobKey), tmpTimestamp.Bytes()); err != nil {
		return err
	}
	return nil
}
