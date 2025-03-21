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
	"fmt"
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
	// Get value from metadata
	metadataTimestamp, metadataErr := b.Metadata().GetCommitTimestamp()
	if metadataErr != nil {
		return fmt.Errorf(
			"failed to get metadata timestamp from plugin: %w",
			metadataErr,
		)
	}
	// No timestamp in the database
	if metadataTimestamp <= 0 {
		return nil
	}
	// Get value from blob
	blobTimestamp, blobErr := b.Blob().GetCommitTimestamp()
	if blobErr != nil {
		return fmt.Errorf(
			"failed to get blob timestamp from plugin: %w",
			blobErr,
		)
	}
	// Compare values
	if blobTimestamp != metadataTimestamp {
		return CommitTimestampError{
			MetadataTimestamp: metadataTimestamp,
			BlobTimestamp:     blobTimestamp,
		}
	}
	return nil
}

func (b *BaseDatabase) updateCommitTimestamp(txn *Txn, timestamp int64) error {
	// Update metadata
	if err := b.Metadata().SetCommitTimestamp(txn.Metadata(), timestamp); err != nil {
		return err
	}
	// Update blob
	if err := b.Blob().SetCommitTimestamp(txn.Blob(), timestamp); err != nil {
		return err
	}
	return nil
}
