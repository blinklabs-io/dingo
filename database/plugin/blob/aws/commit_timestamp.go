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

package aws

import (
	"encoding/json"
	"math/big"
	"time"

	dingosops "github.com/blinklabs-io/dingo/database/sops"
	"github.com/blinklabs-io/dingo/database/types"
)

const commitTimestampBlobKey = "metadata_commit_timestamp"

func (b *BlobStoreS3) GetCommitTimestamp() (int64, error) {
	txn := b.NewTransaction(false)
	if txn == nil {
		return 0, types.ErrNilTxn
	}
	defer txn.Rollback() //nolint:errcheck // no-op for this backend

	ciphertext, err := b.Get(txn, []byte(commitTimestampBlobKey))
	if err != nil {
		return 0, err
	}
	plaintext, err := dingosops.Decrypt(ciphertext)
	if err != nil {
		// Check if this is legacy plaintext (int64 stored as bytes)
		// Plaintext timestamps are small byte arrays (<= 8 bytes) containing valid timestamps
		if len(ciphertext) <= 8 && len(ciphertext) > 0 &&
			!json.Valid(ciphertext) {
			ts := new(big.Int).SetBytes(ciphertext).Int64()
			// Validate timestamp is reasonable (post-2000, not in future)
			now := time.Now().UnixMilli()
			if ts > 946684800000 && ts <= now { // post-2000, not in future
				b.logger.Warningf(
					"commit timestamp stored plaintext in S3, migrating to SOPS encryption: %v",
					err,
				)
				// Create a new transaction for migration
				migrateTxn := b.NewTransaction(true)
				if migrateTxn == nil {
					b.logger.Errorf("failed to create migration transaction")
					return ts, nil
				}
				defer migrateTxn.Rollback() //nolint:errcheck
				if migrateErr := b.SetCommitTimestamp(ts, migrateTxn); migrateErr != nil {
					b.logger.Errorf(
						"failed to migrate plaintext commit timestamp: %v",
						migrateErr,
					)
				} else {
					if migrateErr := migrateTxn.Commit(); migrateErr != nil {
						b.logger.Errorf(
							"failed to commit plaintext commit timestamp migration: %v",
							migrateErr,
						)
					}
					// Rollback is safe no-op after successful commit
				}
				return ts, nil
			}
		}
		b.logger.Errorf("failed to decrypt commit timestamp: %v", err)
		return 0, err
	}
	return new(big.Int).SetBytes(plaintext).Int64(), nil
}

func (b *BlobStoreS3) SetCommitTimestamp(
	ts int64,
	txn types.Txn,
) error {
	if txn == nil {
		return types.ErrNilTxn
	}
	raw := new(big.Int).SetInt64(ts).Bytes()
	ciphertext, err := dingosops.Encrypt(raw)
	if err != nil {
		b.logger.Errorf("failed to encrypt commit timestamp: %v", err)
		return err
	}
	if err := b.Set(txn, []byte(commitTimestampBlobKey), ciphertext); err != nil {
		return err
	}
	b.logger.Infof("commit timestamp %d written to S3", ts)
	return nil
}
