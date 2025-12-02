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
	"context"
	"encoding/json"
	"math/big"

	dingosops "github.com/blinklabs-io/dingo/database/sops"
)

const commitTimestampBlobKey = "metadata_commit_timestamp"

func (b *BlobStoreS3) GetCommitTimestamp(ctx context.Context) (int64, error) {
	ciphertext, err := b.Get(ctx, commitTimestampBlobKey)
	if err != nil {
		return 0, err
	}
	plaintext, err := dingosops.Decrypt(ciphertext)
	if err != nil {
		if !json.Valid(ciphertext) {
			ts := new(big.Int).SetBytes(ciphertext).Int64()
			b.logger.Warningf(
				"commit timestamp stored plaintext in S3, migrating to SOPS encryption: %v",
				err,
			)
			if migrateErr := b.SetCommitTimestamp(ctx, ts); migrateErr != nil {
				b.logger.Errorf(
					"failed to migrate plaintext commit timestamp: %v",
					migrateErr,
				)
			}
			return ts, nil
		}
		b.logger.Errorf("failed to decrypt commit timestamp: %v", err)
		return 0, err
	}
	return new(big.Int).SetBytes(plaintext).Int64(), nil
}

func (b *BlobStoreS3) SetCommitTimestamp(ctx context.Context, ts int64) error {
	raw := new(big.Int).SetInt64(ts).Bytes()
	ciphertext, err := dingosops.Encrypt(raw)
	if err != nil {
		b.logger.Errorf("failed to encrypt commit timestamp: %v", err)
		return err
	}
	if err := b.Put(ctx, commitTimestampBlobKey, ciphertext); err != nil {
		return err
	}
	b.logger.Infof("commit timestamp %d written to S3", ts)
	return nil
}
