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

package gcs

import (
	"context"
	"io"
	"math/big"

	dingosops "github.com/blinklabs-io/dingo/database/sops"
)

const commitTimestampBlobKey = "metadata_commit_timestamp"

func (b *BlobStoreGCS) GetCommitTimestamp(ctx context.Context) (int64, error) {
	r, err := b.bucket.Object(commitTimestampBlobKey).NewReader(ctx)
	if err != nil {
		b.logger.Errorf("failed to read commit timestamp: %v", err)
		return 0, err
	}
	defer r.Close()

	ciphertext, err := io.ReadAll(r)
	if err != nil {
		b.logger.Errorf("failed to read commit timestamp object: %v", err)
		return 0, err
	}

	plaintext, err := dingosops.Decrypt(ciphertext)
	if err != nil {
		b.logger.Errorf("failed to decrypt commit timestamp: %v", err)
		return 0, err
	}

	return new(big.Int).SetBytes(plaintext).Int64(), nil
}

func (b *BlobStoreGCS) SetCommitTimestamp(
	ctx context.Context,
	timestamp int64,
) error {
	raw := new(big.Int).SetInt64(timestamp).Bytes()

	ciphertext, err := dingosops.Encrypt(raw)
	if err != nil {
		b.logger.Errorf("failed to encrypt commit timestamp: %v", err)
		return err
	}

	w := b.bucket.Object(commitTimestampBlobKey).NewWriter(ctx)
	if _, err := w.Write(ciphertext); err != nil {
		_ = w.Close()
		b.logger.Errorf("failed to write commit timestamp: %v", err)
		return err
	}
	if err := w.Close(); err != nil {
		b.logger.Errorf("failed to close writer: %v", err)
		return err
	}
	b.logger.Infof("commit timestamp %d written to GCS", timestamp)
	return nil
}
