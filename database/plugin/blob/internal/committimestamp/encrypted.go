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

//go:build dingo_extra_plugins

package committimestamp

import (
	"encoding/json"
	"errors"
	"math/big"
	"time"

	dingosops "github.com/blinklabs-io/dingo/database/sops"
	"github.com/blinklabs-io/dingo/database/types"
)

const BlobKey = "metadata_commit_timestamp"

type encryptedStore interface {
	NewTransaction(bool) types.Txn
	Get(types.Txn, []byte) ([]byte, error)
	Set(types.Txn, []byte, []byte) error
	SetCommitTimestamp(int64, types.Txn) error
}

type logger interface {
	Infof(string, ...any)
	Warningf(string, ...any)
	Errorf(string, ...any)
}

func GetEncrypted(
	store encryptedStore,
	log logger,
	backend string,
) (int64, error) {
	txn := store.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck

	data, err := store.Get(txn, []byte(BlobKey))
	if err != nil {
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	if !dingosops.IsEnabled() {
		return DecodeLegacy(data)
	}

	plaintext, err := dingosops.Decrypt(data)
	if err == nil {
		return DecodeLegacy(plaintext)
	}
	if len(data) > 0 && len(data) <= 8 && !json.Valid(data) {
		ts, decodeErr := DecodeLegacy(data)
		now := time.Now().UnixMilli()
		if decodeErr == nil && ts > 946684800000 && ts <= now {
			log.Warningf(
				"commit timestamp stored plaintext in %s, migrating to SOPS encryption: %v",
				backend,
				err,
			)
			migrateTxn := store.NewTransaction(true)
			defer migrateTxn.Rollback() //nolint:errcheck
			if migrateErr := store.SetCommitTimestamp(ts, migrateTxn); migrateErr != nil {
				log.Errorf(
					"failed to migrate plaintext commit timestamp: %v",
					migrateErr,
				)
			} else if migrateErr := migrateTxn.Commit(); migrateErr != nil {
				log.Errorf(
					"failed to commit plaintext commit timestamp migration: %v",
					migrateErr,
				)
			}
			return ts, nil
		}
	}
	log.Errorf("failed to decrypt commit timestamp: %v", err)
	return 0, err
}

func SetEncrypted(
	store encryptedStore,
	log logger,
	backend string,
	ts int64,
	txn types.Txn,
) error {
	if txn == nil {
		return types.ErrNilTxn
	}
	raw := new(big.Int).SetInt64(ts).Bytes()
	if !dingosops.IsEnabled() {
		if err := store.Set(txn, []byte(BlobKey), raw); err != nil {
			return err
		}
		log.Infof("commit timestamp %d written to %s (plaintext)", ts, backend)
		return nil
	}

	ciphertext, err := dingosops.Encrypt(raw)
	if err != nil {
		log.Errorf("failed to encrypt commit timestamp: %v", err)
		return err
	}
	if err := store.Set(txn, []byte(BlobKey), ciphertext); err != nil {
		return err
	}
	log.Infof("commit timestamp %d written to %s", ts, backend)
	return nil
}
