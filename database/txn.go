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
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"gorm.io/gorm"
)

// Txn is a wrapper around the transaction objects for the underlying DB engines
type Txn struct {
	lock        sync.Mutex
	finished    bool
	db          Database
	readWrite   bool
	blobTxn     *badger.Txn
	metadataTxn *gorm.DB
}

func NewTxn(db Database, readWrite bool) *Txn {
	return &Txn{
		db:          db,
		readWrite:   readWrite,
		blobTxn:     db.Blob().NewTransaction(readWrite),
		metadataTxn: db.Metadata().Transaction(),
	}
}

func (t *Txn) DB() Database {
	return t.db
}

func (t *Txn) Metadata() *gorm.DB {
	return t.metadataTxn
}

func (t *Txn) Blob() *badger.Txn {
	return t.blobTxn
}

// Do executes the specified function in the context of the transaction. Any errors returned will result
// in the transaction being rolled back
func (t *Txn) Do(fn func(*Txn) error) error {
	if err := fn(t); err != nil {
		if err2 := t.Rollback(); err2 != nil {
			return fmt.Errorf(
				"rollback failed: %w: original error: %w",
				err2,
				err,
			)
		}
		return err
	}
	if err := t.Commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}
	return nil
}

func (t *Txn) Commit() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.finished {
		return nil
	}
	// No need to commit for read-only, but we do want to free up resources
	if !t.readWrite {
		return t.rollback()
	}
	// Update the commit timestamp for both DBs
	commitTimestamp := time.Now().UnixMilli()
	if err := t.db.updateCommitTimestamp(t, commitTimestamp); err != nil {
		return err
	}
	// Commit sqlite transaction
	if result := t.metadataTxn.Commit(); result.Error != nil {
		// Failed to commit metadata DB, so discard blob txn
		t.blobTxn.Discard()
		return result.Error
	}
	// Commit badger transaction
	if err := t.blobTxn.Commit(); err != nil {
		return err
	}
	t.finished = true
	return nil
}

func (t *Txn) Rollback() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.rollback()
}

func (t *Txn) rollback() error {
	if t.finished {
		return nil
	}
	t.blobTxn.Discard()
	if result := t.metadataTxn.Rollback(); result.Error != nil {
		return result.Error
	}
	t.finished = true
	return nil
}
