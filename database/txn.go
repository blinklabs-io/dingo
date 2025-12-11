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

	"github.com/blinklabs-io/dingo/database/types"
)

// Txn is a wrapper that coordinates both metadata and blob transactions.
// Metadata and blob are first-class siblings, not nested.
type Txn struct {
	db          *Database
	blobTxn     types.Txn
	metadataTxn types.Txn
	lock        sync.Mutex
	finished    bool
	readWrite   bool
}

func NewTxn(db *Database, readWrite bool) *Txn {
	return &Txn{
		db:          db,
		readWrite:   readWrite,
		blobTxn:     db.Blob().NewTransaction(readWrite),
		metadataTxn: db.Metadata().Transaction(),
	}
}

func NewBlobOnlyTxn(db *Database, readWrite bool) *Txn {
	return &Txn{
		db:        db,
		readWrite: readWrite,
		blobTxn:   db.Blob().NewTransaction(readWrite),
	}
}

func NewMetadataOnlyTxn(db *Database, readWrite bool) *Txn {
	return &Txn{
		db:          db,
		readWrite:   readWrite,
		metadataTxn: db.Metadata().Transaction(),
	}
}

func (t *Txn) DB() *Database {
	return t.db
}

// Metadata returns the underlying metadata transaction handle
func (t *Txn) Metadata() types.Txn {
	return t.metadataTxn
}

// Blob returns the blob transaction handle
func (t *Txn) Blob() types.Txn {
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
	// Update the commit timestamp in both DBs if using both
	if t.blobTxn != nil && t.metadataTxn != nil {
		commitTimestamp := time.Now().UnixMilli()
		if err := t.db.updateCommitTimestamp(t, commitTimestamp); err != nil {
			if t.blobTxn != nil {
				_ = t.blobTxn.Rollback()
			}
			if t.metadataTxn != nil {
				_ = t.metadataTxn.Rollback()
			}
			return err
		}
	}
	// Commit blob transaction first (so if this fails, metadata never commits)
	if t.blobTxn != nil {
		if err := t.blobTxn.Commit(); err != nil {
			// Failed to commit blob DB, so rollback metadata txn
			if t.metadataTxn != nil {
				_ = t.metadataTxn.Rollback()
			}
			return err
		}
	}
	// Commit metadata transaction
	if t.metadataTxn != nil {
		if err := t.metadataTxn.Commit(); err != nil {
			if t.blobTxn != nil {
				_ = t.blobTxn.Rollback()
			}
			_ = t.metadataTxn.Rollback()
			return err
		}
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
	var rollbackErr error
	if t.blobTxn != nil {
		if err := t.blobTxn.Rollback(); err != nil {
			rollbackErr = err
		}
	}
	if t.metadataTxn != nil {
		if err := t.metadataTxn.Rollback(); err != nil {
			return err
		}
	}
	t.finished = true
	return rollbackErr
}
