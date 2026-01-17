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
	t := &Txn{db: db, readWrite: readWrite}
	if bs := db.Blob(); bs != nil {
		t.blobTxn = bs.NewTransaction(readWrite)
	}
	if ms := db.Metadata(); ms != nil {
		t.metadataTxn = ms.Transaction()
		if t.metadataTxn == nil {
			db.logger.Warn(
				"metadata transaction is nil; callers must nil-check txn.Metadata()",
			)
		}
	}
	return t
}

func NewBlobOnlyTxn(db *Database, readWrite bool) *Txn {
	t := &Txn{db: db, readWrite: readWrite}
	if bs := db.Blob(); bs != nil {
		t.blobTxn = bs.NewTransaction(readWrite)
	}
	return t
}

func NewMetadataOnlyTxn(db *Database, readWrite bool) *Txn {
	t := &Txn{db: db, readWrite: readWrite}
	if ms := db.Metadata(); ms != nil {
		t.metadataTxn = ms.Transaction()
		if t.metadataTxn == nil {
			db.logger.Warn(
				"metadata transaction is nil; callers must nil-check txn.Metadata()",
			)
		}
	}
	return t
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
	// Fail fast if neither store is available for a read-write transaction
	if t.readWrite && t.blobTxn == nil && t.metadataTxn == nil {
		t.finished = true
		return types.ErrNoStoreAvailable
	}
	// No need to commit for read-only, but we do want to free up resources
	if !t.readWrite {
		return t.rollback()
	}
	// Update the commit timestamp in both DBs if using both
	if t.blobTxn != nil && t.metadataTxn != nil {
		commitTimestamp := time.Now().UnixMilli()
		if err := t.db.updateCommitTimestamp(t, commitTimestamp); err != nil {
			// Rollback both transactions on timestamp update failure
			_ = t.blobTxn.Rollback()
			_ = t.metadataTxn.Rollback()
			t.finished = true
			return fmt.Errorf("failed to update commit timestamp: %w", err)
		}
	}
	// Commit blob transaction first (so if this fails, metadata never commits)
	if t.blobTxn != nil {
		if err := t.blobTxn.Commit(); err != nil {
			// Blob commit failed - rollback metadata only
			// Note: Most DB engines auto-rollback on commit failure
			if t.metadataTxn != nil {
				_ = t.metadataTxn.Rollback()
			}
			t.finished = true
			return fmt.Errorf("blob commit failed: %w", err)
		}
	}
	// Commit metadata transaction
	if t.metadataTxn != nil {
		if err := t.metadataTxn.Commit(); err != nil {
			t.db.logger.Error(
				"partial commit: blob committed, metadata failed",
				"error", err,
			)
			_ = t.metadataTxn.Rollback()
			t.finished = true
			return fmt.Errorf(
				"partial commit: metadata commit failed after blob commit: %w",
				err,
			)
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
	var errs []error
	if t.blobTxn != nil {
		if err := t.blobTxn.Rollback(); err != nil {
			errs = append(errs, fmt.Errorf("blob rollback: %w", err))
		}
	}
	if t.metadataTxn != nil {
		if err := t.metadataTxn.Rollback(); err != nil {
			errs = append(errs, fmt.Errorf("metadata rollback: %w", err))
		}
	}
	t.finished = true
	return errors.Join(errs...)
}

// Release releases transaction resources. For read-only transactions, this
// releases locks and resources. For read-write transactions, this is equivalent
// to Rollback. Use this in defer statements for clean resource cleanup.
// Errors are logged but not returned, making this safe for deferred calls.
func (t *Txn) Release() {
	if err := t.Rollback(); err != nil {
		t.db.logger.Debug(
			"transaction release failed",
			"error", err,
			"read_write", t.readWrite,
		)
	}
}
