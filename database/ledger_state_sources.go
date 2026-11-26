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

package database

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ErrNoMetadataStore is wrapped by the error of a read or write of SQL
// ledger state on the ledger's database, which keeps none: its ledger state is
// the LedgerDB's.
var ErrNoMetadataStore = errors.New(
	"the ledger's database keeps no metadata store",
)

func errNoMetadataStore(f nodesettings.Family) error {
	return fmt.Errorf("%w: %s", ErrNoMetadataStore, f)
}

// checkMetadataStore returns an error wrapping ErrNoMetadataStore when d keeps
// no metadata store, so a read cannot return an empty answer for f.
func (d *Database) checkMetadataStore(f nodesettings.Family) error {
	if d.metadata == nil {
		return errNoMetadataStore(f)
	}
	return nil
}

// checkCommitteeLive is checkMetadataStore for the committee and constitution
// family.
func (d *Database) checkCommitteeLive() error {
	return d.checkMetadataStore(nodesettings.FamilyCommitteeConstitution)
}

// NonceSource answers the block nonce reads a rollback of a database with no
// metadata store needs.
type NonceSource interface {
	// BlockNonceAt returns the nonce of the block at point, nil when none.
	BlockNonceAt(point ocommon.Point) ([]byte, error)
	// HasCheckpointAtOrBefore reports whether a valid checkpoint nonce exists
	// at or before slot.
	HasCheckpointAtOrBefore(slot uint64) (bool, error)
}

// ErrNoNonceSource is returned by a rollback of a database with no metadata
// store when no NonceSource was installed.
var ErrNoNonceSource = errors.New(
	"the database keeps no block nonces and no nonce source is installed",
)

// SetNonceSource installs the source a rollback of a database with no metadata
// store reads nonces from.
func (d *Database) SetNonceSource(src NonceSource) {
	d.nonceSource.Store(&src)
}

func (d *Database) nonceSourceOrErr() (NonceSource, error) {
	p := d.nonceSource.Load()
	if p == nil || *p == nil {
		return nil, ErrNoNonceSource
	}
	return *p, nil
}

func (d *Database) truncateTargetNonce(
	point ocommon.Point,
	txn *Txn,
) ([]byte, error) {
	if d.metadata != nil {
		return d.GetBlockNonce(point, txn)
	}
	src, err := d.nonceSourceOrErr()
	if err != nil {
		return nil, err
	}
	return src.BlockNonceAt(point)
}

func (d *Database) truncateHasNonceCheckpoint(
	slot uint64,
	txn *Txn,
) (bool, error) {
	if d.metadata != nil {
		return d.hasBlockNonceCheckpointAtOrBeforeSlot(slot, txn)
	}
	src, err := d.nonceSourceOrErr()
	if err != nil {
		return false, err
	}
	return src.HasCheckpointAtOrBefore(slot)
}

// deleteStoredBlockNoncesAfterPoint is the rollback's nonce delete. A database
// with no metadata store has no rows: the set drops its own nonces in the same
// rollback.
func (d *Database) deleteStoredBlockNoncesAfterPoint(
	point ocommon.Point,
	txn *Txn,
) error {
	if d.metadata == nil {
		return nil
	}
	return d.DeleteBlockNoncesAfterPoint(point, txn)
}

// requireStore refuses the SQL read or write the method named what makes on
// a database with no metadata store, as the ledger's is, rather than
// dereferencing the missing store.
func (d *Database) requireStore(what string) error {
	if d.metadata == nil {
		return fmt.Errorf("%w: %s", ErrNoMetadataStore, what)
	}
	return nil
}
