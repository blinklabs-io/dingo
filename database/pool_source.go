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

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// PoolSource answers the pool reads of a database whose pools-history family
// is drained, from the live UTxO set. It reads the committed set with no
// transaction.
type PoolSource interface {
	// Pool returns nil when the pool has no registration or retirement.
	Pool(pkh lcommon.PoolKeyHash, includeInactive bool) (*models.Pool, error)
	Pools(pkhs []lcommon.PoolKeyHash) ([]models.Pool, error)
	PoolByVrfKeyHash(vrf []byte, epochStartSlot uint64) (*models.Pool, error)
	FuturePoolByVrfKeyHash(
		vrf []byte,
		epochStartSlot uint64,
	) (*models.Pool, error)
	PoolByLeiosKey(key []byte, epochStartSlot uint64) (*models.Pool, error)
	FuturePoolByLeiosKey(
		key []byte,
		epochStartSlot uint64,
	) (*models.Pool, error)
	ActivePoolRelays() ([]models.PoolRegistrationRelay, error)
	ActivePoolKeyHashes() ([][]byte, error)
	PoolRegistrations(
		pkh lcommon.PoolKeyHash,
	) ([]lcommon.PoolRegistrationCertificate, error)
	PoolsRetiringAtEpoch(
		epoch, boundarySlot uint64,
	) ([]models.PoolRetirementRefund, error)
}

// ErrNoPoolSource is returned by a pool read of a database with no metadata
// store when no PoolSource was installed.
var ErrNoPoolSource = errors.New(
	"the database keeps no pools and no pool source is installed",
)

// SetPoolSource installs the source the pool reads of a database with no
// metadata store come from.
func (d *Database) SetPoolSource(src PoolSource) {
	d.poolSource.Store(&src)
}

// poolsFromSource reports whether pool reads come from the PoolSource.
func (d *Database) poolsFromSource() bool {
	return d.metadata == nil
}

func (d *Database) poolSourceOrErr() (PoolSource, error) {
	p := d.poolSource.Load()
	if p == nil || *p == nil {
		return nil, ErrNoPoolSource
	}
	return *p, nil
}
