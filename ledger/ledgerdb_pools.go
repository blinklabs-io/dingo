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

package ledger

import (
	"context"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// setPoolRead serves a pool read from the set. Pool state always uses the
// set layout; an unavailable set family fails closed.
func setPoolRead[T any](
	s stagingLedgerDB,
	txn ledgerdb.Txn,
	read func(b *poolBook) (T, error),
) (v T, err error) {
	b, err := s.ls.poolBook(txn)
	if err != nil {
		return v, err
	}
	v, err = read(b)
	return v, err
}

// ListPoolKeyHashes reads from the set.
func (s stagingLedgerDB) ListPoolKeyHashes(
	txn ledgerdb.Txn,
) ([][]byte, error) {
	return setPoolRead(s, txn, func(b *poolBook) ([][]byte, error) {
		var out [][]byte
		err := b.withEvents(func(pool []byte, _ []PoolEvent) error {
			out = append(out, pool)
			return nil
		})
		return out, err
	})
}

// GetPools reads from the set.
func (s stagingLedgerDB) GetPools(
	ctx context.Context,
	pkhs []lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) ([]models.Pool, error) {
	return setPoolRead(s, txn, func(b *poolBook) ([]models.Pool, error) {
		return b.poolsByKey(pkhs)
	})
}

// GetPoolByVrfKeyHash reads from the set.
func (s stagingLedgerDB) GetPoolByVrfKeyHash(
	ctx context.Context,
	vrfKeyHash []byte,
	epochStartSlot uint64,
	txn ledgerdb.Txn,
) (*models.Pool, error) {
	return setPoolRead(s, txn, func(b *poolBook) (*models.Pool, error) {
		return b.poolByKey(vrfKeyHash, false, epochStartSlot, false)
	})
}

// GetFuturePoolByVrfKeyHash reads from the set.
func (s stagingLedgerDB) GetFuturePoolByVrfKeyHash(
	ctx context.Context,
	vrfKeyHash []byte,
	epochStartSlot uint64,
	txn ledgerdb.Txn,
) (*models.Pool, error) {
	return setPoolRead(s, txn, func(b *poolBook) (*models.Pool, error) {
		return b.poolByKey(vrfKeyHash, false, epochStartSlot, true)
	})
}

// GetPoolByLeiosKey reads from the set.
func (s stagingLedgerDB) GetPoolByLeiosKey(
	ctx context.Context,
	publicKey []byte,
	epochStartSlot uint64,
	txn ledgerdb.Txn,
) (*models.Pool, error) {
	return setPoolRead(s, txn, func(b *poolBook) (*models.Pool, error) {
		return b.poolByKey(publicKey, true, epochStartSlot, false)
	})
}

// GetFuturePoolByLeiosKey reads from the set.
func (s stagingLedgerDB) GetFuturePoolByLeiosKey(
	ctx context.Context,
	publicKey []byte,
	epochStartSlot uint64,
	txn ledgerdb.Txn,
) (*models.Pool, error) {
	return setPoolRead(s, txn, func(b *poolBook) (*models.Pool, error) {
		return b.poolByKey(publicKey, true, epochStartSlot, true)
	})
}

// GetActivePoolRelays reads from the set.
func (s stagingLedgerDB) GetActivePoolRelays(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([]models.PoolRegistrationRelay, error) {
	return setPoolRead(s, txn, func(b *poolBook) ([]models.PoolRegistrationRelay, error) {
		return b.relays()
	})
}

// GetPoolDepositsHeld reads from the set: the deposit each registered pool
// holds now.
func (s stagingLedgerDB) GetPoolDepositsHeld(
	poolKeyHashes []lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	return setPoolRead(s, txn, func(b *poolBook) (map[string]uint64, error) {
		return b.deposits(poolKeyHashes)
	})
}

// GetRetiringPools reads from the set.
func (s stagingLedgerDB) GetRetiringPools(
	currentEpoch uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRetiringRow, error) {
	return setPoolRead(s, txn, func(b *poolBook) ([]models.PoolRetiringRow, error) {
		return b.retiring(currentEpoch)
	})
}

// GetPoolKeyHashesRetiredByEpoch reads from the set.
func (s stagingLedgerDB) GetPoolKeyHashesRetiredByEpoch(
	epoch uint64,
	boundarySlot uint64,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	return setPoolRead(s, txn, func(b *poolBook) ([][]byte, error) {
		return b.retiredByEpoch(epoch, boundarySlot)
	})
}

// setPoolSource serves the database's pool reads from the set. It reads the
// committed set with no transaction: a caller holding a staged write reads
// through the ledger's LedgerDB instead.
type setPoolSource struct {
	ls *LedgerState
}

func poolSourceRead[T any](
	d setPoolSource,
	read func(b *poolBook) (T, error),
) (v T, err error) {
	b, err := d.ls.poolBook(nil)
	if err != nil {
		return v, err
	}
	return read(b)
}

func (d setPoolSource) Pool(
	pkh lcommon.PoolKeyHash,
	includeInactive bool,
) (*models.Pool, error) {
	return poolSourceRead(d, func(b *poolBook) (*models.Pool, error) {
		return b.pool(pkh[:], includeInactive)
	})
}

func (d setPoolSource) Pools(
	pkhs []lcommon.PoolKeyHash,
) ([]models.Pool, error) {
	return poolSourceRead(d, func(b *poolBook) ([]models.Pool, error) {
		return b.poolsByKey(pkhs)
	})
}

func (d setPoolSource) PoolByVrfKeyHash(
	vrf []byte,
	epochStartSlot uint64,
) (*models.Pool, error) {
	return poolSourceRead(d, func(b *poolBook) (*models.Pool, error) {
		return b.poolByKey(vrf, false, epochStartSlot, false)
	})
}

func (d setPoolSource) FuturePoolByVrfKeyHash(
	vrf []byte,
	epochStartSlot uint64,
) (*models.Pool, error) {
	return poolSourceRead(d, func(b *poolBook) (*models.Pool, error) {
		return b.poolByKey(vrf, false, epochStartSlot, true)
	})
}

func (d setPoolSource) PoolByLeiosKey(
	key []byte,
	epochStartSlot uint64,
) (*models.Pool, error) {
	return poolSourceRead(d, func(b *poolBook) (*models.Pool, error) {
		return b.poolByKey(key, true, epochStartSlot, false)
	})
}

func (d setPoolSource) FuturePoolByLeiosKey(
	key []byte,
	epochStartSlot uint64,
) (*models.Pool, error) {
	return poolSourceRead(d, func(b *poolBook) (*models.Pool, error) {
		return b.poolByKey(key, true, epochStartSlot, true)
	})
}

func (d setPoolSource) ActivePoolRelays() (
	[]models.PoolRegistrationRelay,
	error,
) {
	return poolSourceRead(
		d,
		func(b *poolBook) ([]models.PoolRegistrationRelay, error) {
			return b.relays()
		},
	)
}

func (d setPoolSource) ActivePoolKeyHashes() ([][]byte, error) {
	return poolSourceRead(d, func(b *poolBook) ([][]byte, error) {
		return b.active()
	})
}

func (d setPoolSource) PoolRegistrations(
	pkh lcommon.PoolKeyHash,
) ([]lcommon.PoolRegistrationCertificate, error) {
	return poolSourceRead(
		d,
		func(b *poolBook) ([]lcommon.PoolRegistrationCertificate, error) {
			return b.registrations(pkh)
		},
	)
}

func (d setPoolSource) PoolsRetiringAtEpoch(
	epoch, boundarySlot uint64,
) ([]models.PoolRetirementRefund, error) {
	return poolSourceRead(
		d,
		func(b *poolBook) ([]models.PoolRetirementRefund, error) {
			return b.retiringAtEpoch(epoch, boundarySlot)
		},
	)
}

var _ database.PoolSource = setPoolSource{}

// installPoolSource gives the database the set's pools for the reads it
// makes.
func (ls *LedgerState) installPoolSource() {
	if ls.db != nil {
		ls.db.SetPoolSource(setPoolSource{ls})
	}
}

// poolStatusFromEvents is the metadata store's GetPoolStatus over a pool's
// events: the newest registration and newest retirement in effective order
// decide. The retirement is pending unless the registration is ordered after
// it, and it has taken effect once its epoch is at most the current epoch,
// when the pool is no longer active. A synthetic retirement follows every
// other event of its slot, so it outranks a registration of the same slot.
func poolStatusFromEvents(
	events []PoolEvent,
	current uint64,
	haveCurrent bool,
) models.PoolStatus {
	var status models.PoolStatus
	if len(events) == 0 {
		return status
	}
	status.Exists = true
	var reg, ret *PoolEvent
	ordered := byEffectiveSlot(events)
	for i := range ordered {
		if ordered[i].Registration != nil {
			reg = &ordered[i]
		} else {
			ret = &ordered[i]
		}
	}
	if reg == nil {
		return status
	}
	status.Active = true
	if ret == nil || poolEventBeforeEffective(*ret, *reg) {
		return status
	}
	epoch := ret.RetiringEpoch
	if haveCurrent && epoch <= current {
		status.Active = false
		return status
	}
	status.RetirementEpoch = &epoch
	return status
}

// GetPoolStatus reads from the set, answering from the pool's events.
func (s stagingLedgerDB) GetPoolStatus(
	pkh lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (models.PoolStatus, error) {
	return setPoolRead(s, txn, func(b *poolBook) (models.PoolStatus, error) {
		events, err := b.events(pkh[:])
		if err != nil || len(events) == 0 {
			return models.PoolStatus{}, err
		}
		current, haveCurrent, err := b.currentEpoch()
		if err != nil {
			return models.PoolStatus{}, err
		}
		return poolStatusFromEvents(events, current, haveCurrent), nil
	})
}
