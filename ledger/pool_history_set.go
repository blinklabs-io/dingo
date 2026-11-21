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
	"bytes"
	"context"
	"math"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// PoolEventSyntheticPosition positions a retirement a snapshot import wrote
// without a certificate: it follows every other event of its slot.
const PoolEventSyntheticPosition = math.MaxUint64

// PoolEventPosition orders the events of one slot as the metadata store
// does: by the transaction's index in the block, then the certificate's
// index in the transaction.
func PoolEventPosition(blockIndex, certIndex uint32) uint64 {
	return uint64(blockIndex)<<32 | uint64(certIndex)
}

// PoolEvent is a pool registration or retirement certificate, as the
// metadata store's pool_registration and pool_retirement rows keep it.
// Registration is nil for a retirement, which takes effect at RetiringEpoch.
//
// Slot is the slot the certificate was applied at, which journals and rolls
// it back. A certificate of a Leios closure takes effect at the closure's
// context slot instead, EffectiveSlot, which is zero when it is Slot.
type PoolEvent struct {
	Slot          uint64
	Position      uint64
	Registration  *models.PoolRegistration
	RetiringEpoch uint64
	EffectiveSlot uint64
}

// effective is the slot the certificate takes effect at.
func (e PoolEvent) effective() uint64 {
	if e.EffectiveSlot != 0 {
		return e.EffectiveSlot
	}
	return e.Slot
}

// poolEventBeforeEffective reports whether a is ordered before b by the
// order the metadata store's Leios-aware pool reads rank certificates in:
// effective slot, then applied slot, then position.
func poolEventBeforeEffective(a, b PoolEvent) bool {
	if a.effective() != b.effective() {
		return a.effective() < b.effective()
	}
	return poolEventBefore(a, b)
}

// byEffectiveSlot returns events in effective order.
func byEffectiveSlot(events []PoolEvent) []PoolEvent {
	out := slices.Clone(events)
	slices.SortStableFunc(out, func(a, b PoolEvent) int {
		switch {
		case poolEventBeforeEffective(a, b):
			return -1
		case poolEventBeforeEffective(b, a):
			return 1
		}
		return 0
	})
	return out
}

// PoolEventChange is a pool event a commit applied.
type PoolEventChange struct {
	Pool  []byte
	Event PoolEvent
}

// PoolHistoryReader serves pool certificate events from a live UTxO set.
type PoolHistoryReader interface {
	// PoolHistoryLoaded reports whether the reader holds every event up to
	// its tip; until it does, reads stay on the metadata store.
	PoolHistoryLoaded() (bool, error)
	// PoolHistory returns every event of pool, oldest first.
	PoolHistory(pool []byte) ([]PoolEvent, error)
}

// poolEventBefore reports whether a is ordered before b. Two events never
// share a slot and a position.
func poolEventBefore(a, b PoolEvent) bool {
	return a.Slot < b.Slot || (a.Slot == b.Slot && a.Position < b.Position)
}

// poolHistoryReader returns the set's pool history reader once it holds the
// history, or nil when reads stay on the metadata store.
func (ls *LedgerState) poolHistoryReader(txn ledgerdb.Txn) PoolHistoryReader {
	if ls.accountReader(txn) == nil || ls.utxoSetFailed.Load() {
		return nil
	}
	if _, mode := ls.utxoSetReader(); !mode {
		return nil
	}
	r, ok := ls.readSet(txn).(PoolHistoryReader)
	if !ok {
		return nil
	}
	if !ls.poolHistoryLoaded.Load() {
		loaded, err := r.PoolHistoryLoaded()
		if err != nil || !loaded {
			return nil
		}
		ls.poolHistoryLoaded.Store(true)
	}
	return r
}

// poolHistoryView returns pool's events as txn sees the set, oldest first:
// the committed events, cut back by each staged rollback, with the staged
// batches' events laid over them.
func (ls *LedgerState) poolHistoryView(
	r PoolHistoryReader,
	txn ledgerdb.Txn,
	pool []byte,
) ([]PoolEvent, error) {
	events, err := r.PoolHistory(pool)
	if err != nil {
		return nil, err
	}
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			events = slices.DeleteFunc(events, func(e PoolEvent) bool {
				return e.Slot > to
			})
		}
		for _, c := range o.poolEvents {
			if !bytes.Equal(c.Pool, pool) {
				continue
			}
			events = slices.DeleteFunc(events, func(e PoolEvent) bool {
				return e.Slot == c.Event.Slot && e.Position == c.Event.Position
			})
			events = append(events, c.Event)
		}
	}
	slices.SortFunc(events, func(a, b PoolEvent) int {
		if poolEventBefore(a, b) {
			return -1
		}
		if poolEventBefore(b, a) {
			return 1
		}
		return 0
	})
	return events, nil
}

// stagePoolEvent stages a pool certificate as a history event.
func (ls *LedgerState) stagePoolEvent(
	txn ledgerdb.Txn,
	pool []byte,
	ev PoolEvent,
) {
	ls.stageLedgerChanges(
		txn,
		LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			PoolEvents: []PoolEventChange{{Pool: bytes.Clone(pool), Event: ev}},
		},
	)
}

// latestPoolRegistrationAt is the registration the metadata store's
// GetPoolVrfKeyHashAtSlot selects: the newest at or before slot.
func latestPoolRegistrationAt(
	events []PoolEvent,
	slot uint64,
) *models.PoolRegistration {
	for _, e := range slices.Backward(events) {
		if e.Registration != nil && e.Slot <= slot {
			return e.Registration
		}
	}
	return nil
}

// registrationAtEffectiveSlot is e's registration with the added slot the
// Leios-aware reads report: the slot the certificate takes effect at.
func registrationAtEffectiveSlot(e *PoolEvent) *models.PoolRegistration {
	c := *e.Registration
	c.AddedSlot = e.effective()
	return &c
}

// latestPoolRegistrationEffectiveAt is the registration
// GetPoolRegistrationsAtSlot selects (sqlstore/pool.go:2988): the newest by
// effective order among those whose effective slot is at or before slot.
func latestPoolRegistrationEffectiveAt(
	events []PoolEvent,
	slot uint64,
) *models.PoolRegistration {
	for _, e := range slices.Backward(byEffectiveSlot(events)) {
		if e.Registration != nil && e.effective() <= slot {
			return registrationAtEffectiveSlot(&e)
		}
	}
	return nil
}

// earliestPoolRegistrationAt is the registration the metadata store's
// GetPoolEarliestVrfKeyHashAtSlot selects: the oldest at or before slot.
func earliestPoolRegistrationAt(
	events []PoolEvent,
	slot uint64,
) *models.PoolRegistration {
	for _, e := range events {
		if e.Registration != nil && e.Slot <= slot {
			return e.Registration
		}
	}
	return nil
}

// effectivePoolRegistration is the registration the metadata store's
// GetPoolRegistrationsEffectiveForEpoch (sqlstore/pool.go:3071) selects, with
// effective slots below epochStart (<) for the pre-epoch events and within
// [epochStart, snapshotSlot] for the fresh registration: the newest of the
// registrations and the latest retirement, when its epoch has ended, before
// epochStart decides, and a retirement or no event there leaves the oldest
// registration in [epochStart, snapshotSlot].
func effectivePoolRegistration(
	events []PoolEvent,
	epochStart, endedEpoch, snapshotSlot uint64,
) *models.PoolRegistration {
	var reg, ret *PoolEvent
	ordered := byEffectiveSlot(events)
	for i := range ordered {
		e := &ordered[i]
		if e.effective() >= epochStart {
			break
		}
		if e.Registration != nil {
			reg = e
		} else {
			ret = e
		}
	}
	if ret != nil && ret.RetiringEpoch > endedEpoch {
		ret = nil
	}
	if reg != nil && (ret == nil || poolEventBeforeEffective(*ret, *reg)) {
		return registrationAtEffectiveSlot(reg)
	}
	// The fresh registration is the earliest in apply order whose effective
	// slot lies in the window.
	for _, e := range events {
		if e.Registration != nil && e.effective() >= epochStart &&
			e.effective() <= snapshotSlot {
			return registrationAtEffectiveSlot(&e)
		}
	}
	return nil
}

// retiringPoolRegistration is the registration the metadata store's
// GetPoolsRetiringAtEpoch (sqlstore/pool.go:3541) refunds: the pool's latest
// registration whose effective slot is before boundary (<), when its latest retirement before boundary is for epoch and
// follows that registration.
func retiringPoolRegistration(
	events []PoolEvent,
	boundary, epoch uint64,
) *models.PoolRegistration {
	var reg, ret *PoolEvent
	ordered := byEffectiveSlot(events)
	for i := range ordered {
		e := &ordered[i]
		if e.effective() >= boundary {
			break
		}
		if e.Registration != nil {
			reg = e
		} else {
			ret = e
		}
	}
	if reg == nil || ret == nil || ret.RetiringEpoch != epoch ||
		poolEventBeforeEffective(*ret, *reg) {
		return nil
	}
	return reg.Registration
}

func (s stagingLedgerDB) GetPoolVrfKeyHashAtSlot(
	poolKeyHash []byte,
	slot uint64,
	txn ledgerdb.Txn,
) ([]byte, bool, error) {
	a, err := drainedPoolRead(s, txn, func(b *poolBook) (vrfAnswer, error) {
		events, err := b.events(poolKeyHash)
		if err != nil {
			return vrfAnswer{}, err
		}
		return vrfAnswerOf(latestPoolRegistrationAt(events, slot)), nil
	})
	return a.key, a.found, err
}

func (s stagingLedgerDB) GetPoolEarliestVrfKeyHashAtSlot(
	poolKeyHash []byte,
	slot uint64,
	txn ledgerdb.Txn,
) ([]byte, bool, error) {
	a, err := drainedPoolRead(s, txn, func(b *poolBook) (vrfAnswer, error) {
		events, err := b.events(poolKeyHash)
		if err != nil {
			return vrfAnswer{}, err
		}
		return vrfAnswerOf(earliestPoolRegistrationAt(events, slot)), nil
	})
	return a.key, a.found, err
}

// GetPoolVrfState returns the pool row and latest registration's VRF key.
func (s stagingLedgerDB) GetPoolVrfState(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (*models.Pool, error) {
	got, err := drainedPoolRead(s, txn, func(b *poolBook) (*models.Pool, error) {
		return b.pool(pkh[:], true)
	})
	return notFoundIfNil(got, err)
}

// GetPool returns a pool row from its registration and retirement history.
func (s stagingLedgerDB) GetPool(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	includeInactive bool,
	txn ledgerdb.Txn,
) (*models.Pool, error) {
	got, err := drainedPoolRead(s, txn, func(b *poolBook) (*models.Pool, error) {
		return b.pool(pkh[:], includeInactive)
	})
	return notFoundIfNil(got, err)
}

// GetPoolRegistrations returns the pool's registration certificates, newest first.
func (s stagingLedgerDB) GetPoolRegistrations(
	pkh lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) ([]lcommon.PoolRegistrationCertificate, error) {
	return drainedPoolRead(s, txn, func(b *poolBook) ([]lcommon.PoolRegistrationCertificate, error) {
		return b.registrations(pkh)
	})
}

// GetPoolsRetiringAtEpoch returns refunds for pools retiring at the boundary.
func (s stagingLedgerDB) GetPoolsRetiringAtEpoch(
	ctx context.Context,
	epoch uint64,
	boundarySlot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRetirementRefund, error) {
	return drainedPoolRead(s, txn, func(b *poolBook) ([]models.PoolRetirementRefund, error) {
		return b.retiringAtEpoch(epoch, boundarySlot)
	})
}

type vrfAnswer struct {
	key   []byte
	found bool
}

func vrfAnswerOf(reg *models.PoolRegistration) vrfAnswer {
	if reg == nil {
		return vrfAnswer{}
	}
	return vrfAnswer{key: bytes.Clone(reg.VrfKeyHash), found: true}
}

// notFoundIfNil gives a pool read the error the metadata store's readers give
// for a pool with no row.
func notFoundIfNil(p *models.Pool, err error) (*models.Pool, error) {
	if err == nil && p == nil {
		return nil, models.ErrPoolNotFound
	}
	return p, err
}
