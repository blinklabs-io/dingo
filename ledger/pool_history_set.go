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
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
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
	if _, mode := ls.utxoSetReader(); mode == UtxoSetReadsOff {
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

// samePoolCertificate compares a registration certificate the metadata store
// rebuilt with the set's registration: the fields consensus reads. The
// credential tag is not part of a certificate.
func samePoolCertificate(
	c lcommon.PoolRegistrationCertificate,
	r *models.PoolRegistration,
) bool {
	if r == nil || !bytes.Equal(c.Operator[:], r.PoolKeyHash) ||
		!bytes.Equal(c.VrfKeyHash[:], r.VrfKeyHash) ||
		c.Pledge != uint64(r.Pledge) || c.Cost != uint64(r.Cost) ||
		!bytes.Equal(c.RewardAccount[:], r.RewardAccount) ||
		len(c.PoolOwners) != len(r.Owners) {
		return false
	}
	if (c.Margin.Rat == nil) != (r.Margin == nil || r.Margin.Rat == nil) ||
		(c.Margin.Rat != nil && c.Margin.Cmp(r.Margin.Rat) != 0) {
		return false
	}
	if (c.PoolMetadata != nil) != (r.MetadataUrl != "") ||
		(c.PoolMetadata != nil &&
			(c.PoolMetadata.Url != r.MetadataUrl ||
				!bytes.Equal(c.PoolMetadata.Hash[:], r.MetadataHash))) {
		return false
	}
	owners := make(map[string]int, len(r.Owners))
	for _, o := range r.Owners {
		owners[string(o.KeyHash)]++
	}
	for _, o := range c.PoolOwners {
		owners[string(o[:])]--
	}
	for _, n := range owners {
		if n != 0 {
			return false
		}
	}
	return true
}

// verifyPoolHistory compares a read with the answer the pool's history
// gives. A set without its history, or reads off, are not compared.
func (s stagingLedgerDB) verifyPoolHistory(
	g verifyReadGuard,
	what string,
	pool []byte,
	txn ledgerdb.Txn,
	same func(events []PoolEvent) bool,
) {
	r := s.ls.poolHistoryReader(txn)
	if r == nil {
		return
	}
	events, err := s.ls.poolHistoryView(r, txn, pool)
	s.ls.comparePoolRead(
		g, fmt.Sprintf("pool %x %s", pool, what),
		err == nil && same(events), err,
	)
}

func (s stagingLedgerDB) GetPoolVrfKeyHashAtSlot(
	poolKeyHash []byte,
	slot uint64,
	txn ledgerdb.Txn,
) ([]byte, bool, error) {
	if a, ok, err := drainedPoolRead(s, txn, func(b *poolBook) (vrfAnswer, error) {
		events, err := b.events(poolKeyHash)
		if err != nil {
			return vrfAnswer{}, err
		}
		return vrfAnswerOf(latestPoolRegistrationAt(events, slot)), nil
	}); ok {
		return a.key, a.found, err
	}
	g := s.ls.verifyGuard(txn)
	want, found, err := s.LedgerDB.GetPoolVrfKeyHashAtSlot(
		poolKeyHash,
		slot,
		txn,
	)
	if err != nil {
		return want, found, err
	}
	s.verifyPoolHistory(g, fmt.Sprintf("VRF key at slot %d", slot),
		poolKeyHash, txn, func(events []PoolEvent) bool {
			reg := latestPoolRegistrationAt(events, slot)
			if reg == nil || !found {
				return (reg != nil) == found
			}
			return bytes.Equal(reg.VrfKeyHash, want)
		})
	return want, found, nil
}

func (s stagingLedgerDB) GetPoolEarliestVrfKeyHashAtSlot(
	poolKeyHash []byte,
	slot uint64,
	txn ledgerdb.Txn,
) ([]byte, bool, error) {
	if a, ok, err := drainedPoolRead(s, txn, func(b *poolBook) (vrfAnswer, error) {
		events, err := b.events(poolKeyHash)
		if err != nil {
			return vrfAnswer{}, err
		}
		return vrfAnswerOf(earliestPoolRegistrationAt(events, slot)), nil
	}); ok {
		return a.key, a.found, err
	}
	g := s.ls.verifyGuard(txn)
	want, found, err := s.LedgerDB.GetPoolEarliestVrfKeyHashAtSlot(
		poolKeyHash, slot, txn,
	)
	if err != nil {
		return want, found, err
	}
	s.verifyPoolHistory(g, fmt.Sprintf("earliest VRF key at slot %d", slot),
		poolKeyHash, txn, func(events []PoolEvent) bool {
			reg := earliestPoolRegistrationAt(events, slot)
			if reg == nil || !found {
				return (reg != nil) == found
			}
			return bytes.Equal(reg.VrfKeyHash, want)
		})
	return want, found, nil
}

// GetPoolVrfState is compared on whether the pool has a row, which any
// certificate creates, and on the latest registration's VRF key.
func (s stagingLedgerDB) GetPoolVrfState(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (*models.Pool, error) {
	if got, ok, err := drainedPoolRead(s, txn, func(b *poolBook) (*models.Pool, error) {
		return b.pool(pkh[:], true)
	}); ok {
		return notFoundIfNil(got, err)
	}
	g := s.ls.verifyGuard(txn)
	want, err := s.LedgerDB.GetPoolVrfState(ctx, pkh, txn)
	found := err == nil && want != nil
	if err != nil && !errors.Is(err, models.ErrPoolNotFound) {
		return want, err
	}
	s.verifyPoolHistory(
		g,
		"VRF state",
		pkh[:],
		txn,
		func(events []PoolEvent) bool {
			if !found || len(events) == 0 {
				return found == (len(events) > 0)
			}
			reg := latestPoolRegistrationAt(events, math.MaxUint64)
			if len(want.Registration) == 0 {
				return reg == nil
			}
			return reg != nil &&
				bytes.Equal(reg.VrfKeyHash, want.Registration[0].VrfKeyHash)
		},
	)
	return want, err
}

// GetPool is compared for a read that includes inactive pools, on whether
// the pool has a row. Which pools are active depends on the metadata store's
// tip epoch, which the set does not track, so that read is not compared.
func (s stagingLedgerDB) GetPool(
	ctx context.Context,
	pkh lcommon.PoolKeyHash,
	includeInactive bool,
	txn ledgerdb.Txn,
) (*models.Pool, error) {
	if got, ok, err := drainedPoolRead(s, txn, func(b *poolBook) (*models.Pool, error) {
		return b.pool(pkh[:], includeInactive)
	}); ok {
		return notFoundIfNil(got, err)
	}
	g := s.ls.verifyGuard(txn)
	want, err := s.LedgerDB.GetPool(ctx, pkh, includeInactive, txn)
	found := err == nil && want != nil
	if err != nil && !errors.Is(err, models.ErrPoolNotFound) {
		return want, err
	}
	if includeInactive {
		s.verifyPoolHistory(
			g,
			"row",
			pkh[:],
			txn,
			func(events []PoolEvent) bool {
				return found == (len(events) > 0)
			},
		)
	}
	return want, err
}

// GetPoolRegistrations is compared on every registration, newest first.
func (s stagingLedgerDB) GetPoolRegistrations(
	pkh lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) ([]lcommon.PoolRegistrationCertificate, error) {
	if got, ok, err := drainedPoolRead(s, txn, func(b *poolBook) ([]lcommon.PoolRegistrationCertificate, error) {
		return b.registrations(pkh)
	}); ok {
		return got, err
	}
	g := s.ls.verifyGuard(txn)
	want, err := s.LedgerDB.GetPoolRegistrations(pkh, txn)
	if err != nil {
		return want, err
	}
	s.verifyPoolHistory(
		g,
		"registrations",
		pkh[:],
		txn,
		func(events []PoolEvent) bool {
			var regs []*models.PoolRegistration
			for _, e := range slices.Backward(events) {
				if e.Registration != nil {
					regs = append(regs, e.Registration)
				}
			}
			if len(regs) != len(want) {
				return false
			}
			for i := range want {
				if !samePoolCertificate(want[i], regs[i]) {
					return false
				}
			}
			return true
		},
	)
	return want, nil
}

// GetPoolsRetiringAtEpoch is compared on the pools retiring, and on the
// reward account and deposit of each refund. The deposit is the one the
// pool's state holds now: the registrations carry none, and the read runs
// before the reap that removes it.
func (s stagingLedgerDB) GetPoolsRetiringAtEpoch(
	ctx context.Context,
	epoch uint64,
	boundarySlot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRetirementRefund, error) {
	if got, ok, err := drainedPoolRead(s, txn, func(b *poolBook) ([]models.PoolRetirementRefund, error) {
		return b.retiringAtEpoch(epoch, boundarySlot)
	}); ok {
		return got, err
	}
	if got, ok, err := s.retiringFromSet(txn, epoch, boundarySlot); err != nil {
		return nil, err
	} else if ok {
		return got, nil
	}
	g := s.ls.verifyGuard(txn)
	want, err := s.LedgerDB.GetPoolsRetiringAtEpoch(
		ctx,
		epoch,
		boundarySlot,
		txn,
	)
	if err != nil {
		return want, err
	}
	hr := s.ls.poolHistoryReader(txn)
	pr := s.ls.poolReader(txn)
	if hr == nil || pr == nil {
		return want, nil
	}
	got, setErr := s.retiringRefunds(hr, pr, txn, epoch, boundarySlot)
	same := setErr == nil && len(got) == len(want)
	if same {
		byPool := make(map[string]models.PoolRetirementRefund, len(got))
		for _, r := range got {
			byPool[string(r.PoolKeyHash)] = r
		}
		for _, w := range want {
			r, ok := byPool[string(w.PoolKeyHash)]
			if !ok || r.DepositHeld != w.DepositHeld ||
				r.RewardAccountCredentialTag != w.RewardAccountCredentialTag ||
				!bytes.Equal(r.RewardAccount, w.RewardAccount) {
				same = false
				break
			}
		}
	}
	s.ls.comparePoolRead(
		g, fmt.Sprintf("retiring at epoch %d", epoch), same, setErr,
	)
	return want, nil
}

// retiringFromSet answers GetPoolsRetiringAtEpoch from the set in set mode
// and reports whether it did. It stays on the store when a retiring pool's
// state is gone, because the deposit held is read from that state and the
// store's comes from the registration row, and on a node that certifies
// Leios, where the state's deposit follows apply order and the store's row is
// chosen in effective order. A drained family never reaches this read, since
// poolBook answers it. The refunds come in pool order; the store gives none.
func (s stagingLedgerDB) retiringFromSet(
	txn ledgerdb.Txn,
	epoch, boundarySlot uint64,
) ([]models.PoolRetirementRefund, bool, error) {
	pr := s.ls.poolServing(txn)
	if pr == nil || s.ls.leiosCertifies() {
		return nil, false, nil
	}
	hr := s.ls.poolHistoryReader(txn)
	if hr == nil {
		return nil, false, nil
	}
	got, err := s.retiringRefunds(hr, pr, txn, epoch, boundarySlot)
	if err != nil {
		return nil, false, err
	}
	if got == nil {
		got = []models.PoolRetirementRefund{}
	}
	for _, r := range got {
		st, err := s.ls.poolView(pr, txn, r.PoolKeyHash)
		if err != nil {
			return nil, false, err
		}
		if st == nil {
			return nil, false, nil
		}
	}
	slices.SortFunc(got, func(a, b models.PoolRetirementRefund) int {
		return bytes.Compare(a.PoolKeyHash, b.PoolKeyHash)
	})
	return got, true, nil
}

// retiringRefunds computes the refunds of the pools retiring at epoch from
// the registered pools, which is every pool the read can name: a pool is
// removed only at the boundary the read precedes.
func (s stagingLedgerDB) retiringRefunds(
	hr PoolHistoryReader,
	pr PoolReader,
	txn ledgerdb.Txn,
	epoch, boundary uint64,
) ([]models.PoolRetirementRefund, error) {
	pools, err := s.ls.poolKeys(pr, txn)
	if err != nil {
		return nil, err
	}
	var out []models.PoolRetirementRefund
	for _, p := range pools {
		events, err := s.ls.poolHistoryView(hr, txn, p)
		if err != nil {
			return nil, err
		}
		reg := retiringPoolRegistration(events, boundary, epoch)
		if reg == nil {
			continue
		}
		st, err := s.ls.poolView(pr, txn, p)
		if err != nil {
			return nil, err
		}
		var held uint64
		if st != nil && st.DepositHeld != nil {
			held = *st.DepositHeld
		}
		out = append(out, models.PoolRetirementRefund{
			PoolKeyHash:                p,
			RewardAccount:              reg.RewardAccount,
			RewardAccountCredentialTag: reg.RewardAccountCredentialTag,
			DepositHeld:                types.Uint64(held),
		})
	}
	return out, nil
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
