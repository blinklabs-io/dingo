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
	"cmp"
	"math"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// PoolHistoryLister is implemented by a live UTxO set that can name every
// pool it holds certificate events for, including pools since reaped.
type PoolHistoryLister interface {
	PoolHistoryPools() ([][]byte, error)
}

// poolBook answers the reads of the pools-history family from the set once
// the metadata store holds no pool rows. It reads the committed set with the
// staged batches of txn laid over it.
type poolBook struct {
	ls     *LedgerState
	txn    ledgerdb.Txn
	state  PoolReader
	hist   PoolHistoryReader
	lister PoolHistoryLister
}

// poolBook returns the book for a drained pools-history family, or
// ErrDrainedFamilyUnserved when the set cannot answer every read.
func (ls *LedgerState) poolBook(txn ledgerdb.Txn) (*poolBook, error) {
	state := ls.poolServing(txn)
	hist := ls.poolHistoryServes(txn)
	if state == nil || hist == nil {
		return nil, drainedUnserved(nodesettings.FamilyPoolsHistory)
	}
	lister, ok := hist.(PoolHistoryLister)
	if !ok {
		return nil, drainedUnserved(nodesettings.FamilyPoolsHistory)
	}
	return &poolBook{
		ls: ls, txn: txn, state: state, hist: hist, lister: lister,
	}, nil
}

func (b *poolBook) events(pool []byte) ([]PoolEvent, error) {
	return b.ls.poolHistoryView(b.hist, b.txn, pool)
}

// pools returns every pool with a certificate event, sorted by key hash.
func (b *poolBook) pools() ([][]byte, error) {
	listed, err := b.lister.PoolHistoryPools()
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(listed))
	var out [][]byte
	add := func(p []byte) {
		if _, ok := seen[string(p)]; !ok {
			seen[string(p)] = struct{}{}
			out = append(out, bytes.Clone(p))
		}
	}
	for _, p := range listed {
		add(p)
	}
	for _, o := range b.ls.stagedOps(b.txn) {
		for _, c := range o.poolEvents {
			add(c.Pool)
		}
	}
	slices.SortFunc(out, bytes.Compare)
	return out, nil
}

// withEvents calls fn with each pool that still has an event, in key order.
func (b *poolBook) withEvents(
	fn func(pool []byte, events []PoolEvent) error,
) error {
	pools, err := b.pools()
	if err != nil {
		return err
	}
	for _, p := range pools {
		events, err := b.events(p)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			continue
		}
		if err := fn(p, events); err != nil {
			return err
		}
	}
	return nil
}

func (b *poolBook) epochs() ([]models.Epoch, error) {
	return b.ls.epochRows(b.txn)
}

// currentEpoch is the newest epoch row, the epoch the metadata store's
// active-pool reads compare retirements against.
func (b *poolBook) currentEpoch() (uint64, bool, error) {
	epochs, err := b.epochs()
	if err != nil {
		return 0, false, err
	}
	var cur uint64
	found := false
	for _, e := range epochs {
		if !found || e.EpochId > cur {
			cur, found = e.EpochId, true
		}
	}
	return cur, found, nil
}

// splitPoolEvents returns the newest registration and newest retirement of
// events, which are in chain order, and whether the newest event overall is
// the retirement.
func splitPoolEvents(
	events []PoolEvent,
) (reg, ret *PoolEvent, latestIsRetirement bool) {
	for i := range events {
		if events[i].Registration != nil {
			reg = &events[i]
		} else {
			ret = &events[i]
		}
	}
	if n := len(events); n > 0 {
		latestIsRetirement = events[n-1].Registration == nil
	}
	return reg, ret, latestIsRetirement
}

// retiredBefore reports whether retirement ret is ordered before the
// registration reg: an earlier slot, or the same slot and an earlier position
// for a retirement that carries a certificate position.
func retiredBefore(ret, reg *PoolEvent) bool {
	return ret.Slot < reg.Slot ||
		(ret.Slot == reg.Slot && ret.Position != PoolEventSyntheticPosition &&
			ret.Position < reg.Position)
}

// poolActiveAt is the metadata store's active-pool rule over the events at or
// before slot: a registration exists, and the newest retirement is absent,
// ordered before it, or for an epoch after epoch.
func poolActiveAt(events []PoolEvent, slot, epoch uint64) bool {
	var reg, ret *PoolEvent
	for i := range events {
		e := &events[i]
		if e.Slot > slot {
			break
		}
		if e.Registration != nil {
			reg = e
		} else {
			ret = e
		}
	}
	if reg == nil {
		return false
	}
	return ret == nil || retiredBefore(ret, reg) || ret.RetiringEpoch > epoch
}

// poolActiveEffectiveAt is the active-pool rule of the metadata store's
// activePoolKeyHashesAtSlotCTE (sqlstore/pool.go:2539): over the events whose
// effective slot is at or before slot (<=, lines 2557 and 2574), the newest
// registration in effective order is active when no retirement follows it in
// that order or the newest retirement is for an epoch after epoch.
func poolActiveEffectiveAt(events []PoolEvent, slot, epoch uint64) bool {
	var reg, ret *PoolEvent
	ordered := byEffectiveSlot(events)
	for i := range ordered {
		e := &ordered[i]
		if e.effective() > slot {
			break
		}
		if e.Registration != nil {
			reg = e
		} else {
			ret = e
		}
	}
	if reg == nil {
		return false
	}
	return ret == nil || poolEventBeforeEffective(*ret, *reg) ||
		ret.RetiringEpoch > epoch
}

// poolFromEvents builds the pool row the metadata store returns: the newest
// registration's fields, and the registrations and retirements newest first.
// Row IDs are absent; a certificate ID counts the pool's events in chain
// order so ties between them resolve as they do in the store. latestOnly keeps
// the newest registration and retirement only. It returns nil for a pool with
// no event.
func poolFromEvents(
	pool []byte,
	events []PoolEvent,
	latestOnly bool,
) *models.Pool {
	if len(events) == 0 {
		return nil
	}
	out := &models.Pool{PoolKeyHash: bytes.Clone(pool)}
	for i, e := range slices.Backward(events) {
		id := uint(i) + 1 //nolint:gosec // an event count fits a uint
		if e.Registration == nil {
			if latestOnly && len(out.Retirement) > 0 {
				continue
			}
			ret := models.PoolRetirement{
				PoolKeyHash: bytes.Clone(pool),
				Epoch:       e.RetiringEpoch,
				AddedSlot:   e.Slot,
			}
			if e.Position != PoolEventSyntheticPosition {
				ret.CertificateID = id
			}
			out.Retirement = append(out.Retirement, ret)
			continue
		}
		if latestOnly && len(out.Registration) > 0 {
			continue
		}
		reg := *CleanPoolRegistration(e.Registration)
		reg.PoolKeyHash = bytes.Clone(pool)
		reg.CertificateID = id
		if len(out.Registration) == 0 {
			out.Margin = reg.Margin
			out.VrfKeyHash = reg.VrfKeyHash
			out.RewardAccount = reg.RewardAccount
			out.RewardAccountCredentialTag = reg.RewardAccountCredentialTag
			out.Pledge = reg.Pledge
			out.Cost = reg.Cost
			out.LeiosKeyPublic = reg.LeiosKeyPublic
			out.LeiosKeyPossessionProof = reg.LeiosKeyPossessionProof
		}
		out.Registration = append(out.Registration, reg)
	}
	if out.Registration == nil {
		out.Registration = []models.PoolRegistration{}
	}
	if out.Retirement == nil {
		out.Retirement = []models.PoolRetirement{}
	}
	return out
}

// pool is GetPool: the pool with its newest registration and retirement. An
// inactive pool, one with no registration or whose newest event is a
// retirement already in effect, is nil unless includeInactive is set.
func (b *poolBook) pool(
	pool []byte,
	includeInactive bool,
) (*models.Pool, error) {
	events, err := b.events(pool)
	if err != nil {
		return nil, err
	}
	out := poolFromEvents(pool, events, true)
	if out == nil || includeInactive {
		return out, nil
	}
	if len(out.Registration) == 0 {
		return nil, nil
	}
	if len(out.Retirement) == 0 {
		return out, nil
	}
	cur, ok, err := b.currentEpoch()
	if err != nil {
		return nil, err
	}
	_, _, latestIsRetirement := splitPoolEvents(events)
	if ok && out.Retirement[0].Epoch <= cur && latestIsRetirement {
		return nil, nil
	}
	return out, nil
}

// poolsByKey is GetPools: every named pool with an event, with all of its
// registrations and retirements, each named once in request order.
func (b *poolBook) poolsByKey(
	pkhs []lcommon.PoolKeyHash,
) ([]models.Pool, error) {
	out := make([]models.Pool, 0, len(pkhs))
	seen := make(map[string]struct{}, len(pkhs))
	for _, pkh := range pkhs {
		if _, dup := seen[string(pkh[:])]; dup {
			continue
		}
		seen[string(pkh[:])] = struct{}{}
		events, err := b.events(pkh[:])
		if err != nil {
			return nil, err
		}
		if p := poolFromEvents(pkh[:], events, false); p != nil {
			out = append(out, *p)
		}
	}
	return out, nil
}

// poolByKey is the metadata store's lookup of the pool that claims a VRF or
// Leios key as of the epoch holding epochStartSlot. Each pool that ever
// registered the key is a candidate. A candidate's effective registration is
// its newest event before epochStartSlot, or, with none or a retirement that
// took effect, its first registration from epochStartSlot on; it is active
// unless a retirement ordered after that registration is for an epoch the
// bound epoch has reached. The current tier is the active pools whose
// effective registration names the key; the future tier is the active pools
// whose newest registration from epochStartSlot on names it while the
// effective one is another. Candidates rank by first event, then key hash.
func (b *poolBook) poolByKey(
	key []byte,
	leios bool,
	epochStartSlot uint64,
	future bool,
) (*models.Pool, error) {
	epochs, err := b.epochs()
	if err != nil {
		return nil, err
	}
	bound, haveBound := epochAt(epochs, epochStartSlot)
	claims := func(r *models.PoolRegistration) bool {
		if leios {
			return len(key) > 0 && bytes.Equal(r.LeiosKeyPublic, key)
		}
		return bytes.Equal(r.VrfKeyHash, key)
	}
	type candidate struct {
		pool   []byte
		events []PoolEvent
		first  uint64
	}
	var found []candidate
	err = b.withEvents(func(pool []byte, events []PoolEvent) error {
		named := false
		for _, e := range events {
			if e.Registration != nil && claims(e.Registration) {
				named = true
				break
			}
		}
		if !named {
			return nil
		}
		reg := b.effectiveForKey(
			events, epochStartSlot, bound.EpochId, haveBound,
		)
		var sameEpoch *PoolEvent
		for i := range events {
			if events[i].Registration != nil &&
				events[i].Slot >= epochStartSlot {
				sameEpoch = &events[i]
			}
		}
		if reg == nil || !b.keyActive(events, reg, bound.EpochId, haveBound) {
			return nil
		}
		hit := false
		if future {
			hit = sameEpoch != nil && claims(sameEpoch.Registration) &&
				sameEpoch != reg
		} else {
			hit = claims(reg.Registration)
		}
		if hit {
			found = append(found, candidate{
				pool: pool, events: events, first: events[0].Slot,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(found) == 0 {
		return nil, nil
	}
	slices.SortStableFunc(found, func(a, c candidate) int {
		return cmp.Or(
			cmp.Compare(a.first, c.first),
			bytes.Compare(a.pool, c.pool),
		)
	})
	return poolFromEvents(found[0].pool, found[0].events, true), nil
}

// effectiveForKey is the candidate's effective registration for the key
// lookup, or nil when it has none.
func (b *poolBook) effectiveForKey(
	events []PoolEvent,
	epochStartSlot, boundEpoch uint64,
	haveBound bool,
) *PoolEvent {
	var state *PoolEvent
	var retiring *PoolEvent
	for i := range events {
		e := &events[i]
		if e.Slot >= epochStartSlot {
			break
		}
		if e.Registration != nil {
			state = e
			continue
		}
		retiring = e
	}
	// Only the newest retirement before the boundary counts, and only once
	// the bound epoch has reached its epoch.
	if retiring != nil && haveBound && retiring.RetiringEpoch <= boundEpoch {
		if state == nil || !retiredBefore(retiring, state) {
			state = retiring
		}
	}
	if state != nil && state.Registration != nil {
		return state
	}
	for i := range events {
		if events[i].Registration != nil && events[i].Slot >= epochStartSlot {
			return &events[i]
		}
	}
	return nil
}

// keyActive reports whether the pool is active as the key lookup sees it: no
// retirement, one ordered before the effective registration, or one for an
// epoch the bound epoch has not reached.
func (b *poolBook) keyActive(
	events []PoolEvent,
	reg *PoolEvent,
	boundEpoch uint64,
	haveBound bool,
) bool {
	_, ret, _ := splitPoolEvents(events)
	return ret == nil || retiredBefore(ret, reg) ||
		!haveBound || ret.RetiringEpoch > boundEpoch
}

// activeAt is the active pools at slot with retirements compared against
// epoch.
func (b *poolBook) activeAt(slot, epoch uint64) ([][]byte, error) {
	out := [][]byte{}
	err := b.withEvents(func(pool []byte, events []PoolEvent) error {
		if poolActiveEffectiveAt(events, slot, epoch) {
			out = append(out, pool)
		}
		return nil
	})
	return out, err
}

// active is the active pools as of the newest epoch row and every event.
func (b *poolBook) active() ([][]byte, error) {
	cur, ok, err := b.currentEpoch()
	if err != nil {
		return nil, err
	}
	if !ok {
		return [][]byte{}, nil
	}
	return b.activeAt(math.MaxUint64, cur)
}

// activeAtSlot is GetActivePoolKeyHashesAtSlot.
func (b *poolBook) activeAtSlot(slot uint64) ([][]byte, error) {
	epochs, err := b.epochs()
	if err != nil {
		return nil, err
	}
	epoch, err := activePoolEpoch(epochs, slot, slot)
	if err != nil {
		return nil, err
	}
	return b.activeAt(slot, epoch)
}

// relays is GetActivePoolRelays: the relays of each active pool's newest
// registration, in pool order.
func (b *poolBook) relays() ([]models.PoolRegistrationRelay, error) {
	cur, ok, err := b.currentEpoch()
	if err != nil {
		return nil, err
	}
	out := []models.PoolRegistrationRelay{}
	if !ok {
		return out, nil
	}
	err = b.withEvents(func(pool []byte, events []PoolEvent) error {
		reg, _, _ := splitPoolEvents(events)
		if reg == nil || !poolActiveAt(events, math.MaxUint64, cur) {
			return nil
		}
		for _, r := range reg.Registration.Relays {
			r.PoolKeyHash = bytes.Clone(pool)
			out = append(out, r)
		}
		return nil
	})
	return out, err
}

// registrations is GetPoolRegistrations: the registration certificates,
// newest first.
func (b *poolBook) registrations(
	pkh lcommon.PoolKeyHash,
) ([]lcommon.PoolRegistrationCertificate, error) {
	events, err := b.events(pkh[:])
	if err != nil {
		return nil, err
	}
	pool := models.Pool{PoolKeyHash: pkh[:]}
	out := []lcommon.PoolRegistrationCertificate{}
	for _, e := range slices.Backward(events) {
		if e.Registration == nil {
			continue
		}
		cert, err := poolRegistrationCertificate(&pool, e.Registration)
		if err != nil {
			return nil, err
		}
		if len(e.Registration.LeiosKeyPublic) > 0 {
			cert.LeiosKey = &lcommon.LeiosKey{
				PublicKey:       e.Registration.LeiosKeyPublic,
				PossessionProof: e.Registration.LeiosKeyPossessionProof,
			}
		}
		out = append(out, *cert)
	}
	return out, nil
}

// retiring is GetRetiringPools: the pools whose newest event is a retirement
// for an epoch after currentEpoch, ordered by epoch, slot, position and key.
func (b *poolBook) retiring(
	currentEpoch uint64,
) ([]models.PoolRetiringRow, error) {
	type row struct {
		models.PoolRetiringRow
		slot, pos uint64
	}
	var rows []row
	err := b.withEvents(func(pool []byte, events []PoolEvent) error {
		reg, ret, _ := splitPoolEvents(events)
		if ret == nil || ret.RetiringEpoch <= currentEpoch ||
			(reg != nil && !poolEventBefore(*reg, *ret)) {
			return nil
		}
		rows = append(rows, row{
			PoolRetiringRow: models.PoolRetiringRow{
				PoolKeyHash: pool, Epoch: ret.RetiringEpoch,
			},
			slot: ret.Slot, pos: ret.Position,
		})
		return nil
	})
	slices.SortFunc(rows, func(a, c row) int {
		return cmp.Or(
			cmp.Compare(a.Epoch, c.Epoch),
			cmp.Compare(a.slot, c.slot),
			cmp.Compare(a.pos, c.pos),
			bytes.Compare(a.PoolKeyHash, c.PoolKeyHash),
		)
	})
	out := make([]models.PoolRetiringRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.PoolRetiringRow)
	}
	return out, err
}

// retiredByEpoch is GetPoolKeyHashesRetiredByEpoch: the pools whose newest
// event before boundarySlot is a retirement for epoch or earlier.
func (b *poolBook) retiredByEpoch(
	epoch, boundarySlot uint64,
) ([][]byte, error) {
	out := [][]byte{}
	err := b.withEvents(func(pool []byte, events []PoolEvent) error {
		var reg, ret *PoolEvent
		for i := range events {
			e := &events[i]
			if e.Slot >= boundarySlot {
				break
			}
			if e.Registration != nil {
				reg = e
			} else {
				ret = e
			}
		}
		if reg == nil || ret == nil || ret.RetiringEpoch > epoch ||
			retiredBefore(ret, reg) {
			return nil
		}
		out = append(out, pool)
		return nil
	})
	return out, err
}

// deposits is GetPoolDepositsHeld from the pools registered now.
func (b *poolBook) deposits(
	pkhs []lcommon.PoolKeyHash,
) (map[string]uint64, error) {
	out := make(map[string]uint64, len(pkhs))
	for _, pkh := range pkhs {
		events, err := b.events(pkh[:])
		if err != nil {
			return nil, err
		}
		if len(events) == 0 {
			continue
		}
		st, err := b.ls.poolView(b.state, b.txn, pkh[:])
		if err != nil {
			return nil, err
		}
		if st != nil && st.DepositHeld != nil {
			out[string(pkh[:])] = *st.DepositHeld
		}
	}
	return out, nil
}

// effective is GetPoolRegistrationsEffectiveForEpoch over the history, for any
// snapshot slot.
func (b *poolBook) effective(
	pkhs []lcommon.PoolKeyHash,
	epochStartSlot, endedEpoch, snapshotSlot uint64,
) ([]models.PoolRegistration, error) {
	out := []models.PoolRegistration{}
	seen := make(map[string]struct{}, len(pkhs))
	for _, pkh := range pkhs {
		if _, dup := seen[string(pkh[:])]; dup {
			continue
		}
		seen[string(pkh[:])] = struct{}{}
		events, err := b.events(pkh[:])
		if err != nil {
			return nil, err
		}
		if reg := effectivePoolRegistration(
			events, epochStartSlot, endedEpoch, snapshotSlot,
		); reg != nil {
			c := *CleanPoolRegistration(reg)
			c.PoolKeyHash = bytes.Clone(pkh[:])
			out = append(out, c)
		}
	}
	return out, nil
}

// retiringAtEpoch is GetPoolsRetiringAtEpoch for a drained family. A retiring
// pool whose state is gone cannot be refunded from the set.
func (b *poolBook) retiringAtEpoch(
	epoch, boundary uint64,
) ([]models.PoolRetirementRefund, error) {
	out := []models.PoolRetirementRefund{}
	err := b.withEvents(func(pool []byte, events []PoolEvent) error {
		reg := retiringPoolRegistration(events, boundary, epoch)
		if reg == nil {
			return nil
		}
		st, err := b.ls.poolView(b.state, b.txn, pool)
		if err != nil {
			return err
		}
		if st == nil {
			return drainedUnserved(nodesettings.FamilyPoolsHistory)
		}
		var held uint64
		if st.DepositHeld != nil {
			held = *st.DepositHeld
		}
		out = append(out, models.PoolRetirementRefund{
			PoolKeyHash:                bytes.Clone(pool),
			RewardAccount:              reg.RewardAccount,
			RewardAccountCredentialTag: reg.RewardAccountCredentialTag,
			DepositHeld:                types.Uint64(held),
		})
		return nil
	})
	return out, err
}

// leiosCertifies reports whether the chain may hold a Leios certified pool
// registration or retirement, which takes effect at a certifying slot the set
// does not keep, so pool reads stay on the metadata store. Certificates exist
// only from the Dijkstra era on: every node configures a certificate
// validator, so the validator alone does not mean one was ever applied. A
// ledger that has published no era yet is treated as certifying.
func (ls *LedgerState) leiosCertifies() bool {
	if ls.config.ValidateLeiosCertificate == nil {
		return false
	}
	snap := ls.loadConsensusSnapshot()
	if snap == nil {
		return true
	}
	return snap.currentEra.Id >= eras.DijkstraEraDesc.Id
}
