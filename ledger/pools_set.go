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
	"fmt"
	"math"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// PoolState is a registered pool's state as the reference ledger keeps it:
// the registration in effect, a re-registration waiting for the next epoch
// boundary, a pending retirement and the deposit held.
type PoolState struct {
	Pool          []byte
	Params        *models.PoolRegistration
	Future        *models.PoolRegistration
	RetiringEpoch *uint64
	DepositHeld   *uint64
}

// PoolChange is a pool's state as the change at Slot leaves it, or its
// removal when the pool is reaped.
type PoolChange struct {
	Slot   uint64
	State  PoolState
	Delete bool
}

// CleanPoolRegistration copies r without the metadata store's row IDs,
// deposit and Leios registration-epoch bookkeeping, which the set does not
// keep with the registration.
func CleanPoolRegistration(
	r *models.PoolRegistration,
) *models.PoolRegistration {
	if r == nil {
		return nil
	}
	c := models.PoolRegistration{
		Margin:                     r.Margin,
		MetadataUrl:                r.MetadataUrl,
		VrfKeyHash:                 r.VrfKeyHash,
		PoolKeyHash:                r.PoolKeyHash,
		RewardAccount:              r.RewardAccount,
		RewardAccountCredentialTag: r.RewardAccountCredentialTag,
		MetadataHash:               r.MetadataHash,
		LeiosKeyPublic:             r.LeiosKeyPublic,
		LeiosKeyPossessionProof:    r.LeiosKeyPossessionProof,
		Pledge:                     r.Pledge,
		Cost:                       r.Cost,
		AddedSlot:                  r.AddedSlot,
	}
	for _, o := range r.Owners {
		c.Owners = append(
			c.Owners,
			models.PoolRegistrationOwner{KeyHash: o.KeyHash},
		)
	}
	for _, rl := range r.Relays {
		c.Relays = append(c.Relays, models.PoolRegistrationRelay{
			Ipv4: rl.Ipv4, Ipv6: rl.Ipv6, Type: rl.Type,
			Hostname: rl.Hostname, Port: rl.Port,
		})
	}
	return &c
}

// PoolReader serves pool states from a live UTxO set that computes them.
type PoolReader interface {
	PoolsLoaded() (bool, error)
	PoolState(pool []byte) (*PoolState, error)
	PoolStateAt(pool []byte, slot uint64) (*PoolState, error)
	PoolStates(fn func(*PoolState) error) error
}

func (ls *LedgerState) poolReader(txn ledgerdb.Txn) PoolReader {
	if ls.accountReader(txn) == nil || ls.utxoSetFailed.Load() {
		return nil
	}
	r, ok := ls.readSet(txn).(PoolReader)
	if !ok {
		return nil
	}
	return r
}

// poolServing returns the pool reader when pool reads are answered from the
// set, or nil when they stay on the metadata store.
func (ls *LedgerState) poolServing(txn ledgerdb.Txn) PoolReader {
	r := ls.poolReader(txn)
	if r == nil || !ls.setServes(&ls.poolsSetLoaded, r.PoolsLoaded) {
		return nil
	}
	return r
}

// poolView returns pool's state as txn sees the set.
func (ls *LedgerState) poolView(
	r PoolReader,
	txn ledgerdb.Txn,
	pool []byte,
) (*PoolState, error) {
	var ceiling *uint64
	var staged []PoolChange
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			if ceiling == nil || to < *ceiling {
				ceiling = &to
			}
			staged = slices.DeleteFunc(staged, func(c PoolChange) bool {
				return c.Slot > to
			})
		}
		for _, c := range o.pools {
			if bytes.Equal(c.State.Pool, pool) {
				staged = append(staged, c)
			}
		}
	}
	if len(staged) > 0 {
		last := staged[len(staged)-1]
		if last.Delete {
			return nil, nil
		}
		st := last.State
		return &st, nil
	}
	if ceiling != nil {
		return r.PoolStateAt(pool, *ceiling)
	}
	return r.PoolState(pool)
}

func (ls *LedgerState) stagePool(
	txn ledgerdb.Txn,
	slot uint64,
	st PoolState,
	del bool,
) {
	ls.stageLedgerChanges(
		txn,
		LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			Pools: []PoolChange{{Slot: slot, State: st, Delete: del}},
		},
	)
}

// poolEventEffectiveSlot is the EffectiveSlot of an event applied at slot that
// takes effect at effective: zero when they are the same slot.
func poolEventEffectiveSlot(slot, effective uint64) uint64 {
	if effective == slot {
		return 0
	}
	return effective
}

// stagePoolCertificate stages a pool registration or retirement as the
// reference ledger's POOL rule applies it: a registration of a pool that
// is not registered takes effect at once with the deposit charged; one of a
// registered pool waits for the next epoch boundary, cancels a pending
// retirement and keeps the deposit held. A retirement records its epoch.
func (ls *LedgerState) stagePoolCertificate(
	r PoolReader,
	txn ledgerdb.Txn,
	cert lcommon.Certificate,
	slot uint64,
	effective uint64,
	position uint64,
	deposit *uint64,
) (bool, error) {
	switch c := cert.(type) {
	case *lcommon.PoolRegistrationCertificate:
		reg, err := models.PoolRegistrationFromCertificate(c, slot)
		if err != nil {
			return true, err
		}
		prev, err := ls.poolView(r, txn, c.Operator[:])
		if err != nil {
			return true, err
		}
		next := PoolState{Pool: bytes.Clone(c.Operator[:])}
		if prev == nil {
			next.Params = &reg
			if deposit != nil {
				d := *deposit
				next.DepositHeld = &d
			}
		} else {
			next = *prev
			next.Future = &reg
			next.RetiringEpoch = nil
		}
		ls.stagePool(txn, slot, next, false)
		ls.stagePoolEvent(txn, c.Operator[:], PoolEvent{
			Slot: slot, Position: position, Registration: &reg,
			EffectiveSlot: poolEventEffectiveSlot(slot, effective),
		})
		return true, nil
	case *lcommon.PoolRetirementCertificate:
		prev, err := ls.poolView(r, txn, c.PoolKeyHash[:])
		if err != nil {
			return true, err
		}
		if prev == nil {
			return true, fmt.Errorf("retirement of unregistered pool %x", c.PoolKeyHash[:])
		}
		next := *prev
		epoch := c.Epoch
		next.RetiringEpoch = &epoch
		ls.stagePool(txn, slot, next, false)
		ls.stagePoolEvent(txn, c.PoolKeyHash[:], PoolEvent{
			Slot: slot, Position: position, RetiringEpoch: epoch,
			EffectiveSlot: poolEventEffectiveSlot(slot, effective),
		})
		return true, nil
	}
	return false, nil
}

// poolKeys returns every pool the set holds or the batches txn sees have
// staged, each once, and, when a visible batch staged a rollback, every pool
// the rollback restores, which the set's current rows may no longer hold. A
// caller confirms each with poolView.
func (ls *LedgerState) poolKeys(
	r PoolReader,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	var pools [][]byte
	seen := make(map[string]struct{})
	add := func(p []byte) {
		if _, ok := seen[string(p)]; !ok {
			seen[string(p)] = struct{}{}
			pools = append(pools, p)
		}
	}
	if err := r.PoolStates(func(st *PoolState) error {
		add(st.Pool)
		return nil
	}); err != nil {
		return nil, err
	}
	var ceiling *uint64
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil && (ceiling == nil || *o.rollbackTo < *ceiling) {
			to := *o.rollbackTo
			ceiling = &to
		}
		for _, c := range o.pools {
			add(c.State.Pool)
		}
	}
	if ceiling == nil {
		return pools, nil
	}
	ah, ok := r.(PoolKeyAsOfReader)
	if !ok {
		return nil, ls.unavailableAsOf("pools")
	}
	changed, err := ah.PoolKeysChangedAfter(*ceiling)
	if err != nil {
		return nil, err
	}
	for _, p := range changed {
		add(p)
	}
	return pools, nil
}

// stagePoolBoundary applies the epoch boundary into newEpoch to every
// pool: a waiting re-registration takes effect, then a pool whose
// retirement epoch has come is removed. A retirement inside a run of empty
// epochs counts at the next boundary, as the metadata store's effective
// registration treats any retirement at or before the current epoch.
func (ls *LedgerState) stagePoolBoundary(
	txn ledgerdb.Txn,
	newEpoch uint64,
	slot uint64,
) {
	r := ls.poolReader(txn)
	if r == nil {
		return
	}
	pools, err := ls.poolKeys(r, txn)
	if err != nil {
		ls.failAccountStaging(err)
		return
	}
	for _, p := range pools {
		st, err := ls.poolView(r, txn, p)
		if err != nil {
			ls.failAccountStaging(err)
			return
		}
		if st == nil {
			continue
		}
		if st.RetiringEpoch != nil && *st.RetiringEpoch <= newEpoch {
			ls.stagePool(txn, slot, *st, true)
			continue
		}
		if st.Future != nil {
			next := *st
			next.Params, next.Future = st.Future, nil
			ls.stagePool(txn, slot, next, false)
		}
	}
}

// sameRegistrationRow reports whether a and b are one registration: by row ID
// when both rows carry one, else by content, since a registration the set
// serves has no row ID.
func sameRegistrationRow(a, b *models.PoolRegistration) bool {
	if a.ID != 0 && b.ID != 0 {
		return a.ID == b.ID
	}
	return samePoolRegistration(a, b)
}

// samePoolRegistration compares the registration fields consensus reads:
// which certificate it is and what it states.
func samePoolRegistration(a, b *models.PoolRegistration) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.AddedSlot != b.AddedSlot || !bytes.Equal(a.VrfKeyHash, b.VrfKeyHash) ||
		a.Pledge != b.Pledge || a.Cost != b.Cost ||
		!bytes.Equal(a.RewardAccount, b.RewardAccount) ||
		a.RewardAccountCredentialTag != b.RewardAccountCredentialTag ||
		a.MetadataUrl != b.MetadataUrl || !bytes.Equal(a.MetadataHash, b.MetadataHash) ||
		len(a.Owners) != len(b.Owners) {
		return false
	}
	if (a.Margin == nil || a.Margin.Rat == nil) != (b.Margin == nil || b.Margin.Rat == nil) ||
		(a.Margin != nil && a.Margin.Rat != nil && a.Margin.Cmp(b.Margin.Rat) != 0) {
		return false
	}
	owners := make(map[string]int, len(a.Owners))
	for _, o := range a.Owners {
		owners[string(o.KeyHash)]++
	}
	for _, o := range b.Owners {
		owners[string(o.KeyHash)]--
	}
	for _, n := range owners {
		if n != 0 {
			return false
		}
	}
	return true
}

func (ls *LedgerState) comparePoolRead(
	g verifyReadGuard,
	what string,
	same bool,
	setErr error,
) {
	if !ls.verifying() {
		return
	}
	if setErr == nil && !same && ls.verifyReadRaced(g) {
		return
	}
	ls.metrics.poolReadsVerified.Inc()
	if same && setErr == nil {
		return
	}
	n := ls.poolReadMismatches.Add(1)
	ls.metrics.poolReadMismatches.Inc()
	if n > 20 && n%1000 != 0 {
		return
	}
	ls.config.Logger.Warn(
		"live UTxO set pool read differs from the metadata store",
		"component", "ledger",
		"read", what,
		"set_error", setErr,
		"mismatches", n,
	)
}

// poolRegistrationsFromSet answers GetPoolRegistrationsEffectiveForEpoch at
// an unbounded snapshot slot from the set's current pool states, or reports
// false when the set cannot tell what the metadata store would return. It
// keeps no registration history, so a pool whose registration in effect
// began at or after epochStartSlot may have an earlier one the store
// returns, and a re-registration that began before it and has not taken
// effect means the set has not reached that epoch's boundary. A Leios
// certified registration takes effect at its context slot, which the pool
// states do not follow, so a node that certifies stays on the store; a drained
// family never reaches this read, since poolBook answers it from the history.
func (ls *LedgerState) poolRegistrationsFromSet(
	r PoolReader,
	txn ledgerdb.Txn,
	poolKeyHashes []lcommon.PoolKeyHash,
	epochStartSlot, endedEpoch uint64,
) ([]models.PoolRegistration, bool, error) {
	if ls.leiosCertifies() {
		return nil, false, nil
	}
	out := []models.PoolRegistration{}
	seen := make(map[string]struct{}, len(poolKeyHashes))
	for _, pkh := range poolKeyHashes {
		if _, ok := seen[string(pkh[:])]; ok {
			continue
		}
		seen[string(pkh[:])] = struct{}{}
		st, err := ls.poolView(r, txn, pkh[:])
		if err != nil {
			return nil, false, err
		}
		if st == nil || st.Params == nil {
			continue
		}
		if st.Params.AddedSlot >= epochStartSlot ||
			st.Future != nil && st.Future.AddedSlot < epochStartSlot {
			return nil, false, nil
		}
		if st.RetiringEpoch != nil && *st.RetiringEpoch <= endedEpoch {
			continue
		}
		out = append(out, *CleanPoolRegistration(st.Params))
	}
	return out, true, nil
}

// GetPoolRegistrationsEffectiveForEpoch answers the registration in effect
// now (an unbounded snapshot slot) from the set in set mode where the set can
// tell. Otherwise it answers from the metadata store and compares each
// pool's registration with the one the set's history selects for the epoch
// window; a set without its history compares only the current registration,
// and only when the caller asks for the one in effect now.
func (s stagingLedgerDB) GetPoolRegistrationsEffectiveForEpoch(
	poolKeyHashes []lcommon.PoolKeyHash,
	epochStartSlot uint64,
	endedEpoch uint64,
	snapshotSlot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRegistration, error) {
	if got, ok, err := drainedPoolRead(s, txn, func(b *poolBook) ([]models.PoolRegistration, error) {
		return b.effective(
			poolKeyHashes, epochStartSlot, endedEpoch, snapshotSlot,
		)
	}); ok {
		return got, err
	}
	serving := false
	if snapshotSlot >= math.MaxInt64 {
		if r := s.ls.poolServing(txn); r != nil {
			out, ok, err := s.ls.poolRegistrationsFromSet(
				r, txn, poolKeyHashes, epochStartSlot, endedEpoch,
			)
			if err != nil {
				return nil, err
			}
			if ok {
				return out, nil
			}
			serving = true
		}
	}
	g := s.ls.verifyGuard(txn)
	want, err := s.LedgerDB.GetPoolRegistrationsEffectiveForEpoch(
		poolKeyHashes, epochStartSlot, endedEpoch, snapshotSlot, txn,
	)
	if err != nil || serving {
		return want, err
	}
	if _, mode := s.ls.utxoSetReader(); mode == UtxoSetReadsOff {
		return want, nil
	}
	byPool := make(map[string]*models.PoolRegistration, len(want))
	for i := range want {
		byPool[string(want[i].PoolKeyHash)] = &want[i]
	}
	if hr := s.ls.poolHistoryReader(txn); hr != nil {
		for _, pkh := range poolKeyHashes {
			events, setErr := s.ls.poolHistoryView(hr, txn, pkh[:])
			got := effectivePoolRegistration(
				events, epochStartSlot, endedEpoch, snapshotSlot,
			)
			s.ls.comparePoolRead(
				g,
				fmt.Sprintf(
					"pool %x effective registration at slot %d",
					pkh[:],
					snapshotSlot,
				),
				samePoolRegistration(byPool[string(pkh[:])], got),
				setErr,
			)
		}
		return want, nil
	}
	if snapshotSlot < math.MaxInt64 {
		return want, nil
	}
	r := s.ls.poolReader(txn)
	if r == nil {
		return want, nil
	}
	for _, pkh := range poolKeyHashes {
		st, setErr := s.ls.poolView(r, txn, pkh[:])
		var got *models.PoolRegistration
		if st != nil {
			got = st.Params
		}
		s.ls.comparePoolRead(
			g, fmt.Sprintf("pool %x effective registration", pkh[:]),
			samePoolRegistration(byPool[string(pkh[:])], got), setErr,
		)
	}
	return want, nil
}

// UtxoSetTipReader is implemented by a live UTxO set that records the ledger
// point it last applied.
type UtxoSetTipReader interface {
	Tip() (ocommon.Point, bool, error)
}

// poolVerifyReader returns the set's pool reader when pool reads compare.
func (ls *LedgerState) poolVerifyReader(txn ledgerdb.Txn) PoolReader {
	if _, mode := ls.utxoSetReader(); mode == UtxoSetReadsOff {
		return nil
	}
	return ls.poolReader(txn)
}

// activePools is the pools the store's active-pool reads give from the set:
// a registered pool whose pending retirement is after the epoch of the
// read. slot is the read's slot, or nil for the store's tip. The boolean is
// false when the set cannot answer. The store filters registrations and
// retirements by a slot the set does not keep per change, so only a read at
// or after the set's tip, within the set's latest epoch and with no pool
// change staged, is the set's state.
func (ls *LedgerState) activePools(
	r PoolReader,
	txn ledgerdb.Txn,
	slot *uint64,
) (map[string]struct{}, bool, error) {
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil || len(o.pools) > 0 {
			return nil, false, nil
		}
	}
	er, _ := ls.epochStateReader(txn)
	if er == nil {
		return nil, false, nil
	}
	epochs, err := ls.epochStateView(er, txn)
	if err != nil {
		return nil, false, err
	}
	var latest *models.Epoch
	for _, e := range epochs {
		if latest == nil || e.EpochId > latest.EpochId {
			latest = &e
		}
	}
	// The set applied every boundary up to its latest epoch's, so a slot
	// before that epoch sees pools the set has already reaped.
	if latest == nil || (slot != nil &&
		(*slot < latest.StartSlot ||
			*slot >= latest.StartSlot+uint64(latest.LengthInSlots))) {
		return nil, false, nil
	}
	epoch := latest.EpochId
	if slot != nil {
		tr, ok := ls.readSet(txn).(UtxoSetTipReader)
		if !ok {
			return nil, false, nil
		}
		tip, has, err := tr.Tip()
		if err != nil {
			return nil, false, err
		}
		if !has || *slot < tip.Slot {
			return nil, false, nil
		}
	}
	pools, err := ls.poolKeys(r, txn)
	if err != nil {
		return nil, false, err
	}
	out := make(map[string]struct{}, len(pools))
	for _, p := range pools {
		st, err := ls.poolView(r, txn, p)
		if err != nil {
			return nil, false, err
		}
		if st == nil ||
			(st.RetiringEpoch != nil && *st.RetiringEpoch <= epoch) {
			continue
		}
		out[string(p)] = struct{}{}
	}
	return out, true, nil
}

func (s stagingLedgerDB) verifyActivePools(
	g verifyReadGuard,
	what string,
	txn ledgerdb.Txn,
	slot *uint64,
	want [][]byte,
) {
	r := s.ls.poolVerifyReader(txn)
	if r == nil {
		return
	}
	got, ok, setErr := s.ls.activePools(r, txn, slot)
	if setErr == nil && !ok {
		return
	}
	wanted := make(map[string]struct{}, len(want))
	for _, p := range want {
		wanted[string(p)] = struct{}{}
	}
	same := len(got) == len(wanted)
	for p := range wanted {
		_, found := got[p]
		same = same && found
	}
	s.ls.comparePoolRead(g, what, same, setErr)
}

// activePoolsFromSet answers an active-pool read from the set in set mode
// when activePools can tell, and reports whether it did. A Leios certified
// registration or retirement takes effect at its context slot, which the pool
// states do not follow, so a node that certifies stays on the store; a drained
// family never reaches this read, since poolBook answers it from the history.
func (s stagingLedgerDB) activePoolsFromSet(
	txn ledgerdb.Txn,
	slot *uint64,
) ([][]byte, bool, error) {
	r := s.ls.poolServing(txn)
	if r == nil || s.ls.leiosCertifies() {
		return nil, false, nil
	}
	pools, ok, err := s.ls.activePools(r, txn, slot)
	if err != nil || !ok {
		return nil, false, err
	}
	out := make([][]byte, 0, len(pools))
	for p := range pools {
		out = append(out, []byte(p))
	}
	slices.SortFunc(out, bytes.Compare)
	return out, true, nil
}

func (s stagingLedgerDB) GetActivePoolKeyHashes(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	if got, ok, err := drainedPoolRead(s, txn, func(b *poolBook) ([][]byte, error) {
		return b.active()
	}); ok {
		return got, err
	}
	if got, ok, err := s.activePoolsFromSet(txn, nil); err != nil {
		return nil, err
	} else if ok {
		return got, nil
	}
	g := s.ls.verifyGuard(txn)
	want, err := s.LedgerDB.GetActivePoolKeyHashes(ctx, txn)
	if err != nil {
		return want, err
	}
	s.verifyActivePools(g, "active pools", txn, nil, want)
	return want, nil
}

// GetActivePoolKeyHashesAtSlot is answered from the set, and compared, only
// for a slot at or after the set's tip.
func (s stagingLedgerDB) GetActivePoolKeyHashesAtSlot(
	slot uint64,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	if got, ok, err := drainedPoolRead(s, txn, func(b *poolBook) ([][]byte, error) {
		return b.activeAtSlot(slot)
	}); ok {
		return got, err
	}
	if got, ok, err := s.activePoolsFromSet(txn, &slot); err != nil {
		return nil, err
	} else if ok {
		return got, nil
	}
	g := s.ls.verifyGuard(txn)
	want, err := s.LedgerDB.GetActivePoolKeyHashesAtSlot(slot, txn)
	if err != nil {
		return want, err
	}
	s.verifyActivePools(
		g,
		fmt.Sprintf("active pools at slot %d", slot),
		txn,
		&slot,
		want,
	)
	return want, nil
}
