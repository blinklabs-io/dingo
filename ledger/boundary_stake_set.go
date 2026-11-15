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
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/types"
	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gconway "github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// PointerPosition is the certificate position a pointer address names.
type PointerPosition struct {
	Slot      uint64
	TxIndex   uint64
	CertIndex uint64
}

// PointerRegistrationReader resolves pointer positions against the stake
// registration history of a live UTxO set.
type PointerRegistrationReader interface {
	// StakeRegistrationsAt returns the credentials with a stake registration
	// certificate at each of positions.
	StakeRegistrationsAt(
		positions map[PointerPosition]struct{},
	) (map[PointerPosition][]models.StakeCredentialRef, error)
}

// boundaryStake computes what the metadata store's epoch-boundary stake
// reconstruction gives, from the live UTxO set. A boundary is read at its
// snapshot slot and, when the reward credits of the boundary count, its
// boundary slot, which is the snapshot slot's successor.
//
// A read made while the set's tip is at or before the snapshot slot is of the
// state the transaction sees now. A read made later is of the state a rollback
// to the snapshot slot would leave, from the set's journals: accounts and pools
// as of that slot and UTxO stake from the outputs live then.
type boundaryStake struct {
	liveStakeSource
	slot     uint64
	boundary uint64
	// after says SNAP follows the boundary's enactment, so delegations to
	// pools reaped at the boundary are gone from it and the boundary's own
	// credits are visible to it.
	after bool
	past  bool
	// leios says the chain certifies Leios closures, whose certificates and
	// outputs take effect at their context slot.
	leios  bool
	epochs []models.Epoch
	// sums holds every stake sum as of slot, read from the outputs live
	// then, for a past boundary.
	sums     map[string]uint64
	postSnap map[string]uint64
	// withdrawn holds the lovelace withdrawn at the boundary slot per
	// credential (tag then key), for a past boundary.
	withdrawn map[string]uint64
}

// RewardWithdrawalReader lists what transactions withdrew at a slot.
type RewardWithdrawalReader interface {
	// WithdrawnAt returns the lovelace withdrawn at slot per credential,
	// tag then key, and false when a withdrawal at slot has no recorded
	// amount.
	WithdrawnAt(slot uint64) (map[string]uint64, bool, error)
}

// boundaryCurrent reports whether the state txn sees is the state at slot:
// neither the set nor a batch committed by another transaction is past it, and
// no pool changed after it. The boundary's POOLREAP removes a retiring pool at
// the boundary slot, which no block advances the tip to, so a tip at the
// snapshot slot does not show that the pool states are still SNAP's.
func (ls *LedgerState) boundaryCurrent(txn ledgerdb.Txn, slot uint64) bool {
	tr, ok := ls.readSet(txn).(UtxoSetTipReader)
	if !ok {
		return false
	}
	tip, has, err := tr.Tip()
	if err != nil {
		return false
	}
	if has && tip.Slot > slot {
		return false
	}
	var own *utxoSetBatch
	if txn != nil {
		ls.utxoSetMu.Lock()
		own = ls.utxoSetBatches[txn]
		ls.utxoSetMu.Unlock()
	}
	batches := ls.utxoSetBatchesSeenBy(ls.pinOf(txn), own)
	ls.utxoSetMu.Lock()
	for _, b := range batches {
		if b != own && b.tip != nil && b.tip.Slot > slot {
			ls.utxoSetMu.Unlock()
			return false
		}
	}
	ls.utxoSetMu.Unlock()
	return !ls.poolsChangedAfter(txn, slot)
}

// poolsChangedAfter reports whether a pool changed after slot in the set's
// journal or in a batch txn sees. A journal that fails to answer is taken to
// have changed, so the read is answered as of slot or not at all.
func (ls *LedgerState) poolsChangedAfter(txn ledgerdb.Txn, slot uint64) bool {
	for _, o := range ls.stagedOps(txn) {
		for _, c := range o.pools {
			if c.Slot > slot {
				return true
			}
		}
	}
	r := ls.poolReader(txn)
	if r == nil {
		return false
	}
	ah, ok := r.(PoolKeyAsOfReader)
	if !ok {
		return false
	}
	changed, err := ah.PoolKeysChangedAfter(slot)
	return err != nil || len(changed) > 0
}

// epochRows returns the epochs txn sees, oldest first.
func (ls *LedgerState) epochRows(txn ledgerdb.Txn) ([]models.Epoch, error) {
	er, mode := ls.epochStateReader(txn)
	if er == nil || mode == UtxoSetReadsOff {
		return nil, ls.unavailableAsOf("epochs")
	}
	view, err := ls.epochStateView(er, txn)
	if err != nil {
		return nil, err
	}
	return sortedEpochs(view, nil), nil
}

// epochAt returns the newest epoch starting at or before slot, whatever its
// length, as the metadata store resolves the epoch of a slot.
func epochAt(epochs []models.Epoch, slot uint64) (models.Epoch, bool) {
	for _, e := range slices.Backward(epochs) {
		if e.StartSlot <= slot {
			return e, true
		}
	}
	return models.Epoch{}, false
}

// snapAfterEnactment is whether SNAP follows the boundary at boundary: the
// epoch it starts, or the protocol parameters in force there, are Dijkstra's.
func (ls *LedgerState) snapAfterEnactment(
	txn ledgerdb.Txn,
	epochs []models.Epoch,
	boundary uint64,
) (bool, error) {
	if boundary == 0 {
		return false, nil
	}
	e, ok := epochAt(epochs, boundary)
	if !ok {
		return false, nil
	}
	if e.EraId >= gdijkstra.EraIdDijkstra {
		return true, nil
	}
	// At the transition SNAP precedes era translation, so the epoch still
	// names Conway while the enacted parameters carry the incoming major
	// version.
	pr, _ := ls.pparamsReader(txn)
	if pr == nil {
		return false, ls.unavailableAsOf("protocol parameters")
	}
	next, err := latestPParamsAt(pr, gdijkstra.EraIdDijkstra, boundary)
	if err != nil || next != nil {
		return next != nil, err
	}
	conway, err := latestPParamsAt(pr, gconway.EraIdConway, boundary)
	if err != nil || conway == nil || len(conway.Cbor) == 0 {
		return false, err
	}
	var params gconway.ConwayProtocolParameters
	if _, err := gcbor.Decode(conway.Cbor, &params); err != nil {
		return false, fmt.Errorf("decode snapshot boundary parameters: %w", err)
	}
	return params.ProtocolVersion.Major >= lcommon.ProtocolVersionDijkstra, nil
}

// latestPParamsAt returns era's newest parameter row added at or before slot.
func latestPParamsAt(
	r PParamsReader,
	era uint,
	slot uint64,
) (*models.PParams, error) {
	rows, err := r.PParamsForEra(era)
	if err != nil {
		return nil, err
	}
	var latest *models.PParams
	for i := range rows {
		if rows[i].AddedSlot <= slot &&
			(latest == nil || rows[i].AddedSlot >= latest.AddedSlot) {
			latest = &rows[i]
		}
	}
	return latest, nil
}

// boundaryStakeFor returns the source for the boundary at slot and boundary.
func (ls *LedgerState) boundaryStakeFor(
	txn ledgerdb.Txn,
	slot, boundary uint64,
	past bool,
) (*boundaryStake, error) {
	src, ok := ls.liveStakeSource(txn)
	if !ok {
		return nil, ls.unavailableAsOf("epoch boundary stake")
	}
	epochs, err := ls.epochRows(txn)
	if err != nil {
		return nil, err
	}
	after, err := ls.snapAfterEnactment(txn, epochs, boundary)
	if err != nil {
		return nil, err
	}
	return &boundaryStake{
		liveStakeSource: src,
		slot:            slot,
		boundary:        boundary,
		after:           after,
		past:            past,
		leios:           ls.leiosCertifies(),
		epochs:          epochs,
	}, nil
}

// stateSlot is the slot registrations, delegations and pool reaps count at.
func (b *boundaryStake) stateSlot() uint64 {
	if b.after {
		return b.boundary
	}
	return b.slot
}

// rewardSlot is the slot reward balances and pending credits count at.
func (b *boundaryStake) rewardSlot() uint64 {
	if b.boundary > 0 {
		return b.boundary
	}
	return b.slot
}

// account returns ref's account as the boundary counts certificates: no
// later than the snapshot slot, as the store ranks them. The set's journal
// cannot separate the rollover's changes at the boundary slot from those of
// a block at that slot, so nothing at the boundary slot is read here; the
// rollover's pool reaps are applied by reapedAtBoundary.
func (b *boundaryStake) account(
	ref models.StakeCredentialRef,
	slot uint64,
) (*models.Account, error) {
	if b.past {
		return b.ar.AccountAt(ref.Tag, ref.Key, min(slot, b.slot))
	}
	return b.ls.accountView(b.ar, b.txn, ref.Tag, ref.Key)
}

// boundaryDelegation is an account registered and delegated at the boundary.
type boundaryDelegation struct {
	ref  models.StakeCredentialRef
	pool []byte
	acct *models.Account
}

// delegations returns every account registered and delegated to a pool as the
// boundary sees it.
func (b *boundaryStake) delegations() ([]boundaryDelegation, error) {
	if b.leios {
		return b.leiosDelegations()
	}
	refs, err := b.candidateRefs()
	if err != nil {
		return nil, err
	}
	out := make([]boundaryDelegation, 0, len(refs))
	// The delegations the boundary's POOLREAP removed are not in the account
	// read at the snapshot slot, so a past boundary that follows the
	// enactment removes them by the pool's retirement, as the store does.
	var book *poolBook
	for _, ref := range refs {
		a, err := b.account(ref, b.stateSlot())
		if err != nil {
			return nil, err
		}
		if a == nil || !a.Active || len(a.Pool) == 0 {
			continue
		}
		if b.past && b.after {
			if book == nil {
				hist := b.ls.poolHistoryReader(b.txn)
				if hist == nil {
					return nil, b.ls.unavailableAsOf("pool certificate history")
				}
				book = &poolBook{ls: b.ls, txn: b.txn, hist: hist}
			}
			events, err := book.events(a.Pool)
			if err != nil {
				return nil, err
			}
			if b.poolReaped(events, a.AddedSlot) {
				continue
			}
		}
		out = append(out, boundaryDelegation{ref: ref, pool: a.Pool, acct: a})
	}
	return out, nil
}

// pastSums reads every stake sum as of slot from the outputs live then.
func (b *boundaryStake) pastSums() (map[string]uint64, error) {
	if b.sums != nil {
		return b.sums, nil
	}
	h, ok := b.ls.readSet(b.txn).(UtxoSetHistory)
	if !ok {
		return nil, b.ls.unavailableAsOf("UTxO stake")
	}
	view, err := h.UtxoSetAt(ocommon.Point{Slot: b.slot})
	if err != nil {
		if errors.Is(err, ErrUtxoAsOfUnavailable) {
			return nil, fmt.Errorf(
				"%w: %w", ErrLedgerStateAsOfUnavailable, err,
			)
		}
		return nil, err
	}
	defer view.Close()
	sums := make(map[string]uint64)
	err = view.EachUtxo(func(_ []byte, _ uint32, cbor []byte) error {
		key, lovelace, ok := UtxoStakeOf(cbor)
		if !ok {
			return nil
		}
		if lovelace > math.MaxUint64-sums[string(key)] {
			return fmt.Errorf("stake sum %x overflows", key)
		}
		sums[string(key)] += lovelace
		return nil
	})
	if err != nil {
		return nil, err
	}
	if b.leios {
		if err := b.adjustSums(sums); err != nil {
			return nil, err
		}
	}
	b.sums = sums
	return sums, nil
}

func (b *boundaryStake) utxoStake(
	ref models.StakeCredentialRef,
) (uint64, error) {
	key := CredentialStakeKey(ref.Tag, ref.Key)
	if !b.past {
		return b.ls.utxoStakeSeenBy(b.ur, b.txn, key)
	}
	sums, err := b.pastSums()
	if err != nil {
		return 0, err
	}
	return sums[string(key)], nil
}

// pointerSums returns the stake held at each pointer address position.
func (b *boundaryStake) pointerSums() (map[PointerPosition]uint64, error) {
	out := make(map[PointerPosition]uint64)
	add := func(key []byte, n uint64) error {
		slot, tx, cert, ok := ParsePointerStakeKey(key)
		if !ok || n == 0 {
			return nil
		}
		at := PointerPosition{Slot: slot, TxIndex: tx, CertIndex: cert}
		if n > math.MaxUint64-out[at] {
			return fmt.Errorf("pointer stake %x overflows", key)
		}
		out[at] += n
		return nil
	}
	if b.past {
		sums, err := b.pastSums()
		if err != nil {
			return nil, err
		}
		for k, n := range sums {
			if err := add([]byte(k), n); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	sums, ok := b.ls.readSet(b.txn).(UtxoSetSums)
	if !ok {
		return nil, b.ls.unavailableAsOf("pointer stake")
	}
	isPointer := func(key []byte) bool {
		_, _, _, ok := ParsePointerStakeKey(key)
		return ok
	}
	var own *utxoSetBatch
	if b.txn != nil {
		b.ls.utxoSetMu.Lock()
		own = b.ls.utxoSetBatches[b.txn]
		b.ls.utxoSetMu.Unlock()
	}
	b.ls.utxoSetApplyMu.RLock()
	totals := b.ls.stagedStake(
		b.ls.utxoSetBatchesSeenBy(b.ls.pinOf(b.txn), own), isPointer,
	)
	err := sums.EachSum(func(key []byte, n uint64) error {
		if isPointer(key) {
			// Lovelace fits in int64.
			totals[string(key)] += int64(n) //nolint:gosec
		}
		return nil
	})
	b.ls.utxoSetApplyMu.RUnlock()
	if err != nil {
		return nil, err
	}
	for k, n := range totals {
		if n < 0 {
			return nil, fmt.Errorf("pointer stake %x is negative as staged", k)
		}
		if err := add([]byte(k), uint64(n)); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// pointerCounted is whether the boundary counts stake held at pointer
// addresses: the era of the epoch holding its later slot, or incomingEraID.
func (b *boundaryStake) pointerCounted(incomingEraID *uint) bool {
	if incomingEraID != nil {
		return models.PointerStakeCountedInEra(*incomingEraID)
	}
	e, ok := epochAt(b.epochs, max(b.boundary, b.slot))
	return ok && models.PointerStakeCountedInEra(e.EraId)
}

// pointerStake returns the stake held at pointer addresses per credential
// whose registration the addresses name, which no later deregistration at or
// before the snapshot slot removed.
func (b *boundaryStake) pointerStake() (map[string]uint64, error) {
	out := make(map[string]uint64)
	sums, err := b.pointerSums()
	if err != nil || len(sums) == 0 {
		return out, err
	}
	rh := b.ls.registrationHistoryReader(b.txn)
	rr, ok := rh.(PointerRegistrationReader)
	if rh == nil || !ok {
		return nil, b.ls.unavailableAsOf("pointer registrations")
	}
	wanted := make(map[PointerPosition]struct{}, len(sums))
	for at := range sums {
		// A component beyond what a position holds names no certificate.
		if at.Slot <= b.slot && at.TxIndex <= math.MaxUint32 &&
			at.CertIndex <= math.MaxUint32 {
			wanted[at] = struct{}{}
		}
	}
	regs, err := rr.StakeRegistrationsAt(wanted)
	if err != nil {
		return nil, err
	}
	for at, refs := range regs {
		// StakeRegistrationsAt was asked only for positions that fit.
		tx, cert := uint32(at.TxIndex), uint32(at.CertIndex) //nolint:gosec
		position := PoolEventPosition(tx, cert)
		for _, ref := range refs {
			events, err := rh.StakeHistory(ref)
			if err != nil {
				return nil, err
			}
			removed := slices.ContainsFunc(events, func(ev StakeEvent) bool {
				return !ev.Registered && ev.Slot <= b.slot &&
					(ev.Slot > at.Slot ||
						(ev.Slot == at.Slot && ev.Position > position))
			})
			if removed {
				continue
			}
			if sums[at] > math.MaxUint64-out[ref.MapKey()] {
				return nil, fmt.Errorf(
					"pointer stake for %x overflows",
					ref.Key,
				)
			}
			out[ref.MapKey()] += sums[at]
		}
	}
	return out, nil
}

// postSnapshotCredits returns the credits the boundary applied after its SNAP
// per credential.
func (b *boundaryStake) postSnapshotCredits() (map[string]uint64, error) {
	if b.postSnap != nil {
		return b.postSnap, nil
	}
	if b.rr == nil {
		return nil, b.ls.unavailableAsOf("post-snapshot credits")
	}
	out := make(map[string]uint64)
	if b.past {
		credits, err := b.rr.PostSnapshotCredits(b.boundary, nil)
		if err != nil {
			return nil, err
		}
		for _, c := range credits {
			out[c.Ref.MapKey()] += c.Amount
		}
	} else {
		rows, err := b.ls.postSnapshotCreditsView(b.rr, b.txn, b.boundary)
		if err != nil {
			return nil, err
		}
		for _, d := range rows {
			out[models.NewStakeCredentialRef(d.CredentialTag, d.StakingKey).MapKey()] = uint64(d.Amount)
		}
	}
	b.postSnap = out
	return out, nil
}

// pendingCredits returns the reward credits of ref not yet in its balance.
func (b *boundaryStake) pendingCredits(
	ref models.StakeCredentialRef,
	slot uint64,
) (uint64, error) {
	if b.rr == nil {
		return 0, nil
	}
	if !b.past {
		return b.ls.pendingRewardCreditView(b.rr, b.txn, ref.Tag, ref.Key)
	}
	rows, err := b.rr.CredentialRewardOutputs(ref.Tag, ref.Key, &slot)
	if err != nil {
		return 0, err
	}
	var total uint64
	credited := make(map[uint64]bool)
	for _, o := range rows {
		in, seen := credited[o.Epoch]
		if !seen {
			round, ok, err := b.rr.RewardRound(o.Epoch, &slot)
			if err != nil {
				return 0, err
			}
			in = ok && round.BoundarySlot <= slot
			credited[o.Epoch] = in
		}
		if !in {
			continue
		}
		if uint64(o.Amount) > math.MaxUint64-total {
			return 0, fmt.Errorf(
				"pending reward credit overflow for %x",
				ref.Key,
			)
		}
		total += uint64(o.Amount)
	}
	return total, nil
}

// withdrawnAtBoundary returns the lovelace withdrawn at the boundary slot per
// credential.
func (b *boundaryStake) withdrawnAtBoundary() (map[string]uint64, error) {
	if b.withdrawn != nil {
		return b.withdrawn, nil
	}
	wr, ok := b.ar.(RewardWithdrawalReader)
	if !ok {
		return nil, b.ls.unavailableAsOf("withdrawals")
	}
	out, complete, err := wr.WithdrawnAt(b.boundary)
	if err != nil {
		return nil, err
	}
	if !complete {
		return nil, b.ls.unavailableAsOf("withdrawal amounts")
	}
	b.withdrawn = out
	return out, nil
}

// reward is the reward balance the boundary snapshots for d: the balance at
// the reward slot without the credits the boundary applied after SNAP, plus
// the pending credits of the rounds applied by then.
func (b *boundaryStake) reward(d boundaryDelegation) (uint64, error) {
	rs := b.rewardSlot()
	acct := d.acct
	if b.past {
		var err error
		if acct, err = b.ar.AccountAt(d.ref.Tag, d.ref.Key, rs); err != nil {
			return 0, err
		}
	}
	var base uint64
	if acct != nil {
		base = uint64(acct.Reward)
	}
	if b.boundary > 0 && b.past {
		// The store counts the balance the rollover leaves, before the
		// transactions of a block at the boundary slot, which the set's
		// state at that slot includes.
		withdrawn, err := b.withdrawnAtBoundary()
		if err != nil {
			return 0, err
		}
		w := withdrawn[string(append([]byte{d.ref.Tag}, d.ref.Key...))]
		if w > math.MaxUint64-base {
			return 0, errors.New("historical stake overflow")
		}
		base += w
	}
	if b.boundary > 0 && !b.after {
		credits, err := b.postSnapshotCredits()
		if err != nil {
			return 0, err
		}
		// A pruned journal can hold more credit than the balance; the
		// snapshot's stake is unsigned.
		base -= min(base, credits[d.ref.MapKey()])
	}
	pending, err := b.pendingCredits(d.ref, rs)
	if err != nil {
		return 0, err
	}
	if pending > math.MaxUint64-base {
		return 0, errors.New("historical stake overflow")
	}
	return base + pending, nil
}

// boundaryInput is a credential's stake as the boundary snapshots it.
type boundaryInput struct {
	pool  []byte
	ref   models.StakeCredentialRef
	stake uint64
}

// inputs returns the stake of every credential delegated to one of wanted
// that the CIP-0163 gate expiryEpoch leaves counted: UTxO stake, stake at the
// pointer addresses naming it, and its reward balance.
func (b *boundaryStake) inputs(
	wanted map[string]struct{},
	expiryEpoch uint64,
	inactivityPeriod uint64,
	incomingEraID *uint,
) ([]boundaryInput, error) {
	if expiryEpoch > 0 && inactivityPeriod == 0 {
		return nil, errors.New(
			"historical stake expiry enabled with zero inactivity period",
		)
	}
	delegs, err := b.delegations()
	if err != nil {
		return nil, err
	}
	var pointers map[string]uint64
	if b.pointerCounted(incomingEraID) {
		if pointers, err = b.pointerStake(); err != nil {
			return nil, err
		}
	}
	out := make([]boundaryInput, 0, len(delegs))
	for _, d := range delegs {
		if _, ok := wanted[string(d.pool)]; !ok {
			continue
		}
		if b.leios {
			if err := b.leiosWitnessGuard(d, expiryEpoch); err != nil {
				return nil, err
			}
		}
		// A credential with no account row has no expiry, as the store
		// reads it.
		if d.acct != nil && !expiryAllows(d.acct, expiryEpoch) {
			continue
		}
		utxo, err := b.utxoStake(d.ref)
		if err != nil {
			return nil, err
		}
		reward, err := b.reward(d)
		if err != nil {
			return nil, err
		}
		stake := utxo
		for _, n := range []uint64{pointers[d.ref.MapKey()], reward} {
			if n > math.MaxUint64-stake {
				return nil, errors.New("historical stake overflow")
			}
			stake += n
		}
		out = append(out, boundaryInput{pool: d.pool, ref: d.ref, stake: stake})
	}
	slices.SortFunc(out, func(a, c boundaryInput) int {
		return cmp.Or(
			bytes.Compare(a.pool, c.pool),
			cmp.Compare(a.ref.Tag, c.ref.Tag),
			bytes.Compare(a.ref.Key, c.ref.Key),
		)
	})
	return out, nil
}

func poolSet(pools [][]byte) map[string]struct{} {
	out := make(map[string]struct{}, len(pools))
	for _, p := range pools {
		out[string(p)] = struct{}{}
	}
	return out
}

// poolStakeResult is GetEpochBoundaryStakeByPools's answer.
type poolStakeResult struct {
	stakes     map[string]uint64
	delegators map[string]uint64
}

func (r poolStakeResult) figures() map[string]uint64 {
	out := make(map[string]uint64, len(r.stakes)+len(r.delegators))
	for p, v := range r.stakes {
		out["stake/"+p] = v
	}
	for p, v := range r.delegators {
		out["delegators/"+p] = v
	}
	return out
}

func inputFigures(rows []*models.RewardStakeInput) map[string]uint64 {
	out := make(map[string]uint64, len(rows))
	for _, r := range rows {
		out[liveStakeKey(r.PoolKeyHash, r.CredentialTag, r.StakingKey)] += uint64(
			r.Stake,
		)
	}
	return out
}

func sameFigures(want, got map[string]uint64) (bool, []any) {
	return sameLiveStake(want, got, false),
		[]any{"diff", liveStakeDiff(want, got)}
}

func samePoolHashes(want, got [][]byte) (bool, []any) {
	w, g := poolSet(want), poolSet(got)
	same := len(w) == len(g)
	for p := range w {
		_, ok := g[p]
		same = same && ok
	}
	return same, []any{"store_count", len(w), "set_count", len(g)}
}

// boundaryStakeFamilies are the families the metadata store's epoch-boundary
// stake answer reads.
var boundaryStakeFamilies = nodesettings.FamilyMask(0).
	With(nodesettings.FamilyEpochs).
	With(nodesettings.FamilyPParamsUpdates).
	With(nodesettings.FamilyAccounts).
	With(nodesettings.FamilyPoolsHistory).
	With(nodesettings.FamilyRewardInputs).
	With(nodesettings.FamilyLiveStake)

// boundaryPoolFamilies are the families the metadata store's epoch-boundary
// active pool answer reads: the pool rows, and the protocol parameters that
// decide whether the snapshot follows the enactment.
var boundaryPoolFamilies = poolFamilies |
	nodesettings.FamilyMask(0).With(nodesettings.FamilyPParamsUpdates)

// boundaryStakeRead is asOfRead for a read of the epoch-boundary stake. The
// set serves it when it serves live stake and, on a chain that certifies Leios
// closures, holds the stake, delegation, pool, witness and closure output
// histories the effective-slot derivation reads.
func boundaryStakeRead[T any](
	s stagingLedgerDB,
	txn ledgerdb.Txn,
	what string,
	slot uint64,
	current bool,
	fromStore func() (T, error),
	fromSet func(b *boundaryStake) (T, error),
	boundary uint64,
	same func(want, got T) (bool, []any),
) (T, error) {
	leios := s.ls.leiosCertifies()
	return answerAsOf(s.ls, txn, slot, asOfRead[T]{
		what:     what,
		families: boundaryStakeFamilies,
		current:  func() bool { return current },
		serves: func() bool {
			if leios && !s.ls.leiosStakeReady(txn) {
				return false
			}
			_, ok, err := s.liveStakeServed(txn, true)
			return err == nil && ok
		},
		verifies: func() bool {
			if leios && !s.ls.leiosStakeReady(txn) {
				return false
			}
			src, ok := s.ls.liveStakeSource(txn)
			if !ok {
				return false
			}
			if src.rr != nil {
				return true
			}
			pending, err := s.HasPendingRewardCreditRounds(txn)
			return err == nil && !pending
		},
		compare:   s.ls.compareLiveStakeRead,
		fromStore: fromStore,
		fromSet: func() (T, error) {
			b, err := s.ls.boundaryStakeFor(txn, slot, boundary, !current)
			if err != nil {
				var zero T
				return zero, err
			}
			return fromSet(b)
		},
		same: same,
	})
}

func (s stagingLedgerDB) GetEpochBoundaryDelegatedPoolKeyHashes(
	snapshotSlot uint64,
	boundarySlot uint64,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	if boundarySlot <= snapshotSlot {
		boundarySlot = 0
	}
	return boundaryStakeRead(s, txn,
		fmt.Sprintf("epoch boundary delegated pools at slot %d", snapshotSlot),
		snapshotSlot, s.ls.boundaryCurrent(txn, snapshotSlot),
		func() ([][]byte, error) {
			return s.LedgerDB.GetEpochBoundaryDelegatedPoolKeyHashes(
				snapshotSlot, boundarySlot, txn,
			)
		},
		func(b *boundaryStake) ([][]byte, error) {
			delegs, err := b.delegations()
			if err != nil {
				return nil, err
			}
			pools := make(map[string]struct{})
			for _, d := range delegs {
				pools[string(d.pool)] = struct{}{}
			}
			out := make([][]byte, 0, len(pools))
			for p := range pools {
				out = append(out, []byte(p))
			}
			slices.SortFunc(out, bytes.Compare)
			return out, nil
		},
		boundarySlot,
		samePoolHashes,
	)
}

func (s stagingLedgerDB) GetEpochBoundaryStakeByPools(
	poolKeyHashes [][]byte,
	snapshotSlot uint64,
	boundarySlot uint64,
	incomingEraID *uint,
	expiryEpoch uint64,
	inactivityPeriod uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, map[string]uint64, error) {
	if boundarySlot <= snapshotSlot {
		boundarySlot = 0
		incomingEraID = nil
	}
	res, err := boundaryStakeRead(s, txn,
		fmt.Sprintf("epoch boundary stake by pools at slot %d", snapshotSlot),
		snapshotSlot, s.ls.boundaryCurrent(txn, snapshotSlot),
		func() (poolStakeResult, error) {
			stakes, delegators, err := s.LedgerDB.GetEpochBoundaryStakeByPools(
				poolKeyHashes, snapshotSlot, boundarySlot, incomingEraID,
				expiryEpoch, inactivityPeriod, txn,
			)
			return poolStakeResult{stakes, delegators}, err
		},
		func(b *boundaryStake) (poolStakeResult, error) {
			res := poolStakeResult{
				stakes:     make(map[string]uint64, len(poolKeyHashes)),
				delegators: make(map[string]uint64, len(poolKeyHashes)),
			}
			for _, p := range poolKeyHashes {
				res.stakes[string(p)] = 0
				res.delegators[string(p)] = 0
			}
			if len(poolKeyHashes) == 0 {
				return res, nil
			}
			rows, err := b.inputs(
				poolSet(
					poolKeyHashes,
				),
				expiryEpoch,
				inactivityPeriod,
				incomingEraID,
			)
			if err != nil {
				return res, err
			}
			for _, in := range rows {
				res.delegators[string(in.pool)]++
				if in.stake > math.MaxUint64-res.stakes[string(in.pool)] {
					return res, errors.New("historical stake overflow")
				}
				res.stakes[string(in.pool)] += in.stake
			}
			return res, nil
		},
		boundarySlot,
		func(want, got poolStakeResult) (bool, []any) {
			return sameFigures(want.figures(), got.figures())
		},
	)
	return res.stakes, res.delegators, err
}

func (s stagingLedgerDB) GetEpochBoundaryRewardStakeInputsForPools(
	poolKeyHashes [][]byte,
	snapshotSlot uint64,
	boundarySlot uint64,
	incomingEraID *uint,
	expiryEpoch uint64,
	inactivityPeriod uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	if boundarySlot <= snapshotSlot {
		boundarySlot = 0
		incomingEraID = nil
	}
	return boundaryStakeRead(
		s,
		txn,
		fmt.Sprintf(
			"epoch boundary reward stake inputs at slot %d",
			snapshotSlot,
		),
		snapshotSlot,
		s.ls.boundaryCurrent(txn, snapshotSlot),
		func() ([]*models.RewardStakeInput, error) {
			return s.LedgerDB.GetEpochBoundaryRewardStakeInputsForPools(
				poolKeyHashes, snapshotSlot, boundarySlot, incomingEraID,
				expiryEpoch, inactivityPeriod, txn,
			)
		},
		func(b *boundaryStake) ([]*models.RewardStakeInput, error) {
			if len(poolKeyHashes) == 0 {
				return nil, nil
			}
			rows, err := b.inputs(
				poolSet(
					poolKeyHashes,
				),
				expiryEpoch,
				inactivityPeriod,
				incomingEraID,
			)
			if err != nil {
				return nil, err
			}
			out := make([]*models.RewardStakeInput, 0, len(rows))
			for _, in := range rows {
				if in.stake == 0 {
					continue
				}
				out = append(out, &models.RewardStakeInput{
					PoolKeyHash:   bytes.Clone(in.pool),
					CredentialTag: in.ref.Tag,
					StakingKey:    bytes.Clone(in.ref.Key),
					Stake:         types.Uint64(in.stake),
					Registered:    true,
				})
			}
			return out, nil
		},
		boundarySlot,
		func(want, got []*models.RewardStakeInput) (bool, []any) {
			return sameFigures(inputFigures(want), inputFigures(got))
		},
	)
}

func (s stagingLedgerDB) GetPointerStakeInputsForPools(
	poolKeyHashes [][]byte,
	slot uint64,
	boundarySlot uint64,
	expiryEpoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	if len(poolKeyHashes) == 0 {
		return s.LedgerDB.GetPointerStakeInputsForPools(
			poolKeyHashes, slot, boundarySlot, expiryEpoch, txn,
		)
	}
	return boundaryStakeRead(s, txn,
		fmt.Sprintf("pointer stake inputs at slot %d", slot),
		slot, s.ls.boundaryCurrent(txn, slot),
		func() ([]*models.RewardStakeInput, error) {
			return s.LedgerDB.GetPointerStakeInputsForPools(
				poolKeyHashes, slot, boundarySlot, expiryEpoch, txn,
			)
		},
		func(b *boundaryStake) ([]*models.RewardStakeInput, error) {
			if !b.pointerCounted(nil) {
				return nil, nil
			}
			pointers, err := b.pointerStake()
			if err != nil || len(pointers) == 0 {
				return nil, err
			}
			delegs, err := b.delegations()
			if err != nil {
				return nil, err
			}
			wanted := poolSet(poolKeyHashes)
			out := make([]*models.RewardStakeInput, 0, len(pointers))
			for _, d := range delegs {
				n := pointers[d.ref.MapKey()]
				if _, ok := wanted[string(d.pool)]; !ok || n == 0 {
					continue
				}
				// The gate reads the account as it is, not as it was.
				cur := d.acct
				if b.past {
					if cur, err = b.ar.Account(d.ref.Tag, d.ref.Key); err != nil {
						return nil, err
					}
				}
				if cur != nil && !expiryAllows(cur, expiryEpoch) {
					continue
				}
				out = append(out, &models.RewardStakeInput{
					PoolKeyHash:   bytes.Clone(d.pool),
					CredentialTag: d.ref.Tag,
					StakingKey:    bytes.Clone(d.ref.Key),
					Stake:         types.Uint64(n),
					Registered:    true,
				})
			}
			return out, nil
		},
		max(boundarySlot, slot),
		func(want, got []*models.RewardStakeInput) (bool, []any) {
			return sameFigures(inputFigures(want), inputFigures(got))
		},
	)
}

// activePoolEpoch is the epoch whose retirements the active pool filter
// compares against: the one holding epochSlot, or the epoch a boundary slot
// starts when its row is not yet written.
func activePoolEpoch(
	epochs []models.Epoch,
	slot, epochSlot uint64,
) (uint64, error) {
	e, ok := epochAt(epochs, epochSlot)
	if !ok {
		return 0, fmt.Errorf(
			"GetActivePoolKeyHashesAtSlot: %w", types.ErrNoEpochData,
		)
	}
	end := e.StartSlot + uint64(e.LengthInSlots)
	if epochSlot != slot && epochSlot == end {
		return e.EpochId + 1, nil
	}
	if epochSlot >= end {
		return 0, fmt.Errorf(
			"GetActivePoolKeyHashesAtSlot: %w", types.ErrNoEpochData,
		)
	}
	return e.EpochId, nil
}

func (s stagingLedgerDB) GetEpochBoundaryActivePoolKeyHashes(
	slot, boundarySlot uint64,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	current := s.ls.boundaryCurrent(txn, slot)
	return answerAsOf(s.ls, txn, slot, asOfRead[[][]byte]{
		what:     fmt.Sprintf("epoch boundary active pools at slot %d", slot),
		families: boundaryPoolFamilies,
		current:  func() bool { return current },
		serves:   func() bool { return s.ls.poolServing(txn) != nil },
		verifies: func() bool {
			return s.ls.poolVerifyReader(txn) != nil
		},
		compare: func(g verifyReadGuard, what string, same bool, setErr error, _ ...any) {
			s.ls.comparePoolRead(g, what, same, setErr)
		},
		fromStore: func() ([][]byte, error) {
			return s.LedgerDB.GetEpochBoundaryActivePoolKeyHashes(
				slot, boundarySlot, txn,
			)
		},
		fromSet: func() ([][]byte, error) {
			return s.ls.boundaryActivePools(txn, slot, boundarySlot, !current)
		},
		same: samePoolHashes,
	})
}

// boundaryActivePools is GetEpochBoundaryActivePoolKeyHashes from the pool
// states: a registered pool whose pending retirement falls after the epoch
// the snapshot compares against.
func (ls *LedgerState) boundaryActivePools(
	txn ledgerdb.Txn,
	slot, boundary uint64,
	past bool,
) ([][]byte, error) {
	r := ls.poolReader(txn)
	if r == nil {
		return nil, ls.unavailableAsOf("pools")
	}
	epochs, err := ls.epochRows(txn)
	if err != nil {
		return nil, err
	}
	after, err := ls.snapAfterEnactment(txn, epochs, boundary)
	if err != nil {
		return nil, err
	}
	epochSlot := slot
	if after {
		epochSlot = boundary
	}
	epoch, err := activePoolEpoch(epochs, slot, epochSlot)
	if err != nil {
		return nil, err
	}
	if ls.leiosCertifies() {
		// A certified registration or retirement takes effect at the
		// closure's context slot, which only the certificate history keeps,
		// so the pool states cannot say who was active.
		book, err := ls.poolBook(txn)
		if err != nil {
			return nil, ls.unavailableAsOf("pool certificate history")
		}
		return book.activeAt(slot, epoch)
	}
	out := [][]byte{}
	keep := func(st *PoolState) {
		if st != nil && (st.RetiringEpoch == nil || *st.RetiringEpoch > epoch) {
			out = append(out, bytes.Clone(st.Pool))
		}
	}
	if past {
		err = r.(PoolAsOfReader).PoolStatesAt(slot, func(st *PoolState) error {
			keep(st)
			return nil
		})
	} else {
		var pools [][]byte
		if pools, err = ls.poolKeys(r, txn); err == nil {
			for _, p := range pools {
				var st *PoolState
				if st, err = ls.poolView(r, txn, p); err != nil {
					break
				}
				keep(st)
			}
		}
	}
	slices.SortFunc(out, bytes.Compare)
	return out, err
}
