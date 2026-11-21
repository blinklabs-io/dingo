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

// PoolRegistrationSupersedes reports whether latest was registered after
// effective, ranked by (AddedSlot, CertificateID, RegistrationIndex). A
// snapshot stages a pending re-registration beside the parameters in force at
// the same slot, so the slot alone cannot order them. Both rows must still
// carry the identifiers CleanPoolRegistration drops.
func PoolRegistrationSupersedes(latest, effective *models.PoolRegistration) bool {
	if latest == nil || effective == nil {
		return false
	}
	if latest.AddedSlot != effective.AddedSlot {
		return latest.AddedSlot > effective.AddedSlot
	}
	if latest.CertificateID != effective.CertificateID {
		return latest.CertificateID > effective.CertificateID
	}
	return latest.RegistrationIndex > effective.RegistrationIndex
}

// CleanPoolRegistration copies the fields held in live pool state, including
// the Leios key registration epoch used to interpret the key's age.
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
		LeiosKeyRegistrationEpoch:  r.LeiosKeyRegistrationEpoch,
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

func (s stagingLedgerDB) GetPoolRegistrationsEffectiveForEpoch(
	poolKeyHashes []lcommon.PoolKeyHash,
	epochStartSlot uint64,
	endedEpoch uint64,
	snapshotSlot uint64,
	txn ledgerdb.Txn,
) ([]models.PoolRegistration, error) {
	return drainedPoolRead(s, txn, func(b *poolBook) ([]models.PoolRegistration, error) {
		return b.effective(
			poolKeyHashes, epochStartSlot, endedEpoch, snapshotSlot,
		)
	})
}

// UtxoSetTipReader is implemented by a live UTxO set that records the ledger
// point it last applied.
type UtxoSetTipReader interface {
	Tip() (ocommon.Point, bool, error)
}

func (s stagingLedgerDB) GetActivePoolKeyHashes(
	ctx context.Context,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	return drainedPoolRead(s, txn, func(b *poolBook) ([][]byte, error) {
		return b.active()
	})
}

// GetActivePoolKeyHashesAtSlot returns active pools recorded at slot by the set.
func (s stagingLedgerDB) GetActivePoolKeyHashesAtSlot(
	slot uint64,
	txn ledgerdb.Txn,
) ([][]byte, error) {
	return drainedPoolRead(s, txn, func(b *poolBook) ([][]byte, error) {
		return b.activeAtSlot(slot)
	})
}
