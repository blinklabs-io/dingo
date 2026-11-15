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
	"cmp"
	"fmt"
	"math"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// NetworkStateReader serves network state and donation rows from a live
// UTxO set that keeps them.
type NetworkStateReader interface {
	// NetworkStateLoaded reports whether the reader holds every network
	// state and donation row; until it does, reads stay on the metadata
	// store.
	NetworkStateLoaded() (bool, error)
	// NetworkStateAt returns the newest network state at or before slot,
	// or nil.
	NetworkStateAt(slot uint64) (*models.NetworkState, error)
	NetworkDonations() ([]models.NetworkDonation, error)
}

// networkStateReader returns the set's network state reader and the read
// mode, or UtxoSetReadsOff when these reads stay on the metadata store.
func (ls *LedgerState) networkStateReader(
	txn ledgerdb.Txn,
) (NetworkStateReader, string) {
	_, mode := ls.utxoSetReader()
	if mode == UtxoSetReadsOff {
		return nil, UtxoSetReadsOff
	}
	r, ok := ls.readSet(txn).(NetworkStateReader)
	if !ok {
		return nil, UtxoSetReadsOff
	}
	if !ls.networkStateSetLoaded.Load() {
		loaded, err := r.NetworkStateLoaded()
		if err != nil || !loaded {
			return nil, UtxoSetReadsOff
		}
		ls.networkStateSetLoaded.Store(true)
	}
	return r, mode
}

// networkStateAt is the newest network state at or before slot as txn sees
// the set.
func (ls *LedgerState) networkStateAt(
	r NetworkStateReader,
	txn ledgerdb.Txn,
	slot uint64,
) (*models.NetworkState, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	ops := ls.stagedEpochStateOps(txn)
	// A staged rollback hides the committed rows after it, and the staged
	// rows after any later rollback.
	ceiling := slot
	staged := make(map[uint64]models.NetworkState)
	for _, o := range ops {
		if o.rollbackTo != nil {
			ceiling = min(ceiling, *o.rollbackTo)
			for s := range staged {
				if s > *o.rollbackTo {
					delete(staged, s)
				}
			}
		}
		for _, n := range o.networkStates {
			staged[n.Slot] = n
		}
	}
	best, err := r.NetworkStateAt(ceiling)
	if err != nil {
		return nil, err
	}
	for s, n := range staged {
		if s <= slot && (best == nil || s >= best.Slot) {
			c := n
			best = &c
		}
	}
	return best, nil
}

// donationsForEpoch sums epoch's donations as txn sees the set.
func (ls *LedgerState) donationsForEpoch(
	r NetworkStateReader,
	txn ledgerdb.Txn,
	epoch uint64,
) (uint64, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	ops := ls.stagedEpochStateOps(txn)
	rows, err := r.NetworkDonations()
	if err != nil {
		return 0, err
	}
	view := make(map[uint64]models.NetworkDonation, len(rows))
	for _, d := range rows {
		view[d.Slot] = d
	}
	for _, o := range ops {
		if o.rollbackTo != nil {
			for s := range view {
				if s > *o.rollbackTo {
					delete(view, s)
				}
			}
		}
		for _, d := range o.donations {
			view[d.Slot] = d
		}
	}
	var total uint64
	for _, d := range view {
		if d.Epoch != epoch {
			continue
		}
		if total > math.MaxUint64-d.Amount {
			return 0, fmt.Errorf("donations of epoch %d overflow", epoch)
		}
		total += d.Amount
	}
	return total, nil
}

// donationList lists every donation as txn sees the set, in slot order.
func (ls *LedgerState) donationList(
	r NetworkStateReader,
	txn ledgerdb.Txn,
) ([]models.NetworkDonation, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	ops := ls.stagedEpochStateOps(txn)
	rows, err := r.NetworkDonations()
	if err != nil {
		return nil, err
	}
	view := make(map[uint64]models.NetworkDonation, len(rows))
	for _, d := range rows {
		view[d.Slot] = d
	}
	for _, o := range ops {
		if o.rollbackTo != nil {
			for s := range view {
				if s > *o.rollbackTo {
					delete(view, s)
				}
			}
		}
		for _, d := range o.donations {
			view[d.Slot] = d
		}
	}
	out := make([]models.NetworkDonation, 0, len(view))
	for _, d := range view {
		out = append(out, d)
	}
	slices.SortFunc(out, func(a, b models.NetworkDonation) int {
		return cmp.Compare(a.Slot, b.Slot)
	})
	return out, nil
}

func sameNetworkState(a, b *models.NetworkState) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Slot == b.Slot && a.Treasury == b.Treasury &&
		a.Reserves == b.Reserves
}

func (ls *LedgerState) compareNetworkStateRead(
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
	ls.metrics.networkStateReadsVerified.Inc()
	if same && setErr == nil {
		return
	}
	n := ls.networkStateReadMismatches.Add(1)
	ls.metrics.networkStateReadMismatches.Inc()
	if n > 20 && n%1000 != 0 {
		return
	}
	ls.config.Logger.Warn(
		"live UTxO set network state read differs from the metadata store",
		"component", "ledger",
		"read", what,
		"set_error", setErr,
		"mismatches", n,
	)
}

func (s stagingLedgerDB) GetNetworkState(
	txn ledgerdb.Txn,
) (*models.NetworkState, error) {
	return s.networkStateAt(txn, "network state", math.MaxUint64,
		func() (*models.NetworkState, error) {
			return s.LedgerDB.GetNetworkState(txn)
		},
	)
}

func (s stagingLedgerDB) GetNetworkStateAsOfSlot(
	slot uint64,
	txn ledgerdb.Txn,
) (*models.NetworkState, error) {
	return s.networkStateAt(txn, fmt.Sprintf("network state at %d", slot), slot,
		func() (*models.NetworkState, error) {
			return s.LedgerDB.GetNetworkStateAsOfSlot(slot, txn)
		},
	)
}

func (s stagingLedgerDB) networkStateAt(
	txn ledgerdb.Txn,
	what string,
	slot uint64,
	stored func() (*models.NetworkState, error),
) (*models.NetworkState, error) {
	r, mode := s.ls.networkStateReader(txn)
	if mode == UtxoSetReadsOff || r == nil {
		if s.ls.familyDrained(nodesettings.FamilyNetworkDonations) {
			return nil, drainedUnserved(nodesettings.FamilyNetworkDonations)
		}
		return stored()
	}
	g := s.ls.beginVerifyRead(txn)
	fromSet, setErr := s.ls.networkStateAt(r, txn, slot)
	if mode == UtxoSetReadsSet {
		return fromSet, setErr
	}
	want, err := stored()
	if err != nil {
		return nil, err
	}
	s.ls.compareNetworkStateRead(g, what, sameNetworkState(want, fromSet), setErr)
	return want, nil
}

func (s stagingLedgerDB) SumNetworkDonationsForEpoch(
	epoch uint64,
	txn ledgerdb.Txn,
) (uint64, error) {
	r, mode := s.ls.networkStateReader(txn)
	if mode == UtxoSetReadsOff || r == nil {
		if s.ls.familyDrained(nodesettings.FamilyNetworkDonations) {
			return 0, drainedUnserved(nodesettings.FamilyNetworkDonations)
		}
		return s.LedgerDB.SumNetworkDonationsForEpoch(epoch, txn)
	}
	g := s.ls.beginVerifyRead(txn)
	fromSet, setErr := s.ls.donationsForEpoch(r, txn, epoch)
	if mode == UtxoSetReadsSet {
		return fromSet, setErr
	}
	want, err := s.LedgerDB.SumNetworkDonationsForEpoch(epoch, txn)
	if err != nil {
		return 0, err
	}
	s.ls.compareNetworkStateRead(
		g, fmt.Sprintf("donations of epoch %d", epoch), want == fromSet, setErr,
	)
	return want, nil
}

// ListNetworkDonations lists the set's donations, as txn sees them, when the
// family is drained; it is otherwise the metadata store's.
func (s stagingLedgerDB) ListNetworkDonations(
	txn ledgerdb.Txn,
) ([]models.NetworkDonation, error) {
	if !s.ls.familyDrained(nodesettings.FamilyNetworkDonations) {
		return s.LedgerDB.ListNetworkDonations(txn)
	}
	r, _ := s.ls.networkStateReader(txn)
	if r == nil {
		return nil, drainedUnserved(nodesettings.FamilyNetworkDonations)
	}
	return s.ls.donationList(r, txn)
}
