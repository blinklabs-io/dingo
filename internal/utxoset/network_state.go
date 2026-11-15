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

package utxoset

import (
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger"
)

func networkStateRow(n models.NetworkState) badgerutxo.NetworkStateRow {
	return badgerutxo.NetworkStateRow{
		Slot:     n.Slot,
		Treasury: uint64(n.Treasury),
		Reserves: uint64(n.Reserves),
	}
}

func donationRow(d models.NetworkDonation) badgerutxo.DonationRow {
	return badgerutxo.DonationRow{Slot: d.Slot, Epoch: d.Epoch, Amount: d.Amount}
}

// networkStateBatch converts a commit's network state and donation writes
// for the store.
func networkStateBatch(lc ledger.LedgerChanges) (
	[]badgerutxo.NetworkStateRow,
	[]badgerutxo.DonationRow,
) {
	states := make([]badgerutxo.NetworkStateRow, 0, len(lc.NetworkStates))
	for _, n := range lc.NetworkStates {
		states = append(states, networkStateRow(n))
	}
	donations := make([]badgerutxo.DonationRow, 0, len(lc.Donations))
	for _, d := range lc.Donations {
		donations = append(donations, donationRow(d))
	}
	return states, donations
}

// ensureNetworkState loads the metadata store's network state and donation
// rows into a set from before it kept them.
func (s *Set) ensureNetworkState(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	loaded, err := s.store.NetworkStateLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the network state marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	// The metadata store lists network state only by slot, so walk it from
	// the newest row down.
	var states []badgerutxo.NetworkStateRow
	for slot := uint64(math.MaxInt64); ; {
		row, err := ldb.GetNetworkStateAsOfSlot(slot, txn)
		if err != nil {
			return fmt.Errorf("utxoset: read network state: %w", err)
		}
		if row == nil {
			break
		}
		states = append(states, networkStateRow(*row))
		if row.Slot == 0 {
			break
		}
		slot = row.Slot - 1
	}
	slices.Reverse(states)
	rows, err := ldb.ListNetworkDonations(txn)
	if err != nil {
		return fmt.Errorf("utxoset: read network donations: %w", err)
	}
	donations := make([]badgerutxo.DonationRow, 0, len(rows))
	for _, d := range rows {
		donations = append(donations, donationRow(d))
	}
	if err := s.store.LoadNetworkState(states, donations); err != nil {
		return fmt.Errorf("utxoset: load network state: %w", err)
	}
	s.logger.Info("loaded network state into the live UTxO set",
		"network_states", len(states), "donations", len(donations))
	return nil
}

// NetworkStateLoaded reports whether the set holds every network state and
// donation row.
func (s *Set) NetworkStateLoaded() (bool, error) {
	return s.store.NetworkStateLoaded()
}

// NetworkStateAt returns the newest network state at or before slot, or
// nil.
func (s *Set) NetworkStateAt(slot uint64) (*models.NetworkState, error) {
	row, ok, err := s.store.NetworkStateAt(slot)
	if err != nil || !ok {
		return nil, err
	}
	return &models.NetworkState{
		Slot:     row.Slot,
		Treasury: types.Uint64(row.Treasury),
		Reserves: types.Uint64(row.Reserves),
	}, nil
}

// NetworkStates returns every network state row the set holds, in slot
// order.
func (s *Set) NetworkStates() ([]models.NetworkState, error) {
	rows, err := s.store.NetworkStates()
	if err != nil {
		return nil, err
	}
	out := make([]models.NetworkState, 0, len(rows))
	for _, r := range rows {
		out = append(out, models.NetworkState{
			Slot:     r.Slot,
			Treasury: types.Uint64(r.Treasury),
			Reserves: types.Uint64(r.Reserves),
		})
	}
	return out, nil
}

// NetworkDonations returns every donation row the set holds, in slot
// order.
func (s *Set) NetworkDonations() ([]models.NetworkDonation, error) {
	rows, err := s.store.Donations()
	if err != nil {
		return nil, err
	}
	out := make([]models.NetworkDonation, 0, len(rows))
	for _, r := range rows {
		out = append(out, models.NetworkDonation{
			Slot: r.Slot, Epoch: r.Epoch, Amount: r.Amount,
		})
	}
	return out, nil
}
