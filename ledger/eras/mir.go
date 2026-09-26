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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package eras

import (
	"errors"
	"fmt"
	"math/big"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

const (
	mirPotReserves = uint(0)
	mirPotTreasury = uint(1)
)

// MIRCredentialKey identifies one stake credential within one source pot, for
// the purpose of folding move instantaneous rewards deltas accumulated within
// an epoch. All three fields are required: a key hash and a script hash may
// coincidentally share 28 bytes (hence Tag), and the reference keeps reserves
// and treasury deltas in two entirely separate maps -- iRReserves and
// iRTreasury, each keyed by Credential -- so a surplus pending in one pot must
// never offset a deficit in the other (hence Pot). Pot uses the same 0
// (reserves) / 1 (treasury) convention as cert.Reward.Source and
// models.MIREffect.Pot.
type MIRCredentialKey struct {
	Tag        uint8
	Credential lcommon.Blake2b224
	Pot        uint
}

// MIRDelegState is the slice of cardano-ledger's DELEG environment and
// InstantaneousRewards state that the move instantaneous rewards predicates
// read: the chain account pots, the instantaneous rewards and pot transfers
// already committed earlier in the epoch, and the slot at which a certificate
// becomes too late for the epoch.
type MIRDelegState struct {
	// Reserves and Treasury are the chain account pots, which do not move
	// within a Shelley-through-Babbage epoch.
	Reserves uint64
	Treasury uint64
	// DeltaReserves and DeltaTreasury are the signed net pot-to-pot
	// transfers committed earlier in the epoch (deltaReserves and
	// deltaTreasury).
	DeltaReserves *big.Int
	DeltaTreasury *big.Int
	// Pending is iRReserves and iRTreasury for every credential named by a
	// distribution committed earlier in the epoch, folded with the rule the
	// requesting protocol version uses: summed from protocol version 5,
	// replaced by the later certificate's amount before it.
	Pending map[MIRCredentialKey]*big.Int
	// Cutoff is firstSlot(nextEpoch) - stabilityWindow. A certificate is
	// valid only in a slot strictly below it.
	Cutoff uint64
}

// MIRDelegStateProvider is satisfied by the dingo ledger state. The move
// instantaneous rewards predicates need epoch-scoped state that the gouroboros
// LedgerState interface does not expose. Exported so *ledger.LedgerView can
// assert conformance at compile time; a signature drift here would otherwise
// silently disable every predicate that reads it.
type MIRDelegStateProvider interface {
	MIRDelegState(slot uint64, additive bool) (MIRDelegState, error)
}

// MIRProducesNegativeUpdateError indicates a move instantaneous rewards
// certificate whose delta, folded into the InstantaneousRewards accumulated
// for its credential so far this epoch, would drive that credential's pending
// reward below zero.
//
// Reference: MIRProducesNegativeUpdate in delegTransition,
// eras/shelley/impl/src/Cardano/Ledger/Shelley/Rules/Deleg.hs.
type MIRProducesNegativeUpdateError struct {
	CredentialTag uint8
	Credential    lcommon.Blake2b224
	Pot           uint
	Existing      *big.Int
	Delta         *big.Int
}

func (e MIRProducesNegativeUpdateError) Error() string {
	existing := "<nil>"
	if e.Existing != nil {
		existing = e.Existing.String()
	}
	delta := "<nil>"
	if e.Delta != nil {
		delta = e.Delta.String()
	}
	return fmt.Sprintf(
		"MIR produces negative update: credential %x pot %d existing %s delta %s",
		e.Credential[:],
		e.Pot,
		existing,
		delta,
	)
}

// MIRCertificateTooLateError indicates a move instantaneous rewards
// certificate in the final stability window of its epoch.
//
// Reference: MIRCertificateTooLateinEpochDELEG in delegTransition,
// eras/shelley/impl/src/Cardano/Ledger/Shelley/Rules/Deleg.hs.
type MIRCertificateTooLateError struct {
	Slot   uint64
	Cutoff uint64
}

func (e MIRCertificateTooLateError) Error() string {
	return fmt.Sprintf(
		"MIR certificate too late in epoch: slot %d is not before cutoff %d",
		e.Slot,
		e.Cutoff,
	)
}

// MIRTransferNotCurrentlyAllowedError indicates a pot-to-pot move
// instantaneous rewards transfer at a protocol version before the Alonzo hard
// fork.
//
// Reference: MIRTransferNotCurrentlyAllowed in delegTransition,
// eras/shelley/impl/src/Cardano/Ledger/Shelley/Rules/Deleg.hs.
type MIRTransferNotCurrentlyAllowedError struct {
	Pot    uint
	Amount uint64
}

func (e MIRTransferNotCurrentlyAllowedError) Error() string {
	return fmt.Sprintf(
		"MIR pot transfer not allowed at this protocol version: pot %d amount %d",
		e.Pot,
		e.Amount,
	)
}

// InsufficientForTransferError indicates a pot-to-pot move instantaneous
// rewards transfer larger than what its source pot has left.
//
// Reference: InsufficientForTransferDELEG in delegTransition,
// eras/shelley/impl/src/Cardano/Ledger/Shelley/Rules/Deleg.hs.
type InsufficientForTransferError struct {
	Pot       uint
	Amount    *big.Int
	Available *big.Int
}

func (e InsufficientForTransferError) Error() string {
	return fmt.Sprintf(
		"insufficient funds for MIR pot transfer: pot %d amount %s available %s",
		e.Pot,
		e.Amount,
		e.Available,
	)
}

// InsufficientForInstantaneousRewardsError indicates a move instantaneous
// rewards distribution whose pot's accumulated total exceeds the pot.
//
// Reference: InsufficientForInstantaneousRewardsDELEG in delegTransition,
// eras/shelley/impl/src/Cardano/Ledger/Shelley/Rules/Deleg.hs.
type InsufficientForInstantaneousRewardsError struct {
	Pot       uint
	Required  *big.Int
	Available *big.Int
}

func (e InsufficientForInstantaneousRewardsError) Error() string {
	return fmt.Sprintf(
		"insufficient funds for MIR distribution: pot %d required %s available %s",
		e.Pot,
		e.Required,
		e.Available,
	)
}

// mirDeleg carries the DELEG state for move instantaneous rewards across the
// certificates of one transaction, so each certificate is checked against the
// pots and pending rewards its predecessors left rather than against an
// epoch-wide aggregate.
type mirDeleg struct {
	slot uint64
	// transferAllowed is lcommon.MirTransferAllowed for the protocol
	// version. It gates pot transfers and switches the pending-rewards fold
	// from replacement to summation.
	transferAllowed bool
	// state is nil when the ledger state does not provide
	// MIRDelegStateProvider. Only the protocol-version predicate, which
	// needs nothing beyond the certificate, is checked then.
	state *MIRDelegState
}

func newMIRDeleg(
	slot uint64,
	ls lcommon.LedgerState,
	major uint,
) (*mirDeleg, error) {
	ret := &mirDeleg{
		slot:            slot,
		transferAllowed: lcommon.MirTransferAllowed(major),
	}
	provider, ok := ls.(MIRDelegStateProvider)
	if !ok {
		return ret, nil
	}
	state, err := provider.MIRDelegState(slot, ret.transferAllowed)
	if err != nil {
		return nil, fmt.Errorf("MIR DELEG state: %w", err)
	}
	if state.DeltaReserves == nil {
		state.DeltaReserves = new(big.Int)
	}
	if state.DeltaTreasury == nil {
		state.DeltaTreasury = new(big.Int)
	}
	if state.Pending == nil {
		state.Pending = make(map[MIRCredentialKey]*big.Int)
	}
	ret.state = &state
	return ret, nil
}

// apply checks one certificate and, when it is valid, folds its effect into
// the state the next certificate is checked against.
func (m *mirDeleg) apply(
	cert *lcommon.MoveInstantaneousRewardsCertificate,
) error {
	pot := cert.Reward.Source
	if pot != mirPotReserves && pot != mirPotTreasury {
		return fmt.Errorf("unknown MIR source pot %d", pot)
	}
	if m.state != nil && m.slot >= m.state.Cutoff {
		return MIRCertificateTooLateError{
			Slot:   m.slot,
			Cutoff: m.state.Cutoff,
		}
	}
	// The decoder leaves Rewards nil only for the coin target, so an empty
	// map is a distribution to nobody while nil is a transfer, even of zero.
	if cert.Reward.Rewards == nil {
		return m.applyTransfer(pot, cert.Reward.OtherPot)
	}
	return m.applyDistribution(pot, cert)
}

func (m *mirDeleg) applyTransfer(pot uint, amount uint64) error {
	if !m.transferAllowed {
		return MIRTransferNotCurrentlyAllowedError{Pot: pot, Amount: amount}
	}
	if m.state == nil {
		return nil
	}
	// availableAfterMIR: pot + delta - fold(pending distributions from pot).
	available := m.availablePot(pot)
	available.Sub(available, m.pendingTotal(pot))
	requested := new(big.Int).SetUint64(amount)
	if requested.Cmp(available) > 0 {
		return InsufficientForTransferError{
			Pot:       pot,
			Amount:    requested,
			Available: available,
		}
	}
	source, target := m.state.DeltaReserves, m.state.DeltaTreasury
	if pot == mirPotTreasury {
		source, target = target, source
	}
	source.Sub(source, requested)
	target.Add(target, requested)
	return nil
}

func (m *mirDeleg) applyDistribution(
	pot uint,
	cert *lcommon.MoveInstantaneousRewardsCertificate,
) error {
	if m.state == nil {
		return nil
	}
	updates := make(map[MIRCredentialKey]*big.Int, len(cert.Reward.Rewards))
	for cred, delta := range cert.Reward.RewardsAmount() {
		if cred == nil || delta == nil {
			return errors.New(
				"MIR distribution carries a nil credential or delta",
			)
		}
		key := MIRCredentialKey{
			//nolint:gosec // CredType is decode-validated to 0 or 1 by gouroboros
			Tag:        uint8(cred.CredType),
			Credential: cred.Credential,
			Pot:        pot,
		}
		// Before protocol version 5 the reference folds with Map.union, so
		// the later certificate's amount replaces the pending one; from 5
		// it folds with Map.unionWith (<>).
		if !m.transferAllowed {
			updates[key] = new(big.Int).Set(delta)
			continue
		}
		existing := m.state.Pending[key]
		if existing == nil {
			existing = new(big.Int)
		}
		result := new(big.Int).Add(existing, delta)
		if result.Sign() < 0 {
			return MIRProducesNegativeUpdateError{
				CredentialTag: key.Tag,
				Credential:    key.Credential,
				Pot:           key.Pot,
				Existing:      existing,
				Delta:         delta,
			}
		}
		updates[key] = result
	}
	required := m.pendingTotal(pot)
	for key, amount := range updates {
		if existing := m.state.Pending[key]; existing != nil {
			required.Sub(required, existing)
		}
		required.Add(required, amount)
	}
	// Pot transfers are disallowed before protocol version 5, so there the
	// reference compares against the bare pot.
	available := new(big.Int).SetUint64(m.potBalance(pot))
	if m.transferAllowed {
		available = m.availablePot(pot)
	}
	if required.Cmp(available) > 0 {
		return InsufficientForInstantaneousRewardsError{
			Pot:       pot,
			Required:  required,
			Available: available,
		}
	}
	for key, amount := range updates {
		m.state.Pending[key] = amount
	}
	return nil
}

func (m *mirDeleg) potBalance(pot uint) uint64 {
	if pot == mirPotTreasury {
		return m.state.Treasury
	}
	return m.state.Reserves
}

// availablePot returns the pot plus the net transfers into it so far.
func (m *mirDeleg) availablePot(pot uint) *big.Int {
	delta := m.state.DeltaReserves
	if pot == mirPotTreasury {
		delta = m.state.DeltaTreasury
	}
	return new(big.Int).Add(new(big.Int).SetUint64(m.potBalance(pot)), delta)
}

func (m *mirDeleg) pendingTotal(pot uint) *big.Int {
	total := new(big.Int)
	for key, amount := range m.state.Pending {
		if key.Pot == pot {
			total.Add(total, amount)
		}
	}
	return total
}
