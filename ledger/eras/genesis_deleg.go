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

package eras

import (
	"errors"
	"fmt"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// GenesisDelegPair is a genesis delegate's cold key hash and VRF key hash.
type GenesisDelegPair struct {
	Delegate lcommon.Blake2b224
	Vrf      lcommon.Blake2b256
}

// FutureGenesisDelegKey identifies a pending genesis delegation by the slot it
// activates at and the genesis key it replaces the delegate of.
type FutureGenesisDelegKey struct {
	Slot    uint64
	Genesis lcommon.Blake2b224
}

// GenesisDelegState is the slice of cardano-ledger's DELEG state that the
// genesis key delegation predicates read.
type GenesisDelegState struct {
	// Current maps every genesis key to the delegate in force at the slot
	// (dsGenDelegs). Its key set is the set of known genesis roots.
	Current map[lcommon.Blake2b224]GenesisDelegPair
	// Future holds the delegations certified but not yet in force
	// (dsFutureGenDelegs).
	Future map[FutureGenesisDelegKey]GenesisDelegPair
	// StabilityWindow is the delay between a certificate's slot and the slot
	// its delegation takes effect.
	StabilityWindow uint64
}

// GenesisDelegStateProvider is satisfied by the dingo ledger view. Exported so
// *ledger.LedgerView can assert conformance at compile time; a signature drift
// here would otherwise silently disable the genesis delegation predicates.
type GenesisDelegStateProvider interface {
	// GenesisDelegState returns the genesis delegation state at slot. The
	// provider applies the stability window, since it decides which pending
	// delegations have already taken effect.
	GenesisDelegState(slot uint64) (GenesisDelegState, error)
}

// GenesisKeyNotInMappingError indicates a genesis key delegation certificate
// for a key that is not a Shelley genesis key.
//
// Reference: GenesisKeyNotInMappingDELEG in delegTransition,
// eras/shelley/impl/src/Cardano/Ledger/Shelley/Rules/Deleg.hs.
type GenesisKeyNotInMappingError struct {
	GenesisHash lcommon.Blake2b224
}

func (e GenesisKeyNotInMappingError) Error() string {
	return fmt.Sprintf("genesis key not in mapping: %x", e.GenesisHash[:])
}

// DuplicateGenesisDelegateError indicates a genesis key delegation certificate
// whose delegate key is already used by another genesis key, in force or
// pending.
//
// Reference: DuplicateGenesisDelegateDELEG in delegTransition,
// eras/shelley/impl/src/Cardano/Ledger/Shelley/Rules/Deleg.hs.
type DuplicateGenesisDelegateError struct {
	Delegate lcommon.Blake2b224
}

func (e DuplicateGenesisDelegateError) Error() string {
	return fmt.Sprintf("duplicate genesis delegate: %x", e.Delegate[:])
}

// DuplicateGenesisVRFError indicates a genesis key delegation certificate
// whose VRF key is already used by another genesis key, in force or pending.
//
// Reference: DuplicateGenesisVRFDELEG in delegTransition,
// eras/shelley/impl/src/Cardano/Ledger/Shelley/Rules/Deleg.hs.
type DuplicateGenesisVRFError struct {
	Vrf lcommon.Blake2b256
}

func (e DuplicateGenesisVRFError) Error() string {
	return fmt.Sprintf("duplicate genesis VRF key: %x", e.Vrf[:])
}

// genesisDeleg checks genesis key delegation certificates against the state
// earlier certificates left, the way the reference's DELEG rule does.
type genesisDeleg struct {
	slot  uint64
	state GenesisDelegState
}

func newGenesisDeleg(
	slot uint64,
	ls lcommon.LedgerState,
) (*genesisDeleg, error) {
	provider, ok := stateCapability[GenesisDelegStateProvider](ls)
	if !ok {
		return nil, nil
	}
	state, err := provider.GenesisDelegState(slot)
	if err != nil {
		return nil, fmt.Errorf("genesis DELEG state: %w", err)
	}
	return &genesisDeleg{slot: slot, state: state}, nil
}

// apply checks one certificate and, when it is valid, records its delegation
// as pending.
func (g *genesisDeleg) apply(
	cert *lcommon.GenesisKeyDelegationCertificate,
) error {
	if len(cert.GenesisHash) != lcommon.Blake2b224Size ||
		len(cert.GenesisDelegateHash) != lcommon.Blake2b224Size {
		return errors.New(
			"invalid genesis key delegation certificate key lengths",
		)
	}
	genesis := lcommon.NewBlake2b224(cert.GenesisHash)
	delegate := lcommon.NewBlake2b224(cert.GenesisDelegateHash)
	vrf := lcommon.Blake2b256(cert.VrfKeyHash)
	if _, ok := g.state.Current[genesis]; !ok {
		return GenesisKeyNotInMappingError{GenesisHash: genesis}
	}
	// Only the pairs of other genesis keys count: a genesis key may keep its
	// own delegate or VRF key while changing the other.
	others := func(pair GenesisDelegPair) error {
		if pair.Delegate == delegate {
			return DuplicateGenesisDelegateError{Delegate: delegate}
		}
		if pair.Vrf == vrf {
			return DuplicateGenesisVRFError{Vrf: vrf}
		}
		return nil
	}
	for key, pair := range g.state.Current {
		if key == genesis {
			continue
		}
		if err := others(pair); err != nil {
			return err
		}
	}
	for key, pair := range g.state.Future {
		if key.Genesis == genesis {
			continue
		}
		if err := others(pair); err != nil {
			return err
		}
	}
	if g.state.Future == nil {
		g.state.Future = make(map[FutureGenesisDelegKey]GenesisDelegPair)
	}
	g.state.Future[FutureGenesisDelegKey{
		Slot:    g.slot + g.state.StabilityWindow,
		Genesis: genesis,
	}] = GenesisDelegPair{Delegate: delegate, Vrf: vrf}
	return nil
}
