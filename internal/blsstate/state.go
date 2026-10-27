// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blsstate

import (
	"bytes"
	"errors"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ErrProviderUnavailable reports that transaction validation cannot resolve
// active and deferred BLS key claims.
var ErrProviderUnavailable = errors.New("BLS key ownership state unavailable")

// Provider resolves the stake pool that owns a BLS voting public key.
type Provider interface {
	IsBlsKeyInUse([]byte) (bool, lcommon.PoolKeyHash, error)
}

// ProviderFor finds the first BLS ownership provider in a validation-state
// adapter chain.
func ProviderFor(state lcommon.LedgerState) (Provider, bool) {
	for state != nil {
		if provider, ok := state.(Provider); ok {
			return provider, true
		}
		unwrapper, ok := state.(lcommon.LedgerStateUnwrapper)
		if !ok {
			break
		}
		state = unwrapper.UnwrapLedgerState()
	}
	return nil, false
}

type poolState struct {
	current     *lcommon.PoolRegistrationCertificate
	future      *lcommon.PoolRegistrationCertificate
	futureKnown bool
}

// Overlay applies pending pool registrations over a BLS ownership provider.
type Overlay struct {
	state    lcommon.LedgerState
	provider Provider
	pools    map[lcommon.PoolKeyHash]*poolState
}

// NewOverlay returns an empty pending-registration overlay.
func NewOverlay(state lcommon.LedgerState, provider Provider) *Overlay {
	return &Overlay{
		state:    state,
		provider: provider,
		pools:    make(map[lcommon.PoolKeyHash]*poolState),
	}
}

// IsBlsKeyInUse reports the owner after applying pending registrations.
func (o *Overlay) IsBlsKeyInUse(
	key []byte,
) (bool, lcommon.PoolKeyHash, error) {
	for pool, state := range o.pools {
		if registrationUsesKey(state.current, key) ||
			registrationUsesKey(state.future, key) {
			return true, pool, nil
		}
	}
	if o.provider == nil {
		return false, lcommon.PoolKeyHash{}, ErrProviderUnavailable
	}
	inUse, owner, err := o.provider.IsBlsKeyInUse(key)
	if err != nil || !inUse {
		return inUse, owner, err
	}
	state, changed := o.pools[owner]
	if !changed || registrationUsesKey(state.current, key) {
		return true, owner, nil
	}
	if state.futureKnown {
		return registrationUsesKey(state.future, key), owner, nil
	}
	return true, owner, nil
}

// ApplyRegistration records one pool registration in ledger order.
func (o *Overlay) ApplyRegistration(
	registration *lcommon.PoolRegistrationCertificate,
) error {
	if registration == nil {
		return nil
	}
	state, ok := o.pools[registration.Operator]
	if !ok {
		current, _, err := o.state.PoolCurrentState(registration.Operator)
		if err != nil {
			return err
		}
		state = &poolState{current: current}
		o.pools[registration.Operator] = state
	}
	if state.current == nil {
		state.current = registration
		return nil
	}
	state.future = registration
	state.futureKnown = true
	return nil
}

// ApplyTransaction records each valid transaction level in ledger order.
func (o *Overlay) ApplyTransaction(tx lcommon.Transaction) error {
	if tx == nil || !tx.IsValid() {
		return nil
	}
	levels := []lcommon.LedgerEffectLevel{{Body: tx}}
	if leveled, ok := tx.(lcommon.LeveledTransaction); ok {
		var err error
		levels, err = leveled.LedgerEffectLevels()
		if err != nil {
			return err
		}
	}
	for _, level := range levels {
		for _, certificate := range level.Body.Certificates() {
			registration, ok := certificate.(*lcommon.PoolRegistrationCertificate)
			if !ok {
				continue
			}
			if err := o.ApplyRegistration(registration); err != nil {
				return err
			}
		}
	}
	return nil
}

func registrationUsesKey(
	registration *lcommon.PoolRegistrationCertificate,
	key []byte,
) bool {
	return registration != nil && registration.LeiosKey != nil &&
		bytes.Equal(registration.LeiosKey.PublicKey, key)
}
