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

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// StakeKeyAlreadyRegisteredError indicates a legacy stake registration
// certificate for a credential that is already registered, in ledger state or
// by an earlier certificate of the same transaction.
//
// Reference: StakeKeyAlreadyRegisteredDELEG in delegTransition,
// eras/shelley/impl/src/Cardano/Ledger/Shelley/Rules/Deleg.hs.
type StakeKeyAlreadyRegisteredError struct {
	Credential lcommon.Credential
}

func (e StakeKeyAlreadyRegisteredError) Error() string {
	return fmt.Sprintf(
		"stake credential already registered: %x",
		e.Credential.Credential[:],
	)
}

// StakeKeyNotRegisteredError indicates a legacy stake deregistration
// certificate for a credential that is not registered at that point in the
// transaction.
//
// Reference: StakeKeyNotRegisteredDELEG in delegTransition,
// eras/shelley/impl/src/Cardano/Ledger/Shelley/Rules/Deleg.hs.
type StakeKeyNotRegisteredError struct {
	Credential lcommon.Credential
}

func (e StakeKeyNotRegisteredError) Error() string {
	return fmt.Sprintf(
		"stake credential not registered: %x",
		e.Credential.Credential[:],
	)
}

// StakeKeyNonZeroAccountBalanceError indicates a legacy stake deregistration
// certificate for a reward account that still holds rewards after the
// transaction's withdrawals.
//
// Reference: StakeKeyNonZeroAccountBalanceDELEG in delegTransition,
// eras/shelley/impl/src/Cardano/Ledger/Shelley/Rules/Deleg.hs.
type StakeKeyNonZeroAccountBalanceError struct {
	Credential lcommon.Credential
	Balance    uint64
}

func (e StakeKeyNonZeroAccountBalanceError) Error() string {
	return fmt.Sprintf(
		"stake credential has non-zero reward balance: %x balance %d",
		e.Credential.Credential[:],
		e.Balance,
	)
}

// validateShelleyDelegCerts enforces the Shelley-through-Babbage DELEG
// predicates that the upstream gouroboros rules leave out, walking the
// certificates in order the way DELEGS does so each one sees the state its
// predecessors left.
//
// It covers stake registration and deregistration, delegation from a
// credential deregistered earlier in the transaction, and the move
// instantaneous rewards cutoff, protocol-version, sign and pot-capacity
// predicates.
//
// Value conservation credits a key deposit refund for every deregistration and
// charges a deposit for every registration. Rejecting a deregistration of an
// unregistered credential and a duplicate registration here is what makes
// those refunds and deposits correspond to real account state.
//
// A phase-2-invalid transaction runs no DELEGS transition, so it is exempt.
func validateShelleyDelegCerts(
	tx lcommon.Transaction,
	slot uint64,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) error {
	if !tx.IsValid() {
		return nil
	}
	certs := tx.Certificates()
	if len(certs) == 0 {
		return nil
	}
	versioned, ok := pp.(interface{ ProtocolMajorVersion() uint })
	if !ok {
		return errors.New("pparams are not expected type")
	}
	stake, err := newStakeDeleg(tx, ls)
	if err != nil {
		return err
	}
	var mir *mirDeleg
	for _, cert := range certs {
		switch c := cert.(type) {
		case *lcommon.StakeRegistrationCertificate:
			if c == nil {
				continue
			}
			err = stake.register(c.StakeCredential)
		case *lcommon.StakeDeregistrationCertificate:
			if c == nil {
				continue
			}
			err = stake.deregister(c.StakeCredential)
		case *lcommon.StakeDelegationCertificate:
			if c == nil || c.StakeCredential == nil {
				continue
			}
			if !stake.isRegistered(*c.StakeCredential) {
				err = shelley.DelegateUnregisteredStakeCredentialError{
					Credential: *c.StakeCredential,
				}
			}
		case *lcommon.MoveInstantaneousRewardsCertificate:
			if c == nil {
				continue
			}
			if mir == nil {
				mir, err = newMIRDeleg(
					slot, ls, versioned.ProtocolMajorVersion(),
				)
				if err != nil {
					return err
				}
			}
			err = mir.apply(c)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

type stakeCredentialKey struct {
	credType   uint
	credential lcommon.Blake2b224
}

func newStakeCredentialKey(cred lcommon.Credential) stakeCredentialKey {
	return stakeCredentialKey{
		credType:   cred.CredType,
		credential: cred.Credential,
	}
}

// stakeDeleg tracks the registration state of every stake credential a
// transaction's certificates touch.
type stakeDeleg struct {
	ls lcommon.LedgerState
	// registered overrides ledger state for a credential once a certificate
	// of this transaction registers or deregisters it.
	registered map[stakeCredentialKey]bool
	// withdrawn holds the credentials whose reward accounts the
	// transaction's withdrawals drain. DELEGS drains withdrawals before it
	// processes any certificate.
	withdrawn map[stakeCredentialKey]struct{}
}

func newStakeDeleg(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
) (*stakeDeleg, error) {
	ret := &stakeDeleg{
		ls:         ls,
		registered: make(map[stakeCredentialKey]bool),
		withdrawn:  make(map[stakeCredentialKey]struct{}),
	}
	for addr := range tx.Withdrawals() {
		cred, err := addr.RewardAccountCredential()
		if err != nil {
			return nil, err
		}
		ret.withdrawn[newStakeCredentialKey(cred)] = struct{}{}
	}
	return ret, nil
}

func (s *stakeDeleg) isRegistered(cred lcommon.Credential) bool {
	if registered, ok := s.registered[newStakeCredentialKey(cred)]; ok {
		return registered
	}
	return s.ls.IsStakeCredentialRegistered(cred)
}

func (s *stakeDeleg) register(cred lcommon.Credential) error {
	if s.isRegistered(cred) {
		return StakeKeyAlreadyRegisteredError{Credential: cred}
	}
	s.registered[newStakeCredentialKey(cred)] = true
	return nil
}

func (s *stakeDeleg) deregister(cred lcommon.Credential) error {
	key := newStakeCredentialKey(cred)
	if !s.isRegistered(cred) {
		return StakeKeyNotRegisteredError{Credential: cred}
	}
	// A registration earlier in this transaction created the account with
	// a zero balance, and a drained account is zero by construction.
	_, drained := s.withdrawn[key]
	if _, touched := s.registered[key]; !touched && !drained {
		balance, err := s.ls.RewardAccountBalance(cred)
		if err != nil {
			return fmt.Errorf("reward account balance: %w", err)
		}
		if balance != nil && *balance != 0 {
			return StakeKeyNonZeroAccountBalanceError{
				Credential: cred,
				Balance:    *balance,
			}
		}
	}
	s.registered[key] = false
	return nil
}
