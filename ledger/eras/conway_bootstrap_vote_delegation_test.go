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
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stakeRegisteredLedgerState wraps mockLedgerState so a test can report a
// stake credential as registered while leaving DRepRegistration at its
// default (unregistered) stub. mockLedgerState.IsStakeCredentialRegistered
// unconditionally returns false, so a plain mockLedgerState can never reach
// the DRep-registration check in conway.UtxoValidateDelegation: the stake
// credential check fails first.
type stakeRegisteredLedgerState struct {
	*mockLedgerState
}

func (s *stakeRegisteredLedgerState) IsStakeCredentialRegistered(
	_ lcommon.Credential,
) bool {
	return true
}

// voteDelegationTx implements just enough of lcommon.Transaction for
// conway.UtxoValidateDelegation: IsValid (it only runs its checks for
// phase-1-valid transactions) and Certificates.
type voteDelegationTx struct {
	lcommon.Transaction
	certs []lcommon.Certificate
}

func (t *voteDelegationTx) IsValid() bool { return true }

func (t *voteDelegationTx) Certificates() []lcommon.Certificate {
	return t.certs
}

func mkConwayPpMajor(major uint) *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: major,
		},
	}
}

// TestValidateDelegationConwayBootstrapAware_UnregisteredDRep is the
// regression test for the dingo live incident (Preview, epoch 646, slot
// 55847379, block_hash 8cc2e91a1fbda7d61e9781e4a71a5c6e49eb039464b93121cad
// 1208869b283ba): a vote-delegation certificate targeting a DRep credential
// that has not registered yet is valid at PV9 (the Conway bootstrap phase)
// and only becomes invalid from PV10 (Plomin) onward.
//
// Ground truth for the incident's own transaction: Koios shows DRep
// 0e4bdd698b4cc2f2e518b4b1fa5190d0bc566ac42f93c26986ecaa79 first
// registering at block_time 1740052671 (2025-02-20), roughly seven months
// after the delegation certificate at block_time 1722503379 (2024-08-01,
// epoch 646). The transaction is confirmed on the real Preview chain, so
// cardano-ledger accepted it; only dingo's check was wrong.
func TestValidateDelegationConwayBootstrapAware_UnregisteredDRep(t *testing.T) {
	unregisteredDRepCred := []byte{
		0x0e, 0x4b, 0xdd, 0x69, 0x8b, 0x4c, 0xc2, 0xf2, 0xe5, 0x18,
		0xb4, 0xb1, 0xfa, 0x51, 0x90, 0xd0, 0xbc, 0x56, 0x6a, 0xc4,
		0x2f, 0x93, 0xc2, 0x69, 0x86, 0xec, 0xaa, 0x79,
	}
	stakeCred := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224([]byte("some-registered-stake-key-hash")),
	}
	cert := &lcommon.VoteDelegationCertificate{
		CertType:        9,
		StakeCredential: stakeCred,
		Drep: lcommon.Drep{
			Type:       lcommon.DrepTypeAddrKeyHash,
			Credential: unregisteredDRepCred,
		},
	}
	tx := &voteDelegationTx{certs: []lcommon.Certificate{cert}}
	ls := &stakeRegisteredLedgerState{mockLedgerState: newMockLedgerState()}

	t.Run("PV9 bootstrap phase accepts delegation to a not-yet-registered DRep", func(t *testing.T) {
		err := validateDelegationConwayBootstrapAware(
			tx, 55847379, ls, mkConwayPpMajor(lcommon.ProtocolVersionConway),
		)
		require.NoError(
			t,
			err,
			"PV9 must accept a vote delegation to an unregistered DRep, matching "+
				"cardano-ledger's checkDRepRegistered, which is skipped "+
				"`unless (hardforkConwayBootstrapPhase ...)`",
		)
	})

	t.Run("PV10 rejects delegation to an unregistered DRep", func(t *testing.T) {
		err := validateDelegationConwayBootstrapAware(
			tx, 55847379, ls, mkConwayPpMajor(lcommon.ProtocolVersionPlomin),
		)
		var drepErr conway.DelegateVoteToUnregisteredDRepError
		require.ErrorAs(
			t,
			err,
			&drepErr,
			"PV10 (Plomin) and later must still reject delegation to an "+
				"unregistered DRep",
		)
	})

	t.Run("unrelated delegation failures are not swallowed at PV9", func(t *testing.T) {
		unregisteredStakeCert := &lcommon.VoteDelegationCertificate{
			CertType: 9,
			StakeCredential: lcommon.Credential{
				CredType: lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.NewBlake2b224(
					[]byte("unregistered-stake-key-hash"),
				),
			},
			Drep: lcommon.Drep{
				Type:       lcommon.DrepTypeAddrKeyHash,
				Credential: unregisteredDRepCred,
			},
		}
		badTx := &voteDelegationTx{
			certs: []lcommon.Certificate{unregisteredStakeCert},
		}
		plainLs := newMockLedgerState()
		err := validateDelegationConwayBootstrapAware(
			badTx, 55847379, plainLs, mkConwayPpMajor(lcommon.ProtocolVersionConway),
		)
		var stakeErr conway.DelegateUnregisteredStakeCredentialError
		require.ErrorAs(
			t,
			err,
			&stakeErr,
			"the bootstrap relaxation must be scoped to the DRep-registration "+
				"check only; an unregistered stake credential must still fail "+
				"at PV9",
		)
	})
}

func TestIsConwayBootstrapPhase(t *testing.T) {
	assert.True(t, isConwayBootstrapPhase(mkConwayPpMajor(lcommon.ProtocolVersionConway)))
	assert.False(t, isConwayBootstrapPhase(mkConwayPpMajor(lcommon.ProtocolVersionPlomin)))
	assert.False(t, isConwayBootstrapPhase(mkConwayPpMajor(lcommon.ProtocolVersionConway-1)))
	assert.False(t, isConwayBootstrapPhase(&conway.ConwayProtocolParameters{}))
}

// TestBuildConwayValidationRules_DelegationOverride confirms the composed
// Conway rule set replaces the upstream delegation rule (rather than
// dropping it, which would silently disable every other delegation check)
// with the bootstrap-aware wrapper.
func TestBuildConwayValidationRules_DelegationOverride(t *testing.T) {
	descriptors := conway.UtxoValidationRuleDescriptors()
	delegationIndex := requireRuleIdResolvesToFunc(
		t,
		descriptors,
		conway.UtxoValidationRules,
		lcommon.UtxoValidationRuleDelegation,
		"conway.UtxoValidateDelegation",
	)
	requireIndexedRulesReplaceRuleIndex(
		t,
		conwayUtxoValidationRules,
		delegationIndex,
		validateDelegationConwayBootstrapAware,
		"Conway validation must relax the DRep-registration check during "+
			"the PV9 bootstrap phase",
	)
}
