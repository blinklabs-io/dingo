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
	"fmt"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

type drepValidationCredentialKey struct {
	tag  uint
	hash common.Blake2b224
}

func drepValidationKey(credential common.Credential) drepValidationCredentialKey {
	return drepValidationCredentialKey{
		tag:  credential.CredType,
		hash: credential.Credential,
	}
}

type drepLifecycleValidationState struct {
	*mockLedgerState
	dreps map[drepValidationCredentialKey]common.DRepRegistration
	pools map[common.PoolKeyHash]bool
}

func (s *drepLifecycleValidationState) DRepRegistration(
	credential common.Credential,
) (*common.DRepRegistration, error) {
	registration, ok := s.dreps[drepValidationKey(credential)]
	if !ok {
		return nil, nil
	}
	return &registration, nil
}

func (s *drepLifecycleValidationState) IsPoolRegistered(
	pool common.PoolKeyHash,
) bool {
	return s.pools[pool]
}

type drepLifecycleValidationEra struct {
	name     string
	params   common.ProtocolParameters
	validate func(
		common.Transaction,
		uint64,
		common.LedgerState,
		common.ProtocolParameters,
	) error
}

func drepLifecycleValidationEras() []drepLifecycleValidationEra {
	conwayParams := &conway.ConwayProtocolParameters{
		ProtocolVersion: common.ProtocolParametersProtocolVersion{
			Major: common.ProtocolVersionPlomin,
		},
		MaxTxSize: 16_384,
	}
	dijkstraParams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: *conwayParams,
	}
	dijkstraParams.ProtocolVersion.Major = dijkstra.MinProtocolVersionDijkstra
	return []drepLifecycleValidationEra{
		{
			name:   "Conway",
			params: conwayParams,
			validate: func(
				tx common.Transaction,
				slot uint64,
				state common.LedgerState,
				params common.ProtocolParameters,
			) error {
				return ValidateTxConway(tx, slot, state, params)
			},
		},
		{
			name:   "Dijkstra",
			params: dijkstraParams,
			validate: func(
				tx common.Transaction,
				slot uint64,
				state common.LedgerState,
				params common.ProtocolParameters,
			) error {
				return ValidateTxDijkstra(tx, slot, state, params)
			},
		},
	}
}

func newDrepLifecycleValidationState(
	registrations ...common.Credential,
) *drepLifecycleValidationState {
	state := &drepLifecycleValidationState{
		mockLedgerState: &mockLedgerState{
			utxos:                make(map[string]common.Utxo),
			stakeRegistered:      make(map[common.Blake2b224]bool),
			rewardBalances:       make(map[common.Blake2b224]uint64),
			skipPhase2Validation: true,
		},
		dreps: make(map[drepValidationCredentialKey]common.DRepRegistration),
		pools: make(map[common.PoolKeyHash]bool),
	}
	deposit := uint64(0)
	for _, credential := range registrations {
		state.dreps[drepValidationKey(credential)] = common.DRepRegistration{
			Credential: credential,
			Deposit:    &deposit,
		}
	}
	return state
}

type drepLifecycleAssignment struct {
	name                 string
	build                func(common.Credential, common.PoolKeyHash) common.Certificate
	needsRegisteredStake bool
	needsRegisteredPool  bool
}

func drepLifecycleAssignments() []drepLifecycleAssignment {
	return []drepLifecycleAssignment{
		{
			name: "vote delegation",
			build: func(target common.Credential, _ common.PoolKeyHash) common.Certificate {
				return &common.VoteDelegationCertificate{
					CertType:        uint(common.CertificateTypeVoteDelegation),
					StakeCredential: drepLifecycleStakeCredential(),
					Drep: common.Drep{
						Type:       int(target.CredType),
						Credential: target.Credential[:],
					},
				}
			},
			needsRegisteredStake: true,
		},
		{
			name: "stake plus vote delegation",
			build: func(target common.Credential, pool common.PoolKeyHash) common.Certificate {
				return &common.StakeVoteDelegationCertificate{
					CertType:        uint(common.CertificateTypeStakeVoteDelegation),
					StakeCredential: drepLifecycleStakeCredential(),
					PoolKeyHash:     pool,
					Drep: common.Drep{
						Type:       int(target.CredType),
						Credential: target.Credential[:],
					},
				}
			},
			needsRegisteredStake: true,
			needsRegisteredPool:  true,
		},
		{
			name: "vote registration plus assignment",
			build: func(target common.Credential, _ common.PoolKeyHash) common.Certificate {
				return &common.VoteRegistrationDelegationCertificate{
					CertType:        uint(common.CertificateTypeVoteRegistrationDelegation),
					StakeCredential: drepLifecycleStakeCredential(),
					Drep: common.Drep{
						Type:       int(target.CredType),
						Credential: target.Credential[:],
					},
					Amount: 0,
				}
			},
		},
		{
			name: "stake plus vote registration",
			build: func(target common.Credential, pool common.PoolKeyHash) common.Certificate {
				return &common.StakeVoteRegistrationDelegationCertificate{
					CertType:        uint(common.CertificateTypeStakeVoteRegistrationDelegation),
					StakeCredential: drepLifecycleStakeCredential(),
					PoolKeyHash:     pool,
					Drep: common.Drep{
						Type:       int(target.CredType),
						Credential: target.Credential[:],
					},
					Amount: 0,
				}
			},
			needsRegisteredPool: true,
		},
	}
}

func drepLifecycleStakeCredential() common.Credential {
	return common.Credential{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: common.NewBlake2b224([]byte("drep-lifecycle-stake")),
	}
}

func drepLifecycleCertificateTx(
	certificates ...common.Certificate,
) *mockConwayFeeTx {
	return &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			cbor: []byte{0x80},
			fee:  big.NewInt(0),
			witnesses: &mockWitnessSet{
				nativeScripts: []common.NativeScript{
					drepLifecycleNativeScript(),
				},
			},
		},
		certificates: certificates,
	}
}

func drepLifecycleTarget(credentialType uint) common.Credential {
	nativeScript := drepLifecycleNativeScript()
	return common.Credential{
		CredType:   credentialType,
		Credential: common.Blake2b224(nativeScript.Hash()),
	}
}

func drepLifecycleNativeScript() common.NativeScript {
	return common.NativeScript{}
}

func prepareDrepLifecycleValidation(
	t *testing.T,
) {
	t.Helper()

	conwayRules := conwayUtxoValidationRules
	conwayPhase1Rules := conwayPhase1UtxoValidationRules
	dijkstraRules := dijkstraPhase1UtxoValidationRules
	t.Cleanup(func() {
		conwayUtxoValidationRules = conwayRules
		conwayPhase1UtxoValidationRules = conwayPhase1Rules
		dijkstraPhase1UtxoValidationRules = dijkstraRules
	})
	delegationRule := []indexedUtxoValidationRule{{
		validationFunc: validateDelegationConwayBootstrapAware,
	}}
	conwayUtxoValidationRules = delegationRule
	conwayPhase1UtxoValidationRules = delegationRule
	dijkstraPhase1UtxoValidationRules = []indexedUtxoValidationRule{{
		validationFunc: dijkstra.UtxoValidateDelegation,
	}}
}

func TestValidateTxConwayDRepDeregistrationBlocksLaterAssignment(t *testing.T) {
	prepareDrepLifecycleValidation(t)
	pool := common.PoolKeyHash(common.NewBlake2b224([]byte("drep-lifecycle-pool")))

	for _, era := range drepLifecycleValidationEras() {
		for _, targetType := range []uint{
			common.CredentialTypeAddrKeyHash,
			common.CredentialTypeScriptHash,
		} {
			target := drepLifecycleTarget(targetType)
			for _, assignment := range drepLifecycleAssignments() {
				t.Run(fmt.Sprintf("%s/%s/target-type-%d", era.name, assignment.name, targetType), func(t *testing.T) {
					state := newDrepLifecycleValidationState(target)
					if assignment.needsRegisteredStake {
						state.stakeRegistered[drepLifecycleStakeCredential().Credential] = true
					}
					if assignment.needsRegisteredPool {
						state.pools[pool] = true
					}
					tx := drepLifecycleCertificateTx(
						&common.DeregistrationDrepCertificate{
							CertType:       uint(common.CertificateTypeDeregistrationDrep),
							DrepCredential: target,
							Amount:         0,
						},
						assignment.build(target, pool),
					)
					err := era.validate(tx, 0, state, era.params)
					var targetErr conway.DelegateVoteToUnregisteredDRepError
					require.ErrorAs(t, err, &targetErr)
					require.Equal(t, target, targetErr.DRepCredential)
				})
			}
		}
	}
}

func TestValidateTxConwayDRepRegistrationAllowsLaterAssignment(t *testing.T) {
	prepareDrepLifecycleValidation(t)
	pool := common.PoolKeyHash(common.NewBlake2b224([]byte("drep-lifecycle-pool")))

	for _, era := range drepLifecycleValidationEras() {
		for _, targetType := range []uint{
			common.CredentialTypeAddrKeyHash,
			common.CredentialTypeScriptHash,
		} {
			target := drepLifecycleTarget(targetType)
			for _, assignment := range drepLifecycleAssignments() {
				t.Run(fmt.Sprintf("%s/%s/target-type-%d", era.name, assignment.name, targetType), func(t *testing.T) {
					state := newDrepLifecycleValidationState()
					if assignment.needsRegisteredStake {
						state.stakeRegistered[drepLifecycleStakeCredential().Credential] = true
					}
					if assignment.needsRegisteredPool {
						state.pools[pool] = true
					}
					tx := drepLifecycleCertificateTx(
						&common.RegistrationDrepCertificate{
							CertType:       uint(common.CertificateTypeRegistrationDrep),
							DrepCredential: target,
							Amount:         0,
						},
						assignment.build(target, pool),
					)
					require.NoError(t, era.validate(tx, 0, state, era.params))
				})
			}
		}
	}
}

func TestValidateTxConwayDRepRegistrationKeepsCredentialTypeIdentity(t *testing.T) {
	prepareDrepLifecycleValidation(t)
	pool := common.PoolKeyHash(common.NewBlake2b224([]byte("drep-lifecycle-pool")))

	for _, era := range drepLifecycleValidationEras() {
		for _, deregisteredType := range []uint{
			common.CredentialTypeAddrKeyHash,
			common.CredentialTypeScriptHash,
		} {
			targetType := uint(1) - deregisteredType
			deregistered := drepLifecycleTarget(deregisteredType)
			target := drepLifecycleTarget(targetType)
			for _, assignment := range drepLifecycleAssignments() {
				t.Run(fmt.Sprintf("%s/deregister-type-%d/%s", era.name, deregisteredType, assignment.name), func(t *testing.T) {
					state := newDrepLifecycleValidationState(deregistered, target)
					if assignment.needsRegisteredStake {
						state.stakeRegistered[drepLifecycleStakeCredential().Credential] = true
					}
					if assignment.needsRegisteredPool {
						state.pools[pool] = true
					}
					tx := drepLifecycleCertificateTx(
						&common.DeregistrationDrepCertificate{
							CertType:       uint(common.CertificateTypeDeregistrationDrep),
							DrepCredential: deregistered,
							Amount:         0,
						},
						assignment.build(target, pool),
					)
					require.NoError(t, era.validate(tx, 0, state, era.params))
				})
			}
		}
	}
}
