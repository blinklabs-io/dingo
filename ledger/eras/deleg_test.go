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
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var shelleyFamilyMajors = []struct {
	name  string
	major uint
}{
	{"Shelley", lcommon.ProtocolVersionShelley},
	{"Allegra", lcommon.ProtocolVersionAllegra},
	{"Mary", lcommon.ProtocolVersionMary},
	{"Alonzo", lcommon.ProtocolVersionAlonzo},
	{"Babbage", lcommon.ProtocolVersionBabbage},
}

func scriptCredential(seed byte) lcommon.Credential {
	cred := mirCredential(seed)
	cred.CredType = lcommon.CredentialTypeScriptHash
	return cred
}

func stakeReg(cred lcommon.Credential) lcommon.Certificate {
	return &lcommon.StakeRegistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeRegistration),
		StakeCredential: cred,
	}
}

func stakeDereg(cred lcommon.Credential) lcommon.Certificate {
	return &lcommon.StakeDeregistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeDeregistration),
		StakeCredential: cred,
	}
}

func stakeDelegation(cred lcommon.Credential) lcommon.Certificate {
	credCopy := cred
	return &lcommon.StakeDelegationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeDelegation),
		StakeCredential: &credCopy,
	}
}

func mirTransfer(
	pot uint,
	amount uint64,
) *lcommon.MoveInstantaneousRewardsCertificate {
	return &lcommon.MoveInstantaneousRewardsCertificate{
		Reward: lcommon.MoveInstantaneousRewardsCertificateReward{
			Source:   pot,
			OtherPot: amount,
		},
	}
}

func registeredLedgerState(
	cred lcommon.Credential,
	balance uint64,
) *mockLedgerState {
	ls := newMockLedgerState()
	ls.stakeRegistered = map[lcommon.Blake2b224]bool{cred.Credential: true}
	ls.rewardBalances = map[lcommon.Blake2b224]uint64{cred.Credential: balance}
	return ls
}

func rewardAddress(t *testing.T, cred lcommon.Credential) *lcommon.Address {
	t.Helper()
	addrType := uint8(lcommon.AddressTypeNoneKey)
	if cred.CredType == lcommon.CredentialTypeScriptHash {
		addrType = lcommon.AddressTypeNoneScript
	}
	addr, err := lcommon.NewAddressFromParts(
		addrType,
		lcommon.AddressNetworkTestnet,
		nil,
		cred.Credential.Bytes(),
	)
	require.NoError(t, err)
	return &addr
}

func validateCerts(
	ls lcommon.LedgerState,
	major uint,
	certs ...lcommon.Certificate,
) error {
	return validateShelleyDelegCerts(
		&mirTx{certs: certs}, 100, ls, majorPParams{major: major},
	)
}

// TestValidateShelleyDelegCerts_Deregistration covers #4372: a deregistration
// must name a credential registered at that point of the transaction, so no
// key deposit refund is counted for an account that does not exist.
func TestValidateShelleyDelegCerts_Deregistration(t *testing.T) {
	t.Parallel()
	registered := mirCredential(0x10)
	fresh := mirCredential(0x11)
	for _, era := range shelleyFamilyMajors {
		t.Run(era.name, func(t *testing.T) {
			t.Parallel()
			newLS := func() *mockLedgerState {
				return registeredLedgerState(registered, 0)
			}
			var notRegistered StakeKeyNotRegisteredError

			err := validateCerts(newLS(), era.major, stakeDereg(fresh))
			require.ErrorAs(t, err, &notRegistered)
			assert.Equal(t, fresh, notRegistered.Credential)

			require.NoError(t,
				validateCerts(newLS(), era.major, stakeDereg(registered)))
			require.NoError(t, validateCerts(
				newLS(), era.major, stakeReg(fresh), stakeDereg(fresh)))

			err = validateCerts(
				newLS(), era.major,
				stakeDereg(registered), stakeDereg(registered),
			)
			require.ErrorAs(t, err, &notRegistered)

			var unregistered shelley.DelegateUnregisteredStakeCredentialError
			err = validateCerts(
				newLS(), era.major,
				stakeReg(fresh), stakeDereg(fresh), stakeDelegation(fresh),
			)
			require.ErrorAs(t, err, &unregistered,
				"a credential deregistered earlier in the tx is unregistered")
			err = validateCerts(
				newLS(), era.major,
				stakeDereg(registered), stakeDelegation(registered),
			)
			require.ErrorAs(t, err, &unregistered,
				"deregistration hides the ledger registration from later certs")

			// Several fresh credentials would each inflate supply by a key
			// deposit; the first one already rejects the transaction.
			err = validateCerts(
				newLS(), era.major,
				stakeDereg(mirCredential(0x12)),
				stakeDereg(mirCredential(0x13)),
				stakeDereg(mirCredential(0x14)),
			)
			require.ErrorAs(t, err, &notRegistered)
		})
	}
}

// TestValidateShelleyDelegCerts_NonZeroBalance covers #4373: a deregistration
// must leave no rewards behind once the transaction's withdrawals are drained.
func TestValidateShelleyDelegCerts_NonZeroBalance(t *testing.T) {
	t.Parallel()
	for _, era := range shelleyFamilyMajors {
		t.Run(era.name, func(t *testing.T) {
			t.Parallel()
			for _, cred := range []lcommon.Credential{
				mirCredential(0x20), scriptCredential(0x21),
			} {
				ls := registeredLedgerState(cred, 50_000_000)
				var nonZero StakeKeyNonZeroAccountBalanceError
				err := validateCerts(ls, era.major, stakeDereg(cred))
				require.ErrorAs(t, err, &nonZero)
				assert.Equal(t, uint64(50_000_000), nonZero.Balance)

				drained := &mirTx{
					certs: []lcommon.Certificate{stakeDereg(cred)},
					withdrawals: map[*lcommon.Address]*big.Int{
						rewardAddress(t, cred): big.NewInt(50_000_000),
					},
				}
				require.NoError(t, validateShelleyDelegCerts(
					drained, 100, ls, majorPParams{major: era.major},
				), "withdrawals drain before certificates are processed")
			}
		})
	}
}

// TestValidateShelleyDelegCerts_Registration covers #4375: a registration must
// name a credential that is unregistered at that point of the transaction.
func TestValidateShelleyDelegCerts_Registration(t *testing.T) {
	t.Parallel()
	for _, era := range shelleyFamilyMajors {
		t.Run(era.name, func(t *testing.T) {
			t.Parallel()
			for _, pair := range []struct {
				registered, fresh lcommon.Credential
			}{
				{mirCredential(0x30), mirCredential(0x31)},
				{scriptCredential(0x32), scriptCredential(0x33)},
			} {
				newLS := func() *mockLedgerState {
					return registeredLedgerState(pair.registered, 0)
				}
				var already StakeKeyAlreadyRegisteredError

				err := validateCerts(
					newLS(),
					era.major,
					stakeReg(pair.registered),
				)
				require.ErrorAs(t, err, &already)
				assert.Equal(t, pair.registered, already.Credential)

				err = validateCerts(
					newLS(), era.major,
					stakeReg(pair.fresh), stakeReg(pair.fresh),
				)
				require.ErrorAs(t, err, &already)

				require.NoError(t, validateCerts(
					newLS(), era.major, stakeReg(pair.fresh)))
				require.NoError(t, validateCerts(
					newLS(), era.major,
					stakeDereg(pair.registered), stakeReg(pair.registered),
				))
				require.NoError(t, validateCerts(
					newLS(), era.major,
					stakeReg(pair.fresh), stakeDereg(pair.fresh),
					stakeReg(pair.fresh),
				))
			}
		})
	}
}

// TestValidateShelleyDelegCerts_MIRCutoff covers #4362 against the issue's
// worked example: a boundary at 1,000,000 and a 129,600-slot window.
func TestValidateShelleyDelegCerts_MIRCutoff(t *testing.T) {
	t.Parallel()
	const cutoff = 870_400
	cred := mirCredential(0x40)
	for _, era := range shelleyFamilyMajors {
		t.Run(era.name, func(t *testing.T) {
			t.Parallel()
			ls := newMockLedgerState()
			ls.mirState = &MIRDelegState{
				Reserves: 1_000, Treasury: 1_000, Cutoff: cutoff,
			}
			for _, tc := range []struct {
				slot uint64
				late bool
			}{
				{cutoff - 1, false},
				{cutoff, true},
				{999_999, true},
			} {
				tx := &mirTx{certs: []lcommon.Certificate{mirCert(cred, 10)}}
				err := validateShelleyDelegCerts(
					tx, tc.slot, ls, majorPParams{major: era.major},
				)
				if !tc.late {
					require.NoError(t, err, "slot %d", tc.slot)
					continue
				}
				var tooLate MIRCertificateTooLateError
				require.ErrorAs(t, err, &tooLate, "slot %d", tc.slot)
				assert.Equal(t, uint64(cutoff), tooLate.Cutoff)
			}
		})
	}
}

// TestValidateShelleyDelegCerts_MIRTransferVersion covers #4363.
func TestValidateShelleyDelegCerts_MIRTransferVersion(t *testing.T) {
	t.Parallel()
	for _, era := range shelleyFamilyMajors {
		t.Run(era.name, func(t *testing.T) {
			t.Parallel()
			for _, pot := range []uint{mirPotReserves, mirPotTreasury} {
				ls := newMockLedgerState()
				err := validateCerts(ls, era.major, mirTransfer(pot, 5_000_000))
				if lcommon.MirTransferAllowed(era.major) {
					require.NoError(t, err)
					continue
				}
				var notAllowed MIRTransferNotCurrentlyAllowedError
				require.ErrorAs(t, err, &notAllowed)
				assert.Equal(t, pot, notAllowed.Pot)
			}
		})
	}
	// Needing nothing but the protocol version, the predicate holds even
	// when the ledger state cannot report MIR state.
	var notAllowed MIRTransferNotCurrentlyAllowedError
	require.ErrorAs(t, validateCerts(
		noMIRProviderLedgerState{}, 4, mirTransfer(mirPotReserves, 1),
	), &notAllowed)
	require.NoError(t, validateCerts(
		noMIRProviderLedgerState{}, 5, mirTransfer(mirPotReserves, 1),
	))
}

// TestValidateShelleyDelegCerts_MIRCapacity covers #4371: every distribution
// and transfer is checked in order against what its pot has left.
func TestValidateShelleyDelegCerts_MIRCapacity(t *testing.T) {
	t.Parallel()
	const ada = 1_000_000
	a := mirCredential(0x50)
	b := mirCredential(0x51)
	pots := func(pending map[MIRCredentialKey]*big.Int) *mockLedgerState {
		ls := newMockLedgerState()
		ls.mirState = &MIRDelegState{
			Reserves: 100 * ada,
			Treasury: 100 * ada,
			Pending:  pending,
			Cutoff:   1_000_000,
		}
		return ls
	}
	var insufficient InsufficientForInstantaneousRewardsError
	var noTransfer InsufficientForTransferError

	for _, era := range shelleyFamilyMajors {
		t.Run(era.name, func(t *testing.T) {
			t.Parallel()
			var insufficient InsufficientForInstantaneousRewardsError
			for _, pot := range []uint{mirPotReserves, mirPotTreasury} {
				require.ErrorAs(t, validateCerts(
					pots(nil), era.major, mirCertFromPot(a, pot, 110*ada),
				), &insufficient)
				assert.Equal(t, pot, insufficient.Pot)
				require.NoError(t, validateCerts(
					pots(nil), era.major, mirCertFromPot(a, pot, 100*ada),
				))
				require.ErrorAs(t, validateCerts(
					pots(nil), era.major,
					mirCertFromPot(a, pot, 60*ada),
					mirCertFromPot(b, pot, 50*ada),
				), &insufficient, "distributions accumulate across certs")
				require.ErrorAs(t, validateCerts(
					pots(map[MIRCredentialKey]*big.Int{
						mirKeyFromPot(a, pot): big.NewInt(60 * ada),
					}),
					era.major,
					mirCertFromPot(b, pot, 50*ada),
				), &insufficient, "pending distributions count")
			}
		})
	}

	// The fold before protocol version 5 replaces a credential's pending
	// amount, so re-sending it does not double-count; from 5 it sums.
	repeat := map[MIRCredentialKey]*big.Int{
		mirKey(a): big.NewInt(60 * ada),
	}
	require.NoError(t, validateCerts(
		pots(repeat), lcommon.ProtocolVersionMary, mirCert(a, 90*ada)))
	require.ErrorAs(t, validateCerts(
		pots(repeat), lcommon.ProtocolVersionAlonzo, mirCert(a, 90*ada),
	), &insufficient)

	for _, major := range []uint{
		lcommon.ProtocolVersionAlonzo, lcommon.ProtocolVersionBabbage,
	} {
		// The worked example: an earlier transfer funds a later
		// distribution, never the other way round.
		require.NoError(t, validateCerts(
			pots(nil), major,
			mirTransfer(mirPotTreasury, 20*ada),
			mirCert(a, 110*ada),
		))
		require.ErrorAs(t, validateCerts(
			pots(nil), major,
			mirCert(a, 110*ada),
			mirTransfer(mirPotTreasury, 20*ada),
		), &insufficient)

		for _, pot := range []uint{mirPotReserves, mirPotTreasury} {
			require.NoError(t, validateCerts(
				pots(nil), major, mirTransfer(pot, 100*ada)))
			require.ErrorAs(t, validateCerts(
				pots(nil), major, mirTransfer(pot, 100*ada+1),
			), &noTransfer)
			assert.Equal(t, pot, noTransfer.Pot)
			require.ErrorAs(t, validateCerts(
				pots(nil), major,
				mirTransfer(pot, 101*ada),
				mirTransfer(1-pot, 50*ada),
			), &noTransfer, "later movement cannot rescue a transfer")
			require.ErrorAs(t, validateCerts(
				pots(nil), major,
				mirCertFromPot(a, pot, 30*ada),
				mirTransfer(pot, 71*ada),
			), &noTransfer, "a transfer cannot take distributed funds")
		}

		// Transfers committed earlier in the epoch count too.
		ls := pots(nil)
		ls.mirState.DeltaReserves = big.NewInt(-40 * ada)
		ls.mirState.DeltaTreasury = big.NewInt(40 * ada)
		require.ErrorAs(t, validateCerts(ls, major, mirCert(a, 61*ada)),
			&insufficient)
		require.NoError(t, validateCerts(
			ls, major, mirCertFromPot(a, mirPotTreasury, 140*ada)))
	}
}

// TestValidateShelleyDelegCerts_Phase2Invalid proves a phase-2-invalid
// transaction runs none of the certificate predicates, as DELEGS never runs
// for one.
func TestValidateShelleyDelegCerts_Phase2Invalid(t *testing.T) {
	t.Parallel()
	ls := newMockLedgerState()
	ls.mirState = &MIRDelegState{Cutoff: 0}
	tx := &mirTx{
		phase2Invalid: true,
		certs: []lcommon.Certificate{
			stakeDereg(mirCredential(0x60)),
			mirCert(mirCredential(0x61), 10),
		},
	}
	for _, major := range []uint{
		lcommon.ProtocolVersionAlonzo, lcommon.ProtocolVersionBabbage,
	} {
		require.NoError(t, validateShelleyDelegCerts(
			tx, 100, ls, majorPParams{major: major}))
		tx.phase2Invalid = false
		require.Error(t, validateShelleyDelegCerts(
			tx, 100, ls, majorPParams{major: major}))
		tx.phase2Invalid = true
	}
}

// TestValidateTxShelleyFamily_RejectsUnregisteredDeregistration proves every
// Shelley-through-Babbage ValidateTx entry point runs the certificate walk,
// with the #4372 inflation shape: outputs plus fee exceed the only input by
// exactly the key deposit a nonexistent account would refund.
func TestValidateTxShelleyFamily_RejectsUnregisteredDeregistration(
	t *testing.T,
) {
	t.Parallel()
	const keyDeposit = 2_000_000
	fresh := mirCredential(0x70)
	input := shelley.NewShelleyTransactionInput(
		"0000000000000000000000000000000000000000000000000000000000000001", 0,
	)
	addr := newTestKeyAddress(t)
	certs := []lcommon.CertificateWrapper{{
		Type:        uint(lcommon.CertificateTypeStakeDeregistration),
		Certificate: stakeDereg(fresh),
	}}
	newLS := func() *mockLedgerState {
		ls := newMockLedgerState()
		ls.addUtxo(input, &shelley.ShelleyTransactionOutput{
			OutputAddress: addr, OutputAmount: 10_000_000,
		})
		return ls
	}
	shelleyBody := shelley.ShelleyTransactionBody{
		TxInputs: shelley.NewShelleyTransactionInputSet(
			[]shelley.ShelleyTransactionInput{input},
		),
		TxOutputs: []shelley.ShelleyTransactionOutput{{
			OutputAddress: addr, OutputAmount: 11_800_000,
		}},
		TxFee:          200_000,
		Ttl:            1_000,
		TxCertificates: certs,
	}
	shelleyPP := shelley.ShelleyProtocolParameters{KeyDeposit: keyDeposit}

	cases := []struct {
		name     string
		validate func() error
	}{
		{"Shelley", func() error {
			pp := shelleyPP
			pp.ProtocolMajor = lcommon.ProtocolVersionShelley
			tx := &shelley.ShelleyTransaction{Body: shelleyBody}
			return ValidateTxShelley(tx, 100, newLS(), &pp)
		}},
		{"Allegra", func() error {
			pp := allegra.AllegraProtocolParameters(shelleyPP)
			pp.ProtocolMajor = lcommon.ProtocolVersionAllegra
			tx := &allegra.AllegraTransaction{
				Body: allegra.AllegraTransactionBody{
					TxCertificates: certs,
				},
			}
			return ValidateTxAllegra(tx, 100, newLS(), &pp)
		}},
		{"Mary", func() error {
			pp := mary.MaryProtocolParameters{
				KeyDeposit:    keyDeposit,
				ProtocolMajor: lcommon.ProtocolVersionMary,
			}
			tx := &mary.MaryTransaction{
				Body: mary.MaryTransactionBody{TxCertificates: certs},
			}
			return ValidateTxMary(tx, 100, newLS(), &pp)
		}},
		{"Alonzo", func() error {
			pp := alonzo.AlonzoProtocolParameters{
				KeyDeposit:    keyDeposit,
				ProtocolMajor: lcommon.ProtocolVersionAlonzo,
			}
			tx := &alonzo.AlonzoTransaction{
				TxIsValid: true,
				Body:      alonzo.AlonzoTransactionBody{TxCertificates: certs},
			}
			return ValidateTxAlonzo(tx, 100, newLS(), &pp)
		}},
		{"Babbage", func() error {
			pp := babbage.BabbageProtocolParameters{
				KeyDeposit:    keyDeposit,
				ProtocolMajor: lcommon.ProtocolVersionBabbage,
			}
			tx := &babbage.BabbageTransaction{
				TxIsValid: true,
				Body: babbage.BabbageTransactionBody{
					TxCertificates: certs,
				},
			}
			return ValidateTxBabbage(tx, 100, newLS(), &pp)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var notRegistered StakeKeyNotRegisteredError
			require.ErrorAs(t, tc.validate(), &notRegistered)
		})
	}

	// On the balanced Shelley transaction, the only rule that rejects is
	// this one: value conservation counts the refund, as it should for a
	// real account.
	pp := shelleyPP
	pp.ProtocolMajor = lcommon.ProtocolVersionShelley
	err := ValidateTxShelley(
		&shelley.ShelleyTransaction{Body: shelleyBody}, 100, newLS(), &pp,
	)
	var notConserved shelley.ValueNotConservedUtxoError
	assert.NotErrorAs(t, err, &notConserved)
}
