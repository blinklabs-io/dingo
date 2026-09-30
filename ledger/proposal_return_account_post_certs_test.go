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

package ledger

import (
	"bytes"
	"crypto/ed25519"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

func proposalReturnAccountAddress(
	t *testing.T,
	addrType byte,
	hash lcommon.Blake2b224,
) lcommon.Address {
	t.Helper()
	raw := append([]byte{addrType<<4 | byte(lcommon.AddressNetworkTestnet)}, hash[:]...)
	addr, err := lcommon.NewAddressFromBytes(raw)
	require.NoError(t, err)
	return addr
}

// proposalReturnAccountValidate runs a witnessed Conway transaction carrying
// certificates and one proposal through ValidateTxConway.
func proposalReturnAccountValidate(
	t *testing.T,
	lv *LedgerView,
	pparams lcommon.ProtocolParameters,
	payment ed25519.PrivateKey,
	certs []lcommon.Certificate,
	returnAddr lcommon.Address,
	action lcommon.GovAction,
	isValid bool,
) error {
	t.Helper()
	paymentHash := lcommon.Blake2b224Hash(payment.Public().(ed25519.PublicKey))
	input, address := committeeTestAddSpend(t, lv, paymentHash[:])
	output := &shelley.ShelleyTransactionOutput{
		OutputAddress: address,
		OutputAmount:  2_000_000,
	}
	tx := committeeVotingConway.build(input, output, nil, certs).(*conway.ConwayTransaction)
	tx.TxIsValid = isValid
	tx.Body.TxProposalProcedures = []conway.ConwayProposalProcedure{{
		PPRewardAccount: returnAddr,
		PPGovAction:     conway.ConwayGovAction{Action: action},
	}}
	committeeVotingConway.witness(
		tx,
		[]lcommon.VkeyWitness{committeeTestVKeyWitness(tx, payment)},
	)
	return eras.ValidateTxConway(tx, 0, lv, pparams)
}

func stakeRegistrationCertificate(cred lcommon.Credential) lcommon.Certificate {
	return &lcommon.StakeRegistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeRegistration),
		StakeCredential: cred,
	}
}

func stakeDeregistrationCertificate(cred lcommon.Credential) lcommon.Certificate {
	return &lcommon.StakeDeregistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeDeregistration),
		StakeCredential: cred,
	}
}

// GOV checks a proposal's return account and treasury withdrawal accounts
// against the certificate state after the transaction's own certificates.
func TestValidateTxProposalReturnAccountsSeeSameTransactionCertificates(
	t *testing.T,
) {
	t.Parallel()

	const deposit = uint64(2_000_000)
	pparams := committeeVotingConway.pparams(lcommon.ProtocolVersionPlomin)
	_, paymentKey := committeeTestVotingKey(0x51)

	t.Run("return account registered in the same transaction", func(t *testing.T) {
		t.Parallel()
		lv, _ := committeeTestView(t, pparams)
		cred := committeeTestCredential(0x52)
		err := proposalReturnAccountValidate(
			t, lv, pparams, paymentKey,
			[]lcommon.Certificate{stakeRegistrationCertificate(cred)},
			proposalReturnAccountAddress(t, 0xe, cred.Credential),
			&lcommon.InfoGovAction{}, true,
		)
		var missing conway.ProposalReturnAccountDoesNotExistError
		require.NotErrorAs(t, err, &missing)
	})

	t.Run("return account never registered", func(t *testing.T) {
		t.Parallel()
		lv, _ := committeeTestView(t, pparams)
		cred := committeeTestCredential(0x53)
		err := proposalReturnAccountValidate(
			t, lv, pparams, paymentKey, nil,
			proposalReturnAccountAddress(t, 0xe, cred.Credential),
			&lcommon.InfoGovAction{}, true,
		)
		var missing conway.ProposalReturnAccountDoesNotExistError
		require.ErrorAs(t, err, &missing)
	})

	t.Run("return account deregistered in the same transaction", func(t *testing.T) {
		t.Parallel()
		lv, db := committeeTestView(t, pparams)
		cred := committeeTestCredential(0x54)
		d := deposit
		seedStakeRegistration(t, db, cred, &d, 1, 0x60)
		require.True(t, lv.IsStakeCredentialRegistered(cred))
		err := proposalReturnAccountValidate(
			t, lv, pparams, paymentKey,
			[]lcommon.Certificate{stakeDeregistrationCertificate(cred)},
			proposalReturnAccountAddress(t, 0xe, cred.Credential),
			&lcommon.InfoGovAction{}, true,
		)
		var missing conway.ProposalReturnAccountDoesNotExistError
		require.ErrorAs(t, err, &missing)
		require.True(
			t, lv.IsStakeCredentialRegistered(cred),
			"rejected validation must not change stored account state",
		)
	})

	t.Run("treasury withdrawal account registered in the same transaction", func(t *testing.T) {
		t.Parallel()
		lv, db := committeeTestView(t, pparams)
		proposer := committeeTestCredential(0x55)
		d := deposit
		seedStakeRegistration(t, db, proposer, &d, 1, 0x62)
		target := committeeTestCredential(0x56)
		targetAddr := proposalReturnAccountAddress(t, 0xe, target.Credential)
		action := &lcommon.TreasuryWithdrawalGovAction{
			Withdrawals: map[*lcommon.Address]uint64{&targetAddr: 1_000_000},
		}
		err := proposalReturnAccountValidate(
			t, lv, pparams, paymentKey,
			[]lcommon.Certificate{stakeRegistrationCertificate(target)},
			proposalReturnAccountAddress(t, 0xe, proposer.Credential),
			action, true,
		)
		var missing conway.TreasuryWithdrawalReturnAccountsDoNotExistError
		require.NotErrorAs(t, err, &missing)
	})

	t.Run("treasury withdrawal account never registered", func(t *testing.T) {
		t.Parallel()
		lv, db := committeeTestView(t, pparams)
		proposer := committeeTestCredential(0x57)
		d := deposit
		seedStakeRegistration(t, db, proposer, &d, 1, 0x64)
		target := committeeTestCredential(0x58)
		targetAddr := proposalReturnAccountAddress(t, 0xe, target.Credential)
		action := &lcommon.TreasuryWithdrawalGovAction{
			Withdrawals: map[*lcommon.Address]uint64{&targetAddr: 1_000_000},
		}
		err := proposalReturnAccountValidate(
			t, lv, pparams, paymentKey, nil,
			proposalReturnAccountAddress(t, 0xe, proposer.Credential),
			action, true,
		)
		var missing conway.TreasuryWithdrawalReturnAccountsDoNotExistError
		require.ErrorAs(t, err, &missing)
	})
}

// The return address is an account address on the wire; a base address
// carrying the registered stake credential is not one, at every protocol
// version and regardless of phase-2 validity.
func TestValidateTxRejectsNonAccountProposalReturnAddress(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		major   uint
		isValid bool
	}{
		{"PV10 valid", lcommon.ProtocolVersionPlomin, true},
		{"PV9 bootstrap valid", lcommon.ProtocolVersionPlomin - 1, true},
		{"PV9 bootstrap phase-2 invalid", lcommon.ProtocolVersionPlomin - 1, false},
		{"PV10 phase-2 invalid", lcommon.ProtocolVersionPlomin, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pparams := committeeVotingConway.pparams(tc.major)
			lv, db := committeeTestView(t, pparams)
			_, paymentKey := committeeTestVotingKey(0x59)
			cred := committeeTestCredential(0x5a)
			d := uint64(2_000_000)
			seedStakeRegistration(t, db, cred, &d, 1, 0x66)
			base, err := lcommon.NewAddressFromParts(
				lcommon.AddressTypeKeyKey,
				lcommon.AddressNetworkTestnet,
				bytes.Repeat([]byte{0x7a}, lcommon.AddressHashSize),
				cred.Credential[:],
			)
			require.NoError(t, err)
			err = proposalReturnAccountValidate(
				t, lv, pparams, paymentKey, nil, base,
				&lcommon.InfoGovAction{}, tc.isValid,
			)
			require.ErrorContains(t, err, "invalid account address type")
		})
	}
}
