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

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
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

// proposalReturnAccountValidate runs a witnessed transaction of the given era
// carrying certificates and one top-level proposal through that era's
// ValidateTx.
func proposalReturnAccountValidate(
	t *testing.T,
	era committeeVotingEra,
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
	tx := era.build(input, output, nil, certs)
	switch tx := tx.(type) {
	case *conway.ConwayTransaction:
		tx.TxIsValid = isValid
		tx.Body.TxProposalProcedures = []conway.ConwayProposalProcedure{{
			PPRewardAccount: returnAddr,
			PPGovAction:     conway.ConwayGovAction{Action: action},
		}}
	case *gdijkstra.DijkstraTransaction:
		tx.TxIsValid = isValid
		tx.Body.TxProposalProcedures = []gdijkstra.DijkstraProposalProcedure{{
			PPRewardAccount: returnAddr,
			PPGovAction:     gdijkstra.DijkstraGovAction{Action: action},
		}}
	default:
		t.Fatalf("unsupported transaction type %T", tx)
	}
	era.witness(
		tx,
		[]lcommon.VkeyWitness{committeeTestVKeyWitness(tx, payment)},
	)
	return era.validate(tx, 0, lv, pparams)
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
	_, paymentKey := committeeTestVotingKey(0x51)
	for _, ec := range []struct {
		era   committeeVotingEra
		major uint
	}{
		{committeeVotingConway, lcommon.ProtocolVersionPlomin},
		{committeeVotingDijkstra, lcommon.ProtocolVersionDijkstra},
	} {
		era := ec.era
		pparams := era.pparams(ec.major)

		t.Run(era.name+"/return account registered in the same transaction", func(t *testing.T) {
			t.Parallel()
			lv, _ := committeeTestView(t, pparams)
			cred := committeeTestCredential(0x52)
			err := proposalReturnAccountValidate(
				t, era, lv, pparams, paymentKey,
				[]lcommon.Certificate{stakeRegistrationCertificate(cred)},
				proposalReturnAccountAddress(t, 0xe, cred.Credential),
				&lcommon.InfoGovAction{}, true,
			)
			require.NoError(t, err)
		})

		t.Run(era.name+"/return account never registered", func(t *testing.T) {
			t.Parallel()
			lv, _ := committeeTestView(t, pparams)
			cred := committeeTestCredential(0x53)
			err := proposalReturnAccountValidate(
				t, era, lv, pparams, paymentKey, nil,
				proposalReturnAccountAddress(t, 0xe, cred.Credential),
				&lcommon.InfoGovAction{}, true,
			)
			var missing conway.ProposalReturnAccountDoesNotExistError
			require.ErrorAs(t, err, &missing)
		})

		t.Run(era.name+"/return account deregistered in the same transaction", func(t *testing.T) {
			t.Parallel()
			lv, db := committeeTestView(t, pparams)
			cred := committeeTestCredential(0x54)
			d := deposit
			seedStakeRegistration(t, db, cred, &d, 1, 0x60)
			require.True(t, lv.IsStakeCredentialRegistered(cred))
			err := proposalReturnAccountValidate(
				t, era, lv, pparams, paymentKey,
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

		t.Run(era.name+"/treasury withdrawal account registered in the same transaction", func(t *testing.T) {
			t.Parallel()
			lv, db := committeeTestView(t, pparams)
			proposer := committeeTestCredential(0x55)
			d := deposit
			seedStakeRegistration(t, db, proposer, &d, 1, 0x62)
			// Treasury withdrawals are checked against the enacted
			// constitution's guardrails policy; none is set here.
			require.NoError(t, db.SetConstitution(&models.Constitution{
				AnchorURL:  "https://example.invalid/constitution",
				AnchorHash: bytes.Repeat([]byte{0xc1}, lcommon.Blake2b256Size),
				AddedSlot:  1,
			}, nil))
			target := committeeTestCredential(0x56)
			targetAddr := proposalReturnAccountAddress(t, 0xe, target.Credential)
			action := &lcommon.TreasuryWithdrawalGovAction{
				Withdrawals: map[*lcommon.Address]uint64{&targetAddr: 1_000_000},
			}
			err := proposalReturnAccountValidate(
				t, era, lv, pparams, paymentKey,
				[]lcommon.Certificate{stakeRegistrationCertificate(target)},
				proposalReturnAccountAddress(t, 0xe, proposer.Credential),
				action, true,
			)
			require.NoError(t, err)
		})

		t.Run(era.name+"/treasury withdrawal account never registered", func(t *testing.T) {
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
				t, era, lv, pparams, paymentKey, nil,
				proposalReturnAccountAddress(t, 0xe, proposer.Credential),
				action, true,
			)
			var missing conway.TreasuryWithdrawalReturnAccountsDoNotExistError
			require.ErrorAs(t, err, &missing)
		})
	}
}

// The return address is an account address on the wire; a base address
// carrying the registered stake credential is not one, in the Conway
// bootstrap phase and after it, in Dijkstra, and regardless of phase-2
// validity.
func TestValidateTxRejectsNonAccountProposalReturnAddress(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		era     committeeVotingEra
		major   uint
		isValid bool
	}{
		{"Conway PV10 valid", committeeVotingConway, lcommon.ProtocolVersionPlomin, true},
		{"Conway PV9 bootstrap valid", committeeVotingConway, lcommon.ProtocolVersionConway, true},
		{"Conway PV9 bootstrap phase-2 invalid", committeeVotingConway, lcommon.ProtocolVersionConway, false},
		{"Conway PV10 phase-2 invalid", committeeVotingConway, lcommon.ProtocolVersionPlomin, false},
		{"Dijkstra PV12 valid", committeeVotingDijkstra, lcommon.ProtocolVersionDijkstra, true},
		{"Dijkstra PV12 phase-2 invalid", committeeVotingDijkstra, lcommon.ProtocolVersionDijkstra, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pparams := tc.era.pparams(tc.major)
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
				t, tc.era, lv, pparams, paymentKey, nil, base,
				&lcommon.InfoGovAction{}, tc.isValid,
			)
			require.ErrorContains(t, err, "invalid account address type")
		})
	}
}
