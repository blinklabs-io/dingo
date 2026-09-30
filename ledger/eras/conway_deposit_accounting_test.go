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
	"bytes"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	depositTestKeyDeposit  = 2_000_000
	depositTestDRepDeposit = 500_000_000
	depositTestInput       = 1_000_000_000
	depositTestFee         = 200_000
)

func depositTestPparams() *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{Major: 9},
		KeyDeposit:      depositTestKeyDeposit,
		DRepDeposit:     depositTestDRepDeposit,
		MaxTxSize:       16_384,
		MaxValueSize:    5_000,
	}
}

// depositTestTx builds a real Conway transaction whose single output is sized
// so that inputs = output + fee + suppliedDeposit: it balances on the deposit
// the certificate supplies, not on the deposit the protocol parameters
// require.
func depositTestTx(
	isValid bool,
	cert lcommon.Certificate,
	certType lcommon.CertificateType,
	suppliedDeposit uint64,
) (*conway.ConwayTransaction, shelley.ShelleyTransactionInput) {
	input := shelley.ShelleyTransactionInput{
		TxId:        lcommon.Blake2b256{0x4c},
		OutputIndex: 0,
	}
	addr := lcommon.Address{}
	return &conway.ConwayTransaction{
		TxIsValid: isValid,
		Body: conway.ConwayTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{input},
			),
			TxOutputs: []babbage.BabbageTransactionOutput{{
				OutputAddress: addr,
				OutputAmount: mary.MaryTransactionOutputValue{
					Amount: depositTestInput - depositTestFee -
						suppliedDeposit,
				},
			}},
			TxFee: depositTestFee,
			TxCertificates: []lcommon.CertificateWrapper{{
				Type:        uint(certType),
				Certificate: cert,
			}},
		},
	}, input
}

func depositTestCred() lcommon.Credential {
	return lcommon.Credential{
		CredType: lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(
			bytes.Repeat([]byte{0x77}, lcommon.AddressHashSize),
		),
	}
}

// Deposits are derived from protocol parameters, never from the amount a
// certificate supplies, and that holds for isValid=false transactions too
// (where the certificate-deposit rule itself is phase-2 gated). A transaction
// balanced on an understated deposit must be rejected by value conservation;
// the same transaction balanced on the required deposit must not be.
func TestValidateTxConwayDepositsComeFromProtocolParameters(t *testing.T) {
	t.Parallel()
	type scenario struct {
		name     string
		certType lcommon.CertificateType
		required uint64
		supplied uint64
		build    func(amount int64) lcommon.Certificate
	}
	scenarios := []scenario{
		{
			name:     "stake registration",
			certType: lcommon.CertificateTypeRegistration,
			required: depositTestKeyDeposit,
			supplied: 1_000_000,
			build: func(amount int64) lcommon.Certificate {
				return &lcommon.RegistrationCertificate{
					CertType:        uint(lcommon.CertificateTypeRegistration),
					StakeCredential: depositTestCred(),
					Amount:          amount,
				}
			},
		},
		{
			name:     "drep registration",
			certType: lcommon.CertificateTypeRegistrationDrep,
			required: depositTestDRepDeposit,
			supplied: 1_000_000,
			build: func(amount int64) lcommon.Certificate {
				return &lcommon.RegistrationDrepCertificate{
					CertType:       uint(lcommon.CertificateTypeRegistrationDrep),
					DrepCredential: depositTestCred(),
					Amount:         amount,
				}
			},
		},
	}
	for _, sc := range scenarios {
		for _, isValid := range []bool{false, true} {
			name := sc.name + " isValid=false"
			if isValid {
				name = sc.name + " isValid=true"
			}
			t.Run(name+" understated", func(t *testing.T) {
				t.Parallel()
				// #nosec G115 -- small test constants
				tx, in := depositTestTx(
					isValid, sc.build(int64(sc.supplied)),
					sc.certType, sc.supplied,
				)
				ls := newMockLedgerState()
				ls.skipPhase2Validation = true
				ls.addUtxo(in, newTestOutput(depositTestInput))
				err := ValidateTxConway(tx, 0, ls, depositTestPparams())
				require.Error(t, err)
				var notConserved shelley.ValueNotConservedUtxoError
				if isValid {
					var incorrect conway.CertificateDepositIncorrectError
					assert.ErrorAs(t, err, &incorrect)
				} else {
					require.ErrorAs(t, err, &notConserved)
					assert.Equal(
						t,
						0,
						big.NewInt(int64(sc.required-sc.supplied)).
							Cmp(new(big.Int).Sub(
								notConserved.Produced,
								notConserved.Consumed,
							)),
						"produced must exceed consumed by the shortfall",
					)
				}
			})
			t.Run(name+" correct", func(t *testing.T) {
				t.Parallel()
				// #nosec G115 -- small test constants
				tx, in := depositTestTx(
					isValid, sc.build(int64(sc.required)),
					sc.certType, sc.required,
				)
				ls := newMockLedgerState()
				ls.skipPhase2Validation = true
				ls.addUtxo(in, newTestOutput(depositTestInput))
				err := ValidateTxConway(tx, 0, ls, depositTestPparams())
				var notConserved shelley.ValueNotConservedUtxoError
				var incorrect conway.CertificateDepositIncorrectError
				assert.NotErrorAs(t, err, &notConserved)
				assert.NotErrorAs(t, err, &incorrect)
			})
		}
	}
}
