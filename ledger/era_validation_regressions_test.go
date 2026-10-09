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

package ledger

import (
	"bytes"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// Regressions in this file drive Dingo's era validators with a real
// *LedgerView over a real database, the way block and mempool validation reach
// the upstream rules. A mock ledger state would satisfy optional capabilities
// the real view might not, which is how an inert rule goes unnoticed.

const retirementTestMaxEpoch = 18

// TestValidateTxConwayPoolRetirementEpochBound pins the retirement-epoch bound
// (current, current+eMax] through eras.ValidateTxConway with a real
// *LedgerView. The bound reads the current epoch through the optional
// common.EpochState capability and is skipped when the ledger state lacks it,
// so a view that stops providing the capability turns every out-of-range
// retirement into an accepted one.
func TestValidateTxConwayPoolRetirementEpochBound(t *testing.T) {
	t.Parallel()

	const (
		currentEpoch = uint64(5)
		epochLength  = uint(100)
		slot         = uint64(550)
	)
	ls, db := newRewardCalculationTestLedger(t)
	ls.epochCache = []models.Epoch{{
		EpochId:       currentEpoch,
		StartSlot:     500,
		LengthInSlots: epochLength,
	}}
	ls.publishSnapshotsLocked()
	lv := &LedgerView{ls: ls}

	pool := bytes.Repeat([]byte{0x3c}, lcommon.Blake2b224Size)
	vrfKeyHash := bytes.Repeat([]byte{0x3d}, lcommon.Blake2b256Size)
	rewardAccount := bytes.Repeat([]byte{0x3e}, lcommon.Blake2b224Size)
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash:   pool,
			VrfKeyHash:    vrfKeyHash,
			RewardAccount: rewardAccount,
		},
		&models.PoolRegistration{
			PoolKeyHash:   pool,
			VrfKeyHash:    vrfKeyHash,
			RewardAccount: rewardAccount,
			AddedSlot:     10,
		},
		nil,
	))
	pp := stakeRefundTestPparams()
	pp.MaxEpoch = retirementTestMaxEpoch

	for _, tc := range []struct {
		name    string
		epoch   uint64
		allowed bool
	}{
		{"the current epoch", currentEpoch, false},
		{"an earlier epoch", currentEpoch - 1, false},
		{"the next epoch", currentEpoch + 1, true},
		{"the last epoch within eMax", currentEpoch + retirementTestMaxEpoch, true},
		{"one epoch beyond eMax", currentEpoch + retirementTestMaxEpoch + 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cert := &lcommon.PoolRetirementCertificate{
				CertType:    uint(lcommon.CertificateTypePoolRetirement),
				PoolKeyHash: lcommon.PoolKeyHash(pool),
				Epoch:       tc.epoch,
			}
			tx := &conway.ConwayTransaction{
				TxIsValid: true,
				Body: conway.ConwayTransactionBody{
					TxFee: 200_000,
					TxCertificates: []lcommon.CertificateWrapper{{
						Type: uint(
							lcommon.CertificateTypePoolRetirement,
						),
						Certificate: cert,
					}},
				},
			}
			err := eras.ValidateTxConway(tx, slot, lv, pp)
			wrong, rejected := errors.AsType[shelley.StakePoolRetirementWrongEpochError](
				err,
			)
			if tc.allowed {
				require.False(
					t,
					rejected,
					"retirement epoch %d must satisfy the bound: %v",
					tc.epoch,
					err,
				)
				return
			}
			require.True(
				t,
				rejected,
				"retirement epoch %d must be rejected by the bound, got: %v",
				tc.epoch,
				err,
			)
			require.Equal(t, tc.epoch, wrong.Supplied)
			require.Equal(t, currentEpoch, wrong.CurrentEpoch)
			require.Equal(
				t,
				currentEpoch+retirementTestMaxEpoch,
				wrong.LimitEpoch,
			)
		})
	}
}

// newBlockValidationLedger returns a ledger over a real database whose genesis
// names a network, which block application requires before it reaches
// transaction validation.
func newBlockValidationLedger(
	t *testing.T,
) (*LedgerState, *database.Database) {
	t.Helper()
	ls, db := newRewardCalculationTestLedger(t)
	ls.config.CardanoNodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	return ls, db
}

// validateTxInBlock runs tx through ledgerProcessBlock with validation on,
// which is where block application calls an era's ValidateTx function with a
// real LedgerView. Other rules reject these minimal transactions, so callers
// assert on the specific error type they care about, not on success.
func validateTxInBlock(
	t *testing.T,
	ls *LedgerState,
	era lcommon.Era,
	desc eras.EraDesc,
	pp lcommon.ProtocolParameters,
	tx lcommon.Transaction,
) error {
	t.Helper()
	block := &validityOutcomeTestBlock{
		header: &conway.ConwayBlockHeader{},
		txs:    []lcommon.Transaction{tx},
		era:    era,
	}
	return ls.db.Transaction(t.Context(), true).
		Do(func(txn *database.Txn) error {
			_, err := ls.ledgerProcessBlock(t.Context(),
				txn,
				ocommon.NewPoint(200, block.Hash().Bytes()),
				block,
				true,
				false,
				false,
				nil,
				envelopeParent{origin: true},
				nil,
				desc,
				pp,
				nil,
				0,
				0,
				false,
			)
			return err
		})
}

// conwayBlockTestPparams extends the stake-refund fixture with the block size
// limits that block application checks before it validates any transaction.
func conwayBlockTestPparams() *conway.ConwayProtocolParameters {
	pp := stakeRefundTestPparams()
	pp.MaxBlockBodySize = 100_000
	pp.MaxBlockHeaderSize = 100_000
	return pp
}

func conwayCertTx(
	fee uint64,
	certs ...lcommon.Certificate,
) *conway.ConwayTransaction {
	wrapped := make([]lcommon.CertificateWrapper, 0, len(certs))
	for _, cert := range certs {
		wrapped = append(wrapped, lcommon.CertificateWrapper{
			Type:        cert.Type(),
			Certificate: cert,
		})
	}
	return &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxFee:          fee,
			TxCertificates: wrapped,
		},
	}
}

// TestBlockValueConservationRefundsCurrentDepositAfterSameTxReregistration
// drives a credential registered with a deposit that differs from the current
// KeyDeposit through explicit deregistration, legacy registration and legacy
// deregistration in one transaction. The final deregistration refunds the
// deposit the same transaction just paid, so total refunds are 5 ADA against
// 3 ADA of deposits.
func TestBlockValueConservationRefundsCurrentDepositAfterSameTxReregistration(
	t *testing.T,
) {
	t.Parallel()

	const (
		recordedDeposit = uint64(2_000_000)
		keyDeposit      = uint64(3_000_000)
	)
	pp := conwayBlockTestPparams()
	pp.KeyDeposit = uint(keyDeposit)
	for name, credType := range map[string]uint{
		"key hash":    lcommon.CredentialTypeAddrKeyHash,
		"script hash": lcommon.CredentialTypeScriptHash,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ls, db := newBlockValidationLedger(t)
			cred := stakeRefundTestCredential(0xd1)
			cred.CredType = credType
			deposit := recordedDeposit
			seedStakeRegistration(t, db, cred, &deposit, 100, 0xd1)
			sequence := func(fee uint64) *conway.ConwayTransaction {
				return conwayCertTx(
					fee,
					&lcommon.DeregistrationCertificate{
						CertType: uint(
							lcommon.CertificateTypeDeregistration,
						),
						StakeCredential: cred,
						Amount:          int64(recordedDeposit),
					},
					&lcommon.StakeRegistrationCertificate{
						CertType: uint(
							lcommon.CertificateTypeStakeRegistration,
						),
						StakeCredential: cred,
					},
					&lcommon.StakeDeregistrationCertificate{
						CertType: uint(
							lcommon.CertificateTypeStakeDeregistration,
						),
						StakeCredential: cred,
					},
				)
			}
			// Refunds of 5 ADA less the 3 ADA deposit leave a 2 ADA fee.
			err := validateTxInBlock(
				t, ls, conway.EraConway, eras.ConwayEraDesc, pp,
				sequence(2_000_000),
			)
			require.False(t, valueNotConserved(err),
				"balanced at 5 ADA of refunds must conserve value: %v", err)
			// The recorded-deposit accounting would balance at 1 ADA.
			err = validateTxInBlock(
				t, ls, conway.EraConway, eras.ConwayEraDesc, pp,
				sequence(1_000_000),
			)
			require.True(t, valueNotConserved(err),
				"balanced at 4 ADA of refunds must be rejected: %v", err)
		})
	}
}

func valueNotConserved(err error) bool {
	_, ok := errors.AsType[shelley.ValueNotConservedUtxoError](err)
	return ok
}

// depositRuleRejected reports whether err carries one of the errors a
// certificate deposit, refund or value-conservation rule returns.
func depositRuleRejected(err error) bool {
	if _, ok := errors.AsType[conway.CertificateDepositIncorrectError](err); ok {
		return true
	}
	if _, ok := errors.AsType[conway.CertificateRefundIncorrectError](err); ok {
		return true
	}
	if _, ok := errors.AsType[shelley.InvalidCertificateDepositError](err); ok {
		return true
	}
	return valueNotConserved(err)
}

// TestBlockZeroConwayDepositsAndRefundsAreAccepted drives every explicit-amount
// Conway certificate through block validation with KeyDeposit and DRepDeposit
// at zero. A deposit or refund of zero equals the applicable zero deposit, so
// it must be accepted, while a nonzero or negative amount stays rejected.
func TestBlockZeroConwayDepositsAndRefundsAreAccepted(t *testing.T) {
	t.Parallel()

	ls, db := newBlockValidationLedger(t)
	pp := conwayBlockTestPparams()
	pp.KeyDeposit = 0
	pp.DRepDeposit = 0
	registered := stakeRefundTestCredential(0xe1)
	fresh := stakeRefundTestCredential(0xe2)
	drep := stakeRefundTestCredential(0xe3)
	freshDrep := stakeRefundTestCredential(0xe4)
	zero := uint64(0)
	seedStakeRegistration(t, db, registered, &zero, 100, 0xe1)
	seedImportedDrep(t, db, drep, 0, 100, true)
	pool := lcommon.PoolKeyHash(
		bytes.Repeat([]byte{0xe5}, lcommon.Blake2b224Size),
	)
	alwaysAbstain := lcommon.Drep{Type: lcommon.DrepTypeAbstain}

	for _, tc := range []struct {
		name string
		cert func(amount int64) lcommon.Certificate
	}{
		{"stake registration", func(a int64) lcommon.Certificate {
			return &lcommon.RegistrationCertificate{
				CertType:        uint(lcommon.CertificateTypeRegistration),
				StakeCredential: fresh,
				Amount:          a,
			}
		}},
		{"stake deregistration", func(a int64) lcommon.Certificate {
			return &lcommon.DeregistrationCertificate{
				CertType:        uint(lcommon.CertificateTypeDeregistration),
				StakeCredential: registered,
				Amount:          a,
			}
		}},
		{"stake and delegate registration", func(a int64) lcommon.Certificate {
			return &lcommon.StakeRegistrationDelegationCertificate{
				CertType:        uint(lcommon.CertificateTypeStakeRegistrationDelegation),
				StakeCredential: fresh,
				PoolKeyHash:     pool,
				Amount:          a,
			}
		}},
		{"vote and delegate registration", func(a int64) lcommon.Certificate {
			return &lcommon.VoteRegistrationDelegationCertificate{
				CertType:        uint(lcommon.CertificateTypeVoteRegistrationDelegation),
				StakeCredential: fresh,
				Drep:            alwaysAbstain,
				Amount:          a,
			}
		}},
		{"stake vote and delegate registration", func(a int64) lcommon.Certificate {
			return &lcommon.StakeVoteRegistrationDelegationCertificate{
				CertType:        uint(lcommon.CertificateTypeStakeVoteRegistrationDelegation),
				StakeCredential: fresh,
				PoolKeyHash:     pool,
				Drep:            alwaysAbstain,
				Amount:          a,
			}
		}},
		{"drep registration", func(a int64) lcommon.Certificate {
			return &lcommon.RegistrationDrepCertificate{
				CertType:       uint(lcommon.CertificateTypeRegistrationDrep),
				DrepCredential: freshDrep,
				Amount:         a,
			}
		}},
		{"drep deregistration", func(a int64) lcommon.Certificate {
			return &lcommon.DeregistrationDrepCertificate{
				CertType:       uint(lcommon.CertificateTypeDeregistrationDrep),
				DrepCredential: drep,
				Amount:         a,
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			run := func(amount int64) error {
				return validateTxInBlock(
					t, ls, conway.EraConway, eras.ConwayEraDesc, pp,
					conwayCertTx(0, tc.cert(amount)),
				)
			}
			err := run(0)
			require.False(t, depositRuleRejected(err),
				"an explicit zero must match the zero parameter: %v", err)
			for _, amount := range []int64{1, -1} {
				err := run(amount)
				require.True(t, depositRuleRejected(err),
					"amount %d against a zero deposit must be rejected: %v",
					amount, err)
			}
		})
	}
}

// legacyDeregistrationEra describes how to build and parameterise a
// transaction carrying one legacy stake deregistration for a pre-Conway era.
type legacyDeregistrationEra struct {
	name string
	era  lcommon.Era
	desc eras.EraDesc
	pp   func(keyDeposit uint) lcommon.ProtocolParameters
	tx   func(fee uint64, cert lcommon.Certificate) lcommon.Transaction
}

func legacyDeregistrationEras() []legacyDeregistrationEra {
	certs := func(cert lcommon.Certificate) []lcommon.CertificateWrapper {
		return []lcommon.CertificateWrapper{
			{Type: cert.Type(), Certificate: cert},
		}
	}
	return []legacyDeregistrationEra{
		{
			name: "shelley", era: shelley.EraShelley, desc: eras.ShelleyEraDesc,
			pp: func(d uint) lcommon.ProtocolParameters {
				return &shelley.ShelleyProtocolParameters{
					KeyDeposit: d, MaxBlockBodySize: 100_000, MaxBlockHeaderSize: 100_000,
				}
			},
			tx: func(fee uint64, c lcommon.Certificate) lcommon.Transaction {
				return &shelley.ShelleyTransaction{
					Body: shelley.ShelleyTransactionBody{
						TxFee: fee, TxCertificates: certs(c),
					},
				}
			},
		},
		{
			name: "allegra", era: allegra.EraAllegra, desc: eras.AllegraEraDesc,
			pp: func(d uint) lcommon.ProtocolParameters {
				return &allegra.AllegraProtocolParameters{
					KeyDeposit: d, MaxBlockBodySize: 100_000, MaxBlockHeaderSize: 100_000,
				}
			},
			tx: func(fee uint64, c lcommon.Certificate) lcommon.Transaction {
				return &allegra.AllegraTransaction{
					Body: allegra.AllegraTransactionBody{
						TxFee: fee, TxCertificates: certs(c),
					},
				}
			},
		},
		{
			name: "mary", era: mary.EraMary, desc: eras.MaryEraDesc,
			pp: func(d uint) lcommon.ProtocolParameters {
				return &mary.MaryProtocolParameters{
					KeyDeposit: d, MaxBlockBodySize: 100_000, MaxBlockHeaderSize: 100_000,
				}
			},
			tx: func(fee uint64, c lcommon.Certificate) lcommon.Transaction {
				return &mary.MaryTransaction{Body: mary.MaryTransactionBody{
					TxFee: fee, TxCertificates: certs(c),
				}}
			},
		},
		{
			name: "alonzo", era: alonzo.EraAlonzo, desc: eras.AlonzoEraDesc,
			pp: func(d uint) lcommon.ProtocolParameters {
				return &alonzo.AlonzoProtocolParameters{
					KeyDeposit: d, MaxBlockBodySize: 100_000, MaxBlockHeaderSize: 100_000,
				}
			},
			tx: func(fee uint64, c lcommon.Certificate) lcommon.Transaction {
				return &alonzo.AlonzoTransaction{
					TxIsValid: true,
					Body: alonzo.AlonzoTransactionBody{
						TxFee: fee, TxCertificates: certs(c),
					},
				}
			},
		},
		{
			name: "babbage", era: babbage.EraBabbage, desc: eras.BabbageEraDesc,
			pp: func(d uint) lcommon.ProtocolParameters {
				return &babbage.BabbageProtocolParameters{
					KeyDeposit: d, MaxBlockBodySize: 100_000, MaxBlockHeaderSize: 100_000,
				}
			},
			tx: func(fee uint64, c lcommon.Certificate) lcommon.Transaction {
				return &babbage.BabbageTransaction{
					TxIsValid: true,
					Body: babbage.BabbageTransactionBody{
						TxFee: fee, TxCertificates: certs(c),
					},
				}
			},
		},
	}
}

// TestBlockLegacyDeregistrationRefundsRecordedDeposit pins the refund of a
// legacy stake deregistration in every pre-Conway era. The refund is the
// deposit recorded at registration, whichever direction KeyDeposit has moved
// since, and falls back to the current KeyDeposit only when the registration
// recorded no deposit.
func TestBlockLegacyDeregistrationRefundsRecordedDeposit(t *testing.T) {
	t.Parallel()

	const keyDeposit = uint64(3_000_000)
	for _, era := range legacyDeregistrationEras() {
		t.Run(era.name, func(t *testing.T) {
			t.Parallel()
			ls, db := newBlockValidationLedger(t)
			pp := era.pp(uint(keyDeposit))
			// refund is nil for a registration that recorded no deposit.
			for i, sc := range []struct {
				name   string
				refund *uint64
				wrong  uint64
			}{
				{"deposit recorded before KeyDeposit rose", new(uint64(2_000_000)), keyDeposit},
				{"deposit recorded before KeyDeposit fell", new(uint64(5_000_000)), keyDeposit},
				{"deposit unavailable", nil, 2_000_000},
			} {
				for _, credType := range []uint{
					lcommon.CredentialTypeAddrKeyHash,
					lcommon.CredentialTypeScriptHash,
				} {
					seed := byte(0x10*(i+1)) + byte(credType)
					cred := stakeRefundTestCredential(seed)
					cred.CredType = credType
					seedStakeRegistration(t, db, cred, sc.refund, 100, seed)
					want := keyDeposit
					if sc.refund != nil {
						want = *sc.refund
					}
					deregister := func(fee uint64) error {
						return validateTxInBlock(
							t, ls, era.era, era.desc, pp,
							era.tx(fee, &lcommon.StakeDeregistrationCertificate{
								CertType: uint(
									lcommon.CertificateTypeStakeDeregistration,
								),
								StakeCredential: cred,
							}),
						)
					}
					msg := sc.name + "/" + map[uint]string{
						lcommon.CredentialTypeAddrKeyHash: "key hash",
						lcommon.CredentialTypeScriptHash:  "script hash",
					}[credType]
					err := deregister(want)
					require.False(
						t,
						valueNotConserved(err),
						"%s: refund of %d must conserve value: %v",
						msg,
						want,
						err,
					)
					err = deregister(sc.wrong)
					require.True(
						t,
						valueNotConserved(err),
						"%s: refund of %d must be rejected: %v",
						msg,
						sc.wrong,
						err,
					)
				}
			}
		})
	}
}

// TestBlockMinimumAdaUsesWireSizeOfOutput pins the minimum-ADA requirement of
// Babbage and Conway outputs to the bytes the output occupied in the
// transaction. A block can carry an output in a non-canonical encoding, and the
// requirement is computed from that encoding, not from its canonical re-encode.
func TestBlockMinimumAdaUsesWireSizeOfOutput(t *testing.T) {
	t.Parallel()

	const (
		adaPerUtxoByte = uint64(4310)
		overhead       = uint64(160)
	)
	address := append([]byte{0x61}, bytes.Repeat([]byte{0x42}, 28)...)
	canonicalOutput := func(amount uint64) []byte {
		out, err := cbor.Encode(map[uint]any{0: address, 1: amount})
		require.NoError(t, err)
		return out
	}
	// The indefinite-length map is the shortest way to enlarge an output
	// without changing its content.
	indefiniteOutput := func(amount uint64) []byte {
		canonical := canonicalOutput(amount)
		require.Equal(t, byte(0xa2), canonical[0])
		return append(append([]byte{0xbf}, canonical[1:]...), 0xff)
	}
	canonicalMin := adaPerUtxoByte * (overhead + uint64(len(canonicalOutput(1_000_000))))
	wireMin := adaPerUtxoByte * (overhead + uint64(len(indefiniteOutput(1_000_000))))
	require.Greater(t, wireMin, canonicalMin)
	// Satisfies the canonical figure but not the wire figure.
	between := wireMin - 1
	require.GreaterOrEqual(t, between, canonicalMin)

	buildTx := func(output []byte) []byte {
		input := []any{bytes.Repeat([]byte{0x77}, 32), uint64(0)}
		txCbor, err := cbor.Encode([]any{
			map[uint]any{
				0: []any{input},
				1: []any{cbor.RawMessage(output)},
				2: uint64(200_000),
			},
			map[uint]any{},
			true,
			nil,
		})
		require.NoError(t, err)
		return txCbor
	}
	outputTooSmall := func(err error) bool {
		_, ok := errors.AsType[shelley.OutputTooSmallUtxoError](err)
		return ok
	}
	type eraCase struct {
		name   string
		decode func([]byte) (lcommon.Transaction, error)
		era    lcommon.Era
		desc   eras.EraDesc
		pp     lcommon.ProtocolParameters
	}
	for _, era := range []eraCase{
		{
			name: "babbage",
			decode: func(b []byte) (lcommon.Transaction, error) {
				return babbage.NewBabbageTransactionFromCbor(b)
			},
			era:  babbage.EraBabbage,
			desc: eras.BabbageEraDesc,
			pp: &babbage.BabbageProtocolParameters{
				AdaPerUtxoByte: adaPerUtxoByte, MaxBlockBodySize: 100_000,
				MaxBlockHeaderSize: 100_000, MaxValueSize: 5_000,
			},
		},
		{
			name: "conway",
			decode: func(b []byte) (lcommon.Transaction, error) {
				return conway.NewConwayTransactionFromCbor(b)
			},
			era:  conway.EraConway,
			desc: eras.ConwayEraDesc,
			pp: func() lcommon.ProtocolParameters {
				pp := conwayBlockTestPparams()
				pp.AdaPerUtxoByte = adaPerUtxoByte
				return pp
			}(),
		},
	} {
		t.Run(era.name, func(t *testing.T) {
			t.Parallel()
			ls, _ := newBlockValidationLedger(t)
			validate := func(output []byte, amount uint64) ([]byte, error) {
				tx, err := era.decode(buildTx(output))
				require.NoError(t, err)
				require.Len(t, tx.Outputs(), 1)
				require.Equal(t, amount, tx.Outputs()[0].Amount().Uint64())
				err = validateTxInBlock(t, ls, era.era, era.desc, era.pp, tx)
				return tx.Outputs()[0].Cbor(), err
			}

			stored, err := validate(indefiniteOutput(between), between)
			require.Equal(t, indefiniteOutput(between), stored,
				"an output from the network must keep its wire bytes")
			require.True(t, outputTooSmall(err),
				"%d lovelace is below the wire requirement of %d: %v",
				between, wireMin, err)

			_, err = validate(indefiniteOutput(wireMin), wireMin)
			require.False(t, outputTooSmall(err),
				"the wire requirement itself must be accepted: %v", err)
			_, err = validate(canonicalOutput(between), between)
			require.False(
				t,
				outputTooSmall(err),
				"the same amount in canonical encoding must be accepted: %v",
				err,
			)
		})
	}
}
