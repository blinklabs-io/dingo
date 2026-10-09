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
	"bytes"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"

	gledger "github.com/blinklabs-io/gouroboros/ledger"
)

const (
	testRewardHeaderKeyTestnet = 0xe0
	testRewardHeaderKeyMainnet = 0xe1
)

func testRewardAccount(header byte, fill byte) (cbor.ByteString, lcommon.Blake2b224) {
	hash := bytes.Repeat([]byte{fill}, lcommon.AddressHashSize)
	return cbor.NewByteString(append([]byte{header}, hash...)),
		lcommon.NewBlake2b224(hash)
}

func testDijkstraAccountState(
	registered map[lcommon.Blake2b224]uint64,
) *mockLedgerState {
	state := newMockLedgerState()
	state.stakeRegistered = map[lcommon.Blake2b224]bool{}
	state.rewardBalances = map[lcommon.Blake2b224]uint64{}
	for hash, balance := range registered {
		state.stakeRegistered[hash] = true
		state.rewardBalances[hash] = balance
	}
	return state
}

func exactInterval(v uint64) *gdijkstra.DijkstraAccountBalanceInterval {
	return &gdijkstra.DijkstraAccountBalanceInterval{Exact: &v}
}

func boundedInterval(
	lower, upper *uint64,
) *gdijkstra.DijkstraAccountBalanceInterval {
	return &gdijkstra.DijkstraAccountBalanceInterval{
		LowerBound: lower,
		UpperBound: upper,
	}
}

//go:fix inline
func u64(v uint64) *uint64 { return new(v) }

func testSubTransaction(
	deposits gdijkstra.DijkstraDirectDeposits,
	intervals gdijkstra.DijkstraAccountBalanceIntervals,
) gdijkstra.DijkstraSubTransaction {
	return gdijkstra.DijkstraSubTransaction{
		Body: gdijkstra.DijkstraSubTransactionBody{
			TxDirectDeposits:          deposits,
			TxAccountBalanceIntervals: intervals,
		},
	}
}

func testDijkstraBatch(
	top gdijkstra.DijkstraTransactionBody,
	children ...gdijkstra.DijkstraSubTransaction,
) *gdijkstra.DijkstraTransaction {
	if len(children) > 0 {
		top.TxSubTransactions = cbor.NewSetType(children, false)
	}
	return &gdijkstra.DijkstraTransaction{Body: top, TxIsValid: true}
}

// Not t.Parallel: validateDijkstraWithRule swaps the package-level Dijkstra
// rule list.
func TestValidateTxDijkstraDirectDepositFundingIsEnforced(t *testing.T) {
	input := shelley.NewShelleyTransactionInput(
		"d228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee22",
		0,
	)
	account, hash := testRewardAccount(testRewardHeaderKeyTestnet, 0x11)
	state := testDijkstraAccountState(map[lcommon.Blake2b224]uint64{hash: 0})
	state.addUtxo(input, newTestOutput(10_000_000))
	params := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{},
	}
	build := func(
		t *testing.T,
		outputAmount uint64,
		deposits gdijkstra.DijkstraDirectDeposits,
		inSubTx bool,
	) *gdijkstra.DijkstraTransaction {
		top := gdijkstra.DijkstraTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{input},
			),
			TxOutputs: []gdijkstra.DijkstraTransactionOutput{{
				Output: babbage.BabbageTransactionOutput{
					OutputAddress: newTestKeyAddress(t),
					OutputAmount: mary.MaryTransactionOutputValue{
						Amount: outputAmount,
					},
				},
			}},
			TxFee: 1_000_000,
		}
		if inSubTx {
			return testDijkstraBatch(top, testSubTransaction(deposits, nil))
		}
		top.TxDirectDeposits = deposits
		return testDijkstraBatch(top)
	}
	deposits := gdijkstra.DijkstraDirectDeposits{account: 500_000}
	for _, inSubTx := range []bool{false, true} {
		name := "top level"
		if inSubTx {
			name = "subtransaction"
		}
		t.Run(name+" funded", func(t *testing.T) {
			require.NoError(t, validateDijkstraWithRule(
				t, lcommon.UtxoValidationRuleValueNotConserved,
				build(t, 8_500_000, deposits, inSubTx), state, params,
			))
		})
		t.Run(name+" underfunded", func(t *testing.T) {
			err := validateDijkstraWithRule(
				t, lcommon.UtxoValidationRuleValueNotConserved,
				build(t, 9_000_000, deposits, inSubTx), state, params,
			)
			var notConserved shelley.ValueNotConservedUtxoError
			require.ErrorAs(t, err, &notConserved)
		})
	}
}

// Not t.Parallel: validateDijkstraWithRule swaps the package-level Dijkstra
// rule list.
func TestValidateTxDijkstraDirectDepositDestinations(t *testing.T) {
	keyAccount, keyHash := testRewardAccount(testRewardHeaderKeyTestnet, 0x21)
	wrongNetwork, wrongHash := testRewardAccount(testRewardHeaderKeyMainnet, 0x22)
	missing, _ := testRewardAccount(testRewardHeaderKeyTestnet, 0x23)
	state := testDijkstraAccountState(map[lcommon.Blake2b224]uint64{
		keyHash:   7,
		wrongHash: 7,
	})
	params := &gdijkstra.DijkstraProtocolParameters{}
	rule := lcommon.UtxoValidationRuleAccountBalanceIntervals
	for _, inSubTx := range []bool{false, true} {
		validate := func(
			t *testing.T,
			deposits gdijkstra.DijkstraDirectDeposits,
		) error {
			var tx *gdijkstra.DijkstraTransaction
			if inSubTx {
				tx = testDijkstraBatch(
					gdijkstra.DijkstraTransactionBody{},
					testSubTransaction(deposits, nil),
				)
			} else {
				tx = testDijkstraBatch(gdijkstra.DijkstraTransactionBody{
					TxDirectDeposits: deposits,
				})
			}
			return validateDijkstraWithRule(t, rule, tx, state, params)
		}
		name := "top level"
		if inSubTx {
			name = "subtransaction"
		}
		t.Run(name+" registered account", func(t *testing.T) {
			require.NoError(t, validate(t, gdijkstra.DijkstraDirectDeposits{
				keyAccount: 1,
			}))
		})
		t.Run(name+" nonexistent account", func(t *testing.T) {
			var e gdijkstra.DirectDepositAccountsMissingError
			require.ErrorAs(t, validate(t, gdijkstra.DijkstraDirectDeposits{
				missing: 1,
			}), &e)
		})
		t.Run(name+" wrong network", func(t *testing.T) {
			var e gdijkstra.WrongNetworkAccountAddressesError
			require.ErrorAs(t, validate(t, gdijkstra.DijkstraDirectDeposits{
				wrongNetwork: 1,
			}), &e)
		})
	}
}

// Not t.Parallel: validateDijkstraWithRule swaps the package-level Dijkstra
// rule list.
func TestValidateTxDijkstraBalanceIntervalBounds(t *testing.T) {
	account, hash := testRewardAccount(testRewardHeaderKeyTestnet, 0x31)
	missing, _ := testRewardAccount(testRewardHeaderKeyTestnet, 0x32)
	wrongNetwork, _ := testRewardAccount(testRewardHeaderKeyMainnet, 0x33)
	state := testDijkstraAccountState(map[lcommon.Blake2b224]uint64{hash: 100})
	params := &gdijkstra.DijkstraProtocolParameters{}
	rule := lcommon.UtxoValidationRuleAccountBalanceIntervals
	for _, tc := range []struct {
		name     string
		key      cbor.ByteString
		interval *gdijkstra.DijkstraAccountBalanceInterval
		wantErr  any
	}{
		{name: "exact match", key: account, interval: exactInterval(100)},
		{name: "exact below", key: account, interval: exactInterval(99), wantErr: &gdijkstra.BalancesOutsideAccountBalanceIntervalsError{}},
		{name: "lower bound is inclusive", key: account, interval: boundedInterval(new(uint64(100)), nil)},
		{name: "one below lower bound", key: account, interval: boundedInterval(new(uint64(101)), nil), wantErr: &gdijkstra.BalancesOutsideAccountBalanceIntervalsError{}},
		{name: "upper bound is exclusive", key: account, interval: boundedInterval(nil, new(uint64(100))), wantErr: &gdijkstra.BalancesOutsideAccountBalanceIntervalsError{}},
		{name: "one below upper bound", key: account, interval: boundedInterval(nil, new(uint64(101)))},
		{name: "both bounds around balance", key: account, interval: boundedInterval(new(uint64(100)), new(uint64(101)))},
		{name: "nonexistent account", key: missing, interval: exactInterval(0), wantErr: &gdijkstra.MissingAccountsInBalanceIntervalsError{}},
		{name: "wrong network", key: wrongNetwork, interval: exactInterval(0), wantErr: &gdijkstra.WrongNetworkAccountAddressesError{}},
	} {
		for _, level := range []string{"top level", "subtransaction", "starting"} {
			t.Run(level+" "+tc.name, func(t *testing.T) {
				intervals := gdijkstra.DijkstraAccountBalanceIntervals{
					tc.key: tc.interval,
				}
				var tx *gdijkstra.DijkstraTransaction
				switch level {
				case "top level":
					tx = testDijkstraBatch(gdijkstra.DijkstraTransactionBody{
						TxBalanceIntervals: intervals,
					})
				case "starting":
					tx = testDijkstraBatch(gdijkstra.DijkstraTransactionBody{
						TxStartingBalanceIntervals: intervals,
					})
				default:
					tx = testDijkstraBatch(
						gdijkstra.DijkstraTransactionBody{},
						testSubTransaction(nil, intervals),
					)
				}
				err := validateDijkstraWithRule(t, rule, tx, state, params)
				switch tc.wantErr.(type) {
				case nil:
					require.NoError(t, err)
				case *gdijkstra.BalancesOutsideAccountBalanceIntervalsError:
					var e gdijkstra.BalancesOutsideAccountBalanceIntervalsError
					require.ErrorAs(t, err, &e)
				case *gdijkstra.MissingAccountsInBalanceIntervalsError:
					var e gdijkstra.MissingAccountsInBalanceIntervalsError
					require.ErrorAs(t, err, &e)
				case *gdijkstra.WrongNetworkAccountAddressesError:
					var e gdijkstra.WrongNetworkAccountAddressesError
					require.ErrorAs(t, err, &e)
				}
			})
		}
	}
}

// Not t.Parallel: validateDijkstraWithRule swaps the package-level Dijkstra
// rule list.
func TestValidateTxDijkstraBalanceIntervalsFollowAccountStateThreading(t *testing.T) {
	account, hash := testRewardAccount(testRewardHeaderKeyTestnet, 0x41)
	registered, regHash := testRewardAccount(testRewardHeaderKeyTestnet, 0x42)
	state := testDijkstraAccountState(map[lcommon.Blake2b224]uint64{hash: 5})
	params := &gdijkstra.DijkstraProtocolParameters{}
	rule := lcommon.UtxoValidationRuleAccountBalanceIntervals
	at := func(key cbor.ByteString, v uint64) gdijkstra.DijkstraAccountBalanceIntervals {
		return gdijkstra.DijkstraAccountBalanceIntervals{key: exactInterval(v)}
	}

	t.Run("later child sees earlier child deposit", func(t *testing.T) {
		tx := testDijkstraBatch(
			gdijkstra.DijkstraTransactionBody{},
			testSubTransaction(gdijkstra.DijkstraDirectDeposits{account: 20}, nil),
			testSubTransaction(nil, at(account, 25)),
		)
		require.NoError(t, validateDijkstraWithRule(t, rule, tx, state, params))
	})
	t.Run("child interval does not see its own deposit", func(t *testing.T) {
		tx := testDijkstraBatch(
			gdijkstra.DijkstraTransactionBody{},
			testSubTransaction(
				gdijkstra.DijkstraDirectDeposits{account: 20},
				at(account, 25),
			),
		)
		var e gdijkstra.BalancesOutsideAccountBalanceIntervalsError
		require.ErrorAs(t, validateDijkstraWithRule(t, rule, tx, state, params), &e)
	})
	t.Run("top level sees child deposits, starting does not", func(t *testing.T) {
		child := testSubTransaction(gdijkstra.DijkstraDirectDeposits{account: 20}, nil)
		ok := testDijkstraBatch(gdijkstra.DijkstraTransactionBody{
			TxBalanceIntervals:         at(account, 25),
			TxStartingBalanceIntervals: at(account, 5),
		}, child)
		require.NoError(t, validateDijkstraWithRule(t, rule, ok, state, params))
		bad := testDijkstraBatch(gdijkstra.DijkstraTransactionBody{
			TxStartingBalanceIntervals: at(account, 25),
		}, child)
		var e gdijkstra.BalancesOutsideAccountBalanceIntervalsError
		require.ErrorAs(t, validateDijkstraWithRule(t, rule, bad, state, params), &e)
	})
	t.Run("later child sees earlier withdrawal", func(t *testing.T) {
		stakeAddr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeNoneKey,
			lcommon.AddressNetworkTestnet,
			nil,
			bytes.Repeat([]byte{0x41}, lcommon.AddressHashSize),
		)
		require.NoError(t, err)
		withdrawing := gdijkstra.DijkstraSubTransaction{
			Body: gdijkstra.DijkstraSubTransactionBody{
				TxWithdrawals: map[*lcommon.Address]uint64{&stakeAddr: 5},
			},
		}
		tx := testDijkstraBatch(
			gdijkstra.DijkstraTransactionBody{},
			withdrawing,
			testSubTransaction(nil, at(account, 0)),
		)
		require.NoError(t, validateDijkstraWithRule(t, rule, tx, state, params))
	})
	t.Run("later child sees earlier registration then deposit", func(t *testing.T) {
		reg := &lcommon.StakeRegistrationCertificate{
			StakeCredential: lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: regHash,
			},
		}
		first := gdijkstra.DijkstraSubTransaction{
			Body: gdijkstra.DijkstraSubTransactionBody{
				TxCertificates:   []lcommon.CertificateWrapper{{Type: uint(lcommon.CertificateTypeStakeRegistration), Certificate: reg}},
				TxDirectDeposits: gdijkstra.DijkstraDirectDeposits{registered: 9},
			},
		}
		tx := testDijkstraBatch(
			gdijkstra.DijkstraTransactionBody{},
			first,
			testSubTransaction(nil, at(registered, 9)),
		)
		require.NoError(t, validateDijkstraWithRule(t, rule, tx, state, params))
	})
}

// Not t.Parallel: validateDijkstraWithRule swaps the package-level Dijkstra
// rule list.
func TestValidateTxDijkstraDecodesBalanceIntervalsFromRawCbor(t *testing.T) {
	account, hash := testRewardAccount(testRewardHeaderKeyTestnet, 0x51)
	state := testDijkstraAccountState(map[lcommon.Blake2b224]uint64{hash: 100})
	body := func(t *testing.T, ordinary, starting any) []byte {
		t.Helper()
		bodyCbor, err := cbor.Encode(map[uint]any{
			0:  []any{},
			1:  []any{},
			2:  uint64(0),
			26: map[cbor.ByteString]any{account: ordinary},
			27: map[cbor.ByteString]any{account: starting},
		})
		require.NoError(t, err)
		txCbor, err := cbor.Encode([]any{
			cbor.RawMessage(bodyCbor), map[uint]any{}, nil,
		})
		require.NoError(t, err)
		return txCbor
	}
	for _, tc := range []struct {
		name     string
		ordinary any
		starting any
		wantErr  bool
	}{
		{name: "in range", ordinary: []any{uint64(100), uint64(101)}, starting: uint64(100)},
		{name: "starting outside", ordinary: uint64(100), starting: []any{nil, uint64(100)}, wantErr: true},
		{name: "ordinary outside", ordinary: []any{uint64(101), nil}, starting: uint64(100), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := gledger.NewTransactionFromCbor(
				gledger.TxTypeDijkstra, body(t, tc.ordinary, tc.starting),
			)
			require.NoError(t, err)
			dtx, ok := tx.(*gdijkstra.DijkstraTransaction)
			require.True(t, ok)
			require.Len(t, dtx.Body.TxStartingBalanceIntervals, 1)
			err = validateDijkstraWithRule(
				t, lcommon.UtxoValidationRuleAccountBalanceIntervals,
				tx, state, &gdijkstra.DijkstraProtocolParameters{},
			)
			if tc.wantErr {
				var e gdijkstra.BalancesOutsideAccountBalanceIntervalsError
				require.ErrorAs(t, err, &e)
				return
			}
			require.NoError(t, err)
		})
	}
}
