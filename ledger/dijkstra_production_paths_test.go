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
	"errors"
	"maps"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// pathScenario is one set of transactions that every production path must
// decide the same way: mempool admission, live block application, historical
// replay, and rollback followed by reapplication.
type pathScenario struct {
	accounts []pathAccount
	txs      []pathTx
	// reject asserts the error every path reports. Nil means the
	// transactions are valid.
	reject func(*testing.T, error)
	// rewards and scriptRewards are the balances of the key and script
	// accounts after the block applies.
	rewards       map[byte]uint64
	scriptRewards map[byte]uint64
	// pending are transactions outside the block, each valid only against
	// the state the block leaves behind: admission rejects it with
	// pendingReject before the block applies and accepts it after.
	pending       []pathTx
	pendingReject func(*testing.T, error)
}

// rejectWith asserts that err carries a T.
func rejectWith[T error](t *testing.T, err error) {
	t.Helper()
	_, ok := errors.AsType[T](err)
	require.True(t, ok, "want %T, got: %v", *new(T), err)
}

func pathDeposits(
	deposits map[cbor.ByteString]uint64,
) map[uint]any {
	return map[uint]any{25: deposits}
}

func runPathScenario(t *testing.T, sc pathScenario) {
	t.Helper()
	initial := make(map[byte]uint64, len(sc.accounts))
	initialScript := make(map[byte]uint64)
	seeded := make(map[pathAccountID]bool, len(sc.accounts))
	for _, account := range sc.accounts {
		seeded[account.id()] = true
		if account.script {
			initialScript[account.key] = account.reward
		} else {
			initial[account.key] = account.reward
		}
	}
	build := func(t *testing.T) *pathFixture {
		t.Helper()
		return newPathFixture(t, sc.accounts, sc.txs, sc.pending...)
	}
	requireRewards := func(
		t *testing.T,
		f *pathFixture,
		keys, scripts map[byte]uint64,
	) {
		t.Helper()
		for key, reward := range keys {
			require.Equal(t, reward, f.reward(key), "key account %x", key)
		}
		for key, reward := range scripts {
			require.Equal(
				t,
				reward,
				f.rewardOf(1, key),
				"script account %x",
				key,
			)
		}
	}
	requireInputs := func(t *testing.T, f *pathFixture, unspent bool) {
		t.Helper()
		for index := range sc.txs {
			require.Equal(
				t,
				unspent,
				f.inputUnspent(index),
				"tx %d input",
				index,
			)
		}
	}
	// requireNoRegistrationLeaks checks that the scenario's transaction set
	// created no account unless the scenario seeded it.
	requireNoRegistrationLeaks := func(t *testing.T, f *pathFixture) {
		t.Helper()
		for index := range sc.txs {
			for _, id := range f.registeredAccounts(index) {
				require.Equal(
					t,
					seeded[id],
					f.accountPresent(id),
					"tx %d registered account %x",
					index,
					id.hash,
				)
			}
		}
	}
	// requirePending checks the pending transactions against the ledger as
	// it stands: through validation and the mempool.
	requirePending := func(t *testing.T, f *pathFixture, accepted bool) {
		t.Helper()
		for index := len(sc.txs); index < len(f.txs); index++ {
			for _, err := range []error{f.admit(index), f.mempoolAdd(index)} {
				if accepted {
					require.NoError(t, err, "pending tx %d", index)
				} else {
					sc.pendingReject(t, err)
				}
			}
		}
	}
	// outcome asserts the error and the state the path left behind. A
	// rejected block leaves every account and input as it was.
	outcome := func(t *testing.T, f *pathFixture, err error) {
		t.Helper()
		if sc.reject == nil {
			require.NoError(t, err)
			requireRewards(t, f, sc.rewards, sc.scriptRewards)
			requireInputs(t, f, false)
			return
		}
		sc.reject(t, err)
		requireRewards(t, f, initial, initialScript)
		requireNoRegistrationLeaks(t, f)
		requireInputs(t, f, true)
	}
	// Admission sees only the ledger state before the block, so it decides a
	// lone transaction.
	if len(sc.txs) == 1 {
		t.Run("validation", func(t *testing.T) {
			t.Parallel()
			err := build(t).admit(0)
			if sc.reject == nil {
				require.NoError(t, err)
				return
			}
			sc.reject(t, err)
		})
		t.Run("mempool", func(t *testing.T) {
			t.Parallel()
			f := build(t)
			err := f.mempoolAdd(0)
			if sc.reject == nil {
				require.NoError(t, err)
				return
			}
			sc.reject(t, err)
			requireRewards(t, f, initial, initialScript)
			requireNoRegistrationLeaks(t, f)
		})
	}
	t.Run("live block", func(t *testing.T) {
		t.Parallel()
		f := build(t)
		if len(sc.pending) > 0 {
			requirePending(t, f, false)
		}
		outcome(t, f, f.applyBlock())
		if len(sc.pending) > 0 && sc.reject == nil {
			requirePending(t, f, true)
		}
	})
	t.Run("replay", func(t *testing.T) {
		t.Parallel()
		f := build(t)
		outcome(t, f, f.replayBlock())
	})
	if sc.reject != nil {
		return
	}
	t.Run("rollback and reapply", func(t *testing.T) {
		t.Parallel()
		f := build(t)
		require.NoError(t, f.replayBlock())
		requireRewards(t, f, sc.rewards, sc.scriptRewards)
		requirePending(t, f, true)
		require.NoError(t, f.ls.rollback(
			ocommon.Point{Slot: pathOriginSlot, Hash: f.originHash},
		))
		requireRewards(t, f, initial, initialScript)
		requireNoRegistrationLeaks(t, f)
		requireInputs(t, f, true)
		requirePending(t, f, false)
		require.NoError(t, f.applyBlock())
		requireRewards(t, f, sc.rewards, sc.scriptRewards)
		requireInputs(t, f, false)
		requirePending(t, f, true)
	})
}

func TestDijkstraDirectDepositsThroughProductionPaths(t *testing.T) {
	t.Parallel()
	account := []pathAccount{{key: 0x42, reward: 5}}
	deposit := func(account cbor.ByteString, amount uint64) map[uint]any {
		return pathDeposits(map[cbor.ByteString]uint64{account: amount})
	}
	scenarios := []struct {
		name string
		pathScenario
	}{
		{"funded top-level deposit credits the account", pathScenario{
			accounts: account,
			txs: []pathTx{{top: pathLevel{
				fields: deposit(pathKeyAccount(0x42), 20), funds: 20,
			}}},
			rewards: map[byte]uint64{0x42: 25},
		}},
		{"funded child deposit credits the account", pathScenario{
			accounts: account,
			txs: []pathTx{{children: []pathLevel{{
				fields: deposit(pathKeyAccount(0x42), 20), funds: 20,
			}}}},
			rewards: map[byte]uint64{0x42: 25},
		}},
		{"deposits at both levels accumulate", pathScenario{
			accounts: account,
			txs: []pathTx{{
				top: pathLevel{
					fields: deposit(pathKeyAccount(0x42), 7), funds: 7,
				},
				children: []pathLevel{{
					fields: deposit(pathKeyAccount(0x42), 20), funds: 20,
				}},
			}},
			rewards: map[byte]uint64{0x42: 32},
		}},
		{"underfunded top-level deposit", pathScenario{
			accounts: account,
			txs: []pathTx{{top: pathLevel{
				fields: deposit(pathKeyAccount(0x42), 20),
			}}},
			reject: rejectWith[shelley.ValueNotConservedUtxoError],
		}},
		{"underfunded child deposit", pathScenario{
			accounts: account,
			txs: []pathTx{{children: []pathLevel{{
				fields: deposit(pathKeyAccount(0x42), 20),
			}}}},
			reject: rejectWith[shelley.ValueNotConservedUtxoError],
		}},
		{"wrong-network top-level destination", pathScenario{
			accounts: account,
			txs: []pathTx{{top: pathLevel{
				fields: deposit(pathAccountFor(0xe1, 0x42), 20), funds: 20,
			}}},
			reject: rejectWith[dijkstra.WrongNetworkAccountAddressesError],
		}},
		{"wrong-network child destination", pathScenario{
			accounts: account,
			txs: []pathTx{{children: []pathLevel{{
				fields: deposit(pathAccountFor(0xe1, 0x42), 20), funds: 20,
			}}}},
			reject: rejectWith[dijkstra.WrongNetworkAccountAddressesError],
		}},
		{"nonexistent top-level destination", pathScenario{
			accounts: account,
			txs: []pathTx{{top: pathLevel{
				fields: deposit(pathKeyAccount(0x43), 20), funds: 20,
			}}},
			reject: rejectWith[dijkstra.DirectDepositAccountsMissingError],
		}},
		{"nonexistent child destination", pathScenario{
			accounts: account,
			txs: []pathTx{{children: []pathLevel{{
				fields: deposit(pathKeyAccount(0x43), 20), funds: 20,
			}}}},
			reject: rejectWith[dijkstra.DirectDepositAccountsMissingError],
		}},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			runPathScenario(t, sc.pathScenario)
		})
	}
}

// pathRegistration is a registration certificate with a zero deposit, which
// matches the zero key deposit of the fixture's protocol parameters.
func pathRegistration(key byte) any {
	return []any{uint(7), []any{uint(0), pathStakeKey(key)}, uint64(0)}
}

func pathIntervals(
	intervals map[cbor.ByteString]any,
	key uint,
) map[uint]any {
	return map[uint]any{key: intervals}
}

func pathExact(
	account cbor.ByteString,
	amount uint64,
	key uint,
) map[uint]any {
	return pathIntervals(map[cbor.ByteString]any{account: amount}, key)
}

func pathBounds(
	account cbor.ByteString,
	lower, upper any,
	key uint,
) map[uint]any {
	return pathIntervals(
		map[cbor.ByteString]any{account: []any{lower, upper}},
		key,
	)
}

func mergeFields(parts ...map[uint]any) map[uint]any {
	merged := make(map[uint]any)
	for _, part := range parts {
		maps.Copy(merged, part)
	}
	return merged
}

func TestDijkstraDirectDepositOrderingThroughProductionPaths(t *testing.T) {
	t.Parallel()
	registerAndDeposit := mergeFields(
		map[uint]any{4: []any{pathRegistration(pathSignerKey)}},
		pathDeposits(
			map[cbor.ByteString]uint64{pathKeyAccount(pathSignerKey): 20},
		),
	)
	scenarios := []struct {
		name string
		pathScenario
	}{
		{"registration and deposit in the same top-level body", pathScenario{
			txs: []pathTx{
				{top: pathLevel{fields: registerAndDeposit, funds: 20}},
			},
			rewards: map[byte]uint64{pathSignerKey: 20},
		}},
		{"registration and deposit in the same child body", pathScenario{
			txs: []pathTx{{children: []pathLevel{{
				fields: registerAndDeposit, funds: 20,
			}}}},
			rewards: map[byte]uint64{pathSignerKey: 20},
		}},
		{"registration in an earlier child", pathScenario{
			txs: []pathTx{{children: []pathLevel{
				{
					fields: map[uint]any{
						4: []any{pathRegistration(pathSignerKey)},
					},
				},
				{
					fields: pathDeposits(map[cbor.ByteString]uint64{
						pathKeyAccount(pathSignerKey): 20,
					}),
					funds: 20,
				},
			}}},
			rewards: map[byte]uint64{pathSignerKey: 20},
		}},
		{"registration in a later child is too late", pathScenario{
			txs: []pathTx{{children: []pathLevel{
				{
					fields: pathDeposits(map[cbor.ByteString]uint64{
						pathKeyAccount(pathSignerKey): 20,
					}),
					funds: 20,
				},
				{
					fields: map[uint]any{
						4: []any{pathRegistration(pathSignerKey)},
					},
				},
			}}},
			reject: rejectWith[dijkstra.DirectDepositAccountsMissingError],
		}},
		{"rejected block discards an earlier transaction's registration", pathScenario{
			accounts: []pathAccount{{key: 0x42, reward: 5}},
			txs: []pathTx{
				{top: pathLevel{fields: map[uint]any{
					4: []any{pathRegistration(pathSignerKey)},
				}}},
				{top: pathLevel{
					fields: pathDeposits(map[cbor.ByteString]uint64{
						pathKeyAccount(0x77): 20,
					}),
					funds: 20,
				}},
			},
			reject: rejectWith[dijkstra.DirectDepositAccountsMissingError],
		}},
		{"top-level registration is too late for a child deposit", pathScenario{
			txs: []pathTx{{
				top: pathLevel{fields: map[uint]any{
					4: []any{pathRegistration(pathSignerKey)},
				}},
				children: []pathLevel{{
					fields: pathDeposits(map[cbor.ByteString]uint64{
						pathKeyAccount(pathSignerKey): 20,
					}),
					funds: 20,
				}},
			}},
			reject: rejectWith[dijkstra.DirectDepositAccountsMissingError],
		}},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			runPathScenario(t, sc.pathScenario)
		})
	}
}

// A later transaction observes a direct deposit once the block that carries it
// has applied, and no longer observes it once that block is rolled back.
func TestDijkstraLaterTransactionObservesDirectDeposit(t *testing.T) {
	t.Parallel()
	account := []pathAccount{{key: 0x42, reward: 5}}
	deposit := pathTx{top: pathLevel{
		fields: pathDeposits(map[cbor.ByteString]uint64{
			pathKeyAccount(0x42): 20,
		}),
		funds: 20,
	}}
	observer := func(key uint) pathTx {
		return pathTx{top: pathLevel{
			fields: pathExact(pathKeyAccount(0x42), 25, key),
		}}
	}
	outside := rejectWith[dijkstra.BalancesOutsideAccountBalanceIntervalsError]
	for _, test := range []struct {
		name string
		key  uint
	}{
		{"ordinary interval", 26},
		{"starting interval", 27},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			t.Run("in a later block", func(t *testing.T) {
				t.Parallel()
				runPathScenario(t, pathScenario{
					accounts:      account,
					txs:           []pathTx{deposit},
					pending:       []pathTx{observer(test.key)},
					pendingReject: outside,
					rewards:       map[byte]uint64{0x42: 25},
				})
			})
			t.Run("later in the same block", func(t *testing.T) {
				t.Parallel()
				runPathScenario(t, pathScenario{
					accounts: account,
					txs:      []pathTx{deposit, observer(test.key)},
					rewards:  map[byte]uint64{0x42: 25},
				})
			})
			t.Run("earlier in the same block", func(t *testing.T) {
				t.Parallel()
				runPathScenario(t, pathScenario{
					accounts: account,
					txs:      []pathTx{observer(test.key), deposit},
					reject:   outside,
				})
			})
		})
	}
}

func TestDijkstraBalanceIntervalsThroughProductionPaths(t *testing.T) {
	t.Parallel()
	account := []pathAccount{{key: 0x42, reward: 5}}
	acct := pathKeyAccount(0x42)
	outside := rejectWith[dijkstra.BalancesOutsideAccountBalanceIntervalsError]
	// The reference reads the lower bound as inclusive and the upper bound as
	// exclusive.
	type interval struct {
		name  string
		build func(key uint) map[uint]any
		valid bool
	}
	intervals := []interval{
		{
			"exact at the balance",
			func(k uint) map[uint]any { return pathExact(acct, 5, k) },
			true,
		},
		{
			"exact above the balance",
			func(k uint) map[uint]any { return pathExact(acct, 6, k) },
			false,
		},
		{
			"exact below the balance",
			func(k uint) map[uint]any { return pathExact(acct, 4, k) },
			false,
		},
		{
			"lower bound at the balance",
			func(k uint) map[uint]any { return pathBounds(acct, uint64(5), nil, k) },
			true,
		},
		{
			"lower bound above the balance",
			func(k uint) map[uint]any { return pathBounds(acct, uint64(6), nil, k) },
			false,
		},
		{
			"upper bound at the balance is exclusive",
			func(k uint) map[uint]any { return pathBounds(acct, nil, uint64(5), k) },
			false,
		},
		{
			"upper bound above the balance",
			func(k uint) map[uint]any { return pathBounds(acct, nil, uint64(6), k) },
			true,
		},
		{
			"empty range from the balance",
			func(k uint) map[uint]any { return pathBounds(acct, uint64(5), uint64(5), k) },
			false,
		},
		{
			"range starting at the balance",
			func(k uint) map[uint]any { return pathBounds(acct, uint64(5), uint64(6), k) },
			true,
		},
	}
	for _, field := range []struct {
		name  string
		key   uint
		child bool
	}{
		{"top-level account_balance_intervals", 26, false},
		{"child account_balance_intervals", 26, true},
		{"top-level starting_account_balance_intervals", 27, false},
	} {
		for _, iv := range intervals {
			t.Run(field.name+"/"+iv.name, func(t *testing.T) {
				t.Parallel()
				level := pathLevel{fields: iv.build(field.key)}
				tx := pathTx{top: level}
				if field.child {
					tx = pathTx{children: []pathLevel{level}}
				}
				sc := pathScenario{
					accounts: account,
					txs:      []pathTx{tx},
					rewards:  map[byte]uint64{0x42: 5},
				}
				if !iv.valid {
					sc.reject = outside
				}
				runPathScenario(t, sc)
			})
		}
	}
	for _, field := range []struct {
		name string
		key  uint
	}{
		{"account_balance_intervals", 26},
		{"starting_account_balance_intervals", 27},
	} {
		t.Run(field.name+"/wrong-network account", func(t *testing.T) {
			t.Parallel()
			runPathScenario(t, pathScenario{
				accounts: account,
				txs: []pathTx{{top: pathLevel{
					fields: pathExact(pathAccountFor(0xe1, 0x42), 5, field.key),
				}}},
				reject: rejectWith[dijkstra.WrongNetworkAccountAddressesError],
			})
		})
		t.Run(field.name+"/nonexistent account", func(t *testing.T) {
			t.Parallel()
			runPathScenario(t, pathScenario{
				accounts: account,
				txs: []pathTx{{top: pathLevel{
					fields: pathExact(pathKeyAccount(0x43), 0, field.key),
				}}},
				reject: rejectWith[dijkstra.MissingAccountsInBalanceIntervalsError],
			})
		})
	}
}

// Ordinary intervals see the account state entering their own level, so every
// earlier child's withdrawals, certificates and deposits are visible while the
// level's own effects are not. Starting intervals see the state the whole
// batch began with.
func TestDijkstraBalanceIntervalsThreadAccountState(t *testing.T) {
	t.Parallel()
	signer := pathKeyAccount(pathSignerKey)
	acct := pathKeyAccount(0x42)
	account := []pathAccount{{key: 0x42, reward: 5}}
	signerAccount := []pathAccount{{key: pathSignerKey, reward: 5}}
	deposit := func(amount uint64) map[uint]any {
		return pathDeposits(map[cbor.ByteString]uint64{acct: amount})
	}
	outside := rejectWith[dijkstra.BalancesOutsideAccountBalanceIntervalsError]
	missing := rejectWith[dijkstra.MissingAccountsInBalanceIntervalsError]
	scenarios := []struct {
		name string
		pathScenario
	}{
		{"child sees an earlier child's deposit", pathScenario{
			accounts: account,
			txs: []pathTx{{children: []pathLevel{
				{fields: deposit(20), funds: 20},
				{fields: pathExact(acct, 25, 26)},
			}}},
			rewards: map[byte]uint64{0x42: 25},
		}},
		{"child does not see its own deposit", pathScenario{
			accounts: account,
			txs: []pathTx{{children: []pathLevel{{
				fields: mergeFields(deposit(20), pathExact(acct, 25, 26)),
				funds:  20,
			}}}},
			reject: outside,
		}},
		{"child interval holds before its own deposit", pathScenario{
			accounts: account,
			txs: []pathTx{{children: []pathLevel{{
				fields: mergeFields(deposit(20), pathExact(acct, 5, 26)),
				funds:  20,
			}}}},
			rewards: map[byte]uint64{0x42: 25},
		}},
		{"top-level interval sees every child's deposit", pathScenario{
			accounts: account,
			txs: []pathTx{{
				top: pathLevel{fields: pathExact(acct, 25, 26)},
				children: []pathLevel{
					{fields: deposit(20), funds: 20},
				},
			}},
			rewards: map[byte]uint64{0x42: 25},
		}},
		{"starting interval holds the batch-start balance", pathScenario{
			accounts: account,
			txs: []pathTx{{
				top: pathLevel{fields: pathExact(acct, 5, 27)},
				children: []pathLevel{
					{fields: deposit(20), funds: 20},
				},
			}},
			rewards: map[byte]uint64{0x42: 25},
		}},
		{"starting interval rejects the post-child balance", pathScenario{
			accounts: account,
			txs: []pathTx{{
				top: pathLevel{fields: pathExact(acct, 25, 27)},
				children: []pathLevel{
					{fields: deposit(20), funds: 20},
				},
			}},
			reject: outside,
		}},
		{"child sees an earlier child's withdrawal", pathScenario{
			accounts: signerAccount,
			txs: []pathTx{{children: []pathLevel{
				{
					fields: map[uint]any{
						5: map[cbor.ByteString]uint64{signer: 5},
					},
					withdrawn: 5,
				},
				{fields: pathExact(signer, 0, 26)},
			}}},
			rewards: map[byte]uint64{pathSignerKey: 0},
		}},
		{"starting interval ignores a child's withdrawal", pathScenario{
			accounts: signerAccount,
			txs: []pathTx{{
				top: pathLevel{fields: pathExact(signer, 5, 27)},
				children: []pathLevel{{
					fields: map[uint]any{
						5: map[cbor.ByteString]uint64{signer: 5},
					},
					withdrawn: 5,
				}},
			}},
			rewards: map[byte]uint64{pathSignerKey: 0},
		}},
		{"child sees an earlier child's registration", pathScenario{
			txs: []pathTx{{children: []pathLevel{
				{
					fields: map[uint]any{
						4: []any{pathRegistration(pathSignerKey)},
					},
				},
				{fields: pathExact(signer, 0, 26)},
			}}},
			rewards: map[byte]uint64{pathSignerKey: 0},
		}},
		{"starting interval does not see a child's registration", pathScenario{
			txs: []pathTx{{
				top: pathLevel{fields: pathExact(signer, 0, 27)},
				children: []pathLevel{{
					fields: map[uint]any{
						4: []any{pathRegistration(pathSignerKey)},
					},
				}},
			}},
			reject: missing,
		}},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			runPathScenario(t, sc.pathScenario)
		})
	}
}

func pathGuards(hash []byte) cbor.RawMessage {
	raw, err := cbor.Encode(cbor.NewSetType(
		[]lcommon.Credential{pathKeyCredential(hash)},
		true,
	))
	if err != nil {
		panic(err)
	}
	return raw
}

func pathKeyCredential(hash []byte) lcommon.Credential {
	return lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(hash),
	}
}

// pathRequiredGuards is a key 24 map of the key credential to the given
// datum, written as raw CBOR: map(1), [0, hash], datum.
func pathRequiredGuards(hash []byte, datum ...byte) cbor.RawMessage {
	raw := append([]byte{0xa1, 0x82, 0x00, 0x58, 0x1c}, hash...)
	return append(raw, datum...)
}

func TestDijkstraRequiredTopLevelGuardsThroughProductionPaths(t *testing.T) {
	t.Parallel()
	signer := pathSignerHash()
	required := map[uint]any{24: pathRequiredGuards(signer, 0xf6)}
	guarded := mergeFields(required, map[uint]any{14: pathGuards(signer)})
	missing := rejectWith[*dijkstra.MissingRequiredGuards]
	scenarios := []struct {
		name string
		pathScenario
	}{
		{
			"top-level requirement satisfied by the top-level guards",
			pathScenario{
				txs: []pathTx{{top: pathLevel{fields: guarded}}},
			},
		},
		{"top-level requirement without a guard", pathScenario{
			txs:    []pathTx{{top: pathLevel{fields: required}}},
			reject: missing,
		}},
		{"child requirement satisfied by the top-level guards", pathScenario{
			txs: []pathTx{{
				top: pathLevel{
					fields: map[uint]any{14: pathGuards(signer)},
				},
				children: []pathLevel{{fields: required}},
			}},
		}},
		{"child requirement missing from the top-level guards", pathScenario{
			txs:    []pathTx{{children: []pathLevel{{fields: required}}}},
			reject: missing,
		}},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			runPathScenario(t, sc.pathScenario)
		})
	}
}

// A malformed or empty required_top_level_guards field never decodes, so
// every path that reads a transaction or a block refuses it before any ledger
// state is touched: the mempool and block-fetch decoder for what a peer
// sends, the stored-block decoder that replay, backfill and Mithril gap
// processing share.
func TestDijkstraMalformedRequiredGuardsRejectedBeforeStateChange(
	t *testing.T,
) {
	t.Parallel()
	signer := pathSignerHash()
	// Patching a body invalidates the header's body hash, which would reject
	// the block before the field is read.
	skipBodyHash := lcommon.VerifyConfig{SkipBodyHashValidation: true}
	credential := append([]byte{0x82, 0x00, 0x58, 0x1c}, signer...)
	entry := func(datum ...byte) cbor.RawMessage {
		return append(append([]byte{0xa1}, credential...), datum...)
	}
	// A well-formed entry in the same position decodes through every path, so
	// the rejections below come from the field and not from the harness.
	for _, level := range []string{"top-level", "child"} {
		t.Run(level+"/well-formed control", func(t *testing.T) {
			t.Parallel()
			tx := pathTx{top: pathLevel{}}
			child := -1
			if level == "child" {
				tx = pathTx{children: []pathLevel{{}}}
				child = 0
			}
			f := newPathFixture(t, nil, []pathTx{tx})
			txCbor, blockCbor := f.withBodyField(0, child, 24, entry(0xf6))
			_, err := gledger.NewTransactionFromCbor(
				gledger.TxTypeDijkstra,
				txCbor,
			)
			require.NoError(t, err)
			for _, decode := range []func([]byte) (gledger.Block, error){
				func(raw []byte) (gledger.Block, error) {
					return models.DecodeDijkstraPeerBlock(raw, skipBodyHash)
				},
				func(raw []byte) (gledger.Block, error) {
					return models.DecodeBlockCbor(
						gledger.BlockTypeDijkstra, raw, skipBodyHash,
					)
				},
			} {
				_, err := decode(blockCbor)
				require.NoError(t, err)
			}
		})
	}
	for _, test := range []struct {
		name  string
		field cbor.RawMessage
	}{
		{"explicitly empty", cbor.RawMessage{0xa0}},
		{"not a map", cbor.RawMessage{0x01}},
		{"datum is not Plutus data", entry(0xf5)},
		{"datum is CBOR undefined", entry(0xf7)},
		{"unknown credential type", append(
			append([]byte{0xa1, 0x82, 0x02, 0x58, 0x1c}, signer...), 0xf6,
		)},
	} {
		for _, level := range []string{"top-level", "child"} {
			t.Run(level+"/"+test.name, func(t *testing.T) {
				t.Parallel()
				tx := pathTx{top: pathLevel{}}
				if level == "child" {
					tx = pathTx{children: []pathLevel{{}}}
				}
				f := newPathFixture(
					t,
					[]pathAccount{{key: 0x42, reward: 5}},
					[]pathTx{tx},
				)
				child := -1
				if level == "child" {
					child = 0
				}
				txCbor, blockCbor := f.withBodyField(0, child, 24, test.field)
				require.ErrorContains(
					t,
					f.mempoolAddRaw(txCbor),
					"decode transaction",
				)
				_, err := gledger.NewTransactionFromCbor(
					gledger.TxTypeDijkstra,
					txCbor,
				)
				require.Error(t, err)
				for name, decode := range map[string]func() (gledger.Block, error){
					"peer block decoder": func() (gledger.Block, error) {
						return models.DecodeDijkstraPeerBlock(blockCbor, skipBodyHash)
					},
					"stored block decoder": func() (gledger.Block, error) {
						return models.DecodeBlockCbor(
							gledger.BlockTypeDijkstra,
							blockCbor,
							skipBodyHash,
						)
					},
				} {
					block, err := decode()
					require.Error(t, err, name)
					require.Nil(t, block, name)
				}
				require.Equal(t, uint64(5), f.reward(0x42))
				require.True(t, f.inputUnspent(0))
			})
		}
	}
}
