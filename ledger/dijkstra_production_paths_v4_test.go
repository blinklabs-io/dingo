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
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/plutusv4script"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// A guarding script reads its ScriptContext during phase 2, so the same
// transaction bytes must get the same verdict from mempool admission, live
// block application, replay and reapplication after a rollback.
func TestDijkstraPlutusV4ContextThroughProductionPaths(t *testing.T) {
	t.Parallel()
	scriptFailed := rejectWith[conway.PlutusScriptFailedError]
	for _, field := range []struct {
		name string
		sel  func(ctx plutusv4script.Term) plutusv4script.Term
	}{
		{"txInfoSubTxIx", plutusv4script.TxInfoSubTxIx},
		{"guarding TopTxInfo", plutusv4script.GuardingTopTxInfo},
	} {
		for _, level := range []string{"top-level", "child"} {
			guarded := func(script lcommon.PlutusV4Script) pathTx {
				if level == "top-level" {
					return pathTx{top: pathLevel{script: script}}
				}
				return pathTx{children: []pathLevel{{script: script}}}
			}
			t.Run(field.name+"/"+level+" is Nothing", func(t *testing.T) {
				t.Parallel()
				runPathScenario(t, pathScenario{
					txs: []pathTx{
						guarded(plutusv4script.MaybeScript(t, field.sel, 1)),
					},
				})
			})
			t.Run(field.name+"/"+level+" is not Just", func(t *testing.T) {
				t.Parallel()
				runPathScenario(t, pathScenario{
					txs: []pathTx{
						guarded(plutusv4script.MaybeScript(t, field.sel, 0)),
					},
					reject: scriptFailed,
				})
			})
		}
	}
	t.Run("every child of one batch is Nothing", func(t *testing.T) {
		t.Parallel()
		script := plutusv4script.MaybeScript(t, plutusv4script.TxInfoSubTxIx, 1)
		runPathScenario(t, pathScenario{
			txs: []pathTx{{children: []pathLevel{
				{script: script},
				{script: plutusv4script.MaybeScript(
					t, plutusv4script.GuardingTopTxInfo, 1,
				)},
			}}},
		})
	})
}

// The TxInfo maps that carry reward accounts and guards follow the
// reference's AccountAddress and Credential order: network first, then script
// entries before key entries, then the credential bytes. Equal-byte script and
// key accounts are what tell that order apart from a plain sort of the
// encoded address, so the observing script runs against exactly them, and runs
// before the deposit it observes is applied.
func TestDijkstraPlutusV4AccountMapOrderThroughProductionPaths(t *testing.T) {
	t.Parallel()
	const hash = 0x42
	accounts := []pathAccount{
		{key: hash, reward: 5},
		{key: hash, reward: 5, script: true},
	}
	keyAccount := pathKeyAccount(hash)
	scriptAccount := pathAccountFor(0xf0, hash)
	for _, level := range []string{"top-level", "child"} {
		guarded := func(l pathLevel) pathTx {
			if level == "top-level" {
				return pathTx{top: l}
			}
			return pathTx{children: []pathLevel{l}}
		}
		t.Run(level+"/direct deposits", func(t *testing.T) {
			t.Parallel()
			runPathScenario(t, pathScenario{
				accounts: accounts,
				txs: []pathTx{guarded(pathLevel{
					script: plutusv4script.MapOrderScript(
						t, plutusv4script.DirectDepositsField,
					),
					fields: pathDeposits(map[cbor.ByteString]uint64{
						keyAccount:    2,
						scriptAccount: 1,
					}),
					funds: 3,
				})},
				rewards:       map[byte]uint64{hash: 7},
				scriptRewards: map[byte]uint64{hash: 6},
			})
		})
		t.Run(level+"/balance intervals", func(t *testing.T) {
			t.Parallel()
			runPathScenario(t, pathScenario{
				accounts: accounts,
				txs: []pathTx{guarded(pathLevel{
					script: plutusv4script.MapOrderScript(
						t, plutusv4script.BalanceIntervalsField,
					),
					fields: pathIntervals(map[cbor.ByteString]any{
						keyAccount:    uint64(5),
						scriptAccount: uint64(5),
					}, 26),
				})},
				rewards:       map[byte]uint64{hash: 5},
				scriptRewards: map[byte]uint64{hash: 5},
			})
		})
	}
	t.Run("a script that expects the other order fails", func(t *testing.T) {
		t.Parallel()
		runPathScenario(t, pathScenario{
			accounts: accounts,
			txs: []pathTx{{top: pathLevel{
				script: plutusv4script.MapOrderScript(
					t, plutusv4script.DirectDepositsField,
				),
				// One entry cannot be the script-then-key pair the observer
				// requires.
				fields: pathDeposits(map[cbor.ByteString]uint64{keyAccount: 3}),
				funds:  3,
			}}},
			reject: rejectWith[conway.PlutusScriptFailedError],
		})
	})
}

// pathRequiredGuardsPair is a key 24 map holding the signer's key credential
// with no datum and the script's credential with datum 7, as raw CBOR.
func pathRequiredGuardsPair(script lcommon.PlutusV4Script) cbor.RawMessage {
	scriptHash := plutusv4script.ScriptCredential(script).Credential
	raw := []byte{0xa2, 0x82, 0x00, 0x58, 0x1c}
	raw = append(raw, pathSignerHash()...)
	raw = append(raw, 0xf6, 0x82, 0x01, 0x58, 0x1c)
	raw = append(raw, scriptHash[:]...)
	return append(raw, 0x07)
}

// Required top-level guards reach the guarding script in the reference's
// order, and a requirement the guard set does not cover fails before the
// script runs. A child cannot hold this scenario: its requirements are met by
// the top-level guard set, which cannot carry the child's own script.
func TestDijkstraPlutusV4RequiredGuardsThroughProductionPaths(t *testing.T) {
	t.Parallel()
	signerGuard := []lcommon.Credential{pathKeyCredential(pathSignerHash())}
	t.Run("script observes script before key", func(t *testing.T) {
		t.Parallel()
		script := plutusv4script.MapOrderScript(
			t, plutusv4script.RequiredGuardsField,
		)
		runPathScenario(t, pathScenario{
			txs: []pathTx{{top: pathLevel{
				script: script,
				guards: signerGuard,
				fields: map[uint]any{24: pathRequiredGuardsPair(script)},
			}}},
		})
	})
	t.Run("requirement missing from the guard set", func(t *testing.T) {
		t.Parallel()
		script := plutusv4script.MapOrderScript(
			t, plutusv4script.RequiredGuardsField,
		)
		runPathScenario(t, pathScenario{
			txs: []pathTx{{top: pathLevel{
				script: script,
				fields: map[uint]any{24: pathRequiredGuardsPair(script)},
			}}},
			reject: rejectWith[*dijkstra.MissingRequiredGuards],
		})
	})
}
