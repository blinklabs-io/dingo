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

//nolint:gosec // SQL INTEGER mappings preserve the unsigned domain API.
package sqlite

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestSQLiteProviderWidensRetainedOverlayInserts(t *testing.T) {
	t.Parallel()
	for _, tracing := range []bool{false, true} {
		name := "native"
		if tracing {
			name = "traced"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			registry := prometheus.NewRegistry()
			store, raw, _, err := openSQLStore(
				Config{DataDir: t.TempDir()},
				metadata.ProviderDependencies{
					StorageMode: types.StorageModeAPI, PromRegistry: registry, TracingEnabled: tracing,
				},
			)
			require.NoError(t, err)
			require.NoError(t, store.Start(t.Context()))
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			address, err := lcommon.NewAddressFromParts(
				lcommon.AddressTypeKeyNone,
				lcommon.AddressNetworkTestnet,
				bytes.Repeat([]byte{0x61}, 28),
				nil,
			)
			require.NoError(t, err)
			output, err := mockledger.NewTransactionOutputBuilder().
				WithAddress(address.String()).
				WithLovelace(1_000_000).
				Build()
			require.NoError(t, err)
			seed := mockledger.NewTransactionBuilder()
			seed.WithId(bytes.Repeat([]byte{0x62}, 32))
			seed.WithOutputs(output)
			seed.WithValid(true)
			require.NoError(
				t,
				store.SetTransaction(
					seed,
					ocommon.Point{Slot: 10, Hash: seed.Hash().Bytes()},
					0,
					nil,
					true,
					nil,
					0,
				),
			)
			transactions := make([]lcommon.Transaction, 2)
			prior := seed.Hash().Bytes()
			for i := range transactions {
				input, err := mockledger.NewSimpleTransactionInput(prior, 0)
				require.NoError(t, err)
				tx := mockledger.NewTransactionBuilder()
				tx.WithId(bytes.Repeat([]byte{0x63 + byte(i)}, 32))
				tx.WithInputs(input)
				if i == 1 {
					outputs := make([]lcommon.TransactionOutput, 127)
					for j := range outputs {
						outputs[j] = output
					}
					tx.WithOutputs(outputs...)
				} else {
					tx.WithOutputs(output)
				}
				tx.WithValid(true)
				transactions[i] = tx
				prior = tx.Hash().Bytes()
			}
			txn := store.Transaction(t.Context())
			t.Cleanup(func() { _ = txn.Rollback() })
			acc := store.NewLeiosClosureBatchAccumulator()
			t.Cleanup(acc.Reset)
			enabled, err := store.EnableLeiosClosureUtxoOverlay(
				acc,
				txn,
				transactions,
			)
			require.NoError(t, err)
			require.True(t, enabled)
			for i, tx := range transactions {
				executionSlot := uint64(99)
				require.NoError(
					t,
					store.SetTransactionLeiosClosureBatched(
						tx,
						ocommon.Point{
							Slot: 100,
							Hash: bytes.Repeat([]byte{0x65}, 32),
						},
						uint32(i),
						nil,
						true,
						&executionSlot,
						acc,
						txn,
						0,
					),
				)
			}
			require.NoError(t, store.FlushBatch(acc, txn))
			require.NoError(t, txn.Commit())
			var retained int
			require.NoError(
				t,
				raw.QueryRow("SELECT COUNT(*) FROM utxo").Scan(&retained),
			)
			require.Equal(t, 129, retained)
			families, err := registry.Gather()
			require.NoError(t, err)
			var inserts uint64
			for _, family := range families {
				if family.GetName() != "dingo_database_sql_query_duration_seconds" {
					continue
				}
				for _, metric := range family.Metric {
					for _, label := range metric.Label {
						if label.GetName() == "query" &&
							label.GetValue() == "BatchOverlayInsertUtxos" {
							inserts += metric.GetHistogram().GetSampleCount()
						}
					}
				}
			}
			require.Equal(t, uint64(1), inserts)
		})
	}
}
