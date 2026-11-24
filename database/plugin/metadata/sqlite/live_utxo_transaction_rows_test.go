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

package sqlite

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestLedgerStoreKeepsNoTransactionRows applies a transaction that registers
// and delegates a credential and pays its base address to a store whose
// ledger holds the live state. The store keeps no row of it: the LedgerDB
// holds its effects and its index entry, and the IndexerDB writer records the
// history.
func TestLedgerStoreKeepsNoTransactionRows(t *testing.T) {
	t.Parallel()
	for _, batched := range []bool{false, true} {
		name := "unbatched"
		if batched {
			name = "batched"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newPointerStakeFixture(t)
			f.setEra(t, babbage.EraIdBabbage)
			f.store.SetLiveUtxoSource(emptyLiveSource{})
			t.Cleanup(func() { f.store.SetLiveUtxoSource(nil) })
			certs := []lcommon.Certificate{f.register(), f.delegate()}
			output := f.output(700, f.baseAddress(t))
			if batched {
				applyBatched(t, f, certs, output)
			} else {
				f.apply(t, 100, 0, certs, output)
			}
			count := func(table string) int {
				var n int
				require.NoError(t, f.db.QueryRow(
					`SELECT COUNT(*) FROM "`+table+`"`,
				).Scan(&n))
				return n
			}
			for _, table := range []string{
				"transaction",
				"applied_transaction",
				"stake_registration",
				"stake_delegation",
				"account",
				"reward_live_stake",
			} {
				require.Zero(t, count(table), "rows in %s", table)
			}
		})
	}
}

// applyBatched writes one transaction at slot 100 through the batch
// accumulator the ledger's block apply uses, then flushes it.
func applyBatched(
	t *testing.T,
	f *pointerStakeFixture,
	certificates []lcommon.Certificate,
	output lcommon.TransactionOutput,
) {
	t.Helper()
	hash := lcommon.Blake2b256{}
	hash[0] = f.nextTx
	f.nextTx++
	acc := f.store.NewBatchAccumulator()
	require.NoError(t, f.store.SetTransactionBatched(
		&mockTransaction{
			hash:         hash,
			isValid:      true,
			certificates: certificates,
			produced: []lcommon.Utxo{{
				Id:     mockTransactionInput{hash: hash},
				Output: output,
			}},
		},
		ocommon.Point{Slot: 100, Hash: bytes.Repeat([]byte{0xc1}, 32)},
		0,
		map[int]uint64{0: 0, 1: 0},
		false,
		acc,
		nil,
		0,
	))
	require.NoError(t, f.store.FlushBatch(acc, nil))
}
