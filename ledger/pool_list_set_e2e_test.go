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

package ledger_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

var poolListReadNames = []string{
	"GetPoolsRetiringAtEpoch", "GetActivePoolKeyHashes",
	"GetActivePoolKeyHashesAtSlot",
}

// TestPoolListReadsFromTheLiveUtxoSet reads the pools retiring at an epoch and
// the active pools from the live set with those reads failing against the metadata
// store, before and after a retirement and a re-registration, and requires
// every answer to be the store's.
func TestPoolListReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	h := newPoolReadsHarnessWithReads(t, "set")
	reached, _ := h.scenario.HideStoreReads(poolListReadNames...)
	slot := h.cur.StartSlot + 1000
	require.Equal(t, [][]byte{h.pool}, h.active(t, slot, true))
	require.Empty(t, h.retiring(t, h.cur.EpochId+1))

	var during []models.PoolRetirementRefund
	var activeDuring [][]byte
	newPool := h.scenario.Pools(func() {
		during = h.retiring(t, h.cur.EpochId+1)
		activeDuring = h.active(t, slot, false)
	}).Pool
	require.Len(t, during, 1, "the harness pool retires at the next epoch")
	require.Equal(t, h.pool, during[0].PoolKeyHash)
	require.ElementsMatch(t, [][]byte{h.pool, newPool}, activeDuring)
	require.Equal(t, [][]byte{newPool}, h.active(t, h.nextStart()+10, false),
		"the boundary reaps the retired pool")
	require.Zero(t, reached(), "a pool list read reached the metadata store")
}

// TestPoolRetirementReadSeesItsTransactionsStagedRetirementInSetMode requires
// the retirement read in a write transaction to see the certificate that
// transaction has staged, with the store's read failing.
func TestPoolRetirementReadSeesItsTransactionsStagedRetirementInSetMode(
	t *testing.T,
) {
	t.Parallel()
	h := newPoolReadsHarnessWithReads(t, "set")
	reached, _ := h.scenario.HideStoreReads(poolListReadNames...)
	got := h.scenario.RetireInsideTxn(h.pool, h.cur.EpochId+1, h.nextStart())
	require.Len(t, got, 1)
	require.Equal(t, h.pool, got[0].PoolKeyHash)
	require.Zero(t, reached(), "the retirement read reached the metadata store")
}
