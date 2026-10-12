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

package ouroboros

import (
	"bytes"
	"encoding/hex"
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/mempool"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocaltxmonitor "github.com/blinklabs-io/gouroboros/protocol/localtxmonitor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocaltxmonitorServerGetMempoolReportsConfiguredCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		capacity int64
	}{
		{name: "praos default", capacity: 1024 * 1024},
		{name: "leios default", capacity: 25 * 1024 * 1024},
		{name: "custom", capacity: 7 * 1024 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o, _ := newTxSubmissionTestOuroboros(
				t,
				func(cfg *mempool.MempoolConfig) {
					cfg.MempoolCapacity = tt.capacity
				},
			)
			o.ledgerState = newTestLedgerState(t)

			_, capacity, _, err := o.localtxmonitorServerGetMempool(
				olocaltxmonitor.CallbackContext{},
			)

			require.NoError(t, err)
			assert.Equal(
				t,
				uint32(tt.capacity),
				capacity,
			) // #nosec G115 -- test values fit
		})
	}
}

func TestLocaltxmonitorServerGetMempoolRejectsUnrepresentableCapacity(
	t *testing.T,
) {
	t.Parallel()

	o, _ := newTxSubmissionTestOuroboros(t, func(cfg *mempool.MempoolConfig) {
		cfg.MempoolCapacity = int64(math.MaxUint32) + 1
	})
	o.ledgerState = newTestLedgerState(t)

	_, _, _, err := o.localtxmonitorServerGetMempool(
		olocaltxmonitor.CallbackContext{},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be represented")
}

func addLocaltxmonitorTestTxs(
	t *testing.T,
	o *Ouroboros,
	count int,
) []txsubmissionTestFixture {
	t.Helper()
	fixtures := txsubmissionTestFixtures(t)[:count]
	addTxSubmissionTestFixtures(t, o.mempool, fixtures...)
	return fixtures
}

// TestLocaltxmonitorServerGetMempoolListsTransactions proves the snapshot
// carries each mempool transaction's era and raw bytes, in mempool order, and
// is empty when the mempool is.
func TestLocaltxmonitorServerGetMempoolListsTransactions(t *testing.T) {
	t.Parallel()

	o, _ := newTxSubmissionTestOuroboros(t)
	o.ledgerState = newTestLedgerState(t)
	o.ledgerState.SetTipForTesting(ochainsync.Tip{
		Point: ocommon.NewPoint(77, bytes.Repeat([]byte{7}, 32)),
	})

	_, _, txs, err := o.localtxmonitorServerGetMempool(
		olocaltxmonitor.CallbackContext{},
	)
	require.NoError(t, err)
	require.Empty(t, txs)

	fixtures := addLocaltxmonitorTestTxs(t, o, 2)
	slot, _, txs, err := o.localtxmonitorServerGetMempool(
		olocaltxmonitor.CallbackContext{},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(77), slot)
	require.Len(t, txs, len(fixtures))
	for i, fixture := range fixtures {
		assert.Equal(t, uint(txsubmissionRelayTestEraId), txs[i].EraId)
		assert.Equal(t, fixture.body, txs[i].Tx)
	}
}

// TestLocaltxmonitorServerGetMempoolRequiresDependencies covers the
// unavailable-dependency case: a missing ledger state or mempool is an error,
// not a panic.
func TestLocaltxmonitorServerGetMempoolRequiresDependencies(t *testing.T) {
	t.Parallel()

	withMempool, _ := newTxSubmissionTestOuroboros(t)
	withLedger := newOuroboros(OuroborosConfig{})
	withLedger.ledgerState = newTestLedgerState(t)

	for name, o := range map[string]*Ouroboros{
		"no ledger state": withMempool,
		"no mempool":      withLedger,
		"neither":         newOuroboros(OuroborosConfig{}),
	} {
		t.Run(name, func(t *testing.T) {
			require.NotPanics(t, func() {
				_, _, _, err := o.localtxmonitorServerGetMempool(
					olocaltxmonitor.CallbackContext{},
				)
				require.ErrorIs(t, err, errLocalTxMonitorUnavailable)
			})
		})
	}
}

// TestLocaltxmonitorProtocol_SnapshotHasTxNextTx drives the monitor over a
// real connection: an acquired snapshot answers HasTx, NextTx and GetSizes
// from the mempool as it was at Acquire, and a later Acquire sees additions.
func TestLocaltxmonitorProtocol_SnapshotHasTxNextTx(t *testing.T) {
	t.Parallel()

	o, _ := newTxSubmissionTestOuroboros(t)
	o.ledgerState = newTestLedgerState(t)
	fixtures := txsubmissionTestFixtures(t)
	addTxSubmissionTestFixtures(t, o.mempool, fixtures[0])
	conn := newNtCTestClientConn(
		t,
		ouroboros.WithLocalTxMonitorConfig(
			olocaltxmonitor.NewConfig(o.localtxmonitorServerConnOpts()...),
		),
	)
	client := conn.LocalTxMonitor().Client
	require.NotNil(t, client)
	hashOf := func(f txsubmissionTestFixture) []byte {
		id, err := hex.DecodeString(f.hash)
		require.NoError(t, err)
		return id
	}

	require.NoError(t, client.Acquire())
	addTxSubmissionTestFixtures(t, o.mempool, fixtures[1])

	has, err := client.HasTx(hashOf(fixtures[0]))
	require.NoError(t, err)
	require.True(t, has, "the snapshot holds the transaction pooled at Acquire")
	has, err = client.HasTx(hashOf(fixtures[1]))
	require.NoError(t, err)
	require.False(t, has, "the snapshot must not see a later addition")

	capacity, size, count, err := client.GetSizes()
	require.NoError(t, err)
	require.Equal(t, uint32(1024*1024), capacity)
	require.Equal(t, uint32(len(fixtures[0].body)), size)
	require.Equal(t, uint32(1), count)

	next, err := client.NextTx()
	require.NoError(t, err)
	require.Equal(t, fixtures[0].body, next)
	next, err = client.NextTx()
	require.NoError(t, err)
	require.Empty(t, next, "NextTx past the end of the snapshot is empty")
	require.NoError(t, client.Release())

	require.NoError(t, client.Acquire())
	has, err = client.HasTx(hashOf(fixtures[1]))
	require.NoError(t, err)
	require.True(t, has, "a fresh Acquire must see the added transaction")
	require.NoError(t, client.Release())
}
