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

package forging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func ebLogRecord(t *testing.T, logs, msg string) map[string]any {
	t.Helper()
	for line := range strings.SplitSeq(strings.TrimSpace(logs), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		if record["msg"] == msg {
			return record
		}
	}
	t.Fatalf("no %q line in logs: %s", msg, logs)
	return nil
}

func histogramSampleCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, h.Write(m))
	return m.GetHistogram().GetSampleCount()
}

func newEBTimingForger(
	t *testing.T,
	logs *bytes.Buffer,
	clock *ebTestSlotClock,
	validator TxValidator,
	caster *forgerTestLeiosCaster,
	broadcaster *forgerTestBroadcaster,
	txs []MempoolTransaction,
) *BlockForger {
	t.Helper()
	block := newForgerTestBlock(10, 2)
	forger, err := NewBlockForger(ForgerConfig{
		Mode:                ModeProduction,
		Logger:              slog.New(slog.NewJSONHandler(logs, nil)),
		Credentials:         setupTestCredentials(t),
		LeaderChecker:       forgerTestLeader{},
		BlockBuilder:        &forgerTestBuilder{block: block, cbor: block.cbor},
		BlockBroadcaster:    broadcaster,
		SlotClock:           clock,
		LeiosProduceChecker: &forgerTestLeiosChecker{allowed: true},
		LeiosEBBroadcaster:  caster,
		LeiosTxValidator:    validator,
		LeiosMempool:        forgerTestMempoolProvider{txs: txs},
		PromRegistry:        prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger
}

// TestLeiosEBProducedLineCarriesTimingBreakdown makes endorser-block
// construction legible from the node's own logs. The field trace behind
// this change had to be reconstructed from a 3.5-second gap between log
// lines, because nothing recorded how long selection took.
func TestLeiosEBProducedLineCarriesTimingBreakdown(t *testing.T) {
	var logs bytes.Buffer
	forger := newEBTimingForger(
		t,
		&logs,
		&ebTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Hour),
		},
		&sessionMockTxValidator{},
		&forgerTestLeiosCaster{},
		&forgerTestBroadcaster{},
		leiosCandidateTxs(t, 4),
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	record := ebLogRecord(t, logs.String(), "leios endorser block produced")
	for _, key := range []string{
		"eb_select",
		"eb_build",
		"eb_broadcast",
		"candidates",
	} {
		require.Contains(t, record, key)
	}
	require.Equal(t, float64(4), record["tx_refs"])
	require.Equal(
		t,
		uint64(1),
		histogramSampleCount(t, forger.metrics.leiosEbSelectionSeconds),
	)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.leiosEbSelectionTruncated),
	)
}

// TestLeiosEBSelectionTruncationIsCounted follows the runtime composition
// path -- checkAndForgeProduction -> checkAndForgeLeiosEB ->
// selectValidLeiosTransactions -- and proves that the slot deadline
// reaches the pass that spends the slot, that operators get the signal
// that the slot budget (not the mempool) decided endorser-block size, and
// that the ranking block is still forged. A deadline that exists in the
// forger but never arrives at selection bounds nothing.
func TestLeiosEBSelectionTruncationIsCounted(t *testing.T) {
	var logs bytes.Buffer
	validator := &sessionMockTxValidator{}
	caster := &forgerTestLeiosCaster{}
	broadcaster := &forgerTestBroadcaster{}
	forger := newEBTimingForger(
		t,
		&logs,
		&ebTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now().Add(time.Second),
		},
		validator,
		caster,
		broadcaster,
		leiosCandidateTxs(t, 10),
	)
	// Every candidate consumes a tenth of a second of the budget, so the
	// pass cannot get through all ten inside the slot.
	fakeNow := time.Now()
	forger.now = func() time.Time { return fakeNow }
	validator.onValidate = func(int) {
		fakeNow = fakeNow.Add(100 * time.Millisecond)
	}

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.NotEmpty(t, caster.txBodies)
	require.Less(
		t,
		len(caster.txBodies),
		10,
		"endorser-block selection must stop when the slot budget is gone",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.leiosEbSelectionTruncated),
	)
	require.Equal(
		t,
		uint64(1),
		histogramSampleCount(t, forger.metrics.leiosEbSelectionSeconds),
	)
	require.Equal(
		t,
		1,
		broadcaster.calls,
		"the ranking block must still be forged and broadcast",
	)
}
