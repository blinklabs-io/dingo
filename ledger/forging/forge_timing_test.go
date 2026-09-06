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
	"github.com/stretchr/testify/require"
)

func forgeTimingRecord(t *testing.T, logs string) map[string]any {
	t.Helper()
	for line := range strings.SplitSeq(strings.TrimSpace(logs), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		if record["msg"] == "forge timing" {
			return record
		}
	}
	t.Fatalf("no forge timing line in logs: %s", logs)
	return nil
}

func newTimingForger(
	t *testing.T,
	logs *bytes.Buffer,
	clock *retryTestSlotClock,
	builder BlockBuilder,
	broadcaster *forgerTestBroadcaster,
) *BlockForger {
	t.Helper()
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(logs, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger
}

// TestForgeLogsTimingForEveryForge records what the field trace behind this
// change had to be reconstructed from block timestamps: how long the leader
// gate took to clear and how long selection then ran. Without it a lost
// slot shows only an error line and a counter.
func TestForgeLogsTimingForEveryForge(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{block: block, cbor: block.cbor}
	var logs bytes.Buffer
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(time.Second),
	}
	forger := newTimingForger(
		t,
		&logs,
		clock,
		builder,
		&forgerTestBroadcaster{},
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	record := forgeTimingRecord(t, logs.String())
	require.Equal(t, float64(10), record["slot"])
	require.Equal(t, "forged", record["outcome"])
	require.Equal(t, float64(1), record["attempts"])
	for _, key := range []string{
		"leader_check",
		"pre_build",
		"build",
		"tx_count",
	} {
		require.Contains(t, record, key)
	}
}

// TestForgeTimingReportsTheEmptyFallbackOutcome makes the fallback legible
// in the log as well as in the metric.
func TestForgeTimingReportsTheEmptyFallbackOutcome(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &fallbackTestBuilder{
		block:     block,
		cbor:      block.cbor,
		selectErr: errTxValidationSnapshotChanged,
	}
	var logs bytes.Buffer
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now(),
	}
	forger := newTimingForger(
		t,
		&logs,
		clock,
		builder,
		&forgerTestBroadcaster{},
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	record := forgeTimingRecord(t, logs.String())
	require.Equal(t, "empty", record["outcome"])
	require.Equal(t, float64(2), record["attempts"])
}

// TestForgeTimingReportsALostSlot keeps the timing line on the failure path,
// which is the one an operator reads after a slot produced nothing.
func TestForgeTimingReportsALostSlot(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{
		block:     block,
		cbor:      block.cbor,
		failCount: 1000,
		err:       errTxValidationSnapshotChanged,
	}
	var logs bytes.Buffer
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(time.Hour),
	}
	forger := newTimingForger(
		t,
		&logs,
		clock,
		builder,
		&forgerTestBroadcaster{},
	)

	require.Error(t, forger.checkAndForgeProduction(context.Background()))
	record := forgeTimingRecord(t, logs.String())
	require.Equal(t, "lost", record["outcome"])
	// One initial pass, the retry cap, and the empty-block fallback
	// attempt that also failed.
	require.Equal(
		t,
		float64(2+defaultForgeSelectionMaxRetries),
		record["attempts"],
	)
}
