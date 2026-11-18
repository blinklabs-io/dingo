// Copyright 2024 Blink Labs Software
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
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/internal/tracing"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Not t.Parallel: installs a recording OpenTelemetry provider and the block
// tracing configuration, both process globals.
func TestForgedBlockSpanInBlockTrace(t *testing.T) {
	raw := testutil.BuildDecodableConwayBlockBytes(t, 42, 7)
	block, err := gledger.NewBlockFromCbor(gledger.BlockTypeConway, raw)
	require.NoError(t, err)
	spans := testutil.RecordSpans(t)
	testutil.ConfigureBlockTracing(t, tracing.Config{
		SampleRatio:       0,
		AlwaysSamplePools: [][28]byte{[28]byte(tracing.IssuerKeyHash(block))},
	})
	start := time.Unix(2_000, 0)

	traceForgedBlock(
		context.Background(),
		block,
		42,
		start,
		start.Add(time.Second),
	)

	ended := spans.Ended()
	require.Len(t, ended, 1)
	require.Equal(t, "block.forge", ended[0].Name())
	require.Equal(t, start, ended[0].StartTime())
	require.Equal(t, start.Add(time.Second), ended[0].EndTime())
	require.Equal(
		t,
		tracing.BlockTraceID(block.Hash().Bytes()),
		ended[0].SpanContext().TraceID(),
	)
}

// Not t.Parallel: installs the process-wide tracing provider and configuration.
func TestProductionForgeStageSpans(t *testing.T) {
	t.Run("successful broadcast", func(t *testing.T) {
		testProductionForgeStageSpans(t, nil)
	})
	t.Run("failed broadcast", func(t *testing.T) {
		testProductionForgeStageSpans(t, errors.New("broadcast failed"))
	})
}

type tracingSlotArbiter struct {
	fixedSlotArbiter
	reportedAt time.Time
}

func (a *tracingSlotArbiter) ReportSlot(slot uint64, adopted bool) {
	a.reportedAt = time.Now()
	a.fixedSlotArbiter.ReportSlot(slot, adopted)
}

func testProductionForgeStageSpans(t *testing.T, broadcastErr error) {
	t.Helper()
	spans := testutil.RecordSpans(t)
	testutil.ConfigureBlockTracing(t, tracing.Config{SampleRatio: 1})
	raw := testutil.BuildDecodableConwayBlockBytes(t, 10, 2)
	block, err := gledger.NewBlockFromCbor(gledger.BlockTypeConway, raw)
	require.NoError(t, err)
	var logs bytes.Buffer
	clock := newTimingClock()
	forger := newTimingForger(t, &logs, clock,
		&forgerTestBuilder{block: block, cbor: raw},
		&forgerTestBroadcaster{}, &forgerTestValidator{})
	eb := leiosCapacityForger(leiosCapacityCandidates(t, 2, 0),
		scaleTestPParams(), &forgerTestLeiosCaster{err: broadcastErr})
	forger.leiosChecker = eb.leiosChecker
	forger.leiosMempool = eb.leiosMempool
	forger.leiosEBCaster = eb.leiosEBCaster
	forger.eraParams = eb.eraParams
	forger.leiosAnnouncementValidator = func(context.Context, lcommon.BlockHeader) error { return nil }
	now := time.Now()
	clock.slotEnd = now.Add(time.Second)
	forger.now = func() time.Time { return now }
	arbiter := &tracingSlotArbiter{}
	forger.slotArbiter = arbiter
	published := false
	forger.blockForged = func(gledger.Block, []byte, time.Duration) {
		exported := spans.Ended()
		require.Len(t, exported, 1, "child spans must not delay publication")
		require.Equal(
			t,
			"block.forge",
			exported[0].Name(),
			"root ends before the observer",
		)
		require.Equal(
			t,
			[]slotReport{{slot: 10, adopted: true}},
			arbiter.reports,
		)
		published = true
	}

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.True(t, published)
	parents := map[string]string{
		"block.forge.leader_check":                "block.forge",
		"block.forge.pre_build":                   "block.forge",
		"block.forge.leios.prepare":               "block.forge.pre_build",
		"block.forge.leios.rebase":                "block.forge.pre_build",
		"block.forge.leios.endorser":              "block.forge.pre_build",
		"block.forge.leios.mempool_snapshot":      "block.forge.leios.endorser",
		"block.forge.leios.select":                "block.forge.leios.endorser",
		"block.forge.leios.build":                 "block.forge.leios.endorser",
		"block.forge.leios.broadcast":             "block.forge.leios.endorser",
		"block.forge.build":                       "block.forge",
		"block.forge.validate":                    "block.forge",
		"block.forge.leios.validate_announcement": "block.forge",
		"block.forge.adopt":                       "block.forge",
	}
	if broadcastErr != nil {
		require.Contains(t, logs.String(), broadcastErr.Error())
		delete(parents, "block.forge.leios.validate_announcement")
	}
	recorded := spans.Ended()
	require.Len(t, recorded, len(parents)+2, "one span per phase")
	ended := make(map[string]sdktrace.ReadOnlySpan)
	for _, span := range recorded {
		ended[span.Name()] = span
	}
	root := ended["block.forge"]
	require.NotNil(t, root)
	require.Equal(
		t,
		tracing.BlockTraceID(block.Hash().Bytes()),
		root.SpanContext().TraceID(),
	)
	require.False(
		t,
		root.EndTime().After(arbiter.reportedAt),
		"slot reporting is outside the forge span",
	)
	for name, parentName := range parents {
		child := ended[name]
		require.NotNil(t, child, "missing forge stage %s", name)
		parent := ended[parentName]
		require.Equal(
			t,
			root.SpanContext().TraceID(),
			child.SpanContext().TraceID(),
			name,
		)
		require.Equal(
			t,
			parent.SpanContext().SpanID(),
			child.Parent().SpanID(),
			name,
		)
		require.False(t, child.StartTime().Before(parent.StartTime()), name)
		require.False(t, child.EndTime().After(parent.EndTime()), name)
		require.False(t, child.EndTime().Before(child.StartTime()), name)
	}
	selectionAttrs := make(map[attribute.Key]attribute.Value)
	for _, attr := range ended["block.forge.leios.select"].Attributes() {
		selectionAttrs[attr.Key] = attr.Value
	}
	require.Equal(t, int64(2), selectionAttrs["forge.candidates"].AsInt64())
	require.Equal(t, int64(2), selectionAttrs["forge.selected"].AsInt64())
	require.Equal(
		t,
		attribute.BoolValue(false),
		selectionAttrs["forge.selection_truncated"],
	)
	publish := ended["block.forge.publish"]
	require.NotNil(t, publish)
	require.Equal(
		t,
		root.SpanContext().TraceID(),
		publish.SpanContext().TraceID(),
	)
	require.Equal(
		t,
		root.Parent().SpanID(),
		publish.Parent().SpanID(),
		"publication is outside the adoption interval",
	)
	require.False(t, publish.StartTime().Before(root.EndTime()))
}

// Not t.Parallel: installs the process-wide tracing provider and configuration.
func TestForgeStagesRespectSamplingAndAdoption(t *testing.T) {
	for _, test := range []struct {
		name          string
		ratio         float64
		validationErr error
		disabled      bool
	}{
		{name: "disabled", disabled: true},
		{name: "unsampled", ratio: 0},
		{name: "validation rejected", ratio: 1, validationErr: errors.New("invalid block")},
	} {
		t.Run(test.name, func(t *testing.T) {
			spans := testutil.RecordSpans(t)
			testutil.ConfigureBlockTracing(
				t,
				tracing.Config{SampleRatio: test.ratio},
			)
			if test.disabled {
				tracing.Disable()
			}
			block := newForgerTestBlock(10, 2)
			var logs bytes.Buffer
			forger := newTimingForger(
				t,
				&logs,
				newTimingClock(),
				&retryTestBuilder{block: block, cbor: block.cbor},
				&forgerTestBroadcaster{},
				&forgerTestValidator{err: test.validationErr},
			)
			adopted := false
			forger.blockForged = func(gledger.Block, []byte, time.Duration) {
				adopted = true
			}
			err := forger.checkAndForgeProduction(context.Background())
			if test.validationErr != nil {
				require.ErrorIs(t, err, test.validationErr)
				require.False(t, adopted)
			} else {
				require.NoError(t, err)
				require.True(t, adopted)
			}
			require.Empty(
				t,
				spans.Ended(),
				"unadopted or unsampled blocks must not export forge stages",
			)
		})
	}
}
