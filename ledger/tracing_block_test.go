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
	"context"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/internal/tracing"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func spansNamed(
	spans []sdktrace.ReadOnlySpan,
	name string,
) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, span := range spans {
		if span.Name() == name {
			out = append(out, span)
		}
	}
	return out
}

func fixtureLedgerBlocks(t *testing.T) []ledger.Block {
	t.Helper()
	_, raw := loadBlockProcessingFixture(t)
	blocks := make([]ledger.Block, 0, len(raw))
	for _, r := range raw {
		b, err := ledger.NewBlockFromCbor(r.Type, r.Cbor)
		require.NoError(t, err)
		blocks = append(blocks, b)
	}
	return blocks
}

// Not t.Parallel: installs a recording OpenTelemetry provider and the block
// tracing configuration, both process globals.
func TestFailedBlockApplySpanMarksOutcome(t *testing.T) {
	fx := loadUtxoMemoPreprodFixture(t)
	spans := testutil.RecordSpans(t)
	testutil.ConfigureBlockTracing(t, tracing.Config{SampleRatio: 1})
	block := &validityOutcomeTestBlock{header: fx.block.Header()}
	point := ocommon.NewPoint(fx.blockSlot, block.Hash().Bytes())

	err := fx.db.Transaction(context.Background(), true).
		Do(func(txn *database.Txn) error {
			_, err := fx.dingoLS.ledgerProcessBlock(
				context.Background(),
				txn,
				point,
				block,
				true,
				false,
				false,
				[]byte{1},
				envelopeParent{origin: true},
				&database.BlockIngestionResult{},
				eras.ConwayEraDesc,
				fx.dingoLS.currentPParams,
				nil,
				0,
				0,
				false,
			)
			return err
		})
	require.Error(t, err)

	applied := spansNamed(spans.Ended(), "block.apply")
	require.Len(t, applied, 1)
	require.Equal(t, codes.Error, applied[0].Status().Code)
	require.Equal(
		t,
		"error",
		spanAttributes(applied[0])["block.outcome"].AsString(),
	)
	require.Equal(
		t,
		tracing.BlockTraceID(block.Hash().Bytes()),
		applied[0].SpanContext().TraceID(),
	)
}

// Not t.Parallel: installs a recording OpenTelemetry provider and the block
// tracing configuration, both process globals.
func TestCommitSpanPerSampledBlockInBatch(t *testing.T) {
	blocks := fixtureLedgerBlocks(t)
	spans := testutil.RecordSpans(t)
	testutil.ConfigureBlockTracing(t, tracing.Config{SampleRatio: 0.5})
	want := 0
	for _, b := range blocks {
		if tracing.BlockSampled(b.Hash().Bytes(), nil) {
			want++
		}
	}
	require.NotZero(t, want)
	require.Less(t, want, len(blocks))
	start := time.Unix(1_000, 0)
	end := start.Add(time.Second)

	traceBatchCommit(context.Background(), blocks, start, end)

	committed := spansNamed(spans.Ended(), "block.commit")
	require.Len(t, committed, want)
	for _, span := range committed {
		require.Equal(t, start, span.StartTime())
		require.Equal(t, end, span.EndTime())
		attrs := spanAttributes(span)
		require.Equal(t, int64(len(blocks)), attrs["batch.size"].AsInt64())
		require.Equal(t, "adopted", attrs["block.outcome"].AsString())
	}
}

// Not t.Parallel: installs a recording OpenTelemetry provider and the block
// tracing configuration, both process globals.
func TestRollbackSpanPerRolledBackBlock(t *testing.T) {
	spans := testutil.RecordSpans(t)
	testutil.ConfigureBlockTracing(t, tracing.Config{SampleRatio: 1})
	rolledBack := []models.Block{
		{Hash: lcommon.Blake2b256Hash([]byte("a")).Bytes(), Slot: 12},
		{Hash: lcommon.Blake2b256Hash([]byte("b")).Bytes(), Slot: 11},
	}
	to := ocommon.NewPoint(10, lcommon.Blake2b256Hash([]byte("t")).Bytes())

	traceRollback(context.Background(), to, rolledBack)

	got := spansNamed(spans.Ended(), "block.rolled_back")
	require.Len(t, got, 2)
	for i, span := range got {
		require.Equal(
			t,
			tracing.BlockTraceID(rolledBack[i].Hash),
			span.SpanContext().TraceID(),
		)
		attrs := spanAttributes(span)
		require.Equal(t, int64(2), attrs["rollback.depth"].AsInt64())
		require.Equal(t, int64(10), attrs["rollback.to_slot"].AsInt64())
	}
}

// Not t.Parallel: installs a recording OpenTelemetry provider and the block
// tracing configuration, both process globals.
func TestRollbackOfPoolForcedBlockIsTraced(t *testing.T) {
	spans := testutil.RecordSpans(t)
	pool := [28]byte{9}
	testutil.ConfigureBlockTracing(t, tracing.Config{
		SampleRatio:       0,
		AlwaysSamplePools: [][28]byte{pool},
	})
	hash := lcommon.Blake2b256Hash([]byte("forced")).Bytes()
	_, applied := tracing.StartBlock(
		context.Background(),
		tracing.StageApply,
		"block.apply",
		hash,
		pool[:],
	)
	applied.End()

	traceRollback(
		context.Background(),
		ocommon.NewPoint(1, nil),
		[]models.Block{{Hash: hash, Slot: 2}},
	)

	require.Len(t, spansNamed(spans.Ended(), "block.rolled_back"), 1)
}

// Not t.Parallel: installs a recording OpenTelemetry provider and the block
// tracing configuration, both process globals.
func TestEveryCommittedRollbackTracesItsBlocks(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	removed, err := database.BlockByPoint(
		context.Background(),
		fixture.ls.db,
		fixture.currentTip.Point,
	)
	require.NoError(t, err)
	require.NoError(t, fixture.ls.chain.Rollback(
		context.Background(),
		fixture.ancestorTip.Point,
	))
	spans := testutil.RecordSpans(t)
	testutil.ConfigureBlockTracing(t, tracing.Config{SampleRatio: 1})

	require.NoError(t, fixture.ls.rollbackWithBlocks(
		context.Background(),
		fixture.ancestorTip.Point,
		[]models.Block{removed},
		false,
	))

	got := spansNamed(spans.Ended(), "block.rolled_back")
	require.Len(t, got, 1)
	require.Equal(
		t,
		tracing.BlockTraceID(removed.Hash),
		got[0].SpanContext().TraceID(),
	)
}
