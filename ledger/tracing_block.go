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

package ledger

import (
	"context"
	"encoding/hex"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/tracing"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// traceBatchCommit records a block.commit span for every sampled block of a
// committed batch. Blocks commit together in one database transaction, so
// each span covers the whole batch commit: start to end.
func traceBatchCommit(
	ctx context.Context,
	blocks []ledger.Block,
	start, end time.Time,
) {
	for _, block := range blocks {
		_, span := tracing.StartHeader(
			ctx,
			tracing.StageCommit,
			"block.commit",
			block,
			trace.WithTimestamp(start),
		)
		if span.IsRecording() {
			span.SetAttributes(
				attribute.Int("batch.size", len(blocks)),
				attribute.String("block.outcome", "adopted"),
			)
		}
		span.End(trace.WithTimestamp(end))
	}
}

// traceRollback records a block.rolled_back span in the trace of every
// block a rollback to point removed.
func traceRollback(
	ctx context.Context,
	to ocommon.Point,
	blocks []models.Block,
) {
	for _, block := range blocks {
		_, span := tracing.StartBlock(
			ctx,
			tracing.StageRollback,
			"block.rolled_back",
			block.Hash,
			nil,
		)
		if span.IsRecording() {
			span.SetAttributes(
				attribute.Int("rollback.depth", len(blocks)),
				tracing.Uint64("rollback.to_slot", to.Slot),
				attribute.String(
					"rollback.to_hash",
					hex.EncodeToString(to.Hash),
				),
			)
		}
		span.End()
	}
}

// rollbackTraceBlocks returns the blocks a rollback to point removes, for
// traceRollback. Callers usually pass nil and leave the payload to be read
// from the chain, and that read yields nothing when no event consumer is
// subscribed, so the blocks are read here, before they are deleted, whenever
// tracing is enabled. A failed read only loses the spans.
func (ls *LedgerState) rollbackTraceBlocks(
	ctx context.Context,
	point ocommon.Point,
	provided []models.Block,
) []models.Block {
	if provided != nil || !tracing.Enabled() || ls.db == nil {
		return provided
	}
	var blocks []models.Block
	txn := ls.db.Transaction(ctx, false)
	err := txn.Do(func(txn *database.Txn) error {
		var err error
		blocks, err = database.BlocksAfterSlotTxn(txn, point.Slot)
		return err
	})
	if err != nil {
		ls.config.Logger.Debug(
			"cannot read blocks for rollback trace",
			"component", "ledger",
			"error", err,
		)
		return nil
	}
	return blocks
}

// catchupTrace is the ledger.catchup session span, open from startup until
// the node first reaches the upstream tip. Epoch transitions crossed while
// it is open record under it.
type catchupTrace struct {
	mu   sync.Mutex
	span trace.Span
}

func (ls *LedgerState) beginCatchupTrace(ctx context.Context) {
	ls.catchup.mu.Lock()
	defer ls.catchup.mu.Unlock()
	if ls.catchup.span != nil {
		return
	}
	//nolint:spancheck // endCatchupTrace ends it
	_, ls.catchup.span = tracing.Start(
		context.WithoutCancel(ctx),
		"ledger.catchup",
	)
}

func (ls *LedgerState) endCatchupTrace() {
	ls.catchup.mu.Lock()
	defer ls.catchup.mu.Unlock()
	if ls.catchup.span == nil {
		return
	}
	ls.catchup.span.End()
	ls.catchup.span = nil
}

// catchupContext returns ctx carrying the open catch-up span, or ctx
// unchanged once the node has reached the tip. Cancellation and values still
// come from ctx.
func (ls *LedgerState) catchupContext(ctx context.Context) context.Context {
	ls.catchup.mu.Lock()
	defer ls.catchup.mu.Unlock()
	if ls.catchup.span == nil {
		return ctx
	}
	return trace.ContextWithSpan(ctx, ls.catchup.span)
}
