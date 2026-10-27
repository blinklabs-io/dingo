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

package nodeparity

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// eraHistoryRetryInterval bounds how often epochOracle re-asks Dingo for its
// era history after a fetch failed or did not cover the slot it was asked
// for. Every block in between resolves through the per-point fallback, so
// retrying on each of them would add a dial on top of the fallback's own.
const eraHistoryRetryInterval = 30 * time.Second

// eraSpan is one era's slot range from Dingo's era history. Slots in
// [startSlot, endSlot) -- or from startSlot onward when unbounded -- fall in
// epoch startEpoch + (slot-startSlot)/epochSize.
type eraSpan struct {
	startSlot  uint64
	startEpoch uint64
	endSlot    uint64
	unbounded  bool
	epochSize  uint64
}

// eraSpansFromHistory converts a HardForkEraHistoryQuery answer into
// eraSpans, rejecting any span epochOf could not divide by or would
// misplace.
func eraSpansFromHistory(
	history []localstatequery.EraHistoryResult,
) ([]eraSpan, error) {
	spans := make([]eraSpan, 0, len(history))
	for i, h := range history {
		if h.Begin.SlotNo < 0 || h.Begin.EpochNo < 0 ||
			h.End.SlotNo < 0 || h.Params.EpochLength <= 0 {
			return nil, fmt.Errorf(
				"era history span %d: invalid bounds or epoch length", i,
			)
		}
		span := eraSpan{
			startSlot:  uint64(h.Begin.SlotNo),
			startEpoch: uint64(h.Begin.EpochNo),
			endSlot:    uint64(h.End.SlotNo),
			unbounded:  h.End.Unbounded,
			epochSize:  uint64(h.Params.EpochLength),
		}
		if !span.unbounded && span.endSlot < span.startSlot {
			return nil, fmt.Errorf(
				"era history span %d ends at slot %d before it begins at %d",
				i, span.endSlot, span.startSlot,
			)
		}
		spans = append(spans, span)
	}
	return spans, nil
}

// epochOf resolves slot's epoch from spans, reporting false when no span
// covers it.
func epochOf(spans []eraSpan, slot uint64) (uint64, bool) {
	for _, s := range spans {
		if slot < s.startSlot || (!s.unbounded && slot >= s.endSlot) {
			continue
		}
		return s.startEpoch + (slot-s.startSlot)/s.epochSize, true
	}
	return 0, false
}

// epochOracle resolves a slot's epoch locally from Dingo's era history,
// fetching that history only when the cached copy does not cover the slot.
// It is not safe for concurrent use: RunFromGenesis calls it only from its
// ChainSync callbacks, which run one at a time.
type epochOracle struct {
	fetch      func(ctx context.Context) ([]eraSpan, error)
	now        func() time.Time
	spans      []eraSpan
	retryAfter time.Time
}

func newEpochOracle(
	fetch func(ctx context.Context) ([]eraSpan, error),
) *epochOracle {
	return &epochOracle{fetch: fetch, now: time.Now}
}

var errSlotNotInEraHistory = errors.New(
	"slot is not covered by the era history",
)

// epoch returns slot's epoch. On a cache miss it refreshes the era history
// once, unless a refresh failed less than eraHistoryRetryInterval ago. A
// non-nil error means the caller must resolve the epoch some other way.
func (o *epochOracle) epoch(ctx context.Context, slot uint64) (uint64, error) {
	if epoch, ok := epochOf(o.spans, slot); ok {
		return epoch, nil
	}
	if o.now().Before(o.retryAfter) {
		return 0, errSlotNotInEraHistory
	}
	spans, err := o.fetch(ctx)
	if err == nil {
		o.spans = spans
		if epoch, ok := epochOf(spans, slot); ok {
			return epoch, nil
		}
		err = errSlotNotInEraHistory
	}
	o.retryAfter = o.now().Add(eraHistoryRetryInterval)
	return 0, err
}

// fetchEraSpans asks Dingo for its era history on a connection Acquired at
// the volatile tip. A specific point is never acquired: that is what Dingo
// refuses as too old, and the whole history is answered from the tip anyway.
func fetchEraSpans(
	ctx context.Context, dingoAddr string, magic uint32,
) ([]eraSpan, error) {
	conn, err := Dial(ctx, dingoAddr, magic)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close() //nolint:errcheck
	lsq := conn.LocalStateQuery()
	if lsq == nil || lsq.Client == nil {
		return nil, errAcquireWithoutLocalStateQuery
	}
	if err := lsq.Client.AcquireVolatileTip(); err != nil {
		return nil, fmt.Errorf("acquire volatile tip: %w", err)
	}
	history, err := lsq.Client.GetEraHistory()
	if err != nil {
		return nil, fmt.Errorf("GetEraHistory: %w", err)
	}
	_ = lsq.Client.Release() //nolint:errcheck
	return eraSpansFromHistory(history)
}
