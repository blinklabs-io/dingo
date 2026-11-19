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
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

const leiosPrebuildLead = 3 * time.Second

type preparedLeiosEndorserBlock struct {
	slot                          uint64
	parent                        []byte
	caps                          leiosEBCaps
	raw, hash                     []byte
	bodies                        [][]byte
	receipt                       LeiosValidationReceipt
	current                       func(context.Context, []byte, uint64) bool
	candidates                    int
	prebuilt                      bool
	truncated                     bool
	selectDuration, buildDuration time.Duration
}

func (p *preparedLeiosEndorserBlock) isCurrent(
	ctx context.Context,
	parent []byte,
	slot uint64,
) (valid bool) {
	defer func() {
		if recover() != nil {
			valid = false
		}
	}()
	return p.current != nil && p.current(ctx, parent, slot)
}

type leiosPrebuildRequest struct {
	id                          uint64
	slot                        uint64
	parent                      []byte
	caps                        leiosEBCaps
	selectionDeadline, deadline time.Time
}

func (r leiosPrebuildRequest) sameCandidate(other leiosPrebuildRequest) bool {
	return r.slot == other.slot && r.caps == other.caps &&
		bytes.Equal(r.parent, other.parent)
}

// leiosPrebuildWorker has one executing request, one latest pending request,
// and one immutable completed candidate. Replacements cancel the old request;
// the single worker joins it before starting the replacement.
type leiosPrebuildWorker struct {
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	requests  chan leiosPrebuildRequest
	build     func(context.Context, leiosPrebuildRequest) (*preparedLeiosEndorserBlock, error)
	observe   func(string, time.Duration, error)
	mu        sync.Mutex
	sequence  uint64
	requested *leiosPrebuildRequest
	cancelJob context.CancelFunc
	result    *preparedLeiosEndorserBlock
}

func newLeiosPrebuildWorker(
	ctx context.Context,
	build func(context.Context, leiosPrebuildRequest) (*preparedLeiosEndorserBlock, error),
	observe func(string, time.Duration, error),
) *leiosPrebuildWorker {
	ctx, cancel := context.WithCancel(ctx)
	worker := &leiosPrebuildWorker{
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
		requests: make(chan leiosPrebuildRequest, 1),
		build:    build,
		observe:  observe,
	}
	go worker.run(ctx)
	return worker
}

func (p *leiosPrebuildWorker) request(request leiosPrebuildRequest) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ctx.Err() != nil {
		return
	}
	if p.result != nil && p.result.slot == request.slot &&
		p.result.caps == request.caps &&
		bytes.Equal(p.result.parent, request.parent) &&
		p.result.isCurrent(p.ctx, request.parent, request.slot) {
		return
	}
	if p.requested != nil && p.requested.sameCandidate(request) {
		return
	}
	if p.cancelJob != nil {
		p.cancelJob()
	}
	p.sequence++
	request.id = p.sequence
	request.parent = bytes.Clone(request.parent)
	p.requested = &request
	p.result = nil
	select {
	case <-p.requests:
	default:
	}
	p.requests <- request
}

func (p *leiosPrebuildWorker) run(parentCtx context.Context) {
	defer close(p.done)
	for {
		select {
		case <-parentCtx.Done():
			return
		case request := <-p.requests:
			p.mu.Lock()
			if p.requested == nil || p.requested.id != request.id ||
				parentCtx.Err() != nil {
				p.mu.Unlock()
				continue
			}
			ctx, cancel := context.WithDeadline(parentCtx, request.deadline)
			p.cancelJob = cancel
			p.mu.Unlock()
			started := time.Now()
			result, err := p.buildSafely(ctx, request)
			contextErr := ctx.Err()
			cancel()
			outcome := "cancelled"
			p.mu.Lock()
			p.cancelJob = nil
			if p.requested != nil && p.requested.id == request.id {
				p.requested = nil
				switch {
				case parentCtx.Err() != nil || contextErr != nil:
				case err != nil:
					outcome = "failed"
				case result == nil:
					outcome = "empty"
				case result.isCurrent(parentCtx, request.parent, request.slot):
					p.result = result
					outcome = "ready"
				}
			}
			p.mu.Unlock()
			if p.observe != nil {
				p.observe(outcome, time.Since(started), err)
			}
		}
	}
}

func (p *leiosPrebuildWorker) buildSafely(
	ctx context.Context,
	request leiosPrebuildRequest,
) (result *preparedLeiosEndorserBlock, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("leios prebuild callback panic: %v", recovered)
		}
	}()
	return p.build(ctx, request)
}

func (p *leiosPrebuildWorker) expire(slot uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.requested != nil && p.requested.slot <= slot {
		if p.cancelJob != nil {
			p.cancelJob()
		}
		p.requested = nil
	}
	if p.result != nil && p.result.slot < slot {
		p.result = nil
	}
}

func (p *leiosPrebuildWorker) take(
	ctx context.Context,
	slot uint64,
	parent []byte,
	caps leiosEBCaps,
) (*preparedLeiosEndorserBlock, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := p.result
	if result == nil || result.slot != slot {
		return nil, "miss"
	}
	p.result = nil
	if !bytes.Equal(result.parent, parent) {
		return nil, "parent_changed"
	}
	if result.caps != caps {
		return nil, "capacity_changed"
	}
	if !result.isCurrent(ctx, parent, slot) {
		return nil, "stale"
	}
	return result, "hit"
}

func (p *leiosPrebuildWorker) stop() {
	p.cancel()
	p.mu.Lock()
	if p.cancelJob != nil {
		p.cancelJob()
	}
	p.result = nil
	p.requested = nil
	p.mu.Unlock()
	<-p.done
}

// LeiosPreparationChecker checks future committee eligibility for unsigned
// work. It does not open the produce window; MayProduceEndorserBlock must
// still authorize publication at the actual leader slot.
type LeiosPreparationChecker interface {
	MayPrepareEndorserBlock(uint64) (bool, string, error)
}

type leiosPrebuildSlotTimes interface {
	SlotToTime(uint64) (time.Time, error)
}

func (f *BlockForger) scheduleLeiosPrebuild(currentSlot uint64) {
	defer func() {
		if recovered := recover(); recovered != nil {
			f.reportForgeCallbackPanic("leios_prebuild_schedule", recovered)
		}
	}()
	if f.leiosPrebuild == nil || f.slotClock == nil || f.leaderChecker == nil ||
		currentSlot == math.MaxUint64 {
		return
	}
	clock, ok := f.slotClock.(leiosPrebuildSlotTimes)
	if !ok {
		return
	}
	target, ok := f.nextLeaderSlotSafe(currentSlot + 1)
	if !ok || target <= currentSlot || target == math.MaxUint64 {
		return
	}
	start, err := clock.SlotToTime(target)
	if err != nil {
		return
	}
	now := time.Now()
	if !now.Before(start) || start.Sub(now) > leiosPrebuildLead {
		return
	}
	end, err := clock.SlotToTime(target + 1)
	if err != nil || !end.After(start) {
		return
	}
	duration := end.Sub(start)
	planner, ok := f.leiosChecker.(LeiosPreparationChecker)
	if !ok {
		return
	}
	allowed, _, err := planner.MayPrepareEndorserBlock(target)
	if err != nil || !allowed {
		return
	}
	parent := f.slotClock.PrimaryChainTip()
	applied := f.slotClock.ChainTip()
	if len(parent.Hash) == 0 || parent.Slot != applied.Slot ||
		!bytes.Equal(parent.Hash, applied.Hash) {
		return
	}
	caps, ok := f.leiosEBCapsForSlot(target)
	if !ok || !caps.aggregateBounded {
		return
	}
	after, err := f.slotClock.CurrentSlot()
	if err != nil || after != currentSlot {
		return
	}
	reserve := min(defaultForgeEBSelectionReserve, duration/4)
	deadline := start.Add(-min(50*time.Millisecond, duration/10))
	selectionDeadline := start.Add(-reserve)
	if !time.Now().Before(selectionDeadline) {
		return
	}
	f.leiosPrebuild.request(
		leiosPrebuildRequest{
			slot:              target,
			parent:            parent.Hash,
			caps:              caps,
			selectionDeadline: selectionDeadline,
			deadline:          deadline,
		},
	)
}

func (f *BlockForger) prepareUpcomingLeiosEndorserBlock(
	ctx context.Context,
	request leiosPrebuildRequest,
) (*preparedLeiosEndorserBlock, error) {
	var receipt LeiosValidationReceipt
	candidate, err := f.prepareLeiosEndorserBlock(
		ctx,
		request.slot,
		leiosSelectionBasis{
			parentHash: request.parent,
			slot:       request.slot,
			receipt:    &receipt,
		},
		request.caps,
		request.selectionDeadline,
	)
	if err != nil || candidate == nil {
		return nil, err
	}
	prepared, ok := receipt.(LeiosPreparationReceipt)
	if !ok {
		return nil, errors.New("ledger did not issue a prebuild receipt")
	}
	candidate.current = prepared.StillValidForParent
	candidate.prebuilt = true
	return candidate, nil
}

func (f *BlockForger) observeLeiosPrebuild(
	outcome string,
	duration time.Duration,
	err error,
) {
	if f.metrics != nil {
		f.metrics.leiosPrebuildTotal.WithLabelValues(outcome).Inc()
		f.metrics.leiosPrebuildSeconds.Observe(duration.Seconds())
	}
	if err != nil && outcome == "failed" {
		f.logger.Debug("leios prebuild unavailable", "error", err)
	}
}

// The authoritative selector already decoded and measured these exact bytes.
// Assemble references directly so construction cannot re-read mutable mempool
// buffers or repeat the wire conversion after validation.
func buildPreparedLeiosEB(
	bodies [][]byte,
	caps leiosEBCaps,
) ([]byte, []byte, [][]byte, error) {
	capacity := leiosEBCapacity{caps: caps}
	refs := make([]lcommon.LeiosTransactionReference, 0, len(bodies))
	for _, raw := range bodies {
		if !validLeiosTransactionSize(len(raw)) ||
			!capacity.admit(uint64(len(raw))) {
			return nil, nil, nil, errors.New(
				"prepared endorser block exceeds manifest capacity",
			)
		}
		size := uint32(
			len(raw),
		) // #nosec G115 -- validLeiosTransactionSize bounds the conversion.
		refs = append(
			refs,
			lcommon.LeiosTransactionReference{
				TransactionHash: lcommon.Blake2b256Hash(raw),
				TransactionSize: size,
			},
		)
	}
	if len(refs) == 0 {
		return nil, nil, nil, errNoValidTxRefs
	}
	eb := lcommon.LeiosEndorserBlock{TransactionReferences: refs}
	raw, err := eb.MarshalCBOR()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal prepared leios EB: %w", err)
	}
	hash := lcommon.Blake2b256Hash(raw)
	return raw, hash.Bytes(), bodies, nil
}
