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

// Package blockpromote runs the background scheduler that moves block bodies
// older than the security parameter from the badger VolatileDB into the
// ImmutableDB chunk files.
package blockpromote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
)

// TipAdvancedEventType is published after a promotion step moves the
// immutable tip.
const TipAdvancedEventType event.EventType = "database.immutable_tip_advanced"

// TipAdvancedEvent is the payload of TipAdvancedEventType.
type TipAdvancedEvent struct {
	TipHash  []byte
	TipSlot  uint64
	TipIndex uint64
	Promoted int
}

// CeilingFromTip returns the highest block index that may be promoted given
// the index of the ledger's applied tip: security parameter blocks behind it.
// The bool is false while the applied chain is not yet deeper than k.
func CeilingFromTip(appliedTipIndex, securityParam uint64) (uint64, bool) {
	if securityParam == 0 || appliedTipIndex <= securityParam {
		return 0, false
	}
	return appliedTipIndex - securityParam, true
}

// DefaultReclaimInterval is the least time between two space reclaims. A
// reclaim flushes the blob store's memtable and compacts it, so it is
// batched rather than run after every promoted block.
const DefaultReclaimInterval = 5 * time.Minute

// Config configures a Promoter.
type Config struct {
	DB       *database.Database
	EventBus *event.EventBus
	Logger   *slog.Logger
	// Ceiling returns the highest block index that may be promoted. It is
	// taken from the ledger's applied tip rather than the chain tip, so a
	// chain that has run ahead of the ledger never pulls blocks the ledger
	// can still roll back into the immutable range. The bool is false when
	// nothing is promotable yet.
	Ceiling   func(context.Context) (uint64, bool, error)
	Frequency time.Duration
	// ReclaimInterval is the least time between space reclaims; zero uses
	// DefaultReclaimInterval.
	ReclaimInterval time.Duration
	// Reclaim compacts the blob store after promotion; nil uses
	// DB.ReclaimBlobSpace. Promotion replaces each body with a marker, and
	// the replaced bodies stay in the store until a compaction covers both
	// versions.
	Reclaim func() error
	// CheckSpace, when set, runs before each step that promotes through
	// ceiling and refuses the step by returning an error, which leaves every
	// store as it was; the next run asks again.
	CheckSpace func(ctx context.Context, ceiling uint64) error
	// Now reports the current time; nil uses time.Now.
	Now func() time.Time
}

// Promoter periodically promotes blocks through the ceiling.
type Promoter struct {
	cfg    Config
	logger *slog.Logger
	wg     sync.WaitGroup
	cancel context.CancelFunc
	// lookingUp is set while a step resolves its ceiling, which writes
	// nothing; the rest of the step writes the stores. It is set only under
	// phaseMu after checking that ctx has not ended, so once Stop has
	// cancelled ctx no new lookup can begin unseen.
	phaseMu   sync.Mutex
	lookingUp bool

	// reclaimPending is set when blocks were promoted since the last
	// successful reclaim. Both fields belong to the goroutine calling
	// RunOnce.
	reclaimPending bool
	lastReclaim    time.Time
}

// New builds a Promoter.
func New(cfg Config) *Promoter {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return &Promoter{cfg: cfg, logger: logger}
}

// RunOnce promotes through the current ceiling and publishes an event when the
// immutable tip moved. It reports how many blocks were promoted.
func (p *Promoter) RunOnce(ctx context.Context) (int, error) {
	p.phaseMu.Lock()
	if err := ctx.Err(); err != nil {
		p.phaseMu.Unlock()
		return 0, err
	}
	p.lookingUp = true
	p.phaseMu.Unlock()
	ceiling, ok, err := p.cfg.Ceiling(ctx)
	p.phaseMu.Lock()
	p.lookingUp = false
	p.phaseMu.Unlock()
	if err != nil {
		return 0, fmt.Errorf("block promotion ceiling: %w", err)
	}
	// A lookup that returns after Stop gave up on it must not start writing
	// to stores the caller is closing.
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if !ok {
		p.reclaimIfDue(ctx)
		return 0, nil
	}
	if p.cfg.CheckSpace != nil {
		if err := p.cfg.CheckSpace(ctx, ceiling); err != nil {
			return 0, fmt.Errorf("block promotion not started: %w", err)
		}
	}
	res, err := p.cfg.DB.PromoteThrough(ctx, ceiling)
	if res.Promoted > 0 {
		p.reclaimPending = true
	}
	p.reclaimIfDue(ctx)
	if res.Promoted > 0 && p.cfg.EventBus != nil {
		p.cfg.EventBus.Publish(
			TipAdvancedEventType,
			event.NewEvent(TipAdvancedEventType, TipAdvancedEvent{
				TipHash:  res.Tip.TipHash,
				TipSlot:  res.Tip.TipSlot,
				TipIndex: res.Tip.TipIndex,
				Promoted: res.Promoted,
			}),
		)
	}
	if err != nil {
		return res.Promoted, fmt.Errorf("promote blocks: %w", err)
	}
	return res.Promoted, nil
}

// reclaimIfDue compacts the blob store once promotion has left replaced
// bodies behind and the reclaim interval has passed. It is called on every
// run, so the bodies of the last promoted blocks are reclaimed even when no
// further block is promoted. A failure is logged and retried on a later run,
// as is a reclaim skipped because ctx ended.
func (p *Promoter) reclaimIfDue(ctx context.Context) {
	if !p.reclaimPending || ctx.Err() != nil {
		return
	}
	now := time.Now
	if p.cfg.Now != nil {
		now = p.cfg.Now
	}
	interval := p.cfg.ReclaimInterval
	if interval <= 0 {
		interval = DefaultReclaimInterval
	}
	if t := now(); !p.lastReclaim.IsZero() && t.Sub(p.lastReclaim) < interval {
		return
	}
	reclaim := p.cfg.Reclaim
	if reclaim == nil {
		reclaim = p.cfg.DB.ReclaimBlobSpace
	}
	p.lastReclaim = now()
	if err := reclaim(); err != nil {
		p.logger.Error("blob space reclaim failed", "error", err)
		return
	}
	p.reclaimPending = false
}

// Start launches the promotion loop.
func (p *Promoter) Start(ctx context.Context) error {
	if p.cfg.Frequency <= 0 {
		return fmt.Errorf(
			"block promotion: invalid frequency %d (must be > 0)",
			p.cfg.Frequency,
		)
	}
	if p.cfg.DB == nil || p.cfg.Ceiling == nil {
		return errors.New("block promotion: database and ceiling are required")
	}
	// Repair a torn promotion before the first scheduled step.
	if err := p.cfg.DB.ReconcileImmutable(); err != nil {
		return fmt.Errorf("block promotion: %w", err)
	}
	ctx, p.cancel = context.WithCancel(ctx) //nolint:gosec
	p.wg.Go(func() {
		ticker := time.NewTicker(p.cfg.Frequency)
		defer ticker.Stop()
		for {
			if _, err := p.RunOnce(ctx); err != nil && ctx.Err() == nil {
				p.logger.Error("block promotion failed", "error", err)
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	})
	return nil
}

// Stop cancels the loop and waits for it to finish. When ctx ends first, it
// still waits for a step that is writing the blob store and chunk files, since
// the caller closes them next; that write ends with the current batch. A step
// still resolving its ceiling has written nothing and never will once ctx is
// cancelled, so Stop returns ctx's error instead of waiting for it.
func (p *Promoter) Stop(ctx context.Context) error {
	if p.cancel != nil {
		p.cancel()
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.wg.Wait()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
	}
	p.phaseMu.Lock()
	lookingUp := p.lookingUp
	p.phaseMu.Unlock()
	if lookingUp {
		return fmt.Errorf(
			"promotion ceiling lookup still running: %w", ctx.Err(),
		)
	}
	// Not in a lookup, and none can start under a cancelled ctx: the loop
	// is writing or returning, and a write ends with the current batch.
	p.logger.Warn(
		"waiting for the running promotion write past the stop deadline",
		"error", ctx.Err(),
	)
	<-done
	return nil
}
