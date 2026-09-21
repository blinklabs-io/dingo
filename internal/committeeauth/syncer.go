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

// Package committeeauth periodically resolves the chain's live
// rollback-safe immutable point and pushes it into the metadata store's
// committee hot-key authorization pruner.
//
// The pruner (database/plugin/metadata/sqlstore/committee_prune.go) bounds
// how far back it may delete superseded authorizations by a fixed slot-count
// assumption, which under-covers a sparse chain: Ouroboros bounds a legal
// rollback in blocks (securityParam), not slots, so the true bound is the
// slot of the block securityParam blocks behind the tip. sqlstore cannot
// compute that itself -- it would have to import chain, which already
// imports database -- so this package resolves it from the live Chain and
// LedgerState the node already holds, and hands it to the store from
// outside. See that file's package comment for the full retention rule and
// the correctness argument, and issue #4353 for the bug this closes.
package committeeauth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// DefaultFrequency is used when SyncerConfig.Frequency is unset. The value
// only needs to lag the true immutable point by an amount that is safe to
// retain extra rows for in the meantime, so it does not need to track block
// production closely; a live value is always at least as safe as the
// slot-window fallback it improves on.
const DefaultFrequency = 5 * time.Minute

// SyncerConfig contains the dependencies the syncer needs. All fields are
// required except Logger and Frequency.
type SyncerConfig struct {
	// PointAtDepth resolves the point a given number of blocks behind the
	// chain tip. Chain.PointAtDepth satisfies this.
	PointAtDepth func(depth uint64) (point ocommon.Point, found bool, err error)

	// SecurityParam returns the security parameter for the current era, or a
	// non-positive value when it is not yet known. LedgerState.SecurityParam
	// satisfies this.
	SecurityParam func() int

	// SetImmutableSlot receives the resolved immutable slot, or known=false
	// when no live value could be resolved this cycle.
	// Database.SetCommitteeAuthImmutableSlot satisfies this.
	SetImmutableSlot func(slot uint64, known bool)

	Logger    *slog.Logger
	Frequency time.Duration
}

// Syncer runs SyncerConfig's dependencies on a timer, refreshing the
// committee authorization pruner's live immutable-slot bound.
type Syncer struct {
	config SyncerConfig
	logger *slog.Logger

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func NewSyncer(cfg SyncerConfig) *Syncer {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if cfg.Frequency <= 0 {
		cfg.Frequency = DefaultFrequency
	}
	return &Syncer{
		config: cfg,
		logger: cfg.Logger,
	}
}

// sync resolves the current immutable point and pushes it in. Any failure to
// resolve a fresh value pushes known=false rather than leaving a stale value
// in place: a stale slot from an earlier, since-superseded chain state is not
// distinguishable from a fresh one once stored, so this must not guess.
func (s *Syncer) sync() {
	k := s.config.SecurityParam()
	if k <= 0 {
		s.config.SetImmutableSlot(0, false)
		return
	}
	point, found, err := s.config.PointAtDepth(uint64(k)) //nolint:gosec
	if err != nil {
		s.logger.Warn(
			"committee auth immutable slot sync: failed to resolve immutable point",
			"error",
			err,
		)
		s.config.SetImmutableSlot(0, false)
		return
	}
	if !found {
		// Fewer than k blocks on chain yet; nothing is immutable.
		s.config.SetImmutableSlot(0, false)
		return
	}
	s.config.SetImmutableSlot(point.Slot, true)
}

func (s *Syncer) run(ctx context.Context) {
	ticker := time.NewTicker(s.config.Frequency)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.sync()
		case <-ctx.Done():
			return
		}
	}
}

func (s *Syncer) Start(ctx context.Context) error {
	if s.config.PointAtDepth == nil ||
		s.config.SecurityParam == nil ||
		s.config.SetImmutableSlot == nil {
		return errors.New(
			"committee auth immutable slot sync: PointAtDepth, SecurityParam, and SetImmutableSlot are required",
		)
	}

	ctx, s.cancel = context.WithCancel(ctx) //nolint:gosec

	// Resolve once immediately so pruning has a live bound from the start
	// rather than waiting a full tick.
	s.sync()

	s.wg.Go(func() {
		s.run(ctx)
	})
	return nil
}

func (s *Syncer) Stop(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.wg.Wait()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf(
			"committee auth immutable slot sync: failed to stop before context cancellation: %w",
			ctx.Err(),
		)
	}
}
