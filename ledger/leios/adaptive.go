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

package leios

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/big"
	"sync"
	"sync/atomic"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// AdaptiveEbConfig is how the endorser-block capacity follows missed quorums.
// The values are not range-checked, as the reference does not: a Decrease
// outside (0, 1) or a MinScale outside (0, 1] is the operator's to answer for.
type AdaptiveEbConfig struct {
	// Decrease is the factor a missed quorum scales the capacity by.
	Decrease float64
	// Increase is the share of the protocol's capacity a quorum reached
	// adds back.
	Increase float64
	// MinScale is the share of the protocol's capacity the scale never
	// falls below.
	MinScale float64
}

// DefaultAdaptiveEbConfig returns the reference producer's defaults
// (defaultLeiosAdaptiveEbConfig in ouroboros-consensus
// LeiosAdaptiveEndorsements.hs).
func DefaultAdaptiveEbConfig() AdaptiveEbConfig {
	return AdaptiveEbConfig{Decrease: 0.8, Increase: 0.02, MinScale: 0.1}
}

// StepAdaptiveEb moves the share of the protocol's endorser-block capacity to
// forge with for one announcement: a miss scales it down, down to MinScale,
// and a quorum reached adds back, up to the whole capacity. It is float64
// arithmetic throughout, as the reference's is Double, so the scale follows
// the same values.
func StepAdaptiveEb(
	cfg AdaptiveEbConfig,
	missed bool,
	scale float64,
) float64 {
	if missed {
		return math.Max(cfg.MinScale, scale*cfg.Decrease)
	}
	return math.Min(1, scale+cfg.Increase)
}

// AnnouncementVotes is what the votes collected for one announcement amount
// to.
type AnnouncementVotes struct {
	// VerifiedStake is the stake of the voters whose signature was
	// verified, which is what a certificate is built from.
	VerifiedStake uint64
	// TotalActiveStake is the quorum denominator.
	TotalActiveStake uint64
	// Threshold is the quorum stake threshold, a share of TotalActiveStake.
	Threshold *big.Rat
}

// AnnouncementVoteSource reports the votes collected so far for an
// announcement, and an error when the committee or the threshold they are
// measured against is unknown.
type AnnouncementVoteSource interface {
	AnnouncementVotes(
		slot uint64,
		rbHash lcommon.Blake2b256,
		ebHash lcommon.Blake2b256,
	) (AnnouncementVotes, error)
}

// adaptiveBlock is what the controller keeps of a block of the chain.
type adaptiveBlock struct {
	hash     lcommon.Blake2b256
	prevHash lcommon.Blake2b256
	slot     uint64
	// announced is the endorser block the block announces, if any.
	announced *lcommon.Blake2b256
}

// AdaptiveEbControllerConfig configures an AdaptiveEbController.
type AdaptiveEbControllerConfig struct {
	Logger   *slog.Logger
	EventBus *event.EventBus
	Config   AdaptiveEbConfig
	Votes    AnnouncementVoteSource
	// CertificationGap returns the slots that must pass between a block
	// announcing an endorser block and one certifying it. An error means
	// the gap is unknown, and then no announcement is certifiable.
	CertificationGap func(announcingSlot uint64) (uint64, error)
	// CaughtUp reports whether the node follows the tip of the chain, the
	// Genesis state machine's caught-up state. A node that syncs past old
	// announcements has none of their votes.
	CaughtUp func() bool
	// ChainTip returns the tip of the chain the node has selected. Only a
	// block that is still the tip when its update is handled is sampled, so
	// updates queued while the node synced never pass the caught-up gate.
	ChainTip func() ocommon.Point
	// inspect reads a block off a chain update. Tests replace it.
	inspect func(chain.ChainBlockEvent) (adaptiveBlock, bool)
}

// AdaptiveEbController follows the chain the node adopts and, for each block
// that extends one which announced an endorser block, takes whether that
// announcement collected its quorum and moves the share of the protocol's
// endorser-block capacity this node forges with. Every announcement counts,
// whoever made it: a quorum missed anywhere on the chain says the committee
// is not keeping up with endorser blocks of that size.
//
// The committee and the quorum threshold come from the epoch of the
// announcement. The reference reads them from the ledger at the tip, which
// differs only when the block that samples the announcement crosses an epoch
// boundary that changed the parameters.
//
// The scale starts at one on every start, is kept in memory only, is not
// reset at an epoch boundary and is not rewound by a rollback. This follows
// the reference producer, which holds the scale in a variable of the running
// node (runAdaptiveEndorsements in ouroboros-consensus
// LeiosAdaptiveEndorsements.hs).
type AdaptiveEbController struct {
	logger  *slog.Logger
	bus     *event.EventBus
	cfg     AdaptiveEbConfig
	votes   AnnouncementVoteSource
	gap     func(uint64) (uint64, error)
	caught  func() bool
	tip     func() ocommon.Point
	inspect func(chain.ChainBlockEvent) (adaptiveBlock, bool)

	// scale holds the float64 bits of the current scale.
	scale atomic.Uint64

	// last is the most recent block of the chain seen. Only the event loop
	// touches it.
	last *adaptiveBlock

	mu       sync.Mutex
	running  bool
	stopping bool
	cancel   context.CancelFunc
	// done is closed when the event loop of the current run has returned.
	// Every Stop waits on it, so none returns while the loop still queries.
	done  chan struct{}
	subID event.EventSubscriberId
}

// NewAdaptiveEbController creates a controller.
func NewAdaptiveEbController(
	cfg AdaptiveEbControllerConfig,
) (*AdaptiveEbController, error) {
	if cfg.EventBus == nil {
		return nil, errors.New("leios adaptive endorsements: nil event bus")
	}
	if cfg.Votes == nil {
		return nil, errors.New("leios adaptive endorsements: nil vote source")
	}
	if cfg.CertificationGap == nil {
		return nil, errors.New(
			"leios adaptive endorsements: nil certification gap",
		)
	}
	if cfg.CaughtUp == nil {
		return nil, errors.New("leios adaptive endorsements: nil caught-up")
	}
	if cfg.ChainTip == nil {
		return nil, errors.New("leios adaptive endorsements: nil chain tip")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	c := &AdaptiveEbController{
		logger:  logger.With("component", "leios-adaptive-endorsements"),
		bus:     cfg.EventBus,
		cfg:     cfg.Config,
		votes:   cfg.Votes,
		gap:     cfg.CertificationGap,
		caught:  cfg.CaughtUp,
		tip:     cfg.ChainTip,
		inspect: cfg.inspect,
	}
	if c.inspect == nil {
		c.inspect = inspectChainBlock
	}
	c.scale.Store(math.Float64bits(1))
	return c, nil
}

// Scale returns the share of the protocol's endorser-block capacity to forge
// with, in [MinScale, 1].
func (c *AdaptiveEbController) Scale() float64 {
	return math.Float64frombits(c.scale.Load())
}

// Start subscribes to chain updates. Every start begins with the whole
// capacity, as a restart of the reference node does.
func (c *AdaptiveEbController) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		return nil
	}
	if c.stopping {
		return errors.New(
			"leios adaptive endorsements: Stop in progress, cannot Start",
		)
	}
	if ctx == nil {
		return errors.New("leios adaptive endorsements: nil context")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf(
			"leios adaptive endorsements: parent context already done: %w",
			err,
		)
	}
	childCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.running = true
	c.last = nil
	c.scale.Store(math.Float64bits(1))

	subID, ch := c.bus.Subscribe(chain.ChainUpdateEventType)
	c.subID = subID
	done := make(chan struct{})
	c.done = done
	go func() {
		defer close(done)
		closed := c.followChain(childCtx, ch)
		// A loop that ended without Stop leaves nothing following the chain:
		// release the subscription and the running state so Start can be
		// called again, and forge with the whole capacity until it is.
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.stopping || !c.running {
			return
		}
		if closed {
			c.logger.Warn(
				"leios adaptive endorsements stopped: " +
					"chain update subscription closed",
			)
		}
		c.running = false
		if c.cancel != nil {
			c.cancel()
			c.cancel = nil
		}
		c.bus.Unsubscribe(chain.ChainUpdateEventType, c.subID)
		c.scale.Store(math.Float64bits(1))
	}()
	c.logger.Info(
		"leios adaptive endorsements started",
		"decrease", c.cfg.Decrease,
		"increase", c.cfg.Increase,
		"min_scale", c.cfg.MinScale,
	)
	return nil
}

// Stop unsubscribes and waits for the event loop to finish. A Stop that
// finds another in progress waits for the same loop.
func (c *AdaptiveEbController) Stop() error {
	c.mu.Lock()
	done := c.done
	if !c.running {
		c.mu.Unlock()
		if done != nil {
			<-done
		}
		return nil
	}
	c.stopping = true
	c.running = false
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	c.bus.Unsubscribe(chain.ChainUpdateEventType, c.subID)
	c.mu.Unlock()

	<-done

	c.mu.Lock()
	c.stopping = false
	c.mu.Unlock()
	return nil
}

// followChain handles chain updates until the context is done or the bus
// closes the subscription, which it reports.
func (c *AdaptiveEbController) followChain(
	ctx context.Context,
	ch <-chan event.Event,
) (closed bool) {
	for {
		select {
		case <-ctx.Done():
			return false
		case evt, ok := <-ch:
			if !ok {
				return true
			}
			c.handleChainUpdate(evt)
		}
	}
}

// handleChainUpdate follows the blocks the chain adopts. A rollback needs no
// handling of its own: the block that follows one extends the rollback point,
// not the block seen last, so it samples nothing, and the scale is not
// rewound.
func (c *AdaptiveEbController) handleChainUpdate(evt event.Event) {
	if data, ok := evt.Data.(chain.ChainBlockEvent); ok {
		c.handleBlock(data)
	}
}

func (c *AdaptiveEbController) handleBlock(evt chain.ChainBlockEvent) {
	block, ok := c.inspect(evt)
	if !ok {
		c.last = nil
		return
	}
	prev := c.last
	c.last = &block
	if prev == nil || prev.announced == nil || block.prevHash != prev.hash {
		return
	}
	if !samePoint(evt.Point, c.tip()) || !c.caught() {
		return
	}
	c.sample(*prev, block.slot)
}

// sample takes the votes the announcement of announcer collected by the time
// the block at slot arrived. A quorum reached counts towards recovery even
// before the announcement could be certified; a miss counts only once it
// could, because a block that comes sooner has cut its votes short.
func (c *AdaptiveEbController) sample(announcer adaptiveBlock, slot uint64) {
	ebHash := *announcer.announced
	votes, err := c.votes.AnnouncementVotes(
		announcer.slot, announcer.hash, ebHash,
	)
	if err != nil {
		c.logger.Debug(
			"leios announcement outcome unknown",
			"slot", announcer.slot,
			"error", err,
		)
		return
	}
	reached, err := MeetsStakeQuorum(
		votes.VerifiedStake,
		votes.TotalActiveStake,
		votes.Threshold,
	)
	if err != nil {
		c.logger.Debug(
			"leios announcement outcome unknown",
			"slot", announcer.slot,
			"error", err,
		)
		return
	}
	gap, gapErr := c.gap(announcer.slot)
	certifiable := gapErr == nil && slot >= announcer.slot &&
		slot-announcer.slot >= gap
	scale := c.Scale()
	next := scale
	switch {
	case reached:
		next = StepAdaptiveEb(c.cfg, false, scale)
	case certifiable:
		next = StepAdaptiveEb(c.cfg, true, scale)
	}
	c.scale.Store(math.Float64bits(next))
	participation := 0.0
	if votes.TotalActiveStake > 0 {
		participation = float64(votes.VerifiedStake) /
			float64(votes.TotalActiveStake)
	}
	log := c.logger.Debug
	if next != scale {
		log = c.logger.Info
	}
	log(
		"leios adaptive endorsements",
		"slot", announcer.slot,
		"eb_hash", ebHash.String(),
		"participation", participation,
		"missed_quorum", !reached,
		"certifiable", certifiable,
		"eb_capacity_scale", next,
	)
}

func samePoint(a, b ocommon.Point) bool {
	return a.Slot == b.Slot && bytes.Equal(a.Hash, b.Hash)
}

// inspectChainBlock reads the identity of a block and the endorser block its
// header announces, if any.
func inspectChainBlock(evt chain.ChainBlockEvent) (adaptiveBlock, bool) {
	block, err := evt.Block.Decode()
	if err != nil {
		return adaptiveBlock{}, false
	}
	return adaptiveBlockOf(block), true
}

func adaptiveBlockOf(block lcommon.Block) adaptiveBlock {
	header := block.Header()
	out := adaptiveBlock{
		hash:     lcommon.NewBlake2b256(header.Hash().Bytes()),
		prevHash: lcommon.NewBlake2b256(header.PrevHash().Bytes()),
		slot:     header.SlotNumber(),
	}
	if announcer, ok := header.(interface {
		LeiosAnnouncement() (lcommon.Blake2b256, uint64, bool)
	}); ok {
		if ebHash, _, ok := announcer.LeiosAnnouncement(); ok {
			out.announced = &ebHash
		}
	}
	return out
}
