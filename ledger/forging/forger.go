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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/vrf"
)

// Mode represents the forging mode.
type Mode int

const (
	// ModeDev is a simplified mode where the node produces all blocks on a
	// fixed interval without real VRF/KES. Used for single-node devnets.
	ModeDev Mode = iota

	// ModeProduction uses real VRF leader election and KES signing.
	// Requires loaded pool credentials.
	ModeProduction
)

// BlockForger coordinates block production for a stake pool.
type BlockForger struct {
	mode   Mode
	logger *slog.Logger

	// Production mode components
	creds            *PoolCredentials
	leaderChecker    LeaderChecker
	blockBuilder     BlockBuilder
	blockBroadcaster BlockBroadcaster
	slotClock        SlotClockProvider
	slotDuration     time.Duration

	// State
	mu      sync.RWMutex
	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// LeaderChecker determines if the pool should produce a block for a given slot.
type LeaderChecker interface {
	// ShouldProduceBlock returns true if this pool is the leader for the slot.
	ShouldProduceBlock(slot uint64) bool
	// NextLeaderSlot returns the next slot where this pool is leader.
	NextLeaderSlot(fromSlot uint64) (uint64, bool)
}

// BlockBuilder constructs blocks from mempool transactions.
type BlockBuilder interface {
	// BuildBlock creates a new block for the given slot.
	// Returns the block and its CBOR encoding.
	BuildBlock(slot uint64, kesPeriod uint64) (ledger.Block, []byte, error)
}

// BlockBroadcaster submits built blocks to the chain.
type BlockBroadcaster interface {
	// AddBlock adds a block to the local chain and propagates to peers.
	AddBlock(block ledger.Block, cbor []byte) error
}

// SlotClockProvider provides current slot information from the slot clock.
type SlotClockProvider interface {
	// CurrentSlot returns the current slot number based on wall-clock time.
	CurrentSlot() (uint64, error)
	// SlotsPerKESPeriod returns the number of slots in a KES period.
	SlotsPerKESPeriod() uint64
}

// ForgerConfig holds configuration for the block forger.
type ForgerConfig struct {
	Mode         Mode
	Logger       *slog.Logger
	SlotDuration time.Duration

	// Production mode configuration
	Credentials      *PoolCredentials
	LeaderChecker    LeaderChecker
	BlockBuilder     BlockBuilder
	BlockBroadcaster BlockBroadcaster
	SlotClock        SlotClockProvider
}

// NewBlockForger creates a new block forger.
func NewBlockForger(cfg ForgerConfig) (*BlockForger, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	if cfg.SlotDuration == 0 {
		cfg.SlotDuration = time.Second // Default 1 second slots
	}

	f := &BlockForger{
		mode:             cfg.Mode,
		logger:           cfg.Logger,
		slotDuration:     cfg.SlotDuration,
		creds:            cfg.Credentials,
		leaderChecker:    cfg.LeaderChecker,
		blockBuilder:     cfg.BlockBuilder,
		blockBroadcaster: cfg.BlockBroadcaster,
		slotClock:        cfg.SlotClock,
	}

	if cfg.Mode == ModeProduction {
		if cfg.Credentials == nil || !cfg.Credentials.IsLoaded() {
			return nil, errors.New("production mode requires loaded credentials")
		}
		if cfg.LeaderChecker == nil {
			return nil, errors.New("production mode requires leader checker")
		}
		if cfg.BlockBuilder == nil {
			return nil, errors.New("production mode requires block builder")
		}
		if cfg.BlockBroadcaster == nil {
			return nil, errors.New("production mode requires block broadcaster")
		}
		if cfg.SlotClock == nil {
			return nil, errors.New("production mode requires slot clock")
		}
	}

	return f, nil
}

// Start begins the block forging process.
// The provided context controls the forger's lifecycle.
func (f *BlockForger) Start(ctx context.Context) error {
	f.mu.Lock()
	if f.running {
		f.mu.Unlock()
		return errors.New("forger already running")
	}
	f.running = true

	ctx, cancel := context.WithCancel(ctx)
	f.cancel = cancel
	f.wg.Add(1)
	f.mu.Unlock()

	f.logger.Info("block forger started", "mode", f.modeString())

	go f.runLoop(ctx)
	return nil
}

// Stop stops the block forging process.
// It blocks until the runLoop goroutine has exited.
func (f *BlockForger) Stop() {
	f.mu.Lock()
	if !f.running {
		f.mu.Unlock()
		return
	}

	f.running = false
	if f.cancel != nil {
		f.cancel()
	}
	f.mu.Unlock()

	// Wait for the goroutine to finish before returning
	f.wg.Wait()
	f.logger.Info("block forger stopped")
}

// IsRunning returns true if the forger is currently running.
func (f *BlockForger) IsRunning() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.running
}

// runLoop is the main forging loop.
func (f *BlockForger) runLoop(ctx context.Context) {
	defer f.wg.Done()
	defer func() {
		f.mu.Lock()
		f.running = false
		f.mu.Unlock()
	}()

	ticker := time.NewTicker(f.slotDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := f.checkAndForge(ctx); err != nil {
				f.logger.Error("forge check failed", "error", err)
			}
		}
	}
}

// checkAndForge checks if we should forge a block and does so if appropriate.
func (f *BlockForger) checkAndForge(ctx context.Context) error {
	switch f.mode {
	case ModeDev:
		// Dev mode: forge on every tick (handled elsewhere in existing code)
		return nil
	case ModeProduction:
		return f.checkAndForgeProduction(ctx)
	default:
		return fmt.Errorf("unknown forging mode: %d", f.mode)
	}
}

// checkAndForgeProduction implements production mode forging.
func (f *BlockForger) checkAndForgeProduction(_ context.Context) error {
	// Get current slot from slot clock
	currentSlot, err := f.slotClock.CurrentSlot()
	if err != nil {
		return fmt.Errorf("failed to get current slot: %w", err)
	}

	// Check if we're the leader for this slot
	if !f.leaderChecker.ShouldProduceBlock(currentSlot) {
		return nil
	}

	f.logger.Info("producing block", "slot", currentSlot)

	// Calculate KES period for this slot
	// KES period = slot / slots_per_kes_period
	slotsPerKESPeriod := f.slotClock.SlotsPerKESPeriod()
	if slotsPerKESPeriod == 0 {
		return errors.New("slots per KES period is zero")
	}
	kesPeriod := currentSlot / slotsPerKESPeriod

	// Ensure KES key is at correct period
	if err := f.creds.UpdateKESPeriod(kesPeriod); err != nil {
		return fmt.Errorf("failed to update KES period: %w", err)
	}

	// Build the block
	block, blockCbor, err := f.blockBuilder.BuildBlock(currentSlot, kesPeriod)
	if err != nil {
		return fmt.Errorf("failed to build block: %w", err)
	}

	// Add block to chain and broadcast
	if err := f.blockBroadcaster.AddBlock(block, blockCbor); err != nil {
		return fmt.Errorf("failed to add block: %w", err)
	}

	f.logger.Info("block produced successfully", "slot", currentSlot)
	return nil
}

// VRFProofForSlot generates a VRF proof for leader election at the given slot.
// Returns (proof, output, error).
func (f *BlockForger) VRFProofForSlot(
	slot uint64,
	epochNonce []byte,
) ([]byte, []byte, error) {
	if f.mode == ModeDev {
		// Dev mode: return dummy proof
		return make([]byte, vrf.ProofSize), make([]byte, vrf.OutputSize), nil
	}

	if f.creds == nil || !f.creds.IsLoaded() {
		return nil, nil, errors.New("credentials not loaded")
	}

	// Validate slot fits in int64 before conversion
	if slot > math.MaxInt64 {
		return nil, nil, fmt.Errorf("slot %d exceeds int64 max", slot)
	}

	// Create VRF input: MkInputVrf(slot, epochNonce)
	alpha := vrf.MkInputVrf(int64(slot), epochNonce) // #nosec G115 -- validated above

	return f.creds.VRFProve(alpha)
}

// SignBlockHeader signs a block header with KES.
func (f *BlockForger) SignBlockHeader(
	kesPeriod uint64,
	headerBytes []byte,
) ([]byte, error) {
	if f.mode == ModeDev {
		// Dev mode: return dummy signature
		return make([]byte, 448), nil // KES signature size for depth 6
	}

	if f.creds == nil || !f.creds.IsLoaded() {
		return nil, errors.New("credentials not loaded")
	}

	return f.creds.KESSign(kesPeriod, headerBytes)
}

// modeString returns a string representation of the forging mode.
func (f *BlockForger) modeString() string {
	switch f.mode {
	case ModeDev:
		return "dev"
	case ModeProduction:
		return "production"
	default:
		return "unknown"
	}
}
