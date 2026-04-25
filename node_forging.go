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

package dingo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/leader"
	"github.com/blinklabs-io/dingo/mempool"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

func (n *Node) validateBlockProducerStartup() (*forging.PoolCredentials, error) {
	creds := forging.NewPoolCredentials()
	if err := creds.LoadFromFiles(
		n.config.shelleyVRFKey,
		n.config.shelleyKESKey,
		n.config.shelleyOperationalCertificate,
	); err != nil {
		return nil, fmt.Errorf("load pool credentials: %w", err)
	}
	if err := creds.ValidateOpCert(); err != nil {
		return nil, fmt.Errorf("validate operational certificate: %w", err)
	}
	n.config.logger.Info(
		"block producer credentials validated",
		"pool_id", creds.GetPoolID().String(),
		"opcert_expiry_period", creds.OpCertExpiryPeriod(),
	)
	return creds, nil
}

// initBlockForger initializes the block forger for production mode.
// This requires VRF, KES, and OpCert key files to be configured.
func (n *Node) initBlockForger(
	ctx context.Context,
	creds *forging.PoolCredentials,
) error {
	if creds == nil {
		return errors.New("nil pool credentials")
	}
	// Create mempool adapter for the forging package
	mempoolAdapter := &mempoolAdapter{mempool: n.mempool}

	// Create epoch nonce adapter for the builder
	epochNonceAdapter := &epochNonceAdapter{ledgerState: n.ledgerState}

	// Create block builder
	builder, err := forging.NewDefaultBlockBuilder(forging.BlockBuilderConfig{
		Logger:          n.config.logger,
		Mempool:         mempoolAdapter,
		PParamsProvider: n.ledgerState,
		ChainTip:        n.chainManager.PrimaryChain(),
		EpochNonce:      epochNonceAdapter,
		Credentials:     creds,
		TxValidator:     n.ledgerState,
	})
	if err != nil {
		return fmt.Errorf("failed to create block builder: %w", err)
	}

	// Create block broadcaster (uses the chain manager and event bus)
	broadcaster := &blockBroadcaster{
		chain:    n.chainManager.PrimaryChain(),
		eventBus: n.eventBus,
		logger:   n.config.logger,
	}

	// Create the leader election component
	// Convert pool ID from PoolId to PoolKeyHash (both are [28]byte)
	poolID := creds.GetPoolID()
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], poolID[:])

	// Create adapters for the providers that leader.Election needs
	stakeProvider := &stakeDistributionAdapter{ledgerState: n.ledgerState}
	epochProvider := &epochInfoAdapter{ledgerState: n.ledgerState}

	// Get VRF secret key from credentials
	vrfSKey := creds.GetVRFSKey()

	// Create leader election with real stake distribution
	election := leader.NewElection(
		poolKeyHash,
		vrfSKey,
		stakeProvider,
		epochProvider,
		n.eventBus,
		n.config.logger,
	)
	election.SetPromRegistry(n.config.promRegistry)
	if n.db != nil {
		if scheduleStore := leader.NewSyncStateScheduleStore(
			n.db.Metadata(),
		); scheduleStore != nil {
			election.SetScheduleStore(scheduleStore)
		}
	}

	// Start leader election (subscribes to epoch transitions)
	if err := election.Start(ctx); err != nil {
		return fmt.Errorf("failed to start leader election: %w", err)
	}

	// Store election for cleanup during shutdown
	n.leaderElection = election

	// Create slot clock adapter for the forger
	slotClock := &slotClockAdapter{ledgerState: n.ledgerState}

	// Create the block forger with the real leader election
	forger, err := forging.NewBlockForger(forging.ForgerConfig{
		Mode:                        forging.ModeProduction,
		Logger:                      n.config.logger,
		Credentials:                 creds,
		LeaderChecker:               election,
		BlockBuilder:                builder,
		BlockBroadcaster:            broadcaster,
		SlotClock:                   slotClock,
		ForgeSyncToleranceSlots:     n.config.forgeSyncToleranceSlots,
		ForgeStaleGapThresholdSlots: n.config.forgeStaleGapThresholdSlots,
		PromRegistry:                n.config.promRegistry,
	})
	if err != nil {
		// Stop election to prevent goroutine leak
		_ = election.Stop()
		return fmt.Errorf("failed to create block forger: %w", err)
	}

	// Start the forger with the passed context
	if err := forger.Start(ctx); err != nil {
		// Stop election to prevent goroutine leak
		_ = election.Stop()
		return fmt.Errorf("failed to start block forger: %w", err)
	}

	n.blockForger = forger
	n.config.logger.Info(
		"block forger started in production mode with leader election",
		"pool_id", poolID.String(),
	)

	return nil
}

// mempoolAdapter adapts the mempool.Mempool to forging.MempoolProvider.
type mempoolAdapter struct {
	mempool *mempool.Mempool
}

func (a *mempoolAdapter) Transactions() []forging.MempoolTransaction {
	txs := a.mempool.Transactions()
	result := make([]forging.MempoolTransaction, len(txs))
	for i, tx := range txs {
		result[i] = forging.MempoolTransaction{
			Hash: tx.Hash,
			Cbor: tx.Cbor,
			Type: tx.Type,
		}
	}
	return result
}

// blockBroadcaster implements forging.BlockBroadcaster using the chain manager.
type blockBroadcaster struct {
	chain    *chain.Chain
	eventBus *event.EventBus
	logger   *slog.Logger
}

func (b *blockBroadcaster) AddBlock(
	block gledger.Block,
	_ []byte,
) error {
	// Add block to the chain (CBOR is stored internally by the block).
	// addBlockInternal notifies iterators after storing the block.
	if err := b.chain.AddBlock(block, nil); err != nil {
		return fmt.Errorf("failed to add block to chain: %w", err)
	}

	b.logger.Info(
		"block added to chain",
		"slot", block.SlotNumber(),
		"hash", block.Hash(),
		"block_number", block.BlockNumber(),
	)

	return nil
}

// stakeDistributionAdapter adapts ledger.LedgerState to leader.StakeDistributionProvider.
// It queries the metadata store directly with a nil transaction so the SQLite
// read pool is used, avoiding contention with block-processing write locks.
type stakeDistributionAdapter struct {
	ledgerState *ledger.LedgerState
}

func (a *stakeDistributionAdapter) GetPoolStake(
	epoch uint64,
	poolKeyHash []byte,
) (uint64, error) {
	snapshot, err := a.ledgerState.Database().Metadata().GetPoolStakeSnapshot(
		epoch, "mark", poolKeyHash, nil,
	)
	if err != nil {
		return 0, err
	}
	if snapshot == nil {
		return 0, nil
	}
	return uint64(snapshot.TotalStake), nil
}

func (a *stakeDistributionAdapter) GetTotalActiveStake(
	epoch uint64,
) (uint64, error) {
	return a.ledgerState.Database().Metadata().GetTotalActiveStake(
		epoch, "mark", nil,
	)
}

// epochInfoAdapter adapts ledger.LedgerState to leader.EpochInfoProvider.
type epochInfoAdapter struct {
	ledgerState *ledger.LedgerState
}

func (a *epochInfoAdapter) CurrentEpoch() uint64 {
	return a.ledgerState.CurrentEpoch()
}

func (a *epochInfoAdapter) EpochNonce(epoch uint64) []byte {
	return a.ledgerState.EpochNonce(epoch)
}

func (a *epochInfoAdapter) NextEpochNonceReadyEpoch() (uint64, bool) {
	return a.ledgerState.NextEpochNonceReadyEpoch()
}

func (a *epochInfoAdapter) SlotsPerEpoch() uint64 {
	return a.ledgerState.SlotsPerEpoch()
}

func (a *epochInfoAdapter) ActiveSlotCoeff() float64 {
	return a.ledgerState.ActiveSlotCoeff()
}

// slotClockAdapter adapts ledger.LedgerState to forging.SlotClockProvider.
type slotClockAdapter struct {
	ledgerState *ledger.LedgerState
}

func (a *slotClockAdapter) CurrentSlot() (uint64, error) {
	return a.ledgerState.CurrentSlot()
}

func (a *slotClockAdapter) SlotsPerKESPeriod() uint64 {
	return a.ledgerState.SlotsPerKESPeriod()
}

func (a *slotClockAdapter) ChainTipSlot() uint64 {
	return a.ledgerState.ChainTipSlot()
}

func (a *slotClockAdapter) NextSlotTime() (time.Time, error) {
	return a.ledgerState.NextSlotTime()
}

func (a *slotClockAdapter) UpstreamTipSlot() uint64 {
	return a.ledgerState.UpstreamTipSlot()
}

// epochNonceAdapter adapts ledger.LedgerState to forging.EpochNonceProvider.
type epochNonceAdapter struct {
	ledgerState *ledger.LedgerState
}

func (a *epochNonceAdapter) CurrentEpoch() uint64 {
	return a.ledgerState.CurrentEpoch()
}

func (a *epochNonceAdapter) EpochForSlot(slot uint64) (uint64, error) {
	epoch, err := a.ledgerState.SlotToEpoch(slot)
	if err != nil {
		return 0, err
	}
	return epoch.EpochId, nil
}

func (a *epochNonceAdapter) EpochNonce(epoch uint64) []byte {
	return a.ledgerState.EpochNonce(epoch)
}
