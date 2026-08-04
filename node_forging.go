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
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/leiosheader"
	"github.com/blinklabs-io/dingo/kesagent"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/dingo/ledger/leader"
	"github.com/blinklabs-io/dingo/ledger/leios"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/gouroboros/consensus"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func (n *Node) validateBlockProducerStartup() (*forging.PoolCredentials, error) {
	if _, err := n.blockProducerShelleyGenesis(); err != nil {
		return nil, err
	}
	if n.ledgerState == nil {
		return nil, errors.New(
			"block producer mode requires ledger state for current slot",
		)
	}
	currentSlot, err := n.ledgerState.CurrentSlot()
	if err != nil {
		if !errors.Is(err, ledger.ErrBeforeGenesis) {
			return nil, fmt.Errorf("compute current slot: %w", err)
		}
		currentSlot = 0
	}
	return n.validateBlockProducerStartupAtSlot(currentSlot)
}

func (n *Node) validateBlockProducerStartupAtSlot(
	currentSlot uint64,
) (*forging.PoolCredentials, error) {
	creds := forging.NewPoolCredentials()
	if n.kesAgentEnabled() {
		// The KES signing key is sourced from an external KES agent, so only
		// the VRF key and operational certificate are loaded from files. The
		// KES verification key is taken from the operational certificate, so
		// opcert validation, the KES-period check, and the ledger cross-check
		// below are unchanged.
		if err := creds.LoadVRFAndOpCert(
			n.config.shelleyVRFKey,
			n.config.shelleyOperationalCertificate,
		); err != nil {
			return nil, fmt.Errorf("load pool credentials: %w", err)
		}
	} else if err := creds.LoadFromFiles(
		n.config.shelleyVRFKey,
		n.config.shelleyKESKey,
		n.config.shelleyOperationalCertificate,
	); err != nil {
		return nil, fmt.Errorf("load pool credentials: %w", err)
	}
	if err := creds.ValidateOpCert(); err != nil {
		return nil, fmt.Errorf("validate operational certificate: %w", err)
	}
	genesis, err := n.blockProducerShelleyGenesis()
	if err != nil {
		return nil, err
	}
	if err := creds.ValidateKESPeriod(genesis, currentSlot); err != nil {
		return nil, fmt.Errorf("validate KES period: %w", err)
	}
	currentPeriod, err := forging.CurrentKESPeriodFromGenesis(
		genesis,
		currentSlot,
	)
	if err != nil {
		return nil, fmt.Errorf("compute current KES period: %w", err)
	}
	opCert := creds.GetOpCert()
	if opCert == nil {
		return nil, errors.New("block producer operational certificate is nil")
	}
	n.config.logger.Info(
		"block producer credentials validated",
		"component", "node",
		"pool_id", creds.GetPoolID().String(),
		"current_slot", currentSlot,
		"current_kes_period", currentPeriod,
		"opcert_kes_period", opCert.KESPeriod,
		"opcert_counter", opCert.IssueNumber,
		"opcert_expiry_period", creds.OpCertExpiryPeriod(),
	)
	return creds, nil
}

func (n *Node) blockProducerShelleyGenesis() (*shelley.ShelleyGenesis, error) {
	// KES-period plausibility requires a Shelley genesis. Block producer
	// mode without one is unsafe — a node with no genesis cannot tell
	// whether the opcert is current — so refuse to start.
	if n.config.cardanoNodeConfig == nil {
		return nil, errors.New(
			"block producer mode requires Cardano node config with Shelley genesis",
		)
	}
	genesis := n.config.cardanoNodeConfig.ShelleyGenesis()
	if genesis == nil {
		return nil, errors.New(
			"block producer mode requires Shelley genesis information",
		)
	}
	return genesis, nil
}

// blockProducerLedgerView adapts ledger.LedgerState to
// forging.LedgerView. The interface lives in the forging package so the
// credential check can stay free of a ledger import; the concrete
// adapter belongs here in package dingo where both types are visible.
type blockProducerLedgerView struct {
	ls *ledger.LedgerState
}

func (v blockProducerLedgerView) PoolRegistrationVRFKeyHash(
	poolID [28]byte,
) ([32]byte, bool, error) {
	return v.ls.PoolRegistrationVRFKeyHash(poolID)
}

func (v blockProducerLedgerView) LatestOpCertSequence(
	poolID [28]byte,
) (uint64, bool, error) {
	return v.ls.LatestOpCertSequence(poolID)
}

// validateBlockProducerLedger runs the ledger-aware cross-check against
// the loaded credentials. Must be called after the ledger has started so
// pool registrations can be queried. A pool that is not yet registered
// is logged as a warning and the node is allowed to continue.
func (n *Node) validateBlockProducerLedger(
	creds *forging.PoolCredentials,
) error {
	view := blockProducerLedgerView{ls: n.ledgerState}
	return n.validateBlockProducerLedgerWithView(creds, view)
}

func (n *Node) validateBlockProducerLedgerWithView(
	creds *forging.PoolCredentials,
	view forging.LedgerView,
) error {
	if creds == nil {
		return errors.New("nil pool credentials")
	}
	registered, vrfMatched, err := creds.ValidateAgainstLedger(view)
	if err != nil {
		if errors.Is(err, forging.ErrVRFKeyHashMismatch) &&
			n.config.network == "devnet" {
			n.config.logger.Warn(
				"devnet block producer VRF cross-check failed; node will continue",
				"component", "node",
				"pool_id", creds.GetPoolID().String(),
				"error", err,
			)
			return nil
		}
		return err
	}
	poolID := creds.GetPoolID().String()
	switch {
	case !registered:
		n.config.logger.Warn(
			"block producer pool not yet registered on chain; node will continue",
			"component", "node",
			"pool_id", poolID,
		)
	case vrfMatched:
		n.config.logger.Info(
			"block producer pool registration verified on chain",
			"component", "node",
			"pool_id", poolID,
		)
	default:
		n.config.logger.Warn(
			"block producer VRF cross-check skipped (seed-only VRF key)",
			"component", "node",
			"pool_id", poolID,
		)
	}
	return nil
}

// handleGenesisSnapshotError returns a fatal error for block producers (which
// require the genesis snapshot for leader election) and logs a warning for
// relay nodes (which do not perform leader election).
func (n *Node) handleGenesisSnapshotError(err error) error {
	return snapshot.HandleGenesisSnapshotError(
		n.config.blockProducer,
		n.config.logger,
		err,
	)
}

// kesAgentEnabled reports whether the KES signing key should be sourced from
// an external bursa KES agent rather than a local --shelley-kes-key file.
func (n *Node) kesAgentEnabled() bool {
	return n.config.shelleyKESAgentSocket != ""
}

// kesAgentMode returns the configured KES agent service mode, defaulting to
// serve-key when a socket is set without an explicit mode.
func (n *Node) kesAgentMode() string {
	switch n.config.shelleyKESAgentMode {
	case kesagent.ModeSign:
		return kesagent.ModeSign
	default:
		return kesagent.ModeServeKey
	}
}

// newKESAgentSigner builds and starts a KES agent client that satisfies
// forging.KESSigner, sourcing the KES signing key from the agent while the
// operational certificate (placed in the block header and used to vet the
// agent's key) still comes from the local opcert file loaded into creds.
func (n *Node) newKESAgentSigner(
	ctx context.Context,
	creds *forging.PoolCredentials,
) (*kesagent.Client, error) {
	opCert := creds.GetOpCert()
	if opCert == nil {
		return nil, errors.New(
			"KES agent mode requires an operational certificate",
		)
	}
	client, err := kesagent.New(kesagent.Config{
		SocketPath: n.config.shelleyKESAgentSocket,
		Mode:       n.kesAgentMode(),
		OpCert:     opCert,
		Logger:     n.config.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("create KES agent client: %w", err)
	}
	client.Start(ctx)
	n.config.logger.Info(
		"KES signing key sourced from agent",
		"component", "node",
		"socket", n.config.shelleyKESAgentSocket,
		"mode", n.kesAgentMode(),
	)
	return client, nil
}

// initBlockForger initializes the block forger for production mode.
// This requires VRF and OpCert key files to be configured, plus either a local
// KES key file or a KES agent socket (--shelley-kes-agent-socket).
func (n *Node) initBlockForger(
	ctx context.Context,
	creds *forging.PoolCredentials,
) error {
	if creds == nil {
		return errors.New("nil pool credentials")
	}
	// Source the KES signing key from an external agent when configured;
	// otherwise the forger and builder default to the file-based credentials.
	var kesSigner forging.KESSigner
	if n.kesAgentEnabled() {
		client, err := n.newKESAgentSigner(ctx, creds)
		if err != nil {
			return err
		}
		n.kesAgentClient = client
		kesSigner = client
	}
	// Create mempool adapter for the forging package.
	mempoolAdapter := &forgingMempoolAdapter{source: n.mempool}

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
		KESSigner:       kesSigner,
		TxValidator:     n.ledgerState,
	})
	if err != nil {
		return fmt.Errorf("failed to create block builder: %w", err)
	}

	// Create block broadcaster (uses the chain manager and event bus)
	broadcaster := &blockBroadcaster{
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

	// Create slot clock adapter for the forger
	slotClock := &slotClockAdapter{ledgerState: n.ledgerState}

	// Wire Leios EB forging when the pipeline manager is available
	// (i.e. Dijkstra era is enabled). Relay nodes and pre-Dijkstra
	// block producers leave these nil and skip EB production.
	var leiosChecker forging.LeiosProduceChecker
	var leiosCerts forging.LeiosCertificateProvider
	var leiosParent forging.LeiosParentAnnouncementProvider
	var leiosEBCaster forging.EndorserBlockBroadcaster
	var leiosMempool forging.MempoolProvider
	if n.leiosPipelineManager != nil && n.ouroboros != nil {
		adapter := &leiosPipelineAdapter{
			mgr:                   n.leiosPipelineManager,
			chain:                 n.chainManager.PrimaryChain(),
			endorserBlockTxHashes: n.ouroboros.EndorserBlockTxHashesByHash,
		}
		leiosChecker = adapter
		leiosCerts = adapter
		leiosParent = adapter
		leiosEBCaster = n.ouroboros
		leiosMempool = mempoolAdapter
	}
	blockForged := n.ledgerState.RecordForgedBlock
	if n.ouroboros != nil {
		blockForged = func(block gledger.Block, blockCbor []byte, latency time.Duration) {
			n.ledgerState.RecordForgedBlock(block, blockCbor, latency)
			if header, ok := block.Header().(*gdijkstra.DijkstraBlockHeader); ok {
				if _, _, announces := header.LeiosAnnouncement(); announces {
					n.ouroboros.EnqueueLeiosBlockAnnouncement(header.Cbor())
				}
			}
		}
	}

	// Wire self-validation when the operator opts in. The validator runs
	// header crypto, body-hash, and per-tx ledger checks before AddBlock.
	var blockValidator forging.BlockValidator
	if n.config.validateForgedBlock {
		blockValidator = &forgedBlockValidatorAdapter{
			ledgerState: n.ledgerState,
		}
	}

	// Create the block forger with the real leader election
	forger, err := forging.NewBlockForger(forging.ForgerConfig{
		Mode:                            forging.ModeProduction,
		Logger:                          n.config.logger,
		Credentials:                     creds,
		KESSigner:                       kesSigner,
		LeaderChecker:                   election,
		BlockBuilder:                    builder,
		BlockBroadcaster:                broadcaster,
		ConfirmedTxs:                    mempoolAdapter,
		BlockForged:                     blockForged,
		SlotClock:                       slotClock,
		ForgeSyncToleranceSlots:         n.config.forgeSyncToleranceSlots,
		ForgeStaleGapThresholdSlots:     n.config.forgeStaleGapThresholdSlots,
		BlockValidator:                  blockValidator,
		PromRegistry:                    n.config.promRegistry,
		LeiosProduceChecker:             leiosChecker,
		LeiosEBBroadcaster:              leiosEBCaster,
		LeiosMempool:                    leiosMempool,
		LeiosCertificateProvider:        leiosCerts,
		LeiosParentAnnouncementProvider: leiosParent,
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

	// Store election for cleanup during shutdown only after the forger is
	// fully created and running.
	n.leaderElection = election
	n.blockForger = forger
	n.config.logger.Info(
		"block forger started in production mode with leader election",
		"pool_id", poolID.String(),
	)

	return nil
}

type mempoolTxView struct {
	Hash string
	Cbor []byte
	Type uint
}

type mempoolTransactionSource interface {
	Transactions() []mempool.MempoolTransaction
	RemoveTxsByHash(hashes []string)
}

func mempoolTransactions(source mempoolTransactionSource) []mempoolTxView {
	txs := source.Transactions()
	result := make([]mempoolTxView, len(txs))
	for i, tx := range txs {
		result[i] = mempoolTxView{
			Hash: tx.Hash,
			Cbor: tx.Cbor,
			Type: tx.Type,
		}
	}
	return result
}

// ledgerMempoolAdapter adapts mempool.Mempool to ledger.MempoolProvider.
type ledgerMempoolAdapter struct {
	source mempoolTransactionSource
}

func (a *ledgerMempoolAdapter) Transactions() []ledger.PendingTransaction {
	txs := mempoolTransactions(a.source)
	result := make([]ledger.PendingTransaction, len(txs))
	for i, tx := range txs {
		result[i] = ledger.PendingTransaction{
			Hash: tx.Hash,
			Cbor: tx.Cbor,
			Type: tx.Type,
		}
	}
	return result
}

func (a *ledgerMempoolAdapter) RemoveTxsByHash(hashes []string) {
	a.source.RemoveTxsByHash(hashes)
}

// forgingMempoolAdapter adapts mempool.Mempool to forging.MempoolProvider.
type forgingMempoolAdapter struct {
	source mempoolTransactionSource
}

func (a *forgingMempoolAdapter) Transactions() []forging.MempoolTransaction {
	txs := mempoolTransactions(a.source)
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

func (a *forgingMempoolAdapter) RemoveTxsByHash(hashes []string) {
	a.source.RemoveTxsByHash(hashes)
}

// blockBroadcaster implements forging.BlockBroadcaster by proposing locally
// forged blocks to the chain component over the EventBus.
type blockBroadcaster struct {
	eventBus *event.EventBus
	logger   *slog.Logger
}

const blockProposalAckTimeout = 30 * time.Second

func (b *blockBroadcaster) AddBlock(
	block gledger.Block,
	_ []byte,
) error {
	if block == nil {
		return errors.New("proposed block is nil")
	}
	if b.eventBus == nil {
		return errors.New("event bus unavailable")
	}
	if !b.eventBus.HasSubscribers(chain.BlockProposedEventType) {
		return errors.New("no chain block proposal subscribers")
	}

	ack := make(chan error, 1)
	b.eventBus.Publish(
		chain.BlockProposedEventType,
		event.NewEvent(
			chain.BlockProposedEventType,
			chain.BlockProposedEvent{
				Block: block,
				Ack:   ack,
			},
		),
	)

	timer := time.NewTimer(blockProposalAckTimeout)
	defer timer.Stop()

	select {
	case err := <-ack:
		if err != nil {
			return fmt.Errorf("chain rejected proposed block: %w", err)
		}
	case <-timer.C:
		return fmt.Errorf(
			"timed out waiting for proposed block ack after %s",
			blockProposalAckTimeout,
		)
	}

	b.logger.Info(
		"block proposal accepted by chain",
		"slot", block.SlotNumber(),
		"hash", block.Hash(),
		"block_number", block.BlockNumber(),
	)

	return nil
}

// stakeDistributionAdapter adapts ledger.LedgerState to leader.StakeDistributionProvider.
// It queries through LedgerView so leader election observes the same stake
// snapshot rotation semantics as other ledger queries.
type stakeDistributionAdapter struct {
	ledgerState *ledger.LedgerState
}

func (a *stakeDistributionAdapter) getStakeDistribution(
	epoch uint64,
) (_ *ledger.StakeDistribution, err error) {
	if a.ledgerState == nil {
		return nil, errors.New("ledger state unavailable")
	}
	db := a.ledgerState.Database()
	if db == nil {
		return nil, errors.New("database unavailable")
	}
	txn := db.MetadataTxn(false)
	if txn == nil {
		return nil, errors.New("metadata transaction unavailable")
	}
	defer func() {
		if rollbackErr := txn.Rollback(); rollbackErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf(
					"release stake distribution transaction: %w",
					rollbackErr,
				),
			)
		}
	}()
	return a.ledgerState.NewView(txn).GetStakeDistribution(epoch)
}

func (a *stakeDistributionAdapter) GetPoolStake(
	epoch uint64,
	poolKeyHash []byte,
) (uint64, error) {
	dist, err := a.getStakeDistribution(epoch)
	poolKey := hex.EncodeToString(poolKeyHash)
	if err != nil {
		return 0, fmt.Errorf(
			"get stake distribution for epoch %d pool %s: %w",
			epoch,
			poolKey,
			err,
		)
	}
	if dist == nil {
		return 0, nil
	}
	return dist.PoolStakes[poolKey], nil
}

func (a *stakeDistributionAdapter) GetTotalActiveStake(
	epoch uint64,
) (uint64, error) {
	dist, err := a.getStakeDistribution(epoch)
	if err != nil {
		return 0, fmt.Errorf(
			"get stake distribution for epoch %d: %w",
			epoch,
			err,
		)
	}
	if dist == nil {
		return 0, nil
	}
	return dist.TotalStake, nil
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

func (a *epochInfoAdapter) EpochSlotRange(
	epoch uint64,
) (leader.EpochSlotRange, error) {
	info, err := a.ledgerState.EpochInfo(epoch)
	if err != nil {
		if !errors.Is(err, hardfork.ErrPastHorizon) {
			return leader.EpochSlotRange{}, err
		}
		// The nonce-stability cutoff can precede the HFC header horizon. Once
		// the next epoch's nonce is stable, leader election still needs that
		// immediate Praos epoch's slot range to precompute its schedule. All
		// Praos eras use the Shelley genesis epoch/slot dimensions.
		readyEpoch, ready := a.ledgerState.NextEpochNonceReadyEpoch()
		currentEpoch := a.ledgerState.CurrentEpoch()
		if !ready ||
			readyEpoch != epoch ||
			currentEpoch == ^uint64(0) ||
			epoch != currentEpoch+1 {
			return leader.EpochSlotRange{}, err
		}
		currentInfo, currentErr := a.ledgerState.EpochInfo(
			currentEpoch,
		)
		if currentErr != nil {
			return leader.EpochSlotRange{}, currentErr
		}
		slotCount := uint64(currentInfo.LengthInSlots)
		if slotCount == 0 ||
			currentInfo.StartSlot > ^uint64(0)-slotCount {
			return leader.EpochSlotRange{}, fmt.Errorf(
				"current epoch slot range is invalid: start=%d count=%d",
				currentInfo.StartSlot,
				slotCount,
			)
		}
		return leader.EpochSlotRange{
			StartSlot: currentInfo.StartSlot + slotCount,
			SlotCount: slotCount,
		}, nil
	}
	return leader.EpochSlotRange{
		StartSlot: info.StartSlot,
		SlotCount: uint64(info.LengthInSlots),
	}, nil
}

func (a *epochInfoAdapter) EpochForSlot(slot uint64) (uint64, error) {
	epoch, err := a.ledgerState.SlotToEpoch(slot)
	if err != nil {
		return 0, err
	}
	return epoch.EpochId, nil
}

func (a *epochInfoAdapter) ActiveSlotCoeff() float64 {
	return a.ledgerState.ActiveSlotCoeff()
}

// ActiveSlotCoeffRat satisfies leader.ActiveSlotCoeffRatProvider so the leader
// schedule derives its threshold from the exact Shelley genesis rational, the
// same value LedgerState.verifyLeaderEligibility uses, instead of a float64
// approximation of it.
func (a *epochInfoAdapter) ActiveSlotCoeffRat() *big.Rat {
	return a.ledgerState.ActiveSlotCoeffRat()
}

func (a *epochInfoAdapter) ConsensusModeForEpoch(epoch uint64) consensus.ConsensusMode {
	return a.ledgerState.ConsensusModeForEpoch(epoch)
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

// leiosPipelineAdapter adapts leios.PipelineManager and the primary chain to
// the narrow Leios interfaces the forge loop expects.
type leiosPipelineAdapter struct {
	mgr                   *leios.PipelineManager
	chain                 leiosParentChain
	endorserBlockTxHashes func([]byte) ([]string, bool)
}

type leiosParentChain interface {
	Tip() ochainsync.Tip
	BlockByPoint(ocommon.Point, *database.Txn) (models.Block, error)
}

func (a *leiosPipelineAdapter) MayProduceEndorserBlock(
	slot uint64,
) (bool, string, error) {
	dec, err := a.mgr.MayProduceEndorserBlock(slot)
	if err != nil {
		return false, "", err
	}
	return dec.Allowed, dec.Reason, nil
}

func (a *leiosPipelineAdapter) EligibleCertifiedEndorserBlocks() []forging.LeiosCertifiedEndorserBlock {
	eligible := a.mgr.EligibleCertifiedEbs()
	out := make([]forging.LeiosCertifiedEndorserBlock, 0, len(eligible))
	for _, eb := range eligible {
		out = append(out, forging.LeiosCertifiedEndorserBlock{
			SlotNo:            eb.SlotNo,
			EndorserBlockHash: eb.EndorserBlockHash,
			Certificate:       eb.Certificate,
			AnnouncingRbHash:  eb.AnnouncingRbHash,
		})
	}
	return out
}

func (a *leiosPipelineAdapter) CertifiedEndorserBlockTxHashes(
	ebHash lcommon.Blake2b256,
) ([]string, bool) {
	if a.endorserBlockTxHashes == nil {
		return nil, false
	}
	return a.endorserBlockTxHashes(ebHash.Bytes())
}

func (a *leiosPipelineAdapter) MarkEndorserBlockEmbedded(
	ebHash lcommon.Blake2b256,
) {
	a.mgr.MarkEmbedded(ebHash)
}

func (a *leiosPipelineAdapter) ParentLeiosAnnouncement() (
	lcommon.Blake2b256,
	lcommon.Blake2b256,
	bool,
	error,
) {
	if a.chain == nil {
		return lcommon.Blake2b256{}, lcommon.Blake2b256{}, false, errors.New("chain unavailable")
	}
	tip := a.chain.Tip()
	if len(tip.Point.Hash) == 0 {
		return lcommon.Blake2b256{}, lcommon.Blake2b256{}, false, nil
	}
	block, err := a.chain.BlockByPoint(tip.Point, nil)
	if err != nil {
		return lcommon.Blake2b256{}, lcommon.Blake2b256{}, false, fmt.Errorf(
			"resolve parent block: %w",
			err,
		)
	}
	decoded, err := block.Decode()
	if err != nil {
		return lcommon.Blake2b256{}, lcommon.Blake2b256{}, false, fmt.Errorf(
			"decode parent block: %w",
			err,
		)
	}
	hash, _, ok := leiosheader.ReferencedEndorserBlock(decoded.Header())
	rbHash := lcommon.NewBlake2b256(tip.Point.Hash)
	return rbHash, hash, ok, nil
}

// forgedBlockValidatorAdapter adapts ledger.LedgerState to
// forging.BlockValidator so the forger can self-validate blocks before
// adoption without importing the ledger package from within forging.
type forgedBlockValidatorAdapter struct {
	ledgerState *ledger.LedgerState
}

func (a *forgedBlockValidatorAdapter) ValidateForgedBlock(
	block gledger.Block,
	blockCbor []byte,
) error {
	return a.ledgerState.ValidateForgedBlock(block, blockCbor)
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
