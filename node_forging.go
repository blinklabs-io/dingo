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
	"io"
	"log/slog"
	"math/big"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	nodeconsensus "github.com/blinklabs-io/dingo/consensus"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/leiosheader"
	"github.com/blinklabs-io/dingo/kesagent"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/dingo/ledger/leader"
	"github.com/blinklabs-io/dingo/ledger/leios"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/consensus"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
)

func (n *Node) validateBlockProducerStartup() (*forging.PoolCredentials, error) {
	slot, supported, err := n.blockProducerStartupClock()
	if err != nil {
		return nil, err
	}
	return n.validateBlockProducerStartupForClock(slot, supported)
}

// blockProducerStartupClock resolves the wall-clock slot to judge the
// operational certificate against, and whether the confirmed era history
// supports it; see validateBlockProducerStartupForClock.
func (n *Node) blockProducerStartupClock() (uint64, bool, error) {
	if _, err := n.blockProducerShelleyGenesis(); err != nil {
		return 0, false, err
	}
	if n.ledgerState == nil {
		return 0, false, errors.New(
			"block producer mode requires ledger state for current slot",
		)
	}
	if _, err := n.ledgerState.CurrentSlot(); err != nil {
		if !errors.Is(err, ledger.ErrBeforeGenesis) {
			return 0, false, fmt.Errorf("compute current slot: %w", err)
		}
		// Clock before genesis: preserve the historical hard fail through the
		// strict preflight (an opcert can never be "current" before genesis).
		return 0, true, nil
	}
	// The current wall-clock slot only reliably places an operational
	// certificate in time when the confirmed era history spans the wall clock.
	// A node importing from genesis or restarting far behind has confirmed
	// history that stops at its newest applied era: CurrentSlot then
	// extrapolates through that era's slot length — a fresh mainnet node
	// judges the wall clock with Byron's 20s slots even though the real chain
	// moved to one-second slots at epoch 208 — which cannot judge an opcert for
	// the actual chain.
	supportedSlot, supported, err := n.ledgerState.
		WallClockSlotFromConfirmedHistory()
	if err != nil {
		return 0, false, fmt.Errorf(
			"wall-clock slot from confirmed history: %w",
			err,
		)
	}
	return supportedSlot, supported, nil
}

func (n *Node) setEquivocationSelfPoolID(creds *forging.PoolCredentials) {
	if creds != nil {
		n.equivocation.setSelfPoolID(creds.GetPoolID().String())
	}
}

// agentInstallSlot returns the slot an agent key install is validated
// against: the node's current slot, or fallbackSlot when no ledger state is
// wired or the slot clock cannot answer. Production block-producer startup
// always has ledger state -- validateBlockProducerStartup requires it before
// the agent path can run -- so the fallback exists for the same reason
// blockProducerContext's does: a test driving
// validateBlockProducerStartupAtSlot against a bare &Node{config} validates
// against the slot it passed in.
func (n *Node) agentInstallSlot(fallbackSlot uint64) uint64 {
	if n.ledgerState == nil {
		return fallbackSlot
	}
	currentSlot, err := n.ledgerState.CurrentSlot()
	if err != nil {
		return fallbackSlot
	}
	return currentSlot
}

// kesAgentSignClient is a sign-mode KES agent client, local or remote.
type kesAgentSignClient interface {
	forging.RemoteKESSigner
	io.Closer
}

func (n *Node) kesAgentClientMetrics() *kesagent.Metrics {
	if n.kesAgentMetrics == nil && n.config.promRegistry != nil {
		reg := n.config.promRegistry
		if n.producerPoolCount() > 1 {
			// The other pools register the same series with their pool_id,
			// so the first pool's must carry its own or the names collide.
			poolID, err := poolIDFromOpCertFile(
				n.primaryKeySource().keys.opCert,
			)
			if err != nil {
				return nil
			}
			reg = n.poolMetricsRegistry(poolID)
		}
		n.kesAgentMetrics = kesagent.NewMetrics(reg)
	}
	return n.kesAgentMetrics
}

// agentMaterialFromPushedKey adapts a validated kesagent.PushedKey to
// ledger/forging's own AgentKESMaterial shape, keeping ledger/forging free of
// a dependency on the kesagent package.
func agentMaterialFromPushedKey(
	pk kesagent.PushedKey,
) forging.AgentKESMaterial {
	return forging.AgentKESMaterial{
		AbsolutePeriod: pk.AbsolutePeriod,
		KESSKeyData:    pk.KESSKeyData,
		KESVKey:        pk.KESVKey,
		OpCert: forging.OpCert{
			KESVKey:     pk.OpCert.KESVKey,
			IssueNumber: pk.OpCert.IssueNumber,
			KESPeriod:   pk.OpCert.KESPeriod,
			Signature:   pk.OpCert.ColdSig,
			ColdVKey:    pk.OpCert.ColdVKey,
		},
	}
}

// blockProducerContext returns n.ctx, or context.Background() when it is
// nil. Production nodes always have n.ctx set by Run() before block-producer
// startup runs; the fallback exists only so a test that exercises
// validateBlockProducerStartupAtSlot directly against a bare &Node{config}
// (the established pattern in node_forging_test.go) does not have to
// construct a full node lifecycle just to dial a KES agent.
func (n *Node) blockProducerContext() context.Context {
	if n.ctx != nil {
		return n.ctx
	}
	return context.Background()
}

// ReloadBlockProducerCredentials re-reads the VRF, KES and operational
// certificate files, validates them exactly as startup does, and swaps them
// into the running block producer. Any failure leaves the loaded credentials
// in place and forging uninterrupted. With several pools each reloads through
// its own key source, and a pool that fails does not stop the others. It is
// refused while startup, shutdown, or a live restore or truncate is in
// progress.
//
// In KES agent sign mode it re-reads the VRF key and the operational
// certificate and moves signing to a client for the new certificate; in
// serve-key mode it is refused, since the agent pushes each new key itself.
func (n *Node) ReloadBlockProducerCredentials() error {
	// The reload reads n.ledgerState and the live credentials, both of which
	// those operations replace, from its own goroutine. Holding the gates for
	// the whole reload is acceptable where a probe's would not be: it runs
	// once per operator-initiated rotation, not on every probe interval.
	return n.reloadCredentials(reloadTriggerSignal, nil)
}

// reloadBlockProducerCredentials is ReloadBlockProducerCredentials with the
// clock and the ledger cross-check supplied, for the first pool.
func (n *Node) reloadBlockProducerCredentials(
	slot uint64,
	supported bool,
	ledgerCheck func(*forging.PoolCredentials) error,
) error {
	live := n.blockProducerCreds.Load()
	if live == nil {
		return errors.New("block producer credentials are not loaded")
	}
	return n.reloadPoolCredentials(
		n.primaryKeySource(),
		live,
		slot,
		supported,
		ledgerCheck,
	)
}

// reloadPoolCredentials reloads one pool's credentials through its key
// source. The replacement is loaded into credentials of its own and only moved
// into the live set once every check has passed, because loading into the live
// set fails closed on error, which would stop a healthy producer. A failure is
// counted against the pool when the node forges for several.
func (n *Node) reloadPoolCredentials(
	ks *poolKeySource,
	live *forging.PoolCredentials,
	slot uint64,
	supported bool,
	ledgerCheck func(*forging.PoolCredentials) error,
) (retErr error) {
	if n.producerPoolCount() > 1 {
		defer func() {
			if retErr != nil {
				n.countReloadFailure(live.GetPoolID().String())
			}
		}()
	}
	if ks.kesAgentConfigured() {
		if !ks.kesAgentSignMode() {
			return errors.New(
				"block producer credentials come from a KES agent in serve-key mode; rotate the key through the agent, which pushes each new key",
			)
		}
		return n.reloadPoolAgentSignCredentials(
			ks,
			live,
			slot,
			supported,
			ledgerCheck,
			nil,
		)
	}
	if !supported {
		return errors.New(
			"credential reload requires a wall-clock slot supported by confirmed era history",
		)
	}
	next, err := ks.validateBlockProducerStartupForClock(slot, supported)
	if err != nil {
		return fmt.Errorf("validate reloaded credentials: %w", err)
	}
	// ReplaceWith empties next on success, so this only wipes a replacement
	// that was refused.
	defer next.Close()
	if err := ledgerCheck(next); err != nil {
		return fmt.Errorf("reloaded credentials failed ledger check: %w", err)
	}
	oldCounter, oldKESPeriod := opCertCounterAndPeriod(live.GetOpCert())
	if err := live.ReplaceWith(next); err != nil {
		return fmt.Errorf("install reloaded credentials: %w", err)
	}
	newCounter, newKESPeriod := opCertCounterAndPeriod(live.GetOpCert())
	n.config.logger.Info(
		"block producer credentials reloaded",
		"component", "node",
		"pool_id", live.GetPoolID().String(),
		"old_opcert_counter", oldCounter,
		"new_opcert_counter", newCounter,
		"old_opcert_kes_period", oldKESPeriod,
		"new_opcert_kes_period", newKESPeriod,
		"opcert_expiry_period", live.OpCertExpiryPeriod(),
	)
	return nil
}

// opCertCounterAndPeriod reads the fields a reload logs; both are zero for
// credentials that carry no certificate.
func opCertCounterAndPeriod(opCert *forging.OpCert) (uint64, uint64) {
	if opCert == nil {
		return 0, 0
	}
	return opCert.IssueNumber, opCert.KESPeriod
}

// closeBlockProducerCredentials zeroizes the live credentials and forgets
// them. Callers stop the forger, the leader election and the KES agent client
// first, since each reads from or installs into the credentials.
func (n *Node) closeBlockProducerCredentials() {
	if creds := n.blockProducerCreds.Swap(nil); creds != nil {
		creds.Close()
	}
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

// blockProducerEraSource supplies the era context for the startup
// operational-certificate counter check. *ledger.LedgerState implements it.
type blockProducerEraSource interface {
	forging.ProtocolParamsProvider
	// Tip is the applied-chain tip. Its slot scopes the counter rule.
	Tip() ochainsync.Tip
	// CurrentSlot is the wall-clock slot. It is read only to report how far
	// the applied tip lags; it must not scope the counter rule.
	CurrentSlot() (uint64, error)
}

// validateBlockProducerLedger runs the ledger-aware cross-check against
// the loaded credentials. Must be called after the ledger has started so
// pool registrations can be queried. A pool that is not yet registered
// is logged as a warning and the node is allowed to continue.
func (n *Node) validateBlockProducerLedger(
	creds *forging.PoolCredentials,
) error {
	if n.ledgerState == nil {
		return errors.New(
			"block producer ledger cross-check requires ledger state",
		)
	}
	return n.validateBlockProducerLedgerWithSource(
		creds,
		blockProducerLedgerView{ls: n.ledgerState},
		n.ledgerState,
	)
}

// validateBlockProducerLedgerWithSource resolves the slot whose era scopes the
// startup opcert counter rule, then runs the cross-check.
//
// The slot is the applied chain tip, never the wall clock. The counter
// baseline (LedgerView.LatestOpCertSequence) is produced by the applied-chain
// stage, so the era that scopes the rule has to be read from the same stage. A
// wall-clock slot makes LedgerState.ProtocolParamsForSlot forecast forward
// through the era shape: on a node whose applied tip lags -- interrupted sync,
// restart after downtime, restore from an older snapshot -- it resolves a
// Praos era the applied chain has not reached while the baseline is still
// pre-catch-up, so a pool several rotations into its life looks gapped and
// startup is refused, leaving the node unable to sync to the point that would
// make the baseline correct. LedgerState.CurrentOrTipSlot is not a substitute:
// it returns whichever of the two slots is ahead, which is the wall-clock slot
// in exactly that case.
func (n *Node) validateBlockProducerLedgerWithSource(
	creds *forging.PoolCredentials,
	view forging.LedgerView,
	source blockProducerEraSource,
) error {
	var slot uint64
	if source != nil {
		slot = source.Tip().Point.Slot
		if wallSlot, wallErr := source.CurrentSlot(); wallErr == nil &&
			wallSlot > slot {
			n.config.logger.Warn(
				"block producer opcert counter rule scoped to the applied chain tip, which is behind wall-clock time",
				"component",
				"node",
				"tip_slot",
				slot,
				"wall_clock_slot",
				wallSlot,
				"slots_behind",
				wallSlot-slot,
			)
		}
	}
	return n.validateBlockProducerLedgerWithViewAtSlot(
		creds,
		view,
		source,
		slot,
	)
}

func (n *Node) validateBlockProducerLedgerWithViewAtSlot(
	creds *forging.PoolCredentials,
	view forging.LedgerView,
	params forging.ProtocolParamsProvider,
	slot uint64,
) error {
	if creds == nil {
		return errors.New("nil pool credentials")
	}
	result, err := creds.ValidateAgainstLedgerAtSlot(view, params, slot)
	if result.EraUnevaluable != nil {
		// Not evaluable is not violated. Startup continues; the forge loop
		// applies the full era-scoped rule per won leader slot once the
		// node is near the tip, and fails closed there.
		n.config.logger.Warn(
			"block producer opcert counter gap rule not evaluated at startup; the forge loop enforces it per leader slot",
			"component",
			"node",
			"pool_id",
			creds.GetPoolID().String(),
			"slot",
			slot,
			"reason",
			result.EraUnevaluable,
		)
	}
	registered, vrfMatched := result.Registered, result.VRFMatched
	if err != nil {
		if errors.Is(err, forging.ErrVRFKeyHashMismatch) &&
			n.config.network == "devnet" {
			n.config.logger.Warn(
				"devnet block producer VRF cross-check failed; node will continue",
				"component",
				"node",
				"pool_id",
				creds.GetPoolID().String(),
				"error",
				err,
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
			"component",
			"node",
			"pool_id",
			poolID,
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

// applyForgeTuning is the runtime mapping for operator-configured forge knobs.
// Keeping the mapping together lets its test catch any setting that would
// otherwise stop affecting the forger when configuration wiring changes.
func applyForgeTuning(fc *forging.ForgerConfig, cfg *Config) {
	fc.ForgeSyncToleranceSlots = cfg.forgeSyncToleranceSlots
	fc.ForgeStaleGapThresholdSlots = cfg.forgeStaleGapThresholdSlots
	fc.ForgeUpstreamStalenessSlots = cfg.forgeUpstreamStalenessSlots
	fc.ForgeAppliedTipStalenessSlots = cfg.forgeAppliedTipStalenessSlots
	fc.ForgeEndorserBlockStalenessSlots = cfg.forgeEndorserBlockStalenessSlots
	fc.ForgeEBSelectionReserve = cfg.forgeEBSelectionReserve
	fc.ForgeEBMaxTxRefs = cfg.forgeEBMaxTxRefs
	fc.ForgeEBMaxBytes = cfg.forgeEBMaxBytes
}

// startBlockProducer validates the operator's credentials, starts leader
// election and the block forger, and returns Run's startup-cleanup stack with
// their stop appended.
//
// The stop is appended immediately after credential validation because that
// step may start a KES agent loop, and every later startup check can fail.
// Nil-guarded stops also cover failures while validating ledger credentials
// or starting the forger.
//
// The stack is returned rather than mutated in place because append may
// reallocate; the caller must use the returned slice on both the success and
// the error path.
func (n *Node) startBlockProducer(
	ctx context.Context,
	started []func(),
) ([]func(), error) {
	if err := n.prepareMultiPool(); err != nil {
		return started, fmt.Errorf("block producer pools: %w", err)
	}
	//nolint:contextcheck // the KES agent loop is bound to the node lifecycle context, not this call
	creds, err := n.validateBlockProducerStartup()
	if err != nil {
		return started, fmt.Errorf(
			"block producer startup validation failed: %w",
			err,
		)
	}
	n.blockProducerCreds.Store(creds)
	n.setEquivocationSelfPoolID(creds)
	//nolint:contextcheck // the reload loops are bound to the node lifecycle context, not this call
	n.startKeyReloadLoops()
	started = append(started, func() {
		n.stopKeyReloadLoops()
		logErrIfNotNil(
			n.config.logger,
			"failed to stop the other block producer pools during cleanup",
			n.stopPoolProducers(),
		)
		if n.blockForger != nil {
			n.blockForger.Stop()
		}
		n.closeKESAgentClient()
		if n.leaderElection != nil {
			logErrIfNotNil(
				n.config.logger,
				"failed to stop leader election during cleanup",
				n.leaderElection.Stop(),
			)
		}
		n.closeBlockProducerCredentials()
	})
	// Cross-check loaded credentials against ledger state. Mismatch
	// against on-chain pool registration is fatal; "not yet
	// registered" is a warning so operators can stage credentials
	// before submitting the registration cert.
	if err := n.validateBlockProducerLedger(creds); err != nil {
		return started, fmt.Errorf(
			"block producer credentials failed ledger check: %w",
			err,
		)
	}
	if err := n.initBlockForger(ctx, creds); err != nil {
		return started, fmt.Errorf(
			"failed to initialize block forger: %w",
			err,
		)
	}
	// Enable Leios vote emission when a vote signing key is
	// configured (experimental, leios mode only)
	if err := n.enableLeiosVoting(creds); err != nil {
		return started, fmt.Errorf("failed to enable leios voting: %w", err)
	}
	// Start every other pool this node forges for.
	if err := n.startPoolProducers(ctx); err != nil {
		return started, fmt.Errorf(
			"failed to start block producer pools: %w",
			err,
		)
	}
	// Wire the forgers' slot trackers into ledger state for slot
	// battle detection. The forgers are created after the ledger
	// state, so we use the late-binding setter.
	n.wireForgedBlockTracking()
	return started, nil
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
	poolID := creds.GetPoolID().String()
	built, err := n.newPoolForger(
		ctx,
		creds,
		n.poolMetricsRegistry(poolID),
		n.slotArbiterFor(poolID),
	)
	if err != nil {
		return err
	}

	// Store election for cleanup during shutdown only after the forger is
	// fully created and running.
	n.setLeaderElection(built.election)
	n.blockForger = built.forger
	n.addArbiterMember(poolID, creds, built)
	n.config.logger.Info(
		"block forger started in production mode with leader election",
		"pool_id", poolID,
	)

	return nil
}

// poolForging is one pool's running leader election and block forger.
type poolForging struct {
	election nodeconsensus.LeaderScheduler
	forger   *forging.BlockForger
	// praos is true when the election draws a Praos VRF schedule, the only
	// kind whose leader slots a SlotArbiter can rank by VRF output.
	praos bool
}

// newPoolForger builds and starts one pool's leader election and block
// forger. reg is where both register their metrics and arbiter decides slots
// this pool leads together with another pool of the node; either may be nil.
// The caller owns stopping what it returns.
func (n *Node) newPoolForger(
	ctx context.Context,
	creds *forging.PoolCredentials,
	reg prometheus.Registerer,
	arbiter forging.SlotArbiter,
) (poolForging, error) {
	if creds == nil {
		return poolForging{}, errors.New("nil pool credentials")
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
		CIP0203Enabled:  n.config.cip0203Enabled,
		TxValidator:     n.ledgerState,
		// The 12-field prototype header body belongs to Leios networks only.
		LeiosHeaderExtension: n.config.leiosForgingEnabled(),
	})
	if err != nil {
		return poolForging{}, fmt.Errorf("failed to create block builder: %w", err)
	}

	// Create block broadcaster for synchronous local chain adoption.
	broadcaster := &blockBroadcaster{
		chain:  n.chainManager.PrimaryChain(),
		logger: n.config.logger,
	}

	// Create the leader election component
	// Convert pool ID from PoolId to PoolKeyHash (both are [28]byte)
	poolID := creds.GetPoolID()
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], poolID[:])

	// Create adapters for the providers that leader.Election needs
	stakeProvider := &stakeDistributionAdapter{ledgerState: n.ledgerState}
	epochProvider := &epochInfoAdapter{ledgerState: n.ledgerState}

	var scheduleStore leader.ScheduleStore
	if n.db != nil {
		if store := leader.NewSyncStateScheduleStore(
			n.db.Metadata(),
		); store != nil {
			scheduleStore = store
		}
	}
	// The selected provider owns production scheduling. It receives the same
	// ledger views used by header validation, while credentials remain owned by
	// the existing block producer.
	var praosSchedule bool
	election, err := n.selectedConsensusProvider().NewLeaderScheduler(
		func(
			scheduleIdentity string,
			kind nodeconsensus.LeaderScheduleKind,
		) (nodeconsensus.LeaderScheduler, error) {
			var election *leader.Election
			switch kind {
			case nodeconsensus.LeaderSchedulePraos:
				election = leader.NewElection(
					poolKeyHash,
					creds.GetVRFSKey,
					stakeProvider,
					epochProvider,
					n.eventBus,
					n.config.logger,
				)
			case nodeconsensus.LeaderScheduleTachys:
				election = leader.NewTachysElection(
					poolKeyHash,
					stakeProvider,
					stakeProvider,
					epochProvider,
					n.eventBus,
					n.config.logger,
				)
			default:
				return nil, fmt.Errorf(
					"unsupported leader schedule kind %d",
					kind,
				)
			}
			praosSchedule = kind == nodeconsensus.LeaderSchedulePraos
			election.SetConsensusIdentity(scheduleIdentity)
			election.SetPromRegistry(reg)
			if n.scheduleComputeGate != nil {
				election.SetComputeGate(n.scheduleComputeGate)
			}
			if scheduleStore != nil {
				election.SetScheduleStore(scheduleStore)
			}
			return election, nil
		},
	)
	if err != nil {
		return poolForging{}, fmt.Errorf("create consensus leader scheduler: %w", err)
	}

	// Start leader election (subscribes to epoch transitions)
	if err := election.Start(ctx); err != nil {
		return poolForging{}, fmt.Errorf("failed to start leader election: %w", err)
	}

	// Create slot clock adapter for the forger
	slotClock := &slotClockAdapter{ledgerState: n.ledgerState}

	// Wire Leios EB forging when the pipeline manager is available
	// (i.e. Dijkstra era is enabled) and the network speaks Leios. Relay
	// nodes, pre-Dijkstra block producers and plain Dijkstra networks such
	// as DijkstraNet (cardano-node, no Leios) leave these nil and skip EB
	// production: an announced endorser block puts the 12-field Leios
	// header body on the ranking block, which cardano-node cannot decode.
	var leiosChecker forging.LeiosProduceChecker
	var leiosCerts forging.LeiosCertificateProvider
	var leiosParent forging.LeiosParentAnnouncementProvider
	var leiosEBCaster forging.EndorserBlockBroadcaster
	var leiosMempool forging.MempoolProvider
	if n.config.leiosForgingEnabled() &&
		n.leiosPipelineManager != nil && n.ouroboros() != nil {
		adapter := &leiosPipelineAdapter{
			mgr:              n.leiosPipelineManager,
			chain:            n.chainManager.PrimaryChain(),
			endorserBlockTxs: n.ouroboros().EndorserBlockClosureTxsByHash,
		}
		leiosChecker = adapter
		leiosCerts = adapter
		leiosParent = adapter
		leiosEBCaster = n.ouroboros()
		leiosMempool = mempoolAdapter
	}
	blockForged := n.ledgerState.RecordForgedBlock
	if n.ouroboros() != nil {
		//nolint:contextcheck // This observer publishes node-lifetime Leios work; Ouroboros drains its GC worker before storage replacement or Close.
		blockForged = func(block gledger.Block, blockCbor []byte, latency time.Duration) {
			n.ledgerState.RecordForgedBlock(block, blockCbor, latency)
			if header, ok := block.Header().(*gdijkstra.DijkstraBlockHeader); ok {
				if _, _, announces := header.LeiosAnnouncement(); announces {
					n.ouroboros().EnqueueLeiosBlockAnnouncement(header.Cbor())
				}
			}
		}
	}

	// Wire the durable last-forged-slot fence. A block producer must not
	// start without it: the in-memory fallback cannot survive a restart,
	// which is precisely the case the fence exists for. The forging
	// package still tolerates a nil store for embedders and dev-mode
	// wiring, so refuse here rather than there.
	var forgeFence forging.ForgeFenceStore
	if n.db != nil {
		forgeFence = forging.NewSyncStateForgeFenceStore(
			n.db.Metadata(),
			poolKeyHash,
		)
	}
	if forgeFence == nil {
		_ = election.Stop()
		return poolForging{}, errors.New(
			"block producer requires a metadata store for the forge fence",
		)
	}

	// Always enforce aggregate reference-script limits before AddBlock.
	// Full self-validation (header crypto, body-hash, per-tx ledger checks)
	// runs too unless the operator explicitly opts out (fail closed by
	// default).
	blockValidator := newForgedBlockValidator(
		n.ledgerState,
		n.config.validateForgedBlock,
	)

	// Create the block forger with the real leader election
	forgerCfg := forging.ForgerConfig{
		Mode:             forging.ModeProduction,
		Logger:           n.config.logger,
		Credentials:      creds,
		LeaderChecker:    election,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		ConfirmedTxs:     mempoolAdapter,
		BlockForged:      blockForged,
		SlotClock:        slotClock,
		// Equal-slot alternative forging. When a rival block already
		// occupies the slot this node leads, the forger builds an
		// alternative on the rival's predecessor and offers it to chain
		// selection instead of conceding the slot -- ouroboros-consensus
		// mkCurrentBlockContext's EQ case. The primary chain supplies the
		// fork context; LedgerState arbitrates with the same Praos
		// comparison a peer's competing block goes through.
		ChainContext:   n.chainManager.PrimaryChain(),
		SiblingAdopter: n.ledgerState,
		// Closure, not a method value: n.ouroboros is rebuilt live, so this
		// resolves the current instance when the forge loop asks.
		LeiosVerifiedEbSlot: func() uint64 {
			return n.ouroboros().MaxVerifiedEndorserBlockSlot()
		},
		BlockValidator:      blockValidator,
		ForgeFence:          forgeFence,
		PromRegistry:        reg,
		SlotArbiter:         arbiter,
		LeiosProduceChecker: leiosChecker,
		LeiosEBBroadcaster:  leiosEBCaster,
		LeiosMempool:        leiosMempool,
		LeiosTxValidator:    n.ledgerState,
		LeiosAnnouncementValidator: func(
			ctx context.Context,
			header lcommon.BlockHeader,
		) error {
			o := n.ouroboros()
			if o == nil {
				return errOuroborosNotStarted
			}
			// Before adoption the forged block is not the tip, so its
			// endorser block is checked on the parent state. A locally
			// built announcing block carries no transactions, and its
			// endorser block was selected on the parent state rebased onto
			// the certified closure (closure transactions and every
			// transaction spending their inputs excluded), so a selection
			// valid there is valid on the block's own state.
			return classifyLeiosAnnouncementValidationError(
				o.ValidateLeiosEndorserBlockForHeader(
					ctx,
					header,
					ledger.LeiosValidationParentHash(header),
				),
			)
		},
		LeiosCertificateProvider:        leiosCerts,
		LeiosParentAnnouncementProvider: leiosParent,
		LeiosCertificationTimingChecker: n.ledgerState,
		OpCertLedgerView: blockProducerLedgerView{
			ls: n.ledgerState,
		},
		EraParams: n.ledgerState,
	}
	applyForgeTuning(&forgerCfg, &n.config)
	forger, err := forging.NewBlockForger(forgerCfg)
	if err != nil {
		// Stop election to prevent goroutine leak
		_ = election.Stop()
		return poolForging{}, fmt.Errorf("failed to create block forger: %w", err)
	}

	// Start the forger with the passed context
	if err := forger.Start(ctx); err != nil {
		// Stop election to prevent goroutine leak
		_ = election.Stop()
		return poolForging{}, fmt.Errorf("failed to start block forger: %w", err)
	}

	return poolForging{
		election: election,
		forger:   forger,
		praos:    praosSchedule,
	}, nil
}

func classifyLeiosAnnouncementValidationError(err error) error {
	switch {
	case errors.Is(err, ledger.ErrInvalidLeiosEndorserBlockTransactions):
		return errors.Join(forging.ErrInvalidLeiosAnnouncement, err)
	case errors.Is(err, ledger.ErrLeiosValidationParentUnavailable),
		errors.Is(err, ledger.ErrLeiosValidationParentSuperseded),
		errors.Is(err, ledger.ErrLedgerViewStorageFault):
		return errors.Join(forging.ErrLeiosAnnouncementUnavailable, err)
	default:
		return err
	}
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

// The equal-slot alternative path is wired from two existing components
// rather than from adapters, so these assertions are what keeps that wiring
// honest: chain.Chain answers the fork context the alternative is built on,
// and LedgerState arbitrates between the alternative and the block already at
// the tip. A signature change on either side fails the build here instead of
// silently reverting block producers to conceding contested slots, which is
// what a nil provider does.
var (
	_ forging.AlternativeChainContextProvider = (*chain.Chain)(nil)
	_ forging.SiblingBlockAdopter             = (*ledger.LedgerState)(nil)
)

// blockBroadcaster implements forging.BlockBroadcaster through synchronous
// local chain admission. Block proposals are requests, not notifications, so
// they must not wait in the asynchronous EventBus while the chain advances.
type blockBroadcaster struct {
	chain  *chain.Chain
	logger *slog.Logger
}

func (b *blockBroadcaster) AddBlock(ctx context.Context,
	block gledger.Block,
	_ []byte,
) error {
	if block == nil {
		return errors.New("proposed block is nil")
	}
	if b.chain == nil {
		return errors.New("chain unavailable")
	}
	if err := b.chain.AddLocalBlockIfHeaderCurrent(ctx, block); err != nil {
		return fmt.Errorf("chain rejected proposed block: %w", err)
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
	// afterPoolStakeReadFn is a test-only hook for coordinating a concurrent
	// snapshot recapture after the transaction has read the numerator.
	afterPoolStakeReadFn func()
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
	txn := db.MetadataTxn(context.Background(), false)
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

// GetPoolAndTotalActiveStake reads the sigma numerator and denominator for
// one pool inside a single metadata transaction.
//
// Two defects are fixed here, and both are load-bearing for consensus:
//
// Denominator -- the denominator comes from LedgerView.GetTotalActiveStake,
// a txn-scoped wrapper over Metadata().GetTotalActiveStake, which is the
// same store accessor ledger/verify_header.go resolves the denominator
// through when it checks an incoming header's leader eligibility.
// Verification calls that store method directly rather than through a
// LedgerView, with the snapshotType it resolved for the header under check;
// the shared thing is the store accessor, not the call path. The previous
// implementation returned
// ledger.StakeDistribution.TotalStake, which LedgerView.GetStakeDistribution
// accumulates by summing the mark rows itself, while verification reads
// epoch_summary.total_active_stake. Those two agree only "by construction"
// -- one rotation transaction writes both from one calculation
// (ledger/pool_stake_distribution.go documents the exact conditions) -- so
// the equality is a property of the writer, not of the readers, and nothing
// stops the two paths from drifting. A node whose forge denominator differs
// from its verify denominator can forge a block it would itself reject, or
// decline a slot it is genuinely eligible for. Resolving both through one
// accessor removes the second derivation entirely.
//
// Atomicity -- both values are read through one db.MetadataTxn and one
// LedgerView. Opening a transaction per value let a snapshot re-capture land
// between them, yielding a sigma whose halves come from different writes.
func (a *stakeDistributionAdapter) GetPoolAndTotalActiveStake(
	epoch uint64,
	poolKeyHash []byte,
) (poolStake uint64, totalActiveStake uint64, err error) {
	if a.ledgerState == nil {
		return 0, 0, errors.New("ledger state unavailable")
	}
	db := a.ledgerState.Database()
	if db == nil {
		return 0, 0, errors.New("database unavailable")
	}
	txn := db.MetadataTxn(context.Background(), false)
	if txn == nil {
		return 0, 0, errors.New("metadata transaction unavailable")
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
	view := a.ledgerState.NewView(txn)
	poolKey := hex.EncodeToString(poolKeyHash)
	poolStake, err = view.GetPoolStake(epoch, poolKeyHash)
	if err != nil {
		return 0, 0, fmt.Errorf(
			"get pool stake for epoch %d pool %s: %w",
			epoch,
			poolKey,
			err,
		)
	}
	// Test-only synchronization seam. The transaction has already observed
	// the numerator, so a concurrent recapture can commit before the
	// denominator query without changing this transaction's snapshot.
	if a.afterPoolStakeReadFn != nil {
		a.afterPoolStakeReadFn()
	}
	totalActiveStake, err = view.GetTotalActiveStake(epoch)
	if err != nil {
		return 0, 0, fmt.Errorf(
			"get total active stake for epoch %d: %w",
			epoch,
			err,
		)
	}
	return poolStake, totalActiveStake, nil
}

// The forging adapter must resolve sigma through the atomic pair accessor.
// A drift back to two independent reads, or to summing the mark rows for the
// denominator, is the pair of defects described above; make it
// a compile error rather than a silent consensus divergence.
var _ leader.StakeDistributionProvider = (*stakeDistributionAdapter)(nil)

// GetTachysDistribution exposes the ledger-owned immutable election snapshot
// to the production scheduler.
func (a *stakeDistributionAdapter) GetTachysDistribution(
	ctx context.Context,
	epoch uint64,
) (*nodeconsensus.TachysDistribution, error) {
	if a.ledgerState == nil {
		return nil, errors.New("ledger state unavailable")
	}
	return a.ledgerState.TachysDistribution(ctx, epoch)
}

var _ leader.TachysDistributionProvider = (*stakeDistributionAdapter)(nil)

// epochInfoAdapter adapts ledger.LedgerState to leader.EpochInfoProvider.
type epochInfoAdapter struct {
	ledgerState *ledger.LedgerState
}

// computeSchedule discovers the exact-rational coefficient with a runtime type
// assertion and silently falls back to the float64 accessor when it fails,
// which yields a strictly larger leadership threshold than the reference
// node's. Make that drift a compile error;
// TestEpochInfoAdapterProvidesExactActiveSlotCoeff covers the same pairing.
var _ leader.ActiveSlotCoeffRatProvider = (*epochInfoAdapter)(nil)

func (a *epochInfoAdapter) CurrentEpoch() uint64 {
	return a.ledgerState.CurrentEpoch()
}

func (a *epochInfoAdapter) EpochNonce(
	ctx context.Context,
	epoch uint64,
) []byte {
	return a.ledgerState.EpochNonce(ctx, epoch)
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

func (a *epochInfoAdapter) ConsensusModeForEpoch(
	epoch uint64,
) (consensus.ConsensusMode, error) {
	return a.ledgerState.ConsensusModeForEpoch(epoch)
}

func (a *epochInfoAdapter) EraIDForEpoch(epoch uint64) (uint, error) {
	return a.ledgerState.EraIDForEpoch(epoch)
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

// ChainTip returns the ledger-applied tip. LedgerState.Tip reads one atomic
// tip snapshot, so the returned slot and hash are always from the same tip.
func (a *slotClockAdapter) ChainTip() ocommon.Point {
	return a.ledgerState.Tip().Point
}

func (a *slotClockAdapter) ChainTipSnapshot() ochainsync.Tip {
	return a.ledgerState.Tip()
}

func (a *slotClockAdapter) ForgeTipSnapshot() (ochainsync.Tip, int) {
	return a.ledgerState.ForgeTipSnapshot()
}

// ChainTipHash satisfies the deprecated forging.ChainTipHashProvider. The
// forger no longer calls it: it takes the tip hash from ChainTip above,
// which returns slot and hash from one snapshot. Kept so the adapter still
// satisfies that exported interface for any external caller.
func (a *slotClockAdapter) ChainTipHash() []byte {
	return a.ledgerState.Tip().Point.Hash
}

var _ forging.ChainTipHashProvider = (*slotClockAdapter)(nil)

// PrimaryChainTip returns the primary chain's BLOCK tip -- chain.Tip(), the
// newest block added to the chain, which runs ahead of the ledger-applied tip
// while the pipeline replays. The forge safety provider below reads it together
// with the verified queued-header frontier under one chain lock.
func (a *slotClockAdapter) PrimaryChainTip() ocommon.Point {
	return a.ledgerState.PrimaryChainTip().Point
}

func (a *slotClockAdapter) PrimaryChainTipRelation(
	point ocommon.Point,
) (ochainsync.Tip, uint64, bool, error) {
	return a.ledgerState.PrimaryChainTipRelation(point)
}

func (a *slotClockAdapter) PrimaryChainTipRelationAndHeader(
	point ocommon.Point,
) (ochainsync.Tip, ochainsync.Tip, uint64, bool, error) {
	return a.ledgerState.PrimaryChainTipRelationAndHeader(point)
}

func (a *slotClockAdapter) NextSlotTime() (time.Time, error) {
	return a.ledgerState.NextSlotTime()
}

func (a *slotClockAdapter) UpstreamTipSlot() uint64 {
	return a.ledgerState.UpstreamTipSlot()
}

func (a *slotClockAdapter) UpstreamSyncStatus() (uint64, bool) {
	return a.ledgerState.UpstreamSyncStatus()
}

func (a *slotClockAdapter) UpstreamSyncTip() (ochainsync.Tip, bool) {
	return a.ledgerState.UpstreamSyncTip()
}

func (a *slotClockAdapter) SecurityParam() int {
	return a.ledgerState.SecurityParam()
}

// leiosPipelineAdapter adapts leios.PipelineManager and the primary chain to
// the narrow Leios interfaces the forge loop expects.
type leiosPipelineAdapter struct {
	mgr              *leios.PipelineManager
	chain            leiosParentChain
	endorserBlockTxs func(ebHash []byte, ebSlot uint64) ([]cbor.RawMessage, bool)
}

type leiosParentChain interface {
	Tip() ochainsync.Tip
	BlockByPoint(
		context.Context,
		ocommon.Point,
		*database.Txn,
	) (models.Block, error)
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

func (a *leiosPipelineAdapter) CertifiedEndorserBlockTxs(
	ebHash lcommon.Blake2b256,
	ebSlot uint64,
) ([][]byte, bool) {
	if a.endorserBlockTxs == nil {
		return nil, false
	}
	raw, ok := a.endorserBlockTxs(ebHash.Bytes(), ebSlot)
	if !ok {
		return nil, false
	}
	txs := make([][]byte, len(raw))
	for i, tx := range raw {
		txs[i] = tx
	}
	return txs, true
}

func (a *leiosPipelineAdapter) MarkEndorserBlockEmbedded(
	ebHash lcommon.Blake2b256,
	ebSlot uint64,
) {
	a.mgr.MarkEmbedded(ebSlot, ebHash)
}

func (a *leiosPipelineAdapter) ParentLeiosAnnouncement(ctx context.Context) (
	lcommon.Blake2b256,
	lcommon.Blake2b256,
	bool,
	error,
) {
	if a.chain == nil {
		return lcommon.Blake2b256{}, lcommon.Blake2b256{}, false, errors.New(
			"chain unavailable",
		)
	}
	tip := a.chain.Tip()
	if len(tip.Point.Hash) == 0 {
		return lcommon.Blake2b256{}, lcommon.Blake2b256{}, false, nil
	}
	block, err := a.chain.BlockByPoint(ctx, tip.Point, nil)
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
	rbHash, err := lcommon.NewBlake2b256Checked(tip.Point.Hash)
	if err != nil {
		return lcommon.Blake2b256{}, lcommon.Blake2b256{}, false, fmt.Errorf(
			"parent block hash: %w",
			err,
		)
	}
	return rbHash, hash, ok, nil
}

// forgedBlockValidatorAdapter adapts ledger.LedgerState to
// forging.BlockValidator so the forger can self-validate blocks before
// adoption without importing the ledger package from within forging.
type forgedBlockValidatorAdapter struct {
	ledgerState    forgedBlockValidationState
	fullValidation bool
}

type forgedBlockValidationState interface {
	ValidateForgedBlock(context.Context, gledger.Block, []byte) error
	ValidateBlockReferenceScripts(context.Context, gledger.Block) error
}

func newForgedBlockValidator(
	state forgedBlockValidationState,
	fullValidation bool,
) forging.BlockValidator {
	return &forgedBlockValidatorAdapter{
		ledgerState:    state,
		fullValidation: fullValidation,
	}
}

func (a *forgedBlockValidatorAdapter) ValidateForgedBlock(ctx context.Context,
	block gledger.Block,
	blockCbor []byte,
) error {
	if !a.fullValidation {
		return a.ledgerState.ValidateBlockReferenceScripts(ctx, block)
	}
	return a.ledgerState.ValidateForgedBlock(ctx, block, blockCbor)
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

func (a *epochNonceAdapter) EpochNonce(
	ctx context.Context,
	epoch uint64,
) []byte {
	return a.ledgerState.EpochNonce(ctx, epoch)
}

// leaderSchedulerRef boxes an interface value so it can be published
// atomically.
type leaderSchedulerRef struct {
	scheduler nodeconsensus.LeaderScheduler
}

// setLeaderElection publishes the scheduler both to the node lifecycle,
// which owns n.leaderElection, and to the ledger rollback callback, which
// runs on the ledger goroutine and must read it through leaderSchedulerRef.
func (n *Node) setLeaderElection(s nodeconsensus.LeaderScheduler) {
	n.leaderElection = s
	n.leaderSchedulerRef.Store(&leaderSchedulerRef{scheduler: s})
}
