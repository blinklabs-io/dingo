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

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/dingo/chain"
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
	if _, err := n.ledgerState.CurrentSlot(); err != nil {
		if !errors.Is(err, ledger.ErrBeforeGenesis) {
			return nil, fmt.Errorf("compute current slot: %w", err)
		}
		// Clock before genesis: preserve the historical hard fail through the
		// strict preflight (an opcert can never be "current" before genesis).
		return n.validateBlockProducerStartupForClock(0, true)
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
		return nil, fmt.Errorf("wall-clock slot from confirmed history: %w", err)
	}
	return n.validateBlockProducerStartupForClock(supportedSlot, supported)
}

func (n *Node) validateBlockProducerStartupAtSlot(
	currentSlot uint64,
) (creds *forging.PoolCredentials, retErr error) {
	creds = forging.NewPoolCredentials()
	agentBacked := n.config.shelleyKESAgentSocket != ""
	if agentBacked {
		// Agent startup installs the client before the remaining credential
		// validation below. Do not leave that client and its serve-key loop
		// running when a later validation step rejects the credentials.
		defer func() {
			if retErr != nil {
				n.closeKESAgentClient()
			}
		}()
	}
	if n.config.shelleyKESAgentSocket != "" {
		if err := n.loadBlockProducerCredentialsFromAgent(
			creds,
			currentSlot,
		); err != nil {
			return nil, fmt.Errorf(
				"load pool credentials from KES agent: %w",
				err,
			)
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
	return creds, nil
}

// validateBlockProducerStartupForClock decides how to judge the operational
// certificate given a wall-clock slot and whether the confirmed era history
// actually supports it.
//
// When supported, this is the strict historical preflight: a certificate
// staged for the future or already expired fails startup here. When not
// supported, the KES-period plausibility check is deferred rather than
// skipped — credential material is still validated and the protocol lifetime
// is still armed, so the forger's per-slot gate has the data it needs to
// reject the certificate before Praos leader selection.
//
// Split out from validateBlockProducerStartup so both branches are reachable
// without a live LedgerState, the same reason
// validateBlockProducerStartupAtSlot and validateBlockProducerLedgerWithView
// exist separately.
func (n *Node) validateBlockProducerStartupForClock(
	slot uint64,
	supported bool,
) (*forging.PoolCredentials, error) {
	if supported {
		return n.validateBlockProducerStartupAtSlot(slot)
	}
	creds, err := n.validateBlockProducerCredentialMaterial()
	if err != nil {
		return nil, err
	}
	genesis, err := n.blockProducerShelleyGenesis()
	if err != nil {
		return nil, err
	}
	if err := creds.ArmKesProtocolLifetime(genesis); err != nil {
		return nil, fmt.Errorf("arm KES protocol lifetime: %w", err)
	}
	opCert := creds.GetOpCert()
	if opCert == nil {
		return nil, errors.New("block producer operational certificate is nil")
	}
	var tipSlot uint64
	if n.ledgerState != nil {
		tipSlot = n.ledgerState.ChainTipSlot()
	}
	n.config.logger.Warn(
		"block producer startup: confirmed era history does not span the current wall-clock slot yet; "+
			"deferring the operational-certificate KES-period plausibility check until the ledger catches up. "+
			"The opcert KES lifetime is re-checked per slot and forging cannot proceed while it fails; "+
			"sync progress additionally gates forging whenever an upstream peer is active.",
		"component", "node",
		"tip_slot", tipSlot,
		"opcert_kes_period", opCert.KESPeriod,
		"opcert_expiry_period", creds.OpCertExpiryPeriod(),
	)
	return creds, nil
}

// validateBlockProducerCredentialMaterial loads the pool credentials and
// validates the operational certificate's structure and cold-key signature,
// without judging it against any slot. The slot-dependent KES-period
// plausibility check lives in validateBlockProducerStartupAtSlot; callers
// needing the certificate armed but not yet placed in time combine this with
// PoolCredentials.ArmKesProtocolLifetime.
func (n *Node) validateBlockProducerCredentialMaterial() (*forging.PoolCredentials, error) {
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
	return creds, nil
}

// loadBlockProducerCredentialsFromAgent dials the configured KES agent and
// installs its material into creds, in either serve-key or sign mode. VRF and
// the operational certificate are always still read from local files; the
// KES agent protocol carries only the KES signing key (and, in serve-key
// mode, the opcert alongside it).
//
// It closes and replaces any previously dialed agent client first: this
// method also runs on a live-lifecycle rebuild
// (reinitializeBlockProducer -> validateBlockProducerStartup), and must not
// leak the prior connection or its background goroutine.
func (n *Node) loadBlockProducerCredentialsFromAgent(
	creds *forging.PoolCredentials,
	currentSlot uint64,
) error {
	n.closeKESAgentClient()

	mode := n.config.shelleyKESAgentMode
	if mode == "" {
		mode = kesagent.ModeServeKey
	}

	switch mode {
	case kesagent.ModeServeKey:
		return n.startKESAgentServeKey(creds, currentSlot)
	case kesagent.ModeSign:
		return n.startKESAgentSign(creds)
	default:
		return fmt.Errorf("unknown KES agent mode %q", mode)
	}
}

// startKESAgentServeKey blocks for the agent's initial key push, installs it,
// and starts a background loop that installs every subsequent push (a KES
// key rotation) for as long as the node runs.
func (n *Node) startKESAgentServeKey(
	creds *forging.PoolCredentials,
	startupSlot uint64,
) error {
	client, err := kesagent.NewClient(kesagent.Config{
		SocketPath: n.config.shelleyKESAgentSocket,
		Mode:       kesagent.ModeServeKey,
		Logger:     n.config.logger,
	})
	if err != nil {
		return err
	}

	install := func(pk kesagent.PushedKey) error {
		return creds.LoadFromAgentServeKey(
			n.config.shelleyVRFKey,
			agentMaterialFromPushedKey(pk),
		)
	}
	// Installing a push clears the validated KES protocol lifetime, exactly
	// as LoadFromFiles does, so no credential can inherit a policy that was
	// never checked against the material now installed. The initial push is
	// followed by validateBlockProducerStartupAtSlot's own ValidateOpCert /
	// ValidateKESPeriod, which re-establishes it; a push arriving later --
	// a KES evolution, an opcert rotation, or a re-push after a reconnect --
	// is not, so the loop must re-establish it itself. Without this,
	// credentialGeneration.kesSign refuses every signature after the first
	// mid-run push with "operational certificate is not validated" and the
	// node stops forging until it is restarted.
	//
	// Installed and validated in one call rather than two: as two, the
	// credentials are published with their lifetime cleared for the duration
	// of the validation, and a leader slot landing in that window is refused
	// for exactly the reason this callback exists to prevent. Every rotation
	// crosses it.
	loopInstall := func(pk kesagent.PushedKey) error {
		genesis, err := n.blockProducerShelleyGenesis()
		if err != nil {
			return err
		}
		return creds.LoadFromAgentServeKeyValidated(
			n.config.shelleyVRFKey,
			agentMaterialFromPushedKey(pk),
			genesis,
			n.agentInstallSlot(startupSlot),
		)
	}

	// The initial push must succeed before startup can proceed -- the same
	// contract LoadFromFiles has (a load failure fails block-producer
	// startup outright) -- bounded so a dead agent fails startup rather than
	// hanging it.
	//nolint:contextcheck // n.ctx is the node's lifecycle context; startup itself is bounded here
	ctx, cancel := context.WithTimeout(
		n.blockProducerContext(),
		kesagent.DefaultHelloTimeout*2,
	)
	defer cancel()
	pk, err := client.AwaitPushedKey(ctx)
	if err != nil {
		_ = client.Close()
		return fmt.Errorf("await initial KES agent key push: %w", err)
	}
	// ledger/forging copies the key bytes it keeps, so this copy is dead as
	// soon as the install returns; Client.Run does the same for every later
	// push.
	installErr := install(pk)
	pk.Wipe()
	if installErr != nil {
		_ = client.Close()
		return installErr
	}

	n.kesAgentClient = client
	//nolint:contextcheck // background loop is bound to the node's lifecycle, not this call
	runCtx, runCancel := context.WithCancel(n.blockProducerContext())
	n.kesAgentCancel = runCancel
	go func() {
		if err := client.Run(runCtx, loopInstall); err != nil &&
			!errors.Is(err, context.Canceled) {
			n.config.logger.Error(
				"kes agent serve-key loop exited",
				"error", err,
			)
		}
	}()
	return nil
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

// startKESAgentSign installs sign-mode credentials: VRF and the operational
// certificate from local files, KES signing delegated to the agent for the
// lifetime of the client. There is no background loop in this mode -- the
// agent evolves its own key internally on every Sign call.
func (n *Node) startKESAgentSign(creds *forging.PoolCredentials) error {
	opCertKey, err := bursa.LoadKeyFromFile(
		n.config.shelleyOperationalCertificate,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to load operational certificate for KES agent sign mode: %w",
			err,
		)
	}
	client, err := kesagent.NewClient(kesagent.Config{
		SocketPath:        n.config.shelleyKESAgentSocket,
		Mode:              kesagent.ModeSign,
		SignTimeout:       n.config.shelleyKESAgentSignTimeout,
		KESVKey:           opCertKey.VKey,
		OpCertStartPeriod: opCertKey.OpCertKesPeriod,
		Logger:            n.config.logger,
	})
	if err != nil {
		return err
	}
	if err := creds.LoadFromAgentSign(
		n.config.shelleyVRFKey,
		n.config.shelleyOperationalCertificate,
		client,
	); err != nil {
		_ = client.Close()
		return err
	}
	n.kesAgentClient = client
	return nil
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

// closeKESAgentClient stops the serve-key background loop (if any) and
// closes the agent connection. Safe to call when neither exists.
func (n *Node) closeKESAgentClient() {
	if n.kesAgentCancel != nil {
		n.kesAgentCancel()
		n.kesAgentCancel = nil
	}
	if n.kesAgentClient != nil {
		if err := n.kesAgentClient.Close(); err != nil {
			n.config.logger.Warn(
				"failed to close KES agent client",
				"error", err,
			)
		}
		n.kesAgentClient = nil
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
				"component", "node",
				"tip_slot", slot,
				"wall_clock_slot", wallSlot,
				"slots_behind", wallSlot-slot,
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
			"component", "node",
			"pool_id", creds.GetPoolID().String(),
			"slot", slot,
			"reason", result.EraUnevaluable,
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

// initBlockForger initializes the block forger for production mode.
// This requires VRF, KES, and OpCert key files to be configured.
func (n *Node) initBlockForger(
	ctx context.Context,
	creds *forging.PoolCredentials,
) error {
	if creds == nil {
		return errors.New("nil pool credentials")
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
		TxValidator:     n.ledgerState,
	})
	if err != nil {
		return fmt.Errorf("failed to create block builder: %w", err)
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
	if n.leiosPipelineManager != nil && n.ouroboros() != nil {
		adapter := &leiosPipelineAdapter{
			mgr:                   n.leiosPipelineManager,
			chain:                 n.chainManager.PrimaryChain(),
			endorserBlockTxHashes: n.ouroboros().EndorserBlockTxHashesByHash,
		}
		leiosChecker = adapter
		leiosCerts = adapter
		leiosParent = adapter
		leiosEBCaster = n.ouroboros()
		leiosMempool = mempoolAdapter
	}
	blockForged := n.ledgerState.RecordForgedBlock
	if n.ouroboros() != nil {
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
		return errors.New(
			"block producer requires a metadata store for the forge fence",
		)
	}

	// Always enforce aggregate reference-script limits before AddBlock.
	// Full self-validation (header crypto, body-hash, per-tx ledger checks)
	// runs too unless the operator explicitly opts out (issue #3528: fail
	// closed by default).
	blockValidator := newForgedBlockValidator(
		n.ledgerState,
		n.config.validateForgedBlock,
	)

	// Create the block forger with the real leader election
	forger, err := forging.NewBlockForger(forging.ForgerConfig{
		Mode:                               forging.ModeProduction,
		Logger:                             n.config.logger,
		Credentials:                        creds,
		LeaderChecker:                      election,
		BlockBuilder:                       builder,
		BlockBroadcaster:                   broadcaster,
		ConfirmedTxs:                       mempoolAdapter,
		BlockForged:                        blockForged,
		SlotClock:                          slotClock,
		ForgeSyncToleranceSlots:            n.config.forgeSyncToleranceSlots,
		ForgeStaleGapThresholdSlots:        n.config.forgeStaleGapThresholdSlots,
		ForgePrimaryChainTipToleranceSlots: n.config.forgePrimaryChainTipToleranceSlots,
		ForgeUpstreamStalenessSlots:        n.config.forgeUpstreamStalenessSlots,
		ForgeAppliedTipStalenessSlots:      n.config.forgeAppliedTipStalenessSlots,
		ForgeEndorserBlockStalenessSlots:   n.config.forgeEndorserBlockStalenessSlots,
		// Closure, not a method value: n.ouroboros is rebuilt live, so this
		// resolves the current instance when the forge loop asks.
		LeiosVerifiedEbSlot: func() uint64 {
			return n.ouroboros().MaxVerifiedEndorserBlockSlot()
		},
		BlockValidator:                  blockValidator,
		ForgeFence:                      forgeFence,
		PromRegistry:                    n.config.promRegistry,
		LeiosProduceChecker:             leiosChecker,
		LeiosEBBroadcaster:              leiosEBCaster,
		LeiosMempool:                    leiosMempool,
		LeiosTxValidator:                n.ledgerState,
		LeiosCertificateProvider:        leiosCerts,
		LeiosParentAnnouncementProvider: leiosParent,
		OpCertLedgerView: blockProducerLedgerView{
			ls: n.ledgerState,
		},
		EraParams: n.ledgerState,
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

// blockBroadcaster implements forging.BlockBroadcaster through synchronous
// local chain admission. Block proposals are requests, not notifications, so
// they must not wait in the asynchronous EventBus while the chain advances.
type blockBroadcaster struct {
	chain  *chain.Chain
	logger *slog.Logger
}

func (b *blockBroadcaster) AddBlock(
	block gledger.Block,
	_ []byte,
) error {
	if block == nil {
		return errors.New("proposed block is nil")
	}
	if b.chain == nil {
		return errors.New("chain unavailable")
	}
	if err := b.chain.AddLocalBlock(block); err != nil {
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

// GetPoolAndTotalActiveStake reads the sigma numerator and denominator for
// one pool inside a single metadata transaction.
//
// Two defects are fixed here, and both are load-bearing for consensus:
//
// dingo #3814 -- the denominator comes from LedgerView.GetTotalActiveStake,
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
// dingo #3815 -- both values are read through one db.MetadataTxn and one
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
	txn := db.MetadataTxn(false)
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
// denominator, is the pair of defects dingo #3814 and #3815 describe; make it
// a compile error rather than a silent consensus divergence.
var _ leader.StakeDistributionProvider = (*stakeDistributionAdapter)(nil)

// epochInfoAdapter adapts ledger.LedgerState to leader.EpochInfoProvider.
type epochInfoAdapter struct {
	ledgerState *ledger.LedgerState
}

// computeSchedule discovers the exact-rational coefficient with a runtime type
// assertion and silently falls back to the float64 accessor when it fails,
// which yields a strictly larger leadership threshold than the reference node's
// (dingo #2798). Make that drift a compile error;
// TestEpochInfoAdapterProvidesExactActiveSlotCoeff covers the same pairing.
var _ leader.ActiveSlotCoeffRatProvider = (*epochInfoAdapter)(nil)

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

func (a *epochInfoAdapter) ConsensusModeForEpoch(
	epoch uint64,
) consensus.ConsensusMode {
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

// ChainTip returns the ledger-applied tip. LedgerState.Tip reads one atomic
// tip snapshot, so the returned slot and hash are always from the same tip.
func (a *slotClockAdapter) ChainTip() ocommon.Point {
	return a.ledgerState.Tip().Point
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
// while the pipeline replays. It is NOT the header frontier: that is
// chain.HeaderTip(), and nothing in the forge gate reads it. The primary chain
// returns its tip under one lock, so the returned slot and hash are always
// from the same tip.
func (a *slotClockAdapter) PrimaryChainTip() ocommon.Point {
	return a.ledgerState.PrimaryChainTip().Point
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

// leiosPipelineAdapter adapts leios.PipelineManager and the primary chain to
// the narrow Leios interfaces the forge loop expects.
type leiosPipelineAdapter struct {
	mgr                   *leios.PipelineManager
	chain                 leiosParentChain
	endorserBlockTxHashes func(ebHash []byte, ebSlot uint64) ([]string, bool)
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
	ebSlot uint64,
) ([]string, bool) {
	if a.endorserBlockTxHashes == nil {
		return nil, false
	}
	return a.endorserBlockTxHashes(ebHash.Bytes(), ebSlot)
}

func (a *leiosPipelineAdapter) MarkEndorserBlockEmbedded(
	ebHash lcommon.Blake2b256,
	ebSlot uint64,
) {
	a.mgr.MarkEmbedded(ebSlot, ebHash)
}

func (a *leiosPipelineAdapter) ParentLeiosAnnouncement() (
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
	ledgerState    forgedBlockValidationState
	fullValidation bool
}

type forgedBlockValidationState interface {
	ValidateForgedBlock(gledger.Block, []byte) error
	ValidateBlockReferenceScripts(gledger.Block) error
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

func (a *forgedBlockValidatorAdapter) ValidateForgedBlock(
	block gledger.Block,
	blockCbor []byte,
) error {
	if !a.fullValidation {
		return a.ledgerState.ValidateBlockReferenceScripts(block)
	}
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
