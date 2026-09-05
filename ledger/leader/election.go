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

package leader

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/gouroboros/consensus"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
)

// StakeDistributionProvider provides stake distribution data for leader election.
type StakeDistributionProvider interface {
	// GetPoolAndTotalActiveStake returns the sigma numerator (this pool's
	// stake) and the sigma denominator (total active stake) for the given
	// snapshot epoch, which callers select with praos.StakeSnapshotEpoch.
	//
	// Both values MUST be read from a single consistent view of the
	// snapshot. Reading them through two separate transactions lets a
	// snapshot re-capture land between them and yields a sigma whose
	// numerator and denominator come from different writes -- a leader
	// schedule that is not reproducible from either snapshot alone
	// (dingo #3815). The pair is returned by one method precisely so that
	// no implementation can express the torn read.
	//
	// The denominator MUST come from the same store accessor the header
	// verification path resolves it through
	// (Metadata().GetTotalActiveStake), so that a node cannot forge against
	// one denominator and validate against another (dingo #3814). See the
	// reference-rule commentary below this interface for what that value
	// is and why it must not be re-derived per code path.
	GetPoolAndTotalActiveStake(
		epoch uint64,
		poolKeyHash []byte,
	) (poolStake uint64, totalActiveStake uint64, err error)
}

// The sigma denominator, per the cardano-ledger reference implementation
// (IntersectMBO/cardano-ledger@9bac33a, master, 2026-09-03). Written down
// here because two prior investigations (dingo #2798 and #3626) were sent to
// the wrong cause by a stale comment that called this "the sum of all pool
// stakes".
//
// The reference computes the denominator as a sum over resolved stake
// CREDENTIALS, not over the per-pool distribution:
//
//	total = sum { utxoStake(c) + accountBalance(c)
//	            | c is a REGISTERED credential
//	              and c delegates to SOME pool id }
//
//   - Cardano/Ledger/State/Stake.hs:156-160 (sumAllActiveStake; an empty
//     credential set floors at 1 lovelace, not 0)
//   - Cardano/Ledger/State/SnapShots.hs:419-426 (mkSnapShot, the sole
//     production construction: ssTotalActiveStake = sumAllActiveStake
//     ssActiveStake)
//   - Cardano/Ledger/State/SnapShots.hs:472,486 (pdTotalActiveStake is that
//     value verbatim; it is never recomputed from the pool map)
//
// The resolution predicate checks exactly two things -- the credential is in
// the accounts map, and its stake-pool delegation is Just -- and does NOT
// check that the target pool is registered; the pool map is not even in
// scope (Cardano/Ledger/State/Stake.hs:217-246,
// resolveActiveInstantStakeCredentials; Conway/State/Stake.hs:123-130,
// which Dijkstra reuses).
//
// Numerators come only from REGISTERED pools, keyed by psStakePools
// (Cardano/Ledger/State/SnapShots.hs:206-207,237,429-438,471-488,
// calculatePoolDistr').
//
// It is tempting to read that asymmetry as "stake delegated to a retired or
// unregistered pool belongs in the denominator and in no numerator". That
// state is UNREACHABLE in the reference, so the sum of the numerators does
// equal the denominator. Two rules keep it so, and neither lives in the
// stake computation:
//
//   - DELEG rejects a delegation naming an unregistered pool
//     (Conway/Rules/Deleg.hs:218-233,
//     DelegateeStakePoolNotRegisteredDELEG).
//   - POOLREAP clears the delegations of a retiring pool in the SAME state
//     update that drops it from psStakePools
//     (Shelley/Rules/PoolReap.hs:214-228,238-240,
//     removeStakePoolDelegations . delegsToClear), so those credentials
//     leave the denominator too rather than lingering in it.
//   - An assertion enforces the pair
//     (Shelley/Rules/Ledger.hs:274-279,453-468, "Reverse stake pool
//     delegations must match").
//
// Dingo relies on the same invariant, maintained at the same point: see
// ledger/poolreap.go, which calls ClearDelegationsToRetiredPool for each
// reaped pool (dingo #3794 -- failing to clear inflates the total active
// stake above the network's and makes every other pool's threshold too
// small). Dingo also runs its SNAP stake read before POOLREAP, matching
// EPOCH's sub-rule order (Conway/Rules/Epoch.hs:289-294; dingo
// ledger/chainsync.go epoch-rollover step list).
//
// Consequences for this package: summing the numerators is the correct
// denominator ONLY while that invariant holds. It is therefore not a safe
// thing to derive independently in a second code path -- hence the single
// accessor required below (dingo #3814).
//
// One thing the reference does NOT do: there is no stake-credential
// inactivity gate. CIP-0163-style inactivity in the reference is DRep
// expiry, applied only to the DRep voting ratio in RATIFY
// (Conway/Rules/Ratify.hs:258-281); it never touches ActiveStake,
// ssTotalActiveStake, or PoolDistr, and accounts carry no activity field
// (Conway/State/Account.hs:60-80).

// EpochInfoProvider provides epoch-related information.
type EpochInfoProvider interface {
	// CurrentEpoch returns the current epoch number.
	CurrentEpoch() uint64

	// EpochNonce returns the nonce for the given epoch.
	EpochNonce(epoch uint64) []byte

	// NextEpochNonceReadyEpoch reports the next epoch number when the
	// upcoming epoch's nonce is already stable and its leader schedule can
	// be precomputed immediately. Returns false when startup should wait
	// for the normal nonce-ready event path instead.
	NextEpochNonceReadyEpoch() (uint64, bool)

	// EpochSlotRange returns the absolute slot range for an epoch, resolved
	// against the ledger hard-fork summary so Byron prefixes and variable
	// epoch lengths are respected.
	EpochSlotRange(epoch uint64) (EpochSlotRange, error)

	// EpochForSlot returns the epoch containing slot, resolved against
	// the ledger hard-fork summary so era boundaries and variable epoch
	// lengths are respected. Returns an error when slot falls outside
	// the known epoch range (the caller treats that as "schedule
	// unknown" and declines to produce).
	EpochForSlot(slot uint64) (uint64, error)

	// ActiveSlotCoeff returns the active slot coefficient (f parameter).
	ActiveSlotCoeff() float64

	// ConsensusModeForEpoch returns the consensus mode (TPraos or
	// CPraos) governing leader eligibility for the given epoch. The
	// schedule calculator threads this into VRF input construction
	// and threshold derivation; passing the wrong mode produces a
	// leader-slot list that cardano-node will reject.
	ConsensusModeForEpoch(epoch uint64) consensus.ConsensusMode
}

// ActiveSlotCoeffRatProvider is an optional extension of EpochInfoProvider
// for providers that can supply the active slot coefficient (f) as the exact
// rational written in the Shelley genesis rather than a float64.
//
// It is a separate, optionally-implemented interface rather than a method on
// EpochInfoProvider so existing implementations keep compiling. computeSchedule
// prefers it whenever the provider satisfies it and returns a non-nil value,
// and logs the coefficient actually used so a fallback to the float64 form is
// visible in the "leader schedule calculated" record.
//
// The distinction matters because the reference node derives its leadership
// threshold from the exact genesis rational. A float64 round trip of 1/20
// yields 3602879701896397/2^56, which is strictly larger, so every per-slot
// threshold is strictly larger and the resulting eligible-slot set is a strict
// superset of the reference's. dingo's own header verification already uses the
// exact rational, so the float64 form also let the forge path disagree with
// dingo's own validation path. See Calculator.ActiveSlotCoeffRat.
type ActiveSlotCoeffRatProvider interface {
	// ActiveSlotCoeffRat returns the exact active slot coefficient, or nil
	// when the genesis value is unavailable.
	ActiveSlotCoeffRat() *big.Rat
}

// ScheduleStore persists computed schedules for later reuse.
type ScheduleStore interface {
	LoadSchedule(epoch uint64, poolId lcommon.PoolKeyHash) (*Schedule, error)
	SaveSchedule(schedule *Schedule) error
}

// markSnapshotType names the stake snapshot generation the Praos leader check
// reads, for the audit log only. StakeDistributionProvider is documented to
// resolve the mark snapshot selected by praos.StakeSnapshotEpoch; the string is
// duplicated here rather than imported from database/models to keep this
// package free of a database dependency.
const markSnapshotType = "mark"

// consensusModeName renders a consensus mode for logging. gouroboros'
// ConsensusMode is a bare int with no String method, and the numeric value is
// not self-describing in an operator-facing audit record.
func consensusModeName(mode consensus.ConsensusMode) string {
	switch mode {
	case consensus.ConsensusModeTPraos:
		return "tpraos"
	case consensus.ConsensusModeCPraos:
		return "cpraos"
	default:
		return fmt.Sprintf("unknown(%d)", int(mode))
	}
}

// thresholdHex renders a leadership threshold for logging. The threshold is a
// 256- or 512-bit integer, so hex keeps the record compact and directly
// comparable against a certified-natural VRF value.
func thresholdHex(threshold *big.Int) string {
	if threshold == nil {
		return ""
	}
	return threshold.Text(16)
}

// maxCachedSchedules is the number of epoch schedules to keep in memory.
// We keep the current and previous epoch to handle slots near boundaries.
const maxCachedSchedules = 3

// Election manages leader election for a stake pool.
// It maintains schedules for recent epochs and refreshes them on epoch
// transitions (from block processing events) or on demand when the
// slot clock advances into an epoch without a cached schedule.
//
// Schedule computation (VRF for every slot in an epoch) is expensive and
// runs without holding the election lock so that ShouldProduceBlock remains
// a fast, lock-free lookup on the forger's hot path.
type Election struct {
	poolId      lcommon.PoolKeyHash
	poolVrfSkey []byte

	stakeProvider StakeDistributionProvider
	epochProvider EpochInfoProvider
	eventBus      *event.EventBus
	logger        *slog.Logger
	scheduleStore ScheduleStore

	mu             sync.RWMutex
	schedules      map[uint64]*Schedule // epoch -> schedule
	running        bool
	cancel         context.CancelFunc
	stopCh         chan struct{} // signals the monitoring goroutine to exit
	computeCh      chan uint64   // requests background schedule computation
	subscriptionId event.EventSubscriberId
	nonceReadySub  event.EventSubscriberId
	metrics        *electionMetrics

	// wg tracks epochTransitionLoop, epochNonceReadyLoop,
	// scheduleComputeLoop, and the ctx-monitor goroutine, so Stop can
	// actually wait for all of them to exit rather than merely signaling
	// them (closing stopCh/computeCh, cancelling ctx, unsubscribing) and
	// returning immediately. A plain signal-and-return was fine when the
	// only caller was a full process shutdown, but the live database
	// restore/truncate path (node_lifecycle.go) calls Stop and then
	// closes/reopens the node's *database.Database/*ledger.LedgerState
	// while the process keeps running: RefreshScheduleForEpoch (driven by
	// any of these goroutines) reads stakeProvider/epochProvider, both
	// bound to whatever ledgerState existed at construction time
	// (initBlockForger), so a goroutine still in flight when Stop returns
	// can keep running against it after that ledgerState has already been
	// closed and replaced.
	wg sync.WaitGroup
}

// NewElection creates a new leader election manager for a stake pool.
func NewElection(
	poolId lcommon.PoolKeyHash,
	poolVrfSkey []byte,
	stakeProvider StakeDistributionProvider,
	epochProvider EpochInfoProvider,
	eventBus *event.EventBus,
	logger *slog.Logger,
) *Election {
	if logger == nil {
		logger = slog.Default()
	}
	return &Election{
		poolId:        poolId,
		poolVrfSkey:   poolVrfSkey,
		stakeProvider: stakeProvider,
		epochProvider: epochProvider,
		eventBus:      eventBus,
		logger:        logger,
	}
}

// SetScheduleStore configures an optional persistent schedule store.
func (e *Election) SetScheduleStore(store ScheduleStore) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.scheduleStore = store
}

// SetPromRegistry enables leader election metrics.
func (e *Election) SetPromRegistry(reg prometheus.Registerer) {
	if reg == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.metrics == nil {
		e.metrics = initElectionMetrics(reg)
	}
}

// Start begins listening for epoch transitions and maintaining schedules.
// The provided context controls the election's lifecycle. When the context is
// canceled, the election will automatically stop and clean up resources.
//
// The initial schedule for the current epoch is computed asynchronously in
// the background so Start returns immediately and the forger can begin its
// slot-aligned loop without delay. The next epoch is queued later, once the
// ledger reports that its nonce has reached the stability cutoff.
func (e *Election) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel
	e.running = true
	e.stopCh = make(chan struct{})
	e.schedules = make(map[uint64]*Schedule)
	e.computeCh = make(chan uint64, 4)

	// Subscribe to epoch transitions using a channel so we can drain
	// stale events during rapid sync (e.g., devnet with 500ms epochs).
	var evtCh <-chan event.Event
	e.subscriptionId, evtCh = e.eventBus.Subscribe(
		event.EpochTransitionEventType,
	)

	if evtCh == nil {
		e.logger.Warn(
			"event bus not available, epoch transitions will not be tracked",
			"component", "leader",
		)
	} else {
		e.wg.Go(func() {
			e.epochTransitionLoop(ctx, evtCh)
		})
	}

	var nonceReadyCh <-chan event.Event
	e.nonceReadySub, nonceReadyCh = e.eventBus.Subscribe(
		event.EpochNonceReadyEventType,
	)
	if nonceReadyCh == nil {
		e.logger.Warn(
			"event bus not available, next-epoch precompute will not be tracked",
			"component",
			"leader",
		)
	} else {
		e.wg.Go(func() {
			e.epochNonceReadyLoop(ctx, nonceReadyCh)
		})
	}
	// Captured into a local before spawning: e.computeCh is read here while
	// Start still holds e.mu, but the goroutine below reads it again at
	// whatever time it actually gets scheduled to run, with no lock -- an
	// immediate Stop right after Start (as TestElectionStartStop does) can
	// nil the field out first, racing this goroutine's read of the live
	// field. The local copy is never touched by anything else, so passing
	// it in is race-free regardless of scheduling order.
	computeCh := e.computeCh
	e.wg.Go(func() {
		e.scheduleComputeLoop(ctx, computeCh)
	})

	// Kick off initial schedule computation for the current epoch.
	currentEpoch := e.epochProvider.CurrentEpoch()
	select {
	case e.computeCh <- currentEpoch:
	default:
	}
	if nextEpoch, ok := e.epochProvider.NextEpochNonceReadyEpoch(); ok {
		select {
		case e.computeCh <- nextEpoch:
		default:
		}
		e.logger.Info(
			"next epoch nonce already stable at startup, precomputing leader schedule",
			"component",
			"leader",
			"current_epoch",
			currentEpoch,
			"ready_epoch",
			nextEpoch,
		)
	}

	// Monitor context cancellation to automatically stop.
	// The goroutine exits when either the context is canceled or Stop() is called.
	//
	// Tracked in e.wg (like the three loops above), not left to dangle:
	// otherwise a completed Stop() could return with this goroutine still
	// alive, watching the now-defunct ctx/stopCh from this Start generation.
	// A later Start() on the same *Election creates a new ctx/stopCh, but
	// does nothing about a stale monitor from a previous generation still
	// running -- if THAT ctx's parent is ever cancelled afterward, the
	// stale goroutine would call e.Stop() on the new, currently-running
	// generation it has no business touching.
	stopCh := e.stopCh
	e.wg.Go(func() {
		select {
		case <-stopCh:
			// Stop() was called directly, goroutine should exit.
			return
		case <-ctx.Done():
			// ctx can be canceled either because Stop() itself was called
			// directly (which always closes stopCh strictly before its
			// own e.cancel() call below) or because the caller's parent
			// context died externally -- and since both channels can be
			// simultaneously ready by the time this select actually runs,
			// Go may have picked this case even though stopCh is also
			// already closed. Re-check stopCh, non-blockingly: if it's
			// already closed, a direct Stop() is the reason ctx died and
			// must not be re-entered here -- a second, concurrent Stop()
			// call would deadlock waiting on e.wg for this very goroutine
			// (now tracked above). Only a genuinely external cancellation
			// (stopCh still open) should trigger our own Stop() call.
			select {
			case <-stopCh:
				return
			default:
			}
			_ = e.Stop()
		}
	})

	e.logger.Info(
		"leader election started",
		"component", "leader",
		"pool_id", hex.EncodeToString(e.poolId[:]),
	)

	return nil
}

// epochTransitionLoop reads epoch transition events from the channel,
// draining any queued stale events so only the latest is processed.
// This prevents wasted schedule recalculations during rapid sync.
func (e *Election) epochTransitionLoop(
	ctx context.Context,
	evtCh <-chan event.Event,
) {
	for evt := range evtCh {
		// Drain any queued events, keeping only the latest.
		latest := evt
	drain:
		for {
			select {
			case newer, ok := <-evtCh:
				if !ok {
					return
				}
				latest = newer
			default:
				break drain
			}
		}

		if ctx.Err() != nil {
			return
		}

		epochEvent, ok := latest.Data.(event.EpochTransitionEvent)
		if !ok {
			e.logger.Error(
				"invalid event data for epoch transition",
				"component", "leader",
			)
			continue
		}

		e.logger.Info(
			"epoch transition, refreshing leader schedule",
			"component", "leader",
			"new_epoch", epochEvent.NewEpoch,
		)
		if err := e.RefreshScheduleForEpoch(
			ctx,
			epochEvent.NewEpoch,
		); err != nil {
			e.logger.Error(
				"failed to refresh schedule",
				"component", "leader",
				"epoch", epochEvent.NewEpoch,
				"error", err,
			)
		}
	}
}

func (e *Election) epochNonceReadyLoop(
	ctx context.Context,
	evtCh <-chan event.Event,
) {
	for evt := range evtCh {
		latest := evt
	drain:
		for {
			select {
			case newer, ok := <-evtCh:
				if !ok {
					return
				}
				latest = newer
			default:
				break drain
			}
		}

		if ctx.Err() != nil {
			return
		}

		readyEvent, ok := latest.Data.(event.EpochNonceReadyEvent)
		if !ok {
			e.logger.Error(
				"invalid event data for epoch nonce readiness",
				"component", "leader",
			)
			continue
		}

		e.logger.Info(
			"next epoch nonce is stable, precomputing leader schedule",
			"component", "leader",
			"current_epoch", readyEvent.CurrentEpoch,
			"ready_epoch", readyEvent.ReadyEpoch,
			"cutoff_slot", readyEvent.CutoffSlot,
		)
		e.queueScheduleCompute(readyEvent.ReadyEpoch)
	}
}

// scheduleComputeLoop processes background schedule computation requests.
// When ShouldProduceBlock detects a missing schedule for a slot's epoch,
// it sends the epoch number to computeCh. This goroutine picks it up and
// computes the schedule without blocking the forger's hot path.
func (e *Election) scheduleComputeLoop(
	ctx context.Context,
	computeCh <-chan uint64,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case epoch := <-computeCh:
			// Skip if schedule already exists
			e.mu.RLock()
			_, exists := e.schedules[epoch]
			e.mu.RUnlock()
			if exists {
				continue
			}

			e.logger.Info(
				"computing leader schedule",
				"component", "leader",
				"epoch", epoch,
			)
			if err := e.RefreshScheduleForEpoch(
				ctx,
				epoch,
			); err != nil {
				e.logger.Warn(
					"background schedule compute failed",
					"component", "leader",
					"epoch", epoch,
					"error", err,
				)
			}
		}
	}
}

// Stop stops the leader election manager, waiting for epochTransitionLoop,
// epochNonceReadyLoop, and scheduleComputeLoop to actually exit before
// returning -- not just signaling them to stop. A plain signal-and-return
// was fine when the only caller was a full process shutdown, but the live
// database restore/truncate path (node_lifecycle.go) calls Stop and then
// closes/reopens the node's storage while the process keeps running: see
// the wg field's doc comment for why a goroutine still in flight when Stop
// returns is a real use-after-close risk here, not just a benign leak.
func (e *Election) Stop() error {
	e.mu.Lock()

	if !e.running {
		e.mu.Unlock()
		return nil
	}

	// Signal the monitoring goroutine to exit before canceling context.
	// This prevents the goroutine from calling Stop() again.
	if e.stopCh != nil {
		close(e.stopCh)
		e.stopCh = nil
	}
	// Nil out computeCh so ShouldProduceBlock cannot send after Stop.
	e.computeCh = nil
	if e.cancel != nil {
		e.cancel()
	}
	subscriptionId := e.subscriptionId
	nonceReadySub := e.nonceReadySub
	e.subscriptionId = 0
	e.nonceReadySub = 0
	e.running = false
	e.schedules = nil

	e.mu.Unlock()

	// Must run with e.mu released: RefreshScheduleForEpoch and
	// scheduleComputeLoop both take e.mu (RLock), so waiting for them to
	// exit while still holding the write lock here would deadlock.
	if subscriptionId != 0 {
		e.eventBus.UnsubscribeAndWait(
			event.EpochTransitionEventType,
			subscriptionId,
		)
	}
	if nonceReadySub != 0 {
		e.eventBus.UnsubscribeAndWait(
			event.EpochNonceReadyEventType,
			nonceReadySub,
		)
	}
	e.wg.Wait()

	e.logger.Info("leader election stopped", "component", "leader")
	return nil
}

// RefreshSchedule recalculates the leader schedule for the current epoch.
func (e *Election) RefreshSchedule(ctx context.Context) error {
	return e.RefreshScheduleForEpoch(ctx, e.epochProvider.CurrentEpoch())
}

// RefreshScheduleForEpoch computes the leader schedule for the given epoch.
// The expensive VRF computation runs WITHOUT holding the election lock so
// that ShouldProduceBlock callers are never blocked. The lock is only held
// briefly to store the result.
func (e *Election) RefreshScheduleForEpoch(
	ctx context.Context,
	epoch uint64,
) error {
	e.mu.RLock()
	running := e.running
	e.mu.RUnlock()
	if !running {
		return nil
	}

	if schedule, err := e.loadPersistedSchedule(epoch); err != nil {
		e.logger.Warn(
			"failed to load persisted leader schedule",
			"component", "leader",
			"epoch", epoch,
			"error", err,
		)
	} else if schedule != nil {
		valid, reason, err := e.validatePersistedSchedule(epoch, schedule)
		if err != nil {
			e.logger.Warn(
				"failed to validate persisted leader schedule",
				"component", "leader",
				"epoch", epoch,
				"error", err,
			)
		} else if valid {
			e.storeSchedule(epoch, schedule)
			return nil
		} else {
			e.logger.Info(
				"ignoring stale persisted leader schedule",
				"component", "leader",
				"epoch", epoch,
				"reason", reason,
			)
		}
	}

	schedule, err := e.computeSchedule(ctx, epoch)
	if err != nil {
		return err
	}
	if schedule == nil {
		return nil // no stake or nonce not available
	}
	e.storeSchedule(epoch, schedule)
	e.persistSchedule(schedule)
	return nil
}

func (e *Election) loadPersistedSchedule(
	epoch uint64,
) (*Schedule, error) {
	e.mu.RLock()
	store := e.scheduleStore
	e.mu.RUnlock()
	if store == nil {
		return nil, nil
	}
	schedule, err := store.LoadSchedule(epoch, e.poolId)
	if err != nil {
		return nil, err
	}
	if schedule == nil {
		return nil, nil
	}
	e.logger.Info(
		"loaded persisted leader schedule",
		"component", "leader",
		"epoch", epoch,
		"leader_slots", schedule.SlotCount(),
		"leader_slot_list", schedule.LeaderSlotsSnapshot(),
	)
	return schedule, nil
}

func (e *Election) validatePersistedSchedule(
	epoch uint64,
	schedule *Schedule,
) (bool, string, error) {
	if schedule == nil {
		return false, "schedule missing", nil
	}

	// Reject schedules whose compute path is no longer compatible with the
	// running build — pre-PR persisted schedules were derived without the
	// per-era consensus mode and would mis-pick leader slots if reused.
	// Pre-format-version entries decode to FormatVersion == 0.
	if schedule.FormatVersion != ScheduleFormatVersion {
		return false, fmt.Sprintf(
			"schedule format version mismatch: got %d want %d",
			schedule.FormatVersion,
			ScheduleFormatVersion,
		), nil
	}

	expectedNonce := e.epochProvider.EpochNonce(epoch)
	if len(expectedNonce) == 0 {
		return false, "epoch nonce unavailable", nil
	}
	if !bytes.Equal(expectedNonce, schedule.EpochNonce) {
		return false, "epoch nonce changed", nil
	}

	epochRange, err := e.epochProvider.EpochSlotRange(epoch)
	if err != nil {
		return false, "", fmt.Errorf("get epoch slot range: %w", err)
	}
	if epochRange.SlotCount == 0 {
		return false, "epoch slot count is zero", nil
	}
	if epochRange.StartSlot > ^uint64(0)-epochRange.SlotCount {
		return false, "", fmt.Errorf(
			"epoch slot range overflows uint64: start=%d count=%d",
			epochRange.StartSlot,
			epochRange.SlotCount,
		)
	}
	epochEndSlot := epochRange.StartSlot + epochRange.SlotCount
	for _, slot := range schedule.LeaderSlotsSnapshot() {
		if slot < epochRange.StartSlot || slot >= epochEndSlot {
			return false, "leader slot outside epoch range", nil
		}
	}

	snapshotEpoch := praos.StakeSnapshotEpoch(epoch)
	// One atomic read: revalidating a persisted schedule against a torn
	// (numerator, denominator) pair could accept a schedule that matches
	// neither snapshot, or discard a still-valid one (dingo #3815).
	poolStake, totalStake, err := e.stakeProvider.GetPoolAndTotalActiveStake(
		snapshotEpoch,
		e.poolId[:],
	)
	if err != nil {
		return false, "", fmt.Errorf(
			"get pool and total active stake for epoch %d: %w",
			snapshotEpoch,
			err,
		)
	}
	if poolStake != schedule.PoolStake {
		return false, "pool stake changed", nil
	}
	if totalStake != schedule.TotalStake {
		return false, "total stake changed", nil
	}

	return true, "", nil
}

func (e *Election) persistSchedule(schedule *Schedule) {
	e.mu.RLock()
	store := e.scheduleStore
	e.mu.RUnlock()
	if store == nil || schedule == nil {
		return
	}
	if err := store.SaveSchedule(schedule); err != nil {
		e.logger.Warn(
			"failed to persist leader schedule",
			"component", "leader",
			"epoch", schedule.Epoch,
			"error", err,
		)
		return
	}
	e.logger.Info(
		"persisted leader schedule",
		"component", "leader",
		"epoch", schedule.Epoch,
		"leader_slots", schedule.SlotCount(),
		"leader_slot_list", schedule.LeaderSlotsSnapshot(),
	)
}

// computeSchedule gathers stake data, epoch nonce, and runs VRF for every
// slot in the epoch. This is the expensive operation (~70ms per slot) and
// does NOT hold any lock.
func (e *Election) computeSchedule(
	ctx context.Context,
	currentEpoch uint64,
) (*Schedule, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Leader election uses the mark snapshot that is active for the epoch.
	snapshotEpoch := praos.StakeSnapshotEpoch(currentEpoch)

	// Read the sigma numerator and denominator together. Two separate
	// reads let a snapshot re-capture land between them, producing a
	// schedule computed from a sigma that exists in no single snapshot
	// (dingo #3815). The zero-stake short circuit below therefore happens
	// after the pair is in hand rather than between the two reads.
	stakeLookupStart := time.Now()
	poolStake, totalStake, err := e.stakeProvider.GetPoolAndTotalActiveStake(
		snapshotEpoch,
		e.poolId[:],
	)
	if e.metrics != nil {
		e.metrics.stakeLookupDuration.Observe(
			time.Since(stakeLookupStart).Seconds(),
		)
	}
	if err != nil {
		return nil, fmt.Errorf("get pool and total active stake: %w", err)
	}

	e.logger.Info(
		"pool stake from active snapshot",
		"component", "leader",
		"epoch", currentEpoch,
		"snapshot_epoch", snapshotEpoch,
		"pool_stake", poolStake,
	)
	if poolStake == 0 {
		e.logger.Info(
			"pool has no stake in active snapshot, skipping schedule computation",
			"component",
			"leader",
			"epoch",
			currentEpoch,
			"snapshot_epoch",
			snapshotEpoch,
		)
		return nil, nil
	}

	e.logger.Info(
		"total active stake from active snapshot",
		"component", "leader",
		"epoch", currentEpoch,
		"snapshot_epoch", snapshotEpoch,
		"total_stake", totalStake,
	)
	if totalStake == 0 {
		return nil, fmt.Errorf(
			"total stake is zero for epoch %d", snapshotEpoch,
		)
	}

	// Get epoch nonce. The nonce may not be available yet if the slot clock
	// fired the epoch transition before block processing computed the nonce.
	// In that case, skip this schedule — the next epoch transition will retry.
	epochNonce := e.epochProvider.EpochNonce(currentEpoch)
	if len(epochNonce) == 0 {
		e.logger.Info(
			"epoch nonce not yet available, skipping schedule",
			"component", "leader",
			"epoch", currentEpoch,
		)
		return nil, nil
	}

	epochRange, err := e.epochProvider.EpochSlotRange(currentEpoch)
	if err != nil {
		return nil, fmt.Errorf("get epoch slot range: %w", err)
	}

	calc := NewCalculator(e.epochProvider.ActiveSlotCoeff())
	// Prefer the exact Shelley genesis rational over the float64 form; see
	// ActiveSlotCoeffRatProvider.
	if ratProvider, ok := e.epochProvider.(ActiveSlotCoeffRatProvider); ok {
		if exactCoeff := ratProvider.ActiveSlotCoeffRat(); exactCoeff != nil {
			calc.ActiveSlotCoeffRat = exactCoeff
		}
	}

	mode := e.epochProvider.ConsensusModeForEpoch(currentEpoch)

	// Resolve the coefficient up front so an invalid genesis value fails here
	// with a clear message instead of deep inside the VRF loop, and so the
	// audit log below can report the exact value that was used.
	activeSlotCoeff, err := calc.activeSlotCoeffRat()
	if err != nil {
		return nil, fmt.Errorf("resolve active slot coefficient: %w", err)
	}

	vrfEvalStart := time.Now()
	schedule, err := calc.CalculateSchedule(
		currentEpoch,
		epochRange,
		e.poolId,
		e.poolVrfSkey,
		poolStake,
		totalStake,
		epochNonce,
		mode,
	)
	if e.metrics != nil {
		e.metrics.vrfEvalDurationSeconds.Observe(
			time.Since(vrfEvalStart).Seconds(),
		)
	}
	if err != nil {
		return nil, fmt.Errorf("calculate schedule: %w", err)
	}

	// One O(1)-per-epoch record carrying every input to the leader check, so a
	// schedule that disagrees with `cardano-cli query leadership-schedule` can
	// be diffed against the reference node's `query stake-snapshot` and
	// `query protocol-state` from logs alone, without re-running with extra
	// instrumentation (dingo #2798). Never log per slot.
	e.logger.Info(
		"leader schedule calculated",
		"component", "leader",
		"epoch", currentEpoch,
		"snapshot_epoch", snapshotEpoch,
		"snapshot_type", markSnapshotType,
		"epoch_start_slot", epochRange.StartSlot,
		"epoch_slot_count", epochRange.SlotCount,
		"epoch_nonce", hex.EncodeToString(epochNonce),
		"pool_stake", poolStake,
		"total_stake", totalStake,
		"stake_ratio", schedule.StakeRatio(),
		"active_slot_coeff", activeSlotCoeff.RatString(),
		"consensus_mode", consensusModeName(mode),
		"leader_threshold", thresholdHex(schedule.Threshold),
		"leader_slots", schedule.SlotCount(),
		"leader_slot_list", schedule.LeaderSlotsSnapshot(),
	)
	if e.metrics != nil {
		slotsChecked := epochRange.SlotCount
		slotsWon := uint64(len(schedule.LeaderSlotsSnapshot()))
		slotsNotWon := uint64(0)
		if slotsWon <= slotsChecked {
			slotsNotWon = slotsChecked - slotsWon
		}
		e.metrics.lastEpochSlotsChecked.Set(float64(slotsChecked))
		e.metrics.lastEpochSlotsWon.Set(float64(slotsWon))
		e.metrics.lastEpochSlotsNotWon.Set(float64(slotsNotWon))
		e.metrics.lastEvaluatedEpochNumber.Set(float64(currentEpoch))
	}

	return schedule, nil
}

// storeSchedule saves a computed schedule under a brief write lock and
// prunes old epochs from the cache.
func (e *Election) storeSchedule(epoch uint64, schedule *Schedule) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.running {
		return
	}
	e.schedules[epoch] = schedule

	// Prune old schedules to bound memory usage
	for ep := range e.schedules {
		if epoch >= maxCachedSchedules &&
			ep < epoch-maxCachedSchedules+1 {
			delete(e.schedules, ep)
		}
	}
}

func (e *Election) queueScheduleCompute(epoch uint64) {
	e.mu.RLock()
	computeCh := e.computeCh
	e.mu.RUnlock()
	if computeCh == nil {
		return
	}
	select {
	case computeCh <- epoch:
	default:
	}
}

// ShouldProduceBlock returns true if this pool should produce a block for the
// slot. This is a pure lookup with no database access — it checks the cached
// schedule for the slot's epoch. If no schedule is cached, it requests a
// background computation and returns false.
func (e *Election) ShouldProduceBlock(slot uint64) bool {
	slotEpoch, err := e.epochProvider.EpochForSlot(slot)
	if err != nil {
		e.logger.Debug(
			"slot outside known epoch range; declining production",
			"component", "leader",
			"slot", slot,
			"error", err,
		)
		return false
	}

	e.mu.RLock()
	var schedule *Schedule
	if e.schedules != nil {
		schedule = e.schedules[slotEpoch]
	}
	computeCh := e.computeCh
	e.mu.RUnlock()

	if schedule == nil {
		// Schedule not yet computed for this epoch.
		// Request background computation (non-blocking).
		e.logger.Debug(
			"no leader schedule for epoch, requesting computation",
			"component", "leader",
			"slot", slot,
			"epoch", slotEpoch,
		)
		if computeCh != nil {
			e.queueScheduleCompute(slotEpoch)
		}
		return false
	}
	isLeader := schedule.IsLeaderForSlot(slot)
	if e.metrics != nil {
		e.metrics.slotChecksTotal.Inc()
		if isLeader {
			e.metrics.slotWonTotal.Inc()
		} else {
			e.metrics.slotNotWonTotal.Inc()
		}
	}
	return isLeader
}

// CurrentSchedule returns the leader schedule for the current epoch, or nil.
func (e *Election) CurrentSchedule() *Schedule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.schedules == nil {
		return nil
	}
	epoch := e.epochProvider.CurrentEpoch()
	return e.schedules[epoch]
}

// ScheduleForEpoch returns the cached leader schedule for a specific epoch,
// or nil if not computed.
func (e *Election) ScheduleForEpoch(epoch uint64) *Schedule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.schedules == nil {
		return nil
	}
	return e.schedules[epoch]
}

// NextLeaderSlot returns the next slot where this pool is leader, starting from
// the given slot. Returns 0 and false if no leader slot is found.
func (e *Election) NextLeaderSlot(fromSlot uint64) (uint64, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.schedules == nil {
		return 0, false
	}

	epoch, err := e.epochProvider.EpochForSlot(fromSlot)
	if err != nil {
		return 0, false
	}
	schedule := e.schedules[epoch]
	if schedule != nil {
		for _, slot := range schedule.LeaderSlots {
			if slot >= fromSlot {
				return slot, true
			}
		}
	}

	// No leader slot found in the current epoch; check the next epoch's
	// cached schedule in case we are near an epoch boundary.
	nextSchedule := e.schedules[epoch+1]
	if nextSchedule != nil {
		for _, slot := range nextSchedule.LeaderSlots {
			if slot >= fromSlot {
				return slot, true
			}
		}
	}

	return 0, false
}
