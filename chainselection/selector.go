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

package chainselection

import (
	"bytes"
	"cmp"
	"context"
	"io"
	"log/slog"
	"math"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	nodeconsensus "github.com/blinklabs-io/dingo/consensus"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	okeepalive "github.com/blinklabs-io/gouroboros/protocol/keepalive"
)

// safeAddUint64 returns a + b, clamped to math.MaxUint64 on overflow.
func safeAddUint64(a, b uint64) uint64 {
	if a > math.MaxUint64-b {
		return math.MaxUint64
	}
	return a + b
}

const (
	defaultEvaluationInterval = 10 * time.Second
	// A connected peer may need one probe period plus the full server timeout
	// to report activity.
	defaultStaleTipThreshold = time.Duration(okeepalive.DefaultKeepAlivePeriod)*time.Second +
		okeepalive.ServerTimeout

	// DefaultMaxTrackedPeers is the maximum number of peers tracked by
	// the ChainSelector. When a new peer is added and the limit is reached,
	// the least-recently-updated peer is evicted. This bounds memory usage
	// and CPU cost of chain selection, preventing Sybil-based resource
	// exhaustion.
	DefaultMaxTrackedPeers = 200

	// catchUpPinBlockThreshold is the catch-up gap (in blocks, measured as
	// bestKnownPeerBlock - appliedLocalTipBlock) at or above which the node
	// is considered to be in deep catch-up. The anti-flap pin's behavior is
	// identical in catch-up and at the tip; this threshold is only used for
	// observability/diagnostics and to describe the two regimes.
	catchUpPinBlockThreshold = 100

	// catchUpPinHeadMargin is the maximum number of blocks a challenger may
	// lead the incumbent and still be treated as a head micro-fork (a
	// sibling at, or barely past, the same height) rather than a genuinely
	// longer chain. A challenger ahead by more than this margin is a real
	// longer chain and the pin releases so the node converges to it.
	catchUpPinHeadMargin = 2

	// catchUpPinStallTimeout is the progress-aware wall-clock escape window.
	// It is deliberately time-based, not slot-based: if the applied local tip
	// stops advancing for at least this long while the pin is engaged, the pin
	// releases so the node can switch away from a dead/stalled incumbent. This
	// is the key safety valve that guarantees the node can never pin to a
	// non-progressing peer forever.
	catchUpPinStallTimeout = 20 * time.Second

	// defaultSwitchBackCooldown is the default minimum time between
	// discretionary releases of the anti-flap pin (see
	// pinIncumbentDuringCatchUpLocked): per connection, before an abandoned
	// peer may reclaim the active connection, and globally, between any two
	// discretionary hand-offs. See ChainSelectorConfig.SwitchBackCooldown.
	defaultSwitchBackCooldown = 2 * time.Second
)

// safeBlockDiff computes the difference between two block numbers as int64,
// handling potential overflow by clamping to math.MaxInt64.
func safeBlockDiff(a, b uint64) int64 {
	if a >= b {
		diff := a - b
		if diff > math.MaxInt64 {
			return math.MaxInt64
		}
		return int64(diff)
	}
	diff := b - a
	if diff > math.MaxInt64 {
		return math.MinInt64
	}
	return -int64(diff)
}

// safeUint64ToInt64 converts uint64 to int64, clamping to math.MaxInt64 on overflow.
func safeUint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

// RollbackRegistrationOutcome is the result of handling a chainsync rollback
// for a connection the selector is not tracking (see
// registerPeerFromRollbackLocked). It is reported to
// ChainSelectorConfig.OnRollbackRegistration so the composition layer can count
// registrations and refusals without chainselection depending on a metrics
// library.
type RollbackRegistrationOutcome string

const (
	// RollbackRegistrationRegistered means the peer was registered from the
	// rollback and is now visible to chain selection.
	RollbackRegistrationRegistered RollbackRegistrationOutcome = "registered"
	// RollbackRegistrationClosedConnection means the rollback arrived for a
	// connection that is no longer live, so no entry was created.
	RollbackRegistrationClosedConnection RollbackRegistrationOutcome = "rejected_closed_connection"
	// RollbackRegistrationImplausibleTip means the advertised tip failed the
	// same plausibility bound the roll-forward path applies to a new peer.
	RollbackRegistrationImplausibleTip RollbackRegistrationOutcome = "rejected_implausible_tip"
	// RollbackRegistrationAtCapacity means the tracked-peer table was full and
	// no peer could be evicted to make room.
	RollbackRegistrationAtCapacity RollbackRegistrationOutcome = "rejected_at_capacity"
)

// PeerTipRejection says which plausibility bound refused a peer's tip. It is
// reported to ChainSelectorConfig.OnPeerTipRejected so the composition layer
// can count rejections without chainselection depending on a metrics library.
type PeerTipRejection string

const (
	// PeerTipRejectedObservedFrontier means the peer's delivered frontier
	// jumped too far ahead of the trusted reference.
	PeerTipRejectedObservedFrontier PeerTipRejection = "observed_frontier"
	// PeerTipRejectedAdvertisedTip means the peer's advertised tip exceeded
	// the bootstrap bound while no local block had been applied.
	PeerTipRejectedAdvertisedTip PeerTipRejection = "advertised_tip"
)

// ChainSelectorConfig holds configuration for the ChainSelector.
type ChainSelectorConfig struct {
	Consensus          nodeconsensus.Provider
	Logger             *slog.Logger
	EventBus           *event.EventBus
	EvaluationInterval time.Duration
	StaleTipThreshold  time.Duration
	SecurityParam      uint64
	GenesisMode        bool
	GenesisWindowSlots uint64
	// WallClockSlot returns the current wall-clock slot and whether it is
	// known. In Genesis mode a peer whose advertised tip slot plus the
	// Genesis window is below the wall-clock slot is treated as serving an
	// abandoned fork: it is not selectable and its headers are not applied.
	// Other peers' tips play no part in the decision. Optional; the rule is
	// inactive while nil or while it reports false.
	WallClockSlot func() (uint64, bool)
	// AppliedTip reads the live applied ledger tip. When set, peer-tip
	// admission bounds delivered frontiers against it rather than against the
	// tip last passed to SetLocalTip, which the node refreshes only on the
	// stall recycler's tick while the ledger may apply hundreds of blocks in
	// between. It reports false while no ledger is available, which keeps
	// the last SetLocalTip value. It must not block or take locks shared with
	// the selector.
	AppliedTip func() (ochainsync.Tip, bool)
	// PointOnLocalChain confirms a point against the current applied chain.
	// The Genesis ingress gate calls it without holding the selector mutex.
	PointOnLocalChain func(ocommon.Point) bool
	// HeaderCryptoVerificationGenerationCurrent reports whether a nonzero
	// producer-authorization generation still matches the live ledger state.
	// It is optional for callers that do not stage cryptographically verified
	// headers.
	HeaderCryptoVerificationGenerationCurrent func(uint64) bool
	// CurrentHeaderCryptoVerificationGeneration returns the current nonzero
	// ledger snapshot generation, or zero while publication is in flight.
	CurrentHeaderCryptoVerificationGeneration func() uint64
	// PinHeaderCryptoVerificationGeneration keeps a current generation stable
	// until the returned release function is called. Selector promotion uses
	// it to order a committed tip before any ledger snapshot publication that
	// invalidates its producer-authorization verdict.
	PinHeaderCryptoVerificationGeneration func(uint64) (func(), bool)
	// MinCorroboratingPeers is the number of distinct other eligible peers
	// that must report the same recent blocks as a candidate before that
	// candidate can drive chain selection in Genesis mode. It implements the
	// Ouroboros Genesis trust property that a fast (shallow) block source is
	// followed only while corroborated by independent peers. 0 disables
	// corroboration (density-only Genesis selection, the historical default).
	MinCorroboratingPeers int
	ConnectionLive        func(ouroboros.ConnectionId) bool
	// PeerIdentity maps a connection to the independence group used to
	// de-duplicate corroborating witnesses (peer governance's diversity
	// group). A connection for which it returns "" has no identity and
	// fails closed in corroboration: it is not corroborated as a candidate
	// and does not count as a witness. A nil hook groups by remote host.
	PeerIdentity       func(ouroboros.ConnectionId) string
	ConnectionEligible func(ouroboros.ConnectionId) bool
	ConnectionPriority func(ouroboros.ConnectionId) int
	MaxTrackedPeers    int // 0 means use DefaultMaxTrackedPeers
	// DisableEventSubscriptions leaves EventBus configured for publishing
	// selector events but skips automatic input subscriptions. This is useful
	// for deterministic replay harnesses that feed input events synchronously.
	DisableEventSubscriptions bool
	// BlockfetchLatency returns the EWMA first-block latency for a
	// connection and whether any samples exist. Used only to choose a
	// peer when two peers advertise the exact same selected block.
	BlockfetchLatency func(ouroboros.ConnectionId) (time.Duration, bool)
	// OnRollbackRegistration is called, outside the selector lock, with the
	// outcome of every attempt to register a peer from a chainsync rollback for
	// an untracked connection. Optional; the composition layer uses it to
	// export a counter.
	OnRollbackRegistration func(RollbackRegistrationOutcome)
	// OnPeerTipRejected is called once for every peer tip refused as
	// implausible, with the bound that refused it. It runs with the selector
	// lock held, so it must only do non-blocking work such as incrementing a
	// counter and must not call back into the selector. Optional.
	OnPeerTipRejected func(PeerTipRejection)
	// SwitchBackCooldown bounds the rate of active-connection handoffs
	// driven by the anti-flap pin's discretionary escapes -- longer-chain
	// and progress-stall (pinIncumbentDuringCatchUpLocked). Once the active
	// connection moves away from a peer, that peer cannot reclaim it through
	// either escape until this much time has passed, and no discretionary
	// hand-off to any connection happens within this much time of the
	// previous one, even if the challenger currently leads by more than
	// catchUpPinHeadMargin. This is a rate limit, not a correctness rule --
	// the challenger is still adopted once the cooldown expires, and the
	// mandatory incumbent-unselectable release is never gated -- and it
	// exists because peers whose delivered frontiers repeatedly leapfrog each
	// other by more than the margin can otherwise hand the chainsync/
	// blockfetch pipeline back and forth on every such crossing. 0 (the
	// zero value) is replaced with defaultSwitchBackCooldown by
	// NewChainSelector, matching EvaluationInterval and StaleTipThreshold.
	SwitchBackCooldown time.Duration
	// OnGenesisDensityDisconnect is called, outside the selector lock, once
	// per peer the Genesis Density Disconnector finds provably sparser than
	// another candidate chain. The composition layer disconnects and denies
	// the peer. The disconnector is inactive while this is nil, outside
	// Genesis mode, or while GenesisWindowSlots is zero: the 3k-slot
	// fallback is too short to compare densities. Evaluation runs at most
	// once per GenesisDensityEvaluationInterval.
	OnGenesisDensityDisconnect func(GenesisDensityDisconnect)
}

// ChainSelector tracks chain tips from multiple peers and selects the best
// chain using the active mode's comparison rules.
type ChainSelector struct {
	config          ChainSelectorConfig
	securityParam   uint64
	maxTrackedPeers int
	mode            SelectionMode
	peerTips        map[ouroboros.ConnectionId]*PeerChainTip
	// deliveredPeerTips survives admission-frontier rebuilds so delivered,
	// verified headers remain available as corroboration evidence.
	deliveredPeerTips map[ouroboros.ConnectionId]*PeerChainTip
	pendingPeerTips   map[ouroboros.ConnectionId][]peerTipAdmissionCandidate
	// One rolling snapshot per peer; candidate events retain only the bounded
	// admission updates needed to promote or rebuild that snapshot.
	pendingPeerTipFrontiers map[ouroboros.ConnectionId]*PeerChainTip
	// One unverified, connected header per peer may temporarily corroborate
	// verified candidates while that peer waits for the forecast horizon. It
	// is excluded from every selection and admission path.
	forecastHorizonWitnesses map[ouroboros.ConnectionId]forecastHorizonWitness
	committedPeerTips        map[ouroboros.ConnectionId]uint64
	eligible                 map[ouroboros.ConnectionId]bool
	priority                 map[ouroboros.ConnectionId]int
	evaluationTrigger        chan struct{}
	bestPeerConn             *ouroboros.ConnectionId
	localTip                 ochainsync.Tip
	// farTipClaims records, per connection, the most recent delivered
	// frontier that exceeded the catch-up plausibility ceiling. Entries are
	// provisional: they never enter peerTips and so never influence chain
	// selection, corroboration, or the Genesis exit horizon by themselves.
	// They exist so that a second connection delivering a similar far
	// frontier is accepted, the first claim then being marked corroborated
	// and bounding that connection's next update, and so that a lone
	// connection's run of connected headers can be counted. An accepted lone
	// claim remains here alongside its peer tip so it can be revoked, but is
	// never a reference for another peer -- see corroborateFarTipClaimLocked.
	// Bounded to maxTrackedPeers entries and pruned in deletePeerLocked.
	// Guarded by mutex.
	farTipClaims        map[ouroboros.ConnectionId]farTipClaim
	pendingFarTipClaims map[ouroboros.ConnectionId]farTipClaim
	// rejectedAdvertisedClaims holds the advertised tips of connections the
	// bootstrap advertised bound refused, oldest first, so a later connection
	// from another remote host advertising a tip within securityParam is
	// corroborated by it. Entries outlive their connection on purpose: a
	// refused connection is closed before the next honest peer arrives, so a
	// live-only witness set would let one lagging reference refuse every
	// honest peer in turn. Bounded to maxTrackedPeers entries. Guarded by
	// mutex.
	rejectedAdvertisedClaims []rejectedAdvertisedClaim
	// reportedPeerTipRejections holds the candidates whose refusal was
	// already warned about, counted and passed to OnPeerTipRejected, so a
	// replay or re-check does not report it again. Only staged candidates
	// are kept (see prunePeerTipRejectionReportsLocked), which bounds it by
	// the pending admission limits. Guarded by mutex.
	reportedPeerTipRejections map[peerTipRejectionKey]struct{}
	mutex                     sync.RWMutex
	ctx                       context.Context
	cancel                    context.CancelFunc

	// Anti-flap incumbent pin state (guarded by mutex).
	//
	// localTipProgressBlock records the previously observed APPLIED local tip
	// block number. localTipProgressAt records when the applied tip last moved
	// forward relative to that previous tip. The stall clock
	// (catchUpPinStallTimeout) is measured from localTipProgressAt and is only
	// reset on genuine forward progress, so repeated same-tip updates and
	// rollbacks never reset it, while post-rollback advancement does.
	localTipProgressBlock uint64
	localTipProgressAt    time.Time
	// nowFn is injectable for deterministic tests; defaults to time.Now.
	nowFn func() time.Time

	// switchBackCooldown is the resolved value of
	// ChainSelectorConfig.SwitchBackCooldown (defaulted by NewChainSelector).
	switchBackCooldown time.Duration
	// recentlyLeft records, per connection, when the active connection last
	// moved away from it. Read and written under mutex. See
	// switchBackDebouncedLocked and recordSwitchAwayLocked.
	recentlyLeft map[ouroboros.ConnectionId]time.Time
	// lastDiscretionarySwitchAt records when the active connection last moved
	// away from an incumbent via a DISCRETIONARY release of
	// pinIncumbentDuringCatchUpLocked (the longer-chain or progress-stall
	// escape) -- never via the mandatory incumbent-unselectable release, which
	// must stay instantaneous. It is the global counterpart to recentlyLeft:
	// recentlyLeft alone only debounces handing the connection BACK to the
	// SPECIFIC peer just abandoned, so three or more peers whose delivered
	// frontiers take turns marginally leading each other can rotate through
	// the debounce forever -- by the time evaluation returns to a given
	// connection, it is never the one "just" left, so switchBackDebouncedLocked
	// never fires for it, even though the active connection is still handed
	// off roughly once per cooldown window with zero net forward progress
	// (confirmed live: three peers thrashing every ~2s, block height frozen).
	// Gating every discretionary release on this single timestamp bounds the
	// active-connection handoff rate to at most once per switchBackCooldown
	// regardless of how many distinct peers are rotating through it. Read and
	// written under mutex. See switchBackDebouncedLocked and
	// recordSwitchAwayLocked.
	lastDiscretionarySwitchAt time.Time

	// lastGenesisDensityEval is when the Genesis Density Disconnector last
	// ran; genesisDensityDisconnected holds peers already reported so a peer
	// whose connection has not yet closed is not reported on every pass.
	// Pruned in deletePeerLocked. Guarded by mutex.
	lastGenesisDensityEval     time.Time
	genesisDensityDisconnected map[ouroboros.ConnectionId]struct{}

	// lastCorroborationFailedConn dedups GenesisCorroborationFailedEvent so a
	// persistently uncorroborated fast source does not emit an event on every
	// evaluation. Reset when the leading density source becomes corroborated
	// or changes. Guarded by mutex.
	lastCorroborationFailedConn *ouroboros.ConnectionId

	// pendingGenesisExit stages a GenesisModeExitedEvent set while the mutex is
	// held (on the Genesis→Praos transition) for publishing outside the lock.
	// Guarded by mutex.
	pendingGenesisExit *GenesisModeExitedEvent

	// pendingSelectedNone stages a ChainSelectedNoneEvent set while the mutex is
	// held (on a best-peer → none transition) for publishing outside the lock.
	// Guarded by mutex.
	pendingSelectedNone *ChainSelectedNoneEvent

	// genesisSelection caches the pair GenesisSelectionState returns.
	// GenesisSelectionState is a narrow query injected as a callback into
	// other subsystems' hot paths (chainsync.State.RecordObservedHeader,
	// LedgerState.recordPeerHeaderHistory), which each hold their own lock
	// while calling it. Deriving the pair from cs.mode/cs.securityParam under
	// cs.mutex.RLock() on every call created a lock-order inversion with
	// chainsync.State.clientConnIdMutex. This atomic lets
	// GenesisSelectionState answer without cs.mutex at all. It is refreshed
	// under cs.mutex (refreshGenesisSelectionSnapshotLocked) at every point
	// that can change cs.mode or the derived Genesis window -- construction,
	// the one-way Genesis-to-Praos transition, and SetSecurityParam -- so a
	// lock-free read never observes a stale pair for longer than one such
	// update.
	genesisSelection atomic.Pointer[genesisSelectionSnapshot]

	// eagerness caches the Limit on Eagerness for one evaluation. It is set
	// and cleared under mutex by beginEagernessLocked/endEagernessLocked; nil
	// means comparisons apply no cap.
	eagerness *EagernessLimit

	// eagernessAnchor is the last point every candidate fragment shared. The
	// limit stays k past it when the bounded fragments later stop overlapping,
	// instead of falling back to the applied tip, which advances with the
	// chain the limit bounds. It has its own mutex because the limit is
	// computed under the read lock.
	eagernessAnchorMu sync.Mutex
	eagernessAnchor   *eagernessAnchor

	// ingressWake is closed when staged or committed peer frontiers change,
	// waking headers held by AwaitIngressCorroboration. It has its own mutex
	// so that frontier writers can signal while holding cs.mutex.
	ingressWakeMu sync.Mutex
	ingressWake   chan struct{}
}

type eagernessAnchor struct {
	point       ocommon.Point
	blockNumber uint64
}

type peerTipAdmissionCandidate struct {
	update      PeerTipUpdateEvent
	plausible   bool
	farTipClaim *farTipClaim
}

// peerTipRejectionKey identifies a candidate for rejection reporting: the
// connection, the observed header point and the claimed tip block.
type peerTipRejectionKey struct {
	connId       ouroboros.ConnectionId
	observedSlot uint64
	observedHash string
	claimedBlock uint64
}

func newPeerTipRejectionKey(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	observedTip ochainsync.Tip,
) peerTipRejectionKey {
	return peerTipRejectionKey{
		connId:       connId,
		observedSlot: observedTip.Point.Slot,
		observedHash: string(observedTip.Point.Hash),
		claimedBlock: tip.BlockNumber,
	}
}

// markPeerTipRejectionReportedLocked records the candidate and reports
// whether this is its first refusal. Must be called with cs.mutex held.
func (cs *ChainSelector) markPeerTipRejectionReportedLocked(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	observedTip ochainsync.Tip,
) bool {
	if cs.reportedPeerTipRejections == nil {
		cs.reportedPeerTipRejections = make(map[peerTipRejectionKey]struct{})
	}
	key := newPeerTipRejectionKey(connId, tip, observedTip)
	if _, seen := cs.reportedPeerTipRejections[key]; seen {
		return false
	}
	cs.reportedPeerTipRejections[key] = struct{}{}
	return true
}

// prunePeerTipRejectionReportsLocked drops reports for candidates that are no
// longer staged: removed peers, committed or rejected candidates, and
// candidates refused live without being staged. Must be called with cs.mutex
// held.
func (cs *ChainSelector) prunePeerTipRejectionReportsLocked() {
	if len(cs.reportedPeerTipRejections) == 0 {
		return
	}
	staged := make(map[peerTipRejectionKey]struct{})
	for _, pending := range cs.pendingPeerTips {
		for _, candidate := range pending {
			staged[newPeerTipRejectionKey(
				candidate.update.ConnectionId,
				candidate.update.Tip,
				candidate.update.ObservedTip,
			)] = struct{}{}
		}
	}
	for key := range cs.reportedPeerTipRejections {
		if _, ok := staged[key]; !ok {
			delete(cs.reportedPeerTipRejections, key)
		}
	}
}

type forecastHorizonWitness struct {
	admissionID uint64
	peerTip     *PeerChainTip
}

func cloneCandidateFarTipClaim(
	update PeerTipUpdateEvent,
	plausible bool,
	claim farTipClaim,
	ok bool,
) *farTipClaim {
	if !plausible || !ok || !claim.lone ||
		claim.slot != update.ObservedTip.Point.Slot ||
		!bytes.Equal(claim.hash, update.ObservedTip.Point.Hash) {
		return nil
	}
	claim.hash = bytes.Clone(claim.hash)
	return &claim
}

// clonePeerTipAdmissionFrontier copies only state used while a header waits
// for ledger admission. Rollback and admitted-point history belong to the
// committed peer; copying either on every delivered header makes admission
// cost grow with k. Genesis corroboration still needs the committed density
// window as the base for staged points.
func (cs *ChainSelector) clonePeerTipAdmissionFrontier(
	peerTip *PeerChainTip,
) *PeerChainTip {
	if peerTip == nil {
		return nil
	}
	result := *peerTip
	result.Tip = cloneObservedTip(peerTip.Tip)
	result.ObservedTip = cloneObservedTip(peerTip.ObservedTip)
	result.VRFOutput = append([]byte(nil), peerTip.VRFOutput...)
	result.PraosView = clonePraosTiebreakerView(peerTip.PraosView)
	result.observedTipHistory = nil
	result.admittedTipHistory = nil
	if cs.mode == SelectionModeGenesis {
		result.observedSlots = append([]uint64(nil), peerTip.observedSlots...)
		result.observedPoints = cloneObservedPoints(peerTip.observedPoints)
	} else {
		result.observedSlots = nil
		result.observedPoints = nil
		result.observedPointBaseBlock = 0
	}
	return &result
}

func clonePeerTipUpdate(update PeerTipUpdateEvent) PeerTipUpdateEvent {
	update.Tip = cloneObservedTip(update.Tip)
	update.ObservedTip = cloneObservedTip(update.ObservedTip)
	update.VRFOutput = append([]byte(nil), update.VRFOutput...)
	update.PraosView = clonePraosTiebreakerView(update.PraosView)
	return update
}

func (cs *ChainSelector) applyPeerTipAdmissionUpdateLocked(
	peerTip *PeerChainTip,
	update PeerTipUpdateEvent,
) {
	peerTip.UpdateTipWithObservedPraosView(
		update.Tip,
		update.ObservedTip,
		update.VRFOutput,
		update.PraosView,
	)
	trackHashes := cs.mode == SelectionModeGenesis
	peerTip.recordObservedPoint(
		update.ObservedTip.Point,
		update.ObservedTip.BlockNumber,
		cs.genesisWindowSlotsLocked(),
		cs.candidatePointCapacityLocked(trackHashes),
		trackHashes,
	)
	peerTip.recordObservedTipHistory(
		update.ObservedTip,
		safeAddUint64(cs.securityParam, 1),
	)
}

func cloneFarTipClaims(
	claims map[ouroboros.ConnectionId]farTipClaim,
) map[ouroboros.ConnectionId]farTipClaim {
	clone := make(map[ouroboros.ConnectionId]farTipClaim, len(claims))
	for connId, claim := range claims {
		claim.hash = bytes.Clone(claim.hash)
		clone[connId] = claim
	}
	return clone
}

// genesisSelectionSnapshot is the immutable pair GenesisSelectionState
// returns. It is published by whole-value replacement and never mutated in
// place, so a lock-free reader always sees an active/window pair that existed
// together: publishing the two halves as separate atomics would let a reader
// load the old active flag and the new window across one refresh.
type genesisSelectionSnapshot struct {
	active bool
	window uint64
}

// refreshGenesisSelectionSnapshotLocked recomputes the cached
// GenesisSelectionState pair from cs.mode and genesisWindowSlotsLocked().
// Callers must hold cs.mutex (or run before cs is shared across goroutines,
// as NewChainSelector does) and must call this every time cs.mode or a
// genesisWindowSlotsLocked() input (cs.securityParam) changes.
func (cs *ChainSelector) refreshGenesisSelectionSnapshotLocked() {
	cs.genesisSelection.Store(&genesisSelectionSnapshot{
		active: cs.mode == SelectionModeGenesis,
		window: cs.genesisWindowSlotsLocked(),
	})
}

// NewChainSelector creates a new ChainSelector with the given configuration.
func NewChainSelector(cfg ChainSelectorConfig) *ChainSelector {
	if cfg.Consensus == nil {
		cfg.Consensus = nodeconsensus.NewPraosProvider()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	cfg.Logger = cfg.Logger.With("component", "chainselection")
	if cfg.EvaluationInterval == 0 {
		cfg.EvaluationInterval = defaultEvaluationInterval
	}
	if cfg.StaleTipThreshold == 0 {
		cfg.StaleTipThreshold = defaultStaleTipThreshold
	}
	if cfg.SwitchBackCooldown == 0 {
		cfg.SwitchBackCooldown = defaultSwitchBackCooldown
	}
	maxPeers := cfg.MaxTrackedPeers
	if maxPeers <= 0 {
		maxPeers = DefaultMaxTrackedPeers
	}
	// Fail closed on a negative corroboration threshold. Only 0 disables the
	// Genesis corroboration security gate; a negative value (reachable via the
	// public programmatic API, e.g. WithGenesisCorroborationPeers(-1)) must not
	// silently disable it. Clamp to the minimum meaningful gate (require one
	// corroborator) and warn, rather than treating it as disabled.
	if cfg.MinCorroboratingPeers < 0 {
		cfg.Logger.Warn(
			"negative genesis corroboration threshold; failing closed to 1",
			"configured", cfg.MinCorroboratingPeers,
		)
		cfg.MinCorroboratingPeers = 1
	}
	cs := &ChainSelector{
		config:                    cfg,
		securityParam:             cfg.SecurityParam,
		maxTrackedPeers:           maxPeers,
		mode:                      SelectionModePraos,
		peerTips:                  make(map[ouroboros.ConnectionId]*PeerChainTip),
		deliveredPeerTips:         make(map[ouroboros.ConnectionId]*PeerChainTip),
		pendingPeerTips:           make(map[ouroboros.ConnectionId][]peerTipAdmissionCandidate),
		pendingPeerTipFrontiers:   make(map[ouroboros.ConnectionId]*PeerChainTip),
		forecastHorizonWitnesses:  make(map[ouroboros.ConnectionId]forecastHorizonWitness),
		farTipClaims:              make(map[ouroboros.ConnectionId]farTipClaim),
		pendingFarTipClaims:       make(map[ouroboros.ConnectionId]farTipClaim),
		reportedPeerTipRejections: make(map[peerTipRejectionKey]struct{}),
		committedPeerTips:         make(map[ouroboros.ConnectionId]uint64),
		eligible:                  make(map[ouroboros.ConnectionId]bool),
		priority:                  make(map[ouroboros.ConnectionId]int),
		evaluationTrigger:         make(chan struct{}, 1),
		nowFn:                     time.Now,
		switchBackCooldown:        cfg.SwitchBackCooldown,
		recentlyLeft:              make(map[ouroboros.ConnectionId]time.Time),
	}
	if cfg.GenesisMode {
		cs.mode = SelectionModeGenesis
	}
	cs.refreshGenesisSelectionSnapshotLocked()
	if cfg.EventBus != nil && !cfg.DisableEventSubscriptions {
		// SubscribeFuncStrict, not SubscribeFunc: HandlePeerRollbackEvent
		// mutates chain-selection state machine state (per-peer observed
		// history) from a monotonic rollback stream. A handler panic must
		// not be absorbed and silently followed by the next rollback as if
		// this one had actually been applied -- see onPeerRollbackPanic and
		// event.EventBus.SubscribeFuncStrict.
		//
		// SubscriberBackpressureBlock, not the default Detach: this stream is
		// ordering-critical, since a dropped rollback leaves a peer's tracked
		// tip stale relative to its actual (rolled-back) chain, feeding
		// selection from data the peer itself has already disavowed. Nothing
		// here re-subscribes, so Detach would let ordinary backpressure alone
		// (with no panic at all) silently and permanently stop rollback
		// processing -- the same durable loss a panic causes, but far easier
		// to trigger.
		cfg.EventBus.SubscribeFuncStrict(
			PeerRollbackEventType,
			event.DefaultSubscriberBuffer,
			event.SubscriberBackpressureBlock,
			cs.HandlePeerRollbackEvent,
			cs.onPeerRollbackPanic,
		)
	}
	return cs
}

// Start begins the chain selector's background evaluation loop and subscribes
// to relevant events.
func (cs *ChainSelector) Start(ctx context.Context) error {
	cs.ctx, cs.cancel = context.WithCancel(ctx)
	go cs.evaluationLoop() //nolint:contextcheck // evaluationLoop observes the stored lifecycle context
	return nil
}

// Stop stops the chain selector.
func (cs *ChainSelector) Stop() {
	if cs.cancel != nil {
		cs.cancel()
	}
}

func (cs *ChainSelector) genesisWindowSlotsLocked() uint64 {
	if cs.config.GenesisWindowSlots > 0 {
		return cs.config.GenesisWindowSlots
	}
	if cs.securityParam > 0 {
		return safeAddUint64(
			cs.securityParam,
			safeAddUint64(cs.securityParam, cs.securityParam),
		)
	}
	return DefaultGenesisWindowSlots
}

// candidatePointCapacityLocked retains enough delivered points for a complete
// Genesis window and a k+1-header eagerness suffix without duplicating hashes.
func (cs *ChainSelector) candidatePointCapacityLocked(trackHashes bool) uint64 {
	window := cs.genesisWindowSlotsLocked()
	if !trackHashes {
		return window
	}
	return safeAddUint64(window, safeAddUint64(cs.securityParam, 1))
}

// bestKnownGenesisSlotLocked returns the exit horizon: the network tip slot the
// local tip must catch up to (within the window) before leaving Genesis mode.
//
// The advertised tip (pt.Tip) is untrusted and unbounded. Plausibility checks
// intentionally bound the DELIVERED header frontier instead, because an honest
// advertised tip can be arbitrarily far ahead during catch-up. Corroboration
// validates those delivered headers, not the advertised claim, so a peer that
// delivers one shared early header (passing corroboration) can still advertise
// an arbitrary tip. Using that raw advertised slot as the horizon lets a single
// peer pin the node in Genesis mode indefinitely — a liveness DoS.
//
// The horizon is therefore bound to DELIVERED data: a peer's advertised tip
// counts only when the peer is corroborated (selectable) AND it has actually
// delivered headers up to within the window of that advertised tip
// (ObservedTip + window >= Tip). A liar cannot deliver up to a MaxUint64 slot,
// and an honest peer early in from-origin sync has not yet delivered up to its
// far advertised tip, so neither raises the horizon prematurely. Once a
// corroborated peer has served its chain up to (near) its advertised tip, that
// tip is trustworthy and becomes the exit target — reached exactly when the
// local tip has caught up.
//
// Once this transition fires, both peer ranking and authoritative ledger fork
// resolution intentionally return to Praos.
func (cs *ChainSelector) bestKnownGenesisSlotLocked(
	bestKnownBlock *uint64,
) uint64 {
	window := cs.genesisWindowSlotsLocked()
	var best uint64
	for connId, pt := range cs.peerTips {
		if !cs.isPeerSelectableWithBestKnownBlockLocked(
			connId,
			pt,
			false,
			bestKnownBlock,
		) {
			continue
		}
		// A peer registered from a rollback has delivered no header: its
		// ObservedTip is the intersection point the node itself proposed, so
		// crediting it as delivered evidence would let any peer that
		// re-intersects at the local tip and advertises a tip within the
		// window force an immediate Genesis exit. That is precisely what the
		// delivered-vs-advertised guard below exists to prevent, so such a
		// peer raises no exit horizon until its first header arrives.
		if pt.awaitingFirstHeader {
			continue
		}
		advertised := pt.Tip.Point.Slot
		delivered := pt.ObservedTip.Point.Slot
		// Ignore an advertised tip the peer has not delivered headers up to:
		// it is either a lie or a not-yet-synced far tip, and must not raise
		// the exit horizon.
		if safeAddUint64(delivered, window) < advertised {
			continue
		}
		if advertised > best {
			best = advertised
		}
	}
	return best
}

// advertisedTipStaleLocked reports whether, in Genesis mode, this peer's
// advertised tip is older than the wall clock by more than a Genesis window.
// Such a peer holds a chain the network has stopped extending (an abandoned
// fork): it can lead on delivered density and frontier because its whole chain
// is short, so it must not be selected (which also keeps it out of the Genesis
// exit horizon, built from selectable peers), must not define the delivered
// frontier other peers are measured against, and must not have its headers
// applied to the ledger (see ShouldApplyIngress).
//
// The bound is the wall clock alone, not other peers' claims. Advertised tips
// are unverified, so any rule that counts them can be tipped by colluding
// connections into excluding the one honest peer. A live chain's tip is always
// near the wall clock and no peer can make it older, so peers cannot move the
// verdict on an honest peer. A stale peer is excluded even when it is the only
// candidate: letting it through then is exactly how an abandoned fork gets
// selected while honest peers reconnect, or after they all disconnect.
//
// The trade-off is that if the honest network stalls for longer than one
// Genesis window, a syncing Genesis-mode node waits for a fresher peer instead
// of following a chain that old; a stall that long already violates the
// chain-growth assumption Genesis selection rests on. The rule is inactive
// without a wall-clock source, while the clock reports no slot, and outside
// Genesis mode.
func (cs *ChainSelector) advertisedTipStaleLocked(
	peerTip *PeerChainTip,
) bool {
	if peerTip == nil {
		return false
	}
	wall, ok := cs.staleTipRuleWallSlotLocked()
	if !ok {
		return false
	}
	return safeAddUint64(
		peerTip.Tip.Point.Slot,
		cs.genesisWindowSlotsLocked(),
	) < wall
}

// staleTipRuleWallSlotLocked returns the wall-clock slot the stale-tip rule
// measures against, and false while the rule cannot decide. It reports true
// only in Genesis mode, and node composition observes tips synchronously
// throughout Genesis selection, so the apply gate never reads a tracked tip
// that predates the header it is admitting.
func (cs *ChainSelector) staleTipRuleWallSlotLocked() (uint64, bool) {
	if cs.mode != SelectionModeGenesis || cs.securityParam == 0 ||
		cs.config.WallClockSlot == nil {
		return 0, false
	}
	return cs.config.WallClockSlot()
}

func (cs *ChainSelector) shouldExitGenesisModeLocked(
	bestKnownBlock *uint64,
) (bool, uint64) {
	if cs.mode != SelectionModeGenesis {
		return false, 0
	}
	bestSlot := cs.bestKnownGenesisSlotLocked(bestKnownBlock)
	if bestSlot == 0 {
		return false, bestSlot
	}
	return safeAddUint64(
		cs.localTip.Point.Slot,
		cs.genesisWindowSlotsLocked(),
	) >= bestSlot, bestSlot
}

func (cs *ChainSelector) advanceSelectionModeLocked() bool {
	return cs.advanceSelectionModeWithBestKnownBlockLocked(nil)
}

func (cs *ChainSelector) advanceSelectionModeWithBestKnownBlockLocked(
	bestKnownBlock *uint64,
) bool {
	if cs.mode == SelectionModeGenesis &&
		cs.securityParam > 0 && bestKnownBlock == nil {
		bestBlock := cs.bestKnownBlockNumber()
		bestKnownBlock = &bestBlock
	}
	shouldExit, bestSlot := cs.shouldExitGenesisModeLocked(bestKnownBlock)
	if !shouldExit {
		return false
	}
	// Capture exit context while still in Genesis mode so the best-known slot
	// reflects the Genesis-mode selectable set.
	localSlot := cs.localTip.Point.Slot
	window := cs.genesisWindowSlotsLocked()
	cs.mode = SelectionModePraos
	cs.refreshGenesisSelectionSnapshotLocked()
	// Corroboration no longer applies in Praos; drop the per-peer hash
	// frontier so it does not linger for the full window on every tracked peer.
	for _, peerTip := range cs.peerTips {
		peerTip.observedPoints = nil
		peerTip.observedPointBaseBlock = 0
	}
	clear(cs.deliveredPeerTips)
	cs.lastCorroborationFailedConn = nil
	cs.config.Logger.Info(
		"exiting Genesis selection mode",
		"local_slot", localSlot,
		"best_known_slot", bestSlot,
		"genesis_window_slots", window,
	)
	if cs.config.EventBus != nil {
		cs.pendingGenesisExit = &GenesisModeExitedEvent{
			LocalSlot:          localSlot,
			BestKnownSlot:      bestSlot,
			GenesisWindowSlots: window,
		}
	}
	return true
}

// UpdatePeerTip updates the chain tip for a specific peer and triggers
// evaluation if needed. The vrfOutput parameter is the VRF output from the
// tip block header, used for tie-breaking when chains have equal block number
// and slot.
//
// Returns true if the tip was accepted, false if its delivered frontier was
// rejected as implausible. The remote advertised tip is untrusted and may be
// arbitrarily far ahead while the peer legitimately serves the next block the
// node needs, so plausibility is checked against the locally observed header.
// For known peers, the reference is the peer's previous observed frontier; for
// new peers, it is the best observed peer frontier. This avoids rejecting
// legitimate peers during sync while still bounding delivered block-number
// jumps.
func (cs *ChainSelector) UpdatePeerTip(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	vrfOutput []byte,
) bool {
	return cs.updatePeerTipObserved(connId, tip, tip, vrfOutput)
}

func (cs *ChainSelector) updatePeerTipObserved(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	observedTip ochainsync.Tip,
	vrfOutput []byte,
) bool {
	return cs.updatePeerTipObservedPraosView(
		connId,
		tip,
		observedTip,
		vrfOutput,
		PraosTiebreakerViewFromTip(
			observedTip,
			vrfOutput,
			PraosTiebreakerConfigUnknown(),
		),
	)
}

func (cs *ChainSelector) updatePeerTipObservedPraosView(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	observedTip ochainsync.Tip,
	vrfOutput []byte,
	praosView PraosTiebreakerView,
) bool {
	return cs.updatePeerTipObservedLinked(
		connId, tip, observedTip, vrfOutput, praosView, deliveredLink{},
	)
}

// updatePeerTipObservedLinked is updatePeerTipObservedPraosView for a header
// whose parent hash is known, which a lone far frontier needs to be accepted.
func (cs *ChainSelector) updatePeerTipObservedLinked(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	observedTip ochainsync.Tip,
	vrfOutput []byte,
	praosView PraosTiebreakerView,
	link deliveredLink,
) bool {
	return cs.updatePeerTipObservedLinkedDisposition(
		connId, tip, observedTip, vrfOutput, praosView, link, false,
	) == PeerTipUpdateAccepted
}

// updatePeerTipObservedLinkedDisposition records a peer tip and re-evaluates
// selection. generationPinned reports that the caller already holds the
// ledger's header-crypto generation pin; the evaluation must then reuse it,
// because a second read-lock of the ledger's generation RWMutex blocks behind
// any queued publisher, which in turn waits for the caller's pin.
func (cs *ChainSelector) updatePeerTipObservedLinkedDisposition(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	observedTip ochainsync.Tip,
	vrfOutput []byte,
	praosView PraosTiebreakerView,
	link deliveredLink,
	generationPinned bool,
) PeerTipUpdateDisposition {
	if cs.config.ConnectionLive != nil &&
		!cs.config.ConnectionLive(connId) {
		cs.config.Logger.Debug(
			"ignoring tip update from closed connection",
			"connection_id", connId.String(),
			"block_number", tip.BlockNumber,
			"slot", tip.Point.Slot,
		)
		return PeerTipUpdateRejected
	}
	shouldEvaluate := false
	disposition := PeerTipUpdateAccepted
	var evictedConn *ouroboros.ConnectionId
	var modeChanged bool
	var revoke bool

	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()

		if cs.loneFrontierBrokenLocked(
			connId, observedTip, link, cs.farTipClaims,
		) {
			revoke = true
			disposition = PeerTipUpdateRejected
			return
		}
		if !cs.checkPeerTipPlausibleLocked(
			connId, tip, observedTip, link,
		) {
			disposition = PeerTipUpdateRejected
			if claim, ok := cs.farTipClaims[connId]; ok &&
				claim.block == observedTip.BlockNumber &&
				claim.slot == observedTip.Point.Slot &&
				bytes.Equal(claim.hash, observedTip.Point.Hash) &&
				claim.boundary == link.boundary {
				disposition = PeerTipUpdatePending
			}
			return
		}

		trackHashes := cs.mode == SelectionModeGenesis
		if peerTip, exists := cs.peerTips[connId]; exists {
			peerTip.UpdateTipWithObservedPraosView(
				tip,
				observedTip,
				vrfOutput,
				praosView,
			)
			peerTip.recordObservedPoint(
				observedTip.Point,
				observedTip.BlockNumber,
				cs.genesisWindowSlotsLocked(),
				cs.candidatePointCapacityLocked(trackHashes),
				trackHashes,
			)
			peerTip.recordObservedTipHistory(
				observedTip,
				safeAddUint64(cs.securityParam, 1),
			)
		} else {
			var ok bool
			evictedConn, ok = cs.makeRoomForNewPeerLocked(connId)
			if !ok {
				disposition = PeerTipUpdateRejected
				return
			}
			peerTip := &PeerChainTip{
				ConnectionId:   connId,
				Tip:            tip,
				ObservedTip:    observedTip,
				observedTipSet: true,
				VRFOutput:      vrfOutput,
				PraosView:      praosView,
				nowFn:          cs.nowFn,
				LastUpdated:    cs.now(),
			}
			peerTip.recordObservedPoint(
				observedTip.Point,
				observedTip.BlockNumber,
				cs.genesisWindowSlotsLocked(),
				cs.candidatePointCapacityLocked(trackHashes),
				trackHashes,
			)
			peerTip.recordObservedTipHistory(
				observedTip,
				safeAddUint64(cs.securityParam, 1),
			)
			cs.peerTips[connId] = peerTip
		}
		if len(cs.pendingPeerTips[connId]) > 0 {
			cs.rebuildPeerTipAdmissionFrontierLocked(connId)
		}

		// In Genesis mode every update is evaluated below, and evaluation
		// advances the mode with the trusted frontier it computes for
		// selection. Advancing here too would pay that corroboration scan
		// twice per delivered header.
		if !trackHashes {
			modeChanged = cs.advanceSelectionModeLocked()
		}
		// Record the common point while every window still holds it, whether
		// or not anyone is waiting on the limit.
		cs.computeEagernessLimitLocked(nil, cs.localTip)

		cs.config.Logger.Debug(
			"updated peer tip",
			"connection_id", connId.String(),
			"block_number", tip.BlockNumber,
			"slot", tip.Point.Slot,
		)

		// Check if this peer's tip is better than the current best peer's tip
		switch {
		case modeChanged:
			shouldEvaluate = true
		case trackHashes:
			// In Genesis mode any peer's frontier change can grant or revoke
			// corroboration of the incumbent/leader's frontier, so always
			// re-evaluate rather than only when this peer beats the best.
			shouldEvaluate = true
		case cs.bestPeerConn != nil:
			if bestPeerTip, ok := cs.peerTips[*cs.bestPeerConn]; ok {
				shouldEvaluate = cs.comparePeerTips(
					connId,
					cs.peerTips[connId],
					*cs.bestPeerConn,
					bestPeerTip,
				) == ChainABetter
			}
		default:
			// No best peer yet, trigger evaluation
			shouldEvaluate = true
		}
		if len(cs.pendingFarTipClaims) > 0 {
			cs.rebuildPendingAdmissionStateLocked()
		}
	}()

	// Publish eviction event outside the lock to prevent deadlock
	if evictedConn != nil && cs.config.EventBus != nil {
		evt := event.NewEvent(
			PeerEvictedEventType,
			PeerEvictedEvent{ConnectionId: *evictedConn},
		)
		cs.publishSelection(PeerEvictedEventType, evt)
	}

	if revoke {
		cs.removePeer(connId, generationPinned)
	}
	if disposition != PeerTipUpdateAccepted {
		return disposition
	}

	if shouldEvaluate {
		cs.evaluateAndSwitch(generationPinned)
	}

	return PeerTipUpdateAccepted
}

// PreparePeerTipAdmission records an observed header as a candidate. Genesis
// corroboration may use it as a witness, but it cannot become a selectable
// source until CommitPeerTipAdmission.
func (cs *ChainSelector) PreparePeerTipAdmission(
	update PeerTipUpdateEvent,
) bool {
	return cs.PreparePeerTipAdmissionDisposition(update) != PeerTipUpdateRejected
}

// PreparePeerTipAdmissionDisposition stages a candidate and distinguishes
// plausible headers from far-tip claims that must remain withheld.
func (cs *ChainSelector) PreparePeerTipAdmissionDisposition(
	update PeerTipUpdateEvent,
) PeerTipUpdateDisposition {
	if update.AdmissionID == 0 {
		return PeerTipUpdateRejected
	}
	if !cs.peerTipAdmissionGenerationCurrent(update) {
		return PeerTipUpdateRejected
	}
	if cs.config.ConnectionLive != nil &&
		!cs.config.ConnectionLive(update.ConnectionId) {
		return PeerTipUpdateRejected
	}
	var accepted bool
	disposition := PeerTipUpdateAccepted
	commitImmediately := false
	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()
		cs.refreshAppliedLocalTipLocked()
		if cs.truncateStalePendingPeerTipsLocked() {
			cs.rebuildPendingAdmissionStateLocked()
		}
		if !cs.peerTipAdmissionGenerationCurrent(update) {
			return
		}
		if update.AdmissionID <= cs.committedPeerTips[update.ConnectionId] {
			accepted = true
			return
		}
		pending := cs.pendingPeerTips[update.ConnectionId]
		for _, candidate := range pending {
			if candidate.update.AdmissionID == update.AdmissionID {
				accepted = true
				if !candidate.plausible {
					disposition = PeerTipUpdatePending
				}
				return
			}
		}
		if _, committed := cs.peerTips[update.ConnectionId]; !committed {
			if _, pendingPeer := cs.pendingPeerTips[update.ConnectionId]; !pendingPeer &&
				cs.admissionPeerCountLocked() >= cs.maxTrackedPeers {
				return
			}
		}
		if cs.pendingFarTipClaims == nil {
			cs.pendingFarTipClaims = cloneFarTipClaims(cs.farTipClaims)
		}
		link := deliveredLink{
			prevHash: update.ObservedPrevHash,
			boundary: update.ObservedBoundary,
		}
		plausible, claimsChanged := cs.checkPeerTipPlausibleWithClaimsTrackedLocked(
			update.ConnectionId,
			update.Tip,
			update.ObservedTip,
			link,
			cs.pendingFarTipClaims,
			true,
		)
		claim, hasClaim := cs.pendingFarTipClaims[update.ConnectionId]
		evidence := hasClaim &&
			claim.slot == update.ObservedTip.Point.Slot &&
			bytes.Equal(claim.hash, update.ObservedTip.Point.Hash)
		if !plausible && !evidence {
			cs.rebuildPendingAdmissionStateLocked()
			return
		}
		update = clonePeerTipUpdate(update)
		pending = append(pending, peerTipAdmissionCandidate{
			update:    update,
			plausible: plausible,
			farTipClaim: cloneCandidateFarTipClaim(
				update, plausible, claim, hasClaim,
			),
		})
		truncated := false
		pendingLimit := cs.pendingPeerTipAdmissionLimitLocked()
		if len(pending) > pendingLimit {
			pending = pending[len(pending)-pendingLimit:]
			truncated = true
		}
		cs.pendingPeerTips[update.ConnectionId] = pending
		switch {
		case claimsChanged:
			// Far-frontier evidence can change another connection's pending
			// plausibility, so replay the exceptional cross-peer state.
			cs.rebuildPendingAdmissionStateLocked()
		case truncated:
			cs.rebuildPeerTipAdmissionFrontierLocked(update.ConnectionId)
		case plausible:
			cs.appendPeerTipAdmissionFrontierLocked(update)
		}
		for _, candidate := range cs.pendingPeerTips[update.ConnectionId] {
			if candidate.update.AdmissionID == update.AdmissionID {
				plausible = candidate.plausible
				break
			}
		}
		commitImmediately = cs.admittedPeerTipPointLocked(update)
		accepted = true
		if !plausible {
			disposition = PeerTipUpdatePending
		}
	}()
	if accepted && commitImmediately {
		cs.commitPeerTipAdmission(update, true, &update, false)
	}
	if !accepted {
		return PeerTipUpdateRejected
	}
	cs.signalIngressChange()
	return disposition
}

// PrepareForecastHorizonWitness records one connected header solely as a
// Genesis corroboration witness while its ChainSync callback waits for the
// ledger forecast horizon. The header has not passed consensus verification,
// so it is kept outside every selection and admission path. The returned
// connections have verified staged headers that the witness now corroborates
// and should be re-intersected so those headers are delivered again.
func (cs *ChainSelector) PrepareForecastHorizonWitness(
	update PeerTipUpdateEvent,
) []ouroboros.ConnectionId {
	if update.AdmissionID == 0 ||
		(cs.config.ConnectionLive != nil &&
			!cs.config.ConnectionLive(update.ConnectionId)) {
		return nil
	}
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	if !cs.genesisCorroborationActiveLocked() {
		return nil
	}
	base := cs.latestPeerTipCandidateLocked(update.ConnectionId)
	if base == nil {
		base = cs.peerTips[update.ConnectionId]
	}
	if base == nil || update.ObservedTip.Point.Slot <= base.ObservedTip.Point.Slot ||
		len(update.ObservedPrevHash) == 0 ||
		!bytes.Equal(update.ObservedPrevHash, base.ObservedTip.Point.Hash) {
		return nil
	}
	witnessTip := cs.clonePeerTipAdmissionFrontier(base)
	cs.applyPeerTipAdmissionUpdateLocked(witnessTip, update)
	cs.forecastHorizonWitnesses[update.ConnectionId] = forecastHorizonWitness{
		admissionID: update.AdmissionID,
		peerTip:     witnessTip,
	}
	var recoveries []ouroboros.ConnectionId
	for connId := range cs.pendingPeerTips {
		if connId == update.ConnectionId {
			continue
		}
		candidate := cs.latestPeerTipCandidateLocked(connId)
		if candidate != nil && cs.isPeerCorroboratedLocked(connId, candidate) {
			recoveries = append(recoveries, connId)
		}
	}
	slices.SortFunc(recoveries, func(a, b ouroboros.ConnectionId) int {
		return cmp.Compare(a.String(), b.String())
	})
	return recoveries
}

// RemoveForecastHorizonWitness removes the temporary witness created for the
// matching admission wait. A later wait on the same connection is unaffected.
func (cs *ChainSelector) RemoveForecastHorizonWitness(
	update PeerTipUpdateEvent,
) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	witness, ok := cs.forecastHorizonWitnesses[update.ConnectionId]
	if ok && witness.admissionID == update.AdmissionID {
		delete(cs.forecastHorizonWitnesses, update.ConnectionId)
	}
}

func (cs *ChainSelector) admittedPeerTipPointLocked(
	update PeerTipUpdateEvent,
) bool {
	point := update.ObservedTip.Point
	for _, peerTip := range cs.peerTips {
		for _, admittedPoint := range peerTip.admittedTipHistory {
			if admittedPoint.Slot == point.Slot &&
				bytes.Equal(admittedPoint.Hash, point.Hash) {
				return true
			}
		}
	}
	return false
}

func (cs *ChainSelector) latestPeerTipCandidateLocked(
	connId ouroboros.ConnectionId,
) *PeerChainTip {
	for _, candidate := range cs.pendingPeerTips[connId] {
		if !cs.peerTipAdmissionGenerationCurrent(candidate.update) {
			return nil
		}
	}
	return cs.pendingPeerTipFrontiers[connId]
}

func (cs *ChainSelector) peerTipAdmissionGenerationCurrent(
	update PeerTipUpdateEvent,
) bool {
	generation := update.HeaderCryptoVerificationGeneration
	if generation == 0 {
		return true
	}
	if cs.config.HeaderCryptoVerificationGenerationCurrent != nil {
		return cs.config.HeaderCryptoVerificationGenerationCurrent(generation)
	}
	if cs.config.PinHeaderCryptoVerificationGeneration != nil {
		release, current := cs.config.PinHeaderCryptoVerificationGeneration(generation)
		if release != nil {
			release()
		}
		return current
	}
	return true
}

// truncateStalePendingPeerTipsLocked drops the first candidate authorized by
// an obsolete ledger snapshot and every later candidate on that connection,
// because the later suffix depends on the rejected prefix.
func (cs *ChainSelector) truncateStalePendingPeerTipsLocked() bool {
	changed := false
	for connId, pending := range cs.pendingPeerTips {
		for i, candidate := range pending {
			if cs.peerTipAdmissionGenerationCurrent(candidate.update) {
				continue
			}
			if i == 0 {
				delete(cs.pendingPeerTips, connId)
			} else {
				cs.pendingPeerTips[connId] = append(
					[]peerTipAdmissionCandidate(nil),
					pending[:i]...,
				)
			}
			changed = true
			break
		}
	}
	return changed
}

func (cs *ChainSelector) rebuildPeerTipAdmissionFrontierLocked(
	connId ouroboros.ConnectionId,
) {
	cs.prunePeerTipRejectionReportsLocked()
	pending := cs.pendingPeerTips[connId]
	if len(pending) == 0 {
		delete(cs.pendingPeerTipFrontiers, connId)
		return
	}
	var frontier *PeerChainTip
	if committed := cs.peerTips[connId]; committed != nil {
		frontier = cs.clonePeerTipAdmissionFrontier(committed)
	} else {
		frontier = &PeerChainTip{ConnectionId: connId, nowFn: cs.nowFn}
	}
	for _, candidate := range pending {
		if !candidate.plausible {
			continue
		}
		cs.applyPeerTipAdmissionUpdateLocked(frontier, candidate.update)
	}
	if previous := cs.pendingPeerTipFrontiers[connId]; previous != nil {
		frontier.eagernessPaused = previous.eagernessPaused
	}
	cs.pendingPeerTipFrontiers[connId] = frontier
}

func (cs *ChainSelector) appendPeerTipAdmissionFrontierLocked(
	update PeerTipUpdateEvent,
) {
	frontier := cs.pendingPeerTipFrontiers[update.ConnectionId]
	if frontier == nil {
		frontier = cs.clonePeerTipAdmissionFrontier(
			cs.peerTips[update.ConnectionId],
		)
		if frontier == nil {
			frontier = &PeerChainTip{
				ConnectionId: update.ConnectionId,
				nowFn:        cs.nowFn,
			}
		}
	}
	cs.applyPeerTipAdmissionUpdateLocked(frontier, update)
	cs.pendingPeerTipFrontiers[update.ConnectionId] = frontier
}

func (cs *ChainSelector) rebuildPendingAdmissionStateLocked() {
	cs.truncateStalePendingPeerTipsLocked()
	type staged struct {
		connId  ouroboros.ConnectionId
		connKey string
		index   int
		id      uint64
	}
	var stagedUpdates []staged
	for connId, pending := range cs.pendingPeerTips {
		connKey := connId.String()
		for i, candidate := range pending {
			stagedUpdates = append(stagedUpdates, staged{
				connId:  connId,
				connKey: connKey,
				index:   i,
				id:      candidate.update.AdmissionID,
			})
		}
	}
	slices.SortFunc(stagedUpdates, func(a, b staged) int {
		if result := cmp.Compare(a.id, b.id); result != 0 {
			return result
		}
		return cmp.Compare(a.connKey, b.connKey)
	})
	previousFrontiers := cs.pendingPeerTipFrontiers
	cs.pendingPeerTipFrontiers = make(
		map[ouroboros.ConnectionId]*PeerChainTip,
		len(previousFrontiers),
	)
	claims := cloneFarTipClaims(cs.farTipClaims)
	for _, stagedUpdate := range stagedUpdates {
		pending := cs.pendingPeerTips[stagedUpdate.connId]
		if stagedUpdate.index >= len(pending) {
			continue
		}
		candidate := &pending[stagedUpdate.index]
		update := candidate.update
		plausible := cs.checkPeerTipPlausibleWithClaimsLocked(
			update.ConnectionId,
			update.Tip,
			update.ObservedTip,
			deliveredLink{
				prevHash: update.ObservedPrevHash,
				boundary: update.ObservedBoundary,
			},
			claims,
			true,
		)
		claim, hasClaim := claims[update.ConnectionId]
		evidence := hasClaim && claim.slot == update.ObservedTip.Point.Slot &&
			bytes.Equal(claim.hash, update.ObservedTip.Point.Hash)
		candidate.plausible = plausible
		candidate.farTipClaim = cloneCandidateFarTipClaim(
			update,
			plausible,
			claim,
			hasClaim,
		)
		if !plausible && !evidence {
			continue
		}
		if !plausible {
			continue
		}
		frontier := cs.pendingPeerTipFrontiers[update.ConnectionId]
		if frontier == nil {
			if committed := cs.peerTips[update.ConnectionId]; committed != nil {
				frontier = cs.clonePeerTipAdmissionFrontier(committed)
			} else {
				frontier = &PeerChainTip{
					ConnectionId: update.ConnectionId,
					nowFn:        cs.nowFn,
				}
			}
		}
		cs.applyPeerTipAdmissionUpdateLocked(frontier, update)
		if previous := previousFrontiers[update.ConnectionId]; previous != nil {
			frontier.eagernessPaused = previous.eagernessPaused
			*previous = *frontier
			frontier = previous
		}
		cs.pendingPeerTipFrontiers[update.ConnectionId] = frontier
	}
	cs.pendingFarTipClaims = claims
	cs.prunePeerTipRejectionReportsLocked()
}

// RejectPeerTipAdmission removes a candidate and later candidate headers from
// the same peer. Later headers depend on the rejected chain prefix and must be
// observed again after the peer resynchronizes. Staged copies of the same
// point on other peers are removed the same way: a header the ledger declined
// is resolved once for the point, so a duplicate whose own publish was
// suppressed must not remain behind as corroboration evidence.
func (cs *ChainSelector) RejectPeerTipAdmission(
	update PeerTipUpdateEvent,
) {
	if update.AdmissionID == 0 {
		return
	}
	shouldEvaluate := false
	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()
		if update.AdmissionID <= cs.committedPeerTips[update.ConnectionId] {
			return
		}
		pruned := cs.pruneOwnPendingPeerTipsLocked(update)
		if cs.pruneMatchingPendingPeerTipsLocked(update) {
			pruned = true
		}
		if pruned {
			cs.rebuildPendingAdmissionStateLocked()
			cs.signalIngressChange()
			shouldEvaluate = cs.genesisCorroborationActiveLocked()
		}
	}()
	if shouldEvaluate {
		cs.EvaluateAndSwitch()
	}
}

// pruneOwnPendingPeerTipsLocked drops the rejected candidate, or the first
// later one when it is already gone, together with everything staged after it
// on the same connection. Must be called with cs.mutex held.
func (cs *ChainSelector) pruneOwnPendingPeerTipsLocked(
	update PeerTipUpdateEvent,
) bool {
	pending := cs.pendingPeerTips[update.ConnectionId]
	for i, candidate := range pending {
		if candidate.update.AdmissionID >= update.AdmissionID {
			cs.truncatePendingPeerTipsLocked(update.ConnectionId, i)
			return true
		}
	}
	return false
}

// pruneMatchingPendingPeerTipsLocked removes staged candidates at the rejected
// point, and everything staged after them, from every other connection. A
// point another connection already got admitted is left alone: its staged
// copies were committed with it. Must be called with cs.mutex held.
func (cs *ChainSelector) pruneMatchingPendingPeerTipsLocked(
	update PeerTipUpdateEvent,
) bool {
	if cs.admittedPeerTipPointLocked(update) {
		return false
	}
	point := update.ObservedTip.Point
	pruned := false
	for connId, pending := range cs.pendingPeerTips {
		if connId == update.ConnectionId {
			continue
		}
		for i, candidate := range pending {
			candidatePoint := candidate.update.ObservedTip.Point
			if candidatePoint.Slot == point.Slot &&
				bytes.Equal(candidatePoint.Hash, point.Hash) {
				cs.truncatePendingPeerTipsLocked(connId, i)
				pruned = true
				break
			}
		}
	}
	return pruned
}

// truncatePendingPeerTipsLocked keeps the first n staged candidates of a
// connection and rebuilds its staged frontier. Must be called with cs.mutex
// held.
func (cs *ChainSelector) truncatePendingPeerTipsLocked(
	connId ouroboros.ConnectionId,
	n int,
) {
	if n == 0 {
		delete(cs.pendingPeerTips, connId)
	} else {
		pending := cs.pendingPeerTips[connId]
		if n < 0 || n > len(pending) {
			panic("pending peer tip truncation exceeds staged candidates")
		}
		cs.pendingPeerTips[connId] = append(
			[]peerTipAdmissionCandidate(nil),
			pending[:n]...,
		)
	}
	cs.rebuildPeerTipAdmissionFrontierLocked(connId)
}

// CommitPeerTipAdmission promotes an admitted candidate into normal peer
// selection. Events without a staged candidate retain the legacy update path.
func (cs *ChainSelector) CommitPeerTipAdmission(
	update PeerTipUpdateEvent,
) bool {
	return cs.commitPeerTipAdmissionResolved(update, false)
}

// CommitPeerTipAdmissionPinned promotes a candidate while the ledger keeps
// the producer-authorization generation pinned across this call.
func (cs *ChainSelector) CommitPeerTipAdmissionPinned(
	update PeerTipUpdateEvent,
) bool {
	return cs.commitPeerTipAdmissionResolved(update, true)
}

func (cs *ChainSelector) commitPeerTipAdmissionResolved(
	update PeerTipUpdateEvent,
	generationPinned bool,
) bool {
	releaseGeneration := func() {}
	if update.AdmissionID != 0 && !generationPinned {
		var generationCurrent bool
		releaseGeneration, generationCurrent = cs.
			pinPeerTipAdmissionGeneration(update)
		if !generationCurrent {
			cs.RejectPeerTipAdmission(update)
			return false
		}
		// A zero generation takes no pin, so evaluation must still take its own.
		generationPinned = update.HeaderCryptoVerificationGeneration != 0
	}
	defer releaseGeneration()
	if update.AdmissionID == 0 {
		accepted := cs.updatePeerTipObservedLinkedDisposition(
			update.ConnectionId,
			update.Tip,
			update.ObservedTip,
			update.VRFOutput,
			update.PraosView,
			deliveredLink{
				prevHash: update.ObservedPrevHash,
				boundary: update.ObservedBoundary,
			},
			generationPinned,
		) == PeerTipUpdateAccepted
		if !accepted {
			return false
		}
		cs.mutex.Lock()
		peerTip := cs.peerTips[update.ConnectionId]
		point := update.ObservedTip.Point
		if peerTip != nil && peerTip.ObservedTip.Point.Slot == point.Slot &&
			bytes.Equal(peerTip.ObservedTip.Point.Hash, point.Hash) {
			peerTip.recordAdmittedTipPoint(point, maxAdmittedPeerTipPoints)
		}
		cs.mutex.Unlock()
		cs.commitMatchingPeerTipAdmissions(update, generationPinned)
		return true
	}
	if !cs.commitPeerTipAdmission(update, false, nil, generationPinned) {
		return false
	}
	cs.mutex.RLock()
	pointStillAdmitted := cs.admittedPeerTipPointLocked(update)
	cs.mutex.RUnlock()
	if pointStillAdmitted {
		cs.commitMatchingPeerTipAdmissions(update, generationPinned)
	}
	return true
}

func (cs *ChainSelector) commitMatchingPeerTipAdmissions(
	admitted PeerTipUpdateEvent,
	generationPinned bool,
) {
	var candidates []PeerTipUpdateEvent
	func() {
		cs.mutex.RLock()
		defer cs.mutex.RUnlock()
		for _, pending := range cs.pendingPeerTips {
			for _, candidate := range pending {
				point := candidate.update.ObservedTip.Point
				admittedPoint := admitted.ObservedTip.Point
				if point.Slot == admittedPoint.Slot &&
					bytes.Equal(point.Hash, admittedPoint.Hash) {
					candidates = append(candidates, candidate.update)
				}
			}
		}
	}()
	for _, candidate := range candidates {
		candidate.HeaderCryptoVerificationGeneration = admitted.HeaderCryptoVerificationGeneration
		cs.commitPeerTipAdmission(candidate, true, &admitted, generationPinned)
	}
}

func (cs *ChainSelector) commitPeerTipAdmission(
	update PeerTipUpdateEvent,
	requireStaged bool,
	witness *PeerTipUpdateEvent,
	generationPinned bool,
) bool {
	releaseGeneration := func() {}
	if !generationPinned {
		var generationCurrent bool
		releaseGeneration, generationCurrent = cs.
			pinPeerTipAdmissionGeneration(update)
		if !generationCurrent {
			cs.RejectPeerTipAdmission(update)
			return false
		}
	}
	if cs.config.ConnectionLive != nil &&
		!cs.config.ConnectionLive(update.ConnectionId) {
		releaseGeneration()
		return false
	}
	var shouldEvaluate bool
	var evictedConn *ouroboros.ConnectionId
	var accepted bool
	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()
		if witness != nil && !cs.admittedPeerTipPointLocked(*witness) {
			return
		}
		if update.AdmissionID <= cs.committedPeerTips[update.ConnectionId] {
			accepted = true
			return
		}
		pending := cs.pendingPeerTips[update.ConnectionId]
		for i, candidate := range pending {
			if candidate.update.AdmissionID != update.AdmissionID {
				continue
			}
			candidate.update.HeaderCryptoVerificationGeneration = update.HeaderCryptoVerificationGeneration
			link := deliveredLink{
				prevHash: candidate.update.ObservedPrevHash,
				boundary: candidate.update.ObservedBoundary,
			}
			if cs.loneFrontierBrokenLocked(
				update.ConnectionId,
				candidate.update.ObservedTip,
				link,
				cs.farTipClaims,
			) {
				cs.deletePeerLocked(update.ConnectionId)
				shouldEvaluate = true
				accepted = true
				return
			}
			plausible := cs.checkPeerTipPlausibleWithClaimsLocked(
				update.ConnectionId,
				candidate.update.Tip,
				candidate.update.ObservedTip,
				link,
				cs.farTipClaims,
				false,
			)
			if candidate.plausible {
				plausible = true
				if candidate.farTipClaim != nil {
					if cs.farTipClaims == nil {
						cs.farTipClaims = make(map[ouroboros.ConnectionId]farTipClaim)
					}
					claim := *candidate.farTipClaim
					claim.hash = bytes.Clone(claim.hash)
					cs.farTipClaims[update.ConnectionId] = claim
				}
			}
			cs.committedPeerTips[update.ConnectionId] = update.AdmissionID
			if i == len(pending)-1 {
				delete(cs.pendingPeerTips, update.ConnectionId)
			} else {
				cs.pendingPeerTips[update.ConnectionId] = append(
					[]peerTipAdmissionCandidate(nil),
					pending[i+1:]...,
				)
			}
			cs.prunePeerTipRejectionReportsLocked()
			if len(cs.pendingFarTipClaims) > 0 {
				cs.rebuildPendingAdmissionStateLocked()
			} else if len(cs.pendingPeerTips[update.ConnectionId]) == 0 {
				delete(cs.pendingPeerTipFrontiers, update.ConnectionId)
			}
			accepted = true
			if !plausible {
				return
			}
			peerTip := cs.peerTips[update.ConnectionId]
			if peerTip == nil {
				var ok bool
				evictedConn, ok = cs.makeRoomForNewPeerLocked(update.ConnectionId)
				if !ok {
					return
				}
				peerTip = &PeerChainTip{
					ConnectionId: update.ConnectionId,
					nowFn:        cs.nowFn,
				}
			}
			cs.applyPeerTipAdmissionUpdateLocked(peerTip, candidate.update)
			peerTip.recordAdmittedTipPoint(
				candidate.update.ObservedTip.Point,
				maxAdmittedPeerTipPoints,
			)
			cs.peerTips[update.ConnectionId] = peerTip
			modeChanged := cs.advanceSelectionModeLocked()
			trackHashes := cs.genesisCorroborationActiveLocked()
			switch {
			case modeChanged, trackHashes, cs.bestPeerConn == nil:
				shouldEvaluate = true
			default:
				if best, ok := cs.peerTips[*cs.bestPeerConn]; ok {
					shouldEvaluate = cs.comparePeerTips(
						update.ConnectionId,
						cs.peerTips[update.ConnectionId],
						*cs.bestPeerConn,
						best,
					) == ChainABetter
				}
			}
			accepted = true
			return
		}
	}()
	if accepted {
		cs.signalIngressChange()
	}
	if accepted && shouldEvaluate {
		// pinPeerTipAdmissionGeneration takes no pin for a zero generation.
		cs.evaluateAndSwitch(
			generationPinned || update.HeaderCryptoVerificationGeneration != 0,
		)
		shouldEvaluate = false
	}
	releaseGeneration()
	if evictedConn != nil && cs.config.EventBus != nil {
		cs.publishSelection(
			PeerEvictedEventType,
			event.NewEvent(
				PeerEvictedEventType,
				PeerEvictedEvent{ConnectionId: *evictedConn},
			),
		)
	}
	if !accepted {
		if requireStaged {
			return false
		}
		accepted = cs.updatePeerTipObservedLinkedDisposition(
			update.ConnectionId,
			update.Tip,
			update.ObservedTip,
			update.VRFOutput,
			update.PraosView,
			deliveredLink{
				prevHash: update.ObservedPrevHash,
				boundary: update.ObservedBoundary,
			},
			generationPinned,
		) == PeerTipUpdateAccepted
		if accepted {
			cs.mutex.Lock()
			if update.AdmissionID > cs.committedPeerTips[update.ConnectionId] {
				cs.committedPeerTips[update.ConnectionId] = update.AdmissionID
			}
			peerTip := cs.peerTips[update.ConnectionId]
			point := update.ObservedTip.Point
			if peerTip != nil && peerTip.ObservedTip.Point.Slot == point.Slot &&
				bytes.Equal(peerTip.ObservedTip.Point.Hash, point.Hash) {
				peerTip.recordAdmittedTipPoint(point, maxAdmittedPeerTipPoints)
			}
			cs.mutex.Unlock()
		}
		return accepted
	}
	if shouldEvaluate {
		cs.evaluateAndSwitch(generationPinned)
	}
	return true
}

func (cs *ChainSelector) pinPeerTipAdmissionGeneration(
	update PeerTipUpdateEvent,
) (func(), bool) {
	generation := update.HeaderCryptoVerificationGeneration
	if generation == 0 {
		return func() {}, true
	}
	if cs.config.PinHeaderCryptoVerificationGeneration == nil {
		// A point-in-time freshness hook cannot make promotion atomic with
		// ledger publication. Preserve legacy callers with no generation hooks,
		// but fail closed when a caller supplied freshness without a pin.
		return func() {},
			cs.config.HeaderCryptoVerificationGenerationCurrent == nil
	}
	release, current := cs.config.PinHeaderCryptoVerificationGeneration(generation)
	if !current || release == nil {
		return func() {}, false
	}
	return release, true
}

// A zero or just-superseded generation is transient: the ledger reports it
// while a validation transition is in flight. Treating it as final would
// withhold the header from a peer that never re-sends it, so the pin waits a
// bounded time for a stable generation before giving up.
const (
	headerCryptoGenerationStableWait   = 2 * time.Second
	headerCryptoGenerationPollInterval = 2 * time.Millisecond
)

// pinCurrentHeaderCryptoGeneration orders a selector decision before the next
// ledger publication. Callers acquire this pin before cs.mutex, matching the
// ledger admission callback's generation-pin then selector-lock order.
func (cs *ChainSelector) pinCurrentHeaderCryptoGeneration() (func(), bool) {
	if cs.config.CurrentHeaderCryptoVerificationGeneration == nil ||
		cs.config.PinHeaderCryptoVerificationGeneration == nil {
		return func() {}, true
	}
	deadline := time.Now().Add(headerCryptoGenerationStableWait)
	for {
		generation := cs.config.CurrentHeaderCryptoVerificationGeneration()
		if generation != 0 {
			release, current := cs.config.PinHeaderCryptoVerificationGeneration(
				generation,
			)
			if current && release != nil {
				return release, true
			}
		}
		if !time.Now().Before(deadline) {
			return func() {}, false
		}
		time.Sleep(headerCryptoGenerationPollInterval)
	}
}

// ResolvePeerTipAdmission promotes an accepted header or removes a rejected
// candidate. It is the composition-layer callback for ledger admission.
func (cs *ChainSelector) ResolvePeerTipAdmission(
	update PeerTipUpdateEvent,
	admitted bool,
) {
	if admitted {
		cs.CommitPeerTipAdmission(update)
		return
	}
	cs.RejectPeerTipAdmission(update)
}

const (
	minPendingPeerTipAdmissions = 256
	maxAdmittedPeerTipPoints    = 256
)

// pendingPeerTipAdmissionLimitLocked retains enough staged headers to prove a
// connected K+1-header far frontier while keeping a fixed minimum window.
func (cs *ChainSelector) pendingPeerTipAdmissionLimitLocked() int {
	if cs.securityParam < uint64(minPendingPeerTipAdmissions-1) {
		return minPendingPeerTipAdmissions
	}
	maxInt := int(^uint(0) >> 1)
	if cs.securityParam >= uint64(maxInt) {
		return maxInt
	}
	return int(cs.securityParam + 1) //nolint:gosec // The bound above keeps the value within int range.
}

// admissionPeerCountLocked counts committed peers and pending-only peers once
// each, keeping staged frontiers within the same resource bound as selection.
// Must be called with cs.mutex held.
func (cs *ChainSelector) admissionPeerCountLocked() int {
	count := len(cs.peerTips)
	for connId := range cs.pendingPeerTips {
		if _, committed := cs.peerTips[connId]; !committed {
			count++
		}
	}
	return count
}

// checkPeerTipPlausibleLocked reports whether a tip observation from connId
// may be recorded, applying the shared plausibility bound used by every path
// that records a peer frontier (roll forward and the roll-backward
// registration of a peer that has no entry yet). It logs and returns false
// when the observation is rejected. Callers must hold cs.mutex.
func (cs *ChainSelector) checkPeerTipPlausibleLocked(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	observedTip ochainsync.Tip,
	link deliveredLink,
) bool {
	if cs.farTipClaims == nil {
		cs.farTipClaims = make(map[ouroboros.ConnectionId]farTipClaim)
	}
	return cs.checkPeerTipPlausibleWithClaimsLocked(
		connId, tip, observedTip, link, cs.farTipClaims, true,
	)
}

func (cs *ChainSelector) checkPeerTipPlausibleWithClaimsLocked(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	observedTip ochainsync.Tip,
	link deliveredLink,
	claims map[ouroboros.ConnectionId]farTipClaim,
	includePending bool,
) bool {
	plausible, _ := cs.checkPeerTipPlausibleWithClaimsTrackedLocked(
		connId, tip, observedTip, link, claims, includePending,
	)
	return plausible
}

func (cs *ChainSelector) checkPeerTipPlausibleWithClaimsTrackedLocked(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	observedTip ochainsync.Tip,
	link deliveredLink,
	claims map[ouroboros.ConnectionId]farTipClaim,
	includePending bool,
) (bool, bool) {
	claimsChanged := false
	// Reject implausible delivered frontiers that jump too far ahead of a
	// trusted reference point. Before any local block has been applied,
	// retain the advertised-tip bound as well: the first peer bootstraps the
	// reference, and later peers cannot immediately inject a far claim. Once
	// a local tip exists, the advertisement can legitimately be arbitrarily
	// far ahead during catch-up; only its delivered frontier drives
	// selection and must remain plausible. Three cases:
	//  1. Known peer: compare against the peer's own previous
	//     observed frontier — chainsync advances incrementally so the delta
	//     is always small. Always checked (even if prev == 0).
	//  2. New peer with existing peers: compare against the
	//     best observed peer frontier to prevent a malicious newcomer
	//     from injecting an extremely high delivered block number.
	//  3. First peer ever: no reference exists, accept to
	//     allow bootstrap.
	//
	// A peer registered from a chainsync rollback has delivered no header, so
	// it is not a reference for anyone, including itself: counting its zero
	// delivered frontier would turn case 3 into a reference of 0 and reject
	// the next peer's first legitimate header, and counting its unverified
	// advertisement would raise the bootstrap advertised bound. It is bounded
	// like a brand-new peer when its own first header arrives.
	if cs.securityParam > 0 {
		// Callers without a distinct delivered frontier pass the
		// advertised tip for both values, as UpdatePeerTip does. An
		// all-zero delivered frontier therefore means the peer has
		// delivered nothing, and is bounded as block 0 rather than
		// being credited with its untrusted advertisement.
		observedBlock := observedTip.BlockNumber
		observedReject := false
		advertisedReject := false
		hasReference := false
		var referenceBlock uint64
		var advertisedReferenceBlock uint64
		var maxPlausibleBlock uint64
		var maxPlausibleAdvertisedBlock uint64
		prevTip := cs.peerTips[connId]
		if includePending {
			if pendingTip := cs.latestPeerTipCandidateLocked(connId); pendingTip != nil {
				prevTip = pendingTip
			}
		}
		if prevTip != nil && !prevTip.awaitingFirstHeader {
			// Case 1: known peer — compare against the peer's own
			// previous delivered frontier.
			hasReference = true
			referenceBlock = prevTip.SelectionTip().BlockNumber
			advertisedReferenceBlock = prevTip.Tip.BlockNumber
		} else if len(cs.peerTips) > 0 ||
			(includePending && len(cs.pendingPeerTips) > 0) {
			// Case 2: new peer — check against the best observed and
			// advertised frontiers separately. Lone far frontiers do not count
			// as independent references for a new peer.
			// Nor does a peer whose advertised tip is wall-clock stale: it
			// serves an abandoned chain and is never selectable, so before a
			// block is applied its short advertisement would bound every
			// honest peer advertising the real tip.
			excludedLone := false
			for peerConn, committedTip := range cs.peerTips {
				pt := committedTip
				if includePending {
					if pendingTip := cs.latestPeerTipCandidateLocked(peerConn); pendingTip != nil {
						pt = pendingTip
					}
				}
				if pt == nil || pt.awaitingFirstHeader ||
					cs.advertisedTipStaleLocked(pt) {
					continue
				}
				if claim, ok := claims[peerConn]; ok && claim.lone {
					excludedLone = true
					continue
				}
				hasReference = true
				blockNumber := pt.SelectionTip().BlockNumber
				if blockNumber > referenceBlock {
					referenceBlock = blockNumber
				}
				if pt.Tip.BlockNumber > advertisedReferenceBlock {
					advertisedReferenceBlock = pt.Tip.BlockNumber
				}
			}
			if includePending {
				for peerConn := range cs.pendingPeerTips {
					if _, exists := cs.peerTips[peerConn]; exists {
						continue
					}
					pt := cs.latestPeerTipCandidateLocked(peerConn)
					if pt == nil || pt.awaitingFirstHeader ||
						cs.advertisedTipStaleLocked(pt) {
						continue
					}
					if claim, ok := claims[peerConn]; ok && claim.lone {
						excludedLone = true
						continue
					}
					hasReference = true
					if blockNumber := pt.SelectionTip().BlockNumber; blockNumber > referenceBlock {
						referenceBlock = blockNumber
					}
					if pt.Tip.BlockNumber > advertisedReferenceBlock {
						advertisedReferenceBlock = pt.Tip.BlockNumber
					}
				}
			}
			// If every delivered frontier is lone-only, keep the local tip as
			// the trusted reference. Treating the new peer as a bootstrap peer
			// here would admit its first unrelated far header without a run.
			if !hasReference && excludedLone && cs.localTip.BlockNumber > 0 {
				hasReference = true
				referenceBlock = cs.localTip.BlockNumber
			}
		}
		if hasReference {
			maxPlausibleBlock = safeAddUint64(
				referenceBlock,
				cs.securityParam,
			)
			observedReject = observedBlock > maxPlausibleBlock
			maxPlausibleAdvertisedBlock = safeAddUint64(
				advertisedReferenceBlock,
				cs.securityParam,
			)
			advertisedReject = tip.BlockNumber >
				maxPlausibleAdvertisedBlock
		}
		// Catch-up relaxation: after a stall, recorded peer tips
		// go stale while the network advances. A peer whose
		// delta from the stale reference exceeds K looks
		// implausible, but the network legitimately moved on.
		// Keep the peer-relative K bound while the reference is within
		// K of the local tip. A farther reference can be stale while the
		// local tip catches up, however far ahead of the local tip it is.
		// A corroborated far claim is this connection's own reference, as a
		// known peer's previous frontier is in Case 1. The frontier that
		// corroborated it may sit up to K below it, so the Case 2 ceiling
		// (that frontier + K) alone would reject the claimant's next header.
		if observedReject {
			if claim, ok := claims[connId]; ok && claim.corroborated {
				claimCeiling := safeAddUint64(claim.block, cs.securityParam)
				if observedBlock <= claimCeiling {
					observedReject = false
					maxPlausibleBlock = claimCeiling
				}
			}
		}
		catchUpCeiling := safeAddUint64(
			cs.localTip.BlockNumber,
			safeAddUint64(cs.securityParam, cs.securityParam),
		)
		catchUpFloor := safeAddUint64(
			cs.localTip.BlockNumber,
			cs.securityParam,
		)
		referenceStale := referenceBlock <= cs.localTip.BlockNumber
		// A reference beyond the ceiling is as stale as one inside it: the
		// network tip has no upper bound over a node that is far behind, and
		// recorded references only lag the peers delivering the chain.
		referenceAheadOfCatchUpFloor := referenceBlock > catchUpFloor
		if observedReject && cs.localTip.BlockNumber > 0 &&
			(referenceStale || referenceAheadOfCatchUpFloor) {
			maxPlausibleBlock = max(maxPlausibleBlock, catchUpCeiling)
			observedReject = observedBlock > maxPlausibleBlock
			// The 2*K catch-up ceiling still assumes the honest network tip
			// is at most a bounded stall away from the local tip. That
			// assumption fails for a node legitimately far behind (a
			// from-genesis sync, a long outage): the true gap to the honest
			// tip has no upper bound, so a fixed ceiling anchored to
			// localTip makes an honestly-delivered frontier permanently
			// unreachable. A rejected frontier is never recorded, so it can
			// never itself become a fresher reference -- a one-way ratchet.
			//
			// References can be stale while a snapshot-backed or
			// from-genesis local tip lags behind the network.
			// Nothing about this frontier can be verified yet: its epoch is
			// beyond the ledger's forecast horizon, so header verification
			// is deferred before any signature is checked. Require
			// corroboration instead: another, distinct connection has
			// delivered a frontier within K of it, or this connection has
			// delivered a connected header chain
			// (see corroborateFarTipClaimLocked). A single claim, honest or
			// fabricated, cannot break the ratchet by itself.
			//
			// With the reference itself beyond the catch-up ceiling nothing
			// anchors the frontier near the local chain, and one peer can
			// fabricate any number of self-linked headers. There only a
			// connection from another identity group corroborates
			// (corroborateIndependentFarTipClaimLocked).
			if observedReject {
				corroborate := cs.corroborateFarTipClaimTrackedLocked
				if referenceBlock > catchUpCeiling {
					corroborate = cs.corroborateIndependentFarTipClaimLocked
				}
				corroborated, changed := corroborate(
					connId, observedTip, link, claims,
				)
				claimsChanged = claimsChanged || changed
				if corroborated {
					observedReject = false
					maxPlausibleBlock = observedBlock
				}
			}
		}
		// The advertised references are only the peers that delivered a
		// header first, and nothing has verified their advertisements
		// either. A reference lagging far behind the network would otherwise
		// refuse every honest peer, and refused peers never become
		// references, so bootstrap would wedge on it. A claim another remote
		// host independently agrees with is therefore not a lone far claim.
		if advertisedReject && !observedReject &&
			cs.localTip.BlockNumber == 0 {
			if cs.advertisedClaimCorroboratedLocked(
				connId, tip, claims, includePending,
			) {
				advertisedReject = false
			} else {
				cs.recordRejectedAdvertisedClaimLocked(connId, tip)
			}
		}
		// Case 3: len(peerTips)==0 && peer not known → bootstrap
		if observedReject ||
			(advertisedReject && cs.localTip.BlockNumber == 0) {
			// Admission replays and commit re-checks re-evaluate candidates
			// that may already have been refused; report each candidate's
			// first refusal only, whether it happens live or on replay.
			if !cs.markPeerTipRejectionReportedLocked(
				connId, tip, observedTip,
			) {
				return false, claimsChanged
			}
			cs.config.Logger.Warn(
				"rejecting implausible peer tip",
				"connection_id", connId.String(),
				"claimed_block", tip.BlockNumber,
				"observed_block", observedBlock,
				"reference_block", referenceBlock,
				"advertised_reference_block",
				advertisedReferenceBlock,
				"local_block", cs.localTip.BlockNumber,
				"security_param", cs.securityParam,
				"max_plausible_block", maxPlausibleBlock,
				"max_plausible_advertised_block",
				maxPlausibleAdvertisedBlock,
			)
			if cs.config.OnPeerTipRejected != nil {
				// A frontier rejection takes precedence when both bounds
				// refused the tip.
				reason := PeerTipRejectedAdvertisedTip
				if observedReject {
					reason = PeerTipRejectedObservedFrontier
				}
				cs.config.OnPeerTipRejected(reason)
			}
			return false, claimsChanged
		}
		// Accepted: this connection no longer needs to be tracked as a
		// pending far-tip claim, whether or not it ever was one. A claim
		// accepted on its own chain keeps its entry, which marks the
		// connection's frontier for revocation (loneFrontierBrokenLocked).
		if claim, ok := claims[connId]; !ok || !claim.lone {
			if ok {
				delete(claims, connId)
				claimsChanged = true
			}
		}
	}
	return true, claimsChanged
}

// rejectedAdvertisedClaim is an advertised tip the bootstrap advertised bound
// refused, keyed by the remote host that advertised it.
type rejectedAdvertisedClaim struct {
	host  string
	block uint64
}

// connectionRemoteHost returns the remote host of connId without its port, so
// reconnections from one host count as one witness.
func connectionRemoteHost(connId ouroboros.ConnectionId) string {
	if connId.RemoteAddr == nil {
		return connId.String()
	}
	addr := connId.RemoteAddr.String()
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

// advertisedClaimCorroboratedLocked reports whether an advertised tip that
// exceeded the bootstrap advertised bound is supported by a connection from
// another remote host advertising a tip within securityParam of it: a tracked
// peer (including one registered from a rollback that has delivered no header
// yet), or a recently refused claim. A lone far frontier is not a witness, and
// a claim no chain could have reached by its slot is never corroborated. Like
// corroborateFarTipClaimLocked this counts hosts, not operators, so it is no
// Sybil defence; it only stops one connection from moving the bound alone.
// Must be called with cs.mutex held.
func (cs *ChainSelector) advertisedClaimCorroboratedLocked(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	claims map[ouroboros.ConnectionId]farTipClaim,
	includePending bool,
) bool {
	if !cs.blockWithinSlotDistanceLocked(tip) {
		return false
	}
	host := connectionRemoteHost(connId)
	agrees := func(otherHost string, block uint64) bool {
		if otherHost == host {
			return false
		}
		lo, hi := min(tip.BlockNumber, block), max(tip.BlockNumber, block)
		return safeAddUint64(lo, cs.securityParam) >= hi
	}
	witness := func(other ouroboros.ConnectionId, pt *PeerChainTip) bool {
		if pt == nil || other == connId {
			return false
		}
		if claim, ok := claims[other]; ok && claim.lone {
			return false
		}
		return agrees(connectionRemoteHost(other), pt.Tip.BlockNumber)
	}
	for other, pt := range cs.peerTips {
		if witness(other, pt) {
			return true
		}
	}
	if includePending {
		for other := range cs.pendingPeerTips {
			if witness(other, cs.latestPeerTipCandidateLocked(other)) {
				return true
			}
		}
	}
	for _, refused := range cs.rejectedAdvertisedClaims {
		if agrees(refused.host, refused.block) {
			return true
		}
	}
	return false
}

// recordRejectedAdvertisedClaimLocked remembers connId's refused advertised
// tip as a witness for later connections, replacing that host's previous
// entry and evicting the oldest entry when full. Must be called with cs.mutex
// held.
func (cs *ChainSelector) recordRejectedAdvertisedClaimLocked(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
) {
	if cs.maxTrackedPeers <= 0 {
		return
	}
	host := connectionRemoteHost(connId)
	cs.rejectedAdvertisedClaims = slices.DeleteFunc(
		cs.rejectedAdvertisedClaims,
		func(c rejectedAdvertisedClaim) bool { return c.host == host },
	)
	if len(cs.rejectedAdvertisedClaims) >= cs.maxTrackedPeers {
		cs.rejectedAdvertisedClaims = slices.Delete(
			cs.rejectedAdvertisedClaims,
			0,
			len(cs.rejectedAdvertisedClaims)-cs.maxTrackedPeers+1,
		)
	}
	cs.rejectedAdvertisedClaims = append(
		cs.rejectedAdvertisedClaims,
		rejectedAdvertisedClaim{host: host, block: tip.BlockNumber},
	)
}

// corroborateFarTipClaimLocked records connId's delivered frontier, which
// exceeded the catch-up plausibility ceiling, and reports whether it is now
// accepted. Two things accept it:
//
//   - corroboration: at least one OTHER distinct connection has independently
//     delivered a frontier within securityParam of it. Every such other claim
//     is marked corroborated, so its connection's next update is bounded
//     against its own claim rather than against this lower frontier.
//   - a connected header chain: connId alone has delivered more than
//     securityParam consecutive headers, each naming the previous one as its
//     parent, one block and a later slot above it, with the block number
//     within the slot distance from the local tip (a chain has at most one
//     block per slot). A Byron epoch boundary block and the block after it are
//     handled by deliveredLink.step. A repeated header neither advances nor
//     resets the run; any other break restarts it. The accepted connection
//     stays marked lone and is not a witness for another connection's claim.
//
// The chain acceptance is not verification. At these slots the ledger has
// verified nothing about a header, and a liar's own keys would pass any
// signature check that is possible, so a fabricated chain can still be built.
// What it cannot do is claim an arbitrary height, skip or reorder headers, or
// keep standing once a header fails: see loneFrontierBrokenLocked, RemovePeer
// on the failed connection, and the ledger, which verifies every applied
// header. Below the catch-up ceiling nothing here tells two connections to
// one operator from two independent peers, so corroboration there is not a
// Sybil defence either; beyond it see corroborateIndependentFarTipClaimLocked.
//
// Pending entries recorded here are provisional: they never enter cs.peerTips
// and so never influence chain selection, corroboration, or the Genesis exit
// horizon by themselves. An accepted lone claim remains alongside its peer
// tip only to enforce revocation and is excluded from other peers' references.
//
// Must be called with cs.mutex held and cs.securityParam > 0.
func (cs *ChainSelector) corroborateFarTipClaimTrackedLocked(
	connId ouroboros.ConnectionId,
	observedTip ochainsync.Tip,
	link deliveredLink,
	claims map[ouroboros.ConnectionId]farTipClaim,
) (bool, bool) {
	return cs.corroborateFarTipClaimWithLocked(
		connId, observedTip, link, claims, false,
	)
}

// corroborateIndependentFarTipClaimLocked is corroborateFarTipClaimLocked
// for a frontier whose reference is beyond the catch-up ceiling. Only a claim
// from a connection in another identity group (corroboratorIdentity)
// corroborates it, and a connected header chain from connId alone does not:
// its run is still counted but never accepts the claim. A connection without
// an identity fails closed. Must be called with cs.mutex held and
// cs.securityParam > 0.
func (cs *ChainSelector) corroborateIndependentFarTipClaimLocked(
	connId ouroboros.ConnectionId,
	observedTip ochainsync.Tip,
	link deliveredLink,
	claims map[ouroboros.ConnectionId]farTipClaim,
) (bool, bool) {
	return cs.corroborateFarTipClaimWithLocked(
		connId, observedTip, link, claims, true,
	)
}

func (cs *ChainSelector) corroborateFarTipClaimWithLocked(
	connId ouroboros.ConnectionId,
	observedTip ochainsync.Tip,
	link deliveredLink,
	claims map[ouroboros.ConnectionId]farTipClaim,
	independentOnly bool,
) (bool, bool) {
	if !cs.blockWithinSlotDistanceLocked(observedTip) {
		_, existed := claims[connId]
		delete(claims, connId)
		return false, existed
	}
	claimed := observedTip.BlockNumber
	if claims == nil {
		return false, false
	}
	changed := false
	// Bounded like peerTips: connection churn must not grow this map without
	// limit. A claim that does not fit is still compared against the recorded
	// ones, so a full table cannot keep its own claims from being
	// corroborated.
	prev, exists := claims[connId]
	updated := farTipClaim{
		block:        claimed,
		slot:         observedTip.Point.Slot,
		hash:         bytes.Clone(observedTip.Point.Hash),
		boundary:     link.boundary,
		corroborated: prev.corroborated,
		lone:         prev.lone,
		run:          1,
	}
	switch {
	case !exists:
	case len(observedTip.Point.Hash) > 0 &&
		bytes.Equal(observedTip.Point.Hash, prev.hash):
		// A repeat shows no progress and does not break the run.
		updated.run = prev.run
	default:
		switch link.step(prev.block, prev.slot, prev.hash, prev.boundary, observedTip) {
		case stepAdvance:
			updated.run = prev.run + 1
		case stepBoundary:
			updated.run = prev.run
		case stepBroken:
		}
	}
	if exists || len(claims) < cs.maxTrackedPeers {
		changed = !exists || !farTipClaimEqual(prev, updated)
		claims[connId] = updated
	}
	identity, hasIdentity := cs.corroboratorIdentity(connId)
	corroborated := false
	for otherConn, other := range claims {
		if otherConn == connId || other.lone {
			continue
		}
		lo, hi := claimed, other.block
		if lo > hi {
			lo, hi = hi, lo
		}
		if safeAddUint64(lo, cs.securityParam) < hi {
			continue
		}
		otherIdentity, otherHasIdentity := cs.corroboratorIdentity(otherConn)
		independent := hasIdentity && otherHasIdentity &&
			otherIdentity != identity
		if independentOnly && !independent {
			continue
		}
		if !other.corroborated {
			other.corroborated = true
			claims[otherConn] = other
			changed = true
		}
		corroborated = true
	}
	if corroborated {
		return true, changed
	}
	if !independentOnly && updated.run > cs.securityParam {
		if !updated.lone {
			updated.lone = true
			claims[connId] = updated
			changed = true
		}
		return true, changed
	}
	return false, changed
}

func farTipClaimEqual(a, b farTipClaim) bool {
	return a.block == b.block &&
		a.slot == b.slot &&
		bytes.Equal(a.hash, b.hash) &&
		a.boundary == b.boundary &&
		a.corroborated == b.corroborated &&
		a.lone == b.lone &&
		a.run == b.run
}

// blockWithinSlotDistanceLocked reports whether observedTip's block number is
// reachable from the local tip: a chain has at most one block per slot, so it
// may exceed the local block number by at most the slot distance from the
// local tip, plus securityParam for a local tip on a fork within K of the
// delivered chain. It is a loose bound -- the real chain grows at a fraction
// of one block per slot -- that still rejects a claimed height no chain could
// have reached. Must be called with cs.mutex held.
func (cs *ChainSelector) blockWithinSlotDistanceLocked(
	observedTip ochainsync.Tip,
) bool {
	localSlot := cs.localTip.Point.Slot
	slot := max(observedTip.Point.Slot, localSlot)
	maxBlock := safeAddUint64(
		cs.localTip.BlockNumber,
		safeAddUint64(cs.securityParam, slot-localSlot),
	)
	return observedTip.BlockNumber <= maxBlock
}

// loneFrontierBrokenLocked reports whether connId's frontier was accepted on
// its own chain (see corroborateFarTipClaimLocked) and this delivery does not
// continue it: the header must name the previously delivered header as its
// parent and sit at a later slot, and at one block above it when that block
// number is known (a rollback outside the retained history leaves it zero). A
// repeated header continues it. The caller then removes the peer, so a
// frontier that stops being a connected chain loses its standing at once.
// The mark is dropped once the frontier is back within the ordinary
// catch-up ceiling, where the ordinary bound applies, but only from a known
// height: after a rollback outside the retained history the previous height
// reads zero, which is below any ceiling, so the next header is held to the
// chain and, continuing nothing, removes the peer. Must be called with
// cs.mutex held.
func (cs *ChainSelector) loneFrontierBrokenLocked(
	connId ouroboros.ConnectionId,
	observedTip ochainsync.Tip,
	link deliveredLink,
	claims map[ouroboros.ConnectionId]farTipClaim,
) bool {
	claim, ok := claims[connId]
	if !ok || !claim.lone {
		return false
	}
	peerTip := cs.peerTips[connId]
	if peerTip == nil {
		delete(claims, connId)
		return false
	}
	ceiling := safeAddUint64(
		cs.localTip.BlockNumber,
		safeAddUint64(cs.securityParam, cs.securityParam),
	)
	if peerTip.ObservedTip.BlockNumber != 0 &&
		peerTip.ObservedTip.BlockNumber <= ceiling &&
		observedTip.BlockNumber <= ceiling {
		delete(claims, connId)
		return false
	}
	last := peerTip.ObservedTip
	if len(observedTip.Point.Hash) > 0 &&
		bytes.Equal(observedTip.Point.Hash, last.Point.Hash) {
		return false
	}
	// Only the latest delivered header's type is kept. After a RollBackward to
	// an earlier boundary block, last is that block but the claim describes a
	// later header, so lastBoundary is false and a next block sharing the
	// boundary's slot removes the peer. Honest peers roll back only near their
	// own tip, never inside the far history a lone frontier is built from.
	lastBoundary := claim.boundary && bytes.Equal(claim.hash, last.Point.Hash)
	if cs.blockWithinSlotDistanceLocked(observedTip) &&
		link.step(
			last.BlockNumber, last.Point.Slot, last.Point.Hash,
			lastBoundary, observedTip,
		) != stepBroken {
		claim.block = observedTip.BlockNumber
		claim.slot = observedTip.Point.Slot
		claim.hash = bytes.Clone(observedTip.Point.Hash)
		claim.boundary = link.boundary
		claims[connId] = claim
		return false
	}
	cs.config.Logger.Warn(
		"revoking lone far peer frontier: delivery does not continue it",
		"connection_id", connId.String(),
		"observed_block", observedTip.BlockNumber,
		"last_observed_block", last.BlockNumber,
		"local_block", cs.localTip.BlockNumber,
	)
	return true
}

// deliveredLink is what a delivered header says about how it attaches to the
// chain: the hash of the parent it names, and whether it is a Byron epoch
// boundary block.
type deliveredLink struct {
	prevHash []byte
	boundary bool
}

type linkStep int

const (
	// stepBroken means the header does not continue the previous one.
	stepBroken linkStep = iota
	// stepAdvance means the header is the next block of the chain.
	stepAdvance
	// stepBoundary means the header is an epoch boundary block: it continues
	// the chain without adding a block.
	stepBoundary
)

// step classifies observed against the previous delivered header (lastBlock,
// lastSlot, lastHash, lastBoundary). A header must name the previous one as
// its parent and not sit at an earlier slot. An ordinary header is then one
// block above it, and at a later slot -- or at the same slot when the previous
// header was a boundary block, which sits at the first slot of its epoch that
// the epoch's first block may also occupy. A boundary block carries its
// parent's block number and a later slot, and must follow an ordinary header,
// so two consecutive boundary blocks, or a boundary block repeating a number
// that is not its parent's, break the chain. A zero lastBlock means the block
// number is not known (a rollback outside the retained history, which the
// peer chose and nothing validated), so no header can continue it: carrying a
// run or an accepted frontier over an unknown height would let one header
// name any height. The flag is the header type the peer sent, not proof of an era:
// it buys no height, since a boundary block adds none and cannot repeat.
func (l deliveredLink) step(
	lastBlock, lastSlot uint64,
	lastHash []byte,
	lastBoundary bool,
	observed ochainsync.Tip,
) linkStep {
	if len(l.prevHash) == 0 || !bytes.Equal(l.prevHash, lastHash) ||
		observed.Point.Slot < lastSlot {
		return stepBroken
	}
	if lastBlock == 0 {
		return stepBroken
	}
	if l.boundary {
		if lastBoundary || observed.Point.Slot == lastSlot ||
			observed.BlockNumber != lastBlock {
			return stepBroken
		}
		return stepBoundary
	}
	if observed.BlockNumber != safeAddUint64(lastBlock, 1) {
		return stepBroken
	}
	if observed.Point.Slot == lastSlot && !lastBoundary {
		return stepBroken
	}
	return stepAdvance
}

// farTipClaim is a connection's most recent delivered frontier beyond the
// catch-up plausibility ceiling. corroborated is set once another connection
// has delivered a frontier within securityParam of block.
type farTipClaim struct {
	block        uint64
	slot         uint64
	hash         []byte
	boundary     bool
	corroborated bool
	// lone marks a claim accepted on the connection's own header chain rather
	// than by another connection's agreement.
	lone bool
	// run counts the connection's consecutive connected headers, starting at
	// 1. A repeated header leaves it unchanged; anything else that does not
	// continue the previous header restarts it at 1.
	run uint64
}

// makeRoomForNewPeerLocked makes room in the tracked-peer table for a new
// entry for connId, evicting the least-recently-updated peer when the table
// is at capacity. It returns the evicted connection (nil when no eviction was
// needed) and whether there is room; the caller publishes PeerEvictedEvent for
// a non-nil eviction outside the lock. Callers must hold cs.mutex.
func (cs *ChainSelector) makeRoomForNewPeerLocked(
	connId ouroboros.ConnectionId,
) (*ouroboros.ConnectionId, bool) {
	if len(cs.peerTips) < cs.maxTrackedPeers {
		return nil, true
	}
	evicted := cs.evictLeastRecentPeerLocked()
	if evicted == nil {
		cs.config.Logger.Warn(
			"cannot accept new peer: at capacity and best peer is the only tracked peer",
			"connection_id", connId.String(),
			"peer_count", len(cs.peerTips),
			"max_tracked_peers", cs.maxTrackedPeers,
		)
		return nil, false
	}
	return evicted, true
}

func (cs *ChainSelector) TouchPeerActivity(connId ouroboros.ConnectionId) {
	if cs.config.ConnectionLive != nil &&
		!cs.config.ConnectionLive(connId) {
		cs.config.Logger.Debug(
			"ignoring peer activity from closed connection",
			"connection_id", connId.String(),
		)
		return
	}
	var switchEvent *event.Event
	var selectionEvent *event.Event
	var corroborationEvent *event.Event

	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()

		peerTip, exists := cs.peerTips[connId]
		if !exists {
			return
		}
		peerTip.Touch()
		_, switchEvent, selectionEvent, corroborationEvent = cs.evaluateBestPeerLocked()
	}()

	cs.publishSelectionEvents(switchEvent, selectionEvent, corroborationEvent)
}

// SetPeerIdling records an explicit ChainSync MsgAwaitReply at point. A later
// roll-forward or rollback clears it; a delayed event for an older point is
// ignored.
func (cs *ChainSelector) SetPeerIdling(
	connId ouroboros.ConnectionId,
	point ocommon.Point,
) {
	cs.mutex.Lock()
	peerTip := cs.peerTips[connId]
	matched := peerTip != nil &&
		peerTip.ObservedTip.Point.Slot == point.Slot &&
		bytes.Equal(peerTip.ObservedTip.Point.Hash, point.Hash)
	if matched {
		peerTip.idling = true
	}
	cs.mutex.Unlock()
	if matched {
		cs.EvaluateAndSwitch()
	}
}

// evictLeastRecentPeerLocked removes the peer with the oldest LastUpdated
// timestamp from the peerTips map. It never evicts the current best peer.
// When multiple peers share the same LastUpdated timestamp (common on
// Windows where clock resolution is ~15ms), the peer with the lowest
// block number is evicted first. If block numbers also tie, the
// connection ID string is used as a final deterministic tie-breaker.
// Returns a pointer to the evicted connection ID, or nil if no eviction
// was possible (e.g. the only tracked peer is the best peer).
// Must be called with cs.mutex held.
func (cs *ChainSelector) evictLeastRecentPeerLocked() *ouroboros.ConnectionId {
	var oldestConn ouroboros.ConnectionId
	var oldestUpdated time.Time
	var oldestBlockNumber uint64
	found := false

	for connId, peerTip := range cs.peerTips {
		if peerTip == nil {
			continue
		}
		// Never evict the current best peer
		if cs.bestPeerConn != nil && *cs.bestPeerConn == connId {
			continue
		}
		if !found {
			oldestConn = connId
			oldestUpdated = peerTip.LastUpdated
			oldestBlockNumber = peerTip.SelectionTip().BlockNumber
			found = true
			continue
		}
		// Primary: oldest LastUpdated wins eviction
		if peerTip.LastUpdated.Before(oldestUpdated) {
			oldestConn = connId
			oldestUpdated = peerTip.LastUpdated
			oldestBlockNumber = peerTip.SelectionTip().BlockNumber
		} else if peerTip.LastUpdated.Equal(oldestUpdated) {
			// Tie-break on block number: evict the peer with
			// the lower block number (less useful chain)
			peerBlockNumber := peerTip.SelectionTip().BlockNumber
			if peerBlockNumber < oldestBlockNumber {
				oldestConn = connId
				oldestUpdated = peerTip.LastUpdated
				oldestBlockNumber = peerBlockNumber
			} else if peerBlockNumber == oldestBlockNumber {
				// Final tie-break: deterministic by connection ID
				if connId.String() < oldestConn.String() {
					oldestConn = connId
					oldestUpdated = peerTip.LastUpdated
					oldestBlockNumber = peerBlockNumber
				}
			}
		}
	}

	if found {
		cs.config.Logger.Debug(
			"evicting least-recent peer due to tracking limit",
			"connection_id", oldestConn.String(),
			"last_updated", oldestUpdated,
			"peer_count", len(cs.peerTips),
			"max_tracked_peers", cs.maxTrackedPeers,
		)
		cs.deletePeerLocked(oldestConn)
		return &oldestConn
	}
	return nil
}

func (cs *ChainSelector) deletePeerLocked(connId ouroboros.ConnectionId) {
	delete(cs.peerTips, connId)
	delete(cs.deliveredPeerTips, connId)
	delete(cs.pendingPeerTips, connId)
	delete(cs.pendingPeerTipFrontiers, connId)
	delete(cs.forecastHorizonWitnesses, connId)
	delete(cs.committedPeerTips, connId)
	delete(cs.eligible, connId)
	delete(cs.priority, connId)
	delete(cs.recentlyLeft, connId)
	delete(cs.farTipClaims, connId)
	delete(cs.genesisDensityDisconnected, connId)
	cs.rebuildPendingAdmissionStateLocked()
	cs.signalIngressChange()
}

// RemovePeer removes a peer from tracking.
func (cs *ChainSelector) RemovePeer(connId ouroboros.ConnectionId) {
	cs.removePeer(connId, false)
}

func (cs *ChainSelector) removePeer(
	connId ouroboros.ConnectionId,
	generationPinned bool,
) {
	var switchEvent *event.Event
	var reevaluate bool

	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()

		removedTip := cs.peerTips[connId]
		cs.deletePeerLocked(connId)

		// Removing a witness can revoke the incumbent's corroboration even
		// when the removed peer was not itself the best; force a re-evaluation
		// so an incumbent that just lost corroboration is dropped.
		reevaluate = cs.genesisCorroborationActiveLocked() &&
			(cs.bestPeerConn == nil || *cs.bestPeerConn != connId)

		if cs.bestPeerConn != nil && *cs.bestPeerConn == connId {
			previousBest := *cs.bestPeerConn
			cs.bestPeerConn = nil
			cs.config.Logger.Info(
				"best peer disconnected, selecting new best",
				"connection_id", connId.String(),
			)
			// Immediately select a new best peer from remaining peers
			newBest := cs.selectBestChainLocked()
			cs.bestPeerConn = newBest

			if newBest != nil {
				cs.config.Logger.Info(
					"selected new best peer after disconnect",
					"connection_id", newBest.String(),
				)
				// Emit ChainSwitchEvent so subscribers know to switch connections
				if cs.config.EventBus != nil {
					newPeerTip := cs.peerTips[*newBest]
					newSelectionTip := newPeerTip.SelectionTip()
					evt := event.NewEvent(
						ChainSwitchEventType,
						ChainSwitchEvent{
							PreviousConnectionId: previousBest,
							NewConnectionId:      *newBest,
							NewTip:               newPeerTip.Tip,
							NewObservedTip:       newSelectionTip,
							NewObservedTipSet:    true,
							ComparisonResult:     ChainComparisonUnknown,
							BlockDifference: safeUint64ToInt64(
								newPeerTip.Tip.BlockNumber,
							),
							RollbackPoint: fragmentIntersection(
								removedTip, newPeerTip,
							),
						},
					)
					switchEvent = &evt
				}
			} else {
				// No replacement selected: this is a selected-to-none transition,
				// so stage the explicit event like the main evaluation path.
				cs.stageSelectedNoneLocked(previousBest)
			}
		}
	}()

	// Publish event outside the lock to prevent deadlock if subscribers
	// call back into ChainSelector
	if switchEvent != nil {
		cs.publishSelection(ChainSwitchEventType, *switchEvent)
	}
	cs.publishPendingGenesisExitEvent()
	cs.publishPendingSelectedNoneEvent()
	if reevaluate {
		cs.evaluateAndSwitch(generationPinned)
	}
}

// SetLocalTip updates the local chain tip for comparison.
//
// It also records the last time the APPLIED local tip moved FORWARD (its
// block number advanced). The anti-flap incumbent pin uses this timestamp as
// a progress-aware escape: if the applied tip stops advancing while the pin
// is engaged, the pin releases. Same-tip updates and rollbacks deliberately do
// NOT reset the stall clock, so a stalled incumbent cannot keep the pin alive
// by re-reporting an unchanged tip. Forward progress after a rollback does
// reset the clock because the comparison is against the previous applied tip,
// not an all-time high-water mark.
func (cs *ChainSelector) SetLocalTip(tip ochainsync.Tip) {
	shouldEvaluate := false
	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()
		if tip.BlockNumber > cs.localTipProgressBlock {
			cs.localTipProgressAt = cs.now()
		}
		cs.localTipProgressBlock = tip.BlockNumber
		cs.localTip = tip
		if len(cs.pendingFarTipClaims) > 0 {
			cs.rebuildPendingAdmissionStateLocked()
		}
		shouldEvaluate = cs.advanceSelectionModeLocked()
	}()
	if shouldEvaluate {
		cs.EvaluateAndSwitch()
	}
}

// refreshAppliedLocalTipLocked replaces the local tip with the live applied
// tip when an AppliedTip source is configured. It leaves the progress clock
// that SetLocalTip maintains to SetLocalTip. Must be called with cs.mutex held
// for writing.
func (cs *ChainSelector) refreshAppliedLocalTipLocked() {
	if cs.config.AppliedTip == nil {
		return
	}
	if tip, ok := cs.config.AppliedTip(); ok {
		cs.localTip = tip
	}
}

// now returns the current time via the injectable clock. Must be called with
// the mutex held (it reads cs.nowFn). Defaults to time.Now when nowFn is unset
// (e.g. for selectors constructed without NewChainSelector in tests).
func (cs *ChainSelector) now() time.Time {
	if cs.nowFn != nil {
		return cs.nowFn()
	}
	return time.Now()
}

// SetSecurityParam updates the security parameter (k) dynamically.
// This allows the selector to use protocol parameters for density-based
// comparison.
func (cs *ChainSelector) SetSecurityParam(k uint64) {
	shouldEvaluate := false
	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()
		cs.securityParam = k
		pendingLimit := cs.pendingPeerTipAdmissionLimitLocked()
		for connId, pending := range cs.pendingPeerTips {
			if len(pending) > pendingLimit {
				cs.pendingPeerTips[connId] = append(
					[]peerTipAdmissionCandidate(nil),
					pending[len(pending)-pendingLimit:]...,
				)
			}
		}
		cs.rebuildPendingAdmissionStateLocked()
		shouldEvaluate = cs.advanceSelectionModeLocked()
		// advanceSelectionModeLocked already refreshes the snapshot when it
		// transitions the mode. Refresh again unconditionally: the window it
		// caches also depends on securityParam, which just changed here, even
		// when the mode does not transition.
		cs.refreshGenesisSelectionSnapshotLocked()
	}()
	if shouldEvaluate {
		cs.EvaluateAndSwitch()
	}
}

// SelectionMode returns the selector's current mode.
func (cs *ChainSelector) SelectionMode() SelectionMode {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	return cs.mode
}

// GenesisWindowSlots returns the slot window used for Genesis density checks.
func (cs *ChainSelector) GenesisWindowSlots() uint64 {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	return cs.genesisWindowSlotsLocked()
}

// GenesisSelectionState returns an atomic snapshot of whether Genesis
// selection is active and the density window it is using. Composition code
// injects this narrow state query into fork resolution so the authoritative
// local decision follows the selector's one-way Genesis-to-Praos transition.
//
// This deliberately does not take cs.mutex. Callers are function-valued
// callbacks wired into other subsystems' hot paths
// (chainsync.State.RecordObservedHeader, LedgerState.recordPeerHeaderHistory),
// which hold their own lock (chainsync.State.clientConnIdMutex) across the
// call. Taking cs.mutex.RLock() here previously created a lock-order
// inversion against a separate path that holds cs.mutex (write) and calls
// back into chainsync.State.BlockfetchLatency (clientConnIdMutex.RLock),
// which deadlocked chain sync under real peer traffic. The
// snapshot below is kept current by refreshGenesisSelectionSnapshotLocked,
// called under cs.mutex at every point that can change it.
func (cs *ChainSelector) GenesisSelectionState() (bool, uint64) {
	snapshot := cs.genesisSelection.Load()
	if snapshot == nil {
		// A ChainSelector that did not come from NewChainSelector has never
		// published a snapshot. Answer as the pre-cache implementation did
		// for that zero value: Praos, and the default window.
		return false, DefaultGenesisWindowSlots
	}
	return snapshot.active, snapshot.window
}

// GetBestPeer returns the connection ID of the peer with the best chain, or
// nil if no suitable peer is available.
func (cs *ChainSelector) GetBestPeer() *ouroboros.ConnectionId {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	return cs.bestPeerConn
}

// GetPeerTip returns a deep copy of the chain tip for a specific peer.
// Returns nil if the peer is not tracked.
func (cs *ChainSelector) GetPeerTip(
	connId ouroboros.ConnectionId,
) *PeerChainTip {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	pt := cs.peerTips[connId]
	if pt == nil {
		return nil
	}
	tipCopy := *pt
	if pt.VRFOutput != nil {
		tipCopy.VRFOutput = make([]byte, len(pt.VRFOutput))
		copy(tipCopy.VRFOutput, pt.VRFOutput)
	}
	tipCopy.PraosView = clonePraosTiebreakerView(pt.PraosView)
	if len(pt.observedSlots) > 0 {
		tipCopy.observedSlots = make([]uint64, len(pt.observedSlots))
		copy(tipCopy.observedSlots, pt.observedSlots)
	}
	tipCopy.observedPoints = cloneObservedPoints(pt.observedPoints)
	if len(pt.observedTipHistory) > 0 {
		tipCopy.observedTipHistory = make(
			[]ochainsync.Tip,
			len(pt.observedTipHistory),
		)
		for i, tip := range pt.observedTipHistory {
			tipCopy.observedTipHistory[i] = cloneObservedTip(tip)
		}
	}
	return &tipCopy
}

// GetPeerSyncTarget returns a peer's advertised head only after its delivered
// frontier is close enough to corroborate that advertisement.
func (cs *ChainSelector) GetPeerSyncTarget(
	connId ouroboros.ConnectionId,
) (ochainsync.Tip, bool) {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	peerTip := cs.peerTips[connId]
	if !cs.isPeerSelectableLocked(connId, peerTip) {
		return ochainsync.Tip{}, false
	}
	observed := peerTip.SelectionTip()
	// peerTip is nil when connId has no entry, but isPeerSelectableLocked
	// returns false for a nil tip through peerLiveEligibleNonStaleLocked
	// (chainselection/genesis_corroboration.go), so the early return above
	// covers the absent-key case. nilaway does not correlate that guard
	// across the two functions.
	//nolint:nilaway // guarded by isPeerSelectableLocked above
	advertised := peerTip.Tip
	if safeAddUint64(
		observed.Point.Slot,
		cs.genesisWindowSlotsLocked(),
	) < advertised.Point.Slot {
		return observed, observed.Point.Slot != 0 || observed.BlockNumber != 0
	}
	if advertised.Point.Slot == 0 && advertised.BlockNumber == 0 {
		return observed, observed.Point.Slot != 0 || observed.BlockNumber != 0
	}
	return advertised, true
}

// SyncTargetForPeerTipUpdate applies the bounded-target policy to the exact
// observed/advertised pair carried by one chainsync event. It intentionally
// does not read the mutable per-peer tip map.
func (cs *ChainSelector) SyncTargetForPeerTipUpdate(
	update PeerTipUpdateEvent,
) (ochainsync.Tip, bool) {
	cs.mutex.RLock()
	window := cs.genesisWindowSlotsLocked()
	cs.mutex.RUnlock()
	observed := update.ObservedTip
	if observed.Point.Slot == 0 && observed.BlockNumber == 0 {
		return ochainsync.Tip{}, false
	}
	if safeAddUint64(observed.Point.Slot, window) < update.Tip.Point.Slot {
		return observed, true
	}
	if update.Tip.Point.Slot == 0 && update.Tip.BlockNumber == 0 {
		return observed, true
	}
	return update.Tip, true
}

// GetAllPeerTips returns a deep copy of all tracked peer tips.
func (cs *ChainSelector) GetAllPeerTips() map[ouroboros.ConnectionId]*PeerChainTip {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	result := make(
		map[ouroboros.ConnectionId]*PeerChainTip,
		len(cs.peerTips),
	)
	for k, v := range cs.peerTips {
		tipCopy := *v
		if v.VRFOutput != nil {
			tipCopy.VRFOutput = make([]byte, len(v.VRFOutput))
			copy(tipCopy.VRFOutput, v.VRFOutput)
		}
		tipCopy.PraosView = clonePraosTiebreakerView(v.PraosView)
		if len(v.observedSlots) > 0 {
			tipCopy.observedSlots = make([]uint64, len(v.observedSlots))
			copy(tipCopy.observedSlots, v.observedSlots)
		}
		tipCopy.observedPoints = cloneObservedPoints(v.observedPoints)
		if len(v.observedTipHistory) > 0 {
			tipCopy.observedTipHistory = make(
				[]ochainsync.Tip,
				len(v.observedTipHistory),
			)
			for i, tip := range v.observedTipHistory {
				tipCopy.observedTipHistory[i] = cloneObservedTip(tip)
			}
		}
		result[k] = &tipCopy
	}
	return result
}

// PeerCount returns the number of peers being tracked.
func (cs *ChainSelector) PeerCount() int {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	return len(cs.peerTips)
}

// SelectBestChain evaluates all peer tips and returns the connection ID of
// the peer with the best chain.
func (cs *ChainSelector) SelectBestChain() *ouroboros.ConnectionId {
	releaseGeneration, generationCurrent := cs.pinCurrentHeaderCryptoGeneration()
	if !generationCurrent {
		return cs.GetBestPeer()
	}
	defer releaseGeneration()
	cs.mutex.Lock()
	best := cs.selectBestChainLocked()
	cs.mutex.Unlock()
	// selectBestChainLocked may have exited Genesis mode; publish the staged
	// exit event outside the lock.
	cs.publishPendingGenesisExitEvent()
	return best
}

func (cs *ChainSelector) isPeerSelectableLocked(
	connId ouroboros.ConnectionId,
	peerTip *PeerChainTip,
) bool {
	return cs.isPeerSelectableWithBestKnownBlockLocked(
		connId,
		peerTip,
		false,
		nil,
	)
}

func (cs *ChainSelector) isPeerSelectableWithBestKnownBlockLocked(
	connId ouroboros.ConnectionId,
	peerTip *PeerChainTip,
	logSkip bool,
	bestKnownBlock *uint64,
) bool {
	if peerTip == nil {
		return false
	}
	// Shared live/eligible/non-stale prerequisite (single source of truth,
	// also used by the Genesis corroboration witness check).
	if !cs.peerLiveEligibleNonStaleLocked(connId, peerTip) {
		if logSkip {
			cs.config.Logger.Debug(
				"skipping closed, ineligible, or stale peer",
				"connection_id", connId.String(),
			)
		}
		return false
	}
	if cs.advertisedTipStaleLocked(peerTip) {
		if logSkip {
			wallSlot, _ := cs.staleTipRuleWallSlotLocked()
			cs.config.Logger.Debug(
				"skipping peer advertising a tip older than the wall clock",
				"connection_id", connId.String(),
				"advertised_tip_slot", peerTip.Tip.Point.Slot,
				"wall_clock_slot", wallSlot,
				"genesis_window_slots", cs.genesisWindowSlotsLocked(),
			)
		}
		return false
	}
	selectionTip := peerTip.SelectionTip()
	// The two behind-filters below compare delivered block numbers. A peer
	// registered from a rollback has not delivered a header yet, so its
	// delivered block number is 0 meaning "unknown", not "at block 0"; reading
	// it as a block number would skip the peer until its next MsgRollForward,
	// which is exactly the post-recycle chain-selection stall this exemption
	// exists to avoid. Such a peer still never outranks one with a real
	// delivered frontier: block 0 loses every Praos comparison, so it can only
	// be selected when nothing better is tracked.
	//
	// The advertised tip is still an upper bound on what the peer can
	// deliver, so a peer advertising a tip more than k blocks behind the local
	// tip can never extend the local chain and is rejected here. An inflated
	// advertisement passes this check, but it never becomes the selection
	// frontier: selection compares the delivered block number, and each
	// delivered header is bounded by checkPeerTipPlausibleLocked.
	if peerTip.awaitingFirstHeader {
		if cs.securityParam > 0 && cs.localTip.BlockNumber > 0 &&
			safeAddUint64(peerTip.Tip.BlockNumber, cs.securityParam) <
				cs.localTip.BlockNumber {
			if logSkip {
				cs.config.Logger.Debug(
					"skipping rollback-registered peer advertising a tip "+
						"far behind local tip",
					"connection_id", connId.String(),
					"advertised_block_number", peerTip.Tip.BlockNumber,
					"local_block_number", cs.localTip.BlockNumber,
					"security_param", cs.securityParam,
				)
			}
			return false
		}
	} else {
		if cs.securityParam > 0 && cs.localTip.BlockNumber > 0 &&
			safeAddUint64(selectionTip.BlockNumber, cs.securityParam) <
				cs.localTip.BlockNumber {
			if logSkip {
				cs.config.Logger.Debug(
					"skipping implausibly-behind peer",
					"connection_id", connId.String(),
					"peer_block_number", selectionTip.BlockNumber,
					"local_block_number", cs.localTip.BlockNumber,
					"security_param", cs.securityParam,
				)
			}
			return false
		}
		// Skip peers whose tip is far behind the best known peer tip.
		// During catch-up, switching to a behind peer causes pipeline
		// stalls and dropped rollbacks that cost minutes of sync time.
		// Use securityParam (K) as the threshold — peers within K blocks
		// of the best are acceptable (normal fork variance), but peers
		// further behind are not useful for syncing.
		//
		// The gap is measured on DELIVERED frontiers, which is a transport
		// property: it says how many headers each peer has served us so far,
		// not what chain each peer holds. Two peers that advertise the
		// identical canonical tip are TREATED AS the same chain unless
		// retained delivered history contradicts them (see
		// sameCanonicalChain), so a delivered-frontier gap between them is
		// read as delivery speed and must not make either one ineligible.
		// Excluding the trailing peer here also excluded the incumbent, which
		// skipped the anti-flap pin entirely (see
		// pinIncumbentDuringCatchUpLocked) and let the active connection flap
		// between two canonical public roots.
		//
		// A peer awaiting its first header is already exempt from this filter
		// (outer branch) and can never be the leading frontier this escape
		// compares against: its delivered block number is 0, and the escape
		// only considers leaders holding a bestBlock > 0 frontier.
		if cs.securityParam > 0 {
			bestBlock := uint64(0)
			if bestKnownBlock == nil {
				bestBlock = cs.bestKnownBlockNumber()
			} else {
				bestBlock = *bestKnownBlock
			}
			if bestBlock > 0 &&
				safeAddUint64(
					selectionTip.BlockNumber,
					cs.securityParam,
				) < bestBlock &&
				!cs.frontierLeadIsTransportOnlyLocked(peerTip, bestBlock) {
				if logSkip {
					cs.config.Logger.Debug(
						"skipping peer behind best known tip",
						"connection_id", connId.String(),
						"peer_block_number", selectionTip.BlockNumber,
						"best_known_block", bestBlock,
						"security_param", cs.securityParam,
					)
				}
				return false
			}
		}
	}
	// Genesis corroboration gate: a fast source must be corroborated by the
	// configured minimum number of independent peers before it can steer
	// selection. No-op outside Genesis mode / with corroboration disabled.
	if !cs.isPeerCorroboratedLocked(connId, peerTip) {
		if logSkip {
			cs.config.Logger.Debug(
				"skipping uncorroborated genesis fast source",
				"connection_id", connId.String(),
				"min_corroborating_peers", cs.config.MinCorroboratingPeers,
			)
		}
		return false
	}
	return true
}

func (cs *ChainSelector) selectBestChainLocked() *ouroboros.ConnectionId {
	// Advance the mode before caching the limit: the limit is gated on it.
	bestKnownBlock := cs.prepareSelectionLocked()
	defer cs.endEagernessLocked(cs.beginEagernessLocked())
	return cs.selectBestChainWithBestKnownBlockLocked(bestKnownBlock)
}

// prepareSelectionLocked advances the selection mode and returns the best known
// block frontier for one selection pass, or nil when securityParam is 0 and no
// frontier applies. In Genesis mode the frontier costs a corroboration scan, so
// it is computed once and shared by the mode check and every candidate; a mode
// change recomputes it under the new mode's trust rule.
func (cs *ChainSelector) prepareSelectionLocked() *uint64 {
	var bestKnownBlock *uint64
	if cs.mode == SelectionModeGenesis && cs.securityParam > 0 {
		bestBlock := cs.bestKnownBlockNumber()
		bestKnownBlock = &bestBlock
	}
	modeChanged := cs.advanceSelectionModeWithBestKnownBlockLocked(
		bestKnownBlock,
	)
	if modeChanged {
		bestKnownBlock = nil
	}
	if cs.securityParam > 0 && bestKnownBlock == nil {
		bestBlock := cs.bestKnownBlockNumber()
		bestKnownBlock = &bestBlock
	}
	return bestKnownBlock
}

func (cs *ChainSelector) selectBestChainWithBestKnownBlockLocked(
	bestKnownBlock *uint64,
) *ouroboros.ConnectionId {
	if len(cs.peerTips) == 0 {
		return nil
	}

	var bestConnId ouroboros.ConnectionId
	var bestPeerTip *PeerChainTip

	for connId, peerTip := range cs.peerTips {
		if !cs.isPeerSelectableWithBestKnownBlockLocked(
			connId,
			peerTip,
			true,
			bestKnownBlock,
		) {
			continue
		}

		if bestPeerTip == nil {
			bestConnId = connId
			bestPeerTip = peerTip
			continue
		}

		comparison := cs.comparePeerTips(
			connId,
			peerTip,
			bestConnId,
			bestPeerTip,
		)
		switch comparison {
		case ChainABetter:
			bestConnId = connId
			bestPeerTip = peerTip
		case ChainBBetter, ChainComparisonUnknown, ChainEqual:
			// Current best is better (or unknown); no change needed
		}
	}

	if bestPeerTip == nil {
		return nil
	}
	return &bestConnId
}

// bestKnownBlockNumber returns the highest block number delivered by any
// trusted peer (see frontierTrustedLocked). Used to skip peers that are far
// behind the observed frontier during catch-up. Advertised tips are untrusted
// and must not suppress peers that have delivered valid headers.
//
// In Genesis mode trust costs a corroboration scan, so frontiers are tried
// from the highest down and the first trusted one is the answer.
func (cs *ChainSelector) bestKnownBlockNumber() uint64 {
	if cs.mode != SelectionModeGenesis {
		var best uint64
		for connId, pt := range cs.peerTips {
			blockNumber := pt.SelectionTip().BlockNumber
			if blockNumber <= best || !cs.frontierTrustedLocked(connId, pt) {
				continue
			}
			best = blockNumber
		}
		return best
	}
	type frontier struct {
		connId      ouroboros.ConnectionId
		peerTip     *PeerChainTip
		blockNumber uint64
	}
	frontiers := make([]frontier, 0, len(cs.peerTips))
	for connId, pt := range cs.peerTips {
		if cs.advertisedTipStaleLocked(pt) {
			continue
		}
		if blockNumber := pt.SelectionTip().BlockNumber; blockNumber > 0 {
			frontiers = append(frontiers, frontier{connId, pt, blockNumber})
		}
	}
	slices.SortFunc(frontiers, func(a, b frontier) int {
		return cmp.Compare(b.blockNumber, a.blockNumber)
	})
	for _, f := range frontiers {
		if cs.frontierTrustedLocked(f.connId, f.peerTip) {
			return f.blockNumber
		}
	}
	return 0
}

// frontierLeadIsTransportOnlyLocked reports whether peerTip's shortfall against
// the leading delivered frontier can be attributed to delivery speed rather
// than chain quality: some eligible, non-stale peer holding the leading
// frontier classifies as same-chain with peerTip under sameCanonicalChain.
// That classification is an allowance, not proof — see its doc comment.
//
// Every leader is scanned rather than one representative, so the answer does
// not depend on map iteration order when several peers share the leading
// frontier, and one leader on a chain of its own cannot suppress the peers that
// agree with a different leader.
func (cs *ChainSelector) frontierLeadIsTransportOnlyLocked(
	peerTip *PeerChainTip,
	bestBlock uint64,
) bool {
	for connId, pt := range cs.peerTips {
		if pt == peerTip ||
			pt.SelectionTip().BlockNumber != bestBlock {
			continue
		}
		if !cs.isConnectionEligible(connId) || cs.isPeerTipStale(pt) {
			continue
		}
		if sameCanonicalChain(peerTip, pt) {
			return true
		}
	}
	return false
}

// sameAdvertisedTip reports whether two peers name the identical block as their
// chain tip. The hash must be present: an all-zero advertised tip means the
// peer has not told us where its chain ends, which is not agreement.
func sameAdvertisedTip(a, b ochainsync.Tip) bool {
	return len(a.Point.Hash) > 0 && sameSelectionTip(a, b)
}

// sameCanonicalChain classifies two peers as the same canonical chain, so that
// a difference in their DELIVERED frontiers may be attributed to transport
// delivery rather than chain quality.
//
// This is an ALLOWANCE, not proof of chain identity. It is granted when the
// peers name the identical advertised tip and neither one's retained delivered
// history contradicts the other, and withdrawn as soon as a contradiction is
// visible. It is never positive confirmation that the two peers hold the same
// chain: observedHistoryConflictsAt is one-sided by construction, so when the
// other peer's frontier slot falls outside the retained k+1 history window
// there is nothing to check and the allowance stands on the advertisement
// alone.
//
// Agreement on the advertised tip is the primary signal, and it is exactly the
// canonical-public-root case that flapped: both roots named block 4625199 at
// slot 121697834 while their delivered frontiers differed by 742 blocks.
//
// The advertised tip is untrusted on its own, so it is checked against the
// delivered evidence that is available: if either peer's retained delivered
// history holds a different block at the other's frontier slot, they
// demonstrably served conflicting chains and the copied advertisement buys
// nothing. The predicate never grants the allowance over a contradiction it can
// see, and never invents one it cannot.
//
// The allowance is deliberately narrow, which is what bounds a peer that copies
// an advertisement it cannot be contradicted on. It only keeps a peer in the
// candidate pool and only holds the incumbent's pin; it never makes a peer win
// selection, and it cannot hold a pin across a catchUpPinStallTimeout window in
// which the incumbent drove no local tip progress, because the progress-stall
// escape is evaluated before the longer-chain escape. The Praos comparison
// still ranks candidates by their delivered frontiers, the
// implausible-frontier bound in updatePeerTipObservedPraosView still rejects
// delivered jumps beyond k, the k-behind-the-applied-local-tip check above
// still drops peers that cannot serve the block the node needs, and Genesis
// corroboration still gates a fast source on independent witnesses.
func sameCanonicalChain(a, b *PeerChainTip) bool {
	if a == nil || b == nil {
		return false
	}
	if !sameAdvertisedTip(a.Tip, b.Tip) {
		return false
	}
	if a.observedHistoryConflictsAt(b.SelectionTip().Point) ||
		b.observedHistoryConflictsAt(a.SelectionTip().Point) {
		return false
	}
	return true
}

func (cs *ChainSelector) isConnectionEligible(
	connId ouroboros.ConnectionId,
) bool {
	eligible, ok := cs.eligible[connId]
	if !ok {
		return true
	}
	return eligible
}

func (cs *ChainSelector) connectionPriority(
	connId ouroboros.ConnectionId,
) int {
	return cs.priority[connId]
}

func (cs *ChainSelector) isPeerTipStale(peerTip *PeerChainTip) bool {
	// A peer held at the Limit on Eagerness sends no tips because the pause
	// stops them, so the silence is not evidence that it is gone.
	return peerTip != nil && peerTip.eagernessPaused == 0 &&
		peerTip.IsStale(cs.config.StaleTipThreshold)
}

func (cs *ChainSelector) comparePeerTips(
	connIdA ouroboros.ConnectionId,
	peerTipA *PeerChainTip,
	connIdB ouroboros.ConnectionId,
	peerTipB *PeerChainTip,
) ChainComparisonResult {
	if peerTipA == nil || peerTipB == nil {
		return ChainComparisonUnknown
	}
	if cs.mode == SelectionModeGenesis {
		genesisWindow := cs.genesisWindowSlotsLocked()
		densityA := peerTipA.observedDensity(genesisWindow)
		densityB := peerTipB.observedDensity(genesisWindow)
		if densityA > densityB {
			return ChainABetter
		}
		if densityB > densityA {
			return ChainBBetter
		}
	}
	// The limit replaces only the length comparison: density is what tells
	// apart two forks that both run past it, so it must be decided first.
	if cs.indistinguishableUnderEagernessLocked(
		peerTipA.SelectionTip(),
		peerTipB.SelectionTip(),
	) {
		return cs.compareTransportLocked(connIdA, connIdB, true)
	}
	return cs.comparePeerTipsPraos(
		connIdA,
		peerTipA,
		connIdB,
		peerTipB,
	)
}

func (cs *ChainSelector) comparePeerTipsPraos(
	connIdA ouroboros.ConnectionId,
	peerTipA *PeerChainTip,
	connIdB ouroboros.ConnectionId,
	peerTipB *PeerChainTip,
) ChainComparisonResult {
	tipA := peerTipA.SelectionTip()
	tipB := peerTipB.SelectionTip()
	comparison := cs.config.Consensus.CompareTips(
		tipA,
		tipB,
		peerTipA.PraosView,
		peerTipB.PraosView,
	)
	switch comparison {
	case ChainABetter, ChainBBetter, ChainComparisonUnknown:
		return comparison
	case ChainEqual:
		if !sameSelectionTip(tipA, tipB) {
			return ChainEqual
		}
		// The chains are the same block. The remaining checks choose a
		// peer transport for that block, not a different chain.
		return cs.compareTransportLocked(connIdA, connIdB, false)
	default:
		return ChainComparisonUnknown
	}
}

// compareTransportLocked orders two peers whose chains are equivalent for
// selection: connection priority, then (optionally) the incumbent, then
// blockfetch latency, then connection ID. The incumbent is preferred only when
// the equivalence is the Limit on Eagerness, where the peers' tips differ and
// would otherwise swap places on every evaluation.
func (cs *ChainSelector) compareTransportLocked(
	connIdA, connIdB ouroboros.ConnectionId,
	preferIncumbent bool,
) ChainComparisonResult {
	priorityA := cs.connectionPriority(connIdA)
	priorityB := cs.connectionPriority(connIdB)
	if priorityA > priorityB {
		return ChainABetter
	}
	if priorityB > priorityA {
		return ChainBBetter
	}
	if preferIncumbent && cs.bestPeerConn != nil {
		if *cs.bestPeerConn == connIdA {
			return ChainABetter
		}
		if *cs.bestPeerConn == connIdB {
			return ChainBBetter
		}
	}
	// Latency tiebreaker: prefer the peer with lower blockfetch
	// EWMA when VRF and SelectionTip are equal. Only fires when
	// both peers have at least one sample; otherwise fall through
	// to the connId string tiebreaker.
	if cs.config.BlockfetchLatency != nil {
		latencyA, okA := cs.config.BlockfetchLatency(connIdA)
		latencyB, okB := cs.config.BlockfetchLatency(connIdB)
		if okA && okB {
			if latencyA < latencyB {
				return ChainABetter
			}
			if latencyB < latencyA {
				return ChainBBetter
			}
		}
	}
	if connIdA.String() < connIdB.String() {
		return ChainABetter
	}
	if connIdB.String() < connIdA.String() {
		return ChainBBetter
	}
	return ChainEqual
}

func sameSelectionTip(a, b ochainsync.Tip) bool {
	return a.BlockNumber == b.BlockNumber &&
		a.Point.Slot == b.Point.Slot &&
		bytes.Equal(a.Point.Hash, b.Point.Hash)
}

// SetConnectionEligible marks whether a peer connection is eligible for chain
// selection. Ineligible peers are skipped during best-peer evaluation.
// Calling this triggers a re-evaluation of the best peer.
//
// eligible=true is the default (absent key), so we delete instead of storing
// it. This prevents a stale entry when an out-of-order eligible=true event
// arrives after RemovePeer has already cleaned up the maps.
func (cs *ChainSelector) SetConnectionEligible(
	connId ouroboros.ConnectionId,
	eligible bool,
) {
	cs.mutex.Lock()
	if eligible {
		delete(cs.eligible, connId)
	} else {
		cs.eligible[connId] = false
	}
	cs.mutex.Unlock()
	cs.triggerEvaluation()
}

// SetConnectionPriority sets the selection priority for a peer connection.
// When two peers advertise the same chain tip, the peer with the higher
// priority wins. Calling this triggers a re-evaluation of the best peer.
//
// priority=0 is the default (absent key), so we delete instead of storing it.
// This prevents a stale entry when an out-of-order priority=0 event arrives
// after RemovePeer has already cleaned up the maps.
func (cs *ChainSelector) SetConnectionPriority(
	connId ouroboros.ConnectionId,
	priority int,
) {
	cs.mutex.Lock()
	if priority == 0 {
		delete(cs.priority, connId)
	} else {
		cs.priority[connId] = priority
	}
	cs.mutex.Unlock()
	cs.triggerEvaluation()
}

func (cs *ChainSelector) triggerEvaluation() {
	select {
	case cs.evaluationTrigger <- struct{}{}:
	default:
	}
}

// publishSelection hands a chain-selection event to the EventBus without
// parking the calling goroutine on a subscriber that has stopped draining.
//
// Every chainselection event is produced on a goroutine that has to keep
// making progress: the EventBus dispatch goroutines for the internal
// peer_activity, peer_tip_update and conn_closed subscriptions, and the
// selector's own evaluation loop. EventBus.Publish hands the event to each
// subscriber channel inline and waits for buffer capacity, so a subscriber
// that stops draining stops its producer too.
//
// A Preview run showed that chain end to end: a
// chainselection.chain_switch consumer stopped draining, TouchPeerActivity
// parked inside publishSelectionEvents -> EventBus.Publish, the internal
// chainselection.peer_activity subscriber therefore stopped draining, and
// once its 1024-slot buffer filled every keepalive response parked a
// gouroboros protocol goroutine in ouroboros.keepaliveClientResponse.
//
// PublishOrdered keeps both delivery and publisher order: each event type has
// its own FIFO lane drained by exactly one worker, so events of a type reach
// subscribers in the order they were handed to PublishOrdered, and a
// subscriber that blocks parks only that lane's worker. Every chainselection
// publication goes through this helper precisely so a queued chain switch can
// never be overtaken by one published directly.
//
// The order preserved is the order publications arrive here, not the order the
// switches were decided in. Every producer decides under cs.mutex and
// publishes after releasing it -- EvaluateAndSwitch and TouchPeerActivity both
// do -- so two goroutines can decide A then B and still enqueue B then A, and
// a subscriber sees B before A. That window is unchanged from the inline
// Publish this replaced, and closing it would mean publishing while holding
// cs.mutex, which is the deadlock shape this avoids.
func (cs *ChainSelector) publishSelection(
	eventType event.EventType,
	evt event.Event,
) {
	if cs.config.EventBus == nil {
		return
	}
	cs.config.EventBus.PublishOrdered(eventType, evt)
}

func (cs *ChainSelector) publishSelectionEvents(
	switchEvent *event.Event,
	selectionEvent *event.Event,
	corroborationEvent *event.Event,
) {
	if cs.config.EventBus == nil {
		return
	}
	if switchEvent != nil {
		cs.publishSelection(ChainSwitchEventType, *switchEvent)
	}
	if selectionEvent != nil {
		cs.publishSelection(ChainSelectionEventType, *selectionEvent)
	}
	if corroborationEvent != nil {
		cs.publishSelection(
			GenesisCorroborationFailedEventType,
			*corroborationEvent,
		)
	}
	cs.publishPendingGenesisExitEvent()
	cs.publishPendingSelectedNoneEvent()
}

// publishPendingGenesisExitEvent drains and publishes a staged
// GenesisModeExitedEvent outside the selector mutex.
func (cs *ChainSelector) publishPendingGenesisExitEvent() {
	if cs.config.EventBus == nil {
		return
	}
	cs.mutex.Lock()
	pending := cs.pendingGenesisExit
	cs.pendingGenesisExit = nil
	cs.mutex.Unlock()
	if pending != nil {
		cs.publishSelection(
			GenesisModeExitedEventType,
			event.NewEvent(GenesisModeExitedEventType, *pending),
		)
	}
}

// stageSelectedNoneLocked stages a ChainSelectedNoneEvent for a best-peer →
// none transition (a peer was previously selected, now none is). Must be called
// with cs.mutex held; the event is published later by
// publishPendingSelectedNoneEvent outside the lock.
func (cs *ChainSelector) stageSelectedNoneLocked(
	previousBest ouroboros.ConnectionId,
) {
	if cs.config.EventBus == nil {
		return
	}
	cs.pendingSelectedNone = &ChainSelectedNoneEvent{
		PreviousConnectionId: previousBest,
		GenesisCorroboration: cs.genesisCorroborationActiveLocked(),
	}
}

// publishPendingSelectedNoneEvent drains and publishes a staged
// ChainSelectedNoneEvent outside the selector mutex.
func (cs *ChainSelector) publishPendingSelectedNoneEvent() {
	if cs.config.EventBus == nil {
		return
	}
	cs.mutex.Lock()
	pending := cs.pendingSelectedNone
	cs.pendingSelectedNone = nil
	cs.mutex.Unlock()
	if pending != nil {
		cs.publishSelection(
			ChainSelectedNoneEventType,
			event.NewEvent(ChainSelectedNoneEventType, *pending),
		)
	}
}

// appliedLocalTipBlockLocked returns the block number of the last applied
// local tip. Zero means SetLocalTip has never been called (near-genesis /
// no-local-tip), in which case the anti-flap pin is inactive.
func (cs *ChainSelector) appliedLocalTipBlockLocked() uint64 {
	return cs.localTip.BlockNumber
}

// catchingUpLocked reports whether the node is in deep catch-up: the best
// known peer tip is more than catchUpPinBlockThreshold blocks ahead of the
// applied local tip. The pin engages in both regimes; this is used only for
// diagnostics/logging to distinguish catch-up from tip-hold.
//
// bestKnownBlock is the frontier already computed for this selection pass;
// nil computes it.
func (cs *ChainSelector) catchingUpLocked(bestKnownBlock *uint64) bool {
	local := cs.appliedLocalTipBlockLocked()
	if local == 0 {
		return false
	}
	var best uint64
	if bestKnownBlock != nil {
		best = *bestKnownBlock
	} else {
		best = cs.bestKnownBlockNumber()
	}
	if best <= local {
		return false
	}
	return best-local > catchUpPinBlockThreshold
}

// localTipStalledLocked reports whether the applied local tip has stopped
// advancing for at least catchUpPinStallTimeout. This is the progress-aware
// escape: when true, the pin releases so the node can switch away from a
// dead/stalled incumbent. Returns false until forward progress has been
// recorded at least once (localTipProgressAt is zero), so a freshly started
// node never reports a stall before it has had a chance to apply a block.
func (cs *ChainSelector) localTipStalledLocked() bool {
	if cs.localTipProgressAt.IsZero() {
		return false
	}
	return cs.now().Sub(cs.localTipProgressAt) >= catchUpPinStallTimeout
}

// pinIncumbentDuringCatchUpLocked decides whether to KEEP the incumbent active
// connection (previousBest) instead of switching to challenger (newBest).
//
// This is the unified anti-flap incumbent pin. It applies whenever there is an
// established, still-selectable incumbent and SetLocalTip has been called
// (applied local tip > 0). It generalizes the "don't flap on a 1-block head
// micro-fork" behavior to both regimes: deep catch-up and at/near the live
// tip. The reference-implementation Praos comparison (longer chain, then lower
// slot, then VRF/opcert) still governs which chain is canonically best; this
// pin only suppresses the active-CONNECTION handoff between peers that are on
// the same height / sibling head-forks, which would otherwise reset the
// chainsync+blockfetch pipeline on nearly every tip update.
//
// The pin RELEASES (returns false, allow the switch) when:
//   - SetLocalTip has never been called (applied local tip == 0) — keeps
//     near-genesis behavior and existing tests unchanged.
//   - the incumbent is no longer selectable (gone/disconnected/ineligible/
//     stale/implausible).
//   - the incumbent was registered from a rollback and has not delivered a
//     header yet (awaitingFirstHeader) — it has no head to micro-fork from, so
//     a real delivered frontier always replaces it.
//   - the applied local tip has stalled past catchUpPinStallTimeout
//     (progress-aware escape — cannot pin to a dead peer forever).
//   - the challenger is genuinely ahead of the incumbent by more than
//     catchUpPinHeadMargin blocks (a real longer chain, not a head micro-fork).
//
// The last two releases are DISCRETIONARY and are rate-limited by
// SwitchBackCooldown, both per abandoned connection and globally across every
// discretionary hand-off; the releases above them are mandatory and ungated.
//
// Otherwise it PINS (returns true, keep the incumbent).
//
// Must be called with cs.mutex held.
func (cs *ChainSelector) pinIncumbentDuringCatchUpLocked(
	previousBest ouroboros.ConnectionId,
	incumbentTip *PeerChainTip,
	challengerConn ouroboros.ConnectionId,
	challengerTip *PeerChainTip,
	bestKnownBlock *uint64,
) bool {
	// Inactive near genesis / before any local tip has been applied.
	if cs.appliedLocalTipBlockLocked() == 0 {
		return false
	}
	if incumbentTip == nil || challengerTip == nil {
		return false
	}
	// Release if the incumbent was registered from a rollback and has not
	// delivered a header yet. Such a peer has no head to micro-fork from, so
	// there is nothing for the anti-flap pin to protect: it is selectable only
	// because nothing better was tracked, and a peer with a real delivered
	// frontier must always be able to take the pipeline from it. The
	// longer-chain escape below cannot do that on its own, because the
	// incumbent's selection block number is 0 ("nothing delivered yet"), so
	// every challenger within catchUpPinHeadMargin of genesis still pins.
	if incumbentTip.awaitingFirstHeader {
		return false
	}
	// Release if the incumbent is no longer a viable selection target.
	// Release if the incumbent is no longer a viable selection target. This is
	// a MANDATORY release, never subject to the switch-back debounce below:
	// there is no "keeping" a connection that is gone, ineligible, stale, or
	// implausible, whoever the challenger is.
	if !cs.isPeerSelectableWithBestKnownBlockLocked(
		previousBest,
		incumbentTip,
		false,
		bestKnownBlock,
	) {
		return false
	}

	// Every release condition below is DISCRETIONARY: the incumbent is still
	// alive and selectable, and the escape is a per-evaluation judgment call
	// that the challenger looks better right now. Both existing escapes have
	// no memory of very recent switches, so both are gated by the same
	// switch-back debounce: refuse to hand the active connection back to a
	// peer abandoned less than switchBackCooldown ago (switchBackDebouncedLocked,
	// per-connection), AND refuse ANY discretionary hand-off at all less than
	// switchBackCooldown after the previous one, to whichever connection
	// (switchBackRateLimitedLocked, global). The per-connection check alone
	// stops two peers ping-ponging, but three or more peers whose delivered
	// frontiers take turns marginally leading each other defeat it: by the
	// time evaluation cycles back around to a given connection, enough real
	// time has usually passed that IT is never "recently abandoned", even
	// though the active connection is still being handed off roughly once per
	// evaluation with zero net forward progress. Confirmed live: three peer
	// connections thrashing every ~2s indefinitely, applied block height
	// frozen throughout. The global rate limit closes that gap: at most one
	// discretionary hand-off per cooldown window, no matter how many distinct
	// peers are rotating through it. A genuinely new challenger is delayed by
	// at most one cooldown window rather than exempted outright -- consistent
	// with this being a rate limit, not a correctness rule, and required
	// because "never seen as active before" is exactly the property a small
	// rotating set of real peers can each satisfy in turn forever.
	//
	// This matters most for the progress-stall escape just below: once the
	// applied local tip stalls, localTipStalledLocked stays true on EVERY
	// subsequent evaluation until progress resumes, so an unguarded stall
	// escape provides no pinning at all for as long as the stall lasts --
	// every evaluation falls through to bare Praos ranking with zero
	// hysteresis. If the switching itself is what is preventing a batch from
	// ever completing (observed live: rapid reselection racing
	// ledger.handleChainSwitchEvent's connection handoff), the stall
	// condition never clears on its own, and the resulting thrash is
	// indistinguishable from the longer-chain-escape storm this pin was
	// already hardened against -- just reached through the other escape.
	if cs.localTipStalledLocked() || cs.longerChainEscapeLocked(
		incumbentTip,
		challengerTip,
	) {
		if cs.switchBackDebouncedLocked(challengerConn) {
			cs.config.Logger.Debug(
				"debouncing switch back to recently abandoned connection",
				"incumbent", previousBest.String(),
				"challenger", challengerConn.String(),
				"switch_back_cooldown", cs.switchBackCooldown,
			)
			return true
		}
		if cs.switchBackRateLimitedLocked() {
			cs.config.Logger.Debug(
				"debouncing discretionary switch: global rate limit",
				"incumbent", previousBest.String(),
				"challenger", challengerConn.String(),
				"switch_back_cooldown", cs.switchBackCooldown,
			)
			return true
		}
		cs.lastDiscretionarySwitchAt = cs.now()
		return false
	}
	// Otherwise this is a head micro-fork / same-height sibling: pin the
	// incumbent and do not hand off the pipeline.
	return true
}

// longerChainEscapeLocked reports whether challengerTip is genuinely taller
// than incumbentTip by more than catchUpPinHeadMargin -- a real longer chain,
// not a sibling head-fork.
//
// "Taller" is measured on delivered frontiers, so it only means a longer
// chain when the two peers disagree about where the chain ends. When they
// advertise the identical canonical tip they are treated as the same chain
// unless retained delivered history contradicts them (sameCanonicalChain is
// an allowance, not proof), and the challenger is then read as merely further
// along in serving that chain to us; handing the pipeline over buys no chain
// and costs a chainsync/blockfetch reset plus, via the ledger's fresh-cursor
// path, a close of the connection we just selected.
func (cs *ChainSelector) longerChainEscapeLocked(
	incumbentTip *PeerChainTip,
	challengerTip *PeerChainTip,
) bool {
	incumbentBlock := incumbentTip.SelectionTip().BlockNumber
	challengerBlock := challengerTip.SelectionTip().BlockNumber
	return challengerBlock > safeAddUint64(incumbentBlock, catchUpPinHeadMargin) &&
		!sameCanonicalChain(incumbentTip, challengerTip)
}

// switchBackDebouncedLocked reports whether connId was the active connection
// until less than switchBackCooldown ago. Must be called with cs.mutex held.
func (cs *ChainSelector) switchBackDebouncedLocked(
	connId ouroboros.ConnectionId,
) bool {
	if cs.switchBackCooldown <= 0 {
		return false
	}
	leftAt, ok := cs.recentlyLeft[connId]
	if !ok {
		return false
	}
	return cs.now().Sub(leftAt) < cs.switchBackCooldown
}

// switchBackRateLimitedLocked reports whether a discretionary release
// (longer-chain or progress-stall escape) happened less than
// switchBackCooldown ago, regardless of which connection was involved. This
// is the global counterpart to switchBackDebouncedLocked: it bounds the
// discretionary hand-off rate to at most once per cooldown window even when
// three or more distinct peers rotate through the incumbent role, each one
// individually clearing the per-connection debounce by the time evaluation
// returns to it. Must be called with cs.mutex held.
func (cs *ChainSelector) switchBackRateLimitedLocked() bool {
	if cs.switchBackCooldown <= 0 {
		return false
	}
	if cs.lastDiscretionarySwitchAt.IsZero() {
		return false
	}
	return cs.now().Sub(cs.lastDiscretionarySwitchAt) < cs.switchBackCooldown
}

// recordSwitchAwayLocked notes that the active connection just moved away
// from left, arming the switch-back debounce for it, and clears any stale
// entry for the newly adopted connection (it is active now, not abandoned).
// Must be called with cs.mutex held.
func (cs *ChainSelector) recordSwitchAwayLocked(
	left ouroboros.ConnectionId,
	adopted ouroboros.ConnectionId,
) {
	if cs.recentlyLeft == nil {
		cs.recentlyLeft = make(map[ouroboros.ConnectionId]time.Time)
	}
	cs.recentlyLeft[left] = cs.now()
	delete(cs.recentlyLeft, adopted)
}

func (cs *ChainSelector) evaluateBestPeerLocked() (
	bool,
	*event.Event,
	*event.Event,
	*event.Event,
) {
	var switchEvent *event.Event
	var selectionEvent *event.Event
	switchOccurred := false
	// One frontier serves the whole pass: the mode check, every candidate,
	// the incumbent re-check and the anti-flap pin.
	bestKnownBlock := cs.prepareSelectionLocked()
	// Compute Genesis corroboration status once per evaluation, independent of
	// which peer (if any) is ultimately selected, so a stalled selection still
	// reports why the densest fast source was denied.
	corroborationEvent := cs.genesisCorroborationFailureLocked()

	defer cs.endEagernessLocked(cs.beginEagernessLocked())
	newBest := cs.selectBestChainWithBestKnownBlockLocked(bestKnownBlock)
	if newBest == nil {
		// Selection stalled. Stage an explicit selected-to-none transition when
		// we were previously following a peer, so subscribers can observe the
		// stall (ChainSwitchEvent cannot express "none"). Enforcement that the
		// stalled source stops feeding the ledger is handled separately by
		// ShouldApplyIngress.
		if previousBest := cs.bestPeerConn; previousBest != nil {
			cs.stageSelectedNoneLocked(*previousBest)
		}
		// Clear stale reference to avoid returning a disconnected peer
		cs.bestPeerConn = nil
		return false, nil, nil, corroborationEvent
	}

	previousBest := cs.bestPeerConn
	if previousBest != nil && *previousBest != *newBest {
		previousPeerTip, ok := cs.peerTips[*previousBest]
		if ok &&
			cs.isPeerSelectableWithBestKnownBlockLocked(
				*previousBest,
				previousPeerTip,
				false,
				bestKnownBlock,
			) {
			newPeerTip, ok := cs.peerTips[*newBest]
			if !ok {
				return false, nil, nil, corroborationEvent
			}
			if cs.config.Consensus.CompareTips(
				newPeerTip.SelectionTip(),
				previousPeerTip.SelectionTip(),
				newPeerTip.PraosView,
				previousPeerTip.PraosView,
			) == ChainEqual {
				// When two peers have delivered the same observed frontier,
				// or the reference implementation's equal-length Praos
				// tiebreaker is not armed, keep following the incumbent.
				newBest = previousBest
			} else if cs.comparePeerTips(
				*previousBest,
				previousPeerTip,
				*newBest,
				newPeerTip,
			) == ChainABetter {
				// Preserve the incumbent only when it still wins the same
				// full comparison used during normal best-peer selection.
				newBest = previousBest
			} else if cs.pinIncumbentDuringCatchUpLocked(
				*previousBest,
				previousPeerTip,
				*newBest,
				newPeerTip,
				bestKnownBlock,
			) {
				// Anti-flap incumbent pin: the challenger is canonically
				// "better" by the Praos rules, but only via a head micro-fork
				// / same-height sibling (within catchUpPinHeadMargin). Keep the
				// active connection on the incumbent rather than handing off
				// the chainsync+blockfetch pipeline on a 1-block head fork.
				// The longer-chain escape and the progress-stall escape inside
				// the pin guarantee convergence to a genuinely longer chain and
				// prevent pinning to a dead/stalled peer.
				cs.config.Logger.Debug(
					"pinning incumbent active connection (anti-flap)",
					"incumbent", previousBest.String(),
					"challenger", newBest.String(),
					"incumbent_block",
					previousPeerTip.SelectionTip().BlockNumber,
					"challenger_block",
					newPeerTip.SelectionTip().BlockNumber,
					"applied_local_block", cs.appliedLocalTipBlockLocked(),
					"catching_up",
					cs.catchingUpLocked(bestKnownBlock),
				)
				newBest = previousBest
			}
		}
	}

	if previousBest == nil || *previousBest != *newBest {
		newPeerTip, ok := cs.peerTips[*newBest]
		if !ok {
			return false, nil, nil, corroborationEvent
		}
		newTip := newPeerTip.Tip
		newObservedTip := newPeerTip.SelectionTip()
		if previousBest != nil {
			cs.recordSwitchAwayLocked(*previousBest, *newBest)
		}
		cs.bestPeerConn = newBest
		switchOccurred = true

		cs.config.Logger.Info(
			"selected new best peer",
			"connection_id", newBest.String(),
			"block_number", newTip.BlockNumber,
			"slot", newTip.Point.Slot,
			"observed_block_number", newObservedTip.BlockNumber,
			"observed_slot", newObservedTip.Point.Slot,
		)

		if cs.config.EventBus != nil {
			var previousTip ochainsync.Tip
			var previousObservedTip ochainsync.Tip
			var previousConnId ouroboros.ConnectionId
			var rollbackPoint *ocommon.Point
			if previousBest != nil {
				previousConnId = *previousBest
				if pt, ok := cs.peerTips[*previousBest]; ok {
					previousTip = pt.Tip
					previousObservedTip = pt.SelectionTip()
					rollbackPoint = fragmentIntersection(pt, newPeerTip)
				}
			}
			// Compute comparison result and block difference
			comparisonResult := ChainComparisonUnknown
			if previousBest != nil {
				if previousPeerTip, ok := cs.peerTips[*previousBest]; ok {
					comparisonResult = cs.comparePeerTips(
						*newBest,
						newPeerTip,
						*previousBest,
						previousPeerTip,
					)
				}
			}
			blockDiff := safeBlockDiff(
				newTip.BlockNumber,
				previousTip.BlockNumber,
			)
			evt := event.NewEvent(
				ChainSwitchEventType,
				ChainSwitchEvent{
					PreviousConnectionId: previousConnId,
					NewConnectionId:      *newBest,
					NewTip:               newTip,
					PreviousTip:          previousTip,
					NewObservedTip:       newObservedTip,
					NewObservedTipSet:    true,
					PreviousObservedTip:  previousObservedTip,
					ComparisonResult:     comparisonResult,
					BlockDifference:      blockDiff,
					RollbackPoint:        rollbackPoint,
				},
			)
			switchEvent = &evt
		}
	}

	if cs.config.EventBus != nil && cs.bestPeerConn != nil {
		bestPeerTip, ok := cs.peerTips[*cs.bestPeerConn]
		if !ok {
			return false, nil, nil, corroborationEvent
		}
		bestTip := bestPeerTip.Tip
		evt := event.NewEvent(
			ChainSelectionEventType,
			ChainSelectionEvent{
				BestConnectionId: *cs.bestPeerConn,
				BestTip:          bestTip,
				PeerCount:        len(cs.peerTips),
				SwitchOccurred:   switchOccurred,
			},
		)
		selectionEvent = &evt
	}

	return switchOccurred, switchEvent, selectionEvent, corroborationEvent
}

// EvaluateAndSwitch evaluates all peer tips and switches to the best chain if
// it differs from the current best. Returns true if a switch occurred.
func (cs *ChainSelector) EvaluateAndSwitch() bool {
	return cs.evaluateAndSwitch(false)
}

func (cs *ChainSelector) evaluateAndSwitch(generationPinned bool) bool {
	var switchEvent *event.Event
	var selectionEvent *event.Event
	var corroborationEvent *event.Event
	var sparse []GenesisDensityDisconnect
	switchOccurred := false

	func() {
		releaseGeneration := func() {}
		if !generationPinned {
			var generationCurrent bool
			releaseGeneration, generationCurrent = cs.
				pinCurrentHeaderCryptoGeneration()
			if !generationCurrent {
				return
			}
		}
		defer releaseGeneration()
		cs.mutex.Lock()
		defer cs.mutex.Unlock()
		switchOccurred, switchEvent, selectionEvent, corroborationEvent = cs.evaluateBestPeerLocked()
		sparse = cs.genesisDensityDisconnectsLocked()
	}()

	cs.publishSelectionEvents(switchEvent, selectionEvent, corroborationEvent)
	for _, d := range sparse {
		cs.config.OnGenesisDensityDisconnect(d)
	}
	return switchOccurred
}

// HandlePeerTipUpdateEvent handles PeerTipUpdateEvent from the event bus.
func (cs *ChainSelector) HandlePeerTipUpdateEvent(evt event.Event) {
	e, ok := evt.Data.(PeerTipUpdateEvent)
	if !ok {
		cs.config.Logger.Warn(
			"received unexpected event data type",
			"expected", "PeerTipUpdateEvent",
		)
		return
	}
	if e.AdmissionID == 0 {
		cs.updatePeerTipObservedLinked(
			e.ConnectionId,
			e.Tip,
			e.ObservedTip,
			e.VRFOutput,
			e.PraosView,
			deliveredLink{
				prevHash: e.ObservedPrevHash,
				boundary: e.ObservedBoundary,
			},
		)
		return
	}
	if e.Rejected {
		cs.RejectPeerTipAdmission(e)
		return
	}
	if e.Admitted {
		cs.CommitPeerTipAdmission(e)
		return
	}
	cs.PreparePeerTipAdmission(e)
}

// AcceptPeerTipUpdate synchronously applies a peer tip update and reports
// whether the selector accepted the candidate.
func (cs *ChainSelector) AcceptPeerTipUpdate(e PeerTipUpdateEvent) bool {
	return cs.ObservePeerTipUpdate(e) == PeerTipUpdateAccepted
}

// ObservePeerTipUpdate synchronously applies a peer tip update and reports
// whether it was accepted, retained as a pending far-tip claim, or rejected.
func (cs *ChainSelector) ObservePeerTipUpdate(
	e PeerTipUpdateEvent,
) PeerTipUpdateDisposition {
	return cs.updatePeerTipObservedLinkedDisposition(
		e.ConnectionId,
		e.Tip,
		e.ObservedTip,
		e.VRFOutput,
		e.PraosView,
		deliveredLink{
			prevHash: e.ObservedPrevHash,
			boundary: e.ObservedBoundary,
		},
		false,
	)
}

// HandlePeerActivityEvent refreshes a peer's liveness on non-tip protocol
// activity such as keepalive responses.
func (cs *ChainSelector) HandlePeerActivityEvent(evt event.Event) {
	e, ok := evt.Data.(PeerActivityEvent)
	if !ok {
		cs.config.Logger.Warn(
			"received unexpected event data type",
			"expected", "PeerActivityEvent",
		)
		return
	}
	cs.TouchPeerActivity(e.ConnectionId)
}

// HandlePeerRollbackEvent trims Genesis observed history and refreshes the
// tracked peer tip after a rollback. When the connection has no tracked entry
// it registers one from the rollback (see registerPeerFromRollbackLocked),
// because a rollback for an untracked connection is the normal post-
// FindIntersect MsgRollBackward of a fresh or recycled connection and dropping
// it leaves the peer unselectable until its next MsgRollForward.
func (cs *ChainSelector) HandlePeerRollbackEvent(evt event.Event) {
	e, ok := evt.Data.(PeerRollbackEvent)
	if !ok {
		cs.config.Logger.Warn(
			"received unexpected event data type",
			"expected", "PeerRollbackEvent",
		)
		return
	}

	// Fast path: a tracked peer needs no liveness decision, so the rollback is
	// applied in place under a single lock acquisition.
	if cs.applyRollbackToTrackedPeer(e) {
		cs.EvaluateAndSwitch()
		return
	}

	// Unknown connection. Evaluate liveness BEFORE taking cs.mutex:
	// ConnectionLive is supplied by the composition layer and reaches into the
	// connection manager, so calling it under the selector lock lets connection
	// teardown ordering block -- or re-enter -- the chain-selection event path.
	// updatePeerTipObservedPraosView checks liveness outside the lock for the
	// same reason. A rollback can race the ConnectionClosedEvent that removed
	// the peer, and re-registering a dead connection would resurrect an entry
	// nothing will clean up except the stale-peer sweep.
	if cs.config.ConnectionLive != nil &&
		!cs.config.ConnectionLive(e.ConnectionId) {
		cs.config.Logger.Debug(
			"ignoring rollback from closed connection",
			"connection_id", e.ConnectionId.String(),
			"slot", e.Point.Slot,
		)
		if cs.config.OnRollbackRegistration != nil {
			cs.config.OnRollbackRegistration(
				RollbackRegistrationClosedConnection,
			)
		}
		return
	}

	var shouldEvaluate bool
	var evictedConn *ouroboros.ConnectionId
	var outcome RollbackRegistrationOutcome
	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()
		delete(cs.pendingPeerTips, e.ConnectionId)
		delete(cs.pendingPeerTipFrontiers, e.ConnectionId)
		if deliveredTip := cs.deliveredPeerTips[e.ConnectionId]; deliveredTip != nil {
			deliveredTip.ApplyRollback(e.Point, e.Tip)
		}
		cs.rebuildPendingAdmissionStateLocked()
		// Re-check under the lock: a concurrent roll forward (or another
		// rollback) may have created the entry while liveness was evaluated.
		if peerTip, exists := cs.peerTips[e.ConnectionId]; exists {
			peerTip.ApplyRollback(e.Point, e.Tip)
			cs.rebuildPendingAdmissionStateLocked()
			shouldEvaluate = true
			return
		}
		evictedConn, outcome = cs.registerPeerFromRollbackLocked(e)
		shouldEvaluate = outcome == RollbackRegistrationRegistered
	}()

	// Publish eviction and report the registration outcome outside the lock,
	// matching the roll-forward path: subscribers call back into the selector.
	if evictedConn != nil && cs.config.EventBus != nil {
		cs.config.EventBus.Publish(
			PeerEvictedEventType,
			event.NewEvent(
				PeerEvictedEventType,
				PeerEvictedEvent{ConnectionId: *evictedConn},
			),
		)
	}
	if outcome != "" && cs.config.OnRollbackRegistration != nil {
		cs.config.OnRollbackRegistration(outcome)
	}

	if shouldEvaluate {
		cs.EvaluateAndSwitch()
	}
}

// applyRollbackToTrackedPeer applies a rollback to an already-tracked peer and
// reports whether it did. It takes cs.mutex itself and calls no configured
// callback while holding it.
func (cs *ChainSelector) applyRollbackToTrackedPeer(e PeerRollbackEvent) bool {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	peerTip, exists := cs.peerTips[e.ConnectionId]
	if !exists {
		return false
	}
	delete(cs.pendingPeerTips, e.ConnectionId)
	delete(cs.pendingPeerTipFrontiers, e.ConnectionId)
	cs.prunePeerTipRejectionReportsLocked()
	peerTip.ApplyRollback(e.Point, e.Tip)
	if deliveredTip := cs.deliveredPeerTips[e.ConnectionId]; deliveredTip != nil {
		deliveredTip.ApplyRollback(e.Point, e.Tip)
	}
	cs.rebuildPendingAdmissionStateLocked()
	cs.computeEagernessLimitLocked(nil, cs.localTip)
	return true
}

// registerPeerFromRollbackLocked registers a peer that reported a rollback on a
// connection the selector is not tracking. On a fresh connection the client
// sends MsgFindIntersect, the server answers MsgIntersectFound, and the first
// MsgRequestNext yields MsgRollBackward to the intersection point carrying the
// server's current tip -- and that is the only chainsync traffic until the next
// block is minted. A connection recycle (for example the leios-fetch failover,
// which tears down the whole multiplexed connection) deletes the peer's entry,
// so discarding this rollback leaves the replacement connection invisible to
// chain selection for a full block interval. On a node whose only
// chainsync-selectable upstream was recycled that is a complete chain-selection
// outage, with the header frontier frozen while the ledger keeps applying.
//
// Registration applies the same admission checks the roll-forward path applies
// to a new peer -- connection liveness (checked by the caller, outside the
// lock), tip plausibility, and tracked-peer capacity -- and records only the
// confirmed intersection point as the delivered frontier (see
// newPeerChainTipFromRollback). It returns any evicted
// connection, for the caller to publish outside the lock, and the outcome.
// Callers must hold cs.mutex, and must have established that the connection is
// live before acquiring it (HandlePeerRollbackEvent does). The only
// configured callback invoked from here is OnPeerTipRejected, through
// checkPeerTipPlausibleLocked, and its contract confines it to non-blocking
// work that does not call back into the selector, so nothing under the lock
// can re-enter the selector or block on the connection manager.
func (cs *ChainSelector) registerPeerFromRollbackLocked(
	e PeerRollbackEvent,
) (*ouroboros.ConnectionId, RollbackRegistrationOutcome) {
	if !cs.checkPeerTipPlausibleLocked(
		e.ConnectionId,
		e.Tip,
		ochainsync.Tip{Point: e.Point},
		deliveredLink{},
	) {
		return nil, RollbackRegistrationImplausibleTip
	}
	evictedConn, ok := cs.makeRoomForNewPeerLocked(e.ConnectionId)
	if !ok {
		return nil, RollbackRegistrationAtCapacity
	}
	newPeer := newPeerChainTipFromRollback(
		e.ConnectionId,
		e.Point,
		e.Tip,
	)
	newPeer.nowFn = cs.nowFn
	newPeer.LastUpdated = cs.now()
	cs.peerTips[e.ConnectionId] = newPeer
	cs.advanceSelectionModeLocked()
	cs.config.Logger.Info(
		"registered peer from chainsync rollback",
		"connection_id", e.ConnectionId.String(),
		"rollback_slot", e.Point.Slot,
		"advertised_block", e.Tip.BlockNumber,
		"advertised_slot", e.Tip.Point.Slot,
	)
	return evictedConn, RollbackRegistrationRegistered
}

// onPeerRollbackPanic is the SubscribeFuncStrict onPanic hook for the
// PeerRollbackEventType subscription registered in NewChainSelector. The
// EventBus has already recovered and logged the panic and torn down the
// subscription by the time this runs; it adds chain-selection-specific
// context to the log and publishes PeerRollbackHandlerPanicEventType so an
// operator or automated watcher has a durable signal that rollback handling
// for this selector has stopped, rather than silently missing every
// subsequent peer rollback with no trace beyond a generic log line.
func (cs *ChainSelector) onPeerRollbackPanic(evt event.Event, r any) {
	cs.config.Logger.Error(
		"chain selector rollback handler panicked; rollback subscription stopped",
		"event_type",
		evt.Type,
		"panic",
		r,
	)
	if cs.config.EventBus != nil {
		cs.config.EventBus.Publish(
			PeerRollbackHandlerPanicEventType,
			event.NewEvent(
				PeerRollbackHandlerPanicEventType,
				PeerRollbackHandlerPanicEvent{Panic: r},
			),
		)
	}
}

func (cs *ChainSelector) evaluationLoop() {
	ticker := time.NewTicker(cs.config.EvaluationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cs.ctx.Done():
			return
		case <-cs.evaluationTrigger:
			cs.runTriggeredEvaluation()
		case <-ticker.C:
			cs.runEvaluationTick()
		}
	}
}

// runEvaluationTick runs one evaluation tick with panic recovery. If a panic
// occurs, recoverEvaluationPanic surfaces it and the ticker loop continues on
// the next tick.
func (cs *ChainSelector) runEvaluationTick() {
	defer cs.recoverEvaluationPanic(false)
	cs.cleanupStalePeers()
	cs.EvaluateAndSwitch()
}

func (cs *ChainSelector) runTriggeredEvaluation() {
	defer cs.recoverEvaluationPanic(true)
	cs.EvaluateAndSwitch()
}

// recoverEvaluationPanic recovers a panic from one evaluation tick or
// triggered evaluation, logs it, and publishes EvaluationPanicEventType so
// the dropped transition is surfaced to subscribers instead of vanishing
// silently. The evaluation loop itself keeps running afterward -- both
// runEvaluationTick and runTriggeredEvaluation return normally once this
// defer completes, so evaluationLoop's for/select goes on to the next tick or
// trigger. Stopping chain selection entirely for the whole node because one
// evaluation panicked would be a worse outcome than the missed transition:
// the periodic ticker is itself the recovery path, giving the selector
// another chance on fresh peer/local-tip state a moment later.
//
// The logging and publish calls below run after this function's own
// recover() has already consumed the evaluation panic, so nothing further up
// the stack can catch a second one. Each is wrapped in its own nested
// recovery: a misbehaving Logger or EventBus.Publish panicking here would
// otherwise propagate out of this deferred call as a fresh, unrecovered
// panic, which would stop runEvaluationTick/runTriggeredEvaluation from
// returning at all and crash the entire process once it unwinds past
// evaluationLoop's for/select with nothing left to catch it -- taking down
// chain selection (and the node) over what should have been one dropped
// transition.
func (cs *ChainSelector) recoverEvaluationPanic(triggered bool) {
	r := recover()
	if r == nil {
		return
	}
	func() {
		defer func() {
			if r2 := recover(); r2 != nil {
				// The configured Logger just proved unusable; fall back to
				// the stdlib default rather than risk calling it again.
				// safeLog, not a direct call: the fallback itself must not
				// be the thing that finally lets a panic escape.
				safeLog(
					slog.Default(),
					"panic while logging a chain selection evaluation panic",
					"triggered", triggered,
					"original_panic", r,
					"logging_panic", r2,
				)
			}
		}()
		cs.config.Logger.Error(
			"panic in chain selection evaluation; transition dropped",
			"panic", r,
			"triggered", triggered,
		)
	}()
	if cs.config.EventBus == nil {
		return
	}
	func() {
		defer func() {
			if r2 := recover(); r2 != nil {
				safeLog(
					slog.Default(),
					"panic while publishing a chain selection evaluation "+
						"panic event",
					"triggered", triggered,
					"original_panic", r,
					"publish_panic", r2,
				)
			}
		}()
		cs.config.EventBus.Publish(
			EvaluationPanicEventType,
			event.NewEvent(
				EvaluationPanicEventType,
				EvaluationPanicEvent{Panic: r, Triggered: triggered},
			),
		)
	}()
}

// safeLog calls logger.Error, discarding any panic instead of letting it
// propagate. Used only as the last-resort step inside a panic-recovery path
// that has nothing left above it to catch a further panic -- e.g. reporting
// that the configured Logger itself panicked, via slog.Default() instead.
// Even that fallback must not be the thing that finally lets a panic escape,
// so this is the floor: it stops here, silently, rather than one level
// deeper.
func safeLog(logger *slog.Logger, msg string, args ...any) {
	defer func() { _ = recover() }()
	logger.Error(msg, args...)
}

// sweepDeadDeliveredPeerTips removes delivered-history entries that have no
// admission-frontier record and whose connection is no longer live. Such an
// entry can only be created by RecordDeliveredPeerTip racing the peer's
// removal, and the peerTips-driven stale sweep never reaches it, so without
// this it would hold a maxTrackedPeers slot until Genesis exit. ConnectionLive
// reaches into the composition layer and must not run under cs.mutex, so
// candidates are collected under the lock, probed outside it, and re-checked
// after relocking: an entry that gained a peerTips record in between is kept.
func (cs *ChainSelector) sweepDeadDeliveredPeerTips() {
	if cs.config.ConnectionLive == nil {
		return
	}
	cs.mutex.RLock()
	var candidates []ouroboros.ConnectionId
	for connId := range cs.deliveredPeerTips {
		if _, tracked := cs.peerTips[connId]; !tracked {
			candidates = append(candidates, connId)
		}
	}
	cs.mutex.RUnlock()
	var dead []ouroboros.ConnectionId
	for _, connId := range candidates {
		if !cs.config.ConnectionLive(connId) {
			dead = append(dead, connId)
		}
	}
	if len(dead) == 0 {
		return
	}
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	for _, connId := range dead {
		if _, tracked := cs.peerTips[connId]; !tracked {
			delete(cs.deliveredPeerTips, connId)
		}
	}
}

func (cs *ChainSelector) cleanupStalePeers() {
	cs.sweepDeadDeliveredPeerTips()
	var switchEvent *event.Event

	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()

		var previousBest *ouroboros.ConnectionId
		var previousBestTip *PeerChainTip

		for connId, peerTip := range cs.peerTips {
			// Use 2x the stale threshold for "very stale" cleanup. Peers are
			// skipped from selection after StaleTipThreshold, but we keep them
			// tracked for an additional period in case they reconnect or update.
			// After 2x the threshold, we consider them truly gone and remove them.
			if peerTip.eagernessPaused == 0 &&
				peerTip.IsStale(cs.config.StaleTipThreshold*2) {
				cs.config.Logger.Debug(
					"removing very stale peer",
					"connection_id", connId.String(),
					"last_updated", peerTip.LastUpdated,
				)
				cs.deletePeerLocked(connId)
				// Track if this was the best peer
				if cs.bestPeerConn != nil && *cs.bestPeerConn == connId {
					connIdCopy := connId
					previousBest = &connIdCopy
					previousBestTip = peerTip
					cs.bestPeerConn = nil
				}
			}
		}

		// If the best peer was removed, select a new one and emit event
		if previousBest != nil {
			cs.config.Logger.Info(
				"best peer became stale, selecting new best",
				"connection_id", previousBest.String(),
			)
			newBest := cs.selectBestChainLocked()
			cs.bestPeerConn = newBest

			if newBest != nil {
				cs.config.Logger.Info(
					"selected new best peer after stale cleanup",
					"connection_id", newBest.String(),
				)
				// Emit ChainSwitchEvent so subscribers know to switch connections
				if cs.config.EventBus != nil {
					newPeerTip := cs.peerTips[*newBest]
					newSelectionTip := newPeerTip.SelectionTip()
					evt := event.NewEvent(
						ChainSwitchEventType,
						ChainSwitchEvent{
							PreviousConnectionId: *previousBest,
							NewConnectionId:      *newBest,
							NewTip:               newPeerTip.Tip,
							NewObservedTip:       newSelectionTip,
							NewObservedTipSet:    true,
							ComparisonResult:     ChainComparisonUnknown,
							BlockDifference: safeUint64ToInt64(
								newPeerTip.Tip.BlockNumber,
							),
							RollbackPoint: fragmentIntersection(
								previousBestTip, newPeerTip,
							),
						},
					)
					switchEvent = &evt
				}
			} else {
				// No replacement selected after stale cleanup: this is a
				// selected-to-none transition.
				cs.stageSelectedNoneLocked(*previousBest)
			}
		}
	}()

	// Publish event outside the lock to prevent deadlock if subscribers
	// call back into ChainSelector
	if switchEvent != nil {
		cs.publishSelection(ChainSwitchEventType, *switchEvent)
	}
	cs.publishPendingGenesisExitEvent()
	cs.publishPendingSelectedNoneEvent()
}
