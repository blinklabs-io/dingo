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

package ouroboros

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	okeepalive "github.com/blinklabs-io/gouroboros/protocol/keepalive"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ChainSync Jumping (CSJ), after ouroboros-consensus
// MiniProtocol/ChainSync/Client/Jumping.hs. While Genesis selection is active,
// one identified hot outbound peer (the dynamo) streams headers and every
// other identified hot outbound peer (a jumper) sends no RequestNext: every
// jump interval it answers one MsgFindIntersect for a point of the dynamo's
// candidate fragment, and an agreement becomes its Limit on Eagerness record
// in the selector (ChainSelector.RecordJumpAgreement). A jumper that refuses
// a jump becomes an objector and streams from the last point it agreed to
// when the local chain holds it (csjResumeOnLocalChain).
//
// The wire stays that of a plain ChainSync client: a jumper only ever sends
// MsgFindIntersect from StIdle. Every jumper is also jumped once per refresh
// interval whether or not the dynamo progressed, which keeps its jump record
// inside the selector's stale-tip threshold and its connection inside the
// server's StIdle timeout while the dynamo is quiet or parked.

// csjMaxJumpPoints bounds the points offered in one jump: the dynamo's head
// and earlier points at doubling distances, so a jumper that is behind or
// forked still names the dynamo point it shares.
const csjMaxJumpPoints = 8

// csjDefaultRefreshInterval is a quarter of the chain selector's default
// stale-tip threshold (one keep-alive period plus the keep-alive server
// timeout), after which a jump record no longer counts in the Limit on
// Eagerness or as a witness, and far below the ChainSync server's StIdle
// timeout (ochainsync.IdleTimeout).
const csjDefaultRefreshInterval = (time.Duration(okeepalive.DefaultKeepAlivePeriod)*time.Second +
	okeepalive.ServerTimeout) / 4

// csjDefaultDynamoStallTimeout is how long a dynamo may deliver no header while
// the local chain makes no progress before it is replaced. The dynamo is the
// only peer that feeds the jump records, and the node's own stall recycler
// neither watches an observer nor acts within a catch-up's grace period, so
// this bound is the one that keeps Genesis sync moving.
const csjDefaultDynamoStallTimeout = 30 * time.Second

// csjDynamoStallReason is the reason of the recycle request for a stalled
// dynamo.
const csjDynamoStallReason = "chainsync_jumping_dynamo_stall"

// ChainsyncJumpingConfig wires ChainSync Jumping to chain selection. CSJ runs
// only when Enabled is set and every hook is non-nil.
type ChainsyncJumpingConfig struct {
	Enabled bool
	// Active reports whether Genesis selection is active. It is called from
	// ChainSync callbacks, so it must not take the selector lock.
	Active func() bool
	// PeerIdentity returns the corroboration identity of a connection, or ""
	// when it has none. Only identified peers become dynamo or jumpers: a
	// jump record without an identity could not vouch for the dynamo.
	PeerIdentity func(ouroboros.ConnectionId) string
	// BestPeer returns the selector's current best peer, preferred as dynamo.
	BestPeer func() *ouroboros.ConnectionId
	// DynamoFragment returns the points of the dynamo's candidate fragment,
	// headers still held at the Genesis apply gate included, in ascending
	// order and the block number of the last one
	// (ChainSelector.JumpFragment). It must not wait for those headers to be
	// committed: jump agreements are what corroborates them.
	DynamoFragment func(
		ouroboros.ConnectionId,
	) (points []ocommon.Point, headBlock uint64, ok bool)
	// RecordAgreement and RecordRejection forward jump results to the
	// selector (ChainSelector.RecordJumpAgreement and RecordJumpRejection).
	RecordAgreement func(
		jumper ouroboros.ConnectionId,
		dynamo ouroboros.ConnectionId,
		point ocommon.Point,
		jumperTip ochainsync.Tip,
	) bool
	RecordRejection func(ouroboros.ConnectionId) bool
	// JumpBlocks returns the jump interval in blocks of the dynamo's
	// fragment. The selector holds the dynamo's header stream k blocks past
	// the common point of all candidates, jump records included, so the
	// interval must stay below k or the dynamo parks between jumps.
	JumpBlocks func() uint64
	// RefreshInterval is how often every jumper is jumped to the dynamo's
	// candidate head whether or not the dynamo progressed. Zero selects
	// csjDefaultRefreshInterval.
	RefreshInterval time.Duration
	// DynamoStallTimeout is how long a dynamo may deliver no header, and the
	// local chain make no progress, before it is recycled. Zero selects
	// csjDefaultDynamoStallTimeout.
	DynamoStallTimeout time.Duration
	// LocalProgress returns a value that changes whenever the local chain
	// advances. Optional.
	LocalProgress func() uint64
	// Held reports whether the selector holds the connection's header stream
	// on purpose (the Limit on Eagerness). Optional.
	Held func(ouroboros.ConnectionId) bool
}

func (c ChainsyncJumpingConfig) usable() bool {
	return c.Enabled && c.Active != nil && c.PeerIdentity != nil &&
		c.BestPeer != nil && c.DynamoFragment != nil &&
		c.RecordAgreement != nil && c.RecordRejection != nil &&
		c.JumpBlocks != nil
}

type csjJumper struct {
	lastAgreed ocommon.Point
	agreed     bool
	// jumping marks a jump in flight for this jumper.
	jumping bool
}

// chainsyncJumping is the CSJ role table. mu guards every field; it is never
// held across network I/O or a selector call.
type chainsyncJumping struct {
	cfg ChainsyncJumpingConfig

	mu        sync.Mutex
	dynamo    ouroboros.ConnectionId
	hasDynamo bool
	// lastJumpBlock is the block number of the dynamo point last jumped to;
	// jumped is false until the current dynamo's first jump.
	lastJumpBlock uint64
	jumped        bool
	// roundInFlight admits one jump round at a time.
	roundInFlight bool
	// streamBlock is the block of the dynamo's last delivered header, and
	// restartBlock the first header of a stream the dynamo restarted, until
	// a round jumps to a fragment that reaches it. Jump records copy the
	// dynamo's candidate fragment, which starts over when its stream does;
	// until they copy the new one they share no point with it, and the
	// Limit on Eagerness cannot be measured from them.
	streamBlock  uint64
	restartBlock uint64
	// refreshStarted is set once the refresh loop runs.
	refreshStarted bool
	jumpers        map[ouroboros.ConnectionId]*csjJumper
	// streamers are identified outbound peers streaming headers other than
	// the dynamo, objectors included; any of them can replace the dynamo
	// without a protocol transition.
	streamers map[ouroboros.ConnectionId]struct{}
	objectors map[ouroboros.ConnectionId]struct{}
	// disengaged holds peers that sent MsgAwaitReply while streaming: they
	// reached the tip of their chain. They stay connected and stream as any
	// client does, but are never elected dynamo again and are not jumped.
	// Each one's await is also its idle proof for the Genesis exit, which
	// is why a jumper, that sends no RequestNext and so is never answered
	// with one, is elected dynamo in turn until it too reaches its tip.
	disengaged map[ouroboros.ConnectionId]struct{}

	// progressAt is when the dynamo last delivered a header, or the local
	// chain last advanced, or the dynamo was elected. progressMark is the
	// last LocalProgress value seen, valid once markSeen is set.
	progressAt   time.Time
	progressMark uint64
	markSeen     bool

	now func() time.Time

	metrics *csjMetrics
}

type csjMetrics struct {
	sent     prometheus.Counter
	agreed   prometheus.Counter
	rejected prometheus.Counter
	peers    *prometheus.GaugeVec
}

func newChainsyncJumping(
	cfg ChainsyncJumpingConfig,
	registerer prometheus.Registerer,
) *chainsyncJumping {
	j := &chainsyncJumping{
		cfg:        cfg,
		jumpers:    make(map[ouroboros.ConnectionId]*csjJumper),
		streamers:  make(map[ouroboros.ConnectionId]struct{}),
		objectors:  make(map[ouroboros.ConnectionId]struct{}),
		disengaged: make(map[ouroboros.ConnectionId]struct{}),
		now:        time.Now,
	}
	if registerer != nil {
		factory := promauto.With(registerer)
		j.metrics = &csjMetrics{
			sent: factory.NewCounter(prometheus.CounterOpts{
				Name: "dingo_chainsync_csj_jumps_sent_total",
				Help: "ChainSync Jumping MsgFindIntersect jumps sent to jumpers, answered or not",
			}),
			agreed: factory.NewCounter(prometheus.CounterOpts{
				Name: "dingo_chainsync_csj_jumps_agreed_total",
				Help: "ChainSync Jumping jumps a jumper answered with the jump point, recorded by chain selection",
			}),
			rejected: factory.NewCounter(prometheus.CounterOpts{
				Name: "dingo_chainsync_csj_jumps_rejected_total",
				Help: "ChainSync Jumping jumps a jumper answered with any other point or IntersectNotFound, which makes it an objector",
			}),
			peers: factory.NewGaugeVec(prometheus.GaugeOpts{
				Name: "dingo_chainsync_csj_peers",
				Help: "ChainSync Jumping peers by role: dynamo, jumper, objector, disengaged",
			}, []string{"role"}),
		}
	}
	return j
}

// initChainsyncJumping installs the CSJ role table when the configuration
// enables it, and hooks the dynamo's delivered headers, the jump trigger, into
// ChainsyncRecordDeliveredPeerTip, which every delivered header passes.
func (o *Ouroboros) initChainsyncJumping() {
	if !o.config.ChainsyncJumping.usable() {
		return
	}
	var registerer prometheus.Registerer
	if o.config.PromRegistry != nil {
		registerer = o.registerer
	}
	o.csj = newChainsyncJumping(o.config.ChainsyncJumping, registerer)
	record := o.config.ChainsyncRecordDeliveredPeerTip
	if record == nil {
		return
	}
	o.config.ChainsyncRecordDeliveredPeerTip = func(
		update chainselection.PeerTipUpdateEvent,
	) {
		record(update)
		o.csjObserveDelivery(
			update.ConnectionId,
			update.ObservedTip.BlockNumber,
		)
	}
}

// publishGaugesLocked must be called with j.mu held.
func (j *chainsyncJumping) publishGaugesLocked() {
	if j.metrics == nil {
		return
	}
	dynamo := 0.0
	if j.hasDynamo {
		dynamo = 1
	}
	j.metrics.peers.WithLabelValues("dynamo").Set(dynamo)
	j.metrics.peers.WithLabelValues("jumper").Set(float64(len(j.jumpers)))
	j.metrics.peers.WithLabelValues("objector").Set(float64(len(j.objectors)))
	j.metrics.peers.WithLabelValues("disengaged").Set(float64(len(j.disengaged)))
}

func (j *chainsyncJumping) count(c func(*csjMetrics) prometheus.Counter) {
	if j.metrics != nil {
		c(j.metrics).Inc()
	}
}

func (j *chainsyncJumping) identified(connId ouroboros.ConnectionId) bool {
	return j.cfg.PeerIdentity(connId) != ""
}

// csjIsDynamo reports whether connId is the current dynamo.
func (o *Ouroboros) csjIsDynamo(connId ouroboros.ConnectionId) bool {
	j := o.csj
	if j == nil {
		return false
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.hasDynamo && j.dynamo == connId
}

// csjIsJumper reports whether connId is a jumper, which holds no ingress slot
// and must not be promoted to one.
func (o *Ouroboros) csjIsJumper(connId ouroboros.ConnectionId) bool {
	j := o.csj
	if j == nil {
		return false
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	_, ok := j.jumpers[connId]
	return ok
}

type csjRole uint8

const (
	csjRoleNone csjRole = iota
	csjRoleDynamo
	csjRoleJumper
)

// csjAdmit decides the CSJ role of an outbound connection about to start
// ChainSync: the first identified, ingress-eligible connection while Genesis
// selection is active becomes the dynamo, and later ones jumpers. A dynamo
// needs an ingress slot because jumps name points of its committed fragment,
// which only ingress deliveries extend. A connection without a corroboration
// identity is never the dynamo, since no witness can vouch for its headers;
// it is jumped rather than left to stream headers that can only be held. It
// must be called with the connection's outboundChainsyncLocks lock held.
func (o *Ouroboros) csjAdmit(
	connId ouroboros.ConnectionId,
	ingressEligible bool,
) csjRole {
	j := o.csj
	if j == nil || !j.cfg.Active() {
		return csjRoleNone
	}
	role := j.admit(connId, ingressEligible, j.identified(connId))
	if role != csjRoleNone {
		o.csjStartRefresh()
	}
	return role
}

func (j *chainsyncJumping) admit(
	connId ouroboros.ConnectionId,
	ingressEligible bool,
	identified bool,
) csjRole {
	j.mu.Lock()
	defer j.mu.Unlock()
	defer j.publishGaugesLocked()
	if !j.hasDynamo {
		if !ingressEligible || !identified {
			return csjRoleNone
		}
		j.setDynamoLocked(connId)
		return csjRoleDynamo
	}
	if j.dynamo == connId {
		return csjRoleDynamo
	}
	if _, ok := j.jumpers[connId]; !ok {
		j.jumpers[connId] = &csjJumper{}
	}
	delete(j.streamers, connId)
	return csjRoleJumper
}

// csjStarted runs once a streaming client started. A dynamo that registered
// as an observer, because every ingress slot was taken, cannot extend a
// candidate fragment, so it is replaced; it streams on as any observer does.
func (o *Ouroboros) csjStarted(connId ouroboros.ConnectionId, role csjRole) {
	if o.csj == nil {
		return
	}
	observer, _ := o.chainsyncState.ClientObservabilityOnly(connId)
	if role == csjRoleDynamo && observer {
		o.csjForget(connId)
	}
	if !observer {
		o.csjNoteStreaming(connId)
	}
}

// csjNoteStreaming records an identified outbound connection that streams
// headers with an ingress slot outside CSJ, as a dynamo replacement that
// needs no transition.
func (o *Ouroboros) csjNoteStreaming(connId ouroboros.ConnectionId) {
	j := o.csj
	if j == nil || !j.identified(connId) {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.hasDynamo && j.dynamo == connId {
		return
	}
	delete(j.jumpers, connId)
	j.streamers[connId] = struct{}{}
	j.publishGaugesLocked()
}

// setDynamoLocked must be called with j.mu held.
func (j *chainsyncJumping) setDynamoLocked(connId ouroboros.ConnectionId) {
	j.dynamo = connId
	j.hasDynamo = true
	j.jumped = false
	j.lastJumpBlock = 0
	j.streamBlock = 0
	j.restartBlock = 0
	j.progressAt = j.now()
	delete(j.streamers, connId)
	delete(j.jumpers, connId)
}

// csjForget drops a connection whose ChainSync client stopped, by demotion or
// close. A departing dynamo is replaced.
func (o *Ouroboros) csjForget(connId ouroboros.ConnectionId) {
	j := o.csj
	if j == nil {
		return
	}
	j.mu.Lock()
	delete(j.jumpers, connId)
	delete(j.streamers, connId)
	delete(j.objectors, connId)
	delete(j.disengaged, connId)
	wasDynamo := j.hasDynamo && j.dynamo == connId
	if wasDynamo {
		j.hasDynamo = false
	}
	j.publishGaugesLocked()
	j.mu.Unlock()
	if wasDynamo {
		o.csjRotateDynamo()
	}
}

// csjCandidate is a peer that can be elected dynamo.
type csjCandidate struct {
	connId ouroboros.ConnectionId
	// jumper is set for a peer that sends no RequestNext and so is started
	// streaming when elected.
	jumper bool
}

// csjElectionOrderLocked ranks the peers that can replace a dynamo: the
// selector's best peer; streamers that are not objectors, which stream already
// and need no protocol transition; jumpers that agreed to a jump, then those
// that did not, which resume from a dynamo point; and last the objectors,
// which refused a jump, so they follow another chain or lag the dynamo, and
// hold no ingress slot. It must be called with j.mu held.
func (j *chainsyncJumping) csjElectionOrderLocked(
	best *ouroboros.ConnectionId,
) []csjCandidate {
	var streamers, agreed, unagreed, objectors []ouroboros.ConnectionId
	for id := range j.streamers {
		if best != nil && id == *best {
			continue
		}
		if _, objector := j.objectors[id]; objector {
			objectors = append(objectors, id)
		} else {
			streamers = append(streamers, id)
		}
	}
	for id, jumper := range j.jumpers {
		switch {
		case best != nil && id == *best:
		case jumper.agreed:
			agreed = append(agreed, id)
		default:
			unagreed = append(unagreed, id)
		}
	}
	candidates := make([]csjCandidate, 0, len(j.streamers)+len(j.jumpers))
	if best != nil {
		if _, ok := j.streamers[*best]; ok {
			candidates = append(candidates, csjCandidate{connId: *best})
		} else if _, ok := j.jumpers[*best]; ok {
			candidates = append(
				candidates,
				csjCandidate{connId: *best, jumper: true},
			)
		}
	}
	appendAll := func(ids []ouroboros.ConnectionId, jumper bool) {
		slices.SortFunc(ids, compareConnIds)
		for _, id := range ids {
			candidates = append(candidates, csjCandidate{connId: id, jumper: jumper})
		}
	}
	appendAll(streamers, false)
	appendAll(agreed, true)
	appendAll(unagreed, true)
	appendAll(objectors, false)
	return candidates
}

// csjRotateDynamo elects a new dynamo from csjElectionOrderLocked's ranking.
// The elected peer holds an ingress slot, claimed as it is elected: jumps name
// points of the dynamo's candidate fragment, which only ingress deliveries
// extend, so a peer that cannot claim one is passed over. When none can, no
// dynamo is elected; the refresh loop and the next admission retry.
func (o *Ouroboros) csjRotateDynamo() {
	j := o.csj
	if j == nil {
		return
	}
	if !j.cfg.Active() {
		o.csjDisengage()
		return
	}
	best := j.cfg.BestPeer()
	j.mu.Lock()
	if j.hasDynamo {
		j.mu.Unlock()
		return
	}
	candidates := j.csjElectionOrderLocked(best)
	j.mu.Unlock()
	starved := false
	for _, c := range candidates {
		if !j.identified(c.connId) ||
			!o.shouldPublishChainsyncToLedger(c.connId) {
			continue
		}
		if !c.jumper {
			if o.csjPromoteStreamer(c.connId) {
				o.csjLogRotation(c.connId, false)
				return
			}
			starved = true
			continue
		}
		resume, ok := o.csjPromoteJumper(c.connId)
		if !ok {
			starved = true
			continue
		}
		o.csjLogRotation(c.connId, true)
		o.csjStartStreaming(c.connId, resume, "dynamo")
		return
	}
	if starved && o.csjYieldDisengagedSlot() {
		o.csjRotateDynamo()
		return
	}
	o.csjReleaseUnidentified()
}

// csjYieldDisengagedSlot moves one disengaged peer that holds an ingress slot
// to observer, so an elected dynamo can claim the slot, and reports whether
// it did. A disengaged peer has streamed to its tip, and an observer's await
// still counts as idle for the Genesis exit; but a jumper that cannot claim a
// slot is never elected, never streams, and so never becomes idle. Each call
// demotes at most one peer, so the retry that follows is bounded by their
// number.
func (o *Ouroboros) csjYieldDisengagedSlot() bool {
	j := o.csj
	if o.chainsyncState == nil {
		return false
	}
	j.mu.Lock()
	if j.hasDynamo {
		j.mu.Unlock()
		return false
	}
	ids := make([]ouroboros.ConnectionId, 0, len(j.disengaged))
	for id := range j.disengaged {
		ids = append(ids, id)
	}
	j.mu.Unlock()
	slices.SortFunc(ids, compareConnIds)
	for _, id := range ids {
		observer, tracked := o.chainsyncState.ClientObservabilityOnly(id)
		if !tracked || observer {
			continue
		}
		if !o.chainsyncState.SetClientObservabilityOnly(id, true) {
			continue
		}
		o.config.Logger.Info(
			"chainsync jumping: disengaged peer yielded its ingress slot",
			"component", "ouroboros",
			"connection_id", id.String(),
		)
		return true
	}
	return false
}

// csjPromoteStreamer makes streamer the dynamo and claims it an ingress slot.
// A streamer that was an objector holds none. It restores the streamer and
// returns false when it is no longer one, a dynamo was elected meanwhile, or no
// slot is free. Claiming the slot wakes a callback the connection parked as an
// observer.
func (o *Ouroboros) csjPromoteStreamer(connId ouroboros.ConnectionId) bool {
	j := o.csj
	j.mu.Lock()
	if _, ok := j.streamers[connId]; !ok || j.hasDynamo {
		j.mu.Unlock()
		return false
	}
	j.setDynamoLocked(connId)
	j.mu.Unlock()
	claimed := o.reconcileChainsyncIngressAdmission(connId, true)
	j.mu.Lock()
	defer j.mu.Unlock()
	defer j.publishGaugesLocked()
	elected := j.hasDynamo && j.dynamo == connId
	if !claimed && elected {
		j.hasDynamo = false
		j.streamers[connId] = struct{}{}
	}
	if !claimed || !elected {
		return false
	}
	delete(j.objectors, connId)
	return true
}

// csjReleaseUnidentified returns jumpers without a corroboration identity to
// streaming when no dynamo could be elected: they are jumped only to a
// dynamo's fragment, so without one they would receive no headers at all.
func (o *Ouroboros) csjReleaseUnidentified() {
	j := o.csj
	j.mu.Lock()
	ids := make([]ouroboros.ConnectionId, 0, len(j.jumpers))
	for id := range j.jumpers {
		ids = append(ids, id)
	}
	j.mu.Unlock()
	for _, id := range ids {
		if j.identified(id) {
			continue
		}
		j.mu.Lock()
		if j.hasDynamo {
			j.mu.Unlock()
			return
		}
		jumper, ok := j.jumpers[id]
		if !ok {
			j.mu.Unlock()
			continue
		}
		delete(j.jumpers, id)
		j.publishGaugesLocked()
		j.mu.Unlock()
		var resume []ocommon.Point
		if jumper.agreed {
			resume = []ocommon.Point{jumper.lastAgreed}
		}
		o.csjStartStreaming(id, resume, "release")
	}
}

// csjPromoteJumper makes jumper the dynamo and claims it an ingress slot,
// returning the point it resumes streaming from. It restores the jumper and
// returns false when it is no longer a jumper, a dynamo was elected
// meanwhile, or no ingress slot is free.
func (o *Ouroboros) csjPromoteJumper(
	connId ouroboros.ConnectionId,
) ([]ocommon.Point, bool) {
	j := o.csj
	j.mu.Lock()
	jumper, ok := j.jumpers[connId]
	if !ok || j.hasDynamo {
		j.mu.Unlock()
		return nil, false
	}
	// Leaving the jumper set first lets the ingress reconcile, which refuses
	// a jumper, grant the slot.
	j.setDynamoLocked(connId)
	j.mu.Unlock()
	claimed := o.reconcileChainsyncIngressAdmission(connId, true)
	j.mu.Lock()
	defer j.mu.Unlock()
	defer j.publishGaugesLocked()
	elected := j.hasDynamo && j.dynamo == connId
	if !claimed && elected {
		j.hasDynamo = false
		j.jumpers[connId] = jumper
	}
	if !claimed || !elected {
		return nil, false
	}
	delete(j.objectors, connId)
	if !jumper.agreed {
		return nil, true
	}
	return []ocommon.Point{jumper.lastAgreed}, true
}

func (o *Ouroboros) csjLogRotation(
	connId ouroboros.ConnectionId,
	fromJumper bool,
) {
	o.config.Logger.Info(
		"chainsync jumping: dynamo rotated",
		"component", "ouroboros",
		"connection_id", connId.String(),
		"from_jumper", fromJumper,
	)
}

// csjDisengage ends CSJ for the current jumpers: each one streams from its
// last agreed point. It runs when Genesis selection is no longer active.
func (o *Ouroboros) csjDisengage() {
	j := o.csj
	if j == nil {
		return
	}
	j.mu.Lock()
	if len(j.jumpers) == 0 && !j.hasDynamo {
		j.mu.Unlock()
		return
	}
	type resume struct {
		connId ouroboros.ConnectionId
		points []ocommon.Point
	}
	resumes := make([]resume, 0, len(j.jumpers))
	for id, jumper := range j.jumpers {
		var points []ocommon.Point
		if jumper.agreed {
			points = []ocommon.Point{jumper.lastAgreed}
		}
		resumes = append(resumes, resume{connId: id, points: points})
	}
	clear(j.jumpers)
	clear(j.streamers)
	clear(j.objectors)
	clear(j.disengaged)
	j.hasDynamo = false
	j.publishGaugesLocked()
	j.mu.Unlock()
	if len(resumes) > 0 {
		o.config.Logger.Info(
			"chainsync jumping: disengaged, jumpers resume streaming",
			"component", "ouroboros",
			"jumpers", len(resumes),
		)
	}
	for _, r := range resumes {
		o.csjStartStreaming(r.connId, r.points, "disengage")
	}
}

// csjOnAwaitReply runs when a streaming client received MsgAwaitReply: the
// peer has sent every header up to its tip. As in the reference's
// onAwaitReply, a dynamo that did so is disengaged and the next dynamo is
// elected, so that every peer streams to its tip in turn; a streamer, an
// objector included, is disengaged without an election. The peer's idle
// proof reaches chain selection separately. Without this a jumper would
// never become idle, and the Genesis exit, which needs every ChainSync
// connection idle, could never happen while one was connected.
func (o *Ouroboros) csjOnAwaitReply(connId ouroboros.ConnectionId) {
	j := o.csj
	if j == nil || !j.cfg.Active() {
		return
	}
	j.mu.Lock()
	wasDynamo := j.hasDynamo && j.dynamo == connId
	_, streamer := j.streamers[connId]
	if !wasDynamo && !streamer {
		j.mu.Unlock()
		return
	}
	if wasDynamo {
		j.hasDynamo = false
	}
	delete(j.streamers, connId)
	delete(j.objectors, connId)
	j.disengaged[connId] = struct{}{}
	j.publishGaugesLocked()
	j.mu.Unlock()
	o.config.Logger.Info(
		"chainsync jumping: peer reached its tip, disengaged",
		"component", "ouroboros",
		"connection_id", connId.String(),
		"was_dynamo", wasDynamo,
	)
	if wasDynamo {
		// An election claims an ingress slot and signals the new dynamo's
		// client, which a protocol callback must not wait for.
		o.csjGoRound(o.csjRotateDynamo)
	}
}

// csjObserveDelivery runs for every header delivered on any connection, from
// the ChainSync callback, so it only decides and hands work to a goroutine.
func (o *Ouroboros) csjObserveDelivery(
	connId ouroboros.ConnectionId,
	blockNumber uint64,
) {
	j := o.csj
	if j == nil || o.csjDisengageIfInactive() {
		return
	}
	j.mu.Lock()
	isDynamo := j.hasDynamo && j.dynamo == connId
	if isDynamo {
		j.progressAt = j.now()
		// A header at or below the last one starts a new stream.
		if j.streamBlock != 0 && blockNumber <= j.streamBlock {
			j.restartBlock = blockNumber
		}
		j.streamBlock = blockNumber
	}
	due := isDynamo && !j.roundInFlight && len(j.jumpers) > 0 &&
		(!j.jumped || j.restartBlock != 0 ||
			blockNumber >= j.lastJumpBlock+j.jumpBlocks())
	if due {
		j.roundInFlight = true
	}
	j.mu.Unlock()
	if due {
		o.csjGoRound(func() { o.csjJumpRound(nil, false) })
	}
}

// csjDisengageIfInactive disengages CSJ once Genesis selection is no longer
// active and reports whether it is inactive. It only takes j.mu and hands
// the streams to goroutines, so ChainSync callbacks may call it.
func (o *Ouroboros) csjDisengageIfInactive() bool {
	j := o.csj
	if j.cfg.Active() {
		return false
	}
	j.mu.Lock()
	engaged := j.hasDynamo || len(j.jumpers) > 0
	j.mu.Unlock()
	if engaged {
		o.csjDisengage()
	}
	return true
}

// csjStartRefresh starts the refresh loop once, under the outbound ChainSync
// lifecycle, so Close stops it.
func (o *Ouroboros) csjStartRefresh() {
	j := o.csj
	j.mu.Lock()
	if j.refreshStarted {
		j.mu.Unlock()
		return
	}
	j.refreshStarted = true
	interval := j.cfg.RefreshInterval
	j.mu.Unlock()
	if interval <= 0 {
		interval = csjDefaultRefreshInterval
	}
	stallTimeout := j.cfg.DynamoStallTimeout
	if stallTimeout <= 0 {
		stallTimeout = csjDefaultDynamoStallTimeout
	}
	o.csjGo(func(ctx context.Context) {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		// Checked several times per timeout, so a stall is acted on well
		// within the refresh interval.
		stallTicker := time.NewTicker(max(stallTimeout/3, time.Millisecond))
		defer stallTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				o.csjRefresh() //nolint:contextcheck // Its goroutines run under the same lifecycle context.
			case <-stallTicker.C:
				o.csjCheckDynamoStall(j.now())
			}
		}
	})
}

// csjRefresh runs once per refresh interval. Rounds otherwise start only from
// the dynamo's deliveries, and a dynamo that is quiet, or parked by the Limit
// on Eagerness, delivers none: without a round the jumpers' records go stale
// and drop out of the limit and the witness count, and their connections
// idle towards the server's StIdle timeout. So a refresh jumps every jumper
// to the dynamo's candidate head even when it has not moved, and elects a
// dynamo when there is none.
func (o *Ouroboros) csjRefresh() {
	j := o.csj
	if o.csjDisengageIfInactive() {
		return
	}
	j.mu.Lock()
	elect := !j.hasDynamo && len(j.jumpers) > 0
	due := j.hasDynamo && len(j.jumpers) > 0 && !j.roundInFlight
	if due {
		j.roundInFlight = true
	}
	j.mu.Unlock()
	if elect {
		o.csjRotateDynamo()
		return
	}
	if due {
		o.csjJumpRound(nil, true)
	}
	o.csjAdoptUnidentified()
}

// csjAdoptUnidentified jumps outbound peers without a corroboration identity
// that stream outside CSJ, as those that started before any dynamo existed
// do: no witness can vouch for their headers, so streaming them only
// downloads headers to hold. Each is stopped, which drains its session or
// closes the connection, and started again, as a jumper while a dynamo
// exists. An objector streams on purpose and is left alone, and so is a
// disengaged peer, which awaited at its tip and would never await again as a
// jumper.
func (o *Ouroboros) csjAdoptUnidentified() {
	j := o.csj
	if o.chainsyncState == nil || o.connManager == nil {
		return
	}
	for _, id := range o.chainsyncState.GetClientConnIds() {
		if o.connManager.IsInboundConnection(id) || j.identified(id) {
			continue
		}
		j.mu.Lock()
		hasDynamo := j.hasDynamo
		_, jumper := j.jumpers[id]
		_, streamer := j.streamers[id]
		_, objector := j.objectors[id]
		_, disengaged := j.disengaged[id]
		dynamo := hasDynamo && j.dynamo == id
		j.mu.Unlock()
		if !hasDynamo {
			return
		}
		if jumper || streamer || objector || disengaged || dynamo {
			continue
		}
		o.csjGo(func(ctx context.Context) {
			o.csjRestartAsJumper(ctx, id)
		})
	}
}

// csjRestartAsJumper stops connId's ChainSync client and starts it again,
// which admits it as a jumper while a dynamo exists. It serializes with
// temperature start and stop.
func (o *Ouroboros) csjRestartAsJumper(
	ctx context.Context,
	connId ouroboros.ConnectionId,
) {
	defer o.outboundChainsyncLocks.acquire(connId)()
	if ctx.Err() != nil {
		return
	}
	conn := o.connManager.GetConnectionById(connId)
	if conn == nil || !o.outboundChainsyncWanted(connId) {
		return
	}
	o.stopOutboundChainsync(ctx, connId, conn)
	// A session that did not drain closes the connection instead.
	if o.connManager.GetConnectionById(connId) != conn {
		return
	}
	o.startOutboundChainsync(connId, conn) //nolint:contextcheck // Its goroutines run under the same lifecycle context.
}

func (j *chainsyncJumping) jumpBlocks() uint64 {
	return max(j.cfg.JumpBlocks(), 1)
}

// csjGo runs fn in a goroutine that Close cancels and waits for. The work
// belongs to the driver, not to whichever caller triggered it, so it runs
// under the outbound ChainSync lifecycle context.
func (o *Ouroboros) csjGo(fn func(context.Context)) {
	ctx, ok := o.outboundChainsyncLifecycle.begin()
	if !ok {
		return
	}
	go func() {
		defer o.outboundChainsyncLifecycle.wg.Done()
		fn(ctx)
	}()
}

// csjGoRound is csjGo for jump work, which needs no context: a jump is
// bounded by the protocol's own timeout and by the connection closing.
func (o *Ouroboros) csjGoRound(fn func()) {
	o.csjGo(func(context.Context) { fn() })
}

// csjJumpNew jumps a newly admitted jumper to the dynamo's current point
// without waiting for the next round.
func (o *Ouroboros) csjJumpNew(connId ouroboros.ConnectionId) {
	o.csjGoRound(func() {
		o.csjJumpRound([]ouroboros.ConnectionId{connId}, false)
	})
}

// csjJumpRound jumps targets, or every jumper when targets is nil (a round,
// which owns roundInFlight), to the dynamo's candidate head. A round skips
// unless the head is a jump interval past the last jump's, or force is set.
func (o *Ouroboros) csjJumpRound(
	targets []ouroboros.ConnectionId,
	force bool,
) {
	j := o.csj
	round := targets == nil
	defer func() {
		if round {
			j.mu.Lock()
			j.roundInFlight = false
			j.mu.Unlock()
		}
	}()
	if !j.cfg.Active() {
		o.csjDisengage()
		return
	}
	j.mu.Lock()
	if !j.hasDynamo {
		j.mu.Unlock()
		return
	}
	dynamo := j.dynamo
	j.mu.Unlock()
	fragment, headBlock, ok := j.cfg.DynamoFragment(dynamo)
	if !ok || len(fragment) == 0 {
		return
	}
	points := csjJumpPoints(fragment)
	j.mu.Lock()
	if !j.hasDynamo || j.dynamo != dynamo {
		j.mu.Unlock()
		return
	}
	if round {
		// A restart forces a round once the candidate fragment holds the
		// new stream's first header; before that it would copy the old one.
		restarted := j.restartBlock != 0 && headBlock >= j.restartBlock
		if !force && !restarted && j.jumped &&
			headBlock < j.lastJumpBlock+j.jumpBlocks() {
			j.mu.Unlock()
			return
		}
		if restarted {
			j.restartBlock = 0
		}
		j.jumped = true
		j.lastJumpBlock = headBlock
		for id := range j.jumpers {
			targets = append(targets, id)
		}
	} else if !j.jumped {
		// The dynamo's first jump, whichever jumper it was for, is where
		// the interval is counted from.
		j.jumped = true
		j.lastJumpBlock = headBlock
	}
	type job struct {
		connId ouroboros.ConnectionId
		points []ocommon.Point
	}
	jobs := make([]job, 0, len(targets))
	for _, id := range targets {
		jumper, ok := j.jumpers[id]
		if !ok || jumper.jumping {
			continue
		}
		jumper.jumping = true
		offered := points
		if jumper.agreed {
			offered = append(slices.Clone(points), jumper.lastAgreed)
		}
		jobs = append(jobs, job{connId: id, points: offered})
	}
	j.mu.Unlock()
	var wg sync.WaitGroup
	for _, jb := range jobs {
		wg.Go(func() {
			o.csjJump(jb.connId, dynamo, jb.points)
		})
	}
	wg.Wait()
}

// csjJumpPoints returns the fragment's head followed by earlier points at
// doubling distances, newest first.
func csjJumpPoints(fragment []ocommon.Point) []ocommon.Point {
	if len(fragment) == 0 {
		return nil
	}
	points := make([]ocommon.Point, 0, csjMaxJumpPoints)
	last := len(fragment) - 1
	for back := 0; back <= last && len(points) < csjMaxJumpPoints; {
		points = append(points, fragment[last-back])
		if back == 0 {
			back = 1
		} else {
			back *= 2
		}
	}
	return points
}

// csjJump sends one jump to jumper and applies its answer. J is points[0],
// the head of the dynamo's candidate fragment.
func (o *Ouroboros) csjJump(
	jumper ouroboros.ConnectionId,
	dynamo ouroboros.ConnectionId,
	points []ocommon.Point,
) {
	j := o.csj
	defer func() {
		j.mu.Lock()
		if state, ok := j.jumpers[jumper]; ok {
			state.jumping = false
		}
		j.mu.Unlock()
	}()
	unlock := o.outboundChainsyncLocks.acquire(jumper)
	// The role may have changed while the jump waited for the lock: a
	// former jumper streams and must not be asked to jump.
	j.mu.Lock()
	_, isJumper := j.jumpers[jumper]
	j.mu.Unlock()
	if !isJumper {
		unlock()
		return
	}
	point, tip, found, err := o.csjSendJump(jumper, points)
	unlock()
	if !errors.Is(err, errCsjNoClient) &&
		!errors.Is(err, ochainsync.ErrSyncInProgress) {
		j.count(func(m *csjMetrics) prometheus.Counter { return m.sent })
	}
	if err != nil {
		if !errors.Is(err, ochainsync.ErrSyncInProgress) {
			o.config.Logger.Debug(
				"chainsync jumping: jump failed",
				"component", "ouroboros",
				"connection_id", jumper.String(),
				"error", err,
			)
		}
		return
	}
	// The answer, not the selector's acceptance, decides the role. Only J
	// itself is an agreement; any other answer is a rejection, as in the
	// reference's processJumpResult, whether the jumper forks after the
	// returned point or is merely behind J. Either way it streams, where
	// the Limit on Patience and the density disconnector apply to it; left
	// a jumper, it would hold the Limit on Eagerness k past its last
	// agreement. An earlier dynamo point it holds is still recorded, so its
	// frozen record keeps the highest point it shares with the dynamo.
	agreedHead := found && pointsEqual(point, points[0])
	if found && j.cfg.RecordAgreement(jumper, dynamo, point, tip) &&
		agreedHead {
		j.count(func(m *csjMetrics) prometheus.Counter { return m.agreed })
	}
	j.mu.Lock()
	state, ok := j.jumpers[jumper]
	if !ok {
		j.mu.Unlock()
		return
	}
	if found {
		state.lastAgreed = point
		state.agreed = true
	}
	if agreedHead {
		j.mu.Unlock()
		return
	}
	var resume []ocommon.Point
	if state.agreed {
		resume = []ocommon.Point{state.lastAgreed}
	}
	delete(j.jumpers, jumper)
	j.objectors[jumper] = struct{}{}
	j.streamers[jumper] = struct{}{}
	j.publishGaugesLocked()
	j.mu.Unlock()
	j.cfg.RecordRejection(jumper)
	j.count(func(m *csjMetrics) prometheus.Counter { return m.rejected })
	o.config.Logger.Info(
		"chainsync jumping: jumper refused a jump, streaming as objector",
		"component", "ouroboros",
		"connection_id", jumper.String(),
		"jump_slot", points[0].Slot,
		"found", found,
		"intersect_slot", point.Slot,
	)
	o.csjStartStreaming(jumper, resume, "objector")
}

func pointsEqual(a, b ocommon.Point) bool {
	return a.Slot == b.Slot && string(a.Hash) == string(b.Hash)
}

// csjSendJump issues the jump on the connection's ChainSync client.
func (o *Ouroboros) csjSendJump(
	connId ouroboros.ConnectionId,
	points []ocommon.Point,
) (ocommon.Point, ochainsync.Tip, bool, error) {
	conn := o.connManager.GetConnectionById(connId)
	if conn == nil || conn.ChainSync() == nil ||
		conn.ChainSync().Client == nil {
		return ocommon.Point{}, ochainsync.Tip{}, false, errCsjNoClient
	}
	return conn.ChainSync().Client.Jump(points)
}

var errCsjNoClient = errors.New("chainsync client not available")

// csjStartStreaming moves a former jumper to a normal ChainSync stream that
// intersects at the resume points the local chain holds, falling back to the
// default intersect points. It runs in its own goroutine and serializes with
// temperature start and stop.
func (o *Ouroboros) csjStartStreaming(
	connId ouroboros.ConnectionId,
	resume []ocommon.Point,
	role string,
) {
	o.csjGo(func(ctx context.Context) {
		if ctx.Err() != nil {
			return
		}
		defer o.outboundChainsyncLocks.acquire(connId)()
		conn := o.connManager.GetConnectionById(connId)
		if conn == nil || o.chainsyncState == nil ||
			!o.chainsyncState.HasClientConnId(connId) ||
			!o.outboundChainsyncWanted(connId) {
			return
		}
		resume = o.csjResumeOnLocalChain(ctx, resume)
		points, err := o.buildDefaultChainsyncIntersectPoints(ctx, connId)
		if err != nil {
			points = nil
		}
		points = append(slices.Clone(resume), points...)
		if err := o.syncChainsyncClient(connId, points); err != nil {
			o.config.Logger.Warn(
				"chainsync jumping: former jumper failed to start streaming, closing connection",
				"component",
				"ouroboros",
				"connection_id",
				connId.String(),
				"role",
				role,
				"error",
				err,
			)
			o.chainsyncState.RemoveClientConnId(connId)
			o.closeOutboundConnAfterChainsyncFailure(connId, conn)
			return
		}
		o.config.Logger.Debug(
			"chainsync jumping: former jumper streaming",
			"component", "ouroboros",
			"connection_id", connId.String(),
			"role", role,
		)
	})
}

// csjResumeOnLocalChain keeps the resume points the local chain holds. The
// ledger authenticates a delivered header only through ancestry it holds: the
// local chain, or headers the same connection delivered before. A former
// jumper delivered none, and an agreed point above the local chain lies on
// headers only the dynamo delivered, which leave with it when its connection
// closes. A stream intersecting there has every header it delivers withheld
// from chain selection, never forms a candidate fragment, and so starts no
// jump. Dropping the point intersects on the local chain instead, at the cost
// of re-delivering the headers up to it, at most a jump interval.
func (o *Ouroboros) csjResumeOnLocalChain(
	ctx context.Context,
	resume []ocommon.Point,
) []ocommon.Point {
	if o.ledgerState == nil {
		return nil
	}
	chain := o.ledgerState.Chain()
	kept := make([]ocommon.Point, 0, len(resume))
	for _, point := range resume {
		if chain.HoldsPoint(ctx, point) {
			kept = append(kept, point)
		}
	}
	return kept
}

func compareConnIds(a, b ouroboros.ConnectionId) int {
	as, bs := a.String(), b.String()
	switch {
	case as < bs:
		return -1
	case as > bs:
		return 1
	}
	return 0
}

// csjCheckDynamoStall recycles the dynamo when it has delivered no header for
// the stall timeout while the local chain made no progress either. The dynamo
// feeds the jump records, so a dynamo that delivers nothing, parked in a wait
// that nothing releases, idle in its protocol loop, or demoted to an observer,
// stops all of Genesis sync. The node's stall recycler does not catch it:
// it skips observers and delays recycling while a catch-up is under way. A
// dynamo the selector holds at the Limit on Eagerness is waiting on purpose
// and restarts the clock.
func (o *Ouroboros) csjCheckDynamoStall(now time.Time) {
	j := o.csj
	if j == nil {
		return
	}
	var mark uint64
	if j.cfg.LocalProgress != nil {
		mark = j.cfg.LocalProgress()
	}
	j.mu.Lock()
	if !j.hasDynamo {
		j.mu.Unlock()
		return
	}
	dynamo := j.dynamo
	j.mu.Unlock()
	held := j.cfg.Held != nil && j.cfg.Held(dynamo)
	timeout := j.cfg.DynamoStallTimeout
	if timeout <= 0 {
		timeout = csjDefaultDynamoStallTimeout
	}
	j.mu.Lock()
	if !j.hasDynamo || j.dynamo != dynamo {
		j.mu.Unlock()
		return
	}
	if held || (j.markSeen && mark != j.progressMark) {
		j.progressAt = now
	}
	j.markSeen = true
	j.progressMark = mark
	stalled := now.Sub(j.progressAt) > timeout
	idle := now.Sub(j.progressAt)
	if stalled {
		// The close that follows the recycle elects the replacement; until
		// it lands, do not request the recycle again.
		j.progressAt = now
	}
	j.mu.Unlock()
	if !stalled || o.eventBus == nil {
		return
	}
	o.config.Logger.Warn(
		"chainsync jumping: dynamo delivered no header, recycling it",
		"component", "ouroboros",
		"connection_id", dynamo.String(),
		"idle", idle,
		"stall_timeout", timeout,
	)
	o.eventBus.Publish(
		ledger.ConnectionRecycleRequestedEventType,
		event.NewEvent(
			ledger.ConnectionRecycleRequestedEventType,
			ledger.ConnectionRecycleRequestedEvent{
				ConnectionId: dynamo,
				Reason:       csjDynamoStallReason,
			},
		),
	)
}
