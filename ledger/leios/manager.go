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
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// voteStoreTTL bounds how long collected votes are retained,
	// mirroring the endorser block cache in the ouroboros component.
	voteStoreTTL = 10 * time.Minute
	// voteStoreMaxEntries bounds the vote store size. Votes are ~100
	// bytes, so this is roughly a 1 MiB ceiling.
	voteStoreMaxEntries = 8192
)

// ErrVoteManagerStopped is returned by blocking calls when the vote
// manager is not running.
var ErrVoteManagerStopped = errors.New("leios vote manager stopped")

// StakeDistributionProvider supplies the active stake distribution for a
// snapshot epoch: lowercase-hex pool key hash -> stake plus the total
// active stake used as the quorum denominator.
type StakeDistributionProvider interface {
	GetStakeDistribution(
		epoch uint64,
	) (poolStakes map[string]uint64, totalActiveStake uint64, err error)
}

// EpochProvider supplies epoch information from the ledger.
type EpochProvider interface {
	CurrentEpoch() uint64
	EpochForSlot(slot uint64) (uint64, error)
}

// CommitteeParamsProvider supplies the Leios committee protocol
// parameters. Implementations must validate the tau < sigma_c invariant
// (DijkstraProtocolParameters.ValidateLeiosCommitteeParameters) and
// surface failures as errors.
type CommitteeParamsProvider interface {
	LeiosCommitteeParameters() (sigmaC, tau *big.Rat, err error)
}

// VoteManagerConfig configures a VoteManager.
type VoteManagerConfig struct {
	Logger         *slog.Logger
	EventBus       *event.EventBus
	StakeProvider  StakeDistributionProvider
	EpochProvider  EpochProvider
	ParamsProvider CommitteeParamsProvider
	Registry       *VoterRegistry
	PromRegistry   prometheus.Registerer
}

// storedVote is one retained vote with its serving metadata.
type storedVote struct {
	vote       lcommon.LeiosVote
	raw        cbor.RawMessage
	originConn string // empty for locally emitted votes
	verified   bool
	seq        uint64
	epoch      uint64
	insertedAt time.Time
}

// tallyKey identifies the vote tally for one endorser block.
type tallyKey struct {
	slotNo uint64
	ebHash lcommon.Blake2b256
}

// ebTally accumulates vote stake for one endorser block. observedStake
// counts every membership-valid, deduplicated vote; verifiedStake counts
// only signature-verified votes. Certificates are built exclusively from
// verified votes.
type ebTally struct {
	epoch                uint64
	observedStake        uint64
	verifiedStake        uint64
	verifiedVotes        []VerifiedVote
	certBuilt            bool
	observedQuorumLogged bool
	lastUpdated          time.Time
}

// epochEntry memoizes the committee and quorum threshold for an epoch.
type epochEntry struct {
	committee *Committee
	tau       *big.Rat
}

// VoteManager collects, validates, serves, and emits Leios votes. It
// memoizes per-epoch voting committees from stake snapshots, tallies vote
// stake per endorser block, and builds a certificate when verified votes
// meet the stake quorum. All state is in-memory: votes are TTL-bounded
// and committees are recomputed on demand.
type VoteManager struct {
	logger         *slog.Logger
	eventBus       *event.EventBus
	stakeProvider  StakeDistributionProvider
	epochProvider  EpochProvider
	paramsProvider CommitteeParamsProvider
	registry       *VoterRegistry
	metrics        *voteManagerMetrics
	// now is the clock used for vote TTL expiry; tests may override it.
	now func() time.Time
	// voteTTL and maxVotes bound the vote store; tests may lower them.
	voteTTL  time.Duration
	maxVotes int

	mu       sync.Mutex
	running  bool
	stopping bool
	cancel   context.CancelFunc
	loopWg   sync.WaitGroup
	subs     []managerSubscription

	committees map[uint64]*epochEntry
	votesById  map[lcommon.LeiosVoteId]*storedVote
	voteLog    []*storedVote // ascending seq order
	nextSeq    uint64
	cursors    map[string]uint64 // connection key -> next seq to serve
	wakeCh     chan struct{}     // closed and replaced on every insert
	tallies    map[tallyKey]*ebTally

	votingPool []byte // local pool key hash; nil disables voting
	votingKey  *VoteSigningKey
}

type managerSubscription struct {
	eventType event.EventType
	id        event.EventSubscriberId
}

// NewVoteManager creates a vote manager.
func NewVoteManager(cfg VoteManagerConfig) (*VoteManager, error) {
	if cfg.EventBus == nil {
		return nil, errors.New("leios vote manager: nil event bus")
	}
	if cfg.StakeProvider == nil {
		return nil, errors.New("leios vote manager: nil stake provider")
	}
	if cfg.EpochProvider == nil {
		return nil, errors.New("leios vote manager: nil epoch provider")
	}
	if cfg.ParamsProvider == nil {
		return nil, errors.New("leios vote manager: nil params provider")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	registry := cfg.Registry
	if registry == nil {
		var err error
		registry, err = NewVoterRegistry(nil)
		if err != nil {
			return nil, err
		}
	}
	m := &VoteManager{
		logger:         logger.With("component", "leios"),
		eventBus:       cfg.EventBus,
		stakeProvider:  cfg.StakeProvider,
		epochProvider:  cfg.EpochProvider,
		paramsProvider: cfg.ParamsProvider,
		registry:       registry,
		now:            time.Now,
		voteTTL:        voteStoreTTL,
		maxVotes:       voteStoreMaxEntries,
		committees:     make(map[uint64]*epochEntry),
		votesById:      make(map[lcommon.LeiosVoteId]*storedVote),
		voteLog:        make([]*storedVote, 0),
		cursors:        make(map[string]uint64),
		wakeCh:         make(chan struct{}),
		tallies:        make(map[tallyKey]*ebTally),
	}
	if cfg.PromRegistry != nil {
		m.metrics = initVoteManagerMetrics(cfg.PromRegistry)
	}
	return m, nil
}

// Start subscribes to epoch transition and chain update events.
func (m *VoteManager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		return nil
	}
	if m.stopping {
		return errors.New(
			"leios vote manager: Stop in progress, cannot Start",
		)
	}
	if ctx == nil {
		return errors.New("leios vote manager: nil context")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf(
			"leios vote manager: parent context already done: %w",
			err,
		)
	}
	childCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.running = true
	m.wakeCh = make(chan struct{})

	epochSubId, epochCh := m.eventBus.Subscribe(
		event.EpochTransitionEventType,
	)
	chainSubId, chainCh := m.eventBus.Subscribe(chain.ChainUpdateEventType)
	m.subs = []managerSubscription{
		{eventType: event.EpochTransitionEventType, id: epochSubId},
		{eventType: chain.ChainUpdateEventType, id: chainSubId},
	}
	m.loopWg.Go(func() {
		m.eventLoop(childCtx, epochCh, chainCh)
		// If the loop exits because the parent context was cancelled
		// (not via Stop), reset running and unsubscribe so Start can
		// be called again without leaking a stale subscriber.
		m.mu.Lock()
		if !m.stopping {
			m.stopLocked()
		}
		m.mu.Unlock()
	})
	m.logger.Info("leios vote manager started")
	return nil
}

// stopLocked tears down running state. Callers must hold m.mu.
func (m *VoteManager) stopLocked() {
	if !m.running {
		return
	}
	m.running = false
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	for _, sub := range m.subs {
		m.eventBus.Unsubscribe(sub.eventType, sub.id)
	}
	m.subs = nil
	// Wake any blocked NextVotes callers so they observe the stop
	close(m.wakeCh)
}

// Stop stops the vote manager and unblocks any NextVotes waiters.
func (m *VoteManager) Stop() error {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return nil
	}
	m.stopping = true
	m.stopLocked()
	m.mu.Unlock()

	// Wait outside the lock so the event loop's cleanup path can
	// re-acquire it.
	m.loopWg.Wait()

	m.mu.Lock()
	m.stopping = false
	m.mu.Unlock()
	m.logger.Info("leios vote manager stopped")
	return nil
}

// EnableVoting enables local vote emission for the given pool using the
// given signing key. Votes are emitted for endorser blocks observed while
// the pool is a member of the epoch's committee.
func (m *VoteManager) EnableVoting(
	poolKeyHash lcommon.PoolKeyHash,
	key *VoteSigningKey,
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.votingPool = slices.Clone(poolKeyHash[:])
	m.votingKey = key
}

// CommitteeForEpoch returns the memoized voting committee for an epoch,
// computing it from the stake snapshot on first use.
func (m *VoteManager) CommitteeForEpoch(epoch uint64) (*Committee, error) {
	committee, _, err := m.committeeAndParamsForEpoch(epoch)
	return committee, err
}

// committeeAndParamsForEpoch returns the committee and quorum threshold
// for an epoch, computing and memoizing them on first use. Failures
// (snapshot unavailable, invalid parameters) are not memoized so later
// calls can recover.
func (m *VoteManager) committeeAndParamsForEpoch(
	epoch uint64,
) (*Committee, *big.Rat, error) {
	m.mu.Lock()
	if entry, ok := m.committees[epoch]; ok {
		m.mu.Unlock()
		return entry.committee, entry.tau, nil
	}
	m.mu.Unlock()

	// Compute outside the lock: stake lookup hits the database
	sigmaC, tau, err := m.paramsProvider.LeiosCommitteeParameters()
	if err != nil {
		return nil, nil, fmt.Errorf(
			"leios committee parameters: %w",
			err,
		)
	}
	snapshotEpoch := CommitteeSnapshotEpoch(epoch)
	poolStakes, totalActiveStake, err := m.stakeProvider.GetStakeDistribution(
		snapshotEpoch,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"stake distribution for snapshot epoch %d: %w",
			snapshotEpoch,
			err,
		)
	}
	committee, err := ComputeCommittee(
		epoch,
		snapshotEpoch,
		poolStakes,
		totalActiveStake,
		sigmaC,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"compute committee for epoch %d: %w",
			epoch,
			err,
		)
	}

	m.mu.Lock()
	if entry, ok := m.committees[epoch]; ok {
		// Another caller computed it concurrently; both results are
		// deterministic and identical, keep the first.
		m.mu.Unlock()
		return entry.committee, entry.tau, nil
	}
	m.committees[epoch] = &epochEntry{committee: committee, tau: tau}
	m.mu.Unlock()

	if m.metrics != nil {
		m.metrics.committeeSize.Set(float64(committee.Size()))
	}
	m.logger.Info(
		"computed leios voting committee",
		"epoch", epoch,
		"snapshot_epoch", snapshotEpoch,
		"members", committee.Size(),
		"committee_stake", committee.CommitteeStake,
		"total_active_stake", committee.TotalActiveStake,
	)
	return committee, tau, nil
}

// rejectVote logs and counts a dropped vote.
func (m *VoteManager) rejectVote(
	reason string,
	vote lcommon.LeiosVote,
	err error,
) {
	if m.metrics != nil {
		m.metrics.votesRejectedTotal.WithLabelValues(reason).Inc()
	}
	m.logger.Debug(
		"dropping leios vote",
		"reason", reason,
		"slot", vote.SlotNo,
		"voter_id", vote.VoterId,
		"endorser_block_hash", vote.EndorserBlockHash.String(),
		"error", err,
	)
}

// HandleVote validates and stores a vote received from a peer connection.
// Invalid votes are logged and dropped without error so a single bad vote
// does not tear down the peer connection.
func (m *VoteManager) HandleVote(
	connKey string,
	vote lcommon.LeiosVote,
) error {
	if m.metrics != nil {
		m.metrics.votesReceivedTotal.Inc()
	}
	if err := vote.Validate(); err != nil {
		m.rejectVote("structural", vote, err)
		return nil
	}
	epoch, err := m.epochProvider.EpochForSlot(vote.SlotNo)
	if err != nil {
		m.rejectVote("epoch", vote, err)
		return nil
	}
	committee, tau, err := m.committeeAndParamsForEpoch(epoch)
	if err != nil {
		m.rejectVote("committee", vote, err)
		return nil
	}
	member, ok := committee.Member(vote.VoterId)
	if !ok {
		m.rejectVote(
			"membership",
			vote,
			fmt.Errorf(
				"voter id %d out of range for committee size %d",
				vote.VoterId,
				committee.Size(),
			),
		)
		return nil
	}
	verified := false
	if pub, ok := m.registry.PublicKeyFor(member.PoolKeyHash); ok {
		msg := VoteMessageBytes(vote.SlotNo, vote.EndorserBlockHash)
		if err := VerifyVoteSignature(
			pub,
			msg,
			vote.VoteSignature,
		); err != nil {
			m.rejectVote("signature", vote, err)
			return nil
		}
		verified = true
	} else {
		// Lenient mode pending CIP-0164 key registration: the vote
		// counts toward observed stake but cannot be verified or
		// aggregated into a certificate.
		m.logger.Debug(
			"no registered voting key for leios voter, skipping signature verification",
			"slot", vote.SlotNo,
			"voter_id", vote.VoterId,
		)
	}
	m.insertVote(connKey, vote, epoch, committee, member, verified, tau)
	return nil
}

// insertVote stores a validated vote, updates the endorser block tally,
// and publishes a quorum event when verified stake crosses the threshold.
func (m *VoteManager) insertVote(
	originConn string,
	vote lcommon.LeiosVote,
	epoch uint64,
	committee *Committee,
	member CommitteeMember,
	verified bool,
	tau *big.Rat,
) {
	raw, err := vote.MarshalCBOR()
	if err != nil {
		m.rejectVote("encoding", vote, err)
		return
	}
	voteId := lcommon.LeiosVoteId{
		SlotNo:  vote.SlotNo,
		VoterId: vote.VoterId,
	}
	now := m.now()

	m.mu.Lock()
	if existing, ok := m.votesById[voteId]; ok {
		m.mu.Unlock()
		if existing.vote.EndorserBlockHash == vote.EndorserBlockHash {
			// Identical resubmission
			return
		}
		// Equivocation: same voter and slot, different endorser
		// block. The first vote wins.
		if m.metrics != nil {
			m.metrics.votesEquivocationTotal.Inc()
		}
		m.logger.Warn(
			"dropping equivocating leios vote",
			"slot", vote.SlotNo,
			"voter_id", vote.VoterId,
			"kept_endorser_block_hash", existing.vote.EndorserBlockHash.String(),
			"dropped_endorser_block_hash", vote.EndorserBlockHash.String(),
		)
		return
	}
	m.pruneExpiredLocked(now)
	stored := &storedVote{
		vote:       vote,
		raw:        raw,
		originConn: originConn,
		verified:   verified,
		seq:        m.nextSeq,
		epoch:      epoch,
		insertedAt: now,
	}
	m.nextSeq++
	m.votesById[voteId] = stored
	m.voteLog = append(m.voteLog, stored)
	m.enforceSizeLocked()

	key := tallyKey{slotNo: vote.SlotNo, ebHash: vote.EndorserBlockHash}
	tally, ok := m.tallies[key]
	if !ok {
		tally = &ebTally{epoch: epoch}
		m.tallies[key] = tally
	}
	tally.observedStake += member.Stake
	if verified {
		tally.verifiedStake += member.Stake
		tally.verifiedVotes = append(tally.verifiedVotes, VerifiedVote{
			VoterId:   vote.VoterId,
			Signature: vote.VoteSignature,
		})
	}
	tally.lastUpdated = now
	quorumEvt := m.evaluateQuorumLocked(key, tally, committee, tau)

	// Wake blocked NextVotes callers
	if m.running {
		close(m.wakeCh)
		m.wakeCh = make(chan struct{})
	}
	m.mu.Unlock()

	if quorumEvt != nil {
		if m.metrics != nil {
			m.metrics.ebQuorumReachedTotal.Inc()
			m.metrics.certificatesBuiltTotal.Inc()
		}
		m.logger.Info(
			"leios endorser block reached stake quorum",
			"slot", quorumEvt.SlotNo,
			"endorser_block_hash", quorumEvt.EndorserBlockHash.String(),
			"verified_stake", quorumEvt.VerifiedStake,
			"observed_stake", quorumEvt.ObservedStake,
			"total_active_stake", quorumEvt.TotalActiveStake,
		)
		m.eventBus.Publish(
			EbQuorumEventType,
			event.NewEvent(EbQuorumEventType, *quorumEvt),
		)
	}
}

// evaluateQuorumLocked checks the tally against the quorum threshold and
// builds a certificate from verified votes when it is met. Callers must
// hold m.mu; the returned event is published after the lock is released.
func (m *VoteManager) evaluateQuorumLocked(
	key tallyKey,
	tally *ebTally,
	committee *Committee,
	tau *big.Rat,
) *EbQuorumEvent {
	if tally.certBuilt {
		return nil
	}
	verifiedMet, err := MeetsStakeQuorum(
		tally.verifiedStake,
		committee.TotalActiveStake,
		tau,
	)
	if err != nil {
		m.logger.Error(
			"leios stake quorum evaluation failed",
			"slot", key.slotNo,
			"error", err,
		)
		return nil
	}
	if !verifiedMet {
		// Visibility for lenient mode: quorum may be observed without
		// enough verified stake to certify.
		if !tally.observedQuorumLogged {
			observedMet, err := MeetsStakeQuorum(
				tally.observedStake,
				committee.TotalActiveStake,
				tau,
			)
			if err == nil && observedMet {
				tally.observedQuorumLogged = true
				m.logger.Info(
					"leios stake quorum observed but not certifiable: unverified voter signatures",
					"slot", key.slotNo,
					"endorser_block_hash", key.ebHash.String(),
					"observed_stake", tally.observedStake,
					"verified_stake", tally.verifiedStake,
				)
			}
		}
		return nil
	}
	cert, err := BuildEbCertificate(
		key.slotNo,
		key.ebHash,
		committee,
		tally.verifiedVotes,
	)
	if err != nil {
		m.logger.Error(
			"failed to build leios EB certificate",
			"slot", key.slotNo,
			"endorser_block_hash", key.ebHash.String(),
			"error", err,
		)
		return nil
	}
	tally.certBuilt = true
	return &EbQuorumEvent{
		SlotNo:            key.slotNo,
		EndorserBlockHash: key.ebHash,
		Epoch:             tally.epoch,
		Certificate:       cert,
		VerifiedStake:     tally.verifiedStake,
		ObservedStake:     tally.observedStake,
		TotalActiveStake:  committee.TotalActiveStake,
	}
}

// pruneExpiredLocked drops votes and tallies older than the TTL. Callers
// must hold m.mu.
func (m *VoteManager) pruneExpiredLocked(now time.Time) {
	cutoff := now.Add(-m.voteTTL)
	m.filterVotesLocked(func(sv *storedVote) bool {
		return !sv.insertedAt.Before(cutoff)
	})
	for key, tally := range m.tallies {
		if tally.lastUpdated.Before(cutoff) {
			delete(m.tallies, key)
		}
	}
}

// enforceSizeLocked evicts the oldest votes beyond the store bound.
// Callers must hold m.mu.
func (m *VoteManager) enforceSizeLocked() {
	excess := len(m.voteLog) - m.maxVotes
	if excess <= 0 {
		return
	}
	// voteLog is in ascending seq (and so insertion) order
	for _, sv := range m.voteLog[:excess] {
		delete(m.votesById, lcommon.LeiosVoteId{
			SlotNo:  sv.vote.SlotNo,
			VoterId: sv.vote.VoterId,
		})
	}
	m.voteLog = slices.Delete(m.voteLog, 0, excess)
}

// filterVotesLocked retains only votes matching keep, removing the rest
// from both the log and the id index. Callers must hold m.mu.
func (m *VoteManager) filterVotesLocked(keep func(*storedVote) bool) {
	kept := m.voteLog[:0]
	for _, sv := range m.voteLog {
		if keep(sv) {
			kept = append(kept, sv)
			continue
		}
		delete(m.votesById, lcommon.LeiosVoteId{
			SlotNo:  sv.vote.SlotNo,
			VoterId: sv.vote.VoterId,
		})
	}
	clear(m.voteLog[len(kept):])
	m.voteLog = kept
}

// NextVotes blocks until count votes not originating from connKey are
// available and returns exactly count votes, tracking a per-connection
// cursor so each vote is served at most once per connection. It returns
// an error when done is closed or the manager stops.
func (m *VoteManager) NextVotes(
	done <-chan struct{},
	connKey string,
	count uint64,
) ([]lcommon.LeiosVote, error) {
	if count == 0 {
		return nil, errors.New("leios vote request for zero votes")
	}
	collected := make([]lcommon.LeiosVote, 0, count)
	for {
		m.mu.Lock()
		if !m.running {
			m.mu.Unlock()
			return nil, ErrVoteManagerStopped
		}
		m.pruneExpiredLocked(m.now())
		cursor := m.cursors[connKey]
		startIdx := sort.Search(len(m.voteLog), func(i int) bool {
			return m.voteLog[i].seq >= cursor
		})
		for _, sv := range m.voteLog[startIdx:] {
			cursor = sv.seq + 1
			if sv.originConn == connKey {
				continue
			}
			collected = append(collected, sv.vote)
			if uint64(len(collected)) == count {
				break
			}
		}
		m.cursors[connKey] = cursor
		if uint64(len(collected)) == count {
			m.mu.Unlock()
			return collected, nil
		}
		wake := m.wakeCh
		m.mu.Unlock()
		select {
		case <-wake:
		case <-done:
			return nil, errors.New("leios vote request aborted")
		}
	}
}

// VotesByIds returns the raw CBOR for the requested votes. Unknown ids
// are omitted.
func (m *VoteManager) VotesByIds(
	ids []lcommon.LeiosVoteId,
) []cbor.RawMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	ret := make([]cbor.RawMessage, 0, len(ids))
	for _, id := range ids {
		if sv, ok := m.votesById[id]; ok {
			ret = append(ret, slices.Clone(sv.raw))
		}
	}
	return ret
}

// HandleEndorserBlock emits a local vote for an endorser block when
// voting is enabled and the local pool is a member of the slot epoch's
// committee.
func (m *VoteManager) HandleEndorserBlock(
	slot uint64,
	ebHash lcommon.Blake2b256,
) {
	m.mu.Lock()
	votingPool := m.votingPool
	votingKey := m.votingKey
	m.mu.Unlock()
	if len(votingPool) == 0 || votingKey == nil {
		return
	}
	epoch, err := m.epochProvider.EpochForSlot(slot)
	if err != nil {
		m.logger.Debug(
			"cannot resolve epoch for endorser block slot",
			"slot", slot,
			"error", err,
		)
		return
	}
	committee, tau, err := m.committeeAndParamsForEpoch(epoch)
	if err != nil {
		m.logger.Debug(
			"leios committee unavailable, not voting",
			"slot", slot,
			"epoch", epoch,
			"error", err,
		)
		return
	}
	voterId, ok := committee.VoterIdFor(votingPool)
	if !ok {
		m.logger.Debug(
			"local pool is not a leios committee member, not voting",
			"slot", slot,
			"epoch", epoch,
		)
		return
	}
	member, ok := committee.Member(voterId)
	if !ok {
		return
	}
	msg := VoteMessageBytes(slot, ebHash)
	sig, err := SignVote(votingKey, msg)
	if err != nil {
		m.logger.Error(
			"failed to sign leios vote",
			"slot", slot,
			"voter_id", voterId,
			"error", err,
		)
		return
	}
	vote := lcommon.LeiosVote{
		SlotNo:            slot,
		EndorserBlockHash: ebHash,
		VoterId:           voterId,
		VoteSignature:     sig,
	}
	if m.metrics != nil {
		m.metrics.votesReceivedTotal.Inc()
	}
	m.logger.Info(
		"emitting leios vote",
		"slot", slot,
		"voter_id", voterId,
		"endorser_block_hash", ebHash.String(),
	)
	m.insertVote("", vote, epoch, committee, member, true, tau)
}

// RemoveConnection drops the vote-serving cursor for a closed connection.
func (m *VoteManager) RemoveConnection(connKey string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.cursors, connKey)
}

// eventLoop processes epoch transition and chain update events until the
// context is cancelled or both subscriptions close.
func (m *VoteManager) eventLoop(
	ctx context.Context,
	epochCh <-chan event.Event,
	chainCh <-chan event.Event,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-epochCh:
			if !ok {
				return
			}
			if data, ok := evt.Data.(event.EpochTransitionEvent); ok {
				m.handleEpochTransition(data)
			}
		case evt, ok := <-chainCh:
			if !ok {
				return
			}
			if data, ok := evt.Data.(chain.ChainRollbackEvent); ok {
				m.handleRollback(data)
			}
		}
	}
}

// handleEpochTransition prunes committees, votes, and tallies older than
// the previous epoch. The previous epoch is retained so in-flight votes
// near the boundary remain servable.
func (m *VoteManager) handleEpochTransition(
	evt event.EpochTransitionEvent,
) {
	var keepFrom uint64
	if evt.NewEpoch >= 1 {
		keepFrom = evt.NewEpoch - 1
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for epoch := range m.committees {
		if epoch < keepFrom {
			delete(m.committees, epoch)
		}
	}
	m.filterVotesLocked(func(sv *storedVote) bool {
		return sv.epoch >= keepFrom
	})
	for key, tally := range m.tallies {
		if tally.epoch < keepFrom {
			delete(m.tallies, key)
		}
	}
	m.logger.Debug(
		"pruned leios vote state at epoch transition",
		"new_epoch", evt.NewEpoch,
		"retained_votes", len(m.voteLog),
	)
}

// handleRollback drops votes and tallies past the rollback point and
// clears the committee memo: a rollback across an epoch boundary can
// change the stake snapshots committees derive from, and recomputation is
// cheap.
func (m *VoteManager) handleRollback(evt chain.ChainRollbackEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.filterVotesLocked(func(sv *storedVote) bool {
		return sv.vote.SlotNo <= evt.Point.Slot
	})
	for key := range m.tallies {
		if key.slotNo > evt.Point.Slot {
			delete(m.tallies, key)
		}
	}
	m.committees = make(map[uint64]*epochEntry)
	m.logger.Debug(
		"pruned leios vote state after rollback",
		"rollback_slot", evt.Point.Slot,
		"retained_votes", len(m.voteLog),
	)
}
