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
	"encoding/hex"
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
	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// voteStoreTTL bounds how long collected votes are retained,
	// mirroring the endorser block cache in the ouroboros component.
	voteStoreTTL = 10 * time.Minute
	// voteStoreMaxEntries bounds the serving store size. A stored vote
	// is ~100 bytes of fields plus ~120 bytes of raw CBOR and map/log
	// overhead, so this is roughly a 2-3 MiB ceiling.
	voteStoreMaxEntries = 8192
	// voteRecordMaxEntries bounds the dedup record ledger. Records are
	// ~100 bytes each (16-byte map key, 64-byte value, bucket
	// overhead), so this is roughly a 3.3 MiB ceiling. Records must
	// outlive serving entries (votesById is a subset of voteRecords),
	// hence the multiple of the serving store bound. The cap is an
	// admission bound for unverified peer votes only -- the kind that
	// needs no valid signature while key registration is partial and
	// whose only effect is observed-stake visibility. When the ledger
	// is full those are rejected rather than evicting old records,
	// since evicting a record would let a re-received vote re-count
	// its stake into a still-live tally. Verified and locally emitted
	// votes bypass the cap: they are unforgeable, and dedup bounds
	// them to one record per (slot, registered voter) inside the slot
	// window.
	voteRecordMaxEntries = 4 * voteStoreMaxEntries
	// slotWindowFutureTolerance bounds how far ahead of the current (or
	// tip) slot a vote is accepted, allowing for clock skew and diffusion
	// delay. The past bound is the pipeline's VoteWindowSlots (the offset
	// after an EB's produce slot at which voting closes), supplied via
	// VoteManagerConfig so the vote manager and pipeline admit votes over
	// the same window. Both keep the forgeable vote-id space (window x
	// committee size) small.
	slotWindowFutureTolerance = 60
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

// LeiosKeyProvider supplies each active pool's raw (unverified) registered
// Dijkstra/Leios BLS key, keyed by lowercase-hex pool key hash.
// Implementations must not verify proof of possession themselves --
// VoteManager does that once per epoch and caches only the keys that pass,
// so a pool with no usable key is simply absent from the resolved map (a
// "keyless" committee seat: it still occupies a stake-weighted voter id,
// but can never contribute a verified signature).
//
// The lookup is frozen to snapshotEpoch: implementations must return the key
// stored with the same historical stake snapshot used to select the committee.
// A key registered or rotated after that boundary is not visible until a later
// snapshot captures it.
//
// poolKeyHashes names exactly the epoch's committee members (the
// stake-coverage prefix ComputeCommittee already selected from the
// snapshot committeeAndParamsForEpoch fetched), not every pool in the
// stake distribution: resolveVoterKey never looks up a non-member, so
// verifying keys for pools that cannot make the committee only spends
// pairing work on results that are never read. Implementations must look
// up keys for exactly the given set rather than re-fetching the stake
// distribution themselves -- doing so would duplicate a DB round
// trip on every new-epoch committee computation.
type LeiosKeyProvider interface {
	GetLeiosKeys(
		snapshotEpoch uint64,
		poolKeyHashes []string,
	) (map[string]*lcommon.LeiosKey, error)
}

// EpochProvider supplies epoch information from the ledger.
type EpochProvider interface {
	CurrentEpoch() uint64
	EpochForSlot(slot uint64) (uint64, error)
}

// SlotProvider supplies the current slot for the vote acceptance window:
// the wall-clock slot when the slot clock is available, otherwise the
// chain tip slot. *ledger.LedgerState satisfies this directly via
// CurrentOrTipSlot. A node that is far behind the network sees only its
// tip slot and rejects live votes as too far in the future; that is
// acceptable, since votes are unusable to a syncing node and expire from
// peers' stores before it catches up.
type SlotProvider interface {
	CurrentOrTipSlot() uint64
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
	// KeyProvider supplies on-chain registered BLS keys per epoch. When
	// non-nil it is the authoritative key source: a key absent from its
	// PoP-verified result is a keyless seat and Registry is ignored. Nil
	// explicitly selects the Registry-only private test/devnet seam.
	KeyProvider LeiosKeyProvider
	// SlotProvider enables the vote slot acceptance window. When nil
	// the window check is disabled and votes for any resolvable slot
	// are accepted.
	SlotProvider SlotProvider
	// Registry is a private test/devnet key source used only when KeyProvider
	// is nil. Production composition always supplies KeyProvider.
	Registry     *VoterRegistry
	PromRegistry prometheus.Registerer
	// VoteWindowSlots is the offset after an EB's produce slot at which
	// voting closes: a vote whose slot is this many slots or more behind
	// the current (or tip) slot is rejected. It is the pipeline's
	// VoteWindowSlots, passed here so the vote manager and pipeline admit
	// votes over the same window. Zero falls back to
	// DefaultPipelineTiming().VoteWindowSlots.
	VoteWindowSlots uint64
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

// voteRecord is the authoritative dedup and tally-accounting entry for
// one accepted (slot, voter) vote id. Records are decoupled from the
// size-capped serving store (votesById/voteLog): serving entries may be
// evicted to bound raw-CBOR memory, but the record keeps the vote's
// stake from being re-counted into a still-live tally and keeps
// first-wins equivocation detection durable. Invariants:
//   - votesById is a subset of voteRecords (as id sets): records are
//     pruned by the same-or-looser predicates in the same critical
//     sections, and size eviction only shrinks the serving store.
//   - a record's tally always has lastUpdated >= the record's
//     insertedAt, so TTL pruning can drop a record only once its tally
//     is gone (see pruneExpiredLocked).
//   - every tally is created together with a record, so
//     len(tallies) <= len(voteRecords) and voteRecordMaxEntries bounds
//     both maps.
type voteRecord struct {
	ebHash           lcommon.Blake2b256
	announcingRbHash lcommon.Blake2b256
	epoch            uint64
	insertedAt       time.Time
}

// tallyKey identifies the vote tally for one endorser block.
type tallyKey struct {
	slotNo           uint64
	ebHash           lcommon.Blake2b256
	announcingRbHash lcommon.Blake2b256
}

type announcementRecord struct {
	slot   uint64
	epoch  uint64
	ebHash lcommon.Blake2b256
	seenAt time.Time
}

type pendingPrototypeVote struct {
	connKey string
	vote    lcommon.LeiosPrototypeVote
	seenAt  time.Time
}

type acquiredEbRecord struct {
	slot   uint64
	epoch  uint64
	seenAt time.Time
}

// maxPendingPrototypeCandidatesPerVoter bounds alternate signatures retained
// while an announcing ranking block is unknown. More than one candidate is
// required because signatures cannot be verified until the announcement maps
// the vote to an epoch committee; retaining only the first lets a forged vote
// suppress the real voter's later valid vote.
const maxPendingPrototypeCandidatesPerVoter = 4

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

// epochEntry memoizes the committee, quorum threshold, and resolved,
// PoP-verified on-chain voter keys for an epoch. onChainKeys holds only
// keys that passed VerifyLeiosKeyProofOfPossession; a committee member
// absent from it has no usable on-chain key for the epoch and is keyless.
type epochEntry struct {
	committee   *Committee
	tau         *big.Rat
	onChainKeys map[string]*bls12381.G2Affine
}

// VoteManager collects, validates, serves, and emits Leios votes. It
// memoizes per-epoch voting committees from stake snapshots, tallies vote
// stake per endorser block, and builds a certificate when verified votes
// meet the stake quorum. All state is in-memory: raw votes live in a
// TTL- and size-bounded serving store, dedup/tally accounting lives in a
// separate record ledger pruned in lockstep with tallies (see
// voteRecord), and committees are recomputed on demand.
type VoteManager struct {
	logger         *slog.Logger
	eventBus       *event.EventBus
	stakeProvider  StakeDistributionProvider
	epochProvider  EpochProvider
	paramsProvider CommitteeParamsProvider
	keyProvider    LeiosKeyProvider // nil explicitly enables Registry-only mode
	slotProvider   SlotProvider     // nil disables the slot window check
	// voteWindowSlots is the past bound of the vote acceptance window: a
	// vote whose slot is this many slots or more behind the current slot
	// is rejected. It is the pipeline's VoteWindowSlots so the two
	// components admit votes over the same window.
	voteWindowSlots uint64
	registry        *VoterRegistry
	metrics         *voteManagerMetrics
	// now is the clock used for vote TTL expiry; tests may override it.
	now func() time.Time
	// voteTTL, maxVotes, and maxRecords bound the vote stores; tests
	// may lower them.
	voteTTL    time.Duration
	maxVotes   int
	maxRecords int

	mu sync.Mutex
	// prototypeEmissionMu linearizes local vote commit/publication with
	// rollback pruning. This avoids holding mu while publishing an EventBus
	// event, whose subscribers are outside the manager's lock hierarchy.
	prototypeEmissionMu sync.Mutex
	running             bool
	stopping            bool
	cancel              context.CancelFunc
	loopWg              sync.WaitGroup
	subs                []managerSubscription

	committees         map[uint64]*epochEntry
	votesById          map[lcommon.LeiosVoteId]*storedVote
	voteLog            []*storedVote // ascending seq order
	voteRecords        map[lcommon.LeiosVoteId]voteRecord
	nextSeq            uint64
	cursors            map[string]uint64 // connection key -> next seq to serve
	wakeCh             chan struct{}     // closed and replaced on every insert
	tallies            map[tallyKey]*ebTally
	announcements      map[lcommon.Blake2b256]announcementRecord
	acquiredEbs        map[lcommon.Blake2b256]acquiredEbRecord
	votedAnnouncements map[lcommon.Blake2b256]struct{}
	pendingVotes       map[lcommon.Blake2b256]map[uint64][]pendingPrototypeVote
	// pendingVoteCount is the sum of all pending candidate slices;
	// pendingVoteCountByConn partitions that same total by origin connection.
	// Every admission and removal must update both counters together.
	pendingVoteCount       int
	pendingVoteCountByConn map[string]int

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
	voteWindowSlots := cfg.VoteWindowSlots
	if voteWindowSlots == 0 {
		voteWindowSlots = DefaultPipelineTiming().VoteWindowSlots
	}
	m := &VoteManager{
		logger:             logger.With("component", "leios"),
		eventBus:           cfg.EventBus,
		stakeProvider:      cfg.StakeProvider,
		epochProvider:      cfg.EpochProvider,
		paramsProvider:     cfg.ParamsProvider,
		keyProvider:        cfg.KeyProvider,
		slotProvider:       cfg.SlotProvider,
		voteWindowSlots:    voteWindowSlots,
		registry:           registry,
		now:                time.Now,
		voteTTL:            voteStoreTTL,
		maxVotes:           voteStoreMaxEntries,
		maxRecords:         voteRecordMaxEntries,
		committees:         make(map[uint64]*epochEntry),
		votesById:          make(map[lcommon.LeiosVoteId]*storedVote),
		voteLog:            make([]*storedVote, 0),
		voteRecords:        make(map[lcommon.LeiosVoteId]voteRecord),
		cursors:            make(map[string]uint64),
		wakeCh:             make(chan struct{}),
		tallies:            make(map[tallyKey]*ebTally),
		announcements:      make(map[lcommon.Blake2b256]announcementRecord),
		acquiredEbs:        make(map[lcommon.Blake2b256]acquiredEbRecord),
		votedAnnouncements: make(map[lcommon.Blake2b256]struct{}),
		pendingVotes: make(
			map[lcommon.Blake2b256]map[uint64][]pendingPrototypeVote,
		),
		pendingVoteCountByConn: make(map[string]int),
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
// the pool is a member of the epoch's committee; enabling voting itself,
// however, does not require current committee membership (see
// resolveOnChainKeyForPool) -- a pool not selected this epoch must still
// be able to get ready for an epoch that does select it.
func (m *VoteManager) EnableVoting(
	poolKeyHash lcommon.PoolKeyHash,
	key *VoteSigningKey,
) error {
	if key == nil {
		return errors.New("nil leios vote signing key")
	}
	// A resolvable on-chain key for this pool is authoritative: if it
	// disagrees with key, enabling voting anyway would succeed here but
	// emit nothing, since resolveVoterKey (checked by every emission)
	// prefers the same on-chain key and would keep rejecting it. Reject
	// now instead of failing silently later -- this also catches an
	// epoch transition landing a key rotation between a caller's
	// ValidateVotingKey check and this call.
	//
	// Registry is deliberately consulted only in the explicit private
	// harness mode where no KeyProvider is wired. With a provider present,
	// absence is authoritative: auto-registering key here would let this
	// node emit a vote that reference-compatible peers reject.
	onChain, onChainKnown, err := m.resolveOnChainKeyForPool(poolKeyHash[:])
	if err != nil {
		return fmt.Errorf(
			"resolve on-chain leios key for pool %s: %w",
			poolKeyHash.String(),
			err,
		)
	}
	if m.keyProvider != nil {
		if !onChainKnown {
			return fmt.Errorf(
				"no usable on-chain leios voting key for pool %s",
				poolKeyHash.String(),
			)
		}
		if !onChain.Equal(key.PublicKey()) {
			return fmt.Errorf(
				"configured leios voting key does not match the on-chain registered key for pool %s",
				poolKeyHash.String(),
			)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.keyProvider != nil {
		m.logger.Debug(
			"leios voting key matches the on-chain registration, skipping static registry",
			"pool", poolKeyHash.String(),
		)
	} else if err := m.registry.RegisterPublicKey(poolKeyHash[:], key.PublicKey()); err != nil {
		return fmt.Errorf("register local leios voting key: %w", err)
	}
	m.votingPool = slices.Clone(poolKeyHash[:])
	m.votingKey = key
	return nil
}

// ValidateVotingKey verifies that an operator-supplied voting key matches a
// resolvable public key for the pool. A configured KeyProvider is authoritative:
// its PoP-verified on-chain registration must exist and match. Registry is
// consulted only in the explicit private test/devnet mode where KeyProvider is
// nil.
//
// Deliberately not scoped to the current epoch's committee: resolution
// goes through resolveOnChainKeyForPool, not the epoch-cached (and, since
// this pool may not be a committee member today, potentially empty)
// resolveVoterKey path -- a pool outside this epoch's stake-coverage
// selection must still be able to validate and enable its key ahead of an
// epoch that does select it.
func (m *VoteManager) ValidateVotingKey(
	poolKeyHash lcommon.PoolKeyHash,
	key *VoteSigningKey,
) error {
	if key == nil {
		return errors.New("nil leios vote signing key")
	}
	registered, ok, err := m.resolveOnChainKeyForPool(poolKeyHash[:])
	if err != nil {
		return fmt.Errorf(
			"resolve on-chain leios key for pool %x: %w",
			poolKeyHash,
			err,
		)
	}
	if !ok && m.keyProvider == nil {
		registered, ok = m.registry.PublicKeyFor(poolKeyHash[:])
	}
	if !ok {
		if m.keyProvider != nil {
			return fmt.Errorf(
				"no usable on-chain leios voting public key for pool %x",
				poolKeyHash,
			)
		}
		return fmt.Errorf(
			"no static leios voting public key for pool %x in private registry mode",
			poolKeyHash,
		)
	}
	if !registered.Equal(key.PublicKey()) {
		return fmt.Errorf(
			"configured leios voting key does not match the resolved public key for pool %x",
			poolKeyHash,
		)
	}
	return nil
}

// CommitteeForEpoch returns the memoized voting committee for an epoch,
// computing it from the stake snapshot on first use.
func (m *VoteManager) CommitteeForEpoch(epoch uint64) (*Committee, error) {
	entry, err := m.committeeAndParamsForEpoch(epoch)
	if err != nil {
		return nil, err
	}
	return entry.committee, nil
}

// committeeAndParamsForEpoch returns the memoized epochEntry (committee,
// quorum threshold, and resolved on-chain voter keys) for an epoch,
// computing it from the stake snapshot on first use. Failures (snapshot
// unavailable, invalid parameters) are not memoized so later calls can
// recover.
func (m *VoteManager) committeeAndParamsForEpoch(
	epoch uint64,
) (*epochEntry, error) {
	m.mu.Lock()
	if entry, ok := m.committees[epoch]; ok {
		m.mu.Unlock()
		return entry, nil
	}
	m.mu.Unlock()

	// Compute outside the lock: stake lookup hits the database
	sigmaC, tau, err := m.paramsProvider.LeiosCommitteeParameters()
	if err != nil {
		return nil, fmt.Errorf(
			"leios committee parameters: %w",
			err,
		)
	}
	snapshotEpoch := CommitteeSnapshotEpoch(epoch)
	poolStakes, totalActiveStake, err := m.stakeProvider.GetStakeDistribution(
		snapshotEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf(
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
		return nil, fmt.Errorf(
			"compute committee for epoch %d: %w",
			epoch,
			err,
		)
	}
	// Resolve keys only for committee members, not every pool in the stake
	// distribution: resolveVoterKey only ever looks up a member.PoolKeyHash,
	// and ComputeCommittee already trimmed poolStakes down to the
	// stake-coverage prefix that actually made the committee. Verifying a
	// proof of possession is a pairing operation (measured ~0.75ms/key on
	// this branch); at Cardano's pool counts, verifying every registered
	// pool instead of just the committee would burn seconds of pairing
	// work per epoch computation on results that could never be read back.
	poolKeyHashes := make([]string, 0, len(committee.Members))
	for _, member := range committee.Members {
		poolKeyHashes = append(
			poolKeyHashes,
			hex.EncodeToString(member.PoolKeyHash),
		)
	}
	// A failure here must not be cached: caching an empty onChainKeys map
	// for this epoch would make every seat keyless until the process
	// restarts or a rollback clears the memo, even after the underlying
	// store recovers from what may be a transient failure. Returning the
	// error instead means the next call retries from scratch.
	onChainKeys, err := m.resolveOnChainKeys(snapshotEpoch, poolKeyHashes)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	if entry, ok := m.committees[epoch]; ok {
		// Another caller computed it concurrently; both results are
		// deterministic and identical, keep the first.
		m.mu.Unlock()
		return entry, nil
	}
	entry := &epochEntry{
		committee:   committee,
		tau:         tau,
		onChainKeys: onChainKeys,
	}
	m.committees[epoch] = entry
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
	return entry, nil
}

// resolveOnChainKeys fetches raw registered Leios keys from keyProvider for
// a snapshot epoch and returns only those whose proof of possession
// verifies. A nil keyProvider (no ledger wired, e.g. in tests) yields an
// empty map with no error -- this is the explicit Registry-only private
// test/devnet mode. A provider error
// is returned rather than swallowed into an empty map: the caller must not
// cache an empty result for a transient failure, since that would make
// every seat keyless for the rest of the epoch even after the store
// recovers (see committeeAndParamsForEpoch).
func (m *VoteManager) resolveOnChainKeys(
	snapshotEpoch uint64,
	poolKeyHashes []string,
) (map[string]*bls12381.G2Affine, error) {
	verified := make(map[string]*bls12381.G2Affine)
	if m.keyProvider == nil {
		return verified, nil
	}
	raw, err := m.keyProvider.GetLeiosKeys(snapshotEpoch, poolKeyHashes)
	if err != nil {
		return nil, fmt.Errorf(
			"resolve on-chain leios keys for snapshot epoch %d: %w",
			snapshotEpoch,
			err,
		)
	}
	for poolHashHex, key := range raw {
		if err := VerifyLeiosKeyProofOfPossession(key); err != nil {
			m.logger.Warn(
				"registered leios key failed proof of possession, treating as absent",
				"pool",
				poolHashHex,
				"error",
				err,
			)
			continue
		}
		var pub bls12381.G2Affine
		if _, err := pub.SetBytes(key.PublicKey); err != nil {
			// Already validated by VerifyLeiosKeyProofOfPossession; this
			// cannot fail in practice, but skip defensively rather than
			// panic.
			continue
		}
		verified[poolHashHex] = &pub
	}
	return verified, nil
}

// resolveOnChainKeyForPool resolves and PoP-verifies a single pool's
// on-chain key, independent of committee membership. This is deliberately
// not committee-scoped like resolveOnChainKeys' epoch cache:
// ValidateVotingKey/EnableVoting must work for a pool that isn't a member
// of the *current* epoch's committee, since ComputeCommittee re-selects
// every epoch from that epoch's stake snapshot -- a pool outside today's
// selection can still be selected once its stake (or others') shifts, and
// an operator must be able to enable voting in advance of that rather than
// getting rejected today for a reason that has nothing to do with the
// validity of their key.
//
// A key-provider error is returned, not swallowed into "no on-chain key
// found": a transient lookup failure is not the same as a genuine absence,
// and both must block production voting rather than consulting Registry.
func (m *VoteManager) resolveOnChainKeyForPool(
	poolKeyHash []byte,
) (*bls12381.G2Affine, bool, error) {
	poolHashHex := hex.EncodeToString(poolKeyHash)
	snapshotEpoch := CommitteeSnapshotEpoch(m.epochProvider.CurrentEpoch())
	keys, err := m.resolveOnChainKeys(snapshotEpoch, []string{poolHashHex})
	if err != nil {
		return nil, false, err
	}
	pub, ok := keys[poolHashHex]
	return pub, ok, nil
}

// resolveVoterKey resolves a committee member's voting public key. With a
// KeyProvider, the epoch's PoP-verified on-chain result is authoritative and a
// missing key is a keyless seat. Registry is used only when KeyProvider is nil,
// the explicit private test/devnet seam.
func (m *VoteManager) resolveVoterKey(
	entry *epochEntry,
	poolKeyHash []byte,
) (*bls12381.G2Affine, bool) {
	if entry != nil && entry.onChainKeys != nil {
		if pub, ok := entry.onChainKeys[hex.EncodeToString(poolKeyHash)]; ok {
			return pub, true
		}
	}
	if m.keyProvider != nil {
		return nil, false
	}
	return m.registry.PublicKeyFor(poolKeyHash)
}

// slotWindowCheck reports whether a vote slot falls within the
// acceptance window around the current (or tip) slot, returning a
// descriptive error when it does not. A nil slot provider disables the
// check. The past bound is the pipeline's vote window (voteWindowSlots):
// once a vote's slot is that many slots behind the current slot, voting
// has closed for that EB, matching stageFor's StageVote boundary. The
// future bound allows for clock skew and diffusion delay.
func (m *VoteManager) slotWindowCheck(slot uint64) error {
	if m.slotProvider == nil {
		return nil
	}
	cur := m.slotProvider.CurrentOrTipSlot()
	if slot < cur && cur-slot >= m.voteWindowSlots {
		return fmt.Errorf(
			"vote slot %d is %d or more slots behind current slot %d (vote window closed)",
			slot,
			m.voteWindowSlots,
			cur,
		)
	}
	if slot > cur && slot-cur > slotWindowFutureTolerance {
		return fmt.Errorf(
			"vote slot %d is more than %d slots ahead of current slot %d",
			slot,
			uint64(slotWindowFutureTolerance),
			cur,
		)
	}
	return nil
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
	// Window the vote slot before any epoch or committee work:
	// EpochForSlot projects future slots indefinitely and committee
	// computation reaches the stake snapshot in the database, so
	// out-of-window slots must not get that far.
	if err := m.slotWindowCheck(vote.SlotNo); err != nil {
		m.rejectVote("slot_window", vote, err)
		return nil
	}
	epoch, err := m.epochProvider.EpochForSlot(vote.SlotNo)
	if err != nil {
		m.rejectVote("epoch", vote, err)
		return nil
	}
	entry, err := m.committeeAndParamsForEpoch(epoch)
	if err != nil {
		m.rejectVote("committee", vote, err)
		return nil
	}
	committee := entry.committee
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
	if pub, ok := m.resolveVoterKey(entry, member.PoolKeyHash); ok {
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
		// Keyless committee seat: the pool has no usable authoritative key,
		// so the vote counts toward observed
		// stake but cannot be verified or aggregated into a certificate.
		m.logger.Debug(
			"no registered voting key for leios voter, skipping signature verification",
			"slot", vote.SlotNo,
			"voter_id", vote.VoterId,
		)
	}
	m.insertVote(
		connKey,
		vote,
		epoch,
		committee,
		member,
		verified,
		entry.tau,
		lcommon.Blake2b256{},
	)
	return nil
}

// HandlePrototypeVote validates the current three-field prototype vote after
// resolving its announcing ranking block to the slot and EB identity.
func (m *VoteManager) HandlePrototypeVote(
	connKey string,
	vote lcommon.LeiosPrototypeVote,
) error {
	if m.metrics != nil {
		m.metrics.votesReceivedTotal.Inc()
	}
	if err := vote.Validate(); err != nil {
		if m.metrics != nil {
			m.metrics.votesRejectedTotal.WithLabelValues("structural").Inc()
		}
		return nil
	}
	m.mu.Lock()
	record, ok := m.announcements[vote.AnnouncingRbHash]
	if !ok {
		m.queuePrototypeVoteLocked(connKey, vote)
	}
	m.mu.Unlock()
	if !ok {
		m.logger.Debug(
			"queued leios vote pending announcing ranking block",
			"announcing_rb_hash", vote.AnnouncingRbHash.String(),
			"voter_id", vote.VoterId,
		)
		return nil
	}
	return m.handleResolvedPrototypeVote(connKey, vote, record)
}

func (m *VoteManager) handleResolvedPrototypeVote(
	connKey string,
	vote lcommon.LeiosPrototypeVote,
	record announcementRecord,
) error {
	if err := m.slotWindowCheck(record.slot); err != nil {
		m.rejectVote(
			"slot_window",
			lcommon.LeiosVote{SlotNo: record.slot, VoterId: vote.VoterId},
			err,
		)
		return nil
	}
	entry, err := m.committeeAndParamsForEpoch(record.epoch)
	if err != nil {
		m.rejectVote(
			"committee",
			lcommon.LeiosVote{SlotNo: record.slot, VoterId: vote.VoterId},
			err,
		)
		return nil
	}
	committee := entry.committee
	member, ok := committee.Member(vote.VoterId)
	if !ok {
		m.rejectVote(
			"membership",
			lcommon.LeiosVote{SlotNo: record.slot, VoterId: vote.VoterId},
			errors.New("voter id outside committee"),
		)
		return nil
	}
	verified := false
	// A member resolving to no key here is a keyless committee seat: its
	// stake still counts toward membership, but its vote can never be
	// verified or aggregated into a certificate.
	pub, _ := m.resolveVoterKey(entry, member.PoolKeyHash)
	if pub != nil {
		if err := VerifyVoteSignature(pub, PrototypeVoteMessageBytes(vote.AnnouncingRbHash), vote.VoteSignature); err != nil {
			m.rejectVote(
				"signature",
				lcommon.LeiosVote{SlotNo: record.slot, VoterId: vote.VoterId},
				err,
			)
			return nil
		}
		verified = true
	}
	resolved := lcommon.LeiosVote{
		SlotNo:            record.slot,
		EndorserBlockHash: record.ebHash,
		VoterId:           vote.VoterId,
		VoteSignature:     vote.VoteSignature,
	}
	inserted := m.insertVote(
		connKey,
		resolved,
		record.epoch,
		committee,
		member,
		verified,
		entry.tau,
		vote.AnnouncingRbHash,
	)
	// Re-diffuse a newly accepted peer vote the same way a locally emitted
	// one is diffused. insertVote's dedup/equivocation gate above means this
	// fires exactly once per distinct vote, so a relay forwards it to its
	// other peers instead of stopping it at the connection that delivered
	// it, without re-broadcasting a resubmission or an equivocation attempt.
	if inserted {
		m.eventBus.Publish(VoteReceivedEventType, event.NewEvent(
			VoteReceivedEventType,
			VoteReceivedEvent{Vote: vote, OriginConnKey: connKey},
		))
	}
	return nil
}

func (m *VoteManager) queuePrototypeVoteLocked(
	connKey string,
	vote lcommon.LeiosPrototypeVote,
) {
	now := m.now()
	m.prunePrototypeStateLocked(now)
	byVoter := m.pendingVotes[vote.AnnouncingRbHash]
	if byVoter == nil {
		byVoter = make(map[uint64][]pendingPrototypeVote)
		m.pendingVotes[vote.AnnouncingRbHash] = byVoter
	}
	candidates := byVoter[vote.VoterId]
	for _, candidate := range candidates {
		if slices.Equal(candidate.vote.VoteSignature, vote.VoteSignature) {
			return
		}
	}
	if len(candidates) >= maxPendingPrototypeCandidatesPerVoter {
		// Prefer recent alternatives over permanently reserving this voter id
		// for the first unverified arrivals. Verification is impossible until
		// the ranking block resolves the epoch committee.
		evictedConn := candidates[0].connKey
		candidates = candidates[1:]
		m.pendingVoteCount--
		m.decrementPendingConnectionLocked(evictedConn)
		if m.metrics != nil {
			m.metrics.votesRejectedTotal.WithLabelValues("pending_candidates").
				Inc()
		}
	}
	if m.pendingVoteCount >= m.maxRecords {
		mostRepresentedConn := ""
		mostRepresentedCount := 0
		for candidateConn, count := range m.pendingVoteCountByConn {
			if count > mostRepresentedCount {
				mostRepresentedConn = candidateConn
				mostRepresentedCount = count
			}
		}
		// At capacity, make room when the incoming connection is less
		// represented than the largest incumbent. This lets the queue use its
		// full capacity with one healthy peer while preventing that peer from
		// excluding later peers entirely.
		if mostRepresentedCount > m.pendingVoteCountByConn[connKey] {
			if !m.evictOldestPendingForConnectionLocked(mostRepresentedConn) {
				if m.metrics != nil {
					m.metrics.votesRejectedTotal.WithLabelValues("pending_capacity").
						Inc()
				}
				return
			}
			byVoter = m.pendingVotes[vote.AnnouncingRbHash]
			if byVoter == nil {
				byVoter = make(map[uint64][]pendingPrototypeVote)
				m.pendingVotes[vote.AnnouncingRbHash] = byVoter
			}
			candidates = byVoter[vote.VoterId]
		} else {
			if m.metrics != nil {
				m.metrics.votesRejectedTotal.WithLabelValues("pending_capacity").
					Inc()
			}
			return
		}
	}
	copyVote := vote
	copyVote.VoteSignature = slices.Clone(vote.VoteSignature)
	byVoter[vote.VoterId] = append(candidates, pendingPrototypeVote{
		connKey: connKey,
		vote:    copyVote,
		seenAt:  now,
	})
	m.pendingVoteCount++
	m.pendingVoteCountByConn[connKey]++
}

func (m *VoteManager) evictOldestPendingForConnectionLocked(
	connKey string,
) bool {
	var oldestRb lcommon.Blake2b256
	var oldestVoter uint64
	oldestIndex := -1
	var oldestTime time.Time
	for rbHash, byVoter := range m.pendingVotes {
		for voterId, candidates := range byVoter {
			for idx, candidate := range candidates {
				if candidate.connKey != connKey ||
					(oldestIndex >= 0 && !candidate.seenAt.Before(oldestTime)) {
					continue
				}
				oldestRb = rbHash
				oldestVoter = voterId
				oldestIndex = idx
				oldestTime = candidate.seenAt
			}
		}
	}
	if oldestIndex < 0 {
		return false
	}
	byVoter, ok := m.pendingVotes[oldestRb]
	if !ok {
		return false
	}
	candidates, ok := byVoter[oldestVoter]
	if !ok || oldestIndex >= len(candidates) {
		return false
	}
	candidates = slices.Delete(candidates, oldestIndex, oldestIndex+1)
	if len(candidates) == 0 {
		delete(byVoter, oldestVoter)
	} else {
		byVoter[oldestVoter] = candidates
	}
	if len(byVoter) == 0 {
		delete(m.pendingVotes, oldestRb)
	}
	m.pendingVoteCount--
	m.decrementPendingConnectionLocked(connKey)
	return true
}

func (m *VoteManager) decrementPendingConnectionLocked(connKey string) {
	if m.pendingVoteCountByConn[connKey] <= 1 {
		delete(m.pendingVoteCountByConn, connKey)
		return
	}
	m.pendingVoteCountByConn[connKey]--
}

func (m *VoteManager) removePendingAnnouncementLocked(
	rbHash lcommon.Blake2b256,
) map[uint64][]pendingPrototypeVote {
	pendingMap := m.pendingVotes[rbHash]
	delete(m.pendingVotes, rbHash)
	for _, candidates := range pendingMap {
		for _, candidate := range candidates {
			m.pendingVoteCount--
			m.decrementPendingConnectionLocked(candidate.connKey)
		}
	}
	return pendingMap
}

func (m *VoteManager) prunePrototypeStateLocked(now time.Time) {
	cutoff := now.Add(-m.voteTTL)
	for rbHash, record := range m.announcements {
		if record.seenAt.Before(cutoff) {
			delete(m.announcements, rbHash)
			delete(m.votedAnnouncements, rbHash)
			m.removePendingAnnouncementLocked(rbHash)
		}
	}
	for ebHash, record := range m.acquiredEbs {
		if record.seenAt.Before(cutoff) {
			delete(m.acquiredEbs, ebHash)
		}
	}
	for rbHash, byVoter := range m.pendingVotes {
		for voterId, candidates := range byVoter {
			kept := candidates[:0]
			for _, pending := range candidates {
				if pending.seenAt.Before(cutoff) {
					m.pendingVoteCount--
					m.decrementPendingConnectionLocked(pending.connKey)
					continue
				}
				kept = append(kept, pending)
			}
			if len(kept) == 0 {
				delete(byVoter, voterId)
			} else {
				byVoter[voterId] = kept
			}
		}
		if len(byVoter) == 0 {
			delete(m.pendingVotes, rbHash)
		}
	}
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
	announcingRbHash lcommon.Blake2b256,
) bool {
	raw, err := vote.MarshalCBOR()
	if err != nil {
		m.rejectVote("encoding", vote, err)
		return false
	}
	voteId := lcommon.LeiosVoteId{
		SlotNo:  vote.SlotNo,
		VoterId: vote.VoterId,
	}
	now := m.now()

	m.mu.Lock()
	if announcingRbHash != (lcommon.Blake2b256{}) {
		current, ok := m.announcements[announcingRbHash]
		if !ok || current.slot != vote.SlotNo || current.epoch != epoch ||
			current.ebHash != vote.EndorserBlockHash {
			m.mu.Unlock()
			return false
		}
		if originConn == "" {
			if _, acquired := m.acquiredEbs[current.ebHash]; !acquired {
				m.mu.Unlock()
				return false
			}
			if _, voted := m.votedAnnouncements[announcingRbHash]; voted {
				m.mu.Unlock()
				return false
			}
		}
	}
	// Prune before the dedup check so an expired entry cannot block a
	// fresh vote with the same id.
	m.pruneExpiredLocked(now)
	// Dedup against the record ledger, not the serving store: serving
	// entries are size-evicted, and re-counting an evicted vote would
	// inflate its tally and wedge certificate building on a duplicate
	// voter id.
	if record, ok := m.voteRecords[voteId]; ok {
		m.mu.Unlock()
		if record.ebHash == vote.EndorserBlockHash {
			// Identical resubmission. Deliberately not re-stored for
			// serving: a size-evicted vote stays unservable until its
			// record dies, which avoids serving-store churn under
			// re-delivery.
			return false
		}
		// Equivocation: same voter and slot, different endorser
		// block. The first vote wins for as long as its record
		// lives, even after its serving entry is evicted.
		if m.metrics != nil {
			m.metrics.votesEquivocationTotal.Inc()
		}
		m.logger.Warn(
			"dropping equivocating leios vote",
			"slot", vote.SlotNo,
			"voter_id", vote.VoterId,
			"kept_endorser_block_hash", record.ebHash.String(),
			"dropped_endorser_block_hash", vote.EndorserBlockHash.String(),
		)
		return false
	}
	if originConn != "" && !verified && len(m.voteRecords) >= m.maxRecords {
		// Reject rather than evict: dropping a record would let a
		// re-received vote re-count its stake (see voteRecord). The
		// cap gates only unverified peer votes, which anyone can
		// fabricate for committee members without registered keys.
		// Verified votes bypass it -- each requires a valid BLS
		// signature from a registered committee key and dedup bounds
		// them to one record per (slot, registered voter) -- so a
		// flood of unverifiable noise cannot starve the votes that
		// feed certificates. Locally emitted votes bypass it so a
		// flood cannot suppress the node's own committee
		// participation; local volume is bounded by the endorser
		// block cache.
		m.mu.Unlock()
		m.rejectVote(
			"capacity",
			vote,
			errors.New("vote record ledger full"),
		)
		return false
	}
	if originConn == "" && announcingRbHash != (lcommon.Blake2b256{}) {
		m.votedAnnouncements[announcingRbHash] = struct{}{}
	}
	m.voteRecords[voteId] = voteRecord{
		ebHash:           vote.EndorserBlockHash,
		announcingRbHash: announcingRbHash,
		epoch:            epoch,
		insertedAt:       now,
	}
	m.updateRecordsGaugeLocked()
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

	key := tallyKey{
		slotNo: vote.SlotNo, ebHash: vote.EndorserBlockHash,
		announcingRbHash: announcingRbHash,
	}
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
	return true
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
					"slot",
					key.slotNo,
					"endorser_block_hash",
					key.ebHash.String(),
					"observed_stake",
					tally.observedStake,
					"verified_stake",
					tally.verifiedStake,
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
		AnnouncingRbHash:  key.announcingRbHash,
		Certificate:       cert,
		VerifiedStake:     tally.verifiedStake,
		ObservedStake:     tally.observedStake,
		TotalActiveStake:  committee.TotalActiveStake,
	}
}

// pruneExpiredLocked drops votes, tallies, and dedup records older than
// the TTL. Callers must hold m.mu.
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
	// Records are pruned after tallies, in the same pass: an expired
	// record is dropped only once its tally is gone, so a vote whose
	// tally is still accumulating can never be re-counted. The reverse
	// direction also holds within this pass: a tally with
	// lastUpdated < cutoff implies every one of its records has
	// insertedAt <= lastUpdated < cutoff, so a dead tally's records all
	// drop with it and a re-created tally re-counts from a clean slate.
	for id, rec := range m.voteRecords {
		if !rec.insertedAt.Before(cutoff) {
			continue
		}
		if _, ok := m.tallies[tallyKey{
			slotNo:           id.SlotNo,
			ebHash:           rec.ebHash,
			announcingRbHash: rec.announcingRbHash,
		}]; ok {
			continue
		}
		delete(m.voteRecords, id)
	}
	m.updateRecordsGaugeLocked()
}

// updateRecordsGaugeLocked refreshes the record-ledger size gauge.
// Callers must hold m.mu.
func (m *VoteManager) updateRecordsGaugeLocked() {
	if m.metrics != nil {
		m.metrics.voteRecordsCount.Set(float64(len(m.voteRecords)))
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
// from both the log and the id index. It intentionally does not touch
// voteRecords -- callers own record pruning, with predicates that keep
// votesById a subset of voteRecords. Callers must hold m.mu.
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
	// Track the cursor locally and persist it only on successful
	// delivery: an aborted wait must not permanently skip votes that
	// were collected but never returned to the peer.
	cursorLoaded := false
	var cursor uint64
	for {
		m.mu.Lock()
		if !m.running {
			m.mu.Unlock()
			return nil, ErrVoteManagerStopped
		}
		m.pruneExpiredLocked(m.now())
		if !cursorLoaded {
			cursor = m.cursors[connKey]
			cursorLoaded = true
		}
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
		if uint64(len(collected)) == count {
			m.cursors[connKey] = cursor
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

// HandleEndorserBlock records acquisition. The current prototype votes only
// after a selected ranking block announces the acquired EB.
func (m *VoteManager) HandleEndorserBlock(
	slot uint64,
	ebHash lcommon.Blake2b256,
) {
	epoch, err := m.epochProvider.EpochForSlot(slot)
	if err != nil {
		m.logger.Debug(
			"cannot resolve acquired endorser block epoch",
			"error",
			err,
		)
		return
	}
	m.mu.Lock()
	now := m.now()
	m.prunePrototypeStateLocked(now)
	m.acquiredEbs[ebHash] = acquiredEbRecord{
		slot: slot, epoch: epoch, seenAt: now,
	}
	var ready []struct {
		rbHash lcommon.Blake2b256
		record announcementRecord
	}
	for rbHash, record := range m.announcements {
		if record.ebHash == ebHash {
			ready = append(ready, struct {
				rbHash lcommon.Blake2b256
				record announcementRecord
			}{rbHash: rbHash, record: record})
		}
	}
	m.mu.Unlock()
	for _, item := range ready {
		m.emitPrototypeVote(item.rbHash, item.record)
	}
}

func (m *VoteManager) emitPrototypeVote(
	rbHash lcommon.Blake2b256,
	record announcementRecord,
) {
	m.mu.Lock()
	votingPool := m.votingPool
	votingKey := m.votingKey
	_, alreadyVoted := m.votedAnnouncements[rbHash]
	m.mu.Unlock()
	if len(votingPool) == 0 || votingKey == nil || alreadyVoted {
		return
	}
	if err := m.slotWindowCheck(record.slot); err != nil {
		m.logger.Debug(
			"announcing ranking block outside vote window, not voting",
			"slot", record.slot,
			"error", err,
		)
		return
	}
	entry, err := m.committeeAndParamsForEpoch(record.epoch)
	if err != nil {
		m.logger.Debug(
			"leios committee unavailable, not voting",
			"slot", record.slot,
			"epoch", record.epoch,
			"error", err,
		)
		return
	}
	committee := entry.committee
	voterId, ok := committee.VoterIdFor(votingPool)
	if !ok {
		m.logger.Debug(
			"local pool is not a leios committee member, not voting",
			"slot", record.slot,
			"epoch", record.epoch,
		)
		return
	}
	member, ok := committee.Member(voterId)
	if !ok {
		return
	}
	// A vote this node marks verified=true is trusted without a
	// signature check by every local consumer (tallying, certificate
	// aggregation). That trust is only sound if votingKey is what the
	// rest of the network would actually resolve for this pool right
	// now -- otherwise a stale local key (e.g. after an on-chain
	// rotation) would let this node certify a vote no honest peer's
	// signature check would accept.
	resolved, resolvedOK := m.resolveVoterKey(entry, member.PoolKeyHash)
	if !resolvedOK || !resolved.Equal(votingKey.PublicKey()) {
		m.logger.Error(
			"configured leios voting key no longer matches the resolved public key for this pool, not voting",
			"slot", record.slot,
			"voter_id", voterId,
		)
		return
	}
	msg := PrototypeVoteMessageBytes(rbHash)
	sig, err := SignVote(votingKey, msg)
	if err != nil {
		m.logger.Error(
			"failed to sign leios vote",
			"slot", record.slot,
			"voter_id", voterId,
			"error", err,
		)
		return
	}
	vote := lcommon.LeiosVote{
		SlotNo:            record.slot,
		EndorserBlockHash: record.ebHash,
		VoterId:           voterId,
		VoteSignature:     sig,
	}
	if m.metrics != nil {
		m.metrics.votesReceivedTotal.Inc()
	}
	m.logger.Info(
		"emitting leios vote",
		"slot", record.slot,
		"voter_id", voterId,
		"announcing_rb_hash", rbHash.String(),
		"endorser_block_hash", record.ebHash.String(),
	)
	m.prototypeEmissionMu.Lock()
	inserted := m.insertVote(
		"", vote, record.epoch, committee, member, true, entry.tau, rbHash,
	)
	if inserted {
		m.eventBus.Publish(VoteEmittedEventType, event.NewEvent(
			VoteEmittedEventType,
			VoteEmittedEvent{Vote: lcommon.LeiosPrototypeVote{
				AnnouncingRbHash: rbHash,
				VoterId:          voterId,
				VoteSignature:    sig,
			}},
		))
	}
	m.prototypeEmissionMu.Unlock()
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
			switch data := evt.Data.(type) {
			case chain.ChainRollbackEvent:
				m.handleRollback(data)
			case chain.ChainBlockEvent:
				m.handleChainBlock(data)
			}
		}
	}
}

func (m *VoteManager) handleChainBlock(evt chain.ChainBlockEvent) {
	block, err := evt.Block.Decode()
	if err != nil {
		m.logger.Debug(
			"cannot decode chain block for leios announcement",
			"error",
			err,
		)
		return
	}
	header := block.Header()
	announcer, ok := header.(interface {
		LeiosAnnouncement() (lcommon.Blake2b256, uint64, bool)
	})
	if !ok {
		return
	}
	ebHash, _, ok := announcer.LeiosAnnouncement()
	if !ok {
		return
	}
	rbHash := lcommon.NewBlake2b256(header.Hash().Bytes())
	m.ObserveAnnouncement(header.SlotNumber(), rbHash, ebHash)
}

// ObserveAnnouncement records the ranking-block identity used by current
// prototype votes and connects it to the announced EB.
func (m *VoteManager) ObserveAnnouncement(
	slot uint64,
	rbHash lcommon.Blake2b256,
	ebHash lcommon.Blake2b256,
) {
	epoch, err := m.epochProvider.EpochForSlot(slot)
	if err != nil {
		m.logger.Debug(
			"cannot resolve announcing ranking block epoch",
			"error",
			err,
		)
		return
	}
	record := announcementRecord{
		slot: slot, epoch: epoch, ebHash: ebHash, seenAt: m.now(),
	}
	m.mu.Lock()
	m.prunePrototypeStateLocked(record.seenAt)
	m.announcements[rbHash] = record
	_, acquired := m.acquiredEbs[ebHash]
	pendingMap := m.removePendingAnnouncementLocked(rbHash)
	m.mu.Unlock()
	if acquired {
		m.emitPrototypeVote(rbHash, record)
	}
	for _, candidates := range pendingMap {
		for _, pending := range candidates {
			if err := m.handleResolvedPrototypeVote(
				pending.connKey,
				pending.vote,
				record,
			); err != nil {
				m.logger.Debug(
					"failed to handle queued prototype leios vote",
					"announcing_rb_hash", rbHash.String(),
					"voter_id", pending.vote.VoterId,
					"error", err,
				)
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
	m.prototypeEmissionMu.Lock()
	defer m.prototypeEmissionMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prunePrototypeStateLocked(m.now())
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
	// Records share the tally predicate (all votes in a tally share an
	// epoch), so record/tally pairs are dropped together.
	for id, rec := range m.voteRecords {
		if rec.epoch < keepFrom {
			delete(m.voteRecords, id)
		}
	}
	for rbHash, record := range m.announcements {
		if record.epoch < keepFrom {
			delete(m.announcements, rbHash)
			delete(m.votedAnnouncements, rbHash)
			m.removePendingAnnouncementLocked(rbHash)
		}
	}
	for ebHash, record := range m.acquiredEbs {
		if record.epoch < keepFrom {
			delete(m.acquiredEbs, ebHash)
		}
	}
	m.updateRecordsGaugeLocked()
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
	m.prototypeEmissionMu.Lock()
	defer m.prototypeEmissionMu.Unlock()
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
	// Records share the tally predicate, so record/tally pairs are
	// dropped together and a re-vote for the replacement chain is
	// accepted instead of being mistaken for equivocation.
	for id := range m.voteRecords {
		if id.SlotNo > evt.Point.Slot {
			delete(m.voteRecords, id)
		}
	}
	for rbHash, record := range m.announcements {
		if record.slot > evt.Point.Slot {
			delete(m.announcements, rbHash)
			delete(m.votedAnnouncements, rbHash)
			m.removePendingAnnouncementLocked(rbHash)
		}
	}
	for ebHash, record := range m.acquiredEbs {
		if record.slot > evt.Point.Slot {
			delete(m.acquiredEbs, ebHash)
		}
	}
	m.updateRecordsGaugeLocked()
	m.committees = make(map[uint64]*epochEntry)
	m.logger.Debug(
		"pruned leios vote state after rollback",
		"rollback_slot", evt.Point.Slot,
		"retained_votes", len(m.voteLog),
	)
}
