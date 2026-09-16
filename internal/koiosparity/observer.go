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

package koiosparity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"golang.org/x/sync/singleflight"
)

const (
	// defaultFetchRetryAttempts bounds retries of a single epoch's Koios
	// fetch, primarily to ride out the case where Dingo closes an epoch
	// boundary slightly before Koios's own backend has finished processing
	// it (fetchEpoch's end_time==0 rejection) — a transient condition near
	// live tip, not a permanent failure.
	defaultFetchRetryAttempts = 5
	// defaultFetchRetryDelay is the pause between fetch retry attempts.
	defaultFetchRetryDelay = 30 * time.Second
	// defaultQueueBuffer sizes the "an epoch.transition fired" wake channel.
	// Only ever needs 1 slot of headroom in practice (see run's pending-set
	// design below); sized slightly larger purely so a handler is never
	// blocked enqueueing a wake while run is mid-drain.
	defaultQueueBuffer = 4
)

// ObserverConfig configures an Observer.
type ObserverConfig struct {
	// Network is the Koios network to validate against ("preview" or
	// "preprod").
	Network string
	// CachePath is the Koios reference cache.db path.
	CachePath string
	// APIKey is the Koios Bearer token for higher-rate-limit access. Empty
	// uses Koios's unauthenticated rate limit.
	APIKey string
	// BaseURL overrides the public koios.rest host for the network; see
	// NewKoiosClient. Empty selects the public host. Tests point it at an
	// httptest server instead of rewriting the process-wide koiosBaseURLs
	// map, which every concurrently constructed client reads.
	BaseURL string
	// AllowInsecureHTTP permits a plain-HTTP BaseURL; see
	// NewKoiosClient. Local dev and test only, including the httptest
	// servers this package's own tests point BaseURL at.
	AllowInsecureHTTP bool
	// Source is the narrow, Dingo-supplied reward-parity source the
	// observer compares against — typically a *DatabaseSource wrapping the
	// live, in-process *database.Database.
	Source RewardParitySource
	// Strict stops the observer (and, via FatalFunc, the node driving it)
	// on the first Koios/tool error or exact parity mismatch. When false,
	// a failure is logged and recorded in the cache, and the observer keeps
	// validating subsequent epochs — an explicit, non-default choice for
	// advisory/observability-only use, since the issue this implements
	// requires Strict behavior to be available and be the operator default
	// (see dingo.KoiosParityConfig / DefaultKoiosParityConfig), not that
	// non-strict mode is forbidden to exist.
	Strict bool
	// AccountsEnabled runs #3097's per-account exact-parity fetch+check
	// phase (FetchAccountRewardsForEpoch / CompareAccountEpoch) alongside
	// the existing epoch-aggregate/pool phases, for every epoch this
	// observer processes. Unlike the standalone CLI's opt-in-only default
	// (see FetchConfig.AccountsEnabled/CheckConfig.AccountsEnabled), the
	// in-process observer is the operationally-real, continuously-driven
	// path #3098 exists to make possible, so this defaults to true at the
	// dingo.KoiosParityConfig/DefaultKoiosParityConfig level (not here --
	// ObserverConfig itself has no zero-value magic, matching Strict's own
	// pattern) -- set false explicitly to keep the observer pool-level-only,
	// e.g. to bound Koios request volume on a resource-constrained
	// deployment.
	AccountsEnabled bool
	// GraceHours is forwarded to CheckEpoch/CompareEpochAggregates: the
	// window after an epoch closes during which a missing Dingo-side row is
	// reference/sync lag, not a failure. 0 selects the check package's own
	// default handling (no grace window).
	GraceHours int
	// AccountChunkSize/AccountChunkMaxBytes (dingo #3099) bound each
	// /account_reward_history request by both address count and encoded
	// body size. <=0 means "use the package default"
	// (koiosAccountChunkSize/koiosAccountChunkMaxBytesDefault). Unused when
	// AccountsEnabled is false.
	AccountChunkSize     int
	AccountChunkMaxBytes int
	// FatalFunc is invoked at most once, with a non-nil error, the first
	// time Strict validation fails. Wired by the caller (typically
	// node.go's n.cancel) to stop/cancel the driving Dingo instance. May be
	// nil (validation still stops locally; nothing else is cancelled).
	FatalFunc func(error)
	// FetchRetryAttempts/FetchRetryDelay override the bounded retry policy
	// used when a single epoch's Koios fetch fails transiently (e.g. Koios
	// has not yet closed the epoch out on its own backend). 0 selects the
	// package defaults.
	FetchRetryAttempts int
	FetchRetryDelay    time.Duration
	// OnResult, if set, is called after every epoch this observer validates
	// (pass, fail, or error), for tests/observability. Never called
	// concurrently with itself.
	OnResult func(*EpochCompareResult)
	Logger   *slog.Logger
}

// Observer drives Koios fetch+check for each closed epoch as Dingo's own
// EventBus reports event.EpochTransitionEventType, using an in-process
// RewardParitySource instead of polling a separately synced metadata
// database (dingo #3098). It is registered from node.go/internal/node
// composition, not from ledger/database domain packages: this file is the
// only place in the observer's call path that constructs a Koios HTTP
// client or controls node lifecycle (via FatalFunc), matching every other
// cross-component adapter node.go wires up (dblifecycle.Manager,
// historyexpiry.Pruner, offchainmetadata.Fetcher, ...).
//
// HandleEpochTransitionEvent — the EventBus subscriber callback — never
// performs Koios/database I/O itself: it only records the epoch and wakes
// the background goroutines, which do the actual work. This keeps EventBus
// dispatch to this subscriber fast regardless of how long a Koios fetch
// takes, and (together with node.go's own "authoritative epoch-boundary
// snapshot capture happens inside the write transaction; event.Publish
// happens after Unlock" ordering) guarantees Koios/network I/O only ever
// starts after the epoch-boundary transaction has committed and the ledger
// lock has been released — this code never acquires it.
//
// Two independent background goroutines drain two independent queues (dingo
// #4339): run/pending/wake drive the fast pool/epoch-aggregate fetch+check
// (CheckEpoch with accountsEnabled=false, regardless of
// ObserverConfig.AccountsEnabled) for every epoch, and — only when
// AccountsEnabled is true — runAccounts/pendingAccounts/wakeAccounts
// separately drive the slow, rate-limited per-account fetch+check (#3097,
// CheckEpoch with accountsEnabled=true) on its own schedule. Before this
// split, a single goroutine fetched pools+params+accounts and only then
// checked one epoch at a time in strict order, so an epoch's ~5,000-request
// per-account fetch (Preview: tens of minutes) blocked every later epoch's
// fast aggregate check from ever running — exactly the class of exact-parity
// reward-round defect this observer exists to catch could sit undetected for
// as long as the account backlog took to drain. The account queue can now
// fall arbitrarily far behind the aggregate queue without affecting how
// quickly a strict-mode aggregate/pool mismatch fires FatalFunc.
type Observer struct {
	cfg   ObserverConfig
	cache *Cache
	koios *KoiosClient

	mu      sync.Mutex
	pending map[uint64]struct{} // epochs requested for (re)validation

	// pendingAccounts is pending's twin for the slow per-account queue (see
	// the Observer doc comment). Guarded by the same mu as pending: both are
	// small, infrequently-touched sets, so sharing one mutex avoids a second
	// lock without any meaningful contention cost.
	pendingAccounts map[uint64]struct{}

	wake         chan struct{}
	wakeAccounts chan struct{}
	wg           sync.WaitGroup

	// aggFetch collapses the pool/epoch-aggregate reference fetch that both
	// queues need for the same epoch into one round of Koios requests; see
	// fetchAggregateIfNeeded.
	aggFetch singleflight.Group

	// cancel stops both run and runAccounts's background goroutines. It is
	// the CancelFunc of the context derived (in Start) from the ctx passed to
	// Start, rather than a channel Stop closes directly — a
	// context.CancelFunc is inherently safe to invoke more than once
	// (subsequent calls are no-ops), which is what lets Stop be called more
	// than once on the same Observer (e.g. once from node.go's started-stack
	// cleanup on a startup failure, and again from node_shutdown.go's normal
	// shutdown path) without panicking the way closing an already-closed
	// channel would. nil until Start succeeds, so Stop guards against calling
	// a nil func when Start was never called.
	cancel context.CancelFunc

	// fatalFired is set once FatalFunc has been called for a strict-mode
	// failure. Written from fail, which run and runAccounts's goroutines can
	// now both call concurrently (dingo #4339's queue split), so it is an
	// atomic.Bool rather than a plain bool guarded only by "one goroutine at
	// a time".
	fatalFired atomic.Bool

	// started guards against calling Start more than once on the same
	// Observer: a second call would silently overwrite o.cancel (orphaning
	// the first run/runAccounts goroutines, which nothing would ever
	// cancel/wait on again) and launch second concurrent run/runAccounts
	// goroutines racing the first over o.pending/o.pendingAccounts/o.cache.
	// Checked and set under o.mu.
	started bool
}

// NewObserver constructs an Observer. It opens (or creates) the Koios
// reference cache at cfg.CachePath and a Koios client for cfg.Network; call
// Start to begin processing and Stop to release both.
func NewObserver(cfg ObserverConfig) (*Observer, error) {
	if cfg.Source == nil {
		return nil, errors.New(
			"koiosparity: Observer requires a non-nil RewardParitySource",
		)
	}
	if cfg.Network != "preview" && cfg.Network != "preprod" {
		return nil, fmt.Errorf(
			"koiosparity: Observer network must be preview or preprod, got %q",
			cfg.Network,
		)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.FetchRetryAttempts <= 0 {
		cfg.FetchRetryAttempts = defaultFetchRetryAttempts
	}
	if cfg.FetchRetryDelay <= 0 {
		cfg.FetchRetryDelay = defaultFetchRetryDelay
	}

	cache, err := OpenCache(cfg.CachePath, cfg.Logger)
	if err != nil {
		return nil, fmt.Errorf("open koios parity cache: %w", err)
	}
	koios, err := NewKoiosClient(
		cfg.Network,
		cfg.APIKey,
		cfg.BaseURL,
		cfg.AllowInsecureHTTP,
	)
	if err != nil {
		_ = cache.Close()
		return nil, fmt.Errorf("create koios client: %w", err)
	}

	return &Observer{
		cfg:             cfg,
		cache:           cache,
		koios:           koios,
		pending:         make(map[uint64]struct{}),
		pendingAccounts: make(map[uint64]struct{}),
		wake:            make(chan struct{}, defaultQueueBuffer),
		wakeAccounts:    make(chan struct{}, defaultQueueBuffer),
	}, nil
}

// Start seeds the observer's backlog with every already-closed epoch Dingo
// has committed reward state for but this cache has not yet checked
// (backfilling full history for a fresh attach or a restart, using the
// cache's own persisted check/fetch status as the sole resumable checkpoint
// — no separate checkpoint file is introduced), then launches the
// background goroutine(s) that drain pending epochs: run always, for the
// fast pool/aggregate queue, and — only when cfg.AccountsEnabled — a second,
// independent runAccounts goroutine for the slow per-account queue (dingo
// #4339; see the Observer doc comment). Subscribe the returned Observer's
// HandleEpochTransitionEvent to event.EpochTransitionEventType before or
// after calling Start; live events and the seeded backlog feed the same
// pending sets either way.
//
// Start may only be called once per Observer; a second call returns an
// error rather than silently orphaning the first run/runAccounts goroutines
// (construct a new Observer instead, e.g. via NewObserver, if a fresh Start
// is needed).
func (o *Observer) Start(ctx context.Context) error {
	o.mu.Lock()
	if o.started {
		o.mu.Unlock()
		return errors.New("koiosparity: Observer.Start called more than once")
	}
	o.started = true
	o.mu.Unlock()

	// Recorded here rather than in NewObserver because a source change is
	// gated on the new host answering, and that probe needs a context and a
	// startup the caller can fail. Start is called exactly once per Observer
	// and before any fetch, so the stamp still lands before the first row.
	if err := recordKoiosSource(
		ctx, o.cache, o.cfg.Network, o.koios, o.cfg.Logger,
	); err != nil {
		return err
	}

	if err := o.seedBacklog(ctx); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	o.cancel = cancel

	o.wg.Go(func() {
		o.run(runCtx)
	})
	o.signalWake()
	if o.cfg.AccountsEnabled {
		o.wg.Go(func() {
			o.runAccounts(runCtx)
		})
		o.signalWakeAccounts()
	}
	return nil
}

// seedBacklog implements Start's backlog-seed step: see Start's doc comment.
// Factored out (rather than inlined in Start) so a test can exercise exactly
// what gets added to o.pending/o.pendingAccounts without racing the
// background goroutines, which Start launches immediately afterward.
func (o *Observer) seedBacklog(ctx context.Context) error {
	latest, err := o.cfg.Source.GetLatestEpoch(ctx)
	if err != nil {
		o.cfg.Logger.Debug(
			"koiosparity observer: no committed epoch data yet at startup",
			"error", err,
		)
		return nil
	}
	if latest == 0 {
		return nil
	}

	// latest is Dingo's own current (most recently started) epoch; only
	// epochs strictly before it are safely closed — mirrors Fetch's own
	// "tipEpoch - 1" bound.
	throughEpoch := latest - 1

	// seedFrom additionally bounds every backlog-seed query below at
	// this node's own earliest available ledger epoch (its Mithril
	// bootstrap boundary, when one is recorded), on top of the
	// pre-staking floor checkEpoch already applies on its own. Without
	// this, a fresh Mithril-bootstrapped node — which has no local
	// ledger history before that boundary by construction — would seed
	// its entire backlog from epoch 0 and spend a Koios fetch on every
	// one of what can be well over a thousand epochs it can never have
	// local data for (dingo #4172). haveEarliestAvailable is false for
	// a non-Mithril, genesis-synced node, in which case seedFrom stays
	// 0 and behavior is unchanged from before this bound existed.
	seedFrom := uint64(0)
	earliestAvailable, haveEarliestAvailable, err := o.cfg.Source.GetEarliestAvailableEpoch(
		ctx,
	)
	if err != nil {
		return fmt.Errorf(
			"seed koiosparity observer backlog: resolve earliest available epoch: %w",
			err,
		)
	}
	if haveEarliestAvailable && earliestAvailable > seedFrom {
		seedFrom = earliestAvailable
	}

	// A fresh Mithril-bootstrapped node may not have closed a single
	// epoch past its own bootstrap boundary yet, in which case
	// seedFrom > throughEpoch and there is nothing this node could
	// possibly have local data for — leave the backlog empty rather
	// than pass an inverted range to the queries below.
	if seedFrom > throughEpoch {
		return nil
	}

	// Always queried with accountsEnabled=false for o.pending: pool/aggregate
	// staleness (s.epoch IS NULL, or fetched_at > last_checked_at) is
	// independent of AccountsEnabled, so this is exactly the fast queue's own
	// criterion — an epoch selected only by the true-variant's extra
	// account-coverage-staleness clause has nothing stale on the aggregate
	// side and must not be re-queued into the fast queue for it (that would
	// re-report a stale aggregate-only PASS result on every restart purely
	// because per-account coverage happens to be missing, racing whichever
	// queue reaches it first).
	needing, err := o.cache.GetEpochsNeedingCheck(o.cfg.Network, false)
	if err != nil {
		return fmt.Errorf("seed koiosparity observer backlog: %w", err)
	}
	uncached, err := o.cache.GetUncachedEpochs(
		o.cfg.Network,
		seedFrom,
		throughEpoch,
	)
	if err != nil {
		return fmt.Errorf("seed koiosparity observer backlog: %w", err)
	}
	o.mu.Lock()
	for _, e := range needing {
		if e >= seedFrom && e <= throughEpoch {
			o.pending[e] = struct{}{}
		}
	}
	for _, e := range uncached {
		o.pending[e] = struct{}{}
		// An epoch with no koios_epoch_info row at all is invisible to every
		// other seed query below: GetEpochsNeedingCheck selects FROM that
		// table and GetEpochsMissingAccountCoverage requires a row in it. It
		// therefore has to be queued for the per-account check from here, or
		// #3097's comparison would not run for any never-fetched epoch until
		// some later restart re-seeded it — which is the whole backlog on the
		// bulk-sync node dingo #4339 is about. processAccountEpoch fetches
		// the aggregate reference itself, so the account queue does not
		// depend on the aggregate queue having reached the epoch first.
		if o.cfg.AccountsEnabled {
			o.pendingAccounts[e] = struct{}{}
		}
	}
	o.mu.Unlock()

	// The account-coverage-aware variant additionally selects an epoch whose
	// per-account reference data is absent, incomplete, or stale relative to
	// the last check — queue those into the slow per-account queue rather
	// than the fast one (dingo #4339). This can overlap with `needing` above
	// (an epoch can be stale on both dimensions at once); queuing it into
	// pendingAccounts too is harmless since fetchAccountsIfNeeded's own
	// coverage-completeness gate makes it a no-op Koios-request-wise for an
	// epoch whose account coverage was actually already fine.
	if o.cfg.AccountsEnabled {
		needingAccounts, err := o.cache.GetEpochsNeedingCheck(
			o.cfg.Network,
			true,
		)
		if err != nil {
			return fmt.Errorf("seed koiosparity observer backlog: %w", err)
		}
		o.mu.Lock()
		for _, e := range needingAccounts {
			if e >= seedFrom && e <= throughEpoch {
				o.pendingAccounts[e] = struct{}{}
			}
		}
		o.mu.Unlock()
	}

	// An epoch whose pool data/check status is already fine can still be
	// missing #3097's per-account coverage entirely (e.g. it was fetched
	// before AccountsEnabled was turned on) — neither GetEpochsNeedingCheck
	// nor GetUncachedEpochs above would ever flag it purely for that
	// reason once accountsEnabled's own staleness branch is satisfied by a
	// prior check. Add those epochs to the slow per-account queue (dingo
	// #4339), independent of why GetEpochsNeedingCheck/GetUncachedEpochs may
	// or may not have already selected them, the same way
	// fetchAccountsIfNeeded gates on account coverage independently of
	// fetchPoolsIfNeeded. Deliberately not added to o.pending: nothing about
	// missing account coverage implies the pool/aggregate data is stale.
	if o.cfg.AccountsEnabled {
		missingAccounts, err := o.cache.GetEpochsMissingAccountCoverage(
			o.cfg.Network,
			seedFrom,
			throughEpoch,
		)
		if err != nil {
			return fmt.Errorf(
				"seed koiosparity observer backlog: %w",
				err,
			)
		}
		o.mu.Lock()
		for _, e := range missingAccounts {
			o.pendingAccounts[e] = struct{}{}
		}
		o.mu.Unlock()
	}

	// Same shape, for the same reason, one column over: an epoch cached
	// before protocol-parameter comparison existed has a
	// koios_epoch_info row and a stored PASS, so neither
	// GetEpochsNeedingCheck nor GetUncachedEpochs above ever returns it.
	// Without this seed the epoch is never queued, processEpoch never
	// runs, and fetchIfNeeded's fetchParamsIfNeeded gate is never
	// reached — the parameter row would never arrive for exactly the
	// caches that need backfilling. Params are part of the fast
	// pool/aggregate comparison (checkEpoch's step 1d), not the per-account
	// one, so this only ever seeds o.pending.
	missingParams, err := o.cache.GetEpochsMissingParams(
		o.cfg.Network,
		seedFrom,
		throughEpoch,
	)
	if err != nil {
		return fmt.Errorf("seed koiosparity observer backlog: %w", err)
	}
	o.mu.Lock()
	for _, e := range missingParams {
		o.pending[e] = struct{}{}
	}
	o.mu.Unlock()
	return nil
}

// Stop cancels the observer's background processing and releases its cache
// and Koios client. It always blocks until run's goroutine (and
// runAccounts's, when cfg.AccountsEnabled started it) has actually exited
// before closing the cache — closing the cache out from under a still-running
// goroutine would race that goroutine's own cache queries, and the caller's
// node.go composition depends on Stop returning only once it is safe to tear
// down the database/blob store the source reads from. ctx only bounds how
// long Stop waits *quietly*: once ctx expires, Stop logs a warning that
// in-flight work is taking longer than expected but keeps waiting for the
// goroutine(s) to actually exit (each one still exits promptly once the
// in-flight call returns, since it also observes ctx.Done() — the same ctx
// passed to Start — at its next opportunity, so this is expected to resolve
// quickly in practice rather than hang).
//
// Stop is safe to call more than once on the same Observer (e.g. once from
// node.go's started-stack cleanup on a startup failure that occurs after the
// observer was started but before Run finishes, and again from
// node_shutdown.go's normal shutdown path): o.cancel is a context.CancelFunc,
// which is a documented no-op on any call after the first, and wg.Wait/
// cache.Close are both safe to invoke redundantly (Wait returns immediately
// once the goroutine has already exited; sql.DB.Close is idempotent).
func (o *Observer) Stop(ctx context.Context) error {
	if o.cancel != nil {
		o.cancel()
	}
	waitCh := make(chan struct{})
	go func() {
		o.wg.Wait()
		close(waitCh)
	}()
	select {
	case <-waitCh:
	case <-ctx.Done():
		o.cfg.Logger.Warn(
			"koiosparity observer: stop context expired, still waiting for in-flight work before releasing cache",
		)
		<-waitCh
	}
	var errs []error
	if err := o.cache.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// HandleEpochTransitionEvent is the event.EventBus subscriber callback —
// register it with SubscribeFunc(event.EpochTransitionEventType, ...). It
// only records data.PreviousEpoch (the epoch that just closed) and wakes the
// background goroutine(s); see the Observer doc comment for why it must
// never do slower work itself. Every epoch always goes on the fast
// pool/aggregate queue; it additionally goes on the slow per-account queue
// only when cfg.AccountsEnabled, since that queue does not run at all
// otherwise (see Start).
func (o *Observer) HandleEpochTransitionEvent(evt event.Event) {
	data, ok := evt.Data.(event.EpochTransitionEvent)
	if !ok {
		return
	}
	o.mu.Lock()
	o.pending[data.PreviousEpoch] = struct{}{}
	if o.cfg.AccountsEnabled {
		o.pendingAccounts[data.PreviousEpoch] = struct{}{}
	}
	o.mu.Unlock()
	o.signalWake()
	if o.cfg.AccountsEnabled {
		o.signalWakeAccounts()
	}
}

func (o *Observer) signalWake() {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

func (o *Observer) signalWakeAccounts() {
	select {
	case o.wakeAccounts <- struct{}{}:
	default:
	}
}

// run drains o.pending in epoch order until ctx fires — either the parent
// context passed to Start being cancelled, or Stop's own cancellation of the
// child context Start derives from it (see Start/Stop). Using a set (rather
// than a single high-water-mark epoch number) is what lets a rollback-driven
// replay re-request an epoch that was already validated: a fresh
// event.EpochTransitionEvent for the same PreviousEpoch just re-adds it, and
// it is revalidated against whatever Dingo's committed state for it is by
// the time run gets to it — never skipped, and never silently stuck on a
// stale prior result. Duplicate events for the same epoch (dingo emits both
// a slot-clock-driven and a block-driven epoch.transition for the same
// boundary) collapse harmlessly into the same set entry.
//
// run only ever performs the fast pool/epoch-aggregate fetch+check
// (processEpoch, CheckEpoch with accountsEnabled=false); runAccounts is its
// independent twin for the slow per-account fetch+check (dingo #4339). The
// two share o.cache/o.koios and o.fail/o.fatalFired but otherwise never
// block on each other.
func (o *Observer) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		o.mu.Lock()
		todo := make([]uint64, 0, len(o.pending))
		for e := range o.pending {
			todo = append(todo, e)
		}
		clear(o.pending)
		o.mu.Unlock()

		if len(todo) > 0 {
			slices.Sort(todo)
			for _, epoch := range todo {
				select {
				case <-ctx.Done():
					return
				default:
				}
				o.processEpoch(ctx, epoch)
				if o.cfg.Strict && o.stopping() {
					return
				}
			}
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-o.wake:
		}
	}
}

// runAccounts is run's twin for the slow, rate-limited per-account queue
// (dingo #4339): same draining shape, over o.pendingAccounts/o.wakeAccounts,
// calling processAccountEpoch instead of processEpoch. Only launched by
// Start when cfg.AccountsEnabled.
func (o *Observer) runAccounts(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		o.mu.Lock()
		todo := make([]uint64, 0, len(o.pendingAccounts))
		for e := range o.pendingAccounts {
			todo = append(todo, e)
		}
		clear(o.pendingAccounts)
		o.mu.Unlock()

		if len(todo) > 0 {
			slices.Sort(todo)
			for _, epoch := range todo {
				select {
				case <-ctx.Done():
					return
				default:
				}
				o.processAccountEpoch(ctx, epoch)
				if o.cfg.Strict && o.stopping() {
					return
				}
			}
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-o.wakeAccounts:
		}
	}
}

// stopping reports whether a strict-mode failure has already fired
// FatalFunc, so run/runAccounts's loops stop promptly instead of processing
// further epochs after strict validation has already failed.
func (o *Observer) stopping() bool {
	return o.fatalFired.Load()
}

// processEpoch fetches (if not already cached) and checks exactly one
// epoch's fast pool/epoch-aggregate data, then reports/records the outcome.
// Strict-mode cancellation is triggered here, once, on the first failure.
// The per-account phase (#3097) never runs from here — see
// processAccountEpoch — regardless of cfg.AccountsEnabled.
func (o *Observer) processEpoch(ctx context.Context, epoch uint64) {
	if err := o.fetchIfNeeded(ctx, epoch); err != nil {
		if cancelled(ctx, err) {
			o.cfg.Logger.Debug(
				"koiosparity observer: fetch interrupted by shutdown",
				"network",
				o.cfg.Network,
				"epoch",
				epoch,
				"error",
				err,
			)
			return
		}
		o.reportError(epoch, fmt.Errorf("fetch koios reference: %w", err))
		return
	}

	result, err := CheckEpoch(
		ctx,
		o.cache,
		o.cfg.Source,
		o.cfg.Network,
		epoch,
		o.cfg.GraceHours,
		false,
		o.cfg.Logger,
	)
	if err != nil {
		if cancelled(ctx, err) {
			o.cfg.Logger.Debug(
				"koiosparity observer: check interrupted by shutdown",
				"network",
				o.cfg.Network,
				"epoch",
				epoch,
				"error",
				err,
			)
			return
		}
		o.reportError(epoch, fmt.Errorf("check: %w", err))
		return
	}
	if o.cfg.OnResult != nil {
		o.cfg.OnResult(result)
	}
	if result.Status != StatusPass {
		significant := CountSignificant(result.Mismatches)
		o.fail(epoch, fmt.Errorf(
			"parity %s at epoch %d (%d significant of %d mismatch(es))",
			result.Status, epoch, significant, len(result.Mismatches),
		))
		return
	}
	o.cfg.Logger.Info("koiosparity observer: epoch validated",
		"network", o.cfg.Network, "epoch", epoch)
}

// processAccountEpoch fetches (if not already cached) and checks exactly one
// epoch's slow #3097 per-account data, then reports/records the outcome.
// Strict-mode cancellation is triggered here, once, on the first failure —
// independent of processEpoch's own strict-mode cancellation, so an
// aggregate-phase failure and an account-phase failure both reach fail/
// FatalFunc through the same exactly-once path (see fail).
//
// fetchIfNeeded runs here too, even though processEpoch also calls it: this
// queue must not assume the fast queue has already reached this epoch, since
// compareEpochAccounts (via CheckEpoch's accountsEnabled=true path) needs the
// koios_epoch_info row it provides. fetchIfNeeded's own per-epoch gate
// collapses the two queues' overlapping calls into one round of Koios
// requests.
func (o *Observer) processAccountEpoch(ctx context.Context, epoch uint64) {
	if err := o.fetchIfNeeded(ctx, epoch); err != nil {
		if cancelled(ctx, err) {
			o.cfg.Logger.Debug(
				"koiosparity observer: account-queue aggregate fetch interrupted by shutdown",
				"network",
				o.cfg.Network,
				"epoch",
				epoch,
				"error",
				err,
			)
			return
		}
		o.reportError(epoch, fmt.Errorf("fetch koios reference: %w", err))
		return
	}
	if err := o.fetchAccountsIfNeeded(ctx, epoch); err != nil {
		if cancelled(ctx, err) {
			o.cfg.Logger.Debug(
				"koiosparity observer: account fetch interrupted by shutdown",
				"network",
				o.cfg.Network,
				"epoch",
				epoch,
				"error",
				err,
			)
			return
		}
		o.reportError(epoch, fmt.Errorf("fetch koios reference: %w", err))
		return
	}

	result, err := CheckEpoch(
		ctx,
		o.cache,
		o.cfg.Source,
		o.cfg.Network,
		epoch,
		o.cfg.GraceHours,
		true,
		o.cfg.Logger,
	)
	if err != nil {
		if cancelled(ctx, err) {
			o.cfg.Logger.Debug(
				"koiosparity observer: account check interrupted by shutdown",
				"network",
				o.cfg.Network,
				"epoch",
				epoch,
				"error",
				err,
			)
			return
		}
		o.reportError(epoch, fmt.Errorf("check: %w", err))
		return
	}
	if o.cfg.OnResult != nil {
		o.cfg.OnResult(result)
	}
	if err := o.cache.PruneAccountCoverage(o.cfg.Network, epoch); err != nil {
		o.cfg.Logger.Warn(
			"koiosparity observer: prune account coverage failed",
			"network", o.cfg.Network, "epoch", epoch, "error", err,
		)
	}
	if result.Status != StatusPass {
		significant := CountSignificant(result.Mismatches)
		o.fail(epoch, fmt.Errorf(
			"parity %s at epoch %d (%d significant of %d mismatch(es))",
			result.Status, epoch, significant, len(result.Mismatches),
		))
		return
	}
	o.cfg.Logger.Info("koiosparity observer: epoch validated",
		"network", o.cfg.Network, "epoch", epoch)
}

// cancelled reports whether err is (or wraps) a context cancellation/
// deadline error attributable to ctx itself having already been
// cancelled/expired — i.e. a clean Observer.Stop-driven shutdown racing
// fetchIfNeeded/CheckEpoch, not a genuine Koios/tool failure that merely
// happens to surface as a context error. Mirrors check.go's own
// shutdown-vs-failure distinction (its ctx.Err() check before consuming
// errCh in Check). Only ctx.Err() != nil is treated as "shutdown in
// progress" — a context.DeadlineExceeded bubbling up from some inner,
// still-live context (e.g. a per-request Koios timeout) would not also
// mark the outer ctx done, so it is correctly still treated as a real
// failure.
func cancelled(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() == nil {
		return false
	}
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

// reportError records a genuine (non-shutdown) per-epoch fetch/check
// failure: it logs and, in strict mode, fires FatalFunc via fail, and
// additionally invokes OnResult (when set) with a synthesized ERROR-status
// result carrying err's text as a synthetic mismatch — otherwise these two
// error branches in processEpoch would be invisible to OnResult callers,
// unlike every other outcome (PASS/FAIL) processEpoch reports. Not
// persisted to the cache: checkEpoch/CheckEpoch itself does not persist
// check_epoch_status on this class of failure either (a fetch or query
// error that occurs before any comparison could run), so this stays
// consistent with that existing behavior.
func (o *Observer) reportError(epoch uint64, err error) {
	o.fail(epoch, err)
	if o.cfg.OnResult != nil {
		now := time.Now()
		o.cfg.OnResult(&EpochCompareResult{
			Network: o.cfg.Network,
			Epoch:   epoch,
			Status:  StatusError,
			Mismatches: []CheckMismatch{{
				Network:    o.cfg.Network,
				Epoch:      epoch,
				Field:      "observer_error",
				DingoValue: fmt.Sprintf("error: %v", err),
				Category:   CategoryDBError,
				CheckedAt:  now,
			}},
		})
	}
}

// fetchIfNeeded fetches the fast pool/epoch-aggregate Koios reference data
// for epoch only if it is not already cached — a historical epoch's Koios
// reference never changes, so a re-request (e.g. after a Dingo-side rollback
// re-signals the same epoch) would just be wasted work. This deliberately
// never fetches #3097's per-account reference data (fetchAccountsIfNeeded):
// dingo #4339 moved that into its own, independently-scheduled queue (see
// runAccounts/processAccountEpoch) precisely so a slow per-account fetch for
// one epoch can never delay this fast fetch — and therefore the fast
// pool/aggregate check that follows it — for any later epoch.
//
// Both queues call this, since the account queue must not assume the
// aggregate queue has already reached the epoch, so it is gated per epoch
// with singleflight. The cache-presence gates below (fetchPoolsIfNeeded's
// GetUncachedEpochs, fetchParamsIfNeeded's parameter-row lookup) only
// suppress a fetch that has already *finished*, so ungated an epoch
// transition — which wakes both queues at once with the same epoch — has
// each of them resolve the pool universe and fetch /epoch_info,
// /epoch_params, /totals and every chunked /pool_history request
// independently, doubling the aggregate half of a Koios quota this observer
// is explicitly designed around.
//
// Keying per epoch, rather than serializing all aggregate fetches, keeps the
// queues independent except when they are on the same epoch — where the
// later caller was about to do exactly this work anyway.
//
// Sharing one call's result across both callers is sound because both
// goroutines are driven by the same context (the one Start derives and Stop
// cancels), so a cancellation observed by the in-flight fetch is a
// cancellation for the waiting caller too.
func (o *Observer) fetchIfNeeded(ctx context.Context, epoch uint64) error {
	_, err, _ := o.aggFetch.Do(
		strconv.FormatUint(epoch, 10),
		func() (any, error) {
			if err := o.fetchPoolsIfNeeded(ctx, epoch); err != nil {
				return nil, err
			}
			return nil, o.fetchParamsIfNeeded(ctx, epoch)
		},
	)
	return err
}

// fetchParamsIfNeeded fetches the /epoch_params reference row for epoch only
// if the cache does not already hold one.
//
// It is gated on the parameter row itself rather than on GetUncachedEpochs,
// for the same reason fetchAccountsIfNeeded is gated on account coverage: an
// epoch cached before parameter comparison existed has a koios_epoch_info row
// and no koios_epoch_params row, so GetUncachedEpochs reports it as cached and
// fetchPoolsIfNeeded returns early. Without an independent gate the parameter
// row would never arrive, and the epoch would either keep its stored PASS
// having compared no parameters at all, or fail every check with a
// koios_epoch_params dingo_db_missing that no fetch would ever resolve.
//
// Pre-staking epochs are skipped: Koios publishes no parameter row for them
// (preprod /epoch_params returns [] for epochs 0 and 1), and fetchEpoch does
// not request one either.
func (o *Observer) fetchParamsIfNeeded(
	ctx context.Context,
	epoch uint64,
) error {
	info, err := o.cache.GetEpochInfo(o.cfg.Network, epoch)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("get epoch info before params fetch: %w", err)
	}
	if info != nil && info.PreStaking {
		return nil
	}
	existing, err := o.cache.GetEpochParams(o.cfg.Network, epoch)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("get epoch params: %w", err)
	}
	if existing != nil {
		return nil
	}
	return o.fetchParamsWithRetry(ctx, epoch)
}

// fetchParamsWithRetry fetches and commits the /epoch_params row, retrying a
// transient failure on the same schedule as the pool and account fetches. A
// permanent Koios error is returned immediately, since retrying it only burns
// quota.
func (o *Observer) fetchParamsWithRetry(
	ctx context.Context,
	epoch uint64,
) error {
	var lastErr error
	for attempt := 0; attempt < o.cfg.FetchRetryAttempts; attempt++ {
		err := FetchEpochParams(ctx, o.koios, o.cache, o.cfg.Network, epoch)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrKoiosPermanent) {
			return err
		}
		lastErr = err
		if attempt == o.cfg.FetchRetryAttempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(o.cfg.FetchRetryDelay):
		}
	}
	return fmt.Errorf("fetch epoch params %d: %w", epoch, lastErr)
}

// fetchPoolsIfNeeded fetches Koios pool/epoch-info/totals reference data for
// epoch only if it is not already cached.
//
// The pool universe (poolIDs/firstActiveEpochs) is resolved at most once
// across this call's whole retry loop and reused via FetchEpochWithPools:
// using the simpler FetchEpochWithClient here would re-run both full
// /pool_list and /pool_updates scans on every one of cfg.FetchRetryAttempts
// attempts, needlessly burning rate-limit/quota budget on a Koios backend
// blip that has nothing to do with the pool universe at all. The resolution
// itself still participates in the same retry/backoff loop as the per-epoch
// fetch (rather than being resolved once, unconditionally, before the loop),
// so a transient failure fetching the pool universe is retried exactly like
// a transient per-epoch fetch failure would be, instead of failing the whole
// call on the first attempt.
func (o *Observer) fetchPoolsIfNeeded(ctx context.Context, epoch uint64) error {
	uncached, err := o.cache.GetUncachedEpochs(o.cfg.Network, epoch, epoch)
	if err != nil {
		return fmt.Errorf("check cache for epoch %d: %w", epoch, err)
	}
	if len(uncached) == 0 {
		return nil
	}
	var poolIDs []string
	var firstActiveEpochs map[string]uint64
	var poolsResolved bool
	var lastErr error
	for attempt := 0; attempt < o.cfg.FetchRetryAttempts; attempt++ {
		if !poolsResolved {
			poolIDs, firstActiveEpochs, err = resolvePoolUniverse(ctx, o.koios)
			if err != nil {
				if errors.Is(err, ErrKoiosPermanent) {
					return err
				}
				lastErr = err
				poolIDs = nil
				if attempt == o.cfg.FetchRetryAttempts-1 {
					break
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(o.cfg.FetchRetryDelay):
				}
				continue
			}
			poolsResolved = true
		}
		_, err := FetchEpochWithPools(
			ctx,
			o.koios,
			o.cache,
			o.cfg.Network,
			epoch,
			poolIDs,
			firstActiveEpochs,
			o.cfg.Logger,
		)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrKoiosPermanent) {
			return err
		}
		lastErr = err
		if attempt == o.cfg.FetchRetryAttempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(o.cfg.FetchRetryDelay):
		}
	}
	return fmt.Errorf(
		"epoch %d: fetch failed after %d attempt(s): %w",
		epoch, o.cfg.FetchRetryAttempts, lastErr,
	)
}

// fetchAccountsIfNeeded fetches #3097's per-account Koios reference data for
// epoch only if koios_account_coverage is not already marked complete for
// it — independent of fetchPoolsIfNeeded's own koios_epoch_info gate, so
// turning AccountsEnabled on after pool-level data was already fetched for
// this epoch still triggers an account fetch rather than being silently
// skipped.
//
// Koios's full account-address list is resolved at most once across this
// call's retry loop (mirroring fetchPoolsIfNeeded's identical pool-universe
// resolution pattern) and unioned with Dingo's own known addresses via
// BuildAccountAddressUniverse on every attempt (cheap: a single in-process
// RewardParitySource call, not a Koios request). A cached pre-staking marker
// returns before both the coverage lookup and account-universe request: those
// epochs have no account parity surface and intentionally no coverage row.
func (o *Observer) fetchAccountsIfNeeded(
	ctx context.Context,
	epoch uint64,
) error {
	info, infoErr := o.cache.GetEpochInfo(o.cfg.Network, epoch)
	if infoErr != nil && !errors.Is(infoErr, sql.ErrNoRows) {
		return fmt.Errorf("get epoch info before account fetch: %w", infoErr)
	}
	if info != nil && info.PreStaking {
		return nil
	}

	cov, covErr := o.cache.GetAccountCoverage(o.cfg.Network, epoch)
	// sql.ErrNoRows ("no fetch attempted yet") is legitimately incomplete
	// coverage and falls through to the fetch loop below; any other error is
	// a genuine cache/DB failure and must propagate rather than being
	// silently treated as "needs fetching".
	if covErr != nil && !errors.Is(covErr, sql.ErrNoRows) {
		return fmt.Errorf("get account coverage: %w", covErr)
	}
	if covErr == nil && cov != nil && cov.Complete {
		return nil
	}

	var koiosAddrs []string
	var addrsResolved bool
	var lastErr error
	for attempt := 0; attempt < o.cfg.FetchRetryAttempts; attempt++ {
		if !addrsResolved {
			// The epoch's own end time is what the cached crawl has to be
			// no older than; see ResolveKoiosAccountUniverseCached.
			var notBefore time.Time
			if info != nil {
				notBefore = info.EpochEndTime
			}
			addrs, err := ResolveKoiosAccountUniverseCached(
				ctx, o.koios, o.cache, o.cfg.Network, notBefore, o.cfg.Logger,
			)
			if err != nil {
				if errors.Is(err, ErrKoiosPermanent) {
					return err
				}
				lastErr = err
				if attempt == o.cfg.FetchRetryAttempts-1 {
					break
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(o.cfg.FetchRetryDelay):
				}
				continue
			}
			koiosAddrs = addrs
			addrsResolved = true
		}
		_, err := FetchEpochAccountsWithAddrs(
			ctx,
			o.koios,
			o.cache,
			o.cfg.Network,
			epoch,
			o.cfg.Source,
			koiosAddrs,
			o.cfg.GraceHours,
			o.cfg.AccountChunkSize,
			o.cfg.AccountChunkMaxBytes,
			false,
			o.cfg.Logger,
		)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrKoiosPermanent) {
			return err
		}
		lastErr = err
		if attempt == o.cfg.FetchRetryAttempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(o.cfg.FetchRetryDelay):
		}
	}
	return fmt.Errorf(
		"epoch %d: account fetch failed after %d attempt(s): %w",
		epoch, o.cfg.FetchRetryAttempts, lastErr,
	)
}

// fail logs a per-epoch failure and, in strict mode, fires FatalFunc exactly
// once (the first failure across the observer's lifetime). run and
// runAccounts's goroutines can both call this concurrently (dingo #4339's
// queue split), so the exactly-once guarantee is enforced with an atomic
// compare-and-swap on fatalFired rather than a plain read-then-write, which
// could otherwise let both goroutines' near-simultaneous first failures each
// observe fatalFired as false and both invoke FatalFunc.
func (o *Observer) fail(epoch uint64, err error) {
	o.cfg.Logger.Error(
		"koiosparity observer: epoch validation failed",
		"network",
		o.cfg.Network,
		"epoch",
		epoch,
		"error",
		err,
		"strict",
		o.cfg.Strict,
	)
	if !o.cfg.Strict {
		return
	}
	if !o.fatalFired.CompareAndSwap(false, true) {
		return
	}
	if o.cfg.FatalFunc != nil {
		o.cfg.FatalFunc(fmt.Errorf("koios parity: epoch %d: %w", epoch, err))
	}
}
