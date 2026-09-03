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
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/event"
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
	// NewKoiosClient. Empty selects the public host.
	BaseURL string
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
// run's background goroutine, which does the actual work. This keeps
// EventBus dispatch to this subscriber fast regardless of how long a Koios
// fetch takes, and (together with node.go's own "authoritative
// epoch-boundary snapshot capture happens inside the write transaction;
// event.Publish happens after Unlock" ordering) guarantees Koios/network
// I/O only ever starts after the epoch-boundary transaction has committed
// and the ledger lock has been released — this code never acquires it.
type Observer struct {
	cfg   ObserverConfig
	cache *Cache
	koios *KoiosClient

	mu      sync.Mutex
	pending map[uint64]struct{} // epochs requested for (re)validation

	wake chan struct{}
	wg   sync.WaitGroup

	// cancel stops run's background goroutine. It is the CancelFunc of the
	// context derived (in Start) from the ctx passed to Start, rather than a
	// channel Stop closes directly — a context.CancelFunc is inherently safe
	// to invoke more than once (subsequent calls are no-ops), which is what
	// lets Stop be called more than once on the same Observer (e.g. once from
	// node.go's started-stack cleanup on a startup failure, and again from
	// node_shutdown.go's normal shutdown path) without panicking the way
	// closing an already-closed channel would. nil until Start succeeds, so
	// Stop guards against calling a nil func when Start was never called.
	cancel context.CancelFunc

	// fatalFired is set once FatalFunc has been called for a strict-mode
	// failure. Only ever read/written from run's single goroutine (via
	// processEpoch/fail/stopping), so it needs no lock of its own.
	fatalFired bool

	// started guards against calling Start more than once on the same
	// Observer: a second call would silently overwrite o.cancel (orphaning
	// the first run goroutine, which nothing would ever cancel/wait on
	// again) and launch a second concurrent run goroutine racing the first
	// over o.pending/o.cache. Checked and set under o.mu.
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
	koios, err := NewKoiosClient(cfg.Network, cfg.APIKey, cfg.BaseURL)
	if err != nil {
		_ = cache.Close()
		return nil, fmt.Errorf("create koios client: %w", err)
	}

	return &Observer{
		cfg:     cfg,
		cache:   cache,
		koios:   koios,
		pending: make(map[uint64]struct{}),
		wake:    make(chan struct{}, defaultQueueBuffer),
	}, nil
}

// Start seeds the observer's backlog with every already-closed epoch Dingo
// has committed reward state for but this cache has not yet checked
// (backfilling full history for a fresh attach or a restart, using the
// cache's own persisted check/fetch status as the sole resumable checkpoint
// — no separate checkpoint file is introduced), then launches the
// background goroutine that drains pending epochs. Subscribe the returned
// Observer's HandleEpochTransitionEvent to event.EpochTransitionEventType
// before or after calling Start; live events and the seeded backlog feed
// the same pending set either way.
//
// Start may only be called once per Observer; a second call returns an
// error rather than silently orphaning the first run goroutine (construct a
// new Observer instead, e.g. via NewObserver, if a fresh Start is needed).
func (o *Observer) Start(ctx context.Context) error {
	o.mu.Lock()
	if o.started {
		o.mu.Unlock()
		return errors.New("koiosparity: Observer.Start called more than once")
	}
	o.started = true
	o.mu.Unlock()

	latest, err := o.cfg.Source.GetLatestEpoch(ctx)
	if err != nil {
		o.cfg.Logger.Debug(
			"koiosparity observer: no committed epoch data yet at startup",
			"error", err,
		)
	} else if latest > 0 {
		// latest is Dingo's own current (most recently started) epoch; only
		// epochs strictly before it are safely closed — mirrors Fetch's own
		// "tipEpoch - 1" bound.
		throughEpoch := latest - 1
		needing, err := o.cache.GetEpochsNeedingCheck(
			o.cfg.Network,
			o.cfg.AccountsEnabled,
		)
		if err != nil {
			return fmt.Errorf("seed koiosparity observer backlog: %w", err)
		}
		uncached, err := o.cache.GetUncachedEpochs(o.cfg.Network, 0, throughEpoch)
		if err != nil {
			return fmt.Errorf("seed koiosparity observer backlog: %w", err)
		}
		o.mu.Lock()
		for _, e := range needing {
			if e <= throughEpoch {
				o.pending[e] = struct{}{}
			}
		}
		for _, e := range uncached {
			o.pending[e] = struct{}{}
		}
		o.mu.Unlock()

		// An epoch whose pool data/check status is already fine can still be
		// missing #3097's per-account coverage entirely (e.g. it was fetched
		// before AccountsEnabled was turned on) — neither GetEpochsNeedingCheck
		// nor GetUncachedEpochs above would ever flag it purely for that
		// reason once accountsEnabled's own staleness branch is satisfied by a
		// prior check. Add those epochs to the backlog too, independent of why
		// GetEpochsNeedingCheck/GetUncachedEpochs may or may not have already
		// selected them, the same way fetchIfNeeded's fetchAccountsIfNeeded
		// gates on account coverage independently of fetchPoolsIfNeeded.
		if o.cfg.AccountsEnabled {
			missingAccounts, err := o.cache.GetEpochsMissingAccountCoverage(
				o.cfg.Network,
				0,
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
				o.pending[e] = struct{}{}
			}
			o.mu.Unlock()
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	o.cancel = cancel

	o.wg.Go(func() {
		o.run(runCtx)
	})
	o.signalWake()
	return nil
}

// Stop cancels the observer's background processing and releases its cache
// and Koios client. It always blocks until run's goroutine has actually
// exited before closing the cache — closing the cache out from under a
// still-running goroutine would race that goroutine's own cache queries, and
// the caller's node.go composition depends on Stop returning only once it is
// safe to tear down the database/blob store the source reads from. ctx only
// bounds how long Stop waits *quietly*: once ctx expires, Stop logs a warning
// that in-flight work is taking longer than expected but keeps waiting for
// run to actually exit (the goroutine itself still exits promptly once the
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
// background goroutine; see the Observer doc comment for why it must never
// do slower work itself.
func (o *Observer) HandleEpochTransitionEvent(evt event.Event) {
	data, ok := evt.Data.(event.EpochTransitionEvent)
	if !ok {
		return
	}
	o.mu.Lock()
	o.pending[data.PreviousEpoch] = struct{}{}
	o.mu.Unlock()
	o.signalWake()
}

func (o *Observer) signalWake() {
	select {
	case o.wake <- struct{}{}:
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

// stopping reports whether a strict-mode failure has already fired
// FatalFunc, so run's loop stops promptly instead of processing further
// epochs after strict validation has already failed.
func (o *Observer) stopping() bool {
	return o.fatalFired
}

// processEpoch fetches (if not already cached) and checks exactly one
// epoch, then reports/records the outcome. Strict-mode cancellation is
// triggered here, once, on the first failure.
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
		o.cfg.AccountsEnabled,
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
		o.fail(epoch, fmt.Errorf(
			"parity %s at epoch %d (%d mismatch(es))",
			result.Status, epoch, len(result.Mismatches),
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

// fetchIfNeeded fetches Koios reference data for epoch only if it is not
// already cached — a historical epoch's Koios reference never changes, so a
// re-request (e.g. after a Dingo-side rollback re-signals the same epoch)
// would just be wasted work. It fetches the pool-level reference data first
// (fetchPoolsIfNeeded), then — when cfg.AccountsEnabled — the per-account
// reference data (fetchAccountsIfNeeded, #3097), since the two are gated by
// independent cache state (koios_epoch_info presence vs.
// koios_account_coverage completeness) and either can need (re)fetching
// independently of the other (e.g. AccountsEnabled being turned on after
// pool-level data for this epoch was already fetched).
func (o *Observer) fetchIfNeeded(ctx context.Context, epoch uint64) error {
	if err := o.fetchPoolsIfNeeded(ctx, epoch); err != nil {
		return err
	}
	if !o.cfg.AccountsEnabled {
		return nil
	}
	return o.fetchAccountsIfNeeded(ctx, epoch)
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
// once (the first failure across the observer's lifetime) — only ever
// called from run's single goroutine, so fatalFired needs no lock.
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
	if !o.cfg.Strict || o.fatalFired {
		return
	}
	o.fatalFired = true
	if o.cfg.FatalFunc != nil {
		o.cfg.FatalFunc(fmt.Errorf("koios parity: epoch %d: %w", epoch, err))
	}
}
