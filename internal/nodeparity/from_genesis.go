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

package nodeparity

// RunFromGenesis orchestrates the Koios-backed comparison (koios_check.go's
// doc comment) end to end: follows dingoAddr's chain from genesis, and at
// every epoch boundary runs all three checks against Koios, each on its own
// Acquire (see koios_check.go's doc comment for why they must not share
// one). Runs until ctx is cancelled: a chainsync session ending for any
// other reason (a dial failure, a currentEpochNo error, a protocol-level
// session drop -- none of which indicate a real divergence, which is
// reported through EpochResult instead) is not fatal -- see the reconnect
// loop's own doc comment further down.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"golang.org/x/sync/errgroup"
)

// txInfoConcurrency bounds how many /tx_info batch requests
// flushPendingTxInfos has in flight at once -- matches
// koios_check.go's stakeCheckConcurrency: confirmed live against the same
// Koios mirror that individual requests stall often enough (not just a
// rare one-off) that sequential batches stack their stalls additively
// (e.g. 3 stalled batches in one epoch costing 3x a single stall's
// latency). Concurrency lets independent batches overlap instead.
const txInfoConcurrency = 8

// txInfoOpportunisticFlushThreshold is how many hashes accumulate before
// flushPendingTxInfos is called mid-epoch (a forced flush still always
// happens right before each epoch-boundary comparison regardless of this
// threshold -- see that call site). Sized to give the concurrent flush
// below a full set of batches to fan out, rather than flushing a single
// koiosparity.KoiosTxInfoBatchSize-sized batch at a time with nothing to
// parallelize.
const txInfoOpportunisticFlushThreshold = txInfoConcurrency * koiosparity.KoiosTxInfoBatchSize

// currentEpochNo Acquires point and asks Dingo directly which epoch it
// falls in (queryShelleyEpochNo, an unbounded query with no retention
// floor), rather than computing it client-side from the raw slot number.
//
// Dividing block.SlotNumber() by a per-network epoch-length constant is
// wrong on preprod, and not fixable by simply using the right constant:
// preprod's Byron era ran for 4 real epochs
// (verified against config/cardano/preprod/config.json, which carries no
// TestShelleyHardForkAtEpoch, unlike preview's explicit 0) at Byron's own
// epoch length (10*k Byron slots, k=2160 -> 21600 slots/epoch, at Byron's
// own 20s slot duration -- 86400 raw slots in, exactly 4 Byron epochs),
// not Shelley's post-hard-fork 432000-slot epoch. No client-side arithmetic
// over the raw slot number alone can account for a per-network,
// historically-fixed Byron-era length without hardcoding it separately --
// asking Dingo, which already resolves this correctly, avoids needing to
// know it at all. Byron's own startTime equaling Shelley's systemStart in
// both networks' genesis configs is not proof of a zero-length Byron era:
// it holds on mainnet too, where Byron ran 208 real epochs -- systemStart is
// the slot-zero wall-clock reference, unrelated to when the Shelley hard
// fork actually happened.
func currentEpochNo(
	ctx context.Context,
	dingoAddr string,
	magic uint32,
	point pcommon.Point,
) (uint64, error) {
	conn, lsq, err := acquireWithRetry(ctx, dingoAddr, magic, point)
	if err != nil {
		return 0, err
	}
	defer conn.Close() //nolint:errcheck
	epochNo, err := lsq.Client.GetEpochNo()
	if err != nil {
		return 0, fmt.Errorf("GetEpochNo: %w", err)
	}
	if epochNo < 0 {
		return 0, fmt.Errorf("GetEpochNo: node reported a negative epoch %d", epochNo)
	}
	_ = lsq.Client.Release() //nolint:errcheck
	return uint64(epochNo), nil
}

// acquireRetries and acquireRetryDelay bound how long RunFromGenesis retries
// an Acquire that failed with ErrAcquireFailurePointNotOnChain: a brand-new
// connection's own view of the chain can briefly lag the ChainSync
// connection that just delivered this exact block a moment ago (confirmed
// live: without retrying, "point not on chain" fired on nearly every epoch,
// even freshly dialed -- a propagation-delay race, not a permanent
// rejection). ErrAcquireFailurePointTooOld is never retried: more attempts
// only give Dingo's retention floor more time to advance, making a
// genuinely-too-old point even more too-old, never less.
//
// The same budget also bounds retrying a failed Dial itself (confirmed
// live: a v9 from-genesis run died at epoch 129/129 clean on a bare "dial:
// ouroboros.New: connection shutdown initiated: EOF" -- a one-off transient
// hiccup, not a real divergence, but currentEpochNo's caller treats any
// error from acquireWithRetry as fatal to the whole run, unlike the
// protocol-params/stake and UTxO call sites, which already record an
// acquireWithRetry failure into that epoch's EpochResult and continue).
// Unlike ErrAcquireFailurePointNotOnChain, a dial failure carries no signal
// about whether retrying helps, so it gets the same fixed budget rather than
// a special-cased one.
const (
	acquireRetries    = 10
	acquireRetryDelay = 200 * time.Millisecond
)

// protocolParamsAndStakeRetries/protocolParamsAndStakeRetryDelay bound how
// many times runProtocolParamsAndStake redials and retries the whole
// {Acquire, CheckProtocolParams, CheckStakeDistribution} sequence when the
// shared connection dies during or between the two checks -- see that
// function's own doc comment. Matches acquireRetries/acquireRetryDelay's
// existing budget (2s worst case) rather than inventing a second one.
const (
	protocolParamsAndStakeRetries    = acquireRetries
	protocolParamsAndStakeRetryDelay = acquireRetryDelay
)

// isRetryableDingoConnErr reports whether err looks like the shared
// connection itself died -- gouroboros's protocol.ErrProtocolShuttingDown
// (returned once a peer/mux teardown has already happened -- see
// localstatequery.Client's own doc comments in gouroboros), or a raw
// EOF/closed-connection error from beneath it -- as opposed to a genuine
// query-level failure (a malformed response, an era that cannot be
// resolved, a Koios-side error) that redialing cannot fix. Only the former
// is worth runProtocolParamsAndStake's whole-sequence retry: retrying the
// latter would just burn the retry budget for no benefit, and could mask a
// real bug behind "looks like churn."
func isRetryableDingoConnErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, protocol.ErrProtocolShuttingDown) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed)
}

// callbackErr builds the error a chainsync roll-forward or roll-backward
// callback returns, flattening any cause that satisfies
// errors.Is(err, protocol.ErrProtocolShuttingDown) into a plain error
// carrying the same text.
//
// gouroboros' recvLoop treats a callback error matching that sentinel as a
// graceful shutdown and returns WITHOUT calling SendError
// (protocol/protocol.go), so such an error ends the chainsync session
// without ever reaching csConn.ErrorChan(). RunFromGenesis's session select
// would then block forever: no reconnect, no epoch report, no error out of
// the run. Every error these callbacks produce is a session-ending fault
// this function's caller must see, never a graceful stop, so none of them
// may carry that sentinel out.
func callbackErr(format string, args ...any) error {
	err := fmt.Errorf(format, args...)
	if !errors.Is(err, protocol.ErrProtocolShuttingDown) {
		return err
	}
	return errors.New(err.Error())
}

// runProtocolParamsAndStake runs CheckProtocolParams then
// CheckStakeDistribution against dingoAddr at point, on one shared
// connection/Acquire per attempt -- see koios_check.go's doc comment for why
// the two share an Acquire rather than the UTxO check's own separate one.
//
// A connection that dies during or between the two calls
// (isRetryableDingoConnErr) is not a hard failure: the whole pair is retried
// against a freshly redialed connection, up to protocolParamsAndStakeRetries
// times, the same persistent-connection pattern incrementalSession uses
// (dingo#4183: hold one connection per attempt, and recover from its death by
// reconnecting).
func runProtocolParamsAndStake(
	ctx context.Context,
	dingoAddr string,
	magic uint32,
	point pcommon.Point,
	koios *koiosparity.KoiosClient,
	cache *koiosparity.Cache,
	network string,
	epoch uint64,
) (
	ppMismatches []koiosparity.CheckMismatch,
	ppErr error,
	stakeMismatches []StakeMismatch,
	stakeErr error,
) {
	for attempt := 0; attempt < protocolParamsAndStakeRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err(), nil, ctx.Err()
			case <-time.After(protocolParamsAndStakeRetryDelay):
			}
		}

		conn, lsq, err := acquireWithRetry(ctx, dingoAddr, magic, point)
		if err != nil {
			// acquireWithRetry already retried a point-not-on-chain race
			// internally -- a failure here is either ctx cancellation or a
			// non-retryable acquire failure (e.g. the point has aged past
			// Dingo's retention floor), neither of which this loop's own
			// retry can do anything about. Neither check ran at all, so
			// reporting the same err for both is correct here (unlike the
			// exhausted-retries return below, where the two checks DO run
			// independently and must keep their own outcomes).
			return nil, err, nil, err
		}

		// Both checks run every attempt, regardless of whether the first
		// failed -- matching the prior code's own behavior of always
		// attempting CheckStakeDistribution even when CheckProtocolParams
		// errored (e.g. a non-connection era-resolution failure leaves the
		// connection itself perfectly usable for the stake query).
		ppMismatches, ppErr = CheckProtocolParams(ctx, lsq.Client, koios, cache, network, epoch)
		stakeMismatches, stakeErr = CheckStakeDistribution(
			ctx, lsq.Client, koios, cache, network, epoch,
		)
		_ = lsq.Client.Release() //nolint:errcheck
		conn.Close()             //nolint:errcheck

		if isRetryableDingoConnErr(ppErr) || isRetryableDingoConnErr(stakeErr) {
			continue
		}
		return ppMismatches, ppErr, stakeMismatches, stakeErr
	}
	// Retries exhausted: each check keeps its own outcome from the final
	// attempt, never a single conflated error forced into both slots. ppErr
	// may legitimately be nil here -- protocol params succeeded on that
	// attempt and only stakeErr kept the loop going -- and reporting
	// anything else would attribute the stake connection failure to
	// protocol params too.
	return ppMismatches, ppErr, stakeMismatches, stakeErr
}

// EpochResult reports one epoch's check outcomes. A nil error paired with a
// nil/empty mismatch value means that check ran cleanly; a non-nil error
// means it could not run at all (most commonly an Acquire failure once the
// point has aged past Dingo's retention floor -- expected once a from-genesis
// replay has run long enough, not itself a failure of this tool).
type EpochResult struct {
	Epoch uint64

	ProtocolParamsErr        error
	ProtocolParamsMismatches []koiosparity.CheckMismatch

	StakeErr        error
	StakeMismatches []StakeMismatch

	// UTxOAttempted is false whenever the genesis baseline was never
	// captured (see captureGenesisBaseline) -- the fields below are
	// meaningless in that case.
	UTxOAttempted bool
	UTxOErr       error
	UTxOMissing   []string
	UTxOExtra     []string
	UTxODiffers   []string
	UTxORefCount  int

	// Timing breakdown, purely diagnostic: which phase actually spent the
	// wall-clock time this epoch. Any of the three can dominate depending on
	// how much chain activity the epoch carried, so the split is reported
	// every epoch rather than inferred.
	TxInfoFlushCount              int
	TxInfoFlushElapsed            time.Duration
	ProtocolParamsAndStakeElapsed time.Duration
	UTxOElapsed                   time.Duration
}

// FromGenesisReporter receives one EpochResult per epoch boundary
// RunFromGenesis observes, as soon as that epoch's checks finish -- a
// caller (cmd/node-parity) renders/logs/exports metrics for it however it
// wants. Called synchronously from RunFromGenesis's own ChainSync callback,
// so it must not block for long: do expensive reporting (network calls, file
// I/O) on a separate goroutine if needed.
type FromGenesisReporter func(EpochResult)

// acquireWithRetry dials a fresh connection and Acquires point on it,
// retrying both a failed Dial and an Acquire that failed with
// ErrAcquireFailurePointNotOnChain -- see acquireRetries' doc comment.
// Returns ok=false, with any connection it opened already closed, on any
// failure other than context cancellation, on which it returns ctx.Err()
// directly rather than a wrapped dial/acquire error.
func acquireWithRetry(
	ctx context.Context,
	dingoAddr string,
	magic uint32,
	point pcommon.Point,
) (conn *ouroboros.Connection, lsq *localstatequery.LocalStateQuery, err error) {
	var lastErr error
	for attempt := 0; attempt < acquireRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-time.After(acquireRetryDelay):
			}
		}

		dialConn, dialErr := Dial(ctx, dingoAddr, magic)
		if dialErr != nil {
			lastErr = fmt.Errorf("dial: %w", dialErr)
			continue
		}

		dialLsq := dialConn.LocalStateQuery()
		if dialLsq == nil || dialLsq.Client == nil {
			dialConn.Close() //nolint:errcheck
			lastErr = errors.New("LocalStateQuery client unavailable")
			continue
		}

		acquireErr := dialLsq.Client.Acquire(&point)
		if acquireErr == nil {
			return dialConn, dialLsq, nil
		}
		dialConn.Close() //nolint:errcheck
		lastErr = fmt.Errorf("acquire: %w", acquireErr)
		if !errors.Is(acquireErr, localstatequery.ErrAcquireFailurePointNotOnChain) {
			return nil, nil, lastErr
		}
	}
	return nil, nil, lastErr
}

// captureGenesisBaseline Acquires point (the very first block RunFromGenesis
// observed) and returns Dingo's own whole UTxO set at it, to seed the
// running Koios-derived reconstruction. Trusted directly from Dingo rather
// than independently re-derived (e.g. from Preview/preprod's
// byron-genesis.json UTxO-hash algorithm): genesis UTxO derivation is
// deterministic and not the bug surface this comparison exists to catch,
// and every subsequent entry in the reconstruction comes from Koios's own
// /tx_info data, not from Dingo, preserving the comparison's independence
// for everything that actually happens on-chain.
//
// A failure here (most commonly the point having already aged out of
// Dingo's UTxO retention floor before this call ran -- see
// checkUtxoRetentionWindow, or a transient dial/connection error) is
// non-fatal to the caller: the UTxO half of this comparison is simply
// unavailable this epoch, but protocol-params and stake-distribution
// checking continues regardless. It is not permanently disabled for the
// rest of the run -- RunFromGenesis retries this same call again at the
// next epoch boundary for as long as no trustworthy baseline is held (see
// its own utxoRefs == nil retry, keyed off this function's own failure),
// so a transient failure (e.g. a one-off dial error) recovers on its own
// once the underlying issue clears, instead of wedging UTxO comparison off
// forever.
func captureGenesisBaseline(
	ctx context.Context,
	dingoAddr string,
	magic uint32,
	point pcommon.Point,
) (UTxOSet, error) {
	conn, lsq, err := acquireWithRetry(ctx, dingoAddr, magic, point)
	if err != nil {
		return nil, err
	}
	defer conn.Close() //nolint:errcheck
	utxos, err := lsq.Client.GetUTxOWhole()
	if err != nil {
		return nil, fmt.Errorf("genesis GetUTxOWhole: %w", err)
	}
	_ = lsq.Client.Release() //nolint:errcheck
	set := make(UTxOSet, len(utxos.Results))
	for id, out := range utxos.Results {
		key := fmt.Sprintf("%s#%d", id.Hash.String(), id.Idx)
		set[key] = canonicalUTxOEntry(out)
	}
	return set, nil
}

// errUTxOTainted is returned by utxoVerdict for utxoVerdictTainted -- see
// that constant's doc comment.
var errUTxOTainted = errors.New(
	"tx_info fetch failed during this epoch; " +
		"UTxO reconstruction was re-baselined from " +
		"Dingo directly, so this epoch's comparison " +
		"would be against Dingo itself and was skipped",
)

// utxoVerdictMode is utxoVerdict's per-epoch decision of which UTxO-check
// outcome applies this epoch.
type utxoVerdictMode int

const (
	// utxoVerdictNoBaseline: the genesis baseline was never captured (or
	// the most recent attempt to capture/re-baseline it failed) -- the
	// UTxO half of this comparison is unavailable this epoch. Not
	// permanent: RunFromGenesis retries captureGenesisBaseline again at
	// the next epoch boundary for as long as utxoRefs stays nil, so this
	// verdict can give way to utxoVerdictTainted (a successful retry,
	// tainting the recovery epoch itself) or utxoVerdictCompare (once a
	// trustworthy baseline is held again) on a later epoch.
	utxoVerdictNoBaseline utxoVerdictMode = iota
	// utxoVerdictTainted: a tx_info failure this epoch forced a re-baseline
	// from Dingo's own answer (see flushPendingTxInfos) -- comparing now
	// would silently pass by construction, not confirm anything against
	// Koios.
	utxoVerdictTainted
	// utxoVerdictCompare: run the real Dingo-vs-reconstruction comparison.
	utxoVerdictCompare
)

// utxoVerdict decides utxoVerdictMode purely from local state -- no network
// I/O -- extracted from RunFromGenesis's roll-forward callback specifically
// so the regression it fixes is directly assertable: a Koios outage during
// tx_info reconstruction being reported as a false "utxo set match" instead
// of "not run", because utxoTaintedThisEpoch was closure state inside a
// 500+ line function literal that no test exercised. Reverting the taint
// check this function replaces would make TestUTxOVerdict's tainted case
// fail.
//
// utxoBaselineErr is not a parameter: whether the genesis baseline capture
// itself failed (utxoBaselineErr != nil) or was simply never attempted yet
// (both nil), the outcome is identical -- utxoVerdictNoBaseline, with
// UTxOAttempted left false and no error surfaced on EpochResult, matching
// RunFromGenesis's behavior before this extraction. A caller that wants to
// log the baseline failure's cause does so once, from utxoBaselineErr
// directly, at the point that error was captured -- not from this
// per-epoch decision.
func utxoVerdict(
	tainted bool,
	utxoRefs UTxOSet,
) (mode utxoVerdictMode, err error) {
	switch {
	case tainted:
		return utxoVerdictTainted, errUTxOTainted
	case utxoRefs != nil:
		return utxoVerdictCompare, nil
	default:
		return utxoVerdictNoBaseline, nil
	}
}

// nextSessionRetryDelay computes how long RunFromGenesis's reconnect loop
// should wait before its next reconnect attempt (sleepFor), and what delay
// the attempt after that should use if it also fails (nextDelay).
// progressed being true resets the backoff to base rather than letting it
// keep growing across a run that is mostly healthy with only occasional
// hiccups -- growth (doubling, capped at maxDelay) is reserved for a
// session that fails immediately on every reconnect attempt (e.g. Dingo
// genuinely down).
func nextSessionRetryDelay(
	delay time.Duration,
	progressed bool,
	base time.Duration,
	maxDelay time.Duration,
) (sleepFor, nextDelay time.Duration) {
	if progressed {
		delay = base
	}
	return delay, min(delay*2, maxDelay)
}

// applyTxInfoResults applies each successful chunk's tx_info changes to
// utxoRefs and reports whether any chunk failed -- pulled out of
// flushPendingTxInfos, which threads that result straight into
// utxoTaintedThisEpoch, so a test can drive the actual failure decision
// directly with synthetic chunks/results/errs instead of needing a real
// Koios server: the fix for a Koios outage being reported as a false "utxo
// set match" was pinned only at utxoVerdict, the function that reads
// utxoTaintedThisEpoch, never at anything that decides what sets it. Does
// no network I/O itself -- chunks/results/errs are already-fetched,
// matching the same already-fetched-input shape evaluatePoolStake uses for
// the identical reason.
func applyTxInfoResults(
	utxoRefs UTxOSet,
	chunks [][]string,
	results [][]koiosparity.KoiosTxInfoItem,
	errs []error,
	logf func(format string, args ...any),
) (anyFailed bool) {
	for i, chunk := range chunks {
		if err := errs[i]; err != nil {
			logf(
				"nodeparity: koios tx_info fetch failed for %d pending tx(es): %v",
				len(chunk), err,
			)
			anyFailed = true
			continue
		}
		UTxOChanges(utxoRefs, results[i])
	}
	return anyFailed
}

// resolveStartPoint converts resumeFrom into the chain point RunFromGenesis
// should start its ChainSync session (and UTxO baseline capture) from,
// defaulting to Origin when resumeFrom is nil.
func resolveStartPoint(resumeFrom *Tip) (pcommon.Point, error) {
	if resumeFrom == nil {
		return pcommon.NewPointOrigin(), nil
	}
	resolved, err := resumeFrom.point()
	if err != nil {
		return pcommon.Point{}, fmt.Errorf("resume point: %w", err)
	}
	return resolved, nil
}

// RunFromGenesis is documented at the top of this file. cache, when non-nil,
// is threaded into every Koios-backed check: CheckProtocolParams and
// CheckStakeDistribution per epoch, and the UTxO reconstruction's own
// /tx_info fetches per chunk (fetchTxInfosCached, called from
// flushPendingTxInfos below) -- see koios_check.go's doc comments on those
// three for what gets cached and why it is safe for dingo's own embedded
// koios-parity observer to be writing the same cache.db concurrently.
//
// The reconstruction itself stays a stateful, order-dependent walk: what is
// cached is only each transaction's own immutable /tx_info answer, keyed by
// transaction hash rather than by epoch, so a re-run from genesis replays the
// same walk without re-asking Koios about transactions it has already seen.
func RunFromGenesis(
	ctx context.Context,
	dingoAddr string,
	network string,
	magic uint32,
	koios *koiosparity.KoiosClient,
	cache *koiosparity.Cache,
	report FromGenesisReporter,
	logf func(format string, args ...any),
	resumeFrom *Tip,
) error {
	if !KoiosNetworks[network] {
		return fmt.Errorf(
			"koios-backed comparison only supports preview or preprod, got %q",
			network,
		)
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}

	startPoint, err := resolveStartPoint(resumeFrom)
	if err != nil {
		return err
	}

	var (
		lastEpoch          uint64
		haveLastEpoch      bool
		utxoRefs           UTxOSet
		utxoAttempted      bool
		pendingTxHashes    []string
		txInfoFlushCount   int
		txInfoFlushElapsed time.Duration
		// lastPoint is the most recent chain point either callback below has
		// started processing, updated before any of that point's own work
		// runs (see the reconnect loop's doc comment further down) -- a
		// fresh session after a reconnect resumes chainsync from here
		// instead of Origin, so hours of already-verified epochs are never
		// replayed. Starts at startPoint instead of Origin when resumeFrom
		// is set (dingo#4152 follow-up: a killed node-parity process has no
		// on-disk checkpoint, so a caller that already trusts a prior run's
		// epochs up to some point passes it back in here to skip re-deriving
		// them, the same way a mid-run reconnect already skips re-deriving
		// anything before its own lastPoint) -- captureGenesisBaseline is
		// keyed off this same variable's first value, so the UTxO
		// reconstruction is seeded from Dingo's live answer at startPoint
		// rather than genesis too, with no separate code path needed.
		lastPoint = startPoint
		// progressed records whether the current chainsync session has
		// successfully processed at least one point since it was
		// (re)established -- see sessionRetryDelay's doc comment for why
		// this gates resetting the reconnect backoff.
		progressed bool
		// utxoTaintedThisEpoch is set whenever a tx_info failure forced a
		// mid-epoch re-baseline (see flushPendingTxInfos), a rollback forced
		// one (see the roll-backward callback below), or a previously failed
		// baseline capture is successfully retried at this epoch's own
		// boundary (see the utxoRefs == nil retry just before the verdict
		// switch below) -- and cleared once that epoch's result has been
		// reported. Each of these replaces utxoRefs with Dingo's own current
		// answer, which the epoch comparison below would otherwise diff
		// against Dingo's own answer again -- trivially equal by
		// construction, not a real confirmation that Koios agrees with
		// anything. Without this flag, a Koios outage (or a transient
		// baseline-capture failure) during an epoch would be reported as
		// "utxo set match" instead of "not run": the re-baseline is the
		// right recovery for later epochs, but this epoch's own verdict must
		// say the comparison did not happen.
		utxoTaintedThisEpoch bool
	)

	// flushPendingTxInfos applies every buffered transaction hash's
	// input/output changes to utxoRefs. Hashes accumulate across blocks
	// rather than being fetched per block: the cost of a /tx_info lookup is
	// dominated by the round trip to Koios, not by payload size, so one call
	// per block with a transaction in it collapses a run's epoch cadence
	// from seconds to tens of minutes once chain activity picks up.
	//
	// Splits pendingTxHashes into koiosparity.KoiosTxInfoBatchSize-sized
	// chunks and fetches them with bounded concurrency (txInfoConcurrency)
	// rather than one at a time: confirmed live that individual requests to
	// this Koios mirror stall often enough that sequential batches stack
	// their stalls additively (multiple ~20s-60s stalls in a single busy
	// epoch, one after another). Concurrent fetching lets independent
	// batches' stalls overlap instead.
	//
	// Chunk results are applied to utxoRefs in their original chunk order
	// (not completion order) once every fetch finishes: two batches can
	// still be causally related (a UTxO created in an earlier block and
	// spent in a later one, both pending at flush time), so applying them
	// out of order could silently no-op a spend against a UTxO that
	// (out of order) looks like it doesn't exist yet.
	//
	// point is the chain point the caller is currently at (the block just
	// processed), used only to re-baseline if any chunk fails: a failed
	// chunk means the reconstruction is now missing an unknown set of
	// spends/creates, so comparing it against Dingo's live answer could
	// report a false divergence. Re-baselining at the current point --
	// reusing captureGenesisBaseline, the same recovery already used on
	// rollback -- discards the untrustworthy incremental state in favor of
	// Dingo's own live truth, rather than silently comparing a
	// known-incomplete set.
	flushPendingTxInfos := func(point pcommon.Point) {
		if utxoRefs == nil || len(pendingTxHashes) == 0 {
			return
		}
		chunks := make([][]string, 0, (len(pendingTxHashes)+koiosparity.KoiosTxInfoBatchSize-1)/koiosparity.KoiosTxInfoBatchSize)
		for start := 0; start < len(pendingTxHashes); start += koiosparity.KoiosTxInfoBatchSize {
			end := min(start+koiosparity.KoiosTxInfoBatchSize, len(pendingTxHashes))
			chunks = append(chunks, pendingTxHashes[start:end])
		}
		results := make([][]koiosparity.KoiosTxInfoItem, len(chunks))
		errs := make([]error, len(chunks))

		flushStart := time.Now()
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(txInfoConcurrency)
		for i, chunk := range chunks {
			g.Go(func() error {
				txInfos, err := fetchTxInfosCached(
					gctx, koios, cache, network, chunk,
				)
				results[i] = txInfos
				errs[i] = err
				return nil
			})
		}
		_ = g.Wait() // errors are per-chunk in errs; nothing here to fail on
		txInfoFlushCount++
		txInfoFlushElapsed += time.Since(flushStart)

		failed := applyTxInfoResults(utxoRefs, chunks, results, errs, logf)
		pendingTxHashes = pendingTxHashes[:0]

		if failed {
			utxoTaintedThisEpoch = true
			refs, err := captureGenesisBaseline(ctx, dingoAddr, magic, point)
			if err != nil {
				utxoRefs = nil
				logf(
					"nodeparity: re-baseline after tx_info failure also failed "+
						"(UTxO comparison skipped this epoch, will retry at the "+
						"next epoch boundary): %v",
					err,
				)
			} else {
				utxoRefs = refs
				logf(
					"nodeparity: re-baselined the UTxO reconstruction after a "+
						"tx_info failure: %d refs",
					len(refs),
				)
			}
		}
	}

	// sessionRetryDelay/sessionRetryMaxDelay bound the backoff between
	// reconnect attempts after the chainsync session itself ends for a
	// reason unrelated to any real mismatch -- a raw dial failure,
	// ouroboros.New failing, cs.Client.Sync failing, or (confirmed live: a
	// from-genesis run 129 clean epochs in died outright on exactly this)
	// currentEpochNo returning an error, which gouroboros' chainsync client
	// treats identically to any other roll-forward/roll-backward callback
	// error -- tearing the whole session down rather than skipping one
	// block. None of these indicate a real UTxO/stake/protocol-params
	// divergence (those are already recorded per-epoch into EpochResult and
	// never reach here), so the right response is to reconnect and resume
	// from lastPoint, not to give up on the whole run. Only ctx
	// cancellation (the tool's normal Ctrl-C stop) exits this loop.
	//
	// The backoff resets to sessionRetryDelay once a session has made real
	// progress (progressed=true, i.e. it got at least one point past
	// currentEpochNo) before failing again, rather than growing without
	// bound over a run that is mostly healthy with only occasional
	// hiccups -- growth is reserved for a session that fails immediately on
	// every reconnect attempt (e.g. Dingo genuinely down).
	const (
		sessionRetryDelay    = 1 * time.Second
		sessionRetryMaxDelay = 30 * time.Second
	)
	delay := sessionRetryDelay

	// retryOrStop sleeps for delay (advanced per nextSessionRetryDelay),
	// returning ctx.Err() early if ctx is cancelled during the wait -- the
	// caller's loop must return immediately if ok is false. logMsg is
	// logged first, unless ctx is already done.
	retryOrStop := func(logMsg string, cause error) (ok bool, err error) {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		sleepFor, next := nextSessionRetryDelay(
			delay, progressed, sessionRetryDelay, sessionRetryMaxDelay,
		)
		logf(logMsg+" (retrying in %s): %v", sleepFor, cause)
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(sleepFor):
		}
		delay = next
		return true, nil
	}

	for {
		progressed = false

		rawConn, dialErr := dialRaw(ctx, protoFromAddr(dingoAddr), dingoAddr)
		if dialErr != nil {
			ok, err := retryOrStop(
				"nodeparity: dial raw chainsync connection failed",
				dialErr,
			)
			if !ok {
				return err
			}
			continue
		}

		csConn, connErr := ouroboros.New(
			ouroboros.WithConnection(rawConn),
			ouroboros.WithNetworkMagic(magic),
			ouroboros.WithNodeToNode(false),
			ouroboros.WithMuxerSegmentReadTimeout(0),
			ouroboros.WithChainSyncConfig(chainsync.NewConfig(
				chainsync.WithRollForwardFunc(
					func(
						_ chainsync.CallbackContext,
						_ uint,
						blockData any,
						_ chainsync.Tip,
					) error {
						block, ok := blockData.(lcommon.Block)
						if !ok {
							return fmt.Errorf(
								"unexpected roll-forward payload type %T",
								blockData,
							)
						}
						point := pcommon.NewPoint(
							block.SlotNumber(), block.Hash().Bytes(),
						)
						// Recorded before any of this block's own work runs --
						// see lastPoint's doc comment above.
						lastPoint = point

						if !utxoAttempted {
							utxoAttempted = true
							refs, err := captureGenesisBaseline(ctx, dingoAddr, magic, point)
							if err != nil {
								logf(
									"nodeparity: genesis UTxO baseline unavailable "+
										"(UTxO comparison skipped until a retry "+
										"succeeds, at the next epoch boundary): %v",
									err,
								)
							} else {
								utxoRefs = refs
								logf(
									"nodeparity: genesis UTxO baseline captured: %d refs",
									len(refs),
								)
							}
						} else if utxoRefs != nil {
							for _, tx := range block.Transactions() {
								pendingTxHashes = append(
									pendingTxHashes, tx.Hash().String(),
								)
							}
							if len(pendingTxHashes) >= txInfoOpportunisticFlushThreshold {
								flushPendingTxInfos(point)
							}
						}

						epoch, err := currentEpochNo(ctx, dingoAddr, magic, point)
						if err != nil {
							// callbackErr, not fmt.Errorf: GetEpochNo's own
							// connection dying returns an error wrapping
							// protocol.ErrProtocolShuttingDown, which %w would
							// carry out of this callback and silently end the
							// session -- see callbackErr's doc comment.
							return callbackErr(
								"determine current epoch at slot %d: %w",
								block.SlotNumber(), err,
							)
						}
						progressed = true
						if haveLastEpoch && epoch <= lastEpoch {
							return nil
						}
						haveLastEpoch = true
						lastEpoch = epoch

						// About to Acquire and compare at this exact point --
						// flush any hashes still buffered below the threshold
						// above so the reconstruction is current through this
						// block, not just through the last flush.
						flushPendingTxInfos(point)

						result := EpochResult{Epoch: epoch}

						// Protocol params and stake share one connection/Acquire
						// per attempt, separate from UTxO's own -- see
						// koios_check.go's doc comment for why sharing with
						// UTxO would needlessly cut them off at its much
						// tighter retention floor, and runProtocolParamsAndStake's
						// own doc comment for why a connection that dies
						// during or between the two calls is retried, not
						// reported as an immediate hard failure.
						psStart := time.Now()
						result.ProtocolParamsMismatches, result.ProtocolParamsErr,
							result.StakeMismatches, result.StakeErr = runProtocolParamsAndStake(
							ctx, dingoAddr, magic, point, koios, cache, network, epoch,
						)
						result.ProtocolParamsAndStakeElapsed = time.Since(psStart)

						utxoStart := time.Now()
						// No trustworthy baseline is currently held -- either
						// the initial capture above never succeeded, or a
						// later re-baseline (flushPendingTxInfos or the
						// roll-backward callback below) itself failed. Retry
						// now, at this epoch boundary, instead of leaving
						// UTxO comparison permanently disabled for the rest
						// of the run once any single captureGenesisBaseline
						// call fails: confirmed live (dingo#1900) that a
						// one-off transient dial error ("can't assign
						// requested address") during a re-baseline attempt
						// otherwise wedged utxoRefs at nil for the rest of a
						// multi-hour run, even though the connectivity issue
						// itself cleared within seconds. utxoAttempted gates
						// this so the very first, still-in-progress capture
						// attempt above (same block, same point) is not
						// immediately redialed a second time here.
						if utxoAttempted && utxoRefs == nil {
							refs, err := captureGenesisBaseline(ctx, dingoAddr, magic, point)
							if err != nil {
								logf(
									"nodeparity: UTxO baseline retry failed this "+
										"epoch (comparison skipped again, will "+
										"retry next epoch boundary): %v",
									err,
								)
							} else {
								utxoRefs = refs
								// Freshly captured directly from Dingo at
								// this exact point -- comparing it against
								// Dingo's own answer for the same point right
								// below would trivially match by
								// construction, exactly like the
								// tx_info-chunk-failure and rollback
								// re-baselines elsewhere in this function --
								// taint this recovery epoch too.
								utxoTaintedThisEpoch = true
								logf(
									"nodeparity: UTxO baseline retry "+
										"succeeded, comparison resumes next "+
										"epoch: %d refs",
									len(refs),
								)
							}
						}
						switch mode, verdictErr := utxoVerdict(
							utxoTaintedThisEpoch, utxoRefs,
						); mode {
						case utxoVerdictTainted:
							result.UTxOAttempted = true
							result.UTxOErr = verdictErr
						case utxoVerdictCompare:
							result.UTxOAttempted = true
							if utxoConn, lsqUtxo, err := acquireWithRetry(ctx, dingoAddr, magic, point); err != nil {
								result.UTxOErr = err
							} else {
								utxos, err := lsqUtxo.Client.GetUTxOWhole()
								if err != nil {
									result.UTxOErr = fmt.Errorf("dingo GetUTxOWhole: %w", err)
								} else {
									dingoSet := make(UTxOSet, len(utxos.Results))
									for id, out := range utxos.Results {
										key := fmt.Sprintf("%s#%d", id.Hash.String(), id.Idx)
										dingoSet[key] = canonicalUTxOEntry(out)
									}
									result.UTxORefCount = len(dingoSet)
									result.UTxOMissing, result.UTxOExtra, result.UTxODiffers = UTxODiff(utxoRefs, dingoSet)
								}
								_ = lsqUtxo.Client.Release() //nolint:errcheck
								utxoConn.Close()             //nolint:errcheck
							}
						case utxoVerdictNoBaseline:
							result.UTxOAttempted = false
						}
						result.UTxOElapsed = time.Since(utxoStart)
						utxoTaintedThisEpoch = false

						result.TxInfoFlushCount = txInfoFlushCount
						result.TxInfoFlushElapsed = txInfoFlushElapsed
						txInfoFlushCount = 0
						txInfoFlushElapsed = 0

						report(result)
						return nil
					},
				),
				chainsync.WithRollBackwardFunc(
					func(_ chainsync.CallbackContext, point pcommon.Point, _ chainsync.Tip) error {
						// Recorded before any of this rollback's own work runs
						// -- see lastPoint's doc comment above.
						lastPoint = point

						// A rollback invalidates every transaction the running
						// UTxO reconstruction applied from the now-abandoned
						// fork -- it must not keep silently building on top of
						// them. Genesis bulk replay against multiple competing
						// peers makes short rollbacks a routine occurrence
						// (confirmed live via Dingo's own "chain switch:
						// updating active connection" log lines throughout this
						// tool's own validation runs), so this is not a rare
						// edge case to leave unhandled.
						//
						// Re-baselining at the rollback point -- the same
						// trusted-from-Dingo approach captureGenesisBaseline
						// already uses for the very first block -- is simpler
						// and safer than trying to precisely unwind only the
						// rolled-back blocks' own changes, and costs only one
						// GetUTxOWhole call, not repeated per rollback depth.
						// Any buffered hashes belong to blocks on the
						// now-abandoned fork -- discard them rather than
						// applying them on top of the re-baselined (or
						// disabled) reconstruction below.
						pendingTxHashes = pendingTxHashes[:0]

						if utxoRefs != nil {
							// A rollback-triggered re-baseline replaces utxoRefs
							// with Dingo's own current answer, same as the
							// tx_info-chunk-failure re-baseline in
							// flushPendingTxInfos above -- comparing this epoch's
							// result against that would trivially match by
							// construction, not confirm anything against Koios.
							// Taint this epoch the same way so the verdict
							// reports "not run" instead of a false match.
							utxoTaintedThisEpoch = true
							refs, err := captureGenesisBaseline(ctx, dingoAddr, magic, point)
							if err != nil {
								utxoRefs = nil
								logf(
									"nodeparity: rollback to slot %d invalidated the "+
										"UTxO reconstruction and re-baselining failed "+
										"(UTxO comparison skipped this epoch, will "+
										"retry at the next epoch boundary): %v",
									point.Slot, err,
								)
							} else {
								utxoRefs = refs
								logf(
									"nodeparity: rolled back to slot %d, "+
										"UTxO reconstruction re-baselined: %d refs",
									point.Slot, len(refs),
								)
							}
						}
						// lastEpoch tracks the highest epoch confirmed on the
						// canonical chain -- a rollback across an epoch
						// boundary (possible, if rare, given how far apart
						// preview/preprod epoch boundaries are relative to a
						// typical bulk-replay rollback depth) must retreat it
						// too, or a re-crossing of that same boundary on the
						// new fork would be silently skipped as already seen.
						epoch, err := currentEpochNo(ctx, dingoAddr, magic, point)
						if err != nil {
							// callbackErr for the same reason as the
							// roll-forward callback above.
							return callbackErr(
								"determine current epoch at rollback slot %d: %w",
								point.Slot, err,
							)
						}
						progressed = true
						// Retreat only. Raising lastEpoch here would skip an
						// epoch entirely: every reconnect opens with a
						// RollBackward to lastPoint, which is already the
						// block the failed session died on, so assigning
						// unconditionally would mark that block's own epoch as
						// confirmed without ever running its checks, and the
						// roll-forward callback's `epoch <= lastEpoch` guard
						// would then return early for every remaining block in
						// it. An epoch is confirmed by being checked, which
						// only happens in the roll-forward callback.
						if !haveLastEpoch || epoch < lastEpoch {
							haveLastEpoch = true
							lastEpoch = epoch
						}
						return nil
					},
				),
			)),
		)
		if connErr != nil {
			rawConn.Close() //nolint:errcheck
			ok, err := retryOrStop(
				"nodeparity: ouroboros.New (chainsync) failed",
				connErr,
			)
			if !ok {
				return err
			}
			continue
		}
		// Registered as soon as csConn exists, matching this loop's
		// single-shot predecessor: a ctx cancellation during cs.Client.Sync
		// itself (not just during the final select below) must still close
		// csConn to unblock it, not wait for Sync to return on its own.
		stopOnCancel := context.AfterFunc(ctx, func() { csConn.Close() }) //nolint:errcheck

		cs := csConn.ChainSync()
		if cs == nil || cs.Client == nil {
			stopOnCancel()
			csConn.Close() //nolint:errcheck
			return errors.New("ChainSync client unavailable")
		}
		if err := cs.Client.Sync([]pcommon.Point{lastPoint}); err != nil {
			stopOnCancel()
			csConn.Close() //nolint:errcheck
			ok, retryErr := retryOrStop(
				fmt.Sprintf(
					"nodeparity: start chainsync from slot %d failed",
					lastPoint.Slot,
				),
				err,
			)
			if !ok {
				return retryErr
			}
			continue
		}

		var sessionErr error
		select {
		case <-ctx.Done():
			stopOnCancel()
			csConn.Close() //nolint:errcheck
			return ctx.Err()
		case err, ok := <-csConn.ErrorChan():
			stopOnCancel()
			csConn.Close() //nolint:errcheck
			if !ok {
				return nil
			}
			sessionErr = err
		}

		ok, err := retryOrStop(
			fmt.Sprintf(
				"nodeparity: chainsync session ended, resuming from slot %d",
				lastPoint.Slot,
			),
			sessionErr,
		)
		if !ok {
			return err
		}
	}
}
