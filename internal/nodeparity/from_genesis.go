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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
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

// txInfoIndexWaits is the wait before each round that fetches again the
// /tx_info chunks Koios had no result for. Following the tip, Dingo serves a
// block before Koios has indexed it: on preprod a flush at the tip failed on
// a just-served hash that Koios answered for when asked later, and the
// failure re-baselined the reconstruction and left that epoch's UTxO check
// not run. Koios's /tip was observed about 30s behind the chain, so the
// rounds, 70s in all, cover a lag of several 20s preprod blocks; a hash
// still missing after them is treated as any other failed chunk.
// RunFromGenesis reads it once, and only a test changes it.
var txInfoIndexWaits = []time.Duration{
	10 * time.Second,
	20 * time.Second,
	40 * time.Second,
}

// fetchTxInfoChunks fetches every chunk with at most txInfoConcurrency
// requests in flight, then, after each wait in waits, fetches again each
// chunk that failed with koiosparity.ErrTxInfoMissing. A chunk is fetched
// whole every time, so a result is only ever a complete chunk. Other
// failures are not fetched again: the shared Koios client has already
// retried them. It returns ctx's error when ctx ends during a fetch or a
// wait, and the caller must then discard results and errs.
func fetchTxInfoChunks(
	ctx context.Context,
	chunks [][]string,
	fetch func(context.Context, []string) ([]koiosparity.KoiosTxInfoItem, error),
	waits []time.Duration,
	wait func(context.Context, time.Duration) error,
	logf func(format string, args ...any),
) ([][]koiosparity.KoiosTxInfoItem, []error, error) {
	results := make([][]koiosparity.KoiosTxInfoItem, len(chunks))
	errs := make([]error, len(chunks))
	fetchAll := func(indexes []int) error {
		var g errgroup.Group
		g.SetLimit(txInfoConcurrency)
		for _, i := range indexes {
			g.Go(func() error {
				results[i], errs[i] = fetch(ctx, chunks[i])
				return nil
			})
		}
		_ = g.Wait() // failures are per chunk in errs
		return ctx.Err()
	}
	pending := make([]int, 0, len(chunks))
	for i := range chunks {
		pending = append(pending, i)
	}
	if err := fetchAll(pending); err != nil {
		return nil, nil, err
	}
	for _, d := range waits {
		pending = pending[:0]
		for i, err := range errs {
			if errors.Is(err, koiosparity.ErrTxInfoMissing) {
				pending = append(pending, i)
			}
		}
		if len(pending) == 0 {
			break
		}
		logf(
			"nodeparity: koios has not indexed every pending tx yet; "+
				"fetching %d chunk(s) again in %s: %v",
			len(pending), d, errs[pending[0]],
		)
		if err := wait(ctx, d); err != nil {
			return nil, nil, err
		}
		if err := fetchAll(pending); err != nil {
			return nil, nil, err
		}
	}
	return results, errs, nil
}

var errAcquireWithoutLocalStateQuery = errors.New(
	"acquire succeeded without a local state query client",
)

// errSessionEnded is what a chainsync callback returns when gouroboros
// delivers it after RunFromGenesis has already ended that callback's
// session; the state it would have written belongs to the next session.
var errSessionEnded = errors.New("chainsync session already ended")

// currentEpochNo Acquires point and asks Dingo directly which epoch it
// falls in (queryShelleyEpochNo, an unbounded query with no retention
// floor). RunFromGenesis uses it only as a fallback when Dingo's era history
// (epochOracle) does not cover a point: one dial and Acquire per block is
// what Dingo times out on under sync load.
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
	// A too-old refusal is not retried here: before validation starts it is
	// the expected answer that makes RunFromGenesis skip the block.
	conn, lsq, err := acquireWithRetry(ctx, dingoAddr, magic, point, false)
	if err != nil {
		return 0, err
	}
	if conn == nil || lsq == nil || lsq.Client == nil {
		return 0, errAcquireWithoutLocalStateQuery
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
// rejection).
//
// The epoch checks also retry ErrAcquireFailurePointTooOld (see
// acquireWithRetry's retryTooOld). Dingo answers it not only for a point
// below its retention floor but for any Acquire whose read snapshot it could
// not open within its own server-side wait, which at an epoch boundary is
// routinely commit contention on the block just delivered. Each such refusal
// already spent that wait on Dingo's side, so the fixed budget here tolerates
// tens of seconds of contention, while a point genuinely below the floor,
// which Dingo refuses at once, still fails within a couple of seconds.
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

var errDingoClosedConnection = errors.New(
	"chainsync connection closed without an error",
)

// epochLookupTimeout bounds one epoch lookup: the era-history fetch and the
// per-point GetEpochNo fallback. Both run inside a chainsync callback on
// connections Dial opens with no query timeout, so a query Dingo accepts and
// never answers would hold the callback forever. Chainsync is idle with
// nothing outstanding while a callback runs, so its connection reports no
// error: the run would neither progress nor reconnect. Both answers are a few
// bytes; the bound only has to cover Dingo's own Acquire wait across
// acquireWithRetry's attempts. A variable so tests can shorten it.
var epochLookupTimeout = 5 * time.Minute

var errEpochLookupTimedOut = errors.New("no answer from Dingo to the epoch lookup")

// errEpochChecksInterrupted ends a chainsync session whose roll-forward
// callback was running an epoch's checks when the session ended. The epoch is
// not reported: its checks run again at the next block of that epoch the
// following session delivers.
var errEpochChecksInterrupted = errors.New(
	"chainsync session ended during the epoch checks",
)

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
// (hold one connection per attempt, and recover from its death by
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
	for attempt := range protocolParamsAndStakeRetries {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err(), nil, ctx.Err()
			case <-time.After(protocolParamsAndStakeRetryDelay):
			}
		}

		conn, lsq, err := acquireWithRetry(ctx, dingoAddr, magic, point, true)
		if err != nil {
			// acquireWithRetry already retried a point-not-on-chain race and
			// a too-old refusal internally -- a failure here is either ctx
			// cancellation or an acquire failure that outlasted that budget
			// (e.g. the point has aged past Dingo's retention floor), neither
			// of which this loop's own
			// retry can do anything about. Neither check ran at all, so
			// reporting the same err for both is correct here (unlike the
			// exhausted-retries return below, where the two checks DO run
			// independently and must keep their own outcomes).
			return nil, err, nil, err
		}
		if conn == nil || lsq == nil || lsq.Client == nil {
			return nil, errAcquireWithoutLocalStateQuery,
				nil, errAcquireWithoutLocalStateQuery
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

// utxoComparisonRetries/utxoComparisonRetryDelay bound how many times
// compareUTxOWithRetry redials and repeats the whole {Acquire, GetUTxOWhole}
// sequence after the connection dies, with the same budget as
// runProtocolParamsAndStake.
const (
	utxoComparisonRetries    = acquireRetries
	utxoComparisonRetryDelay = acquireRetryDelay
)

type utxoComparison struct {
	refCount int
	missing  []string
	extra    []string
	differs  []string
}

// compareUTxOWithRetry compares Dingo's whole UTxO set at point against want.
// A connection that dies during Acquire or GetUTxOWhole
// (isRetryableDingoConnErr) is retried on a fresh connection, as
// runProtocolParamsAndStake does; any other error is returned at once. Each
// attempt only reads want, so a retry cannot apply the reconstruction twice
// or change what a later epoch compares against.
func compareUTxOWithRetry(
	ctx context.Context,
	dingoAddr string,
	magic uint32,
	point pcommon.Point,
	want UTxOSet,
) (utxoComparison, error) {
	var lastErr error
	for attempt := range utxoComparisonRetries {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return utxoComparison{}, ctx.Err()
			case <-time.After(utxoComparisonRetryDelay):
			}
		}
		cmp, err := compareUTxOOnce(ctx, dingoAddr, magic, point, want)
		if err == nil || !isRetryableDingoConnErr(err) {
			return cmp, err
		}
		lastErr = err
	}
	return utxoComparison{}, lastErr
}

func compareUTxOOnce(
	ctx context.Context,
	dingoAddr string,
	magic uint32,
	point pcommon.Point,
	want UTxOSet,
) (utxoComparison, error) {
	conn, lsq, err := acquireWithRetry(ctx, dingoAddr, magic, point, true)
	if err != nil {
		return utxoComparison{}, err
	}
	if conn == nil || lsq == nil || lsq.Client == nil {
		if conn != nil {
			conn.Close() //nolint:errcheck
		}
		return utxoComparison{}, errAcquireWithoutLocalStateQuery
	}
	defer func() {
		_ = lsq.Client.Release() //nolint:errcheck
		conn.Close()             //nolint:errcheck
	}()
	utxos, err := lsq.Client.GetUTxOWhole()
	if err != nil {
		return utxoComparison{}, fmt.Errorf("dingo GetUTxOWhole: %w", err)
	}
	got := make(UTxOSet, len(utxos.Results))
	for id, out := range utxos.Results {
		key := fmt.Sprintf("%s#%d", id.Hash.String(), id.Idx)
		got[key] = canonicalUTxOEntry(out)
	}
	cmp := utxoComparison{refCount: len(got)}
	cmp.missing, cmp.extra, cmp.differs = UTxODiff(want, got)
	return cmp, nil
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

	// UTxOSkipped is true when the run was started with the UTxO check
	// disabled: the check was not wanted, so it is neither verified nor
	// incomplete, and UTxOAttempted is false.
	UTxOSkipped bool
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
// ErrAcquireFailurePointNotOnChain, plus one that failed with
// ErrAcquireFailurePointTooOld when retryTooOld is set -- see
// acquireRetries' doc comment.
// Returns ok=false, with any connection it opened already closed, on any
// failure other than context cancellation, on which it returns ctx.Err()
// directly rather than a wrapped dial/acquire error.
func acquireWithRetry(
	ctx context.Context,
	dingoAddr string,
	magic uint32,
	point pcommon.Point,
	retryTooOld bool,
) (conn *ouroboros.Connection, lsq *localstatequery.LocalStateQuery, err error) {
	var lastErr error
	for attempt := range acquireRetries {
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
		tooOld := errors.Is(acquireErr, localstatequery.ErrAcquireFailurePointTooOld)
		if tooOld && retryTooOld {
			lastErr = fmt.Errorf(
				"acquire: Dingo still refused slot %d as too old after %d attempts: %w",
				point.Slot, acquireRetries, acquireErr,
			)
			continue
		}
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
	conn, lsq, err := acquireWithRetry(ctx, dingoAddr, magic, point, true)
	if err != nil {
		return nil, err
	}
	if conn == nil || lsq == nil || lsq.Client == nil {
		return nil, errAcquireWithoutLocalStateQuery
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

// errUTxOTainted is wrapped by utxoVerdict for utxoVerdictTainted, prefixed
// with the cause that re-baselined the reconstruction -- see that constant's
// doc comment.
var errUTxOTainted = errors.New(
	"UTxO reconstruction was re-baselined from Dingo this epoch, " +
		"so comparing it against Dingo was skipped",
)

// Causes recorded in RunFromGenesis's utxoTaintCause. A rollback's cause
// names its slot instead (taintCauseRollback).
const (
	taintCauseTxInfo   = "tx_info fetch failed"
	taintCauseBaseline = "UTxO baseline captured at this epoch boundary"
)

func taintCauseRollback(slot uint64) string {
	return fmt.Sprintf("rolled back to slot %d", slot)
}

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
	// utxoVerdictTainted: a tx_info failure, a rollback or a baseline retry
	// this epoch replaced the reconstruction with Dingo's own answer --
	// comparing now would silently pass by construction, not confirm
	// anything against Koios.
	utxoVerdictTainted
	// utxoVerdictCompare: run the real Dingo-vs-reconstruction comparison.
	utxoVerdictCompare
)

// utxoVerdict decides utxoVerdictMode purely from local state -- no network
// I/O -- extracted from RunFromGenesis's roll-forward callback specifically
// so the regression it fixes is directly assertable: a Koios outage during
// tx_info reconstruction being reported as a false "utxo set match" instead
// of "not run", because the taint flag was closure state inside a
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
//
// taintCause is empty when nothing re-baselined the reconstruction this
// epoch; otherwise it names what did, and prefixes the returned error.
func utxoVerdict(
	taintCause string,
	utxoRefs UTxOSet,
) (mode utxoVerdictMode, err error) {
	switch {
	case taintCause != "":
		return utxoVerdictTainted, fmt.Errorf("%s: %w", taintCause, errUTxOTainted)
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
// utxoTaintCause, so a test can drive the actual failure decision
// directly with synthetic chunks/results/errs instead of needing a real
// Koios server: the fix for a Koios outage being reported as a false "utxo
// set match" was pinned only at utxoVerdict, the function that reads
// utxoTaintCause, never at anything that decides what sets it. Does
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

// pointsEqual reports whether a and b name the same block.
func pointsEqual(a, b pcommon.Point) bool {
	return a.Slot == b.Slot && bytes.Equal(a.Hash, b.Hash)
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
// skipUTxO disables the UTxO-set comparison: no baseline is captured and no
// /tx_info is fetched, and each EpochResult carries UTxOSkipped.
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
	skipUTxO bool,
) error {
	if !KoiosNetworks[network] {
		return fmt.Errorf(
			"koios-backed comparison only supports preview, preprod or mainnet, got %q",
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
		// is set (a killed node-parity process has no
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
		// utxoTaintCause is set whenever a tx_info failure forced a
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
		//
		// It holds the first cause seen this epoch, so the verdict can say
		// which one it was; empty means untainted.
		utxoTaintCause string
		// lastEpochInterrupted is set when lastEpoch's checks were abandoned
		// because the session ended while they ran (errEpochChecksInterrupted),
		// so the next block in lastEpoch runs them instead of being skipped as
		// already checked. A resumed session starts after lastPoint, the block
		// whose checks were abandoned, so the re-run is at a later block of the
		// same epoch.
		lastEpochInterrupted bool
	)
	// taint records cause unless an earlier one this epoch already did.
	taint := func(cause string) {
		if utxoTaintCause == "" {
			utxoTaintCause = cause
		}
	}

	// acquirable becomes true once validation starts: when Dingo first
	// answers for a point, or when the roll-forward callback first resolves a
	// post-Byron block's epoch by either path. Until then a refusal as too old
	// is expected, not an error: Dingo cannot answer for the chain's earliest
	// points. A network that starts in Byron, such as preprod, has no protocol
	// parameters or stake distribution there, and Koios holds none either, so
	// nothing in those epochs can be compared. A refusal after that is a real
	// failure and ends the session.
	//
	// The roll-forward callback sets the latch even when the era history
	// resolved the epoch, or a run the history covered would skip a later
	// refusal past the cached history and consume the block unchecked. epochAt
	// does not set it on the era-history path, because the roll-backward
	// callback can pass it a Byron point.
	var (
		acquirable     bool
		skippedBlocks  int
		lastRefusal    error
		loggedFallback bool
		// skippedByron counts Byron blocks consumed without epoch checks.
		skippedByron int
	)
	oracle := newEpochOracle(func(ctx context.Context) ([]eraSpan, error) {
		return fetchEraSpans(ctx, dingoAddr, magic)
	})
	// Read once: a callback can outlive the return on ctx cancellation.
	lookupTimeout := epochLookupTimeout
	indexWaits := slices.Clone(txInfoIndexWaits)
	// epochAt resolves point's epoch from Dingo's era history, or from Dingo
	// directly for a point the history does not cover. skip is true when
	// Dingo refused point as too old before ever answering: the caller
	// consumes the block without checking it.
	//
	// sessionCtx is cancelled when the chainsync session calling it ends,
	// which closes the lookup's connections, so a callback blocked in one
	// returns: the reconnect loop waits for that callback before starting the
	// next session. Every other query the callbacks make -- the UTxO baseline
	// captures, the /tx_info flushes and the epoch checks -- runs under the
	// same sessionCtx for the same reason; only the lookup carries a
	// deadline, since a whole-UTxO reply can legitimately take minutes.
	epochAt := func(
		sessionCtx context.Context, point pcommon.Point,
	) (epoch uint64, skip bool, err error) {
		lookupCtx, cancelLookup := context.WithTimeout(
			sessionCtx, lookupTimeout,
		)
		defer cancelLookup()
		defer func() {
			if err != nil && ctx.Err() == nil &&
				errors.Is(lookupCtx.Err(), context.DeadlineExceeded) {
				err = fmt.Errorf(
					"%w within %s: %w",
					errEpochLookupTimedOut, lookupTimeout, err,
				)
			}
		}()
		epoch, oracleErr := oracle.epoch(lookupCtx, point.Slot)
		if oracleErr == nil {
			return epoch, false, nil
		}
		if !loggedFallback {
			loggedFallback = true
			logf(
				"nodeparity: Dingo's era history does not resolve slot %d, "+
					"resolving epochs by querying Dingo per point: %v",
				point.Slot, oracleErr,
			)
		}
		epoch, err = currentEpochNo(lookupCtx, dingoAddr, magic, point)
		if err == nil {
			if !acquirable {
				acquirable = true
				if skippedBlocks > 0 {
					logf(
						"nodeparity: validation starts at slot %d (epoch %d): "+
							"Dingo could not answer for the %d earlier block(s), "+
							"so they are not compared against Koios (last "+
							"refusal: %v)",
						point.Slot, epoch, skippedBlocks, lastRefusal,
					)
				}
			}
			return epoch, false, nil
		}
		if !acquirable &&
			errors.Is(err, localstatequery.ErrAcquireFailurePointTooOld) {
			skippedBlocks++
			lastRefusal = err
			if skippedBlocks == 1 {
				logf(
					"nodeparity: Dingo cannot answer for the point at slot %d; "+
						"skipping blocks until it can: %v",
					point.Slot, err,
				)
			}
			return 0, true, nil
		}
		return 0, false, err
	}

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
	//
	// A chunk Koios had no result for is fetched again after each wait in
	// txInfoIndexWaits (see fetchTxInfoChunks) before it counts as failed.
	//
	// It returns sessionCtx's error, and changes nothing, when the session
	// ended during the fetches or the waits between them: they failed because of the cancellation, not
	// because of Koios, so applying the chunks that did succeed and
	// re-baselining would discard an exact reconstruction. The hashes stay
	// buffered and the next session fetches each one once. A session that
	// ends during the re-baseline after a real failure leaves utxoRefs nil,
	// as any failed re-baseline does.
	flushPendingTxInfos := func(
		sessionCtx context.Context, point pcommon.Point,
	) error {
		if utxoRefs == nil || len(pendingTxHashes) == 0 {
			return nil
		}
		chunks := make([][]string, 0, (len(pendingTxHashes)+koiosparity.KoiosTxInfoBatchSize-1)/koiosparity.KoiosTxInfoBatchSize)
		for start := 0; start < len(pendingTxHashes); start += koiosparity.KoiosTxInfoBatchSize {
			end := min(start+koiosparity.KoiosTxInfoBatchSize, len(pendingTxHashes))
			chunks = append(chunks, pendingTxHashes[start:end])
		}
		flushStart := time.Now()
		results, errs, err := fetchTxInfoChunks(
			sessionCtx, chunks,
			func(
				ctx context.Context, chunk []string,
			) ([]koiosparity.KoiosTxInfoItem, error) {
				return fetchTxInfosCached(ctx, koios, cache, network, chunk)
			},
			indexWaits, waitStakeRetry, logf,
		)
		if err != nil {
			return err
		}
		txInfoFlushCount++
		txInfoFlushElapsed += time.Since(flushStart)

		failed := applyTxInfoResults(utxoRefs, chunks, results, errs, logf)
		pendingTxHashes = pendingTxHashes[:0]

		if failed {
			taint(taintCauseTxInfo)
			refs, err := captureGenesisBaseline(
				sessionCtx, dingoAddr, magic, point,
			)
			if err != nil {
				utxoRefs = nil
				if sessionErr := sessionCtx.Err(); sessionErr != nil {
					return sessionErr
				}
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
		return nil
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

	// sessionMu orders each chainsync session's callbacks against this loop.
	// gouroboros runs the callbacks on its own goroutines and its Close does
	// not wait for one that is still running, so without it the loop below
	// reads lastPoint and progressed, and the next session's callbacks write
	// the shared state, while the previous session's last callback may still
	// be writing them.
	var sessionMu sync.Mutex
	for {
		progressed = false
		// ended is per session. retireSession sets it under sessionMu, which
		// waits out a callback already running and turns away any that
		// gouroboros delivers afterwards. Call it only after endSession has
		// cancelled sessionCtx and the connection is closed: a callback holds
		// sessionMu while it waits on a query, and only that cancellation
		// ends the wait.
		ended := false
		retireSession := func() {
			sessionMu.Lock()
			ended = true
			sessionMu.Unlock()
		}
		enterCallback := func() error {
			sessionMu.Lock()
			if ended {
				sessionMu.Unlock()
				return errSessionEnded
			}
			return nil
		}

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

		sessionCtx, endSession := context.WithCancel(ctx)
		csConn, connErr := ouroboros.New(
			ouroboros.WithConnection(rawConn),
			ouroboros.WithNetworkMagic(magic),
			ouroboros.WithNodeToNode(false),
			ouroboros.WithMuxerSegmentReadTimeout(0),
			ouroboros.WithChainSyncConfig(chainsync.NewConfig(
				chainsync.WithPipelineLimit(1),
				chainsync.WithRollForwardFunc(
					func(
						_ chainsync.CallbackContext,
						_ uint,
						blockData any,
						_ chainsync.Tip,
					) error {
						if err := enterCallback(); err != nil {
							return err
						}
						defer sessionMu.Unlock()
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

						if !utxoAttempted && !skipUTxO {
							utxoAttempted = true
							refs, err := captureGenesisBaseline(
								sessionCtx, dingoAddr, magic, point,
							)
							if sessionErr := sessionCtx.Err(); sessionErr != nil {
								// Not a failed capture: the next session
								// captures at its first block instead, which
								// covers this one, since a resumed session
								// starts after it.
								utxoAttempted = false
								return callbackErr(
									"capture UTxO baseline at slot %d: %w",
									point.Slot, sessionErr,
								)
							}
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
								if err := flushPendingTxInfos(
									sessionCtx, point,
								); err != nil {
									return callbackErr(
										"flush tx_info at slot %d: %w",
										point.Slot, err,
									)
								}
							}
						}

						// Byron has no protocol parameters or stake
						// distribution in Dingo or Koios, so its epochs are
						// never compared. Deciding by the block's own era,
						// not by Dingo refusing the point, keeps a Byron
						// point from ever being acquired here, and leaving
						// lastEpoch unset lets the first Shelley-era epoch
						// be checked.
						if block.Era().Id == byron.EraIdByron {
							skippedByron++
							progressed = true
							return nil
						}

						epoch, skip, err := epochAt(sessionCtx, point)
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
						if skip {
							return nil
						}
						acquirable = true
						if haveLastEpoch && (epoch < lastEpoch ||
							epoch == lastEpoch && !lastEpochInterrupted) {
							return nil
						}
						if lastEpochInterrupted && epoch != lastEpoch {
							logf(
								"nodeparity: epoch %d is not checked: its "+
									"checks were interrupted by the session "+
									"ending, and the resumed session's next "+
									"block is in epoch %d",
								lastEpoch, epoch,
							)
						}
						lastEpochInterrupted = false
						haveLastEpoch = true
						lastEpoch = epoch
						// interrupted abandons this epoch's checks when the
						// session ends while they run. Every check below
						// queries Dingo or Koios under sessionCtx, so a
						// cancelled one fails with the cancellation rather
						// than a finding; reporting it would record a
						// verdict for a check that never ran. The next
						// block of this epoch runs them again.
						//
						// Logged here because the reconnect loop usually
						// reports the session's own end instead: the
						// callback error arrives after it.
						interrupted := func() error {
							lastEpochInterrupted = true
							err := callbackErr(
								"%w: epoch %d at slot %d: %w",
								errEpochChecksInterrupted, epoch,
								point.Slot, context.Cause(sessionCtx),
							)
							logf(
								"nodeparity: %v (they run again at the "+
									"next block of epoch %d)",
								err, epoch,
							)
							return err
						}
						if skippedByron > 0 {
							logf(
								"nodeparity: validation starts at slot "+
									"%d (epoch %d): the %d earlier Byron "+
									"block(s) have no protocol parameters "+
									"or stake distribution, so they are not "+
									"compared against Koios",
								point.Slot, epoch, skippedByron,
							)
							skippedByron = 0
						}

						// About to Acquire and compare at this exact point --
						// flush any hashes still buffered below the threshold
						// above so the reconstruction is current through this
						// block, not just through the last flush.
						if err := flushPendingTxInfos(
							sessionCtx, point,
						); err != nil {
							return interrupted()
						}

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
							sessionCtx, dingoAddr, magic, point, koios, cache, network, epoch,
						)
						result.ProtocolParamsAndStakeElapsed = time.Since(psStart)
						if sessionCtx.Err() != nil {
							return interrupted()
						}

						utxoStart := time.Now()
						// No trustworthy baseline is currently held -- either
						// the initial capture above never succeeded, or a
						// later re-baseline (flushPendingTxInfos or the
						// roll-backward callback below) itself failed. Retry
						// now, at this epoch boundary, instead of leaving
						// UTxO comparison permanently disabled for the rest
						// of the run once any single captureGenesisBaseline
						// call fails: confirmed live that a
						// one-off transient dial error ("can't assign
						// requested address") during a re-baseline attempt
						// otherwise wedged utxoRefs at nil for the rest of a
						// multi-hour run, even though the connectivity issue
						// itself cleared within seconds. utxoAttempted gates
						// this so the very first, still-in-progress capture
						// attempt above (same block, same point) is not
						// immediately redialed a second time here.
						if !skipUTxO && utxoAttempted && utxoRefs == nil {
							refs, err := captureGenesisBaseline(
								sessionCtx, dingoAddr, magic, point,
							)
							if sessionCtx.Err() != nil {
								// utxoRefs is still nil, so the rerun
								// retries the capture.
								return interrupted()
							}
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
								taint(taintCauseBaseline)
								logf(
									"nodeparity: UTxO baseline retry "+
										"succeeded, comparison resumes next "+
										"epoch: %d refs",
									len(refs),
								)
							}
						}
						mode, verdictErr := utxoVerdict(utxoTaintCause, utxoRefs)
						if skipUTxO {
							result.UTxOSkipped = true
							mode = utxoVerdictNoBaseline
						}
						switch mode {
						case utxoVerdictTainted:
							result.UTxOAttempted = true
							result.UTxOErr = verdictErr
						case utxoVerdictCompare:
							result.UTxOAttempted = true
							cmp, err := compareUTxOWithRetry(
								sessionCtx, dingoAddr, magic, point, utxoRefs,
							)
							if sessionCtx.Err() != nil {
								// The comparison only reads utxoRefs, and
								// utxoTaintCause is reset only once a
								// result is reported, so the rerun compares
								// the same reconstruction.
								return interrupted()
							}
							if err != nil {
								result.UTxOErr = err
							} else {
								result.UTxORefCount = cmp.refCount
								result.UTxOMissing = cmp.missing
								result.UTxOExtra = cmp.extra
								result.UTxODiffers = cmp.differs
							}
						case utxoVerdictNoBaseline:
							result.UTxOAttempted = false
						}
						result.UTxOElapsed = time.Since(utxoStart)
						utxoTaintCause = ""

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
						if err := enterCallback(); err != nil {
							return err
						}
						defer sessionMu.Unlock()
						// A rollback to lastPoint itself invalidates nothing.
						// Every reconnect opens with exactly this rollback,
						// since the session resumes from lastPoint, the block
						// last fully processed: the roll-forward callback
						// buffers a block's transactions immediately after
						// recording its point, and can fail only later, so
						// utxoRefs plus pendingTxHashes are exact through it.
						// ChainSync resumes after the intersection, so that
						// block is not delivered again.
						rollbackInvalidates := !pointsEqual(point, lastPoint)
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
						if rollbackInvalidates {
							pendingTxHashes = pendingTxHashes[:0]
						}

						if rollbackInvalidates && utxoRefs != nil {
							// A rollback-triggered re-baseline replaces utxoRefs
							// with Dingo's own current answer, same as the
							// tx_info-chunk-failure re-baseline in
							// flushPendingTxInfos above -- comparing this epoch's
							// result against that would trivially match by
							// construction, not confirm anything against Koios.
							// Taint this epoch the same way so the verdict
							// reports "not run" instead of a false match.
							taint(taintCauseRollback(point.Slot))
							refs, err := captureGenesisBaseline(
								sessionCtx, dingoAddr, magic, point,
							)
							if err != nil {
								// The reconstruction is invalid whether or
								// not the session ended, and nil makes the
								// next epoch boundary capture a baseline.
								utxoRefs = nil
								if sessionErr := sessionCtx.Err(); sessionErr != nil {
									return callbackErr(
										"re-baseline UTxO at rollback slot %d: %w",
										point.Slot, sessionErr,
									)
								}
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
						// Every session opens with a RollBackward to the origin
						// when it starts from genesis. Dingo refuses to acquire
						// that point -- slot 0 reads as "live" in its query
						// model -- and the origin lies in epoch 0 by definition,
						// so it is not asked.
						var epoch uint64
						if point.Slot != 0 || len(point.Hash) != 0 {
							var (
								skip bool
								err  error
							)
							epoch, skip, err = epochAt(sessionCtx, point)
							if err != nil {
								// callbackErr for the same reason as the
								// roll-forward callback above.
								return callbackErr(
									"determine current epoch at rollback slot %d: %w",
									point.Slot, err,
								)
							}
							if skip {
								progressed = true
								return nil
							}
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
						//
						// A retreat also drops an interrupted epoch's pending
						// re-run: the chain crosses into that epoch again and
						// checks it then.
						if !haveLastEpoch || epoch < lastEpoch {
							haveLastEpoch = true
							lastEpoch = epoch
							lastEpochInterrupted = false
						}
						return nil
					},
				),
			)),
		)
		if connErr != nil {
			endSession()
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
			endSession()
			stopOnCancel()
			csConn.Close() //nolint:errcheck
			retireSession()
			return errors.New("ChainSync client unavailable")
		}
		if err := cs.Client.Sync([]pcommon.Point{lastPoint}); err != nil {
			endSession()
			stopOnCancel()
			csConn.Close() //nolint:errcheck
			retireSession()
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
		csDone := cs.Client.DoneChan()

		var sessionErr error
		select {
		case <-ctx.Done():
			endSession()
			stopOnCancel()
			csConn.Close() //nolint:errcheck
			retireSession()
			return ctx.Err()
		case err, ok := <-csConn.ErrorChan():
			endSession()
			stopOnCancel()
			csConn.Close() //nolint:errcheck
			retireSession()
			// gouroboros closes ErrorChan without sending anything when the
			// peer closes the connection while every protocol is idle,
			// which includes chainsync with no RequestNext outstanding --
			// the callback for the last reply of a pipelined batch. A closed
			// channel is therefore not a clean stop: unless ctx ended it,
			// Dingo went away and the session is retried.
			if !ok {
				err = errDingoClosedConnection
			}
			sessionErr = err
		}
		// retireSession above has already waited out a running callback
		// and turned away later ones. Waiting for the protocol's receive
		// loop, which runs them, to exit as well keeps the next session from
		// starting while this one's connection is still delivering.
		select {
		case <-csDone:
		case <-ctx.Done():
			return ctx.Err()
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
