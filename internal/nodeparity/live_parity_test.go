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

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/blinklabs-io/dingo/internal/secretfile"
	ouroboros "github.com/blinklabs-io/gouroboros"
)

// The live parity tests compare a running Dingo against a running
// cardano-node (check, watch) or against Koios (from-genesis) over their
// node-to-client sockets. Each is skipped unless its environment is set, so
// a normal go test run never dials anything. Addresses are host:port, or a
// leading-"/" Unix socket path.
const (
	liveEnvNetwork     = "DINGO_NODE_PARITY_NETWORK"
	liveEnvDingoAddr   = "DINGO_NODE_PARITY_DINGO_ADDR"
	liveEnvCardanoAddr = "DINGO_NODE_PARITY_CARDANO_ADDR"
	liveEnvAtSlot      = "DINGO_NODE_PARITY_AT_SLOT"
	liveEnvAtHash      = "DINGO_NODE_PARITY_AT_HASH"

	liveEnvWatch             = "DINGO_NODE_PARITY_WATCH"
	liveEnvFallbackInterval  = "DINGO_NODE_PARITY_FALLBACK_INTERVAL"
	liveEnvCheckTimeout      = "DINGO_NODE_PARITY_CHECK_TIMEOUT"
	liveEnvFullCheckInterval = "DINGO_NODE_PARITY_FULL_CHECK_INTERVAL"
	liveEnvFullCheckTimeout  = "DINGO_NODE_PARITY_FULL_CHECK_TIMEOUT"
	liveEnvCursorFile        = "DINGO_NODE_PARITY_CURSOR_FILE"

	liveEnvFromGenesis           = "DINGO_NODE_PARITY_FROM_GENESIS"
	liveEnvKoiosAPIKey           = "DINGO_NODE_PARITY_KOIOS_API_KEY"
	liveEnvKoiosAPIKeyFile       = "DINGO_NODE_PARITY_KOIOS_API_KEY_FILE"
	liveEnvKoiosBaseURL          = "DINGO_NODE_PARITY_KOIOS_BASE_URL"
	liveEnvKoiosInsecureHTTP     = "DINGO_NODE_PARITY_KOIOS_ALLOW_INSECURE_HTTP"
	liveEnvKoiosPrivateAddresses = "DINGO_NODE_PARITY_KOIOS_ALLOW_PRIVATE_ADDRESSES"
	liveEnvKoiosCachePath        = "DINGO_NODE_PARITY_KOIOS_CACHE_PATH"
	liveEnvKoiosTxInfoBatch      = "DINGO_NODE_PARITY_KOIOS_TX_INFO_BATCH_SIZE"
	liveEnvKoiosTxInfoConc       = "DINGO_NODE_PARITY_KOIOS_TX_INFO_CONCURRENCY"
	liveEnvSkipUTxO              = "DINGO_NODE_PARITY_SKIP_UTXO"
	liveEnvCaptureDir            = "DINGO_NODE_PARITY_BOUNDARY_CAPTURE_DIR"
	liveEnvVerbose               = "DINGO_NODE_PARITY_VERBOSE"
)

const (
	// The fallback runs a full-mode check regardless of block activity, in
	// case a watcher's ChainSync subscription stalls without erroring.
	defaultLiveFallbackInterval = 2 * time.Minute
	// A whole-UTxO comparison against a Preview-scale node takes 7-9+
	// minutes, so one full-mode cycle needs its own bound, far above the
	// fallback interval; sharing one value self-cancelled every cycle.
	defaultLiveCheckTimeout = 20 * time.Minute
	// About 5-6 hours of blocks between incremental full checkpoints.
	defaultLiveFullCheckInterval = 1000
	// liveDeadlineMargin leaves room to stop cleanly and report before go
	// test's own -timeout panics the binary.
	liveDeadlineMargin = time.Minute
	// utxoRefSample bounds the refs a UTxO mismatch logs, enough to identify
	// a class of divergence without logging a whole set.
	utxoRefSample = 20
)

// liveTarget is the network and node addresses a live test compares.
type liveTarget struct {
	network     string
	magic       uint32
	dingoAddr   string
	cardanoAddr string
}

// requireLiveTarget skips t unless the Dingo address is set and, when
// needCardano, the cardano-node address too.
func requireLiveTarget(t *testing.T, needCardano bool) liveTarget {
	t.Helper()
	target := liveTarget{
		network:     os.Getenv(liveEnvNetwork),
		dingoAddr:   os.Getenv(liveEnvDingoAddr),
		cardanoAddr: os.Getenv(liveEnvCardanoAddr),
	}
	if target.dingoAddr == "" || (needCardano && target.cardanoAddr == "") {
		t.Skipf("set %s and %s to run against live nodes",
			liveEnvDingoAddr, liveEnvCardanoAddr)
	}
	magic, err := liveNetworkMagic(target.network)
	if err != nil {
		t.Fatal(err)
	}
	target.magic = magic
	return target
}

// liveNetworkMagic resolves one of the supported network names to its
// Ouroboros network magic.
func liveNetworkMagic(network string) (uint32, error) {
	switch network {
	case "preview", "preprod", "mainnet":
	case "":
		return 0, fmt.Errorf(
			"%s is required (preview, preprod or mainnet)", liveEnvNetwork,
		)
	default:
		return 0, fmt.Errorf(
			"%s must be preview, preprod or mainnet, got %q",
			liveEnvNetwork, network,
		)
	}
	n, ok := ouroboros.NetworkByName(network)
	if !ok {
		return 0, fmt.Errorf("unknown network: %s", network)
	}
	return n.NetworkMagic, nil
}

// liveAtPoint parses the explicit historical point. Both the slot and the
// hash are needed to name a block unambiguously across a fork, so it is
// both or neither; neither means "compare at the agreed live tip".
func liveAtPoint(slotText, hashText string) (*Tip, error) {
	if slotText == "" && hashText == "" {
		return nil, nil
	}
	if slotText == "" || hashText == "" {
		return nil, fmt.Errorf(
			"%s and %s must both be set, or neither",
			liveEnvAtSlot, liveEnvAtHash,
		)
	}
	slot, err := strconv.ParseUint(slotText, 10, 64)
	if err != nil || slot == 0 {
		return nil, fmt.Errorf(
			"%s must be a positive slot, got %q", liveEnvAtSlot, slotText,
		)
	}
	decoded, err := hex.DecodeString(hashText)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", liveEnvAtHash, err)
	}
	if len(decoded) != 32 {
		return nil, fmt.Errorf(
			"%s must decode to 32 bytes, got %d", liveEnvAtHash, len(decoded),
		)
	}
	return &Tip{Slot: slot, Hash: hashText}, nil
}

// liveCheckResultErr returns a non-nil error for anything other than a clean
// match: a discarded cycle must not read as a pass.
func liveCheckResultErr(result *CheckResult) error {
	if result.Skipped {
		return fmt.Errorf("check skipped: %s", result.SkipDetail)
	}
	if result.Diff.Empty() {
		return nil
	}
	return fmt.Errorf(
		"ledger state diverged at slot %d (block %d), %d difference(s):\n%s",
		result.Tip.Slot,
		result.Tip.BlockNumber,
		result.Diff.Count(),
		strings.Join(result.Diff.Lines(), "\n"),
	)
}

// liveContext ends liveDeadlineMargin before go test's -timeout, so a
// long-running comparison stops and reports instead of being killed.
func liveContext(t *testing.T) context.Context {
	t.Helper()
	deadline, ok := t.Deadline()
	if !ok {
		return t.Context()
	}
	ctx, cancel := context.WithDeadline(
		t.Context(), deadline.Add(-liveDeadlineMargin),
	)
	t.Cleanup(cancel)
	return ctx
}

func liveDuration(
	t *testing.T, name string, fallback time.Duration,
) time.Duration {
	t.Helper()
	text := os.Getenv(name)
	if text == "" {
		return fallback
	}
	d, err := time.ParseDuration(text)
	if err != nil || d <= 0 {
		t.Fatalf("%s must be a positive duration, got %q", name, text)
	}
	return d
}

func liveInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	text := os.Getenv(name)
	if text == "" {
		return fallback
	}
	n, err := strconv.Atoi(text)
	if err != nil || n <= 0 {
		t.Fatalf("%s must be a positive integer, got %q", name, text)
	}
	return n
}

func liveBool(t *testing.T, name string) bool {
	t.Helper()
	text := os.Getenv(name)
	if text == "" {
		return false
	}
	b, err := strconv.ParseBool(text)
	if err != nil {
		t.Fatalf("%s must be a boolean, got %q", name, text)
	}
	return b
}

func liveLogger(t *testing.T, level slog.Level) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewJSONHandler(liveLogWriter{t}, &slog.HandlerOptions{
		Level: level,
	}))
}

type liveLogWriter struct{ t *testing.T }

func (w liveLogWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// TestLiveNodeParityCheck runs one comparison cycle of Dingo's ledger state
// (protocol parameters, stake distribution, the whole UTxO set) against
// cardano-node's, at the agreed live tip or at an explicit historical point.
func TestLiveNodeParityCheck(t *testing.T) {
	target := requireLiveTarget(t, true)
	at, err := liveAtPoint(os.Getenv(liveEnvAtSlot), os.Getenv(liveEnvAtHash))
	if err != nil {
		t.Fatal(err)
	}
	result, err := Check(
		liveContext(t), target.dingoAddr, target.cardanoAddr, target.magic, at,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := liveCheckResultErr(result); err != nil {
		t.Fatal(err)
	}
	t.Logf("matched at slot %d (block %d)",
		result.Tip.Slot, result.Tip.BlockNumber)
}

// TestLiveNodeParityWatch keeps comparing the two nodes as their chains
// advance, until go test's -timeout (less a margin), and fails on any
// divergence. DINGO_NODE_PARITY_WATCH selects the mode: "incremental"
// validates block by block with periodic full checkpoints and needs
// DINGO_NODE_PARITY_CURSOR_FILE; "full" runs a whole-ledger check whenever
// either tip changes. Check errors are logged and retried, since a transient
// node hiccup must not end the watch.
func TestLiveNodeParityWatch(t *testing.T) {
	mode := os.Getenv(liveEnvWatch)
	if mode == "" {
		t.Skipf("set %s to full or incremental to watch live nodes",
			liveEnvWatch)
	}
	target := requireLiveTarget(t, true)
	if os.Getenv(liveEnvAtSlot) != "" || os.Getenv(liveEnvAtHash) != "" {
		t.Fatalf("%s/%s apply to the single check only; watch follows the live tip",
			liveEnvAtSlot, liveEnvAtHash)
	}
	ctx := liveContext(t)
	logger := liveLogger(t, slog.LevelInfo)
	var completed int
	switch mode {
	case "full":
		completed = liveWatchFull(
			ctx,
			t,
			target,
			liveDuration(t, liveEnvFallbackInterval, defaultLiveFallbackInterval),
			liveDuration(t, liveEnvCheckTimeout, defaultLiveCheckTimeout),
			logger,
		).completed
	case "incremental":
		cursorFile := os.Getenv(liveEnvCursorFile)
		if cursorFile == "" {
			t.Fatalf("%s is required for incremental mode", liveEnvCursorFile)
		}
		err := RunIncremental(ctx, IncrementalConfig{
			DingoAddr:   target.dingoAddr,
			CardanoAddr: target.cardanoAddr,
			Magic:       target.magic,
			FullCheckInterval: uint64(liveInt( //nolint:gosec // validated positive
				t, liveEnvFullCheckInterval, defaultLiveFullCheckInterval,
			)),
			FullCheckTimeout: liveDuration(
				t, liveEnvFullCheckTimeout, DefaultFullCheckTimeout,
			),
			CursorFile: cursorFile,
			Logger:     logger,
			OnFullCheck: func(
				reason FullCheckReason, result *CheckResult, err error,
			) {
				t.Logf("full checkpoint (%s)", reason)
				if liveHandleCheck(t, result, err) {
					completed++
				}
			},
			OnBlockCheck: func(tip Tip, diff Diff) {
				completed++
				if !diff.Empty() {
					t.Errorf("block %d (slot %d) diverged: %s",
						tip.BlockNumber, tip.Slot,
						strings.Join(diff.Lines(), "; "))
				}
			},
		})
		if err != nil && !errors.Is(err, context.DeadlineExceeded) &&
			!errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	default:
		t.Fatalf("%s must be full or incremental, got %q", liveEnvWatch, mode)
	}
	if err := liveWatchVerdict(mode, completed); err != nil {
		t.Fatal(err)
	}
}

// liveWatchVerdict fails a watch that never completed a comparison. Check
// errors are only logged so a transient hiccup does not end the watch, which
// would otherwise let an unreachable node pass as validated.
func liveWatchVerdict(mode string, completed int) error {
	if completed == 0 {
		return fmt.Errorf("%s watch completed no comparison", mode)
	}
	return nil
}

// liveWatchTally counts a full-mode watch's checks: attempts that returned
// before the watch ended, and those that compared the nodes.
type liveWatchTally struct {
	attempts  int
	completed int
}

// liveWatchFull runs a full check after every tip change on either node,
// and on the fallback timer, until ctx ends.
func liveWatchFull(
	ctx context.Context,
	t *testing.T,
	target liveTarget,
	fallbackInterval, checkTimeout time.Duration,
	logger *slog.Logger,
) liveWatchTally {
	t.Helper()
	var tally liveWatchTally
	watcherLog := func(format string, args ...any) {
		logger.Warn(fmt.Sprintf(format, args...))
	}
	dingoWatcher := WatchBlocks(ctx, target.dingoAddr, target.magic, watcherLog)
	defer dingoWatcher.Close()
	cardanoWatcher := WatchBlocks(
		ctx, target.cardanoAddr, target.magic, watcherLog,
	)
	defer cardanoWatcher.Close()

	fallback := time.NewTimer(fallbackInterval)
	defer fallback.Stop()
	fallbackFired := false
	for {
		cycleCtx, cancel := context.WithTimeout(ctx, checkTimeout)
		result, err := Check(
			cycleCtx, target.dingoAddr, target.cardanoAddr, target.magic, nil,
		)
		cancel()
		if ctx.Err() != nil {
			return tally
		}
		tally.attempts++
		if liveHandleCheck(t, result, err) {
			tally.completed++
		}
		resetFallbackTimer(fallback, fallbackInterval, fallbackFired)
		fallbackFired = false
		select {
		case <-ctx.Done():
			return tally
		case <-dingoWatcher.Events:
		case <-cardanoWatcher.Events:
		case <-fallback.C:
			fallbackFired = true
		}
	}
}

// liveHandleCheck fails the test on a divergence and only logs a check error
// or a discarded cycle, which the next cycle retries. It reports whether the
// nodes were compared.
func liveHandleCheck(t *testing.T, result *CheckResult, err error) bool {
	t.Helper()
	switch {
	case result == nil:
		t.Logf("check error: %v", err)
		return false
	case result.Skipped:
		t.Logf("check skipped: %s", result.SkipDetail)
		return false
	case result.Diff.Empty():
		t.Logf("matched at slot %d (block %d)",
			result.Tip.Slot, result.Tip.BlockNumber)
	default:
		t.Error(liveCheckResultErr(result))
	}
	return true
}

// resetFallbackTimer rearms fallback for the next cycle. A timer that already
// fired during a long check schedules the next cycle at once rather than a
// whole interval later, so an overrun does not compound into two intervals
// between checks. consumed says the fire was what started this cycle; Stop
// reports false for it too, and treating it as an overrun would run checks
// back to back on idle nodes.
func resetFallbackTimer(
	fallback *time.Timer, interval time.Duration, consumed bool,
) {
	if consumed {
		fallback.Reset(interval)
		return
	}
	if !fallback.Stop() {
		select {
		case <-fallback.C:
		default:
		}
		fallback.Reset(0)
		return
	}
	fallback.Reset(interval)
}

// TestLiveNodeParityFromGenesis follows Dingo's chain from genesis (or from
// an explicit validated point) and compares protocol parameters, stake
// distribution and the UTxO set against Koios at every epoch boundary, until
// go test's -timeout less a margin. A cardano-node cannot be the reference
// here: its own replay outruns a freshly started Dingo, leaving no matching
// historical block to compare against. It fails on any real mismatch, and
// on a run that reached an epoch boundary but verified nothing.
func TestLiveNodeParityFromGenesis(t *testing.T) {
	if !liveBool(t, liveEnvFromGenesis) {
		t.Skipf("set %s=true to replay against Koios", liveEnvFromGenesis)
	}
	target := requireLiveTarget(t, false)
	resumeFrom, err := liveAtPoint(
		os.Getenv(liveEnvAtSlot), os.Getenv(liveEnvAtHash),
	)
	if err != nil {
		t.Fatal(err)
	}
	koios, err := liveKoiosClient(t, target.network)
	if err != nil {
		t.Fatal(err)
	}
	level := slog.LevelInfo
	if liveBool(t, liveEnvVerbose) {
		level = slog.LevelDebug
	}
	logger := liveLogger(t, level)

	var cache *koiosparity.Cache
	if cachePath := os.Getenv(liveEnvKoiosCachePath); cachePath != "" {
		cache, err = koiosparity.OpenCache(cachePath, logger)
		if err != nil {
			t.Fatalf("open koios cache: %v", err)
		}
		defer cache.Close() //nolint:errcheck
		if err := requireMatchingKoiosSource(
			cache, koios, target.network, cachePath,
		); err != nil {
			t.Fatal(err)
		}
	}

	var counters fromGenesisCounters
	err = RunFromGenesis(
		liveContext(t),
		target.dingoAddr,
		target.network,
		target.magic,
		koios,
		cache,
		func(r EpochResult) { counters.recordEpoch(r, logger) },
		func(format string, args ...any) {
			logger.Info(fmt.Sprintf(format, args...))
		},
		resumeFrom,
		liveBool(t, liveEnvSkipUTxO),
		WithBoundaryCaptureDir(os.Getenv(liveEnvCaptureDir)),
	)
	t.Logf("run summary: %+v", counters)
	// The run ends at the deadline by design, so only another error fails it.
	if err != nil && !errors.Is(err, context.DeadlineExceeded) &&
		!errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := counters.result(); err != nil {
		t.Fatal(err)
	}
}

func liveKoiosClient(
	t *testing.T, network string,
) (*koiosparity.KoiosClient, error) {
	t.Helper()
	key, keySet := os.LookupEnv(liveEnvKoiosAPIKey)
	file, fileSet := os.LookupEnv(liveEnvKoiosAPIKeyFile)
	apiKey, err := secretfile.Resolve(
		key, keySet, file, fileSet, liveEnvKoiosAPIKey, liveEnvKoiosAPIKeyFile,
	)
	if err != nil {
		return nil, err
	}
	koios, err := NewKoiosClient(
		network,
		apiKey,
		os.Getenv(liveEnvKoiosBaseURL),
		liveBool(t, liveEnvKoiosInsecureHTTP),
		liveBool(t, liveEnvKoiosPrivateAddresses),
	)
	if err != nil {
		return nil, err
	}
	if err := koios.SetTxInfoLimits(
		liveInt(t, liveEnvKoiosTxInfoBatch, koiosparity.KoiosTxInfoBatchSize),
		liveInt(t, liveEnvKoiosTxInfoConc, koiosparity.KoiosTxInfoConcurrency),
	); err != nil {
		return nil, err
	}
	return koios, nil
}

// requireMatchingKoiosSource refuses a cache stamped with a different Koios
// API root than koios queries, and pins the matching root for the rest of
// the run.
//
// The cache is typically a Dingo node's own observer cache, holding hours of
// fetching. Recording the new root instead would discard every cached row for
// the network, so a mismatch is refused. Pinning makes every later write fail
// if another process re-points the cache mid-run, rather than letting this
// run write rows under a source its answers never came from.
func requireMatchingKoiosSource(
	cache *koiosparity.Cache,
	koios *koiosparity.KoiosClient,
	network string,
	cachePath string,
) error {
	resolved := koios.ResolvedBaseURL()
	mismatch, recorded, err := cache.PendingKoiosSourceChange(network, resolved)
	if err != nil {
		return fmt.Errorf("check koios cache source: %w", err)
	}
	if mismatch {
		return fmt.Errorf(
			"koios cache %q holds %s answers from %q but this run queries %q; "+
				"point %s at the recorded host, or give %s a cache of its own",
			cachePath, network, recorded, resolved,
			liveEnvKoiosBaseURL, liveEnvKoiosCachePath,
		)
	}
	if err := cache.PinRecordedSource(network); err != nil {
		return fmt.Errorf("pin koios cache source: %w", err)
	}
	return nil
}

// splitStakeMismatches separates real Dingo/Koios divergences from
// KoiosFault entries: an unparseable Koios active_stake value is a
// comparison Koios's own data made untrustworthy, not a Dingo divergence.
func splitStakeMismatches(
	mismatches []StakeMismatch,
) (real, faults []StakeMismatch) {
	for _, m := range mismatches {
		if m.KoiosFault {
			faults = append(faults, m)
		} else {
			real = append(real, m)
		}
	}
	return real, faults
}

// fromGenesisCounters accumulates the from-genesis verdicts across a run.
//
// The incomplete counters matter as much as the mismatch ones: a check that
// could not be trusted (a Koios fetch failure, a Dingo query error, a
// Koios-side data fault) is not a match. The verified counters count checks
// that reached a trustworthy verdict, match or mismatch. Without them a run
// whose every check was degraded finds zero mismatches and passes,
// indistinguishable from one that verified everything.
type fromGenesisCounters struct {
	epochsChecked                                 int
	ppMismatches, stakeMismatches, utxoMismatches int
	ppIncomplete, stakeIncomplete, utxoIncomplete int
	ppVerified, stakeVerified, utxoVerified       int
}

func (c *fromGenesisCounters) recordEpoch(
	r EpochResult,
	logger *slog.Logger,
) {
	c.epochsChecked++
	logger.Debug("epoch timing",
		"epoch", r.Epoch,
		"tx_info_flushes", r.TxInfoFlushCount,
		"tx_info_flush_elapsed", r.TxInfoFlushElapsed.String(),
		"protocol_params_and_stake_elapsed", r.ProtocolParamsAndStakeElapsed.String(),
		"utxo_elapsed", r.UTxOElapsed.String(),
	)

	// A Koios fetch failure arrives as a CategoryDBError mismatch entry, not
	// an error; DetermineStatus tells it (StatusError) apart from a real
	// disagreement (StatusFail).
	if r.ProtocolParamsErr != nil {
		c.ppIncomplete++
		logger.Warn("protocol params check did not run",
			"epoch", r.Epoch, "error", r.ProtocolParamsErr)
	} else {
		switch koiosparity.DetermineStatus(r.ProtocolParamsMismatches) {
		case koiosparity.StatusFail:
			c.ppMismatches++
			c.ppVerified++
			for _, m := range r.ProtocolParamsMismatches {
				logger.Warn("protocol params mismatch",
					"epoch", r.Epoch, "field", m.Field,
					"dingo", m.DingoValue, "koios", m.KoiosValue,
					"category", m.Category)
			}
		case koiosparity.StatusError:
			c.ppIncomplete++
			for _, m := range r.ProtocolParamsMismatches {
				logger.Warn("protocol params check incomplete",
					"epoch", r.Epoch, "field", m.Field,
					"dingo", m.DingoValue, "category", m.Category)
			}
		default:
			c.ppVerified++
			logger.Info("protocol params match", "epoch", r.Epoch)
		}
	}

	if r.StakeErr != nil {
		c.stakeIncomplete++
		logger.Warn("stake distribution check did not run",
			"epoch", r.Epoch, "error", r.StakeErr)
	} else {
		realMismatches, faults := splitStakeMismatches(r.StakeMismatches)
		if len(faults) > 0 {
			c.stakeIncomplete++
		} else {
			c.stakeVerified++
		}
		for _, m := range faults {
			logger.Warn("stake distribution check incomplete",
				"epoch", r.Epoch, "pool", m.PoolIDBech32,
				"dingo_stake", m.DingoStake, "koios_stake", m.KoiosStake,
				"reason", m.Reason)
		}
		if len(realMismatches) > 0 {
			c.stakeMismatches++
			for _, m := range realMismatches {
				logger.Warn("stake distribution mismatch",
					"epoch", r.Epoch, "pool", m.PoolIDBech32,
					"dingo_stake", m.DingoStake, "koios_stake", m.KoiosStake,
					"diff_lovelace", m.DiffLovelace, "reason", m.Reason)
			}
		} else if len(faults) == 0 {
			logger.Info("stake distribution match", "epoch", r.Epoch)
		}
	}

	switch {
	case r.UTxOSkipped:
		logger.Debug("utxo check skipped", "epoch", r.Epoch)
	case !r.UTxOAttempted:
		c.utxoIncomplete++
		logger.Debug("utxo check skipped (no genesis baseline)", "epoch", r.Epoch)
	case r.UTxOErr != nil:
		c.utxoIncomplete++
		logger.Warn("utxo check did not run",
			"epoch", r.Epoch, "error", r.UTxOErr)
	case len(r.UTxOMissing) > 0 || len(r.UTxOExtra) > 0 || len(r.UTxODiffers) > 0:
		c.utxoMismatches++
		c.utxoVerified++
		logger.Warn("utxo set mismatch",
			"epoch", r.Epoch, "missing", len(r.UTxOMissing),
			"extra", len(r.UTxOExtra), "differs", len(r.UTxODiffers),
			"dingo_ref_count", r.UTxORefCount,
			"missing_sample", r.UTxOMissing[:min(len(r.UTxOMissing), utxoRefSample)],
			"extra_sample", r.UTxOExtra[:min(len(r.UTxOExtra), utxoRefSample)])
		for _, d := range r.UTxODiffers {
			logger.Debug("utxo content differs", "epoch", r.Epoch, "detail", d)
		}
	default:
		c.utxoVerified++
		logger.Info("utxo set match",
			"epoch", r.Epoch, "ref_count", r.UTxORefCount)
	}
}

// result fails a run with a real mismatch, and a run that reached an epoch
// boundary but verified nothing in any of the three checks.
func (c *fromGenesisCounters) result() error {
	if c.ppMismatches > 0 || c.stakeMismatches > 0 || c.utxoMismatches > 0 {
		return fmt.Errorf(
			"ledger state diverged from Koios: %d protocol-param, %d stake, %d utxo mismatch epoch(s)",
			c.ppMismatches, c.stakeMismatches, c.utxoMismatches,
		)
	}
	if c.epochsChecked == 0 {
		return errors.New(
			"compared no epoch against Koios: check the Dingo address, or raise -timeout to reach an epoch boundary",
		)
	}
	if c.ppVerified == 0 && c.stakeVerified == 0 &&
		c.utxoVerified == 0 {
		return fmt.Errorf(
			"verified nothing against Koios across %d epoch(s): every protocol-param, "+
				"stake, and utxo check was incomplete (%d/%d/%d)",
			c.epochsChecked, c.ppIncomplete, c.stakeIncomplete, c.utxoIncomplete,
		)
	}
	return nil
}
