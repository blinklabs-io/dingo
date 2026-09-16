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
// one). Runs until ctx is cancelled or the ChainSync session ends.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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
// A prior version of this file divided block.SlotNumber() by a per-network
// epoch-length constant -- wrong on preprod, and not fixable by simply
// using the right constant: preprod's Byron era ran for 4 real epochs
// (verified against config/cardano/preprod/config.json, which carries no
// TestShelleyHardForkAtEpoch, unlike preview's explicit 0) at Byron's own
// epoch length (10*k Byron slots, k=2160 -> 21600 slots/epoch, at Byron's
// own 20s slot duration -- 86400 raw slots in, exactly 4 Byron epochs),
// not Shelley's post-hard-fork 432000-slot epoch. No client-side arithmetic
// over the raw slot number alone can account for a per-network,
// historically-fixed Byron-era length without hardcoding it separately --
// asking Dingo, which already resolves this correctly, avoids needing to
// know it at all. (Byron's own startTime equaling Shelley's systemStart in
// both networks' genesis configs, cited by an earlier version of this
// comment as proof of a zero-length Byron era, proves no such thing: it
// holds on mainnet too, where Byron ran 208 real epochs -- systemStart is
// the slot-zero wall-clock reference, unrelated to when the Shelley hard
// fork actually happened.)
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
const (
	acquireRetries    = 10
	acquireRetryDelay = 200 * time.Millisecond
)

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
	// wall-clock time this epoch. Added after live testing found each of
	// TxInfoFlushCount, ProtocolParamsAndStakeElapsed, and UTxOElapsed had,
	// in turn, been the dominant cost at different points as chain activity
	// grew -- rather than continuing to guess and re-fix one at a time,
	// this makes the split visible every epoch going forward.
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
// retrying on ErrAcquireFailurePointNotOnChain -- see acquireRetries' doc
// comment. Returns ok=false, with the connection already closed, on any
// failure.
func acquireWithRetry(
	ctx context.Context,
	dingoAddr string,
	magic uint32,
	point pcommon.Point,
) (conn *ouroboros.Connection, lsq *localstatequery.LocalStateQuery, err error) {
	conn, dialErr := Dial(ctx, dingoAddr, magic)
	if dialErr != nil {
		return nil, nil, fmt.Errorf("dial: %w", dialErr)
	}
	lsq = conn.LocalStateQuery()
	if lsq == nil || lsq.Client == nil {
		conn.Close() //nolint:errcheck
		return nil, nil, errors.New("LocalStateQuery client unavailable")
	}
	var acquireErr error
	for attempt := 0; attempt < acquireRetries; attempt++ {
		acquireErr = lsq.Client.Acquire(&point)
		if acquireErr == nil {
			return conn, lsq, nil
		}
		if !errors.Is(acquireErr, localstatequery.ErrAcquireFailurePointNotOnChain) {
			break
		}
		select {
		case <-ctx.Done():
			conn.Close() //nolint:errcheck
			return nil, nil, ctx.Err()
		case <-time.After(acquireRetryDelay):
		}
	}
	conn.Close() //nolint:errcheck
	return nil, nil, fmt.Errorf("acquire: %w", acquireErr)
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
// checkUtxoRetentionWindow) is non-fatal to the caller: the UTxO half of
// this comparison is simply unavailable for the rest of this run, but
// protocol-params and stake-distribution checking continues regardless.
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

// RunFromGenesis is documented at the top of this file.
func RunFromGenesis(
	ctx context.Context,
	dingoAddr string,
	network string,
	magic uint32,
	koios *koiosparity.KoiosClient,
	report FromGenesisReporter,
	logf func(format string, args ...any),
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

	rawConn, dialErr := dialRaw(ctx, protoFromAddr(dingoAddr), dingoAddr)
	if dialErr != nil {
		return fmt.Errorf("dial raw chainsync connection: %w", dialErr)
	}

	var (
		lastEpoch          uint64
		haveLastEpoch      bool
		utxoRefs           UTxOSet
		utxoBaselineErr    error
		utxoAttempted      bool
		pendingTxHashes    []string
		txInfoFlushCount   int
		txInfoFlushElapsed time.Duration
		// utxoTaintedThisEpoch is set whenever a tx_info failure forced a
		// mid-epoch re-baseline (see flushPendingTxInfos) and cleared once
		// that epoch's result has been reported. A re-baseline replaces
		// utxoRefs with Dingo's own current answer, which the epoch
		// comparison below then diffs against Dingo's own answer again --
		// trivially equal by construction, not a real confirmation that
		// Koios agrees with anything. Without this flag, a Koios outage
		// during an epoch would be reported as "utxo set match" instead of
		// "not run": the re-baseline is the right recovery for later
		// epochs, but this epoch's own verdict must say the comparison did
		// not happen.
		utxoTaintedThisEpoch bool
	)

	// flushPendingTxInfos applies every buffered transaction hash's
	// input/output changes to utxoRefs, instead of the one-call-per-block
	// approach this originally replaced: confirmed live that firing a
	// separate /tx_info round trip for every block with at least one
	// transaction made a from-genesis run's epoch cadence collapse from
	// seconds to tens of minutes per epoch as real chain activity picked
	// up -- the per-call network latency to Koios, not payload size, was
	// the actual bottleneck.
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
				txInfos, err := koios.GetTxInfos(gctx, chunk)
				results[i] = txInfos
				errs[i] = err
				return nil
			})
		}
		_ = g.Wait() // errors are per-chunk in errs; nothing here to fail on
		txInfoFlushCount++
		txInfoFlushElapsed += time.Since(flushStart)

		failed := false
		for i, chunk := range chunks {
			if err := errs[i]; err != nil {
				logf(
					"nodeparity: koios tx_info fetch failed for %d pending tx(es): %v",
					len(chunk), err,
				)
				failed = true
				continue
			}
			UTxOChanges(utxoRefs, results[i])
		}
		pendingTxHashes = pendingTxHashes[:0]

		if failed {
			utxoTaintedThisEpoch = true
			refs, err := captureGenesisBaseline(ctx, dingoAddr, magic, point)
			if err != nil {
				utxoRefs = nil
				logf(
					"nodeparity: re-baseline after tx_info failure also failed "+
						"(UTxO comparison disabled for the rest of this run): %v",
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

					if !utxoAttempted {
						utxoAttempted = true
						refs, err := captureGenesisBaseline(ctx, dingoAddr, magic, point)
						if err != nil {
							utxoBaselineErr = err
							logf(
								"nodeparity: genesis UTxO baseline unavailable "+
									"(UTxO comparison disabled for this run): %v",
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
						return fmt.Errorf(
							"determine current epoch at slot %d: %w",
							block.SlotNumber(), err,
						)
					}
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

					// Protocol params and stake each get their own Acquire,
					// on their own connection -- see koios_check.go's doc
					// comment for why sharing one would needlessly cut them
					// off at UTxO's much tighter retention floor.
					psStart := time.Now()
					if psConn, lsqPS, err := acquireWithRetry(ctx, dingoAddr, magic, point); err != nil {
						result.ProtocolParamsErr = err
						result.StakeErr = err
					} else {
						mismatches, err := CheckProtocolParams(ctx, lsqPS.Client, koios, network, epoch)
						result.ProtocolParamsErr = err
						result.ProtocolParamsMismatches = mismatches

						stakeMismatches, err := CheckStakeDistribution(ctx, lsqPS.Client, koios, epoch)
						result.StakeErr = err
						result.StakeMismatches = stakeMismatches

						_ = lsqPS.Client.Release() //nolint:errcheck
						psConn.Close()             //nolint:errcheck
					}
					result.ProtocolParamsAndStakeElapsed = time.Since(psStart)

					utxoStart := time.Now()
					if utxoTaintedThisEpoch {
						// See utxoTaintedThisEpoch's doc comment: utxoRefs
						// was just re-baselined from Dingo's own answer at
						// this same point, so comparing it now would
						// silently pass by construction rather than
						// confirming anything against Koios.
						result.UTxOAttempted = true
						result.UTxOErr = errors.New(
							"tx_info fetch failed during this epoch; " +
								"UTxO reconstruction was re-baselined from " +
								"Dingo directly, so this epoch's comparison " +
								"would be against Dingo itself and was skipped",
						)
					} else if utxoRefs != nil {
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
					} else if utxoBaselineErr != nil {
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
						refs, err := captureGenesisBaseline(ctx, dingoAddr, magic, point)
						if err != nil {
							utxoRefs = nil
							logf(
								"nodeparity: rollback to slot %d invalidated the "+
									"UTxO reconstruction and re-baselining failed "+
									"(UTxO comparison disabled for the rest of this "+
									"run): %v",
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
						return fmt.Errorf(
							"determine current epoch at rollback slot %d: %w",
							point.Slot, err,
						)
					}
					haveLastEpoch = true
					lastEpoch = epoch
					return nil
				},
			),
		)),
	)
	if connErr != nil {
		rawConn.Close() //nolint:errcheck
		return fmt.Errorf("ouroboros.New (chainsync): %w", connErr)
	}
	defer csConn.Close() //nolint:errcheck

	stopOnCancel := context.AfterFunc(ctx, func() { csConn.Close() }) //nolint:errcheck
	defer stopOnCancel()

	cs := csConn.ChainSync()
	if cs == nil || cs.Client == nil {
		return errors.New("ChainSync client unavailable")
	}
	if err := cs.Client.Sync([]pcommon.Point{pcommon.NewPointOrigin()}); err != nil {
		return fmt.Errorf("start chainsync from origin: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case sessionErr, ok := <-csConn.ErrorChan():
		if !ok {
			return nil
		}
		return sessionErr
	}
}
