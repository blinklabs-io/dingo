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
)

// previewPreprodEpochLengthSlots is the epoch length, in slots, on both
// preview and preprod (config/cardano/{preview,preprod}/shelley-genesis.json's
// epochLength) -- the two networks this comparison ever runs against (see
// koios_check.go's doc comment), so a single constant is sufficient rather
// than a per-network lookup.
const previewPreprodEpochLengthSlots = 86400

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
		lastEpoch       uint64
		haveLastEpoch   bool
		utxoRefs        UTxOSet
		utxoBaselineErr error
		utxoAttempted   bool
		pendingTxHashes []string
	)

	// flushPendingTxInfos applies every buffered transaction hash's
	// input/output changes to utxoRefs in one koios.GetTxInfos call (which
	// itself still chunks at koiosparity.KoiosTxInfoBatchSize internally),
	// instead of the one-call-per-block approach this replaced: confirmed
	// live that firing a separate /tx_info round trip for every block with
	// at least one transaction made a from-genesis run's epoch cadence
	// collapse from seconds to tens of minutes per epoch as real chain
	// activity picked up -- the per-call network latency to Koios, not
	// payload size, was the actual bottleneck. Buffering up to
	// KoiosTxInfoBatchSize hashes across multiple blocks before flushing
	// cuts the number of round trips roughly in proportion to the average
	// number of transactions per block.
	flushPendingTxInfos := func() {
		if utxoRefs == nil || len(pendingTxHashes) == 0 {
			return
		}
		txInfos, err := koios.GetTxInfos(ctx, pendingTxHashes)
		if err != nil {
			logf(
				"nodeparity: koios tx_info fetch failed for %d pending tx(es), "+
					"UTxO reconstruction may now drift: %v",
				len(pendingTxHashes), err,
			)
		} else {
			UTxOChanges(utxoRefs, txInfos)
		}
		pendingTxHashes = pendingTxHashes[:0]
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

					if !utxoAttempted {
						utxoAttempted = true
						point := pcommon.NewPoint(
							block.SlotNumber(), block.Hash().Bytes(),
						)
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
						if len(pendingTxHashes) >= koiosparity.KoiosTxInfoBatchSize {
							flushPendingTxInfos()
						}
					}

					epoch := block.SlotNumber() / previewPreprodEpochLengthSlots
					if haveLastEpoch && epoch <= lastEpoch {
						return nil
					}
					haveLastEpoch = true
					lastEpoch = epoch

					// About to Acquire and compare at this exact point --
					// flush any hashes still buffered below the threshold
					// above so the reconstruction is current through this
					// block, not just through the last flush.
					flushPendingTxInfos()

					point := pcommon.NewPoint(
						block.SlotNumber(), block.Hash().Bytes(),
					)
					result := EpochResult{Epoch: epoch}

					// Protocol params and stake each get their own Acquire,
					// on their own connection -- see koios_check.go's doc
					// comment for why sharing one would needlessly cut them
					// off at UTxO's much tighter retention floor.
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

					if utxoRefs != nil {
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
								result.UTxOMissing, result.UTxOExtra, result.UTxODiffers =
									UTxODiff(utxoRefs, dingoSet)
							}
							_ = lsqUtxo.Client.Release() //nolint:errcheck
							utxoConn.Close()             //nolint:errcheck
						}
					} else if utxoBaselineErr != nil {
						result.UTxOAttempted = false
					}

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
					haveLastEpoch = true
					lastEpoch = point.Slot / previewPreprodEpochLengthSlots
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
