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

package ledger

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/blinklabs-io/dingo/database"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func (ls *LedgerState) preverifyReplayHeaders(
	ctx context.Context,
	blocks []gledger.Block,
) ([]*replayHeaderVerification, error) {
	results, jobs, err := ls.prepareReplayHeaders(ctx, blocks)
	if err != nil {
		return nil, err
	}
	workers := startReplayHeaderJobs(
		ctx,
		blocks,
		results,
		jobs,
		verifyReplayHeader,
	)
	defer workers.stop()
	if err := workers.wait(); err != nil {
		return nil, err
	}
	return results, nil
}

func TestReplayHeaderPreverificationRejectsBadCrypto(t *testing.T) {
	t.Parallel()
	for name, tamper := range map[string]tamperOption{
		"valid": tamperNone, "KES": tamperKESSig,
		"VRF": tamperVRFProof, "opcert": tamperOpCertSig,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tb := createTestBlock(t, [32]byte{90 + byte(tamper)}, 0, tamper)
			ls, db := newEligibilityTestLedger(t, tb.epochNonce)
			ls.config.VerifyReplayHeaders = true
			seedBlockPoolRegistration(t, db, tb.block)
			pool := tb.block.IssuerVkey().Hash()
			seedPoolStakeSnapshot(t, db, 4, pool[:], 1_000_000_000)
			results, err := ls.preverifyReplayHeaders(
				t.Context(),
				[]gledger.Block{tb.block, tb.block},
			)
			require.NoError(t, err)
			require.NotNil(t, results[0])
			ctx := withReplayHeaderVerification(t.Context(), results, 0)
			point := ocommon.NewPoint(
				tb.block.SlotNumber(),
				tb.block.Hash().Bytes(),
			)
			err = ls.verifyDeferredBlockHeaderState(ctx, nil, point, tb.block)
			if tamper == tamperNone {
				require.NoError(t, err)
			} else {
				require.Error(t, results[0].err)
				var rejected *headerValidationError
				require.ErrorAs(t, err, &rejected)
				require.Equal(t, point, rejected.BlockPoint)
			}
		})
	}
}

func TestReplayHeaderReceiptIgnoredOutsideReplay(t *testing.T) {
	t.Parallel()
	for name, tamper := range map[string]tamperOption{
		"valid": tamperNone, "invalid KES": tamperKESSig,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tb := createTestBlock(t, [32]byte{108 + byte(tamper)}, 0, tamper)
			ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
			ls.config.VerifyReplayHeaders = true
			results, err := ls.preverifyReplayHeaders(
				t.Context(),
				[]gledger.Block{tb.block, tb.block},
			)
			require.NoError(t, err)
			require.NotNil(t, results[0])
			// A receipt must not override either crypto verdict outside replay.
			if tamper == tamperNone {
				results[0].err = errors.New("replay-only verdict")
			} else {
				results[0].err = nil
			}
			ls.config.VerifyReplayHeaders = false
			ctx := withReplayHeaderVerification(t.Context(), results, 0)
			_, _, err = ls.verifyBlockHeaderStatelessCryptoWithCache(
				ctx,
				tb.block,
				false,
			)
			if tamper == tamperNone && err != nil {
				t.Fatalf(
					"replay-disabled verification consumed receipt: %v",
					err,
				)
			}
			if tamper != tamperNone && err == nil {
				t.Fatal(
					"replay-disabled verification accepted bad KES from receipt",
				)
			}
		})
	}
}

func TestReplayHeaderPreverificationBindsInputsAndParameters(t *testing.T) {
	t.Parallel()
	tb := createTestBlock(t, [32]byte{95}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.config.VerifyReplayHeaders = true
	results, err := ls.preverifyReplayHeaders(
		t.Context(),
		[]gledger.Block{tb.block, tb.block},
	)
	require.NoError(t, err)
	require.NotNil(t, results[0])
	result := results[0]
	ctx := withReplayHeaderVerification(t.Context(), results, 0)
	lookup := func(epoch uint64, nonce string, slots, evolutions uint64) bool {
		_, ok := replayHeaderVerificationFrom(
			ctx,
			tb.block,
			epoch,
			nonce,
			slots,
			evolutions,
		)
		return ok
	}
	require.True(
		t,
		lookup(
			result.epoch,
			result.nonce,
			result.kesSlots,
			result.kesEvolutions,
		),
	)
	require.False(
		t,
		lookup(
			result.epoch+1,
			result.nonce,
			result.kesSlots,
			result.kesEvolutions,
		),
	)
	if lookup(
		result.epoch,
		"different",
		result.kesSlots,
		result.kesEvolutions,
	) {
		t.Fatal("changed epoch nonce reused verification")
	}
	require.False(
		t,
		lookup(
			result.epoch,
			result.nonce,
			result.kesSlots+1,
			result.kesEvolutions,
		),
	)
	require.False(
		t,
		lookup(
			result.epoch,
			result.nonce,
			result.kesSlots,
			result.kesEvolutions+1,
		),
	)
	// Changing a decoded signature must invalidate a result even though
	// DecodeStoreCbor still holds the original header encoding.
	tb.block.header.Signature[0] ^= 1
	if lookup(
		result.epoch,
		result.nonce,
		result.kesSlots,
		result.kesEvolutions,
	) {
		t.Fatal("changed KES signature reused verification")
	}
	_, _, err = ls.verifyBlockHeaderStatelessCryptoWithCache(
		ctx,
		tb.block,
		false,
	)
	require.Error(t, err)
}

func TestReplayHeaderPreverificationKeepsOrderedStateChecks(t *testing.T) {
	t.Parallel()
	tb := createTestBlock(t, [32]byte{96}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.config.VerifyReplayHeaders = true
	results, err := ls.preverifyReplayHeaders(
		t.Context(),
		[]gledger.Block{tb.block, tb.block},
	)
	require.NoError(t, err)
	require.NotNil(t, results[0])
	require.NoError(t, results[0].err)
	ctx := withReplayHeaderVerification(t.Context(), results, 0)
	point := ocommon.NewPoint(tb.block.SlotNumber(), tb.block.Hash().Bytes())
	err = ls.verifyDeferredBlockHeaderState(ctx, nil, point, tb.block)
	if _, ok := errors.AsType[*headerValidationError](err); !ok {
		t.Fatalf("missing ordered state rejection: %v", err)
	}
	require.Contains(t, err.Error(), "registration lookup failed")
}

func TestReplayHeaderPreverificationPreservesWorkerFailure(t *testing.T) {
	t.Parallel()
	tb := createTestBlock(t, [32]byte{97}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.config.VerifyReplayHeaders = true
	results, err := ls.preverifyReplayHeaders(
		t.Context(),
		[]gledger.Block{tb.block, tb.block},
	)
	require.NoError(t, err)
	require.NotNil(t, results[0])
	workerErr := errors.New("worker verification failed")
	results[0].err = workerErr
	ctx := withReplayHeaderVerification(t.Context(), results, 0)
	_, _, err = ls.verifyBlockHeaderStatelessCryptoWithCache(
		ctx,
		tb.block,
		false,
	)
	if !errors.Is(err, workerErr) {
		t.Fatalf("lost ordered worker failure: %v", err)
	}
}

func TestReplayHeaderWorkersDrainOnCancellation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		started, release := make(chan struct{}, 2), make(chan struct{})
		releaseAll := sync.OnceFunc(func() { close(release) })
		jobs := make(chan int, 2)
		jobs <- 0
		jobs <- 1
		close(jobs)
		var active atomic.Int32
		done := make(chan error, 1)
		results := []*replayHeaderVerification{
			{done: make(chan struct{})}, {done: make(chan struct{})},
		}
		workers := startReplayHeaderJobs(
			ctx,
			make([]gledger.Block, 2),
			results,
			jobs,
			func(gledger.Block, *replayHeaderVerification) {
				active.Add(1)
				started <- struct{}{}
				<-release
				active.Add(-1)
			},
		)
		defer func() {
			releaseAll()
			workers.stop()
		}()
		<-started
		go func() { done <- workers.wait() }()
		cancel()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("returned before worker drained: %v", err)
		default:
		}
		// Release every worker before any failing assertion or teardown.
		releaseAll()
		require.ErrorIs(t, <-done, context.Canceled)
		require.Zero(t, active.Load())
	})
}

func TestReplayHeaderVerdictWaitsForCrypto(t *testing.T) {
	t.Parallel()
	tb := createTestBlock(t, [32]byte{100}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.config.VerifyReplayHeaders = true
	synctest.Test(t, func(t *testing.T) {
		results, jobs, err := ls.prepareReplayHeaders(
			t.Context(),
			[]gledger.Block{tb.block, tb.block},
		)
		require.NoError(t, err)
		require.NotNil(t, results[0])
		release := make(chan struct{})
		releaseAll := sync.OnceFunc(func() { close(release) })
		workerErr := errors.New("unfinished header crypto rejected")
		workers := startReplayHeaderJobs(
			t.Context(),
			[]gledger.Block{tb.block, tb.block},
			results,
			jobs,
			func(_ gledger.Block, result *replayHeaderVerification) {
				<-release
				result.err = workerErr
			},
		)
		defer func() {
			releaseAll()
			workers.stop()
		}()
		ctx := withReplayHeaderVerification(t.Context(), results, 0)
		verdict := make(chan error, 1)
		go func() {
			_, _, err := ls.verifyBlockHeaderStatelessCryptoWithCache(
				ctx,
				tb.block,
				false,
			)
			verdict <- err
		}()
		synctest.Wait()
		select {
		case err := <-verdict:
			t.Fatalf("used header verdict before crypto completed: %v", err)
		default:
		}
		releaseAll()
		require.ErrorIs(t, <-verdict, workerErr)
	})
}

func TestReplayHeaderCancellationIsNotRejection(t *testing.T) {
	t.Parallel()
	for name, workerErr := range map[string]error{
		"canceled": context.Canceled,
		"deadline": context.DeadlineExceeded,
		"wrapped":  fmt.Errorf("header work canceled: %w", context.Canceled),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tb := createTestBlock(t, [32]byte{101}, 0, tamperNone)
			ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
			ls.config.VerifyReplayHeaders = true
			results, err := ls.preverifyReplayHeaders(
				t.Context(),
				[]gledger.Block{tb.block, tb.block},
			)
			require.NoError(t, err)
			require.NotNil(t, results[0])
			results[0].err = workerErr
			ctx := withReplayHeaderVerification(t.Context(), results, 0)
			point := ocommon.NewPoint(
				tb.block.SlotNumber(),
				tb.block.Hash().Bytes(),
			)
			err = ls.verifyDeferredBlockHeaderState(ctx, nil, point, tb.block)
			require.ErrorIs(t, err, workerErr)
			if _, ok := errors.AsType[*headerValidationError](err); ok {
				t.Fatalf(
					"cancellation classified as a rejected header: %v",
					err,
				)
			}
		})
	}
}

func TestReplayHeaderWorkersStopDrainsPending(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const count = blockPipelineMaxWorkers + 3
		results := make([]*replayHeaderVerification, count)
		jobs := make(chan int, count)
		for i := range count {
			results[i] = &replayHeaderVerification{done: make(chan struct{})}
			jobs <- i
		}
		close(jobs)
		started, release := make(chan struct{}, count), make(chan struct{})
		releaseAll := sync.OnceFunc(func() { close(release) })
		var active atomic.Int32
		workers := startReplayHeaderJobs(
			t.Context(),
			make([]gledger.Block, count),
			results,
			jobs,
			func(gledger.Block, *replayHeaderVerification) {
				active.Add(1)
				started <- struct{}{}
				<-release
				active.Add(-1)
			},
		)
		defer func() {
			releaseAll()
			workers.stop()
		}()
		<-started
		stopped := make(chan struct{})
		go func() {
			workers.stop()
			close(stopped)
		}()
		synctest.Wait()
		select {
		case <-stopped:
			t.Fatal("stop returned before crypto workers drained")
		default:
		}
		releaseAll()
		<-stopped
		require.Zero(t, active.Load())
		for _, result := range results {
			select {
			case <-result.done:
			default:
				t.Fatal("abandoned pending header verification")
			}
		}
		require.ErrorIs(t, results[count-1].err, context.Canceled)
	})
}

type replayHeaderPanicBlock struct{ gledger.Block }

func (replayHeaderPanicBlock) Header() gledger.BlockHeader {
	panic("header verifier panic")
}

func TestReplayHeaderPreverificationPreservesPanicRecovery(t *testing.T) {
	t.Parallel()
	tb := createTestBlock(t, [32]byte{98}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	ls.config.VerifyReplayHeaders = true
	results, err := ls.preverifyReplayHeaders(
		t.Context(),
		[]gledger.Block{tb.block, tb.block},
	)
	require.NoError(t, err)
	require.NotNil(t, results[0])
	require.NotPanics(t, func() {
		verifyReplayHeader(replayHeaderPanicBlock{tb.block}, results[0])
	})
	require.True(t, results[0].panicked)
	ctx := withReplayHeaderVerification(t.Context(), results, 0)
	point := ocommon.NewPoint(tb.block.SlotNumber(), tb.block.Hash().Bytes())
	err = db.MetadataTxn(ctx, true).Do(func(txn *database.Txn) error {
		return ls.verifyDeferredBlockHeaderState(ctx, txn, point, tb.block)
	})
	if !errors.Is(err, database.ErrTxnPanic) {
		t.Fatalf("verifier panic did not reach transaction rollback: %v", err)
	}
	require.ErrorContains(t, err, "header verifier panic")
	results, err = ls.preverifyReplayHeaders(t.Context(), []gledger.Block{
		replayHeaderPanicBlock{tb.block}, tb.block,
	})
	require.Nil(t, results)
	require.ErrorIs(t, err, database.ErrTxnPanic)
}
