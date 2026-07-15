package ledger

import (
	"fmt"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestLedgerStateSnapshotPublicationIsImmutable verifies that publishing a
// replacement snapshot does not mutate snapshots retained by existing readers.
func TestLedgerStateSnapshotPublicationIsImmutable(t *testing.T) {
	ls := &LedgerState{
		currentEpoch: models.Epoch{
			EpochId: 7,
			Nonce:   []byte{7},
		},
		epochCache: []models.Epoch{{EpochId: 7, Nonce: []byte{7}}},
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.Point{Slot: 70, Hash: []byte{7}},
		},
		currentTipBlockNonce: []byte{17},
		transitionInfo:       hardfork.NewTransitionUnknown(),
	}
	// Publish the initial writer-owned state and retain the exact pointers a
	// reader could still be using when a later update is published.
	ls.publishSnapshotsLocked()

	oldConsensus := ls.consensus.Load()
	oldTip := ls.tip.Load()
	// Mutate both the scalar and slice-backed writer state, then publish a new
	// generation. The retained snapshots must continue to expose generation 7.
	ls.currentEpoch.EpochId = 8
	ls.currentEpoch.Nonce[0] = 8
	ls.epochCache[0].Nonce[0] = 8
	ls.currentTip.Point.Hash[0] = 8
	ls.currentTipBlockNonce[0] = 18
	ls.publishSnapshotsLocked()

	require.Equal(t, uint64(7), oldConsensus.currentEpoch.EpochId)
	require.Equal(t, byte(7), oldConsensus.currentEpoch.Nonce[0])
	require.Equal(t, byte(7), oldConsensus.epochCache[0].Nonce[0])
	require.Equal(t, byte(7), oldTip.currentTip.Point.Hash[0])
	require.Equal(t, byte(17), oldTip.currentTipBlockNonce[0])
}

// TestLedgerStateTipGetterReturnsDefensiveHashCopy verifies that callers cannot
// mutate the published tip hash through the value returned by Tip.
func TestLedgerStateTipGetterReturnsDefensiveHashCopy(t *testing.T) {
	ls := &LedgerState{currentTip: ochainsync.Tip{
		Point: ocommon.Point{Slot: 1, Hash: []byte{1, 2, 3}},
	}}
	ls.publishSnapshotsLocked()

	// Mutate only the caller-owned return value. A subsequent read must still
	// return the hash stored in the immutable tip snapshot.
	tip := ls.Tip()
	tip.Point.Hash[0] = 9
	require.Equal(t, byte(1), ls.Tip().Point.Hash[0])
}

// TestLedgerStateSnapshotsStayConsistentWithConcurrentReaders verifies that
// readers never observe fields from different generations within either the
// consensus snapshot or the tip snapshot while a writer repeatedly publishes.
func TestLedgerStateSnapshotsStayConsistentWithConcurrentReaders(
	t *testing.T,
) {
	ls := &LedgerState{}
	ls.publishSnapshotsLocked()

	const generations = 500
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	done := make(chan struct{})

	// Readers continuously validate generation markers encoded in the paired
	// fields. Any mismatched marker would indicate a torn snapshot read.
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				consensusState := ls.loadConsensusSnapshot()
				if consensusState.currentEpoch.EpochId !=
					uint64(consensusState.currentEra.Id) {
					select {
					case errCh <- fmt.Errorf("torn consensus snapshot"):
					default:
					}
					return
				}
				tipState := ls.loadTipSnapshot()
				if len(tipState.currentTipBlockNonce) > 0 &&
					tipState.currentTip.Point.Slot !=
						uint64(tipState.currentTipBlockNonce[0]) {
					select {
					case errCh <- fmt.Errorf("torn tip snapshot"):
					default:
					}
					return
				}
			}
		}()
	}

	// Publish many generations while all readers are active. The existing
	// LedgerState lock continues to serialize writers; readers use only atomics.
	for generation := 1; generation <= generations; generation++ {
		marker := byte(generation % 256)
		ls.Lock()
		ls.currentEpoch.EpochId = uint64(marker)
		ls.currentEra.Id = uint(marker)
		ls.currentTip.Point.Slot = uint64(marker)
		ls.currentTipBlockNonce = []byte{marker}
		ls.publishSnapshotsLocked()
		ls.Unlock()
	}
	close(done)
	wg.Wait()
	// Report the first consistency failure, if any. The channel is buffered so
	// a reader can record an error without blocking other goroutines.
	select {
	case err := <-errCh:
		require.NoError(t, err)
	default:
	}
}

// TestLedgerStatePairedSnapshotsUseOneGeneration verifies that callers which
// combine consensus and tip fields never observe adjacent publications while a
// writer is between the two atomic stores.
func TestLedgerStatePairedSnapshotsUseOneGeneration(t *testing.T) {
	ls := &LedgerState{}
	ls.publishSnapshotsLocked()

	const generations = 1_000
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	done := make(chan struct{})

	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				consensusState, tipState := ls.loadStateSnapshots()
				if consensusState.generation != tipState.generation ||
					consensusState.currentEpoch.EpochId !=
						tipState.currentTip.Point.Slot {
					select {
					case errCh <- fmt.Errorf(
						"cross-snapshot generation was torn",
					):
					default:
					}
					return
				}
			}
		}()
	}

	for generation := 1; generation <= generations; generation++ {
		ls.Lock()
		ls.currentEpoch.EpochId = uint64(generation)
		ls.currentTip.Point.Slot = uint64(generation)
		ls.publishSnapshotsLocked()
		ls.Unlock()
	}
	close(done)
	wg.Wait()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	default:
	}
}
