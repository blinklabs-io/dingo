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
	"bytes"
	"context"
	"encoding/binary"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type forkPathFreshnessFixture struct {
	ls      *LedgerState
	headers []mockHeader
	stamps  map[string]uint64
	lookups int
}

// newForkPathFreshnessFixture builds an applied anchor block and a peer
// suffix of queued headers that descends from it, as during catch-up.
func newForkPathFreshnessFixture(t *testing.T, queued int) *forkPathFreshnessFixture {
	t.Helper()
	nonce := bytes.Repeat([]byte{0x31}, lcommon.Blake2b256Size)
	anchor := createTestBlockWithNonceAtSlots(
		t,
		[32]byte{70},
		nonce,
		10,
		11,
		make([]byte, lcommon.Blake2b256Size),
		tamperNone,
	)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.PrimaryChain().AddLocalBlock(context.Background(), anchor.block),
	)
	anchorPoint := ocommon.NewPoint(
		anchor.block.SlotNumber(),
		anchor.block.Hash().Bytes(),
	)
	epoch := models.Epoch{
		EpochId:       5,
		StartSlot:     0,
		SlotLength:    1_000,
		LengthInSlots: 10_000,
		EraId:         eras.BabbageEraDesc.Id,
		Nonce:         nonce,
	}
	f := &forkPathFreshnessFixture{
		ls: &LedgerState{
			db:           db,
			chain:        cm.PrimaryChain(),
			epochCache:   []models.Epoch{epoch},
			currentEpoch: epoch,
			currentEra:   eras.BabbageEraDesc,
			currentTip: ochainsync.Tip{
				Point:       anchorPoint,
				BlockNumber: anchor.block.BlockNumber(),
			},
		},
		stamps: make(map[string]uint64),
	}
	f.ls.publishSnapshotsLocked()
	prev := anchor.block.Hash()
	for i := range queued {
		var hash lcommon.Blake2b256
		binary.BigEndian.PutUint64(hash[:8], uint64(1000+i))
		header := mockHeader{
			hash:        hash,
			prevHash:    prev,
			blockNumber: anchor.block.BlockNumber() + uint64(i) + 1,
			slot:        anchor.block.SlotNumber() + uint64(i) + 1,
		}
		f.headers = append(f.headers, header)
		prev = hash
	}
	f.ls.config.PeerHeaderLookupFunc = func(
		connID ouroboros.ConnectionId,
		hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		f.lookups++
		for _, header := range f.headers {
			stamp, ok := f.stamps[string(header.hash[:])]
			if !ok || !bytes.Equal(header.hash[:], hash) {
				continue
			}
			return ChainsyncEvent{
				ConnectionId:                       connID,
				Point:                              ocommon.NewPoint(header.slot, header.hash[:]),
				BlockHeader:                        header,
				HeaderCryptoVerificationGeneration: stamp,
			}, header.prevHash[:], true
		}
		return ChainsyncEvent{}, nil, false
	}
	return f
}

// stamp records header i as verified under the current snapshot generation.
func (f *forkPathFreshnessFixture) stamp(i int) {
	f.stamps[string(f.headers[i].hash[:])] =
		f.ls.CurrentHeaderCryptoVerificationGeneration()
}

// TestPeerHeaderForkPathKeepsVerdictAcrossTipOnlyPublication covers catch-up:
// a block applied while the peer's next headers are still queued publishes a
// snapshot that moves only the tip. The queued ancestor's verdict was made
// against the same epoch nonce, stake snapshot, parameters and era, so its
// child must not be deferred.
func TestPeerHeaderForkPathKeepsVerdictAcrossTipOnlyPublication(t *testing.T) {
	t.Parallel()
	f := newForkPathFreshnessFixture(t, 2)
	f.stamp(0)
	f.ls.publishSnapshotsLocked()

	ancestor, path, found, err := f.ls.peerHeaderForkPath(
		ouroboros.ConnectionId{},
		f.headers[1],
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, path, 2)
	require.Equal(t, f.headers[0].slot-1, ancestor.Slot)
}

func TestPeerHeaderForkPathRevalidatesUnstampedAncestryAndItsCachedChild(
	t *testing.T,
) {
	t.Parallel()
	f := newForkPathFreshnessFixture(t, 4)
	connID := ouroboros.ConnectionId{}
	for index := range 3 {
		f.stamps[string(f.headers[index].hash[:])] = 0
	}

	_, path, found, err := f.ls.peerHeaderForkPath(connID, f.headers[2])
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, path, 3)

	_, path, found, err = f.ls.peerHeaderForkPath(connID, f.headers[3])
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, path, 4)
}

// TestPeerHeaderForkPathDefersVerdictAfterConsensusChange keeps the security
// half of the freshness rule: a verdict made before the epoch nonce, era or
// stake-bearing epoch state changed is not reused.
func TestPeerHeaderForkPathDefersVerdictAfterConsensusChange(t *testing.T) {
	t.Parallel()
	cases := map[string]func(ls *LedgerState){
		"epoch nonce": func(ls *LedgerState) {
			epochs := slices.Clone(ls.epochCache)
			epochs[0].Nonce = bytes.Repeat([]byte{0x32}, lcommon.Blake2b256Size)
			ls.epochCache = epochs
			ls.currentEpoch = epochs[0]
		},
		"epoch rollover": func(ls *LedgerState) {
			next := ls.currentEpoch
			next.EpochId++
			next.StartSlot += uint64(next.LengthInSlots)
			ls.epochCache = append(slices.Clone(ls.epochCache), next)
			ls.currentEpoch = next
		},
		"era": func(ls *LedgerState) {
			ls.currentEra = eras.ConwayEraDesc
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newForkPathFreshnessFixture(t, 2)
			f.stamp(0)
			change(f.ls)
			f.ls.publishSnapshotsLocked()

			_, _, found, err := f.ls.peerHeaderForkPath(
				ouroboros.ConnectionId{},
				f.headers[1],
			)
			require.False(t, found)
			require.ErrorIs(t, err, errHeaderVerificationDeferred)
		})
	}
}

// TestPeerHeaderForkPathCacheSurvivesTipOnlyPublication checks that the
// per-connection path cache is reused across applied blocks, so each new
// queued header looks up only its parent instead of walking the whole queue,
// and that the parent must itself carry a verdict in the current context.
func TestPeerHeaderForkPathCacheSurvivesTipOnlyPublication(t *testing.T) {
	t.Parallel()
	f := newForkPathFreshnessFixture(t, 7)
	for i := range 4 {
		f.stamp(i)
	}
	_, _, found, err := f.ls.peerHeaderForkPath(
		ouroboros.ConnectionId{},
		f.headers[4],
	)
	require.NoError(t, err)
	require.True(t, found)
	f.stamp(4)
	f.ls.publishSnapshotsLocked()
	lookups := f.lookups

	_, path, found, err := f.ls.peerHeaderForkPath(
		ouroboros.ConnectionId{},
		f.headers[5],
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, path, 6)
	require.Equal(t, lookups+1, f.lookups, "cached path was walked again")

	// headers[5] is never stamped: its child must not extend the cache.
	_, _, found, err = f.ls.peerHeaderForkPath(
		ouroboros.ConnectionId{},
		f.headers[6],
	)
	require.False(t, found)
	require.ErrorIs(t, err, errHeaderVerificationDeferred)
}

func TestValidatePeerForkOpCertCountersKeepsCacheAcrossTipOnlyPublication(
	t *testing.T,
) {
	t.Parallel()
	f := newForkPathFreshnessFixture(t, 0)
	ctx := context.Background()
	connID := ouroboros.ConnectionId{}
	anchor := f.ls.currentTip.Point
	blockNumber := f.ls.currentTip.BlockNumber + 1
	nonce := bytes.Repeat([]byte{0x31}, lcommon.Blake2b256Size)
	localBlock := createTestBlockWithNonceAtSlotsAndBlockNumber(
		t,
		[32]byte{72},
		nonce,
		anchor.Slot+1,
		anchor.Slot+100,
		anchor.Hash,
		&blockNumber,
		tamperNone,
		0,
	)
	targetBlock := createTestBlockWithNonceAtSlotsAndBlockNumber(
		t,
		[32]byte{71},
		nonce,
		localBlock.block.SlotNumber()+1,
		anchor.Slot+200,
		anchor.Hash,
		&blockNumber,
		tamperNone,
		0,
	)
	target := targetBlock.block.Header()

	_, path, found, err := f.ls.peerHeaderForkPathContext(
		ctx,
		connID,
		target,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, path, 1)
	initialGeneration, contextGeneration :=
		f.ls.currentHeaderVerificationContext()

	require.NoError(t, f.ls.chain.AddLocalBlock(ctx, localBlock.block))
	f.ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(
			localBlock.block.SlotNumber(),
			localBlock.block.Hash().Bytes(),
		),
		BlockNumber: localBlock.block.BlockNumber(),
	}
	f.ls.publishSnapshotsLocked()
	currentGeneration, currentContext := f.ls.currentHeaderVerificationContext()
	require.Greater(t, currentGeneration, initialGeneration)
	require.Equal(t, contextGeneration, currentContext)
	require.True(t, headerVerdictInContext(
		initialGeneration,
		currentContext,
		currentGeneration,
	))

	require.NoError(t, f.ls.validatePeerForkOpCertCounters(
		ctx,
		connID,
		target,
	))
}

// TestPeerChainSelectionEpochNonceTreatsQueuedParentAsLocalChain covers the
// first headers of an epoch whose nonce the ledger has not computed yet. Such
// a header is deferred, and when it extends the local chain it is queued for
// block-fetch but kept out of the peer's observed history. Its child therefore
// has no recorded ancestry; because the parent is queued on the local chain
// the child is a linear extension, not a fork, and must fall through to local
// verification (which defers it the same way) rather than be withheld. A
// withheld child is dropped silently, every later header from the peer is
// deferred behind it, and the queue never fills enough to start block-fetch.
func TestPeerChainSelectionEpochNonceTreatsQueuedParentAsLocalChain(
	t *testing.T,
) {
	t.Parallel()
	f := newForkPathFreshnessFixture(t, 2)
	require.NoError(
		t,
		f.ls.chain.AddBlockHeader(context.Background(), f.headers[0]),
	)
	snapshot, _ := f.ls.loadConsensusAndTipSnapshots()

	_, _, fork, err := f.ls.peerChainSelectionEpochNonce(
		ouroboros.ConnectionId{},
		f.headers[1],
		snapshot,
	)
	require.NoError(t, err)
	require.False(t, fork)
}

func TestPeerChainSelectionEpochNonceTreatsUnverifiedQueuedParentAsLocalChain(
	t *testing.T,
) {
	t.Parallel()
	f := newForkPathFreshnessFixture(t, 2)
	connID := ouroboros.ConnectionId{}
	parentKey := string(f.headers[0].hash[:])
	f.stamps[parentKey] = 0
	require.NoError(
		t,
		f.ls.chain.AddBlockHeader(context.Background(), f.headers[0]),
	)
	snapshot, _ := f.ls.loadConsensusAndTipSnapshots()

	_, _, fork, err := f.ls.peerChainSelectionEpochNonce(
		connID,
		f.headers[1],
		snapshot,
	)
	require.NoError(t, err)
	require.False(t, fork)
}

func TestPeerHeaderForkPathTreatsUnverifiedQueuedParentAsLocalChain(
	t *testing.T,
) {
	t.Parallel()
	f := newForkPathFreshnessFixture(t, 2)
	connID := ouroboros.ConnectionId{}
	f.stamp(0)
	_, _, found, err := f.ls.peerHeaderForkPath(connID, f.headers[0])
	require.NoError(t, err)
	require.True(t, found)
	require.NoError(
		t,
		f.ls.chain.AddBlockHeader(context.Background(), f.headers[0]),
	)
	f.stamps[string(f.headers[0].hash[:])] = 0

	_, _, found, err = f.ls.peerHeaderForkPath(connID, f.headers[1])
	require.NoError(t, err)
	require.False(t, found)
}

// TestPeerChainSelectionEpochNonceWithholdsUnknownParent keeps the other half:
// a header whose parent is neither queued nor in the peer's history still
// cannot be authenticated for chain selection.
func TestPeerChainSelectionEpochNonceWithholdsUnknownParent(t *testing.T) {
	t.Parallel()
	f := newForkPathFreshnessFixture(t, 2)
	snapshot, _ := f.ls.loadConsensusAndTipSnapshots()

	_, _, fork, err := f.ls.peerChainSelectionEpochNonce(
		ouroboros.ConnectionId{},
		f.headers[1],
		snapshot,
	)
	require.ErrorIs(t, err, ErrHeaderVerificationWithheld)
	require.False(t, fork)
}
