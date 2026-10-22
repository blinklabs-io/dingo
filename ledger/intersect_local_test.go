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
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// remoteCountingBlobStore counts GetBlock calls, the entry point a Bark
// wrapper uses to fall through to a remote archive. GetBlockLocal is the
// bounded local probe and is deliberately not counted.
type remoteCountingBlobStore struct {
	blob.BlobStore
	getBlockCalls      atomic.Int64
	getBlockLocalCalls atomic.Int64
}

func (s *remoteCountingBlobStore) GetBlock(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	s.getBlockCalls.Add(1)
	return s.BlobStore.GetBlock(txn, slot, hash)
}

func (s *remoteCountingBlobStore) GetBlockLocal(
	txn types.Txn,
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	s.getBlockLocalCalls.Add(1)
	return s.BlobStore.GetBlock(txn, slot, hash)
}

// GetIntersectPoint answers a peer-supplied membership question, so a point
// the node does not hold must not reach a block read that can fall through to
// a remote archive or a cloud object store.
func TestGetIntersectPointNonexistentPointsSkipRemoteCapableReads(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
	)
	knownHash := testHashBytes("intersect-known")
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(context.Background(), []chain.RawBlock{
			{
				Slot:        100,
				Hash:        knownHash,
				BlockNumber: 1,
				Type:        1,
				Cbor:        []byte{0x80},
			},
		}),
	)
	ls, err := NewLedgerState(
		LedgerStateConfig{
			Database:          db,
			ChainManager:      cm,
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	)
	require.NoError(t, err)
	ls.currentTip = ls.chain.Tip()
	ls.publishSnapshotsLocked()

	store := &remoteCountingBlobStore{BlobStore: db.Blob()}
	db.SetBlobStore(store)

	missing := make([]ocommon.Point, 0, 51)
	for i := range 50 {
		missing = append(missing, ocommon.NewPoint(
			uint64(50+i),
			testHashBytes(fmt.Sprintf("missing-%d", i)),
		))
	}
	intersect, err := ls.GetIntersectPoint(t.Context(), missing)
	require.NoError(t, err)
	require.Nil(t, intersect)
	require.Zero(
		t,
		store.getBlockCalls.Load(),
		"nonexistent points must not reach a remote-capable block read",
	)

	// A held point still intersects when mixed with the missing ones.
	known := ocommon.NewPoint(100, knownHash)
	intersect, err = ls.GetIntersectPoint(
		t.Context(),
		append(missing, known),
	)
	require.NoError(t, err)
	require.NotNil(t, intersect)
	require.Equal(t, known, *intersect)
	require.Zero(t, store.getBlockCalls.Load())
}

// Membership needs only the block's ID, so GetIntersectPoint must not load a
// block body for any point, held or not. On the s3 and gcs plugins a body
// read is a whole-object GET plus a hash check, which a peer can trigger with
// publicly known points.
func TestGetIntersectPointSkipsBlockBodyReads(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
	)
	blocks := make([]chain.RawBlock, 0, 20)
	known := make([]ocommon.Point, 0, 20)
	var prevHash []byte
	for i := range 20 {
		hash := testHashBytes(fmt.Sprintf("intersect-held-%d", i))
		blocks = append(blocks, chain.RawBlock{
			Slot:        uint64(100 + i),
			Hash:        hash,
			BlockNumber: uint64(1 + i),
			Type:        1,
			PrevHash:    prevHash,
			Cbor:        []byte{0x80},
		})
		prevHash = hash
		known = append(known, ocommon.NewPoint(uint64(100+i), hash))
	}
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(context.Background(), blocks),
	)
	ls, err := NewLedgerState(
		LedgerStateConfig{
			Database:          db,
			ChainManager:      cm,
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	)
	require.NoError(t, err)
	ls.currentTip = ls.chain.Tip()
	ls.publishSnapshotsLocked()

	store := &remoteCountingBlobStore{BlobStore: db.Blob()}
	db.SetBlobStore(store)

	points := append([]ocommon.Point{
		ocommon.NewPoint(99, testHashBytes("intersect-not-held")),
	}, known...)
	intersect, err := ls.GetIntersectPoint(t.Context(), points)
	require.NoError(t, err)
	require.NotNil(t, intersect)
	require.Equal(t, known[len(known)-1], *intersect)
	require.Zero(t, store.getBlockCalls.Load())
	require.Zero(
		t,
		store.getBlockLocalCalls.Load(),
		"intersection membership must not read block bodies",
	)
}
