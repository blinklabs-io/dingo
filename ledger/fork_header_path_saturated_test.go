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
	"fmt"
	"sync"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
)

type saturatedForkPathScenario struct {
	fixture *chainsyncRollbackFixture
	headers []mockHeader
	mu      sync.Mutex
	stored  map[string]models.Block
	known   map[string]mockHeader
	// blockReads and peerReads count block-store and peer-record lookups.
	blockReads int
	peerReads  int
}

// newSaturatedForkPathScenario builds a peer chain of limit+extra headers
// extending the local tip. Headers become visible to the peer lookup only
// once delivered.
func newSaturatedForkPathScenario(
	t *testing.T,
	extra int,
) *saturatedForkPathScenario {
	t.Helper()
	fixture := newChainsyncRollbackFixture(t)
	s := &saturatedForkPathScenario{
		fixture: fixture,
		stored:  make(map[string]models.Block),
		known:   make(map[string]mockHeader),
	}
	s.stored[string(fixture.currentTip.Point.Hash)] = models.Block{
		Hash:   fixture.currentTip.Point.Hash,
		Slot:   fixture.currentTip.Point.Slot,
		Number: fixture.currentTip.BlockNumber,
		Type:   uint(gledger.BlockTypeBabbage),
	}
	fixture.ls.lookupBlockByHash = func(hash []byte) (models.Block, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.blockReads++
		if block, ok := s.stored[string(hash)]; ok {
			return block, nil
		}
		return models.Block{}, models.ErrBlockNotFound
	}
	s.headers = make([]mockHeader, fixture.ls.peerHeaderHistoryLimit()+extra)
	prevHash := fixture.currentTip.Point.Hash
	for i := range s.headers {
		s.headers[i] = mockHeader{
			hash: lcommon.NewBlake2b256(
				testHashBytes(fmt.Sprintf("saturated-%d", i)),
			),
			prevHash:    lcommon.NewBlake2b256(prevHash),
			blockNumber: fixture.currentTip.BlockNumber + uint64(i) + 1,
			slot:        fixture.currentTip.Point.Slot + uint64(i) + 1,
		}
		prevHash = s.headers[i].Hash().Bytes()
	}
	fixture.ls.config.PeerHeaderLookupFunc = func(
		_ ouroboros.ConnectionId,
		hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.peerReads++
		header, ok := s.known[string(hash)]
		if !ok {
			return ChainsyncEvent{}, nil, false
		}
		return ChainsyncEvent{
			Point:       ocommon.NewPoint(header.slot, hash),
			BlockHeader: header,
			HeaderCryptoVerificationGeneration: fixture.ls.
				CurrentHeaderCryptoVerificationGeneration(),
		}, header.PrevHash().Bytes(), true
	}
	return s
}

// deliver records header i in the peer history, resets the lookup counters
// and resolves the header's fork path.
func (s *saturatedForkPathScenario) deliver(
	i int,
) (ocommon.Point, []gledger.BlockHeader, bool, error) {
	header := s.headers[i]
	s.mu.Lock()
	s.known[string(header.Hash().Bytes())] = header
	s.blockReads, s.peerReads = 0, 0
	s.mu.Unlock()
	return s.fixture.ls.peerHeaderForkPath(s.fixture.connId, header)
}

func (s *saturatedForkPathScenario) reads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.blockReads + s.peerReads
}

// adopt adds headers to the primary chain and the block store.
func (s *saturatedForkPathScenario) adopt(t *testing.T, headers []mockHeader) {
	t.Helper()
	raw := make([]chain.RawBlock, 0, len(headers))
	s.mu.Lock()
	for _, header := range headers {
		raw = append(raw, chain.RawBlock{
			Slot:        header.slot,
			Hash:        header.Hash().Bytes(),
			BlockNumber: header.blockNumber,
			Type:        uint(gledger.BlockTypeBabbage),
			PrevHash:    header.PrevHash().Bytes(),
			Cbor:        []byte{0x80},
		})
		s.stored[string(header.Hash().Bytes())] = models.Block{
			Hash:   header.Hash().Bytes(),
			Slot:   header.slot,
			Number: header.blockNumber,
			Type:   uint(gledger.BlockTypeBabbage),
		}
	}
	s.mu.Unlock()
	require.NoError(t, s.fixture.ls.chain.AddRawBlocks(t.Context(), raw))
}

// A peer far ahead of the local tip keeps delivering headers after its cached
// path reaches the history limit. Such a header cannot be verified until the
// local tip advances, so each delivery must not repeat the full ancestry walk
// (one block lookup and one peer record lookup per step).
func TestPeerHeaderForkPathSaturatedCacheSkipsAncestryWalk(t *testing.T) {
	t.Parallel()

	s := newSaturatedForkPathScenario(t, 4)
	limit := s.fixture.ls.peerHeaderHistoryLimit()
	// The walk accepts limit+1 headers; the next one is past what can be
	// verified.
	for i := range s.headers {
		_, path, found, err := s.deliver(i)
		if i <= limit {
			require.NoError(t, err, "header %d", i)
			require.True(t, found, "header %d", i)
			require.Len(t, path, i+1)
			continue
		}
		require.ErrorIs(t, err, errHeaderVerificationDeferred, "header %d", i)
		require.False(t, found, "header %d", i)
		require.Zero(
			t,
			s.reads(),
			"header %d beyond the history limit must not re-walk its ancestry",
			i,
		)
	}
}

// The primary chain can adopt the cached path and the deferred headers after
// it while the ledger tip has not reached any of them, so the cached path is
// never re-anchored. The next header then has a held parent and must be
// verified against it rather than deferred.
func TestPeerHeaderForkPathSaturatedCacheResumesWhenChainCatchesUp(
	t *testing.T,
) {
	t.Parallel()

	s := newSaturatedForkPathScenario(t, 4)
	limit := s.fixture.ls.peerHeaderHistoryLimit()
	for i := 0; i <= limit+2; i++ {
		_, _, _, err := s.deliver(i)
		if i > limit {
			require.ErrorIs(t, err, errHeaderVerificationDeferred)
		}
	}

	s.adopt(t, s.headers[:limit+3])
	parent := s.headers[limit+2]
	require.Equal(t, parent.blockNumber, s.fixture.ls.chain.Tip().BlockNumber)

	ancestor, path, found, err := s.deliver(limit + 3)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, parent.Hash().Bytes(), ancestor.Hash)
	require.Len(t, path, 1)
}

// The walk reads the limit+1 ancestors closest to a header. A chain tip one
// block below the lowest of them still defers without the walk; a tip at it
// lets the walk reach a held ancestor.
func TestPeerHeaderForkPathSaturatedCacheWalkBoundary(t *testing.T) {
	t.Parallel()

	s := newSaturatedForkPathScenario(t, 5)
	limit := s.fixture.ls.peerHeaderHistoryLimit()
	for i := 0; i <= limit+2; i++ {
		_, _, _, err := s.deliver(i)
		if i > limit {
			require.ErrorIs(t, err, errHeaderVerificationDeferred)
		}
	}
	// Header limit+3 reads ancestors limit+2 down to 2; the tip sits one
	// block below them.
	s.adopt(t, s.headers[:2])
	_, _, found, err := s.deliver(limit + 3)
	require.ErrorIs(t, err, errHeaderVerificationDeferred)
	require.False(t, found)
	require.Zero(t, s.reads())

	// Header limit+4 extends that deferred tail and reads ancestors limit+3
	// down to 3; the tip sits at the lowest of them.
	s.adopt(t, s.headers[2:4])
	ancestor, path, found, err := s.deliver(limit + 4)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, s.headers[3].Hash().Bytes(), ancestor.Hash)
	require.Len(t, path, limit+1)
}
