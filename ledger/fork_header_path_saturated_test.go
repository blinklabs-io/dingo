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
	"fmt"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
)

// A peer far ahead of the local tip keeps delivering headers after its cached
// path reaches the history limit. Such a header cannot be verified until the
// local tip advances, so each delivery must not repeat the full ancestry walk
// (one block lookup and one peer record lookup per step).
func TestPeerHeaderForkPathSaturatedCacheSkipsAncestryWalk(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	blockReads := 0
	fixture.ls.lookupBlockByHash = func(hash []byte) (models.Block, error) {
		blockReads++
		if bytes.Equal(hash, fixture.currentTip.Point.Hash) {
			return models.Block{
				Hash:   fixture.currentTip.Point.Hash,
				Slot:   fixture.currentTip.Point.Slot,
				Number: fixture.currentTip.BlockNumber,
				Type:   uint(gledger.BlockTypeBabbage),
			}, nil
		}
		return models.Block{}, models.ErrBlockNotFound
	}
	limit := fixture.ls.peerHeaderHistoryLimit()
	// The walk accepts limit+1 headers; the next one is past what can be
	// verified.
	headers := make([]mockHeader, limit+4)
	byHash := make(map[string]mockHeader, len(headers))
	prevHash := fixture.currentTip.Point.Hash
	for i := range headers {
		headers[i] = mockHeader{
			hash: lcommon.NewBlake2b256(
				testHashBytes(fmt.Sprintf("saturated-%d", i)),
			),
			prevHash:    lcommon.NewBlake2b256(prevHash),
			blockNumber: fixture.currentTip.BlockNumber + uint64(i) + 1,
			slot:        fixture.currentTip.Point.Slot + uint64(i) + 1,
		}
		prevHash = headers[i].Hash().Bytes()
	}
	peerReads := 0
	fixture.ls.config.PeerHeaderLookupFunc = func(
		_ ouroboros.ConnectionId,
		hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		peerReads++
		header, ok := byHash[string(hash)]
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

	for i, header := range headers {
		byHash[string(header.Hash().Bytes())] = header
		blockReads, peerReads = 0, 0
		_, path, found, err := fixture.ls.peerHeaderForkPath(
			fixture.connId,
			header,
		)
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
			blockReads+peerReads,
			"header %d beyond the history limit must not re-walk its ancestry",
			i,
		)
	}
}
