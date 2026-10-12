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
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (m announcingMockHeader) LeiosAnnouncement() (
	lcommon.Blake2b256,
	uint64,
	bool,
) {
	return m.ebHash, m.ebSize, true
}

// TestChainsyncHeaderQueueClearedInvalidatesAnnouncement covers the case where
// header admission succeeds but blockfetch startup then fails: the queue is
// discarded and no rollback is published, because no block was ever added.
// Without the invalidation on the same stream, the announcement would outlive
// the header and the vote manager could vote for a ranking block that is not
// on our chain.
//
// The announcing header is admitted verified (the only kind that announces);
// the header that drives the handler into its failing blockfetch start chains
// onto it and announces nothing of its own, so the single announcement under
// test is unambiguous.
func TestChainsyncHeaderQueueClearedInvalidatesAnnouncement(t *testing.T) {
	fixture := newHeaderStreamLedger(t)
	// No BlockfetchRequestRangeFunc is wired, so every blockfetch start
	// attempt fails and the handler exhausts its fallbacks.
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	announcing := announcingHeader(
		577, "hdr-1", lcommon.NewBlake2b256(nil), 1, ebHash,
	)
	require.NoError(t, fixture.ls.chain.AddVerifiedBlockHeader(context.Background(), announcing))

	follower := mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-2")),
		prevHash:    announcing.hash,
		blockNumber: 2,
		slot:        578,
	}
	point := ocommon.NewPoint(follower.slot, follower.hash.Bytes())

	require.NoError(
		t,
		fixture.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
			ConnectionId: fixture.connId,
			BlockHeader:  follower,
			Point:        point,
			// Tip equal to the header keeps the handler out of the
			// header-accumulation branches so it reaches blockfetch.
			Tip: ochainsync.Tip{Point: point, BlockNumber: 2},
		}),
	)
	assert.Zero(
		t,
		fixture.ls.chain.HeaderCount(),
		"failed blockfetch start discards the queued headers",
	)

	announcement := testutil.RequireReceive(
		t, fixture.ch, testutil.AsyncWait, "announcement",
	)
	announced, ok := announcement.Data.(chain.ChainHeaderAnnouncementEvent)
	require.True(t, ok, "got %T", announcement.Data)
	assert.Equal(t, announcing.hash, announced.RbHash)

	invalidation := testutil.RequireReceive(
		t, fixture.ch, testutil.AsyncWait, "invalidation for the discarded header",
	)
	invalid, ok := invalidation.Data.(chain.ChainHeaderInvalidationEvent)
	require.True(t, ok, "got %T", invalidation.Data)
	assert.Equal(t, chain.HeaderInvalidationQueueCleared, invalid.Reason)
	assert.Contains(t, invalid.RbHashes, announcing.hash)
	assert.Greater(t, invalid.Seq, announced.Seq)
}
