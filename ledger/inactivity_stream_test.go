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
	"testing"
	"time"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// enableDelegatorInactivity turns the CIP-0163 gate on for a harness that has
// not processed anything yet.
func enableDelegatorInactivity(h *externalIngestHarness, window uint64) {
	h.ls.config.DelegatorInactivityEnabled = true
	h.ls.config.DelegatorInactivity = window
}

// TestStreamModeDerivesAccountExpiration checks that a Mithril stream batch
// applied through ApplyTrustedBatch renews CIP-0163 account expirations
// exactly as the live block path does, so a node that caught up by stream
// can enable the inactivity gate without a resync.
func TestStreamModeDerivesAccountExpiration(t *testing.T) {
	t.Parallel()

	const window = 20
	// The scenario's delegate transaction delegates the stake credential
	// 0x22 * 28, which its certificates make a witness.
	stakeKey := rowBatchingKey(0x22)
	blocks, _ := externalIngestBlocks(t, 2)
	last := blockPoint(blocks[len(blocks)-1])

	live := newExternalIngestHarness(t, false)
	enableDelegatorInactivity(live, window)
	live.startLiveLoop(t)
	require.NoError(t, live.cm.PrimaryChain().AddBlocks(t.Context(), blocks))
	require.Eventually(t, func() bool {
		return pointMatches(live.ls.Tip().Point, last)
	}, 5*time.Second, 10*time.Millisecond)

	stream := newExternalIngestHarness(t, false)
	enableDelegatorInactivity(stream, window)
	tip, err := stream.applyPaused(t, ocommon.Point{}, blocks)
	require.NoError(t, err)
	require.Equal(t, last, tip)

	liveAcct, err := live.db.GetAccountByCredential(
		t.Context(), 0, stakeKey, true, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(window), liveAcct.ExpirationEpoch,
		"the live path must renew the witnessed credential in epoch 0")

	streamAcct, err := stream.db.GetAccountByCredential(
		t.Context(), 0, stakeKey, true, nil,
	)
	require.NoError(t, err)
	require.Equal(t, liveAcct.ExpirationEpoch, streamAcct.ExpirationEpoch,
		"stream apply must derive the same CIP-0163 expiration as live apply")
	// Whole-store digests differ by design: the stream path drops outputs a
	// batch both creates and spends. The account row is what the gate reads.
	require.Equal(t, liveAcct.Active, streamAcct.Active)
	require.Equal(t, liveAcct.Pool, streamAcct.Pool)
	require.Equal(t, liveAcct.Drep, streamAcct.Drep)
}
