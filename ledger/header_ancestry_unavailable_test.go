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
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestPeerHeaderAncestryUnavailableForDiscardedAncestor(t *testing.T) {
	t.Parallel()
	f := newForkPathFreshnessFixture(t, 2)
	f.stamp(0)
	snapshot := f.ls.loadConsensusSnapshot()
	headers := []gledger.BlockHeader{f.headers[0], f.headers[1]}
	verify := func() error {
		return f.ls.verifyPeerForkTargetEpochAncestors(
			context.Background(), ouroboros.ConnectionId{}, headers,
			f.ls.currentEpoch, f.ls.currentEpoch.Nonce, snapshot,
		)
	}
	require.NoError(t, verify())
	delete(f.stamps, string(f.headers[0].hash[:]))
	err := verify()
	require.ErrorIs(t, err, ErrHeaderAncestryUnavailable)
	require.True(t, IsHeaderVerificationDeferred(err))
	require.False(t, IsHeaderVerificationContextTransient(err))
	// Re-delivery restores the retained verdict without weakening verification.
	f.stamp(0)
	require.NoError(t, verify())
}

func TestPeerHeaderAncestryUnavailableForMissingOpCertPath(t *testing.T) {
	t.Parallel()
	f := newForkPathFreshnessFixture(t, 0)
	blockNumber := f.ls.currentTip.BlockNumber + 2
	target := createTestBlockWithNonceAtSlotsAndBlockNumber(
		t, [32]byte{94}, f.ls.currentEpoch.Nonce,
		f.ls.currentTip.Point.Slot+1, f.ls.currentTip.Point.Slot+200,
		bytes.Repeat([]byte{0xa1}, lcommon.Blake2b256Size),
		&blockNumber, tamperNone, 0,
	)
	err := f.ls.validatePeerForkOpCertCounters(
		context.Background(), ouroboros.ConnectionId{}, target.block.Header(),
	)
	require.ErrorIs(t, err, ErrHeaderAncestryUnavailable)
	require.True(t, IsHeaderVerificationDeferred(err))
	require.False(t, IsHeaderVerificationContextTransient(err))
}
