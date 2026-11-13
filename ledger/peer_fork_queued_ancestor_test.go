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

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

// queuedAncestryScenario applies two blocks and offers the next three as a
// peer's unapplied path, as a catch-up peer delivers them. The two older
// headers carry current verdicts and optionally sit on the ledger's header
// queue. Verifying the middle header caches the candidate path, after which
// the connection's retained record of the oldest header is dropped while its
// descendants remain.
func queuedAncestryScenario(
	t *testing.T,
	tamper tamperOption,
	queuedAncestors bool,
) verifiedAncestryFixture {
	t.Helper()
	f := newVerifiedAncestryFixtureAt(t, tamper, 2, nil, 2)
	for _, block := range f.blocks[2:4] {
		if queuedAncestors {
			require.NoError(
				t,
				f.ls.chain.AddBlockHeader(t.Context(), block.Header()),
			)
		}
		f.stamps[string(block.Hash().Bytes())] =
			f.ls.CurrentHeaderCryptoVerificationGeneration()
	}
	require.NoError(
		t,
		f.ls.ValidatePeerChainSelectionHeaderCrypto(
			ouroboros.ConnectionId{},
			f.blocks[3].Header(),
		),
	)
	delete(f.raw, string(f.blocks[2].Hash().Bytes()))
	return f
}

// TestPeerForkAncestorLostFromHistoryResolvesThroughQueue covers a candidate
// whose oldest unapplied ancestor is no longer retained for the connection
// but is still held by the ledger's header queue: the header must verify
// instead of being deferred and then withheld.
func TestPeerForkAncestorLostFromHistoryResolvesThroughQueue(t *testing.T) {
	t.Parallel()
	f := queuedAncestryScenario(t, tamperNone, true)
	err := f.ls.ValidatePeerChainSelectionHeaderCrypto(
		ouroboros.ConnectionId{},
		f.blocks[4].Header(),
	)
	require.NoError(t, err)
}

// TestPeerForkQueuedAncestorLostFromHistoryStaysAuthenticated proves the
// queue stands in only for the retained record: the ancestor's crypto is
// still verified, so a bad signature is rejected rather than inherited.
func TestPeerForkQueuedAncestorLostFromHistoryStaysAuthenticated(
	t *testing.T,
) {
	t.Parallel()
	f := queuedAncestryScenario(t, tamperKESSig, true)
	err := f.ls.ValidatePeerChainSelectionHeaderCrypto(
		ouroboros.ConnectionId{},
		f.blocks[4].Header(),
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "signature")
	require.NotErrorIs(t, err, ErrHeaderAncestryUnavailable)
}

// TestPeerForkUnqueuedAncestorLostFromHistoryIsUnavailable keeps the
// deferral for an ancestor that neither the connection nor the queue holds.
func TestPeerForkUnqueuedAncestorLostFromHistoryIsUnavailable(t *testing.T) {
	t.Parallel()
	f := queuedAncestryScenario(t, tamperNone, false)
	err := f.ls.ValidatePeerChainSelectionHeaderCrypto(
		ouroboros.ConnectionId{},
		f.blocks[4].Header(),
	)
	require.ErrorIs(t, err, ErrHeaderAncestryUnavailable)
	require.True(t, IsHeaderVerificationDeferred(err))
}
