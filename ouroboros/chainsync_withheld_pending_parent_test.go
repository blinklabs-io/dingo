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

package ouroboros

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

func pendingParentHeader(
	hash, prev byte,
	block uint64,
) nonByronTestHeader {
	var h, p gledger.Blake2b256
	h[0], p[0] = hash, prev
	return nonByronTestHeader{&testBlockHeader{
		hash:        h,
		prevHash:    p,
		blockNumber: block,
		slotNumber:  block * 20,
	}}
}

// The ledger queues an accepted header asynchronously, so the verifier can
// run on a child while its parent, already accepted on this connection, is
// not yet queued. The verifier then reports the ancestry unavailable. Such a
// child extends this connection's accepted chain: it must reach the ledger
// queue rather than be withheld, and must not re-intersect the connection,
// which would purge the queued suffix.
func TestChainsyncClientChildOfPendingAcceptedHeaderIsNotWithheld(
	t *testing.T,
) {
	t.Parallel()

	var localTip gledger.Blake2b256
	localTip[0] = 0x01
	f := newDeferredHeaderFixture(t, localTip)
	first := pendingParentHeader(0x02, 0x01, 10)
	child := pendingParentHeader(0x03, 0x02, 11)
	grandchild := pendingParentHeader(0x04, 0x03, 12)
	orphan := pendingParentHeader(0x05, 0x7f, 13)

	firstVerify := f.o.chainSelectionVerifyHeaderCrypto
	f.o.chainSelectionVerifyHeaderCrypto = func(
		conn ouroboros.ConnectionId,
		header gledger.BlockHeader,
	) (uint64, error) {
		if header.Hash() == first.Hash() {
			return firstVerify(conn, header)
		}
		return 0, ledger.ErrHeaderVerificationWithheld
	}

	for _, header := range []gledger.BlockHeader{first, child, grandchild} {
		f.rollForward(t, header)
		evt := testutil.RequireReceive(
			t,
			f.ledgerCh,
			5*time.Second,
			"a header extending the connection's accepted chain must reach the ledger queue",
		)
		data, ok := evt.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, header.Hash().Bytes(), data.Point.Hash)
	}
	f.o.futureHeaderResyncMu.Lock()
	_, scheduled := f.o.futureHeaderResyncs[f.conn]
	f.o.futureHeaderResyncMu.Unlock()
	require.False(t, scheduled,
		"a header whose parent is pending ledger enqueue must not re-intersect the connection")

	// A header that does not extend any accepted header stays withheld.
	f.rollForward(t, orphan)
	testutil.RequireNoReceive(
		t,
		f.ledgerCh,
		100*time.Millisecond,
		"a header with unavailable ancestry must stay withheld",
	)
}
