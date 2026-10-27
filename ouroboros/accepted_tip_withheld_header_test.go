package ouroboros

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
)

// TestChainsyncClientWithheldHeaderIsNotAcceptedTip checks that a header the
// ledger never sees (here: withheld because the peer is not apply eligible)
// does not become the connection's accepted header tip, so a later deferred
// header cannot count as a linear extension of one that never reached the
// ledger queue.
func TestChainsyncClientWithheldHeaderIsNotAcceptedTip(t *testing.T) {
	t.Parallel()

	var localTip, firstHash gledger.Blake2b256
	localTip[0], firstHash[0] = 0x01, 0x02
	f := newDeferredHeaderFixture(t, localTip)
	// The peer is observed but its blocks are withheld from the ledger.
	f.o.config.ChainsyncApplyEligible = func(ouroboros.ConnectionId) bool {
		return false
	}

	first := nonByronTestHeader{&testBlockHeader{
		hash:        firstHash,
		prevHash:    localTip,
		blockNumber: 10,
		slotNumber:  86400,
	}}
	f.rollForward(t, first)
	testutil.RequireNoReceive(
		t,
		f.ledgerCh,
		100*time.Millisecond,
		"a header withheld from the ledger must not reach the ledger queue",
	)

	f.o.acceptedHeaderMu.Lock()
	_, recorded := f.o.acceptedHeaderTips[f.conn]
	f.o.acceptedHeaderMu.Unlock()
	require.False(
		t,
		recorded,
		"a header that never reached the ledger queue must not become the accepted tip",
	)
}
