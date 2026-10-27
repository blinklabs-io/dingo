package ouroboros

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
)

// TestChainsyncClientAcceptedTipSurvivesStaleLedgerRejection checks that a late ledger
// rejection of an OLDER header leaves the connection's accepted header tip
// alone while the tip names a NEWER accepted header.
func TestChainsyncClientAcceptedTipSurvivesStaleLedgerRejection(t *testing.T) {
	t.Parallel()

	var localTip, firstHash, secondHash gledger.Blake2b256
	localTip[0], firstHash[0], secondHash[0] = 0x01, 0x02, 0x03
	f := newDeferredHeaderFixture(t, localTip)

	first := nonByronTestHeader{&testBlockHeader{
		hash:        firstHash,
		prevHash:    localTip,
		blockNumber: 10,
		slotNumber:  86400,
	}}
	second := nonByronTestHeader{&testBlockHeader{
		hash:        secondHash,
		prevHash:    firstHash,
		blockNumber: 11,
		slotNumber:  86420,
	}}

	f.rollForward(t, first)
	evt := testutil.RequireReceive(
		t,
		f.ledgerCh,
		5*time.Second,
		"the first header must reach the ledger queue",
	)
	firstEvent, ok := evt.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.NotNil(t, firstEvent.PeerTipAdmission)

	// The pipelined child extends the local chain through the accepted tip.
	f.rollForward(t, second)
	_ = testutil.RequireReceive(
		t,
		f.ledgerCh,
		5*time.Second,
		"the second header must reach the ledger queue",
	)

	f.o.acceptedHeaderMu.Lock()
	got, recorded := f.o.acceptedHeaderTips[f.conn]
	f.o.acceptedHeaderMu.Unlock()
	require.True(t, recorded)
	require.Equal(t, secondHash.Bytes(), got)

	// The ledger rejects the older header only now: its admission callback runs
	// asynchronously, after the newer header was already accepted.
	firstEvent.PeerTipAdmission(false)

	f.o.acceptedHeaderMu.Lock()
	got, recorded = f.o.acceptedHeaderTips[f.conn]
	f.o.acceptedHeaderMu.Unlock()
	require.True(
		t,
		recorded,
		"rejecting an older header must not clear the accepted tip of a newer accepted header",
	)
	require.Equal(t, secondHash.Bytes(), got)
}
