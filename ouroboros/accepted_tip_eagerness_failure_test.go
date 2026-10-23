package ouroboros

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// TestChainsyncClientEagernessFailureIsNotAcceptedTip checks that a header
// whose delivery aborts at the Limit on Eagerness wait never reaches the
// ledger queue and is not left as the connection's accepted header tip, so
// its pipelined deferred child does not enter the ledger queue.
func TestChainsyncClientEagernessFailureIsNotAcceptedTip(t *testing.T) {
	t.Parallel()

	var localTip, firstHash, childHash gledger.Blake2b256
	localTip[0], firstHash[0], childHash[0] = 0x01, 0x02, 0x03
	f := newDeferredHeaderFixture(t, localTip)

	first := nonByronTestHeader{&testBlockHeader{
		hash:        firstHash,
		prevHash:    localTip,
		blockNumber: 10,
		slotNumber:  86400,
	}}
	child := nonByronTestHeader{&testBlockHeader{
		hash:        childHash,
		prevHash:    firstHash,
		blockNumber: 11,
		slotNumber:  86420,
	}}

	// The eagerness wait fails: the header never reaches the ledger.
	f.o.config.ChainsyncAwaitEagerness = func(
		context.Context,
		ouroboros.ConnectionId,
		uint64,
		func() ochainsync.Tip,
	) error {
		return errors.New("eagerness wait failed")
	}
	err := f.o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: f.conn},
		0,
		first,
		ochainsync.Tip{
			Point:       ocommon.NewPoint(first.SlotNumber(), first.Hash().Bytes()),
			BlockNumber: first.BlockNumber(),
		},
	)
	require.Error(t, err)
	testutil.RequireNoReceive(
		t,
		f.ledgerCh,
		100*time.Millisecond,
		"the header must not reach the ledger queue",
	)

	f.o.acceptedHeaderMu.Lock()
	got, recorded := f.o.acceptedHeaderTips[f.conn]
	f.o.acceptedHeaderMu.Unlock()
	t.Logf("accepted tip recorded=%v hash=%x", recorded, got)
	require.False(
		t,
		recorded,
		"a header that never reached the ledger queue must not be the accepted tip",
	)

	// The pipelined child of that header must not enter the ledger queue.
	f.o.config.ChainsyncAwaitEagerness = nil
	require.NoError(t, f.o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: f.conn},
		0,
		child,
		ochainsync.Tip{
			Point:       ocommon.NewPoint(child.SlotNumber(), child.Hash().Bytes()),
			BlockNumber: child.BlockNumber(),
		},
	))
	testutil.RequireNoReceive(
		t,
		f.ledgerCh,
		100*time.Millisecond,
		"a deferred child of a header that never reached the ledger queue "+
			"must not enter the ledger queue",
	)
}
