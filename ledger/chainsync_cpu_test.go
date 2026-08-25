package ledger

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRecoverPeerHeaderHistoryPathWorkIsLinear guards the rollback recovery
// hot path. Every retained suffix head used to rescan the same ancestry while
// chainsyncMutex was held, producing quadratic block-hash lookups when the
// requested rollback point was not present in that peer's history.
func TestRecoverPeerHeaderHistoryPathWorkIsLinear(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.config.GenesisSelectionStateFunc = func() (bool, uint64) {
		return true, ^uint64(0)
	}
	lookupCalls := 0
	fixture.ls.lookupBlockByHash = func([]byte) (models.Block, error) {
		lookupCalls++
		return models.Block{}, models.ErrBlockNotFound
	}
	const headerCount = 2000
	prevHash := testHashBytes("unresolved-root")
	for i := range headerCount {
		hash := testHashBytes(fmt.Sprintf("cpu-probe-%d", i))
		header := mockHeader{
			hash:        lcommon.NewBlake2b256(hash),
			prevHash:    lcommon.NewBlake2b256(prevHash),
			blockNumber: fixture.currentTip.BlockNumber + uint64(i) + 1,
			slot:        fixture.currentTip.Point.Slot + uint64(i) + 1,
		}
		fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        ocommon.NewPoint(header.slot, hash),
			BlockHeader:  header,
		})
		prevHash = hash
	}

	fixture.ls.chainsyncMutex.Lock()
	_, err := fixture.ls.recoverPeerHeaderHistoryFromPointLocked(
		fixture.connId,
		fixture.ancestorTip.Point,
	)
	fixture.ls.chainsyncMutex.Unlock()

	require.NoError(t, err)
	assert.Equal(t, headerCount, lookupCalls)
}

// TestRecoverPeerHeaderHistoryPathWorkHonorsDepthLimit preserves the existing
// safety bound when the external peer-history lookup returns an endless,
// non-cyclic chain. Memoization must not turn a bounded recovery walk into an
// unbounded one.
func TestRecoverPeerHeaderHistoryPathWorkHonorsDepthLimit(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	lookupCalls := 0
	fixture.ls.lookupBlockByHash = func([]byte) (models.Block, error) {
		lookupCalls++
		return models.Block{}, models.ErrBlockNotFound
	}
	peerLookupCalls := 0
	limit := fixture.ls.peerHeaderHistoryLimit()
	fixture.ls.config.PeerHeaderLookupFunc = func(
		_ ouroboros.ConnectionId,
		hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		peerLookupCalls++
		if peerLookupCalls > 2*limit {
			return ChainsyncEvent{}, nil, false
		}
		nextHash := testHashBytes(fmt.Sprintf("depth-next-%d", peerLookupCalls))
		header := mockHeader{
			hash:        lcommon.NewBlake2b256(hash),
			prevHash:    lcommon.NewBlake2b256(nextHash),
			blockNumber: uint64(peerLookupCalls),
			slot:        uint64(peerLookupCalls),
		}
		return ChainsyncEvent{
			Point:       ocommon.NewPoint(header.slot, hash),
			BlockHeader: header,
		}, nextHash, true
	}

	ancestor, path, err := fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		testHashBytes("depth-limit-head"),
		fixture.ancestorTip.Point,
		nil,
		make(map[string]peerHeaderHistoryPathCacheEntry),
	)

	require.NoError(t, err)
	assert.Nil(t, ancestor)
	assert.Nil(t, path)
	assert.Equal(t, limit, lookupCalls)
	assert.Equal(t, limit, peerLookupCalls)
}

func TestFindPeerForkPathCachedTreatsMalformedRetainedRecordAsMissing(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.lookupBlockByHash = func([]byte) (models.Block, error) {
		return models.Block{}, models.ErrBlockNotFound
	}
	malformedHash := testHashBytes("malformed-retained-header")
	history := &peerHeaderChain{
		byHash: map[string]peerHeaderRecord{
			fmt.Sprintf("%x", malformedHash): {
				event: ChainsyncEvent{
					ConnectionId: fixture.connId,
					Point:        ocommon.NewPoint(30, malformedHash),
					Type:         1,
				},
				headerCbor: []byte{0xff},
				prevHash:   fixture.ancestorTip.Point.Hash,
				decodeType: 1,
			},
		},
	}
	peerLookupCalls := 0
	fixture.ls.config.PeerHeaderLookupFunc = func(
		_ ouroboros.ConnectionId,
		_ []byte,
	) (ChainsyncEvent, []byte, bool) {
		peerLookupCalls++
		return ChainsyncEvent{BlockHeader: mockHeader{}},
			fixture.ancestorTip.Point.Hash,
			true
	}

	ancestor, path, err := fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		malformedHash,
		fixture.ancestorTip.Point,
		history,
		make(map[string]peerHeaderHistoryPathCacheEntry),
	)

	require.NoError(t, err)
	assert.Nil(t, ancestor)
	assert.Nil(t, path)
	assert.Zero(t, peerLookupCalls)
}

func TestFindPeerForkPathCachedPreservesShorterSuffixAfterDepthLimit(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	limit := fixture.ls.peerHeaderHistoryLimit()
	lookupCalls := 0
	fixture.ls.lookupBlockByHash = func(hash []byte) (models.Block, error) {
		lookupCalls++
		if bytes.Equal(hash, fixture.ancestorTip.Point.Hash) {
			return models.Block{
				Hash: fixture.ancestorTip.Point.Hash,
				Slot: fixture.ancestorTip.Point.Slot,
			}, nil
		}
		return models.Block{}, models.ErrBlockNotFound
	}
	links := make(map[string][]byte, limit)
	hashes := make([][]byte, limit)
	for i := range limit {
		hashes[i] = testHashBytes(fmt.Sprintf("bounded-suffix-%d", i))
	}
	for i, hash := range hashes {
		nextHash := fixture.ancestorTip.Point.Hash
		if i+1 < len(hashes) {
			nextHash = hashes[i+1]
		}
		links[fmt.Sprintf("%x", hash)] = nextHash
	}
	peerLookupCalls := 0
	fixture.ls.config.PeerHeaderLookupFunc = peerHistoryLookupForTest(
		fixture.connId,
		links,
		&peerLookupCalls,
	)
	cache := make(map[string]peerHeaderHistoryPathCacheEntry)

	ancestor, path, err := fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		hashes[0],
		fixture.ancestorTip.Point,
		nil,
		cache,
	)
	require.NoError(t, err)
	assert.Nil(t, ancestor)
	assert.Nil(t, path)
	assert.Equal(t, limit, lookupCalls)
	assert.Equal(t, limit, peerLookupCalls)

	ancestor, path, err = fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		hashes[1],
		fixture.ancestorTip.Point,
		nil,
		cache,
	)
	require.NoError(t, err)
	require.NotNil(t, ancestor)
	assert.True(t, pointMatches(*ancestor, fixture.ancestorTip.Point))
	assert.Len(t, path, limit-1)
}

func TestFindPeerForkPathCachedChargesAndPropagatesCachedSuffix(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	limit := fixture.ls.peerHeaderHistoryLimit()
	lookupCalls := 0
	fixture.ls.lookupBlockByHash = func(hash []byte) (models.Block, error) {
		lookupCalls++
		if bytes.Equal(hash, fixture.ancestorTip.Point.Hash) {
			return models.Block{
				Hash: fixture.ancestorTip.Point.Hash,
				Slot: fixture.ancestorTip.Point.Slot,
			}, nil
		}
		return models.Block{}, models.ErrBlockNotFound
	}
	const suffixLength = 128
	prefixLength := limit - suffixLength
	links := make(map[string][]byte, limit)
	suffix := make([][]byte, suffixLength)
	for i := range suffixLength {
		suffix[i] = testHashBytes(fmt.Sprintf("cached-suffix-%d", i))
		nextHash := fixture.ancestorTip.Point.Hash
		if i > 0 {
			links[fmt.Sprintf("%x", suffix[i-1])] = suffix[i]
		}
		links[fmt.Sprintf("%x", suffix[i])] = nextHash
	}
	prefix := make([][]byte, prefixLength)
	for i := range prefixLength {
		prefix[i] = testHashBytes(fmt.Sprintf("cached-prefix-%d", i))
		if i > 0 {
			links[fmt.Sprintf("%x", prefix[i-1])] = prefix[i]
		}
		links[fmt.Sprintf("%x", prefix[i])] = suffix[0]
	}
	peerLookupCalls := 0
	fixture.ls.config.PeerHeaderLookupFunc = peerHistoryLookupForTest(
		fixture.connId,
		links,
		&peerLookupCalls,
	)
	cache := make(map[string]peerHeaderHistoryPathCacheEntry)

	ancestor, _, err := fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		suffix[0],
		fixture.ancestorTip.Point,
		nil,
		cache,
	)
	require.NoError(t, err)
	require.NotNil(t, ancestor)

	ancestor, path, err := fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		prefix[0],
		fixture.ancestorTip.Point,
		nil,
		cache,
	)
	require.NoError(t, err)
	assert.Nil(t, ancestor)
	assert.Nil(t, path)

	lookupsBeforeShorterSuffix := lookupCalls
	peerLookupsBeforeShorterSuffix := peerLookupCalls
	ancestor, path, err = fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		prefix[1],
		fixture.ancestorTip.Point,
		nil,
		cache,
	)
	require.NoError(t, err)
	require.NotNil(t, ancestor)
	assert.True(t, pointMatches(*ancestor, fixture.ancestorTip.Point))
	assert.Len(t, path, limit-1)
	assert.Equal(t, lookupsBeforeShorterSuffix, lookupCalls)
	assert.Equal(t, peerLookupsBeforeShorterSuffix, peerLookupCalls)
}

func TestFindPeerForkPathCachedPropagatesMismatchedAncestor(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	lookupCalls := 0
	fixture.ls.lookupBlockByHash = func(hash []byte) (models.Block, error) {
		lookupCalls++
		if bytes.Equal(hash, fixture.ancestorTip.Point.Hash) {
			return models.Block{
				Hash: fixture.ancestorTip.Point.Hash,
				Slot: fixture.ancestorTip.Point.Slot,
			}, nil
		}
		return models.Block{}, models.ErrBlockNotFound
	}
	suffixHead := testHashBytes("mismatch-suffix-head")
	suffixTail := testHashBytes("mismatch-suffix-tail")
	prefixHead := testHashBytes("mismatch-prefix-head")
	prefixTail := testHashBytes("mismatch-prefix-tail")
	links := map[string][]byte{
		fmt.Sprintf("%x", suffixHead): suffixTail,
		fmt.Sprintf("%x", suffixTail): fixture.ancestorTip.Point.Hash,
		fmt.Sprintf("%x", prefixHead): prefixTail,
		fmt.Sprintf("%x", prefixTail): suffixHead,
	}
	peerLookupCalls := 0
	fixture.ls.config.PeerHeaderLookupFunc = peerHistoryLookupForTest(
		fixture.connId,
		links,
		&peerLookupCalls,
	)
	cache := make(map[string]peerHeaderHistoryPathCacheEntry)

	ancestor, _, err := fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		suffixHead,
		fixture.ancestorTip.Point,
		nil,
		cache,
	)
	require.NoError(t, err)
	require.NotNil(t, ancestor)

	expectedAncestor := ocommon.NewPoint(999, testHashBytes("other-ancestor"))
	ancestor, path, err := fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		prefixHead,
		expectedAncestor,
		nil,
		cache,
	)
	require.NoError(t, err)
	require.NotNil(t, ancestor)
	assert.True(t, pointMatches(*ancestor, fixture.ancestorTip.Point))
	assert.Nil(t, path)

	lookupsBeforeCachedPrefix := lookupCalls
	peerLookupsBeforeCachedPrefix := peerLookupCalls
	ancestor, path, err = fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		prefixTail,
		expectedAncestor,
		nil,
		cache,
	)
	require.NoError(t, err)
	require.NotNil(t, ancestor)
	assert.True(t, pointMatches(*ancestor, fixture.ancestorTip.Point))
	assert.Nil(t, path)
	assert.Equal(t, lookupsBeforeCachedPrefix, lookupCalls)
	assert.Equal(t, peerLookupsBeforeCachedPrefix, peerLookupCalls)
}

func peerHistoryLookupForTest(
	connId ouroboros.ConnectionId,
	links map[string][]byte,
	lookupCalls *int,
) PeerHeaderLookupFunc {
	return func(
		lookupConnId ouroboros.ConnectionId,
		hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		if lookupConnId != connId {
			return ChainsyncEvent{}, nil, false
		}
		nextHash, ok := links[fmt.Sprintf("%x", hash)]
		if !ok {
			return ChainsyncEvent{}, nil, false
		}
		*lookupCalls++
		header := mockHeader{
			hash:        lcommon.NewBlake2b256(hash),
			prevHash:    lcommon.NewBlake2b256(nextHash),
			blockNumber: uint64(*lookupCalls),
			slot:        uint64(*lookupCalls),
		}
		return ChainsyncEvent{
			ConnectionId: lookupConnId,
			Point:        ocommon.NewPoint(header.slot, hash),
			BlockHeader:  header,
		}, append([]byte(nil), nextHash...), true
	}
}

func TestRecoverPeerHeaderHistoryIncompleteLookupReintersects(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	missingHash := testHashBytes("incomplete-lookup")
	headerHash := testHashBytes("incomplete-lookup-head")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(headerHash),
		prevHash:    lcommon.NewBlake2b256(missingHash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 1,
	}
	fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        ocommon.NewPoint(header.slot, headerHash),
		BlockHeader:  header,
	})
	fixture.ls.config.PeerHeaderLookupFunc = func(
		_ ouroboros.ConnectionId,
		_ []byte,
	) (ChainsyncEvent, []byte, bool) {
		return ChainsyncEvent{}, fixture.ancestorTip.Point.Hash, true
	}

	fixture.ls.chainsyncMutex.Lock()
	headerCount, err := fixture.ls.recoverPeerHeaderHistoryFromPointLocked(
		fixture.connId,
		fixture.ancestorTip.Point,
	)
	fixture.ls.chainsyncMutex.Unlock()

	require.NoError(t, err)
	assert.Zero(t, headerCount)
}
