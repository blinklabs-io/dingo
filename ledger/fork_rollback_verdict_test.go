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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedBehindPeerShelleyEpoch gives the fixture a persisted Shelley epoch and
// protocol parameters, so that a rollback reloads them from the database as it
// does on a running node instead of taking the genesis (empty database)
// branch, which zeroes the whole consensus context.
func seedBehindPeerShelleyEpoch(t *testing.T, f *behindPeerFixture) {
	t.Helper()
	nonce := bytes.Repeat([]byte{0x31}, lcommon.Blake2b256Size)
	pparams := &shelley.ShelleyProtocolParameters{
		ProtocolMajor:      2,
		MaxBlockBodySize:   65_536,
		MaxBlockHeaderSize: 1_100,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)
	require.NoError(t, f.ls.db.SetEpoch(
		0, 0, nonce, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1_000, 432_000, nil,
	))
	require.NoError(t, f.ls.db.SetPParams(
		pparamsCbor, 0, 0, eras.ShelleyEraDesc.Id, nil,
	))
	epochs, err := f.ls.db.GetEpochs(nil)
	require.NoError(t, err)
	require.Len(t, epochs, 1)
	f.ls.Lock()
	f.ls.epochCache = epochs
	f.ls.currentEpoch = epochs[0]
	f.ls.currentEra = eras.ShelleyEraDesc
	f.ls.currentPParams = pparams
	f.ls.publishSnapshotsLocked()
	f.ls.Unlock()
}

// TestPeerForkVerdictsSurviveLocalRollbackInSameEpoch reproduces, without
// docker, the step that keeps a dingo node from following the winning fork
// after a slot battle.
//
// Dingo applied two blocks (19 and 20) that lose against a peer fork of three
// blocks (19', 20', 21') from the same ancestor (18): a depth 2 reorg inside
// one epoch. The peer's headers arrive and are verified (stamped with the
// header-verification generation) before the ledger rolls its own blocks
// back, which is the order of the chainsync pipeline. Rolling back does not
// change the epoch nonce, era, stake or protocol parameter values, so the
// verdicts stamped before it describe exactly the context that follows it,
// and the next header of the winning fork must be verifiable.
//
// While the bug is present the rollback publishes a new consensus context
// (it reloads the protocol parameters from the database, and parameters are
// compared by pointer identity), every earlier verdict is discarded, and the
// children of the already-verified fork headers are deferred with "candidate
// ancestor/parent has no current crypto verdict". The deferred headers are
// never queued, so the node adopts one block of the winning fork per
// reconnect instead of the whole fork.
func TestPeerForkVerdictsSurviveLocalRollbackInSameEpoch(t *testing.T) {
	t.Parallel()

	f := newBehindPeerFixture(t)
	seedBehindPeerShelleyEpoch(t, f)
	ctx := context.Background()

	// Peer fork: three headers on top of block 18 (depth 2 from our tip).
	ancestor := f.tipAtDepth(2)
	var forkHeaders []mockHeader
	prev := lcommon.NewBlake2b256(ancestor.Point.Hash)
	for i := range 3 {
		hash := lcommon.NewBlake2b256(
			testHashBytes(fmt.Sprintf("winning-fork-%d", i)),
		)
		forkHeaders = append(forkHeaders, mockHeader{
			hash:        hash,
			prevHash:    prev,
			blockNumber: ancestor.BlockNumber + uint64(i) + 1,
			slot:        ancestor.Point.Slot + uint64(i+1)*10 + 1,
		})
		prev = hash
	}

	// The ouroboros layer verifies each header as it arrives and keeps the
	// verdict with the header in the peer's observed history.
	generationAtArrival := f.ls.CurrentHeaderCryptoVerificationGeneration()
	require.NotZero(t, generationAtArrival)
	stamps := make(map[string]uint64)
	for _, header := range forkHeaders[:2] {
		stamps[string(header.hash[:])] = generationAtArrival
	}
	f.ls.config.PeerHeaderLookupFunc = func(
		connID ouroboros.ConnectionId,
		hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		for _, header := range forkHeaders {
			stamp, ok := stamps[string(header.hash[:])]
			if !ok || !bytes.Equal(header.hash[:], hash) {
				continue
			}
			return ChainsyncEvent{
				ConnectionId:                       connID,
				Point:                              ocommon.NewPoint(header.slot, header.hash[:]),
				BlockHeader:                        header,
				HeaderCryptoVerificationGeneration: stamp,
			}, header.prevHash[:], true
		}
		return ChainsyncEvent{}, nil, false
	}

	before := f.ls.loadConsensusSnapshot()
	require.NotNil(t, before)

	// Depth 2 local rollback: our blocks 19 and 20 are abandoned.
	require.NoError(
		t,
		f.ls.rollbackChainAndStateDeferred(ctx, ancestor.Point, nil),
	)
	require.Equal(t, ancestor.Point, f.ls.chain.Tip().Point)

	generation, contextGeneration := f.ls.currentHeaderVerificationContext()
	require.NotZero(t, generation)
	after := f.ls.loadConsensusSnapshot()
	require.NotNil(t, after)
	assert.Truef(
		t,
		headerVerdictInContext(
			generationAtArrival,
			contextGeneration,
			generation,
		),
		"a header verified before a same-epoch rollback must keep a current "+
			"verdict after it: stamp=%d context=%d current=%d; consensus "+
			"inputs unchanged by the rollback: era=%t pparams(by value)=%t "+
			"prevEraPParams=%t transition=%t",
		generationAtArrival, contextGeneration, generation,
		before.currentEra.Id == after.currentEra.Id,
		sameProtocolParameters(before.currentPParams, after.currentPParams),
		sameProtocolParameters(before.prevEraPParams, after.prevEraPParams),
		before.transitionInfo == after.transitionInfo,
	)

	// The child of the verified fork headers must not be deferred for lack of
	// a verdict. The fixture's stored blocks are synthetic, so the walk may
	// still fail later on decoding the ancestor envelope; only the deferral
	// is the subject here.
	_, _, found, err := f.ls.peerHeaderForkPath(
		ouroboros.ConnectionId{}, forkHeaders[2],
	)
	// found==false with a nil error means the walk never ran, which would
	// make the deferral check below vacuous.
	if !found {
		require.Error(
			t, err,
			"the winning fork's next header was neither resolved nor failed",
		)
	}
	require.NotErrorIs(
		t, err, errHeaderVerificationDeferred,
		"the winning fork's next header was deferred after the rollback",
	)
}
