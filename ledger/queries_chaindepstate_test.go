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

	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chainDepStateQuery wraps the leaf query the way the wire delivers it, so the
// test exercises the same dispatch path a client reaches.
func chainDepStateQuery() *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyDebugChainDepStateQuery{},
		},
	}
}

// TestQueryShelleyDebugChainDepState_Dispatches is the regression for #2997.
//
// The query was absent from the dispatch table, so it fell through to the
// "unsupported query type" default. That error aborts the LocalStateQuery
// protocol rather than failing one query, so the node drops the connection and
// cardano-cli reports only a closed bearer — which is what `query
// leadership-schedule` hits, since it reads the epoch nonce from this state.
func TestQueryShelleyDebugChainDepState_Dispatches(t *testing.T) {
	db := newTestDB(t)
	ls := &LedgerState{db: db}
	ls.publishSnapshotsLocked()

	result, err := ls.Query(chainDepStateQuery())
	require.NoError(t, err,
		"the query must be handled rather than aborting the protocol")
	require.NotNil(t, result)
}

// TestQueryShelleyDebugChainDepState_DecodesAsPraosState checks the reply
// against the decoder a client actually uses, rather than against our own
// idea of the shape.
//
// cardano-cli decodes this with the node's Haskell decoder, which requires
// `encodeVersion 0` wrapping an 8-element Praos record: last slot, opcert
// counters, then the evolving, candidate, epoch, previous-epoch, lab and
// last-epoch-block nonces in that order. Getting the arity or order wrong
// still produces valid CBOR, so the shape is pinned by decoding it.
func TestQueryShelleyDebugChainDepState_DecodesAsPraosState(t *testing.T) {
	// The ledger state under test reports epoch 0, so the record has to sit
	// there for the query to find it.
	const epochID = 0
	epochNonce := make([]byte, 32)
	for i := range epochNonce {
		epochNonce[i] = 0x11
	}
	evolvingNonce := make([]byte, 32)
	for i := range evolvingNonce {
		evolvingNonce[i] = 0x22
	}
	candidateNonce := make([]byte, 32)
	for i := range candidateNonce {
		candidateNonce[i] = 0x33
	}

	db := newTestDB(t)
	require.NoError(t, db.Metadata().SetEpoch(
		0,              // slot
		epochID,        // epoch
		epochNonce,     // nonce
		evolvingNonce,  // evolvingNonce
		candidateNonce, // candidateNonce
		nil,            // lastEpochBlockNonce
		0,              // era
		0,              // slotLength
		0,              // lengthInSlots
		nil,            // txn
	))

	ls := &LedgerState{db: db}
	ls.publishSnapshotsLocked()

	result, err := ls.Query(chainDepStateQuery())
	require.NoError(t, err)

	// Results travel wrapped in the single-element MsgResult array.
	arr, ok := result.([]any)
	require.True(t, ok, "expected the []any result wrapper")
	require.Len(t, arr, 1)

	encoded, err := cbor.Encode(arr[0])
	require.NoError(t, err)

	var decoded olocalstatequery.DebugChainDepStateResult
	require.NoError(t, decoded.UnmarshalCBOR(encoded),
		"reply must decode with the client-side ChainDepState decoder")

	assert.Equal(t, olocalstatequery.ChainDepStateProtocolPraos,
		decoded.Protocol, "Conway-era nodes serialise the Praos layout")
	require.NotNil(t, decoded.EpochNonce,
		"epoch nonce is what leadership-schedule reads")
	assert.Equal(t, lcommon.Nonce{
		Type:  lcommon.NonceTypeNonce,
		Value: [32]byte(epochNonce),
	}, *decoded.EpochNonce)
	assert.Equal(t, lcommon.Nonce{
		Type:  lcommon.NonceTypeNonce,
		Value: [32]byte(evolvingNonce),
	}, decoded.EvolvingNonce)
	assert.Equal(t, lcommon.Nonce{
		Type:  lcommon.NonceTypeNonce,
		Value: [32]byte(candidateNonce),
	}, decoded.CandidateNonce)
	assert.NotNil(t, decoded.OpCertCounters,
		"counters must be a map even when no pool has minted")
}

// TestQueryShelleyDebugChainDepState_ReportsOpCertCounters covers the other
// half of the reply: the operational-certificate counters the chain has
// accepted, keyed by each pool's cold-key hash.
func TestQueryShelleyDebugChainDepState_ReportsOpCertCounters(t *testing.T) {
	db := newTestDB(t)
	poolKeyHash := make([]byte, 28)
	for i := range poolKeyHash {
		poolKeyHash[i] = 0xAB
	}
	// Active-pool lookup is relative to the tip, so a fresh database with no
	// tip reports no pools at all.
	require.NoError(t, db.SetTip(
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("tip"))},
		nil,
	))
	// The lookup resolves the tip slot to an epoch, so that epoch has to span
	// it.
	require.NoError(t, db.Metadata().SetEpoch(
		0,    // slot
		0,    // epoch
		nil,  // nonce
		nil,  // evolvingNonce
		nil,  // candidateNonce
		nil,  // lastEpochBlockNonce
		0,    // era
		1,    // slotLength
		1000, // lengthInSlots
		nil,
	))

	pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolKeyHash))
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  make([]byte, 32),
		},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  make([]byte, 32),
			AddedSlot:   1,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 5, 1, nil))

	ls := &LedgerState{db: db}
	ls.publishSnapshotsLocked()

	result, err := ls.Query(chainDepStateQuery())
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)

	encoded, err := cbor.Encode(arr[0])
	require.NoError(t, err)
	var decoded olocalstatequery.DebugChainDepStateResult
	require.NoError(t, decoded.UnmarshalCBOR(encoded))

	counter, found := decoded.OpCertCounter(
		lcommon.NewBlake2b224(poolKeyHash),
	)
	assert.True(t, found, "a pool that minted must have a counter")
	assert.Equal(t, uint64(5), counter)
}
