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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
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

// newChainDepStateLedger builds the ledger state these tests query.
//
// Recomputing the evolving and candidate nonces logs through config.Logger and
// takes the randomness-stability window from the Shelley genesis. A real node
// gets both from NewLedgerState, which defaults a nil logger; these tests
// construct LedgerState directly and so have to supply them.
func newChainDepStateLedger(
	t *testing.T,
	db *database.Database,
) *LedgerState {
	t.Helper()
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			CardanoNodeConfig: newConwayBootstrapStabilityCfg(t),
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()
	return ls
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
	ls := newChainDepStateLedger(t, db)

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
	// Left nil, this decoded to the neutral nonce, and so did the
	// previous-epoch and lab fields. Three fields sharing one indistinguishable
	// value cannot pin an ordering: swapping any two of them would still
	// satisfy the assertions. Giving it a distinct value separates the tail of
	// the record from the head.
	lastEpochBlockNonce := make([]byte, 32)
	for i := range lastEpochBlockNonce {
		lastEpochBlockNonce[i] = 0x44
	}

	db := newTestDB(t)
	// The epoch has to span the slot the reply reports, since the nonces are
	// read from whichever epoch contains it.
	require.NoError(t, db.Metadata().SetEpoch(
		0,                     // slot
		epochID,               // epoch
		epochNonce,            // nonce
		evolvingNonce,         // evolvingNonce
		candidateNonce,        // candidateNonce
		lastEpochBlockNonce,   // lastEpochBlockNonce
		eras.ConwayEraDesc.Id, // era
		1,                     // slotLength
		1000,                  // lengthInSlots
		nil,                   // txn
	))

	ls := newChainDepStateLedger(t, db)

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
	// The last two fields of the record. Both are served from the ledger's
	// last-epoch-block nonce, since dingo keeps no separate value for the last
	// applied block, so they cannot be told apart from each other -- but a
	// swap with any earlier field now shows up.
	require.NotNil(t, decoded.LastEpochBlockNonce)
	require.NotNil(t, decoded.LabNonce)
	assert.Equal(t, lcommon.Nonce{
		Type:  lcommon.NonceTypeNonce,
		Value: [32]byte(lastEpochBlockNonce),
	}, *decoded.LastEpochBlockNonce)
	assert.Equal(t, lcommon.Nonce{
		Type:  lcommon.NonceTypeNonce,
		Value: [32]byte(lastEpochBlockNonce),
	}, *decoded.LabNonce)
	assert.NotNil(t, decoded.OpCertCounters,
		"counters must be a map even when no pool has minted")
}

// TestQueryShelleyDebugChainDepState_TPraosEraUsesTPraosLayout covers a tip in
// a TPraos era.
//
// The two protocols do not merely tag the reply differently, they serialise
// different records: TPraos writes version 1 wrapping [lastSlot, [counters,
// evolving, candidate]], where Praos writes version 0 wrapping a flat
// eight-field array. Emitting the Praos form in a TPraos era hands the client
// a payload whose nesting and arity do not match what the version promises.
//
// dingo supports Shelley through Alonzo, and a node syncing from genesis sits
// in them for a long stretch, serving queries the whole time.
func TestQueryShelleyDebugChainDepState_TPraosEraUsesTPraosLayout(t *testing.T) {
	db := newTestDB(t)

	// Shelley's window is 3k/f = 3*6/0.4 = 45 slots, so with the epoch running
	// [1000, 2000) the candidate freezes at slot 1955.
	const (
		epochStart    uint64 = 1000
		epochLength   uint64 = 1000
		preCutoffSlot uint64 = 1100
		tipSlot       uint64 = 1960
	)

	checkpointEvolving := bytes.Repeat([]byte{0x81}, 32)
	checkpointCandidate := bytes.Repeat([]byte{0x82}, 32)
	preCutoffNonce := bytes.Repeat([]byte{0x83}, 32)
	tipNonce := bytes.Repeat([]byte{0x84}, 32)
	preCutoffHash := bytes.Repeat([]byte{0x85}, 32)
	tipHash := bytes.Repeat([]byte{0x86}, 32)

	poolKeyHash := bytes.Repeat([]byte{0x87}, 28)
	pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolKeyHash))

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot:     preCutoffSlot,
			Hash:     preCutoffHash,
			PrevHash: bytes.Repeat([]byte{0x88}, 32),
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     tipSlot,
			Hash:     tipHash,
			PrevHash: preCutoffHash,
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			preCutoffHash, preCutoffSlot, preCutoffNonce, false, txn,
		); err != nil {
			return err
		}
		return db.SetBlockNonce(tipHash, tipSlot, tipNonce, false, txn)
	}))

	require.NoError(t, db.Metadata().SetEpoch(
		epochStart,                  // slot
		1,                           // epoch
		bytes.Repeat([]byte{4}, 32), // nonce
		checkpointEvolving,          // evolvingNonce
		checkpointCandidate,         // candidateNonce
		bytes.Repeat([]byte{5}, 32), // lastEpochBlockNonce
		eras.ShelleyEraDesc.Id,      // era
		1,                           // slotLength
		uint(epochLength),           // lengthInSlots
		nil,                         // txn
	))
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 3, preCutoffSlot, nil))
	require.NoError(t, db.SetTip(
		ochainsync.Tip{Point: ocommon.NewPoint(tipSlot, tipHash)},
		nil,
	))

	ls := newChainDepStateLedger(t, db)

	result, err := ls.Query(chainDepStateQuery())
	require.NoError(t, err)
	arr, _ := result.([]any)
	require.Len(t, arr, 1)
	encoded, err := cbor.Encode(arr[0])
	require.NoError(t, err)
	var decoded olocalstatequery.DebugChainDepStateResult
	require.NoError(t, decoded.UnmarshalCBOR(encoded),
		"the reply must decode with the client-side decoder")

	assert.Equal(t, olocalstatequery.ChainDepStateProtocolTPraos,
		decoded.Protocol,
		"a Shelley-era tip serialises the TPraos layout")
	// The Praos-only fields have no place in the TPraos record, and the
	// decoder leaves them nil precisely so a caller can tell.
	assert.Nil(t, decoded.EpochNonce,
		"TPraos carries no epoch nonce")
	assert.Nil(t, decoded.PreviousEpochNonce)
	assert.Nil(t, decoded.LabNonce)
	assert.Nil(t, decoded.LastEpochBlockNonce)

	// The fields TPraos does carry still have to be right, and still have to
	// be the tip's rather than the epoch's opening checkpoint.
	require.True(t, decoded.LastSlot.HasSlot)
	assert.Equal(t, tipSlot, decoded.LastSlot.Slot)
	assert.Equal(t, lcommon.Nonce{
		Type:  lcommon.NonceTypeNonce,
		Value: [32]byte(tipNonce),
	}, decoded.EvolvingNonce)
	assert.Equal(t, lcommon.Nonce{
		Type:  lcommon.NonceTypeNonce,
		Value: [32]byte(preCutoffNonce),
	}, decoded.CandidateNonce)
	counter, found := decoded.OpCertCounter(lcommon.NewBlake2b224(poolKeyHash))
	assert.True(t, found, "counters survive into the TPraos record")
	assert.Equal(t, uint64(3), counter)
}

// TestQueryShelleyDebugChainDepState_NoncesTrackTipNotEpochCheckpoint covers
// the evolving and candidate nonces mid-epoch.
//
// The epoch row's copies are checkpoints: they hold what the two nonces were
// when the epoch opened, and nothing rewrites them as blocks land. Both move
// with every block -- the evolving nonce folds in each block's VRF output, and
// the candidate tracks it until the randomness-stabilisation cutoff, then
// freezes. Serving the row's values therefore describes the chain as it stood
// at the boundary, not at the tip the reply reports.
//
// The fixture separates all three: the epoch opens with one pair of values, a
// block before the cutoff moves them to a second, and a block after the cutoff
// moves the evolving nonce alone to a third. A reply built from the checkpoint
// shows neither of the later two, and one that ignores the cutoff reports the
// tip's nonce as the candidate.
func TestQueryShelleyDebugChainDepState_NoncesTrackTipNotEpochCheckpoint(
	t *testing.T,
) {
	db := newTestDB(t)

	// Conway's window is 4k/f = 4*6/0.4 = 60 slots, so with the epoch running
	// [1000, 2000) the candidate freezes at slot 1940.
	const (
		epochStart    uint64 = 1000
		epochLength   uint64 = 1000
		preCutoffSlot uint64 = 1100
		tipSlot       uint64 = 1950
	)

	checkpointEvolving := bytes.Repeat([]byte{0x71}, 32)
	checkpointCandidate := bytes.Repeat([]byte{0x72}, 32)
	preCutoffNonce := bytes.Repeat([]byte{0x73}, 32)
	tipNonce := bytes.Repeat([]byte{0x74}, 32)

	preCutoffHash := bytes.Repeat([]byte{0x75}, 32)
	tipHash := bytes.Repeat([]byte{0x76}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot:     preCutoffSlot,
			Hash:     preCutoffHash,
			PrevHash: bytes.Repeat([]byte{0x77}, 32),
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     tipSlot,
			Hash:     tipHash,
			PrevHash: preCutoffHash,
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		// The evolving nonce as of each block, which is what the ledger
		// records per block and what consensus folds forward.
		if err := db.SetBlockNonce(
			preCutoffHash, preCutoffSlot, preCutoffNonce, false, txn,
		); err != nil {
			return err
		}
		return db.SetBlockNonce(
			tipHash, tipSlot, tipNonce, false, txn,
		)
	}))

	require.NoError(t, db.Metadata().SetEpoch(
		epochStart,                  // slot
		1,                           // epoch
		bytes.Repeat([]byte{2}, 32), // nonce
		checkpointEvolving,          // evolvingNonce
		checkpointCandidate,         // candidateNonce
		bytes.Repeat([]byte{3}, 32), // lastEpochBlockNonce
		eras.ConwayEraDesc.Id,       // era
		1,                           // slotLength
		uint(epochLength),           // lengthInSlots
		nil,                         // txn
	))
	require.NoError(t, db.SetTip(
		ochainsync.Tip{Point: ocommon.NewPoint(tipSlot, tipHash)},
		nil,
	))

	ls := newChainDepStateLedger(t, db)

	result, err := ls.Query(chainDepStateQuery())
	require.NoError(t, err)
	arr, _ := result.([]any)
	require.Len(t, arr, 1)
	encoded, err := cbor.Encode(arr[0])
	require.NoError(t, err)
	var decoded olocalstatequery.DebugChainDepStateResult
	require.NoError(t, decoded.UnmarshalCBOR(encoded))

	assert.Equal(t, lcommon.Nonce{
		Type:  lcommon.NonceTypeNonce,
		Value: [32]byte(tipNonce),
	}, decoded.EvolvingNonce,
		"the evolving nonce is the tip's, not the value the epoch opened with")
	assert.Equal(t, lcommon.Nonce{
		Type:  lcommon.NonceTypeNonce,
		Value: [32]byte(preCutoffNonce),
	}, decoded.CandidateNonce,
		"the candidate froze at the cutoff, so it is the last pre-cutoff "+
			"block's nonce -- neither the checkpoint nor the tip's")
}

// TestQueryShelleyDebugChainDepState_LabNonceTracksTipParent covers the lab
// nonce, which is the only field of the record that moves with every block
// rather than only at an epoch boundary.
//
// In Praos the lab is prevHashToNonce(block.prevHash) for the last block
// applied -- the PARENT hash of the tip, a deliberate one-block lag (see
// epochLabNonce and #2734). The last-epoch-block nonce is the value that lag
// had reached when the epoch opened. The two are equal only until the first
// block of the epoch lands; after that, reporting the carried value in the lab
// field is a stale answer for a field the chain has already moved on from.
func TestQueryShelleyDebugChainDepState_LabNonceTracksTipParent(t *testing.T) {
	db := newTestDB(t)

	carriedLab := bytes.Repeat([]byte{0x31}, 32)
	grandparentHash := bytes.Repeat([]byte{0x32}, 32)
	parentHash := bytes.Repeat([]byte{0x33}, 32)
	tipHash := bytes.Repeat([]byte{0x34}, 32)

	// Two blocks inside the epoch, so the tip's parent is a block of this
	// epoch rather than the carried value by coincidence.
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot:     1100,
			Hash:     parentHash,
			PrevHash: grandparentHash,
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     1200,
			Hash:     tipHash,
			PrevHash: parentHash,
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		// A synced node records each block's evolving nonce as it applies it.
		// Without the rows the nonce fold falls back to re-decoding block
		// CBOR, which these placeholder bodies cannot satisfy.
		if err := db.SetBlockNonce(
			parentHash, 1100, bytes.Repeat([]byte{0x35}, 32), false, txn,
		); err != nil {
			return err
		}
		return db.SetBlockNonce(
			tipHash, 1200, bytes.Repeat([]byte{0x36}, 32), false, txn,
		)
	}))
	require.NoError(t, db.Metadata().SetEpoch(
		1000,       // slot
		1,          // epoch
		nil,        // nonce
		nil,        // evolvingNonce
		nil,        // candidateNonce
		carriedLab, // lastEpochBlockNonce
		0,          // era
		1,          // slotLength
		1000,       // lengthInSlots
		nil,        // txn
	))
	require.NoError(t, db.SetTip(
		ochainsync.Tip{Point: ocommon.NewPoint(1200, tipHash)},
		nil,
	))

	ls := newChainDepStateLedger(t, db)

	result, err := ls.Query(chainDepStateQuery())
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	encoded, err := cbor.Encode(arr[0])
	require.NoError(t, err)
	var decoded olocalstatequery.DebugChainDepStateResult
	require.NoError(t, decoded.UnmarshalCBOR(encoded))

	require.NotNil(t, decoded.LabNonce)
	assert.Equal(t, lcommon.Nonce{
		Type:  lcommon.NonceTypeNonce,
		Value: [32]byte(parentHash),
	}, *decoded.LabNonce,
		"the lab is the tip's parent hash, not the epoch's carried value")
	require.NotNil(t, decoded.LastEpochBlockNonce)
	assert.Equal(t, lcommon.Nonce{
		Type:  lcommon.NonceTypeNonce,
		Value: [32]byte(carriedLab),
	}, *decoded.LastEpochBlockNonce,
		"the carried value stays in its own field")
}

// TestQueryShelleyDebugChainDepState_LabNonceWithoutHashIndex covers a tip
// whose block is stored but has no entry in the block hash index.
//
// The index has only been written since #1915, and a lookup that misses it
// reports ErrBlockNotFound rather than scanning -- a deliberate trade, since
// the scan was a top CPU consumer during catch-up. Blocks predating the index
// need an offline backfill, so a database carrying them answers "no such
// block" for a block it holds. A node restarted on such a database has
// precisely one of them as its tip.
//
// Taking that answer at face value would quietly fall back to the epoch's
// carried value and report a stale lab. The tip's slot and hash together
// address the block directly, so the index is not needed to find it.
func TestQueryShelleyDebugChainDepState_LabNonceWithoutHashIndex(t *testing.T) {
	db := newTestDB(t)

	carriedLab := bytes.Repeat([]byte{0x51}, 32)
	parentHash := bytes.Repeat([]byte{0x52}, 32)
	tipHash := bytes.Repeat([]byte{0x53}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot:     1200,
			Hash:     tipHash,
			PrevHash: parentHash,
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		return db.SetBlockNonce(
			tipHash, 1200, bytes.Repeat([]byte{0x37}, 32), false, txn,
		)
	}))
	// Drop the hash-index entry, leaving the block blob in place: the state a
	// block written before the index existed is in.
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		return db.Blob().Delete(
			txn.Blob(), dbtypes.BlockHashIndexKey(tipHash),
		)
	}))
	// The block must still be unreachable by hash, or the test proves nothing.
	require.NoError(t, db.Transaction(false).Do(func(txn *database.Txn) error {
		_, err := database.BlockByHashTxn(txn, tipHash)
		require.ErrorIs(t, err, models.ErrBlockNotFound,
			"fixture must reproduce the index miss")
		return nil
	}))

	require.NoError(t, db.Metadata().SetEpoch(
		1000, 1, nil, nil, nil, carriedLab, 0, 1, 1000, nil,
	))
	require.NoError(t, db.SetTip(
		ochainsync.Tip{Point: ocommon.NewPoint(1200, tipHash)},
		nil,
	))

	ls := newChainDepStateLedger(t, db)

	result, err := ls.Query(chainDepStateQuery())
	require.NoError(t, err)
	arr, _ := result.([]any)
	require.Len(t, arr, 1)
	encoded, err := cbor.Encode(arr[0])
	require.NoError(t, err)
	var decoded olocalstatequery.DebugChainDepStateResult
	require.NoError(t, decoded.UnmarshalCBOR(encoded))

	require.NotNil(t, decoded.LabNonce)
	assert.Equal(t, lcommon.Nonce{
		Type:  lcommon.NonceTypeNonce,
		Value: [32]byte(parentHash),
	}, *decoded.LabNonce,
		"a block missing only from the hash index is still the tip we hold")
}

// TestQueryShelleyDebugChainDepState_LabNonceCarriesWithoutBlocks covers the
// other half of the lab: a chain with no block to take a parent hash from.
//
// Consensus only moves the lab when a block is applied, so before the first
// one it holds whatever the epoch opened with.
func TestQueryShelleyDebugChainDepState_LabNonceCarriesWithoutBlocks(t *testing.T) {
	db := newTestDB(t)
	carriedLab := bytes.Repeat([]byte{0x41}, 32)
	require.NoError(t, db.Metadata().SetEpoch(
		0, 0, nil, nil, nil, carriedLab, 0, 1, 1000, nil,
	))

	ls := newChainDepStateLedger(t, db)

	result, err := ls.Query(chainDepStateQuery())
	require.NoError(t, err)
	arr, _ := result.([]any)
	require.Len(t, arr, 1)
	encoded, err := cbor.Encode(arr[0])
	require.NoError(t, err)
	var decoded olocalstatequery.DebugChainDepStateResult
	require.NoError(t, decoded.UnmarshalCBOR(encoded))

	require.NotNil(t, decoded.LabNonce)
	assert.Equal(t, lcommon.Nonce{
		Type:  lcommon.NonceTypeNonce,
		Value: [32]byte(carriedLab),
	}, *decoded.LabNonce,
		"with no block applied the lab is the value the epoch opened with")
}

// TestQueryShelleyDebugChainDepState_LabNonceTipBlockUnavailable covers a tip
// naming a block the node does not hold the body of, which is where a
// Mithril-bootstrapped node starts.
//
// There is no parent hash to derive a lab from, and the query must not fail:
// an error here aborts the LocalStateQuery protocol and drops the connection,
// which is the failure this whole handler exists to remove. Carrying the
// epoch's value is wrong by at most one block.
func TestQueryShelleyDebugChainDepState_LabNonceTipBlockUnavailable(t *testing.T) {
	db := newTestDB(t)
	carriedLab := bytes.Repeat([]byte{0x61}, 32)
	absentTip := bytes.Repeat([]byte{0x62}, 32)

	require.NoError(t, db.Metadata().SetEpoch(
		1000, 1, nil, nil, nil, carriedLab, 0, 1, 1000, nil,
	))
	// A well-formed tip hash with no block stored under it.
	require.NoError(t, db.SetTip(
		ochainsync.Tip{Point: ocommon.NewPoint(1200, absentTip)},
		nil,
	))

	ls := newChainDepStateLedger(t, db)

	result, err := ls.Query(chainDepStateQuery())
	require.NoError(t, err,
		"an unreadable tip block must not abort the protocol")
	arr, _ := result.([]any)
	require.Len(t, arr, 1)
	encoded, err := cbor.Encode(arr[0])
	require.NoError(t, err)
	var decoded olocalstatequery.DebugChainDepStateResult
	require.NoError(t, decoded.UnmarshalCBOR(encoded))

	require.NotNil(t, decoded.LabNonce)
	assert.Equal(t, lcommon.Nonce{
		Type:  lcommon.NonceTypeNonce,
		Value: [32]byte(carriedLab),
	}, *decoded.LabNonce)
}

// TestQueryShelleyDebugChainDepState_ReportsPreviousEpochNonce covers the
// previous-epoch nonce, which the epoch-0 cases above cannot reach: epoch 0 has
// no predecessor, so that field stays neutral there and the branch that fills
// it never runs.
//
// It sits between the epoch and lab nonces in the record, so a value landing in
// the wrong slot is a reordering the earlier tests cannot see -- every field
// around it there decodes to a value that is either neutral or shared with its
// neighbour.
func TestQueryShelleyDebugChainDepState_ReportsPreviousEpochNonce(t *testing.T) {
	previousNonce := make([]byte, 32)
	for i := range previousNonce {
		previousNonce[i] = 0x55
	}
	currentNonce := make([]byte, 32)
	for i := range currentNonce {
		currentNonce[i] = 0x66
	}

	db := newTestDB(t)
	// Epoch 0's nonce is what the reply must carry as the previous-epoch
	// nonce once the chain has moved on to epoch 1.
	require.NoError(t, db.Metadata().SetEpoch(
		0,             // slot
		0,             // epoch
		previousNonce, // nonce
		nil,           // evolvingNonce
		nil,           // candidateNonce
		nil,           // lastEpochBlockNonce
		0,             // era
		1,             // slotLength
		1000,          // lengthInSlots
		nil,           // txn
	))
	require.NoError(t, db.Metadata().SetEpoch(
		1000,         // slot
		1,            // epoch
		currentNonce, // nonce
		nil,          // evolvingNonce
		nil,          // candidateNonce
		nil,          // lastEpochBlockNonce
		0,            // era
		1,            // slotLength
		1000,         // lengthInSlots
		nil,          // txn
	))
	require.NoError(t, db.SetTip(
		ochainsync.Tip{Point: ocommon.NewPoint(1500, []byte("tip"))},
		nil,
	))

	ls := newChainDepStateLedger(t, db)

	result, err := ls.Query(chainDepStateQuery())
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)

	encoded, err := cbor.Encode(arr[0])
	require.NoError(t, err)
	var decoded olocalstatequery.DebugChainDepStateResult
	require.NoError(t, decoded.UnmarshalCBOR(encoded))

	require.NotNil(t, decoded.EpochNonce)
	assert.Equal(t, lcommon.Nonce{
		Type:  lcommon.NonceTypeNonce,
		Value: [32]byte(currentNonce),
	}, *decoded.EpochNonce, "the epoch nonce is the current epoch's")
	require.NotNil(t, decoded.PreviousEpochNonce,
		"an epoch with a predecessor must carry its nonce")
	assert.Equal(t, lcommon.Nonce{
		Type:  lcommon.NonceTypeNonce,
		Value: [32]byte(previousNonce),
	}, *decoded.PreviousEpochNonce,
		"the previous-epoch nonce is the preceding epoch's, not the current one's")
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

	ls := newChainDepStateLedger(t, db)

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

// TestQueryShelleyDebugChainDepState_CountersOutliveRegistration covers a pool
// that minted blocks but is not in the active set.
//
// The counters are the chain's record of the highest operational-certificate
// issue number it has accepted, and that record does not expire when a pool
// leaves the active set: the chain still holds it, and the node still enforces
// it against any block claiming that cold key. Deriving the counters from the
// currently registered pools instead would drop a retired pool's entry, so a
// caller reading this reply would be told the chain has accepted nothing for a
// cold key it would in fact reject a replayed certificate for.
func TestQueryShelleyDebugChainDepState_CountersOutliveRegistration(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.SetTip(
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("tip"))},
		nil,
	))
	require.NoError(t, db.Metadata().SetEpoch(
		0, 0, nil, nil, nil, nil, 0, 1, 1000, nil,
	))

	// A cold key the chain has accepted a certificate for, with no pool row
	// backing it -- the state a pool reaches once its registration is gone.
	gone := make([]byte, 28)
	for i := range gone {
		gone[i] = 0xC0
	}
	gonePkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(gone))
	require.NoError(t, db.UpdatePoolOpCertSequence(gonePkh, 7, 1, nil))

	ls := newChainDepStateLedger(t, db)

	result, err := ls.Query(chainDepStateQuery())
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	encoded, err := cbor.Encode(arr[0])
	require.NoError(t, err)
	var decoded olocalstatequery.DebugChainDepStateResult
	require.NoError(t, decoded.UnmarshalCBOR(encoded))

	counter, found := decoded.OpCertCounter(lcommon.NewBlake2b224(gone))
	require.True(t, found,
		"the chain's accepted counter survives the pool leaving the active set")
	assert.Equal(t, uint64(7), counter)
}

// TestQueryShelleyDebugChainDepState_HighestCounterPerPool covers a pool that
// has minted under several operational certificates.
//
// The chain accepts a certificate only if its issue number is at least the
// highest already accepted, so the reply has to carry that highest number
// rather than whichever row happens to come back first.
func TestQueryShelleyDebugChainDepState_HighestCounterPerPool(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.SetTip(
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("tip"))},
		nil,
	))
	require.NoError(t, db.Metadata().SetEpoch(
		0, 0, nil, nil, nil, nil, 0, 1, 1000, nil,
	))

	poolKeyHash := make([]byte, 28)
	for i := range poolKeyHash {
		poolKeyHash[i] = 0x5A
	}
	pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolKeyHash))
	// Rotated certificates, recorded newest-slot-last so a query returning the
	// last row rather than the maximum would still pass; the middle rotation is
	// the highest, so ordering by slot cannot stand in for the maximum.
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 2, 10, nil))
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 9, 20, nil))
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 4, 30, nil))

	ls := newChainDepStateLedger(t, db)

	result, err := ls.Query(chainDepStateQuery())
	require.NoError(t, err)
	arr, _ := result.([]any)
	require.Len(t, arr, 1)
	encoded, err := cbor.Encode(arr[0])
	require.NoError(t, err)
	var decoded olocalstatequery.DebugChainDepStateResult
	require.NoError(t, decoded.UnmarshalCBOR(encoded))

	counter, found := decoded.OpCertCounter(
		lcommon.NewBlake2b224(poolKeyHash),
	)
	require.True(t, found)
	assert.Equal(t, uint64(9), counter,
		"the counter is the highest accepted, not the most recent")
}
