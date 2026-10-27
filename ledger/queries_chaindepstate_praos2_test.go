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
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// praos2TestIssuerVkey is the issuer key every Praos2 test block carries; the
// announcement reports its blake2b-224 hash.
var praos2TestIssuerVkey = bytes.Repeat([]byte{0x07}, 32)

// praos2TestBlock encodes a Dijkstra ranking block at slot whose header
// announces the endorser block ebHash of ebSize bytes, or announces nothing
// when ebHash is nil. Only the header matters to the query, so the body is
// the in-process default.
func praos2TestBlock(
	t *testing.T,
	number, slot uint64,
	prevHash []byte,
	ebHash []byte,
	ebSize uint64,
) models.Block {
	t.Helper()
	announcement := cbor.RawMessage{0xf6}
	if ebHash != nil {
		announcement = leiosTestRaw(t, []any{ebHash, ebSize})
	}
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: number,
					Slot:        slot,
					IssuerVkey:  lcommon.IssuerVkey(praos2TestIssuerVkey),
					VrfKey:      bytes.Repeat([]byte{0x08}, 32),
					VrfResult: lcommon.VrfResult{
						Output: bytes.Repeat([]byte{0x02}, 64),
						Proof:  bytes.Repeat([]byte{0x03}, 80),
					},
					OpCert: babbage.BabbageOpCert{
						HotVkey:   bytes.Repeat([]byte{0x09}, 32),
						Signature: bytes.Repeat([]byte{0x01}, 64),
					},
				},
				Signature: bytes.Repeat([]byte{0x04}, 448),
			},
			LeiosHeaderExtension: []cbor.RawMessage{
				leiosTestRaw(t, false),
				announcement,
			},
		},
	}
	encoded, err := cbor.Encode(block)
	require.NoError(t, err)
	return models.Block{
		Slot:     slot,
		Hash:     bytes.Repeat([]byte{byte(number)}, 32),
		PrevHash: prevHash,
		Cbor:     encoded,
		Number:   number,
		Type:     dijkstra.BlockTypeDijkstra,
	}
}

// praos2TestLedger stores the blocks, one epoch row per epochStarts entry (each
// 1000 slots long, in era), and a tip at the last block. praos2 sets the
// ledger's Praos2ChainDepState gate.
func praos2TestLedger(
	t *testing.T,
	praos2 bool,
	eraId uint,
	blocks []models.Block,
	epochStarts []uint64,
) *LedgerState {
	t.Helper()
	db := newTestDB(t)
	require.NoError(t, db.Transaction(t.Context(), true).
		Do(func(txn *database.Txn) error {
			for _, block := range blocks {
				if err := db.BlockCreate(block, txn); err != nil {
					return err
				}
				// The nonce fold reads each block's recorded nonce.
				if err := db.SetBlockNonce(
					block.Hash,
					block.Slot,
					bytes.Repeat([]byte{0x35}, 32),
					false,
					txn,
				); err != nil {
					return err
				}
			}
			return nil
		}))
	for idx, start := range epochStarts {
		require.NoError(t, db.Metadata().SetEpoch(
			start, uint64(idx), nil, nil, nil, nil,
			eraId, 1, 1000, nil,
		))
	}
	last := blocks[len(blocks)-1]
	require.NoError(t, db.SetTip(
		ochainsync.Tip{Point: ocommon.NewPoint(last.Slot, last.Hash)},
		nil,
	))
	ls := newChainDepStateLedger(t, db)
	ls.config.Praos2ChainDepState = praos2
	return ls
}

// praos2QueryBytes runs GetChainDepState and returns the state exactly as it
// goes on the wire inside the result wrapper.
func praos2QueryBytes(
	t *testing.T,
	ls *LedgerState,
	at QueryPoint,
) []byte {
	t.Helper()
	result, err := ls.Query(t.Context(), chainDepStateQuery(), at)
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	encoded, err := cbor.Encode(arr[0])
	require.NoError(t, err)
	return encoded
}

func decodeChainDepState(
	t *testing.T,
	encoded []byte,
) olocalstatequery.DebugChainDepStateResult {
	t.Helper()
	var decoded olocalstatequery.DebugChainDepStateResult
	require.NoError(t, decoded.UnmarshalCBOR(encoded))
	return decoded
}

// TestQueryShelleyDebugChainDepState_Praos2WithAnnouncement answers the Leios
// prototype's Dijkstra era with the Praos2 layout, version 1 and nine fields,
// carrying the announcement of the tip block's header.
func TestQueryShelleyDebugChainDepState_Praos2WithAnnouncement(t *testing.T) {
	t.Parallel()
	ebHash := bytes.Repeat([]byte{0xEE}, 32)
	ls := praos2TestLedger(t, true, eras.DijkstraEraDesc.Id,
		[]models.Block{
			praos2TestBlock(t, 1, 100, bytes.Repeat([]byte{0x77}, 32), ebHash, 4096),
		},
		[]uint64{0},
	)

	encoded := praos2QueryBytes(t, ls, QueryPoint{})
	// version 1, a nine-field state.
	require.Equal(t, "820189", hex.EncodeToString(encoded[:3]))
	// The announcement is a one-element list of [issuer, ebHash, ebSize].
	issuer := lcommon.Blake2b224Hash(praos2TestIssuerVkey)
	wantTail := "8183581c" + hex.EncodeToString(issuer[:]) +
		"5820" + hex.EncodeToString(ebHash) + "191000"
	require.Equal(t, wantTail, hex.EncodeToString(encoded[len(encoded)-len(wantTail)/2:]))

	decoded := decodeChainDepState(t, encoded)
	assert.Equal(t, olocalstatequery.ChainDepStateProtocolPraos2, decoded.Protocol)
	require.NotNil(t, decoded.Announcement)
	assert.Equal(t, issuer, decoded.Announcement.Issuer)
	assert.Equal(t, ebHash, decoded.Announcement.EbHash[:])
	assert.Equal(t, uint32(4096), decoded.Announcement.EbSize)
	assert.Equal(t, uint64(100), decoded.LastSlot.Slot)
}

// TestQueryShelleyDebugChainDepState_Praos2WithoutAnnouncement writes the
// empty list, never a CBOR null, for a header that announced nothing.
func TestQueryShelleyDebugChainDepState_Praos2WithoutAnnouncement(t *testing.T) {
	t.Parallel()
	ls := praos2TestLedger(t, true, eras.DijkstraEraDesc.Id,
		[]models.Block{
			praos2TestBlock(t, 1, 100, bytes.Repeat([]byte{0x77}, 32), nil, 0),
		},
		[]uint64{0},
	)

	encoded := praos2QueryBytes(t, ls, QueryPoint{})
	require.Equal(t, "820189", hex.EncodeToString(encoded[:3]))
	require.Equal(t, byte(0x80), encoded[len(encoded)-1], "absent is an empty list")
	decoded := decodeChainDepState(t, encoded)
	assert.Equal(t, olocalstatequery.ChainDepStateProtocolPraos2, decoded.Protocol)
	assert.Nil(t, decoded.Announcement)
}

// TestQueryShelleyDebugChainDepState_Praos2AnnouncementIsLastHeaders follows
// the most recently applied header: an announcement in an earlier epoch is
// not carried into a later epoch whose last header announced nothing, and a
// point pinned at the earlier block still reports it.
func TestQueryShelleyDebugChainDepState_Praos2AnnouncementIsLastHeaders(
	t *testing.T,
) {
	t.Parallel()
	ebHash := bytes.Repeat([]byte{0xEE}, 32)
	first := praos2TestBlock(t, 1, 900, bytes.Repeat([]byte{0x77}, 32), ebHash, 7)
	second := praos2TestBlock(t, 2, 1100, first.Hash, nil, 0)
	ls := praos2TestLedger(t, true, eras.DijkstraEraDesc.Id,
		[]models.Block{first, second}, []uint64{0, 1000})

	tip := decodeChainDepState(t, praos2QueryBytes(t, ls, QueryPoint{}))
	assert.Equal(t, uint64(1100), tip.LastSlot.Slot)
	assert.Nil(t, tip.Announcement,
		"the tip header, in the next epoch, announced nothing")

	pinned := decodeChainDepState(t, praos2QueryBytes(
		t, ls, QueryPoint{Slot: first.Slot, Hash: first.Hash},
	))
	assert.Equal(t, uint64(900), pinned.LastSlot.Slot)
	require.NotNil(t, pinned.Announcement,
		"the earlier block's own announcement, read back at its point")
	assert.Equal(t, uint32(7), pinned.Announcement.EbSize)
}

// TestQueryShelleyDebugChainDepState_Praos2NeedsPrototypeGate keeps every
// other configuration on the layout released consensus uses: the Dijkstra era
// with the gate off stays Praos, and with the gate on the eras before Dijkstra
// keep their own layouts.
func TestQueryShelleyDebugChainDepState_Praos2NeedsPrototypeGate(t *testing.T) {
	t.Parallel()
	block := func() models.Block {
		return praos2TestBlock(t, 1, 100, bytes.Repeat([]byte{0x77}, 32),
			bytes.Repeat([]byte{0xEE}, 32), 4096)
	}
	for _, tc := range []struct {
		name         string
		praos2       bool
		era          uint
		wantPrefix   string
		wantProtocol olocalstatequery.ChainDepStateProtocol
	}{
		{"dijkstra gate off", false, eras.DijkstraEraDesc.Id, "820088",
			olocalstatequery.ChainDepStateProtocolPraos},
		{"conway gate on", true, eras.ConwayEraDesc.Id, "820088",
			olocalstatequery.ChainDepStateProtocolPraos},
		{"babbage gate on", true, eras.BabbageEraDesc.Id, "820088",
			olocalstatequery.ChainDepStateProtocolPraos},
		{"alonzo gate on", true, eras.AlonzoEraDesc.Id, "820182",
			olocalstatequery.ChainDepStateProtocolTPraos},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ls := praos2TestLedger(t, tc.praos2, tc.era,
				[]models.Block{block()}, []uint64{0})
			encoded := praos2QueryBytes(t, ls, QueryPoint{})
			require.Equal(t, tc.wantPrefix, hex.EncodeToString(encoded[:3]))
			decoded := decodeChainDepState(t, encoded)
			assert.Equal(t, tc.wantProtocol, decoded.Protocol)
			assert.Nil(t, decoded.Announcement)
		})
	}
}

// praos2ConsensusGolden is the Praos2 chain-dependent state of the last era in
// ouroboros-consensus' disk golden golden/cardano/disk/ChainDepState_Dijkstra
// (revision 840e789b), without its announcement: version 1, nine fields, the
// ninth the empty list. It is independent expected bytes for the encoder.
const praos2ConsensusGolden = "820189820101a2581ca646474b8f5431261506b6c273d307c7569a4eb6c96b42dd4a29520a02581ce0a714319812c3f773ba04ec5d6b3ffcd5aad85006805b047b082541018201582012dd0a6a7d0e222a97926da03adb5a7768d31cc7c5c2bd6828e14a7d25fa3a608201582012dd0a6a7d0e222a97926da03adb5a7768d31cc7c5c2bd6828e14a7d25fa3a60810081008201582012dd0a6a7d0e222a97926da03adb5a7768d31cc7c5c2bd6828e14a7d25fa3a608201582012dd0a6a7d0e222a97926da03adb5a7768d31cc7c5c2bd6828e14a7d25fa3a6080"

// TestPraos2ChainDepStateMatchesConsensusGolden encodes the fields the golden
// holds and requires the golden's bytes back. The golden's six nonces take only
// two distinct values, so it pins arity, version, nesting and the empty-list
// announcement, not the order of the nonces, which the query tests cover
// through the stored epoch rows.
func TestPraos2ChainDepStateMatchesConsensusGolden(t *testing.T) {
	t.Parallel()
	golden, err := hex.DecodeString(praos2ConsensusGolden)
	require.NoError(t, err)
	decoded := decodeChainDepState(t, golden)
	require.Equal(t, olocalstatequery.ChainDepStateProtocolPraos2, decoded.Protocol)
	require.Nil(t, decoded.Announcement)

	state := newPraos2ChainDepState(praosChainDepState{
		LastSlot:            decoded.LastSlot,
		OpCertCounters:      decoded.OpCertCounters,
		EvolvingNonce:       decoded.EvolvingNonce,
		CandidateNonce:      decoded.CandidateNonce,
		EpochNonce:          *decoded.EpochNonce,
		PreviousEpochNonce:  *decoded.PreviousEpochNonce,
		LabNonce:            *decoded.LabNonce,
		LastEpochBlockNonce: *decoded.LastEpochBlockNonce,
	}, nil)
	got, err := cbor.Encode(versionedChainDepState{
		Version: chainDepStateVersionTPraos,
		Inner:   state,
	})
	require.NoError(t, err)
	require.Equal(t, praos2ConsensusGolden, hex.EncodeToString(got))
}
