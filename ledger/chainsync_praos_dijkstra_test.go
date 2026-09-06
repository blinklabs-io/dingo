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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// setOpCertSequenceNumber writes an operational certificate counter into a
// gouroboros header's field whatever width that release declares it at.
// cardano-ledger decodes the counter as Word64, so the test's own values are
// uint64; the assignment is written this way rather than as a composite
// literal so it does not have to be edited when the header type's width
// changes.
func setOpCertSequenceNumber[T uint32 | uint64](dst *T, value uint64) {
	*dst = T(value) //nolint:gosec // test-only counters are small
}

// newTestDijkstraBlockCbor builds a minimal, decodable Dijkstra block (empty
// body, plain Babbage-shaped header) and returns its CBOR. The body hash is
// computed from the actual empty body so NewDijkstraBlockFromCbor's
// body-hash check (run by models.Block.Decode via ledger.NewBlockFromCbor)
// passes; header signature/KES verification is not exercised at decode time.
func newTestDijkstraBlockCbor(
	t *testing.T,
	slot, blockNumber uint64,
	issuerFirstByte byte,
	opCertSeqNo uint64,
	vrfOutput []byte,
) []byte {
	t.Helper()
	body := dijkstra.DijkstraBlockBody{}
	var issuer lcommon.IssuerVkey
	issuer[0] = issuerFirstByte
	header := &dijkstra.DijkstraBlockHeader{
		BabbageBlockHeader: babbage.BabbageBlockHeader{
			Body: babbage.BabbageBlockHeaderBody{
				BlockNumber:   blockNumber,
				Slot:          slot,
				IssuerVkey:    issuer,
				VrfResult:     lcommon.VrfResult{Output: vrfOutput},
				BlockBodyHash: body.Hash(),
			},
		},
	}
	setOpCertSequenceNumber(
		&header.Body.OpCert.SequenceNumber,
		opCertSeqNo,
	)
	block := &dijkstra.DijkstraBlock{BlockHeader: header, BlockBody: body}
	cborData, err := block.MarshalCBOR()
	require.NoError(t, err)
	return cborData
}

// insertTestDijkstraBlock stores a Dijkstra block built by
// newTestDijkstraBlockCbor at the given hash so localTipPraosView's
// database.BlockByHash + Decode round trip can find and decode it.
func insertTestDijkstraBlock(
	t *testing.T,
	db *database.Database,
	slot, blockNumber uint64,
	hash []byte,
	issuerFirstByte byte,
	opCertSeqNo uint64,
	vrfOutput []byte,
) {
	t.Helper()
	cborData := newTestDijkstraBlockCbor(
		t, slot, blockNumber, issuerFirstByte, opCertSeqNo, vrfOutput,
	)
	require.NoError(t, db.BlockCreate(models.Block{
		Slot:   slot,
		Hash:   hash,
		Cbor:   cborData,
		Type:   dijkstra.BlockTypeDijkstra,
		Number: blockNumber,
	}, nil))
}

// TestCompareIncomingHeaderToLocalTip_Dijkstra exercises the actual
// chain-selection caller (ledger/chainsync.go's compareIncomingHeaderToLocalTip,
// the mechanism issue #3075 identified as reachable from
// ledger/chainsync.go:1855-1904 and ouroboros/chainsync.go:758) end to end for
// Dijkstra headers: the local tip is a real Dijkstra block round-tripped
// through storage (database.BlockByHash -> models.Block.Decode), and the
// incoming header is a plain in-process *dijkstra.DijkstraBlockHeader as
// chainsync delivers it. Before the view.go fix this always resolved
// ChainEqual for a Dijkstra local tip (GetPraosTiebreakerView returned
// ok=false), silently disarming the VRF tiebreaker in exactly this path.
func TestCompareIncomingHeaderToLocalTip_Dijkstra(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	const blockNumber = 50
	localHash := []byte("dijkstra-local-tip-hash-32-bytes")[:32]
	localSlot := uint64(200)
	localTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: localSlot, Hash: localHash},
		BlockNumber: blockNumber,
	}

	testCases := []struct {
		name          string
		localVRF      []byte
		incomingVRF   []byte
		incomingSlot  uint64
		expectedBeats praos.ChainComparisonResult
	}{
		{
			name:          "incoming lower VRF beats local tip",
			localVRF:      make64ByteVRFFirstByteLedger(0xFF),
			incomingVRF:   make64ByteVRFFirstByteLedger(0x01),
			incomingSlot:  202,
			expectedBeats: praos.ChainABetter,
		},
		{
			name:          "incoming higher VRF loses to local tip",
			localVRF:      make64ByteVRFFirstByteLedger(0x01),
			incomingVRF:   make64ByteVRFFirstByteLedger(0xFF),
			incomingSlot:  202,
			expectedBeats: praos.ChainBBetter,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Re-seed the local tip block for each subtest under a
			// case-specific hash so BlockByHash resolves the intended VRF.
			hash := append([]byte(nil), localHash...)
			hash[len(hash)-1] = byte(len(tc.name)) // vary hash per case
			localTip := localTip
			localTip.Point.Hash = hash
			insertTestDijkstraBlock(
				t, db, localSlot, blockNumber, hash,
				0xAB, 3, tc.localVRF,
			)

			incomingHeader := &dijkstra.DijkstraBlockHeader{
				BabbageBlockHeader: babbage.BabbageBlockHeader{
					Body: babbage.BabbageBlockHeaderBody{
						BlockNumber: blockNumber,
						Slot:        tc.incomingSlot,
						VrfResult:   lcommon.VrfResult{Output: tc.incomingVRF},
						OpCert: babbage.BabbageOpCert{
							SequenceNumber: 3,
						},
					},
				},
			}
			event := ChainsyncEvent{
				BlockHeader: incomingHeader,
				Point: ocommon.Point{
					Slot: tc.incomingSlot,
					Hash: []byte("incoming-hash"),
				},
			}

			result := ls.compareIncomingHeaderToLocalTip(event, localTip)
			require.Equal(
				t,
				tc.expectedBeats,
				result,
				"Dijkstra local tip must participate in the VRF tiebreaker through the real storage/decode path",
			)
		})
	}
}

// make64ByteVRFFirstByteLedger mirrors consensus/praos's test helper of the
// same shape; duplicated here since it is unexported in another package.
func make64ByteVRFFirstByteLedger(first byte) []byte {
	vrf := make([]byte, praos.VRFOutputSize)
	vrf[0] = first
	return vrf
}
