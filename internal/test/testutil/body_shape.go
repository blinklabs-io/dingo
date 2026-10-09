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

package testutil

import (
	"bytes"
	"maps"
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

// BodyShapeCase is one transaction body whose Conway or Dijkstra wire shape
// is either legal (WantErr empty) or violates a body constraint, in which case
// WantErr is a substring of the error that names the violated constraint.
// Extra holds the body keys added to a minimal valid body.
type BodyShapeCase struct {
	Name    string
	Extra   map[uint]any
	WantErr string
}

// ShapeHash returns n bytes of fill.
func ShapeHash(n int, fill byte) []byte {
	return bytes.Repeat([]byte{fill}, n)
}

// ShapeTxBody returns a minimal Conway or Dijkstra transaction body map with
// extra keys merged over it.
func ShapeTxBody(extra map[uint]any) map[uint]any {
	body := map[uint]any{
		0: cbor.Tag{
			Number:  258,
			Content: []any{[]any{ShapeHash(32, 0x11), uint64(0)}},
		},
		1: []any{[]any{
			append([]byte{0x61}, ShapeHash(28, 0x22)...),
			uint64(1_000_000),
		}},
		2: uint64(200_000),
	}
	maps.Copy(body, extra)
	return body
}

func shapeEncode(t testing.TB, v any) cbor.RawMessage {
	t.Helper()
	b, err := cbor.Encode(v)
	require.NoError(t, err)
	return b
}

// ConwayShapeTxBytes returns a Conway transaction carrying body, in the
// four-element form a mempool submission and a block both use.
func ConwayShapeTxBytes(t testing.TB, body map[uint]any, isValid bool) []byte {
	t.Helper()
	return shapeEncode(t, []any{body, map[uint]any{}, isValid, nil})
}

// DijkstraShapeTxBytes returns a Dijkstra block_transaction carrying body.
func DijkstraShapeTxBytes(t testing.TB, body map[uint]any, isValid bool) []byte {
	t.Helper()
	return shapeEncode(t, []any{body, map[uint]any{}, nil, isValid})
}

// DijkstraShapeMempoolTxBytes returns a Dijkstra mempool_transaction carrying
// body, which has no is_valid flag.
func DijkstraShapeMempoolTxBytes(t testing.TB, body map[uint]any) []byte {
	t.Helper()
	return shapeEncode(t, []any{body, map[uint]any{}, nil})
}

// BuildConwayBlockBytesWithTx wraps one transaction body in a Conway block
// whose header carries the matching block body hash. The header has no valid
// VRF, KES or operational-certificate material, so the block only suits
// decode tests. When isValid is false the transaction is listed as invalid.
func BuildConwayBlockBytesWithTx(
	t *testing.T,
	body map[uint]any,
	isValid bool,
) []byte {
	t.Helper()
	invalid := []uint{}
	if !isValid {
		invalid = []uint{0}
	}
	return BuildBlockBytesFromComponents(
		t,
		gledger.BlockTypeConway,
		100,
		7,
		shapeEncode(t, []any{body}),
		shapeEncode(t, []any{map[uint]any{}}),
		shapeEncode(t, map[uint]any{}),
		shapeEncode(t, invalid),
	)
}

// BuildDijkstraBlockBytesWithTx wraps one Dijkstra block_transaction in a
// Dijkstra block whose header carries the matching block body hash.
func BuildDijkstraBlockBytesWithTx(t testing.TB, txBytes []byte) []byte {
	t.Helper()
	blockBody := shapeEncode(t, []any{
		[]cbor.RawMessage{txBytes}, nil, nil,
	})
	vrf := lcommon.VrfResult{
		Output: make([]byte, 32),
		Proof:  make([]byte, 80),
	}
	header := &dijkstra.DijkstraBlockHeader{
		BabbageBlockHeader: babbage.BabbageBlockHeader{
			Body: babbage.BabbageBlockHeaderBody{
				BlockNumber:   7,
				Slot:          100,
				BlockBodyHash: lcommon.Blake2b256Hash(blockBody),
				VrfKey:        make([]byte, 32),
				VrfResult:     vrf,
				OpCert: babbage.BabbageOpCert{
					HotVkey:   make([]byte, 32),
					Signature: make([]byte, 64),
				},
				ProtoVersion: babbage.BabbageProtoVersion{Major: 12},
			},
			Signature: make([]byte, 448),
		},
	}
	return shapeEncode(t, []any{header, blockBody})
}

func shapeVoteDelegCert(drep any) []any {
	return []any{9, []any{0, ShapeHash(28, 0x33)}, drep}
}

func shapePoolRegCert(owners cbor.Tag) []any {
	return []any{
		3,
		ShapeHash(28, 0x55),
		ShapeHash(32, 0x66),
		uint64(1_000_000),
		uint64(340_000_000),
		cbor.Tag{Number: 30, Content: []any{1, 2}},
		append([]byte{0xe0}, ShapeHash(28, 0x77)...),
		owners,
		[]any{},
		nil,
	}
}

func shapeRegDRepCert(url string) []any {
	return []any{
		16,
		[]any{0, ShapeHash(28, 0xa1)},
		uint64(500_000_000),
		[]any{url, ShapeHash(32, 0xa2)},
	}
}

// shapeVotingProcedures builds voting_procedures with one voter voting on one
// action whose index is idx. Arrays cannot be Go map keys, so the nested maps
// are assembled from encoded items.
func shapeVotingProcedures(t testing.TB, idx uint64) cbor.RawMessage {
	t.Helper()
	voter := shapeEncode(t, []any{4, ShapeHash(28, 0xb1)})
	actionID := shapeEncode(t, []any{ShapeHash(32, 0xb2), idx})
	procedure := shapeEncode(t, []any{1, nil})
	votes := make([]byte, 0, len(voter)+len(actionID)+len(procedure)+2)
	votes = append(votes, 0xa1)
	votes = append(votes, voter...)
	votes = append(votes, 0xa1)
	votes = append(votes, actionID...)
	votes = append(votes, procedure...)
	return votes
}

// ConwayBodyShapeCases returns the Conway body shapes that CDDL, or a
// reference-implementation body constraint, rejects, each paired with the
// legal neighbour on either side of the boundary it crosses.
func ConwayBodyShapeCases(t testing.TB) []BodyShapeCase {
	t.Helper()
	owner := func(first, last byte) []byte {
		h := ShapeHash(28, 0x90)
		h[0], h[27] = first, last
		return h
	}
	owners := func(items ...any) cbor.Tag {
		return cbor.Tag{Number: 258, Content: items}
	}
	cert := func(c []any) map[uint]any {
		return map[uint]any{4: []any{c}}
	}
	return []BodyShapeCase{
		{"drep key hash", cert(shapeVoteDelegCert([]any{0, ShapeHash(28, 0x44)})), ""},
		{"drep abstain", cert(shapeVoteDelegCert([]any{2})), ""},
		{"drep no confidence", cert(shapeVoteDelegCert([]any{3})), ""},
		{"drep short key hash", cert(shapeVoteDelegCert([]any{0, ShapeHash(27, 0x44)})), "drep credential"},
		{"drep long script hash", cert(shapeVoteDelegCert([]any{1, ShapeHash(29, 0x44)})), "drep credential"},
		{"drep abstain extra payload", cert(shapeVoteDelegCert([]any{2, ShapeHash(28, 0x44)})), "exactly 1 list item"},
		{"drep no confidence extra payload", cert(shapeVoteDelegCert([]any{3, uint64(1)})), "exactly 1 list item"},
		{"drep unknown type", cert(shapeVoteDelegCert([]any{4})), "unknown drep type: 4"},
		{"pool owners distinct", cert(shapePoolRegCert(owners(owner(1, 1), owner(2, 2)))), ""},
		{"pool owners differ in last byte", cert(shapePoolRegCert(owners(owner(1, 1), owner(1, 2)))), ""},
		{"pool owners duplicate", cert(shapePoolRegCert(owners(owner(1, 1), owner(2, 2), owner(1, 1)))), "duplicate owner"},
		{"anchor url 128 bytes", cert(shapeRegDRepCert(strings.Repeat("a", 128))), ""},
		{"anchor url 129 bytes", cert(shapeRegDRepCert(strings.Repeat("a", 129))), lcommon.ErrGovAnchorURLTooLong.Error()},
		{"treasury absent", nil, ""},
		{"treasury present zero", map[uint]any{21: int64(0)}, ""},
		{"treasury negative", map[uint]any{21: int64(-1)}, "current treasury value must not be negative"},
		{"donation minimal", map[uint]any{22: uint64(1)}, ""},
		{"donation max word64", map[uint]any{22: uint64(1<<64 - 1)}, ""},
		{"donation zero", map[uint]any{22: uint64(0)}, "field 22 must be positive"},
		{"gov action index 255", map[uint]any{19: shapeVotingProcedures(t, 255)}, ""},
		{"gov action index 256", map[uint]any{19: shapeVotingProcedures(t, 256)}, ""},
		{"gov action index 65535", map[uint]any{19: shapeVotingProcedures(t, 65535)}, ""},
		{"gov action index 65536", map[uint]any{19: shapeVotingProcedures(t, 65536)}, "exceeds the maximum of 65535"},
	}
}

// DijkstraSubTransactionBytes returns a Dijkstra sub-transaction whose body
// carries extra keys. A sub-transaction body has no fee field.
func DijkstraSubTransactionBytes(t testing.TB, extra map[uint]any) []byte {
	t.Helper()
	body := ShapeTxBody(extra)
	delete(body, 2)
	return shapeEncode(t, []any{body, map[uint]any{}, nil})
}

// DijkstraBodyShapeCases returns Dijkstra top-level bodies, and bodies nesting
// a sub-transaction, that differ only in the donation (body key 22) value.
func DijkstraBodyShapeCases(t testing.TB) []BodyShapeCase {
	t.Helper()
	nested := func(extra map[uint]any) map[uint]any {
		return map[uint]any{
			23: cbor.Tag{
				Number: 258,
				Content: []any{
					cbor.RawMessage(DijkstraSubTransactionBytes(t, extra)),
				},
			},
		}
	}
	return []BodyShapeCase{
		{"top-level donation absent", nil, ""},
		{"top-level donation minimal", map[uint]any{22: uint64(1)}, ""},
		{"top-level donation max word64", map[uint]any{22: uint64(1<<64 - 1)}, ""},
		{"top-level donation zero", map[uint]any{22: uint64(0)}, "field 22 must be positive"},
		{"top-level treasury present zero", map[uint]any{21: uint64(0)}, ""},
		{"sub-transaction donation absent", nested(nil), ""},
		{"sub-transaction donation minimal", nested(map[uint]any{22: uint64(1)}), ""},
		{"sub-transaction donation max word64", nested(map[uint]any{22: uint64(1<<64 - 1)}), ""},
		{"sub-transaction donation zero", nested(map[uint]any{22: uint64(0)}), "field 22 must be positive"},
		{"sub-transaction treasury present zero", nested(map[uint]any{21: uint64(0)}), ""},
	}
}
