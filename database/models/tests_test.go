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

package models

import (
	"bytes"
	"maps"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

// Conway transaction-body and certificate shapes that the CDDL forbids must
// fail block decode through DecodeBlockCbor, the decoder every stored and
// replayed Conway block goes through. Each malformed case is also placed in an
// isValid=false transaction: structure is checked regardless of validity.

func shapeHash(n int, fill byte) []byte {
	return bytes.Repeat([]byte{fill}, n)
}

// shapeBodyMap returns a minimal Conway transaction body with extra keys.
func shapeBodyMap(extra map[uint]any) map[uint]any {
	body := map[uint]any{
		0: cbor.Tag{
			Number:  258,
			Content: []any{[]any{shapeHash(32, 0x11), uint64(0)}},
		},
		1: []any{[]any{append([]byte{0x61}, shapeHash(28, 0x22)...), uint64(1_000_000)}},
		2: uint64(200_000),
	}
	maps.Copy(body, extra)
	return body
}

// buildShapeBlock wraps one transaction body in an otherwise valid Conway
// block, recomputing the header body hash that DecodeBlockCbor verifies.
func buildShapeBlock(t *testing.T, body map[uint]any, isValid bool) []byte {
	t.Helper()
	base := testutil.BuildDecodableConwayBlockBytes(t, 100, 7)
	var comps []cbor.RawMessage
	_, err := cbor.Decode(base, &comps)
	require.NoError(t, err)
	require.Len(t, comps, 5)
	enc := func(v any) cbor.RawMessage {
		b, encErr := cbor.Encode(v)
		require.NoError(t, encErr)
		return b
	}
	comps[1] = enc([]any{body})
	comps[2] = enc([]any{map[uint]any{}})
	comps[3] = enc(map[uint]any{})
	if isValid {
		comps[4] = enc([]uint{})
	} else {
		comps[4] = enc([]uint{0})
	}
	// The header commits to the body components, and decode verifies it.
	var concat []byte
	for _, comp := range comps[1:] {
		h := lcommon.Blake2b256Hash(comp)
		concat = append(concat, h.Bytes()...)
	}
	bodyHash := lcommon.Blake2b256Hash(concat)
	var header []cbor.RawMessage
	_, err = cbor.Decode(comps[0], &header)
	require.NoError(t, err)
	var headerBody []cbor.RawMessage
	_, err = cbor.Decode(header[0], &headerBody)
	require.NoError(t, err)
	headerBody[7] = enc(bodyHash.Bytes())
	header[0] = enc(headerBody)
	comps[0] = enc(header)
	raw, err := cbor.Encode(comps)
	require.NoError(t, err)
	return raw
}

func decodeShapeBlock(raw []byte) error {
	_, err := DecodeBlockCbor(gledger.BlockTypeConway, raw)
	return err
}

// requireShapeOutcome asserts the body decodes (wantErr == "") or fails with
// an error naming wantErr, for both validity flags.
func requireShapeOutcome(
	t *testing.T,
	body map[uint]any,
	wantErr string,
) {
	t.Helper()
	for _, isValid := range []bool{true, false} {
		name := "isValid"
		if !isValid {
			name = "isInvalid"
		}
		t.Run(name, func(t *testing.T) {
			err := decodeShapeBlock(buildShapeBlock(t, body, isValid))
			if wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), wantErr)
		})
	}
}

func voteDelegCert(drep any) []any {
	return []any{9, []any{0, shapeHash(28, 0x33)}, drep}
}

func TestConwayBlockDecodeDRepShape(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		drep    any
		wantErr string
	}{
		{"key hash", []any{0, shapeHash(28, 0x44)}, ""},
		{"script hash", []any{1, shapeHash(28, 0x44)}, ""},
		{"abstain", []any{2}, ""},
		{"no confidence", []any{3}, ""},
		{"short key hash", []any{0, shapeHash(27, 0x44)}, "drep credential"},
		{"long key hash", []any{0, shapeHash(29, 0x44)}, "drep credential"},
		{"short script hash", []any{1, shapeHash(27, 0x44)}, "drep credential"},
		{"long script hash", []any{1, shapeHash(29, 0x44)}, "drep credential"},
		{"key hash extra item", []any{0, shapeHash(28, 0x44), 1}, "exactly 2 list items"},
		{"key hash missing hash", []any{0}, "exactly 2 list items"},
		{"abstain extra payload", []any{2, shapeHash(28, 0x44)}, "exactly 1 list item"},
		{"no confidence extra payload", []any{3, uint64(1)}, "exactly 1 list item"},
		{"unknown type", []any{4}, "unknown drep type: 4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireShapeOutcome(t, shapeBodyMap(map[uint]any{
				4: []any{voteDelegCert(tc.drep)},
			}), tc.wantErr)
		})
	}
}

func TestDRepDirectDecodeShape(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		drep    any
		wantErr string
	}{
		{"key hash", []any{0, shapeHash(28, 0x44)}, ""},
		{"abstain", []any{2}, ""},
		{"short hash", []any{0, shapeHash(27, 0x44)}, "drep credential"},
		{"long hash", []any{1, shapeHash(29, 0x44)}, "drep credential"},
		{"no confidence extra", []any{3, 0}, "exactly 1 list item"},
		{"unknown type", []any{7}, "unknown drep type: 7"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, err := cbor.Encode(tc.drep)
			require.NoError(t, err)
			var drep lcommon.Drep
			err = drep.UnmarshalCBOR(raw)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func conwaySet(items ...any) cbor.Tag {
	return cbor.Tag{Number: 258, Content: items}
}

func poolRegCert(owners any, relays []any) []any {
	return []any{
		3,
		shapeHash(28, 0x55),
		shapeHash(32, 0x66),
		uint64(1_000_000),
		uint64(340_000_000),
		cbor.Tag{Number: 30, Content: []any{1, 2}},
		append([]byte{0xe0}, shapeHash(28, 0x77)...),
		owners,
		relays,
		nil,
	}
}

func TestConwayBlockDecodePoolRelayShape(t *testing.T) {
	t.Parallel()
	host := "relay.example.com"
	for _, tc := range []struct {
		name    string
		relay   any
		wantErr string
	}{
		{"address", []any{0, 3001, shapeHash(4, 1), shapeHash(16, 2)}, ""},
		{"address all absent", []any{0, nil, nil, nil}, ""},
		{"host name", []any{1, 3001, host}, ""},
		{"host name no port", []any{1, nil, host}, ""},
		{"multi host name", []any{2, host}, ""},
		{"ipv4 too short", []any{0, 3001, shapeHash(3, 1), nil}, lcommon.ErrPoolRelayAddressWidth.Error()},
		{"ipv4 too long", []any{0, 3001, shapeHash(5, 1), nil}, lcommon.ErrPoolRelayAddressWidth.Error()},
		{"ipv6 too short", []any{0, 3001, nil, shapeHash(15, 2)}, lcommon.ErrPoolRelayAddressWidth.Error()},
		{"ipv6 too long", []any{0, 3001, nil, shapeHash(17, 2)}, lcommon.ErrPoolRelayAddressWidth.Error()},
		{"single host name missing name", []any{1, 3001, nil}, lcommon.ErrPoolRelayMissingHostname.Error()},
		{"multi host name missing name", []any{2, nil}, lcommon.ErrPoolRelayMissingHostname.Error()},
		{"port above range", []any{1, 65536, host}, "port"},
		{"host name above range", []any{2, strings.Repeat("a", 129)}, "hostname"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireShapeOutcome(t, shapeBodyMap(map[uint]any{
				4: []any{poolRegCert(conwaySet(shapeHash(28, 0x88)), []any{tc.relay})},
			}), tc.wantErr)
		})
	}
}

func TestConwayBlockDecodePoolOwnerDuplicates(t *testing.T) {
	t.Parallel()
	owner := func(first, last byte) []byte {
		h := shapeHash(28, 0x90)
		h[0], h[27] = first, last
		return h
	}
	many := func(n int, tail []byte) []any {
		out := make([]any, 0, n+1)
		for i := range n {
			out = append(out, owner(byte(i), 0))
		}
		return append(out, tail)
	}
	for _, tc := range []struct {
		name    string
		owners  []any
		wantErr string
	}{
		{"single", []any{owner(1, 1)}, ""},
		{"distinct", []any{owner(1, 1), owner(2, 2)}, ""},
		{"differ only in last byte", []any{owner(1, 1), owner(1, 2)}, ""},
		{"differ only in first byte", []any{owner(1, 1), owner(2, 1)}, ""},
		{"adjacent duplicate", []any{owner(1, 1), owner(1, 1)}, "duplicate owner"},
		{"separated duplicate", []any{owner(1, 1), owner(2, 2), owner(1, 1)}, "duplicate owner"},
		{"many distinct", many(12, owner(200, 1)), ""},
		{"many duplicate at end", many(12, owner(3, 0)), "duplicate owner"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireShapeOutcome(t, shapeBodyMap(map[uint]any{
				4: []any{poolRegCert(conwaySet(tc.owners...), []any{})},
			}), tc.wantErr)
		})
	}
}

// TestBabbageBlockDecodeAcceptsDuplicatePoolOwners pins the historical
// behaviour: the duplicate-owner rule starts at Conway.
func TestBabbageBlockDecodeAcceptsDuplicatePoolOwners(t *testing.T) {
	t.Parallel()
	dup := shapeHash(28, 0x91)
	// Babbage has no set tag: inputs and owners are plain arrays.
	body := map[uint]any{
		0: []any{[]any{shapeHash(32, 0x11), uint64(0)}},
		1: []any{[]any{append([]byte{0x61}, shapeHash(28, 0x22)...), uint64(1_000_000)}},
		2: uint64(200_000),
		4: []any{poolRegCert([]any{dup, dup}, []any{})},
	}
	raw := buildShapeBlock(t, body, true)
	_, err := gledger.NewBlockFromCbor(gledger.BlockTypeBabbage, raw)
	require.NoError(t, err)
	err = decodeShapeBlock(raw)
	require.Error(t, err, "control: the same bytes are rejected as Conway")
	require.Contains(t, err.Error(), "duplicate owner")
}

func regDRepCert(url string) []any {
	return []any{
		16,
		[]any{0, shapeHash(28, 0xa1)},
		uint64(500_000_000),
		[]any{url, shapeHash(32, 0xa2)},
	}
}

func TestConwayBlockDecodeGovernanceBodyBounds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		extra   map[uint]any
		wantErr string
	}{
		{"anchor url 128 bytes", map[uint]any{4: []any{regDRepCert(strings.Repeat("a", 128))}}, ""},
		{"anchor url 129 bytes", map[uint]any{4: []any{regDRepCert(strings.Repeat("a", 129))}}, lcommon.ErrGovAnchorURLTooLong.Error()},
		{"treasury absent", nil, ""},
		{"treasury present zero", map[uint]any{21: int64(0)}, ""},
		{"treasury positive", map[uint]any{21: int64(5)}, ""},
		{"treasury negative", map[uint]any{21: int64(-1)}, "current treasury value"},
		{"donation absent", nil, ""},
		{"donation minimal", map[uint]any{22: uint64(1)}, ""},
		{"donation max word64", map[uint]any{22: uint64(1<<64 - 1)}, ""},
		{"donation zero", map[uint]any{22: uint64(0)}, "22"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireShapeOutcome(t, shapeBodyMap(tc.extra), tc.wantErr)
		})
	}
}

func TestConwayBlockDecodeGovActionIndexRange(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		index   uint64
		wantErr string
	}{
		{"255", 255, ""},
		{"256", 256, ""},
		{"65535", 65535, ""},
		{"65536", 65536, "exceeds the maximum of 65535"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			enc := func(v any) []byte {
				b, err := cbor.Encode(v)
				require.NoError(t, err)
				return b
			}
			// voting_procedures = {voter => {gov_action_id => [vote, anchor / null]}}
			// Arrays cannot be Go map keys, so the nested maps are assembled
			// from encoded items.
			var votes []byte
			votes = append(votes, 0xa1)
			votes = append(votes, enc([]any{4, shapeHash(28, 0xb1)})...)
			votes = append(votes, 0xa1)
			votes = append(votes, enc([]any{shapeHash(32, 0xb2), tc.index})...)
			votes = append(votes, enc([]any{1, nil})...)
			body := shapeBodyMap(map[uint]any{19: cbor.RawMessage(votes)})
			for _, isValid := range []bool{true, false} {
				err := decodeShapeBlock(buildShapeBlock(t, body, isValid))
				if tc.wantErr != "" {
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.wantErr)
					continue
				}
				require.NoError(t, err)
			}
		})
	}
}

func TestGovActionIdStringDoesNotPanicAboveCip0129Range(t *testing.T) {
	t.Parallel()
	for _, index := range []uint32{255, 256, 65535, math.MaxUint32} {
		id := lcommon.GovActionId{
			TransactionId: [32]byte{0xc1},
			GovActionIdx:  index,
		}
		require.NotPanics(t, func() { _ = id.String() })
		require.NotEmpty(t, id.String())
	}
}

// TestDijkstraBodyDecodeDonationRange covers body key 22, typed
// positive_coin in both the top-level and the subtransaction body.
func TestDijkstraBodyDecodeDonationRange(t *testing.T) {
	t.Parallel()
	for _, level := range []struct {
		name   string
		noFee  bool
		decode func([]byte) error
	}{
		{"top-level body", false, func(raw []byte) error {
			var body gdijkstra.DijkstraTransactionBody
			return body.UnmarshalCBOR(raw)
		}},
		{"subtransaction body", true, func(raw []byte) error {
			var body gdijkstra.DijkstraSubTransactionBody
			return body.UnmarshalCBOR(raw)
		}},
	} {
		t.Run(level.name, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				name    string
				extra   map[uint]any
				wantErr bool
			}{
				{"donation absent", nil, false},
				{"donation minimal", map[uint]any{22: uint64(1)}, false},
				{"donation max word64", map[uint]any{22: uint64(1<<64 - 1)}, false},
				{"donation zero", map[uint]any{22: uint64(0)}, true},
				{"treasury present zero", map[uint]any{21: uint64(0)}, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					body := shapeBodyMap(tc.extra)
					if level.noFee {
						// A subtransaction body has no fee field.
						delete(body, 2)
					}
					raw, err := cbor.Encode(body)
					require.NoError(t, err)
					err = level.decode(raw)
					if tc.wantErr {
						require.Error(t, err)
						require.Contains(t, err.Error(), "22")
						return
					}
					require.NoError(t, err)
				})
			}
		})
	}
}

func updateCommitteeProposal(quorum any) []any {
	return []any{
		uint64(1_000_000_000),
		append([]byte{0xe0}, shapeHash(28, 0xd1)...),
		[]any{4, nil, conwaySet(), map[uint]uint{}, quorum},
		[]any{"https://example.test/committee", shapeHash(32, 0xd2)},
	}
}

func TestConwayBlockDecodeUpdateCommitteeQuorum(t *testing.T) {
	t.Parallel()
	rat := func(n, d int64) any {
		return cbor.Tag{Number: 30, Content: []any{n, d}}
	}
	for _, tc := range []struct {
		name    string
		quorum  any
		wantErr string
	}{
		{"zero", rat(0, 1), ""},
		{"one", rat(1, 1), ""},
		{"one half", rat(1, 2), ""},
		{"negative", rat(-1, 2), "outside [0,1]"},
		{"above one", rat(3, 2), "outside [0,1]"},
		{"zero denominator", rat(1, 0), "denominator"},
		{"missing", nil, "invalid cbor.Rat"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body := shapeBodyMap(map[uint]any{
				20: []any{updateCommitteeProposal(tc.quorum)},
			})
			for _, isValid := range []bool{true, false} {
				err := decodeShapeBlock(buildShapeBlock(t, body, isValid))
				if tc.wantErr != "" {
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.wantErr)
					continue
				}
				require.NoError(t, err)
			}
		})
	}
}

func TestNewUpdateCommitteeGovActionQuorum(t *testing.T) {
	t.Parallel()
	_, err := lcommon.NewUpdateCommitteeGovAction(
		nil, nil, nil, cbor.Rat{},
	)
	require.Error(t, err)
	for _, tc := range []struct {
		name    string
		quorum  *big.Rat
		wantErr bool
	}{
		{"zero", big.NewRat(0, 1), false},
		{"one half", big.NewRat(1, 2), false},
		{"one", big.NewRat(1, 1), false},
		{"negative", big.NewRat(-1, 2), true},
		{"above one", big.NewRat(3, 2), true},
	} {
		_, err = lcommon.NewUpdateCommitteeGovAction(
			nil, nil, nil, cbor.Rat{Rat: tc.quorum},
		)
		if tc.wantErr {
			require.Error(t, err, tc.name)
			require.Contains(t, err.Error(), "outside [0,1]", tc.name)
			continue
		}
		require.NoError(t, err, tc.name)
	}
}

// TestDRepUnknownDiscriminatorIsNotShapeError keeps the two rejection classes
// apart: an unknown discriminator names the type, and a malformed shape of a
// known type never reports an unknown type.
func TestDRepUnknownDiscriminatorIsNotShapeError(t *testing.T) {
	t.Parallel()
	decode := func(drep any) error {
		raw, err := cbor.Encode(drep)
		require.NoError(t, err)
		var d lcommon.Drep
		return d.UnmarshalCBOR(raw)
	}
	for _, tc := range []struct {
		name string
		drep any
	}{
		{"type 4 bare", []any{4}},
		{"type 4 with hash", []any{4, shapeHash(28, 0x44)}},
		{"type 255", []any{255}},
	} {
		t.Run("unknown/"+tc.name, func(t *testing.T) {
			err := decode(tc.drep)
			require.ErrorContains(t, err, "unknown drep type")
			require.NotContains(t, err.Error(), "exactly")
			require.NotContains(t, err.Error(), "drep credential")
		})
	}
	for _, tc := range []struct {
		name string
		drep any
	}{
		{"short key hash", []any{0, shapeHash(27, 0x44)}},
		{"long script hash", []any{1, shapeHash(29, 0x44)}},
		{"key hash missing", []any{0}},
		{"abstain extra payload", []any{2, shapeHash(28, 0x44)}},
		{"no confidence extra payload", []any{3, uint64(1)}},
	} {
		t.Run("malformed/"+tc.name, func(t *testing.T) {
			err := decode(tc.drep)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "unknown drep type")
		})
	}
}
