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

package ouroboros

import (
	"bytes"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

const (
	// The Dijkstra header body carries ten Babbage fields, leios_certified
	// and leios_announcement.
	dijkstraHeaderBodyFields = 12
	mainnetMagic             = 764824073
)

// headerBodyVariant rewrites the header body of a Dijkstra header or block
// header. keep is how many leading body fields survive and extra are appended
// after them; the signature and every other byte are left alone.
func headerBodyVariant(
	t *testing.T,
	header []byte,
	keep int,
	extra ...any,
) []byte {
	t.Helper()
	var top []cbor.RawMessage
	_, err := cbor.Decode(header, &top)
	require.NoError(t, err)
	require.Len(t, top, 2)
	var body []cbor.RawMessage
	_, err = cbor.Decode(top[0], &body)
	require.NoError(t, err)
	require.Len(t, body, dijkstraHeaderBodyFields)
	body = body[:keep:keep]
	for _, field := range extra {
		raw, err := cbor.Encode(field)
		require.NoError(t, err)
		body = append(body, raw)
	}
	top[0], err = cbor.Encode(body)
	require.NoError(t, err)
	out, err := cbor.Encode(top)
	require.NoError(t, err)
	return out
}

type headerArityCase struct {
	name  string
	keep  int
	extra []any
	valid bool
}

// headerArityCases cover every arity a header body can take and the
// announcement shapes of the twelve-field form. Only a boolean followed by a
// null or an exactly [hash32, uint32] announcement is a current header.
func headerArityCases() []headerArityCase {
	hash := bytes.Repeat([]byte{0x5a}, 32)
	const babbageFields = 10
	return []headerArityCase{
		{"ten fields", babbageFields, nil, false},
		{"eleven fields", babbageFields, []any{true}, false},
		{"thirteen fields", babbageFields, []any{true, nil, nil}, false},
		{"fourteen fields", babbageFields, []any{true, nil, nil, nil}, false},
		{"null announcement", babbageFields, []any{false, nil}, true},
		{"certified null announcement", babbageFields, []any{true, nil}, true},
		{
			"valid announcement",
			babbageFields,
			[]any{false, []any{hash, uint64(4096)}},
			true,
		},
		{
			"announcement at the uint32 maximum",
			babbageFields,
			[]any{false, []any{hash, uint64(1<<32 - 1)}},
			true,
		},
		{
			"certified is not a boolean",
			babbageFields,
			[]any{uint64(1), nil},
			false,
		},
		{"certified is null", babbageFields, []any{nil, nil}, false},
		{
			"announcement hash too short",
			babbageFields,
			[]any{false, []any{hash[:31], uint64(1)}},
			false,
		},
		{
			"announcement without a size",
			babbageFields,
			[]any{false, []any{hash}},
			false,
		},
		{
			"announcement size exceeds uint32",
			babbageFields,
			[]any{false, []any{hash, uint64(1 << 32)}},
			false,
		},
		{
			"announcement is not an array",
			babbageFields,
			[]any{false, uint64(7)},
			false,
		},
	}
}

// Current Dijkstra headers have exactly twelve body fields. Every chain-sync
// path that decodes a peer header must reject the other arities and the
// malformed announcement shapes, and keep the wire bytes of an accepted
// header, which the KES check is computed over.
func TestDecodeChainsyncHeaderEnforcesDijkstraBodyArity(t *testing.T) {
	t.Parallel()
	valid := readHexFixture(t, musashiType8HeaderFixture)
	for _, network := range []struct {
		name  string
		magic uint32
		types []uint
	}{
		{"mainnet", mainnetMagic, []uint{gledger.BlockTypeDijkstra}},
		{
			"musashi",
			musashiNetworkMagic,
			[]uint{gledger.BlockTypeDijkstra, gledger.BlockTypeConway},
		},
	} {
		o := newOuroboros(OuroborosConfig{
			Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
			NetworkMagic: network.magic,
		})
		for _, blockType := range network.types {
			for _, tc := range headerArityCases() {
				t.Run(network.name+"/"+tc.name, func(t *testing.T) {
					t.Parallel()
					raw := headerBodyVariant(t, valid, tc.keep, tc.extra...)
					header, err := o.decodeChainsyncHeader(blockType, raw)
					if !tc.valid {
						require.Error(t, err)
						require.Nil(t, header)
						return
					}
					require.NoError(t, err)
					require.Equal(t, raw, header.Cbor())
				})
			}
		}
	}
}

// A rejected header stops at the raw chain-sync callback, which is the last
// step before the header is offered to chain selection and the ledger.
func TestChainsyncRollForwardRawRejectsMalformedDijkstraHeaders(t *testing.T) {
	t.Parallel()
	valid := readHexFixture(t, musashiType8HeaderFixture)
	ctx := ochainsync.CallbackContext{ConnectionId: decodeCacheTestConnId()}
	for _, tc := range headerArityCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := newMusashiOuroboros(t, nil)
			raw := headerBodyVariant(t, valid, tc.keep, tc.extra...)
			err := o.chainsyncClientRollForwardRaw(
				ctx,
				gledger.BlockTypeDijkstra,
				raw,
				ochainsync.Tip{},
			)
			if tc.valid {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, "decode chain-sync header")
		})
	}
}

// The leios-notify announcement header and the header inside a fetched block
// are held to the same arity.
func TestLeiosAnnouncementAndBlockfetchHeadersEnforceDijkstraBodyArity(
	t *testing.T,
) {
	t.Parallel()
	validHeader := readHexFixture(t, musashiType8HeaderFixture)
	validBlock := readHexFixture(t, musashiType8BlockFixture)
	var blockParts []cbor.RawMessage
	_, err := cbor.Decode(validBlock, &blockParts)
	require.NoError(t, err)
	require.Len(t, blockParts, 2)
	o := newMusashiOuroboros(t, nil)
	for _, tc := range headerArityCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			header := headerBodyVariant(t, validHeader, tc.keep, tc.extra...)
			announced, headerErr := decodeLeiosAnnouncementHeader(header)

			parts := append([]cbor.RawMessage(nil), blockParts...)
			parts[0] = headerBodyVariant(t, blockParts[0], tc.keep, tc.extra...)
			block, err := cbor.Encode(parts)
			require.NoError(t, err)
			fetched, blockErr := o.decodeBlockfetchBlock(
				gledger.BlockTypeDijkstra,
				block,
			)
			if tc.valid {
				require.NoError(t, headerErr)
				require.NotNil(t, announced)
				require.NoError(t, blockErr)
				require.NotNil(t, fetched)
				return
			}
			require.Error(t, headerErr)
			require.Nil(t, announced)
			require.Error(t, blockErr)
			require.Nil(t, fetched)
		})
	}
}
