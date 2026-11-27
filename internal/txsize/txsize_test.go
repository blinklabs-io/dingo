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

package txsize_test

import (
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/internal/txsize"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

var (
	testBody      = []byte{0xa1, 0x00, 0x80}                           // {0: []}
	testWitnesses = []byte{0xa0}                                       // {}
	auxNull       = []byte{0xf6}                                       // null
	auxMap        = []byte{0xa1, 0x01, 0x61, 0x61}                     // {1: "a"}
	auxMapScripts = append([]byte{0x82, 0xa1, 0x01, 0x61, 0x61}, 0x80) // [{1: "a"}, []]
)

// frame wraps pre-encoded components in an outer array with the named header.
func frame(t *testing.T, framing string, parts ...[]byte) []byte {
	t.Helper()
	n := byte(len(parts))
	var out []byte
	switch framing {
	case "definite":
		out = []byte{0x80 | n}
	case "wide8":
		out = []byte{0x98, n}
	case "wide16":
		out = []byte{0x99, 0x00, n}
	case "indefinite":
		out = []byte{0x9f}
	default:
		t.Fatalf("unknown framing %q", framing)
	}
	for _, p := range parts {
		out = append(out, p...)
	}
	if framing == "indefinite" {
		out = append(out, 0xff)
	}
	return out
}

var framings = []string{"definite", "wide8", "wide16", "indefinite"}

// The size is one array-header byte plus the three components, whatever the
// received outer framing (cardano-ledger Shelley/Tx.hs sizeShelleyTxF,
// Allegra/Tx.hs and Mary/Tx.hs sizeTxF).
func TestFromCBORShelleyFamilyIgnoresOuterFraming(t *testing.T) {
	t.Parallel()
	eras := []struct {
		name   string
		txType uint
		auxes  map[string][]byte
	}{
		{"shelley", uint(ledger.TxTypeShelley), map[string][]byte{
			"null": auxNull, "map": auxMap,
		}},
		{"allegra", uint(ledger.TxTypeAllegra), map[string][]byte{
			"null": auxNull, "map": auxMap, "mapScripts": auxMapScripts,
		}},
		{"mary", uint(ledger.TxTypeMary), map[string][]byte{
			"null": auxNull, "map": auxMap, "mapScripts": auxMapScripts,
		}},
	}
	for _, era := range eras {
		for auxName, aux := range era.auxes {
			want := uint64(1 + len(testBody) + len(testWitnesses) + len(aux))
			for _, framing := range framings {
				t.Run(
					fmt.Sprintf("%s/%s/%s", era.name, auxName, framing),
					func(t *testing.T) {
						t.Parallel()
						data := frame(t, framing, testBody, testWitnesses, aux)
						require.Equal(
							t, want, txsize.FromCBOR(era.txType, data),
						)
					},
				)
			}
		}
	}
}

// Input that is not exactly a three-component array keeps its received length.
func TestFromCBORShelleyFamilyFallsBackToReceivedLength(t *testing.T) {
	t.Parallel()
	cases := map[string][]byte{
		"two components": frame(t, "wide8", testBody, testWitnesses),
		"four components": frame(
			t, "wide8", testBody, testWitnesses, auxNull, auxNull,
		),
		"trailing bytes": append(
			frame(t, "indefinite", testBody, testWitnesses, auxNull), 0x00,
		),
		"not an array": {0xa0},
		"truncated":    {0x9f, 0xa0},
	}
	for _, txType := range []int{
		ledger.TxTypeShelley, ledger.TxTypeAllegra, ledger.TxTypeMary,
	} {
		for name, data := range cases {
			t.Run(fmt.Sprintf("%d/%s", txType, name), func(t *testing.T) {
				t.Parallel()
				require.Equal(
					t, uint64(len(data)), txsize.FromCBOR(uint(txType), data),
				)
			})
		}
	}
}

// Types outside Shelley-Mary keep their existing measure.
func TestFromCBOROtherTypesUnchanged(t *testing.T) {
	t.Parallel()
	for _, framing := range framings {
		three := frame(t, framing, testBody, testWitnesses, auxNull)
		want := uint64(1 + len(testBody) + len(testWitnesses) + len(auxNull))
		for _, txType := range []int{
			ledger.TxTypeAlonzo, ledger.TxTypeBabbage, ledger.TxTypeConway,
			ledger.TxTypeDijkstra,
		} {
			t.Run(fmt.Sprintf("%d/%s", txType, framing), func(t *testing.T) {
				t.Parallel()
				require.Equal(t, want, txsize.FromCBOR(uint(txType), three))
			})
		}
	}
	// Byron-style and unknown types return the received length.
	data := frame(t, "wide8", testBody, testWitnesses, auxNull)
	require.Equal(t, uint64(len(data)), txsize.FromCBOR(0, data))
	require.Equal(t, uint64(len(data)), txsize.FromCBOR(99, data))
}

// A component is measured by its received bytes, a tag in front of its item
// included: the reference re-encodes each component as the bytes it was
// decoded from. Only the outer array framing is replaced.
func TestFromCBORCountsComponentTags(t *testing.T) {
	t.Parallel()
	selfDescribe := []byte{0xd9, 0xd9, 0xf7}
	taggedWitnesses := append(append([]byte{}, selfDescribe...), testWitnesses...)
	want := uint64(1 + len(testBody) + len(taggedWitnesses) + len(auxNull))
	for _, txType := range []int{
		ledger.TxTypeShelley, ledger.TxTypeAllegra, ledger.TxTypeMary,
		ledger.TxTypeAlonzo, ledger.TxTypeBabbage, ledger.TxTypeConway,
		ledger.TxTypeDijkstra,
	} {
		for _, framing := range framings {
			t.Run(fmt.Sprintf("%d/%s", txType, framing), func(t *testing.T) {
				t.Parallel()
				data := frame(t, framing, testBody, taggedWitnesses, auxNull)
				require.Equal(t, want, txsize.FromCBOR(uint(txType), data))
			})
		}
	}
}

// A tag around the whole transaction is not an outer array, so the received
// length is kept rather than measuring the tagged array's components.
func TestFromCBORTaggedOuterArrayKeepsReceivedLength(t *testing.T) {
	t.Parallel()
	data := append(
		[]byte{0xd9, 0xd9, 0xf7},
		frame(t, "definite", testBody, testWitnesses, auxNull)...,
	)
	for _, txType := range []int{
		ledger.TxTypeShelley, ledger.TxTypeAlonzo, ledger.TxTypeConway,
		ledger.TxTypeDijkstra,
	} {
		require.Equal(
			t, uint64(len(data)), txsize.FromCBOR(uint(txType), data),
			"type %d", txType,
		)
	}
}
