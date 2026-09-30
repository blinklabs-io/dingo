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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// auxTxBody returns a transaction body carrying auxiliary_data_hash (field 7)
// equal to the blake2b-256 of aux, so a decode failure cannot be a hash
// mismatch. The same minimal body decodes in every Shelley-family era;
// Shelley requires the ttl.
func auxTxBody(aux []byte) []byte {
	hash := lcommon.Blake2b256Hash(aux)
	// {0: [], 1: [], 2: 0, 3: 1000, 7: h'<hash>'}
	body := []byte{
		0xa5, 0x00, 0x80, 0x01, 0x80, 0x02, 0x00, 0x03, 0x19, 0x03, 0xe8,
		0x07, 0x58, 0x20,
	}
	return append(body, hash.Bytes()...)
}

// auxBlockCase is one block body: txCount transactions, each carrying the
// auxiliary data at its index in auxByIndex when present. auxByIndex may name
// an index past the transaction list.
type auxBlockCase struct {
	name       string
	txCount    int
	auxByIndex map[uint][]byte
	wantErr    string
}

func buildAuxBlock(t *testing.T, blockType uint, tc auxBlockCase) []byte {
	t.Helper()
	metadataMap := []byte{0xa1, 0x01, 0x01}
	bodies := []byte{0x80 + byte(tc.txCount)}
	witnesses := []byte{0x80 + byte(tc.txCount)}
	for i := range tc.txCount {
		aux := tc.auxByIndex[uint(i)]
		if aux == nil {
			aux = metadataMap
		}
		bodies = append(bodies, auxTxBody(aux)...)
		witnesses = append(witnesses, 0xa0)
	}
	auxMap := []byte{0xa0 + byte(len(tc.auxByIndex))}
	for index := range uint(4) {
		if aux, ok := tc.auxByIndex[index]; ok {
			auxMap = append(auxMap, byte(index))
			auxMap = append(auxMap, aux...)
		}
	}
	components := [][]byte{bodies, witnesses, auxMap}
	if blockType >= gledger.BlockTypeAlonzo {
		components = append(components, []byte{0x80})
	}
	return testutil.BuildBlockBytesFromComponents(
		t, blockType, 100, 1, components...,
	)
}

// TestDecodeBlockAuxiliaryDataRules drives raw Shelley-through-Conway block
// bytes through every dingo entry point that decodes a block body from the
// wire or from storage, and asserts on the auxiliary-data rule that rejected
// the block rather than on rejection alone, since an unrelated failure would
// also reject these blocks. A block that fails to decode never reaches ledger
// application.
func TestDecodeBlockAuxiliaryDataRules(t *testing.T) {
	t.Parallel()

	metadataMap := []byte{0xa1, 0x01, 0x01}                // {1: 1}
	shelleyMaArray := []byte{0x82, 0xa1, 0x01, 0x01, 0x80} // [{1: 1}, []]
	shelleyMaOneElement := []byte{0x81, 0xa0}              // [{}]
	// 259({0: {1: 1}})
	taggedMetadata := []byte{0xd9, 0x01, 0x03, 0xa1, 0x00, 0xa1, 0x01, 0x01}
	// 259({0: {1: 1}, 6: []})
	taggedUnknownField := []byte{
		0xd9, 0x01, 0x03, 0xa2, 0x00, 0xa1, 0x01, 0x01, 0x06, 0x80,
	}
	// taggedPlutus returns 259({key: []}); key 2 carries Plutus V1 scripts
	// and key 5 Plutus V4.
	taggedPlutus := func(key byte) []byte {
		return []byte{0xd9, 0x01, 0x03, 0xa1, key, 0x80}
	}
	const (
		arrayRejected  = "Shelley-MA auxiliary-data arrays are not supported in this era"
		taggedRejected = "tagged auxiliary-data maps are not supported in this era"
	)
	plutusRejected := func(version int) string {
		return "auxiliary scripts for Plutus V" +
			string(rune('0'+version)) + " are not supported in this era"
	}

	indexCases := []auxBlockCase{
		{name: "empty auxiliary-data map", txCount: 1},
		{
			name:       "last valid index",
			txCount:    2,
			auxByIndex: map[uint][]byte{1: metadataMap},
		},
		{
			name:       "first invalid index",
			txCount:    2,
			auxByIndex: map[uint][]byte{2: metadataMap},
			wantErr:    "outside transaction list length 2",
		},
		{
			name:       "index with no transactions",
			txCount:    0,
			auxByIndex: map[uint][]byte{0: metadataMap},
			wantErr:    "outside transaction list length 0",
		},
		{
			name:       "metadata map",
			txCount:    1,
			auxByIndex: map[uint][]byte{0: metadataMap},
		},
	}
	aux := func(name string, data []byte, wantErr string) auxBlockCase {
		return auxBlockCase{
			name:       name,
			txCount:    1,
			auxByIndex: map[uint][]byte{0: data},
			wantErr:    wantErr,
		}
	}
	// Each era accepts the formats of earlier eras and rejects those of
	// later ones, and a tagged map admits only the Plutus languages the era
	// defines.
	eras := []struct {
		name      string
		blockType uint
		cases     []auxBlockCase
	}{
		{
			name:      "shelley",
			blockType: gledger.BlockTypeShelley,
			cases: []auxBlockCase{
				aux("shelley-ma array", shelleyMaArray, arrayRejected),
				aux("tagged map", taggedMetadata, taggedRejected),
			},
		},
		{
			name:      "allegra",
			blockType: gledger.BlockTypeAllegra,
			cases: []auxBlockCase{
				aux("shelley-ma array", shelleyMaArray, ""),
				aux("shelley-ma array with one element", shelleyMaOneElement,
					"must have 2 elements"),
				aux("tagged map", taggedMetadata, taggedRejected),
			},
		},
		{
			name:      "mary",
			blockType: gledger.BlockTypeMary,
			cases: []auxBlockCase{
				aux("shelley-ma array", shelleyMaArray, ""),
				aux("tagged map", taggedMetadata, taggedRejected),
			},
		},
		{
			name:      "alonzo",
			blockType: gledger.BlockTypeAlonzo,
			cases: []auxBlockCase{
				aux("shelley-ma array", shelleyMaArray, ""),
				aux("tagged map", taggedMetadata, ""),
				aux("tagged map unknown field", taggedUnknownField,
					"unknown auxiliary-data field 6"),
				aux("plutus v1 scripts", taggedPlutus(2), ""),
				aux("plutus v2 scripts", taggedPlutus(3), plutusRejected(2)),
			},
		},
		{
			name:      "babbage",
			blockType: gledger.BlockTypeBabbage,
			cases: []auxBlockCase{
				aux("shelley-ma array", shelleyMaArray, ""),
				aux("tagged map", taggedMetadata, ""),
				aux("tagged map unknown field", taggedUnknownField,
					"unknown auxiliary-data field 6"),
				aux("plutus v2 scripts", taggedPlutus(3), ""),
				aux("plutus v3 scripts", taggedPlutus(4), plutusRejected(3)),
			},
		},
		{
			name:      "conway",
			blockType: gledger.BlockTypeConway,
			cases: []auxBlockCase{
				aux("shelley-ma array", shelleyMaArray, ""),
				aux("shelley-ma array with one element", shelleyMaOneElement,
					"must have 2 elements"),
				aux("tagged map", taggedMetadata, ""),
				aux("tagged map unknown field", taggedUnknownField,
					"unknown auxiliary-data field 6"),
				aux("plutus v3 scripts", taggedPlutus(4), ""),
				aux("plutus v4 scripts", taggedPlutus(5), plutusRejected(4)),
			},
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	decoders := []struct {
		name   string
		conway bool
		decode func(blockType uint, raw []byte) (gledger.Block, error)
	}{
		{
			name: "blockfetch",
			decode: newOuroboros(OuroborosConfig{
				Logger:       logger,
				NetworkMagic: ouroboros.NetworkMainnet.NetworkMagic,
			}).decodeBlockfetchBlock,
		},
		{
			name:   "musashi blockfetch",
			conway: true,
			decode: newOuroboros(OuroborosConfig{
				Logger:       logger,
				NetworkMagic: ouroboros.NetworkCardanoMusashi.NetworkMagic,
			}).decodeBlockfetchBlock,
		},
		{
			// The ledger decodes stored block CBOR through this entry
			// before applying it.
			name: "stored block",
			decode: func(blockType uint, raw []byte) (gledger.Block, error) {
				return models.DecodeBlockCbor(blockType, raw)
			},
		},
	}

	for _, era := range eras {
		for _, tc := range append(append([]auxBlockCase{}, indexCases...), era.cases...) {
			for _, decoder := range decoders {
				if decoder.conway && era.blockType != gledger.BlockTypeConway {
					continue
				}
				t.Run(era.name+"/"+tc.name+"/"+decoder.name, func(t *testing.T) {
					t.Parallel()
					raw := buildAuxBlock(t, era.blockType, tc)
					block, err := decoder.decode(era.blockType, raw)
					if tc.wantErr != "" {
						require.ErrorContains(t, err, tc.wantErr)
						return
					}
					require.NoError(t, err)
					require.Len(t, block.Transactions(), tc.txCount)
				})
			}
		}
	}
}
