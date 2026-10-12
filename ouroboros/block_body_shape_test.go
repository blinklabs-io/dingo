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
	"github.com/stretchr/testify/require"
)

type blockDecoder struct {
	name   string
	decode func(blockType uint, raw []byte) (gledger.Block, error)
}

// shapeBlockDecoders lists every entry that turns block bytes into a block:
// live blockfetch (standard and Musashi networks), the block-decode pipeline
// stage, and the stored-block decoder replay and backfill use. The same bytes
// must get the same verdict from all of them.
func shapeBlockDecoders() []blockDecoder {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return []blockDecoder{
		{
			name: "blockfetch",
			decode: newOuroboros(OuroborosConfig{
				Logger:       logger,
				NetworkMagic: ouroboros.NetworkMainnet.NetworkMagic,
			}).decodeBlockfetchBlock,
		},
		{
			name: "musashi blockfetch",
			decode: newOuroboros(OuroborosConfig{
				Logger:       logger,
				NetworkMagic: ouroboros.NetworkCardanoMusashi.NetworkMagic,
			}).decodeBlockfetchBlock,
		},
		{
			name: "pipeline decode stage",
			decode: func(blockType uint, raw []byte) (gledger.Block, error) {
				return gledger.NewBlockFromCbor(blockType, raw)
			},
		},
		{
			name: "stored block",
			decode: func(blockType uint, raw []byte) (gledger.Block, error) {
				return models.DecodeBlockCbor(blockType, raw)
			},
		},
	}
}

func requireBlockShapeOutcome(
	t *testing.T,
	decode func(uint, []byte) (gledger.Block, error),
	blockType uint,
	raw []byte,
	wantErr string,
) {
	t.Helper()
	block, err := decode(blockType, raw)
	if wantErr == "" {
		require.NoError(t, err)
		require.Len(t, block.Transactions(), 1)
		return
	}
	require.ErrorContains(t, err, wantErr)
	require.Nil(t, block, "a block that fails to decode never reaches ledger application")
}

func TestConwayBlockBodyShapesRejectedByEveryDecoder(t *testing.T) {
	t.Parallel()
	for _, tc := range testutil.ConwayBodyShapeCases(t) {
		for _, isValid := range []bool{true, false} {
			name := tc.Name + "/isValid"
			if !isValid {
				name = tc.Name + "/isInvalid"
			}
			raw := testutil.BuildConwayBlockBytesWithTx(
				t, testutil.ShapeTxBody(tc.Extra), isValid,
			)
			for _, decoder := range shapeBlockDecoders() {
				t.Run(name+"/"+decoder.name, func(t *testing.T) {
					t.Parallel()
					requireBlockShapeOutcome(
						t, decoder.decode, gledger.BlockTypeConway, raw, tc.WantErr,
					)
				})
			}
		}
	}
}

func TestDijkstraBlockBodyShapesRejectedByEveryDecoder(t *testing.T) {
	t.Parallel()
	for _, tc := range testutil.DijkstraBodyShapeCases(t) {
		for _, isValid := range []bool{true, false} {
			name := tc.Name + "/isValid"
			if !isValid {
				name = tc.Name + "/isInvalid"
			}
			raw := testutil.BuildDijkstraBlockBytesWithTx(
				t,
				testutil.DijkstraShapeTxBytes(
					t, testutil.ShapeTxBody(tc.Extra), isValid,
				),
			)
			for _, decoder := range shapeBlockDecoders() {
				t.Run(name+"/"+decoder.name, func(t *testing.T) {
					t.Parallel()
					requireBlockShapeOutcome(
						t, decoder.decode, gledger.BlockTypeDijkstra, raw, tc.WantErr,
					)
				})
			}
		}
	}
}
