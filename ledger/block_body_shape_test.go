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
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/pipeline"
	"github.com/stretchr/testify/require"
)

// requireReadChainBatchShapeOutcome drives stored block bytes through the
// decode the ledger runs before it applies a batch, in both the serial and the
// pipelined mode. A body that violates a wire-level constraint fails the whole
// batch with an error naming the constraint, so no block of that batch,
// including the well-formed one ahead of it, is handed to application. A legal
// neighbour decodes.
func requireReadChainBatchShapeOutcome(
	t *testing.T,
	blockType uint,
	raw []byte,
	wantErr string,
	pipelined bool,
) {
	t.Helper()
	ctx := t.Context()
	good, _ := buildDecodableTestBlock(t, 10, 1)
	shaped := models.Block{
		Slot:   20,
		Number: 2,
		Hash:   []byte("shape-hash-shape-hash-shape-32by"),
		Type:   blockType,
		Cbor:   raw,
	}
	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	if pipelined {
		ls.blockPipeline = pipeline.NewBlockPipeline(
			pipeline.WithDecodeWorkers(2),
		)
		require.NoError(t, ls.blockPipeline.Start(ctx))
		defer func() {
			require.NoError(t, ls.blockPipeline.Stop())
		}()
	}
	decoded, err := ls.decodeReadChainBatchWithError(
		ctx, []models.Block{good, shaped},
	)
	if wantErr == "" {
		require.NoError(t, err)
		require.Len(t, decoded, 2)
		return
	}
	require.ErrorContains(t, err, wantErr)
	require.Empty(t, decoded)
}

func TestDecodeReadChainBatchBodyShapes(t *testing.T) {
	t.Parallel()
	type era struct {
		name      string
		blockType uint
		cases     []testutil.BodyShapeCase
		build     func(*testing.T, map[uint]any, bool) []byte
	}
	for _, e := range []era{
		{
			"conway",
			ledger.BlockTypeConway,
			testutil.ConwayBodyShapeCases(t),
			testutil.BuildConwayBlockBytesWithTx,
		},
		{
			"dijkstra",
			ledger.BlockTypeDijkstra,
			testutil.DijkstraBodyShapeCases(t),
			func(t *testing.T, body map[uint]any, isValid bool) []byte {
				return testutil.BuildDijkstraBlockBytesWithTx(
					t, testutil.DijkstraShapeTxBytes(t, body, isValid),
				)
			},
		},
	} {
		for _, tc := range e.cases {
			for _, isValid := range []bool{true, false} {
				validity := "isValid"
				if !isValid {
					validity = "isInvalid"
				}
				raw := e.build(t, testutil.ShapeTxBody(tc.Extra), isValid)
				for _, pipelined := range []bool{false, true} {
					mode := "serial"
					if pipelined {
						mode = "pipeline"
					}
					t.Run(e.name+"/"+tc.Name+"/"+validity+"/"+mode, func(t *testing.T) {
						t.Parallel()
						requireReadChainBatchShapeOutcome(
							t, e.blockType, raw, tc.WantErr, pipelined,
						)
					})
				}
			}
		}
	}
}
