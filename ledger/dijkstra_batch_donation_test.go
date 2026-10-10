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
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// processAndApply runs tx's block through ledgerProcessBlock and applies the
// delta it hands back, as the block pipeline does. With validation on, that
// delta carries only what the per-transaction applications left, the block's
// donations among it.
func (h *stateBatchHarness) processAndApply(
	t *testing.T,
	tx *dijkstra.DijkstraTransaction,
	validate bool,
) error {
	t.Helper()
	block, point, offsets := h.block(t, tx)
	pparams := dijkstraTestProtocolParameters()
	pparams.MaxBlockBodySize = 100_000
	pparams.MaxBlockHeaderSize = 100_000
	h.ls.config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	h.ls.config.SkipDijkstraTxValidation = true
	h.ls.config.CardanoNodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	return h.db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
		delta, err := h.ls.ledgerProcessBlock(
			context.Background(),
			txn, point, block,
			validate, false, false,
			nil, envelopeParent{}, offsets,
			eras.DijkstraEraDesc, pparams, nil,
			0, 0, false,
		)
		if err != nil || delta == nil {
			return err
		}
		defer delta.Release()
		return delta.apply(context.Background(), h.ls, txn)
	})
}

// TestDijkstraBatchDonationsRecordedOncePerLevelAcrossPaths applies a batch
// with child and top-level treasury donations through live block processing
// and replay. Each records every level's donation exactly once, a rollback
// removes the record, and a reapply records it once again.
func TestDijkstraBatchDonationsRecordedOncePerLevelAcrossPaths(t *testing.T) {
	t.Parallel()
	for name, validate := range map[string]bool{"live": true, "replay": false} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newStateBatchHarness(t)
			parent := bytes.Repeat([]byte{0x01}, 32)
			require.NoError(t, h.db.BlockCreate(models.Block{
				Slot: 1,
				Hash: parent,
				Type: gledger.BlockTypeDijkstra,
			}, nil))
			require.NoError(t, h.db.SetBlockNonce(
				parent, 1, bytes.Repeat([]byte{0x02}, 32), true, nil,
			))
			childIn := batchRef{seed: 0xd5}
			topIn := batchRef{seed: 0xd6}
			for _, ref := range []batchRef{childIn, topIn} {
				h.seedUtxo(t, ref, 5_000_000)
			}
			tx := buildStateBatch(t,
				[]batchLevel{
					{
						inputs:   []batchRef{childIn},
						outputs:  []uint64{1_000_000},
						donation: 11,
					},
					{donation: 12},
				},
				batchLevel{
					inputs:   []batchRef{topIn},
					outputs:  []uint64{1_000_000},
					donation: 100,
				},
				false,
			)
			recorded := func() uint64 {
				t.Helper()
				sum, err := h.db.Metadata().SumNetworkDonationsForEpoch(0, nil)
				require.NoError(t, err)
				return sum
			}
			const want = uint64(11 + 12 + 100)

			require.NoError(t, h.processAndApply(t, tx, validate))
			require.Equal(t, want, recorded())

			require.NoError(
				t,
				h.db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
					_, _, err := h.db.TruncateAfterSlot(
						context.Background(), ocommon.Point{Slot: 1, Hash: parent}, 0, txn,
					)
					return err
				}),
			)
			require.Zero(t, recorded(), "rollback drops the block's donations")

			require.NoError(t, h.processAndApply(t, tx, validate))
			require.Equal(
				t,
				want,
				recorded(),
				"reapply records each level once",
			)
		})
	}
}
