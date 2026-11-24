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

package node

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/indexer"
	"github.com/blinklabs-io/dingo/internal/test/leiosclosurefixture"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// requireClosurePayloads checks that the datum, script and metadata of the
// closure's first transaction read back byte for byte from the IndexerDB, and
// that their columns name the endorser block's transaction list.
func requireClosurePayloads(
	t *testing.T,
	idx *database.Database,
	scenario *leiosclosurefixture.Scenario,
) {
	t.Helper()
	ctx := context.Background()
	first := scenario.Txs[0]
	datum, err := idx.GetDatum(
		ctx, lcommon.Blake2b256Hash(leiosclosurefixture.PayloadDatum).Bytes(),
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, leiosclosurefixture.PayloadDatum, datum.RawDatum)

	scripts := first.Witnesses().PlutusV2Scripts()
	require.Len(t, scripts, 1)
	script, err := idx.GetScript(ctx, scripts[0].Hash().Bytes(), nil)
	require.NoError(t, err)
	require.Equal(t, scripts[0].RawScriptBytes(), script.Content)

	metadata, err := idx.GetTransactionMetadataByHash(
		ctx, first.Hash().Bytes(), nil,
	)
	require.NoError(t, err)
	require.Equal(t, first.Metadata().Cbor(), metadata)
	require.NotEmpty(t, metadata)
}

// The datum, script and metadata of a certified closure's transaction are in
// no chain block: the writer names them by their position in the endorser
// block's transaction list, and every reader returns the bytes.
func TestIndexBlocksStoresClosurePayloadsAsEndorserOffsets(t *testing.T) {
	t.Parallel()
	scenario, err := leiosclosurefixture.NewWithPayloads()
	require.NoError(t, err)
	bf, idx, raw := closureTestBackfill(t, scenario, false)
	require.NoError(t, idx.Transaction(context.Background(), true).Do(
		func(txn *database.Txn) error {
			return bf.IndexBlocks(context.Background(), txn, []models.Block{
				leiosclosurefixture.Model(scenario.Announcing),
				leiosclosurefixture.Model(scenario.Certifying),
			})
		}))
	requireClosurePayloads(t, idx, scenario)

	var datumColumn []byte
	require.NoError(t, raw.QueryRow(
		"SELECT raw_datum FROM datum",
	).Scan(&datumColumn))
	offset, err := types.DecodeEndorserOffset(datumColumn)
	require.NoError(t, err, "the datum column names the endorser block")
	require.Equal(t, leiosclosurefixture.AnnouncingSlot, offset.BlockSlot)
	require.Equal(t, scenario.EBHash.Bytes(), offset.BlockHash[:])
	require.True(t, bytes.Contains(
		[]byte(scenario.Raw[0]), leiosclosurefixture.PayloadDatum,
	))
}

// scenarioSource serves the scenario's two ranking blocks as the ledger's
// canonical chain.
type scenarioSource struct {
	blocks []models.Block
}

func (s *scenarioSource) LedgerTip(context.Context) (ochainsync.Tip, error) {
	last := s.blocks[len(s.blocks)-1]
	return ochainsync.Tip{
		Point:       ocommon.NewPoint(last.Slot, last.Hash),
		BlockNumber: last.Number,
	}, nil
}

func (s *scenarioSource) BlocksAfter(
	_ context.Context,
	after indexerdb.Tip,
	throughSlot uint64,
	limit int,
) ([]models.Block, error) {
	var out []models.Block
	for _, block := range s.blocks {
		if !after.IsZero() && block.Number <= after.BlockNumber {
			continue
		}
		if block.Slot > throughSlot || len(out) >= limit {
			break
		}
		out = append(out, block)
	}
	return out, nil
}

func (s *scenarioSource) IsCanonical(
	_ context.Context,
	hash []byte,
) (bool, error) {
	for _, block := range s.blocks {
		if bytes.Equal(block.Hash, hash) {
			return true, nil
		}
	}
	return false, nil
}

func (s *scenarioSource) BlockByHash(
	_ context.Context,
	hash []byte,
) (models.Block, error) {
	for _, block := range s.blocks {
		if bytes.Equal(block.Hash, hash) {
			return block, nil
		}
	}
	return models.Block{}, models.ErrBlockNotFound
}

// The writer indexes a certifying ranking block whose closure carries a datum,
// script and metadata, and its tip moves onto that block.
func TestIndexerWriterContinuesPastACertifyingRankingBlock(t *testing.T) {
	t.Parallel()
	scenario, err := leiosclosurefixture.NewWithPayloads()
	require.NoError(t, err)
	bf, idx, _ := closureTestBackfill(t, scenario, false)
	blocks := []models.Block{
		leiosclosurefixture.Model(scenario.Announcing),
		leiosclosurefixture.Model(scenario.Certifying),
	}
	for i := range blocks {
		blocks[i].ID = uint64(i) + 1
	}
	store, err := indexerdb.NewStore(idx, idx, 10)
	require.NoError(t, err)
	w := indexer.NewWriter(
		store, &scenarioSource{blocks: blocks}, bf,
		slog.New(slog.NewTextHandler(io.Discard, nil)), 10,
	)

	require.NoError(t, w.CatchUp(context.Background()))
	tip, err := store.Tip(nil)
	require.NoError(t, err)
	require.Equal(t, blocks[1].Hash, tip.Hash)
	requireClosurePayloads(t, idx, scenario)
}
