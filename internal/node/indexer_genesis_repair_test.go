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
	"context"
	"encoding/hex"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/internal/indexer"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// TestIndexerServiceRemovesLegacyZeroValueByronGenesis opens an IndexerDB a
// released node seeded with the preview genesis, zero-coin Byron outputs
// included, and requires building the writer, which runs before the writer
// replays toward a Mithril anchor, to remove exactly those outputs with their
// address rows and genesis transactions.
func TestIndexerServiceRemovesLegacyZeroValueByronGenesis(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	consensus, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(), Network: "preview",
	})
	require.NoError(t, err)
	indexerDB, err := dbtest.NewIndexerDatabase(
		t, consensus, dbtest.IndexerOptions{},
	)
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, indexerDB)
	require.NoError(t, err)

	genesisHash, err := ledger.GenesisBlockHash(nodeCfg)
	require.NoError(t, err)
	byronUtxos, err := nodeCfg.ByronGenesis().GenesisUtxos()
	require.NoError(t, err)
	var zero, funded []lcommon.Utxo
	for _, u := range byronUtxos {
		if u.Output.Amount().Sign() == 0 {
			zero = append(zero, u)
		} else {
			funded = append(funded, u)
		}
	}
	require.Len(t, zero, 7)
	require.NotEmpty(t, funded)
	for _, u := range append(append([]lcommon.Utxo{}, zero...), funded...) {
		data, err := cbor.Encode(u.Output)
		require.NoError(t, err)
		output := u.Output.(byron.ByronTransactionOutput)
		(&output).SetCbor(data)
		id := u.Id.Id()
		ref := database.UtxoRef{TxId: id, OutputIdx: u.Id.Index()}
		require.NoError(t, indexerDB.SetGenesisTransaction(
			t.Context(), id.Bytes(), genesisHash[:],
			[]lcommon.Utxo{{Id: u.Id, Output: output}},
			map[database.UtxoRef]database.CborOffset{ref: {}}, nil,
		))
	}
	var addresses int
	require.NoError(t, raw.QueryRow(
		`SELECT COUNT(*) FROM utxo_address`).Scan(&addresses))
	require.Equal(
		t,
		7+len(funded),
		addresses,
		"the API store indexes every output",
	)

	_, err = NewIndexerService(dingo.IndexerWriterDeps{
		Consensus: consensus, Indexer: indexerDB, NodeConfig: nodeCfg,
		Logger: logger, MaxReadyLagBlocks: 10,
	})
	require.NoError(t, err)

	var utxos, transactions int
	require.NoError(t, raw.QueryRow(
		`SELECT COUNT(*) FROM utxo`).Scan(&utxos))
	require.NoError(t, raw.QueryRow(
		`SELECT COUNT(*) FROM utxo_address`).Scan(&addresses))
	require.NoError(t, raw.QueryRow(
		`SELECT COUNT(*) FROM "transaction"`).Scan(&transactions))
	require.Equal(t, len(funded), utxos, "only the funded outputs stay")
	require.Equal(t, len(funded), addresses)
	require.Equal(t, len(funded), transactions)
	marker, err := indexerDB.GetSyncState(
		ledger.ByronGenesisZeroRepairSyncKey, nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, marker)
}

// TestIndexerServiceRepairLetsALegacyReplayPassTheAnchor stops a Mithril
// bootstrap replay after its genesis seed, adds what a released seed also
// wrote there (the zero-coin Byron genesis outputs, their genesis
// transactions, and those transactions in the replay's count), and requires
// the IndexerDB built by the service to pass the anchor check: the repair
// removes the transactions' application rows, so the count the check compares
// them with has to lose them too.
func TestIndexerServiceRepairLetsALegacyReplayPassTheAnchor(t *testing.T) {
	t.Parallel()
	f := newAnchorFixture(t)
	ctx := context.Background()

	stopCtx, stop := context.WithCancel(ctx)
	defer stop()
	src := &cancelingSource{
		ChainSource: f.cappedAtAnchor(), cancel: stop, afterCalls: 3,
	}
	w, store := f.writer(t, src, 7)
	require.Error(t, w.CatchUp(stopCtx))
	phase, err := store.Bootstrap(nil)
	require.NoError(t, err)
	require.Equal(t, indexerdb.PhaseReplay, phase)

	nodeCfg := f.deps.NodeConfig
	genesisHash, err := ledger.GenesisBlockHash(nodeCfg)
	require.NoError(t, err)
	byronUtxos, err := nodeCfg.ByronGenesis().GenesisUtxos()
	require.NoError(t, err)
	var zero []lcommon.Utxo
	for _, u := range byronUtxos {
		if u.Output.Amount().Sign() == 0 {
			zero = append(zero, u)
		}
	}
	require.Len(t, zero, 7)
	txn := f.indexerDB.Transaction(ctx, true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		for _, u := range zero {
			data, err := cbor.Encode(u.Output)
			if err != nil {
				return err
			}
			output := u.Output.(byron.ByronTransactionOutput)
			(&output).SetCbor(data)
			id := u.Id.Id()
			ref := database.UtxoRef{TxId: id, OutputIdx: u.Id.Index()}
			if err := f.indexerDB.SetGenesisTransaction(
				ctx, id.Bytes(), genesisHash[:],
				[]lcommon.Utxo{{Id: u.Id, Output: output}},
				map[database.UtxoRef]database.CborOffset{ref: {}}, txn,
			); err != nil {
				return err
			}
		}
		return store.AddReplayTransactions(uint64(len(zero)), txn)
	}))

	_, err = NewIndexerService(f.deps)
	require.NoError(t, err)
	w, store = f.writer(t, f.cappedAtAnchor(), indexer.DefaultBatchBlocks)
	require.NoError(t, w.CatchUp(ctx))
	phase, err = store.Bootstrap(nil)
	require.NoError(t, err)
	require.Equal(t, indexerdb.PhaseDone, phase)
}

// TestIndexerServiceKeepsOutputAtSharedZeroByronReference adds a zero-coin
// Shelley initialFunds entry, which the reference keeps, at the address of a
// zero-coin Byron genesis output, so both have the same reference, seeds the
// Shelley output in the IndexerDB, and requires building the writer to keep it
// and its transaction.
func TestIndexerServiceKeepsOutputAtSharedZeroByronReference(t *testing.T) {
	t.Parallel()
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	byronUtxos, err := nodeCfg.ByronGenesis().GenesisUtxos()
	require.NoError(t, err)
	var zeroByron lcommon.Utxo
	for _, u := range byronUtxos {
		if u.Output.Amount().Sign() == 0 {
			zeroByron = u
			break
		}
	}
	require.NotNil(t, zeroByron.Output)
	addrBytes, err := zeroByron.Output.Address().Bytes()
	require.NoError(t, err)
	shelleyGenesis := nodeCfg.ShelleyGenesis()
	if shelleyGenesis.InitialFunds == nil {
		shelleyGenesis.InitialFunds = map[string]uint64{}
	}
	shelleyGenesis.InitialFunds[hex.EncodeToString(addrBytes)] = 0
	shelleyUtxos, err := shelleyGenesis.GenesisUtxos()
	require.NoError(t, err)
	var shared lcommon.Utxo
	for _, u := range shelleyUtxos {
		if u.Id.Id() == zeroByron.Id.Id() {
			shared = u
		}
	}
	require.NotNil(t, shared.Output)

	consensus, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(), Network: "preview",
	})
	require.NoError(t, err)
	indexerDB, err := dbtest.NewIndexerDatabase(
		t, consensus, dbtest.IndexerOptions{},
	)
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, indexerDB)
	require.NoError(t, err)
	genesisHash, err := ledger.GenesisBlockHash(nodeCfg)
	require.NoError(t, err)
	data, err := cbor.Encode(shared.Output)
	require.NoError(t, err)
	output := shared.Output.(shelley.ShelleyTransactionOutput)
	(&output).SetCbor(data)
	id := shared.Id.Id()
	ref := database.UtxoRef{TxId: id, OutputIdx: shared.Id.Index()}
	require.NoError(t, indexerDB.SetGenesisTransaction(
		t.Context(), id.Bytes(), genesisHash[:],
		[]lcommon.Utxo{{Id: shared.Id, Output: output}},
		map[database.UtxoRef]database.CborOffset{ref: {}}, nil,
	))

	_, err = NewIndexerService(dingo.IndexerWriterDeps{
		Consensus: consensus, Indexer: indexerDB, NodeConfig: nodeCfg,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxReadyLagBlocks: 10,
	})
	require.NoError(t, err)

	var utxos, transactions int
	require.NoError(t, raw.QueryRow(`
SELECT COUNT(*) FROM utxo x JOIN "transaction" t ON t.id = x.tx_id
WHERE t.hash = ? AND x.output_idx = ?`,
		id.Bytes(), shared.Id.Index()).Scan(&utxos))
	require.NoError(t, raw.QueryRow(
		`SELECT COUNT(*) FROM "transaction" WHERE hash = ?`,
		id.Bytes()).Scan(&transactions))
	require.Equal(t, 1, utxos, "the Shelley output stays")
	require.Equal(t, 1, transactions, "and so does its transaction")
}
