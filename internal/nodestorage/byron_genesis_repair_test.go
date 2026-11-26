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

package nodestorage_test

import (
	"database/sql"
	"encoding/hex"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/nodestorage"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// legacyGenesis is the preview genesis with a zero-coin Shelley initialFunds
// output added: the seven zero-coin Byron outputs a released node stored, a
// funded Byron output and the Shelley output, which the reference keeps.
type legacyGenesis struct {
	nodeCfg     *cardano.CardanoNodeConfig
	zeroByron   []lcommon.Utxo
	fundedByron lcommon.Utxo
	zeroShelley lcommon.Utxo
}

func newLegacyGenesis(t *testing.T) legacyGenesis {
	t.Helper()
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	shelleyGenesis := nodeCfg.ShelleyGenesis()
	if shelleyGenesis.InitialFunds == nil {
		shelleyGenesis.InitialFunds = map[string]uint64{}
	}
	shelleyAddr := make([]byte, 57)
	for i := range shelleyAddr[1:] {
		shelleyAddr[i+1] = 0x77
	}
	shelleyGenesis.InitialFunds[hex.EncodeToString(shelleyAddr)] = 0

	g := legacyGenesis{nodeCfg: nodeCfg}
	byronUtxos, err := nodeCfg.ByronGenesis().GenesisUtxos()
	require.NoError(t, err)
	for _, u := range byronUtxos {
		switch {
		case u.Output.Amount().Sign() == 0:
			g.zeroByron = append(g.zeroByron, u)
		case g.fundedByron.Output == nil:
			g.fundedByron = u
		}
	}
	require.Len(t, g.zeroByron, 7)
	require.NotNil(t, g.fundedByron.Output)
	shelleyUtxos, err := shelleyGenesis.GenesisUtxos()
	require.NoError(t, err)
	for _, u := range shelleyUtxos {
		if u.Output.Amount().Sign() == 0 {
			g.zeroShelley = u
		}
	}
	require.NotNil(t, g.zeroShelley.Output)
	return g
}

func (g legacyGenesis) all() []lcommon.Utxo {
	return append(
		append([]lcommon.Utxo{}, g.zeroByron...),
		g.fundedByron, g.zeroShelley,
	)
}

// withCbor returns u with its output's CBOR set, as the genesis seed does.
func withCbor(t *testing.T, u lcommon.Utxo) (lcommon.Utxo, []byte) {
	t.Helper()
	data, err := cbor.Encode(u.Output)
	require.NoError(t, err)
	switch o := u.Output.(type) {
	case byron.ByronTransactionOutput:
		(&o).SetCbor(data)
		u.Output = o
	case shelley.ShelleyTransactionOutput:
		(&o).SetCbor(data)
		u.Output = o
	default:
		t.Fatalf("unsupported genesis output %T", u.Output)
	}
	return u, data
}

// seedLegacyStore stores each output of utxos as its own genesis transaction,
// the way a node did before it dropped zero-coin Byron outputs.
func seedLegacyStore(
	t *testing.T,
	db *database.Database,
	nodeCfg *cardano.CardanoNodeConfig,
	utxos []lcommon.Utxo,
) {
	t.Helper()
	genesisHash, err := ledger.GenesisBlockHash(nodeCfg)
	require.NoError(t, err)
	for _, u := range utxos {
		u, _ = withCbor(t, u)
		id := u.Id.Id()
		ref := database.UtxoRef{TxId: id, OutputIdx: u.Id.Index()}
		require.NoError(t, db.SetGenesisTransaction(
			t.Context(), id.Bytes(), genesisHash[:], []lcommon.Utxo{u},
			map[database.UtxoRef]database.CborOffset{ref: {}}, nil,
		))
	}
}

// seedLiveSet writes utxos into the live UTxO set at the origin point, the
// state of a set built before the first block.
func seedLiveSet(
	t *testing.T,
	set *utxoset.Set,
	nodeCfg *cardano.CardanoNodeConfig,
	utxos []lcommon.Utxo,
) {
	t.Helper()
	genesisHash, err := ledger.GenesisBlockHash(nodeCfg)
	require.NoError(t, err)
	evt := ledger.UtxoEvent{
		Point: ocommon.Point{Slot: 0, Hash: genesisHash[:]},
		Cause: ledger.UtxoChangeGenesis,
	}
	for _, u := range utxos {
		_, data := withCbor(t, u)
		id := u.Id.Id()
		evt.Added = append(evt.Added, ledger.UtxoOutput{
			Ref:  models.UtxoId{Hash: id.Bytes(), Idx: u.Id.Index()},
			Cbor: data,
		})
	}
	tip := ocommon.Point{}
	require.NoError(t, set.ApplyUtxoChanges(&tip, []ledger.UtxoEvent{evt}))
}

func newLegacyLiveSet(
	t *testing.T,
	g legacyGenesis,
) (*utxoset.Set, string) {
	t.Helper()
	dir := t.TempDir()
	set, _, err := nodestorage.OpenUtxoSet(nodestorage.UtxoSetConfig{
		DataDir:           dir,
		CardanoNodeConfig: g.nodeCfg,
		CheckpointKeep:    1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, set.Close()) })
	return set, dir
}

func liveSetHolds(t *testing.T, set *utxoset.Set, u lcommon.Utxo) bool {
	t.Helper()
	id := u.Id.Id()
	_, ok, err := set.Get(id.Bytes(), u.Id.Index())
	require.NoError(t, err)
	return ok
}

func metadataRows(t *testing.T, raw *sql.DB, u lcommon.Utxo) (utxos, txs int) {
	t.Helper()
	id := u.Id.Id()
	require.NoError(t, raw.QueryRow(`
SELECT (SELECT COUNT(*) FROM utxo x JOIN "transaction" t ON t.id = x.tx_id
        WHERE t.hash = ?),
       (SELECT COUNT(*) FROM "transaction" WHERE hash = ?)`,
		id.Bytes(), id.Bytes()).Scan(&utxos, &txs))
	return utxos, txs
}

// runReconcile moves db, a v0.94.2 database, into set, then reconciles set
// with a ledger database as the node does once the move is done.
func runReconcile(
	t *testing.T,
	db *database.Database,
	set *utxoset.Set,
	dir string,
	nodeCfg *cardano.CardanoNodeConfig,
) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	require.NoError(t, nodestorage.ReleasedMigration{
		UtxoSet: nodestorage.UtxoSetConfig{
			DataDir:           dir,
			CardanoNodeConfig: nodeCfg,
		},
		Logger: logger,
	}.Move(t.Context(), db, set))
	ledgerDB, err := dbtest.NewDatabase(
		t, &database.Config{DataDir: t.TempDir()},
	)
	require.NoError(t, err)
	require.NoError(t, nodestorage.Reconcile{
		DB:                ledgerDB,
		Set:               set,
		Logger:            logger,
		CardanoNodeConfig: nodeCfg,
	}.Run(t.Context()))
}

// TestReconcileRemovesLegacyZeroValueByronGenesis starts over a store a
// released node wrote, holding the zero-coin Byron genesis outputs in both the
// metadata store and the live UTxO set, and requires exactly those outputs,
// with their genesis transactions, to be gone from both while the funded
// Byron output and the zero-coin Shelley output stay.
func TestReconcileRemovesLegacyZeroValueByronGenesis(t *testing.T) {
	t.Parallel()
	g := newLegacyGenesis(t)
	db, err := dbtest.NewReleasedDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	seedLegacyStore(t, db, g.nodeCfg, g.all())
	set, dir := newLegacyLiveSet(t, g)
	seedLiveSet(t, set, g.nodeCfg, g.all())

	runReconcile(t, db, set, dir, g.nodeCfg)

	for _, u := range g.zeroByron {
		utxos, txs := metadataRows(t, raw, u)
		require.Zero(t, utxos, "metadata output %s", u.Id.Id())
		require.Zero(t, txs, "metadata transaction %s", u.Id.Id())
		require.False(t, liveSetHolds(t, set, u),
			"live set output %s", u.Id.Id())
	}
	for _, u := range []lcommon.Utxo{g.fundedByron, g.zeroShelley} {
		utxos, txs := metadataRows(t, raw, u)
		require.Equal(t, 1, utxos)
		require.Equal(t, 1, txs)
		require.True(t, liveSetHolds(t, set, u))
	}
	count, err := set.Count()
	require.NoError(t, err)
	require.Equal(t, uint64(2), count)
	marker, err := db.GetSyncState(ledger.ByronGenesisZeroRepairSyncKey, nil)
	require.NoError(t, err)
	require.NotEmpty(t, marker)
}

// TestReconcileResumesInterruptedGenesisRepair leaves the metadata store
// repaired and its marker set but the live UTxO set untouched, the state a
// stop between the two removals leaves, and requires the next start to finish
// the live set, and a third start to change nothing.
func TestReconcileResumesInterruptedGenesisRepair(t *testing.T) {
	t.Parallel()
	g := newLegacyGenesis(t)
	db, err := dbtest.NewReleasedDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	seedLegacyStore(t, db, g.nodeCfg, g.all())
	zero, err := ledger.ZeroValueByronGenesisOutputs(g.nodeCfg)
	require.NoError(t, err)
	require.NoError(t, ledger.RepairZeroValueByronGenesis(
		t.Context(), db, zero, nil, nil,
	))
	set, dir := newLegacyLiveSet(t, g)
	seedLiveSet(t, set, g.nodeCfg, g.all())
	for _, u := range g.zeroByron {
		require.True(t, liveSetHolds(t, set, u))
	}

	runReconcile(t, db, set, dir, g.nodeCfg)

	for _, u := range g.zeroByron {
		require.False(t, liveSetHolds(t, set, u))
	}
	count, err := set.Count()
	require.NoError(t, err)
	require.Equal(t, uint64(2), count)

	runReconcile(t, db, set, dir, g.nodeCfg)
	count, err = set.Count()
	require.NoError(t, err)
	require.Equal(t, uint64(2), count)
}

// emptyLiveSource stands for the live UTxO set a store without utxo rows
// reads; the genesis rows this test writes never query it.
type emptyLiveSource struct{}

func (emptyLiveSource) LiveUtxoCbor(
	types.Txn, []byte, uint32,
) ([]byte, bool, error) {
	return nil, false, nil
}

func (emptyLiveSource) LiveUtxoStake(
	types.Txn, uint8, []byte,
) (uint64, error) {
	return 0, nil
}

func (emptyLiveSource) LivePointerStakes(
	types.Txn,
) ([]types.PointerStake, error) {
	return nil, nil
}

func (emptyLiveSource) EachLiveUtxoStake(
	types.Txn, func(uint8, []byte, uint64) error,
) error {
	return nil
}

// TestReconcileRemovesGenesisFromAStoreWithoutUtxoRows repairs a store that
// keeps its UTxO set only in the live set: the metadata store has the genesis
// transaction rows but no utxo rows, so the transactions go on the genesis
// configuration alone.
func TestReconcileRemovesGenesisFromAStoreWithoutUtxoRows(t *testing.T) {
	t.Parallel()
	g := newLegacyGenesis(t)
	db, err := dbtest.NewReleasedDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	// A released store whose outputs already moved to the live set: the
	// transaction rows stay behind without their utxo rows. A store with a
	// live source writes neither, so the rows are seeded before it is set.
	seedLegacyStore(t, db, g.nodeCfg, g.all())
	_, err = raw.ExecContext(t.Context(), `DELETE FROM utxo`)
	require.NoError(t, err)
	require.NoError(t, db.SetLiveUtxoSource(emptyLiveSource{}))
	for _, u := range g.zeroByron {
		utxos, txs := metadataRows(t, raw, u)
		require.Zero(t, utxos, "the store keeps no utxo rows")
		require.Equal(t, 1, txs)
	}
	set, dir := newLegacyLiveSet(t, g)
	seedLiveSet(t, set, g.nodeCfg, g.all())

	runReconcile(t, db, set, dir, g.nodeCfg)

	for _, u := range g.zeroByron {
		_, txs := metadataRows(t, raw, u)
		require.Zero(t, txs, "transaction %s", u.Id.Id())
		require.False(t, liveSetHolds(t, set, u))
	}
	for _, u := range []lcommon.Utxo{g.fundedByron, g.zeroShelley} {
		_, txs := metadataRows(t, raw, u)
		require.Equal(t, 1, txs)
		require.True(t, liveSetHolds(t, set, u))
	}
}

// TestReconcileKeepsOutputAtSharedZeroByronReference adds a Shelley
// initialFunds entry at the address of a zero-coin Byron genesis output, so
// both have the same reference, seeds the Shelley output in the metadata store
// and the live UTxO set, and requires Reconcile to keep it in both while the
// other zero-coin Byron outputs go. Byron genesis rejects two outputs at one
// reference, so a retained Byron output cannot share one.
func TestReconcileKeepsOutputAtSharedZeroByronReference(t *testing.T) {
	t.Parallel()
	for name, coin := range map[string]uint64{"funded": 7, "zero-coin": 0} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			g := newLegacyGenesis(t)
			addrBytes, err := g.zeroByron[0].Output.Address().Bytes()
			require.NoError(t, err)
			g.nodeCfg.ShelleyGenesis().InitialFunds[hex.EncodeToString(addrBytes)] = coin
			shelleyUtxos, err := g.nodeCfg.ShelleyGenesis().GenesisUtxos()
			require.NoError(t, err)
			var shared lcommon.Utxo
			for _, u := range shelleyUtxos {
				if u.Id.Id() == g.zeroByron[0].Id.Id() {
					shared = u
				}
			}
			require.NotNil(t, shared.Output)
			require.Equal(t, g.zeroByron[0].Id.Index(), shared.Id.Index())
			seeded := append(
				append([]lcommon.Utxo{}, g.zeroByron[1:]...),
				g.fundedByron, g.zeroShelley, shared,
			)
			db, err := dbtest.NewReleasedDatabase(
				t, &database.Config{DataDir: t.TempDir()},
			)
			require.NoError(t, err)
			raw, err := dbtest.RawSQLiteMetadata(t, db)
			require.NoError(t, err)
			seedLegacyStore(t, db, g.nodeCfg, seeded)
			set, dir := newLegacyLiveSet(t, g)
			seedLiveSet(t, set, g.nodeCfg, seeded)

			runReconcile(t, db, set, dir, g.nodeCfg)

			for _, u := range g.zeroByron[1:] {
				utxos, txs := metadataRows(t, raw, u)
				require.Zero(t, utxos)
				require.Zero(t, txs)
				require.False(t, liveSetHolds(t, set, u))
			}
			for _, u := range []lcommon.Utxo{
				shared, g.fundedByron, g.zeroShelley,
			} {
				utxos, txs := metadataRows(t, raw, u)
				require.Equal(t, 1, utxos, "metadata output %s", u.Id.Id())
				require.Equal(t, 1, txs)
				require.True(t, liveSetHolds(t, set, u),
					"live set output %s", u.Id.Id())
			}
			count, err := set.Count()
			require.NoError(t, err)
			require.Equal(t, uint64(3), count)
		})
	}
}

// TestRemoveZeroCoinOutputsKeepsOutputWithCoin pins that the live-set removal
// checks the coin of each output it is given: a funded output named with a
// zero-coin one stays.
func TestRemoveZeroCoinOutputsKeepsOutputWithCoin(t *testing.T) {
	t.Parallel()
	g := newLegacyGenesis(t)
	set, _ := newLegacyLiveSet(t, g)
	seedLiveSet(t, set, g.nodeCfg, g.all())
	refs := make([]models.UtxoId, 0, 2)
	for _, u := range []lcommon.Utxo{g.fundedByron, g.zeroByron[0]} {
		refs = append(refs, models.UtxoId{
			Hash: u.Id.Id().Bytes(), Idx: u.Id.Index(),
		})
	}

	removed, err := set.RemoveZeroCoinOutputs(refs)
	require.NoError(t, err)

	require.Equal(t, 1, removed)
	require.True(t, liveSetHolds(t, set, g.fundedByron))
	require.False(t, liveSetHolds(t, set, g.zeroByron[0]))
}
