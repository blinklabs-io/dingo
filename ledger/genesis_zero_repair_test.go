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
	"database/sql"
	"encoding/hex"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// zeroGenesisFixture is a genesis configuration with two zero-coin Byron
// outputs, one funded Byron output and one zero-coin Shelley output, and the
// outputs of each kind.
type zeroGenesisFixture struct {
	nodeCfg     *cardano.CardanoNodeConfig
	zeroByron   []lcommon.Utxo
	fundedByron lcommon.Utxo
	zeroShelley lcommon.Utxo
}

func newZeroGenesisFixture(t *testing.T) zeroGenesisFixture {
	t.Helper()
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	byronGenesis := nodeCfg.ByronGenesis()
	if byronGenesis.NonAvvmBalances == nil {
		byronGenesis.NonAvvmBalances = map[string]string{}
	}
	addrs := make([]string, 3)
	for i, seed := range []byte{0x5a, 0x5b, 0x5c} {
		addr, err := lcommon.NewByronAddressRedeem(
			bytes.Repeat([]byte{seed}, 32),
			lcommon.ByronAddressAttributes{},
		)
		require.NoError(t, err)
		addrs[i] = addr.String()
	}
	byronGenesis.NonAvvmBalances[addrs[0]] = "0"
	byronGenesis.NonAvvmBalances[addrs[1]] = "0"
	byronGenesis.NonAvvmBalances[addrs[2]] = "1000000"

	shelleyGenesis := nodeCfg.ShelleyGenesis()
	if shelleyGenesis.InitialFunds == nil {
		shelleyGenesis.InitialFunds = map[string]uint64{}
	}
	shelleyAddr := append([]byte{0x00}, bytes.Repeat([]byte{0x77}, 56)...)
	shelleyGenesis.InitialFunds[hex.EncodeToString(shelleyAddr)] = 0

	f := zeroGenesisFixture{nodeCfg: nodeCfg}
	byronUtxos, err := byronGenesis.GenesisUtxos()
	require.NoError(t, err)
	for _, u := range byronUtxos {
		switch u.Output.Address().String() {
		case addrs[0], addrs[1]:
			f.zeroByron = append(f.zeroByron, u)
		case addrs[2]:
			f.fundedByron = u
		}
	}
	require.Len(t, f.zeroByron, 2)
	require.NotNil(t, f.fundedByron.Output)
	shelleyUtxos, err := shelleyGenesis.GenesisUtxos()
	require.NoError(t, err)
	for _, u := range shelleyUtxos {
		if u.Output.Amount().Sign() == 0 {
			f.zeroShelley = u
		}
	}
	require.NotNil(t, f.zeroShelley.Output)
	return f
}

// all returns every output the fixture holds that a released node seeded.
func (f zeroGenesisFixture) all() []lcommon.Utxo {
	return append(
		append([]lcommon.Utxo{}, f.zeroByron...),
		f.fundedByron, f.zeroShelley,
	)
}

// seedUnfilteredGenesis writes each of utxos as its own genesis transaction,
// the way a node did before it dropped zero-coin Byron outputs.
func seedUnfilteredGenesis(
	t *testing.T,
	db *database.Database,
	nodeCfg *cardano.CardanoNodeConfig,
	utxos []lcommon.Utxo,
) {
	t.Helper()
	genesisHash, err := GenesisBlockHash(nodeCfg)
	require.NoError(t, err)
	for _, u := range utxos {
		cborData, err := cbor.Encode(u.Output)
		require.NoError(t, err)
		var output lcommon.TransactionOutput
		switch o := u.Output.(type) {
		case byron.ByronTransactionOutput:
			(&o).SetCbor(cborData)
			output = o
		case shelley.ShelleyTransactionOutput:
			(&o).SetCbor(cborData)
			output = o
		default:
			t.Fatalf("unsupported genesis output %T", u.Output)
		}
		txHash := u.Id.Id()
		ref := database.UtxoRef{TxId: txHash, OutputIdx: u.Id.Index()}
		require.NoError(t, db.SetGenesisTransaction(
			t.Context(),
			txHash.Bytes(),
			genesisHash[:],
			[]lcommon.Utxo{{Id: u.Id, Output: output}},
			map[database.UtxoRef]database.CborOffset{ref: {}},
			nil,
		))
	}
}

func rowCount(t *testing.T, raw *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, raw.QueryRow(query, args...).Scan(&n))
	return n
}

func utxoRows(t *testing.T, raw *sql.DB, u lcommon.Utxo) int {
	t.Helper()
	return rowCount(t, raw, `
SELECT COUNT(*) FROM utxo x JOIN "transaction" t ON t.id = x.tx_id
WHERE t.hash = ? AND x.output_idx = ?`, u.Id.Id().Bytes(), u.Id.Index())
}

func txRows(t *testing.T, raw *sql.DB, u lcommon.Utxo) int {
	t.Helper()
	return rowCount(t, raw, `
SELECT (SELECT COUNT(*) FROM "transaction" WHERE hash = ?)
     + (SELECT COUNT(*) FROM applied_transaction a
        JOIN "transaction" t ON t.id = a.tx_id WHERE t.hash = ?)`,
		u.Id.Id().Bytes(), u.Id.Id().Bytes())
}

func newSeededGenesisStore(
	t *testing.T,
	mode string,
	f zeroGenesisFixture,
) (*database.Database, *sql.DB) {
	t.Helper()
	db, err := dbtest.NewReleasedDatabase(t, &database.Config{
		DataDir:     t.TempDir(),
		StorageMode: mode,
	})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	seedUnfilteredGenesis(t, db, f.nodeCfg, f.all())
	return db, raw
}

func requireRepairKeepsKeptOutputs(
	t *testing.T,
	raw *sql.DB,
	f zeroGenesisFixture,
) {
	t.Helper()
	for _, u := range f.zeroByron {
		require.Zero(t, utxoRows(t, raw, u),
			"zero-coin Byron output %s is removed", u.Id.Id())
		require.Zero(t, txRows(t, raw, u),
			"its genesis transaction rows are removed")
	}
	for name, u := range map[string]lcommon.Utxo{
		"funded Byron":      f.fundedByron,
		"zero-coin Shelley": f.zeroShelley,
	} {
		require.Equal(t, 1, utxoRows(t, raw, u), "%s output is kept", name)
		require.Equal(t, 2, txRows(t, raw, u),
			"%s genesis transaction rows are kept", name)
	}
}

func repairFixture(
	t *testing.T,
	db *database.Database,
	f zeroGenesisFixture,
) {
	t.Helper()
	zero, err := ZeroValueByronGenesisOutputs(f.nodeCfg)
	require.NoError(t, err)
	require.NoError(t, RepairZeroValueByronGenesis(
		t.Context(), db, zero, slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
	))
}

// TestRepairZeroValueByronGenesisCoreStore repairs a store shaped like one a
// released node wrote in core storage mode: it holds the genesis outputs and
// transactions but no utxo_address rows, so nothing about an output's address
// can identify it as Byron.
func TestRepairZeroValueByronGenesisCoreStore(t *testing.T) {
	t.Parallel()
	f := newZeroGenesisFixture(t)
	db, raw := newSeededGenesisStore(t, types.StorageModeCore, f)
	require.Zero(t, rowCount(t, raw, `SELECT COUNT(*) FROM utxo_address`),
		"a core store keeps no utxo_address rows")
	require.Equal(t, 4, rowCount(t, raw, `SELECT COUNT(*) FROM utxo`))

	repairFixture(t, db, f)

	requireRepairKeepsKeptOutputs(t, raw, f)
	require.Equal(t, 2, rowCount(t, raw, `SELECT COUNT(*) FROM utxo`))
	marker, err := db.GetSyncState(ByronGenesisZeroRepairSyncKey, nil)
	require.NoError(t, err)
	require.NotEmpty(t, marker, "the marker is set once the repair finished")
}

// TestRepairZeroValueByronGenesisAPIStore repairs an API storage mode store,
// whose outputs also have utxo_address rows.
func TestRepairZeroValueByronGenesisAPIStore(t *testing.T) {
	t.Parallel()
	f := newZeroGenesisFixture(t)
	db, raw := newSeededGenesisStore(t, types.StorageModeAPI, f)
	require.Equal(t, 4, rowCount(t, raw, `SELECT COUNT(*) FROM utxo_address`))

	repairFixture(t, db, f)

	requireRepairKeepsKeptOutputs(t, raw, f)
	require.Equal(t, 2, rowCount(t, raw, `SELECT COUNT(*) FROM utxo`))
	require.Equal(t, 2, rowCount(t, raw, `SELECT COUNT(*) FROM utxo_address`),
		"the removed outputs' address rows go with them")
}

// TestRepairZeroValueByronGenesisLeavesSpentOutput pins that only an unspent
// zero-coin Byron output is removed: a custom genesis can spend one.
func TestRepairZeroValueByronGenesisLeavesSpentOutput(t *testing.T) {
	t.Parallel()
	f := newZeroGenesisFixture(t)
	db, raw := newSeededGenesisStore(t, types.StorageModeAPI, f)
	spent := f.zeroByron[0]
	require.NoError(t, db.MarkUtxosDeletedAtSlot(
		t.Context(), nil,
		[]types.UtxoKey{{
			TxId:      spent.Id.Id().Bytes(),
			OutputIdx: spent.Id.Index(),
		}},
		5,
	))

	repairFixture(t, db, f)

	require.Equal(t, 1, utxoRows(t, raw, spent), "the spent output stays")
	require.Equal(t, 2, txRows(t, raw, spent), "and so does its transaction")
	require.Zero(t, utxoRows(t, raw, f.zeroByron[1]))
	require.Zero(t, txRows(t, raw, f.zeroByron[1]))
}

// TestRepairZeroValueByronGenesisIsRepeatable runs the repair twice and
// requires the second run to find nothing to remove.
func TestRepairZeroValueByronGenesisIsRepeatable(t *testing.T) {
	t.Parallel()
	f := newZeroGenesisFixture(t)
	db, raw := newSeededGenesisStore(t, types.StorageModeCore, f)

	repairFixture(t, db, f)
	repairFixture(t, db, f)

	requireRepairKeepsKeptOutputs(t, raw, f)
	require.Equal(t, 2, rowCount(t, raw, `SELECT COUNT(*) FROM utxo`))
}

// TestRemoveGenesisUtxosRollsBackWithItsTransaction pins that the removal is
// one unit with the transaction it runs in, so an interrupted repair leaves
// every row and sets no marker.
func TestRemoveGenesisUtxosRollsBackWithItsTransaction(t *testing.T) {
	t.Parallel()
	f := newZeroGenesisFixture(t)
	db, raw := newSeededGenesisStore(t, types.StorageModeAPI, f)
	zero, err := ZeroValueByronGenesisOutputs(f.nodeCfg)
	require.NoError(t, err)
	require.Len(t, zero.Refs, 2)

	txn := db.Transaction(t.Context(), true)
	removed, _, err := db.RemoveGenesisUtxos(
		context.Background(), zero.Refs, zero.TxHashes, txn,
	)
	require.NoError(t, err)
	require.Len(t, removed, 2)
	require.NoError(t, txn.Rollback())

	for _, u := range f.all() {
		require.Equal(t, 1, utxoRows(t, raw, u))
		require.Equal(t, 2, txRows(t, raw, u))
	}
	marker, err := db.GetSyncState(ByronGenesisZeroRepairSyncKey, nil)
	require.NoError(t, err)
	require.Empty(t, marker)
}

// TestZeroValueByronGenesisOutputs pins the exact set: only zero-coin Byron
// outputs, and only the transactions no retained genesis output shares.
func TestZeroValueByronGenesisOutputs(t *testing.T) {
	t.Parallel()
	f := newZeroGenesisFixture(t)
	zero, err := ZeroValueByronGenesisOutputs(f.nodeCfg)
	require.NoError(t, err)
	want := map[string]bool{}
	for _, u := range f.zeroByron {
		want[hex.EncodeToString(u.Id.Id().Bytes())] = true
	}
	require.Len(t, zero.Refs, 2)
	for _, r := range zero.Refs {
		require.True(t, want[hex.EncodeToString(r.Hash)])
		require.Zero(t, r.Idx)
	}
	require.Len(t, zero.TxHashes, 2)
	for _, h := range zero.TxHashes {
		require.True(t, want[hex.EncodeToString(h)])
	}
}

// TestZeroValueByronGenesisOutputsKeepsSharedTransaction pins that a genesis
// transaction a retained output shares, here a funded Shelley initialFunds
// entry at the Byron address, is not named for removal: the zero-coin output
// is still dropped.
func TestZeroValueByronGenesisOutputsKeepsSharedTransaction(t *testing.T) {
	t.Parallel()
	f := newZeroGenesisFixture(t)
	addrBytes, err := f.zeroByron[0].Output.Address().Bytes()
	require.NoError(t, err)
	f.nodeCfg.ShelleyGenesis().InitialFunds[hex.EncodeToString(addrBytes)] = 7

	zero, err := ZeroValueByronGenesisOutputs(f.nodeCfg)
	require.NoError(t, err)

	require.Equal(t, []models.UtxoId{{
		Hash: f.zeroByron[1].Id.Id().Bytes(),
		Idx:  f.zeroByron[1].Id.Index(),
	}}, zero.Refs, "the Shelley output holds the shared reference")
	require.Equal(t, [][]byte{f.zeroByron[1].Id.Id().Bytes()}, zero.TxHashes)
}

// shelleyAtByronAddress adds a Shelley initialFunds entry of coin at the
// address of u, a zero-coin Byron genesis output, and returns its output,
// which has the same reference as u.
func shelleyAtByronAddress(
	t *testing.T,
	nodeCfg *cardano.CardanoNodeConfig,
	u lcommon.Utxo,
	coin uint64,
) lcommon.Utxo {
	t.Helper()
	addrBytes, err := u.Output.Address().Bytes()
	require.NoError(t, err)
	shelleyGenesis := nodeCfg.ShelleyGenesis()
	shelleyGenesis.InitialFunds[hex.EncodeToString(addrBytes)] = coin
	shelleyUtxos, err := shelleyGenesis.GenesisUtxos()
	require.NoError(t, err)
	for _, s := range shelleyUtxos {
		if s.Id.Id() == u.Id.Id() {
			require.Equal(t, u.Id.Index(), s.Id.Index())
			return s
		}
	}
	t.Fatalf("no Shelley genesis output at %s", u.Id.Id())
	return lcommon.Utxo{}
}

// TestZeroValueByronGenesisOutputsLeavesRetainedReference pins that a
// reference a retained Shelley initialFunds output occupies, funded or
// zero-coin, is never named for removal, though the Byron output there is
// dropped.
func TestZeroValueByronGenesisOutputsLeavesRetainedReference(t *testing.T) {
	t.Parallel()
	for name, coin := range map[string]uint64{"funded": 7, "zero-coin": 0} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newZeroGenesisFixture(t)
			shelleyAtByronAddress(t, f.nodeCfg, f.zeroByron[0], coin)

			zero, err := ZeroValueByronGenesisOutputs(f.nodeCfg)
			require.NoError(t, err)

			kept := f.zeroByron[1]
			require.Equal(t, []models.UtxoId{{
				Hash: kept.Id.Id().Bytes(), Idx: kept.Id.Index(),
			}}, zero.Refs)
			require.Equal(t, [][]byte{kept.Id.Id().Bytes()}, zero.TxHashes)
		})
	}
}

// TestRepairZeroValueByronGenesisKeepsOutputAtSharedReference seeds the
// Shelley initialFunds output that shares a zero-coin Byron output's
// reference, as the store of a node that dropped the Byron output holds it,
// and requires the metadata repair to keep it and its transaction in both
// storage modes. A zero-coin one passes the repair's amount check, so only
// the reference set protects it.
func TestRepairZeroValueByronGenesisKeepsOutputAtSharedReference(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{types.StorageModeCore, types.StorageModeAPI} {
		for name, coin := range map[string]uint64{"funded": 7, "zero-coin": 0} {
			t.Run(mode+"/"+name, func(t *testing.T) {
				t.Parallel()
				f := newZeroGenesisFixture(t)
				shared := shelleyAtByronAddress(
					t, f.nodeCfg, f.zeroByron[0], coin,
				)
				db, err := dbtest.NewReleasedDatabase(t, &database.Config{
					DataDir:     t.TempDir(),
					StorageMode: mode,
				})
				require.NoError(t, err)
				raw, err := dbtest.RawSQLiteMetadata(t, db)
				require.NoError(t, err)
				seedUnfilteredGenesis(t, db, f.nodeCfg, []lcommon.Utxo{
					f.zeroByron[1], f.fundedByron, f.zeroShelley, shared,
				})

				repairFixture(t, db, f)

				require.Equal(t, 1, utxoRows(t, raw, shared),
					"the Shelley output at the shared reference stays")
				require.Equal(t, 2, txRows(t, raw, shared))
				require.Zero(t, utxoRows(t, raw, f.zeroByron[1]))
				require.Zero(t, txRows(t, raw, f.zeroByron[1]))
				require.Equal(t, 3, rowCount(t, raw, `SELECT COUNT(*) FROM utxo`))
			})
		}
	}
}

// TestZeroValueByronGenesisOutputsOfPreview pins the zero-coin Byron outputs
// of the preview genesis.
func TestZeroValueByronGenesisOutputsOfPreview(t *testing.T) {
	t.Parallel()
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	zero, err := ZeroValueByronGenesisOutputs(nodeCfg)
	require.NoError(t, err)
	require.Len(t, zero.Refs, 7)
	require.Len(t, zero.TxHashes, 7)
}
