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
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	"testing"
)

// OpenLiveSetForTest is installed by the external tests so internal ledger
// fixtures can use the production live set without importing its adapter back
// into the package whose interfaces it implements.
var OpenLiveSetForTest func(testing.TB, *database.Database) UtxoSetWriter

// NewMemLiveSetForTest is installed by the external tests so internal ledger
// tests can use an in-memory live UTxO set that holds the UTxO set only.
var NewMemLiveSetForTest func() UtxoSetWriter

// OpenUtxoOnlyLiveSetForTest is installed by the external tests. It opens
// the production live set's outputs beside the store in dataDir, where a
// harness restarted over that directory finds them again. Like the in-memory
// set it holds the UTxO set only; close releases it so the directory can be
// reopened.
var OpenUtxoOnlyLiveSetForTest func(
	tb testing.TB, dataDir string,
) (set UtxoSetWriter, close func() error)

func attachTestLiveSet(t testing.TB, ls *LedgerState) {
	t.Helper()
	ls.config.UtxoSet = OpenLiveSetForTest(t, ls.db)
	if ls.config.Logger == nil {
		ls.config.Logger = testLogger()
	}
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
}

func seedTestOutputCBOR(t *testing.T, db *database.Database, txID []byte, index uint32, amount uint64) []byte {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(lcommon.AddressTypeKeyNone, lcommon.AddressNetworkTestnet, bytes.Repeat([]byte{0x37}, lcommon.AddressHashSize), nil)
	require.NoError(t, err)
	encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{OutputAddress: addr, OutputAmount: amount})
	require.NoError(t, err)
	require.NoError(t, db.BlobTxn(true).Do(func(txn *database.Txn) error { return db.Blob().SetUtxo(txn.Blob(), txID, index, encoded) }))
	return encoded
}

func seedTestLiveOutput(t *testing.T, ls *LedgerState, txID []byte, index uint32, amount uint64) {
	t.Helper()
	encoded := seedTestOutputCBOR(t, ls.db, txID, index, amount)
	require.NoError(t, ls.config.UtxoSet.ApplyUtxoChanges(nil, []UtxoEvent{{Added: []UtxoOutput{{Ref: models.UtxoId{Hash: txID, Idx: index}, Cbor: encoded}}}}))
}

// importLedgerStateForTest imports cfg through a production live UTxO set,
// as a Mithril import does, then gives its outputs to ls's set and hands the
// database's live UTxO reads back to ls. The families the import wrote stay
// in the metadata store, where a ledger on the in-memory set reads them.
func importLedgerStateForTest(t *testing.T, ls *LedgerState, cfg ledgerstate.ImportConfig) error {
	t.Helper()
	set := OpenLiveSetForTest(t, ls.db)
	loader, ok := set.(ledgerstate.UtxoSetLoader)
	require.True(t, ok)
	cfg.UtxoSet = loader
	err := ledgerstate.ImportLedgerState(t.Context(), cfg)
	if ls.config.UtxoSet == nil {
		ls.config.UtxoSet = NewMemLiveSetForTest()
	}
	var added []UtxoOutput
	require.NoError(t, set.(UtxoSetIterator).EachUtxo(
		func(txID []byte, index uint32, cbor []byte) error {
			added = append(added, UtxoOutput{
				Ref:  models.UtxoId{Hash: bytes.Clone(txID), Idx: index},
				Cbor: bytes.Clone(cbor),
			})
			return nil
		},
	))
	require.NoError(t, ls.config.UtxoSet.ApplyUtxoChanges(
		nil, []UtxoEvent{{Added: added}},
	))
	require.NoError(t, ls.db.SetLiveUtxoSource(ls))
	return err
}

// seedTestStakedOutput puts a live output of amount lovelace at a base
// address delegating to stakeKey into ls's live UTxO set. The account of
// stakeKey must exist.
func seedTestStakedOutput(t *testing.T, ls *LedgerState, txID []byte, index uint32, stakeKey []byte, amount uint64) {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey, lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x37}, lcommon.AddressHashSize), stakeKey,
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
		OutputAddress: addr, OutputAmount: amount,
	})
	require.NoError(t, err)
	require.NoError(t, ls.config.UtxoSet.ApplyUtxoChanges(nil, []UtxoEvent{{
		Added: []UtxoOutput{{
			Ref:  models.UtxoId{Hash: txID, Idx: index},
			Cbor: encoded,
		}},
	}}))
	// The store's live stake aggregate follows the outputs a block applies;
	// one seeded past the ledger is folded in here.
	require.NoError(t, ls.db.RefreshRewardLiveStakeRefs(
		[]models.StakeCredentialRef{models.NewStakeCredentialRef(0, stakeKey)},
		0, nil,
	))
}

// mirrorSeededUtxoRowsForTest gives ls's live UTxO set an output for every
// staked utxo row a fixture seeded in the metadata store, at the slot the row
// was added, as the startup migration moves rows into the set. The rows carry
// no output CBOR, so each output is rebuilt from its staking key and amount.
func mirrorSeededUtxoRowsForTest(t *testing.T, ls *LedgerState) {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, ls.db)
	require.NoError(t, err)
	rows, err := raw.Query(
		`SELECT tx_id, output_idx, staking_key, CAST(amount AS INTEGER), added_slot
		 FROM utxo WHERE deleted_slot = 0 AND staking_key IS NOT NULL
		 ORDER BY added_slot, id`,
	)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var (
			txID, stakeKey []byte
			index          uint32
			amount, slot   uint64
		)
		require.NoError(t, rows.Scan(&txID, &index, &stakeKey, &amount, &slot))
		addr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeKeyKey, lcommon.AddressNetworkTestnet,
			bytes.Repeat([]byte{0x37}, lcommon.AddressHashSize), stakeKey,
		)
		require.NoError(t, err)
		encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
			OutputAddress: addr, OutputAmount: amount,
		})
		require.NoError(t, err)
		require.NoError(t, ls.config.UtxoSet.ApplyUtxoChanges(nil, []UtxoEvent{{
			Point: ocommon.Point{Slot: slot, Hash: txID},
			Added: []UtxoOutput{{
				Ref:  models.UtxoId{Hash: txID, Idx: index},
				Cbor: encoded,
			}},
		}}))
	}
	require.NoError(t, rows.Err())
}
