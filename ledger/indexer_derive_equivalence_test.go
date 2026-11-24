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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/indexer"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

// newIndexerDBForDerive is an empty IndexerDB the deriver runs over.
func newIndexerDBForDerive(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		Role: database.RoleIndexer, DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	return db
}

func modelBlock(b gledger.Block) models.Block {
	return models.Block{
		Slot:     b.SlotNumber(),
		Hash:     b.Hash().Bytes(),
		PrevHash: b.PrevHash().Bytes(),
		Number:   b.BlockNumber(),
		Type:     uint(b.Type()),
		Cbor:     b.Cbor(),
	}
}

// seedIndexerUtxo adds a live output to the IndexerDB's rows and blob store,
// where the block indexer would have written it.
func seedIndexerUtxo(
	t *testing.T,
	db *database.Database,
	txID []byte,
	idx uint32,
	amount uint64,
	cbor []byte,
) {
	t.Helper()
	require.NoError(t, db.Transaction(t.Context(), true).Do(
		func(txn *database.Txn) error {
			if err := db.CreateUtxo(t.Context(), txn, &models.Utxo{
				TxId: txID, OutputIdx: idx, AddedSlot: 1,
				Amount: types.Uint64(amount), Cbor: cbor,
			}); err != nil {
				return err
			}
			return db.Blob().SetUtxo(txn.Blob(), txID, idx, cbor)
		},
	))
}

// crossBlocks derives, as the IndexerDB's writer does, every epoch boundary the
// blocks open, without indexing the blocks' rows.
func crossBlocks(
	t *testing.T,
	d *indexer.Deriver,
	db *database.Database,
	blocks ...models.Block,
) {
	t.Helper()
	require.NoError(t, db.Transaction(t.Context(), true).Do(
		func(txn *database.Txn) error {
			for _, b := range blocks {
				rest := []models.Block{b}
				for len(rest) > 0 {
					n, err := d.Split(txn, rest)
					if err != nil {
						return err
					}
					if n > 0 {
						rest = rest[n:]
						continue
					}
					if err := d.Cross(t.Context(), txn, rest[0]); err != nil {
						return err
					}
				}
			}
			return nil
		},
	))
}

// The facts the IndexerDB derives at the Byron-to-Shelley boundary of real
// mainnet blocks equal those the ledger computes for the same blocks over the
// same live UTxO: the translated reserves, the Shelley parameters, the ADA
// pots and the epoch that opens.
func TestDerivedByronTranslationEqualsTheLedgers(t *testing.T) {
	t.Parallel()
	ls, lastByron, firstShelley := newUnseededByronShelleyBoundaryLedger(t)
	const live = uint64(1_000_000_000)
	utxoID := []byte{0x03}
	seedBoundaryFixtureLiveUtxo(t, ls)

	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: []gledger.Block{firstShelley}}
	close(results)
	require.NoError(t, ls.ledgerProcessBlocksFromSource(t.Context(), results))

	idx := newIndexerDBForDerive(t)
	d, err := indexer.NewDeriver(idx, indexer.DeriveConfig{
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		NodeConfig: ls.config.CardanoNodeConfig,
	})
	require.NoError(t, err)
	crossBlocks(t, d, idx, modelBlock(lastByron))
	seedIndexerUtxo(t, idx, append(utxoID, make([]byte, 31)...), 0, live,
		boundaryOutputCbor(t, live))
	crossBlocks(t, d, idx, modelBlock(firstShelley))

	want := ls.ledgerDB()
	got := dbadapter.New(idx)
	wantState, err := want.GetNetworkState(nil)
	require.NoError(t, err)
	gotState, err := got.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, wantState)
	require.NotNil(t, gotState)
	wantState.ID, gotState.ID = 0, 0
	require.Equal(t, wantState, gotState)
	require.EqualValues(t, 45_000_000_000_000_000-live, gotState.Reserves)

	wantEpoch, err := want.GetEpoch(208, nil)
	require.NoError(t, err)
	gotEpoch, err := got.GetEpoch(208, nil)
	require.NoError(t, err)
	require.NotNil(t, wantEpoch)
	require.NotNil(t, gotEpoch)
	require.Equal(t, wantEpoch.StartSlot, gotEpoch.StartSlot)
	require.Equal(t, wantEpoch.EraId, gotEpoch.EraId)
	require.Equal(t, wantEpoch.SlotLength, gotEpoch.SlotLength)
	require.Equal(t, wantEpoch.LengthInSlots, gotEpoch.LengthInSlots)

	wantPParams, err := want.GetPParamsRows(208, wantEpoch.EraId, nil)
	require.NoError(t, err)
	gotPParams, err := got.GetPParamsRows(208, gotEpoch.EraId, nil)
	require.NoError(t, err)
	require.NotEmpty(t, wantPParams)
	require.Len(t, gotPParams, len(wantPParams))
	require.Equal(t, wantPParams[0].Cbor, gotPParams[0].Cbor)

	wantPots, err := want.GetRewardAdaPots(208, nil)
	require.NoError(t, err)
	gotPots, err := got.GetRewardAdaPots(208, nil)
	require.NoError(t, err)
	require.NotNil(t, wantPots)
	require.NotNil(t, gotPots)
	wantPots.ID, gotPots.ID = 0, 0
	require.Equal(t, wantPots, gotPots)
}

// byronOutputCbor is a Byron output of amount at addr.
func byronOutputCbor(
	t *testing.T,
	addr lcommon.Address,
	amount uint64,
) []byte {
	t.Helper()
	encoded, err := cbor.Encode(byron.ByronTransactionOutput{
		OutputAddress: addr, OutputAmount: amount,
	})
	require.NoError(t, err)
	return encoded
}

// deriveShelleyToAllegra derives the Shelley-to-Allegra boundary over an
// IndexerDB that holds avvm redeem outputs and the same pubkey output, and
// returns the network state it derives.
func deriveShelleyToAllegra(
	t *testing.T,
	avvm []uint64,
	pubkey uint64,
) *models.NetworkState {
	t.Helper()
	cfg := newTestEraHistoryCfg(t)
	cfg.ShelleyGenesisHash = treasuryRolloverGenesisHash
	cfg.ShelleyGenesis().MaxLovelaceSupply = 45_000_000_000_000_000
	cfg.ShelleyGenesis().ProtocolParameters.ProtocolVersion.Major = 2
	cfg.ShelleyGenesis().ProtocolParameters.ProtocolVersion.Minor = 0
	shelley, err := fixtures.GenerateShelleyChain(1, lcommon.Blake2b256{}, 100, 1, 1)
	require.NoError(t, err)
	allegra, err := fixtures.GenerateAllegraChain(
		2, shelley[0].Hash(), 432_000+100, 1, 1,
	)
	require.NoError(t, err)

	idx := newIndexerDBForDerive(t)
	d, err := indexer.NewDeriver(idx, indexer.DeriveConfig{
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		NodeConfig: cfg,
	})
	require.NoError(t, err)
	crossBlocks(t, d, idx, modelBlock(shelley[0]))

	redeemAddr, err := lcommon.NewByronAddressRedeem(
		bytes.Repeat([]byte{0x42}, 32), lcommon.ByronAddressAttributes{},
	)
	require.NoError(t, err)
	pubkeyAddr, err := lcommon.NewByronAddressFromParts(
		lcommon.ByronAddressTypePubkey,
		bytes.Repeat([]byte{0x55}, lcommon.AddressHashSize),
		lcommon.ByronAddressAttributes{},
	)
	require.NoError(t, err)
	for i, amount := range avvm {
		seedIndexerUtxo(t, idx, bytes.Repeat([]byte{0xA0 + byte(i)}, 32), 0,
			amount, byronOutputCbor(t, redeemAddr, amount))
	}
	seedIndexerUtxo(t, idx, bytes.Repeat([]byte{0xBB}, 32), 0, pubkey,
		byronOutputCbor(t, pubkeyAddr, pubkey))
	crossBlocks(t, d, idx, modelBlock(allegra[0]))

	state, err := dbadapter.New(idx).GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	return state
}

// The Allegra boundary of the IndexerDB returns the value at AVVM redeem
// addresses to the reserves, and only that value, as the ledger's HARDFORK
// rule does for the same live outputs: the reserves the deriver ends with
// exceed those of a chain holding no AVVM output by the ledger's credit.
func TestDerivedAllegraAvvmReturnEqualsTheLedgers(t *testing.T) {
	t.Parallel()
	avvm := []uint64{1_000, 2_500, 700}
	const pubkey = uint64(9_000)
	const boundarySlot = uint64(432_000)
	total := uint64(0)
	for _, a := range avvm {
		total += a
	}

	// The ledger's credit for the same outputs, over a reserves figure of its
	// own.
	const reservesBefore = uint64(5_000)
	db := newTestDB(t)
	redeemAddr, err := lcommon.NewByronAddressRedeem(
		bytes.Repeat([]byte{0x42}, 32), lcommon.ByronAddressAttributes{},
	)
	require.NoError(t, err)
	pubkeyAddr, err := lcommon.NewByronAddressFromParts(
		lcommon.ByronAddressTypePubkey,
		bytes.Repeat([]byte{0x55}, lcommon.AddressHashSize),
		lcommon.ByronAddressAttributes{},
	)
	require.NoError(t, err)
	for i, amount := range avvm {
		seedByronUtxoWithAmount(t, db, 0xA0+byte(i), redeemAddr, amount)
	}
	seedByronUtxoWithAmount(t, db, 0xBB, pubkeyAddr, pubkey)
	require.NoError(t, seedDB(t, db).SetNetworkState(7_000, reservesBefore, 100, nil))
	ls := newTestLSForHardForkRule(t, db)
	require.NoError(t, ls.applyIntraEraHardForkRule(
		t.Context(), nil, 3, boundarySlot, 1,
	))
	ledgerState, err := seedDB(t, db).GetNetworkState(nil)
	require.NoError(t, err)
	ledgerCredit := uint64(ledgerState.Reserves) - reservesBefore
	require.Equal(t, total, ledgerCredit)

	with := deriveShelleyToAllegra(t, avvm, pubkey)
	without := deriveShelleyToAllegra(t, nil, pubkey)
	require.Equal(t, boundarySlot, with.Slot)
	require.Equal(t, ledgerCredit, uint64(with.Reserves)-uint64(without.Reserves))
	require.Equal(t, without.Treasury, with.Treasury)
}
