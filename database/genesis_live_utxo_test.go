// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package database

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// fixedStakeLiveSource answers every credential with the same stake.
type fixedStakeLiveSource struct{ stake uint64 }

func (fixedStakeLiveSource) LiveUtxoCbor(
	types.Txn, []byte, uint32,
) ([]byte, bool, error) {
	return nil, false, nil
}

func (s fixedStakeLiveSource) LiveUtxoStake(
	types.Txn, uint8, []byte,
) (uint64, error) {
	return s.stake, nil
}

func (fixedStakeLiveSource) LivePointerStakes(
	types.Txn,
) ([]types.PointerStake, error) {
	return nil, nil
}

func (fixedStakeLiveSource) EachLiveUtxoStake(
	types.Txn, func(uint8, []byte, uint64) error,
) error {
	return nil
}

// A database whose ledger keeps the live UTxO set records nothing of a genesis
// transaction in its metadata store: the LedgerDB holds the outputs, their
// stake and the transaction's index entry.
func TestSetGenesisTransactionWritesNoRowsUnderALiveUtxoSource(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	require.NoError(t, db.SetLiveUtxoSource(fixedStakeLiveSource{stake: 777}))
	stakeKey := bytes.Repeat([]byte{0x52}, lcommon.AddressHashSize)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x51}, lcommon.AddressHashSize),
		stakeKey,
	)
	require.NoError(t, err)
	output := shelley.ShelleyTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  777,
	}
	raw, err := cbor.Encode(&output)
	require.NoError(t, err)
	output.SetCbor(raw)
	txHash := bytes.Repeat([]byte{0xa1}, 32)
	var txHashArray [32]byte
	copy(txHashArray[:], txHash)
	input, err := shelley.NewShelleyTransactionInput(
		lcommon.NewBlake2b256(txHash).String(), 0,
	)
	require.NoError(t, err)
	utxo := lcommon.Utxo{Id: input, Output: output}
	err = db.SetGenesisTransaction(
		context.Background(),
		txHash,
		bytes.Repeat([]byte{0xb1}, 32),
		[]lcommon.Utxo{utxo},
		map[UtxoRef]CborOffset{{TxId: txHashArray, OutputIdx: 0}: {}},
		nil,
	)
	require.NoError(t, err)

	sqlDB := rawSQLiteMetadataFixture(t, db)
	for _, table := range []string{
		`"transaction"`, "applied_transaction", "utxo", "reward_live_stake",
	} {
		var rows int
		require.NoError(t, sqlDB.QueryRow(
			"SELECT COUNT(*) FROM "+table,
		).Scan(&rows))
		require.Zero(t, rows, "rows in %s", table)
	}
}

// A genesis transaction written in a transaction without a metadata store, as
// every transaction of the ledger's database is, touches no metadata store:
// the live UTxO set holds the outputs and the transaction index.
func TestSetGenesisTransactionWithoutAMetadataTransactionWritesNoRows(
	t *testing.T,
) {
	t.Parallel()
	db := openTestDB(t)
	output := shelley.ShelleyTransactionOutput{OutputAmount: 777}
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x61}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	output.OutputAddress = addr
	raw, err := cbor.Encode(&output)
	require.NoError(t, err)
	output.SetCbor(raw)
	txHash := bytes.Repeat([]byte{0xa2}, 32)
	var txHashArray [32]byte
	copy(txHashArray[:], txHash)
	input, err := shelley.NewShelleyTransactionInput(
		lcommon.NewBlake2b256(txHash).String(), 0,
	)
	require.NoError(t, err)
	txn := db.BlobTxn(true)
	require.Nil(t, txn.Metadata())
	require.NoError(t, db.SetGenesisTransaction(
		context.Background(),
		txHash,
		bytes.Repeat([]byte{0xb2}, 32),
		[]lcommon.Utxo{{Id: input, Output: output}},
		map[UtxoRef]CborOffset{{TxId: txHashArray, OutputIdx: 0}: {}},
		txn,
	))
	require.NoError(t, txn.Commit())

	sqlDB := rawSQLiteMetadataFixture(t, db)
	for _, table := range []string{`"transaction"`, "utxo"} {
		var rows int
		require.NoError(t, sqlDB.QueryRow(
			"SELECT COUNT(*) FROM "+table,
		).Scan(&rows))
		require.Zero(t, rows, "rows in %s", table)
	}
}

// A block's transaction written in a transaction without a metadata store, as
// every transaction of the ledger's database is, touches no metadata store.
func TestSetTransactionWithoutAMetadataTransactionWritesNoRows(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	candidate := findGapRollbackCandidateWithoutCertificates(t)
	for _, block := range candidate.producerBlocks {
		storeBlockOffsetsOnly(t, db, block)
	}
	storeBlockOffsetsOnly(t, db, candidate.consumerBlock)
	txn := db.BlobTxn(true)
	require.Nil(t, txn.Metadata())
	require.NoError(t, db.SetTransaction(
		context.Background(),
		candidate.consumerTx,
		candidate.consumerPoint,
		0,
		0,
		nil,
		nil,
		mustBlockOffsets(t, candidate.consumerBlock),
		txn, 0,
	))
	require.NoError(t, txn.Commit())

	sqlDB := rawSQLiteMetadataFixture(t, db)
	for _, table := range []string{`"transaction"`, "utxo"} {
		var rows int
		require.NoError(t, sqlDB.QueryRow(
			"SELECT COUNT(*) FROM "+table,
		).Scan(&rows))
		require.Zero(t, rows, "rows in %s", table)
	}
}
