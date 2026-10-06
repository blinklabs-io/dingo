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
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// mainnetMIRBlockFile is mainnet block 4495464 (slot 4592300, epoch 208),
// fetched over NtN blockfetch. Its first transaction carries a move
// instantaneous rewards certificate drawing on the reserves, witnessed by all
// seven mainnet genesis delegates.
const (
	mainnetMIRBlockFile = "mainnet-shelley-mir-4592300.cbor"
	mainnetMIRBlockHash = "ba3b7a00f1a7ac1f5f29911bf46bd4ac0016b525ebc63642c6d0f4f9139542cd"
	mainnetMIRTxHash    = "27dff3f43c460e779e35eff505f5f159c4283a8221b31ee17cdcd5b31ad221ba"
	// The one output the MIR transaction spends, as recorded on chain.
	mainnetMIRInputTxHash  = "05fa54879fabb47c2eca82396b0de189a87a74345fc8d73c9845802e4d9ac4e2"
	mainnetMIRInputAddress = "addr1v8vqle5aa50ljr6pu5ndqve29luch29qmpwwhz2pk5tcggqn3q8mu"
	mainnetMIRInputAmount  = uint64(19_597_234_235)
	// Reserves at the start of epoch 209. The 208/209 boundary only drew on
	// the reserves, so this is a lower bound for the balance the certificate
	// was judged against.
	mainnetEpoch209Reserves = uint64(13_286_160_713_028_443)
)

func mainnetMIRTransaction(t *testing.T) (gledger.Block, lcommon.Transaction) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", mainnetMIRBlockFile))
	require.NoError(t, err)
	block, err := gledger.NewBlockFromCbor(gledger.BlockTypeShelley, raw)
	require.NoError(t, err)
	require.Equal(t, mainnetMIRBlockHash, block.Hash().String())
	require.Equal(t, uint64(4_592_300), block.SlotNumber())
	for _, tx := range block.Transactions() {
		for _, cert := range tx.Certificates() {
			if _, ok := cert.(*lcommon.MoveInstantaneousRewardsCertificate); ok {
				require.Equal(t, mainnetMIRTxHash, tx.Hash().String())
				return block, tx
			}
		}
	}
	require.FailNow(t, "fixture block carries no MIR certificate")
	return nil, nil
}

// mainnetMIRView is a LedgerView over the named network's embedded genesis in
// mainnet epoch 208, with the MIR transaction's spent output and the reserves
// present.
func mainnetMIRView(
	t *testing.T,
	network string,
) (*LedgerState, lcommon.ProtocolParameters) {
	t.Helper()
	cfg, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		network+"/config.json",
	)
	require.NoError(t, err)
	pp, err := eras.HardForkShelley(cfg, nil)
	require.NoError(t, err)

	db := newTestDB(t)
	inputTxId, err := hex.DecodeString(mainnetMIRInputTxHash)
	require.NoError(t, err)
	addr, err := lcommon.NewAddress(mainnetMIRInputAddress)
	require.NoError(t, err)
	encoded, err := cbor.Encode(shelley.ShelleyTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  mainnetMIRInputAmount,
	})
	require.NoError(t, err)
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.CreateUtxo(txn, &models.Utxo{
			TxId:   inputTxId,
			Amount: types.Uint64(mainnetMIRInputAmount),
		}); err != nil {
			return err
		}
		return db.Blob().SetUtxo(txn.Blob(), inputTxId, 0, encoded)
	}))
	require.NoError(t, db.Metadata().SetNetworkState(
		0, mainnetEpoch209Reserves, 4_492_800, nil,
	))

	epoch := models.Epoch{
		EpochId:       208,
		StartSlot:     4_492_800,
		LengthInSlots: 432_000,
	}
	ls := &LedgerState{
		currentEpoch:   epoch,
		epochCache:     []models.Epoch{epoch},
		db:             db,
		currentPParams: pp,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            testLogger(),
		},
	}
	ls.publishSnapshotsLocked()
	return ls, pp
}

// withMIRView runs fn against a read view of ls inside one database
// transaction, as block application does.
func withMIRView(t *testing.T, ls *LedgerState, fn func(*LedgerView)) {
	t.Helper()
	require.NoError(
		t,
		ls.db.Transaction(false).Do(func(txn *database.Txn) error {
			fn(ls.NewView(txn))
			return nil
		}),
	)
}

// TestMIRCertificateValidatesAgainstMainnetBlock validates a mainnet
// transaction carrying an MIR certificate through the Shelley era validator,
// the entry point block application calls per transaction, against a ledger
// view built from mainnet genesis. The genesis-delegate quorum is met only by
// the delegates that genesis names; the same bytes judged under another
// network's genesis delegates count no signer at all.
func TestMIRCertificateValidatesAgainstMainnetBlock(t *testing.T) {
	t.Parallel()

	block, tx := mainnetMIRTransaction(t)

	t.Run("mainnet genesis", func(t *testing.T) {
		t.Parallel()
		ls, pp := mainnetMIRView(t, "mainnet")
		withMIRView(t, ls, func(lv *LedgerView) {
			require.NoError(
				t,
				eras.ValidateTxShelley(tx, block.SlotNumber(), lv, pp),
			)
			delegates, err := lv.GenesisDelegateKeyHashes(block.SlotNumber())
			require.NoError(t, err)
			require.Len(t, delegates, 7)
			quorum, err := lv.GenesisUpdateQuorum()
			require.NoError(t, err)
			require.Equal(t, uint(5), quorum)
		})
	})

	t.Run("other network genesis", func(t *testing.T) {
		t.Parallel()
		ls, pp := mainnetMIRView(t, "preprod")
		withMIRView(t, ls, func(lv *LedgerView) {
			err := shelley.UtxoValidateMIRGenesisQuorum(
				tx, block.SlotNumber(), lv, pp,
			)
			var quorumErr lcommon.MIRInsufficientGenesisSigsError
			require.True(t, errors.As(err, &quorumErr), "got %v", err)
			require.Equal(t, uint(0), quorumErr.Provided)
			require.Equal(t, uint(5), quorumErr.Required)
		})
	})
}
