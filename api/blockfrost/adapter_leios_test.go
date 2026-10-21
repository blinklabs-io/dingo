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

package blockfrost

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/safedecode"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

// seedCertifiedEndorserBlock stores a Dijkstra announcing block and its
// certifying child, plus the endorser block the certifier represents, and
// returns the fixture with the endorser transaction's hash.
func seedCertifiedEndorserBlock(
	t *testing.T,
	db *database.Database,
) (*testfixtures.CertifiedEndorserBlock, string) {
	t.Helper()
	f, err := testfixtures.GenerateCertifiedEndorserBlock(100)
	require.NoError(t, err)
	for _, block := range []gledger.Block{f.Announcer, f.Certifier} {
		require.NoError(t, db.BlockCreate(models.Block{
			Hash:     block.Hash().Bytes(),
			PrevHash: block.PrevHash().Bytes(),
			Slot:     block.SlotNumber(),
			Number:   block.BlockNumber(),
			ID:       block.BlockNumber() + database.BlockInitialIndex,
			Type:     uint(block.Type()),
			Cbor:     block.Cbor(),
		}, nil))
	}
	require.NoError(t, db.SetLeiosEB(
		f.Announcer.SlotNumber(), f.EndorserHash.Bytes(), f.Manifest,
		f.Transactions,
	))
	tx, err := safedecode.DijkstraBlockTransaction(f.Transactions[0])
	require.NoError(t, err)
	return f, tx.Hash().String()
}

func TestNodeAdapterCertifiedEndorserBlockTransactions(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	f, txHash := seedCertifiedEndorserBlock(t, db)
	ctx := context.Background()
	certifierHash := f.Certifier.Hash().String()

	require.Empty(t, f.Certifier.Transactions(), "ranking block has no body txs")

	hashes, err := adapter.BlockTransactionHashes(ctx, certifierHash)
	require.NoError(t, err)
	require.Equal(t, []string{txHash}, hashes)

	cbors, err := adapter.BlockTransactionCBOR(ctx, certifierHash)
	require.NoError(t, err)
	require.Equal(t, []BlockTransactionCBORInfo{{
		TxHash: txHash,
		CBOR:   hex.EncodeToString(f.Transactions[0]),
	}}, cbors)

	// The announcing block announces but does not certify, so it represents
	// no transactions.
	hashes, err = adapter.BlockTransactionHashes(
		ctx, f.Announcer.Hash().String(),
	)
	require.NoError(t, err)
	require.Empty(t, hashes)

	// Transactions are indexed under the certifying block; the transaction
	// lookup must find the endorser transaction through it.
	hashBytes, err := hex.DecodeString(txHash)
	require.NoError(t, err)
	insertAdapterTransaction(t, raw, &models.Transaction{
		Hash:       hashBytes,
		BlockHash:  f.Certifier.Hash().Bytes(),
		BlockIndex: 0,
		Slot:       f.Certifier.SlotNumber(),
		Valid:      true,
	})
	txCbor, err := adapter.TransactionCBOR(ctx, hashBytes)
	require.NoError(t, err)
	require.Equal(t, []byte(f.Transactions[0]), txCbor)
}

func TestNodeAdapterCertifiedEndorserBlockMissingBody(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	seedCertifiedEndorserBlock(t, db)
	// A certifier without a retained endorser block must surface an error
	// rather than report an empty block.
	other, err := testfixtures.GenerateCertifiedEndorserBlock(200)
	require.NoError(t, err)
	require.NoError(t, db.BlockCreate(models.Block{
		Hash:     other.Announcer.Hash().Bytes(),
		PrevHash: other.Announcer.PrevHash().Bytes(),
		Slot:     other.Announcer.SlotNumber(),
		Number:   3,
		ID:       3 + database.BlockInitialIndex,
		Type:     uint(other.Announcer.Type()),
		Cbor:     other.Announcer.Cbor(),
	}, nil))
	require.NoError(t, db.BlockCreate(models.Block{
		Hash:     other.Certifier.Hash().Bytes(),
		PrevHash: other.Certifier.PrevHash().Bytes(),
		Slot:     other.Certifier.SlotNumber(),
		Number:   4,
		ID:       4 + database.BlockInitialIndex,
		Type:     uint(other.Certifier.Type()),
		Cbor:     other.Certifier.Cbor(),
	}, nil))
	_, err = adapter.BlockTransactionHashes(
		context.Background(), other.Certifier.Hash().String(),
	)
	require.ErrorContains(t, err, "load certified endorser-block")
}
