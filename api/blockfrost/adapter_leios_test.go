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

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/safedecode"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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

// certifiedRankingBlock builds a Dijkstra ranking block the way the shared
// fixture does, so a test can announce an existing endorser block again.
func certifiedRankingBlock(
	number, slot uint64,
	prevHash common.Blake2b256,
	certified bool,
	announcement []byte,
) (*dijkstra.DijkstraBlock, error) {
	certifiedFlag, err := cbor.Encode(certified)
	if err != nil {
		return nil, err
	}
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: number,
					Slot:        slot,
					PrevHash:    prevHash,
					VrfKey:      make([]byte, 32),
					VrfResult: common.VrfResult{
						Output: make([]byte, 64),
						Proof:  make([]byte, 80),
					},
					OpCert: babbage.BabbageOpCert{
						HotVkey:   make([]byte, 32),
						Signature: make([]byte, 64),
					},
					ProtoVersion: babbage.BabbageProtoVersion{
						Major: dijkstra.MinProtocolVersionDijkstra,
					},
				},
				Signature: make([]byte, 448),
			},
			LeiosHeaderExtension: []cbor.RawMessage{
				certifiedFlag,
				announcement,
			},
		},
	}
	if certified {
		block.BlockBody.LeiosCertificate = &dijkstra.DijkstraLeiosCertificate{
			Signers:             []byte{0x80},
			AggregatedSignature: make([]byte, 48),
		}
	}
	block.BlockHeader.Body.BlockBodyHash = block.BlockBody.Hash()
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	if err != nil {
		return nil, err
	}
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	if err != nil {
		return nil, err
	}
	return dijkstra.NewDijkstraBlockFromCbor(blockCbor)
}

func TestNodeAdapterCertifiedEndorserBlockBlockInfo(t *testing.T) {
	t.Parallel()
	nodeConfig, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		cardano.EmbeddedConfigPath("preview"),
	)
	require.NoError(t, err)
	adapter, raw, db := newDBBackedAdapter(t, nodeConfig)
	f, txHash := seedCertifiedEndorserBlock(t, db)
	ctx := context.Background()
	certifierHash := f.Certifier.Hash().String()

	_, err = raw.Exec(
		`INSERT INTO epoch (epoch_id, start_slot, length_in_slots, slot_length, era_id) VALUES (0, 0, 86400, 1000, 6)`,
	)
	require.NoError(t, err)
	require.NoError(
		t,
		adapter.ledgerState.PrepareEpochCacheForStartup(ctx),
	)
	adapter.ledgerState.SetTipForTesting(ochainsync.Tip{
		Point: ocommon.NewPoint(
			f.Certifier.SlotNumber(),
			f.Certifier.Hash().Bytes(),
		),
		BlockNumber: f.Certifier.BlockNumber(),
	})
	hashBytes, err := hex.DecodeString(txHash)
	require.NoError(t, err)
	insertAdapterTransaction(t, raw, &models.Transaction{
		Hash:       hashBytes,
		BlockHash:  f.Certifier.Hash().Bytes(),
		BlockIndex: 0,
		Slot:       f.Certifier.SlotNumber(),
		Valid:      true,
	})

	info, err := adapter.BlockByHashOrNumber(ctx, certifierHash)
	require.NoError(t, err)
	require.Equal(t, 1, info.TxCount)

	latest, err := adapter.LatestBlock(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, latest.TxCount)

	hashes, err := adapter.LatestBlockTxHashes(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{txHash}, hashes)

	addresses, err := adapter.BlockAddresses(ctx, certifierHash)
	require.NoError(t, err)
	// The fixture transaction has no outputs or inputs to touch.
	require.Empty(t, addresses)
}

func TestNodeAdapterBlockAddressesRepeatedEndorserTransaction(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	f, txHash := seedCertifiedEndorserBlock(t, db)
	ctx := context.Background()

	// The first certified block applied the endorser transaction, so its row
	// is indexed under that block.
	hashBytes, err := hex.DecodeString(txHash)
	require.NoError(t, err)
	insertAdapterTransaction(t, raw, &models.Transaction{
		Hash:       hashBytes,
		BlockHash:  f.Certifier.Hash().Bytes(),
		BlockIndex: 0,
		Slot:       f.Certifier.SlotNumber(),
		Valid:      true,
	})

	// A second certified block announces the same endorser block; the
	// ledger skips the already applied transaction.
	announcement, err := cbor.Encode([]any{f.EndorserHash.Bytes(), uint64(4096)})
	require.NoError(t, err)
	announcer2, err := certifiedRankingBlock(
		3, 200, f.Certifier.Hash(), false, announcement,
	)
	require.NoError(t, err)
	certifier2, err := certifiedRankingBlock(
		4, 201, announcer2.Hash(), true, []byte{0xf6},
	)
	require.NoError(t, err)
	for _, block := range []*dijkstra.DijkstraBlock{announcer2, certifier2} {
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
		announcer2.SlotNumber(), f.EndorserHash.Bytes(), f.Manifest,
		f.Transactions,
	))

	hashes, err := adapter.BlockTransactionHashes(ctx, certifier2.Hash().String())
	require.NoError(t, err)
	require.Equal(t, []string{txHash}, hashes)

	_, err = adapter.BlockAddresses(ctx, certifier2.Hash().String())
	require.NoError(t, err)
}
