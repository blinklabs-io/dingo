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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mesh

import (
	"bytes"
	"errors"
	"net/http"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// blockRequest builds a /block request for the given identifier.
func blockRequest(id *PartialBlockIdentifier) BlockRequest {
	return BlockRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
		BlockIdentifier: id,
	}
}

// byIndex and byHash build partial block identifiers.
func byIndex(idx int64) *PartialBlockIdentifier {
	return &PartialBlockIdentifier{Index: &idx}
}

func byHash(hash string) *PartialBlockIdentifier {
	return &PartialBlockIdentifier{Hash: &hash}
}

func TestBlockByIndex(t *testing.T) {
	deps := newTestDeps()
	blockHash := testHash(0x11)
	prevHash := testHash(0x10)
	deps.database.blockByIndex = func(
		idx uint64,
	) (models.Block, error) {
		require.Equal(t, uint64(42), idx)
		return models.Block{
			Hash:     blockHash,
			PrevHash: prevHash,
			Number:   42,
			Slot:     4200,
		}, nil
	}
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/block", blockRequest(byIndex(42)))

	resp := decodeResponse[BlockResponse](t, rec)
	require.NotNil(t, resp.Block)
	require.Equal(
		t,
		&BlockIdentifier{
			Index: 42, Hash: hexString(blockHash),
		},
		resp.Block.BlockIdentifier,
	)
	require.Equal(
		t,
		&BlockIdentifier{
			Index: 41, Hash: hexString(prevHash),
		},
		resp.Block.ParentBlockIdentifier,
	)
	require.Equal(
		t,
		(testGenesisStartTimeSec+4200)*1000,
		resp.Block.Timestamp,
	)
	require.Empty(t, resp.Block.Transactions)
}

func TestBlockByHash(t *testing.T) {
	deps := newTestDeps()
	blockHash := testHash(0x22)
	deps.database.blockByHash = func(
		hash []byte,
	) (models.Block, error) {
		require.True(t, bytes.Equal(blockHash, hash))
		return models.Block{
			Hash:     blockHash,
			PrevHash: testHash(0x21),
			Number:   7,
			Slot:     700,
		}, nil
	}
	h := newTestHandler(t, deps)

	rec := postJSON(
		t, h, "/block",
		blockRequest(byHash(hexString(blockHash))),
	)

	resp := decodeResponse[BlockResponse](t, rec)
	require.Equal(
		t, hexString(blockHash), resp.Block.BlockIdentifier.Hash,
	)
}

// TestBlockHashTakesPrecedenceOverIndex pins the resolution order when a
// client sends both fields: the hash identifies the block exactly, so it
// must win over the ambiguous index.
func TestBlockHashTakesPrecedenceOverIndex(t *testing.T) {
	deps := newTestDeps()
	wanted := testHash(0x33)
	deps.database.blockByHash = func(
		[]byte,
	) (models.Block, error) {
		return models.Block{
			Hash: wanted, PrevHash: testHash(0x32), Number: 5,
		}, nil
	}
	deps.database.blockByIndex = func(
		uint64,
	) (models.Block, error) {
		t.Fatal("BlockByIndex must not be consulted")
		return models.Block{}, nil
	}
	h := newTestHandler(t, deps)

	idx := int64(999)
	hashStr := hexString(wanted)
	rec := postJSON(t, h, "/block", blockRequest(
		&PartialBlockIdentifier{Hash: &hashStr, Index: &idx},
	))

	resp := decodeResponse[BlockResponse](t, rec)
	require.Equal(t, int64(5), resp.Block.BlockIdentifier.Index)
}

// TestBlockGenesisParentIsSelf covers the Mesh requirement that the
// genesis block reports itself as its own parent.
func TestBlockGenesisParentIsSelf(t *testing.T) {
	deps := newTestDeps()
	genesisHash := mustDecodeHex(t, testGenesisHash)
	deps.database.blockByIndex = func(
		uint64,
	) (models.Block, error) {
		return models.Block{
			Hash:   genesisHash,
			Number: 0,
			Slot:   0,
		}, nil
	}
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/block", blockRequest(byIndex(0)))

	resp := decodeResponse[BlockResponse](t, rec)
	require.Equal(
		t,
		resp.Block.BlockIdentifier,
		resp.Block.ParentBlockIdentifier,
	)
}

func TestBlockWithTransactions(t *testing.T) {
	deps := newTestDeps()
	blockHash := testHash(0x44)
	txHash := testHash(0x45)
	paymentKey := testKeyHash(0x01)
	deps.database.blockByIndex = func(
		uint64,
	) (models.Block, error) {
		return models.Block{
			Hash:     blockHash,
			PrevHash: testHash(0x43),
			Number:   9,
			Slot:     900,
		}, nil
	}
	deps.database.txsByBlockHash = func(
		hash []byte,
	) ([]models.Transaction, error) {
		require.True(t, bytes.Equal(blockHash, hash))
		return []models.Transaction{
			{
				Hash:  txHash,
				Valid: true,
				Inputs: []models.Utxo{
					testUtxo(
						testHash(0x40), 0, 5_000_000,
						paymentKey, nil,
					),
				},
				Outputs: []models.Utxo{
					testUtxo(
						txHash, 0, 4_000_000,
						paymentKey, nil,
					),
				},
			},
		}, nil
	}
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/block", blockRequest(byIndex(9)))

	resp := decodeResponse[BlockResponse](t, rec)
	require.Len(t, resp.Block.Transactions, 1)
	tx := resp.Block.Transactions[0]
	require.Equal(t, hexString(txHash), tx.TransactionIdentifier.Hash)
	require.Len(t, tx.Operations, 2)
	require.Equal(t, OpInput, tx.Operations[0].Type)
	require.Equal(t, "-5000000", tx.Operations[0].Amount.Value)
	require.Equal(t, OpOutput, tx.Operations[1].Type)
	require.Equal(t, "4000000", tx.Operations[1].Amount.Value)
	// Outputs relate back to the inputs they consume.
	require.Equal(
		t,
		[]*OperationIdentifier{{Index: 0}},
		tx.Operations[1].RelatedOperations,
	)
}

func TestBlockNotFound(t *testing.T) {
	tests := map[string]*PartialBlockIdentifier{
		"by index": byIndex(1234),
		"by hash":  byHash(hexString(testHash(0x55))),
	}
	for name, id := range tests {
		t.Run(name, func(t *testing.T) {
			deps := newTestDeps()
			deps.database.blockByIndex = func(
				uint64,
			) (models.Block, error) {
				return models.Block{},
					models.ErrBlockNotFound
			}
			deps.database.blockByHash = func(
				[]byte,
			) (models.Block, error) {
				return models.Block{},
					models.ErrBlockNotFound
			}
			h := newTestHandler(t, deps)

			rec := postJSON(t, h, "/block", blockRequest(id))

			requireMeshError(
				t, rec, ErrBlockNotFound,
				http.StatusNotFound,
			)
		})
	}
}

func TestBlockInvalidIdentifier(t *testing.T) {
	tests := map[string]*PartialBlockIdentifier{
		"missing identifier": nil,
		"empty identifier":   {},
		"empty hash string":  byHash(""),
		"invalid hex hash":   byHash("nothex"),
		"negative index":     byIndex(-1),
	}
	for name, id := range tests {
		t.Run(name, func(t *testing.T) {
			h := newTestHandler(t, newTestDeps())

			rec := postJSON(t, h, "/block", blockRequest(id))

			requireMeshError(
				t, rec, ErrInvalidRequest,
				http.StatusBadRequest,
			)
		})
	}
}

func TestBlockDatabaseErrors(t *testing.T) {
	t.Run("block lookup fails", func(t *testing.T) {
		deps := newTestDeps()
		deps.database.blockByIndex = func(
			uint64,
		) (models.Block, error) {
			return models.Block{}, errors.New("disk on fire")
		}
		h := newTestHandler(t, deps)

		rec := postJSON(t, h, "/block", blockRequest(byIndex(1)))

		got := requireMeshError(
			t, rec, ErrInternal,
			http.StatusInternalServerError,
		)
		require.Equal(t, "disk on fire", got.Details["error"])
	})

	t.Run("transaction lookup fails", func(t *testing.T) {
		deps := newTestDeps()
		deps.database.blockByIndex = func(
			uint64,
		) (models.Block, error) {
			return models.Block{
				Hash:     testHash(0x66),
				PrevHash: testHash(0x65),
				Number:   3,
			}, nil
		}
		deps.database.txsByBlockHash = func(
			[]byte,
		) ([]models.Transaction, error) {
			return nil, errors.New("query failed")
		}
		h := newTestHandler(t, deps)

		rec := postJSON(t, h, "/block", blockRequest(byIndex(3)))

		requireMeshError(
			t, rec, ErrInternal,
			http.StatusInternalServerError,
		)
	})
}

// TestBlockAfterRollbackIsNotFound covers the reorg case: a block that
// was rolled back is no longer resolvable by its hash, so a client
// holding the old identifier gets a stable not-found rather than the
// block that replaced it at the same index.
func TestBlockAfterRollbackIsNotFound(t *testing.T) {
	rolledBack := testHash(0x77)
	replacement := testHash(0x78)
	deps := newTestDeps()
	deps.database.blockByHash = func(
		hash []byte,
	) (models.Block, error) {
		if bytes.Equal(hash, rolledBack) {
			return models.Block{}, models.ErrBlockNotFound
		}
		return models.Block{
			Hash:     replacement,
			PrevHash: testHash(0x76),
			Number:   12,
		}, nil
	}
	deps.database.blockByIndex = func(
		uint64,
	) (models.Block, error) {
		return models.Block{
			Hash:     replacement,
			PrevHash: testHash(0x76),
			Number:   12,
		}, nil
	}
	h := newTestHandler(t, deps)

	rec := postJSON(
		t, h, "/block",
		blockRequest(byHash(hexString(rolledBack))),
	)
	requireMeshError(
		t, rec, ErrBlockNotFound, http.StatusNotFound,
	)

	// The same height now resolves to the replacement block.
	rec = postJSON(t, h, "/block", blockRequest(byIndex(12)))
	resp := decodeResponse[BlockResponse](t, rec)
	require.Equal(
		t, hexString(replacement), resp.Block.BlockIdentifier.Hash,
	)
}

// blockTxRequest builds a /block/transaction request.
func blockTxRequest(
	blockHash string,
	txHash string,
) BlockTransactionRequest {
	req := BlockTransactionRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
	}
	if blockHash != "" {
		req.BlockIdentifier = &BlockIdentifier{Hash: blockHash}
	}
	if txHash != "" {
		req.TransactionIdentifier = &TransactionIdentifier{
			Hash: txHash,
		}
	}
	return req
}

func TestBlockTransaction(t *testing.T) {
	deps := newTestDeps()
	blockHash := testHash(0x88)
	txHash := testHash(0x89)
	paymentKey := testKeyHash(0x02)
	deps.database.txByHash = func(
		hash []byte,
	) (*models.Transaction, error) {
		require.True(t, bytes.Equal(txHash, hash))
		return &models.Transaction{
			Hash:      txHash,
			BlockHash: blockHash,
			Valid:     true,
			Outputs: []models.Utxo{
				testUtxo(
					txHash, 0, 1_500_000, paymentKey, nil,
				),
			},
		}, nil
	}
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/block/transaction", blockTxRequest(
		hexString(blockHash), hexString(txHash),
	))

	resp := decodeResponse[BlockTransactionResponse](t, rec)
	require.Equal(
		t,
		hexString(txHash),
		resp.Transaction.TransactionIdentifier.Hash,
	)
	require.Len(t, resp.Transaction.Operations, 1)
	require.Equal(
		t, "1500000", resp.Transaction.Operations[0].Amount.Value,
	)
}

// TestBlockTransactionWrongBlock covers the cross-block ambiguity guard:
// a transaction that exists but belongs to another block must not be
// served for the requested block.
func TestBlockTransactionWrongBlock(t *testing.T) {
	deps := newTestDeps()
	txHash := testHash(0x8a)
	deps.database.txByHash = func(
		[]byte,
	) (*models.Transaction, error) {
		return &models.Transaction{
			Hash:      txHash,
			BlockHash: testHash(0x8b),
		}, nil
	}
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/block/transaction", blockTxRequest(
		hexString(testHash(0x8c)), hexString(txHash),
	))

	requireMeshError(
		t, rec, ErrTransactionNotFound, http.StatusNotFound,
	)
}

func TestBlockTransactionNotFound(t *testing.T) {
	deps := newTestDeps()
	deps.database.txByHash = func(
		[]byte,
	) (*models.Transaction, error) {
		return nil, nil
	}
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/block/transaction", blockTxRequest(
		hexString(testHash(0x8d)), hexString(testHash(0x8e)),
	))

	requireMeshError(
		t, rec, ErrTransactionNotFound, http.StatusNotFound,
	)
}

func TestBlockTransactionInvalidRequest(t *testing.T) {
	tests := map[string]BlockTransactionRequest{
		"missing transaction identifier": blockTxRequest(
			hexString(testHash(0x8f)), "",
		),
		"missing block identifier": blockTxRequest(
			"", hexString(testHash(0x90)),
		),
		"invalid transaction hash hex": blockTxRequest(
			hexString(testHash(0x91)), "zz",
		),
		"invalid block hash hex": blockTxRequest(
			"zz", hexString(testHash(0x92)),
		),
	}
	for name, req := range tests {
		t.Run(name, func(t *testing.T) {
			deps := newTestDeps()
			deps.database.txByHash = func(
				[]byte,
			) (*models.Transaction, error) {
				return &models.Transaction{
					Hash:      testHash(0x92),
					BlockHash: testHash(0x91),
				}, nil
			}
			h := newTestHandler(t, deps)

			rec := postJSON(t, h, "/block/transaction", req)

			requireMeshError(
				t, rec, ErrInvalidRequest,
				http.StatusBadRequest,
			)
		})
	}
}

func TestBlockTransactionDatabaseError(t *testing.T) {
	deps := newTestDeps()
	deps.database.txByHash = func(
		[]byte,
	) (*models.Transaction, error) {
		return nil, errors.New("query failed")
	}
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/block/transaction", blockTxRequest(
		hexString(testHash(0x93)), hexString(testHash(0x94)),
	))

	requireMeshError(
		t, rec, ErrInternal, http.StatusInternalServerError,
	)
}
