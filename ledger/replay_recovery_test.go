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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	sqliteplugin "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	pdata "github.com/blinklabs-io/plutigo/data"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

type replayRecoveryInput struct {
	txId  []byte
	index uint32
}

func (m *replayRecoveryInput) Id() lcommon.Blake2b256 {
	return lcommon.NewBlake2b256(m.txId)
}

func (m *replayRecoveryInput) Index() uint32 {
	return m.index
}

func (m *replayRecoveryInput) String() string {
	return fmt.Sprintf(
		"%s#%d",
		lcommon.NewBlake2b256(m.txId).String(),
		m.index,
	)
}

func (m *replayRecoveryInput) Utxorpc() (*cardano.TxInput, error) {
	return nil, nil
}

func (m *replayRecoveryInput) ToPlutusData() pdata.PlutusData {
	return nil
}

func TestTryRecoverFromTxValidationErrorRollsBackToEarliestProducerParent(
	t *testing.T,
) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	parentBlock := testRawBlock("recovery-parent", 100, 1, nil)
	producerOneBlock := testRawBlock(
		"producer-one",
		120,
		2,
		parentBlock.Hash,
	)
	producerTwoBlock := testRawBlock(
		"producer-two",
		140,
		3,
		producerOneBlock.Hash,
	)
	currentBlock := testRawBlock(
		"recovery-current",
		160,
		4,
		producerTwoBlock.Hash,
	)
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(
			[]chain.RawBlock{
				parentBlock,
				producerOneBlock,
				producerTwoBlock,
				currentBlock,
			},
		),
	)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	parentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(parentBlock.Slot, parentBlock.Hash),
		BlockNumber: parentBlock.BlockNumber,
	}
	currentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(currentBlock.Slot, currentBlock.Hash),
		BlockNumber: currentBlock.BlockNumber,
	}

	require.NoError(
		t,
		db.SetBlockNonce(
			parentTip.Point.Hash,
			parentTip.Point.Slot,
			[]byte("nonce-parent"),
			true,
			nil,
		),
	)
	require.NoError(
		t,
		db.SetBlockNonce(
			currentTip.Point.Hash,
			currentTip.Point.Slot,
			[]byte("nonce-current"),
			false,
			nil,
		),
	)
	require.NoError(t, db.SetTip(currentTip, nil))

	ls.currentTip = currentTip
	ls.currentTipBlockNonce = []byte("nonce-current")

	store, ok := db.Metadata().(*sqliteplugin.MetadataStoreSqlite)
	require.True(t, ok)
	require.NoError(
		t,
		store.DB().Create(&models.Transaction{
			Hash:       testHashBytes("producer-tx-1"),
			BlockHash:  producerOneBlock.Hash,
			Slot:       producerOneBlock.Slot,
			Type:       1,
			Valid:      true,
			BlockIndex: 0,
		}).Error,
	)
	require.NoError(
		t,
		store.DB().Create(&models.Transaction{
			Hash:       testHashBytes("producer-tx-2"),
			BlockHash:  producerTwoBlock.Hash,
			Slot:       producerTwoBlock.Slot,
			Type:       1,
			Valid:      true,
			BlockIndex: 0,
		}).Error,
	)

	recovered, err := ls.tryRecoverFromTxValidationError(
		&txValidationError{
			BlockPoint: currentTip.Point,
			TxHash:     testHashBytes("failing-tx"),
			Inputs: []lcommon.TransactionInput{
				&replayRecoveryInput{
					txId:  testHashBytes("producer-tx-2"),
					index: 0,
				},
				&replayRecoveryInput{
					txId:  testHashBytes("producer-tx-1"),
					index: 1,
				},
			},
			Cause: errors.New("bad input"),
		},
	)
	require.NoError(t, err)
	require.True(t, recovered)

	assert.Equal(t, parentTip, ls.currentTip)
	assert.Equal(t, currentTip, ls.chain.Tip())
	dbTip, err := ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, parentTip, dbTip)
}

func TestTryRecoverFromTxValidationErrorFallsBackToTxBlobOffsets(
	t *testing.T,
) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	parentBlock := testRawBlock("blob-parent", 200, 1, nil)
	producerBlock := testRawBlock("blob-producer", 220, 2, parentBlock.Hash)
	currentBlock := testRawBlock("blob-current", 260, 3, producerBlock.Hash)
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(
			[]chain.RawBlock{
				parentBlock,
				producerBlock,
				currentBlock,
			},
		),
	)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	parentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(parentBlock.Slot, parentBlock.Hash),
		BlockNumber: parentBlock.BlockNumber,
	}
	currentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(currentBlock.Slot, currentBlock.Hash),
		BlockNumber: currentBlock.BlockNumber,
	}
	require.NoError(
		t,
		db.SetBlockNonce(
			parentTip.Point.Hash,
			parentTip.Point.Slot,
			[]byte("nonce-parent"),
			true,
			nil,
		),
	)
	require.NoError(
		t,
		db.SetBlockNonce(
			currentTip.Point.Hash,
			currentTip.Point.Slot,
			[]byte("nonce-current"),
			false,
			nil,
		),
	)
	require.NoError(t, db.SetTip(currentTip, nil))

	txHash := testHashBytes("blob-only-producer-tx")
	offset := &database.CborOffset{
		BlockSlot:  producerBlock.Slot,
		ByteOffset: 10,
		ByteLength: 20,
	}
	copy(offset.BlockHash[:], producerBlock.Hash)
	blobTxn := db.BlobTxn(true)
	require.NotNil(t, blobTxn)
	require.NoError(
		t,
		blobTxn.Do(func(txn *database.Txn) error {
			return db.Blob().SetTx(
				txn.Blob(),
				txHash,
				database.EncodeTxOffset(offset),
			)
		}),
	)

	ls.currentTip = currentTip
	ls.currentTipBlockNonce = []byte("nonce-current")

	recovered, err := ls.tryRecoverFromTxValidationError(
		&txValidationError{
			BlockPoint: currentTip.Point,
			TxHash:     testHashBytes("blob-only-failing"),
			Inputs: []lcommon.TransactionInput{
				&replayRecoveryInput{
					txId:  txHash,
					index: 0,
				},
			},
			Cause: errors.New("bad input"),
		},
	)
	require.NoError(t, err)
	require.True(t, recovered)
	assert.Less(t, ls.currentTip.Point.Slot, currentTip.Point.Slot)
	assert.LessOrEqual(t, ls.currentTip.Point.Slot, parentTip.Point.Slot)
}

func TestTryRecoverFromTxValidationErrorFallsBackToChainScan(t *testing.T) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	imm, err := immutable.New("../database/immutable/testdata")
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()

	type replayRecoveryChainBlock struct {
		raw   chain.RawBlock
		block gledger.Block
	}
	var chainBlocks []replayRecoveryChainBlock
	producerIdx := -1
	for i := 0; i < 200 && (len(chainBlocks) < 12 || producerIdx < 0); i++ {
		immBlock, err := iter.Next()
		require.NoError(t, err)
		require.NotNil(t, immBlock)
		block, err := gledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)
		chainBlocks = append(chainBlocks, replayRecoveryChainBlock{
			raw: chain.RawBlock{
				Slot:        block.SlotNumber(),
				Hash:        block.Hash().Bytes(),
				BlockNumber: block.BlockNumber(),
				Type:        uint(block.Type()),
				PrevHash:    block.PrevHash().Bytes(),
				Cbor:        block.Cbor(),
			},
			block: block,
		})
		if producerIdx < 0 &&
			len(chainBlocks) > 1 &&
			len(block.Transactions()) > 0 {
			producerIdx = len(chainBlocks) - 1
		}
	}
	require.GreaterOrEqual(t, len(chainBlocks), 12)
	require.GreaterOrEqual(t, producerIdx, 1)
	currentIdx := len(chainBlocks) - 1
	require.Greater(t, currentIdx, producerIdx)
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(func() []chain.RawBlock {
			ret := make([]chain.RawBlock, 0, len(chainBlocks))
			for _, block := range chainBlocks {
				ret = append(ret, block.raw)
			}
			return ret
		}()),
	)

	parentBlock := chainBlocks[producerIdx-1]
	producerBlock := chainBlocks[producerIdx]
	currentBlock := chainBlocks[currentIdx]
	parentTip := ochainsync.Tip{
		Point: ocommon.NewPoint(
			parentBlock.raw.Slot,
			parentBlock.raw.Hash,
		),
		BlockNumber: parentBlock.raw.BlockNumber,
	}
	currentTip := ochainsync.Tip{
		Point: ocommon.NewPoint(
			currentBlock.raw.Slot,
			currentBlock.raw.Hash,
		),
		BlockNumber: currentBlock.raw.BlockNumber,
	}
	require.NoError(
		t,
		db.SetBlockNonce(
			parentTip.Point.Hash,
			parentTip.Point.Slot,
			[]byte("nonce-parent"),
			true,
			nil,
		),
	)
	require.NoError(
		t,
		db.SetBlockNonce(
			currentTip.Point.Hash,
			currentTip.Point.Slot,
			[]byte("nonce-current"),
			false,
			nil,
		),
	)
	require.NoError(t, db.SetTip(currentTip, nil))

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	ls.currentTip = currentTip
	ls.currentTipBlockNonce = []byte("nonce-current")

	producerTx := producerBlock.block.Transactions()[0]
	recovered, err := ls.tryRecoverFromTxValidationError(
		&txValidationError{
			BlockPoint: currentTip.Point,
			TxHash:     testHashBytes("chain-scan-failing"),
			Inputs: []lcommon.TransactionInput{
				&replayRecoveryInput{
					txId:  producerTx.Hash().Bytes(),
					index: 0,
				},
			},
			Cause: errors.New("bad input"),
		},
	)
	require.NoError(t, err)
	require.True(t, recovered)
	assert.Less(t, ls.currentTip.Point.Slot, currentTip.Point.Slot)
	assert.LessOrEqual(t, ls.currentTip.Point.Slot, parentTip.Point.Slot)
}

func TestTryRecoverFromTxValidationErrorRecoversDependencyClosure(
	t *testing.T,
) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	imm, err := immutable.New("../database/immutable/testdata")
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()

	type replayRecoveryChainBlock struct {
		raw   chain.RawBlock
		block gledger.Block
	}
	type replayRecoverySeenTx struct {
		block replayRecoveryChainBlock
		tx    lcommon.Transaction
	}
	var chainBlocks []replayRecoveryChainBlock
	seenTxs := make(map[string]replayRecoverySeenTx)
	var producerBlock replayRecoveryChainBlock
	var intermediateBlock replayRecoveryChainBlock
	var failingBlock replayRecoveryChainBlock
	var failingInput lcommon.TransactionInput
	found := false
	for i := 0; i < 20000 && !found; i++ {
		immBlock, err := iter.Next()
		require.NoError(t, err)
		require.NotNil(t, immBlock)
		block, err := gledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)
		chainBlock := replayRecoveryChainBlock{
			raw: chain.RawBlock{
				Slot:        block.SlotNumber(),
				Hash:        block.Hash().Bytes(),
				BlockNumber: block.BlockNumber(),
				Type:        uint(block.Type()),
				PrevHash:    block.PrevHash().Bytes(),
				Cbor:        block.Cbor(),
			},
			block: block,
		}
		chainBlocks = append(chainBlocks, chainBlock)
		for _, tx := range block.Transactions() {
			for _, input := range collectReferencedInputs(tx) {
				mid, ok := seenTxs[string(input.Id().Bytes())]
				if !ok {
					continue
				}
				for _, midInput := range collectReferencedInputs(mid.tx) {
					producer, ok := seenTxs[string(midInput.Id().Bytes())]
					if !ok {
						continue
					}
					producerBlock = producer.block
					intermediateBlock = mid.block
					failingBlock = chainBlock
					failingInput = input
					found = true
					break
				}
				if found {
					break
				}
			}
			seenTxs[string(tx.Hash().Bytes())] = replayRecoverySeenTx{
				block: chainBlock,
				tx:    tx,
			}
			if found {
				break
			}
		}
	}
	require.True(t, found, "expected to find a two-hop tx dependency in immutable test data")
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(func() []chain.RawBlock {
			ret := make([]chain.RawBlock, 0, len(chainBlocks))
			for _, block := range chainBlocks {
				ret = append(ret, block.raw)
			}
			return ret
		}()),
	)

	currentTip := ochainsync.Tip{
		Point: ocommon.NewPoint(
			failingBlock.raw.Slot,
			failingBlock.raw.Hash,
		),
		BlockNumber: failingBlock.raw.BlockNumber,
	}
	require.NoError(
		t,
		db.SetBlockNonce(
			currentTip.Point.Hash,
			currentTip.Point.Slot,
			[]byte("nonce-current"),
			false,
			nil,
		),
	)
	require.NoError(t, db.SetTip(currentTip, nil))

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	ls.currentTip = currentTip
	ls.currentTipBlockNonce = []byte("nonce-current")

	recovered, err := ls.tryRecoverFromTxValidationError(
		&txValidationError{
			BlockPoint: currentTip.Point,
			TxHash:     testHashBytes("dependency-closure-failing"),
			Inputs: []lcommon.TransactionInput{
				failingInput,
			},
			Cause: errors.New("bad input"),
		},
	)
	require.NoError(t, err)
	require.True(t, recovered)
	assert.Less(t, ls.currentTip.Point.Slot, producerBlock.raw.Slot)
	assert.Less(
		t,
		ls.currentTip.Point.Slot,
		intermediateBlock.raw.Slot,
	)
}

func TestTryRecoverFromTxValidationErrorFallsBackToSecurityParamWindow(
	t *testing.T,
) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	blocks := []chain.RawBlock{
		testRawBlock("fallback-1", 100, 1, nil),
		testRawBlock("fallback-2", 120, 2, testHashBytes("fallback-1")),
		testRawBlock("fallback-3", 140, 3, testHashBytes("fallback-2")),
		testRawBlock("fallback-4", 160, 4, testHashBytes("fallback-3")),
	}
	blocks[1].PrevHash = blocks[0].Hash
	blocks[2].PrevHash = blocks[1].Hash
	blocks[3].PrevHash = blocks[2].Hash
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(blocks))

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	currentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(blocks[3].Slot, blocks[3].Hash),
		BlockNumber: blocks[3].BlockNumber,
	}
	require.NoError(
		t,
		db.SetBlockNonce(
			currentTip.Point.Hash,
			currentTip.Point.Slot,
			[]byte("nonce-current"),
			false,
			nil,
		),
	)
	require.NoError(t, db.SetTip(currentTip, nil))
	ls.currentTip = currentTip
	ls.currentTipBlockNonce = []byte("nonce-current")

	recovered, err := ls.tryRecoverFromTxValidationError(
		&txValidationError{
			BlockPoint: currentTip.Point,
			TxHash:     testHashBytes("fallback-failing"),
			Inputs: []lcommon.TransactionInput{
				&replayRecoveryInput{
					txId:  testHashBytes("missing-producer"),
					index: 0,
				},
			},
			Cause: errors.New("bad input"),
		},
	)
	require.NoError(t, err)
	require.True(t, recovered)
	assert.Equal(t, ochainsync.Tip{}, ls.currentTip)
}

func TestTryRecoverFromTxValidationErrorSkipsUnknownProducer(t *testing.T) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	recovered, err := ls.tryRecoverFromTxValidationError(
		&txValidationError{
			BlockPoint: ocommon.NewPoint(100, testHashBytes("current")),
			TxHash:     testHashBytes("failing"),
			Inputs: []lcommon.TransactionInput{
				&replayRecoveryInput{
					txId:  testHashBytes("missing-producer"),
					index: 0,
				},
			},
			Cause: errors.New("bad input"),
		},
	)
	require.NoError(t, err)
	assert.False(t, recovered)
}

func testRawBlock(
	seed string,
	slot uint64,
	blockNumber uint64,
	prevHash []byte,
) chain.RawBlock {
	hash := testHashBytes(seed)
	return chain.RawBlock{
		Slot:        slot,
		Hash:        hash,
		BlockNumber: blockNumber,
		Type:        1,
		PrevHash:    bytes.Clone(prevHash),
		Cbor:        []byte{0x80},
	}
}
