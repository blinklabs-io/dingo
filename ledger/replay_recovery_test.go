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
	"github.com/blinklabs-io/dingo/database/models"
	sqliteplugin "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
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
