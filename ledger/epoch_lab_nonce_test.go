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
	"encoding/hex"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

func TestEpochNonceUsesCurrentEpochLastBlockHash(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		preCutSlot  uint64 = 1014
		postCutSlot uint64 = 1070
	)

	importedNonce := bytes.Repeat([]byte{0xaa}, 32)
	nonceAtPreCut := bytes.Repeat([]byte{0xbb}, 32)
	nonceAtPostCut := bytes.Repeat([]byte{0xcc}, 32)
	carriedLab := bytes.Repeat([]byte{0xfa}, 32)

	hashAtPreCut := bytes.Repeat([]byte{0x14}, 32)
	hashAtPostCut := bytes.Repeat([]byte{0x70}, 32)
	prevHashAtPreCut := bytes.Repeat([]byte{0x09}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot:     preCutSlot,
			Hash:     hashAtPreCut,
			PrevHash: prevHashAtPreCut,
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     postCutSlot,
			Hash:     hashAtPostCut,
			PrevHash: hashAtPreCut,
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtPreCut, preCutSlot, nonceAtPreCut, false, txn,
		); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn,
		)
	}))

	prevEpoch := models.Epoch{
		EpochId:             100,
		StartSlot:           epochStart,
		LengthInSlots:       uint(epochLength),
		SlotLength:          1000,
		EraId:               eras.ConwayEraDesc.Id,
		Nonce:               bytes.Repeat([]byte{0xee}, 32),
		EvolvingNonce:       importedNonce,
		CandidateNonce:      importedNonce,
		LastEpochBlockNonce: carriedLab,
	}

	ls := &LedgerState{
		db:           db,
		currentEra:   eras.ConwayEraDesc,
		currentEpoch: prevEpoch,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	hvNonce, _, hvCandidate, hvLab, err :=
		ls.computeEpochNonceForSlot(epochEnd, prevEpoch)
	require.NoError(t, err)

	var rNonce, rCandidate, rLab []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		n, _, c, lab, err := ls.calculateEpochNonce(
			txn,
			epochEnd,
			eras.ConwayEraDesc,
			prevEpoch,
		)
		rNonce = n
		rCandidate = c
		rLab = lab
		return err
	}))

	expected, err := lcommon.CalculateEpochNonce(
		nonceAtPreCut,
		hashAtPostCut,
		nil,
	)
	require.NoError(t, err)

	require.Equal(t, hex.EncodeToString(nonceAtPreCut), hex.EncodeToString(rCandidate))
	require.Equal(t, hex.EncodeToString(rCandidate), hex.EncodeToString(hvCandidate))
	require.Equal(
		t,
		hex.EncodeToString(hashAtPostCut),
		hex.EncodeToString(rLab),
		"lastEpochBlockNonce must be the current epoch's last block hash",
	)
	require.Equal(t, hex.EncodeToString(rLab), hex.EncodeToString(hvLab))
	require.NotEqual(
		t,
		hex.EncodeToString(hashAtPreCut),
		hex.EncodeToString(rLab),
		"lastEpochBlockNonce must not be the boundary block's PrevHash",
	)
	require.NotEqual(t, hex.EncodeToString(carriedLab), hex.EncodeToString(rLab))
	require.Equal(t, expected.Bytes(), rNonce)
	require.Equal(t, expected.Bytes(), hvNonce)
}

func TestEpochLabNonceNormalizesOldPrevHashCarryForEmptyEpoch(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	const (
		epochStart uint64 = 200
		epochEnd   uint64 = 300
	)

	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0x02}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		Slot:     150,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   1,
		Type:     conway.BlockTypeConway,
	}, nil))

	ls := &LedgerState{db: db}
	lab, err := ls.epochLabNonce(nil, epochStart, epochEnd, boundaryPrevHash)
	require.NoError(t, err)
	require.Equal(t, boundaryHash, lab)
}

func TestEpochLabNonceUsesCanonicalChainWhenForkBlobHasHigherSlot(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	canonicalChain := cm.PrimaryChain()

	canonicalBlocks, err := fixtures.GenerateConwayChain(
		1,
		lcommon.Blake2b256{},
		10,
		10,
		2,
	)
	require.NoError(t, err)
	require.Len(t, canonicalBlocks, 2)
	canonicalPrevHash := canonicalBlocks[0].Hash().Bytes()
	canonicalBoundaryHash := canonicalBlocks[1].Hash().Bytes()
	forkHash := bytes.Repeat([]byte{0xf0}, 32)

	require.NoError(t, canonicalChain.AddBlock(canonicalBlocks[0], nil))
	require.NoError(t, canonicalChain.AddBlock(canonicalBlocks[1], nil))
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       99,
		Slot:     30,
		Hash:     forkHash,
		PrevHash: canonicalPrevHash,
		Cbor:     []byte{0x80},
		Number:   99,
		Type:     conway.BlockTypeConway,
	}, nil))

	rawBlock, err := database.BlockBeforeSlot(db, 40)
	require.NoError(t, err)
	require.Equal(t, forkHash, rawBlock.Hash)

	ls := &LedgerState{
		db:    db,
		chain: canonicalChain,
	}
	lab, err := ls.epochLabNonce(nil, 0, 40, nil)
	require.NoError(t, err)
	require.Equal(t, canonicalBoundaryHash, lab)
}
