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

// TestEpochNonceStoresClosingEpochLastBlockPrevHashAsLab verifies the
// epoch-nonce assembly (#2734). The epoch nonce mixes the frozen candidate with
// the CARRIED lastEpochBlockNonce (prevEpoch.LastEpochBlockNonce, == cardano-
// ledger praosStateLastEpochBlockNonce; ouroboros-consensus Praos.hs
// tickChainDepState uses candidateNonce ⭒ praosStateLastEpochBlockNonce). The
// value stored on the NEW epoch record (the carried lab for the NEXT boundary)
// is prevHashToNonce(lastBlock.prevHash) — the PARENT hash of the closing
// epoch's last block, NOT the last block's OWN hash (a one-block Praos lag).
// Confirmed on preview mainnet: koios eta_1348 =
// blake2b(frozenCandidate || epoch1347.LastEpochBlockNonce); eta_1349 lab =
// prevHash(94d3083a) = 08a8dd12.
func TestEpochNonceStoresClosingEpochLastBlockPrevHashAsLab(t *testing.T) {
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
	ls.publishSnapshotsLocked()

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

	// Correct assembly: eta = candidate ⭒ carried lastEpochBlockNonce.
	wantEta, err := lcommon.CalculateEpochNonce(
		nonceAtPreCut,
		carriedLab,
		nil,
	)
	require.NoError(t, err)
	// Old (buggy) assembly used the closing epoch's own last block hash.
	oldBuggyEta, err := lcommon.CalculateEpochNonce(
		nonceAtPreCut,
		hashAtPostCut,
		nil,
	)
	require.NoError(t, err)

	require.Equal(t, hex.EncodeToString(nonceAtPreCut), hex.EncodeToString(rCandidate))
	require.Equal(t, hex.EncodeToString(rCandidate), hex.EncodeToString(hvCandidate))
	// The STORED lastEpochBlockNonce is the PARENT hash of the closing epoch's
	// last block (prevHashToNonce(lastBlock.prevHash) == the post-cutoff block's
	// PrevHash == hashAtPreCut), which becomes the carried lab at the NEXT
	// boundary — a one-block Praos lag, NOT the last block's own hash.
	require.Equal(
		t,
		hex.EncodeToString(hashAtPreCut),
		hex.EncodeToString(rLab),
		"stored lastEpochBlockNonce must be the closing epoch's last-block PrevHash (for the next boundary)",
	)
	require.Equal(t, hex.EncodeToString(rLab), hex.EncodeToString(hvLab))
	require.NotEqual(
		t,
		hex.EncodeToString(hashAtPostCut),
		hex.EncodeToString(rLab),
		"stored lastEpochBlockNonce must NOT be the closing epoch's last block's own hash (the #2734 eta_1349 off-by-one)",
	)
	require.NotEqual(t, hex.EncodeToString(carriedLab), hex.EncodeToString(rLab))
	// The epoch nonce for THIS boundary uses the CARRIED lastEpochBlockNonce
	// (#2734), not the closing epoch's own last block.
	require.Equal(
		t,
		hex.EncodeToString(wantEta.Bytes()),
		hex.EncodeToString(rNonce),
		"epoch nonce must be candidate ⭒ carried lastEpochBlockNonce",
	)
	require.NotEqual(
		t,
		hex.EncodeToString(oldBuggyEta.Bytes()),
		hex.EncodeToString(rNonce),
		"epoch nonce must not use the closing epoch's own last block",
	)
	require.Equal(t, hex.EncodeToString(rNonce), hex.EncodeToString(hvNonce))
}

// TestEpochLabNonceEmptyEpochCarriesPrevNonceForward verifies that when the
// closing epoch has NO blocks of its own, epochLabNonce carries the previous
// boundary nonce forward UNCHANGED. Under the corrected Praos prevHash semantic
// (#2734) the carried value is already a prevHash-shape nonce, so it must be
// returned as-is — NOT converted to the boundary block's own hash (the old
// "normalize PrevHash -> Hash" behavior was the eta_1349 off-by-one regression).
func TestEpochLabNonceEmptyEpochCarriesPrevNonceForward(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	const (
		epochStart uint64 = 200
		epochEnd   uint64 = 300
	)

	// Only block is at slot 150, BEFORE epochStart (200): the closing epoch
	// [200,300) has no blocks of its own, so the carried nonce is preserved.
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

	carried := bytes.Repeat([]byte{0x03}, 32)
	ls := &LedgerState{db: db}
	lab, err := ls.epochLabNonce(nil, epochStart, epochEnd, carried)
	require.NoError(t, err)
	require.Equal(t, carried, lab,
		"empty closing epoch must carry the previous (prevHash-shape) nonce forward unchanged")
	require.NotEqual(t, boundaryHash, lab,
		"empty closing epoch must NOT normalize the carried nonce to the boundary block's own hash")
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
	// canonicalBlocks[1] is the boundary (last) block; its PrevHash is
	// canonicalBlocks[0].Hash. Under the corrected prevHash semantic (#2734)
	// epochLabNonce returns the boundary block's PrevHash, so the fork blob is
	// given a DISTINCT PrevHash to keep the canonical-vs-fork distinction sharp.
	canonicalPrevHash := canonicalBlocks[0].Hash().Bytes()
	canonicalBoundaryHash := canonicalBlocks[1].Hash().Bytes()
	forkHash := bytes.Repeat([]byte{0xf0}, 32)
	forkPrevHash := bytes.Repeat([]byte{0xfa}, 32)

	require.NoError(t, canonicalChain.AddBlock(canonicalBlocks[0], nil))
	require.NoError(t, canonicalChain.AddBlock(canonicalBlocks[1], nil))
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       99,
		Slot:     30,
		Hash:     forkHash,
		PrevHash: forkPrevHash,
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
	// Must be the canonical boundary block's PrevHash (from the canonical chain),
	// NOT the higher-slot fork blob's PrevHash, and NOT the boundary block's own
	// hash.
	require.Equal(t, canonicalPrevHash, lab)
	require.NotEqual(t, forkPrevHash, lab)
	require.NotEqual(t, canonicalBoundaryHash, lab)
}

// TestEpochLabNonceReturnsPrevHashNotHash pins the #2734 eta_1349 root cause:
// for a closing epoch with blocks, epochLabNonce returns the PARENT hash (the
// last block's PrevHash), NOT the last block's own hash. cardano-ledger's
// praosStateLastEpochBlockNonce = prevHashToNonce(lastBlock.prevHash), a
// one-block lag. Using the last block's own hash shifts the carried lab by one
// block and wedges every following epoch at the tip. koios preview:
// eta_1349 lab = prevHash(94d3083a, last block of 1347) = 08a8dd12.
func TestEpochLabNonceReturnsPrevHashNotHash(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	const (
		epochStart uint64 = 1000
		epochEnd   uint64 = 2000
	)
	parentHash := bytes.Repeat([]byte{0xa1}, 32) // second-to-last block
	lastHash := bytes.Repeat([]byte{0xb2}, 32)   // last block of the closing epoch
	require.NoError(t, db.BlockCreate(models.Block{
		Slot:     1500,
		Hash:     lastHash,
		PrevHash: parentHash, // last block's parent == the carried lab
		Cbor:     []byte{0x80},
		Number:   2,
		Type:     conway.BlockTypeConway,
	}, nil))

	ls := &LedgerState{db: db}
	lab, err := ls.epochLabNonce(nil, epochStart, epochEnd, nil)
	require.NoError(t, err)
	require.Equal(t, parentHash, lab,
		"epochLabNonce must return the last block's PrevHash (the parent hash)")
	require.NotEqual(t, lastHash, lab,
		"epochLabNonce must NOT return the last block's own hash (the eta_1349 off-by-one)")
}
