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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// TestCalculateEpochNonce_PostMithrilBootstrapFreezesCandidateAtCutoff
// reproduces the persisted-state shape that issue #2128 reports after a
// Mithril bootstrap inside a Conway epoch:
//
//   - The bootstrap epoch row carries CandidateNonce == EvolvingNonce
//     because the snapshot was taken before the candidate-freeze cutoff
//     (psCandidateNonce in cardano-ledger tracks evolving until the
//     stability window closes).
//   - importTip wrote a block_nonce checkpoint at the snapshot tip slot
//     so the resume logic can find the seam between the imported tip
//     and post-import per-block accumulation.
//   - Post-import sync produced block_nonce rows for blocks past the
//     snapshot tip, including blocks straddling the freeze cutoff.
//
// The Conway→Conway rollover that closes the bootstrap epoch must:
//
//   - return candidateNonce frozen at the latest pre-cutoff block's
//     stored nonce (NOT the imported tip-time value), and
//   - return evolvingNonce equal to the last-block-of-epoch's stored
//     nonce.
//
// If the rollover instead returns the imported tip-time value as the
// candidate (i.e. it inherited prevEpoch.CandidateNonce without ever
// iterating past the cutoff), the next epoch's nonce diverges from peers
// and every header in that epoch fails VRF verification — the freeze
// described in #2128.
func TestCalculateEpochNonce_PostMithrilBootstrapFreezesCandidateAtCutoff(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cfg := newConwayBootstrapStabilityCfg(t)

	// k=6, f=0.4 → 4k/f = 60 slots. Epoch length 75, start slot 1000,
	// end slot 1075. cutoffSlot = 1075 - 60 = 1015.
	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		cutoffSlot  uint64 = 1015
		snapTipSlot uint64 = 1010 // snapshot taken before cutoff
		preCutSlot  uint64 = 1014 // last block strictly before cutoff
		postCutSlot uint64 = 1070 // last block of epoch (post-cutoff)
	)

	// Imported snapshot tip-time evolving == candidate (psCandidate
	// tracks evolving until the stability window closes).
	importedNonce := bytes.Repeat([]byte{0xaa}, 32)
	// Per-block evolving nonces stored by post-import processing.
	// Distinct, deterministic values so a wrong return is unambiguous.
	nonceAtPreCut := bytes.Repeat([]byte{0xbb}, 32)
	nonceAtPostCut := bytes.Repeat([]byte{0xcc}, 32)

	hashAtSnap := bytes.Repeat([]byte{0x10}, 32)
	hashAtPreCut := bytes.Repeat([]byte{0x14}, 32)
	hashAtPostCut := bytes.Repeat([]byte{0x70}, 32)
	prevHashAtSnap := bytes.Repeat([]byte{0x09}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		// Mithril-imported tip block (no per-block VRF processing
		// for it; importTip writes a single block_nonce checkpoint).
		if err := db.BlockCreate(models.Block{
			Slot:     snapTipSlot,
			Hash:     hashAtSnap,
			PrevHash: prevHashAtSnap,
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		// Post-import blocks. CBOR is a stub byte; the fast path
		// computeCandidateNonceFast does not decode block bodies.
		if err := db.BlockCreate(models.Block{
			Slot:     preCutSlot,
			Hash:     hashAtPreCut,
			PrevHash: hashAtSnap,
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     postCutSlot,
			Hash:     hashAtPostCut,
			PrevHash: hashAtPreCut,
			Cbor:     []byte{0x80},
			Number:   3,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		// importTip checkpoint at the snapshot tip — Branch B in
		// calculateEpochNonce relies on this row to find the seam
		// between imported state and post-import accumulation.
		if err := db.SetBlockNonce(
			hashAtSnap, snapTipSlot, importedNonce, true, txn,
		); err != nil {
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

	// LedgerState shaped like the bootstrap epoch row written by
	// generateAndSaveEpochs at import time: EvolvingNonce and
	// CandidateNonce both seeded from the snapshot's mid-epoch value.
	// currentTipBlockNonce intentionally left empty so the
	// resume-from-tip optimisation in calculateEpochNonce takes the
	// Branch B path (block_nonce row search).
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentEpoch: models.Epoch{
			EpochId:             100,
			StartSlot:           epochStart,
			LengthInSlots:       uint(epochLength),
			SlotLength:          1000,
			EraId:               eras.ConwayEraDesc.Id,
			Nonce:               bytes.Repeat([]byte{0xee}, 32),
			EvolvingNonce:       importedNonce,
			CandidateNonce:      importedNonce,
			LastEpochBlockNonce: bytes.Repeat([]byte{0xfa}, 32),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	var candidate, evolving []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		_, ev, c, _, err := ls.calculateEpochNonce(
			txn,
			epochEnd,
			eras.ConwayEraDesc,
			ls.currentEpoch,
		)
		candidate = c
		evolving = ev
		return err
	}))

	require.Equalf(
		t,
		hex.EncodeToString(nonceAtPreCut),
		hex.EncodeToString(candidate),
		"candidate must freeze at the last pre-cutoff block (slot %d). "+
			"Got %x. If this equals importedNonce (0xaa...0xaa), the "+
			"computation inherited the snapshot's mid-epoch candidate "+
			"and never replaced it with the frozen-at-cutoff value — "+
			"#2128 freeze. cutoff=%d, epoch=[%d,%d).",
		preCutSlot, candidate, cutoffSlot, epochStart, epochEnd,
	)
	require.Equalf(
		t,
		hex.EncodeToString(nonceAtPostCut),
		hex.EncodeToString(evolving),
		"evolving must equal the last block's stored nonce (slot %d). "+
			"Got %x.",
		postCutSlot, evolving,
	)
	require.NotEqualf(
		t,
		hex.EncodeToString(candidate),
		hex.EncodeToString(evolving),
		"candidate and evolving must differ once the chain crosses "+
			"the cutoff. Equal values mean the freeze was not applied.",
	)
}

// TestCalculateEpochNonce_PostMithrilBootstrapNoBlocksBeforeCutoff covers
// the edge case where the chain produces NO blocks in the window between
// the snapshot tip and the freeze cutoff (legal under low active-slots
// coefficient or just unlucky leader assignment). The snapshot tip slot
// is itself the last pre-cutoff slot, so the imported tip-time
// EvolvingNonce IS the correct frozen-at-cutoff value: in cardano-ledger,
// psCandidateNonce tracks evolving until the cutoff fires, so a snapshot
// taken at the very last pre-cutoff block has psCandidateNonce ==
// psEvolvingNonce == that block's accumulated evolving nonce.
//
// The rollover must therefore return candidate == importedNonce, NOT
// some other value derived from a phantom pre-cutoff block.
func TestCalculateEpochNonce_PostMithrilBootstrapNoBlocksBeforeCutoff(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		cutoffSlot  uint64 = 1015
		snapTipSlot uint64 = 1014 // last block strictly before cutoff
		postCutSlot uint64 = 1070 // last block of epoch (post-cutoff)
	)

	importedNonce := bytes.Repeat([]byte{0xaa}, 32)
	nonceAtPostCut := bytes.Repeat([]byte{0xcc}, 32)

	hashAtSnap := bytes.Repeat([]byte{0x14}, 32)
	hashAtPostCut := bytes.Repeat([]byte{0x70}, 32)
	prevHashAtSnap := bytes.Repeat([]byte{0x09}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot:     snapTipSlot,
			Hash:     hashAtSnap,
			PrevHash: prevHashAtSnap,
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     postCutSlot,
			Hash:     hashAtPostCut,
			PrevHash: hashAtSnap,
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtSnap, snapTipSlot, importedNonce, true, txn,
		); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn,
		)
	}))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentEpoch: models.Epoch{
			EpochId:             100,
			StartSlot:           epochStart,
			LengthInSlots:       uint(epochLength),
			SlotLength:          1000,
			EraId:               eras.ConwayEraDesc.Id,
			Nonce:               bytes.Repeat([]byte{0xee}, 32),
			EvolvingNonce:       importedNonce,
			CandidateNonce:      importedNonce,
			LastEpochBlockNonce: bytes.Repeat([]byte{0xfa}, 32),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	var candidate, evolving []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		_, ev, c, _, err := ls.calculateEpochNonce(
			txn,
			epochEnd,
			eras.ConwayEraDesc,
			ls.currentEpoch,
		)
		candidate = c
		evolving = ev
		return err
	}))

	require.Equalf(
		t,
		hex.EncodeToString(importedNonce),
		hex.EncodeToString(candidate),
		"with no post-import blocks before the cutoff (slot %d), the "+
			"snapshot tip slot %d itself IS the last pre-cutoff block "+
			"and its stored block_nonce (= imported tip-time evolving) "+
			"is the correct frozen candidate. Got %x.",
		cutoffSlot, snapTipSlot, candidate,
	)
	require.Equalf(
		t,
		hex.EncodeToString(nonceAtPostCut),
		hex.EncodeToString(evolving),
		"evolving must equal the last block's stored nonce (slot %d). "+
			"Got %x.",
		postCutSlot, evolving,
	)
}

// TestCalculateEpochNonce_PostMithrilBootstrapWithoutCheckpoint covers
// the operational hazard where a deployment was bootstrapped with an
// importer that did not write the block_nonce checkpoint at the
// snapshot tip slot (older code paths predating PR #2032). Branch B
// in calculateEpochNonce searches for a block_nonce row matching
// prevEpoch.EvolvingNonce; with no checkpoint that row does not
// exist, and the resume seam is not found.
//
// The fast path should still produce correct results because it does
// NOT depend on Branch B — it directly looks up the cutoff block and
// the last block of the epoch by slot, and uses their stored
// block_nonce rows. Those rows exist for every post-import block
// (per-block accumulation correctly chains from the imported
// EvolvingNonce, even though the seed itself is not in block_nonce).
//
// This test guards against any future change that makes the fast
// path require a Branch B match.
func TestCalculateEpochNonce_PostMithrilBootstrapWithoutCheckpoint(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		cutoffSlot  uint64 = 1015
		snapTipSlot uint64 = 1010
		preCutSlot  uint64 = 1014
		postCutSlot uint64 = 1070
	)

	importedNonce := bytes.Repeat([]byte{0xaa}, 32)
	nonceAtPreCut := bytes.Repeat([]byte{0xbb}, 32)
	nonceAtPostCut := bytes.Repeat([]byte{0xcc}, 32)

	hashAtSnap := bytes.Repeat([]byte{0x10}, 32)
	hashAtPreCut := bytes.Repeat([]byte{0x14}, 32)
	hashAtPostCut := bytes.Repeat([]byte{0x70}, 32)
	prevHashAtSnap := bytes.Repeat([]byte{0x09}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot:     snapTipSlot,
			Hash:     hashAtSnap,
			PrevHash: prevHashAtSnap,
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     preCutSlot,
			Hash:     hashAtPreCut,
			PrevHash: hashAtSnap,
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     postCutSlot,
			Hash:     hashAtPostCut,
			PrevHash: hashAtPreCut,
			Cbor:     []byte{0x80},
			Number:   3,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		// NOTE: deliberately NO checkpoint row at snapTipSlot.
		// Post-import block_nonce rows only.
		if err := db.SetBlockNonce(
			hashAtPreCut, preCutSlot, nonceAtPreCut, false, txn,
		); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn,
		)
	}))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentEpoch: models.Epoch{
			EpochId:             100,
			StartSlot:           epochStart,
			LengthInSlots:       uint(epochLength),
			SlotLength:          1000,
			EraId:               eras.ConwayEraDesc.Id,
			Nonce:               bytes.Repeat([]byte{0xee}, 32),
			EvolvingNonce:       importedNonce,
			CandidateNonce:      importedNonce,
			LastEpochBlockNonce: bytes.Repeat([]byte{0xfa}, 32),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	var candidate, evolving []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		_, ev, c, _, err := ls.calculateEpochNonce(
			txn,
			epochEnd,
			eras.ConwayEraDesc,
			ls.currentEpoch,
		)
		candidate = c
		evolving = ev
		return err
	}))

	require.Equalf(
		t,
		hex.EncodeToString(nonceAtPreCut),
		hex.EncodeToString(candidate),
		"without the snap-tip checkpoint, the fast path must still "+
			"freeze candidate at the last pre-cutoff block (slot %d) "+
			"via direct slot lookup. Got %x.",
		preCutSlot, candidate,
	)
	require.Equalf(
		t,
		hex.EncodeToString(nonceAtPostCut),
		hex.EncodeToString(evolving),
		"without the snap-tip checkpoint, evolving must still equal "+
			"the last block's stored nonce (slot %d). Got %x.",
		postCutSlot, evolving,
	)
}

// TestComputeEpochNonceForSlot_PostMithrilBootstrapMatchesRollover
// mirrors the basic bootstrap scenario but exercises the header
// verification path (advanceEpochCache → computeEpochNonceForSlot)
// instead of the rollover path (calculateEpochNonce). The two paths
// must agree: header verification of any block in the new epoch
// uses the cached epoch-nonce computed by computeEpochNonceForSlot,
// while the persisted epoch row written by processEpochRollover uses
// calculateEpochNonce. Disagreement means peer headers verifying
// against one nonce while we recompute another — the freeze pattern
// in #2128.
func TestComputeEpochNonceForSlot_PostMithrilBootstrapMatchesRollover(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		snapTipSlot uint64 = 1010
		preCutSlot  uint64 = 1014
		postCutSlot uint64 = 1070
	)

	importedNonce := bytes.Repeat([]byte{0xaa}, 32)
	nonceAtPreCut := bytes.Repeat([]byte{0xbb}, 32)
	nonceAtPostCut := bytes.Repeat([]byte{0xcc}, 32)

	hashAtSnap := bytes.Repeat([]byte{0x10}, 32)
	hashAtPreCut := bytes.Repeat([]byte{0x14}, 32)
	hashAtPostCut := bytes.Repeat([]byte{0x70}, 32)
	prevHashAtSnap := bytes.Repeat([]byte{0x09}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot:     snapTipSlot,
			Hash:     hashAtSnap,
			PrevHash: prevHashAtSnap,
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     preCutSlot,
			Hash:     hashAtPreCut,
			PrevHash: hashAtSnap,
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     postCutSlot,
			Hash:     hashAtPostCut,
			PrevHash: hashAtPreCut,
			Cbor:     []byte{0x80},
			Number:   3,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtSnap, snapTipSlot, importedNonce, true, txn,
		); err != nil {
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
		LastEpochBlockNonce: bytes.Repeat([]byte{0xfa}, 32),
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

	// Header verification path.
	hvNonce, hvEvolving, hvCandidate, hvLab, err :=
		ls.computeEpochNonceForSlot(epochEnd, prevEpoch)
	require.NoError(t, err)

	// Rollover path, run in a transaction (production behaviour).
	var rNonce, rEvolving, rCandidate, rLab []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		n, ev, c, lab, err := ls.calculateEpochNonce(
			txn,
			epochEnd,
			eras.ConwayEraDesc,
			prevEpoch,
		)
		rNonce = n
		rEvolving = ev
		rCandidate = c
		rLab = lab
		return err
	}))

	require.Equalf(
		t,
		hex.EncodeToString(rCandidate),
		hex.EncodeToString(hvCandidate),
		"header-verification candidate must match rollover candidate. "+
			"hv=%x, rollover=%x.", hvCandidate, rCandidate,
	)
	require.Equalf(
		t,
		hex.EncodeToString(rEvolving),
		hex.EncodeToString(hvEvolving),
		"header-verification evolving must match rollover evolving. "+
			"hv=%x, rollover=%x.", hvEvolving, rEvolving,
	)
	require.Equalf(
		t,
		hex.EncodeToString(rNonce),
		hex.EncodeToString(hvNonce),
		"header-verification epoch nonce must match rollover epoch "+
			"nonce. hv=%x, rollover=%x. Disagreement here is the "+
			"#2128 freeze: peer headers pass one nonce, our cache "+
			"verifies against another.", hvNonce, rNonce,
	)
	require.Equalf(
		t,
		hex.EncodeToString(rLab),
		hex.EncodeToString(hvLab),
		"header-verification labNonce must match rollover labNonce. "+
			"hv=%x, rollover=%x.", hvLab, rLab,
	)
	// And separately confirm both paths produce the expected candidate
	// (the last pre-cutoff block's stored nonce), just so a future
	// change that breaks both in lock-step doesn't pass this test.
	require.Equal(
		t,
		hex.EncodeToString(nonceAtPreCut),
		hex.EncodeToString(rCandidate),
		"both paths must freeze candidate at last pre-cutoff block",
	)
}

// newConwayBootstrapStabilityCfg builds a CardanoNodeConfig with k=6,
// f=0.4 so 4k/f = 60 — the Conway nonce stability window. With epoch
// length 75 starting at slot 1000, the candidate-freeze cutoff lands at
// slot 1015, which lets the bootstrap test place blocks on each side of
// the cutoff with single-digit slot gaps.
func newConwayBootstrapStabilityCfg(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, cfg.LoadByronGenesisFromReader(strings.NewReader(`{
		"protocolConsts": {
			"k": 6,
			"protocolMagic": 42
		}
	}`)))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"systemStart": "2026-01-01T00:00:00Z",
		"securityParam": 6,
		"activeSlotsCoeff": 0.4,
		"epochLength": 75,
		"slotLength": 1
	}`)))
	return cfg
}
