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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// TestEpochNonce_SnapshotTipPastCutoff covers the one Mithril-bootstrap
// shape the existing #2128 suite does not: a snapshot whose tip slot lies
// PAST the candidate-freeze cutoff of its epoch. In that shape the imported
// epoch row carries CandidateNonce != EvolvingNonce — psCandidateNonce
// froze at the cutoff (before the tip) while psEvolvingNonce kept rolling
// to the tip. The existing tests always set the two equal (snapshot taken
// before the cutoff), so this exercises the distinct-values path.
//
// Expected: the bootstrap epoch's rollover must return candidate equal to
// the imported (frozen) CandidateNonce — NOT the imported EvolvingNonce,
// and NOT a value re-derived from a pre-cutoff block (there are none with
// stored nonces; pre-cutoff blocks were imported as immutable). Both the
// rollover path (calculateEpochNonce) and the header-verification path
// (computeEpochNonceForSlot) must agree, because the first header of the
// new epoch is verified against the eagerly-cached value.
func TestEpochNonce_SnapshotTipPastCutoff(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	cfg := newConwayBootstrapStabilityCfg(t)

	// k=6, f=0.4 -> 4k/f = 60. Epoch [1000,1075), cutoff = 1075-60 = 1015.
	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		cutoffSlot  uint64 = 1015
		preImpSlot  uint64 = 1010 // pre-cutoff, imported immutable (no nonce row)
		snapTipSlot uint64 = 1040 // snapshot tip: PAST the cutoff
		postCutSlot uint64 = 1070 // last block of epoch (post-import)
	)

	// Imported tip-time evolving nonce (psEvolvingNonce at slot 1040).
	importedEvolving := bytes.Repeat([]byte{0xaa}, 32)
	// Imported frozen candidate (psCandidateNonce, frozen at the cutoff
	// well before the tip). Distinct from evolving on purpose.
	importedCandidate := bytes.Repeat([]byte{0xdd}, 32)
	// Per-block evolving nonce stored by post-import processing.
	nonceAtPostCut := bytes.Repeat([]byte{0xcc}, 32)

	hashPreImp := bytes.Repeat([]byte{0x10}, 32)
	hashAtSnap := bytes.Repeat([]byte{0x40}, 32)
	hashAtPostCut := bytes.Repeat([]byte{0x70}, 32)
	prevHashPreImp := bytes.Repeat([]byte{0x09}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		// Pre-cutoff block imported as immutable: present in the blob
		// store, but with NO block_nonce row (importTip only checkpoints
		// the tip).
		if err := db.BlockCreate(models.Block{
			Slot:     preImpSlot,
			Hash:     hashPreImp,
			PrevHash: prevHashPreImp,
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		// Snapshot tip block (past the cutoff).
		if err := db.BlockCreate(models.Block{
			Slot:     snapTipSlot,
			Hash:     hashAtSnap,
			PrevHash: hashPreImp,
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		// Post-import block.
		if err := db.BlockCreate(models.Block{
			Slot:     postCutSlot,
			Hash:     hashAtPostCut,
			PrevHash: hashAtSnap,
			Cbor:     []byte{0x80},
			Number:   3,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		// importTip checkpoint at the snapshot tip = imported evolving.
		if err := db.SetBlockNonce(
			hashAtSnap, snapTipSlot, importedEvolving, true, txn,
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
		EvolvingNonce:       importedEvolving,
		CandidateNonce:      importedCandidate,
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

	// Header-verification (eager) path.
	hvNonce, hvEvolving, hvCandidate, hvLab, err :=
		ls.computeEpochNonceForSlot(epochEnd, prevEpoch)
	require.NoError(t, err)

	// Rollover (authoritative) path.
	var rNonce, rEvolving, rCandidate, rLab []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		n, ev, c, lab, err := ls.calculateEpochNonce(
			txn, epochEnd, eras.ConwayEraDesc, prevEpoch,
		)
		rNonce, rEvolving, rCandidate, rLab = n, ev, c, lab
		return err
	}))

	require.Equalf(
		t,
		hex.EncodeToString(importedCandidate),
		hex.EncodeToString(rCandidate),
		"with the snapshot tip past the cutoff (tip=%d, cutoff=%d), the "+
			"frozen candidate is the imported psCandidateNonce. Got %x. "+
			"If this equals importedEvolving (0xaa...) the computation "+
			"confused evolving for candidate.",
		snapTipSlot, cutoffSlot, rCandidate,
	)
	require.Equalf(
		t,
		hex.EncodeToString(nonceAtPostCut),
		hex.EncodeToString(rEvolving),
		"evolving must equal the last block's stored nonce (slot %d). Got %x.",
		postCutSlot, rEvolving,
	)
	require.NotEqual(
		t,
		hex.EncodeToString(rCandidate),
		hex.EncodeToString(rEvolving),
		"candidate (frozen pre-tip) and evolving (at tip) must differ",
	)

	// Eager path must match rollover, or the first new-epoch header is
	// verified against a nonce the rollover later disagrees with -> VRF
	// failure at turnover.
	require.Equal(t,
		hex.EncodeToString(rCandidate), hex.EncodeToString(hvCandidate),
		"eager candidate must match rollover candidate")
	require.Equal(t,
		hex.EncodeToString(rEvolving), hex.EncodeToString(hvEvolving),
		"eager evolving must match rollover evolving")
	require.Equalf(t,
		hex.EncodeToString(rNonce), hex.EncodeToString(hvNonce),
		"eager epoch nonce must match rollover epoch nonce. hv=%x rollover=%x",
		hvNonce, rNonce)
	require.Equal(t,
		hex.EncodeToString(rLab), hex.EncodeToString(hvLab),
		"eager labNonce must match rollover labNonce")
}
