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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestHealEmptyLabNoncesRepairsAndRecomputes verifies that healEmptyLabNonces
// restores an epoch's empty LastEpochBlockNonce from its boundary block's
// PrevHash and recomputes that epoch's nonce from the PREVIOUS epoch's carried
// lab (η(E) = candidate(E) ⭒ lab(E-1), the cardano-ledger assembly — NOT the
// epoch's own lab, which would shift eta by one epoch). A pre-fix
// BlockBeforeSlot endorser-block collision could persist an empty lab,
// collapsing the next epoch's nonce to the NeutralNonce identity
// (η == candidateNonce) and failing every leader-VRF check in that epoch (the
// Dijkstra/Leios at-tip wedge).
func TestHealEmptyLabNoncesRepairsAndRecomputes(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	// The last block of the epoch preceding epoch 5 (slot < 200). Its PrevHash
	// is the lab value epoch 5 must recover to.
	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     150,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	// Epoch 4 is covered by the Mithril trust boundary: its imported carried
	// lab is trusted verbatim and feeds epoch 5's nonce.
	carriedLab := bytes.Repeat([]byte{0xfa}, 32)
	ls := &LedgerState{
		db:                db,
		mithrilLedgerSlot: 150,
		epochCache: []models.Epoch{
			{
				EpochId:             4,
				StartSlot:           100,
				LengthInSlots:       100,
				Nonce:               bytes.Repeat([]byte{0xee}, 32),
				CandidateNonce:      bytes.Repeat([]byte{0xed}, 32),
				LastEpochBlockNonce: carriedLab,
			},
			{
				EpochId:        5,
				StartSlot:      200,
				LengthInSlots:  100,
				CandidateNonce: candidate,
				// NeutralNonce-collapsed (wrong) nonce: η == candidateNonce.
				Nonce:               append([]byte(nil), candidate...),
				LastEpochBlockNonce: nil, // corrupted: empty lab
			},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.healEmptyLabNonces()

	// Epoch 5's lab is recovered from the boundary block's PrevHash.
	require.Equal(
		t,
		boundaryPrevHash,
		ls.epochCache[1].LastEpochBlockNonce,
		"empty lab must be restored from the boundary block's PrevHash",
	)

	// Epoch 5's nonce is recomputed as candidateNonce ⭒ epoch 4's carried lab,
	// no longer the NeutralNonce-collapsed value.
	want, err := lcommon.CalculateEpochNonce(candidate, carriedLab, nil)
	require.NoError(t, err)
	require.Equal(t, want.Bytes(), ls.epochCache[1].Nonce)
	require.NotEqual(
		t,
		candidate,
		ls.epochCache[1].Nonce,
		"epoch nonce must no longer be the NeutralNonce-collapsed candidate",
	)
	// The one-epoch-shifted assembly (candidate ⭒ epoch 5's OWN lab) must NOT
	// be produced — that is the #2734 divergence.
	shifted, err := lcommon.CalculateEpochNonce(candidate, boundaryPrevHash, nil)
	require.NoError(t, err)
	require.NotEqual(
		t,
		shifted.Bytes(),
		ls.epochCache[1].Nonce,
		"epoch nonce must not mix the candidate with the epoch's own lab",
	)
}

// TestHealEmptyLabNoncesBoundsToRecentEpochs verifies the repair only touches
// the recent window: repairing every historical epoch on each restart is one
// block lookup per epoch and needlessly slow, and older labs never feed a
// runtime nonce, so an epoch older than the window must be left untouched even
// when it has a repairable (empty) lab.
func TestHealEmptyLabNoncesBoundsToRecentEpochs(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	// A single boundary block precedes every epoch's start slot, so any epoch
	// that is actually processed repairs its empty lab to this PrevHash.
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       1,
		Slot:     50,
		Hash:     bytes.Repeat([]byte{0x01}, 32),
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   1,
		Type:     6,
	}, nil))

	const n = healLabNonceRecentEpochs + 3
	epochs := make([]models.Epoch, n)
	for i := range epochs {
		epochs[i] = models.Epoch{
			EpochId:        uint64(i + 1),
			StartSlot:      uint64((i + 1) * 100),
			LengthInSlots:  100,
			Nonce:          bytes.Repeat([]byte{0xee}, 32),
			CandidateNonce: bytes.Repeat([]byte{0xaa}, 32),
			// LastEpochBlockNonce left empty (repairable)
		}
	}
	ls := &LedgerState{
		db:         db,
		epochCache: epochs,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.healEmptyLabNonces()

	require.Empty(t, ls.epochCache[0].LastEpochBlockNonce,
		"epoch older than the recent window must be left untouched, not repaired")
	require.Equal(t, boundaryPrevHash, ls.epochCache[n-1].LastEpochBlockNonce,
		"the most recent epoch must still be repaired")
}

// TestHealEmptyLabNoncesRepairsOldestInWindowNonce verifies the bounded scan
// includes one predecessor epoch so the oldest in-window epoch's nonce — which
// mixes the PREVIOUS epoch's lab — is repaired from a verified predecessor lab
// rather than left stale. Without the predecessor the first in-window nonce
// check has no verified previous lab and is skipped.
func TestHealEmptyLabNoncesRepairsOldestInWindowNonce(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	prevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       1,
		Slot:     50,
		Hash:     bytes.Repeat([]byte{0x01}, 32),
		PrevHash: prevHash,
		Cbor:     []byte{0x80},
		Number:   1,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	const n = healLabNonceRecentEpochs + 3
	epochs := make([]models.Epoch, n)
	for i := range epochs {
		epochs[i] = models.Epoch{
			EpochId:        uint64(i + 1),
			StartSlot:      uint64((i + 1) * 100),
			LengthInSlots:  100,
			CandidateNonce: candidate,
			// NeutralNonce-collapsed (wrong) nonce and empty lab, both repairable.
			Nonce: append([]byte(nil), candidate...),
		}
	}
	ls := &LedgerState{
		db:         db,
		epochCache: epochs,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.healEmptyLabNonces()

	// The oldest in-window epoch is scanned right after the predecessor whose
	// lab the scan verifies. Its nonce must be recomputed as candidate ⭒ the
	// predecessor's repaired lab (prevHash), not left as the collapsed candidate.
	oldest := n - healLabNonceRecentEpochs
	want, err := lcommon.CalculateEpochNonce(candidate, prevHash, nil)
	require.NoError(t, err)
	require.Equal(t, want.Bytes(), ls.epochCache[oldest].Nonce,
		"oldest in-window epoch nonce must be repaired from the verified predecessor lab")
	require.NotEqual(t, candidate, ls.epochCache[oldest].Nonce,
		"oldest in-window epoch nonce must no longer be the collapsed candidate")
}

// TestHealEmptyLabNoncesLeavesValidRecordsUntouched verifies the recovery is a
// no-op when no epoch has a repairable lab mismatch — it must not perturb
// correct state.
func TestHealEmptyLabNoncesLeavesValidRecordsUntouched(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	lab := bytes.Repeat([]byte{0xcc}, 32)
	nonce := bytes.Repeat([]byte{0xdd}, 32)
	ls := &LedgerState{
		db: db,
		epochCache: []models.Epoch{
			{
				EpochId:             6,
				StartSlot:           300,
				LengthInSlots:       100,
				LastEpochBlockNonce: lab,
				Nonce:               nonce,
				CandidateNonce:      bytes.Repeat([]byte{0xaa}, 32),
			},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.healEmptyLabNonces()

	require.Equal(t, lab, ls.epochCache[0].LastEpochBlockNonce)
	require.Equal(t, nonce, ls.epochCache[0].Nonce)
}

func TestHealEmptyLabNoncesLeavesParentHashLabUntouched(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     250,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	nonce, err := lcommon.CalculateEpochNonce(candidate, boundaryPrevHash, nil)
	require.NoError(t, err)
	epochs := []models.Epoch{
		{
			EpochId:             6,
			StartSlot:           300,
			LengthInSlots:       100,
			Nonce:               nonce.Bytes(),
			CandidateNonce:      candidate,
			LastEpochBlockNonce: boundaryPrevHash,
		},
	}
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	repaired := ls.healEmptyLabNoncesInPlace(epochs)

	require.False(t, repaired)
	require.Equal(t, boundaryPrevHash, epochs[0].LastEpochBlockNonce)
	require.NotEqual(t, boundaryHash, epochs[0].LastEpochBlockNonce)
	require.Equal(t, nonce.Bytes(), epochs[0].Nonce)
}

// TestHealEmptyLabNoncesRepairsLabWhenCandidateMissing verifies that the lab
// repair does not depend on a stored candidate nonce: the lab feeds the NEXT
// boundary's eta directly, so leaving it in a stale shape just because this
// epoch's nonce cannot be re-verified would wedge the next rollover. The
// nonce itself must be left untouched (no candidate to recompute it from).
func TestHealEmptyLabNoncesRepairsLabWhenCandidateMissing(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     250,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	cfg := newConwayBootstrapStabilityCfg(t)
	oldLab := bytes.Repeat([]byte{0x99}, 32)
	oldNonce := bytes.Repeat([]byte{0xaa}, 32)
	epochs := []models.Epoch{
		{
			EpochId:             6,
			StartSlot:           300,
			LengthInSlots:       100,
			Nonce:               oldNonce,
			CandidateNonce:      nil,
			LastEpochBlockNonce: oldLab,
		},
	}
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	repaired := ls.healEmptyLabNoncesInPlace(epochs)

	require.True(t, repaired)
	require.Equal(t, boundaryPrevHash, epochs[0].LastEpochBlockNonce,
		"stale lab must be repaired even without a stored candidate")
	require.Equal(t, oldNonce, epochs[0].Nonce,
		"nonce must be untouched when no candidate is stored")
	require.Empty(t, epochs[0].CandidateNonce)
}

// TestHealEmptyLabNoncesRepairsEmptyLabWithoutCandidate mirrors the test above
// for an empty (rather than stale) lab.
func TestHealEmptyLabNoncesRepairsEmptyLabWithoutCandidate(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     250,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	oldNonce := bytes.Repeat([]byte{0xaa}, 32)
	epochs := []models.Epoch{
		{
			EpochId:             6,
			StartSlot:           300,
			LengthInSlots:       100,
			Nonce:               oldNonce,
			CandidateNonce:      nil,
			LastEpochBlockNonce: nil,
		},
	}
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	repaired := ls.healEmptyLabNoncesInPlace(epochs)

	require.True(t, repaired)
	require.Equal(t, boundaryPrevHash, epochs[0].LastEpochBlockNonce,
		"empty lab must be repaired even without a stored candidate")
	require.Equal(t, oldNonce, epochs[0].Nonce,
		"nonce must be untouched when no candidate is stored")
}

func TestHealEmptyLabNoncesSkipsMissingCandidateBeforeBoundaryLookup(
	t *testing.T,
) {
	oldLab := bytes.Repeat([]byte{0x99}, 32)
	oldNonce := bytes.Repeat([]byte{0xaa}, 32)
	epochs := []models.Epoch{
		{
			EpochId:             6,
			StartSlot:           300,
			LengthInSlots:       100,
			Nonce:               oldNonce,
			CandidateNonce:      nil,
			LastEpochBlockNonce: oldLab,
		},
	}
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	require.NotPanics(t, func() {
		repaired := ls.healEmptyLabNoncesInPlace(epochs)
		require.False(t, repaired)
	})
	require.Equal(t, oldLab, epochs[0].LastEpochBlockNonce)
	require.Equal(t, oldNonce, epochs[0].Nonce)
}

// TestHealEmptyLabNoncesLeavesFirstPraosEpochLabNeutral verifies the heal does
// not "repair" the first Praos epoch's nil lab to the last pre-Praos (Byron)
// block's parent hash. cardano-ledger initializes praosStateLastEpochBlockNonce
// to NeutralNonce at the Praos start (initialChainDepState csLabNonce =
// NeutralNonce), and the initial-epoch branch of calculateEpochNonce stores a
// nil lab — rewriting it would diverge the next boundary's eta on any chain
// with a Byron era (mainnet, preprod).
func TestHealEmptyLabNoncesLeavesFirstPraosEpochLabNeutral(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	// Last Byron block before the Shelley start at slot 200. Without the
	// first-Praos guard, its PrevHash would be written as the lab.
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     150,
		Hash:     bytes.Repeat([]byte{0x01}, 32),
		PrevHash: bytes.Repeat([]byte{0xbb}, 32),
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     1, // Byron main
	}, nil))

	genesisNonce := bytes.Repeat([]byte{0x11}, 32)
	epochs := []models.Epoch{
		{
			// Pre-Praos (Byron) epoch: no nonce, no candidate, no lab.
			EpochId:       3,
			StartSlot:     100,
			LengthInSlots: 100,
		},
		{
			// First Praos epoch: nonce/candidate are the genesis nonce, lab is
			// Neutral (nil).
			EpochId:        4,
			StartSlot:      200,
			LengthInSlots:  100,
			Nonce:          append([]byte(nil), genesisNonce...),
			CandidateNonce: append([]byte(nil), genesisNonce...),
		},
	}
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	repaired := ls.healEmptyLabNoncesInPlace(epochs)

	require.False(t, repaired)
	require.Empty(t, epochs[0].LastEpochBlockNonce)
	require.Empty(t, epochs[1].LastEpochBlockNonce,
		"first Praos epoch's lab must stay NeutralNonce (nil)")
	require.Equal(t, genesisNonce, epochs[1].Nonce,
		"first Praos epoch's nonce must stay the genesis nonce")
}

func TestHealEmptyLabNoncesTrustsMithrilCoveredEpoch(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     250,
		Hash:     boundaryHash,
		PrevHash: bytes.Repeat([]byte{0xbb}, 32),
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	importedLab := bytes.Repeat([]byte{0x99}, 32)
	importedNonce := bytes.Repeat([]byte{0xdd}, 32)
	epochs := []models.Epoch{
		{
			EpochId:             6,
			StartSlot:           300,
			LengthInSlots:       100,
			Nonce:               importedNonce,
			CandidateNonce:      candidate,
			LastEpochBlockNonce: importedLab,
		},
	}
	ls := &LedgerState{
		db:                db,
		mithrilLedgerSlot: 350,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	repaired := ls.healEmptyLabNoncesInPlace(epochs)

	require.False(t, repaired)
	require.Equal(t, importedLab, epochs[0].LastEpochBlockNonce)
	require.Equal(t, importedNonce, epochs[0].Nonce)
	require.NotEqual(t, boundaryHash, epochs[0].LastEpochBlockNonce)
}

func TestHealEmptyLabNoncesInPlaceRepairsReloadedEpochs(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     250,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	// Epoch 5 is Mithril-trusted; its carried lab feeds epoch 6's nonce.
	carriedLab := bytes.Repeat([]byte{0xfa}, 32)
	epochs := []models.Epoch{
		{
			EpochId:             5,
			StartSlot:           200,
			LengthInSlots:       100,
			Nonce:               bytes.Repeat([]byte{0xee}, 32),
			CandidateNonce:      bytes.Repeat([]byte{0xed}, 32),
			LastEpochBlockNonce: carriedLab,
		},
		{
			EpochId:             6,
			StartSlot:           300,
			LengthInSlots:       100,
			Nonce:               append([]byte(nil), candidate...),
			CandidateNonce:      candidate,
			LastEpochBlockNonce: nil,
		},
	}
	ls := &LedgerState{
		db:                db,
		mithrilLedgerSlot: 250,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	repaired := ls.healEmptyLabNoncesInPlace(epochs)

	want, err := lcommon.CalculateEpochNonce(candidate, carriedLab, nil)
	require.NoError(t, err)
	require.True(t, repaired)
	require.Equal(t, boundaryPrevHash, epochs[1].LastEpochBlockNonce)
	require.Equal(t, want.Bytes(), epochs[1].Nonce)
}

func TestLoadEpochsRefreshesCurrentEpochAfterHealing(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	// Last block of epoch 4 (before slot 200): its PrevHash is epoch 5's lab.
	epoch5BoundaryPrevHash := bytes.Repeat([]byte{0x44}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       2,
		Slot:     150,
		Hash:     bytes.Repeat([]byte{0x02}, 32),
		PrevHash: epoch5BoundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   2,
		Type:     6,
	}, nil))
	// Last block of epoch 5 (before slot 300): its PrevHash is epoch 6's lab.
	boundaryHash := bytes.Repeat([]byte{0x01}, 32)
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     250,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	require.NoError(t, db.SetEpoch(
		200,
		5,
		bytes.Repeat([]byte{0x55}, 32),
		nil,
		nil,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		100,
		nil,
	))
	require.NoError(t, db.SetEpoch(
		300,
		6,
		append([]byte(nil), candidate...),
		nil,
		candidate,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		100,
		nil,
	))

	// Epoch 6's nonce mixes its candidate with epoch 5's carried lab (repaired
	// to the epoch-5 boundary block's PrevHash), not with epoch 6's own lab.
	want, err := lcommon.CalculateEpochNonce(
		candidate,
		epoch5BoundaryPrevHash,
		nil,
	)
	require.NoError(t, err)

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	require.NoError(t, ls.loadEpochs(nil))

	require.Equal(t, want.Bytes(), ls.epochCache[1].Nonce)
	require.Equal(t, want.Bytes(), ls.currentEpoch.Nonce)
	require.Equal(t, want.Bytes(), ls.EpochNonce(6))
	require.NotEqual(t, candidate, ls.EpochNonce(6))
}
