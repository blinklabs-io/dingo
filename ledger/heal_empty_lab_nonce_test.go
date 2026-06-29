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
// restores an epoch's empty LastEpochBlockNonce from its boundary block and
// recomputes the next epoch's nonce. A pre-fix BlockBeforeSlot endorser-block
// collision could persist an empty lab, collapsing the next epoch's nonce to
// the NeutralNonce identity (η == candidateNonce) and failing every leader-VRF
// check in that epoch (the Dijkstra/Leios at-tip wedge).
func TestHealEmptyLabNoncesRepairsAndRecomputes(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	// The last block of the epoch preceding epoch 5 (slot < 200). Its
	// PrevHash is the lab value epoch 5 must recover to.
	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     150,
		Hash:     bytes.Repeat([]byte{0x01}, 32),
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     6,
	}, nil))

	candidate := bytes.Repeat([]byte{0xaa}, 32)
	ls := &LedgerState{
		db: db,
		epochCache: []models.Epoch{
			{
				EpochId:             5,
				StartSlot:           200,
				LengthInSlots:       100,
				LastEpochBlockNonce: nil, // corrupted: empty lab
			},
			{
				EpochId:        6,
				StartSlot:      300,
				LengthInSlots:  100,
				CandidateNonce: candidate,
				// NeutralNonce-collapsed (wrong) nonce: η == candidateNonce.
				Nonce: append([]byte(nil), candidate...),
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
		ls.epochCache[0].LastEpochBlockNonce,
		"empty lab must be restored from the boundary block's PrevHash",
	)

	// Epoch 6's nonce is recomputed as candidateNonce ⭒ lab, no longer the
	// NeutralNonce-collapsed value.
	want, err := lcommon.CalculateEpochNonce(candidate, boundaryPrevHash, nil)
	require.NoError(t, err)
	require.Equal(t, want.Bytes(), ls.epochCache[1].Nonce)
	require.NotEqual(
		t,
		candidate,
		ls.epochCache[1].Nonce,
		"epoch nonce must no longer be the NeutralNonce-collapsed candidate",
	)
}

// TestHealEmptyLabNoncesLeavesValidRecordsUntouched verifies the recovery is a
// no-op when no epoch has an empty lab — it must not perturb correct state.
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

func TestLoadEpochsRefreshesCurrentEpochAfterHealing(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	boundaryPrevHash := bytes.Repeat([]byte{0xbb}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     150,
		Hash:     bytes.Repeat([]byte{0x01}, 32),
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
		bytes.Repeat([]byte{0x66}, 32),
		eras.ShelleyEraDesc.Id,
		1,
		100,
		nil,
	))

	want, err := lcommon.CalculateEpochNonce(candidate, boundaryPrevHash, nil)
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
