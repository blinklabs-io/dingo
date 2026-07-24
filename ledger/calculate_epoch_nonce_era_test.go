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
	"github.com/stretchr/testify/require"
)

// TestCalculateEpochNonce_TPraosToPraosUsesSourceEpochStabilityWindow proves
// that the candidate-freeze cutoff at the Alonzo→Babbage epoch boundary is
// driven by the source epoch's protocol family (TPraos, 3k/f) — not the
// post-transition era (Babbage, Praos, 4k/f).
//
// State up to entering this rollover:
//
//   - currentEpoch = Alonzo (epoch 3, EraId=4, slots 225–299)
//   - currentEra   = Babbage (5)              ← already advanced by
//     applyHardForkTransition
//   - k=6, f=0.4 → TPraos stability window = 3k/f = 45 slots,
//     Praos stability window  = 4k/f = 60 slots
//   - Alonzo cutoff (TPraos): 225 + 75 - 45 = 255
//   - Alonzo cutoff (Praos):  225 + 75 - 60 = 240
//
// The test seeds two pre-stored block-nonce rows at slots 230 and 245:
//
//   - slot 230 is below both cutoffs (always contributes to candidate)
//   - slot 245 sits between the Praos cutoff (240) and the TPraos cutoff
//     (255). It should contribute to candidate iff the cutoff is taken
//     from the SOURCE epoch's era (Alonzo, TPraos)
//
// The fast path freezes candidate at the latest pre-cutoff block's stored
// nonce. So:
//
//   - Correct (source-era cutoff = 255): candidate = nonce(slot 245) =
//     0xab*32. The block at slot 245 is the latest block strictly before
//     255.
//   - Buggy   (post-transition cutoff = 240): candidate = nonce(slot 230)
//     = 0x99*32. The block at slot 245 is past the cutoff and does not
//     contribute, so the latest pre-cutoff block is slot 230.
//
// The test asserts the correct value. Today, calculateEpochNonce passes
// `currentEra.Id` (Babbage) into computeCandidateNonce — i.e. the
// post-transition era — which selects the Praos window and produces the
// buggy value. The fix is to pass `currentEpoch.EraId` (the source
// epoch's era) instead, matching what verify_header.go already does and
// what the function's own comment claims it does.
//
// This is the smaller of the two distinct VRF wedges in #2125: the bug
// only fires at TPraos→Praos boundaries (Alonzo→Babbage) because that's
// the only transition where the two stability-window formulas disagree.
// All other era boundaries within TPraos (Shelley→Allegra, Allegra→Mary,
// Mary→Alonzo) and within Praos (Babbage→Conway) use the same multiplier
// either way and therefore mask the bug.
func TestCalculateEpochNonce_TPraosToPraosUsesSourceEpochStabilityWindow(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cfg := newAlonzoToBabbageStabilityCfg(t)

	// Deterministic distinct nonces for slots 230 and 245 so the buggy
	// vs correct candidate values are unambiguous.
	nonceAt230 := bytes.Repeat([]byte{0x99}, 32)
	nonceAt245 := bytes.Repeat([]byte{0xab}, 32)
	hashAt230 := bytes.Repeat([]byte{0x01}, 32)
	hashAt245 := bytes.Repeat([]byte{0x02}, 32)
	prevHashAt230 := bytes.Repeat([]byte{0x10}, 32)
	prevHashAt245 := bytes.Repeat([]byte{0x20}, 32)

	// Insert two Alonzo blocks (slot 230 before any cutoff, slot 245
	// between the Praos and TPraos cutoffs) into the blob store, plus
	// pre-stored block-nonce rows so the fast path can compute the
	// candidate without re-decoding CBOR.
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot: 230, Hash: hashAt230, PrevHash: prevHashAt230,
			Cbor:   []byte{0x80}, // empty CBOR array, never decoded by fast path
			Number: 1, Type: 4,   // Alonzo block type
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot: 245, Hash: hashAt245, PrevHash: prevHashAt245,
			Cbor:   []byte{0x80},
			Number: 2, Type: 4,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAt230, 230, nonceAt230, false, txn,
		); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAt245, 245, nonceAt245, false, txn,
		)
	}))

	// currentTipBlockNonce is intentionally left empty so the
	// resume-from-tip optimisation in computeEpochNonceForSlot
	// /calculateEpochNonce does not short-circuit and the candidate
	// is recomputed across the full epoch range from prevEvolvingNonce.
	ls := &LedgerState{
		db:         db,
		currentEra: eras.BabbageEraDesc,
		currentEpoch: models.Epoch{
			EpochId:             3,
			StartSlot:           225,
			LengthInSlots:       75,
			SlotLength:          1000,
			EraId:               eras.AlonzoEraDesc.Id,
			Nonce:               bytes.Repeat([]byte{0xee}, 32),
			EvolvingNonce:       bytes.Repeat([]byte{0xee}, 32),
			CandidateNonce:      bytes.Repeat([]byte{0xcc}, 32),
			LastEpochBlockNonce: bytes.Repeat([]byte{0xfa}, 32),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Drive the rollover that fires at the Alonzo→Babbage boundary.
	// processEpochRollover is called with currentEra already advanced
	// to Babbage (state.go:2457 passes eras.Eras[workingEraId] after
	// applyHardForkTransition). What we want to assert is that the
	// inner candidate-nonce computation still picks the cutoff slot
	// from the era of the epoch being CLOSED, not the era being
	// entered.
	var candidate []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		_, _, c, _, err := ls.calculateEpochNonce(
			txn,
			ls.currentEpoch.StartSlot+uint64(ls.currentEpoch.LengthInSlots),
			eras.BabbageEraDesc,
			ls.currentEpoch,
		)
		candidate = c
		return err
	}))

	require.Equalf(
		t,
		hex.EncodeToString(nonceAt245),
		hex.EncodeToString(candidate),
		"candidate must freeze at the source epoch's TPraos cutoff "+
			"(slot 255), so the block at slot 245 is the latest "+
			"pre-cutoff contributor and its stored nonce becomes the "+
			"candidate. Got %x — that's the nonce at slot 230, which "+
			"means the freeze cutoff was computed against Babbage's "+
			"Praos window (240) instead of Alonzo's TPraos window "+
			"(255). #2125 Alonzo→Babbage VRF wedge.",
		candidate,
	)
}

// newAlonzoToBabbageStabilityCfg builds a CardanoNodeConfig with concrete
// k and f values that put the Praos cutoff (4k/f = 60) and the TPraos
// cutoff (3k/f = 45) on opposite sides of slot 245 inside an Alonzo epoch
// of length 75 starting at slot 225. The Shelley genesis hash is set so
// computeCandidateNonce's fall-back paths have something to decode.
func newAlonzoToBabbageStabilityCfg(t *testing.T) *cardano.CardanoNodeConfig {
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
