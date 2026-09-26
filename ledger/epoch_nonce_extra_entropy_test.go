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
	"encoding/hex"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// Mainnet epoch 259 is the only epoch in Cardano's history whose protocol
// parameters carried a non-neutral extraEntropy. The values below are the real
// inputs and the real resulting eta0, so a node that drops the extraEntropy
// term computes a different leader schedule for that whole epoch and rejects
// every header in it.
//
//   - extraEntropy and the resulting nonce: Koios epoch_params for epoch 259
//     (extra_entropy / nonce); every other epoch from 208 to 656 carries
//     extra_entropy null.
//   - the same triple is pinned as a test vector in gouroboros
//     ledger/common/nonce_test.go.
//
// The carried lab (mainnetEpoch259Lab) is a mainnet block in epoch 257, which
// is what identifies the consuming epoch as 259 rather than 260: the TICKN
// state's prev-hash nonce lags the boundary by one epoch, so the boundary INTO
// epoch E mixes a block hash from epoch E-2.
const (
	mainnetEpoch259ExtraEntropy = "d982e06fd33e7440b43cefad529b7ecafbaa255e38178ad4189a37e4ce9bf1fa"
	mainnetEpoch259Candidate    = "d1340a9c1491f0face38d41fd5c82953d0eb48320d65e952414a0c5ebaf87587"
	mainnetEpoch259Lab          = "ee91d679b0a6ce3015b894c575c799e971efac35c7a8cbdc2b3f579005e69abd"
	mainnetEpoch259Nonce        = "0022cfa563a5328c4fb5c8017121329e964c26ade5d167b1bd9b2ec967772b60"
)

// maryPParamsWithExtraEntropy returns a minimally-populated Mary parameter set
// carrying extraEntropy, plus its CBOR. A nil rational field encodes as CBOR
// null and decodes back, so only ExtraEntropy needs a real value here.
func maryPParamsWithExtraEntropy(
	t *testing.T,
	entropy []byte,
) (*mary.MaryProtocolParameters, []byte) {
	t.Helper()
	pp := &mary.MaryProtocolParameters{
		ProtocolMajor: 4,
	}
	if len(entropy) == lcommon.Blake2b256Size {
		pp.ExtraEntropy.Type = lcommon.NonceTypeNonce
		copy(pp.ExtraEntropy.Value[:], entropy)
	}
	data, err := cbor.Encode(pp)
	require.NoError(t, err)
	// Guard the fixture: the production path reads this back through the era's
	// own decoder, so a shape that does not round-trip would make the test
	// pass for the wrong reason.
	decoded, err := eras.DecodePParamsMary(data)
	require.NoError(t, err)
	decodedMary, ok := decoded.(*mary.MaryProtocolParameters)
	require.True(t, ok)
	require.Equal(t, pp.ExtraEntropy, decodedMary.ExtraEntropy)
	return pp, data
}

// TestComputeEpochNonceForSlotFoldsExtraEntropy covers the header-verification
// path (advanceEpochCache -> computeEpochNonceForSlot), which computes the new
// epoch's nonce speculatively, before the rollover enacts that epoch's
// protocol parameters. The extraEntropy it must fold therefore comes from the
// pending update submitted in the previous epoch, exactly as cardano-ledger's
// TICKF forecast supplies it to TICKN.
func TestComputeEpochNonceForSlotFoldsExtraEntropy(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		preCutSlot  uint64 = 1014
		postCutSlot uint64 = 1070
		prevEpochID uint64 = 258
	)

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	frozenCandidate := mustDecodeHex(t, mainnetEpoch259Candidate)
	carriedLab := mustDecodeHex(t, mainnetEpoch259Lab)

	importedNonce := mustDecodeHex(t, mainnetEpoch259Lab)
	nonceAtPostCut := mustDecodeHex(t, mainnetEpoch259Nonce)
	hashAtPreCut := mustDecodeHex(t, mainnetEpoch259Candidate)
	hashAtPostCut := mustDecodeHex(t, mainnetEpoch259Nonce)
	prevHashAtPreCut := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot: preCutSlot, Hash: hashAtPreCut, PrevHash: prevHashAtPreCut,
			Cbor: []byte{0x80}, Number: 1, Type: mary.BlockTypeMary,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot: postCutSlot, Hash: hashAtPostCut, PrevHash: hashAtPreCut,
			Cbor: []byte{0x80}, Number: 2, Type: mary.BlockTypeMary,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtPreCut, preCutSlot, frozenCandidate, false, txn); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn)
	}))

	// Parameters in effect for the ending epoch: no extra entropy yet.
	_, baseCbor := maryPParamsWithExtraEntropy(t, nil)
	require.NoError(t, db.SetPParams(
		baseCbor, epochStart, prevEpochID, eras.MaryEraDesc.Id, nil,
	))

	// The genesis-key update proposal submitted during the ending epoch, which
	// the rollover will enact as the new epoch's parameters.
	entropyNonce := lcommon.Nonce{Type: lcommon.NonceTypeNonce}
	copy(entropyNonce.Value[:], entropy)
	// A parameter update is a sparse map. Encoding the struct also serializes
	// unset rational fields as null, which the classic update decoder rejects.
	updateCbor, err := cbor.Encode(map[uint]any{
		13: entropyNonce,
	})
	require.NoError(t, err)
	require.NoError(t, db.SetPParamUpdate(
		[]byte{0x01}, updateCbor, epochStart+1, prevEpochID, nil,
	))

	prevEpoch := models.Epoch{
		EpochId:             prevEpochID,
		StartSlot:           epochStart,
		LengthInSlots:       uint(epochLength),
		SlotLength:          1000,
		EraId:               eras.MaryEraDesc.Id,
		Nonce:               mustDecodeHex(t, mainnetEpoch259Lab),
		EvolvingNonce:       importedNonce,
		CandidateNonce:      importedNonce,
		LastEpochBlockNonce: carriedLab,
	}

	ls := &LedgerState{
		db:           db,
		currentEra:   eras.MaryEraDesc,
		currentEpoch: prevEpoch,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	nonce, _, candidate, _, err := ls.computeEpochNonceForSlot(
		epochEnd, prevEpoch,
	)
	require.NoError(t, err)
	// Guard the fixture: the frozen candidate must be the mainnet value, or
	// the nonce comparison below would be testing arithmetic on other inputs.
	require.Equal(
		t,
		frozenCandidate,
		candidate,
		"candidate nonce must freeze at the pre-cutoff block nonce",
	)

	withoutEntropy, err := lcommon.CalculateEpochNonce(
		frozenCandidate, carriedLab, nil,
	)
	require.NoError(t, err)
	require.NotEqual(
		t,
		withoutEntropy.Bytes(),
		nonce,
		"epoch nonce must not be the extraEntropy-free value",
	)
	require.Equal(
		t,
		mainnetEpoch259Nonce,
		hex.EncodeToString(nonce),
		"epoch nonce must fold the pending extraEntropy update",
	)
}

// TestCalculateEpochNonceFoldsExtraEntropy covers the authoritative rollover
// path, which writes the epoch record every later consumer reads. Unlike the
// header-verification path it does not forecast: the boundary has already
// enacted the new epoch's protocol parameters, and their extraEntropy is what
// the nonce must mix.
func TestCalculateEpochNonceFoldsExtraEntropy(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		preCutSlot  uint64 = 1014
		postCutSlot uint64 = 1070
		prevEpochID uint64 = 258
	)

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	frozenCandidate := mustDecodeHex(t, mainnetEpoch259Candidate)
	carriedLab := mustDecodeHex(t, mainnetEpoch259Lab)

	importedNonce := mustDecodeHex(t, mainnetEpoch259Lab)
	nonceAtPostCut := mustDecodeHex(t, mainnetEpoch259Nonce)
	hashAtPreCut := mustDecodeHex(t, mainnetEpoch259Candidate)
	hashAtPostCut := mustDecodeHex(t, mainnetEpoch259Nonce)
	prevHashAtPreCut := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot: preCutSlot, Hash: hashAtPreCut, PrevHash: prevHashAtPreCut,
			Cbor: []byte{0x80}, Number: 1, Type: mary.BlockTypeMary,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot: postCutSlot, Hash: hashAtPostCut, PrevHash: hashAtPreCut,
			Cbor: []byte{0x80}, Number: 2, Type: mary.BlockTypeMary,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtPreCut, preCutSlot, frozenCandidate, false, txn); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn)
	}))

	prevEpoch := models.Epoch{
		EpochId:             prevEpochID,
		StartSlot:           epochStart,
		LengthInSlots:       uint(epochLength),
		SlotLength:          1000,
		EraId:               eras.MaryEraDesc.Id,
		Nonce:               mustDecodeHex(t, mainnetEpoch259Lab),
		EvolvingNonce:       importedNonce,
		CandidateNonce:      importedNonce,
		LastEpochBlockNonce: carriedLab,
	}

	ls := &LedgerState{
		db:           db,
		currentEra:   eras.MaryEraDesc,
		currentEpoch: prevEpoch,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	enacted, _ := maryPParamsWithExtraEntropy(t, entropy)
	neutral, _ := maryPParamsWithExtraEntropy(t, nil)

	var withEntropy, withoutParam []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		n, _, candidate, _, err := ls.calculateEpochNonce(
			txn, epochEnd, eras.MaryEraDesc, prevEpoch, enacted,
		)
		if err != nil {
			return err
		}
		require.Equal(
			t,
			frozenCandidate,
			candidate,
			"candidate nonce must freeze at the pre-cutoff block nonce",
		)
		withEntropy = n
		n, _, _, _, err = ls.calculateEpochNonce(
			txn, epochEnd, eras.MaryEraDesc, prevEpoch, neutral,
		)
		withoutParam = n
		return err
	}))

	require.Equal(
		t,
		mainnetEpoch259Nonce,
		hex.EncodeToString(withEntropy),
		"epoch nonce must fold the enacted extraEntropy",
	)

	// Negative case: the same inputs with a neutral extraEntropy must produce
	// the unmixed nonce, so the parameter is what moves the result rather than
	// anything else in the fixture.
	expectedNeutral, err := lcommon.CalculateEpochNonce(
		frozenCandidate, carriedLab, nil,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		expectedNeutral.Bytes(),
		withoutParam,
		"a neutral extraEntropy must leave the epoch nonce unmixed",
	)
}

// TestCalculateEpochNonceNeutralLabMixesExtraEntropy covers the boundary where
// the carried lastEpochBlockNonce is NeutralNonce and the extraEntropy is not.
// NeutralNonce is the identity of the nonce operator, so the assembly collapses
// to candidateNonce ⭒ extraEntropy -- not to candidateNonce alone, which is
// what the NeutralNonce short-circuit returns when the entropy term is dropped.
func TestCalculateEpochNonceNeutralLabMixesExtraEntropy(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		preCutSlot  uint64 = 1014
		postCutSlot uint64 = 1070
		prevEpochID uint64 = 258
	)

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	frozenCandidate := mustDecodeHex(t, mainnetEpoch259Candidate)

	importedNonce := mustDecodeHex(t, mainnetEpoch259Lab)
	nonceAtPostCut := mustDecodeHex(t, mainnetEpoch259Nonce)
	hashAtPreCut := mustDecodeHex(t, mainnetEpoch259Candidate)
	hashAtPostCut := mustDecodeHex(t, mainnetEpoch259Nonce)
	prevHashAtPreCut := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot: preCutSlot, Hash: hashAtPreCut, PrevHash: prevHashAtPreCut,
			Cbor: []byte{0x80}, Number: 1, Type: mary.BlockTypeMary,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot: postCutSlot, Hash: hashAtPostCut, PrevHash: hashAtPreCut,
			Cbor: []byte{0x80}, Number: 2, Type: mary.BlockTypeMary,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtPreCut, preCutSlot, frozenCandidate, false, txn); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn)
	}))

	prevEpoch := models.Epoch{
		EpochId:        prevEpochID,
		StartSlot:      epochStart,
		LengthInSlots:  uint(epochLength),
		SlotLength:     1000,
		EraId:          eras.MaryEraDesc.Id,
		Nonce:          mustDecodeHex(t, mainnetEpoch259Lab),
		EvolvingNonce:  importedNonce,
		CandidateNonce: importedNonce,
		// NeutralNonce: no carried last-block nonce.
		LastEpochBlockNonce: nil,
	}

	ls := &LedgerState{
		db:           db,
		currentEra:   eras.MaryEraDesc,
		currentEpoch: prevEpoch,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	enacted, _ := maryPParamsWithExtraEntropy(t, entropy)

	var nonce []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		n, _, _, _, err := ls.calculateEpochNonce(
			txn, epochEnd, eras.MaryEraDesc, prevEpoch, enacted,
		)
		nonce = n
		return err
	}))

	require.NotEqual(
		t,
		frozenCandidate,
		nonce,
		"a non-neutral extraEntropy must not leave the candidate nonce unmixed",
	)
	want, err := lcommon.CalculateRollingNonce(frozenCandidate, entropy)
	require.NoError(t, err)
	require.Equal(t, want.Bytes(), nonce)
}

// TestHealEmptyLabNoncesFoldsExtraEntropy covers the startup lab-recovery path,
// which recomputes a stored epoch's nonce from chain data. That epoch is in the
// past, so its extraEntropy comes from the protocol parameters recorded for it
// rather than from a forecast.
func TestHealEmptyLabNoncesFoldsExtraEntropy(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	const (
		entropyEpoch uint64 = 259
		prevEpoch    uint64 = 258
	)

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	candidate := mustDecodeHex(t, mainnetEpoch259Candidate)
	carriedLab := mustDecodeHex(t, mainnetEpoch259Lab)

	boundaryHash := mustDecodeHex(t, mainnetEpoch259Nonce)
	boundaryPrevHash := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:       3,
		Slot:     150,
		Hash:     boundaryHash,
		PrevHash: boundaryPrevHash,
		Cbor:     []byte{0x80},
		Number:   3,
		Type:     mary.BlockTypeMary,
	}, nil))

	_, entropyCbor := maryPParamsWithExtraEntropy(t, entropy)
	require.NoError(t, db.SetPParams(
		entropyCbor, 200, entropyEpoch, eras.MaryEraDesc.Id, nil,
	))

	ls := &LedgerState{
		db:                db,
		mithrilLedgerSlot: 150,
		epochCache: []models.Epoch{
			{
				EpochId:             prevEpoch,
				StartSlot:           100,
				LengthInSlots:       100,
				EraId:               eras.MaryEraDesc.Id,
				Nonce:               mustDecodeHex(t, mainnetEpoch259Lab),
				CandidateNonce:      mustDecodeHex(t, mainnetEpoch259Nonce),
				LastEpochBlockNonce: carriedLab,
			},
			{
				EpochId:        entropyEpoch,
				StartSlot:      200,
				LengthInSlots:  100,
				EraId:          eras.MaryEraDesc.Id,
				CandidateNonce: candidate,
				// NeutralNonce-collapsed (wrong) nonce: eta == candidateNonce.
				Nonce:               append([]byte(nil), candidate...),
				LastEpochBlockNonce: nil, // corrupted: empty lab
			},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.healEmptyLabNonces()

	withoutEntropy, err := lcommon.CalculateEpochNonce(
		candidate, carriedLab, nil,
	)
	require.NoError(t, err)
	require.NotEqual(
		t,
		withoutEntropy.Bytes(),
		ls.epochCache[1].Nonce,
		"recomputed epoch nonce must not be the extraEntropy-free value",
	)
	require.Equal(
		t,
		mainnetEpoch259Nonce,
		hex.EncodeToString(ls.epochCache[1].Nonce),
		"recomputed epoch nonce must fold the epoch's recorded extraEntropy",
	)
}

// TestRecordedExtraEntropyForEpochTracksParameterHistory pins the parameter
// history the nonce assembly depends on: a pparams row is written for the epoch
// its change takes effect in, and a later epoch resolves to the newest row at or
// before it. Mainnet set extraEntropy for epoch 259 and reset it for 260, so a
// value that stayed sticky past its reset would corrupt every later epoch's
// nonce -- the opposite failure to dropping it.
func TestRecordedExtraEntropyForEpochTracksParameterHistory(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)

	_, entropyCbor := maryPParamsWithExtraEntropy(t, entropy)
	require.NoError(t, db.SetPParams(
		entropyCbor, 200, 259, eras.MaryEraDesc.Id, nil,
	))
	_, neutralCbor := maryPParamsWithExtraEntropy(t, nil)
	require.NoError(t, db.SetPParams(
		neutralCbor, 300, 260, eras.MaryEraDesc.Id, nil,
	))

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	for _, tc := range []struct {
		name  string
		epoch uint64
		want  []byte
	}{
		{"before the update", 258, nil},
		{"the epoch it was enacted for", 259, entropy},
		{"the epoch it was reset for", 260, nil},
		{"after the reset", 261, nil},
	} {
		got, err := ls.recordedExtraEntropyForEpoch(
			tc.epoch, eras.MaryEraDesc.Id,
		)
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.want, got, tc.name)
	}

	// Conway has no extraEntropy parameter at all, so the Mary rows must not
	// leak into a later era's lookup.
	got, err := ls.recordedExtraEntropyForEpoch(259, eras.ConwayEraDesc.Id)
	require.NoError(t, err)
	require.Nil(t, got)
}

// TestExtraEntropyFromPParamsStopsAtPraos pins the era boundary of the TICKN
// extraEntropy term against the parameter set's protocol version rather than
// its Go type. A hard fork out of Alonzo enacts Alonzo-typed parameters whose
// protocol version is already Babbage's, and the first Praos epoch takes no
// extraEntropy term.
func TestExtraEntropyFromPParamsStopsAtPraos(t *testing.T) {
	t.Parallel()

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	var nonce lcommon.Nonce
	nonce.Type = lcommon.NonceTypeNonce
	copy(nonce.Value[:], entropy)

	for _, tc := range []struct {
		name   string
		params lcommon.ProtocolParameters
		want   []byte
	}{
		{
			"shelley",
			&shelley.ShelleyProtocolParameters{
				ProtocolMajor: 2,
				ExtraEntropy:  nonce,
			},
			entropy,
		},
		{
			"mary",
			&mary.MaryProtocolParameters{
				ProtocolMajor: 4,
				ExtraEntropy:  nonce,
			},
			entropy,
		},
		{
			"alonzo",
			&alonzo.AlonzoProtocolParameters{
				ProtocolMajor: 6,
				ExtraEntropy:  nonce,
			},
			entropy,
		},
		{
			"alonzo parameters carrying the babbage protocol version",
			&alonzo.AlonzoProtocolParameters{
				ProtocolMajor: babbage.MinProtocolVersionBabbage,
				ExtraEntropy:  nonce,
			},
			nil,
		},
	} {
		require.Equal(
			t,
			tc.want,
			extraEntropyFromPParams(tc.params),
			tc.name,
		)
	}
}

// TestAssembleEpochNonceNeutralCandidateReturnsEntropy pins the identity law of
// the nonce ⭒ operator on its left operand. cardano-ledger's Semigroup Nonce
// gives NeutralNonce <> x = x, so a neutral candidate with a non-neutral
// extraEntropy and no lab must yield the entropy itself.
//
// gouroboros' CalculateRollingNonce cannot serve this case: it coerces its
// right operand from a raw VRF output, so for an all-zero left operand it
// returns blake2b_256(right) rather than right.
func TestAssembleEpochNonceNeutralCandidateReturnsEntropy(t *testing.T) {
	t.Parallel()

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	neutral := make([]byte, lcommon.Blake2b256Size)

	got, err := assembleEpochNonce(neutral, nil, entropy)
	require.NoError(t, err)
	require.Equal(
		t,
		entropy,
		got,
		"NeutralNonce is the identity of the nonce operator, so a neutral "+
			"candidate must leave extraEntropy unchanged",
	)

	hashed := lcommon.Blake2b256Hash(entropy)
	require.NotEqual(
		t,
		hashed.Bytes(),
		got,
		"the entropy must not be re-hashed, which is what the rolling-nonce "+
			"helper would do for a neutral left operand",
	)
}
