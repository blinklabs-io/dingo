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

package ledgerstate

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// inlineUTxOMap encodes the legacy inline UTxO map ImportLedgerState reads
// from RawLedgerState.UTxOData: CBOR map[TxIn]TxOut, with the TxIn a 34-byte
// binary key (32-byte hash plus big-endian output index) and the TxOut an
// [address, coin] array.
func inlineUTxOMap(
	tb testing.TB,
	addr []byte,
	amounts []uint64,
) cbor.RawMessage {
	tb.Helper()
	// A Go map cannot carry []byte keys, so build the CBOR map body by
	// hand: header, then key/value pairs in order.
	require.LessOrEqual(tb, len(amounts), 23, "short map header only")
	body := []byte{0xa0 | byte(len(amounts))}
	for i, amount := range amounts {
		txHash := bytes.Repeat([]byte{byte(0x40 + i)}, 32)
		key := append(append([]byte{}, txHash...), 0x00, 0x00)
		keyRaw, err := cbor.Encode(key)
		require.NoError(tb, err)
		valRaw, err := cbor.Encode([]any{addr, amount})
		require.NoError(tb, err)
		body = append(body, keyRaw...)
		body = append(body, valRaw...)
	}
	return cbor.RawMessage(body)
}

// TestImportLedgerStateRebuildsDeferredRewardLiveStake covers the invariant
// the deferred per-batch refresh depends on: importUTxOs skips the aggregate
// refresh on every batch, so ImportLedgerState's own
// RebuildRewardLiveStake is the only thing that leaves reward_live_stake
// correct. A return added between the two phases, or a rebuild removed,
// ships an empty aggregate on a bootstrapped node.
//
// It runs against the real metadata store, which implements
// deferredRewardLiveStakeImporter; the assertion below is meaningless
// against a store that does not, so that is checked first.
func TestImportLedgerStateRebuildsDeferredRewardLiveStake(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	_, ok := db.Metadata().(deferredRewardLiveStakeImporter)
	require.True(
		t, ok,
		"the metadata store must take the deferred import path for this "+
			"test to cover the rebuild it depends on",
	)

	stakeHash := bytes.Repeat([]byte{0x22}, 28)
	addr := buildShelleyAddr(
		0,
		1,
		bytes.Repeat([]byte{0x11}, 28),
		stakeHash,
	)
	nonce := make([]byte, 32)
	eraBounds := make([]EraBound, EraConway+1)

	require.NoError(t, ImportLedgerState(
		context.Background(),
		ImportConfig{
			Database: db,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			State: &RawLedgerState{
				UTxOData: inlineUTxOMap(
					t,
					addr,
					[]uint64{1_000_000, 2_000_000},
				),
				Epoch:               100,
				EraIndex:            EraConway,
				EraBounds:           eraBounds,
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Tip: &SnapshotTip{
					Slot:      1_000,
					BlockHash: make([]byte, 32),
				},
			},
			EpochLength: func(uint) (uint, uint, error) {
				return 1, 1_000, nil
			},
		},
	))

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	var utxoStake string
	require.NoError(t, raw.QueryRow(
		"SELECT utxo_stake FROM reward_live_stake "+
			"WHERE credential_tag = 0 AND staking_key = ?",
		stakeHash,
	).Scan(&utxoStake))
	require.Equal(
		t, "3000000", utxoStake,
		"the post-import rebuild must aggregate every deferred batch",
	)

	blobTxn := db.BlobTxn(false)
	defer blobTxn.Release()
	for i, amount := range []uint64{1_000_000, 2_000_000} {
		txHash := bytes.Repeat([]byte{byte(0x40 + i)}, 32)
		utxoCbor, err := db.Blob().GetUtxo(
			blobTxn.Blob(), txHash, 0,
		)
		require.NoError(t, err,
			"imported live UTxO %x#0 must remain replayable after repair",
			txHash,
		)
		output, err := ledger.NewTransactionOutputFromCbor(utxoCbor)
		require.NoError(t, err)
		require.Equal(t, amount, output.Amount().Uint64())
	}
}

// TestImportDRepsCarriesExpiryEpoch pins the snapshot's DRep expiry through the
// import write path.
//
// ParseCertState decodes DRepState[0] into ParsedDRep.ExpiryEpoch, but
// importDReps built its models.Drep literal without that field, so every
// Mithril-imported DRep landed with expiry_epoch = 0. Zero is exempt from expiry
// in both places that decide it -- drepActiveAtEpoch
// (ledger/governance/epoch.go) and the SQL expiry sweep, whose predicate is
// `expiry_epoch > 0 AND expiry_epoch <= ?` -- so imported DReps stayed in
// countActiveDReps permanently and inflated the ratification quorum denominator
// for the life of the database.
//
// The two DReps carry distinct non-zero expiries, so dropping the field fails
// both assertions and a fix that stamped one shared constant would fail too.
// Asserting a zero expiry would prove nothing here: that is the pre-fix value,
// so such an assertion holds with the fix reverted.
func TestImportDRepsCarriesExpiryEpoch(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	const (
		keyExpiry    = uint64(700)
		scriptExpiry = uint64(812)
		slot         = uint64(197983346)
		deposit      = uint64(500000000)
	)

	keyCred := Credential{
		Type: CredentialTypeKey,
		Hash: bytes.Repeat([]byte{0xd1}, 28),
	}
	scriptCred := Credential{
		Type: CredentialTypeScript,
		Hash: bytes.Repeat([]byte{0xd2}, 28),
	}

	cfg := ImportConfig{
		Database: db,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	require.NoError(t, importDReps(
		context.Background(),
		cfg,
		[]ParsedDRep{
			{
				Credential:  keyCred,
				ExpiryEpoch: keyExpiry,
				Deposit:     deposit,
				Active:      true,
			},
			{
				Credential:  scriptCred,
				ExpiryEpoch: scriptExpiry,
				Deposit:     deposit,
				Active:      true,
			},
		},
		slot,
	))

	for _, tc := range []struct {
		name       string
		cred       Credential
		wantExpiry uint64
	}{
		{name: "key credential", cred: keyCred, wantExpiry: keyExpiry},
		{
			name:       "script credential",
			cred:       scriptCred,
			wantExpiry: scriptExpiry,
		},
	} {
		tag, err := models.CredentialTagFromUint(uint(tc.cred.Type))
		require.NoError(t, err, tc.name)

		got, err := db.GetDrepByCredential(tag, tc.cred.Hash, true, nil)
		require.NoError(t, err, tc.name)
		require.NotNil(t, got, tc.name)
		require.Equal(
			t,
			tc.wantExpiry,
			got.ExpiryEpoch,
			"%s: imported DRep must keep the snapshot expiry; a zero here is "+
				"treated as never-expiring and inflates the ratification quorum",
			tc.name,
		)
	}
}

// mark, set and go span three epochs, so an import landing in the first two
// epochs of a new era has set or go sitting in the era before it -- with a
// different epoch length and a different boundary slot. Deriving their start
// from the current era's bound puts the window edge in the wrong place, and
// for an epoch wholly before that bound it lands past the epoch entirely:
// every registration made during it then counts as pre-epoch, so the latest
// one wins and the epoch is seeded with parameters that only took effect
// afterwards. That is precisely the one-epoch-early error
// GetPoolRegistrationsEffectiveForEpoch exists to avoid, reintroduced at the
// window instead of the query.
func TestImportedEpochStartSlotUsesTheEpochsOwnEra(t *testing.T) {
	t.Parallel()

	// Era 0 runs epochs 0..9 from slot 0 with 100-slot epochs; era 1 starts
	// at epoch 10, slot 1000, with 500-slot epochs.
	cfg := ImportConfig{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		State: &RawLedgerState{
			EraBounds: []EraBound{
				{Slot: 0, Epoch: 0},
				{Slot: 1000, Epoch: 10},
			},
			EraIndex:      1,
			EraBoundEpoch: 10,
			EraBoundSlot:  1000,
			Epoch:         11,
		},
		EpochLength: func(era uint) (uint, uint, error) {
			switch era {
			case 0:
				return 1, 100, nil
			case 1:
				return 1, 500, nil
			}
			return 0, 0, errors.New("unknown era")
		},
	}

	for _, c := range []struct {
		name  string
		epoch uint64
		want  uint64
	}{
		{"mark, in the current era", 11, 1500},
		{"set, the current era's first epoch", 10, 1000},
		{"go, in the previous era", 9, 900},
		{"an earlier epoch of the previous era", 3, 300},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, ok := importedEpochStartSlot(cfg, c.epoch)
			require.True(t, ok, "this epoch is covered by the era bounds")
			require.Equal(t, c.want, got)
		})
	}
}

// With no era bounds to consult, the current era's boundary is all there is.
// It stays a usable window edge -- registrations before it fall on the
// pre-epoch side, where the most recent wins -- but it is a fallback, not the
// epoch's real start, so it must not be reached when bounds are available.
func TestImportedEpochStartSlotFallsBackWithoutEraBounds(t *testing.T) {
	t.Parallel()

	cfg := ImportConfig{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		State: &RawLedgerState{
			EraIndex:      1,
			EraBoundEpoch: 10,
			EraBoundSlot:  1000,
			Epoch:         11,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 500, nil
		},
	}
	for _, c := range []struct {
		epoch uint64
		want  uint64
	}{
		{11, 1500},
		{10, 1000},
	} {
		got, ok := importedEpochStartSlot(cfg, c.epoch)
		require.True(t, ok)
		require.Equal(t, c.want, got)
	}
	// Epoch 9 began before this era did, and without bounds there is nothing
	// describing where. The era boundary is not a stand-in: it follows the
	// epoch, so every registration made during it would count as pre-epoch.
	_, ok := importedEpochStartSlot(cfg, 9)
	require.False(t, ok,
		"an epoch the current era's arithmetic cannot reach has no window")
}

// Bounds that do not reach back to genesis leave an epoch with no era to
// measure from, and neither available guess is safe. The current era's
// boundary sits after the epoch, so every registration made during it counts
// as pre-epoch and the newest wins. Widening to zero is the mirror image:
// they all look in-epoch, so the pool's earliest registration wins and a
// re-registration made before the target epoch is ignored. Both seed rewards
// from parameters that were not in force, so neither is offered.
func TestImportedEpochStartSlotHasNoWindowBelowTheFirstEraBound(t *testing.T) {
	t.Parallel()

	cfg := ImportConfig{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		State: &RawLedgerState{
			EraBounds:     []EraBound{{Slot: 1000, Epoch: 10}},
			EraIndex:      0,
			EraBoundEpoch: 10,
			EraBoundSlot:  1000,
			Epoch:         11,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 500, nil
		},
	}
	_, ok := importedEpochStartSlot(cfg, 9)
	require.False(t, ok,
		"an epoch before every known era bound has no window: the boundary "+
			"that follows it is too late, and widening to zero is too early")
	got, ok := importedEpochStartSlot(cfg, 11)
	require.True(t, ok, "epochs the bounds do cover are unaffected")
	require.Equal(t, uint64(1500), got)
}

const previewHistoricalPParamsSnapshotEpoch = uint64(1397)

// A Preview snapshot in epoch 1397 seeds Mark/Set/Go reward bases for
// 1397/1396/1395. The first boundary into 1398 consumes the Go basis and
// evaluates epoch 1396's block performance, so both the snapshot's previous
// and current parameters must survive the import as distinct historical rows.
func TestImportPParamsPersistsPreviewRewardHistory(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	seedPreviewRewardBases(t, db)
	current, previous := distinctConwayPParams(t)
	cfg := previewPParamsImportConfig(db, current, previous)

	require.NoError(t, importPParams(context.Background(), cfg))

	previousRows, err := db.Metadata().GetPParams(1396, EraConway, nil)
	require.NoError(t, err)
	require.Len(t, previousRows, 1)
	require.Equal(t, previous, previousRows[0].Cbor)
	require.Equal(t, uint64(1396), previousRows[0].Epoch)

	currentRows, err := db.Metadata().GetPParams(1397, EraConway, nil)
	require.NoError(t, err)
	require.Len(t, currentRows, 1)
	require.Equal(t, current, currentRows[0].Cbor)
	require.Equal(t, uint64(1397), currentRows[0].Epoch)
	require.NotEqual(t, previousRows[0].Cbor, currentRows[0].Cbor,
		"current parameters must not substitute for the historical epoch")
}

// An imported Go basis whose historical parameters are unavailable must be
// left ineligible by snapshot seeding. The pparams phase still persists the
// usable current parameters instead of turning one skipped reward round into a
// permanently failing bootstrap.
func TestImportPParamsStoresCurrentWithoutUnavailableHistory(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	seedPreviewRewardBases(t, db)
	current, _ := distinctConwayPParams(t)
	cfg := previewPParamsImportConfig(db, current, nil)

	for range 2 {
		require.NoError(t, importPParams(context.Background(), cfg))

		previousRows, queryErr := db.Metadata().GetPParams(
			1396, EraConway, nil,
		)
		require.NoError(t, queryErr)
		require.Empty(t, previousRows)

		currentRows, queryErr := db.Metadata().GetPParams(
			1397, EraConway, nil,
		)
		require.NoError(t, queryErr)
		require.Len(t, currentRows, 1)
		require.Equal(t, current, currentRows[0].Cbor)
	}
}

func TestImportPParamsReentryUsesStoredCrossEraHistory(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	// Model the first Conway epoch. GovState's previous field has already been
	// translated to Conway, while reward performance for E-1 still needs a
	// Babbage row. Recover it by applying the ledger's Conway-to-Babbage
	// downgrade.
	seedRewardBasisMarkers(t, db, 1395)
	current, translatedPrevious := distinctConwayPParams(t)
	storedPrevious, err := cbor.Encode(testBabbagePParams())
	require.NoError(t, err)
	expectedPrevious, err := previousPParamsForEra(
		EraConway, translatedPrevious, EraBabbage,
	)
	require.NoError(t, err)
	require.NoError(t, db.Metadata().SetPParams(
		storedPrevious, 99_999, 1396, EraBabbage, nil,
	))
	cfg := previewPParamsImportConfig(db, current, translatedPrevious)
	cfg.State.EraBoundEpoch = previewHistoricalPParamsSnapshotEpoch
	cfg.State.EraBounds[EraConway] = EraBound{
		Slot:  100_000,
		Epoch: previewHistoricalPParamsSnapshotEpoch,
	}

	for range 2 {
		require.NoError(t, importPParams(context.Background(), cfg))

		previousRows, queryErr := db.Metadata().GetPParams(
			1396, EraBabbage, nil,
		)
		require.NoError(t, queryErr)
		require.Len(t, previousRows, 1)
		require.Equal(t, expectedPrevious, previousRows[0].Cbor)
		currentRows, queryErr := db.Metadata().GetPParams(
			1397, EraConway, nil,
		)
		require.NoError(t, queryErr)
		require.Len(t, currentRows, 1,
			"re-entry must not duplicate an already-satisfying current row")
		require.Equal(t, current, currentRows[0].Cbor)
	}
}

func TestImportPParamsDowngradesTranslatedCrossEraHistory(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	seedPreviewRewardBases(t, db)
	current, previous := distinctConwayPParams(t)
	cfg := previewPParamsImportConfig(db, current, previous)
	// Model an import at the first Conway epoch. GovState's previous payload
	// is translated to the current-era shape, but the performance epoch is
	// still Babbage. Recover its parameters with the ledger downgrade.
	cfg.State.EraBoundEpoch = previewHistoricalPParamsSnapshotEpoch
	cfg.State.EraBounds[EraConway] = EraBound{
		Slot:  100_000,
		Epoch: previewHistoricalPParamsSnapshotEpoch,
	}

	require.NoError(t, importPParams(context.Background(), cfg))

	previousRows, queryErr := db.Metadata().GetPParams(
		1396, EraBabbage, nil,
	)
	require.NoError(t, queryErr)
	require.Len(t, previousRows, 1)
	expectedPrevious, err := previousPParamsForEra(
		EraConway, previous, EraBabbage,
	)
	require.NoError(t, err)
	require.Equal(t, expectedPrevious, previousRows[0].Cbor)
	currentRows, queryErr := db.Metadata().GetPParams(
		1397, EraConway, nil,
	)
	require.NoError(t, queryErr)
	require.Len(t, currentRows, 1)
	require.Equal(t, current, currentRows[0].Cbor)
}

func TestImportSnapShotsSeedsGoBasisWithDowngradedCrossEraHistory(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	require.GreaterOrEqual(t, state.Epoch, uint64(2))
	current, translatedPrevious := distinctConwayPParams(t)
	state.PParamsData = current
	state.PrevPParamsData = translatedPrevious
	state.EraIndex = EraConway
	state.EraBoundEpoch = state.Epoch
	state.EraBoundSlot = state.Tip.Slot
	state.EraBounds = previewEraBounds()
	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: state,
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 500, nil
		},
	}
	noProgress := func(ImportProgress) {}
	_, err = importCertState(
		context.Background(), cfg, state.Tip.Slot, noProgress,
	)
	require.NoError(t, err)
	// Seed the provisional Go basis before protocol parameters are imported;
	// the importer later recovers the old-era parameters by downgrade.
	require.NoError(t, importSnapShots(
		context.Background(), cfg, state.Tip.Slot, noProgress, false,
	))
	preexistingGo, err := db.Metadata().GetRewardSnapshot(
		state.Epoch-2, "mark", nil,
	)
	require.NoError(t, err)
	require.NotNil(t, preexistingGo)
	require.False(t, preexistingGo.Authoritative)
	preexistingPools, err := db.Metadata().GetRewardPoolInputs(
		state.Epoch-2, nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, preexistingPools)

	state.EraBounds[EraConway] = EraBound{
		Slot:  state.Tip.Slot,
		Epoch: state.Epoch,
	}
	require.NoError(t, importSnapShots(
		context.Background(), cfg, state.Tip.Slot, noProgress, false,
	))

	goBasis, err := db.Metadata().GetRewardSnapshot(
		state.Epoch-2, "mark", nil,
	)
	require.NoError(t, err)
	require.NotNil(t, goBasis,
		"the Go basis remains usable after historical pparams are downgraded")
	goPools, err := db.Metadata().GetRewardPoolInputs(state.Epoch-2, nil)
	require.NoError(t, err)
	require.NotEmpty(t, goPools)
	goStake, err := db.Metadata().GetRewardStakeInputs(state.Epoch-2, nil)
	require.NoError(t, err)
	require.NotEmpty(t, goStake)
	for _, epoch := range []uint64{state.Epoch - 1, state.Epoch} {
		basis, queryErr := db.Metadata().GetRewardSnapshot(epoch, "mark", nil)
		require.NoError(t, queryErr)
		require.NotNil(t, basis,
			"epoch %d does not depend on unavailable old-era history", epoch)
	}

	require.NoError(t, importPParams(context.Background(), cfg))
	previousRows, err := db.Metadata().GetPParams(
		state.Epoch-1, EraBabbage, nil,
	)
	require.NoError(t, err)
	require.Len(t, previousRows, 1)
	expectedPrevious, err := previousPParamsForEra(
		EraConway, translatedPrevious, EraBabbage,
	)
	require.NoError(t, err)
	require.Equal(t, expectedPrevious, previousRows[0].Cbor)
	currentRows, err := db.Metadata().GetPParams(
		state.Epoch, EraConway, nil,
	)
	require.NoError(t, err)
	require.Len(t, currentRows, 1)
}

func TestImportSnapShotsPreservesAuthoritativeRewardBasis(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: state,
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 500, nil
		},
	}
	noProgress := func(ImportProgress) {}
	_, err = importCertState(
		context.Background(), cfg, state.Tip.Slot, noProgress,
	)
	require.NoError(t, err)

	const retainedPoolCount = uint64(999)
	require.NoError(t, db.Metadata().SaveRewardSnapshot(
		&models.RewardSnapshot{
			Epoch:          state.Epoch - 2,
			SnapshotType:   "mark",
			TotalPoolCount: retainedPoolCount,
			Authoritative:  true,
		},
		nil,
	))
	require.NoError(t, importSnapShots(
		context.Background(), cfg, state.Tip.Slot, noProgress, false,
	))

	basis, err := db.Metadata().GetRewardSnapshot(
		state.Epoch-2, "mark", nil,
	)
	require.NoError(t, err)
	require.NotNil(t, basis)
	require.True(t, basis.Authoritative)
	require.Equal(t, retainedPoolCount, basis.TotalPoolCount)
}

func seedPreviewRewardBases(t *testing.T, db *database.Database) {
	t.Helper()
	seedRewardBasisMarkers(t, db, 1395, 1396, 1397)
}

func seedRewardBasisMarkers(
	t *testing.T,
	db *database.Database,
	epochs ...uint64,
) {
	t.Helper()
	for _, epoch := range epochs {
		require.NoError(t, db.Metadata().SaveRewardSnapshot(
			&models.RewardSnapshot{
				Epoch:        epoch,
				SnapshotType: "mark",
			},
			nil,
		))
	}
}

func distinctConwayPParams(t *testing.T) (current, previous []byte) {
	t.Helper()
	currentParams := testConwayPParams()
	previousParams := *currentParams
	previousParams.MinFeeA++

	var err error
	current, err = cbor.Encode(currentParams)
	require.NoError(t, err)
	previous, err = cbor.Encode(&previousParams)
	require.NoError(t, err)
	return current, previous
}

func previewPParamsImportConfig(
	db *database.Database,
	current []byte,
	previous []byte,
) ImportConfig {
	return ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: &RawLedgerState{
			PParamsData:     current,
			PrevPParamsData: previous,
			Epoch:           previewHistoricalPParamsSnapshotEpoch,
			EraIndex:        EraConway,
			EraBoundEpoch:   1200,
			EraBoundSlot:    100_000,
			EraBounds:       previewEraBounds(),
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	}
}

func previewEraBounds() []EraBound {
	// Preview starts several historical eras at epoch zero. The last bound at
	// or before a target epoch therefore has to win, just as it does for a real
	// imported telescope.
	return []EraBound{
		{Slot: 0, Epoch: 0}, // Byron
		{Slot: 0, Epoch: 0}, // Shelley
		{Slot: 0, Epoch: 0}, // Allegra
		{Slot: 0, Epoch: 0}, // Mary
		{Slot: 0, Epoch: 0}, // Alonzo
		{Slot: 0, Epoch: 0}, // Babbage
		{Slot: 0, Epoch: 0}, // Conway
	}
}

// Driving importSnapShots itself, rather than the seeding function it calls.
//
// The distinction has already cost twice. The seeding's derivation was covered
// by unit tests and checked against the reference, and the seeding function was
// covered against a real snapshot -- and the glue between importSnapShots and
// the store still carried a defect that would have failed the first Mithril
// bootstrap outright (it passed the outer *database.Txn where the metadata
// store wants txn.Metadata()). Reading that path did not find it; running it
// did, immediately.
//
// So this runs the sequence the import actually performs: cert state first,
// which is what puts the pool registrations in the database, then the stake
// snapshots, whose seeding reads them back. Anything wired wrong between the
// two -- ordering, transaction plumbing, a parameter source that turns out to
// be empty -- shows up here rather than on an operator's first bootstrap.
func TestImportSnapShotsSeedsRewardInputs(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err, "parsing the fixture snapshot")
	require.NotNil(t, state.Tip)

	cfg := ImportConfig{
		Database: db,
		State:    state,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 500, nil
		},
	}
	noProgress := func(ImportProgress) {}
	ctx := context.Background()
	slot := state.Tip.Slot

	// Cert state first, exactly as the import sequences it: this is what
	// populates the pool registrations the seeding takes its parameters from.
	// If this ever stops running before the snapshots, the seeding silently
	// finds no parameters and writes nothing.
	poolsImported, err := importCertState(ctx, cfg, slot, noProgress)
	require.NoError(t, err)
	require.Positive(t, poolsImported,
		"the fixture must import pools, or the seeding below would be "+
			"vacuous for the same reason the pool-distr defect was")

	require.NoError(t, importSnapShots(ctx, cfg, slot, noProgress, false))

	// The three epochs mark, set and go cover are the ones whose reward
	// rounds a bootstrapped node cannot otherwise compute.
	for _, epoch := range []uint64{
		state.Epoch, state.Epoch - 1, state.Epoch - 2,
	} {
		snapshot, err := db.Metadata().GetRewardSnapshot(epoch, "mark", nil)
		require.NoError(t, err)
		require.NotNil(t, snapshot,
			"epoch %d has no reward snapshot after a full import, so its "+
				"reward round would be skipped and never made up", epoch)

		poolInputs, err := db.Metadata().GetRewardPoolInputs(epoch, nil)
		require.NoError(t, err)
		require.Len(t, poolInputs, int(snapshot.TotalPoolCount))
		for _, pool := range poolInputs {
			require.NotEmpty(t, pool.RewardAccount,
				"pool inputs seeded through the import path must carry a "+
					"reward account; empty is what the snapshot's own "+
					"pool-distr entries produce, and the ledger rejects it")
		}

		stakeInputs, err := db.Metadata().GetRewardStakeInputs(epoch, nil)
		require.NoError(t, err)
		require.NotEmpty(t, stakeInputs)
	}

	// The pots row is the other half of what a first reward round needs, and
	// it is seeded by a different part of the import; assert the two agree on
	// the epoch, since a mismatch leaves the round just as skipped.
	pots, err := db.Metadata().GetRewardAdaPots(state.Epoch, nil)
	require.NoError(t, err)
	if pots != nil {
		require.Equal(t, state.Epoch, pots.Epoch)
	}
}

// The reward basis is derived from the pool registrations in the database, so
// it can only be right if every registration the import is going to create
// already exists when it runs. importSnapShots creates registrations in two
// later stages -- the fallback pool import, and the retired-but-scheduled
// synthesis at the end -- and the seeding used to sit ahead of both. Whatever
// those stages added was therefore invisible to it, and which pools the
// seeding could describe depended on where in the function it happened to sit
// rather than on what the import knew.
//
// The fallback path shows it plainly: it exists for a resume where cert state
// completed in an earlier run, so it is exactly the case where the pools come
// from that later stage and nowhere else. Seeded ahead of it, the seeding sees
// an empty pool table.
//
// Ordering is the assertion because it is the invariant -- the seeding must be
// downstream of every stage that writes a pool. The rows are checked too, but
// they cannot carry the ordering on their own: the snapshots describe their
// own pools, so a basis seeds here whether or not the fallback import ran
// first, and a row-level assertion alone would pass for reasons that have
// nothing to do with the sequence.
func TestImportSnapShotsSeedsAfterEveryPoolImportStage(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	require.NotNil(t, state.Tip)

	recorder := &messageRecorder{}
	cfg := ImportConfig{
		Database: db,
		State:    state,
		Logger:   slog.New(recorder),
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 500, nil
		},
	}

	// No importCertState: this is the resume the fallback exists for, where
	// the only pools this run learns about come from the fallback itself.
	require.NoError(t, importSnapShots(
		context.Background(),
		cfg,
		state.Tip.Slot,
		func(ImportProgress) {},
		true,
	))

	// Matched against the two messages seedImportedRewardInputs emits, in
	// full, rather than a shared fragment of them: "not seeding ..."
	// contains "seeding ...", so a fragment would match both by accident and
	// leave it unclear which one the fixture actually produces.
	const (
		seededMsg  = "seeded reward inputs for an imported epoch"
		droppedMsg = "not seeding reward inputs for an imported epoch: " +
			"the derived basis does not reconcile, so that epoch's reward " +
			"round will be skipped and its rewards never credited"
	)
	messages := recorder.snapshot()
	if len(messages) == 0 {
		t.Fatal("the seeding did not run at all, so this test proves nothing")
	}
	seedIdx := firstIndexContaining(messages, seededMsg, droppedMsg)
	if seedIdx < 0 {
		t.Fatal("the seeding did not run at all, so this test proves nothing")
	}
	// Which one fires is a claim worth pinning: the snapshots carry their own
	// pool parameters, so the basis must actually be seeded here. If that
	// ever regresses to a drop, this says so rather than leaving the
	// ordering assertions below to pass over an epoch nothing was written
	// for.
	require.Equal(t, seededMsg, messages[seedIdx],
		"the snapshots carry pool parameters, so the basis must be seeded; "+
			"a dropped basis means the parameters stopped being read")

	for _, stage := range []string{
		// The fallback pool import, which writes the registrations the
		// seeding reads.
		"importing pools",
		// The last stage that can add a pool: retired-but-scheduled
		// synthesis runs immediately after this one.
		"imported active pool distribution",
	} {
		idx := firstIndexContaining(messages, stage)
		require.GreaterOrEqual(t, idx, 0,
			"stage %q did not run, so the ordering it anchors is untested",
			stage)
		require.Less(t, idx, seedIdx,
			"the reward basis was seeded before %q, so any pool that stage "+
				"created was invisible to it", stage)
	}

	// And the seeding actually produced a basis, so the ordering above is
	// ordering of work that happened rather than of a no-op.
	snapshot, err := db.Metadata().GetRewardSnapshot(state.Epoch, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, snapshot,
		"the epoch the snapshots describe must be seeded")
	require.Positive(t, snapshot.TotalPoolCount)
}

func firstIndexContaining(messages []string, needles ...string) int {
	for i, msg := range messages {
		if slices.ContainsFunc(needles, func(n string) bool {
			return strings.Contains(msg, n)
		}) {
			return i
		}
	}
	return -1
}

// messageRecorder is a slog.Handler that keeps messages in emission order.
// Ordering is the thing under test, so the handler records sequence rather
// than the test scraping a formatted buffer.
type messageRecorder struct {
	mu       sync.Mutex
	messages []string
}

func (r *messageRecorder) Enabled(context.Context, slog.Level) bool {
	return true
}

func (r *messageRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, rec.Message)
	return nil
}

func (r *messageRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *messageRecorder) WithGroup(string) slog.Handler      { return r }

func (r *messageRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.messages)
}

func importTestPool(
	t *testing.T,
	db *database.Database,
	pool *models.Pool,
) {
	t.Helper()
	require.NoError(t, db.Metadata().ImportPool(
		pool,
		&models.PoolRegistration{
			PoolKeyHash:                pool.PoolKeyHash,
			VrfKeyHash:                 pool.VrfKeyHash,
			RewardAccount:              pool.RewardAccount,
			RewardAccountCredentialTag: pool.RewardAccountCredentialTag,
			Margin:                     pool.Margin,
			Pledge:                     pool.Pledge,
			Cost:                       pool.Cost,
			AddedSlot:                  1,
		},
		nil,
	))
}

func testPoolKeyHash(value []byte) lcommon.PoolKeyHash {
	var ret lcommon.PoolKeyHash
	copy(ret[:], value)
	return ret
}

func TestImportOpCertCountersStoresCertifiedBaseline(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	poolKeyHash := bytes.Repeat([]byte{0x77}, 28)
	txn := db.MetadataTxn(true)
	require.NoError(t, importOpCertCounters(
		db.Metadata(),
		map[string]uint64{string(poolKeyHash): 490},
		100,
		txn.Metadata(),
	))
	require.NoError(t, txn.Commit())
	txn.Release()

	sequence, found, err := db.LatestPoolOpCertSequence(
		testPoolKeyHash(poolKeyHash), nil,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(490), sequence)
}

// TestImportOpCertCountersRefusesUnpersistableCounter covers the one write
// path into pool_opcert_sequence carrying counters that were never checked
// against a chain rule. decodeOpCertCounters decodes the certified
// HeaderState map at the reference's full uint64 width, so a counter above
// eras.MaxPersistableOpCertCounter reaches this path and must be refused by
// name, not at checkedInt64, whose message reports only that an unsigned SQL
// value exceeds int64.
func TestImportOpCertCountersRefusesUnpersistableCounter(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	poolKeyHash := bytes.Repeat([]byte{0x78}, 28)
	txn := db.MetadataTxn(true)
	err = importOpCertCounters(
		db.Metadata(),
		map[string]uint64{
			string(poolKeyHash): eras.MaxPersistableOpCertCounter + 1,
		},
		100,
		txn.Metadata(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "9223372036854775807")
	require.Contains(t, err.Error(), "pool_opcert_sequence")
	require.NotContains(t, err.Error(), "exceeds int64")
	require.NoError(t, txn.Rollback())
	txn.Release()

	sequence, found, err := db.LatestPoolOpCertSequence(
		testPoolKeyHash(poolKeyHash), nil,
	)
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, uint64(0), sequence)
}

// TestImportOpCertCountersStoresCounterAtBound is the other side of that
// boundary: the highest counter the metadata store records must still import,
// so the new check cannot be satisfied by refusing more than it should.
func TestImportOpCertCountersStoresCounterAtBound(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	poolKeyHash := bytes.Repeat([]byte{0x79}, 28)
	txn := db.MetadataTxn(true)
	require.NoError(t, importOpCertCounters(
		db.Metadata(),
		map[string]uint64{
			string(poolKeyHash): eras.MaxPersistableOpCertCounter,
		},
		100,
		txn.Metadata(),
	))
	require.NoError(t, txn.Commit())
	txn.Release()

	sequence, found, err := db.LatestPoolOpCertSequence(
		testPoolKeyHash(poolKeyHash), nil,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, eras.MaxPersistableOpCertCounter, sequence)
}

func TestSnapshotImportTargetsAlignWithRotation(t *testing.T) {
	t.Parallel()

	snapshots := &ParsedSnapShots{}

	targets := snapshotImportTargets(1237, snapshots)
	if len(targets) != 3 {
		t.Fatalf("expected 3 targets, got %d", len(targets))
	}

	expected := []struct {
		name  string
		epoch uint64
	}{
		{name: "mark", epoch: 1237},
		{name: "set", epoch: 1236},
		{name: "go", epoch: 1235},
	}

	for i, target := range targets {
		if target.name != expected[i].name {
			t.Fatalf(
				"target %d: expected name %q, got %q",
				i,
				expected[i].name,
				target.name,
			)
		}
		if target.targetEpoch != expected[i].epoch {
			t.Fatalf(
				"target %d: expected epoch %d, got %d",
				i,
				expected[i].epoch,
				target.targetEpoch,
			)
		}
	}
}

func TestSnapshotImportTargetsSkipNegativeEpochs(t *testing.T) {
	t.Parallel()

	snapshots := &ParsedSnapShots{}

	targets0 := snapshotImportTargets(0, snapshots)
	if len(targets0) != 1 {
		t.Fatalf("epoch 0: expected 1 target, got %d", len(targets0))
	}
	if targets0[0].name != "mark" || targets0[0].targetEpoch != 0 {
		t.Fatalf("epoch 0: unexpected target %+v", targets0[0])
	}

	targets1 := snapshotImportTargets(1, snapshots)
	if len(targets1) != 2 {
		t.Fatalf("epoch 1: expected 2 targets, got %d", len(targets1))
	}
	if targets1[0].name != "mark" || targets1[0].targetEpoch != 1 {
		t.Fatalf("epoch 1 mark: unexpected target %+v", targets1[0])
	}
	if targets1[1].name != "set" || targets1[1].targetEpoch != 0 {
		t.Fatalf("epoch 1 set: unexpected target %+v", targets1[1])
	}
}

func TestSnapshotImportTargetsNilSnapshots(t *testing.T) {
	t.Parallel()

	targets := snapshotImportTargets(7, nil)
	if targets != nil {
		t.Fatalf("expected nil targets, got %+v", targets)
	}
}

func TestImportSnapShotsPreservesBoundaryCaptureProvenance(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	state, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	require.NotNil(t, state.Tip)

	cfg := ImportConfig{
		Database: db,
		State:    state,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 500, nil
		},
	}
	ctx := context.Background()
	progress := func(ImportProgress) {}
	_, err = importCertState(ctx, cfg, state.Tip.Slot, progress)
	require.NoError(t, err)
	require.NoError(t, importSnapShots(
		ctx,
		cfg,
		state.Tip.Slot,
		progress,
		false,
	))

	snapshots, err := ParseSnapShots(state.SnapShotsData)
	require.NoError(t, err)
	for _, target := range snapshotImportTargets(state.Epoch, snapshots) {
		rows, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(
			target.targetEpoch,
			models.PoolStakeSnapshotTypeMark,
			nil,
		)
		require.NoError(t, err)
		require.NotEmpty(t, rows, "%s snapshot has no pool rows", target.name)

		epochStart, ok := importedEpochStartSlot(cfg, target.targetEpoch)
		require.True(t, ok)
		wantCaptureSlot := uint64(0)
		if epochStart > 0 {
			wantCaptureSlot = epochStart - 1
		}
		for _, row := range rows {
			require.Equal(
				t,
				wantCaptureSlot,
				row.CapturedSlot,
				"%s snapshot must retain its epoch-boundary provenance",
				target.name,
			)
		}
	}
}

func TestImportedEpochSummaryUsesCurrentEpochMetadata(t *testing.T) {
	t.Parallel()

	nonce := []byte{0x01, 0x02, 0x03}

	summary := importedEpochSummary(
		nil,
		1237,
		1237,
		456789,
		nonce,
		100,
		2,
		3,
	)

	if summary.Epoch != 1237 {
		t.Fatalf("expected epoch 1237, got %d", summary.Epoch)
	}
	if summary.BoundarySlot != 456789 {
		t.Fatalf(
			"expected boundary slot 456789, got %d",
			summary.BoundarySlot,
		)
	}
	if !bytes.Equal(summary.EpochNonce, nonce) {
		t.Fatalf(
			"expected epoch nonce %x, got %x",
			nonce,
			summary.EpochNonce,
		)
	}
	if !summary.SnapshotReady {
		t.Fatal("expected snapshot summary to be marked ready")
	}
}

func TestImportedEpochSummaryLeavesHistoricalMetadataUnknown(t *testing.T) {
	t.Parallel()

	summary := importedEpochSummary(
		nil,
		1237,
		1235,
		456789,
		[]byte{0x01, 0x02, 0x03},
		100,
		2,
		3,
	)

	if summary.BoundarySlot != 0 {
		t.Fatalf(
			"expected historical boundary slot to remain unknown, got %d",
			summary.BoundarySlot,
		)
	}
	if len(summary.EpochNonce) != 0 {
		t.Fatalf(
			"expected historical epoch nonce to remain unknown, got %x",
			summary.EpochNonce,
		)
	}
}

func TestImportedEpochSummaryPreservesExistingMetadata(t *testing.T) {
	t.Parallel()

	existing := &models.EpochSummary{
		Epoch:        1235,
		EpochNonce:   []byte{0xaa, 0xbb, 0xcc},
		BoundarySlot: 777,
	}

	summary := importedEpochSummary(
		existing,
		1237,
		1235,
		456789,
		[]byte{0x01, 0x02, 0x03},
		100,
		2,
		3,
	)

	if summary.BoundarySlot != existing.BoundarySlot {
		t.Fatalf(
			"expected boundary slot %d, got %d",
			existing.BoundarySlot,
			summary.BoundarySlot,
		)
	}
	if !bytes.Equal(summary.EpochNonce, existing.EpochNonce) {
		t.Fatalf(
			"expected preserved epoch nonce %x, got %x",
			existing.EpochNonce,
			summary.EpochNonce,
		)
	}
	if summary.TotalPoolCount != 2 {
		t.Fatalf(
			"expected updated total pool count 2, got %d",
			summary.TotalPoolCount,
		)
	}
}

func TestImportedEpochSummaryKeepsCurrentEpochMetadataWhenExisting(
	t *testing.T,
) {
	t.Parallel()

	existing := &models.EpochSummary{
		Epoch:        1237,
		EpochNonce:   []byte{0xaa, 0xbb, 0xcc},
		BoundarySlot: 777,
	}
	currentNonce := []byte{0x01, 0x02, 0x03}

	summary := importedEpochSummary(
		existing,
		1237,
		1237,
		456789,
		currentNonce,
		100,
		2,
		3,
	)

	if summary.BoundarySlot != 456789 {
		t.Fatalf(
			"expected current boundary slot 456789, got %d",
			summary.BoundarySlot,
		)
	}
	if !bytes.Equal(summary.EpochNonce, currentNonce) {
		t.Fatalf(
			"expected current epoch nonce %x, got %x",
			currentNonce,
			summary.EpochNonce,
		)
	}
}

func TestPersistImportedSnapshotClearsEpochWhenEmpty(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	store := db.Metadata()
	targetEpoch := uint64(100)
	poolKeyHash := make([]byte, 28)
	poolKeyHash[0] = 0x01

	require.NoError(t, store.SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			{
				Epoch:          targetEpoch,
				SnapshotType:   "mark",
				PoolKeyHash:    poolKeyHash,
				TotalStake:     10,
				DelegatorCount: 2,
				CapturedSlot:   55,
			},
		},
		nil,
	))

	existingSummary := &models.EpochSummary{
		Epoch:            targetEpoch,
		TotalActiveStake: 10,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		BoundarySlot:     777,
		EpochNonce:       []byte{0xaa, 0xbb, 0xcc},
	}
	require.NoError(t, store.SaveEpochSummary(existingSummary, nil))

	err = persistImportedSnapshot(
		ImportConfig{
			Database: db,
			State: &RawLedgerState{
				Epoch:      102,
				EpochNonce: []byte{0x01, 0x02, 0x03},
			},
		},
		999,
		snapshotImportTarget{
			name:        "set",
			targetEpoch: targetEpoch,
		},
		nil,
	)
	require.NoError(t, err)

	snapshots, err := store.GetPoolStakeSnapshotsByEpoch(
		targetEpoch,
		"mark",
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, snapshots)

	summary, err := store.GetEpochSummary(targetEpoch, nil)
	require.NoError(t, err)
	require.NotNil(t, summary)
	require.Equal(t, targetEpoch, summary.Epoch)
	require.Equal(t, uint64(0), uint64(summary.TotalActiveStake))
	require.Zero(t, summary.TotalPoolCount)
	require.Zero(t, summary.TotalDelegators)
	require.True(t, summary.SnapshotReady)
	require.Equal(t, existingSummary.BoundarySlot, summary.BoundarySlot)
	require.True(t, bytes.Equal(existingSummary.EpochNonce, summary.EpochNonce))
}

func TestPersistImportedActivePoolDistribution(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	poolKeyHash := make([]byte, 28)
	poolKeyHash[0] = 0x4a
	publicKey := bytes.Repeat([]byte{0x7b}, 96)
	possessionProof := bytes.Repeat([]byte{0x8c}, 48)
	rows := ActivePoolDistributionSnapshots(
		[]ParsedActivePoolStake{
			{
				PoolKeyHash:             poolKeyHash,
				StakeNumerator:          3,
				StakeDenominator:        10,
				VrfKeyHash:              bytes.Repeat([]byte{0x9b}, 32),
				LeiosKeyPublic:          publicKey,
				LeiosKeyPossessionProof: possessionProof,
			},
		},
		298,
		127178646,
	)

	require.NoError(t, persistImportedActivePoolDistribution(
		ImportConfig{
			Database: db,
			State:    &RawLedgerState{Epoch: 298},
		},
		rows,
	))

	stored, err := db.Metadata().GetPoolStakeSnapshot(
		298,
		models.PoolStakeSnapshotTypeActive,
		poolKeyHash,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, uint64(3), uint64(stored.TotalStake))
	require.Equal(t, uint64(10), uint64(stored.StakeDenominator))
	require.Equal(t, uint64(127178646), stored.CapturedSlot)
	require.Equal(t, publicKey, stored.LeiosKeyPublic)
	require.Equal(t, possessionProof, stored.LeiosKeyPossessionProof)
}

func TestPersistImportedMarkSnapshotPreservesLeiosKey(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	poolKeyHash := bytes.Repeat([]byte{0x4d}, 28)
	publicKey := bytes.Repeat([]byte{0x5e}, 96)
	possessionProof := bytes.Repeat([]byte{0x6f}, 48)
	rows := []*models.PoolStakeSnapshot{{
		Epoch:                   101,
		SnapshotType:            models.PoolStakeSnapshotTypeMark,
		PoolKeyHash:             poolKeyHash,
		TotalStake:              123,
		DelegatorCount:          1,
		CapturedSlot:            456,
		LeiosKeyPublic:          publicKey,
		LeiosKeyPossessionProof: possessionProof,
	}}
	require.NoError(t, persistImportedSnapshot(
		ImportConfig{
			Database: db,
			State:    &RawLedgerState{Epoch: 102},
		},
		999,
		snapshotImportTarget{name: "set", targetEpoch: 101},
		rows,
	))

	stored, err := db.Metadata().GetPoolStakeSnapshot(
		101,
		models.PoolStakeSnapshotTypeMark,
		poolKeyHash,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, publicKey, stored.LeiosKeyPublic)
	require.Equal(t, possessionProof, stored.LeiosKeyPossessionProof)

	// SQL conversion and the model returned by the store must not alias caller
	// buffers used to construct the imported snapshot.
	wantPublicKey := append([]byte(nil), publicKey...)
	wantPossessionProof := append([]byte(nil), possessionProof...)
	rows[0].LeiosKeyPublic[0] ^= 0xff
	rows[0].LeiosKeyPossessionProof[0] ^= 0xff
	again, err := db.Metadata().GetPoolStakeSnapshot(
		101,
		models.PoolStakeSnapshotTypeMark,
		poolKeyHash,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, wantPublicKey, again.LeiosKeyPublic)
	require.Equal(t, wantPossessionProof, again.LeiosKeyPossessionProof)
}

// TestPersistImportedSnapshotResolvesAutoVoteOnlyForMark verifies the
// CIP-1694 reward-account auto-vote resolver runs against live Pool /
// Account state for the "mark" rotation (whose target epoch equals
// the import-time epoch and therefore matches the live state) but is
// SKIPPED for "set" and "go" rotations (whose target epochs are
// older than the live state). The set/go rows are still written but
// must carry RewardAccountAutoVoteResolved=false so the tally
// fallback treats them as implicit no rather than freezing today's
// delegation map into a historical boundary.
func TestPersistImportedSnapshotResolvesAutoVoteOnlyForMark(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	poolKeyHash := make([]byte, 28)
	poolKeyHash[0] = 0x42
	rewardAccount := make([]byte, 28)
	rewardAccount[0] = 0x43

	// Seed Pool + Account state so the resolver, if called, would
	// produce a non-default outcome (Abstain). Set/go must NOT pick
	// this up — that's the regression we're guarding against.
	importTestPool(t, db, &models.Pool{
		PoolKeyHash:   poolKeyHash,
		RewardAccount: rewardAccount,
	})
	require.NoError(t, db.Metadata().CreateAccount(nil, &models.Account{
		StakingKey: rewardAccount,
		DrepType:   models.DrepTypeAlwaysAbstain,
		AddedSlot:  1,
		Active:     true,
	}))

	mkSnapshot := func(epoch uint64) []*models.PoolStakeSnapshot {
		return []*models.PoolStakeSnapshot{
			{
				Epoch:          epoch,
				SnapshotType:   "mark",
				PoolKeyHash:    poolKeyHash,
				TotalStake:     100,
				DelegatorCount: 1,
				CapturedSlot:   55,
			},
		}
	}

	cases := []struct {
		name         string
		targetEpoch  uint64
		wantResolved bool
		wantAutoVote uint8
	}{
		{
			name:         "mark",
			targetEpoch:  102,
			wantResolved: true,
			wantAutoVote: models.PoolRewardAccountAutoVoteAbstain,
		},
		{
			name:         "set",
			targetEpoch:  101,
			wantResolved: false,
			wantAutoVote: models.PoolRewardAccountAutoVoteNone,
		},
		{
			name:         "go",
			targetEpoch:  100,
			wantResolved: false,
			wantAutoVote: models.PoolRewardAccountAutoVoteNone,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := persistImportedSnapshot(
				ImportConfig{
					Database: db,
					State: &RawLedgerState{
						Epoch:      102,
						EpochNonce: []byte{0x01},
					},
				},
				999,
				snapshotImportTarget{
					name:        tc.name,
					targetEpoch: tc.targetEpoch,
				},
				mkSnapshot(tc.targetEpoch),
			)
			require.NoError(t, err)

			stored, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(
				tc.targetEpoch, "mark", nil,
			)
			require.NoError(t, err)
			require.Len(t, stored, 1)
			require.Equal(
				t, tc.wantResolved, stored[0].RewardAccountAutoVoteResolved,
				"RewardAccountAutoVoteResolved mismatch for %s", tc.name,
			)
			require.Equal(
				t, tc.wantAutoVote, stored[0].RewardAccountAutoVote,
				"RewardAccountAutoVote mismatch for %s", tc.name,
			)
		})
	}
}

// TestPersistImportedSnapshotMissingPoolsNotResolved verifies that when no
// pool rows exist in the DB (e.g. the fallback pool import has not yet run),
// the current-epoch snapshot is NOT falsely marked Resolved=true. This is the
// main correctness invariant: a missing pool row must not
// produce an authoritative Resolved=true, AutoVote=None entry.
func TestPersistImportedSnapshotMissingPoolsNotResolved(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	poolKeyHash := make([]byte, 28)
	poolKeyHash[0] = 0x11
	// Deliberately do NOT seed any Pool rows — simulating the state
	// before fallback pool import.

	snapshots := []*models.PoolStakeSnapshot{
		{
			Epoch:          102,
			SnapshotType:   "mark",
			PoolKeyHash:    poolKeyHash,
			TotalStake:     100,
			DelegatorCount: 1,
			CapturedSlot:   55,
		},
	}

	err = persistImportedSnapshot(
		ImportConfig{
			Database: db,
			State: &RawLedgerState{
				Epoch:      102,
				EpochNonce: []byte{0x01},
			},
		},
		999,
		snapshotImportTarget{
			name:        "mark",
			targetEpoch: 102,
		},
		snapshots,
	)
	require.NoError(t, err)

	stored, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(102, "mark", nil)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.False(t, stored[0].RewardAccountAutoVoteResolved,
		"pool row absent: must NOT be falsely resolved")
	require.Equal(
		t,
		models.PoolRewardAccountAutoVoteNone,
		stored[0].RewardAccountAutoVote,
	)
}

// TestPersistImportedSnapshotPoolPresentAccountStates exercises the
// current-epoch resolver's three account outcomes:
//   - reward account row absent entirely → Resolved=false (data may not be
//     imported yet; must not be persisted as a false confirmed None);
//   - reward account present but inactive (deregistered) → Resolved=true,
//     AutoVote=None (CIP-1694 treats unregistered reward accounts as
//     implicit no, but it is a confirmed outcome);
//   - reward account present and active, delegated to AlwaysNoConfidence →
//     Resolved=true, AutoVote=NoConfidence.
func TestPersistImportedSnapshotPoolPresentAccountStates(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	poolAbsent := bytes.Repeat(
		[]byte{0x60},
		28,
	) // pool present, account absent
	poolInactive := bytes.Repeat(
		[]byte{0x61},
		28,
	) // pool present, account inactive
	poolNoConf := bytes.Repeat(
		[]byte{0x62},
		28,
	) // pool present, account active NoConf
	rewardAbsent := bytes.Repeat([]byte{0x70}, 28)
	rewardInactive := bytes.Repeat([]byte{0x71}, 28)
	rewardNoConf := bytes.Repeat([]byte{0x72}, 28)

	// All three pools exist in the DB. Only two of their reward accounts
	// have account rows; poolAbsent's reward account has none.
	for _, p := range []struct {
		pool   []byte
		reward []byte
	}{
		{poolAbsent, rewardAbsent},
		{poolInactive, rewardInactive},
		{poolNoConf, rewardNoConf},
	} {
		importTestPool(t, db, &models.Pool{
			PoolKeyHash:   p.pool,
			RewardAccount: p.reward,
		})
	}
	// Inactive (deregistered) account that still carries an Always* flag.
	inactive := models.Account{
		StakingKey: rewardInactive,
		DrepType:   models.DrepTypeAlwaysAbstain,
		AddedSlot:  1,
		Active:     false,
	}
	require.NoError(t, db.Metadata().CreateAccount(nil, &inactive))
	// Active account delegated to AlwaysNoConfidence.
	require.NoError(t, db.Metadata().CreateAccount(nil, &models.Account{
		StakingKey: rewardNoConf,
		DrepType:   models.DrepTypeAlwaysNoConfidence,
		AddedSlot:  1,
		Active:     true,
	}))

	cases := []struct {
		name         string
		pool         []byte
		wantResolved bool
		wantAutoVote uint8
	}{
		{
			"account_absent",
			poolAbsent,
			false,
			models.PoolRewardAccountAutoVoteNone,
		},
		{
			"account_inactive",
			poolInactive,
			true,
			models.PoolRewardAccountAutoVoteNone,
		},
		{
			"account_active_noconfidence",
			poolNoConf,
			true,
			models.PoolRewardAccountAutoVoteNoConfidence,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snaps := []*models.PoolStakeSnapshot{{
				Epoch:          102,
				SnapshotType:   "mark",
				PoolKeyHash:    tc.pool,
				TotalStake:     100,
				DelegatorCount: 1,
				CapturedSlot:   55,
			}}
			require.NoError(t, persistImportedSnapshot(
				ImportConfig{
					Database: db,
					State: &RawLedgerState{
						Epoch:      102,
						EpochNonce: []byte{0x01},
					},
				},
				999,
				snapshotImportTarget{name: "mark", targetEpoch: 102},
				snaps,
			))

			stored, err := db.Metadata().
				GetPoolStakeSnapshotsByEpoch(102, "mark", nil)
			require.NoError(t, err)
			var row *models.PoolStakeSnapshot
			for i := range stored {
				if bytes.Equal(stored[i].PoolKeyHash, tc.pool) {
					row = stored[i]
					break
				}
			}
			require.NotNil(t, row)
			require.Equal(t, tc.wantResolved, row.RewardAccountAutoVoteResolved,
				"RewardAccountAutoVoteResolved mismatch for %s", tc.name)
			require.Equal(t, tc.wantAutoVote, row.RewardAccountAutoVote,
				"RewardAccountAutoVote mismatch for %s", tc.name)
		})
	}
}

// TestPersistImportedSnapshotHistoricalLeftUnresolved verifies that historical
// N-1/N-2 imported rows (target epoch != import epoch) are left
// RewardAccountAutoVoteResolved=false even when the snapshot bundle carries
// pool params and a live reward account delegates to an Always* DRep.
//
// Faithful historical resolution would need the reward account's DRep
// delegation AS OF the historical boundary, which is not recoverable after a
// Mithril restore. Freezing live DRep state onto a historical boundary could
// persist a value that was changed after the boundary, so the row is left
// unresolved and the tally treats it as implicit no.
func TestPersistImportedSnapshotHistoricalLeftUnresolved(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	poolKeyHash := bytes.Repeat([]byte{0x20}, 28)
	rewardAccount := bytes.Repeat([]byte{0x30}, 28)

	// Live account delegates to AlwaysAbstain. If historical resolution
	// (incorrectly) used live state, the row would become Abstain/resolved.
	importTestPool(t, db, &models.Pool{
		PoolKeyHash:   poolKeyHash,
		RewardAccount: rewardAccount,
	})
	require.NoError(t, db.Metadata().CreateAccount(nil, &models.Account{
		StakingKey: rewardAccount,
		DrepType:   models.DrepTypeAlwaysAbstain,
		AddedSlot:  1,
		Active:     true,
	}))

	targetEpoch := uint64(101) // N-1, import epoch is 102
	snaps := []*models.PoolStakeSnapshot{{
		Epoch:          targetEpoch,
		SnapshotType:   "mark",
		PoolKeyHash:    poolKeyHash,
		TotalStake:     100,
		DelegatorCount: 1,
		CapturedSlot:   55,
	}}
	err = persistImportedSnapshot(
		ImportConfig{
			Database: db,
			State: &RawLedgerState{
				Epoch:      102,
				EpochNonce: []byte{0x01},
			},
		},
		999,
		snapshotImportTarget{
			name:        "set",
			targetEpoch: targetEpoch,
			snap: &ParsedSnapShot{
				PoolParams: map[string]*ParsedPool{
					string(poolKeyHash): {
						PoolKeyHash:   poolKeyHash,
						RewardAccount: rewardAccount,
					},
				},
			},
		},
		snaps,
	)
	require.NoError(t, err)

	stored, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(
		targetEpoch, "mark", nil,
	)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.False(t, stored[0].RewardAccountAutoVoteResolved,
		"historical N-1 row must remain unresolved (no historical DRep state)")
	require.Equal(
		t,
		models.PoolRewardAccountAutoVoteNone,
		stored[0].RewardAccountAutoVote,
	)
}

// TestCollectPoolsFromSnapshotsMarkWins verifies that when a pool appears in
// more than one snapshot (Mark/Set/Go) with different reward accounts, the
// Mark-era params win. The fallback import feeds the current-epoch auto-vote
// resolver, which must read the current (Mark) reward account.
func TestCollectPoolsFromSnapshotsMarkWins(t *testing.T) {
	t.Parallel()

	poolKeyHash := bytes.Repeat([]byte{0x42}, 28)
	markReward := bytes.Repeat([]byte{0x01}, 28)
	goReward := bytes.Repeat([]byte{0x02}, 28)

	snapshots := &ParsedSnapShots{
		Mark: ParsedSnapShot{
			PoolParams: map[string]*ParsedPool{
				string(poolKeyHash): {
					PoolKeyHash:   poolKeyHash,
					RewardAccount: markReward,
				},
			},
		},
		Go: ParsedSnapShot{
			PoolParams: map[string]*ParsedPool{
				string(poolKeyHash): {
					PoolKeyHash:   poolKeyHash,
					RewardAccount: goReward,
				},
			},
		},
	}

	pools := collectPoolsFromSnapshots(snapshots)
	require.Len(t, pools, 1)
	require.Equal(t, markReward, pools[0].RewardAccount,
		"Mark-era reward account must take precedence over Go-era")
}

// TestImportSnapShotsFallbackPopulatesReconcileKeys verifies that pools
// imported via the stake-snapshot fallback are recorded in the reconcile key
// set. Without this, a catch-up whose cert-state parses zero pools would see
// an empty pool key set in the reconcile pass and retire the very pools the
// fallback just imported.
func TestImportSnapShotsFallbackPopulatesReconcileKeys(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	poolHash := toFixed28([]byte("fallback pool for reconcile"))
	vrfHash := bytes.Repeat([]byte{0x44}, 32)

	poolMap := encodeCredentialMapEntry(
		t,
		poolHash[:],
		[]any{
			uint64(0),
			[]any{uint64(0), uint64(1)},
			[]any{},
			uint64(0),
			vrfHash,
			uint64(0),
			uint64(0),
			[]any{uint64(0), uint64(1)},
			uint64(0),
			[]any{},
		},
	)
	emptyMap, err := cbor.Encode(map[uint64]uint64{})
	require.NoError(t, err)
	snapshot, err := cbor.Encode([]any{
		cbor.RawMessage(emptyMap),
		cbor.RawMessage(poolMap),
	})
	require.NoError(t, err)
	data, err := cbor.Encode([]any{
		cbor.RawMessage(snapshot),
		cbor.RawMessage(snapshot),
		cbor.RawMessage(snapshot),
	})
	require.NoError(t, err)

	cfg := ImportConfig{
		Database:      db,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		State:         &RawLedgerState{Epoch: 2, SnapShotsData: data},
		Reconcile:     true,
		reconcileKeys: newReconcileKeys(),
	}
	// The fixture's pool carries no reward account and the state no protocol
	// parameters, so its reward basis cannot be seeded and the import fails
	// there. The fallback pool import, and its key recording, run before it.
	err = importSnapShots(
		context.Background(), cfg, 999, func(ImportProgress) {}, true,
	)
	require.ErrorIs(t, err, errImportedRewardBasisUnusable)
	_, ok := cfg.reconcileKeys.pools[string(poolHash[:])]
	require.True(t, ok,
		"fallback-imported pool must be recorded in the reconcile key set")
}

// TestImportSnapShotsFallbackPoolsResolveCurrentEpoch verifies the end-to-end
// fix: when pools come from the snapshot-pool fallback path
// (importPools runs before persistImportedSnapshot), the current-epoch snapshot
// is correctly resolved rather than left with a false Resolved=true, AutoVote=None.
func TestImportSnapShotsFallbackPoolsResolveCurrentEpoch(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	poolKeyHash := bytes.Repeat([]byte{0x42}, 28)
	rewardAccount := bytes.Repeat([]byte{0x43}, 28)

	require.NoError(t, db.Metadata().CreateAccount(nil, &models.Account{
		StakingKey: rewardAccount,
		DrepType:   models.DrepTypeAlwaysAbstain,
		AddedSlot:  1,
		Active:     true,
	}))

	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
	}
	// Simulate the fallback pool import that now runs BEFORE snapshot
	// processing in importSnapShots.
	require.NoError(t, importPools(
		context.Background(),
		cfg,
		[]ParsedPool{{
			PoolKeyHash:   poolKeyHash,
			RewardAccount: rewardAccount,
			VrfKeyHash:    bytes.Repeat([]byte{0x44}, 32),
			MarginDen:     1,
		}},
		999,
		nil,
	))

	// Now call persistImportedSnapshot for the current epoch,
	// which should find the pool row and correctly resolve auto-votes.
	poolSnapshots := []*models.PoolStakeSnapshot{{
		Epoch:          102,
		SnapshotType:   "mark",
		PoolKeyHash:    poolKeyHash,
		TotalStake:     5_000_000,
		DelegatorCount: 1,
		CapturedSlot:   999,
	}}
	require.NoError(t, persistImportedSnapshot(
		ImportConfig{
			Database: db,
			State: &RawLedgerState{
				Epoch:      102,
				EpochNonce: []byte{0x01},
			},
		},
		999,
		snapshotImportTarget{name: "mark", targetEpoch: 102},
		poolSnapshots,
	))

	stored, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(102, "mark", nil)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.True(t, stored[0].RewardAccountAutoVoteResolved,
		"current-epoch snapshot must be resolved after pool fallback import")
	require.Equal(
		t,
		models.PoolRewardAccountAutoVoteAbstain,
		stored[0].RewardAccountAutoVote,
		"AlwaysAbstain reward account must produce Abstain auto-vote",
	)
}

func TestImportPParamsAnchorsAddedSlotToCurrentEpochStart(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	pparamsCbor, err := cbor.Encode(testConwayPParams())
	require.NoError(t, err)

	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: &RawLedgerState{
			PParamsData:   pparamsCbor,
			Epoch:         1277,
			EraIndex:      EraConway,
			EraBoundEpoch: 1200,
			EraBoundSlot:  10_000,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	}

	require.NoError(t, importPParams(context.Background(), cfg))

	pparams, err := db.Metadata().GetPParams(1277, EraConway, nil)
	require.NoError(t, err)
	require.Len(t, pparams, 1)
	require.Equal(t, uint64(17_700), pparams[0].AddedSlot)
}

func TestImportAccountsPreservesCredentialTag(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	stakeKey := bytes.Repeat([]byte{0xA4}, 28)
	keyDeposit := uint64(2_000_000)
	scriptDeposit := uint64(3_000_000)
	zeroDeposit := uint64(0)
	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
	}

	require.NoError(t, importAccounts(
		context.Background(),
		cfg,
		[]ParsedAccount{
			{
				StakingKey: Credential{
					Type: CredentialTypeKey,
					Hash: stakeKey,
				},
				Reward:  1,
				Deposit: &keyDeposit,
				Active:  true,
			},
			{
				StakingKey: Credential{
					Type: CredentialTypeScript,
					Hash: stakeKey,
				},
				Reward:  2,
				Deposit: &scriptDeposit,
				Active:  true,
			},
			{
				StakingKey: Credential{
					Type: CredentialTypeKey,
					Hash: bytes.Repeat([]byte{0xA5}, 28),
				},
				Reward:  3,
				Deposit: &zeroDeposit,
				Active:  true,
			},
			{
				StakingKey: Credential{
					Type: CredentialTypeKey,
					Hash: bytes.Repeat([]byte{0xA6}, 28),
				},
				Reward: 4,
				Active: true,
			},
		},
		123,
	))

	keyAcct, err := db.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint8(0), keyAcct.CredentialTag)
	require.Equal(t, types.Uint64(1), keyAcct.Reward)

	scriptAcct, err := db.GetAccountByCredential(1, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint8(1), scriptAcct.CredentialTag)
	require.Equal(t, types.Uint64(2), scriptAcct.Reward)

	for _, tc := range []struct {
		name    string
		tag     uint8
		deposit uint64
	}{
		{name: "key", tag: 0, deposit: 2_000_000},
		{name: "script", tag: 1, deposit: 3_000_000},
	} {
		t.Run(tc.name+" registration deposit", func(t *testing.T) {
			registration, err := db.GetAccountImportRegistrationByCredential(
				tc.tag,
				stakeKey,
				nil,
			)
			require.NoError(t, err)
			require.NotNil(t, registration)
			require.Equal(t, uint64(123), registration.AddedSlot)
			require.NotNil(t, registration.Deposit)
			require.Equal(t, tc.deposit, *registration.Deposit)
		})
	}

	zeroRegistration, err := db.GetAccountImportRegistrationByCredential(
		0,
		bytes.Repeat([]byte{0xA5}, 28),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, zeroRegistration)
	require.NotNil(t, zeroRegistration.Deposit)
	require.Zero(t, *zeroRegistration.Deposit)

	unknownRegistration, err := db.GetAccountImportRegistrationByCredential(
		0,
		bytes.Repeat([]byte{0xA6}, 28),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, unknownRegistration)
	require.Nil(t, unknownRegistration.Deposit)
}

// TestImportPoolsPreservesRewardAccountCredentialTag verifies snapshot
// pool import stores reward account tags on Pool and PoolRegistration.
func TestImportPoolsPreservesRewardAccountCredentialTag(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	poolKeyHash := bytes.Repeat([]byte{0x51}, 28)
	vrfKeyHash := bytes.Repeat([]byte{0x52}, 32)
	rewardAccount := bytes.Repeat([]byte{0x53}, 28)
	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
	}

	require.NoError(t, importPools(
		context.Background(),
		cfg,
		[]ParsedPool{
			{
				PoolKeyHash:                poolKeyHash,
				VrfKeyHash:                 vrfKeyHash,
				RewardAccount:              rewardAccount,
				RewardAccountCredentialTag: 1,
				MarginDen:                  1,
				LeiosKeyPublic:             bytes.Repeat([]byte{0x71}, 96),
				LeiosKeyPossessionProof:    bytes.Repeat([]byte{0x72}, 48),
			},
		},
		456,
		nil,
	))

	pool, err := db.Metadata().GetPool(
		testPoolKeyHash(poolKeyHash),
		true,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, pool)
	require.Equal(t, rewardAccount, []byte(pool.RewardAccount))
	require.Equal(t, uint8(1), pool.RewardAccountCredentialTag)

	require.NotEmpty(t, pool.Registration)
	require.Equal(t, rewardAccount, []byte(pool.Registration[0].RewardAccount))
	require.Equal(
		t,
		uint8(1),
		pool.Registration[0].RewardAccountCredentialTag,
	)
	require.True(t, pool.Registration[0].LeiosKeyRegistrationAgeUnknown)
}

func TestImportPoolsWritesPendingRetirementForBothQueries(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	poolKeyHash := bytes.Repeat([]byte{0x61}, 28)
	deposit := uint64(500_000_000)
	cfg := ImportConfig{
		Database: db,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		State:    &RawLedgerState{Epoch: 650},
	}
	require.NoError(t, importPools(
		context.Background(),
		cfg,
		[]ParsedPool{{
			PoolKeyHash:   poolKeyHash,
			VrfKeyHash:    bytes.Repeat([]byte{0x62}, 32),
			RewardAccount: bytes.Repeat([]byte{0x63}, 28),
			Deposit:       deposit,
		}},
		10,
		map[uint64][][]byte{656: {poolKeyHash}},
	))

	retiring, err := db.Metadata().GetRetiringPools(650, nil)
	require.NoError(t, err)
	require.Len(t, retiring, 1)
	require.Equal(t, poolKeyHash, retiring[0].PoolKeyHash)
	require.Equal(t, uint64(656), retiring[0].Epoch)

	refunds, err := db.GetPoolsRetiringAtEpoch(656, 11, nil)
	require.NoError(t, err)
	require.Len(t, refunds, 1)
	require.Equal(t, poolKeyHash, refunds[0].PoolKeyHash)
	require.Equal(t, deposit, uint64(refunds[0].DepositHeld))
}

func TestImportPoolsRejectsUnmatchedRetirementBeforeWritingRows(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	poolKeyHash := bytes.Repeat([]byte{0x64}, 28)
	missingKeyHash := bytes.Repeat([]byte{0x65}, 28)
	cfg := ImportConfig{
		Database: db,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		State:    &RawLedgerState{Epoch: 650},
	}
	err = importPools(
		context.Background(),
		cfg,
		[]ParsedPool{{
			PoolKeyHash: poolKeyHash,
			VrfKeyHash:  bytes.Repeat([]byte{0x66}, 32),
		}},
		10,
		map[uint64][][]byte{
			656: {poolKeyHash},
			657: {missingKeyHash},
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found in the pool table")

	retiring, err := db.Metadata().GetRetiringPools(650, nil)
	require.NoError(t, err)
	require.Empty(t, retiring)
	refunds, err := db.GetPoolsRetiringAtEpoch(656, 11, nil)
	require.NoError(t, err)
	require.Empty(t, refunds)
}

func TestImportPoolsRejectsPastPendingRetirement(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	poolKeyHash := bytes.Repeat([]byte{0x67}, 28)
	cfg := ImportConfig{
		Database: db,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		State:    &RawLedgerState{Epoch: 650},
	}
	err = importPools(
		context.Background(),
		cfg,
		[]ParsedPool{{
			PoolKeyHash: poolKeyHash,
			VrfKeyHash:  bytes.Repeat([]byte{0x68}, 32),
		}},
		10,
		map[uint64][][]byte{3: {poolKeyHash}},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not after snapshot epoch")
	retiring, err := db.Metadata().GetRetiringPools(650, nil)
	require.NoError(t, err)
	require.Empty(t, retiring)
}

// TestIndefiniteUTxOMapPartialCommitIsSafeToRetry verifies that the
// indefinite-length UTxO map's
// running entry-count check can only reject entry `limit`+1 after earlier
// batches have already been streamed to the UTxO callback and committed to
// the database (there is no header count to check up front, unlike the
// definite-length path). This is safe rather than a partial-import bug
// because every UTxO write is an idempotent "insert if absent" upsert: a
// later re-run over the same data (e.g. after the cap is raised or
// corrupted data is replaced) reapplies the same rows without duplicating
// them, converging to exactly one row per UTxO.
func TestIndefiniteUTxOMapPartialCommitIsSafeToRetry(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	const (
		// One more entry than the cap below, so the running check
		// rejects the map -- but only after two full batches were
		// already committed.
		//
		// batchSize is the production utxoBatchSize (10,000) scaled
		// down. What this proves is that the running check cannot fire
		// before whole batches have reached the callback, which is a
		// property of the batch boundary rather than of its size; every
		// assertion below is unchanged. At the production size the two
		// committed batches plus the retry put 40,002 UTxO rows through
		// SQLite under -race, and that one test cost 45.4s of the
		// ledgerstate package's 49.1s.
		batchSize    = 10
		totalEntries = 2*batchSize + 1
		limit        = 2 * batchSize
		slot         = uint64(500)
	)
	data := buildIndefiniteUTxOMapCbor(t, totalEntries)
	store := db.Metadata()

	importBatch := func(batch []ParsedUTxO) error {
		utxos := make([]models.Utxo, 0, len(batch))
		for i := range batch {
			utxos = append(utxos, UTxOToModel(&batch[i], slot))
		}
		txn := db.MetadataTxn(true)
		defer txn.Release()
		if err := store.ImportUtxos(utxos, txn.Metadata()); err != nil {
			return err
		}
		return txn.Commit()
	}

	_, err = parseIndefiniteUTxOMapWithProgressLimit(
		data, importBatch, nil, limit, batchSize,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeded max entries")
	require.Contains(t, err.Error(), "duplicate-safe")

	committed, err := store.GetUtxosAddedAfterSlot(slot-1, nil)
	require.NoError(t, err)
	require.Len(
		t, committed, limit,
		"the two full batches before the rejected entry "+
			"should already be committed",
	)

	// Simulate a retry (checkpoint-resumed or from scratch) once the
	// underlying issue is resolved: re-running over a limit that now
	// covers every entry must converge without duplicating the rows
	// the first pass already committed.
	total, err := parseIndefiniteUTxOMapWithProgressLimit(
		data, importBatch, nil, totalEntries, batchSize,
	)
	require.NoError(t, err)
	require.Equal(t, totalEntries, total)

	final, err := store.GetUtxosAddedAfterSlot(slot-1, nil)
	require.NoError(t, err)
	require.Len(
		t, final, totalEntries,
		"retry must converge to exactly one row per UTxO, no duplicates",
	)
}

// TestSynthesizeRetiredScheduledPoolsResolvesVrfKey verifies that a pool
// present only in the imported active pool distribution (absent from the live
// pool table) becomes resolvable via GetPool(includeInactive=true) carrying the
// VRF key hash from the distribution, and is tombstoned with a retirement at
// the snapshot epoch. This mirrors a pool that retired at the epoch boundary
// but still leads the current epoch's fixed schedule, whose header VRF-key
// binding check would otherwise fail on a Mithril-imported node.
func TestSynthesizeRetiredScheduledPoolsResolvesVrfKey(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	// A currently-registered pool that the import already wrote.
	livePoolKeyHash := bytes.Repeat([]byte{0x11}, 28)
	liveVrfKeyHash := bytes.Repeat([]byte{0x12}, 32)
	importTestPool(t, db, &models.Pool{
		PoolKeyHash: livePoolKeyHash,
		VrfKeyHash:  liveVrfKeyHash,
	})

	// A retired-but-scheduled pool present only in the active pool distr.
	retiredPoolKeyHash := bytes.Repeat([]byte{0x21}, 28)
	retiredVrfKeyHash := bytes.Repeat([]byte{0x22}, 32)

	const epoch = uint64(305)
	const slot = uint64(130267768)

	cfg := ImportConfig{
		Database: db,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	require.NoError(t, synthesizeRetiredScheduledPools(
		context.Background(),
		cfg,
		[]ParsedActivePoolStake{
			{
				PoolKeyHash:      livePoolKeyHash,
				StakeNumerator:   1,
				StakeDenominator: 10,
				VrfKeyHash:       liveVrfKeyHash,
			},
			{
				PoolKeyHash:      retiredPoolKeyHash,
				StakeNumerator:   2,
				StakeDenominator: 10,
				VrfKeyHash:       retiredVrfKeyHash,
			},
		},
		epoch,
		slot,
	))
	require.NoError(t, db.Metadata().SetEpoch(
		slot-100,
		epoch,
		nil,
		nil,
		nil,
		nil,
		0,
		1,
		200,
		nil,
	))
	active, err := db.Metadata().GetActivePoolKeyHashesAtSlot(slot, nil)
	require.NoError(t, err)
	require.Contains(t, active, livePoolKeyHash)
	require.NotContains(t, active, retiredPoolKeyHash)

	// The retired-but-scheduled pool now resolves with its VRF key hash on
	// both the denormalized pool row and its registration, the two fields the
	// header VRF-key binding check reads.
	retired, err := db.Metadata().GetPool(
		testPoolKeyHash(retiredPoolKeyHash),
		true,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, retired)
	require.Equal(t, retiredVrfKeyHash, []byte(retired.VrfKeyHash))
	require.NotEmpty(t, retired.Registration)
	require.Equal(
		t,
		retiredVrfKeyHash,
		retired.Registration[0].VrfKeyHash,
	)
	require.Equal(t, slot, retired.Registration[0].AddedSlot)

	// It carries a retirement tombstone at the snapshot epoch (synthetic
	// certificate_id 0), keeping it out of active-pool/stake/reward queries.
	require.NotEmpty(t, retired.Retirement)
	require.Equal(t, epoch, retired.Retirement[0].Epoch)
	require.Equal(t, uint(0), retired.Retirement[0].CertificateID)

	// The already-registered pool is left untouched: no duplicate registration
	// and no retirement tombstone.
	live, err := db.Metadata().GetPool(
		testPoolKeyHash(livePoolKeyHash),
		true,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, live)
	require.Len(t, live.Registration, 1)
	require.Empty(t, live.Retirement)
}

func TestImportGovStateAnchorsProposalAndConstitutionSlots(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	txHash := bytes.Repeat([]byte{0x91}, 32)
	cfg := ImportConfig{
		Database: db,
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: &RawLedgerState{
			GovStateData:  testGovStateData(t, txHash, 1275),
			Epoch:         1277,
			EraIndex:      EraConway,
			EraBoundEpoch: 1200,
			EraBoundSlot:  10_000,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	}

	require.NoError(t, importGovState(
		context.Background(),
		cfg,
		func(ImportProgress) {},
	))

	proposal, err := db.Metadata().GetGovernanceProposal(
		txHash,
		0,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, proposal)
	// Proposal AddedSlot is anchored to the proposal's original epoch
	// (ProposedIn=1275 → 10_000 + (1275-1200)*100 = 17_500), not the
	// snapshot's current epoch, so older proposals retain their original
	// slot for rollback/pruning purposes.
	require.Equal(t, uint64(17_500), proposal.AddedSlot)

	constitution, err := db.Metadata().GetConstitution(nil)
	require.NoError(t, err)
	require.NotNil(t, constitution)
	require.Equal(t, uint64(17_700), constitution.AddedSlot)
}

func TestSnapshotEpochAnchorSlotUsesMatchingEraBound(t *testing.T) {
	t.Parallel()

	cfg := ImportConfig{
		Logger: slog.New(
			slog.NewTextHandler(io.Discard, nil),
		),
		State: &RawLedgerState{
			EraBounds: []EraBound{
				{Slot: 0, Epoch: 0},
				{Slot: 1_000, Epoch: 10},
				{Slot: 2_000, Epoch: 20},
			},
			EraIndex:      EraConway,
			EraBoundEpoch: 20,
			EraBoundSlot:  2_000,
		},
		EpochLength: func(eraId uint) (uint, uint, error) {
			switch eraId {
			case 0:
				return 1, 50, nil
			case 1:
				return 1, 100, nil
			default:
				return 1, 200, nil
			}
		},
	}

	require.Equal(t, uint64(1_500), snapshotEpochAnchorSlot(cfg, 15))
}

func TestSnapshotEpochAnchorSlotWarnsOnFallback(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	cfg := ImportConfig{
		Logger: slog.New(
			slog.NewTextHandler(&logBuf, nil),
		),
		State: &RawLedgerState{
			EraBounds: []EraBound{
				{Slot: 1_000, Epoch: 10},
				{Slot: 2_000, Epoch: 20},
			},
			EraIndex:      EraConway,
			EraBoundEpoch: 20,
			EraBoundSlot:  2_000,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, 100, nil
		},
	}

	require.Zero(t, snapshotEpochAnchorSlot(cfg, 5))
	require.Contains(t, logBuf.String(), "snapshotEpochAnchorSlot")
	require.Contains(t, logBuf.String(), "epoch precedes first era bound")
	require.Contains(t, logBuf.String(), "epoch=5")
	require.Contains(t, logBuf.String(), "era_bound_epoch=20")
}

func TestSnapshotEpochAnchorSlotWarnsOnMissingEpochLength(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	cfg := ImportConfig{
		Logger: slog.New(
			slog.NewTextHandler(&logBuf, nil),
		),
		State: &RawLedgerState{
			EraBounds: []EraBound{
				{Slot: 1_000, Epoch: 10},
			},
			EraIndex:      EraConway,
			EraBoundEpoch: 10,
			EraBoundSlot:  1_000,
		},
	}

	require.Equal(t, uint64(1_000), snapshotEpochAnchorSlot(cfg, 12))
	require.Contains(t, logBuf.String(), "snapshotEpochAnchorSlot")
	require.Contains(t, logBuf.String(), "epoch length unavailable")
	require.Contains(t, logBuf.String(), "epoch=12")
}

func TestSnapshotEpochAnchorSlotWarnsOnEpochLengthError(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	cfg := ImportConfig{
		Logger: slog.New(
			slog.NewTextHandler(&logBuf, nil),
		),
		State: &RawLedgerState{
			EraBounds: []EraBound{
				{Slot: 0, Epoch: 0},
			},
			EraIndex:      EraConway,
			EraBoundEpoch: 0,
			EraBoundSlot:  0,
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 0, 0, errors.New("boom")
		},
	}

	require.Zero(t, snapshotEpochAnchorSlot(cfg, 3))
	require.Contains(t, logBuf.String(), "snapshotEpochAnchorSlot")
	require.Contains(t, logBuf.String(), "failed to resolve epoch length")
	require.Contains(t, logBuf.String(), "boom")
}

func testGovStateData(
	t *testing.T,
	txHash []byte,
	proposedEpoch uint64,
) []byte {
	t.Helper()

	proposal := []any{
		[]any{txHash, uint64(0)},
		map[uint64]uint64{},
		map[uint64]uint64{},
		map[uint64]uint64{},
		[]any{
			uint64(100_000_000),
			bytes.Repeat([]byte{0xa1}, 29),
			[]any{uint8(2)},
			[]any{
				"https://example.com/proposal",
				bytes.Repeat([]byte{0xb2}, 32),
			},
		},
		proposedEpoch,
		proposedEpoch + 5,
	}
	govState := []any{
		[]any{[]any{}, []any{proposal}},
		[]any{},
		[]any{
			[]any{
				"https://example.com/constitution",
				bytes.Repeat([]byte{0xc3}, 32),
			},
			nil,
		},
		map[uint64]uint64{},
		map[uint64]uint64{},
		map[uint64]uint64{},
		drepPulsingStateWithEnactCommittee(t, []any{}),
	}
	data, err := cbor.Encode(govState)
	require.NoError(t, err)
	return data
}

// partialCertStateData returns the fixture's cert state with a DState whose
// only credential entry has an undecodable account payload, so ParseCertState
// returns a result together with a warning.
func partialCertStateData(
	t *testing.T,
	fixture *RawLedgerState,
) cbor.RawMessage {
	t.Helper()

	parts, err := decodeRawArray(fixture.CertStateData)
	require.NoError(t, err)
	require.Len(t, parts, 3)
	credential, err := cbor.Encode([]any{uint64(0), make([]byte, 28)})
	require.NoError(t, err)
	// map(1) { credential: 5 }, where an account payload must be an array.
	credentialMap := append([]byte{0xa1}, credential...)
	credentialMap = append(credentialMap, 0x05)
	dstate, err := cbor.Encode([]cbor.RawMessage{credentialMap})
	require.NoError(t, err)
	data, err := cbor.Encode([]cbor.RawMessage{parts[0], parts[1], dstate})
	require.NoError(t, err)

	parsed, parseErr := ParseCertState(data)
	require.NotNil(t, parsed)
	require.Error(t, parseErr, "the fixture must be a partial parse")
	return data
}

// TestImportLedgerStateRejectsMalformedInputBeforePersisting gives an
// otherwise valid state one malformed consensus input and requires the import
// to fail with nothing persisted. The UTxO phase runs first, so an input that
// is only checked by a later phase leaves imported UTxOs behind.
func TestImportLedgerStateRejectsMalformedInputBeforePersisting(t *testing.T) {
	t.Parallel()

	fixture, err := ParseSnapshot(testdataLedgerSnapshot)
	require.NoError(t, err)
	garbage := cbor.RawMessage{0xff}
	partialCertState := partialCertStateData(t, fixture)
	snapshotParts, err := decodeRawArray(fixture.SnapShotsData)
	require.NoError(t, err)
	snapshotsWithFee, err := cbor.Encode([]any{
		cbor.RawMessage(snapshotParts[0]),
		cbor.RawMessage(snapshotParts[1]),
		cbor.RawMessage(snapshotParts[2]),
		uint64(1),
	})
	require.NoError(t, err)
	currentPParams, previousPParams := distinctConwayPParams(t)

	tests := []struct {
		name    string
		mutate  func(*RawLedgerState)
		wantErr string
	}{
		{name: "valid input imports"},
		{
			name:    "cert state",
			mutate:  func(s *RawLedgerState) { s.CertStateData = garbage },
			wantErr: "parsing cert state",
		},
		{
			name: "partially parsed cert state",
			mutate: func(s *RawLedgerState) {
				s.CertStateData = partialCertState
			},
			wantErr: "parsing cert state",
		},
		{
			name: "stake snapshots",
			mutate: func(s *RawLedgerState) {
				s.SnapShotsData = garbage
			},
			wantErr: "parsing stake snapshots",
		},
		{
			name: "active pool distribution",
			mutate: func(s *RawLedgerState) {
				s.SnapShotsData = fixture.SnapShotsData
				s.PoolDistrData = garbage
			},
			wantErr: "parsing active pool distribution",
		},
		{
			name:    "governance state",
			mutate:  func(s *RawLedgerState) { s.GovStateData = garbage },
			wantErr: "parsing governance state",
		},
		{
			name:    "protocol parameters",
			mutate:  func(s *RawLedgerState) { s.PParamsData = garbage },
			wantErr: "validating protocol parameters",
		},
		{
			name:    "previous protocol parameters",
			mutate:  func(s *RawLedgerState) { s.PrevPParamsData = garbage },
			wantErr: "validating previous protocol parameters",
		},
		{
			name: "fees below snapshot fee pot",
			mutate: func(s *RawLedgerState) {
				s.SnapShotsData = snapshotsWithFee
				s.Fees = 0
			},
			wantErr: "less than the snapshot fee pot",
		},
		{
			name: "previous parameters era unknown",
			mutate: func(s *RawLedgerState) {
				s.SnapShotsData = fixture.SnapShotsData
				s.Fees = fixture.Fees
				s.PParamsData = currentPParams
				s.PrevPParamsData = previousPParams
				s.EraBounds = nil
				s.EraBoundEpoch = s.Epoch
			},
			wantErr: "previous protocol parameters for epoch 99: era cannot be determined",
		},
		{
			name: "go snapshot without historical parameters",
			mutate: func(s *RawLedgerState) {
				s.SnapShotsData = fixture.SnapShotsData
				s.Fees = fixture.Fees
				s.PParamsData = currentPParams
			},
			wantErr: "historical protocol parameters for epoch 99 are unavailable",
		},
		{
			name: "opcert counter key",
			mutate: func(s *RawLedgerState) {
				s.OpCertCounters = map[string]uint64{"short": 1}
			},
			wantErr: "opcert pool key has length 5",
		},
		{
			name: "block count key",
			mutate: func(s *RawLedgerState) {
				s.BlocksCur = map[string]uint64{"short": 1}
			},
			wantErr: "block count pool key has length 5",
		},
		{
			name: "tip hash",
			mutate: func(s *RawLedgerState) {
				s.Tip.BlockHash = []byte{0x01}
			},
			wantErr: "tip hash has 1 bytes",
		},
		{
			name: "evolving nonce",
			mutate: func(s *RawLedgerState) {
				s.EvolvingNonce = []byte{0x01}
			},
			wantErr: "invalid evolving nonce length 1",
		},
		{
			name: "epoch nonce",
			mutate: func(s *RawLedgerState) {
				s.EpochNonce = []byte{0x01}
			},
			wantErr: "invalid epoch nonce length 1",
		},
		{
			name: "candidate nonce",
			mutate: func(s *RawLedgerState) {
				s.CandidateNonce = []byte{0x01}
			},
			wantErr: "invalid candidate nonce length 1",
		},
		{
			name: "last epoch block nonce",
			mutate: func(s *RawLedgerState) {
				s.LastEpochBlockNonce = []byte{0x01}
			},
			wantErr: "invalid last epoch block nonce length 1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, err := dbtest.NewDatabase(
				t, &database.Config{DataDir: t.TempDir()},
			)
			require.NoError(t, err)
			t.Cleanup(func() { dbtest.CloseDatabase(db) })

			addr := buildShelleyAddr(
				0, 1, bytes.Repeat([]byte{0x11}, 28),
				bytes.Repeat([]byte{0x22}, 28),
			)
			nonce := make([]byte, 32)
			state := &RawLedgerState{
				UTxOData: inlineUTxOMap(
					t,
					addr,
					[]uint64{1_000_000},
				),
				Epoch:               100,
				EraIndex:            EraConway,
				EraBounds:           make([]EraBound, EraConway+1),
				EpochNonce:          nonce,
				EvolvingNonce:       nonce,
				CandidateNonce:      nonce,
				LastEpochBlockNonce: nonce,
				Tip: &SnapshotTip{
					Slot:      1_000,
					BlockHash: make([]byte, 32),
				},
			}
			if tt.mutate != nil {
				tt.mutate(state)
			}

			err = ImportLedgerState(context.Background(), ImportConfig{
				Database: db,
				Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
				State:    state,
				EpochLength: func(uint) (uint, uint, error) {
					return 1, 1_000, nil
				},
			})

			raw, rawErr := dbtest.RawSQLiteMetadata(t, db)
			require.NoError(t, rawErr)
			var utxos int
			require.NoError(t, raw.QueryRow(
				"SELECT COUNT(*) FROM utxo",
			).Scan(&utxos))
			if tt.wantErr == "" {
				require.NoError(t, err)
				require.Equal(t, 1, utxos)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
			require.Zero(t, utxos, "a failed import must persist no UTxOs")
		})
	}
}

func TestValidateImportStateRetainsStoredPreviousParameters(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })
	current, previous := distinctConwayPParams(t)
	cfg := previewPParamsImportConfig(db, current, previous)
	cfg.State.Tip = &SnapshotTip{BlockHash: make([]byte, 32)}
	require.NoError(t, importPParams(t.Context(), cfg))
	cfg.State.PrevPParamsData = cbor.RawMessage{0xff}
	require.NoError(
		t,
		validateImportState(cfg),
		"valid stored previous parameters must support catch-up reentry",
	)
}
