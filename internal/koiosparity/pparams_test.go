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

package koiosparity

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// loadPParamsFixture reads one of the real preview `pparams.cbor` blobs
// captured from a synced Dingo metadata database (see testdata/*.hex). Using
// the bytes the node actually stored — rather than a struct re-encoded by the
// test — is what makes these tests evidence that the production decode path
// works on production data.
func loadPParamsFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/" + name)
	require.NoError(t, err)
	b, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	require.NoError(t, err)
	return b
}

// dingoPParamsPreview380 is the effective Dingo-side parameter set for preview
// epoch 380 (resolved from the epoch-360 pparams row), written the way Dingo
// stores it: rationals as num/denom.
func dingoPParamsPreview380() *DingoProtocolParams {
	return &DingoProtocolParams{
		SourceEpoch:          360,
		EraID:                5,
		EraName:              "Babbage",
		MinFeeA:              "44",
		MinFeeB:              "155381",
		MaxBlockBodySize:     "90112",
		MaxTxSize:            "16384",
		MaxBlockHeaderSize:   "1100",
		KeyDeposit:           "2000000",
		PoolDeposit:          "500000000",
		MaxEpoch:             "18",
		NOpt:                 "500",
		A0:                   "3/10",
		Rho:                  "3/1000",
		Tau:                  "1/5",
		ProtocolMajor:        "8",
		ProtocolMinor:        "0",
		MinPoolCost:          "170000000",
		PriceMem:             "577/10000",
		PriceStep:            "721/10000000",
		MaxTxExMem:           "14000000",
		MaxTxExSteps:         "10000000000",
		MaxBlockExMem:        "62000000",
		MaxBlockExSteps:      "20000000000",
		MaxValueSize:         "5000",
		CollateralPercentage: "150",
		MaxCollateralInputs:  "3",
	}
}

// koiosPParamsPreview380 is the same parameter set as Koios /epoch_params
// publishes it for preview epoch 380: rationals as decimals, including the
// exponent form Koios emits for price_step.
func koiosPParamsPreview380() *KoiosEpochParams {
	return &KoiosEpochParams{
		Network:              "preview",
		Epoch:                380,
		Era:                  "Babbage",
		MinFeeA:              "44",
		MinFeeB:              "155381",
		MaxBlockBodySize:     "90112",
		MaxTxSize:            "16384",
		MaxBlockHeaderSize:   "1100",
		KeyDeposit:           "2000000",
		PoolDeposit:          "500000000",
		MaxEpoch:             "18",
		NOpt:                 "500",
		A0:                   "0.3",
		Rho:                  "0.003",
		Tau:                  "0.2",
		ProtocolMajor:        "8",
		ProtocolMinor:        "0",
		MinPoolCost:          "170000000",
		PriceMem:             "0.0577",
		PriceStep:            "7.21e-05",
		MaxTxExMem:           "14000000",
		MaxTxExSteps:         "10000000000",
		MaxBlockExMem:        "62000000",
		MaxBlockExSteps:      "20000000000",
		MaxValueSize:         "5000",
		CollateralPercentage: "150",
		MaxCollateralInputs:  "3",
	}
}

// TestCompareEpochProtocolParamsRationalsMatchKoiosDecimals is trap #2 from
// dingo #3931: Dingo stores execution prices and the reward-formula constants
// as exact rationals ("577/10000", "721/10000000", "3/1000") while Koios
// publishes the same numbers as decimals, sometimes in exponent form
// ("0.0577", "7.21e-05", "0.003"). They are equal, so a checker comparing the
// strings would report five permanent, bogus FAILs on every epoch of every
// run. This is real preview epoch-380 data on both sides.
//
// Discriminates: replacing the rational comparison with string equality makes
// this test report a0/rho/tau/price_mem/price_step mismatches.
func TestCompareEpochProtocolParamsRationalsMatchKoiosDecimals(t *testing.T) {
	now := time.Now()
	got := CompareEpochProtocolParams(
		"preview",
		380,
		koiosPParamsPreview380(),
		dingoPParamsPreview380(),
		nil,
		now,
		0,
		time.Time{},
	)
	require.Empty(
		t,
		got,
		"rational/decimal representations of the same number must not be reported as a divergence",
	)
	require.Equal(t, StatusPass, DetermineStatus(got))
}

// TestCompareEpochProtocolParamsReportsWedgeClassMismatch covers the reason
// the check exists: a stored parameter that differs from the network's is
// wedge-class, so it must be a value_mismatch (FAIL), never an informational
// or ERROR category that a run could be configured to tolerate.
func TestCompareEpochProtocolParamsReportsWedgeClassMismatch(t *testing.T) {
	now := time.Now()
	dingo := dingoPParamsPreview380()
	dingo.MaxTxSize = "32768" // the #3928-class wedge: wrong accepted tx size

	got := CompareEpochProtocolParams(
		"preview",
		380,
		koiosPParamsPreview380(),
		dingo,
		nil,
		now,
		0,
		time.Time{},
	)
	require.Len(t, got, 1)
	require.Equal(t, "pparams_max_tx_size", got[0].Field)
	require.Equal(t, CategoryValueMismatch, got[0].Category)
	require.Equal(t, "32768", got[0].DingoValue)
	require.Equal(t, "16384", got[0].KoiosValue)
	require.Equal(t, uint64(380), got[0].Epoch)
	require.Equal(t, "preview", got[0].Network)
	require.Equal(t, StatusFail, DetermineStatus(got))
}

// TestCompareEpochProtocolParamsReportsEveryDivergingField makes sure the
// comparison is per-field rather than an all-or-nothing struct compare, and
// pins the execution-unit parameters the issue calls out as the sharper
// silent-failure case.
func TestCompareEpochProtocolParamsReportsEveryDivergingField(t *testing.T) {
	now := time.Now()
	dingo := dingoPParamsPreview380()
	dingo.MaxBlockExSteps = "40000000000" // the pre-epoch-107 value
	dingo.MaxTxExMem = "10000000"         // the pre-epoch-9 value
	dingo.CollateralPercentage = "100"
	dingo.PriceMem = "1/10"

	got := CompareEpochProtocolParams(
		"preview",
		380,
		koiosPParamsPreview380(),
		dingo,
		nil,
		now,
		0,
		time.Time{},
	)
	fields := make([]string, 0, len(got))
	for _, m := range got {
		require.Equal(t, CategoryValueMismatch, m.Category)
		fields = append(fields, m.Field)
	}
	require.ElementsMatch(t, []string{
		"pparams_max_block_ex_steps",
		"pparams_max_tx_ex_mem",
		"pparams_collateral_percentage",
		"pparams_price_mem",
	}, fields)
}

// TestCompareEpochProtocolParamsReportsEraMismatch: the era in force decides
// which validation rules run at all, so a disagreement about it is at least
// as serious as any single parameter.
func TestCompareEpochProtocolParamsReportsEraMismatch(t *testing.T) {
	now := time.Now()
	dingo := dingoPParamsPreview380()
	dingo.EraName = "Alonzo"

	got := CompareEpochProtocolParams(
		"preview",
		380,
		koiosPParamsPreview380(),
		dingo,
		nil,
		now,
		0,
		time.Time{},
	)
	require.Len(t, got, 1)
	require.Equal(t, "pparams_era", got[0].Field)
	require.Equal(t, CategoryValueMismatch, got[0].Category)
	require.Equal(t, StatusFail, DetermineStatus(got))
}

// TestCompareEpochProtocolParamsPresenceDisagreementIsAMismatch: a parameter
// Koios reports but Dingo's era-decoded row does not define (or vice versa)
// is a disagreement about the shape of the ledger state, not something to
// skip quietly. Skipping it is what would let an era-gating bug read as PASS.
func TestCompareEpochProtocolParamsPresenceDisagreementIsAMismatch(t *testing.T) {
	now := time.Now()

	dingoAbsent := dingoPParamsPreview380()
	dingoAbsent.PriceStep = "" // era decoded without execution pricing
	got := CompareEpochProtocolParams(
		"preview", 380, koiosPParamsPreview380(), dingoAbsent, nil, now, 0, time.Time{},
	)
	require.Len(t, got, 1)
	require.Equal(t, "pparams_price_step", got[0].Field)
	require.Equal(t, CategoryValueMismatch, got[0].Category)
	require.Empty(t, got[0].DingoValue)
	require.Equal(t, "7.21e-05", got[0].KoiosValue)

	koiosAbsent := koiosPParamsPreview380()
	koiosAbsent.MaxCollateralInputs = "" // Koios published null
	got = CompareEpochProtocolParams(
		"preview", 380, koiosAbsent, dingoPParamsPreview380(), nil, now, 0, time.Time{},
	)
	require.Len(t, got, 1)
	require.Equal(t, "pparams_max_collateral_inputs", got[0].Field)
	require.Equal(t, CategoryValueMismatch, got[0].Category)
}

// TestCompareEpochProtocolParamsBothAbsentIsNotAMismatch: on a pre-Alonzo
// era neither side defines the execution-unit parameters, and both agreeing
// that a parameter does not exist is agreement, not divergence.
func TestCompareEpochProtocolParamsBothAbsentIsNotAMismatch(t *testing.T) {
	now := time.Now()
	dingo := dingoPParamsPreview380()
	koios := koiosPParamsPreview380()
	dingo.EraName, koios.Era = "Mary", "Mary"
	for _, p := range []*string{
		&dingo.PriceMem, &dingo.PriceStep, &dingo.MaxTxExMem, &dingo.MaxTxExSteps,
		&dingo.MaxBlockExMem, &dingo.MaxBlockExSteps, &dingo.MaxValueSize,
		&dingo.CollateralPercentage, &dingo.MaxCollateralInputs,
	} {
		*p = ""
	}
	for _, p := range []*string{
		&koios.PriceMem, &koios.PriceStep, &koios.MaxTxExMem, &koios.MaxTxExSteps,
		&koios.MaxBlockExMem, &koios.MaxBlockExSteps, &koios.MaxValueSize,
		&koios.CollateralPercentage, &koios.MaxCollateralInputs,
	} {
		*p = ""
	}

	got := CompareEpochProtocolParams(
		"preview", 380, koios, dingo, nil, now, 0, time.Time{},
	)
	require.Empty(t, got)
}

// TestCompareEpochProtocolParamsMissingDingoRow: an unresolvable Dingo row is
// not the same finding as a differing value. Nothing was compared, so it must
// not be a FAIL that claims a divergence was found — and it must not be a
// silent PASS either. Inside the grace window after epoch close it is
// reference_lag; past it, dingo_db_missing. Both are ERROR.
func TestCompareEpochProtocolParamsMissingDingoRow(t *testing.T) {
	now := time.Now()
	closed := now.Add(-2 * time.Hour)

	got := CompareEpochProtocolParams(
		"preview", 380, koiosPParamsPreview380(), nil, nil, now, 0, time.Time{},
	)
	require.Len(t, got, 1)
	require.Equal(t, "protocol_params", got[0].Field)
	require.Equal(t, CategoryDBMissing, got[0].Category)
	require.Equal(t, StatusError, DetermineStatus(got))

	got = CompareEpochProtocolParams(
		"preview", 380, koiosPParamsPreview380(), nil, nil, now, 6, closed,
	)
	require.Len(t, got, 1)
	require.Equal(t, "protocol_params", got[0].Field)
	require.Equal(t, CategoryReferenceLag, got[0].Category)
	require.Equal(t, StatusError, DetermineStatus(got))

	// Past the grace window the absence is a real gap again.
	got = CompareEpochProtocolParams(
		"preview", 380, koiosPParamsPreview380(), nil, nil, now, 1, closed,
	)
	require.Len(t, got, 1)
	require.Equal(t, CategoryDBMissing, got[0].Category)
}

// TestCompareEpochProtocolParamsMissingKoiosRow mirrors CompareEpochTotals'
// koios_totals handling: a cache with no /epoch_params row for this epoch
// (fetched before this comparison existed, or a --skip-fetch run) must be
// reported, never treated as "nothing to compare" and folded into a PASS.
func TestCompareEpochProtocolParamsMissingKoiosRow(t *testing.T) {
	now := time.Now()
	got := CompareEpochProtocolParams(
		"preview", 380, nil, dingoPParamsPreview380(), nil, now, 0, time.Time{},
	)
	require.Len(t, got, 1)
	require.Equal(t, "koios_epoch_params", got[0].Field)
	require.Equal(t, CategoryDBMissing, got[0].Category)
	require.Equal(t, StatusError, DetermineStatus(got))
}

// TestCompareEpochProtocolParamsFetchError: a failed Dingo read is a
// dingo_db_error, distinct from an absent row, and must suppress the
// field comparisons rather than compare against a zero value.
func TestCompareEpochProtocolParamsFetchError(t *testing.T) {
	now := time.Now()
	got := CompareEpochProtocolParams(
		"preview",
		380,
		koiosPParamsPreview380(),
		nil,
		errors.New("boom"),
		now,
		0,
		time.Time{},
	)
	require.Len(t, got, 1)
	require.Equal(t, "protocol_params", got[0].Field)
	require.Equal(t, CategoryDBError, got[0].Category)
	require.Contains(t, got[0].DingoValue, "boom")
	require.Equal(t, StatusError, DetermineStatus(got))
}

// TestDingoDBGetProtocolParamsResolvesEffectiveRow is trap #1 from dingo
// #3931: `pparams` holds one row per parameter *change*, not one per epoch —
// preview has ~12 rows spanning epochs 0-415. Asking for epoch 200 must
// resolve the latest row at or before it (the epoch-107 row), not report the
// parameters as missing and not fall back to an older row.
//
// The fixtures are the real preview CBOR blobs for the epoch-22 and epoch-107
// rows, which differ in max_block_ex_steps (40000000000 -> 20000000000). That
// makes the assertion a real comparison of a value that genuinely changed,
// not a match on a static genesis default.
//
// Discriminates: an exact-epoch lookup returns nil here; a lookup ordered the
// wrong way returns 40000000000.
func TestDingoDBGetProtocolParamsResolvesEffectiveRow(t *testing.T) {
	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck

	seedEpochEra(t, gdb, 200, 5)
	seedPParams(t, gdb, 1, 1_900_800, 22, 5,
		loadPParamsFixture(t, "pparams_preview_epoch22_babbage.hex"))
	seedPParams(t, gdb, 2, 9_244_800, 107, 5,
		loadPParamsFixture(t, "pparams_preview_epoch107_babbage.hex"))

	got, err := dingo.GetProtocolParams(context.Background(), 200)
	require.NoError(t, err)
	require.NotNil(t, got, "epoch 200 has no pparams row of its own; the effective row must still resolve")
	require.Equal(t, uint64(107), got.SourceEpoch)
	require.Equal(t, "Babbage", got.EraName)
	require.Equal(t, "20000000000", got.MaxBlockExSteps)
	require.Equal(t, "62000000", got.MaxBlockExMem)
	require.Equal(t, "16384", got.MaxTxSize)
	require.Equal(t, "577/10000", got.PriceMem)
	require.Equal(t, "721/10000000", got.PriceStep)
	require.Equal(t, "3/10", got.A0)

	// The epoch-22 row is still the effective one for an epoch before the
	// change, which is what makes the resolution above a real selection.
	seedEpochEra(t, gdb, 50, 5)
	got, err = dingo.GetProtocolParams(context.Background(), 50)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint64(22), got.SourceEpoch)
	require.Equal(t, "40000000000", got.MaxBlockExSteps)
}

// TestDingoDBGetProtocolParamsUsesTheEpochsOwnEra pins the second trap in the
// same table: at an era boundary Dingo writes BOTH an old-era row and a
// new-era row at the same epoch (preview really does have two epoch-2 rows,
// era 4 and era 5, and two epoch-3 rows). The `epoch` table is what says
// which era is actually in force, and the decoder must be chosen from that —
// the two rows have different CBOR shapes, so picking the wrong one either
// fails to decode or yields another era's parameter layout.
//
// Discriminates: an era-unfiltered "latest row wins" lookup picks the
// Babbage row (id 2 below) and reports protocol_major 7, not Alonzo's 6.
func TestDingoDBGetProtocolParamsUsesTheEpochsOwnEra(t *testing.T) {
	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck

	// Preview's real layout: epoch 2 is still Alonzo, epoch 3 is Babbage.
	seedEpochEra(t, gdb, 2, 4)
	seedEpochEra(t, gdb, 3, 5)
	seedPParams(t, gdb, 1, 172_800, 2, 4,
		loadPParamsFixture(t, "pparams_preview_epoch2_alonzo.hex"))
	seedPParams(t, gdb, 2, 259_200, 2, 5,
		loadPParamsFixture(t, "pparams_preview_epoch2_babbage.hex"))

	got, err := dingo.GetProtocolParams(context.Background(), 2)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint(4), got.EraID)
	require.Equal(t, "Alonzo", got.EraName)
	require.Equal(t, "6", got.ProtocolMajor)

	got, err = dingo.GetProtocolParams(context.Background(), 3)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint(5), got.EraID)
	require.Equal(t, "Babbage", got.EraName)
	require.Equal(t, "7", got.ProtocolMajor)
}

// TestDingoDBGetProtocolParamsAbsent: no epoch row, or an epoch whose era has
// no parameter row at or before it, resolves to nil rather than an error, so
// CompareEpochProtocolParams classifies it as a missing row (ERROR) instead
// of a DB failure.
func TestDingoDBGetProtocolParamsAbsent(t *testing.T) {
	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck

	got, err := dingo.GetProtocolParams(context.Background(), 200)
	require.NoError(t, err)
	require.Nil(t, got)

	seedEpochEra(t, gdb, 200, 5)
	got, err = dingo.GetProtocolParams(context.Background(), 200)
	require.NoError(t, err)
	require.Nil(t, got, "an epoch row with no pparams row for its era resolves to nil")
}

func seedEpochEra(t *testing.T, gdb *testDB, epoch uint64, eraID uint) {
	t.Helper()
	require.NoError(t, gdb.Exec(
		`INSERT INTO epoch (epoch_id, start_slot, era_id, slot_length, length_in_slots) VALUES (?,?,?,?,?)`,
		epoch, 0, eraID, 1000, 86400,
	).Error)
}

func seedPParams(
	t *testing.T,
	gdb *testDB,
	id uint,
	addedSlot, epoch uint64,
	eraID uint,
	cborBytes []byte,
) {
	t.Helper()
	require.NoError(t, gdb.Exec(
		`INSERT INTO pparams (id, cbor, added_slot, epoch, era_id) VALUES (?,?,?,?,?)`,
		id, cborBytes, addedSlot, epoch, eraID,
	).Error)
}

// previewBabbageEpochParamsTmpl is the real Koios /epoch_params body for
// preview epoch 107 with the epoch number templated, matching the
// pparams_preview_epoch107_babbage.hex CBOR fixture field for field —
// including both cost models (PlutusV1: 166 entries, PlutusV2: 175). Test
// fake servers serve this so an epoch fetched through the normal path lands a
// parameter row that agrees with seedDingoBabbageProtocolParams' Dingo side.
//
// Loaded at package init rather than through a testing.T helper because the
// fake Koios servers write it from HTTP handler goroutines, where require/
// t.Fatalf must not be called.
var previewBabbageEpochParamsTmpl = mustLoadEpochParamsTemplate()

func mustLoadEpochParamsTemplate() string {
	raw, err := os.ReadFile(
		"testdata/koios_epoch_params_preview_epoch107.json",
	)
	if err != nil {
		panic("koiosparity test fixture: " + err.Error())
	}
	return strings.TrimSpace(string(raw))
}

// seedDingoBabbageProtocolParams gives a Dingo metadata fixture the two rows
// GetProtocolParams needs: an `epoch` row naming the era in force, and a
// single `pparams` row at epoch 0 that every requested epoch then resolves to
// as its effective parameter set. The CBOR is the real preview epoch-107
// blob, so the values agree with previewBabbageEpochParamsTmpl.
func seedDingoBabbageProtocolParams(
	t *testing.T,
	gdb *testDB,
	epochs ...uint64,
) {
	t.Helper()
	require.NoError(t, gdb.Exec(
		`INSERT OR IGNORE INTO pparams (id, cbor, added_slot, epoch, era_id) VALUES (1,?,0,0,5)`,
		loadPParamsFixture(t, "pparams_preview_epoch107_babbage.hex"),
	).Error)
	for _, epoch := range epochs {
		require.NoError(t, gdb.Exec(
			`INSERT OR IGNORE INTO epoch (epoch_id, start_slot, era_id, slot_length, length_in_slots)
			 VALUES (?,0,5,1000,86400)`,
			epoch,
		).Error)
	}
}

// seedKoiosBabbageProtocolParams caches the Koios side of the same fixture,
// for tests that build their cache directly instead of fetching through a
// fake Koios server.
func seedKoiosBabbageProtocolParams(
	t *testing.T,
	cache *Cache,
	network string,
	epochs ...uint64,
) {
	t.Helper()
	for _, epoch := range epochs {
		var resp []KoiosEpochParamsResp
		require.NoError(t, json.Unmarshal(
			fmt.Appendf(nil, previewBabbageEpochParamsTmpl, strconv.FormatUint(epoch, 10)),
			&resp,
		))
		require.Len(t, resp, 1)
		require.NoError(t, cache.UpsertEpochParams(
			epochParamsFromKoios(network, epoch, &resp[0], time.Now()),
		))
	}
}

// TestSeededProtocolParamsFixturesAgree pins the two halves of the shared
// fixture to each other: if the CBOR blob and the Koios JSON above ever drift
// apart, every test that relies on them would start reporting parameter
// mismatches for reasons unrelated to what it is testing, so the drift is
// caught here instead.
func TestSeededProtocolParamsFixturesAgree(t *testing.T) {
	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck
	seedDingoBabbageProtocolParams(t, gdb, 10)

	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck
	seedKoiosBabbageProtocolParams(t, cache, "preview", 10)

	dingoParams, err := dingo.GetProtocolParams(context.Background(), 10)
	require.NoError(t, err)
	require.NotNil(t, dingoParams)
	koiosParams, err := cache.GetEpochParams("preview", 10)
	require.NoError(t, err)

	require.Empty(t, CompareEpochProtocolParams(
		"preview", 10, koiosParams, dingoParams, nil, time.Now(), 0, time.Time{},
	))
}

// TestDatabaseSourceGetProtocolParams exercises the in-process observer's
// source against the same live *database.Database a running node writes
// through, confirming both RewardParitySource implementations resolve the
// effective row identically — the property source.go's doc comment requires
// of every implementation.
func TestDatabaseSourceGetProtocolParams(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	sqlDB := sourceSQLDB(t, db)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	// Epoch 200 has no pparams row of its own; the epoch-107 row is the
	// effective one, exactly as in TestDingoDBGetProtocolParamsResolvesEffectiveRow.
	seedEpochEra(t, sqlDB, 200, 5)
	seedPParams(t, sqlDB, 1, 1_900_800, 22, 5,
		loadPParamsFixture(t, "pparams_preview_epoch22_babbage.hex"))
	seedPParams(t, sqlDB, 2, 9_244_800, 107, 5,
		loadPParamsFixture(t, "pparams_preview_epoch107_babbage.hex"))

	got, err := source.GetProtocolParams(context.Background(), 200)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint64(107), got.SourceEpoch)
	require.Equal(t, "Babbage", got.EraName)
	require.Equal(t, "20000000000", got.MaxBlockExSteps)
	require.Equal(t, "16384", got.MaxTxSize)
	require.Equal(t, "577/10000", got.PriceMem)

	// An epoch with no `epoch` row at all resolves to nil, not an error.
	got, err = source.GetProtocolParams(context.Background(), 900)
	require.NoError(t, err)
	require.Nil(t, got)
}

// seedProtocolParamsCheckFixture builds the smallest Dingo+cache pair that
// Check reports a clean PASS for at koiosEpoch, so a test can then perturb
// exactly one protocol-parameter input and attribute the result to it.
func seedProtocolParamsCheckFixture(
	t *testing.T,
	network string,
	koiosEpoch uint64,
	seedKoiosParams bool,
	mutateDingo func(gdb *testDB),
) (dingoDir, cachePath string) {
	t.Helper()
	dingoDir, gdb := newTestDingoDB(t)

	require.NoError(t, gdb.Create(&models.EpochSummary{
		Epoch:            koiosEpoch - 1,
		TotalActiveStake: types.Uint64(5_000_000),
		SnapshotReady:    true,
	}).Error)
	require.NoError(t, gdb.Create(&models.EpochSummary{
		Epoch:            koiosEpoch,
		TotalActiveStake: types.Uint64(5_000_000),
		SnapshotReady:    true,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardAdaPots{
		Epoch:    koiosEpoch,
		Treasury: types.Uint64(1_000),
		Reserves: types.Uint64(2_000),
		Fees:     types.Uint64(300),
	}).Error)
	seedDingoBabbageProtocolParams(t, gdb, koiosEpoch)
	if mutateDingo != nil {
		mutateDingo(gdb)
	}

	sqlDB, err := gdb.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	cachePath = filepath.Join(t.TempDir(), "cache.db")
	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	fetchedAt := time.Now().Add(-time.Hour).UTC()
	require.NoError(t, cache.CommitEpochData(
		KoiosEpochInfo{
			Network:      network,
			Epoch:        koiosEpoch,
			ActiveStake:  "5000000",
			EpochEndTime: fetchedAt,
			FetchedAt:    fetchedAt,
		},
		nil,
		&KoiosTotals{
			Network:   network,
			Epoch:     koiosEpoch,
			Treasury:  "1000",
			Reserves:  "2000",
			Fees:      "300",
			FetchedAt: fetchedAt,
		},
	))
	if seedKoiosParams {
		seedKoiosBabbageProtocolParams(t, cache, network, koiosEpoch)
	}
	return dingoDir, cachePath
}

func runProtocolParamsCheck(
	t *testing.T,
	network, dingoDir, cachePath string,
	koiosEpoch uint64,
) ([]CheckMismatch, *CheckResult) {
	t.Helper()
	result, err := Check(context.Background(), CheckConfig{
		Network:   network,
		DingoDB:   DingoDBConfig{Plugin: "sqlite", DataDir: dingoDir},
		CachePath: cachePath,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(t, 1, result.EpochsChecked)

	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck
	mismatches, err := cache.GetMismatches(network, koiosEpoch, "")
	require.NoError(t, err)
	return mismatches, result
}

// TestCheckPassesWithMatchingProtocolParams is the control for the two tests
// below: with both sides present and agreeing, the new comparison contributes
// nothing, so any finding they report is attributable to what they changed.
func TestCheckPassesWithMatchingProtocolParams(t *testing.T) {
	const network, koiosEpoch = "preview", uint64(10)
	dingoDir, cachePath := seedProtocolParamsCheckFixture(
		t, network, koiosEpoch, true, nil,
	)
	mismatches, result := runProtocolParamsCheck(
		t, network, dingoDir, cachePath, koiosEpoch,
	)
	require.Empty(t, mismatches)
	require.Empty(t, result.FailEpochs)
	require.Empty(t, result.ErrorEpochs)
}

// TestCheckDetectsWedgeClassProtocolParamDivergence is the end-to-end version
// of the reason this comparison exists: a stored max_tx_size that disagrees
// with the network is the #3928 wedge, and before this change the parity run
// reported PASS for exactly this database.
func TestCheckDetectsWedgeClassProtocolParamDivergence(t *testing.T) {
	const network, koiosEpoch = "preview", uint64(10)
	dingoDir, cachePath := seedProtocolParamsCheckFixture(
		t, network, koiosEpoch, true,
		func(gdb *testDB) {
			// Replace the effective row with the preview epoch-2 Babbage
			// parameters, whose max_block_body_size (65536), n_opt (150),
			// max_tx_ex_mem and max_block_ex_* all predate later updates.
			require.NoError(t, gdb.Exec(
				`UPDATE pparams SET cbor = ? WHERE id = 1`,
				loadPParamsFixture(t, "pparams_preview_epoch2_babbage.hex"),
			).Error)
		},
	)
	mismatches, result := runProtocolParamsCheck(
		t, network, dingoDir, cachePath, koiosEpoch,
	)
	require.Equal(t, []uint64{koiosEpoch}, result.FailEpochs)

	fields := make([]string, 0, len(mismatches))
	for _, m := range mismatches {
		require.Equal(t, CategoryValueMismatch, m.Category)
		fields = append(fields, m.Field)
	}
	require.ElementsMatch(t, []string{
		"pparams_max_block_body_size",
		"pparams_n_opt",
		"pparams_protocol_major",
		"pparams_max_tx_ex_mem",
		"pparams_max_block_ex_mem",
		"pparams_max_block_ex_steps",
		// Preview repriced both Plutus cost models between epoch 2 and
		// epoch 107, so swapping the row also exercises the cost-model
		// comparison end to end through the real Check path.
		"pparams_cost_model_plutus_v1",
		"pparams_cost_model_plutus_v2",
	}, fields)

	for _, m := range mismatches {
		if !strings.HasPrefix(m.Field, "pparams_cost_model_") {
			continue
		}
		require.Contains(t, m.DingoValue, "entries differ",
			"a cost-model finding must name the entry, not dump the array")
		require.Less(t, len(m.DingoValue), 120,
			"a cost-model finding must stay small enough to read in a report")
	}
}

// TestCheckDetectsMissingKoiosEpochParamsOnUpgradedCache mirrors
// TestCheckDetectsMissingKoiosTotalsOnUpgradedCache for /epoch_params: a
// cache.db written before protocol-parameter fetching existed has a
// koios_epoch_info row but no koios_epoch_params row, and that must surface
// as ERROR rather than a PASS that validated no parameters at all.
func TestCheckDetectsMissingKoiosEpochParamsOnUpgradedCache(t *testing.T) {
	const network, koiosEpoch = "preview", uint64(10)
	dingoDir, cachePath := seedProtocolParamsCheckFixture(
		t, network, koiosEpoch, false, nil,
	)
	mismatches, result := runProtocolParamsCheck(
		t, network, dingoDir, cachePath, koiosEpoch,
	)
	require.Empty(t, result.FailEpochs)
	require.Equal(t, []uint64{koiosEpoch}, result.ErrorEpochs)
	require.Len(t, mismatches, 1)
	require.Equal(t, "koios_epoch_params", mismatches[0].Field)
	require.Equal(t, CategoryDBMissing, mismatches[0].Category)
}

// TestCheckDetectsMissingDingoProtocolParams: a node with no resolvable
// parameter row for the epoch has nothing to compare, which is an ERROR
// (dingo_db_missing) and specifically not a FAIL — no divergence was shown.
func TestCheckDetectsMissingDingoProtocolParams(t *testing.T) {
	const network, koiosEpoch = "preview", uint64(10)
	dingoDir, cachePath := seedProtocolParamsCheckFixture(
		t, network, koiosEpoch, true,
		func(gdb *testDB) {
			require.NoError(t, gdb.Exec(`DELETE FROM pparams`).Error)
		},
	)
	mismatches, result := runProtocolParamsCheck(
		t, network, dingoDir, cachePath, koiosEpoch,
	)
	require.Empty(t, result.FailEpochs)
	require.Equal(t, []uint64{koiosEpoch}, result.ErrorEpochs)
	require.Len(t, mismatches, 1)
	require.Equal(t, "protocol_params", mismatches[0].Field)
	require.Equal(t, CategoryDBMissing, mismatches[0].Category)
}

// costModelFixture returns the PlutusV1/PlutusV2 cost models the shared
// preview epoch-107 fixture carries on both sides, so a test can perturb one
// entry and attribute the finding to it.
func costModelFixture(t *testing.T) map[string][]int64 {
	t.Helper()
	var resp []struct {
		CostModels map[string][]int64 `json:"cost_models"`
	}
	require.NoError(t, json.Unmarshal(
		fmt.Appendf(nil, previewBabbageEpochParamsTmpl, "107"),
		&resp,
	))
	require.Len(t, resp, 1)
	require.Len(t, resp[0].CostModels["PlutusV1"], 166)
	require.Len(t, resp[0].CostModels["PlutusV2"], 175)
	return resp[0].CostModels
}

func koiosCostModelsJSON(t *testing.T, models map[string][]int64) string {
	t.Helper()
	b, err := json.Marshal(models)
	require.NoError(t, err)
	return string(b)
}

// TestCompareEpochProtocolParamsCostModelsMatch is the control: the real
// preview epoch-107 cost models, decoded from Dingo's CBOR into
// map[uint][]int64 (0 = PlutusV1, 1 = PlutusV2) and from Koios's
// name-keyed dict, are entry-for-entry identical and must compare clean.
func TestCompareEpochProtocolParamsCostModelsMatch(t *testing.T) {
	models := costModelFixture(t)
	dingo := dingoPParamsPreview380()
	dingo.CostModels = models
	koios := koiosPParamsPreview380()
	koios.CostModels = koiosCostModelsJSON(t, models)

	require.Empty(t, CompareEpochProtocolParams(
		"preview", 380, koios, dingo, nil, time.Now(), 0, time.Time{},
	))
}

// TestCompareEpochProtocolParamsCostModelEntryDiverges: a single mispriced
// operation is the divergence this coverage is for, and the report has to
// name the language and the entry — dumping 166 integers into a mismatch row
// would be unusable.
func TestCompareEpochProtocolParamsCostModelEntryDiverges(t *testing.T) {
	models := costModelFixture(t)
	dingo := dingoPParamsPreview380()
	dingo.CostModels = models
	koios := koiosPParamsPreview380()

	// Bound to a local and length-guarded explicitly rather than indexed
	// straight out of the map: a map read yields a nil slice for a missing
	// key, and indexing that would be a panic rather than a test failure.
	perturbedV1 := append([]int64(nil), models["PlutusV1"]...)
	if len(perturbedV1) <= 42 {
		t.Fatalf("fixture PlutusV1 model has %d entries", len(perturbedV1))
	}
	perturbedV1[42]++
	koios.CostModels = koiosCostModelsJSON(t, map[string][]int64{
		"PlutusV1": perturbedV1,
		"PlutusV2": models["PlutusV2"],
	})

	got := CompareEpochProtocolParams(
		"preview", 380, koios, dingo, nil, time.Now(), 0, time.Time{},
	)
	require.Len(t, got, 1)
	require.Equal(t, "pparams_cost_model_plutus_v1", got[0].Field)
	require.Equal(t, CategoryValueMismatch, got[0].Category)
	require.Contains(t, got[0].DingoValue, "entry 42 = ")
	require.Contains(t, got[0].KoiosValue, "entry 42 = ")
	require.Contains(t, got[0].DingoValue, "1 of 166 entries differ")
	require.NotEqual(t, got[0].DingoValue, got[0].KoiosValue)
	require.Equal(t, StatusFail, DetermineStatus(got))
}

// TestCompareEpochProtocolParamsCostModelLengthDiverges: a model with the
// wrong number of operations prices every later operation wrongly, so the
// entry count is reported on its own rather than as a first-index diff.
func TestCompareEpochProtocolParamsCostModelLengthDiverges(t *testing.T) {
	models := costModelFixture(t)
	fullV1 := models["PlutusV1"]
	if len(fullV1) != 166 {
		t.Fatalf("fixture PlutusV1 model has %d entries, want 166", len(fullV1))
	}
	dingo := dingoPParamsPreview380()
	dingo.CostModels = map[string][]int64{
		"PlutusV1": fullV1[:165],
		"PlutusV2": models["PlutusV2"],
	}
	koios := koiosPParamsPreview380()
	koios.CostModels = koiosCostModelsJSON(t, models)

	got := CompareEpochProtocolParams(
		"preview", 380, koios, dingo, nil, time.Now(), 0, time.Time{},
	)
	require.Len(t, got, 1)
	require.Equal(t, "pparams_cost_model_plutus_v1", got[0].Field)
	require.Equal(t, "165 entries", got[0].DingoValue)
	require.Equal(t, "166 entries", got[0].KoiosValue)
}

// TestCompareEpochProtocolParamsCostModelLanguagePresence: a language one
// side prices and the other does not is a divergence about which scripts can
// run at all, and must not be skipped just because the key is absent from one
// map.
func TestCompareEpochProtocolParamsCostModelLanguagePresence(t *testing.T) {
	models := costModelFixture(t)

	dingo := dingoPParamsPreview380()
	dingo.CostModels = map[string][]int64{"PlutusV1": models["PlutusV1"]}
	koios := koiosPParamsPreview380()
	koios.CostModels = koiosCostModelsJSON(t, models)

	got := CompareEpochProtocolParams(
		"preview", 380, koios, dingo, nil, time.Now(), 0, time.Time{},
	)
	require.Len(t, got, 1)
	require.Equal(t, "pparams_cost_model_plutus_v2", got[0].Field)
	require.Empty(t, got[0].DingoValue)
	require.Equal(t, "175 entries", got[0].KoiosValue)

	// ...and the reverse: Dingo prices a language Koios does not.
	dingo.CostModels = map[string][]int64{
		"PlutusV1": models["PlutusV1"],
		"PlutusV2": models["PlutusV2"],
		"PlutusV3": {1, 2, 3},
	}
	got = CompareEpochProtocolParams(
		"preview", 380, koios, dingo, nil, time.Now(), 0, time.Time{},
	)
	require.Len(t, got, 1)
	require.Equal(t, "pparams_cost_model_plutus_v3", got[0].Field)
	require.Equal(t, "3 entries", got[0].DingoValue)
	require.Empty(t, got[0].KoiosValue)
}

// TestCompareEpochProtocolParamsCostModelsAbsentBothSides: pre-Alonzo eras
// price no scripts at all, and both sides agreeing on that is agreement.
func TestCompareEpochProtocolParamsCostModelsAbsentBothSides(t *testing.T) {
	dingo := dingoPParamsPreview380()
	koios := koiosPParamsPreview380()
	require.Nil(t, dingo.CostModels)
	require.Empty(t, koios.CostModels)

	require.Empty(t, CompareEpochProtocolParams(
		"preview", 380, koios, dingo, nil, time.Now(), 0, time.Time{},
	))
}

// TestCompareEpochProtocolParamsRejectsMalformedKoiosCostModels: cached cost
// models that will not parse must surface, never silently drop the whole
// cost-model comparison and let the epoch read as PASS.
func TestCompareEpochProtocolParamsRejectsMalformedKoiosCostModels(t *testing.T) {
	models := costModelFixture(t)
	dingo := dingoPParamsPreview380()
	dingo.CostModels = models
	koios := koiosPParamsPreview380()
	koios.CostModels = `{"PlutusV1": "not-an-array"}`

	got := CompareEpochProtocolParams(
		"preview", 380, koios, dingo, nil, time.Now(), 0, time.Time{},
	)
	require.Len(t, got, 1)
	require.Equal(t, "pparams_cost_models", got[0].Field)
	require.Equal(t, CategoryValueMismatch, got[0].Category)
	require.Contains(t, got[0].KoiosValue, "unparseable")
	require.Equal(t, StatusFail, DetermineStatus(got))
}

// TestDingoDBGetProtocolParamsDecodesCostModels pins the language-key mapping
// against real stored CBOR: Dingo keys cost models 0/1 where Koios names them
// PlutusV1/PlutusV2, and the preview epoch-107 row carries 166 and 175
// entries respectively.
func TestDingoDBGetProtocolParamsDecodesCostModels(t *testing.T) {
	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck
	seedDingoBabbageProtocolParams(t, gdb, 107)

	got, err := dingo.GetProtocolParams(context.Background(), 107)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Len(t, got.CostModels, 2)
	require.Len(t, got.CostModels["PlutusV1"], 166)
	require.Len(t, got.CostModels["PlutusV2"], 175)

	// Entry-for-entry equality with what Koios publishes for the same epoch
	// is the property the comparison depends on.
	require.Equal(t, costModelFixture(t), got.CostModels)
}
