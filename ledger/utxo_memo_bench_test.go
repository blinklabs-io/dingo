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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"

	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// This file benchmarks (*LedgerState).ValidateTx and LedgerView.UtxoById
// using real Preprod chain bytes for blinklabs-io/dingo#4226 (the per-view
// UTxO memo): the same fixtures
// ledger/eras/conway_plutus_preprod_fixture_test.go uses to pin execution
// units against producer-declared budgets. That test documents the
// fixture's provenance: a producer-accepted Preprod block and the funding
// transactions for one Plutus transaction inside it, fetched over NtN
// blockfetch.
//
// The existing BenchmarkTransactionValidation (ledger/benchmark_test.go)
// validates against an unfunded ledger and discards the error, so it times
// the missing-input failure path rather than real validation and cannot show
// this issue's DB-read cost or its removal (see the issue's "Relationship to
// other issues" section). This file adds a DB-backed benchmark on a funded
// fixture instead of changing that one.

const utxoMemoBenchFixtureDir = "eras/testdata"

// preprod slot/time conversion constants, duplicated from
// ledger/eras/conway_plutus_preprod_fixture_test.go (unexported there). The
// Conway V3 script context encodes the tx validity range as POSIX
// milliseconds, so an approximate conversion would change the bytes the
// Plutus script branches on and desync the benchmark from the real,
// producer-accepted execution path.
const (
	utxoMemoBenchSystemStart   = 1_654_041_600
	utxoMemoBenchByronSlots    = 86_400
	utxoMemoBenchByronSlotSecs = 20
)

func utxoMemoBenchSlotToTime(slot uint64) (time.Time, error) {
	if slot < utxoMemoBenchByronSlots {
		return time.Unix(
			utxoMemoBenchSystemStart+int64(slot)*utxoMemoBenchByronSlotSecs,
			0,
		).UTC(), nil
	}
	byronEnd := int64(utxoMemoBenchSystemStart) +
		int64(utxoMemoBenchByronSlots)*utxoMemoBenchByronSlotSecs
	return time.Unix(byronEnd+int64(slot-utxoMemoBenchByronSlots), 0).UTC(), nil
}

func utxoMemoBenchTimeToSlot(t time.Time) (uint64, error) {
	byronEnd := int64(utxoMemoBenchSystemStart) +
		int64(utxoMemoBenchByronSlots)*utxoMemoBenchByronSlotSecs
	if t.Unix() < byronEnd {
		return uint64(
			(t.Unix() - utxoMemoBenchSystemStart) / utxoMemoBenchByronSlotSecs,
		), nil
	}
	return utxoMemoBenchByronSlots + uint64(t.Unix()-byronEnd), nil
}

func readUtxoMemoBenchFixture(tb testing.TB, name string) []byte {
	tb.Helper()
	raw, err := os.ReadFile(filepath.Join(utxoMemoBenchFixtureDir, name))
	require.NoError(tb, err)
	return raw
}

// utxoMemoBenchProtocolParams builds Conway protocol parameters for the
// benchmark fixture. The base is ouroboros-mock's NewMockConwayProtocolParams
// (representative mainnet-scale MinFeeA/B, size limits, deposits); CostModels,
// ProtocolVersion and MaxTxExUnits are overridden from the real Preprod
// epoch 309/310 fixture so Plutus cost accounting matches chain-observed
// values exactly.
func utxoMemoBenchProtocolParams(
	tb testing.TB,
) *conway.ConwayProtocolParameters {
	tb.Helper()
	var costModels struct {
		PlutusV3 []int64 `json:"PlutusV3"`
	}
	require.NoError(tb, json.Unmarshal(
		readUtxoMemoBenchFixture(tb, "preprod-costmodels-plutusv3-pv11.json"),
		&costModels,
	))
	require.NotEmpty(tb, costModels.PlutusV3)

	pp := mockledger.NewMockConwayProtocolParams()
	pp.ProtocolVersion = lcommon.ProtocolParametersProtocolVersion{
		Major: 11,
		Minor: 0,
	}
	pp.CostModels = map[uint][]int64{2: costModels.PlutusV3}
	pp.MaxTxExUnits = lcommon.ExUnits{
		Memory: 17_500_000,
		Steps:  10_000_000_000,
	}
	return &pp
}

// utxoMemoBenchFundingUtxo is one funding output resolved from a
// preprod-conway-inputs-* fixture: the real chain bytes of the transaction
// that funded a benchmark target transaction's inputs.
type utxoMemoBenchFundingUtxo struct {
	txIdBytes []byte
	idx       int
	output    lcommon.TransactionOutput
}

// loadUtxoMemoBenchFundingUtxos decodes a preprod-conway-inputs-*.cbor
// fixture (a CBOR array of raw funding-transaction bytes) into resolvable
// UTxOs, matching the decode in TestEvaluateTxConwayPreprodFixtures.
func loadUtxoMemoBenchFundingUtxos(
	tb testing.TB,
	inputsFile string,
) []utxoMemoBenchFundingUtxo {
	tb.Helper()
	var inputTxBytes [][]byte
	_, err := cbor.Decode(
		readUtxoMemoBenchFixture(tb, inputsFile),
		&inputTxBytes,
	)
	require.NoError(tb, err)
	var out []utxoMemoBenchFundingUtxo
	for _, raw := range inputTxBytes {
		inputTx, err := conway.NewConwayTransactionFromCbor(raw)
		require.NoError(tb, err)
		txIdBytes := inputTx.Hash().Bytes()
		for idx, output := range inputTx.Outputs() {
			out = append(out, utxoMemoBenchFundingUtxo{
				txIdBytes: txIdBytes,
				idx:       idx,
				output:    output,
			})
		}
	}
	return out
}

// utxoMemoPreprodFixture wires the Hydra Head V2 increment fixture
// (preprod-conway-block-132228934.cbor, tx
// d81392f4def652323c5648067ffbe6d45812e415a53d867336aa5026db9ea2eb) into a
// real, seeded, in-memory dingo LedgerState/database for
// (*LedgerState).ValidateTx and UTxO input resolution through the
// production database.UtxoByRef path.
//
// Only this one transaction's 3 inputs are funded by the fixture (verified
// by decoding preprod-conway-inputs-132228934.cbor): the block's other two
// transactions have no funding data available.
type utxoMemoPreprodFixture struct {
	block     gledger.Block
	blockSlot uint64
	tx        lcommon.Transaction
	db        *database.Database
	dingoLS   *LedgerState
}

func loadUtxoMemoPreprodFixture(tb testing.TB) *utxoMemoPreprodFixture {
	tb.Helper()
	const (
		blockFile  = "preprod-conway-block-132228934.cbor"
		inputsFile = "preprod-conway-inputs-132228934.cbor"
		txId       = "d81392f4def652323c5648067ffbe6d45812e415a53d867336aa5026db9ea2eb"
	)
	blockRaw := readUtxoMemoBenchFixture(tb, blockFile)
	block, err := gledger.NewBlockFromCbor(gledger.BlockTypeConway, blockRaw)
	require.NoError(tb, err)
	var tx lcommon.Transaction
	for _, candidate := range block.Transactions() {
		if candidate.Hash().String() == txId {
			tx = candidate
		}
	}
	require.NotNil(tb, tx, "fixture block must contain %s", txId)

	pp := utxoMemoBenchProtocolParams(tb)
	funding := loadUtxoMemoBenchFundingUtxos(tb, inputsFile)

	db, err := dbtest.NewDatabase(tb, &database.Config{DataDir: ""})
	require.NoError(tb, err)
	require.NoError(tb, db.Transaction(true).Do(func(txn *database.Txn) error {
		for _, fu := range funding {
			if err := db.CreateUtxo(txn, &models.Utxo{
				TxId:      fu.txIdBytes,
				OutputIdx: uint32(fu.idx), // #nosec G115 -- small fixture index
				AddedSlot: 0,
			}); err != nil {
				return err
			}
			encoded, err := cbor.Encode(fu.output)
			if err != nil {
				return err
			}
			if err := db.Blob().SetUtxo(
				txn.Blob(),
				fu.txIdBytes,
				uint32(fu.idx), // #nosec G115 -- small fixture index
				encoded,
			); err != nil {
				return err
			}
		}
		return nil
	}))

	era := eras.ConwayEraDesc
	nodeConfig := newTestShelleyGenesisCfg(tb)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	// The production (*LedgerState).SlotToTime path (ledger/slot.go) has no
	// Byron-era segment here: it extrapolates the single flat-rate epoch
	// below (1000ms/slot from slot 0) using only ShelleyGenesis().SystemStart.
	// Real Preprod mixes 86400 Byron slots at 20s with 1s Shelley+ slots
	// (utxoMemoBenchSlotToTime above), so SystemStart is back-dated by the
	// Byron/Shelley slot-length difference so that a flat 1s/slot walk from
	// slot 0 lands on the same wall-clock instant at the fixture's real slot
	// that the real mixed schedule does.
	nodeConfig.ShelleyGenesis().SystemStart = time.Unix(
		utxoMemoBenchSystemStart+
			utxoMemoBenchByronSlots*utxoMemoBenchByronSlotSecs-
			utxoMemoBenchByronSlots,
		0,
	).UTC()
	// A single wide epoch covering the fixture's real slot: script-context
	// construction looks up the current epoch via the hardfork summary built
	// from activeEras+epochCache, and an empty cache fails closed.
	benchEpoch := models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		EraId:         era.Id,
		SlotLength:    1000,
		LengthInSlots: 200_000_000,
	}
	dingoLS := &LedgerState{
		db:             db,
		activeEras:     []eras.EraDesc{era},
		currentEra:     era,
		currentPParams: pp,
		currentEpoch:   benchEpoch,
		epochCache:     []models.Epoch{benchEpoch},
		config: LedgerStateConfig{
			Logger:            testLogger(),
			CardanoNodeConfig: nodeConfig,
		},
	}
	dingoLS.metrics.init(prometheus.NewRegistry())
	dingoLS.publishSnapshotsLocked()

	return &utxoMemoPreprodFixture{
		block:     block,
		blockSlot: block.SlotNumber(),
		tx:        tx,
		db:        db,
		dingoLS:   dingoLS,
	}
}

// BenchmarkLedgerStateValidateTxUtxoMemo measures the DB-backed
// (*LedgerState).ValidateTx path and the LedgerView.UtxoById resolution it
// repeats per input, before/after blinklabs-io/dingo#4226's per-view memo.
func BenchmarkLedgerStateValidateTxUtxoMemo(b *testing.B) {
	fx := loadUtxoMemoPreprodFixture(b)

	b.Run("UtxoInputResolution", func(b *testing.B) {
		// Real dingo database.UtxoByRef path (LedgerView.UtxoById ->
		// ls.db.UtxoByRef -> blob fetch + CBOR decode), one open read
		// transaction and one LedgerView reused across iterations. After
		// the first iteration, every further call hits the per-view memo
		// added by #4226 instead of the database.
		b.ReportAllocs()
		txn := fx.db.Transaction(false)
		defer txn.Release()
		view := fx.dingoLS.NewView(txn)
		input := fx.tx.Inputs()[0]
		if _, err := view.UtxoById(input); err != nil {
			b.Fatal(err)
		}
		for b.Loop() {
			if _, err := view.UtxoById(input); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("FullLedgerStateValidateTx", func(b *testing.B) {
		// (*LedgerState).ValidateTx against a real, seeded, in-memory dingo
		// database: a fresh LedgerView (and so a fresh memo) per call, the
		// same as production's per-transaction validation.
		b.ReportAllocs()
		if err := fx.dingoLS.ValidateTx(fx.tx); err != nil {
			b.Fatal(err)
		}
		for b.Loop() {
			if err := fx.dingoLS.ValidateTx(fx.tx); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// TestUtxoMemoBenchmarkNegativeControls proves both benchmarks above time the
// full check rather than an early-exit/error path.
func TestUtxoMemoBenchmarkNegativeControls(t *testing.T) {
	t.Parallel()
	fx := loadUtxoMemoPreprodFixture(t)

	t.Run("UtxoInputResolution_MissingInput", func(t *testing.T) {
		t.Parallel()
		txn := fx.db.Transaction(false)
		defer txn.Release()
		view := fx.dingoLS.NewView(txn)
		missing := shelley.NewShelleyTransactionInput(
			"0000000000000000000000000000000000000000000000000000000000000000",
			0,
		)
		_, err := view.UtxoById(missing)
		require.Error(t, err)
	})

	t.Run("FullLedgerStateValidateTx_TamperedFee", func(t *testing.T) {
		t.Parallel()
		tamperedPP := *fx.dingoLS.currentPParams.(*conway.ConwayProtocolParameters)
		tamperedPP.MinFeeA = 1_000_000_000
		tamperedLS := &LedgerState{
			db:             fx.db,
			activeEras:     []eras.EraDesc{eras.ConwayEraDesc},
			currentEra:     eras.ConwayEraDesc,
			currentPParams: &tamperedPP,
			currentEpoch:   fx.dingoLS.currentEpoch,
			epochCache:     fx.dingoLS.epochCache,
			config:         fx.dingoLS.config,
		}
		tamperedLS.metrics.init(prometheus.NewRegistry())
		tamperedLS.publishSnapshotsLocked()

		require.Error(t, tamperedLS.ValidateTx(fx.tx))
	})
}
