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
	"context"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/common/script"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	omockfixtures "github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mainnet block 6400998 (slot 43287653, epoch 297, Alonzo) carries Plutus
// transaction 3cc151aa...c898 with invalidHereafter 44287078 and no lower
// bound. Its predecessor, block 6400997, is at slot 43287619. Measured from
// that predecessor the Alonzo safe zone (3k/f = 129600) ends at 43417220,
// which snaps up to the epoch 299 boundary at 43804800, so the transaction's
// upper bound is 482278 slots past the forecast horizon. cardano-node applied
// the block anyway.
const (
	mainnetForecastParentSlot = 43_287_619
	mainnetForecastBlockSlot  = 43_287_653
	mainnetForecastUpperBound = 44_287_078
	mainnetForecastHorizon    = 43_804_800 // epoch 299
	mainnetForecastBlockNo    = 6_400_998

	mainnetByronEpochSize   = 21_600
	mainnetShelleyEpochSize = 432_000
	mainnetShelleyStartSlot = 4_492_800 // epoch 208

	// mainnetForecastUpperBoundUnix is the POSIX second of slot 44287078 on
	// mainnet: SystemStart 1506203091, plus 4492800 Byron slots of 20s, plus
	// one second per Shelley-based slot after that. The block's own slot gives
	// 1634853944 by the same arithmetic, which is the block time Koios reports.
	mainnetForecastUpperBoundUnix = 1_635_853_369
	mainnetForecastBlockUnix      = 1_634_853_944
)

// mainnetForecastLedgerState is a mainnet-shaped ledger at epoch 297 whose
// published tip is block 6400998's predecessor. The epoch cache carries every
// era boundary from Byron onwards so the summary's relative times are the
// real ones. ledgerEra stands in for Alonzo from epoch 290, so the same
// history can be asked what a later era's rules make of the same bound.
func mainnetForecastLedgerState(
	t testing.TB,
	ledgerEra eras.EraDesc,
) *LedgerState {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, loadByronGenesisForTest(t, cfg, strings.NewReader(`{
		"blockVersionData": { "slotDuration": "20000" },
		"protocolConsts": { "k": 2160 }
	}`)))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.05,
		"networkId": "Mainnet",
		"networkMagic": 764824073,
		"securityParam": 2160,
		"slotLength": 1,
		"epochLength": 432000,
		"systemStart": "2017-09-23T21:44:51Z"
	}`)))

	// Mainnet hard-fork epochs: Shelley 208, Allegra 236, Mary 251, Alonzo 290.
	eraFor := func(epoch uint64) uint {
		switch {
		case epoch >= 290:
			return ledgerEra.Id
		case epoch >= 251:
			return eras.MaryEraDesc.Id
		case epoch >= 236:
			return eras.AllegraEraDesc.Id
		case epoch >= 208:
			return eras.ShelleyEraDesc.Id
		default:
			return eras.ByronEraDesc.Id
		}
	}
	epochs := make([]models.Epoch, 0, 298)
	for epoch := range uint64(298) {
		e := models.Epoch{EpochId: epoch, EraId: eraFor(epoch)}
		if epoch < 208 {
			e.StartSlot = epoch * mainnetByronEpochSize
			e.SlotLength = 20_000
			e.LengthInSlots = mainnetByronEpochSize
		} else {
			e.StartSlot = mainnetShelleyStartSlot +
				(epoch-208)*mainnetShelleyEpochSize
			e.SlotLength = 1_000
			e.LengthInSlots = mainnetShelleyEpochSize
		}
		epochs = append(epochs, e)
	}
	ls := &LedgerState{
		epochCache: epochs,
		currentEra: ledgerEra,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				mainnetForecastParentSlot,
				[]byte("block-6400997"),
			),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            testLogger(),
		},
	}
	ls.publishSnapshotsLocked()
	return ls
}

func mainnetForecastTx(t testing.TB) *alonzo.AlonzoTransaction {
	t.Helper()
	raw, err := os.ReadFile(
		filepath.Join("testdata", "mainnet-alonzo-tx-3cc151aa.cbor"),
	)
	require.NoError(t, err)
	tx, err := alonzo.NewAlonzoTransactionFromCbor(raw)
	require.NoError(t, err)
	require.Equal(t,
		"3cc151aa187c9cdba5539cb16773148654ff30567182955b00babb229336c898",
		tx.Hash().String())
	upper, present := lcommon.TransactionValidityIntervalUpperBound(tx)
	require.True(t, present)
	require.Equal(t, uint64(mainnetForecastUpperBound), upper)
	return tx
}

// TestLedgerProcessBlockExtendsPlutusForecastFromBlockSlot replays the
// validation view block application hands to the era rules for mainnet block
// 6400998, and runs the real transaction through the OutsideForecast rule with
// it.
//
// Every case probes that view, not the era's own validator: the callback
// ignores the block's transaction and applies the mainnet Alonzo transaction
// to the shared UtxoValidateOutsideForecast rule. The Babbage and Conway cases
// pin that a ledger in those eras hands the rules a bounded view; the era
// validators then see the same SlotToTime result.
//
// cardano-ledger's Alonzo LEDGERS rule builds every transaction's Plutus
// script context through unsafeLinearExtendEpochInfo slot ei
// (Cardano.Ledger.Alonzo.Rules.Ledgers, mkStAnnTx), and the UTXO rule's
// OutsideForecast check extends the same way
// (Cardano.Ledger.Alonzo.Rules.Utxo.validateOutsideForecast): past the block's
// own slot, time grows linearly at that slot's length. A bound past the HFC
// horizon therefore translates in Alonzo. Babbage's LEDGERS rule, which Conway
// reuses, passes the bounded epoch info to the script context instead, so the
// same bound fails translation (TimeTranslationPastHorizon) and the
// transaction is rejected there.
func TestLedgerProcessBlockExtendsPlutusForecastFromBlockSlot(t *testing.T) {
	t.Parallel()

	tx := mainnetForecastTx(t)
	tests := []struct {
		name      string
		ledgerEra eras.EraDesc
		block     func(t *testing.T) (lcommon.Block, lcommon.ProtocolParameters)
		extended  bool
	}{
		{
			name:      "alonzo",
			ledgerEra: eras.AlonzoEraDesc,
			extended:  true,
			block: func(t *testing.T) (lcommon.Block, lcommon.ProtocolParameters) {
				blocks, err := omockfixtures.GenerateAlonzoChain(
					mainnetForecastBlockNo,
					lcommon.Blake2b256{},
					mainnetForecastBlockSlot,
					1,
					1,
				)
				require.NoError(t, err)
				block, ok := blocks[0].(*alonzo.AlonzoBlock)
				require.True(t, ok)
				block.TransactionBodies = []alonzo.AlonzoTransactionBody{
					tx.Body,
				}
				block.TransactionWitnessSets = []alonzo.AlonzoTransactionWitnessSet{
					tx.WitnessSet,
				}
				return block, &alonzo.AlonzoProtocolParameters{
					ProtocolMajor:      6,
					MaxBlockBodySize:   100_000,
					MaxBlockHeaderSize: 100_000,
					// Mainnet's epoch 297 values.
					MaxTxExUnits: lcommon.ExUnits{
						Memory: 10_000_000,
						Steps:  10_000_000_000,
					},
					MaxBlockExUnits: lcommon.ExUnits{
						Memory: 50_000_000,
						Steps:  40_000_000_000,
					},
				}
			},
		},
		{
			name:      "babbage",
			ledgerEra: eras.BabbageEraDesc,
			block: func(t *testing.T) (lcommon.Block, lcommon.ProtocolParameters) {
				blocks, err := omockfixtures.GenerateBabbageChain(
					mainnetForecastBlockNo,
					lcommon.Blake2b256{},
					mainnetForecastBlockSlot,
					1,
					1,
				)
				require.NoError(t, err)
				block, ok := blocks[0].(*babbage.BabbageBlock)
				require.True(t, ok)
				block.TransactionBodies = []babbage.BabbageTransactionBody{{}}
				block.TransactionWitnessSets = []babbage.BabbageTransactionWitnessSet{
					{},
				}
				return block, &babbage.BabbageProtocolParameters{
					ProtocolMajor:      7,
					MaxBlockBodySize:   100_000,
					MaxBlockHeaderSize: 100_000,
				}
			},
		},
		{
			name:      "conway",
			ledgerEra: eras.ConwayEraDesc,
			block: func(t *testing.T) (lcommon.Block, lcommon.ProtocolParameters) {
				blocks, err := omockfixtures.GenerateConwayChain(
					mainnetForecastBlockNo,
					lcommon.Blake2b256{},
					mainnetForecastBlockSlot,
					1,
					1,
				)
				require.NoError(t, err)
				block, ok := blocks[0].(*conway.ConwayBlock)
				require.True(t, ok)
				block.TransactionBodies = []conway.ConwayTransactionBody{{}}
				block.TransactionWitnessSets = []conway.ConwayTransactionWitnessSet{
					{},
				}
				return block, &conway.ConwayProtocolParameters{
					ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
						Major: 9,
					},
					MaxBlockBodySize:   100_000,
					MaxBlockHeaderSize: 100_000,
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			db := newTestDB(t)
			ls := mainnetForecastLedgerState(t, test.ledgerEra)
			ls.db = db

			// Premise: the HFC summary anchored at the real predecessor ends
			// before the transaction's bound. If it stops doing so, this test
			// no longer reproduces the mainnet rejection.
			anchored, err := ls.hardForkSummaryAnchoredAt(
				mainnetForecastParentSlot,
			)
			require.NoError(t, err)
			current := anchored.CurrentEra()
			require.NotNil(t, current)
			require.NotNil(t, current.End)
			require.Equal(t, uint64(mainnetForecastHorizon), current.End.Slot)
			_, err = ls.SlotToTimeWithHorizonFrom(
				mainnetForecastParentSlot,
				mainnetForecastUpperBound,
			)
			require.ErrorIs(t, err, hardfork.ErrPastHorizon)

			var (
				probed       bool
				probeSlot    uint64
				upperTime    time.Time
				upperErr     error
				blockTime    time.Time
				blockTimeErr error
				ruleErr      error
			)
			testEra := test.ledgerEra
			testEra.ValidateTxFunc = func(
				_ lcommon.Transaction,
				slot uint64,
				view lcommon.LedgerState,
				pp lcommon.ProtocolParameters,
			) error {
				probed = true
				probeSlot = slot
				upperTime, upperErr = view.SlotToTime(mainnetForecastUpperBound)
				blockTime, blockTimeErr = view.SlotToTime(
					mainnetForecastBlockSlot,
				)
				ruleErr = lcommon.UtxoValidateOutsideForecast(
					tx, slot, view, pp,
				)
				return errHorizonProbeDone
			}
			ls.activeEras = []eras.EraDesc{testEra}

			block, pparams := test.block(t)
			processErr := db.Transaction(context.Background(), true).
				Do(func(txn *database.Txn) error {
					_, err := ls.ledgerProcessBlock(context.Background(),
						txn,
						ocommon.NewPoint(
							mainnetForecastBlockSlot,
							block.Hash().Bytes(),
						),
						block,
						true,
						false,
						false,
						nil,
						envelopeParent{
							slot:        mainnetForecastParentSlot,
							blockNumber: mainnetForecastBlockNo - 1,
						},
						&database.BlockIngestionResult{},
						testEra,
						pparams,
						nil,
						297,
						0,
						false,
					)
					return err
				})
			require.ErrorIs(t, processErr, errHorizonProbeDone)
			require.True(t, probed)
			require.Equal(t, uint64(mainnetForecastBlockSlot), probeSlot)

			require.NoError(t, blockTimeErr)
			assert.Equal(t, int64(mainnetForecastBlockUnix), blockTime.Unix())

			if !test.extended {
				require.ErrorIs(t, upperErr, hardfork.ErrPastHorizon,
					"after Alonzo the script context translates through "+
						"the bounded epoch info")
				var outsideForecast *lcommon.OutsideForecastError
				require.ErrorAs(t, ruleErr, &outsideForecast)
				return
			}
			require.NoError(t, upperErr,
				"an Alonzo Plutus validity bound past the HFC horizon must "+
					"translate linearly from the block's slot, as "+
					"unsafeLinearExtendEpochInfo does")
			assert.Equal(t,
				int64(mainnetForecastUpperBoundUnix),
				upperTime.Unix(),
			)
			assert.Equal(t,
				int64(mainnetForecastUpperBoundUnix)*1000,
				upperTime.UnixMilli(),
				"the script context's POSIX upper bound")
			assert.NoError(t, ruleErr,
				"OutsideForecast must accept the canonical transaction")
		})
	}
}

// TestSlotToTimeExtendedFromMirrorsLinearExtension pins
// SlotToTimeExtendedFrom to cardano-slotting's unsafeLinearExtendEpochInfo
// (Cardano.Slotting.EpochInfo.Extend): at or before the basis slot the
// underlying epoch info answers, after it time advances by the basis slot's
// length per slot, and a basis the underlying epoch info cannot translate is
// still an error.
func TestSlotToTimeExtendedFromMirrorsLinearExtension(t *testing.T) {
	t.Parallel()

	ls := mainnetForecastLedgerState(t, eras.AlonzoEraDesc)
	at := func(slot uint64) int64 {
		// Every slot from the basis on is one second, so this is the
		// reference's linear translation written out.
		return int64(mainnetForecastBlockUnix) +
			int64(slot) - int64(mainnetForecastBlockSlot)
	}
	tests := []struct {
		name     string
		basis    uint64
		slot     uint64
		wantUnix int64
		wantErr  error
	}{
		{
			name:     "before the basis",
			basis:    mainnetForecastBlockSlot,
			slot:     mainnetForecastParentSlot,
			wantUnix: at(mainnetForecastParentSlot),
		},
		{
			name:     "at the basis",
			basis:    mainnetForecastBlockSlot,
			slot:     mainnetForecastBlockSlot,
			wantUnix: at(mainnetForecastBlockSlot),
		},
		{
			name:     "last slot inside the horizon",
			basis:    mainnetForecastBlockSlot,
			slot:     mainnetForecastHorizon - 1,
			wantUnix: at(mainnetForecastHorizon - 1),
		},
		{
			name:     "first slot past the horizon",
			basis:    mainnetForecastBlockSlot,
			slot:     mainnetForecastHorizon,
			wantUnix: at(mainnetForecastHorizon),
		},
		{
			name:     "the mainnet transaction's upper bound",
			basis:    mainnetForecastBlockSlot,
			slot:     mainnetForecastUpperBound,
			wantUnix: mainnetForecastUpperBoundUnix,
		},
		{
			// Past time.Duration's 292-year range from SystemStart, which
			// the bounded summary cannot express at all.
			name:     "beyond a time.Duration from system start",
			basis:    mainnetForecastBlockSlot,
			slot:     1 << 52,
			wantUnix: at(1 << 52),
		},
		{
			name:    "POSIX milliseconds past int64",
			basis:   mainnetForecastBlockSlot,
			slot:    1 << 62,
			wantErr: hardfork.ErrDurationOverflow,
		},
		{
			name:    "basis past the horizon",
			basis:   mainnetForecastHorizon,
			slot:    mainnetForecastHorizon + 1,
			wantErr: hardfork.ErrPastHorizon,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := ls.SlotToTimeExtendedFrom(
				mainnetForecastParentSlot,
				test.basis,
				test.slot,
			)
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.wantUnix, got.Unix())
			assert.Zero(t, got.Nanosecond())
		})
	}

	// A view with no basis, such as standalone script evaluation, keeps the
	// bounded summary.
	_, err := (&LedgerView{
		ls:                ls,
		horizonAnchorSlot: mainnetForecastParentSlot,
	}).SlotToTime(mainnetForecastUpperBound)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon)
}

// TestAlonzoPlutusValidityRangeMilliseconds builds the Plutus V1 TxInfo an
// Alonzo script receives through an extended view and reads its validity
// interval upper bound back out of the encoded context.
//
// cardano-ledger's POSIXTime is an unbounded Integer of milliseconds, while
// the script context carries them as int64 (time.Time.UnixMilli). A bound
// whose milliseconds fit must arrive exact; one whose milliseconds do not fit
// must fail translation rather than reach the script wrapped around to an
// unrelated time.
func TestAlonzoPlutusValidityRangeMilliseconds(t *testing.T) {
	t.Parallel()

	ls := mainnetForecastLedgerState(t, eras.AlonzoEraDesc)
	view := (&LedgerView{
		ls:                ls,
		horizonAnchorSlot: mainnetForecastParentSlot,
	}).extendForecastFrom(eras.AlonzoEraDesc.Id, mainnetForecastBlockSlot)

	// The last slot whose start, in POSIX milliseconds, still fits in int64.
	lastSlot := uint64(math.MaxInt64/1000) -
		uint64(mainnetForecastBlockUnix) + mainnetForecastBlockSlot

	upperBound := func(t *testing.T, ttl uint64) (*big.Int, error) {
		t.Helper()
		tx := &alonzo.AlonzoTransaction{
			Body: alonzo.AlonzoTransactionBody{},
		}
		tx.Body.Ttl = ttl
		info, err := script.NewTxInfoV1FromTransaction(view, tx, nil, false, 6)
		if err != nil {
			return nil, err
		}
		rng, ok := info.ValidRange.ToPlutusData().(*data.Constr)
		require.True(t, ok)
		require.Len(t, rng.Fields, 2)
		upper, ok := rng.Fields[1].(*data.Constr)
		require.True(t, ok)
		finite, ok := upper.Fields[0].(*data.Constr)
		require.True(t, ok)
		require.Zero(t, finite.Tag.Cmp(big.NewInt(1)), "finite upper bound")
		ms, ok := finite.Fields[0].(*data.Integer)
		require.True(t, ok)
		return ms.Inner, nil
	}

	tests := []struct {
		name   string
		ttl    uint64
		wantMs *big.Int
	}{
		{
			name:   "the mainnet transaction's upper bound",
			ttl:    mainnetForecastUpperBound,
			wantMs: big.NewInt(int64(mainnetForecastUpperBoundUnix) * 1000),
		},
		{
			name:   "last slot whose milliseconds fit",
			ttl:    lastSlot,
			wantMs: big.NewInt(math.MaxInt64 / 1000 * 1000),
		},
		{
			name: "first slot whose milliseconds overflow",
			ttl:  lastSlot + 1,
		},
		{
			name: "far-future TTL",
			ttl:  1 << 62,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := upperBound(t, test.ttl)
			if test.wantMs == nil {
				require.ErrorIs(t, err, hardfork.ErrDurationOverflow,
					"a bound the script context cannot carry exactly "+
						"must not be translated")
				return
			}
			require.NoError(t, err)
			assert.Zero(t, test.wantMs.Cmp(got),
				"script POSIX upper bound: want %s, got %s",
				test.wantMs, got)
		})
	}
}
