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

package eras

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// Preprod protocol parameters for both fixture blocks. Epochs 309 and 310
// carry an identical PlutusV3 cost model, so one parameter file serves both.
const (
	preprodCostModelsFile = "preprod-costmodels-plutusv3-pv11.json"
	preprodPlutusV3Params = 350
	preprodProtoMajor     = 11
	preprodProtoMinor     = 0
	preprodMaxTxExMem     = 17_500_000
	preprodMaxTxExSteps   = 10_000_000_000

	// Preprod Shelley genesis: systemStart 1654041600, a Byron prefix of four
	// epochs of 21600 20-second slots, 1-second slots afterwards.
	preprodSystemStart   = 1_654_041_600
	preprodByronSlots    = 86_400
	preprodByronSlotSecs = 20
)

// preprodPlutusFixture is a producer-accepted Preprod block and the
// transactions that funded the inputs and reference inputs of one Plutus
// transaction inside it, both taken from the chain over NtN blockfetch.
type preprodPlutusFixture struct {
	name       string
	blockFile  string
	inputsFile string
	txId       string
}

// Both transactions declare invalidHereafter and no invalidBefore, so their
// script context validity interval has a finite upper bound and no lower
// bound. cardano-ledger's Conway translation encodes that upper bound
// EXCLUSIVE; encoding it CLOSED changes what every validator reading
// txInfoValidRange sees.
var preprodPlutusFixtures = []preprodPlutusFixture{
	{
		// Hydra Head V2 increment. The affected spending validator still
		// succeeds under a CLOSED upper bound but takes a longer path: two
		// extra equalsInteger calls, two extra ifThenElse calls and 24 extra
		// CEK machine steps, for 640764 CPU and 2404 memory over the declared
		// budget. The transaction's other redeemer does not read the validity
		// range and is unaffected, so it holds the era gating to the one
		// value that changed.
		name:       "hydra_head_v2_increment_slot_132228934",
		blockFile:  "preprod-conway-block-132228934.cbor",
		inputsFile: "preprod-conway-inputs-132228934.cbor",
		txId:       "d81392f4def652323c5648067ffbe6d45812e415a53d867336aa5026db9ea2eb",
	},
	{
		// Midgard hub oracle mint. Under a CLOSED upper bound the minting
		// validator does not merely cost more, it calls Plutus `error`, so the
		// producer-accepted block is rejected outright rather than for a
		// budget overage.
		name:       "midgard_hub_oracle_mint_slot_132657325",
		blockFile:  "preprod-conway-block-132657325.cbor",
		inputsFile: "preprod-conway-inputs-132657325.cbor",
		txId:       "5728f55704a202ce7c627d59eac086ee7a756fc61e8951855accbaf137f677e7",
	},
}

// preprodLedgerState gives the mock ledger state preprod's real slot/time
// conversion. The script context carries the transaction's validity range as
// POSIX milliseconds, so a placeholder conversion would not reproduce the
// bytes the block producer's evaluator saw.
type preprodLedgerState struct {
	*mockLedgerState
}

func (preprodLedgerState) SlotToTime(slot uint64) (time.Time, error) {
	if slot < preprodByronSlots {
		return time.Unix(
			preprodSystemStart+int64(slot)*preprodByronSlotSecs,
			0,
		).UTC(), nil
	}
	byronEnd := int64(preprodSystemStart) +
		int64(preprodByronSlots)*preprodByronSlotSecs
	return time.Unix(byronEnd+int64(slot-preprodByronSlots), 0).UTC(), nil
}

func (preprodLedgerState) TimeToSlot(t time.Time) (uint64, error) {
	byronEnd := int64(preprodSystemStart) +
		int64(preprodByronSlots)*preprodByronSlotSecs
	if t.Unix() < byronEnd {
		return uint64(
			(t.Unix() - preprodSystemStart) / preprodByronSlotSecs,
		), nil
	}
	return preprodByronSlots + uint64(t.Unix()-byronEnd), nil
}

func readErasFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)
	return raw
}

func preprodFixtureProtocolParams(t *testing.T) *conway.ConwayProtocolParameters {
	t.Helper()
	var costModels struct {
		PlutusV3 []int64 `json:"PlutusV3"`
	}
	require.NoError(t, json.Unmarshal(
		readErasFixture(t, preprodCostModelsFile),
		&costModels,
	))
	require.Len(t, costModels.PlutusV3, preprodPlutusV3Params)
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: preprodProtoMajor,
			Minor: preprodProtoMinor,
		},
		CostModels: map[uint][]int64{2: costModels.PlutusV3},
		MaxTxExUnits: lcommon.ExUnits{
			Memory: preprodMaxTxExMem,
			Steps:  preprodMaxTxExSteps,
		},
	}
}

// TestEvaluateTxConwayPreprodFixtures pins the execution units of real
// producer-accepted Preprod transactions against the budgets their producers
// declared. cardano-node computed those budgets with the reference evaluator,
// so each is an external oracle: equality in both directions catches an
// overcharge, which rejects a block the network accepted, and an undercharge,
// which accepts a transaction the network rejects.
func TestEvaluateTxConwayPreprodFixtures(t *testing.T) {
	pp := preprodFixtureProtocolParams(t)
	for _, fixture := range preprodPlutusFixtures {
		t.Run(fixture.name, func(t *testing.T) {
			blk, err := gledger.NewBlockFromCbor(
				gledger.BlockTypeConway,
				readErasFixture(t, fixture.blockFile),
			)
			require.NoError(t, err)

			var tx lcommon.Transaction
			for _, candidate := range blk.Transactions() {
				if candidate.Hash().String() == fixture.txId {
					tx = candidate
				}
			}
			require.NotNil(
				t,
				tx,
				"fixture block must contain %s",
				fixture.txId,
			)

			var inputTxBytes [][]byte
			_, err = cbor.Decode(
				readErasFixture(t, fixture.inputsFile),
				&inputTxBytes,
			)
			require.NoError(t, err)

			ls := preprodLedgerState{mockLedgerState: newMockLedgerState()}
			ls.networkId = uint(lcommon.AddressNetworkTestnet)
			for _, raw := range inputTxBytes {
				inputTx, err := conway.NewConwayTransactionFromCbor(raw)
				require.NoError(t, err)
				for idx, output := range inputTx.Outputs() {
					ls.addUtxo(
						shelley.NewShelleyTransactionInput(
							inputTx.Hash().String(),
							idx,
						),
						output,
					)
				}
			}

			_, _, redeemerExUnits, err := EvaluateTxConway(tx, ls, pp)
			require.NoError(t, err)

			declared := map[lcommon.RedeemerKey]lcommon.ExUnits{}
			for key, value := range tx.Witnesses().Redeemers().Iter() {
				declared[key] = value.ExUnits
			}
			require.NotEmpty(t, declared)
			require.Equal(
				t,
				declared,
				redeemerExUnits,
				"evaluated execution units must equal the "+
					"producer-declared budget exactly",
			)
		})
	}
}
