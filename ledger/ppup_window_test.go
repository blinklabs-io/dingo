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
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// newPPUPWindowLedgerState builds a ledger whose Shelley genesis carries the
// given security parameter and active-slot coefficient and whose epoch cache
// holds epochs.
func newPPUPWindowLedgerState(
	t *testing.T,
	securityParam int,
	activeSlotsCoeff *big.Rat,
	epochs []models.Epoch,
) *LedgerState {
	t.Helper()
	cfg := newGenesisDelegateShelleyGenesisCfg(
		t,
		strings.Repeat("aa", lcommon.Blake2b224Size),
		strings.Repeat("bb", lcommon.Blake2b256Size),
	)
	genesis := cfg.ShelleyGenesis()
	genesis.SecurityParam = securityParam
	genesis.ActiveSlotsCoeff = cbor.Rat{Rat: activeSlotsCoeff}
	ls := &LedgerState{}
	ls.config.CardanoNodeConfig = cfg
	ls.consensus.Store(&consensusSnapshot{epochCache: epochs})
	return ls
}

// The reference slot of no return is
// epochInfoFirst (succ e) *- Duration (2 * stabilityWindow), with
// stabilityWindow = computeStabilityWindow k f = ceiling (3k/f)
// (cardano-ledger Cardano.Ledger.Slot.getTheSlotOfNoReturn and
// Cardano.Ledger.Shelley.StabilityWindow). When 3k/f is not an integer,
// 2 * ceiling (3k/f) is larger than floor (6k/f).
func TestProtocolParameterUpdateWindowMatchesReferenceSlotOfNoReturn(
	t *testing.T,
) {
	t.Parallel()
	for _, tc := range []struct {
		name             string
		securityParam    int
		activeSlotsCoeff *big.Rat
		epoch            models.Epoch
		noReturn         uint64
	}{
		{
			// Mainnet and preprod: 2 * 3 * 2160 / 0.05 = 259200.
			name:             "mainnet first Shelley epoch",
			securityParam:    2160,
			activeSlotsCoeff: big.NewRat(1, 20),
			epoch:            models.Epoch{EpochId: 208, StartSlot: 4_492_800, LengthInSlots: 432_000},
			noReturn:         4_492_800 + 432_000 - 259_200,
		},
		{
			// Preview: 2 * 3 * 432 / 0.05 = 51840.
			name:             "preview",
			securityParam:    432,
			activeSlotsCoeff: big.NewRat(1, 20),
			epoch:            models.Epoch{EpochId: 700, StartSlot: 60_480_000, LengthInSlots: 86_400},
			noReturn:         60_480_000 + 86_400 - 51_840,
		},
		{
			// 3k/f = 30/7: ceiling 5, so 2 * 5 = 10 where floor (60/7) = 8.
			name:             "fractional window below one half",
			securityParam:    1,
			activeSlotsCoeff: big.NewRat(7, 10),
			epoch:            models.Epoch{EpochId: 4, StartSlot: 500, LengthInSlots: 100},
			noReturn:         600 - 10,
		},
		{
			// 3k/f = 300/7: ceiling 43, so 2 * 43 = 86 where floor (600/7) = 85.
			name:             "fractional window above one half",
			securityParam:    1,
			activeSlotsCoeff: big.NewRat(7, 100),
			epoch:            models.Epoch{EpochId: 4, StartSlot: 500, LengthInSlots: 200},
			noReturn:         700 - 86,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ls := newPPUPWindowLedgerState(
				t,
				tc.securityParam,
				tc.activeSlotsCoeff,
				[]models.Epoch{tc.epoch},
			)
			view := &LedgerView{ls: ls}
			for _, slot := range []uint64{
				tc.epoch.StartSlot,
				tc.noReturn - 1,
				tc.noReturn,
				tc.epoch.StartSlot + uint64(tc.epoch.LengthInSlots) - 1,
			} {
				epoch, noReturn, err := view.ProtocolParameterUpdateWindow(slot)
				require.NoError(t, err, "slot %d", slot)
				require.Equal(t, tc.epoch.EpochId, epoch, "slot %d", slot)
				require.Equal(t, tc.noReturn, noReturn, "slot %d", slot)
			}
		})
	}
}

type ppupWindowTestTx struct {
	lcommon.Transaction
	epoch   uint64
	updates map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate
	witness lcommon.TransactionWitnessSet
}

func (tx ppupWindowTestTx) ProtocolParameterUpdates() (
	uint64,
	map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
) {
	return tx.epoch, tx.updates
}

func (tx ppupWindowTestTx) Witnesses() lcommon.TransactionWitnessSet {
	return tx.witness
}

type ppupWindowTestWitnessSet struct {
	lcommon.TransactionWitnessSet
	vkeys []lcommon.VkeyWitness
}

func (w ppupWindowTestWitnessSet) Vkey() []lcommon.VkeyWitness {
	return w.vkeys
}

// ppupWindowRuleState takes the voting window from the real LedgerView and
// stubs only the genesis-delegate lookup, which reads the metadata store.
type ppupWindowRuleState struct {
	*LedgerView
	genesisKey lcommon.Blake2b224
	delegate   lcommon.Blake2b224
}

func (s ppupWindowRuleState) GenesisDelegateForGenesisKey(
	genesisKey lcommon.Blake2b224,
	_ uint64,
) (lcommon.Blake2b224, bool, error) {
	if genesisKey != s.genesisKey {
		return lcommon.Blake2b224{}, false, nil
	}
	return s.delegate, true, nil
}

func ppupWindowValidator(
	t *testing.T,
	descriptors []lcommon.UtxoValidationRuleDescriptor,
) lcommon.UtxoValidationRuleFunc {
	t.Helper()
	for _, descriptor := range descriptors {
		if descriptor.Id == lcommon.UtxoValidationRuleProtocolParameterUpdates {
			return descriptor.Validator
		}
	}
	t.Fatal("classic protocol parameter update rule is not registered")
	return nil
}

// TestClassicPPUPWindowBoundariesThroughEraRules drives each Shelley-family
// era's registered PPUP rule with the LedgerView window on both sides of
// every boundary of the first mainnet Shelley epoch.
func TestClassicPPUPWindowBoundariesThroughEraRules(t *testing.T) {
	t.Parallel()
	const (
		epoch     = uint64(208)
		start     = uint64(4_492_800)
		length    = uint64(432_000)
		noReturn  = start + length - 259_200
		nextStart = start + length
	)
	ls := newPPUPWindowLedgerState(
		t,
		2160,
		big.NewRat(1, 20),
		[]models.Epoch{
			{EpochId: epoch, StartSlot: start, LengthInSlots: uint(length)},
			{
				EpochId:       epoch + 1,
				StartSlot:     nextStart,
				LengthInSlots: uint(length),
			},
		},
	)
	vkey := bytes.Repeat([]byte{7}, 32)
	state := ppupWindowRuleState{
		LedgerView: &LedgerView{ls: ls},
		genesisKey: lcommon.Blake2b224Hash(bytes.Repeat([]byte{6}, 32)),
		delegate:   lcommon.Blake2b224Hash(vkey),
	}
	witness := ppupWindowTestWitnessSet{
		vkeys: []lcommon.VkeyWitness{{Vkey: vkey}},
	}
	eraCases := []struct {
		name        string
		descriptors func() []lcommon.UtxoValidationRuleDescriptor
		update      lcommon.ProtocolParameterUpdate
		pparams     lcommon.ProtocolParameters
	}{
		{
			"Shelley",
			shelley.UtxoValidationRuleDescriptors,
			shelley.ShelleyProtocolParameterUpdate{},
			&shelley.ShelleyProtocolParameters{},
		},
		{
			"Allegra",
			allegra.UtxoValidationRuleDescriptors,
			allegra.AllegraProtocolParameterUpdate{},
			&allegra.AllegraProtocolParameters{},
		},
		{
			"Mary",
			mary.UtxoValidationRuleDescriptors,
			mary.MaryProtocolParameterUpdate{},
			&mary.MaryProtocolParameters{},
		},
		{
			"Alonzo",
			alonzo.UtxoValidationRuleDescriptors,
			alonzo.AlonzoProtocolParameterUpdate{},
			&alonzo.AlonzoProtocolParameters{},
		},
		{
			"Babbage",
			babbage.UtxoValidationRuleDescriptors,
			babbage.BabbageProtocolParameterUpdate{},
			&babbage.BabbageProtocolParameters{},
		},
	}
	slots := []struct {
		name         string
		slot         uint64
		currentEpoch uint64
		forNext      bool
	}{
		{"first slot", start, epoch, false},
		{"last slot before no return", noReturn - 1, epoch, false},
		{"slot of no return", noReturn, epoch, true},
		{"last slot of epoch", nextStart - 1, epoch, true},
		{"first slot of next epoch", nextStart, epoch + 1, false},
	}
	for _, era := range eraCases {
		validate := ppupWindowValidator(t, era.descriptors())
		for _, sc := range slots {
			expected := sc.currentEpoch
			if sc.forNext {
				expected++
			}
			newTx := func(target uint64) ppupWindowTestTx {
				return ppupWindowTestTx{
					epoch: target,
					updates: map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate{
						state.genesisKey: era.update,
					},
					witness: witness,
				}
			}
			t.Run(era.name+"/"+sc.name, func(t *testing.T) {
				t.Parallel()
				require.NoError(
					t,
					validate(newTx(expected), sc.slot, state, era.pparams),
				)
				wrong := expected - 1
				if !sc.forNext {
					wrong = expected + 1
				}
				var epochErr lcommon.ProtocolParameterUpdateEpochError
				require.ErrorAs(
					t,
					validate(newTx(wrong), sc.slot, state, era.pparams),
					&epochErr,
				)
				require.Equal(t, sc.currentEpoch, epochErr.Current)
				require.Equal(t, expected, epochErr.Expected)
				require.Equal(t, sc.forNext, epochErr.ForNextEpoch)
			})
		}
	}
}
