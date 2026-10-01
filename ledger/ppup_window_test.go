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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

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
