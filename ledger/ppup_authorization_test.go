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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ledger

import (
	"bytes"
	"encoding/hex"
	"strings"
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

// The classic PPUP rule runs at validation, before any proposal row is
// written, so a proposal it refuses never reaches the vote store and cannot
// count toward quorum.
func TestClassicPPUPRuleRejectsUnauthorizedProposals(t *testing.T) {
	t.Parallel()
	const (
		epoch  = uint64(208)
		start  = uint64(4_492_800)
		length = uint64(432_000)
	)
	vkey := bytes.Repeat([]byte{7}, 32)
	genesisKey := lcommon.Blake2b224{}
	copy(genesisKey[:], bytes.Repeat([]byte{0x11}, lcommon.Blake2b224Size))
	// The real LedgerView resolves the delegate from Shelley genesis, whose
	// only genesis key is genesisKey.
	cfg := newGenesisDelegateShelleyGenesisCfg(
		t,
		hex.EncodeToString(lcommon.Blake2b224Hash(vkey).Bytes()),
		strings.Repeat("bb", lcommon.Blake2b256Size),
	)
	ls, _ := newEligibilityTestLedger(t, nil)
	ls.config.CardanoNodeConfig = cfg
	ls.epochCache = []models.Epoch{
		{EpochId: epoch, StartSlot: start, LengthInSlots: uint(length)},
	}
	ls.publishSnapshotsLocked()
	state := &LedgerView{ls: ls}
	signed := ppupWindowTestWitnessSet{
		vkeys: []lcommon.VkeyWitness{{Vkey: vkey}},
	}
	unsigned := ppupWindowTestWitnessSet{
		vkeys: []lcommon.VkeyWitness{{Vkey: bytes.Repeat([]byte{8}, 32)}},
	}
	fabricated := func(n int) []lcommon.Blake2b224 {
		keys := make([]lcommon.Blake2b224, n)
		for i := range keys {
			keys[i] = lcommon.Blake2b224Hash([]byte{0xfa, byte(i)})
		}
		return keys
	}
	eraCases := []struct {
		name        string
		descriptors func() []lcommon.UtxoValidationRuleDescriptor
		update      lcommon.ProtocolParameterUpdate
		pparams     lcommon.ProtocolParameters
	}{
		{
			"Shelley", shelley.UtxoValidationRuleDescriptors,
			shelley.ShelleyProtocolParameterUpdate{},
			&shelley.ShelleyProtocolParameters{},
		},
		{
			"Allegra", allegra.UtxoValidationRuleDescriptors,
			allegra.AllegraProtocolParameterUpdate{},
			&allegra.AllegraProtocolParameters{},
		},
		{
			"Mary", mary.UtxoValidationRuleDescriptors,
			mary.MaryProtocolParameterUpdate{},
			&mary.MaryProtocolParameters{},
		},
		{
			"Alonzo", alonzo.UtxoValidationRuleDescriptors,
			alonzo.AlonzoProtocolParameterUpdate{},
			&alonzo.AlonzoProtocolParameters{},
		},
		{
			"Babbage", babbage.UtxoValidationRuleDescriptors,
			babbage.BabbageProtocolParameterUpdate{},
			&babbage.BabbageProtocolParameters{},
		},
	}
	for _, era := range eraCases {
		validate := ppupWindowValidator(t, era.descriptors())
		newTx := func(
			keys []lcommon.Blake2b224,
			witness lcommon.TransactionWitnessSet,
		) ppupWindowTestTx {
			updates := make(
				map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
				len(keys),
			)
			for _, key := range keys {
				updates[key] = era.update
			}
			return ppupWindowTestTx{epoch: epoch, updates: updates, witness: witness}
		}
		t.Run(era.name+"/unknown genesis key", func(t *testing.T) {
			t.Parallel()
			keys := fabricated(1)
			var delegateErr lcommon.ProtocolParameterUpdateDelegateError
			require.ErrorAs(
				t,
				validate(newTx(keys, signed), start, state, era.pparams),
				&delegateErr,
			)
			require.Equal(t, keys[0], delegateErr.Delegate)
		})
		t.Run(era.name+"/fabricated keys beside an authorized one", func(t *testing.T) {
			t.Parallel()
			keys := append(fabricated(7), genesisKey)
			var delegateErr lcommon.ProtocolParameterUpdateDelegateError
			require.ErrorAs(
				t,
				validate(newTx(keys, signed), start, state, era.pparams),
				&delegateErr,
			)
		})
		t.Run(era.name+"/delegate without a witness", func(t *testing.T) {
			t.Parallel()
			keys := []lcommon.Blake2b224{genesisKey}
			for _, witness := range []lcommon.TransactionWitnessSet{
				unsigned,
				ppupWindowTestWitnessSet{},
			} {
				var witnessErr lcommon.ProtocolParameterUpdateWitnessError
				require.ErrorAs(
					t,
					validate(newTx(keys, witness), start, state, era.pparams),
					&witnessErr,
				)
				require.Equal(t, genesisKey, witnessErr.Delegate)
			}
		})
		t.Run(era.name+"/authorized proposal is accepted", func(t *testing.T) {
			t.Parallel()
			keys := []lcommon.Blake2b224{genesisKey}
			require.NoError(
				t,
				validate(newTx(keys, signed), start, state, era.pparams),
			)
		})
	}
}
