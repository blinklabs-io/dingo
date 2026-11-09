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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestTxValidationSessionTicksEpochBoundary(t *testing.T) {
	t.Parallel()

	const (
		oldTreasury = uint64(1_000)
		donation    = uint64(100)
	)
	fixture := newTreasuryRolloverFixture(t, oldTreasury)
	seedEmptyRewardBasisForRollover(
		t,
		fixture.db,
		fixture.currentEpoch,
		fixture.currentPParams,
	)
	require.NoError(t, fixture.db.Metadata().AddNetworkDonation(
		fixture.currentEpoch.StartSlot+50,
		fixture.currentEpoch.EpochId,
		donation,
		nil,
	))

	wantTreasury := oldTreasury
	validatorCalls := 0
	validationEra := eras.ConwayEraDesc
	validationEra.ValidateTxFunc = func(
		_ lcommon.Transaction,
		_ uint64,
		state lcommon.LedgerState,
		_ lcommon.ProtocolParameters,
	) error {
		validatorCalls++
		provider, ok := state.(interface {
			TreasuryValue() (uint64, error)
		})
		if !ok {
			return fmt.Errorf("validation state does not expose treasury")
		}
		got, err := provider.TreasuryValue()
		if err != nil {
			return err
		}
		if got != wantTreasury {
			return fmt.Errorf("treasury = %d, want %d", got, wantTreasury)
		}
		return nil
	}
	fixture.ls.currentEra = validationEra
	fixture.ls.activeEras = []eras.EraDesc{validationEra}
	fixture.ls.epochCache = []models.Epoch{fixture.currentEpoch}
	fixture.ls.metrics.init(prometheus.NewRegistry())
	fixture.ls.publishSnapshotsLocked()

	validateAt := func(slot uint64) error {
		return fixture.ls.WithTxValidationSessionAtSlot(
			t.Context(),
			slot,
			func(
				validate func(
					lcommon.Transaction,
					map[utxoref.Key]struct{},
					map[utxoref.Key]lcommon.Utxo,
					*utxoref.StateOverlay,
				) error,
				_ func() bool,
				_ func(func() error) (bool, error),
			) error {
				return validate(
					&conway.ConwayTransaction{},
					nil,
					nil,
					nil,
				)
			},
		)
	}

	wantTreasury = oldTreasury
	require.NoError(t, validateAt(fixture.currentEpoch.StartSlot+99))
	wantTreasury = oldTreasury + donation
	require.NoError(t, validateAt(fixture.currentEpoch.StartSlot+100))
	require.Equal(
		t,
		2,
		validatorCalls,
		"the injected validator must run for both slot validations",
	)

	requireTreasuryValue(t, fixture.ls, nil, oldTreasury)
	nextEpoch, err := fixture.db.Metadata().GetEpoch(
		fixture.currentEpoch.EpochId+1,
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, nextEpoch)
}

func TestValidateBlockReferenceScriptsUsesTickedSession(t *testing.T) {
	t.Parallel()

	fixture := newTreasuryRolloverFixture(t, 1_000)
	seedEmptyRewardBasisForRollover(
		t,
		fixture.db,
		fixture.currentEpoch,
		fixture.currentPParams,
	)
	fixture.ls.epochCache = []models.Epoch{fixture.currentEpoch}
	fixture.ls.metrics.init(prometheus.NewRegistry())
	fixture.ls.publishSnapshotsLocked()

	blockAt := func(slot uint64) *conway.ConwayBlock {
		return &conway.ConwayBlock{
			BlockHeader: &conway.ConwayBlockHeader{
				BabbageBlockHeader: babbage.BabbageBlockHeader{
					Body: babbage.BabbageBlockHeaderBody{Slot: slot},
				},
			},
			TransactionBodies: []conway.ConwayTransactionBody{{
				TxReferenceInputs: cbor.NewSetType(
					[]shelley.ShelleyTransactionInput{{
						TxId: lcommon.NewBlake2b256(
							bytes.Repeat([]byte{7}, 32),
						),
					}},
					false,
				),
			}},
			TransactionWitnessSets: []conway.ConwayTransactionWitnessSet{{}},
		}
	}
	boundary := fixture.currentEpoch.StartSlot +
		uint64(fixture.currentEpoch.LengthInSlots)
	nextEpochEnd := boundary + uint64(
		fixture.ls.config.CardanoNodeConfig.ShelleyGenesis().EpochLength,
	)

	// A slot in the next epoch ticks across the boundary and passes.
	err := fixture.ls.ValidateBlockReferenceScripts(
		t.Context(),
		blockAt(boundary+1),
	)
	require.NoError(t, err)

	// Only a ticked view refuses a slot the ledger cannot reach from its
	// applied epoch in one boundary.
	err = fixture.ls.ValidateBlockReferenceScripts(
		t.Context(),
		blockAt(nextEpochEnd+1),
	)
	require.ErrorContains(t, err, "more than one epoch beyond")
}
