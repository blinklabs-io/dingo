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
	"errors"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

// TestTxValidationSessionTicksTwoEraBoundary pins that the forger's tick
// follows the era the ledger decides for a boundary even when that crosses two
// eras. The ledger enters both scheduled eras at one boundary whether or not a
// block header announces the second, so a mempool validation session at a slot
// past the boundary must tick the same path the rollover takes.
func TestTxValidationSessionTicksTwoEraBoundary(t *testing.T) {
	t.Parallel()

	ls, block := newBoundaryEraLedger(t, boundaryEraCase{
		pparamsMajor:   8,
		enableDijkstra: true,
		configure: func(cfg *cardano.CardanoNodeConfig) {
			enabled := true
			cfg.ExperimentalHardForksEnabled = &enabled
			epoch := uint64(1)
			cfg.TestConwayHardForkAtEpoch = &epoch
			cfg.TestDijkstraHardForkAtEpoch = &epoch
		},
	})
	require.Equal(t, eras.BabbageEraDesc.Id, ls.currentEra.Id)

	err := ls.WithTxValidationSessionAtSlot(
		t.Context(),
		block.SlotNumber(),
		func(
			_ func(
				lcommon.Transaction,
				map[utxoref.Key]struct{},
				map[utxoref.Key]lcommon.Utxo,
				*utxoref.StateOverlay,
			) error,
			_ func() bool,
			_ func(func() error) (bool, error),
		) error {
			return nil
		},
	)
	require.NoError(t, err)
}

// TestValidateBlockReferenceScriptsUsesEnactedProtocolVersion pins that the
// aggregate reference-script check reads the parameters the boundary enacts.
// Under major 10 a reference input missing from the UTxO set contributes
// nothing to the aggregate; from major 11 it is an error. A HardForkInitiation
// to major 11 ratified in the ending epoch must therefore change the verdict on
// a block forged at the first slot of the next epoch and leave one in the
// ending epoch alone.
func TestValidateBlockReferenceScriptsUsesEnactedProtocolVersion(t *testing.T) {
	t.Parallel()

	fixture := newTreasuryRolloverFixture(t, 1_000)
	seedEmptyRewardBasisForRollover(
		t,
		fixture.db,
		fixture.currentEpoch,
		fixture.currentPParams,
	)
	action := &lcommon.HardForkInitiationGovAction{Type: 1}
	action.ProtocolVersion.Major = lcommon.ProtocolVersionVanRossem
	actionCbor, err := cbor.Encode(action)
	require.NoError(t, err)
	ratifiedEpoch := fixture.currentEpoch.EpochId
	ratifiedSlot := fixture.currentEpoch.StartSlot + 50
	proposal := &models.GovernanceProposal{
		TxHash:        bytes.Repeat([]byte{0x61}, 32),
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch: fixture.currentEpoch.EpochId,
		ExpiresEpoch:  fixture.currentEpoch.EpochId + 20,
		ReturnAddress: bytes.Repeat([]byte{0}, 29),
		AnchorURL:     "https://example.invalid/van-rossem",
		AnchorHash:    bytes.Repeat([]byte{0x62}, 32),
		GovActionCbor: actionCbor,
		AddedSlot:     fixture.currentEpoch.StartSlot + 1,
		RatifiedEpoch: &ratifiedEpoch,
		RatifiedSlot:  &ratifiedSlot,
	}
	require.NoError(t, fixture.db.SetGovernanceProposal(
		t.Context(), proposal, nil,
	))
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

	require.NoError(t, fixture.ls.ValidateBlockReferenceScripts(
		t.Context(),
		blockAt(boundary-1),
	), "the ending epoch still runs under the unenacted major")
	require.Error(t, fixture.ls.ValidateBlockReferenceScripts(
		t.Context(),
		blockAt(boundary+1),
	), "the next epoch runs under the enacted major")
}

// boundaryHookCalls counts the calls an epoch rollover makes into the
// snapshot manager through the ledger's boundary hooks.
type boundaryHookCalls struct {
	snapshot, stake, spo, stakeInputs, capture, discard, prepare int
}

func (c *boundaryHookCalls) total() int {
	return c.snapshot + c.stake + c.spo + c.stakeInputs +
		c.capture + c.discard + c.prepare
}

func (c *boundaryHookCalls) install(ls *LedgerState, deferred bool) {
	ls.SetEpochBoundarySnapshotHook(
		func(*database.Txn, event.EpochTransitionEvent) error {
			c.snapshot++
			return nil
		},
	)
	ls.SetEpochBoundarySnapshotStakeHook(
		func(*database.Txn, event.EpochTransitionEvent) error {
			c.stake++
			return nil
		},
	)
	ls.SetCurrentBoundarySPOStakeHook(
		func(
			*database.Txn,
			event.EpochTransitionEvent,
			uint,
		) ([]*models.PoolStakeSnapshot, error) {
			c.spo++
			return nil, nil
		},
	)
	ls.SetEpochBoundaryDeferredStakeInputsHook(
		func(*database.Txn) (uint64, uint64, []*models.RewardStakeInput, bool) {
			c.stakeInputs++
			return 0, 0, nil, false
		},
	)
	if !deferred {
		return
	}
	ls.SetDeferredEpochBoundarySnapshotHooks(
		func(*database.Txn, event.EpochTransitionEvent) error {
			c.capture++
			return nil
		},
		func(uint64) { c.discard++ },
		func(*database.Txn, event.EpochTransitionEvent) (
			DeferredBoundarySnapshot, error,
		) {
			c.prepare++
			return nil, errors.New("unexpected prepare")
		},
	)
}

// TestTxValidationSessionTickLeavesBoundaryStateUntouched pins that the
// validation-only tick of an epoch boundary never reaches the snapshot
// manager. Its hooks keep shared state (the deferred captures, the pending
// SNAP distribution, the staged reward inputs) that a tick landing after the
// real rollover committed would overwrite from post-POOLREAP state.
func TestTxValidationSessionTickLeavesBoundaryStateUntouched(t *testing.T) {
	t.Parallel()

	for _, deferred := range []bool{false, true} {
		t.Run(fmt.Sprintf("deferred=%t", deferred), func(t *testing.T) {
			t.Parallel()

			fixture := newTreasuryRolloverFixture(t, 1_000)
			ls := fixture.ls
			seedEmptyRewardBasisForRollover(
				t, fixture.db, fixture.currentEpoch, fixture.currentPParams,
			)
			ls.epochCache = []models.Epoch{fixture.currentEpoch}
			ls.metrics.init(prometheus.NewRegistry())
			ls.publishSnapshotsLocked()
			var calls boundaryHookCalls
			calls.install(ls, deferred)

			// The same rollover without the tick flag reaches the hooks,
			// so a zero count below is not a hook that never fires here.
			errRollBack := errors.New("roll back")
			txn := fixture.db.Transaction(t.Context(), true)
			require.ErrorIs(t, txn.Do(func(txn *database.Txn) error {
				_, err := ls.processEpochRollover(
					t.Context(),
					txn,
					fixture.currentEpoch,
					ls.currentEra,
					ls.currentPParams,
				)
				require.NoError(t, err)
				return errRollBack
			}), errRollBack)
			require.NotZero(t, calls.total(),
				"a real rollover must call the boundary hooks")
			if deferred {
				require.NotZero(t, calls.capture)
			} else {
				require.NotZero(t, calls.stake)
				require.NotZero(t, calls.spo)
				require.NotZero(t, calls.snapshot)
			}
			calls = boundaryHookCalls{}
			phases := testutil.CollectAndCount(
				ls.metrics.epochRolloverPhaseDuration,
			)
			seq := ls.ratificationSeq.Load()

			boundary := fixture.currentEpoch.StartSlot +
				uint64(fixture.currentEpoch.LengthInSlots)
			require.NoError(t, ls.WithTxValidationSessionAtSlot(
				t.Context(),
				boundary,
				func(
					_ func(
						lcommon.Transaction,
						map[utxoref.Key]struct{},
						map[utxoref.Key]lcommon.Utxo,
						*utxoref.StateOverlay,
					) error,
					_ func() bool,
					_ func(func() error) (bool, error),
				) error {
					return nil
				},
			))

			require.Zero(t, calls.total(),
				"the validation tick called the boundary hooks: %+v", calls)
			require.Equal(t, seq, ls.ratificationSeq.Load(),
				"the validation tick scheduled a boundary job")
			require.Equal(t, phases, testutil.CollectAndCount(
				ls.metrics.epochRolloverPhaseDuration,
			), "the validation tick recorded rollover phase metrics")
		})
	}
}
