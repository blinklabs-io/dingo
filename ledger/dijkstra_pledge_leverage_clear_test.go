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
	"crypto/ed25519"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

const pledgeLeverageProposalDeposit = uint64(1_000_000)

// pledgeLeverageProposalTx builds a signed Dijkstra transaction that spends the
// fixture's regular input and carries one parameter-change proposal whose
// update is the given raw map. The update is encoded by the test, not by a
// ledger type, so tag 38 reaches the decoder as it would arrive on the wire.
func pledgeLeverageProposalTx(
	t *testing.T,
	fx *dijkstraCollateralReturnFixture,
	update map[uint]any,
) *gdijkstra.DijkstraTransaction {
	t.Helper()
	addressBytes, err := fx.address.Bytes()
	require.NoError(t, err)
	output, err := cbor.Encode([]any{
		addressBytes,
		10_000_000 - 1_000_000 - pledgeLeverageProposalDeposit,
	})
	require.NoError(t, err)
	rewardAccount := append(
		[]byte{0xe0},
		bytes.Repeat([]byte{0xc7}, lcommon.AddressHashSize)...,
	)
	body, err := cbor.Encode(map[uint]any{
		0: []any{[]any{fx.inputIds[0], uint64(0)}},
		1: []any{cbor.RawMessage(output)},
		2: uint64(1_000_000),
		20: []any{[]any{
			pledgeLeverageProposalDeposit,
			rewardAccount,
			[]any{uint64(0), nil, update, nil},
			[]any{
				"https://example.invalid/proposal",
				bytes.Repeat([]byte{0x5c}, 32),
			},
		}},
	})
	require.NoError(t, err)
	bodyHash := lcommon.Blake2b256Hash(body)
	txCbor, err := cbor.Encode([]any{
		cbor.RawMessage(body),
		map[uint]any{0: []any{[]any{
			fx.key.Public().(ed25519.PublicKey),
			ed25519.Sign(fx.key, bodyHash.Bytes()),
		}}},
		nil,
	})
	require.NoError(t, err)
	tx, err := gdijkstra.NewDijkstraTransactionFromCbor(txCbor)
	require.NoError(t, err)
	return tx
}

// TestDijkstraPledgeLeverageClearProposalThroughProduction drives a parameter
// change whose tag 38 is null, alone and beside another field, through mempool
// validation and then through validated block application, unvalidated replay
// and backfill. Each must persist the action as it appeared on the wire, which
// still clears the parameter, and the database rollback must drop it.
func TestDijkstraPledgeLeverageClearProposalThroughProduction(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		update map[uint]any
	}{
		{"alone", map[uint]any{38: nil}},
		{"beside another field", map[uint]any{0: uint64(44), 38: nil}},
	} {
		for _, mode := range []string{"validated", "replayed", "backfilled"} {
			validate := mode == "validated"
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				t.Parallel()
				fx := newDijkstraCollateralReturnFixture(
					t,
					lcommon.AddressTypeKeyNone,
				)
				pp := fx.ls.currentPParams.(*gdijkstra.DijkstraProtocolParameters)
				pp.GovActionDeposit = pledgeLeverageProposalDeposit
				pp.GovActionValidityPeriod = 6
				fx.ls.publishSnapshotsLocked()
				deposit := uint64(2_000_000)
				rewardCred := lcommon.Credential{
					CredType: lcommon.CredentialTypeAddrKeyHash,
					Credential: lcommon.NewBlake2b224(
						bytes.Repeat([]byte{0xc7}, lcommon.AddressHashSize),
					),
				}
				seedStakeRegistration(t, fx.db, rewardCred, &deposit, 1, 0xc7)
				require.NoError(t, fx.db.SetConstitution(&models.Constitution{
					AnchorURL:  "https://example.invalid/constitution",
					AnchorHash: bytes.Repeat([]byte{0xc1}, 32),
				}, nil))

				tx := pledgeLeverageProposalTx(t, fx, tc.update)
				require.NoError(t, fx.ls.ValidateTx(tx), "mempool validation")

				block := newDijkstraCollateralReturnBlock(t, tx)
				var txHash [32]byte
				copy(txHash[:], tx.Hash().Bytes())
				offsets := &database.BlockIngestionResult{
					TxOffsets: map[[32]byte]database.CborOffset{
						txHash: {
							BlockSlot: dijkstraCollateralReturnTestSlot,
							ByteLength: uint32(
								len(tx.Cbor()),
							), //nolint:gosec // fixture size
						},
					},
					UtxoOffsets: map[database.UtxoRef]database.CborOffset{},
				}
				for _, utxo := range tx.Produced() {
					var producedHash [32]byte
					copy(producedHash[:], utxo.Id.Id().Bytes())
					offsets.UtxoOffsets[database.UtxoRef{
						TxId:      producedHash,
						OutputIdx: uint32(utxo.Id.Index()), //nolint:gosec // small fixture index
					}] = database.CborOffset{BlockSlot: dijkstraCollateralReturnTestSlot, ByteLength: 1}
				}
				applyBlock := func(txn *database.Txn) error {
					delta, err := fx.ls.ledgerProcessBlock(t.Context(),
						txn,
						ocommon.NewPoint(
							dijkstraCollateralReturnTestSlot,
							block.Hash().Bytes(),
						),
						block,
						validate,
						false,
						false,
						nil,
						envelopeParent{origin: true},
						offsets,
						eras.DijkstraEraDesc,
						fx.ls.currentPParams,
						nil,
						0,
						0,
						false,
					)
					if err != nil || delta == nil {
						return err
					}
					// Without validation the block's delta is returned for the
					// caller to apply in a batch.
					defer delta.Release()
					return delta.apply(t.Context(), fx.ls, txn)
				}
				// Backfill persists proposals through the same function as
				// block application, without applying the block.
				backfill := func(txn *database.Txn) error {
					return governance.ProcessProposals(t.Context(),
						tx,
						ocommon.NewPoint(
							dijkstraCollateralReturnTestSlot,
							block.Hash().Bytes(),
						),
						0,
						0,
						pp.GovActionValidityPeriod,
						pp,
						fx.db,
						txn,
						nil,
					)
				}
				apply := applyBlock
				if mode == "backfilled" {
					apply = backfill
				}
				require.NoError(
					t,
					fx.db.Transaction(t.Context(), true).Do(apply),
				)

				stored, err := fx.db.GetGovernanceProposal(t.Context(),
					tx.Hash().Bytes(),
					0,
					nil,
				)
				require.NoError(t, err)
				wantAction, err := cbor.Encode(
					[]any{uint64(0), nil, tc.update, nil},
				)
				require.NoError(t, err)
				require.Equal(t, wantAction, stored.GovActionCbor,
					"the persisted action must keep tag 38 as null")
				action, err := governance.DecodeGovActionForPParams(
					stored.GovActionCbor,
					stored.ActionType,
					fx.ls.currentPParams,
				)
				require.NoError(t, err)
				update := action.(*gdijkstra.DijkstraParameterChangeGovAction).ParamUpdate
				require.True(t, update.MaxPledgeLeverageSet)
				require.Nil(t, update.MaxPledgeLeverage)

				require.NoError(
					t,
					fx.db.Transaction(t.Context(), true).
						Do(func(txn *database.Txn) error {
							return fx.db.DeleteGovernanceProposalsAfterSlot(
								t.Context(),
								0,
								txn,
							)
						}),
				)
				_, err = fx.db.GetGovernanceProposal(
					t.Context(),
					tx.Hash().Bytes(),
					0,
					nil,
				)
				require.Error(t, err, "rollback must drop the proposal")
			})
		}
	}
}
