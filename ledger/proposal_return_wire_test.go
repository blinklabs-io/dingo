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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

// proposalReturnWireAddress returns a testnet account address, or a base
// address carrying the same stake credential, as the proposal return address.
func proposalReturnWireAddress(t *testing.T, base bool) lcommon.Address {
	t.Helper()
	stake := bytes.Repeat([]byte{0x42}, lcommon.AddressHashSize)
	if !base {
		return proposalReturnAccountAddress(
			t, 0xe, lcommon.Blake2b224(stake),
		)
	}
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x7a}, lcommon.AddressHashSize),
		stake,
	)
	require.NoError(t, err)
	return addr
}

func proposalReturnWireInput() shelley.ShelleyTransactionInput {
	return shelley.NewShelleyTransactionInput(
		"1128b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee88",
		0,
	)
}

func proposalReturnWireOutput(t *testing.T) babbage.BabbageTransactionOutput {
	t.Helper()
	return babbage.BabbageTransactionOutput{
		OutputAddress: proposalReturnAccountAddress(
			t, 0x6, lcommon.Blake2b224(bytes.Repeat([]byte{0x11}, 28)),
		),
		OutputAmount: mary.MaryTransactionOutputValue{Amount: 1_000_000},
	}
}

func conwayWireTx(t *testing.T, base bool) *conway.ConwayTransaction {
	t.Helper()
	return &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{proposalReturnWireInput()},
			),
			TxOutputs: []babbage.BabbageTransactionOutput{
				proposalReturnWireOutput(t),
			},
			TxFee: 1,
			TxProposalProcedures: []conway.ConwayProposalProcedure{{
				PPDeposit:       1,
				PPRewardAccount: proposalReturnWireAddress(t, base),
				PPAnchor: lcommon.GovAnchor{
					Url:      "https://example.com/proposal",
					DataHash: [32]byte(bytes.Repeat([]byte{0x01}, 32)),
				},
				PPGovAction: conway.ConwayGovAction{
					Type: uint(lcommon.GovActionTypeInfo),
					Action: &lcommon.InfoGovAction{
						Type: uint(lcommon.GovActionTypeInfo),
					},
				},
			}},
		},
		TxIsValid: true,
	}
}

func dijkstraWireProposal(
	t *testing.T,
	base bool,
) dijkstra.DijkstraProposalProcedure {
	t.Helper()
	return dijkstra.DijkstraProposalProcedure{
		PPDeposit:       1,
		PPRewardAccount: proposalReturnWireAddress(t, base),
		PPAnchor: lcommon.GovAnchor{
			Url:      "https://example.com/proposal",
			DataHash: [32]byte(bytes.Repeat([]byte{0x01}, 32)),
		},
		PPGovAction: dijkstra.DijkstraGovAction{
			Type: uint(lcommon.GovActionTypeInfo),
			Action: &lcommon.InfoGovAction{
				Type: uint(lcommon.GovActionTypeInfo),
			},
		},
	}
}

func dijkstraWireTx(
	t *testing.T,
	base bool,
	child bool,
) *dijkstra.DijkstraTransaction {
	t.Helper()
	tx := &dijkstra.DijkstraTransaction{
		Body: dijkstra.DijkstraTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{proposalReturnWireInput()},
			),
			TxOutputs: []dijkstra.DijkstraTransactionOutput{
				{Output: proposalReturnWireOutput(t)},
			},
			TxFee: 1,
		},
		TxIsValid: true,
	}
	proposals := []dijkstra.DijkstraProposalProcedure{
		dijkstraWireProposal(t, base),
	}
	if child {
		tx.Body.TxSubTransactions = cbor.NewSetType(
			[]dijkstra.DijkstraSubTransaction{{
				Body: dijkstra.DijkstraSubTransactionBody{
					TxProposalProcedures: proposals,
				},
			}},
			true,
		)
		return tx
	}
	tx.Body.TxProposalProcedures = proposals
	return tx
}

// dijkstraWireBlock encodes tx in a Dijkstra block, the only encoding that
// carries is_valid=false, and decodes it through the live block entry point
// and the stored-block decoder, which must reach one decision.
func dijkstraWireBlock(
	t *testing.T,
	tx *dijkstra.DijkstraTransaction,
	isValid bool,
) error {
	t.Helper()
	tx.TxIsValid = isValid
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber:  1,
					Slot:         1,
					ProtoVersion: babbage.BabbageProtoVersion{Major: 12},
				},
			},
		},
		BlockBody: dijkstra.DijkstraBlockBody{
			Transactions: []dijkstra.DijkstraTransaction{*tx},
		},
	}
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	verify := lcommon.VerifyConfig{SkipBodyHashValidation: true}
	_, liveErr := gledger.NewBlockFromCbor(
		gledger.BlockTypeDijkstra, blockCbor, verify,
	)
	_, storedErr := models.DecodeBlockCbor(
		gledger.BlockTypeDijkstra, blockCbor, verify,
	)
	require.Equal(
		t,
		liveErr == nil,
		storedErr == nil,
		"live and stored decode disagree: live=%v stored=%v",
		liveErr,
		storedErr,
	)
	return liveErr
}

// conwayWireBlock replaces the transactions of a real Conway block with tx and
// decodes the result through the live block entry point and the stored-block
// decoder replay and backfill read blocks with, which must reach one decision.
func conwayWireBlock(
	t *testing.T,
	tx *conway.ConwayTransaction,
	isValid bool,
) error {
	t.Helper()
	root, err := fixtures.ExtractEmbeddedFixtures(t.TempDir())
	require.NoError(t, err)
	fixture, err := fixtures.NewFixture(
		root,
		root+"/ouroboros-consensus/ouroboros-consensus-cardano/golden/"+
			"cardano/CardanoNodeToNodeVersion2/Block_Conway",
	)
	require.NoError(t, err)
	raw, err := fixture.ConsensusLedgerBlockBytes()
	require.NoError(t, err)
	var parts []cbor.RawMessage
	_, err = cbor.Decode(raw, &parts)
	require.NoError(t, err)
	require.Len(t, parts, 5)
	txCbor, err := tx.MarshalCBOR()
	require.NoError(t, err)
	var txParts []cbor.RawMessage
	_, err = cbor.Decode(txCbor, &txParts)
	require.NoError(t, err)
	require.Len(t, txParts, 4)
	invalid := []uint{}
	if !isValid {
		invalid = []uint{0}
	}
	// The stored-block decoder checks the header's body hash, so rebuild it
	// over the replacement body: a hash of the hashes of the four parts.
	bodyHash := func(parts ...[]byte) []byte {
		var joined []byte
		for _, part := range parts {
			sum := lcommon.Blake2b256Hash(part)
			joined = append(joined, sum[:]...)
		}
		sum := lcommon.Blake2b256Hash(joined)
		return sum[:]
	}
	oldHash := bodyHash(parts[1], parts[2], parts[3], parts[4])
	require.True(
		t,
		bytes.Contains(parts[0], oldHash),
		"fixture header must carry the body hash derived from its parts",
	)
	bodies, err := cbor.Encode([]cbor.RawMessage{txParts[0]})
	require.NoError(t, err)
	witnesses, err := cbor.Encode([]cbor.RawMessage{txParts[1]})
	require.NoError(t, err)
	auxData, err := cbor.Encode(map[uint]cbor.RawMessage{})
	require.NoError(t, err)
	invalidCbor, err := cbor.Encode(invalid)
	require.NoError(t, err)
	header := bytes.Replace(
		parts[0],
		oldHash,
		bodyHash(bodies, witnesses, auxData, invalidCbor),
		1,
	)
	blockCbor, err := cbor.Encode([]any{
		cbor.RawMessage(header),
		cbor.RawMessage(bodies),
		cbor.RawMessage(witnesses),
		cbor.RawMessage(auxData),
		cbor.RawMessage(invalidCbor),
	})
	require.NoError(t, err)
	_, liveErr := gledger.NewBlockFromCbor(gledger.BlockTypeConway, blockCbor)
	_, storedErr := models.DecodeBlockCbor(gledger.BlockTypeConway, blockCbor)
	require.Equal(
		t,
		liveErr == nil,
		storedErr == nil,
		"live and stored decode disagree: live=%v stored=%v",
		liveErr,
		storedErr,
	)
	return liveErr
}

// TestProposalReturnAddressWireDecisionIsIdenticalAcrossPaths feeds the same
// proposal bytes to the transaction decoder mempool admission and the
// submission APIs use (safedecode.Transaction) and to the block decoders. A
// base address as return address is refused whether the transaction claims
// phase-2 validity or not, at top level and in a child, and a valid account
// address is accepted by each, so no entry point decides differently from
// another for the same bytes.
func TestProposalReturnAddressWireDecisionIsIdenticalAcrossPaths(
	t *testing.T,
) {
	t.Parallel()

	for _, base := range []bool{false, true} {
		name := "account address"
		if base {
			name = "base address"
		}
		check := func(t *testing.T, err error) {
			t.Helper()
			if base {
				require.ErrorContains(t, err, "invalid account address type")
				return
			}
			require.NoError(t, err)
		}
		t.Run("conway tx/"+name, func(t *testing.T) {
			t.Parallel()
			txCbor, err := conwayWireTx(t, base).MarshalCBOR()
			require.NoError(t, err)
			_, err = safedecode.Transaction(
				uint(gledger.TxTypeConway), txCbor,
			)
			check(t, err)
		})
		t.Run("dijkstra tx/"+name, func(t *testing.T) {
			t.Parallel()
			txCbor, err := dijkstraWireTx(t, base, false).MarshalCBOR()
			require.NoError(t, err)
			_, err = safedecode.Transaction(
				uint(gledger.TxTypeDijkstra), txCbor,
			)
			check(t, err)
		})
		t.Run("dijkstra child tx/"+name, func(t *testing.T) {
			t.Parallel()
			txCbor, err := dijkstraWireTx(t, base, true).MarshalCBOR()
			require.NoError(t, err)
			_, err = safedecode.Transaction(
				uint(gledger.TxTypeDijkstra), txCbor,
			)
			check(t, err)
		})
		for _, isValid := range []bool{true, false} {
			validity := "phase-2 invalid"
			if isValid {
				validity = "valid"
			}
			t.Run("conway block "+validity+"/"+name, func(t *testing.T) {
				t.Parallel()
				check(t, conwayWireBlock(t, conwayWireTx(t, base), isValid))
			})
			t.Run("dijkstra block "+validity+"/"+name, func(t *testing.T) {
				t.Parallel()
				check(t, dijkstraWireBlock(
					t, dijkstraWireTx(t, base, false), isValid,
				))
			})
			t.Run(
				"dijkstra child block "+validity+"/"+name,
				func(t *testing.T) {
					t.Parallel()
					check(t, dijkstraWireBlock(
						t, dijkstraWireTx(t, base, true), isValid,
					))
				},
			)
		}
	}
}

// TestRejectedProposalReturnAddressPersistsNoGovernanceProposal checks that a
// Dijkstra block whose transaction, valid or phase-2 invalid, carries a child
// proposal with a base return address fails to decode, so nothing reaches
// block application and no governance proposal exists for the child's id. The
// same child with an account address is applied and persists, so the absence
// is the rejection's doing.
func TestRejectedProposalReturnAddressPersistsNoGovernanceProposal(
	t *testing.T,
) {
	t.Parallel()
	ls, db := newSubGovLedger(t)

	for _, tc := range []struct {
		name    string
		base    bool
		isValid bool
	}{
		{"account address valid", false, true},
		{"base address valid", true, true},
		{"base address phase-2 invalid", true, false},
	} {
		child := dijkstra.DijkstraSubTransactionBody{
			TxProposalProcedures: []dijkstra.DijkstraProposalProcedure{
				dijkstraWireProposal(t, tc.base),
			},
		}
		// The child body is encoded without the return-address check, so its
		// id is the one the proposal would be keyed by if it were persisted.
		childCbor, err := cbor.Encode(&child)
		require.NoError(t, err)
		proposalID := lcommon.Blake2b256Hash(childCbor)

		tx := dijkstraWireTx(t, tc.base, true)
		tx.TxIsValid = tc.isValid
		bodyCbor, err := dijkstra.DijkstraBlockBody{
			Transactions: []dijkstra.DijkstraTransaction{*tx},
		}.MarshalCBOR()
		require.NoError(t, err)
		var body dijkstra.DijkstraBlockBody
		decodeErr := body.UnmarshalCBOR(bodyCbor)
		if tc.base {
			require.ErrorContains(
				t, decodeErr, "invalid account address type", tc.name,
			)
			_, err := db.GetGovernanceProposal(proposalID.Bytes(), 0, nil)
			require.ErrorIs(
				t, err, models.ErrGovernanceProposalNotFound, tc.name,
			)
			continue
		}
		require.NoError(t, decodeErr, tc.name)
		require.NoError(
			t, applySubGovBlock(t, ls, 1, &body.Transactions[0]), tc.name,
		)
		requireSubGovProposal(
			t,
			db,
			body.Transactions[0].Body.TxSubTransactions.Items()[0].Body.Id(),
		)
	}
}
