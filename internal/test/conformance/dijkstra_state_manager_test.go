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

package conformance

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	mockconformance "github.com/blinklabs-io/ouroboros-mock/conformance"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func TestDijkstraStateManagerAppliesChildBodyOutputsAndDeposits(t *testing.T) {
	t.Parallel()
	manager, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, manager.Close()) }()

	stakeCredential := testHash28(0x42)
	accountKey := mockledger.RewardAccountKey{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: stakeCredential,
	}
	require.NoError(t, manager.LoadInitialState(
		&mockconformance.ParsedInitialState{
			StakeRegistrationsByCredential: map[mockledger.RewardAccountKey]bool{
				accountKey: true,
			},
			RewardAccountBalances: map[mockledger.RewardAccountKey]uint64{
				accountKey: 5,
			},
		},
		&dijkstra.DijkstraProtocolParameters{},
	))

	outputAddress := append([]byte{0x60}, bytes.Repeat([]byte{0x24}, 28)...)
	rewardAddress := append([]byte{0xe0}, stakeCredential[:]...)
	childBody, err := cbor.Encode(map[uint]any{
		0:  []any{},
		1:  []any{map[uint]any{0: outputAddress, 1: uint64(1_000_000)}},
		25: map[cbor.ByteString]uint64{cbor.NewByteString(rewardAddress): 20},
	})
	require.NoError(t, err)
	childTx, err := cbor.Encode([]any{
		cbor.RawMessage(childBody), map[uint]any{}, nil,
	})
	require.NoError(t, err)
	rootBody, err := cbor.Encode(map[uint]any{
		0:  []any{},
		1:  []any{},
		2:  uint64(0),
		23: cbor.NewSetType([]cbor.RawMessage{childTx}, true),
		25: map[cbor.ByteString]uint64{cbor.NewByteString(rewardAddress): 5},
	})
	require.NoError(t, err)
	txCbor, err := cbor.Encode([]any{
		cbor.RawMessage(rootBody), map[uint]any{}, nil,
	})
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(gledger.TxTypeDijkstra, txCbor)
	require.NoError(t, err)
	dijkstraTx, ok := tx.(*dijkstra.DijkstraTransaction)
	require.True(t, ok)
	childHash := dijkstraTx.Body.TxSubTransactions.Items()[0].Body.Id()
	rootHash := tx.Hash()

	require.NoError(t, manager.ApplyTransaction(tx, 10))

	childUtxo, err := manager.db.UtxoByRef(childHash.Bytes(), 0, nil)
	require.NoError(t, err)
	require.NotNil(t, childUtxo)
	rootUtxoExists, err := manager.db.UtxoExists(rootHash.Bytes(), 0, nil)
	require.NoError(t, err)
	require.False(t, rootUtxoExists)

	account, err := manager.db.GetAccountByCredential(
		uint8(common.CredentialTypeAddrKeyHash),
		stakeCredential[:],
		false,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(30), uint64(account.Reward))
	require.Equal(
		t,
		uint64(30),
		manager.govState.RewardAccountBalances[accountKey],
	)

	childTxRow, err := manager.db.Metadata().GetTransactionByHash(
		childHash.Bytes(), nil,
	)
	require.NoError(t, err)
	require.NotNil(t, childTxRow)
	rootTxRow, err := manager.db.Metadata().GetTransactionByHash(
		rootHash.Bytes(), nil,
	)
	require.NoError(t, err)
	require.NotNil(t, rootTxRow)
	_, err = manager.db.Metadata().GetUtxo(childHash.Bytes(), 0, nil)
	require.NoError(t, err)
	rootUtxo, err := manager.db.Metadata().GetUtxo(rootHash.Bytes(), 0, nil)
	require.NoError(t, err)
	require.Nil(t, rootUtxo)
}

func TestDijkstraStateManagerEnactsDijkstraParameterChange(t *testing.T) {
	t.Parallel()
	manager, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, manager.Close()) }()

	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MinFeeA: 1,
		},
	}
	manager.protocolParams = pparams
	manager.currentEpoch = 42
	newMinFeeA := uint(1234)
	action := &dijkstra.DijkstraParameterChangeGovAction{
		Type: uint(common.GovActionTypeParameterChange),
		ParamUpdate: dijkstra.DijkstraProtocolParameterUpdate{
			MinFeeA: &newMinFeeA,
		},
	}
	actionCbor, err := cbor.Encode(action)
	require.NoError(t, err)
	proposalHash := bytes.Repeat([]byte{0x35}, 32)
	proposal := &models.GovernanceProposal{
		TxHash:        proposalHash,
		ActionType:    uint8(common.GovActionTypeParameterChange),
		GovActionCbor: actionCbor,
		AddedSlot:     10,
		ExpiresEpoch:  100,
		AnchorURL:     "https://example.invalid/dijkstra-pparam",
		AnchorHash:    bytes.Repeat([]byte{0x36}, 32),
		ReturnAddress: append([]byte{0xe0}, bytes.Repeat([]byte{0x37}, 28)...),
	}
	require.NoError(t, manager.db.SetGovernanceProposal(proposal, nil))

	proposalID := hex.EncodeToString(proposalHash) + "#0"
	txn := manager.db.Transaction(true)
	defer txn.Release()
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return manager.persistEnactment(txn, proposalID, 2000)
	}))
	updated, ok := manager.protocolParams.(*dijkstra.DijkstraProtocolParameters)
	require.True(t, ok, "Dijkstra parameter enactment must retain Dijkstra parameters")
	require.Equal(t, newMinFeeA, updated.MinFeeA)
}
