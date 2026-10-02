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

// Package dijkstrabatchfixture provides one encoded Dijkstra governance batch
// shared by live ledger, replay, and metadata backfill tests.
package dijkstrabatchfixture

import (
	"bytes"
	"fmt"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

type Fixture struct {
	Transaction *dijkstra.DijkstraTransaction
	ProposalIDs [2]common.Blake2b256
	RootID      common.Blake2b256
}

// New builds and decodes a single batch: child 0 proposes a root action,
// child 1 proposes a child action and votes for the root, and the enclosing
// body casts a second vote for the root.
func New() (Fixture, error) {
	rewardAddress, err := common.NewAddressFromBytes(
		append([]byte{0xe0}, bytes.Repeat([]byte{0x42}, 28)...),
	)
	if err != nil {
		return Fixture{}, err
	}
	proposal := func(marker byte, parent *common.GovActionId) dijkstra.DijkstraProposalProcedure {
		return dijkstra.DijkstraProposalProcedure{
			PPDeposit:       42,
			PPRewardAccount: rewardAddress,
			PPGovAction: dijkstra.DijkstraGovAction{
				Type: uint(common.GovActionTypeNoConfidence),
				Action: &common.NoConfidenceGovAction{
					Type:     uint(common.GovActionTypeNoConfidence),
					ActionId: parent,
				},
			},
			PPAnchor: common.GovAnchor{
				Url:      "https://example.invalid/dijkstra-batch-shared",
				DataHash: [32]byte(bytes.Repeat([]byte{marker}, 32)),
			},
		}
	}
	child0 := dijkstra.DijkstraSubTransactionBody{
		TxProposalProcedures: []dijkstra.DijkstraProposalProcedure{
			proposal(0x31, nil),
		},
	}
	child0ID, err := bodyID(child0)
	if err != nil {
		return Fixture{}, err
	}
	rootAction := &common.GovActionId{TransactionId: child0ID}
	child1 := dijkstra.DijkstraSubTransactionBody{
		TxProposalProcedures: []dijkstra.DijkstraProposalProcedure{
			proposal(0x32, rootAction),
		},
		TxVotingProcedures: vote(0x41, child0ID),
	}
	child1ID, err := bodyID(child1)
	if err != nil {
		return Fixture{}, err
	}
	_ = child1ID
	top := dijkstra.DijkstraTransactionBody{
		TxVotingProcedures: vote(0x42, child0ID),
	}
	subs := []dijkstra.DijkstraSubTransaction{
		{Body: child0},
		{Body: child1},
	}
	tx := &dijkstra.DijkstraTransaction{TxIsValid: true}
	tx.Body = top
	tx.Body.TxSubTransactions = cbor.NewSetType(subs, true)
	txCbor, err := tx.MarshalCBOR()
	if err != nil {
		return Fixture{}, err
	}
	decoded, err := gledger.NewTransactionFromCbor(gledger.TxTypeDijkstra, txCbor)
	if err != nil {
		return Fixture{}, err
	}
	return Fixture{
		Transaction: decoded.(*dijkstra.DijkstraTransaction),
		ProposalIDs: [2]common.Blake2b256{child0ID, child1ID},
		RootID:      child0ID,
	}, nil
}

func bodyID(body dijkstra.DijkstraSubTransactionBody) (common.Blake2b256, error) {
	tx := &dijkstra.DijkstraTransaction{TxIsValid: true}
	tx.Body.TxSubTransactions = cbor.NewSetType(
		[]dijkstra.DijkstraSubTransaction{{Body: body}}, true,
	)
	encoded, err := tx.MarshalCBOR()
	if err != nil {
		return common.Blake2b256{}, fmt.Errorf("marshal child body: %w", err)
	}
	decoded, err := gledger.NewTransactionFromCbor(gledger.TxTypeDijkstra, encoded)
	if err != nil {
		return common.Blake2b256{}, fmt.Errorf("decode child body: %w", err)
	}
	return decoded.(*dijkstra.DijkstraTransaction).Body.TxSubTransactions.Items()[0].Body.Id(), nil
}

func vote(marker byte, action common.Blake2b256) common.VotingProcedures {
	voter := common.Voter{
		Type: common.VoterTypeDRepKeyHash,
		Hash: [28]byte(bytes.Repeat([]byte{marker}, 28)),
	}
	return common.VotingProcedures{
		&voter: {
			&common.GovActionId{TransactionId: action}: {
				Vote: common.GovVoteYes,
			},
		},
	}
}
