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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func TestProcessGovernanceAcceptsDijkstraProtocolParameters(t *testing.T) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	pparams := mockledger.NewMockConwayProtocolParams()
	pparams.GovActionValidityPeriod = 20
	pparams.DRepInactivityPeriod = 20
	ls := &LedgerState{
		db: db,
		currentEpoch: models.Epoch{
			EpochId: 12,
		},
		currentPParams: &dijkstra.DijkstraProtocolParameters{
			ConwayProtocolParameters: pparams,
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}

	rewardAddress, err := lcommon.NewAddressFromBytes(
		append([]byte{0xE1}, bytes.Repeat([]byte{0xAB}, 28)...),
	)
	require.NoError(t, err)
	var anchorHash [32]byte
	copy(anchorHash[:], bytes.Repeat([]byte{0xCD}, 32))
	proposal := conway.ConwayProposalProcedure{
		PPDeposit:       42,
		PPRewardAccount: rewardAddress,
		PPGovAction: conway.ConwayGovAction{
			Type: uint(lcommon.GovActionTypeInfo),
			Action: &lcommon.InfoGovAction{
				Type: uint(lcommon.GovActionTypeInfo),
			},
		},
		PPAnchor: lcommon.GovAnchor{
			Url:      "https://example.com/dijkstra-proposal",
			DataHash: anchorHash,
		},
	}
	tx := mockledger.NewTransactionBuilder()
	tx.WithId(bytes.Repeat([]byte{0x11}, 32))
	tx.WithType(gledger.TxTypeDijkstra)
	tx.WithProposalProcedures(proposal)
	tx.WithValid(true)

	delta := NewLedgerDelta(
		ocommon.NewPoint(100, bytes.Repeat([]byte{0x22}, 32)),
		uint(dijkstra.EraIdDijkstra),
		1,
	)
	defer delta.Release()

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return delta.processGovernance(ls, tx, txn)
	}))

	got, err := db.GetGovernanceProposal(tx.Hash().Bytes(), 0, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(12), got.ProposedEpoch)
	require.Equal(t, uint64(32), got.ExpiresEpoch)
}

func TestConwayProtocolParametersNilDijkstra(t *testing.T) {
	var pparams *dijkstra.DijkstraProtocolParameters

	require.Nil(t, conwayProtocolParameters(pparams))
}

func TestApplyTransactionMetadataOnlyRecordsNetworkDonations(t *testing.T) {
	db := newDonationTestDB(t)
	ls := &LedgerState{
		db: db,
		currentEpoch: models.Epoch{
			EpochId: 9,
		},
	}

	tx1 := mockledger.NewTransactionBuilder()
	tx1.WithId(bytes.Repeat([]byte{0x31}, lcommon.Blake2b256Size))
	tx1.WithType(gledger.TxTypeDijkstra)
	tx1.WithValid(true)
	tx1.WithDonation(42)

	tx2 := mockledger.NewTransactionBuilder()
	tx2.WithId(bytes.Repeat([]byte{0x32}, lcommon.Blake2b256Size))
	tx2.WithType(gledger.TxTypeDijkstra)
	tx2.WithValid(true)
	tx2.WithDonation(58)

	delta := NewLedgerDelta(
		ocommon.NewPoint(123, bytes.Repeat([]byte{0x33}, 32)),
		uint(dijkstra.EraIdDijkstra),
		1,
	)
	defer delta.Release()
	delta.addTransaction(tx1, 0)
	delta.addTransaction(tx2, 1)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return delta.applyTransactionMetadataOnlyWithDonationRecording(ls, txn, true)
	}))

	sum, err := db.Metadata().SumNetworkDonationsForEpoch(9, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(100), sum)
}

func TestApplyTransactionMetadataOnlyAggregatesPendingNetworkDonations(
	t *testing.T,
) {
	db := newDonationTestDB(t)
	ls := &LedgerState{
		db: db,
		currentEpoch: models.Epoch{
			EpochId: 9,
		},
	}

	endorserTx := mockledger.NewTransactionBuilder()
	endorserTx.WithId(bytes.Repeat([]byte{0x41}, lcommon.Blake2b256Size))
	endorserTx.WithType(gledger.TxTypeDijkstra)
	endorserTx.WithValid(true)
	endorserTx.WithDonation(42)

	rankingTx := mockledger.NewTransactionBuilder()
	rankingTx.WithId(bytes.Repeat([]byte{0x42}, lcommon.Blake2b256Size))
	rankingTx.WithType(gledger.TxTypeDijkstra)
	rankingTx.WithValid(true)
	rankingTx.WithDonation(58)

	point := ocommon.NewPoint(123, bytes.Repeat([]byte{0x43}, 32))
	endorserDelta := NewLedgerDelta(point, uint(dijkstra.EraIdDijkstra), 1)
	defer endorserDelta.Release()
	endorserDelta.addTransaction(endorserTx, 0)

	rankingDelta := NewLedgerDelta(point, uint(dijkstra.EraIdDijkstra), 1)
	defer rankingDelta.Release()
	rankingDelta.addTransaction(rankingTx, 0)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if err := endorserDelta.applyTransactionMetadataOnlyWithoutRecordingDonations(
			ls,
			txn,
		); err != nil {
			return err
		}
		rankingDelta.donate(endorserDelta.donation)
		return rankingDelta.applyTransactionMetadataOnlyWithDonationRecording(
			ls,
			txn,
			true,
		)
	}))

	sum, err := db.Metadata().SumNetworkDonationsForEpoch(9, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(100), sum)
}
