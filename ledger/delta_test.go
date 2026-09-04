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
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func TestProcessGovernanceAcceptsDijkstraProtocolParameters(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

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

func TestLedgerDeltaPersistsMultipleCertificateDepositsFromOneSnapshot(
	t *testing.T,
) {
	const (
		keyDeposit = uint64(2_000_000)
		certCount  = 64
	)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	ls := &LedgerState{
		db: db,
		currentPParams: &shelley.ShelleyProtocolParameters{
			KeyDeposit: uint(keyDeposit),
		},
	}
	ls.publishSnapshotsLocked()
	secondPParams := &shelley.ShelleyProtocolParameters{KeyDeposit: 4_000_000}

	certs := make([]lcommon.Certificate, certCount)
	for i := range certs {
		credential := bytes.Repeat([]byte{0}, lcommon.Blake2b224Size)
		credential[0] = byte(i)
		certs[i] = &lcommon.StakeRegistrationCertificate{
			CertType: uint(lcommon.CertificateTypeStakeRegistration),
			StakeCredential: lcommon.Credential{
				CredType: lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.CredentialHash(
					lcommon.NewBlake2b224(credential),
				),
			},
		}
	}
	txBuilder := mockledger.NewTransactionBuilder()
	txBuilder.WithId(bytes.Repeat([]byte{0x71}, lcommon.Blake2b256Size))
	txBuilder.WithCertificates(certs...)
	var tx lcommon.Transaction = txBuilder
	txHash := tx.Hash()
	var txHashArray [32]byte
	copy(txHashArray[:], txHash.Bytes())
	point := ocommon.Point{
		Slot: 42,
		Hash: bytes.Repeat([]byte{0x72}, lcommon.Blake2b256Size),
	}
	delta := NewLedgerDelta(point, uint(shelley.EraIdShelley), 1)
	defer delta.Release()
	delta.addTransaction(tx, 0)
	delta.Offsets = &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			txHashArray: {},
		},
		UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
	}
	stopPublisher := make(chan struct{})
	var publisherWG sync.WaitGroup
	publisherWG.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stopPublisher:
				return
			default:
			}
			ls.Lock()
			if i%2 == 0 {
				ls.currentPParams = secondPParams
			} else {
				ls.currentPParams = &shelley.ShelleyProtocolParameters{
					KeyDeposit: uint(keyDeposit),
				}
			}
			ls.publishSnapshotsLocked()
			ls.Unlock()
		}
	})
	defer func() {
		close(stopPublisher)
		publisherWG.Wait()
	}()

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return delta.apply(ls, txn)
	}))

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	rows, err := raw.Query(`
SELECT hex(c.block_hash), sr.deposit_amount
FROM stake_registration sr
JOIN certs c ON c.id = sr.certificate_id
ORDER BY c.cert_index`)
	require.NoError(t, err)
	defer rows.Close()
	var deposits []string
	for rows.Next() {
		var blockHash string
		var deposit string
		require.NoError(t, rows.Scan(&blockHash, &deposit))
		require.Equal(t, fmt.Sprintf("%X", point.Hash), blockHash)
		deposits = append(deposits, deposit)
	}
	require.NoError(t, rows.Err())
	require.Len(t, deposits, certCount)
	firstDeposit := deposits[0]
	require.Contains(t, []string{"2000000", "4000000"}, firstDeposit)
	for i, deposit := range deposits {
		require.Equal(t, firstDeposit, deposit, "certificate %d", i)
	}
}

func TestProcessGovernanceRenewsDRepFromCertificateOnly(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	pparams := mockledger.NewMockConwayProtocolParams()
	pparams.DRepInactivityPeriod = 20
	ls := &LedgerState{
		db: db,
		currentEpoch: models.Epoch{
			EpochId: 100,
		},
		currentPParams: &pparams,
	}

	credentialBytes := bytes.Repeat([]byte{0xAB}, 28)
	var credentialHash lcommon.CredentialHash
	copy(credentialHash[:], credentialBytes)
	require.NoError(t, db.CreateDrep(nil, &models.Drep{
		CredentialTag:     0,
		Credential:        credentialBytes,
		AddedSlot:         10,
		LastActivityEpoch: 5,
		ExpiryEpoch:       25,
		Active:            true,
	}))

	tx := mockledger.NewTransactionBuilder()
	tx.WithCertificates(&lcommon.RegistrationDrepCertificate{
		CertType: uint(lcommon.CertificateTypeRegistrationDrep),
		DrepCredential: lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: credentialHash,
		},
	})
	tx.WithValid(true)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return (&LedgerDelta{}).processGovernance(ls, tx, txn)
	}))

	drep, err := db.GetDrepByCredential(0, credentialBytes, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(100), drep.LastActivityEpoch)
	require.Equal(t, uint64(120), drep.ExpiryEpoch)
}

func TestConwayProtocolParametersDijkstra(t *testing.T) {
	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			GovActionValidityPeriod: 42,
			DRepInactivityPeriod:    99,
		},
	}

	got := conwayProtocolParameters(pparams)
	require.Same(t, &pparams.ConwayProtocolParameters, got)
	require.NotNil(t, got)
	require.Equal(t, uint64(42), got.GovActionValidityPeriod)
	require.Equal(t, uint64(99), got.DRepInactivityPeriod)
}

func TestConwayProtocolParametersNilDijkstra(t *testing.T) {
	var pparams *dijkstra.DijkstraProtocolParameters

	require.Nil(t, conwayProtocolParameters(pparams))
}

func TestConwayProtocolParametersTypedNil(t *testing.T) {
	var conwayPParams *conway.ConwayProtocolParameters
	var dijkstraPParams *dijkstra.DijkstraProtocolParameters

	require.Nil(t, conwayProtocolParameters(conwayPParams))
	require.Nil(t, conwayProtocolParameters(dijkstraPParams))
}

func TestProcessGovernanceTypedNilPParams(t *testing.T) {
	tests := []struct {
		name    string
		pparams lcommon.ProtocolParameters
	}{
		{
			name:    "conway",
			pparams: (*conway.ConwayProtocolParameters)(nil),
		},
		{
			name:    "dijkstra",
			pparams: (*dijkstra.DijkstraProtocolParameters)(nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ls := &LedgerState{
				currentPParams: tt.pparams,
			}
			tx := mockledger.NewTransactionBuilder().
				WithProposalProcedures(nil)

			var err error
			require.NotPanics(t, func() {
				err = (&LedgerDelta{}).processGovernance(ls, tx, nil)
			})
			require.Error(t, err)
			require.Contains(
				t,
				err.Error(),
				"governance requires Conway protocol parameters",
			)
		})
	}
}

// Network-donation aggregation is covered by network_donation_test.go. The
// former metadata-only endorser apply path (and its two dedicated tests here)
// was removed when the Musashi endorser-block apply switched to the full
// ValidateNone effect apply (see ledger/leios_apply.go).
