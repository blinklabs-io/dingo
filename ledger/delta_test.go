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
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type commitFailingBlobStore struct {
	blob.BlobStore
	err error
}

func (s commitFailingBlobStore) NewTransaction(readWrite bool) dbtypes.Txn {
	txn := s.BlobStore.NewTransaction(readWrite)
	if !readWrite {
		return txn
	}
	return &commitFailingBlobTxn{Txn: txn, err: s.err}
}

func (s commitFailingBlobStore) SetTx(
	txn dbtypes.Txn,
	txHash []byte,
	offsetData []byte,
) error {
	return s.BlobStore.SetTx(
		unwrapCommitFailingBlobTxn(txn),
		txHash,
		offsetData,
	)
}

func (s commitFailingBlobStore) SetCommitTimestamp(
	timestamp int64,
	txn dbtypes.Txn,
) error {
	return s.BlobStore.SetCommitTimestamp(
		timestamp,
		unwrapCommitFailingBlobTxn(txn),
	)
}

type commitFailingBlobTxn struct {
	dbtypes.Txn
	err error
}

func (t *commitFailingBlobTxn) Commit() error {
	_ = t.Txn.Rollback()
	return t.err
}

func unwrapCommitFailingBlobTxn(txn dbtypes.Txn) dbtypes.Txn {
	if wrapped, ok := txn.(*commitFailingBlobTxn); ok {
		return wrapped.Txn
	}
	return txn
}

func newTransactionEventTestLedger(
	t *testing.T,
) (*LedgerState, *database.Database, <-chan event.Event) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	subID, events := bus.SubscribeWithBuffer(TransactionEventType, 16)
	require.NotEqual(t, event.EventSubscriberId(0), subID)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, subID) })

	return &LedgerState{
		db: db,
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}, db, events
}

func newTransactionEventTestDelta(
	t *testing.T,
	seed byte,
	index int,
) *LedgerDelta {
	t.Helper()

	tx := mockledger.NewTransactionBuilder()
	tx.WithId(bytes.Repeat([]byte{seed}, 32))
	tx.WithType(gledger.TxTypeDijkstra)
	tx.WithValid(true)

	point := ocommon.Point{
		Slot: uint64(seed),
		Hash: bytes.Repeat([]byte{seed + 1}, 32),
	}
	var txHash [32]byte
	copy(txHash[:], tx.Hash().Bytes())
	var blockHash [32]byte
	copy(blockHash[:], point.Hash)

	delta := NewLedgerDelta(
		point,
		uint(dijkstra.EraIdDijkstra),
		uint64(seed),
	)
	delta.Offsets = &database.BlockIngestionResult{
		TxOffsets:   make(map[[32]byte]database.CborOffset),
		UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
	}
	delta.Offsets.TxOffsets[txHash] = database.CborOffset{
		BlockSlot:  point.Slot,
		BlockHash:  blockHash,
		ByteLength: 1,
	}
	delta.addTransaction(tx, index)
	return delta
}

func addTransactionEventTestTransaction(
	t *testing.T,
	delta *LedgerDelta,
	seed byte,
	index int,
) {
	t.Helper()

	tx := mockledger.NewTransactionBuilder()
	tx.WithId(bytes.Repeat([]byte{seed}, 32))
	tx.WithType(gledger.TxTypeDijkstra)
	tx.WithValid(true)
	var txHash [32]byte
	copy(txHash[:], tx.Hash().Bytes())
	var blockHash [32]byte
	copy(blockHash[:], delta.Point.Hash)
	delta.Offsets.TxOffsets[txHash] = database.CborOffset{
		BlockSlot:  delta.Point.Slot,
		BlockHash:  blockHash,
		ByteLength: 1,
	}
	delta.addTransaction(tx, index)
}

func requireTransactionEvent(
	t *testing.T,
	events <-chan event.Event,
	wantIndex uint32,
) TransactionEvent {
	t.Helper()
	evt := testutil.RequireReceive(
		t,
		events,
		testutil.AsyncWait,
		"post-commit transaction event",
	)
	txEvt, ok := evt.Data.(TransactionEvent)
	require.True(t, ok, "unexpected payload %T", evt.Data)
	require.Equal(t, wantIndex, txEvt.TxIndex)
	require.False(t, txEvt.Rollback)
	return txEvt
}

type ledgerDeltaBatchStoreRecorder struct {
	metadata.MetadataStore
	events []string
}

func (s *ledgerDeltaBatchStoreRecorder) SetTransactionBatched(
	tx lcommon.Transaction,
	point ocommon.Point,
	index uint32,
	certDeposits map[int]uint64,
	skipWithdrawalWitness bool,
	acc dbtypes.MetadataBatchAccumulator,
	txn dbtypes.Txn,
	protocolMajor uint64,
) error {
	s.events = append(s.events, "set-transaction")
	return s.MetadataStore.SetTransactionBatched(
		tx, point, index, certDeposits, skipWithdrawalWitness, acc, txn, protocolMajor,
	)
}

func (s *ledgerDeltaBatchStoreRecorder) FlushBatchStakeDeltas(
	acc dbtypes.MetadataBatchAccumulator,
	txn dbtypes.Txn,
) error {
	s.events = append(s.events, "flush-stake-deltas")
	flusher, ok := s.MetadataStore.(interface {
		FlushBatchStakeDeltas(dbtypes.MetadataBatchAccumulator, dbtypes.Txn) error
	})
	if !ok {
		return nil
	}
	return flusher.FlushBatchStakeDeltas(acc, txn)
}

func TestLedgerDeltaBatchFlushesStakeBetweenDeltas(t *testing.T) {
	t.Parallel()
	baseDB, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     "",
		StorageMode: dbtypes.StorageModeAPI,
	})
	require.NoError(t, err)
	store := &ledgerDeltaBatchStoreRecorder{MetadataStore: baseDB.Metadata()}
	db, err := database.New(context.Background(), baseDB.Config(), database.Stores{
		Blob:     baseDB.Blob(),
		Metadata: store,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	first := newTransactionEventTestDelta(t, 31, 0)
	second := newTransactionEventTestDelta(t, 32, 0)
	batch := NewLedgerDeltaBatch()
	batch.addDelta(first)
	batch.addDelta(second)
	t.Cleanup(batch.Release)

	err = db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
		return batch.apply(context.Background(), ls, txn)
	})
	require.NoError(t, err)
	require.Equal(t, []string{
		"set-transaction",
		"flush-stake-deltas",
		"set-transaction",
		"flush-stake-deltas",
	}, store.events)
}

func TestLedgerDeltaBatchAPIStorageFlushesTransactionMetadata(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     "",
		StorageMode: dbtypes.StorageModeAPI,
	})
	require.NoError(t, err)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}

	t.Run("commit persists queued metadata", func(t *testing.T) {
		delta := newTransactionEventTestDelta(t, 21, 0)
		batch := NewLedgerDeltaBatch()
		batch.addDelta(delta)
		t.Cleanup(batch.Release)

		err := db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
			return batch.apply(context.Background(), ls, txn)
		})
		require.NoError(t, err)
		stored, err := db.Metadata().GetTransactionByHash(
			delta.Transactions[0].Tx.Hash().Bytes(), nil,
		)
		require.NoError(t, err)
		require.NotNil(t, stored)
	})

	t.Run("failed batch rolls back earlier metadata", func(t *testing.T) {
		first := newTransactionEventTestDelta(t, 22, 0)
		invalid := newTransactionEventTestDelta(t, 23, -1)
		batch := NewLedgerDeltaBatch()
		batch.addDelta(first)
		batch.addDelta(invalid)
		t.Cleanup(batch.Release)

		err := db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
			return batch.apply(context.Background(), ls, txn)
		})
		require.ErrorContains(t, err, "transaction index out of range")
		stored, err := db.Metadata().GetTransactionByHash(
			first.Transactions[0].Tx.Hash().Bytes(), nil,
		)
		require.NoError(t, err)
		require.Nil(t, stored)
	})
}

func TestLedgerDeltaPublishesApplyEventsOnlyAfterCommit(t *testing.T) {
	t.Parallel()

	t.Run("commit publishes in transaction order", func(t *testing.T) {
		ls, db, events := newTransactionEventTestLedger(t)
		delta := newTransactionEventTestDelta(t, 1, 0)
		defer delta.Release()
		addTransactionEventTestTransaction(t, delta, 2, 1)

		txn := db.Transaction(context.Background(), true)
		require.NoError(t, delta.apply(context.Background(), ls, txn))
		testutil.RequireNoReceive(
			t,
			events,
			100*time.Millisecond,
			"apply event before commit",
		)
		require.NoError(t, txn.Commit())

		firstEvt := requireTransactionEvent(t, events, 0)
		secondEvt := requireTransactionEvent(t, events, 1)
		require.Equal(
			t,
			delta.Transactions[0].Tx.Hash(),
			firstEvt.Transaction.Hash(),
		)
		require.Equal(
			t,
			delta.Transactions[1].Tx.Hash(),
			secondEvt.Transaction.Hash(),
		)
	})

	t.Run("rollback publishes nothing", func(t *testing.T) {
		ls, db, events := newTransactionEventTestLedger(t)
		delta := newTransactionEventTestDelta(t, 3, 0)
		defer delta.Release()

		txn := db.Transaction(context.Background(), true)
		require.NoError(t, delta.apply(context.Background(), ls, txn))
		require.NoError(t, txn.Rollback())
		testutil.RequireNoReceive(
			t,
			events,
			100*time.Millisecond,
			"apply event after rollback",
		)
	})

	t.Run("later delta failure publishes nothing", func(t *testing.T) {
		ls, db, events := newTransactionEventTestLedger(t)
		first := newTransactionEventTestDelta(t, 4, 0)
		second := newTransactionEventTestDelta(t, 5, -1)
		batch := NewLedgerDeltaBatch()
		batch.addDelta(first)
		batch.addDelta(second)
		defer batch.Release()

		err := db.Transaction(context.Background(), true).
			Do(func(txn *database.Txn) error {
				return batch.apply(context.Background(), ls, txn)
			})
		require.ErrorContains(t, err, "transaction index out of range")
		testutil.RequireNoReceive(
			t,
			events,
			100*time.Millisecond,
			"apply event after later delta failure",
		)
	})

	t.Run("commit failure publishes nothing", func(t *testing.T) {
		ls, baseDB, events := newTransactionEventTestLedger(t)
		commitErr := errors.New("injected blob commit failure")
		failingDB, err := database.New(
			context.Background(),
			baseDB.Config(),
			database.Stores{
				Blob: commitFailingBlobStore{
					BlobStore: baseDB.Blob(),
					err:       commitErr,
				},
				Metadata: baseDB.Metadata(),
			},
		)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, failingDB.Close()) })
		ls.db = failingDB
		delta := newTransactionEventTestDelta(t, 6, 0)
		defer delta.Release()

		err = failingDB.Transaction(context.Background(), true).
			Do(func(txn *database.Txn) error {
				return delta.apply(context.Background(), ls, txn)
			})
		require.ErrorIs(t, err, commitErr)
		testutil.RequireNoReceive(
			t,
			events,
			100*time.Millisecond,
			"apply event after commit failure",
		)
	})
}

func TestProcessGovernanceAcceptsDijkstraProtocolParameters(t *testing.T) {
	t.Parallel()

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

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return delta.processGovernance(context.Background(), ls, tx, 0, txn)
	}))

	got, err := db.GetGovernanceProposal(
		context.Background(),
		tx.Hash().Bytes(),
		0,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(12), got.ProposedEpoch)
	require.Equal(t, uint64(32), got.ExpiresEpoch)
}

func TestLedgerDeltaUsesBabbageProtocolMajorForDRepCertificates(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })
	drepOne := bytes.Repeat([]byte{0x31}, lcommon.Blake2b224Size)
	drepTwo := bytes.Repeat([]byte{0x32}, lcommon.Blake2b224Size)
	stakeCredential := bytes.Repeat([]byte{0x33}, lcommon.Blake2b224Size)
	require.NoError(t, db.Metadata().ImportDrep(
		&models.Drep{
			CredentialTag: 0,
			Credential:    drepOne,
			AddedSlot:     1,
			Active:        true,
			Delegators: []models.StakeCredentialRef{{
				Tag: 0,
				Key: stakeCredential,
			}},
		},
		&models.RegistrationDrep{
			CredentialTag:  0,
			DrepCredential: drepOne,
			AddedSlot:      1,
		},
		nil,
	))
	require.NoError(t, db.CreateDrep(context.Background(), nil, &models.Drep{
		CredentialTag: 0,
		Credential:    drepTwo,
		AddedSlot:     1,
		Active:        true,
	}))
	require.NoError(t, db.Metadata().ImportAccount(&models.Account{
		StakingKey:    stakeCredential,
		CredentialTag: 0,
		Drep:          drepOne,
		DrepType:      models.DrepTypeAddrKeyHash,
		AddedSlot:     1,
		CreatedSlot:   1,
		Active:        true,
	}, nil))

	ls := &LedgerState{
		db:             db,
		currentPParams: &babbage.BabbageProtocolParameters{ProtocolMajor: 9},
	}
	ls.publishSnapshotsLocked()
	apply := func(tx lcommon.Transaction, slot uint64, marker byte) {
		t.Helper()
		point := ocommon.Point{
			Slot: slot,
			Hash: bytes.Repeat([]byte{marker}, lcommon.Blake2b256Size),
		}
		var txHash [32]byte
		copy(txHash[:], tx.Hash().Bytes())
		delta := NewLedgerDelta(point, uint(babbage.EraIdBabbage), slot)
		t.Cleanup(delta.Release)
		delta.addTransaction(tx, 0)
		delta.Offsets = &database.BlockIngestionResult{
			TxOffsets:   map[[32]byte]database.CborOffset{txHash: {}},
			UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
		}
		txn := db.Transaction(context.Background(), true)
		defer txn.Release()
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			return delta.apply(context.Background(), ls, txn)
		}))
	}
	delegate := func(id byte, drep []byte) lcommon.Transaction {
		credentialHash := lcommon.NewBlake2b224(stakeCredential)
		tx := mockledger.NewTransactionBuilder().WithCertificates(
			&lcommon.VoteDelegationCertificate{
				CertType:        uint(lcommon.CertificateTypeVoteDelegation),
				StakeCredential: lcommon.Credential{CredType: 0, Credential: credentialHash},
				Drep:            lcommon.Drep{Type: lcommon.DrepTypeAddrKeyHash, Credential: drep},
			},
		)
		tx.WithId(bytes.Repeat([]byte{id}, lcommon.Blake2b256Size))
		tx.WithValid(true)
		return tx
	}
	apply(delegate(0x41, drepTwo), 2, 0x42)
	account, err := db.GetAccountByCredential(
		context.Background(), 0, stakeCredential, true, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, account)
	require.Equal(t, drepTwo, account.Drep,
		"PV9 redelegation updates the account's forward delegation")

	ls.Lock()
	ls.currentPParams = &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{Major: 9},
	}
	ls.Unlock()
	drepHash := lcommon.NewBlake2b224(drepOne)
	unregister := mockledger.NewTransactionBuilder().WithCertificates(
		&lcommon.DeregistrationDrepCertificate{
			CertType:       uint(lcommon.CertificateTypeDeregistrationDrep),
			DrepCredential: lcommon.Credential{CredType: 0, Credential: drepHash},
			Amount:         500,
		},
	)
	unregister.WithId(bytes.Repeat([]byte{0x43}, lcommon.Blake2b256Size))
	unregister.WithValid(true)
	apply(unregister, 3, 0x44)
	account, err = db.GetAccountByCredential(
		context.Background(), 0, stakeCredential, true, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, account)
	require.Nil(t, account.Drep, "PV9 DRep deregistration clears the stale reverse delegation")
}

func TestLedgerDeltaResetsDormancyBeforeDRepRegistration(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	drepCredential := bytes.Repeat([]byte{0x51}, lcommon.Blake2b224Size)
	var credentialHash lcommon.CredentialHash
	copy(credentialHash[:], drepCredential)
	require.NoError(t, db.SetImportedDormantDRepEpochs(context.Background(), 3, nil))

	pparams := mockledger.NewMockConwayProtocolParams()
	pparams.ProtocolVersion.Major = 9
	pparams.DRepDeposit = 500
	pparams.DRepInactivityPeriod = 20
	pparams.GovActionValidityPeriod = 20
	ls := &LedgerState{
		db: db,
		currentEpoch: models.Epoch{
			EpochId: 100,
		},
		currentPParams: &pparams,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	rewardHash := bytes.Repeat([]byte{0x52}, lcommon.Blake2b224Size)
	rewardAddress, err := lcommon.NewAddressFromBytes(
		append([]byte{0xE1}, rewardHash...),
	)
	require.NoError(t, err)
	var anchorHash [32]byte
	copy(anchorHash[:], bytes.Repeat([]byte{0x53}, lcommon.Blake2b256Size))
	proposal := conway.ConwayProposalProcedure{
		PPDeposit:       1,
		PPRewardAccount: rewardAddress,
		PPGovAction: conway.ConwayGovAction{
			Type:   uint(lcommon.GovActionTypeInfo),
			Action: &lcommon.InfoGovAction{Type: uint(lcommon.GovActionTypeInfo)},
		},
		PPAnchor: lcommon.GovAnchor{Url: "https://example.com/proposal", DataHash: anchorHash},
	}
	tx := mockledger.NewTransactionBuilder()
	tx.WithId(bytes.Repeat([]byte{0x54}, lcommon.Blake2b256Size))
	tx.WithValid(true)
	tx.WithCertificates(&lcommon.RegistrationDrepCertificate{
		CertType: uint(lcommon.CertificateTypeRegistrationDrep),
		DrepCredential: lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: credentialHash,
		},
		Amount: 500,
	})
	tx.WithProposalProcedures(proposal)
	txHash := tx.Hash()
	var txHashArray [32]byte
	copy(txHashArray[:], txHash.Bytes())
	point := ocommon.Point{
		Slot: 100,
		Hash: bytes.Repeat([]byte{0x55}, lcommon.Blake2b256Size),
	}
	delta := NewLedgerDelta(point, uint(conway.EraIdConway), point.Slot)
	defer delta.Release()
	delta.addTransaction(tx, 0)
	delta.Offsets = &database.BlockIngestionResult{
		TxOffsets:   map[[32]byte]database.CborOffset{txHashArray: {}},
		UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
	}

	txn := db.Transaction(context.Background(), true)
	defer txn.Release()
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return delta.apply(context.Background(), ls, txn)
	}))
	drep, err := db.GetDrepByCredential(
		context.Background(), 0, drepCredential, true, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, drep)
	require.Equal(t, uint64(100), drep.LastActivityEpoch)
	require.Equal(t, uint64(120), drep.ExpiryEpoch)
	storedProposal, err := db.GetGovernanceProposal(
		context.Background(), tx.Hash().Bytes(), 0, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, storedProposal)
	require.Equal(t, uint64(100), storedProposal.ProposedEpoch)
	require.Equal(t, uint64(120), storedProposal.ExpiresEpoch)
	dormantEpochs, err := db.GetDormantDRepEpochs(context.Background(), nil)
	require.NoError(t, err)
	require.Zero(t, dormantEpochs)
}

func TestLedgerDeltaAppliesDRepVoteActivityBeforeRegistrationCertificates(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })
	require.NoError(t, db.SetImportedDormantDRepEpochs(context.Background(), 3, nil))

	drepCredential := bytes.Repeat([]byte{0x61}, lcommon.Blake2b224Size)
	proposalHash := bytes.Repeat([]byte{0x62}, lcommon.Blake2b256Size)
	require.NoError(t, db.SetGovernanceProposal(
		context.Background(),
		&models.GovernanceProposal{
			TxHash:        proposalHash,
			ActionIndex:   0,
			ActionType:    uint8(lcommon.GovActionTypeInfo),
			ProposedEpoch: 99,
			ExpiresEpoch:  120,
			AnchorURL:     "https://example.com/activity-order",
			AnchorHash:    bytes.Repeat([]byte{0x63}, lcommon.Blake2b256Size),
			Deposit:       1,
			ReturnAddress: append(
				[]byte{0xE1},
				bytes.Repeat([]byte{0x64}, lcommon.Blake2b224Size)...,
			),
			AddedSlot: 90,
		},
		nil,
	))

	pparams := mockledger.NewMockConwayProtocolParams()
	pparams.ProtocolVersion.Major = 9
	pparams.DRepDeposit = 500
	pparams.DRepInactivityPeriod = 20
	ls := &LedgerState{
		db: db,
		currentEpoch: models.Epoch{
			EpochId: 100,
		},
		currentPParams: &pparams,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	var credentialHash lcommon.CredentialHash
	copy(credentialHash[:], drepCredential)
	var actionTxHash [lcommon.Blake2b256Size]byte
	copy(actionTxHash[:], proposalHash)
	tx := mockledger.NewTransactionBuilder()
	tx.WithId(bytes.Repeat([]byte{0x65}, lcommon.Blake2b256Size))
	tx.WithValid(true)
	tx.WithCertificates(&lcommon.RegistrationDrepCertificate{
		CertType: uint(lcommon.CertificateTypeRegistrationDrep),
		DrepCredential: lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: credentialHash,
		},
		Amount: 500,
	})
	var voterHash [lcommon.Blake2b224Size]byte
	copy(voterHash[:], drepCredential)
	tx.WithVotingProcedures(lcommon.VotingProcedures{
		&lcommon.Voter{
			Type: lcommon.VoterTypeDRepKeyHash,
			Hash: voterHash,
		}: {
			&lcommon.GovActionId{
				TransactionId: actionTxHash,
				GovActionIdx:  0,
			}: {Vote: models.VoteYes},
		},
	})
	txHash := tx.Hash()
	var txHashArray [lcommon.Blake2b256Size]byte
	copy(txHashArray[:], txHash.Bytes())
	point := ocommon.Point{
		Slot: 100,
		Hash: bytes.Repeat([]byte{0x66}, lcommon.Blake2b256Size),
	}
	delta := NewLedgerDelta(point, uint(conway.EraIdConway), point.Slot)
	defer delta.Release()
	delta.addTransaction(tx, 0)
	delta.Offsets = &database.BlockIngestionResult{
		TxOffsets:   map[[32]byte]database.CborOffset{txHashArray: {}},
		UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
	}

	txn := db.Transaction(context.Background(), true)
	defer txn.Release()
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return delta.apply(context.Background(), ls, txn)
	}))

	drep, err := db.GetDrepByCredential(
		context.Background(), 0, drepCredential, true, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, drep)
	require.Equal(t, uint64(100), drep.LastActivityEpoch)
	require.Equal(t, uint64(123), drep.ExpiryEpoch,
		"PV9 registration must include the imported dormant epochs after same-transaction vote activity")
	proposal, err := db.GetGovernanceProposal(
		context.Background(), proposalHash, 0, nil,
	)
	require.NoError(t, err)
	votes, err := db.GetGovernanceVotes(context.Background(), proposal.ID, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1)
	require.Equal(t, uint8(models.VoteYes), votes[0].Vote)
	dormantEpochs, err := db.GetDormantDRepEpochs(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3), dormantEpochs)
}

func TestProcessGovernanceAllowsDRepDeregistrationWithoutConwayParameters(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	tx := mockledger.NewTransactionBuilder()
	tx.WithId(bytes.Repeat([]byte{0x31}, lcommon.Blake2b256Size))
	tx.WithCertificates(&lcommon.DeregistrationDrepCertificate{
		CertType: uint(lcommon.CertificateTypeDeregistrationDrep),
		DrepCredential: lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.CredentialHash(bytes.Repeat([]byte{0x32}, lcommon.Blake2b224Size)),
		},
	})
	ls := &LedgerState{
		db:           db,
		currentEpoch: models.Epoch{EpochId: 10},
	}
	delta := NewLedgerDelta(
		ocommon.NewPoint(100, bytes.Repeat([]byte{0x33}, lcommon.Blake2b256Size)),
		uint(conway.EraIdConway),
		0,
	)
	defer delta.Release()

	err = db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
		return delta.processGovernance(context.Background(), ls, tx, 0, txn)
	})
	require.NoError(t, err)
}

func TestLedgerDeltaPersistsMultipleCertificateDepositsFromOneSnapshot(
	t *testing.T,
) {
	t.Parallel()

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
			runtime.Gosched()
		}
	})
	defer func() {
		close(stopPublisher)
		publisherWG.Wait()
	}()

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return delta.apply(context.Background(), ls, txn)
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
	t.Parallel()

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
	require.NoError(t, db.CreateDrep(context.Background(), nil, &models.Drep{
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

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return (&LedgerDelta{}).processGovernance(
			context.Background(),
			ls,
			tx,
			0,
			txn,
		)
	}))

	drep, err := db.GetDrepByCredential(
		context.Background(),
		0,
		credentialBytes,
		true,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(100), drep.LastActivityEpoch)
	require.Equal(t, uint64(120), drep.ExpiryEpoch)
}

func TestProcessGovernanceClearsDRepVotesAfterVotes(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	credentialBytes := bytes.Repeat([]byte{0x71}, 28)
	var credentialHash lcommon.CredentialHash
	copy(credentialHash[:], credentialBytes)
	require.NoError(t, db.CreateDrep(context.Background(), nil, &models.Drep{
		CredentialTag: 0,
		Credential:    credentialBytes,
		AddedSlot:     1,
		Active:        true,
	}))
	require.NoError(t, db.CreateDrep(context.Background(), nil, &models.Drep{
		CredentialTag: 1,
		Credential:    credentialBytes,
		AddedSlot:     1,
		Active:        true,
	}))
	proposalHash := bytes.Repeat([]byte{0x73}, 32)
	proposal := &models.GovernanceProposal{
		TxHash:       proposalHash,
		ExpiresEpoch: 200,
		AddedSlot:    3,
	}
	require.NoError(t, db.SetGovernanceProposal(context.Background(), proposal, nil))
	var actionHash [32]byte
	copy(actionHash[:], proposalHash)
	keyVoter := &lcommon.Voter{
		Type: lcommon.VoterTypeDRepKeyHash,
		Hash: credentialHash,
	}
	actionID := &lcommon.GovActionId{TransactionId: actionHash, GovActionIdx: 0}
	updatedSlot := uint64(10)
	require.NoError(t, db.SetGovernanceVote(context.Background(), &models.GovernanceVote{
		ProposalID:         proposal.ID,
		VoterType:          models.VoterTypeDRep,
		VoterCredentialTag: 0,
		VoterCredential:    credentialBytes,
		Vote:               models.VoteYes,
		AddedSlot:          updatedSlot,
		VoteUpdatedSlot:    &updatedSlot,
	}, nil))
	require.NoError(t, db.SetGovernanceVote(context.Background(), &models.GovernanceVote{
		ProposalID:         proposal.ID,
		VoterType:          models.VoterTypeDRep,
		VoterCredentialTag: 1,
		VoterCredential:    credentialBytes,
		Vote:               models.VoteYes,
		AddedSlot:          updatedSlot,
		VoteUpdatedSlot:    &updatedSlot,
	}, nil))

	tx := mockledger.NewTransactionBuilder().
		WithVotingProcedures(lcommon.VotingProcedures{
			keyVoter: {
				actionID: {Vote: models.VoteYes},
			},
		}).
		WithCertificates(&lcommon.DeregistrationDrepCertificate{
			CertType: uint(lcommon.CertificateTypeDeregistrationDrep),
			DrepCredential: lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: credentialHash,
			},
		})
	pparams := mockledger.NewMockConwayProtocolParams()
	ls := &LedgerState{
		db:             db,
		currentEpoch:   models.Epoch{EpochId: 10},
		currentPParams: &pparams,
	}
	point := ocommon.Point{Slot: 30, Hash: bytes.Repeat([]byte{0x74}, 32)}
	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if err := db.Metadata().DeactivateDreps(txn.Metadata(), []models.StakeCredentialRef{
			models.NewStakeCredentialRef(0, credentialBytes),
		}); err != nil {
			return err
		}
		return (&LedgerDelta{Point: point}).processGovernance(
			context.Background(), ls, tx, 0, txn,
		)
	}))

	votes, err := db.GetGovernanceVotes(context.Background(), proposal.ID, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1)
	require.Equal(t, uint8(1), votes[0].VoterCredentialTag,
		"the same hash under a script credential remains distinct")
}

func TestProcessGovernanceClearsDRepVotesForDeregistrationOnlyTransaction(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	credentialBytes := bytes.Repeat([]byte{0x75}, lcommon.Blake2b224Size)
	var credentialHash lcommon.CredentialHash
	copy(credentialHash[:], credentialBytes)
	require.NoError(t, db.CreateDrep(context.Background(), nil, &models.Drep{
		CredentialTag: 0,
		Credential:    credentialBytes,
		AddedSlot:     1,
		Active:        true,
	}))
	proposalHash := bytes.Repeat([]byte{0x76}, lcommon.Blake2b256Size)
	proposal := &models.GovernanceProposal{
		TxHash:       proposalHash,
		ExpiresEpoch: 200,
		AddedSlot:    3,
	}
	require.NoError(t, db.SetGovernanceProposal(context.Background(), proposal, nil))
	updatedSlot := uint64(10)
	require.NoError(t, db.SetGovernanceVote(context.Background(), &models.GovernanceVote{
		ProposalID:         proposal.ID,
		VoterType:          models.VoterTypeDRep,
		VoterCredentialTag: 0,
		VoterCredential:    credentialBytes,
		Vote:               models.VoteYes,
		AddedSlot:          updatedSlot,
		VoteUpdatedSlot:    &updatedSlot,
	}, nil))

	tx := mockledger.NewTransactionBuilder()
	tx.WithCertificates(&lcommon.DeregistrationDrepCertificate{
		CertType: uint(lcommon.CertificateTypeDeregistrationDrep),
		DrepCredential: lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: credentialHash,
		},
	})
	ls := &LedgerState{db: db, currentEpoch: models.Epoch{EpochId: 10}}
	point := ocommon.Point{
		Slot: 30,
		Hash: bytes.Repeat([]byte{0x77}, lcommon.Blake2b256Size),
	}
	txn := db.Transaction(context.Background(), true)
	defer txn.Release()
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return (&LedgerDelta{Point: point}).processGovernance(
			context.Background(), ls, tx, 0, txn,
		)
	}))

	votes, err := db.GetGovernanceVotes(context.Background(), proposal.ID, nil)
	require.NoError(t, err)
	require.Empty(t, votes)
}

func TestLedgerDeltaDRepDeregistrationPreservesLaterDelegation(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	drepCredential := bytes.Repeat([]byte{0x51}, lcommon.Blake2b224Size)
	stakeCredential := bytes.Repeat([]byte{0x52}, lcommon.Blake2b224Size)
	require.NoError(t, db.CreateDrep(context.Background(), nil, &models.Drep{
		CredentialTag: 0,
		Credential:    drepCredential,
		AddedSlot:     10,
		Active:        true,
	}))
	require.NoError(t, db.Metadata().ImportAccount(&models.Account{
		StakingKey:    stakeCredential,
		CredentialTag: 0,
		AddedSlot:     10,
		CreatedSlot:   10,
		Active:        true,
	}, nil))

	drepHash := lcommon.NewBlake2b224(drepCredential)
	stakeHash := lcommon.NewBlake2b224(stakeCredential)
	tx := mockledger.NewTransactionBuilder().WithCertificates(
		&lcommon.DeregistrationDrepCertificate{
			CertType:       uint(lcommon.CertificateTypeDeregistrationDrep),
			DrepCredential: lcommon.Credential{CredType: lcommon.CredentialTypeAddrKeyHash, Credential: drepHash},
			Amount:         500,
		},
		&lcommon.RegistrationDrepCertificate{
			CertType:       uint(lcommon.CertificateTypeRegistrationDrep),
			DrepCredential: lcommon.Credential{CredType: lcommon.CredentialTypeAddrKeyHash, Credential: drepHash},
			Amount:         500,
		},
		&lcommon.VoteDelegationCertificate{
			CertType:        uint(lcommon.CertificateTypeVoteDelegation),
			StakeCredential: lcommon.Credential{CredType: lcommon.CredentialTypeAddrKeyHash, Credential: stakeHash},
			Drep:            lcommon.Drep{Type: lcommon.DrepTypeAddrKeyHash, Credential: drepCredential},
		},
	)
	tx.WithId(bytes.Repeat([]byte{0x53}, 32))
	tx.WithValid(true)
	txHash := tx.Hash().Bytes()
	var txHashArray [32]byte
	copy(txHashArray[:], txHash)

	pparams := mockledger.NewMockConwayProtocolParams()
	pparams.DRepDeposit = 500
	ls := &LedgerState{
		db:             db,
		currentEpoch:   models.Epoch{EpochId: 10},
		currentPParams: &pparams,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()
	point := ocommon.Point{Slot: 20, Hash: bytes.Repeat([]byte{0x54}, lcommon.Blake2b256Size)}
	delta := NewLedgerDelta(point, uint(conway.EraIdConway), 1)
	defer delta.Release()
	delta.addTransaction(tx, 0)
	delta.Offsets = &database.BlockIngestionResult{
		TxOffsets:   map[[32]byte]database.CborOffset{txHashArray: {}},
		UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
	}

	txn := db.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return delta.apply(context.Background(), ls, txn)
	}))

	account, err := db.GetAccountByCredential(
		context.Background(), 0, stakeCredential, true, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, account)
	require.Equal(t, drepCredential, account.Drep)
	activeDrep, err := db.GetDrepByCredential(
		context.Background(), 0, drepCredential, true, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, activeDrep)
}

func TestConwayProtocolParametersDijkstra(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	var pparams *dijkstra.DijkstraProtocolParameters

	require.Nil(t, conwayProtocolParameters(pparams))
}

func TestConwayProtocolParametersTypedNil(t *testing.T) {
	t.Parallel()

	var conwayPParams *conway.ConwayProtocolParameters
	var dijkstraPParams *dijkstra.DijkstraProtocolParameters

	require.Nil(t, conwayProtocolParameters(conwayPParams))
	require.Nil(t, conwayProtocolParameters(dijkstraPParams))
}

func TestProcessGovernanceTypedNilPParams(t *testing.T) {
	t.Parallel()

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
				err = (&LedgerDelta{}).processGovernance(
					context.Background(),
					ls,
					tx,
					0,
					nil,
				)
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

// Network-donation aggregation is covered by state_test.go. The
// former metadata-only endorser apply path (and its two dedicated tests here)
// was removed when the Musashi endorser-block apply switched to the full
// ValidateNone effect apply (see ledger/leios_apply.go).

// TestAddUint64Overflow exercises addUint64 at the exact uint64 max
// boundary: maxUint64-1 plus 1 is the largest sum that fits, plus 2
// overflows.
func TestAddUint64Overflow(t *testing.T) {
	t.Parallel()

	maxUint64 := ^uint64(0)

	sum, err := addUint64(maxUint64-1, 1)
	require.NoError(t, err)
	assert.Equal(t, maxUint64, sum)

	_, err = addUint64(maxUint64-1, 2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflows uint64")
}

// TestLedgerDeltaDonateOverflow exercises LedgerDelta.donate at the exact
// uint64 max boundary for d.donation.
func TestLedgerDeltaDonateOverflow(t *testing.T) {
	t.Parallel()

	maxUint64 := ^uint64(0)

	d := &LedgerDelta{donation: maxUint64 - 1}
	require.NoError(t, d.donate(1))
	assert.Equal(t, maxUint64, d.donation)

	d2 := &LedgerDelta{donation: maxUint64 - 1}
	err := d2.donate(2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflows uint64")
	assert.Equal(
		t, maxUint64-1, d2.donation,
		"donation left unchanged on overflow",
	)
}

// conwayDonationTx builds a valid Conway transaction whose body carries the
// given treasury donation, for feeding into LedgerDelta donation aggregation.
func conwayDonationTx(donation uint64) *conway.ConwayTransaction {
	return &conway.ConwayTransaction{
		Body:      conway.ConwayTransactionBody{TxDonation: donation},
		TxIsValid: true,
	}
}

// TestLedgerDeltaAccumulateNetworkDonationsOverflow drives the per-tx
// donation summation in accumulateNetworkDonations to the exact uint64 max
// boundary using two real Conway transactions.
func TestLedgerDeltaAccumulateNetworkDonationsOverflow(t *testing.T) {
	t.Parallel()

	maxUint64 := ^uint64(0)

	newDelta := func(donationB uint64) *LedgerDelta {
		return &LedgerDelta{
			Transactions: []TransactionRecord{
				{Tx: conwayDonationTx(maxUint64 - 1), Index: 0},
				{Tx: conwayDonationTx(donationB), Index: 1},
			},
		}
	}

	t.Run("just below overflow succeeds", func(t *testing.T) {
		d := newDelta(1)
		require.NoError(t, d.accumulateNetworkDonations(nil))
		assert.Equal(t, maxUint64, d.donation)
	})

	t.Run("just above overflow fails", func(t *testing.T) {
		d := newDelta(2)
		err := d.accumulateNetworkDonations(nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "overflows uint64")
	})
}

// TestLedgerDeltaRecordNetworkDonationsOverflowPreservesState verifies that
// a donation-sum overflow aborts before any database write: no
// network_donation row is recorded and the network state is untouched.
func TestLedgerDeltaRecordNetworkDonationsOverflowPreservesState(t *testing.T) {
	t.Parallel()

	db := newDonationTestDB(t)
	ls := &LedgerState{db: db}
	maxUint64 := ^uint64(0)

	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))

	delta := &LedgerDelta{
		Transactions: []TransactionRecord{
			{Tx: conwayDonationTx(maxUint64 - 1), Index: 0},
			{Tx: conwayDonationTx(2), Index: 1},
		},
	}

	txn := db.Transaction(context.Background(), true)
	err := txn.Do(func(txn *database.Txn) error {
		return delta.recordNetworkDonations(ls, txn, nil)
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflows uint64")

	total, err := db.Metadata().SumNetworkDonationsForEpoch(0, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), total, "no donation row recorded on overflow")

	treasury, reserves, slot := networkState(t, db)
	assert.Equal(t, uint64(1_000), treasury, "treasury untouched on overflow")
	assert.Equal(t, uint64(5_000), reserves, "reserves untouched on overflow")
	assert.Equal(
		t,
		uint64(50),
		slot,
		"network state slot untouched on overflow",
	)
}
