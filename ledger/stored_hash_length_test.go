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
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// storedHashStore returns malformed rows from selected metadata reads and
// delegates everything else to the real store. Each field left nil keeps the
// real method.
type storedHashStore struct {
	metadata.MetadataStore
	epoch             *models.Epoch
	rebuiltInputs     []*models.RewardStakeInput
	rebuiltErr        error
	registrationsSeen [][]lcommon.PoolKeyHash
}

func (s *storedHashStore) GetEpoch(
	epoch uint64,
	txn types.Txn,
) (*models.Epoch, error) {
	if s.epoch == nil {
		return s.MetadataStore.GetEpoch(epoch, txn)
	}
	return s.epoch, nil
}

func (s *storedHashStore) GetEpochBoundaryRewardStakeInputsForPools(
	poolKeyHashes [][]byte,
	snapshotSlot uint64,
	boundarySlot uint64,
	incomingEraID *uint,
	expiryEpoch uint64,
	inactivityPeriod uint64,
	txn types.Txn,
) ([]*models.RewardStakeInput, error) {
	if s.rebuiltErr != nil {
		return nil, s.rebuiltErr
	}
	if s.rebuiltInputs == nil {
		return s.MetadataStore.GetEpochBoundaryRewardStakeInputsForPools(
			poolKeyHashes,
			snapshotSlot,
			boundarySlot,
			incomingEraID,
			expiryEpoch,
			inactivityPeriod,
			txn,
		)
	}
	return s.rebuiltInputs, nil
}

func (s *storedHashStore) GetPoolRegistrationsEffectiveForEpoch(
	poolKeyHashes []lcommon.PoolKeyHash,
	epochStartSlot uint64,
	endedEpoch uint64,
	snapshotSlot uint64,
	txn types.Txn,
) ([]models.PoolRegistration, error) {
	s.registrationsSeen = append(s.registrationsSeen, poolKeyHashes)
	return nil, nil
}

func newStoredHashDB(
	t *testing.T,
	configure func(*storedHashStore),
) (*database.Database, *storedHashStore) {
	t.Helper()
	store := &storedHashStore{}
	// Rows a release that kept ledger state in SQL left behind are read only
	// from a v0.94.2 database.
	db, err := dbtest.NewDatabaseWithMetadataWrapper(
		t,
		dbtest.Options{Config: &database.Config{
			DataDir: "", Role: database.RoleReleased,
		}},
		func(inner metadata.MetadataStore) metadata.MetadataStore {
			store.MetadataStore = inner
			configure(store)
			return store
		},
	)
	require.NoError(t, err)
	return db, store
}

func storedHashView(t *testing.T, db *database.Database) *LedgerView {
	t.Helper()
	txn := db.Transaction(context.Background(), false)
	t.Cleanup(txn.Release)
	return &LedgerView{ls: &LedgerState{db: db}, txn: txn}
}

// storedHashSetView is a ledger view over a LedgerDB set holding changes, for
// a row the set stores as given and its reader must reject.
func storedHashSetView(t *testing.T, changes LedgerChanges) *LedgerView {
	t.Helper()
	db := newTestDB(t)
	ls := &LedgerState{db: db}
	fxAttachLedgerDB(t, ls)
	fxSeed(t, db, changes)
	txn := db.Transaction(context.Background(), false)
	t.Cleanup(txn.Release)
	return &LedgerView{ls: ls, txn: txn}
}

// shortStoredHash is one byte narrower than a hash of size bytes. An unchecked
// conversion zero-pads it into a well-formed hash.
func shortStoredHash(size int, fill byte) []byte {
	return bytes.Repeat([]byte{fill}, size-1)
}

func TestMIRPendingRejectsMalformedStoredCredential(t *testing.T) {
	t.Parallel()
	ls, db, _ := newMIRTestLedger(t)
	withMIRCutoffEpoch(
		t,
		ls,
		models.Epoch{StartSlot: 100, LengthInSlots: 432_000},
	)
	seedMIRDistribution(t, db, mirPotReserves, 150,
		[]models.MoveInstantaneousRewardsReward{{
			Credential: shortStoredHash(lcommon.Blake2b224Size, 0x77),
			Amount:     big.NewInt(10),
		}})

	txn := db.Transaction(context.Background(), false)
	err := txn.Do(func(txn *database.Txn) error {
		lv := &LedgerView{ls: ls, txn: txn, epochStartSlot: 100}
		_, err := lv.InstantaneousRewardsTotal(mirPotReserves)
		return err
	})
	require.ErrorContains(t, err, "MIR reward credential")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
}

func TestIsVrfKeyInUseRejectsMalformedStoredPoolKey(t *testing.T) {
	t.Parallel()
	short := shortStoredHash(lcommon.Blake2b224Size, 0x21)
	vrf := lcommon.Blake2b256{0x01}
	reg := &models.PoolRegistration{
		PoolKeyHash: short,
		VrfKeyHash:  vrf[:],
		AddedSlot:   1,
	}
	lv := storedHashSetView(t, LedgerChanges{
		Pools: []PoolChange{{
			Slot:  1,
			State: PoolState{Pool: short, Params: reg},
		}},
		PoolEvents: []PoolEventChange{{
			Pool:  short,
			Event: PoolEvent{Slot: 1, Registration: reg},
		}},
	})
	lv.epochStartSlot = 100
	inUse, owner, err := lv.IsVrfKeyInUse(vrf)
	require.ErrorContains(t, err, "VRF key owner pool key hash")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.ErrorIs(t, storageFaultOrErr(lv, err), ErrLedgerViewStorageFault)
	require.False(t, inUse)
	require.Equal(t, lcommon.PoolKeyHash{}, owner)
}

func TestCommitteeMembersRejectsMalformedStoredColdCredential(t *testing.T) {
	t.Parallel()
	lv := storedHashSetView(t, LedgerChanges{Committee: []CommitteeChange{{
		Slot: 1,
		Member: &models.CommitteeMember{
			ColdCredHash: shortStoredHash(lcommon.Blake2b224Size, 0x31),
			ExpiresEpoch: 10,
		},
	}}})
	members, err := lv.CommitteeMembers()
	require.ErrorContains(t, err, "committee cold credential")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.Nil(t, members)
}

// A malformed hash seated under both credential tags is omitted from the list
// as ambiguous, so it must be validated before that filter or it is dropped
// silently.
func TestCommitteeMembersRejectsMalformedColdCredentialUnderBothTags(
	t *testing.T,
) {
	t.Parallel()
	hash := shortStoredHash(lcommon.Blake2b224Size, 0x32)
	lv := storedHashSetView(t, LedgerChanges{Committee: []CommitteeChange{
		{Slot: 1, Member: &models.CommitteeMember{
			ColdCredHash: hash, ExpiresEpoch: 10,
		}},
		{Slot: 1, Member: &models.CommitteeMember{
			ColdCredentialTag: 1, ColdCredHash: hash, ExpiresEpoch: 10,
		}},
	}})
	members, err := lv.CommitteeMembers()
	require.ErrorContains(t, err, "committee cold credential")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.Nil(t, members)
}

func TestCommitteeHotCredentialRejectsMalformedStoredHash(t *testing.T) {
	t.Parallel()
	cold := bytes.Repeat([]byte{0x41}, lcommon.Blake2b224Size)
	changes := LedgerChanges{Committee: []CommitteeChange{
		{Slot: 1, Member: &models.CommitteeMember{
			ColdCredHash: cold, ExpiresEpoch: 10,
		}},
		{Slot: 1, Auth: &models.AuthCommitteeHot{
			ColdCredential: cold,
			HotCredential:  shortStoredHash(lcommon.Blake2b224Size, 0x42),
			AddedSlot:      1,
		}},
	}}

	t.Run("CommitteeMembers", func(t *testing.T) {
		t.Parallel()
		members, err := storedHashSetView(t, changes).CommitteeMembers()
		require.ErrorContains(t, err, "committee hot credential")
		require.ErrorContains(t, err, "invalid blake2b-224 hash")
		require.Nil(t, members)
	})
	t.Run("CommitteeCredentialMember", func(t *testing.T) {
		t.Parallel()
		member, err := storedHashSetView(t, changes).CommitteeCredentialMember(
			lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.CredentialHash(cold),
			},
		)
		require.ErrorContains(t, err, "committee hot credential")
		require.ErrorContains(t, err, "invalid blake2b-224 hash")
		require.Nil(t, member)
	})
}

// TestElectingVrfKeyHashRejectsMalformedCutoffRegistration covers the
// registration in force at the electing snapshot's parameter cutoff. Read as
// a miss, a malformed key there falls through to the earliest-registration
// lookup and resolves an older registration's key, so a header signed with a
// retired VRF key would verify.
func TestElectingVrfKeyHashRejectsMalformedCutoffRegistration(t *testing.T) {
	t.Parallel()
	nonce := bytes.Repeat([]byte{0x07}, 32)
	tb := createTestBlock(t, [32]byte{52}, 52, tamperNone)
	ls, db := newEligibilityTestLedger(t, nonce)
	ls.epochCache = previewEpochs(35, 39, nonce)
	ls.publishSnapshotsLocked()

	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x12}, 28))
	originalKey := bytes.Repeat([]byte{0xC4}, 32)
	// Cutoff for epoch 38 is 3110399 and capture is 3196799.
	seedPoolRegistrationAtSlot(t, db, pool[:], originalKey, 2_479_516)
	seedPoolRegistrationAtSlot(
		t, db, pool[:], shortStoredHash(lcommon.Blake2b256Size, 0xB6),
		3_000_000,
	)
	seedPoolStakeSnapshotOfTypeAtSlot(t, db, 37,
		models.PoolStakeSnapshotTypeMark, pool[:], 1_000, 10_000, 3_196_799)

	got, ok, err := ls.electingVrfKeyHash(tb.block, 38, pool)
	require.ErrorContains(t, err, "VRF key hash at cutoff slot")
	require.ErrorContains(t, err, "invalid blake2b-256 hash")
	require.False(t, ok)
	require.NotEqual(t, lcommon.NewBlake2b256(originalKey), got)
}

func TestRegisteredPoolVrfKeyHashRejectsMalformedStoredKey(t *testing.T) {
	t.Parallel()
	valid := bytes.Repeat([]byte{0xA5}, lcommon.Blake2b256Size)
	short := shortStoredHash(lcommon.Blake2b256Size, 0xA6)
	poolKey := bytes.Repeat([]byte{0x13}, lcommon.Blake2b224Size)

	t.Run("latest registration", func(t *testing.T) {
		t.Parallel()
		// Read as absent, the malformed registration would fall back to the
		// pool row's key.
		_, ok, err := registeredPoolVrfKeyHash(&models.Pool{
			PoolKeyHash:  poolKey,
			VrfKeyHash:   valid,
			Registration: []models.PoolRegistration{{VrfKeyHash: short}},
		})
		require.ErrorContains(t, err, "invalid blake2b-256 hash")
		require.False(t, ok)
	})
	t.Run("pool row", func(t *testing.T) {
		t.Parallel()
		_, ok, err := registeredPoolVrfKeyHash(&models.Pool{
			PoolKeyHash:  poolKey,
			VrfKeyHash:   short,
			Registration: []models.PoolRegistration{{}},
		})
		require.ErrorContains(t, err, "invalid blake2b-256 hash")
		require.False(t, ok)
	})
	t.Run("as of slot", func(t *testing.T) {
		t.Parallel()
		_, ok, err := registeredPoolVrfKeyHashAsOfSlot(&models.Pool{
			PoolKeyHash: poolKey,
			Registration: []models.PoolRegistration{{
				VrfKeyHash: short,
				AddedSlot:  5,
			}},
		}, 10)
		require.ErrorContains(t, err, "invalid blake2b-256 hash")
		require.False(t, ok)
	})
	t.Run(
		"absent registration key falls back to the pool row",
		func(t *testing.T) {
			t.Parallel()
			got, ok, err := registeredPoolVrfKeyHash(&models.Pool{
				PoolKeyHash:  poolKey,
				VrfKeyHash:   valid,
				Registration: []models.PoolRegistration{{}},
			})
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, lcommon.NewBlake2b256(valid), got)
		},
	)
}

func TestChainDepStateRejectsMalformedStoredEpochNonce(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	valid := bytes.Repeat([]byte{0x22}, lcommon.Blake2b256Size)
	require.NoError(t, seedDB(t, db).SetEpoch(
		0,
		0,
		shortStoredHash(lcommon.Blake2b256Size, 0x11),
		valid,
		valid,
		valid,
		eras.ConwayEraDesc.Id,
		1,
		1000,
		nil,
	))
	ls := newChainDepStateLedger(t, db)

	result, err := ls.Query(t.Context(), chainDepStateQuery(), QueryPoint{})
	require.ErrorContains(t, err, "chain dep state epoch nonce")
	require.ErrorContains(t, err, "invalid blake2b-256 hash")
	require.Nil(t, result)
}

func TestNonceFromBytesKeepsEmptyNeutral(t *testing.T) {
	t.Parallel()
	nonce, err := nonceFromBytes(nil)
	require.NoError(t, err)
	require.Equal(t, lcommon.Nonce{Type: lcommon.NonceTypeNeutral}, nonce)
}

func TestRewardBlockCountsRejectsMalformedPoolInput(t *testing.T) {
	t.Parallel()
	db, _ := newStoredHashDB(t, func(s *storedHashStore) {
		s.epoch = &models.Epoch{EpochId: 5, StartSlot: 100, LengthInSlots: 100}
	})
	ls := &LedgerState{config: LedgerStateConfig{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}}
	_, _, _, err := ls.rewardBlockCounts(
		dbadapter.New(db),
		nil,
		5,
		[]*models.RewardPoolInput{{
			PoolKeyHash: shortStoredHash(lcommon.Blake2b224Size, 0x51),
		}},
	)
	require.ErrorContains(t, err, "reward block-count pool input")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
}

func TestRebuildPrunedRewardStakeInputsRejectsMalformedPoolInput(t *testing.T) {
	t.Parallel()
	short := shortStoredHash(lcommon.Blake2b224Size, 0x61)
	db, store := newStoredHashDB(t, func(s *storedHashStore) {
		s.epoch = &models.Epoch{EpochId: 5, StartSlot: 100, LengthInSlots: 100}
		s.rebuiltInputs = []*models.RewardStakeInput{{
			PoolKeyHash: short,
			StakingKey:  bytes.Repeat([]byte{0x62}, lcommon.Blake2b224Size),
			Stake:       1,
		}}
	})
	ls := &LedgerState{config: LedgerStateConfig{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}}
	_, err := ls.rebuildPrunedRewardStakeInputs(
		dbadapter.New(db),
		nil,
		5,
		&models.RewardSnapshot{CapturedSlot: 150, BoundarySlot: 199},
		[]*models.RewardPoolInput{{PoolKeyHash: short}},
	)
	require.ErrorIs(t, err, errRewardStakeInputsUnrecoverable)
	require.ErrorContains(t, err, "reward pool input for epoch 5")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.Empty(t, store.registrationsSeen,
		"a padded pool id must not reach the owner-resolution query")
}

func TestRebuildPrunedRewardStakeInputsKeepsQueryFailureRetryable(
	t *testing.T,
) {
	t.Parallel()
	queryErr := errors.New("database temporarily unavailable")
	db, _ := newStoredHashDB(t, func(s *storedHashStore) {
		s.rebuiltErr = queryErr
	})
	ls := &LedgerState{config: LedgerStateConfig{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}}
	_, err := ls.rebuildPrunedRewardStakeInputs(
		dbadapter.New(db),
		nil,
		5,
		&models.RewardSnapshot{CapturedSlot: 150, BoundarySlot: 199},
		[]*models.RewardPoolInput{{
			PoolKeyHash: bytes.Repeat([]byte{0x61}, lcommon.Blake2b224Size),
		}},
	)
	require.ErrorIs(t, err, queryErr)
	require.NotErrorIs(t, err, errRewardStakeInputsUnrecoverable)
}
