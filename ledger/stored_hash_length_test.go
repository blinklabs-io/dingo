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
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
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
	poolByVrfKeyHash  *models.Pool
	committeeMembers  []*models.CommitteeMember
	committeeHot      *models.AuthCommitteeHot
	epoch             *models.Epoch
	rebuiltInputs     []*models.RewardStakeInput
	registrationsSeen [][]lcommon.PoolKeyHash
}

func (s *storedHashStore) GetPoolByVrfKeyHash(
	vrfKeyHash []byte,
	epochStartSlot uint64,
	txn types.Txn,
) (*models.Pool, error) {
	if s.poolByVrfKeyHash == nil {
		return s.MetadataStore.GetPoolByVrfKeyHash(
			vrfKeyHash,
			epochStartSlot,
			txn,
		)
	}
	return s.poolByVrfKeyHash, nil
}

func (s *storedHashStore) GetCommitteeMembers(
	txn types.Txn,
) ([]*models.CommitteeMember, error) {
	if s.committeeMembers == nil {
		return s.MetadataStore.GetCommitteeMembers(txn)
	}
	return s.committeeMembers, nil
}

func (s *storedHashStore) GetCommitteeMember(
	coldTag uint8,
	coldKey []byte,
	termStartSlot uint64,
	txn types.Txn,
) (*models.AuthCommitteeHot, error) {
	if s.committeeHot == nil {
		return s.MetadataStore.GetCommitteeMember(
			coldTag,
			coldKey,
			termStartSlot,
			txn,
		)
	}
	return s.committeeHot, nil
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
	expiryEpoch uint64,
	inactivityPeriod uint64,
	txn types.Txn,
) ([]*models.RewardStakeInput, error) {
	if s.rebuiltInputs == nil {
		return s.MetadataStore.GetEpochBoundaryRewardStakeInputsForPools(
			poolKeyHashes,
			snapshotSlot,
			boundarySlot,
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
	db, err := dbtest.NewDatabaseWithMetadataWrapper(
		t,
		dbtest.Options{Config: &database.Config{DataDir: ""}},
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

// shortStoredHash is one byte narrower than a hash of size bytes. An unchecked
// conversion zero-pads it into a well-formed hash.
func shortStoredHash(size int, fill byte) []byte {
	return bytes.Repeat([]byte{fill}, size-1)
}

func TestMIRDelegStateRejectsMalformedStoredCredential(t *testing.T) {
	t.Parallel()
	ls, db, raw := newMIRTestLedger(t)
	withMIRCutoffEpoch(
		t,
		ls,
		models.Epoch{StartSlot: 100, LengthInSlots: 432_000},
	)
	seedMIRDistribution(t, raw, mirPotReserves, 150,
		[]models.MoveInstantaneousRewardsReward{{
			Credential: shortStoredHash(lcommon.Blake2b224Size, 0x77),
			Amount:     big.NewInt(10),
		}})

	txn := db.Transaction(context.Background(), false)
	err := txn.Do(func(txn *database.Txn) error {
		lv := &LedgerView{ls: ls, txn: txn, epochStartSlot: 100}
		_, err := lv.MIRDelegState(200, true)
		return err
	})
	require.ErrorContains(t, err, "MIR reward credential")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
}

func TestIsVrfKeyInUseRejectsMalformedStoredPoolKey(t *testing.T) {
	t.Parallel()
	db, _ := newStoredHashDB(t, func(s *storedHashStore) {
		s.poolByVrfKeyHash = &models.Pool{
			PoolKeyHash: shortStoredHash(lcommon.Blake2b224Size, 0x21),
		}
	})
	inUse, owner, err := storedHashView(t, db).IsVrfKeyInUse(
		lcommon.Blake2b256{0x01},
	)
	require.ErrorContains(t, err, "VRF key owner pool key hash")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.False(t, inUse)
	require.Equal(t, lcommon.PoolKeyHash{}, owner)
}

func TestCommitteeMembersRejectsMalformedStoredColdCredential(t *testing.T) {
	t.Parallel()
	db, _ := newStoredHashDB(t, func(s *storedHashStore) {
		s.committeeMembers = []*models.CommitteeMember{{
			ID:           1,
			ColdCredHash: shortStoredHash(lcommon.Blake2b224Size, 0x31),
			ExpiresEpoch: 10,
		}}
	})
	members, err := storedHashView(t, db).CommitteeMembers()
	require.ErrorContains(t, err, "committee cold credential")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.Nil(t, members)
}

func TestCommitteeHotCredentialRejectsMalformedStoredHash(t *testing.T) {
	t.Parallel()
	cold := bytes.Repeat([]byte{0x41}, lcommon.Blake2b224Size)
	configure := func(s *storedHashStore) {
		s.committeeMembers = []*models.CommitteeMember{{
			ID:           1,
			ColdCredHash: cold,
			ExpiresEpoch: 10,
		}}
		s.committeeHot = &models.AuthCommitteeHot{
			ColdCredential: cold,
			HotCredential:  shortStoredHash(lcommon.Blake2b224Size, 0x42),
		}
	}

	t.Run("CommitteeMembers", func(t *testing.T) {
		t.Parallel()
		db, _ := newStoredHashDB(t, configure)
		members, err := storedHashView(t, db).CommitteeMembers()
		require.ErrorContains(t, err, "committee hot credential")
		require.ErrorContains(t, err, "invalid blake2b-224 hash")
		require.Nil(t, members)
	})
	t.Run("CommitteeCredentialMember", func(t *testing.T) {
		t.Parallel()
		db, _ := newStoredHashDB(t, configure)
		member, err := storedHashView(t, db).CommitteeCredentialMember(
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
	require.NoError(t, db.Metadata().SetEpoch(
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
	db, store := newStoredHashDB(t, func(s *storedHashStore) {
		s.epoch = &models.Epoch{EpochId: 5, StartSlot: 100, LengthInSlots: 100}
	})
	_ = db
	ls := &LedgerState{config: LedgerStateConfig{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}}
	_, _, _, err := ls.rewardBlockCounts(
		store,
		nil,
		5,
		[]*models.RewardPoolInput{{
			PoolKeyHash: shortStoredHash(lcommon.Blake2b224Size, 0x51),
		}},
		big.NewRat(0, 1),
	)
	require.ErrorContains(t, err, "reward block-count pool input")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
}

func TestRebuildPrunedRewardStakeInputsRejectsMalformedPoolInput(t *testing.T) {
	t.Parallel()
	short := shortStoredHash(lcommon.Blake2b224Size, 0x61)
	_, store := newStoredHashDB(t, func(s *storedHashStore) {
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
		store,
		nil,
		5,
		&models.RewardSnapshot{CapturedSlot: 150, BoundarySlot: 199},
		[]*models.RewardPoolInput{{PoolKeyHash: short}},
	)
	require.ErrorContains(t, err, "reward pool input for epoch 5")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.Empty(t, store.registrationsSeen,
		"a padded pool id must not reach the owner-resolution query")
}
