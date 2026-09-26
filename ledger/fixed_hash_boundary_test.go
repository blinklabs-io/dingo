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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

type fixedHashMetadataStore struct {
	metadata.MetadataStore
	pool                *models.Pool
	dreps               []*models.Drep
	activePoolKeyHashes [][]byte
	poolRows            []models.Pool
}

func (s *fixedHashMetadataStore) GetPool(
	_ lcommon.PoolKeyHash,
	_ bool,
	_ types.Txn,
) (*models.Pool, error) {
	return s.pool, nil
}

func (s *fixedHashMetadataStore) GetActiveDreps(
	_ types.Txn,
) ([]*models.Drep, error) {
	return s.dreps, nil
}

func (s *fixedHashMetadataStore) GetActivePoolKeyHashes(
	_ types.Txn,
) ([][]byte, error) {
	return s.activePoolKeyHashes, nil
}

func (s *fixedHashMetadataStore) GetPools(
	_ []lcommon.PoolKeyHash,
	_ types.Txn,
) ([]models.Pool, error) {
	return s.poolRows, nil
}

func newFixedHashBoundaryDB(
	t *testing.T,
	configure func(*fixedHashMetadataStore),
) *database.Database {
	t.Helper()
	var store fixedHashMetadataStore
	db, err := dbtest.NewDatabaseWithMetadataWrapper(
		t,
		dbtest.Options{Config: &database.Config{DataDir: ""}},
		func(inner metadata.MetadataStore) metadata.MetadataStore {
			store.MetadataStore = inner
			configure(&store)
			return &store
		},
	)
	require.NoError(t, err)
	return db
}

func TestLedgerViewPoolCurrentStateRejectsMalformedHashes(t *testing.T) {
	t.Parallel()

	for _, field := range []string{"operator", "vrf", "reward account", "owner"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			pool := &models.Pool{
				PoolKeyHash:   make([]byte, lcommon.Blake2b224Size),
				VrfKeyHash:    make([]byte, lcommon.Blake2b256Size),
				RewardAccount: make([]byte, lcommon.Blake2b224Size),
				Registration: []models.PoolRegistration{{
					Owners: []models.PoolRegistrationOwner{{
						KeyHash: make([]byte, lcommon.Blake2b224Size),
					}},
				}},
			}
			switch field {
			case "operator":
				pool.PoolKeyHash = pool.PoolKeyHash[:lcommon.Blake2b224Size-1]
			case "vrf":
				pool.VrfKeyHash = pool.VrfKeyHash[:lcommon.Blake2b256Size-1]
			case "reward account":
				pool.RewardAccount = pool.RewardAccount[:lcommon.Blake2b224Size-1]
			case "owner":
				ownerKeyHash := pool.Registration[0].Owners[0].KeyHash
				pool.Registration[0].Owners[0].KeyHash =
					ownerKeyHash[:lcommon.Blake2b224Size-1]
			}
			db := newFixedHashBoundaryDB(t, func(store *fixedHashMetadataStore) {
				store.pool = pool
			})
			txn := db.Transaction(false)
			defer txn.Release()
			view := &LedgerView{ls: &LedgerState{db: db}, txn: txn}

			_, _, err := view.PoolCurrentState(lcommon.PoolKeyHash{})
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid blake2b-")
		})
	}
}

func TestLedgerViewDRepRegistrationsRejectsMalformedCredential(t *testing.T) {
	t.Parallel()
	db := newFixedHashBoundaryDB(t, func(store *fixedHashMetadataStore) {
		store.dreps = []*models.Drep{{Credential: make([]byte, lcommon.Blake2b224Size-1)}}
	})
	txn := db.Transaction(false)
	defer txn.Release()
	view := &LedgerView{ls: &LedgerState{db: db}, txn: txn}

	_, err := view.DRepRegistrations()
	require.Error(t, err)
	require.Contains(t, err.Error(), "DRep registrations credential")
	require.Contains(t, err.Error(), "invalid blake2b-224 hash")
}

func TestPoolKeyHashesFromStakeSnapshotRejectsMalformedKey(t *testing.T) {
	t.Parallel()
	_, err := poolKeyHashesFromStakeByPool(map[string]uint64{
		string(make([]byte, lcommon.Blake2b224Size-1)): 1,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot pool key")
	require.Contains(t, err.Error(), "invalid blake2b-224 hash")
}

func TestPoolVrfKeyHashesRejectsMalformedReturnedPoolKey(t *testing.T) {
	t.Parallel()
	poolKey := make([]byte, lcommon.Blake2b224Size-1)
	vrfKey := make([]byte, lcommon.Blake2b256Size)
	db := newFixedHashBoundaryDB(t, func(store *fixedHashMetadataStore) {
		store.poolRows = []models.Pool{{
			PoolKeyHash: poolKey,
			Registration: []models.PoolRegistration{{
				VrfKeyHash: vrfKey,
			}},
		}}
	})
	ls := &LedgerState{db: db}

	_, err := ls.poolVrfKeyHashes([]lcommon.PoolKeyHash{lcommon.PoolKeyHash{}}, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "registered pool key")
	require.Contains(t, err.Error(), "invalid blake2b-224 hash")
}

func TestPoolKeyHashesFromActivePoolBytesRejectsMalformedKey(t *testing.T) {
	t.Parallel()
	_, err := poolKeyHashesFromActivePoolBytes([][]byte{
		make([]byte, lcommon.Blake2b224Size-1),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "active pool key")
	require.Contains(t, err.Error(), "invalid blake2b-224 hash")
}

func TestQueryLedgerPeerSnapshotRejectsMalformedActivePoolKey(t *testing.T) {
	t.Parallel()
	db := newFixedHashBoundaryDB(t, func(store *fixedHashMetadataStore) {
		store.activePoolKeyHashes = [][]byte{make([]byte, lcommon.Blake2b224Size-1)}
	})
	ls := &LedgerState{db: db}

	_, err := ls.queryLedgerPeerSnapshot(olocalstatequery.LedgerPeerKindAll)
	require.Error(t, err)
	require.Contains(t, err.Error(), "active pool key")
	require.Contains(t, err.Error(), "invalid blake2b-224 hash")
}
