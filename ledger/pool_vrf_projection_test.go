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
	"context"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

type poolVrfLegacyStore struct {
	metadata.MetadataStore
	fullReads atomic.Uint64
}

func (s *poolVrfLegacyStore) GetPool(
	key lcommon.PoolKeyHash, inactive bool, txn types.Txn,
) (*models.Pool, error) {
	s.fullReads.Add(1)
	return s.MetadataStore.GetPool(key, inactive, txn)
}

type poolVrfProjectionStore struct {
	*poolVrfLegacyStore
	projectedReads        atomic.Uint64
	malformedRegistration bool
	malformedFallback     bool
	fail                  bool
}

func (s *poolVrfProjectionStore) GetPoolVrfState(
	key lcommon.PoolKeyHash, txn types.Txn,
) (*models.Pool, error) {
	s.projectedReads.Add(1)
	if s.fail {
		return nil, errInjectedStorage
	}
	pool, err := s.MetadataStore.(interface {
		GetPoolVrfState(lcommon.PoolKeyHash, types.Txn) (*models.Pool, error)
	}).GetPoolVrfState(key, txn)
	if err != nil || pool == nil {
		return pool, err
	}
	if s.malformedRegistration {
		pool.Registration[0].VrfKeyHash = []byte{1}
	}
	if s.malformedFallback {
		pool.Registration[0].VrfKeyHash = nil
		pool.VrfKeyHash = []byte{1}
	}
	return pool, nil
}

func TestHeaderStateUsesPoolVrfProjectionWithLegacyFallback(t *testing.T) {
	t.Parallel()
	for _, optimized := range []bool{true, false} {
		name := "projection"
		if !optimized {
			name = "legacy provider"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			legacy := &poolVrfLegacyStore{}
			projected := &poolVrfProjectionStore{poolVrfLegacyStore: legacy}
			db, err := dbtest.NewDatabaseWithMetadataWrapper(t, dbtest.Options{
				Config: &database.Config{DataDir: t.TempDir()},
			}, func(inner metadata.MetadataStore) metadata.MetadataStore {
				legacy.MetadataStore = inner
				if optimized {
					return projected
				}
				return legacy
			})
			require.NoError(t, err)
			block := createTestBlock(t, [32]byte{72}, 0, tamperNone)
			ls := newEligibilityTestLedgerOnDB(t, block.epochNonce, db)
			key := block.block.IssuerVkey().Hash()
			seedBlockPoolRegistration(t, db, block.block)
			seedPoolStakeSnapshot(t, db, 4, key[:], 1_000_000_000)
			legacy.fullReads.Store(0)
			projected.projectedReads.Store(0)
			require.NoError(t, ls.verifyBlockHeaderState(block.block, 5, false))
			if optimized {
				require.Positive(
					t,
					projected.projectedReads.Load(),
					"production header state must use the projection",
				)
				require.Zero(
					t,
					legacy.fullReads.Load(),
					"VRF authorization must not hydrate a full pool",
				)
			} else {
				require.Positive(t, legacy.fullReads.Load(), "old metadata providers must keep resolving current keys")
			}
			_, err = db.GetPoolVrfState(
				context.Background(),
				lcommon.PoolKeyHash{0xff},
				nil,
			)
			require.ErrorIs(t, err, models.ErrPoolNotFound)
		})
	}
}

func TestPoolVrfProjectionFailuresRemainLocalStateErrors(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{"registration hash", "fallback hash", "database failure"} {
		t.Run(fault, func(t *testing.T) {
			t.Parallel()
			store := &poolVrfProjectionStore{
				poolVrfLegacyStore: &poolVrfLegacyStore{},
			}
			db, err := dbtest.NewDatabaseWithMetadataWrapper(t, dbtest.Options{
				Config: &database.Config{DataDir: t.TempDir()},
			}, func(inner metadata.MetadataStore) metadata.MetadataStore {
				store.MetadataStore = inner
				return store
			})
			require.NoError(t, err)
			block := createTestBlock(t, [32]byte{73}, 0, tamperNone)
			ls := newEligibilityTestLedgerOnDB(t, block.epochNonce, db)
			key := block.block.IssuerVkey().Hash()
			seedBlockPoolRegistration(t, db, block.block)
			seedPoolStakeSnapshot(t, db, 4, key[:], 1_000_000_000)
			require.NoError(t, ls.verifyBlockHeaderState(block.block, 5, false))
			store.projectedReads.Store(0)
			switch fault {
			case "registration hash":
				store.malformedRegistration = true
			case "fallback hash":
				store.malformedFallback = true
			case "database failure":
				store.fail = true
			}
			err = ls.verifyBlockHeaderState(block.block, 5, false)
			require.ErrorIs(t, err, errHeaderStateLookupFailed)
			require.False(t, headerFailureBlamesPeer(err))
			require.Positive(
				t,
				store.projectedReads.Load(),
				"fault must reach the optimized production read",
			)
			if store.fail {
				require.ErrorIs(t, err, errInjectedStorage)
			}
		})
	}
}
