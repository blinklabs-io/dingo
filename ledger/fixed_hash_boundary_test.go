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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// newFixedHashBoundaryLedger builds a ledger over an empty database and its
// LedgerDB set.
func newFixedHashBoundaryLedger(t *testing.T) *LedgerState {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	ls := &LedgerState{db: db}
	attachTestLiveSet(t, ls)
	require.NoError(t, ls.ledgerDB().SetEpoch(
		0, 0, nil, nil, nil, nil, 0, 1000, 100, nil,
	))
	return ls
}

// seedFixedHashBoundaryPool writes pool's first registration to ls's set as
// the registration in force, keeping every hash at the length the test gave
// it.
func seedFixedHashBoundaryPool(ls *LedgerState, pool *models.Pool) {
	reg := pool.Registration[0]
	reg.AddedSlot = 1
	ls.stageLedgerChanges(nil, LedgerChanges{
		Pools: []PoolChange{{
			Slot:  1,
			State: PoolState{Pool: pool.PoolKeyHash, Params: &reg},
		}},
		PoolEvents: []PoolEventChange{{
			Pool:  pool.PoolKeyHash,
			Event: PoolEvent{Slot: 1, Registration: &reg},
		}},
	})
}

func TestLedgerViewPoolCurrentStateRejectsMalformedHashes(t *testing.T) {
	t.Parallel()

	for _, field := range []string{"vrf", "reward account", "owner"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			pool := &models.Pool{
				PoolKeyHash:   make([]byte, lcommon.Blake2b224Size),
				VrfKeyHash:    make([]byte, lcommon.Blake2b256Size),
				RewardAccount: make([]byte, lcommon.Blake2b224Size),
				Registration: []models.PoolRegistration{{
					PoolKeyHash:   make([]byte, lcommon.Blake2b224Size),
					VrfKeyHash:    make([]byte, lcommon.Blake2b256Size),
					RewardAccount: make([]byte, lcommon.Blake2b224Size),
					Owners: []models.PoolRegistrationOwner{{
						KeyHash: make([]byte, lcommon.Blake2b224Size),
					}},
				}},
			}
			switch field {
			case "vrf":
				reg := &pool.Registration[0]
				reg.VrfKeyHash = reg.VrfKeyHash[:lcommon.Blake2b256Size-1]
			case "reward account":
				reg := &pool.Registration[0]
				reg.RewardAccount = reg.RewardAccount[:lcommon.Blake2b224Size-1]
			case "owner":
				ownerKeyHash := pool.Registration[0].Owners[0].KeyHash
				pool.Registration[0].Owners[0].KeyHash =
					ownerKeyHash[:lcommon.Blake2b224Size-1]
			}
			ls := newFixedHashBoundaryLedger(t)
			seedFixedHashBoundaryPool(ls, pool)
			txn := ls.db.Transaction(context.Background(), false)
			defer txn.Release()
			view := &LedgerView{ls: ls, txn: txn}

			_, _, err := view.PoolCurrentState(lcommon.PoolKeyHash{})
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid blake2b-")
		})
	}
}

func TestLedgerViewDRepRegistrationsRejectsMalformedCredential(t *testing.T) {
	t.Parallel()
	ls := newFixedHashBoundaryLedger(t)
	ls.stageLedgerChanges(nil, LedgerChanges{DReps: []DRepChange{{
		Slot: 1,
		Drep: models.Drep{
			Credential: make([]byte, lcommon.Blake2b224Size-1),
			AddedSlot:  1,
			Active:     true,
		},
	}}})
	txn := ls.db.Transaction(context.Background(), false)
	defer txn.Release()
	view := &LedgerView{ls: ls, txn: txn}

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
	ls := newFixedHashBoundaryLedger(t)
	poolKey := make([]byte, lcommon.Blake2b224Size-1)
	seedFixedHashBoundaryPool(ls, &models.Pool{
		PoolKeyHash: poolKey,
		Registration: []models.PoolRegistration{{
			PoolKeyHash: poolKey,
			VrfKeyHash:  make([]byte, lcommon.Blake2b256Size),
		}},
	})

	_, err := ls.queryLedgerPeerSnapshot(
		context.Background(),
		&olocalstatequery.ShelleyGetLedgerPeerSnapshotQuery{
			PeerKind: olocalstatequery.LedgerPeerKindAll,
		},
		nil,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "active pool key")
	require.Contains(t, err.Error(), "invalid blake2b-224 hash")
}

// PoolCurrentState returns the registration in force for a registered pool.
func TestLedgerViewPoolCurrentStateReturnsTheRegistration(t *testing.T) {
	t.Parallel()
	poolKey := bytes.Repeat([]byte{0x21}, lcommon.Blake2b224Size)
	vrfKey := bytes.Repeat([]byte{0x22}, lcommon.Blake2b256Size)
	ls := newFixedHashBoundaryLedger(t)
	seedFixedHashBoundaryPool(ls, &models.Pool{
		PoolKeyHash: poolKey,
		Registration: []models.PoolRegistration{{
			PoolKeyHash:   poolKey,
			VrfKeyHash:    vrfKey,
			RewardAccount: make([]byte, lcommon.Blake2b224Size),
		}},
	})
	txn := ls.db.Transaction(context.Background(), false)
	defer txn.Release()
	view := &LedgerView{ls: ls, txn: txn, epochStartSlot: 100}

	cert, _, err := view.PoolCurrentState(lcommon.NewBlake2b224(poolKey))
	require.NoError(t, err)
	require.NotNil(t, cert)
	require.Equal(t, poolKey, cert.Operator.Bytes())
	require.Equal(t, vrfKey, cert.VrfKeyHash.Bytes())
}
