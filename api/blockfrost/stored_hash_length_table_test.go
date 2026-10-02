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

package blockfrost

import (
	"bytes"
	"encoding/hex"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// malformedHashStore returns rows carrying a malformed hash from the reads
// behind each blockfrost path under test; a nil field keeps the real read.
type malformedHashStore struct {
	metadata.MetadataStore
	retiring       []models.PoolRetiringRow
	activePools    [][]byte
	delegations    []models.AccountDelegationHistoryRow
	delegationSize int
}

func (s *malformedHashStore) GetRetiringPools(
	currentEpoch uint64,
	txn types.Txn,
) ([]models.PoolRetiringRow, error) {
	if s.retiring == nil {
		return s.MetadataStore.GetRetiringPools(currentEpoch, txn)
	}
	return s.retiring, nil
}

func (s *malformedHashStore) GetActivePoolKeyHashes(
	txn types.Txn,
) ([][]byte, error) {
	if s.activePools == nil {
		return s.MetadataStore.GetActivePoolKeyHashes(txn)
	}
	return s.activePools, nil
}

func (s *malformedHashStore) CountAccountDelegationHistoryByCredential(
	tag uint8,
	key []byte,
	txn types.Txn,
) (int, error) {
	if s.delegations == nil {
		return s.MetadataStore.CountAccountDelegationHistoryByCredential(
			tag, key, txn,
		)
	}
	return len(s.delegations), nil
}

func (s *malformedHashStore) GetAccountDelegationHistoryByCredential(
	tag uint8,
	key []byte,
	limit int,
	offset int,
	order string,
	txn types.Txn,
) ([]models.AccountDelegationHistoryRow, error) {
	if s.delegations == nil {
		return s.MetadataStore.GetAccountDelegationHistoryByCredential(
			tag, key, limit, offset, order, txn,
		)
	}
	return s.delegations, nil
}

func newMalformedHashAdapter(
	t *testing.T,
	store *malformedHashStore,
	seed func(db *database.Database),
) *NodeAdapter {
	t.Helper()
	db, err := dbtest.NewDatabaseWithMetadataWrapper(
		t,
		dbtest.Options{Config: &database.Config{DataDir: t.TempDir()}},
		func(inner metadata.MetadataStore) metadata.MetadataStore {
			store.MetadataStore = inner
			return store
		},
	)
	require.NoError(t, err)
	// Account resolves its active epoch from the ledger's epoch cache.
	require.NoError(t, db.Metadata().SetEpoch(
		0, 0, nil, nil, nil, nil, 6, 1, 1000, nil,
	))
	if seed != nil {
		seed(db)
	}
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: cm,
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	adapter, err := NewNodeAdapter(ls, nil)
	require.NoError(t, err)
	return adapter
}

// TestBlockfrostRejectsMalformedStoredHashes drives each blockfrost path
// that renders a stored hash through a row whose hash is one byte short.
// Padded, the value would render as the id of an unrelated pool or account.
func TestBlockfrostRejectsMalformedStoredHashes(t *testing.T) {
	t.Parallel()
	short := bytes.Repeat([]byte{0x51}, lcommon.Blake2b224Size-1)
	stakingKey := bytes.Repeat([]byte{0x52}, lcommon.Blake2b224Size)
	poolKey := bytes.Repeat([]byte{0x53}, lcommon.Blake2b224Size)
	vrfKey := bytes.Repeat([]byte{0x54}, lcommon.Blake2b256Size)
	page := PaginationParams{Count: 10, Page: 1}
	importPool := func(rewardAccount, owner []byte) func(*database.Database) {
		return func(db *database.Database) {
			require.NoError(t, db.Metadata().ImportPool(
				&models.Pool{
					PoolKeyHash:   poolKey,
					VrfKeyHash:    vrfKey,
					RewardAccount: rewardAccount,
				},
				&models.PoolRegistration{
					PoolKeyHash:   poolKey,
					VrfKeyHash:    vrfKey,
					RewardAccount: rewardAccount,
					Owners: []models.PoolRegistrationOwner{{
						KeyHash: owner,
					}},
				},
				nil,
			))
		}
	}
	createAccount := func(pool []byte) func(*database.Database) {
		return func(db *database.Database) {
			require.NoError(t, db.CreateAccount(nil, &models.Account{
				StakingKey: stakingKey,
				Pool:       pool,
				Active:     true,
			}))
		}
	}
	stakeAddress := func(t *testing.T) string {
		return newRewardHistoryStakeAddress(t, stakingKey)
	}

	for _, tc := range []struct {
		name  string
		store *malformedHashStore
		seed  func(*database.Database)
		call  func(*testing.T, *NodeAdapter) error
		want  string
	}{
		{
			name: "PoolsRetiring",
			store: &malformedHashStore{retiring: []models.PoolRetiringRow{{
				PoolKeyHash: short,
				Epoch:       5,
			}}},
			call: func(_ *testing.T, a *NodeAdapter) error {
				_, _, err := a.PoolsRetiring(page)
				return err
			},
			want: "retiring pool key hash",
		},
		{
			name:  "PoolsExtended",
			store: &malformedHashStore{activePools: [][]byte{short}},
			call: func(_ *testing.T, a *NodeAdapter) error {
				_, err := a.PoolsExtended()
				return err
			},
			want: "active pool key hash",
		},
		{
			name:  "Account",
			store: &malformedHashStore{},
			seed:  createAccount(short),
			call: func(t *testing.T, a *NodeAdapter) error {
				_, err := a.Account(stakeAddress(t))
				return err
			},
			want: "delegated pool key hash",
		},
		{
			name: "AccountDelegationHistory",
			store: &malformedHashStore{
				delegations: []models.AccountDelegationHistoryRow{{
					PoolKeyHash: short,
					TxHash:      bytes.Repeat([]byte{0x55}, 32),
				}},
			},
			seed: createAccount(nil),
			call: func(t *testing.T, a *NodeAdapter) error {
				_, _, err := a.AccountDelegationHistory(stakeAddress(t), page)
				return err
			},
			want: "delegation pool key hash",
		},
		{
			name:  "AccountRewardHistory",
			store: &malformedHashStore{},
			seed: func(db *database.Database) {
				createAccount(nil)(db)
				require.NoError(t, db.Metadata().SaveRewardAccountOutputs(
					[]*models.RewardAccountOutput{{
						Epoch:         10,
						CredentialTag: 0,
						StakingKey:    stakingKey,
						PoolKeyHash:   short,
						RewardType:    "member",
						Amount:        1,
						Spendable:     true,
					}},
					nil,
				))
			},
			call: func(t *testing.T, a *NodeAdapter) error {
				_, _, err := a.AccountRewardHistory(stakeAddress(t), page)
				return err
			},
			want: "reward pool key hash",
		},
		{
			name:  "PoolDetail reward account",
			store: &malformedHashStore{},
			seed:  importPool(short, stakingKey),
			call: func(_ *testing.T, a *NodeAdapter) error {
				_, err := a.PoolDetail(hex.EncodeToString(poolKey))
				return err
			},
			want: "pool reward account",
		},
		{
			name:  "PoolDetail owner",
			store: &malformedHashStore{},
			seed:  importPool(stakingKey, short),
			call: func(_ *testing.T, a *NodeAdapter) error {
				_, err := a.PoolDetail(hex.EncodeToString(poolKey))
				return err
			},
			want: "pool owner key hash",
		},
		{
			name:  "Asset",
			store: &malformedHashStore{},
			call: func(_ *testing.T, a *NodeAdapter) error {
				_, err := a.Asset(hex.EncodeToString(short), nil)
				return err
			},
			want: "asset policy ID",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			adapter := newMalformedHashAdapter(t, tc.store, tc.seed)
			err := tc.call(t, adapter)
			require.ErrorContains(t, err, tc.want)
			require.ErrorContains(t, err, "invalid blake2b-224 hash")
		})
	}
}
