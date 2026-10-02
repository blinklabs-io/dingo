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

package database

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

func TestRebuildRewardLiveStakeRejectsInvalidTransaction(t *testing.T) {
	t.Parallel()

	db, err := newTestDatabase(t, &Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	t.Run("blob-only transaction", func(t *testing.T) {
		txn := db.BlobTxn(true)
		t.Cleanup(func() {
			require.NoError(t, txn.Rollback())
		})

		err := db.RebuildRewardLiveStake(context.Background(), 1, txn)

		require.ErrorIs(t, err, types.ErrTxnWrongType)
	})

	t.Run("read-only metadata transaction", func(t *testing.T) {
		txn := db.MetadataTxn(context.Background(), false)
		t.Cleanup(func() {
			require.NoError(t, txn.Rollback())
		})

		err := db.RebuildRewardLiveStake(context.Background(), 1, txn)

		require.ErrorIs(t, err, types.ErrTxnWrongType)
	})

	t.Run("transaction from another database", func(t *testing.T) {
		other, err := newTestDatabase(t, &Config{DataDir: ""})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, other.Close()) })
		txn := other.MetadataTxn(context.Background(), true)
		t.Cleanup(func() { require.NoError(t, txn.Rollback()) })

		err = db.RebuildRewardLiveStake(context.Background(), 1, txn)

		require.ErrorIs(t, err, types.ErrTxnWrongType)
	})
}

type batchRewardLiveStakeMetadataStore struct {
	metadata.MetadataStore
	batchSlot     uint64
	batchCalls    int
	transactional int
	credential    []byte
}

func (s *batchRewardLiveStakeMetadataStore) RebuildRewardLiveStakeFromRunningTotals(
	uint64,
	types.Txn,
) error {
	s.transactional++
	return nil
}

func (s *batchRewardLiveStakeMetadataStore) RebuildRewardLiveStakeFromRunningTotalsInBatches(
	slot uint64,
	runTxn func(func(types.Txn) error) error,
) error {
	s.batchCalls++
	s.batchSlot = slot
	if runTxn == nil {
		return nil
	}
	return runTxn(func(txn types.Txn) error {
		return s.MetadataStore.CreateAccount(txn, &models.Account{
			CredentialTag: 0,
			StakingKey:    s.credential,
			Active:        true,
		})
	})
}

func TestRebuildRewardLiveStakeFromRunningTotalsUsesBatchFinalizer(
	t *testing.T,
) {
	t.Parallel()
	db, err := newTestDatabase(t, &Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	store := &batchRewardLiveStakeMetadataStore{
		MetadataStore: db.Metadata(),
		credential:    bytes.Repeat([]byte{0x42}, 28),
	}
	db.metadata = store

	require.NoError(t, db.RebuildRewardLiveStakeFromRunningTotals(123, nil))
	require.Equal(t, 1, store.batchCalls)
	require.Equal(t, uint64(123), store.batchSlot)
	require.Zero(t, store.transactional)
	account, err := db.Metadata().GetAccountByCredential(0, store.credential, true, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
}
