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

package sqlstore

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func TestAddAccountRewardsByCredentialRespectsSQLiteParameterLimit(
	t *testing.T,
) {
	t.Parallel()
	store := newManagementTestStore(t)
	stakingKey := bytesRepeat(0x61, 28)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakingKey,
		Active:     true,
	}))

	ctx := context.Background()
	conn, err := store.writeDB.Conn(ctx)
	require.NoError(t, err)
	_, err = sqlite.Limit(
		conn,
		sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER,
		store.dialect.ParameterLimit(),
	)
	require.NoError(t, err)
	tx, err := conn.BeginTx(ctx, store.dialect.BeginOptions(false))
	require.NoError(t, err)
	txn := &sqlTxn{owner: store, tx: tx, ctx: ctx}
	t.Cleanup(func() {
		require.NoError(t, txn.Rollback())
		require.NoError(t, conn.Close())
	})

	credits := make([]models.AccountRewardCredit, 200)
	for index := range credits {
		sourceHash := make([]byte, 32)
		binary.BigEndian.PutUint64(sourceHash[24:], uint64(index+1))
		credits[index] = models.AccountRewardCredit{
			StakingKey:    stakingKey,
			SourceHash:    sourceHash,
			Amount:        1,
			Slot:          100,
			CredentialTag: 0,
		}
	}
	require.NoError(t, store.AddAccountRewardsByCredential(credits, txn))
	require.NoError(t, txn.Commit())

	account, err := store.GetAccountByCredential(0, stakingKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(len(credits)), uint64(account.Reward))
}
