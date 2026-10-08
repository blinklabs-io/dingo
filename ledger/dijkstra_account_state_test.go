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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

func dijkstraTxFromBodyFields(
	t *testing.T,
	fields map[uint]any,
) lcommon.Transaction {
	t.Helper()
	fields[0] = []any{}
	fields[1] = []any{}
	fields[2] = uint64(0)
	body, err := cbor.Encode(fields)
	require.NoError(t, err)
	txCbor, err := cbor.Encode([]any{cbor.RawMessage(body), map[uint]any{}, true, nil})
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(gledger.TxTypeDijkstra, txCbor)
	require.NoError(t, err)
	return tx
}

// A transaction's account-balance intervals are validated against the
// balance the database holds, so a direct deposit applied by an earlier
// transaction is visible to a later one, a rollback hides it again, and a
// rejected validation leaves the stored balance untouched.
func TestDijkstraBalanceIntervalsObserveAppliedDirectDeposits(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	stakeKey := bytes.Repeat([]byte{0x42}, lcommon.AddressHashSize)
	rewardAccount := cbor.NewByteString(append([]byte{0xe0}, stakeKey...))
	require.NoError(t, db.CreateAccount(context.Background(), nil, &models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		AddedSlot:     1,
		Reward:        dbtypes.Uint64(5),
		Active:        true,
	}))
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		},
	}
	view := &LedgerView{ls: ls}

	deposit := dijkstraTxFromBodyFields(t, map[uint]any{
		25: map[cbor.ByteString]uint64{rewardAccount: 20},
	})
	observer := dijkstraTxFromBodyFields(t, map[uint]any{
		26: map[cbor.ByteString]uint64{rewardAccount: 25},
	})
	startingObserver := dijkstraTxFromBodyFields(t, map[uint]any{
		27: map[cbor.ByteString]uint64{rewardAccount: 25},
	})
	validate := func(tx lcommon.Transaction) error {
		return dijkstra.UtxoValidateAccountBalanceIntervals(
			tx, 2, view, &dijkstra.DijkstraProtocolParameters{},
		)
	}
	balance := func() dbtypes.Uint64 {
		account, err := db.GetAccountByCredential(context.Background(), 0, stakeKey, false, nil)
		require.NoError(t, err)
		return account.Reward
	}

	var outside dijkstra.BalancesOutsideAccountBalanceIntervalsError
	require.ErrorAs(t, validate(observer), &outside)
	require.ErrorAs(t, validate(startingObserver), &outside)
	require.Equal(t, dbtypes.Uint64(5), balance())

	apply := func() {
		require.NoError(t, db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
			return ApplyDijkstraDirectDeposits(context.Background(), db, deposit, 2, txn)
		}))
	}
	apply()
	require.Equal(t, dbtypes.Uint64(25), balance())
	require.NoError(t, validate(observer))
	require.NoError(t, validate(startingObserver))

	require.NoError(t, db.DeleteAccountRewardsAfterSlot(context.Background(), 1, nil))
	require.Equal(t, dbtypes.Uint64(5), balance())
	require.ErrorAs(t, validate(observer), &outside)

	apply()
	require.NoError(t, validate(observer))
}
