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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// TestLedgerViewRewardWithdrawalValidation exercises the protocol rule against
// Dingo's database-backed LedgerView. The upstream rule is responsible for the
// era-specific amount policy; storage remains era-neutral and rejects only
// overdrafts before subtracting the accepted amount.
func TestLedgerViewRewardWithdrawalValidation(t *testing.T) {
	const balance = uint64(100)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)

	key := bytes.Repeat([]byte{0xa1}, lcommon.AddressHashSize)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: key,
		Reward:     types.Uint64(balance),
		Active:     true,
	}))
	lv := &LedgerView{ls: &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}}
	rewardAddr, err := lcommon.NewAddressFromBytes(
		append([]byte{0xe1}, key...),
	)
	require.NoError(t, err)

	validateShelley := func(amount uint64) error {
		tx := mockledger.NewTransactionBuilder().WithWithdrawals(
			map[*lcommon.Address]uint64{&rewardAddr: amount},
		)
		return shelley.UtxoValidateWithdrawals(tx, 0, lv, nil)
	}
	require.NoError(t, validateShelley(balance))
	var incorrectAmount shelley.IncorrectWithdrawalAmountError
	require.ErrorAs(t, validateShelley(balance/2), &incorrectAmount)
	incorrectAmount = shelley.IncorrectWithdrawalAmountError{}
	require.ErrorAs(t, validateShelley(balance+1), &incorrectAmount)

	validateDijkstra := func(amount uint64) error {
		tx := mockledger.NewTransactionBuilder().WithWithdrawals(
			map[*lcommon.Address]uint64{&rewardAddr: amount},
		)
		pp := &conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: lcommon.ProtocolVersionDijkstra,
			},
		}
		return conway.UtxoValidateWithdrawals(tx, 0, lv, pp)
	}
	require.NoError(t, validateDijkstra(balance/2))
	incorrectAmount = shelley.IncorrectWithdrawalAmountError{}
	require.ErrorAs(t, validateDijkstra(balance+1), &incorrectAmount)
}
