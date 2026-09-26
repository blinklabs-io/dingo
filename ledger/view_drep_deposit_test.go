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

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLedgerViewGetDRepVotingPowerIncludesActiveProposalDeposit proves the
// local-state-query GetDRepVotingPower path (LedgerView.GetDRepVotingPower,
// which currently has no wired caller but is exported for a future
// GetDRepState handler) reports the same CIP-1694 deposit-inclusive voting
// power ledger/governance.LoadDRepVotingState uses for real ratification and
// the Blockfrost adapter's DRep reads (blinklabs-io/dingo#4355), not the
// plain UTxO+reward figure GetDRepVotingPower alone returns.
func TestLedgerViewGetDRepVotingPowerIncludesActiveProposalDeposit(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:     db,
		ChainManager: cm,
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })

	drepCred := bytes.Repeat([]byte{0x11}, 28)
	drepStakeCred := bytes.Repeat([]byte{0x22}, 28)
	returnStakeCred := bytes.Repeat([]byte{0x33}, 28)

	require.NoError(t, db.CreateDrep(nil, &models.Drep{
		Credential: drepCred,
		Active:     true,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: drepStakeCred,
		Drep:       drepCred,
		DrepType:   models.DrepTypeAddrKeyHash,
		AddedSlot:  1,
		Active:     true,
	}))
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:       bytes.Repeat([]byte{0x44}, 32),
		OutputIdx:  0,
		StakingKey: drepStakeCred,
		AddedSlot:  1,
		Amount:     100,
	}))

	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: returnStakeCred,
		Drep:       drepCred,
		DrepType:   models.DrepTypeAddrKeyHash,
		AddedSlot:  1,
		Active:     true,
	}))
	returnAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		returnStakeCred,
	)
	require.NoError(t, err)
	returnAddrBytes, err := returnAddr.Bytes()
	require.NoError(t, err)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        bytes.Repeat([]byte{0x55}, 32),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 0,
		ExpiresEpoch:  100,
		Deposit:       50,
		ReturnAddress: returnAddrBytes,
		AnchorURL:     "https://example.invalid/deposit",
		AnchorHash:    bytes.Repeat([]byte{0x66}, 32),
		AddedSlot:     1,
	}, nil))

	lv := &LedgerView{ls: ls}
	power, err := lv.GetDRepVotingPower(0, drepCred)
	require.NoError(t, err)
	assert.Equal(t, uint64(150), power)
}
