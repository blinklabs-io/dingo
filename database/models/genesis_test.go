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

package models_test

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

func TestGenesisDelegateeOfStatesEachDelegationType(t *testing.T) {
	t.Parallel()
	pool := lcommon.PoolId(bytes.Repeat([]byte{0xb1}, 28))
	drepKey := bytes.Repeat([]byte{0xd1}, 28)
	for _, tc := range []struct {
		name string
		in   conway.ConwayGenesisDelegatee
		want models.GenesisDelegatee
	}{
		{
			"stake only, with no usable DRep",
			conway.ConwayGenesisDelegatee{
				Type: conway.ConwayGenesisDelegateeTypeStake, PoolId: pool,
				DRep: lcommon.Drep{Type: 9},
			},
			models.GenesisDelegatee{Pool: pool[:], DrepType: models.DrepTypeAddrKeyHash},
		},
		{
			"vote for a credential DRep",
			conway.ConwayGenesisDelegatee{
				Type: conway.ConwayGenesisDelegateeTypeVote,
				DRep: lcommon.Drep{Type: lcommon.DrepTypeScriptHash, Credential: drepKey},
			},
			models.GenesisDelegatee{Drep: drepKey, DrepType: models.DrepTypeScriptHash},
		},
		{
			"vote for a predefined DRep",
			conway.ConwayGenesisDelegatee{
				Type: conway.ConwayGenesisDelegateeTypeVote,
				DRep: lcommon.Drep{Type: lcommon.DrepTypeNoConfidence},
			},
			models.GenesisDelegatee{DrepType: models.DrepTypeAlwaysNoConfidence},
		},
		{
			"stake and vote",
			conway.ConwayGenesisDelegatee{
				Type: conway.ConwayGenesisDelegateeTypeStakeVote, PoolId: pool,
				DRep: lcommon.Drep{Type: lcommon.DrepTypeAddrKeyHash, Credential: drepKey},
			},
			models.GenesisDelegatee{
				Pool: pool[:], Drep: drepKey, DrepType: models.DrepTypeAddrKeyHash,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := models.GenesisDelegateeOf(tc.in)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestGenesisDelegateeOfRefusesWhatTheStoreRefuses(t *testing.T) {
	t.Parallel()
	_, err := models.GenesisDelegateeOf(conway.ConwayGenesisDelegatee{Type: 7})
	require.ErrorContains(t, err, "unknown genesis delegatee type")
	_, err = models.GenesisDelegateeOf(conway.ConwayGenesisDelegatee{
		Type: conway.ConwayGenesisDelegateeTypeVote,
		DRep: lcommon.Drep{Type: 9},
	})
	require.ErrorContains(t, err, "genesis delegatee drep type")
}

func TestGenesisStakeAccountsAreOrderedAndHoldTheKeyDeposit(t *testing.T) {
	t.Parallel()
	a, b := bytes.Repeat([]byte{0x01}, 28), bytes.Repeat([]byte{0x02}, 28)
	pool := bytes.Repeat([]byte{0x0f}, 28)
	accounts, err := models.GenesisStakeAccounts(map[string]string{
		hex.EncodeToString(b): hex.EncodeToString(pool),
		hex.EncodeToString(a): hex.EncodeToString(pool),
	}, 2_000_000)
	require.NoError(t, err)
	require.Len(t, accounts, 2)
	require.Equal(t, a, accounts[0].StakingKey)
	require.Equal(t, b, accounts[1].StakingKey)
	for _, acc := range accounts {
		require.True(t, acc.Active)
		require.Equal(t, pool, acc.Pool)
		require.Zero(t, acc.AddedSlot)
		require.NotNil(t, acc.ImportDeposit)
		require.Equal(t, uint64(2_000_000), uint64(*acc.ImportDeposit))
	}
	_, err = models.GenesisStakeAccounts(map[string]string{"zz": "00"}, 0)
	require.ErrorContains(t, err, "decode staker hash")
}

func TestGenesisDRepIsActiveFromSlotZeroWithItsDeposit(t *testing.T) {
	t.Parallel()
	cred := &lcommon.Credential{
		CredType:   lcommon.CredentialTypeScriptHash,
		Credential: lcommon.CredentialHash(bytes.Repeat([]byte{0x44}, 28)),
	}
	drep, reg, err := models.GenesisDRep(cred, conway.ConwayGenesisDRepState{
		Expiry: 12, Deposit: 7,
		Anchor: &lcommon.GovAnchor{
			Url:      "https://example.invalid",
			DataHash: [32]byte{1},
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint8(1), drep.CredentialTag)
	require.True(t, drep.Active)
	require.Equal(t, uint64(12), drep.ExpiryEpoch)
	require.Equal(t, "https://example.invalid", drep.AnchorURL)
	require.Equal(t, uint64(7), uint64(reg.DepositAmount))
	require.Equal(t, drep.AnchorHash, reg.AnchorHash)
	require.Equal(t, drep.Credential, reg.DrepCredential)
}
