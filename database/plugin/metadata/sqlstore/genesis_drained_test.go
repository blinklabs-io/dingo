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
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// The genesis writes of a database whose accounts and DReps are drained leave
// those families' tables empty, since the set holds them, and still write the
// delegation and DRep registration certificate history.
func TestGenesisWritesSkipDrainedFamilyTables(t *testing.T) {
	t.Parallel()
	familyTables := []string{
		"account", "account_import_baseline", "registration", "drep",
		"drep_delegator", "drep_expiry_history",
	}
	historyTables := []string{
		"registration_drep", "vote_delegation", "stake_delegation",
	}
	for _, drained := range []bool{false, true} {
		store := newDepositHeldStore(t)
		if drained {
			store.SetDrainedFamilies(nodesettings.FamilyMask(0).
				With(nodesettings.FamilyAccounts).
				With(nodesettings.FamilyDReps).
				With(nodesettings.FamilyLiveStake))
		}
		drepCred := &lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.CredentialHash(bytes.Repeat([]byte{0xd1}, 28)),
		}
		voter := &lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.CredentialHash(bytes.Repeat([]byte{0xc1}, 28)),
		}
		staker := &lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.CredentialHash(bytes.Repeat([]byte{0xc2}, 28)),
		}
		require.NoError(t, store.SetGenesisGovernance(
			conway.ConwayGenesisInitialDReps{
				drepCred: {Expiry: 7, Deposit: 500},
			},
			conway.ConwayGenesisDelegs{
				voter: {
					Type: conway.ConwayGenesisDelegateeTypeVote,
					DRep: lcommon.Drep{
						Type:       lcommon.DrepTypeAddrKeyHash,
						Credential: bytes.Repeat([]byte{0xd1}, 28),
					},
				},
				staker: {
					Type:   conway.ConwayGenesisDelegateeTypeStake,
					PoolId: lcommon.PoolId(bytes.Repeat([]byte{0xb1}, 28)),
				},
			},
			nil, nil,
		))
		pool := depositHeldPoolKey(0xb2)
		require.NoError(t, store.SetGenesisStakingWithDeposits(
			map[string]lcommon.PoolRegistrationCertificate{
				hex.EncodeToString(pool.Bytes()): *depositHeldRegistration(pool),
			},
			map[string]string{
				hex.EncodeToString(bytes.Repeat([]byte{0xc3}, 28)): hex.EncodeToString(pool.Bytes()),
			},
			2_000_000, 500, nil, nil,
		))
		for _, table := range familyTables {
			if drained {
				require.Zero(t, tableCount(t, store, table), table)
			} else {
				require.Positive(t, tableCount(t, store, table), table)
			}
		}
		for _, table := range historyTables {
			require.Equal(
				t, 1, tableCount(t, store, table),
				"drained=%v %s", drained, table,
			)
		}
	}
}
