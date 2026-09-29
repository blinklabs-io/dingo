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

//go:build dingo_extra_plugins

package ledger

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// drepPowerOracle is the DRep voting power definition computed in Go from
// the raw rows: every active delegator's live UTxO amounts plus its reward,
// excluding delegators expired before expiryEpoch.
func drepPowerOracle(
	t *testing.T,
	raw *sql.DB,
	expiryEpoch uint64,
) (map[string]uint64, map[uint64]uint64) {
	t.Helper()
	utxo := make(map[string]uint64)
	rows, err := raw.Query(`SELECT credential_tag, staking_key, amount FROM utxo
WHERE deleted_slot = 0 AND staking_key IS NOT NULL`)
	require.NoError(t, err)
	for rows.Next() {
		var tag int64
		var key []byte
		var amount string
		require.NoError(t, rows.Scan(&tag, &key, &amount))
		value, err := strconv.ParseUint(amount, 10, 64)
		require.NoError(t, err)
		utxo[fmt.Sprintf("%d/%x", tag, key)] += value
	}
	require.NoError(t, rows.Close())
	byCredential := make(map[string]uint64)
	byType := make(map[uint64]uint64)
	rows, err = raw.Query(`SELECT credential_tag, staking_key, drep, drep_type,
    reward, expiration_epoch FROM account WHERE active = TRUE`)
	require.NoError(t, err)
	for rows.Next() {
		var tag, drepType, expiration int64
		var key, drep []byte
		var reward sql.NullString
		require.NoError(t, rows.Scan(
			&tag, &key, &drep, &drepType, &reward, &expiration,
		))
		if expiryEpoch > 0 && expiration != 0 &&
			uint64(expiration) < expiryEpoch { //nolint:gosec
			continue
		}
		stake := utxo[fmt.Sprintf("%d/%x", tag, key)]
		if reward.Valid {
			value, err := strconv.ParseUint(reward.String, 10, 64)
			require.NoError(t, err)
			stake += value
		}
		if drepType <= 1 && len(drep) > 0 {
			byCredential[models.NewStakeCredentialRef(
				uint8(drepType), drep, //nolint:gosec
			).MapKey()] += stake
		}
		byType[uint64(drepType)] += stake //nolint:gosec
	}
	require.NoError(t, rows.Close())
	return byCredential, byType
}

// TestDRepVotingPowerFromLiveStakeMatchesDefinition pins the DRep voting
// power reads to their definition on each backend, over delegators whose
// reward_live_stake row is current, stale or missing, spent and live UTxOs,
// inactive and expired delegators, and every DRep kind.
func TestDRepVotingPowerFromLiveStakeMatchesDefinition(t *testing.T) {
	t.Parallel()
	for _, backend := range rewardBatchBackends() {
		t.Run(backend.name, func(t *testing.T) {
			t.Parallel()
			db, raw, _ := backend.open(t)
			const accounts = 240
			const dreps = 9
			drepKey := func(i int) []byte { return rewardBatchKey(0x41, i%dreps) }
			for i := range accounts {
				account := &models.Account{
					StakingKey:      rewardBatchKey(0x31, i),
					Reward:          types.Uint64(uint64(i) * 1_000_003),
					Active:          i%13 != 0,
					ExpirationEpoch: uint64(i % 5),
					AddedSlot:       10,
				}
				switch i % 7 {
				case 0:
					account.DrepType = models.DrepTypeAlwaysAbstain
				case 1:
					account.DrepType = models.DrepTypeAlwaysNoConfidence
				case 2:
					account.DrepType = models.DrepTypeScriptHash
					account.Drep = drepKey(i)
				default:
					account.DrepType = models.DrepTypeAddrKeyHash
					account.Drep = drepKey(i)
				}
				require.NoError(t, db.CreateAccount(nil, account))
				for u := range 3 {
					txID := make([]byte, 32)
					binary.BigEndian.PutUint32(
						txID[28:],
						uint32(i*3+u),
					) //nolint:gosec
					deleted := 0
					if u == 2 {
						deleted = 99
					}
					_, err := raw.Exec(backendRebind(backend.name, `
INSERT INTO utxo (tx_id, output_idx, payment_key, staking_key, credential_tag,
    added_slot, deleted_slot, amount, payment_script)
VALUES (?, 0, ?, ?, 0, 1, ?, ?, FALSE)`),
						txID, rewardBatchKey(0x51, i*3+u), account.StakingKey,
						deleted, strconv.Itoa(10_000_000+i*17+u),
					)
					require.NoError(t, err)
				}
			}
			// Refresh the live stake aggregate of the active accounts the
			// way block application does, then leave some rows missing and
			// some stale, which the live read must not trust.
			txn := db.Transaction(true)
			require.NoError(t, txn.Do(func(txn *database.Txn) error {
				var credits []models.AccountRewardCredit
				for i := range accounts {
					if i%13 == 0 {
						continue
					}
					credits = append(credits, models.AccountRewardCredit{
						StakingKey: rewardBatchKey(0x31, i),
						Amount:     1,
						Slot:       20,
						SourceHash: rewardBatchKey(0x61, i),
					})
				}
				return db.AddAccountRewardsByCredential(credits, txn)
			}))
			require.NoError(t, db.Metadata().RebuildRewardLiveStake(100, nil))
			fastPathTypes := []uint64{
				models.DrepTypeAlwaysAbstain,
				models.DrepTypeAlwaysNoConfidence,
			}
			for _, expiryEpoch := range []uint64{0, 3} {
				_, wantByType := drepPowerOracle(t, raw, expiryEpoch)
				gotByType, err := db.GetDRepVotingPowerByType(
					fastPathTypes,
					expiryEpoch,
					nil,
				)
				require.NoError(t, err)
				for _, drepType := range fastPathTypes {
					require.Equal(
						t, wantByType[drepType], gotByType[drepType],
						"live-stake fast path, type %d expiry %d",
						drepType,
						expiryEpoch,
					)
				}
			}
			for i := 3; i < accounts; i += 29 {
				_, err := raw.Exec(backendRebind(backend.name,
					`DELETE FROM reward_live_stake WHERE staking_key = ?`),
					rewardBatchKey(0x31, i),
				)
				require.NoError(t, err)
			}
			for i := 5; i < accounts; i += 31 {
				_, err := raw.Exec(backendRebind(backend.name,
					`UPDATE reward_live_stake SET calculation_version = 0,
    utxo_stake = '1' WHERE staking_key = ?`),
					rewardBatchKey(0x31, i),
				)
				require.NoError(t, err)
			}
			refs := make([]models.StakeCredentialRef, 0, dreps*2)
			for d := range dreps {
				refs = append(refs,
					models.NewStakeCredentialRef(0, rewardBatchKey(0x41, d)),
					models.NewStakeCredentialRef(1, rewardBatchKey(0x41, d)),
				)
			}
			for _, expiryEpoch := range []uint64{0, 3} {
				wantCredential, wantType := drepPowerOracle(t, raw, expiryEpoch)
				got, err := db.GetDRepVotingPowerBatch(refs, expiryEpoch, nil)
				require.NoError(t, err)
				want := make(map[string]uint64)
				for _, ref := range refs {
					if value, ok := wantCredential[ref.MapKey()]; ok {
						want[ref.MapKey()] = value
					}
				}
				require.Equal(t, want, got, "expiry %d", expiryEpoch)
				types := []uint64{
					models.DrepTypeAlwaysAbstain, models.DrepTypeAlwaysNoConfidence,
				}
				gotType, err := db.GetDRepVotingPowerByType(
					types,
					expiryEpoch,
					nil,
				)
				require.NoError(t, err)
				for _, drepType := range types {
					require.Equal(
						t, wantType[drepType], gotType[drepType],
						"type %d expiry %d", drepType, expiryEpoch,
					)
				}
			}
		})
	}
}

func backendRebind(name, query string) string {
	if name != "postgres" {
		return query
	}
	n := 0
	out := make([]byte, 0, len(query)+8)
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			n++
			out = append(out, []byte("$"+strconv.Itoa(n))...)
			continue
		}
		out = append(out, query[i])
	}
	return string(out)
}
