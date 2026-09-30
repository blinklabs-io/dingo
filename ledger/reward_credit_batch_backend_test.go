//go:build dingo_extra_plugins

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
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/mysql"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/postgres"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
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

func rewardBatchBackends() []rewardRaceBackend {
	return []rewardRaceBackend{
		{name: "sqlite", open: openSQLiteRewardRaceBackend},
		{name: "postgres", open: openPostgresRewardRaceBackend},
		{name: "mysql", open: openMySQLRewardRaceBackend},
	}
}

func rewardBatchKey(domain byte, index int) []byte {
	key := make([]byte, 28)
	key[0] = domain
	binary.BigEndian.PutUint32(key[24:], uint32(index)) //nolint:gosec
	return key
}

// dumpRewardBatchTables renders every column the credit and stake-input
// writers touch, in a stable order, so two databases can be compared as text.
func dumpRewardBatchTables(t *testing.T, raw *sql.DB) string {
	t.Helper()
	queries := []string{
		`SELECT credential_tag, staking_key, reward, active FROM account
ORDER BY credential_tag, staking_key`,
		`SELECT staking_key, credential_tag, tx_hash, amount, previous_reward,
    added_slot, withdrawal, post_snapshot FROM account_reward_delta
ORDER BY credential_tag, staking_key, tx_hash, added_slot`,
		`SELECT credential_tag, staking_key, pool_key_hash, utxo_stake,
    reward_stake, total_stake, registered, pool_delegation_slot, updated_slot,
    calculation_version FROM reward_live_stake
ORDER BY credential_tag, staking_key`,
		`SELECT epoch, pool_key_hash, credential_tag, staking_key, stake, owner,
    registered, captured_slot, boundary_slot FROM reward_stake_input
ORDER BY epoch, pool_key_hash, credential_tag, staking_key`,
	}
	var sb strings.Builder
	for _, query := range queries {
		rows, err := raw.Query(query)
		require.NoError(t, err)
		cols, err := rows.Columns()
		require.NoError(t, err)
		for rows.Next() {
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			require.NoError(t, rows.Scan(ptrs...))
			for i, v := range values {
				if b, ok := v.([]byte); ok {
					v = fmt.Sprintf("%x", b)
				}
				fmt.Fprintf(&sb, "%s=%v ", cols[i], v)
			}
			sb.WriteString("\n")
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		sb.WriteString("--\n")
	}
	return sb.String()
}

// seedRewardBatchAccounts creates the same accounts in db: every fifth is
// deregistered, and balances start non-zero so the credit adds to them.
func seedRewardBatchAccounts(t *testing.T, db *database.Database, n int) {
	t.Helper()
	for i := range n {
		require.NoError(t, db.CreateAccount(nil, &models.Account{
			StakingKey: rewardBatchKey(0x31, i),
			Pool:       rewardBatchKey(0x41, i%7),
			Reward:     types.Uint64(1_000 + i),
			Active:     true,
			AddedSlot:  10,
		}))
	}
}

// rewardBatchCredits spans more than one internal batch, repeats credentials
// (a leader and a member reward to the same account), repeats one credit
// exactly, and includes a zero amount.
func rewardBatchCredits(n int) []models.AccountRewardCredit {
	var credits []models.AccountRewardCredit
	for i := range n {
		if i%5 == 4 {
			continue
		}
		credits = append(credits, models.AccountRewardCredit{
			CredentialTag: 0,
			StakingKey:    rewardBatchKey(0x31, i),
			Amount:        uint64(7_000_000_007 + i),
			Slot:          5_000,
			SourceHash:    rewardBatchKey(0x51, i),
		})
		if i%3 == 0 {
			credits = append(credits, models.AccountRewardCredit{
				StakingKey: rewardBatchKey(0x31, i),
				Amount:     uint64(i) + 1,
				Slot:       5_000,
				SourceHash: rewardBatchKey(0x52, i),
			})
		}
	}
	credits = append(credits, credits[3], models.AccountRewardCredit{
		StakingKey: rewardBatchKey(0x31, 1),
		Amount:     0,
		Slot:       5_000,
		SourceHash: rewardBatchKey(0x53, 1),
	})
	return credits
}

// TestAddAccountRewardsByCredentialMatchesOneAtATime is the differential for
// the batched credit writer: on each backend, the batch and the
// one-credit-at-a-time writer leave byte-identical account, journal and
// live-stake rows, including a replay of the same credits.
func TestAddAccountRewardsByCredentialMatchesOneAtATime(t *testing.T) {
	t.Parallel()
	const accounts = 530
	for _, backend := range rewardBatchBackends() {
		t.Run(backend.name, func(t *testing.T) {
			t.Parallel()
			oneDB, oneRaw, _ := backend.open(t)
			batchDB, batchRaw, _ := backend.open(t)
			seedRewardBatchAccounts(t, oneDB, accounts)
			seedRewardBatchAccounts(t, batchDB, accounts)
			// Deregister every fifth account after seeding, so the batch
			// must skip none of them: they receive no credits.
			for i := 4; i < accounts; i += 5 {
				for _, db := range []*database.Database{oneDB, batchDB} {
					require.NoError(t, db.Metadata().DeactivateAccounts(
						nil,
						[]models.StakeCredentialRef{
							models.NewStakeCredentialRef(
								0, rewardBatchKey(0x31, i),
							),
						},
					),
					)
				}
			}
			credits := rewardBatchCredits(accounts)
			for replay := range 2 {
				for _, credit := range credits {
					require.NoError(t, oneDB.AddAccountRewardByCredential(
						credit.CredentialTag, credit.StakingKey,
						credit.Amount, credit.Slot, credit.SourceHash, nil,
					))
				}
				require.NoError(
					t, batchDB.AddAccountRewardsByCredential(credits, nil),
				)
				require.Equal(
					t, dumpRewardBatchTables(t, oneRaw),
					dumpRewardBatchTables(t, batchRaw),
					"pass %d", replay,
				)
			}
			// A credit to a deregistered account fails before writing.
			before := dumpRewardBatchTables(t, batchRaw)
			err := batchDB.AddAccountRewardsByCredential(
				[]models.AccountRewardCredit{
					{
						StakingKey: rewardBatchKey(0x31, 0), Amount: 5,
						Slot: 6_000, SourceHash: rewardBatchKey(0x54, 0),
					},
					{
						StakingKey: rewardBatchKey(0x31, 4), Amount: 5,
						Slot: 6_000, SourceHash: rewardBatchKey(0x54, 4),
					},
				}, nil,
			)
			require.ErrorIs(t, err, models.ErrAccountNotFound)
			require.Equal(t, before, dumpRewardBatchTables(t, batchRaw))
		})
	}
}

// TestSaveRewardStakeInputsBatchUpsertsLikeRowByRow pins the batched
// stake-input writer to the upsert contract of the one-row-per-statement
// shape it replaced: every (epoch, pool, credential) key holds its last row's
// values, a later save over existing rows updates them in place, and every
// input -- including a duplicate -- carries its row's ID.
func TestSaveRewardStakeInputsBatchUpsertsLikeRowByRow(t *testing.T) {
	t.Parallel()
	for _, backend := range rewardBatchBackends() {
		t.Run(backend.name, func(t *testing.T) {
			t.Parallel()
			db, _, _ := backend.open(t)
			build := func(stakeBase uint64) []*models.RewardStakeInput {
				var inputs []*models.RewardStakeInput
				for i := range 700 {
					inputs = append(inputs, &models.RewardStakeInput{
						Epoch:         7,
						PoolKeyHash:   rewardBatchKey(0x41, i%9),
						CredentialTag: uint8(i % 2), //nolint:gosec
						StakingKey:    rewardBatchKey(0x31, i),
						Stake:         types.Uint64(stakeBase + uint64(i)),
						Owner:         i%11 == 0,
						Registered:    i%13 != 0,
						CapturedSlot:  99 + stakeBase%7,
						BoundarySlot:  100,
					})
				}
				dup := *inputs[10]
				dup.Stake = types.Uint64(stakeBase + 42)
				dup.Owner = !dup.Owner
				return append(inputs, &dup)
			}
			var firstIDs []uint
			for pass, base := range []uint64{
				1_000, 9_000_000_000_000_000,
			} {
				inputs := build(base)
				require.NoError(
					t, db.Metadata().SaveRewardStakeInputs(inputs, nil),
				)
				want := make(map[string]*models.RewardStakeInput)
				for index, input := range inputs {
					require.NotZero(t, input.ID, "pass %d", pass)
					if pass == 1 {
						require.Equal(
							t, firstIDs[index], input.ID,
							"an upsert keeps the row",
						)
					}
					want[fmt.Sprintf(
						"%x/%d/%x", input.PoolKeyHash, input.CredentialTag,
						input.StakingKey,
					)] = input
				}
				require.Equal(t, inputs[10].ID, inputs[len(inputs)-1].ID)
				got, err := db.Metadata().GetRewardStakeInputs(7, nil)
				require.NoError(t, err)
				require.Len(t, got, len(want))
				for _, row := range got {
					expect := want[fmt.Sprintf(
						"%x/%d/%x", row.PoolKeyHash, row.CredentialTag,
						row.StakingKey,
					)]
					require.NotNil(t, expect)
					require.Equal(t, expect.ID, row.ID)
					require.Equal(t, expect.Stake, row.Stake)
					require.Equal(t, expect.Owner, row.Owner)
					require.Equal(t, expect.Registered, row.Registered)
					require.Equal(t, expect.CapturedSlot, row.CapturedSlot)
					require.Equal(t, expect.BoundarySlot, row.BoundarySlot)
				}
				if pass == 0 {
					for _, input := range inputs {
						firstIDs = append(firstIDs, input.ID)
					}
				}
			}
		})
	}
}

// TestPendingRewardCreditStoreAcrossBackends pins the pending reward round
// storage and the reads that add a pending round's credits, on each backend:
// the live stake read and DRep voting power include exactly the round's
// spendable, unguarded outputs, per-credential lookups and journal checks
// find the right rows, a rollback drops a round applied after its target,
// and the registration-event and rollback-recheck feeds return their
// credentials.
func TestPendingRewardCreditStoreAcrossBackends(t *testing.T) {
	t.Parallel()
	for _, backend := range rewardBatchBackends() {
		t.Run(backend.name, func(t *testing.T) {
			t.Parallel()
			db, raw, _ := backend.open(t)
			meta := db.Metadata()
			pool := rewardBatchKey(0x41, 1)
			drep := rewardBatchKey(0x42, 1)
			const n = 40
			for i := range n {
				require.NoError(t, db.CreateAccount(nil, &models.Account{
					StakingKey: rewardBatchKey(0xf1, i),
					Pool:       pool,
					Drep:       drep,
					DrepType:   models.DrepTypeAddrKeyHash,
					Reward:     types.Uint64(100),
					Active:     true,
					AddedSlot:  10,
				}))
			}
			var credits []models.AccountRewardCredit
			for i := range n {
				credits = append(credits, models.AccountRewardCredit{
					StakingKey: rewardBatchKey(0xf1, i),
					Amount:     1,
					Slot:       20,
					SourceHash: rewardBatchKey(0x61, i),
				})
			}
			require.NoError(t, db.AddAccountRewardsByCredential(credits, nil))
			var outputs []*models.RewardAccountOutput
			var want uint64
			for i := range n {
				output := &models.RewardAccountOutput{
					Epoch:         8,
					CredentialTag: 0,
					StakingKey:    rewardBatchKey(0xf1, i),
					PoolKeyHash:   pool,
					RewardType:    "member",
					Amount:        types.Uint64(1_000_000 + uint64(i)),
					Spendable:     i%5 != 0,
					Guarded:       i%7 == 0,
					CapturedSlot:  30,
					BoundarySlot:  50,
				}
				if output.Spendable && !output.Guarded {
					want += uint64(output.Amount)
				}
				outputs = append(outputs, output)
			}
			require.NoError(t, meta.SaveRewardAccountOutputs(outputs, nil))
			liveTotal := func() uint64 {
				inputs, err := meta.GetLiveStakeInputsForPools(
					[][]byte{pool}, 0, nil,
				)
				require.NoError(t, err)
				require.Len(t, inputs, n)
				var total uint64
				for _, input := range inputs {
					total += uint64(input.Stake)
				}
				return total
			}
			powerTotal := func() uint64 {
				power, err := db.GetDRepVotingPowerBatch(
					[]models.StakeCredentialRef{
						models.NewStakeCredentialRef(0, drep),
					}, 0, nil,
				)
				require.NoError(t, err)
				return power[models.NewStakeCredentialRef(0, drep).MapKey()]
			}
			liveBefore, powerBefore := liveTotal(), powerTotal()
			require.NoError(t, meta.SetPendingRewardCreditRounds(
				[]models.RewardCreditRound{
					{SnapshotEpoch: 8, BoundarySlot: 50},
				},
				nil,
			))
			require.Equal(t, liveBefore+want, liveTotal())
			require.Equal(t, powerBefore+want, powerTotal())

			got, err := meta.GetRewardAccountOutputsForCredential(
				[]uint64{8}, 0, rewardBatchKey(0xf1, 3), nil,
			)
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Equal(t, uint64(1_000_003), uint64(got[0].Amount))
			ranged, err := meta.GetRewardAccountOutputsInPoolKeyHashRange(
				8, pool, pool, nil,
			)
			require.NoError(t, err)
			require.Len(t, ranged, n)
			applied, err := meta.RewardCreditsAlreadyApplied(
				[]models.AccountRewardCredit{credits[4], {
					StakingKey: rewardBatchKey(0xf1, 4),
					Amount:     1, Slot: 20, SourceHash: rewardBatchKey(0x62, 4),
				}}, nil,
			)
			require.NoError(t, err)
			require.Equal(t, []bool{true, false}, applied)

			require.NoError(t, meta.DeleteRewardStateAfterSlot(60, nil))
			rounds, err := meta.GetPendingRewardCreditRounds(nil)
			require.NoError(t, err)
			require.Len(t, rounds, 1, "a rollback above the boundary keeps it")
			require.NoError(t, meta.DeleteRewardStateAfterSlot(49, nil))
			rounds, err = meta.GetPendingRewardCreditRounds(nil)
			require.NoError(t, err)
			require.Empty(t, rounds, "a rollback below the boundary drops it")
			require.Equal(t, liveBefore, liveTotal())

			_, err = raw.Exec(backendRebind(backend.name, `
INSERT INTO deregistration (added_slot, staking_key, credential_tag, amount)
VALUES (?, ?, 0, '2000000')`), 70, rewardBatchKey(0xf1, 9))
			require.NoError(t, err)
			changed, err := meta.GetStakeCredentialsWithRegistrationEvents(
				65, 75, nil,
			)
			require.NoError(t, err)
			require.Equal(t, []models.StakeCredentialRef{
				models.NewStakeCredentialRef(0, rewardBatchKey(0xf1, 9)),
			}, changed)
			require.NoError(t, meta.RestoreAccountStateAtSlot(5, nil))
			restored, err := meta.TakeRewardEligibilityRecheck(nil)
			require.NoError(t, err)
			require.Len(
				t,
				restored,
				n,
				"a rollback records every restored account",
			)
			again, err := meta.TakeRewardEligibilityRecheck(nil)
			require.NoError(t, err)
			require.Empty(t, again)
		})
	}
}

// TestBoundaryRewardApplicationMatchesEagerPathAcrossBackends is the
// differential of the boundary's reward application on each backend: a round
// precomputed per pool, applied with its credits pending and written in the
// background, leaves exactly the account, journal, live-stake, pot and output
// rows of the same round precomputed in one pass and credited inside the
// boundary transaction -- including delegators deregistered after the
// precompute ran.
func TestBoundaryRewardApplicationMatchesEagerPathAcrossBackends(t *testing.T) {
	t.Parallel()
	const pools, delegators = 9, 6
	deregistered := []uint64{2, 17, 41}
	run := func(
		t *testing.T,
		backend rewardRaceBackend,
		perPool bool,
	) string {
		db, raw, _ := backend.open(t)
		ls := &LedgerState{
			db:         db,
			currentEra: eras.ShelleyEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: newRewardCalculationTestNodeConfig(t),
				Logger: slog.New(
					slog.NewTextHandler(io.Discard, nil),
				),
			},
		}
		seedMultiPoolRewardInputs(t, db, pools, delegators, 7)
		if perPool {
			ls.rewardPrecomputeChunkPoolsOverride = 2
			require.NoError(
				t,
				ls.runChunkedStakeRewardPrecompute(4, 200, 1_200),
			)
		} else {
			txn := db.Transaction(true)
			require.NoError(t, txn.Do(func(txn *database.Txn) error {
				return ls.precomputeStakeRewards(txn, 4, 200, 1_200)
			}))
		}
		for _, index := range deregistered {
			ref := models.NewStakeCredentialRef(
				0, chunkedFixtureCredential(0x60, index),
			)
			require.NoError(t, db.Metadata().DeactivateAccounts(
				nil, []models.StakeCredentialRef{ref},
			))
			_, err := raw.Exec(backendRebind(backend.name, `
INSERT INTO deregistration (added_slot, staking_key, credential_tag, amount)
VALUES (?, ?, 0, '2000000')`), 300+index, ref.Key)
			require.NoError(t, err)
		}
		txn := db.Transaction(true)
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			return ls.applyStakeRewards(txn, 4, 1_200)
		}))
		settleRewardCredits(t, ls)
		var sb strings.Builder
		for _, query := range []string{
			`SELECT slot, treasury, reserves FROM network_state ORDER BY slot`,
			`SELECT epoch, treasury, reserves, fees, rewards FROM reward_ada_pots
ORDER BY epoch`,
			`SELECT pool_key_hash, total_reward, leader_reward, member_reward_total,
    undistributed, unspendable FROM reward_pool_output ORDER BY pool_key_hash`,
			`SELECT credential_tag, staking_key, pool_key_hash, reward_type, amount,
    spendable, guarded FROM reward_account_output
ORDER BY credential_tag, staking_key, pool_key_hash, reward_type`,
		} {
			rows, err := raw.Query(query)
			require.NoError(t, err)
			cols, err := rows.Columns()
			require.NoError(t, err)
			for rows.Next() {
				values := make([]any, len(cols))
				ptrs := make([]any, len(cols))
				for i := range values {
					ptrs[i] = &values[i]
				}
				require.NoError(t, rows.Scan(ptrs...))
				for _, v := range values {
					if b, ok := v.([]byte); ok {
						v = fmt.Sprintf("%x", b)
					}
					fmt.Fprintf(&sb, "%v|", v)
				}
				sb.WriteString("\n")
			}
			require.NoError(t, rows.Close())
		}
		return sb.String() + dumpRewardBatchTables(t, raw)
	}
	for _, backend := range rewardBatchBackends() {
		t.Run(backend.name, func(t *testing.T) {
			t.Parallel()
			eager := run(t, backend, false)
			perPool := run(t, backend, true)
			require.Equal(t, eager, perPool)
		})
	}
}

// poolKeyHashFill returns a 28-byte pool-key hash filled with a single byte,
// which sorts by that byte across sqlite, PostgreSQL, and MySQL alike (all
// three order a fixed-length binary column bytewise).
func poolKeyHashFill(b byte) []byte {
	h := make([]byte, 28)
	for i := range h {
		h[i] = b
	}
	return h
}

// TestGetRewardStakeInputsInPoolKeyHashRangeAcrossBackends proves the chunked
// precompute's pool-batch query returns exactly the rows within an inclusive
// [lo, hi] pool_key_hash range -- neither leaking a neighboring pool's rows
// nor dropping a boundary pool's own rows -- identically on sqlite,
// PostgreSQL, and MySQL. This is the differential proof for the one query
// the chunked design adds; TestRewardPrecomputeWriteCannotOutliveConcurrentRollback
// covers the concurrency property (guard-then-write ordering against a
// racing rollback) on the same three backends.
func TestGetRewardStakeInputsInPoolKeyHashRangeAcrossBackends(t *testing.T) {
	t.Parallel()

	backends := []rewardRaceBackend{
		{name: "sqlite", open: openSQLiteRewardRaceBackend},
		{name: "postgres", open: openPostgresRewardRaceBackend},
		{name: "mysql", open: openMySQLRewardRaceBackend},
	}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			t.Parallel()
			db, _, _ := backend.open(t)
			meta := db.Metadata()

			const epoch = uint64(5)
			pools := []byte{0x11, 0x22, 0x33, 0x44}
			var inputs []*models.RewardStakeInput
			for _, pool := range pools {
				poolKey := poolKeyHashFill(pool)
				for i := range 2 {
					inputs = append(inputs, &models.RewardStakeInput{
						Epoch:         epoch,
						PoolKeyHash:   poolKey,
						CredentialTag: 0,
						StakingKey: poolKeyHashFillWithSuffix(
							pool, byte(i),
						),
						Stake:        100,
						Registered:   true,
						CapturedSlot: 10,
						BoundarySlot: 9,
					})
				}
			}
			require.NoError(t, meta.SaveRewardStakeInputs(inputs, nil))

			assertRange := func(
				lo, hi byte, wantPools ...byte,
			) {
				t.Helper()
				rows, err := meta.GetRewardStakeInputsInPoolKeyHashRange(
					epoch, poolKeyHashFill(lo), poolKeyHashFill(hi), nil,
				)
				require.NoError(t, err)
				gotPools := make(map[byte]int)
				for _, row := range rows {
					require.Len(t, row.PoolKeyHash, 28)
					gotPools[row.PoolKeyHash[0]]++
				}
				wantCounts := make(map[byte]int, len(wantPools))
				for _, p := range wantPools {
					wantCounts[p] = 2
				}
				require.Equal(t, wantCounts, gotPools)
			}

			// A single pool at the low end of the stored set.
			assertRange(0x11, 0x11, 0x11)
			// A single pool at the high end.
			assertRange(0x44, 0x44, 0x44)
			// A contiguous middle range covering two pools.
			assertRange(0x22, 0x33, 0x22, 0x33)
			// The whole stored range.
			assertRange(0x11, 0x44, 0x11, 0x22, 0x33, 0x44)
			// A range strictly between two stored pools: no rows at all,
			// not the nearest neighbor's.
			assertRange(0x23, 0x32)
			// A range entirely below every stored pool.
			assertRange(0x01, 0x10)
			// A range entirely above every stored pool.
			assertRange(0x45, 0xf0)
		})
	}
}

// poolKeyHashFillWithSuffix gives each pool's rows distinct staking keys
// (the fill byte plus an index byte at the end) so SaveRewardStakeInputs'
// (epoch, pool_key_hash, credential_tag, staking_key) uniqueness never
// collides two rows meant to coexist within the same pool.
func poolKeyHashFillWithSuffix(fill, suffix byte) []byte {
	h := poolKeyHashFill(fill)
	h[len(h)-1] = suffix
	return h
}

// rewardRaceBackend opens a metadata backend plus a raw connection that can
// seed certificate history the database API only writes from real blocks.
type rewardRaceBackend struct {
	name string
	open func(t *testing.T) (*database.Database, *sql.DB, string)
}

// A rollback's truncation and a precompute write are separate transactions.
// SQLite's single write connection orders them; Postgres and MySQL run them
// concurrently, so the ledger itself must keep a write that passed the
// rollback guard from committing after the truncation that should delete it.
func TestRewardPrecomputeWriteCannotOutliveConcurrentRollback(
	t *testing.T,
) {
	t.Parallel()

	backends := []rewardRaceBackend{
		{name: "sqlite", open: openSQLiteRewardRaceBackend},
		{name: "postgres", open: openPostgresRewardRaceBackend},
		{name: "mysql", open: openMySQLRewardRaceBackend},
	}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			t.Parallel()
			db, raw, placeholder := backend.open(t)
			testRewardPrecomputeWriteRacesRollback(t, db, raw, placeholder)
		})
	}
}

func testRewardPrecomputeWriteRacesRollback(
	t *testing.T,
	db *database.Database,
	raw *sql.DB,
	dialect string,
) {
	seedRewardPrecomputeTimingInputs(t, db, 6)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))
	nonce := testHashBytes("reward-epoch")
	require.NoError(t, db.SetEpoch(
		200, 3, nonce, nil, nil, nil,
		eras.ShelleyEraDesc.Id, 1, 1_000, nil,
	))
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newRewardCalculationTestNodeConfig(t),
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	epoch, err := db.Metadata().GetEpoch(3, nil)
	require.NoError(t, err)
	ls.currentEpoch = *epoch
	ls.currentEra = eras.ShelleyEraDesc
	cutoff, err := ls.rewardPrefilterSlot(db.Metadata(), nil, 3)
	require.NoError(t, err)
	member := rewardCalcHash(0x6a)
	// The abandoned chain deregisters member after the rollback point and
	// before the RUPD slot, so its precompute excludes member.
	seedRewardRaceStakeCert(
		t, raw, dialect, 21, member, 150,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	seedRewardRaceStakeCert(
		t, raw, dialect, 22, member, cutoff-5,
		uint(lcommon.CertificateTypeStakeDeregistration),
	)
	ancestor := chain.RawBlock{
		Slot: cutoff - 10, Hash: testHashBytes("race-ancestor"),
		BlockNumber: 1, Type: 1, Cbor: []byte{0x80},
	}
	abandoned := chain.RawBlock{
		Slot: cutoff + 1, Hash: testHashBytes("race-abandoned"),
		PrevHash:    ancestor.Hash,
		BlockNumber: 2, Type: 1, Cbor: []byte{0x80},
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(
		[]chain.RawBlock{ancestor, abandoned},
	))
	for _, block := range []chain.RawBlock{ancestor, abandoned} {
		require.NoError(t, db.SetBlockNonce(
			block.Hash, block.Slot, nonce, true, nil,
		))
	}
	ls.currentTip = ochainsync.Tip{
		Point:       ocommon.NewPoint(abandoned.Slot, abandoned.Hash),
		BlockNumber: abandoned.BlockNumber,
	}
	require.NoError(t, db.SetTip(ls.currentTip, nil))

	rollbackDone := make(chan error, 1)
	var hookCalls atomic.Int32
	ls.rewardPrecomputeBeforeSaveHook = func() {
		if hookCalls.Add(1) > 1 {
			return
		}
		go func() {
			rollbackDone <- ls.rollbackWithBlocks(
				ocommon.NewPoint(ancestor.Slot, ancestor.Hash), nil, false,
			)
		}()
		// The guard has passed and the outputs are not written yet. A
		// rollback that finished now would have truncated before this
		// write's rows exist.
		assert.Never(t, func() bool { return len(rollbackDone) > 0 },
			2*time.Second, 10*time.Millisecond,
			"a rollback completed inside a precompute write that passed "+
				"its guard")
	}
	require.NoError(t, ls.precomputeStakeRewardsAfterEpochTransition(
		event.EpochTransitionEvent{
			NewEpoch:     3,
			BoundarySlot: abandoned.Slot,
			EpochNonce:   nonce,
		},
	))
	require.Equal(t, int32(1), hookCalls.Load(),
		"the abandoned-chain write reached the seam")
	require.NoError(t, testutil.RequireReceive(
		t, rollbackDone, 30*time.Second, "rollback did not finish",
	))
	ls.rewardPrecomputeWG.Wait()

	outputs, err := db.Metadata().GetRewardAccountOutputs(1, nil)
	require.NoError(t, err)
	assert.Empty(t, outputs,
		"an output from the abandoned chain survived the rollback")

	replacement := ocommon.NewPoint(
		cutoff+1, testHashBytes("race-replacement"),
	)
	ls.Lock()
	ls.currentTip = ochainsync.Tip{Point: replacement, BlockNumber: 2}
	ls.Unlock()
	ls.maybeQueueStakeRewardPrecomputeRetry(replacement.Slot)
	ls.rewardPrecomputeWG.Wait()

	var wantMember uint64
	readTxn := db.Transaction(false)
	require.NoError(t, readTxn.Do(func(txn *database.Txn) error {
		want, ok, err := ls.calculateStakeRewardApplication(
			txn, 4, replacement.Slot, 1_200, false,
		)
		require.NoError(t, err)
		require.True(t, ok)
		for _, output := range want.accountOutputs {
			if string(output.StakingKey) == string(member) {
				wantMember += uint64(output.Amount)
			}
		}
		return nil
	}))
	require.NotZero(t, wantMember,
		"control: the surviving chain pays member")
	writeTxn := db.Transaction(true)
	require.NoError(t, writeTxn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, 4, 1_200)
	}))
	settleRewardCredits(t, ls)
	account, err := db.GetAccountByCredential(0, member, true, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	require.Equal(t, wantMember, uint64(account.Reward),
		"the boundary must credit member from the surviving chain")
}

func seedRewardRaceStakeCert(
	t *testing.T,
	raw *sql.DB,
	dialect string,
	id uint,
	stakingKey []byte,
	slot uint64,
	certType uint,
) {
	t.Helper()
	table := `"transaction"`
	bind := func(n int) string { return fmt.Sprintf("$%d", n) }
	if dialect == "mysql" {
		table = "`transaction`"
		bind = func(int) string { return "?" }
	} else if dialect == "sqlite" {
		bind = func(int) string { return "?" }
	}
	hash := make([]byte, 32)
	binary.BigEndian.PutUint64(hash[24:], uint64(id))
	_, err := raw.Exec(fmt.Sprintf(
		"INSERT INTO %s (id, hash, slot, block_index) VALUES (%s, %s, %s, 0)",
		table, bind(1), bind(2), bind(3),
	), id, hash, slot)
	require.NoError(t, err)
	_, err = raw.Exec(fmt.Sprintf(
		"INSERT INTO certs (id, transaction_id, cert_index, slot, cert_type) "+
			"VALUES (%s, %s, 0, %s, %s)",
		bind(1), bind(2), bind(3), bind(4),
	), id, id, slot, certType)
	require.NoError(t, err)
	certTable := "stake_registration"
	if certType == uint(lcommon.CertificateTypeStakeDeregistration) {
		certTable = "stake_deregistration"
	}
	_, err = raw.Exec(fmt.Sprintf(
		"INSERT INTO %s "+
			"(id, staking_key, credential_tag, certificate_id, added_slot) "+
			"VALUES (%s, %s, 0, %s, %s)",
		certTable, bind(1), bind(2), bind(3), bind(4),
	), id, stakingKey, id, slot)
	require.NoError(t, err)
}

func openSQLiteRewardRaceBackend(
	t *testing.T,
) (*database.Database, *sql.DB, string) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	return db, raw, "sqlite"
}

// premigrateRewardRaceBackend migrates a fresh schema under a lock keyed to
// that schema. The provider's migration lock is one server-wide key with a 30s
// wait, so tests migrating fresh schemas in parallel, in this package or any
// other, time out behind each other; with the schema already current the
// provider holds that lock only for its version check.
func premigrateRewardRaceBackend(
	t *testing.T,
	raw *sql.DB,
	dialect sqlstore.Dialect,
	registry func() ([]migrations.Migration, error),
	namespace string,
) {
	t.Helper()
	versions, err := registry()
	require.NoError(t, err)
	digest := sha256.Sum256([]byte(namespace))
	runner := migrations.Runner{
		DB:       raw,
		Dialect:  dialect.Name(),
		Registry: versions,
		Locker: migrations.NewAdvisoryLocker(
			dialect.Name(),
			int64(binary.BigEndian.Uint64(digest[:8])),
			30*time.Second,
		),
		Rebind: dialect.Rebind,
	}
	require.NoError(t, runner.Run(t.Context()))
}

func openPostgresRewardRaceBackend(
	t *testing.T,
) (*database.Database, *sql.DB, string) {
	t.Helper()
	if os.Getenv("POSTGRES_PASSWORD") == "" &&
		os.Getenv("POSTGRES_DSN") == "" {
		t.Skip(
			"postgres not configured (set POSTGRES_PASSWORD or POSTGRES_DSN)",
		)
	}
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "host=" + storagetest.EscapeLibpqValue(
			envOr("POSTGRES_HOST", "localhost"),
		) +
			" port=" + storagetest.EscapeLibpqValue(
			envOr("POSTGRES_PORT", "5432"),
		) +
			" user=" + storagetest.EscapeLibpqValue(
			envOr("POSTGRES_USER", "postgres"),
		) +
			" password=" + storagetest.EscapeLibpqValue(
			os.Getenv("POSTGRES_PASSWORD"),
		) +
			" dbname=" + storagetest.EscapeLibpqValue(
			envOr("POSTGRES_DATABASE", "dingo_test"),
		) +
			" sslmode=" + storagetest.EscapeLibpqValue(
			envOr("POSTGRES_SSLMODE", "disable"),
		)
	}
	schema := fmt.Sprintf("reward_race_%d", time.Now().UnixNano())
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(t.Context()))
	_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
		_ = admin.Close()
	})
	scoped := storagetest.PostgresDSNWithSearchPath(dsn, schema)
	raw, err := sql.Open("pgx", scoped)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	premigrateRewardRaceBackend(
		t, raw, sqlstore.PostgresDialect(), migrations.PostgresRegistry, schema,
	)
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{
			DataDir: filepath.Join(t.TempDir(), "blob"),
		},
		Metadata: dbtest.StorageProvider{
			Name:     "postgres",
			Config:   map[string]any{"dsn": scoped},
			Register: postgres.RegisterProvider,
		},
	})
	require.NoError(t, err)
	return db, raw, "postgres"
}

func openMySQLRewardRaceBackend(
	t *testing.T,
) (*database.Database, *sql.DB, string) {
	t.Helper()
	if os.Getenv("MYSQL_ROOT_PASSWORD") == "" &&
		os.Getenv("MYSQL_DSN") == "" {
		t.Skip("mysql not configured (set MYSQL_ROOT_PASSWORD or MYSQL_DSN)")
	}
	rootDSN := os.Getenv("MYSQL_DSN")
	if rootDSN == "" {
		cfg := mysqldriver.Config{
			User:   "root",
			Passwd: os.Getenv("MYSQL_ROOT_PASSWORD"),
			Net:    "tcp",
			Addr: envOr("MYSQL_HOST", "localhost") + ":" +
				envOr("MYSQL_PORT", "3306"),
			ParseTime:            true,
			AllowNativePasswords: true,
		}
		rootDSN = cfg.FormatDSN()
	}
	dbName := fmt.Sprintf("reward_race_%d", time.Now().UnixNano())
	admin, err := sql.Open("mysql", rootDSN)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(t.Context()))
	_, err = admin.Exec("CREATE DATABASE `" + dbName + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE `" + dbName + "`")
		_ = admin.Close()
	})
	parsed, err := mysqldriver.ParseDSN(rootDSN)
	require.NoError(t, err)
	parsed.DBName = dbName
	raw, err := sql.Open("mysql", parsed.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	premigrateRewardRaceBackend(
		t, raw, sqlstore.MySQLDialect(), migrations.MySQLRegistry, dbName,
	)
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{
			DataDir: filepath.Join(t.TempDir(), "blob"),
		},
		Metadata: dbtest.StorageProvider{
			Name:     "mysql",
			Config:   map[string]any{"dsn": parsed.FormatDSN()},
			Register: mysql.RegisterProvider,
		},
	})
	require.NoError(t, err)
	return db, raw, "mysql"
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
