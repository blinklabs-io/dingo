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
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/stretchr/testify/require"
)

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
		ls.rewardCreditFoldWG.Wait()
		rounds, err := db.Metadata().GetPendingRewardCreditRounds(nil)
		require.NoError(t, err)
		require.Empty(t, rounds)
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
