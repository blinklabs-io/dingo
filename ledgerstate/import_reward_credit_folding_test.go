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

package ledgerstate

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

func TestImportAccountsFoldsCoveredRewardCredits(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	meta := db.Metadata()
	key := hash28(0x31)
	withdrawn := hash28(0x32)
	other := hash28(0x33)
	for _, k := range [][]byte{key, withdrawn, other} {
		require.NoError(
			t,
			db.CreateAccount(
				context.Background(),
				nil,
				&models.Account{StakingKey: k, Reward: 20, Active: true},
			),
		)
	}
	require.NoError(
		t,
		meta.SetGenesisTransaction(
			bytes.Repeat([]byte{0x41}, 32), nil, nil, nil,
		),
	)
	require.NoError(
		t,
		meta.ApplyAccountRewardWithdrawal(
			0,
			withdrawn,
			20,
			220,
			bytes.Repeat([]byte{0x41}, 32),
			nil,
		),
	)
	for _, r := range []models.RewardCreditRound{{SnapshotEpoch: 1, BoundarySlot: 100}, {SnapshotEpoch: 2, BoundarySlot: 200}, {SnapshotEpoch: 3, BoundarySlot: 300}} {
		require.NoError(t, meta.AddAppliedRewardCreditRound(r, nil))
	}
	outputs := []*models.RewardAccountOutput{
		{
			Epoch:        1,
			StakingKey:   key,
			PoolKeyHash:  hash28(0x51),
			RewardType:   "member",
			Amount:       10,
			Spendable:    true,
			BoundarySlot: 100,
		},
		{
			Epoch:        2,
			StakingKey:   key,
			PoolKeyHash:  hash28(0x51),
			RewardType:   "member",
			Amount:       20,
			Spendable:    true,
			BoundarySlot: 200,
		},
		{
			Epoch:        3,
			StakingKey:   key,
			PoolKeyHash:  hash28(0x51),
			RewardType:   "member",
			Amount:       30,
			Spendable:    true,
			BoundarySlot: 300,
		},
		{
			Epoch:        1,
			StakingKey:   withdrawn,
			PoolKeyHash:  hash28(0x51),
			RewardType:   "member",
			Amount:       20,
			Spendable:    true,
			BoundarySlot: 100,
		},
		{
			Epoch:        1,
			StakingKey:   other,
			PoolKeyHash:  hash28(0x51),
			RewardType:   "member",
			Amount:       40,
			Spendable:    true,
			BoundarySlot: 100,
		},
	}
	require.NoError(t, meta.SaveRewardAccountOutputs(outputs, nil))
	require.NoError(t, meta.FoldRewardAccountOutputs([]uint64{2}, 0, key, nil))
	cfg := ImportConfig{
		Database: db,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	require.NoError(
		t,
		importAccounts(context.Background(), cfg, []ParsedAccount{
			{
				StakingKey: Credential{Type: CredentialTypeKey, Hash: key},
				Reward:     75,
				Active:     true,
			},
			{
				StakingKey: Credential{
					Type: CredentialTypeKey,
					Hash: withdrawn,
				},
				Reward: 0,
				Active: true,
			},
		}, 250),
	)
	account, err := meta.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(t, types.Uint64(75), account.Reward)
	pending, err := meta.GetPendingRewardAccountOutputsForCredential(
		0,
		key,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, uint64(3), pending[0].Epoch)
	account, err = meta.GetAccountByCredential(0, withdrawn, true, nil)
	require.NoError(t, err)
	require.Zero(t, account.Reward)
	pending, err = meta.GetPendingRewardAccountOutputsForCredential(
		0,
		withdrawn,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, pending)
	pending, err = meta.GetPendingRewardAccountOutputsForCredential(
		0,
		other,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.NoError(
		t,
		foldImportedRewardCredits(context.Background(), cfg, 250),
	)
	pending, err = meta.GetPendingRewardAccountOutputsForCredential(
		0,
		other,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, pending)
	account, err = meta.GetAccountByCredential(0, other, true, nil)
	require.NoError(t, err)
	require.Equal(t, types.Uint64(20), account.Reward)
	pending, err = meta.GetPendingRewardAccountOutputsForCredential(0, key, nil)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, uint64(3), pending[0].Epoch)
	rounds, err := meta.GetPendingRewardCreditRounds(nil)
	require.NoError(t, err)
	require.Len(t, rounds, 3)
}

func TestImportAccountRewardFoldFailureRollsBackBalance(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	meta := db.Metadata()
	key := hash28(0x61)
	require.NoError(
		t,
		db.CreateAccount(
			context.Background(),
			nil,
			&models.Account{StakingKey: key, Reward: 7, Active: true},
		),
	)
	require.NoError(
		t,
		meta.AddAppliedRewardCreditRound(
			models.RewardCreditRound{SnapshotEpoch: 1, BoundarySlot: 100},
			nil,
		),
	)
	require.NoError(
		t,
		meta.SaveRewardAccountOutputs(
			[]*models.RewardAccountOutput{
				{
					Epoch:        1,
					StakingKey:   key,
					PoolKeyHash:  hash28(0x71),
					RewardType:   "member",
					Amount:       3,
					Spendable:    true,
					BoundarySlot: 100,
				},
			},
			nil,
		),
	)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	defer raw.Close()
	_, err = raw.Exec(
		`CREATE TRIGGER reject_import_credit_fold BEFORE UPDATE OF folded ON reward_account_output WHEN NEW.folded = 1 BEGIN SELECT RAISE(ABORT, 'fold rejected'); END`,
	)
	require.NoError(t, err)
	cfg := ImportConfig{
		Database: db,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	err = importAccounts(
		context.Background(),
		cfg,
		[]ParsedAccount{
			{
				StakingKey: Credential{Type: CredentialTypeKey, Hash: key},
				Reward:     10,
				Active:     true,
			},
		},
		150,
	)
	require.ErrorContains(t, err, "fold rejected")
	account, err := meta.GetAccountByCredential(0, key, true, nil)
	require.NoError(t, err)
	require.Equal(t, types.Uint64(7), account.Reward)
	pending, err := meta.GetPendingRewardAccountOutputsForCredential(
		0,
		key,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, pending, 1)
}

func TestImportCoveredCreditDrainResumesBeforeCertCheckpoint(t *testing.T) {
	t.Parallel()
	for _, legacyCertCheckpoint := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "interrupted drain", true: "legacy cert checkpoint"}[legacyCertCheckpoint],
			func(t *testing.T) {
				t.Parallel()
				db, err := dbtest.NewDatabase(
					t,
					&database.Config{DataDir: t.TempDir()},
				)
				require.NoError(t, err)
				meta := db.Metadata()
				signedKey := hash28(0x91)
				require.NoError(
					t,
					ImportLedgerState(
						context.Background(),
						resumeImportConfig(t, db, 0x81, signedKey, 75, ""),
					),
				)
				require.NoError(
					t,
					meta.AddAppliedRewardCreditRound(
						models.RewardCreditRound{
							SnapshotEpoch: 98,
							BoundarySlot:  99000,
						},
						nil,
					),
				)
				const foldBatchSize = 2
				outputs := make([]*models.RewardAccountOutput, 2*foldBatchSize+1)
				for i := range outputs {
					key := hash28(0xa1)
					binary.BigEndian.PutUint64(key[:8], uint64(i))
					outputs[i] = &models.RewardAccountOutput{
						Epoch:        98,
						StakingKey:   key,
						PoolKeyHash:  hash28(0xb1),
						RewardType:   "member",
						Amount:       1,
						Spendable:    true,
						BoundarySlot: 99000,
					}
				}
				require.NoError(t, meta.SaveRewardAccountOutputs(outputs, nil))
				cfg := resumeImportConfig(
					t,
					db,
					0x81,
					signedKey,
					75,
					"covered-credit-resume",
				)
				cfg.rewardCreditFoldBatchSize = foldBatchSize
				raw, err := dbtest.RawSQLiteMetadata(t, db)
				require.NoError(t, err)
				defer raw.Close()
				if legacyCertCheckpoint {
					require.NoError(
						t,
						setCheckpoint(
							context.Background(),
							cfg,
							models.ImportPhaseCertState,
						),
					)
				} else {
					_, err = raw.Exec(fmt.Sprintf(`CREATE TRIGGER reject_last_covered_credit BEFORE UPDATE OF folded ON reward_account_output WHEN NEW.id = %d AND NEW.folded = 1 BEGIN SELECT RAISE(ABORT, 'last fold rejected'); END`, outputs[len(outputs)-1].ID))
					require.NoError(t, err)
					err = ImportLedgerState(context.Background(), cfg)
					require.ErrorContains(t, err, "last fold rejected")
					checkpoint, err := meta.GetImportCheckpoint(cfg.ImportKey, nil)
					require.NoError(t, err)
					require.NotNil(t, checkpoint)
					require.False(t, models.IsPhaseCompleted(checkpoint.Phase, models.ImportPhaseCertState))
					var folded int
					require.NoError(t, raw.QueryRow(`SELECT count(*) FROM reward_account_output WHERE epoch=98 AND folded`).Scan(&folded))
					require.Equal(t, 2*foldBatchSize, folded)
					_, err = raw.Exec(`DROP TRIGGER reject_last_covered_credit`)
					require.NoError(t, err)
				}
				require.NoError(t, ImportLedgerState(context.Background(), cfg))
				var folded int
				require.NoError(
					t,
					raw.QueryRow(`SELECT count(*) FROM reward_account_output WHERE epoch=98 AND folded`).
						Scan(&folded),
				)
				require.Equal(t, len(outputs), folded)
				account, err := meta.GetAccountByCredential(
					0,
					signedKey,
					true,
					nil,
				)
				require.NoError(t, err)
				require.Equal(t, types.Uint64(75), account.Reward)
				checkpoint, err := meta.GetImportCheckpoint(cfg.ImportKey, nil)
				require.NoError(t, err)
				require.Equal(t, models.ImportPhaseTip, checkpoint.Phase)
			},
		)
	}
}
