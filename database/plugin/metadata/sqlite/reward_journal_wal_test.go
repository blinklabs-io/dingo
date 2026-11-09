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

package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// rewardJournalWALEnv gates the write-amplification measurement: it builds a
// file-backed database, so it stays out of the default run.
const rewardJournalWALEnv = "DINGO_REWARD_JOURNAL_WAL"

const (
	walAccounts     = 40_000
	walPools        = 300
	walWithdrawals  = 2_000
	walChunk        = 1_000
	walFoldedRounds = 2
)

// walFramesByBTree reads the frames appended to the write-ahead log after
// skip frames and counts them per b-tree, mapping each frame's page to the
// table or index that owns it. Run with automatic checkpoints off: a
// checkpoint resets the log and the counts with it.
func walFramesByBTree(
	t *testing.T,
	db *sql.DB,
	walPath string,
	skip int,
) (map[string]int, int) {
	t.Helper()
	data, err := os.ReadFile(walPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), 32)
	pageSize := int(binary.BigEndian.Uint32(data[8:12]))
	frameSize := 24 + pageSize
	frames := (len(data) - 32) / frameSize
	owner := make(map[uint32]string)
	rows, err := db.Query(`SELECT name, pageno FROM dbstat`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var name string
		var page int64
		require.NoError(t, rows.Scan(&name, &page))
		owner[uint32(page)] = name //nolint:gosec
	}
	require.NoError(t, rows.Err())
	counts := make(map[string]int)
	for frame := skip; frame < frames; frame++ {
		page := binary.BigEndian.Uint32(data[32+frame*frameSize:])
		name, ok := owner[page]
		if !ok {
			name = "(free or unowned)"
		}
		counts[name]++
	}
	return counts, frames
}

func btreePages(t *testing.T, db *sql.DB) map[string][2]int64 {
	t.Helper()
	rows, err := db.Query(
		`SELECT name, COUNT(*), SUM(pgsize) FROM dbstat GROUP BY name`,
	)
	require.NoError(t, err)
	defer rows.Close()
	out := make(map[string][2]int64)
	for rows.Next() {
		var name string
		var pages, bytes int64
		require.NoError(t, rows.Scan(&name, &pages, &bytes))
		out[name] = [2]int64{pages, bytes}
	}
	require.NoError(t, rows.Err())
	return out
}

func reportWAL(
	t *testing.T,
	label string,
	counts map[string]int,
	units int,
	pages map[string][2]int64,
) {
	t.Helper()
	names := make([]string, 0, len(counts))
	total := 0
	for name, n := range counts {
		names = append(names, name)
		total += n
	}
	sort.Slice(
		names,
		func(i, j int) bool { return counts[names[i]] > counts[names[j]] },
	)
	var b strings.Builder
	fmt.Fprintf(
		&b,
		"WAL %s: %d frames over %d units = %.3f per unit\n",
		label,
		total,
		units,
		float64(total)/float64(units),
	)
	for _, name := range names[:min(len(names), 8)] {
		fmt.Fprintf(
			&b,
			"  %-45s %7d  %.3f/unit  btree %d pages\n",
			name,
			counts[name],
			float64(counts[name])/float64(units),
			pages[name][0],
		)
	}
	t.Log(b.String())
}

func reportJournalSizes(t *testing.T, pages map[string][2]int64) {
	t.Helper()
	var names []string
	for name := range pages {
		if strings.Contains(name, "reward_credit") || strings.Contains(name, "reward_event") ||
			strings.Contains(name, "reward_source") ||
			strings.Contains(name, "reward_withdrawal") ||
			strings.Contains(name, "account_reward_delta") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	var b strings.Builder
	var total int64
	b.WriteString("journal b-trees:\n")
	for _, name := range names {
		fmt.Fprintf(
			&b,
			"  %-45s %6d pages %9d bytes\n",
			name,
			pages[name][0],
			pages[name][1],
		)
		total += pages[name][1]
	}
	fmt.Fprintf(&b, "  total %d bytes\n", total)
	t.Log(b.String())
}

// reportReadTimings logs the best of five runs of the journal readers, before
// and after ANALYZE.
func reportReadTimings(
	t *testing.T,
	store interface {
		GetAccountsByCredentialAtSlot([]models.StakeCredentialRef, uint64, types.Txn) (map[string]*models.Account, error)
		AccountLastWitnessSlots([]models.StakeCredentialRef, uint64, types.Txn) (map[string]uint64, error)
		AccountsWitnessedAfterSlot(uint64, types.Txn) ([]models.StakeCredentialRef, error)
		GetPostSnapshotRewardCredits(uint64, types.Txn) ([]*models.AccountRewardDelta, error)
		GetAccountWithdrawalHistoryByCredential(uint8, []byte, int, int, string, types.Txn) ([]models.AccountWithdrawalHistoryRow, error)
		GetAccountSumsByCredential(uint8, []byte, types.Txn) (models.AccountSums, error)
	},
	db *sql.DB,
	keys [][]byte,
) {
	t.Helper()
	refs := make([]models.StakeCredentialRef, 0, 1000)
	for _, key := range keys[:1000] {
		refs = append(refs, models.NewStakeCredentialRef(0, key))
	}
	witnessRefs := refs[:400]
	readers := []struct {
		name string
		run  func() error
	}{
		{
			"credit and withdrawal history of 1000 credentials, 2 rounds back",
			func() error {
				_, err := store.GetAccountsByCredentialAtSlot(refs, 2000, nil)
				return err
			},
		},
		{"withdrawal sum, one credential", func() error {
			_, err := store.GetAccountSumsByCredential(0, keys[5], nil)
			return err
		}},
		{"withdrawal history page, one credential", func() error {
			_, err := store.GetAccountWithdrawalHistoryByCredential(
				0,
				keys[5],
				20,
				0,
				"desc",
				nil,
			)
			return err
		}},
		{"last witness slots of 400 credentials", func() error {
			_, err := store.AccountLastWitnessSlots(witnessRefs, 1<<40, nil)
			return err
		}},
		{"credentials witnessed after a slot", func() error {
			_, err := store.AccountsWitnessedAfterSlot(10_005, nil)
			return err
		}},
		{"post-snapshot credits at a boundary slot", func() error {
			_, err := store.GetPostSnapshotRewardCredits(3000, nil)
			return err
		}},
	}
	for _, analyzed := range []bool{false, true} {
		if analyzed {
			_, err := db.Exec(`ANALYZE`)
			require.NoError(t, err)
		}
		var b strings.Builder
		fmt.Fprintf(&b, "read timings (best of 5, analyzed=%v):\n", analyzed)
		for _, reader := range readers {
			best := time.Duration(1 << 62)
			for range 5 {
				start := time.Now()
				require.NoError(t, reader.run())
				best = min(best, time.Since(start))
			}
			fmt.Fprintf(
				&b,
				"  %-70s %8.2f ms\n",
				reader.name,
				float64(best.Microseconds())/1000,
			)
		}
		t.Log(b.String())
	}
}

// TestRewardJournalWriteAmplification measures the WAL frames one round's
// compaction fold and a batch of withdrawals write, per b-tree, on a
// file-backed database filled in the order the live node fills it, and bounds
// the fold's journal frames per credit. The fold of a round claims accounts in
// id order, so its journal rows append and its account updates touch
// neighbouring pages; a fold in output order, or a journal keyed with the
// account first, raises the figure by an order of magnitude.
func TestRewardJournalWriteAmplification(t *testing.T) {
	t.Parallel()
	if os.Getenv(rewardJournalWALEnv) == "" {
		t.Skipf(
			"set %s=1 to measure WAL write amplification",
			rewardJournalWALEnv,
		)
	}
	dir := t.TempDir()
	store, db, _, err := openSQLStore(
		Config{DataDir: dir}, metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	// The write pool has one connection, so the pragma holds for every write.
	_, err = db.Exec(`PRAGMA wal_autocheckpoint=0`)
	require.NoError(t, err)
	var walPath string
	require.NoError(t, db.QueryRow(
		`SELECT file FROM pragma_database_list WHERE name = 'main'`,
	).Scan(&walPath))
	walPath += "-wal"

	rng := rand.New(rand.NewSource(7))
	keys := make([][]byte, walAccounts)
	tx, err := db.Begin()
	require.NoError(t, err)
	for i := range keys {
		keys[i] = make([]byte, 28)
		_, _ = rng.Read(keys[i])
		_, err := tx.Exec(`INSERT INTO account
(staking_key, credential_tag, added_slot, created_slot, active, reward)
VALUES (?, 0, 1, 1, TRUE, '1000000')`, keys[i])
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())

	// Outputs are saved the way a round is: by pool, then by credential.
	pools := make([][]byte, walPools)
	for i := range pools {
		pools[i] = bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 14)
	}
	membership := make([][]int, walPools)
	for account := range keys {
		pool := rng.Intn(walPools)
		membership[pool] = append(membership[pool], account)
	}
	saveRound := func(epoch uint64) {
		t.Helper()
		boundary := (epoch + 2) * 1000
		tx, err := db.Begin()
		require.NoError(t, err)
		for pool, members := range membership {
			sort.Slice(members, func(i, j int) bool {
				return bytes.Compare(keys[members[i]], keys[members[j]]) < 0
			})
			for _, account := range members {
				_, err := tx.Exec(`INSERT INTO reward_account_output
(staking_key, pool_key_hash, reward_type, epoch, credential_tag, amount,
 spendable, guarded, captured_slot, boundary_slot)
VALUES (?, ?, 'member', ?, 0, '1234567', TRUE, FALSE, ?, ?)`,
					keys[account], pools[pool], epoch, boundary-1, boundary)
				require.NoError(t, err)
			}
		}
		require.NoError(t, tx.Commit())
		require.NoError(t, store.AddAppliedRewardCreditRound(
			models.RewardCreditRound{
				SnapshotEpoch: epoch,
				BoundarySlot:  boundary,
			},
			nil,
		))
	}

	driver := rewardJournalDriver{store: store, cursors: map[uint64]uint{}}
	for epoch := uint64(1); epoch <= walFoldedRounds; epoch++ {
		saveRound(epoch)
		if epoch < walFoldedRounds {
			for driver.foldChunk(t, epoch, walChunk) > 0 {
			}
		}
	}
	measured := uint64(walFoldedRounds)
	_, framesBefore := walFramesByBTree(t, db, walPath, 1<<30)
	credits := 0
	for {
		claimed := driver.foldChunk(t, measured, walChunk)
		if claimed == 0 {
			break
		}
		credits += claimed
	}
	require.Equal(t, walAccounts, credits)
	fold, _ := walFramesByBTree(t, db, walPath, framesBefore)
	pages := btreePages(t, db)
	reportWAL(t, "fold of one round", fold, credits, pages)

	_, framesBefore = walFramesByBTree(t, db, walPath, 1<<30)
	for batch := 0; batch < walWithdrawals/20; batch++ {
		txn := store.Transaction(context.Background())
		for i := range 20 {
			account := rng.Intn(walAccounts)
			address, err := lcommon.NewAddressFromParts(
				lcommon.AddressTypeNoneKey,
				lcommon.AddressNetworkTestnet,
				nil,
				keys[account],
			)
			require.NoError(t, err)
			var hash lcommon.Blake2b256
			binary.BigEndian.PutUint64(hash[:8], uint64(batch*20+i+1))
			rng.Read(hash[8:]) //nolint:errcheck
			require.NoError(t, store.SetTransaction(
				&mockTransaction{
					hash: hash, isValid: true,
					withdrawals: map[*lcommon.Address]*big.Int{
						&address: big.NewInt(1),
					},
				},
				ocommon.Point{
					Slot: uint64(10_000 + batch),
					Hash: bytes.Repeat([]byte{byte(batch)}, 32),
				},
				uint32(i),
				nil,
				true,
				txn,
				0,
			))
		}
		require.NoError(t, txn.Commit())
	}
	withdrawals, _ := walFramesByBTree(t, db, walPath, framesBefore)
	reportWAL(t, "withdrawals", withdrawals, walWithdrawals, btreePages(t, db))

	reportJournalSizes(t, btreePages(t, db))
	reportReadTimings(t, store, db, keys)

	journal := 0
	for name, n := range fold {
		if strings.Contains(name, "reward_credit") ||
			strings.Contains(name, "account_reward_delta") {
			journal += n
		}
	}
	accountFrames := fold["account"]
	t.Logf(
		"fold journal frames per credit %.3f, account frames per credit %.3f",
		float64(
			journal,
		)/float64(
			credits,
		),
		float64(accountFrames)/float64(credits),
	)
	// Measured on the keyed-by-slot journal: 0.07 and 0.01. A fold in output
	// order measures about 0.5 and 0.45; the former journal's five b-trees
	// measured 1.9 and 0.45.
	require.LessOrEqual(t, float64(journal)/float64(credits), 0.2,
		"journal frames per credit of a fold")
	require.LessOrEqual(t, float64(accountFrames)/float64(credits), 0.1,
		"account frames per credit of a fold")
}
