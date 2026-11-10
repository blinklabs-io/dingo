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
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// The workload below mirrors one mainnet epoch of reward bookkeeping at a
// reduced scale. Every dirtied b-tree page of a committed write transaction
// is one WAL frame, and the table's indexes keyed by stake credential see a
// different leaf per inserted row once the index holds more leaves than a
// chunk has rows, so the frames written per delegator row, attributed to each
// b-tree through dbstat, is the quantity the schema controls. The chunk size
// is scaled with the history so that chunk rows over index leaves matches
// mainnet's 5000 rows over a 17.9M-row index.
const (
	walWorkloadPools          = 400
	walWorkloadDelegators     = 100_000
	walWorkloadHistoryEpochs  = 4
	walWorkloadChunkRows      = 500
	walWorkloadFoldBatch      = 5000
	walWorkloadWithdrawals    = 2000
	walWorkloadFlagFlips      = 2000
	walWorkloadFirstEpoch     = 100
	walWorkloadRoundsUnfolded = 2
)

type walWorkloadStore struct {
	*Store
	path string
}

func newWALWorkloadStore(t *testing.T) *walWorkloadStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wal_workload.db")
	dsn := "file:" + path +
		"?_pragma=journal_mode(WAL)&_pragma=synchronous(OFF)" +
		"&_pragma=busy_timeout(30000)&_pragma=wal_autocheckpoint(0)"
	writeDB, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)
	writeDB.SetConnMaxLifetime(0)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         writeDB,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return &walWorkloadStore{Store: store, path: path}
}

func walWorkloadHash(prefix string, n int) []byte {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s-%d", prefix, n))
	return sum[:28]
}

type walWorkloadDelegator struct {
	key  []byte
	pool int
}

type walWorkload struct {
	pools      [][]byte
	delegators []walWorkloadDelegator
	byPool     [][]int
}

func newWALWorkload() *walWorkload {
	w := &walWorkload{
		pools:  make([][]byte, walWorkloadPools),
		byPool: make([][]int, walWorkloadPools),
	}
	for i := range w.pools {
		w.pools[i] = walWorkloadHash("pool", i)
	}
	// Pools are processed in ascending pool_key_hash order, as the chunked
	// precompute does.
	slices.SortFunc(w.pools, func(a, b []byte) int {
		return strings.Compare(string(a), string(b))
	})
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range walWorkloadDelegators {
		// Skewed pool sizes: low-numbered pools are larger.
		pool := int(float64(walWorkloadPools) * rng.Float64() * rng.Float64())
		w.delegators = append(w.delegators, walWorkloadDelegator{
			key:  walWorkloadHash("cred", i),
			pool: pool,
		})
		w.byPool[pool] = append(w.byPool[pool], i)
	}
	return w
}

func (w *walWorkload) output(
	epoch uint64,
	index int,
	rng *rand.Rand,
) *models.RewardAccountOutput {
	d := w.delegators[index]
	return &models.RewardAccountOutput{
		Epoch:         epoch,
		CredentialTag: 0,
		StakingKey:    d.key,
		PoolKeyHash:   w.pools[d.pool],
		RewardType:    "member",
		Amount:        types.Uint64(1_000_000 + uint64(rng.IntN(5_000_000))),
		Spendable:     index%20 != 0,
		Guarded:       index%97 == 0,
		CapturedSlot:  epoch*100 - 50,
		BoundarySlot:  epoch * 100,
	}
}

// loadHistory inserts the history epochs with the table's indexes dropped and
// rebuilt afterwards, so building the baseline does not cost a random insert
// per row per index.
func (w *walWorkload) loadHistory(t *testing.T, store *walWorkloadStore) {
	t.Helper()
	rows, err := store.writeDB.Query(`
SELECT sql FROM sqlite_master
WHERE type = 'index' AND tbl_name = 'reward_account_output'
  AND sql IS NOT NULL`)
	require.NoError(t, err)
	var ddl []string
	for rows.Next() {
		var stmt string
		require.NoError(t, rows.Scan(&stmt))
		ddl = append(ddl, stmt)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	// The unique index must be rebuilt last-but-correct; order is immaterial.
	names := make([]string, 0, len(ddl))
	nameRows, err := store.writeDB.Query(`
SELECT name FROM sqlite_master
WHERE type = 'index' AND tbl_name = 'reward_account_output'
  AND sql IS NOT NULL`)
	require.NoError(t, err)
	for nameRows.Next() {
		var name string
		require.NoError(t, nameRows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, nameRows.Close())
	for _, name := range names {
		_, err := store.writeDB.Exec("DROP INDEX `" + name + "`")
		require.NoError(t, err)
	}
	tx, err := store.writeDB.Begin()
	require.NoError(t, err)
	stmt, err := tx.Prepare(`
INSERT INTO reward_account_output (
    staking_key, pool_key_hash, reward_type, epoch, credential_tag, amount,
    spendable, guarded, captured_slot, boundary_slot, folded
) VALUES (?, ?, 'member', ?, 0, ?, ?, FALSE, ?, ?, ?)`)
	require.NoError(t, err)
	rng := rand.New(rand.NewPCG(3, 4))
	firstEpoch := uint64(walWorkloadFirstEpoch)
	for epoch := firstEpoch; epoch < firstEpoch+walWorkloadHistoryEpochs; epoch++ {
		folded := epoch < firstEpoch+walWorkloadHistoryEpochs-
			walWorkloadRoundsUnfolded
		for _, poolIndexes := range w.byPool {
			for _, index := range poolIndexes {
				d := w.delegators[index]
				_, err := stmt.Exec(
					d.key, w.pools[d.pool], epoch,
					fmt.Sprint(1_000_000+rng.IntN(5_000_000)),
					index%20 != 0, epoch*100-50, epoch*100, folded,
				)
				require.NoError(t, err)
			}
		}
	}
	require.NoError(t, stmt.Close())
	require.NoError(t, tx.Commit())
	for _, stmt := range ddl {
		_, err := store.writeDB.Exec(stmt)
		require.NoError(t, err)
	}
	for epoch := firstEpoch + walWorkloadHistoryEpochs - walWorkloadRoundsUnfolded; epoch < firstEpoch+walWorkloadHistoryEpochs; epoch++ {
		require.NoError(t, store.AddAppliedRewardCreditRound(
			models.RewardCreditRound{
				SnapshotEpoch: epoch, BoundarySlot: epoch * 100,
			}, nil,
		))
	}
	_, err = store.writeDB.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
	require.NoError(t, err)
}

type walPhase struct {
	name  string
	start int64
	end   int64
}

func (s *walWorkloadStore) walFrames(t *testing.T) int64 {
	t.Helper()
	info, err := os.Stat(s.path + "-wal")
	if err != nil {
		return 0
	}
	const frameHeader = 24
	var pageSize int64
	require.NoError(t, s.writeDB.QueryRow(`PRAGMA page_size`).Scan(&pageSize))
	if info.Size() <= 32 {
		return 0
	}
	return (info.Size() - 32) / (pageSize + frameHeader)
}

// walFramePages returns the page number of every WAL frame in order.
func (s *walWorkloadStore) walFramePages(t *testing.T) []uint32 {
	t.Helper()
	raw, err := os.ReadFile(s.path + "-wal")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(raw), 32)
	pageSize := int(binary.BigEndian.Uint32(raw[8:12]))
	frameSize := 24 + pageSize
	var pages []uint32
	for off := 32; off+frameSize <= len(raw); off += frameSize {
		pages = append(pages, binary.BigEndian.Uint32(raw[off:off+4]))
	}
	return pages
}

func (s *walWorkloadStore) pageOwners(t *testing.T) map[uint32]string {
	t.Helper()
	rows, err := s.writeDB.Query(
		`SELECT name, pageno FROM dbstat WHERE aggregate = FALSE`,
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	owners := make(map[uint32]string)
	for rows.Next() {
		var name string
		var page uint32
		require.NoError(t, rows.Scan(&name, &page))
		owners[page] = name
	}
	require.NoError(t, rows.Err())
	return owners
}

// walFrameReport attributes the frames of each phase to the b-tree owning
// the page, restricted to the reward_account_output table and its indexes.
type walFrameReport struct {
	phases map[string]map[string]int
	total  map[string]int
	order  []string
}

func (s *walWorkloadStore) report(
	t *testing.T,
	phases []walPhase,
) *walFrameReport {
	t.Helper()
	pages := s.walFramePages(t)
	owners := s.pageOwners(t)
	rep := &walFrameReport{
		phases: map[string]map[string]int{},
		total:  map[string]int{},
	}
	for _, phase := range phases {
		counts := map[string]int{}
		for _, page := range pages[phase.start:phase.end] {
			name := owners[page]
			if name == "" {
				name = "(other)"
			}
			if !strings.HasPrefix(name, "reward_account_output") &&
				!strings.HasPrefix(name, "idx_reward_account_output") {
				name = "(other)"
			}
			counts[name]++
			rep.total[name]++
		}
		rep.phases[phase.name] = counts
		rep.order = append(rep.order, phase.name)
	}
	return rep
}

func (r *walFrameReport) log(t *testing.T) {
	t.Helper()
	names := make([]string, 0, len(r.total))
	for name := range r.total {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	fmt.Fprintf(&b, "%-62s", "b-tree")
	for _, phase := range r.order {
		fmt.Fprintf(&b, " %12s", phase)
	}
	fmt.Fprintf(&b, " %12s\n", "total")
	for _, name := range names {
		fmt.Fprintf(&b, "%-62s", name)
		for _, phase := range r.order {
			fmt.Fprintf(&b, " %12d", r.phases[phase][name])
		}
		fmt.Fprintf(&b, " %12d\n", r.total[name])
	}
	t.Log("\n" + b.String())
}

func (r *walFrameReport) rewardOutputFrames() int {
	var total int
	for name, count := range r.total {
		if name != "(other)" {
			total += count
		}
	}
	return total
}

// runWALWorkload drives one epoch through the store's write path and returns
// the per-b-tree frame report: the chunked precompute of the epoch, its
// credit at the boundary, the compaction fold of the oldest credited round,
// withdrawals that fold single credentials, and spendable/guarded
// transitions saved over precomputed outputs.
func runWALWorkload(t *testing.T) *walFrameReport {
	t.Helper()
	store := newWALWorkloadStore(t)
	w := newWALWorkload()
	loadStart := time.Now()
	w.loadHistory(t, store)
	t.Logf("history loaded in %s", time.Since(loadStart))

	epoch := uint64(walWorkloadFirstEpoch + walWorkloadHistoryEpochs)
	rng := rand.New(rand.NewPCG(5, 6))
	var phases []walPhase
	mark := store.walFrames(t)
	started := time.Now()
	closePhase := func(name string) {
		t.Logf("phase %s done after %s", name, time.Since(started))
		end := store.walFrames(t)
		phases = append(phases, walPhase{name: name, start: mark, end: end})
		mark = end
	}

	var chunk []*models.RewardAccountOutput
	flush := func() {
		if len(chunk) == 0 {
			return
		}
		require.NoError(t, store.SaveRewardAccountOutputs(chunk, nil))
		chunk = chunk[:0]
	}
	for _, indexes := range w.byPool {
		order := slices.Clone(indexes)
		rng.Shuffle(len(order), func(i, j int) {
			order[i], order[j] = order[j], order[i]
		})
		for _, index := range order {
			chunk = append(chunk, w.output(epoch, index, rng))
			if len(chunk) >= walWorkloadChunkRows {
				flush()
			}
		}
	}
	flush()
	closePhase("precompute")

	require.NoError(t, store.AddAppliedRewardCreditRound(
		models.RewardCreditRound{
			SnapshotEpoch: epoch, BoundarySlot: epoch * 100,
		}, nil,
	))
	closePhase("credit")

	oldest := epoch - walWorkloadRoundsUnfolded
	for {
		claimed, err := store.ClaimUnfoldedRewardCredits(
			oldest, walWorkloadFoldBatch, nil,
		)
		require.NoError(t, err)
		if len(claimed) == 0 {
			break
		}
	}
	closePhase("fold")

	for range walWorkloadWithdrawals {
		d := w.delegators[rng.IntN(len(w.delegators))]
		require.NoError(t, store.FoldPendingRewardAccountOutputs(0, d.key, nil))
	}
	closePhase("withdraw")

	flips := make([]*models.RewardAccountOutput, 0, walWorkloadFlagFlips)
	for range walWorkloadFlagFlips {
		output := w.output(epoch, rng.IntN(len(w.delegators)), rng)
		output.Spendable = !output.Spendable
		output.Guarded = !output.Guarded
		flips = append(flips, output)
	}
	require.NoError(t, store.SaveRewardAccountOutputs(flips, nil))
	closePhase("flags")

	return store.report(t, phases)
}

// TestRewardAccountOutputWALFrames bounds the WAL frames one epoch of reward
// bookkeeping writes to each b-tree of reward_account_output. Measured with
// the two credential-leading indexes, the credential-and-folded covering index
// and the credential-leading unique index, the same workload wrote about 6.0
// frames per delegator row; one credential index and a pool-ordered unique
// index write under 2.
func TestRewardAccountOutputWALFrames(t *testing.T) {
	t.Parallel()
	testutil.SkipUnderRace(
		t,
		"single-goroutine SQLite page counting over a 100k-delegator "+
			"workload; the frame bounds are asserted in the un-instrumented run",
	)
	rep := runWALWorkload(t)
	rep.log(t)
	perRow := float64(rep.rewardOutputFrames()) / walWorkloadDelegators
	t.Logf("reward_account_output frames per delegator row: %.3f", perRow)
	for name, frames := range rep.total {
		if strings.HasPrefix(name, "idx_reward_account_output_epoch_") {
			require.Less(t, frames, walWorkloadDelegators/10,
				"%s: the precompute's pool-ordered inserts must append", name)
		}
	}
	require.Less(t, perRow, 2.5,
		"frames per delegator row across all reward_account_output b-trees")

	// Folding, withdrawing and flipping spendable or guarded move no entry of
	// a credential-keyed index.
	for _, phase := range []string{"fold", "withdraw", "flags"} {
		for name, frames := range rep.phases[phase] {
			if strings.Contains(name, "credential") {
				require.Zero(t, frames, "%s wrote %s", phase, name)
			}
		}
	}
}
