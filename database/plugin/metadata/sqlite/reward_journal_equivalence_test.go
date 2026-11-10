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
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// The reward-journal equivalence test drives a seeded random sequence of
// credit rounds (deferred and eager), partial compaction, event credits,
// withdrawals and rollbacks through the store's public API, and records what
// every reader of the journal returns at checkpoints. The recorded digests in
// testdata/reward_journal_equivalence.golden were produced by the
// account_reward_delta journal that migration 59 replaced; the test holds the
// current journal to them.
//
// Set DINGO_UPDATE_REWARD_JOURNAL_GOLDEN=1 to rewrite the golden file and
// DINGO_REWARD_JOURNAL_TRANSCRIPT_DIR to a directory to keep each seed's full
// transcript for diffing.

// rewardJournalEpochLength makes boundary slots a function of the epoch, as on
// a real chain, so a replayed boundary lands on the slot it first had.
const rewardJournalEpochLength = 120

func rewardJournalEpochOf(slot uint64) uint64 {
	return slot/rewardJournalEpochLength + 3
}

const (
	rewardJournalSeeds       = 24
	rewardJournalSteps       = 100
	rewardJournalAccounts    = 14
	rewardJournalGoldenFile  = "testdata/reward_journal_equivalence.golden"
	rewardJournalUpdateEnv   = "DINGO_UPDATE_REWARD_JOURNAL_GOLDEN"
	rewardJournalTranscripts = "DINGO_REWARD_JOURNAL_TRANSCRIPT_DIR"
)

type rewardJournalCoverage struct {
	deferredRounds       int
	eagerRounds          int
	partialCompactions   int
	fullCompactions      int
	withdrawals          int
	withdrawalReplays    int
	zeroEventReplays     int
	postSnapshotEvents   int
	mirEvents            int
	rollbacks            int
	rollbackAcrossRound  int
	rollbackMidCompact   int
	rollbackAtBoundary   int
	rollbackAfterWithdrw int
	foldBeforeWithdrawal int
}

type rewardJournalRound struct {
	epoch    uint64
	slot     uint64
	deferred bool
	// partial is set once a compaction chunk claimed some, but not all, of the
	// round's outputs.
	partial bool
}

type rewardJournalRun struct {
	t      *testing.T
	store  *sqlstore.Store
	driver rewardJournalDriver
	rng    *rand.Rand
	refs   []models.StakeCredentialRef
	slot   uint64
	epoch  uint64
	txSeq  uint64
	rounds []rewardJournalRound
	events []uint64
	// withdrawals holds the slot of every applied withdrawal not yet rolled
	// back.
	withdrawals []uint64
	cov         rewardJournalCoverage
	out         strings.Builder
	cpHashes    []string
	cpCount     int
}

func TestRewardJournalEquivalence(t *testing.T) {
	t.Parallel()
	golden := readRewardJournalGolden(t)
	update := os.Getenv(rewardJournalUpdateEnv) != ""
	var mu = make(chan struct{}, 1)
	results := make(map[int][]string, rewardJournalSeeds)
	var total rewardJournalCoverage
	for seed := 1; seed <= rewardJournalSeeds; seed++ {
		t.Run(fmt.Sprintf("seed-%02d", seed), func(t *testing.T) {
			t.Parallel()
			run := newRewardJournalRun(t, int64(seed))
			run.execute()
			hashes := run.cpHashes
			if dir := os.Getenv(rewardJournalTranscripts); dir != "" {
				require.NoError(t, os.MkdirAll(dir, 0o750))
				require.NoError(t, os.WriteFile(
					filepath.Join(dir, fmt.Sprintf("seed-%02d.txt", seed)),
					[]byte(run.out.String()), 0o600,
				))
			}
			mu <- struct{}{}
			results[seed] = hashes
			addRewardJournalCoverage(&total, run.cov)
			<-mu
			if update {
				return
			}
			want, ok := golden[seed]
			require.True(t, ok, "no golden digests for seed %d", seed)
			require.Equal(t, len(want), len(hashes),
				"checkpoint count differs for seed %d", seed)
			for i := range want {
				require.Equalf(t, want[i], hashes[i],
					"seed %d checkpoint %d reader output differs", seed, i)
			}
		})
	}
	t.Cleanup(func() {
		if update {
			writeRewardJournalGolden(t, results)
		}
		// Coverage floors keep a workload change from silently turning the
		// comparison into a vacuous one.
		require.Positive(t, total.deferredRounds)
		require.Positive(t, total.eagerRounds)
		require.Positive(t, total.partialCompactions)
		require.Positive(t, total.fullCompactions)
		require.Positive(t, total.withdrawals)
		require.Positive(t, total.withdrawalReplays)
		require.Positive(t, total.zeroEventReplays)
		require.Positive(t, total.postSnapshotEvents)
		require.Positive(t, total.mirEvents)
		require.Positive(t, total.rollbacks)
		require.Positive(t, total.rollbackAcrossRound)
		require.Positive(t, total.rollbackMidCompact)
		require.Positive(t, total.rollbackAtBoundary)
		require.Positive(t, total.rollbackAfterWithdrw)
		require.Positive(t, total.foldBeforeWithdrawal)
	})
}

func addRewardJournalCoverage(
	dst *rewardJournalCoverage,
	src rewardJournalCoverage,
) {
	dst.deferredRounds += src.deferredRounds
	dst.eagerRounds += src.eagerRounds
	dst.partialCompactions += src.partialCompactions
	dst.fullCompactions += src.fullCompactions
	dst.withdrawals += src.withdrawals
	dst.withdrawalReplays += src.withdrawalReplays
	dst.zeroEventReplays += src.zeroEventReplays
	dst.postSnapshotEvents += src.postSnapshotEvents
	dst.mirEvents += src.mirEvents
	dst.rollbacks += src.rollbacks
	dst.rollbackAcrossRound += src.rollbackAcrossRound
	dst.rollbackMidCompact += src.rollbackMidCompact
	dst.rollbackAtBoundary += src.rollbackAtBoundary
	dst.rollbackAfterWithdrw += src.rollbackAfterWithdrw
	dst.foldBeforeWithdrawal += src.foldBeforeWithdrawal
}

func readRewardJournalGolden(t *testing.T) map[int][]string {
	t.Helper()
	out := make(map[int][]string)
	file, err := os.Open(rewardJournalGoldenFile)
	if err != nil {
		if os.Getenv(rewardJournalUpdateEnv) != "" {
			return out
		}
		require.NoError(t, err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 3 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		seed, err := strconv.Atoi(fields[0])
		require.NoError(t, err)
		out[seed] = append(out[seed], fields[2])
	}
	require.NoError(t, scanner.Err())
	return out
}

func writeRewardJournalGolden(t *testing.T, results map[int][]string) {
	t.Helper()
	var b strings.Builder
	b.WriteString(
		"# seed checkpoint digest (first 16 hex of sha256 over the reader transcript)\n",
	)
	seeds := make([]int, 0, len(results))
	for seed := range results {
		seeds = append(seeds, seed)
	}
	sort.Ints(seeds)
	for _, seed := range seeds {
		for i, h := range results[seed] {
			fmt.Fprintf(&b, "%d %d %s\n", seed, i, h)
		}
	}
	require.NoError(
		t,
		os.MkdirAll(filepath.Dir(rewardJournalGoldenFile), 0o750),
	)
	require.NoError(
		t,
		os.WriteFile(rewardJournalGoldenFile, []byte(b.String()), 0o600),
	)
}

func newRewardJournalRun(t *testing.T, seed int64) *rewardJournalRun {
	t.Helper()
	store, _ := newSharedSQLStore(t)
	run := &rewardJournalRun{
		t:      t,
		store:  store,
		driver: rewardJournalDriver{store: store},
		rng:    rand.New(rand.NewSource(seed)),
		slot:   10,
		epoch:  rewardJournalEpochOf(10),
	}
	for i := range rewardJournalAccounts {
		tag := uint8(i % 2)
		key := bytes.Repeat([]byte{byte(0x40 + i)}, 28)
		require.NoError(t, store.CreateAccount(nil, &models.Account{
			StakingKey:    key,
			CredentialTag: tag,
			AddedSlot:     1,
			CreatedSlot:   1,
			CertificateID: uint(i + 1),
			Active:        true,
		}))
		run.refs = append(run.refs, models.NewStakeCredentialRef(tag, key))
	}
	return run
}

func (r *rewardJournalRun) execute() {
	for step := 1; step <= rewardJournalSteps; step++ {
		r.slot += uint64(1 + r.rng.Intn(40))
		for r.epoch < rewardJournalEpochOf(r.slot) {
			r.boundary()
		}
		switch roll := r.rng.Intn(100); {
		case roll < 30:
			r.compact()
		case roll < 62:
			r.withdraw()
		case roll < 74:
			r.eventCredit()
		case roll < 84:
			r.rollback()
		default:
			// A quiet slot: readers still run at the checkpoint below.
		}
		if step%7 == 0 {
			r.checkpoint()
		}
	}
	r.checkpoint()
}

func (r *rewardJournalRun) pick() models.StakeCredentialRef {
	return r.refs[r.rng.Intn(len(r.refs))]
}

// boundary applies the reward round of a new epoch at the current slot, with a
// POOLREAP-style and a MIR-style event credit at the same slot.
func (r *rewardJournalRun) boundary() {
	t := r.t
	r.epoch++
	snapshot := r.epoch - 2
	boundarySlot := min((r.epoch-3)*rewardJournalEpochLength, r.slot)
	round := rewardJournalRound{
		epoch:    snapshot,
		slot:     boundarySlot,
		deferred: r.rng.Intn(100) < 70,
	}
	surviving := false
	for _, ref := range r.refs {
		existing, err := r.store.GetRewardAccountOutputsForEligibility(
			[]uint64{snapshot}, ref.Tag, ref.Key, nil,
		)
		require.NoError(t, err)
		if len(existing) > 0 {
			surviving = true
		}
	}
	var outputs []*models.RewardAccountOutput
	pools := [][]byte{
		bytes.Repeat([]byte{0xa1}, 28),
		bytes.Repeat([]byte{0xa2}, 28),
		bytes.Repeat([]byte{0xa3}, 28),
	}
	for _, ref := range r.refs {
		if r.rng.Intn(100) < 25 || surviving {
			continue
		}
		for _, rewardType := range []string{"member", "leader"} {
			if rewardType == "leader" && r.rng.Intn(100) < 60 {
				continue
			}
			outputs = append(outputs, &models.RewardAccountOutput{
				StakingKey:    ref.Key,
				PoolKeyHash:   pools[r.rng.Intn(len(pools))],
				RewardType:    rewardType,
				Epoch:         snapshot,
				CredentialTag: ref.Tag,
				Amount: types.Uint64(
					1_000_000 + uint64Value(r.rng.Intn(9_000_000)),
				),
				Spendable:    r.rng.Intn(100) >= 8,
				Guarded:      r.rng.Intn(100) < 6,
				CapturedSlot: boundarySlot - 1,
				BoundarySlot: boundarySlot,
			})
		}
	}
	require.NoError(t, r.store.SaveRewardAccountOutputs(outputs, nil))
	require.NoError(t, r.store.AddAppliedRewardCreditRound(
		models.RewardCreditRound{
			SnapshotEpoch: snapshot,
			BoundarySlot:  boundarySlot,
		},
		nil,
	))
	if round.deferred {
		r.cov.deferredRounds++
	} else {
		r.cov.eagerRounds++
		r.driver.foldRound(t, snapshot)
	}
	r.rounds = append(r.rounds, round)
	for range 1 + r.rng.Intn(3) {
		ref := r.pick()
		hash := bytes.Repeat([]byte{byte(0xc0 + r.rng.Intn(6))}, 28)
		amount := uint64Value(1 + r.rng.Intn(5_000_000))
		require.NoError(t, r.store.AddPostSnapshotAccountRewardByCredential(
			ref.Tag, ref.Key, amount, boundarySlot, hash, nil,
		))
		r.cov.postSnapshotEvents++
		if r.rng.Intn(3) == 0 {
			// A replay of the same event at the same slot is a no-op.
			require.NoError(t, r.store.AddPostSnapshotAccountRewardByCredential(
				ref.Tag, ref.Key, amount, boundarySlot, hash, nil,
			))
			r.cov.zeroEventReplays++
		}
	}
	if r.rng.Intn(100) < 60 {
		ref := r.pick()
		pot := byte(r.rng.Intn(2))
		hash := append([]byte("mir-reward:"), pot)
		require.NoError(t, r.store.AddAccountRewardByCredential(
			ref.Tag,
			ref.Key,
			uint64Value(1+r.rng.Intn(900_000)),
			boundarySlot,
			hash,
			nil,
		))
		r.cov.mirEvents++
	}
	r.events = append(r.events, boundarySlot)
}

// eventCredit credits an event outside a boundary: a governance-style refund
// keyed by a 32-byte hash, which a replay must not apply twice.
func (r *rewardJournalRun) eventCredit() {
	ref := r.pick()
	hash := bytes.Repeat([]byte{byte(0xe0 + r.rng.Intn(8))}, 32)
	amount := uint64Value(1 + r.rng.Intn(2_000_000))
	require.NoError(r.t, r.store.AddAccountRewardByCredential(
		ref.Tag, ref.Key, amount, r.slot, hash, nil,
	))
	if r.rng.Intn(3) == 0 {
		require.NoError(r.t, r.store.AddAccountRewardByCredential(
			ref.Tag, ref.Key, amount, r.slot, hash, nil,
		))
		r.cov.zeroEventReplays++
	}
	r.events = append(r.events, r.slot)
}

// compact claims one chunk of a deferred round, or the whole round.
func (r *rewardJournalRun) compact() {
	var due []int
	for i, round := range r.rounds {
		if round.deferred {
			due = append(due, i)
		}
	}
	if len(due) == 0 {
		return
	}
	index := due[r.rng.Intn(len(due))]
	round := &r.rounds[index]
	if r.rng.Intn(100) < 55 {
		limit := 1 + r.rng.Intn(9)
		claimed := r.driver.foldChunk(r.t, round.epoch, limit)
		if claimed > 0 {
			round.partial = r.roundHasUnfolded(round.epoch)
			if round.partial {
				r.cov.partialCompactions++
			}
		}
		return
	}
	r.driver.foldRound(r.t, round.epoch)
	round.partial = false
	r.cov.fullCompactions++
}

// roundHasUnfolded reports whether any account still has an unfolded output
// of the round.
func (r *rewardJournalRun) roundHasUnfolded(epoch uint64) bool {
	for _, ref := range r.refs {
		pending, err := r.store.GetPendingRewardAccountOutputsForCredential(
			ref.Tag,
			ref.Key,
			nil,
		)
		require.NoError(r.t, err)
		for _, output := range pending {
			if output.Epoch == epoch {
				return true
			}
		}
	}
	return false
}

// withdraw folds the credential's pending credits first, as the ledger does
// before every withdrawal, then withdraws part or all of the balance.
func (r *rewardJournalRun) withdraw() {
	t := r.t
	ref := r.pick()
	pending, err := r.store.GetPendingRewardAccountOutputsForCredential(
		ref.Tag,
		ref.Key,
		nil,
	)
	require.NoError(t, err)
	if len(pending) > 0 {
		r.cov.foldBeforeWithdrawal++
	}
	r.driver.foldCredential(t, ref.Tag, ref.Key)
	account, err := r.store.GetAccountByCredential(ref.Tag, ref.Key, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	balance := uint64(account.Reward)
	if balance == 0 {
		return
	}
	amount := balance
	if r.rng.Intn(100) < 55 {
		amount = min(
			1+uint64Value(r.rng.Intn(int(min(balance, 1<<30)))),
			balance,
		)
	}
	r.txSeq++
	var txHash lcommon.Blake2b256
	binary.BigEndian.PutUint64(txHash[:8], r.txSeq)
	copy(txHash[8:], bytes.Repeat([]byte{0x7e}, 24))
	addrType := byte(lcommon.AddressTypeNoneKey)
	if ref.Tag == 1 {
		addrType = byte(lcommon.AddressTypeNoneScript)
	}
	address, err := lcommon.NewAddressFromParts(
		uint8(addrType), lcommon.AddressNetworkTestnet, nil, ref.Key,
	)
	require.NoError(t, err)
	tx := &mockTransaction{
		hash:    txHash,
		isValid: true,
		withdrawals: map[*lcommon.Address]*big.Int{
			&address: new(big.Int).SetUint64(amount),
		},
	}
	blockHash := bytes.Repeat([]byte{byte(r.slot % 251)}, 32)
	index := uint32(r.rng.Intn(4))
	apply := func() error {
		return r.store.SetTransaction(
			tx,
			ocommon.Point{Slot: r.slot, Hash: blockHash},
			index, nil, true, nil, 0,
		)
	}
	require.NoError(t, apply())
	r.withdrawals = append(r.withdrawals, r.slot)
	r.cov.withdrawals++
	if r.rng.Intn(100) < 15 {
		require.NoError(t, apply())
		r.cov.withdrawalReplays++
	}
}

// rollback undoes everything above a past slot in the order the database
// truncation does: reward deltas, transactions, then reward state.
func (r *rewardJournalRun) rollback() {
	t := r.t
	if r.slot < 20 {
		return
	}
	target := 10 + uint64(r.rng.Int63n(int64(r.slot-10)))
	if r.rng.Intn(100) < 30 && len(r.rounds) > 0 {
		// Land exactly on a boundary slot, which keeps that boundary's credits.
		target = r.rounds[r.rng.Intn(len(r.rounds))].slot
		r.cov.rollbackAtBoundary++
		if r.rng.Intn(2) == 0 {
			// Just below the boundary: the round goes, its outputs stay and
			// are unfolded.
			target--
		}
	}
	for _, round := range r.rounds {
		if round.slot > target {
			r.cov.rollbackAcrossRound++
			if round.partial {
				r.cov.rollbackMidCompact++
			}
		}
	}
	if slices.ContainsFunc(r.withdrawals, func(slot uint64) bool {
		return slot > target
	}) {
		r.cov.rollbackAfterWithdrw++
	}
	require.NoError(t, r.store.DeleteAccountRewardsAfterSlot(target, nil))
	require.NoError(t, r.store.DeleteTransactionsAfterSlot(target, nil))
	require.NoError(t, r.store.DeleteRewardStateAfterSlot(target, nil))
	r.cov.rollbacks++
	kept := r.rounds[:0]
	for _, round := range r.rounds {
		if round.slot <= target {
			kept = append(kept, round)
		}
	}
	r.rounds = kept
	r.slot = target
	r.epoch = rewardJournalEpochOf(target)
	keptEvents := r.events[:0]
	for _, slot := range r.events {
		if slot <= target {
			keptEvents = append(keptEvents, slot)
		}
	}
	r.events = keptEvents
	r.withdrawals = slices.DeleteFunc(r.withdrawals, func(slot uint64) bool {
		return slot > target
	})
}

func (r *rewardJournalRun) checkpoint() {
	t := r.t
	var b strings.Builder
	fmt.Fprintf(&b, "checkpoint %d slot=%d\n", r.cpCount, r.slot)
	sorted := append([]models.StakeCredentialRef(nil), r.refs...)
	for _, ref := range sorted {
		account, err := r.store.GetAccountByCredential(
			ref.Tag,
			ref.Key,
			true,
			nil,
		)
		require.NoError(t, err)
		fmt.Fprintf(
			&b,
			"account %d/%x reward=%d\n",
			ref.Tag,
			ref.Key[:2],
			account.Reward,
		)
		pending, err := r.store.GetPendingRewardAccountOutputsForCredential(
			ref.Tag,
			ref.Key,
			nil,
		)
		require.NoError(t, err)
		var total uint64
		for _, output := range pending {
			total += uint64(output.Amount)
		}
		fmt.Fprintf(&b, "  pending n=%d total=%d\n", len(pending), total)
		sums, err := r.store.GetAccountSumsByCredential(ref.Tag, ref.Key, nil)
		require.NoError(t, err)
		fmt.Fprintf(&b, "  sums withdrawals=%d\n", sums.WithdrawalsSum)
		count, err := r.store.CountAccountWithdrawalHistoryByCredential(
			ref.Tag,
			ref.Key,
			nil,
		)
		require.NoError(t, err)
		fmt.Fprintf(&b, "  history count=%d\n", count)
		for _, order := range []string{"asc", "desc"} {
			rows, err := r.store.GetAccountWithdrawalHistoryByCredential(
				ref.Tag, ref.Key, 3, 1, order, nil,
			)
			require.NoError(t, err)
			for _, row := range rows {
				fmt.Fprintf(
					&b,
					"  history %s %x amount=%d slot=%d index=%d block=%x\n",
					order,
					row.TxHash[:9],
					row.Amount,
					row.TxSlot,
					row.BlockIndex,
					row.BlockHash[:2],
				)
			}
		}
	}
	slots := []uint64{r.slot, r.slot / 2, r.slot * 3 / 4, 11}
	for _, round := range r.rounds {
		slots = append(slots, round.slot, round.slot-1, round.slot+1)
	}
	for _, slot := range slots {
		accounts, err := r.store.GetAccountsByCredentialAtSlot(
			r.refs,
			slot,
			nil,
		)
		require.NoError(t, err)
		keys := make([]string, 0, len(accounts))
		for key := range accounts {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		fmt.Fprintf(&b, "at-slot %d\n", slot)
		for _, key := range keys {
			fmt.Fprintf(&b, "  %x reward=%d\n", key, accounts[key].Reward)
		}
	}
	for _, slot := range append(append([]uint64(nil), r.events...), 1<<40) {
		totals, err := r.store.GetPostSnapshotRewardCredits(slot, nil)
		require.NoError(t, err)
		events, err := r.store.GetPostSnapshotRewardCreditEvents(slot, nil)
		require.NoError(t, err)
		fmt.Fprintf(&b, "post-snapshot %d\n", slot)
		for _, delta := range totals {
			fmt.Fprintf(
				&b,
				"  total %d/%x amount=%d post=%v\n",
				delta.CredentialTag,
				delta.StakingKey[:2],
				delta.Amount,
				delta.PostSnapshot,
			)
		}
		for _, delta := range events {
			fmt.Fprintf(&b, "  event %d/%x hash=%x amount=%d slot=%d\n",
				delta.CredentialTag, delta.StakingKey[:2], delta.TxHash,
				delta.Amount, delta.AddedSlot)
		}
	}
	for _, maxSlot := range []uint64{r.slot, r.slot / 2} {
		last, err := r.store.AccountLastWitnessSlots(r.refs, maxSlot, nil)
		require.NoError(t, err)
		keys := make([]string, 0, len(last))
		for key := range last {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		fmt.Fprintf(&b, "last-witness %d\n", maxSlot)
		for _, key := range keys {
			fmt.Fprintf(&b, "  %x %d\n", key, last[key])
		}
	}
	for _, after := range []uint64{0, r.slot / 3, r.slot * 2 / 3} {
		witnessed, err := r.store.AccountsWitnessedAfterSlot(after, nil)
		require.NoError(t, err)
		lines := make([]string, 0, len(witnessed))
		for _, ref := range witnessed {
			lines = append(lines, fmt.Sprintf("%d/%x", ref.Tag, ref.Key[:2]))
		}
		sort.Strings(lines)
		fmt.Fprintf(
			&b,
			"witnessed-after %d: %s\n",
			after,
			strings.Join(lines, ","),
		)
	}
	r.out.WriteString(b.String())
	digest := sha256.Sum256([]byte(b.String()))
	r.cpHashes = append(r.cpHashes, hex.EncodeToString(digest[:8]))
	r.cpCount++
}

func uint64Value(v int) uint64 {
	return uint64(v) //nolint:gosec // callers pass non-negative values
}
