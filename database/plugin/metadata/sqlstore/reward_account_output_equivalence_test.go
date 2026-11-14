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
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// rewardAccountOutputReaderDigest is the digest of every reader result below
// on the seeded workload. It was computed against the indexes and queries
// that preceded the reward_account_output index reshape, so a match shows
// the reshaped schema and statements return the same credits, balances,
// history and order.
const rewardAccountOutputReaderDigest = "e7302827a31936b69f0cd84ba6d64c2fcf6d92206bb40a9bc16e3332cca743c6"

func outputLine(o *models.RewardAccountOutput) string {
	return fmt.Sprintf("%d %d %x %x %s %d %t %t %d %d",
		o.Epoch, o.CredentialTag, o.StakingKey, o.PoolKeyHash, o.RewardType,
		o.Amount, o.Spendable, o.Guarded, o.CapturedSlot, o.BoundarySlot)
}

func outputLines(outputs []*models.RewardAccountOutput, sorted bool) []string {
	lines := make([]string, 0, len(outputs))
	for _, o := range outputs {
		lines = append(lines, outputLine(o))
	}
	if sorted {
		slices.Sort(lines)
	}
	return lines
}

// TestRewardAccountOutputReadersMatchRecordedDigest drives a seeded reward
// history through the store's fold, claim, flag and rollback paths and
// digests what each reader returns. Row ids are excluded: they follow the
// insertion order, which the store is free to choose. Results whose order is
// not fixed by an ORDER BY are digested sorted.
func TestRewardAccountOutputReadersMatchRecordedDigest(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	require.Equal(t, rewardAccountOutputReaderDigest,
		rewardAccountOutputReaderDigestOf(t, store))
}

// rewardAccountOutputReaderDigestOf seeds the workload into an empty, migrated
// store and returns the digest of what its readers return.
func rewardAccountOutputReaderDigestOf(t *testing.T, store *Store) string {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, store.SetPendingRewardCreditRounds(
		[]models.RewardCreditRound{
			{SnapshotEpoch: 2, BoundarySlot: 200},
			{SnapshotEpoch: 3, BoundarySlot: 300},
		}, nil,
	))
	rng := rand.New(rand.NewPCG(7, 8))
	pools := [][]byte{bytesRepeat(0x31, 28), bytesRepeat(0x32, 28), bytesRepeat(0x33, 28)}
	var outputs []*models.RewardAccountOutput
	for epoch := uint64(1); epoch <= 4; epoch++ {
		for i := range 50 {
			for _, rewardType := range []string{"member", "leader"} {
				if rewardType == "leader" && i%10 != 0 {
					continue
				}
				outputs = append(outputs, &models.RewardAccountOutput{
					Epoch:         epoch,
					CredentialTag: uint8(i % 2),
					StakingKey:    bytesRepeat(byte(0x10+i/2), 28),
					PoolKeyHash:   pools[(i+int(epoch))%3],
					RewardType:    rewardType,
					Amount:        types.Uint64(1000*epoch + uint64(i)),
					Spendable:     i%6 != 0,
					Guarded:       i%11 == 3,
					CapturedSlot:  epoch*100 - 50,
					BoundarySlot:  epoch * 100,
				})
			}
		}
	}
	rng.Shuffle(len(outputs), func(i, j int) {
		outputs[i], outputs[j] = outputs[j], outputs[i]
	})
	require.NoError(t, store.SaveRewardAccountOutputs(outputs, nil))

	var lines []string
	add := func(label string, values ...string) {
		lines = append(lines, label)
		lines = append(lines, values...)
	}
	readers := func(label string) {
		for epoch := uint64(1); epoch <= 4; epoch++ {
			got, err := store.GetRewardAccountOutputs(epoch, nil)
			require.NoError(t, err)
			add(fmt.Sprintf("%s epoch %d", label, epoch), outputLines(got, false)...)
			ranged, err := store.GetRewardAccountOutputsInPoolKeyHashRange(
				epoch, pools[0], pools[1], nil)
			require.NoError(t, err)
			add(fmt.Sprintf("%s range %d", label, epoch), outputLines(ranged, false)...)
		}
		for i := range 50 {
			tag := uint8(i % 2)
			key := bytesRepeat(byte(0x10+i/2), 28)
			for _, order := range []string{"asc", "desc"} {
				got, err := store.GetRewardAccountOutputsByCredential(
					tag, key, 3, 1, order, nil)
				require.NoError(t, err)
				add(fmt.Sprintf("%s hist %d %s", label, i, order), outputLines(got, false)...)
			}
			count, err := store.CountRewardAccountOutputsByCredential(tag, key, nil)
			require.NoError(t, err)
			add(fmt.Sprintf("%s count %d", label, i), fmt.Sprint(count))
			spendable, err := store.GetRewardAccountOutputsForCredential(
				[]uint64{1, 2, 3}, tag, key, nil)
			require.NoError(t, err)
			add(fmt.Sprintf("%s cred %d", label, i), outputLines(spendable, false)...)
			eligibility, err := store.GetRewardAccountOutputsForEligibility(
				[]uint64{2, 3, 4}, tag, key, nil)
			require.NoError(t, err)
			add(fmt.Sprintf("%s elig %d", label, i), outputLines(eligibility, false)...)
			pending, err := store.GetPendingRewardAccountOutputsForCredential(
				tag, key, nil)
			require.NoError(t, err)
			add(fmt.Sprintf("%s pending %d", label, i), outputLines(pending, false)...)
		}
		totals, err := store.pendingRewardCreditTotals(ctx, store.writeDB)
		require.NoError(t, err)
		keys := make([]string, 0, len(totals))
		for key := range totals {
			keys = append(keys, fmt.Sprintf("%x %d", key, totals[key]))
		}
		slices.Sort(keys)
		add(label+" totals", keys...)
		var sums []string
		for i := range 50 {
			var sum int64
			require.NoError(t, store.writeDB.QueryRow(
				store.dialect.Rebind(
					`SELECT `+store.pendingRewardCreditSubquery("?", "?")),
				i%2, bytesRepeat(byte(0x10+i/2), 28),
			).Scan(&sum))
			sums = append(sums, fmt.Sprint(sum))
		}
		add(label+" subquery", sums...)
		selected := map[historicalRewardKey]struct{}{}
		for i := range 50 {
			selected[historicalRewardKey{
				tag: uint8(i % 2), key: string(bytesRepeat(byte(0x10+i/2), 28)),
			}] = struct{}{}
		}
		for _, visibleAt := range []uint64{100, 200, 300, 400} {
			credits, err := store.pendingCreditsForCredentials(
				ctx, store.writeDB, visibleAt, selected)
			require.NoError(t, err)
			var creditLines []string
			for key, amount := range credits {
				creditLines = append(creditLines,
					fmt.Sprintf("%d %x %d", key.tag, key.key, amount))
			}
			slices.Sort(creditLines)
			add(fmt.Sprintf("%s visible %d", label, visibleAt), creditLines...)
		}
		has, err := store.HasPendingRewardCreditRounds(nil)
		require.NoError(t, err)
		add(label+" has", fmt.Sprint(has))
	}

	readers("initial")

	for i := 0; i < 50; i += 7 {
		tag := uint8(i % 2)
		key := bytesRepeat(byte(0x10+i/2), 28)
		if i%2 == 0 {
			require.NoError(t, store.FoldPendingRewardAccountOutputs(tag, key, nil))
		} else {
			require.NoError(t, store.FoldRewardAccountOutputs(
				[]uint64{2}, tag, key, nil))
		}
	}
	claimed, err := store.ClaimPendingRewardCreditsForCredential(
		0, bytesRepeat(byte(0x10+2), 28), nil)
	require.NoError(t, err)
	add("claimed credential", outputLines(claimed, true)...)
	readers("after fold")

	// The rows a bounded claim takes follow row-id order, which the store may
	// choose, so the batches are digested by size and as one set.
	var round []*models.RewardAccountOutput
	var batches []string
	for {
		claimed, err := store.ClaimUnfoldedRewardCredits(2, 0, 17, nil)
		require.NoError(t, err)
		if len(claimed) == 0 {
			break
		}
		batches = append(batches, fmt.Sprint(len(claimed)))
		round = append(round, claimed...)
	}
	add("claimed round batches", batches...)
	add("claimed round", outputLines(round, true)...)
	readers("after claim")

	flips := make([]*models.RewardAccountOutput, 0, 20)
	for i := 0; i < 40; i += 2 {
		flip := *outputs[i]
		flip.Spendable = !flip.Spendable
		flip.Guarded = !flip.Guarded
		flips = append(flips, &flip)
	}
	require.NoError(t, store.SaveRewardAccountOutputs(flips, nil))
	readers("after flags")

	require.NoError(t, store.DeleteRewardStateAfterSlot(250, nil))
	readers("after rollback")

	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return hex.EncodeToString(sum[:])
}
