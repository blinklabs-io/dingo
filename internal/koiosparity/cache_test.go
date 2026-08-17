// Copyright 2025 Blink Labs Software
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

package koiosparity

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCommitEpochDataWithTotals exercises the actual SQL generated for the
// koios_totals upsert against a real SQLite file — a pure-Go struct test
// cannot catch a column-name mismatch (e.g. DepositsDRep versus the persisted
// "deposits_drep" spelling; the column must be pinned explicitly in cache.go).
// CommitEpochData's
// AssignmentColumns list is a hardcoded string literal that must match the
// real migrated column for every field, so this has to run against a real
// DB, not just construct the struct in memory.
func TestCommitEpochDataWithTotals(t *testing.T) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	now := time.Now().UTC().Truncate(time.Second)
	info := KoiosEpochInfo{
		Network:      "preview",
		Epoch:        1367,
		ActiveStake:  "3250414888938614",
		Fees:         "1292170362",
		TotalRewards: "410988812047",
		EpochEndTime: now,
		FetchedAt:    now,
	}
	totals := &KoiosTotals{
		Treasury:           "6931231163186226",
		Reserves:           "7792082362166766",
		Fees:               "1245791321",
		Reward:             "292608261256804",
		Circulation:        "29979007817598883",
		Supply:             "37207917637833234",
		DepositsStake:      "536150000000",
		DepositsDRep:       "4474000000000",
		DepositsProposal:   "59000000000",
		TreasuryDonation:   "0",
		TreasuryWithdrawal: "0",
		ReservesWithdrawal: "0",
		FetchedAt:          now,
	}

	// First insert, then a second commit for the same (network, epoch) to
	// exercise the ON CONFLICT DO UPDATE path too, not just the initial INSERT.
	require.NoError(t, cache.CommitEpochData(info, nil, totals))
	require.NoError(t, cache.CommitEpochData(info, nil, totals))

	got, err := cache.GetTotals("preview", 1367)
	require.NoError(t, err)
	require.Equal(t, totals.Treasury, got.Treasury)
	require.Equal(t, totals.Reserves, got.Reserves)
	require.Equal(t, totals.Fees, got.Fees)
	require.Equal(t, totals.Reward, got.Reward)
	require.Equal(t, totals.DepositsDRep, got.DepositsDRep)
	require.Equal(t, totals.DepositsStake, got.DepositsStake)
	require.Equal(t, totals.DepositsProposal, got.DepositsProposal)
}
