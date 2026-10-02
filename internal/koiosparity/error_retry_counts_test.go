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

package koiosparity

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestObserverAutomaticallyRecoversErrorEpochWithoutNewEvent(t *testing.T) {
	t.Parallel()
	for _, accounts := range []bool{false, true} {
		t.Run(map[bool]string{false: "aggregate", true: "accounts"}[accounts], func(t *testing.T) {
			source, err := NewDatabaseSource(newTestDatabaseSourceDB(t))
			require.NoError(t, err)
			srv := newFakeKoiosServer(t, map[uint64]*fakeEpochRef{
				5: {activeStake: "1000000", treasury: "10", reserves: "20", fees: "30", endTimeUnix: time.Now().Add(-time.Hour).Unix()},
			})
			results := make(chan *EpochCompareResult, 32)
			o, err := NewObserver(ObserverConfig{Network: "preview", CachePath: filepath.Join(t.TempDir(), "cache.db"), Source: source, BaseURL: srv.URL, AllowInsecureHTTP: true, AllowPrivateAddresses: true, Strict: true, GraceHours: 24, AccountsEnabled: accounts, ErrorRetryDelay: 100 * time.Millisecond, OnResult: func(r *EpochCompareResult) { results <- r }})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, o.Stop(context.Background())) })
			require.NoError(t, o.Start(context.Background()))
			eb := event.NewEventBus(nil, nil)
			t.Cleanup(eb.Stop)
			eb.SubscribeFunc(event.EpochTransitionEventType, o.HandleEpochTransitionEvent)
			publishEpochTransition(eb, 5)
			first := testutil.RequireReceive(t, results, 5*time.Second, "initial reference lag")
			require.Equal(t, StatusError, first.Status)
			require.True(t, referenceLagOnly(first.Mismatches))
			require.False(t, o.stopping())
			if !accounts {
				statuses, e := o.cache.GetStatusSummary("preview")
				require.NoError(t, e)
				require.Len(t, statuses, 1)
				require.Equal(t, CountSignificant(first.Mismatches), statuses[0].SignificantMismatchCount)
				select {
				case unexpected := <-results:
					t.Fatalf("retry occurred before minimum delay: %s", unexpected.Status)
				case <-time.After(o.cfg.ErrorRetryDelay / 2):
				}
			}

			seedDingoEpochAggregate(t, source, 5, 1_000_000, 10, 20, 30)
			testutil.WaitForCondition(t, func() bool {
				statuses, e := o.cache.GetStatusSummary("preview")
				return e == nil && len(statuses) == 1 && statuses[0].Status == StatusPass && (!accounts || statuses[0].AccountStatus == StatusPass)
			}, 5*time.Second, "error must recover without another event or reference refresh")
			if !accounts {
				for {
					r := testutil.RequireReceive(t, results, 5*time.Second, "automatic successful recheck")
					if r.Status == StatusPass {
						break
					}
				}
				select {
				case unexpected := <-results:
					t.Fatalf("healthy epoch retried without event: %s", unexpected.Status)
				case <-time.After(3 * o.cfg.ErrorRetryDelay):
				}
			}
			require.NoError(t, o.Stop(context.Background()))
		})
	}
}

func TestSignificantCountsSurviveIndependentPhaseWritesAndReports(t *testing.T) {
	t.Parallel()
	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	write := func(s CheckEpochStatus) {
		s.Network = "preview"
		s.Epoch = 9
		s.LastCheckedAt = time.Now()
		require.NoError(t, cache.UpsertCheckEpochStatus(s))
	}
	write(CheckEpochStatus{AggregateStatus: StatusFail, AggregateMismatchCount: 10, AggregateSignificantMismatchCount: 2, AccountStatus: StatusError, AccountMismatchCount: 4, AccountSignificantMismatchCount: 1})
	write(CheckEpochStatus{AggregateStatus: StatusPass, AggregateMismatchCount: 7, AggregateSignificantMismatchCount: 0})
	statuses, err := cache.GetStatusSummary("preview")
	require.NoError(t, err)
	require.Len(t, statuses, 1)
	require.Equal(t, 11, statuses[0].MismatchCount)
	require.Equal(t, 1, statuses[0].SignificantMismatchCount)
	require.Equal(t, StatusError, statuses[0].Status)
	report, err := BuildJSONReport("preview", "", nil, statuses, nil)
	require.NoError(t, err)
	var out bytes.Buffer
	require.NoError(t, WriteJSONReport(&out, report))
	require.Contains(t, out.String(), `"mismatch_count": 11`)
	require.Contains(t, out.String(), `"significant_mismatch_count": 1`)
	write(CheckEpochStatus{AggregateStatus: StatusFail, AggregateMismatchCount: 7, AggregateSignificantMismatchCount: 2, AccountStatus: StatusPass})
	statuses, err = cache.GetStatusSummary("preview")
	require.NoError(t, err)
	require.Equal(t, 7, statuses[0].MismatchCount)
	require.Equal(t, 2, statuses[0].SignificantMismatchCount)
	out.Reset()
	PrintStatus(&out, BuildStatusSummary("preview", nil, statuses), true, statuses)
	require.Contains(t, out.String(), "2 significant of 7 mismatches")
}

func TestErrorEpochsAreSelectedAfterReferenceBecomesUnchanged(t *testing.T) {
	t.Parallel()
	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	now := time.Now().UTC()
	for epoch, status := range map[uint64]string{5: StatusError, 6: StatusPass, 7: StatusFail} {
		require.NoError(t, cache.UpsertEpochInfo(KoiosEpochInfo{Network: "preview", Epoch: epoch, FetchedAt: now.Add(-time.Hour)}))
		require.NoError(t, cache.UpsertCheckEpochStatus(CheckEpochStatus{Network: "preview", Epoch: epoch, LastCheckedAt: now, Status: status}))
	}
	fresh, err := cache.GetEpochsNeedingCheck("preview", false)
	require.NoError(t, err)
	require.Empty(t, fresh)
	epochs, err := cache.GetEpochsNeedingRetry("preview", false)
	require.NoError(t, err)
	require.Equal(t, []uint64{5}, epochs)
	accounts, err := cache.GetEpochsNeedingRetry("preview", true)
	require.NoError(t, err)
	require.Empty(t, accounts)
}

func TestSignificantCountMigrationUsesStoredCategoryAndScope(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "cache.db")
	cache, err := openTestCache(path, nil)
	require.NoError(t, err)
	categories := []string{CategoryPoolDeparted, CategoryPoolZeroStake, CategoryAcctZeroReward, CategoryAcctZeroRewardRow, CategoryAcctNewlyRegistered, CategoryAcctDeregistered, CategoryCostModelSynthetic, CategoryDBError, CategoryReferenceLag, CategoryValueMismatch, "unknown_category"}
	mismatches := make([]CheckMismatch, 0, len(categories))
	for i, category := range categories {
		scope := ScopeAggregate
		if i%2 == 0 {
			scope = ScopeAccount
		}
		mismatches = append(mismatches, CheckMismatch{Network: "preview", Epoch: 9, Field: category, Category: category, Scope: scope, CheckedAt: time.Now()})
	}
	require.NoError(t, cache.CommitEpochMismatches("preview", 9, mismatches, ScopeAggregate, ScopeAccount))
	require.NoError(t, cache.UpsertCheckEpochStatus(CheckEpochStatus{Network: "preview", Epoch: 9, LastCheckedAt: time.Now(), AggregateStatus: StatusFail, AggregateMismatchCount: 5, AccountStatus: StatusFail, AccountMismatchCount: 6}))
	require.NoError(t, cache.Close())
	db, err := sql.Open("sqlite", path+"?"+legacySeedPragmas)
	require.NoError(t, err)
	for _, column := range []string{"significant_mismatch_count", "aggregate_significant_mismatch_count", "account_significant_mismatch_count"} {
		_, err = db.Exec("ALTER TABLE check_epoch_status DROP COLUMN " + column)
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())
	for range 2 {
		cache, err = openTestCache(path, nil)
		require.NoError(t, err)
		statuses, err := cache.GetStatusSummary("preview")
		require.NoError(t, err)
		require.Len(t, statuses, 1)
		require.Equal(t, len(categories), statuses[0].MismatchCount)
		require.Equal(t, CountSignificant(mismatches), statuses[0].SignificantMismatchCount)
		require.Equal(t, 2, statuses[0].AggregateSignificantMismatchCount)
		require.Equal(t, 2, statuses[0].AccountSignificantMismatchCount)
		require.NoError(t, cache.Close())
	}
}

func TestObserverSeedsErrorRetriesForTheOwningPhase(t *testing.T) {
	t.Parallel()
	for _, accountError := range []bool{false, true} {
		t.Run(map[bool]string{false: "aggregate", true: "account"}[accountError], func(t *testing.T) {
			db := newTestDatabaseSourceDB(t)
			source, err := NewDatabaseSource(db)
			require.NoError(t, err)
			require.NoError(t, db.SetEpoch(1000, 4, nil, nil, nil, nil, 5, 20, 432000, nil))
			require.NoError(t, db.SetSyncState("mithril_ledger_slot", "1000", nil))
			require.NoError(t, sourceSQLDB(t, db).Create(&models.EpochSummary{Epoch: 6, SnapshotReady: true}).Error)
			o, err := NewObserver(ObserverConfig{Network: "preview", Source: source, CachePath: filepath.Join(t.TempDir(), "cache.db"), AccountsEnabled: true})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, o.Stop(context.Background())) })
			fetched := time.Now().Add(-time.Hour)
			require.NoError(t, o.cache.CommitEpochData(KoiosEpochInfo{Network: "preview", Epoch: 5, ActiveStake: "1", FetchedAt: fetched}, nil, &KoiosTotals{Treasury: "1", Reserves: "1", Fees: "1", FetchedAt: fetched}))
			require.NoError(t, o.cache.CommitAccountRewardsForEpoch("preview", 5, nil, 0, true, fetched))
			seedKoiosBabbageProtocolParams(t, o.cache, "preview", 5)
			aggregate, account := StatusError, StatusPass
			if accountError {
				aggregate, account = StatusPass, StatusError
			}
			require.NoError(t, o.cache.UpsertCheckEpochStatus(CheckEpochStatus{Network: "preview", Epoch: 5, LastCheckedAt: time.Now(), AggregateStatus: aggregate, AccountStatus: account}))
			require.NoError(t, o.seedBacklog(context.Background()))
			if accountError {
				require.Empty(t, o.pending)
				require.Equal(t, map[uint64]struct{}{5: {}}, o.pendingAccounts)
			} else {
				require.Equal(t, map[uint64]struct{}{5: {}}, o.pending)
				require.Empty(t, o.pendingAccounts)
			}
		})
	}
}

func TestAccountRetryUsesItsOwnCompletedPhase(t *testing.T) {
	t.Parallel()
	for _, accountError := range []bool{false, true} {
		t.Run(map[bool]string{false: "aggregate error account pass", true: "aggregate fail account error"}[accountError], func(t *testing.T) {
			source, err := NewDatabaseSource(newTestDatabaseSourceDB(t))
			require.NoError(t, err)
			seedDingoEpochAggregate(t, source, 5, 2_000_000, 10, 20, 30)
			db := sourceSQLDB(t, source.db)
			if accountError {
				require.NoError(t, db.Create(&models.RewardAccountOutput{
					Epoch: 4, StakingKey: []byte{1}, PoolKeyHash: testPoolKeyHash(t, 0x66),
					RewardType: "member", Amount: types.Uint64(1), Spendable: true,
				}).Error)
			} else {
				require.NoError(t, db.Exec("DELETE FROM epoch_summary WHERE epoch = ?", 4).Error)
			}
			srv := newFakeKoiosServer(t, map[uint64]*fakeEpochRef{
				5: {activeStake: "1000000", treasury: "10", reserves: "20", fees: "30", endTimeUnix: time.Now().Add(-time.Hour).Unix()},
			})
			var result *EpochCompareResult
			o, err := NewObserver(ObserverConfig{
				Network: "preview", Source: source, CachePath: filepath.Join(t.TempDir(), "cache.db"),
				BaseURL: srv.URL, AllowInsecureHTTP: true, AllowPrivateAddresses: true,
				AccountsEnabled: true, GraceHours: 24,
				OnResult: func(r *EpochCompareResult) { result = r },
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, o.Stop(context.Background())) })
			o.retryAccounts[5] = time.Now()
			o.processAccountEpoch(context.Background(), 5)
			require.NotNil(t, result)
			require.True(t, result.CoversScope(ScopeAccount))
			statuses, err := o.cache.GetStatusSummary("preview")
			require.NoError(t, err)
			require.Len(t, statuses, 1)
			if accountError {
				require.Equal(t, StatusFail, result.Status)
				require.Equal(t, StatusFail, statuses[0].AggregateStatus)
				require.Equal(t, StatusError, statuses[0].AccountStatus)
				require.Contains(t, o.retryAccounts, uint64(5))
				require.True(t, o.retryAccounts[5].After(time.Now()))
			} else {
				require.Equal(t, StatusError, result.Status)
				require.Equal(t, StatusError, statuses[0].AggregateStatus)
				require.Equal(t, StatusPass, statuses[0].AccountStatus)
				require.NotContains(t, o.retryAccounts, uint64(5))
			}
		})
	}
}
