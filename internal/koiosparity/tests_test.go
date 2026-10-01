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
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// TestAccountLifecycleMismatchesReportsZeroReward proves
// zero-reward-confirmed reporting: an address Koios answered for with no
// reward rows is reported via CategoryAcctZeroReward — a dimension
// merged CompareAccountEpoch structurally cannot see (it only ever compares
// keys present in at least one side's row map). Reported as one aggregate
// row (count + a capped sample), not one row per address — see
// aggregateAccountLifecycleMismatch's doc comment. stakeEpoch=0 skips the
// separate lifecycle (newly-registered/deregistered) diff entirely, keeping
// this test focused on zero-reward alone.
func TestAccountLifecycleMismatchesReportsZeroReward(t *testing.T) {
	t.Parallel()

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	now := time.Now()
	// addr1 earns a reward (1 staged row); addr2 is confirmed checked with
	// zero reward rows (present in addressesInChunk, no matching row).
	require.NoError(t, cache.SaveAccountFetchChunkProgress(
		"preview",
		500,
		"chunkA",
		[]KoiosAccountRewards{
			{StakeAddress: "addr1", RewardType: "member", Earned: "1000"},
		},
		[]string{"addr1", "addr2"},
		now,
	))

	mismatches := accountLifecycleMismatches(
		context.Background(), cache, nil, "preview", 500, 0, nil, now,
	)
	require.Len(t, mismatches, 1)
	require.Equal(t, CategoryAcctZeroReward, mismatches[0].Category)
	require.Equal(
		t,
		"1",
		mismatches[0].KoiosValue,
		"KoiosValue carries the affected-address count, not a single address",
	)
	require.Contains(t, mismatches[0].DingoValue, "addr2")
}

// TestAccountLifecycleMismatchesReportsNewlyRegisteredAndDeregistered proves
// the epoch-over-epoch universe diff: an address present in the current
// stake epoch's Dingo-committed reward_account_output rows but not the
// previous stake epoch's is newly registered; the reverse is deregistered.
// Uses a real DingoDB (sqlite fixture), not a hand-rolled fake, per this
// package's existing RewardParitySource test convention
// (dingo_db_test.go/fetch_accounts_test.go).
func TestAccountLifecycleMismatchesReportsNewlyRegisteredAndDeregistered(
	t *testing.T,
) {
	t.Parallel()

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck

	addrOldKey := testPoolKeyHash(t, 0x01)
	addrBothKey := testPoolKeyHash(t, 0x02)
	addrNewKey := testPoolKeyHash(t, 0x03)
	poolKey := testPoolKeyHash(t, 0xAA)

	// Previous stake epoch (498): addrOld, addrBoth.
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 498, StakingKey: addrOldKey, PoolKeyHash: poolKey,
		RewardType: "member", Amount: types.Uint64(1000), Spendable: true,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 498, StakingKey: addrBothKey, PoolKeyHash: poolKey,
		RewardType: "member", Amount: types.Uint64(1000), Spendable: true,
	}).Error)

	// Current stake epoch (499): addrBoth, addrNew.
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 499, StakingKey: addrBothKey, PoolKeyHash: poolKey,
		RewardType: "member", Amount: types.Uint64(1000), Spendable: true,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 499, StakingKey: addrNewKey, PoolKeyHash: poolKey,
		RewardType: "member", Amount: types.Uint64(1000), Spendable: true,
	}).Error)

	ctx := context.Background()
	currentOutputs, err := dingo.GetRewardAccountOutputs(ctx, 499)
	require.NoError(t, err)
	require.Len(t, currentOutputs, 2)

	now := time.Now()
	mismatches := accountLifecycleMismatches(
		ctx, cache, dingo, "preview", 500, 499, currentOutputs, now,
	)

	wantNewAddr, err := StakeAddressFromCredential(addrNewKey, 0)
	require.NoError(t, err)
	wantOldAddr, err := StakeAddressFromCredential(addrOldKey, 0)
	require.NoError(t, err)

	var sawNew, sawDeregistered bool
	for _, m := range mismatches {
		switch m.Category {
		case CategoryAcctNewlyRegistered:
			require.Equal(t, "1", m.KoiosValue)
			require.Contains(t, m.DingoValue, wantNewAddr)
			sawNew = true
		case CategoryAcctDeregistered:
			require.Equal(t, "1", m.KoiosValue)
			require.Contains(t, m.DingoValue, wantOldAddr)
			sawDeregistered = true
		default:
			t.Fatalf("unexpected category %q", m.Category)
		}
	}
	require.True(
		t,
		sawNew,
		"the new address must be reported as newly registered",
	)
	require.True(
		t,
		sawDeregistered,
		"the old address must be reported as deregistered",
	)
}

// TestAccountLifecycleMismatchesZeroRewardRowCountIsBounded proves the fix
// for a real scale problem: reporting one CheckMismatch row per zero-reward
// address would make cache growth, insert time, and JSON report size scale
// with the size of the account universe (Koios never emits a row at all for
// a zero-reward account, so on a large network most checked addresses can
// fall into this category). Regardless of how many zero-reward addresses
// exist, exactly one aggregate row must be produced, with an accurate total
// count and a sample capped at maxAccountLifecycleSample.
func TestAccountLifecycleMismatchesZeroRewardRowCountIsBounded(t *testing.T) {
	t.Parallel()

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	now := time.Now()
	const numZeroReward = maxAccountLifecycleSample * 5
	addrs := make([]string, numZeroReward)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("addr%04d", i)
	}
	require.NoError(t, cache.SaveAccountFetchChunkProgress(
		"preview", 500, "chunkA", nil, addrs, now,
	))

	mismatches := accountLifecycleMismatches(
		context.Background(), cache, nil, "preview", 500, 0, nil, now,
	)
	require.Len(
		t,
		mismatches,
		1,
		"any number of zero-reward addresses must still produce exactly one aggregate row",
	)
	require.Equal(t, strconv.Itoa(numZeroReward), mismatches[0].KoiosValue)
	sampleAddrs := strings.Split(
		strings.TrimPrefix(mismatches[0].DingoValue, "sample: "),
		",",
	)
	require.Len(
		t,
		sampleAddrs,
		maxAccountLifecycleSample,
		"the embedded sample must never grow with the total count",
	)
}

// TestAccountLifecycleMismatchesStakeEpochZeroSkipsLifecycleReport proves
// stakeEpoch==0 (no possible previous stake epoch, stakeEpoch-1 would
// underflow) skips the newly-registered/deregistered diff entirely, without
// ever dereferencing the dingo source.
func TestAccountLifecycleMismatchesStakeEpochZeroSkipsLifecycleReport(
	t *testing.T,
) {
	t.Parallel()

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	mismatches := accountLifecycleMismatches(
		context.Background(), cache, nil, "preview", 500, 0, nil, time.Now(),
	)
	require.Empty(t, mismatches)
}

// TestAccountLifecycleMismatchesPropagatesDingoErrorAsDBError proves a
// genuine Dingo DB failure while fetching the previous stake epoch's
// reward_account_output rows is reported as CategoryDBError, never silently
// swallowed as if there were simply no lifecycle changes to report.
func TestAccountLifecycleMismatchesPropagatesDingoErrorAsDBError(t *testing.T) {
	t.Parallel()

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck

	// Simulate a genuine Dingo DB failure (not "no rows") by closing the
	// underlying connection before it's queried — mirrors
	// TestCheckAccountsCoverageDBErrorIsNotConflatedWithIncompleteCoverage's
	// established technique for this exact distinction.
	sqlDB, err := gdb.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	mismatches := accountLifecycleMismatches(
		context.Background(),
		cache,
		dingo,
		"preview",
		500,
		499,
		nil,
		time.Now(),
	)
	require.Len(t, mismatches, 1)
	require.Equal(t, CategoryDBError, mismatches[0].Category)
}

// TestAccountLifecycleMismatchesReportsMalformedPreviousRowAsDBError proves
// a previous-stake-epoch reward_account_output row with an unsupported
// credential tag is reported as CategoryDBError rather than silently
// dropped — silently dropping it would make that row's address look
// deregistered (present last epoch, "gone" this epoch) purely because it
// failed to decode, not because it actually changed.
func TestAccountLifecycleMismatchesReportsMalformedPreviousRowAsDBError(
	t *testing.T,
) {
	t.Parallel()

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck

	goodKey := testPoolKeyHash(t, 0x10)
	badKey := testPoolKeyHash(t, 0x11)
	poolKey := testPoolKeyHash(t, 0xAA)

	// Previous stake epoch (498): one well-formed row, one with an
	// unsupported credential tag (only 0 and 1 are valid — see
	// StakeAddressFromCredential).
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 498, StakingKey: goodKey, PoolKeyHash: poolKey,
		RewardType: "member", CredentialTag: 0,
		Amount: types.Uint64(1000), Spendable: true,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 498, StakingKey: badKey, PoolKeyHash: poolKey,
		RewardType: "member", CredentialTag: 9,
		Amount: types.Uint64(1000), Spendable: true,
	}).Error)

	ctx := context.Background()
	mismatches := accountLifecycleMismatches(
		ctx, cache, dingo, "preview", 500, 499, nil, time.Now(),
	)

	var sawDecodeError bool
	for _, m := range mismatches {
		if m.Category == CategoryDBError &&
			m.Field == "reward_account_output_address_decode" {
			sawDecodeError = true
			require.Contains(t, m.DingoValue, "1")
		}
	}
	require.True(
		t,
		sawDecodeError,
		"the malformed previous-epoch row must be reported, not silently dropped",
	)
}

// TestAccountLifecycleMismatchesSkipsLifecycleDiffWhenCurrentRowsFailToDecode
// proves the diff itself is skipped — not just reported alongside — when the
// *current* stake epoch has a malformed reward_account_output row. Before
// this fix, dingoRewardAddressSet's decodeErrs return value was discarded
// for currentOutputs, so a decode failure there silently produced an
// incomplete currSet: a well-formed address present in both epochs would
// then look deregistered purely because a different, unrelated row failed
// to decode, not because it actually changed.
func TestAccountLifecycleMismatchesSkipsLifecycleDiffWhenCurrentRowsFailToDecode(
	t *testing.T,
) {
	t.Parallel()

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck

	goodKey := testPoolKeyHash(t, 0x20)
	badKey := testPoolKeyHash(t, 0x21)
	poolKey := testPoolKeyHash(t, 0xAA)

	// Previous stake epoch (498): the well-formed address, plus badKey —
	// well-formed here (tag 0) so it decodes fine into prevSet. Without this,
	// badKey (only ever present in currentOutputs) would be absent from both
	// sets regardless of whether currDecodeErrs is honored, and the test
	// would pass even with the pre-fix code that silently discarded it — see
	// this test's own history for why that made it a non-regression-test.
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 498, StakingKey: goodKey, PoolKeyHash: poolKey,
		RewardType: "member", CredentialTag: 0,
		Amount: types.Uint64(1000), Spendable: true,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 498, StakingKey: badKey, PoolKeyHash: poolKey,
		RewardType: "member", CredentialTag: 0,
		Amount: types.Uint64(1000), Spendable: true,
	}).Error)

	// Current stake epoch (499): the same well-formed address (still
	// registered) plus badKey's row now with an unsupported credential tag —
	// so pre-fix, badKey would be dropped from currSet, found in prevSet, and
	// falsely reported as CategoryAcctDeregistered.
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 499, StakingKey: goodKey, PoolKeyHash: poolKey,
		RewardType: "member", CredentialTag: 0,
		Amount: types.Uint64(1000), Spendable: true,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 499, StakingKey: badKey, PoolKeyHash: poolKey,
		RewardType: "member", CredentialTag: 9,
		Amount: types.Uint64(1000), Spendable: true,
	}).Error)

	ctx := context.Background()
	currentOutputs, err := dingo.GetRewardAccountOutputs(ctx, 499)
	require.NoError(t, err)
	require.Len(t, currentOutputs, 2)

	now := time.Now()
	mismatches := accountLifecycleMismatches(
		ctx, cache, dingo, "preview", 500, 499, currentOutputs, now,
	)

	for _, m := range mismatches {
		require.NotEqual(
			t,
			CategoryAcctNewlyRegistered,
			m.Category,
			"the lifecycle diff must be skipped entirely, not just under-reported",
		)
		require.NotEqual(
			t,
			CategoryAcctDeregistered,
			m.Category,
			"the still-registered address must never be misreported as deregistered "+
				"just because an unrelated current-epoch row failed to decode",
		)
	}
}

// TestAccountLifecycleMismatchesSkipsLifecycleDiffForPrunableSource proves
// the newly-registered/deregistered diff is skipped entirely for a
// *DatabaseSource — the in-process observer's reward source reads through
// core-mode's rolling pruning window and cannot distinguish "the previous
// stake epoch genuinely had no reward accounts" from "its rows have since
// been pruned" (both surface as an empty, error-free result). Treating that
// ambiguous empty result as a complete previous-epoch universe would make
// every current account look newly registered — so the diff must not even
// attempt it for this source type. Zero-reward reporting must still work,
// since it doesn't depend on any historical epoch's data.
func TestAccountLifecycleMismatchesSkipsLifecycleDiffForPrunableSource(
	t *testing.T,
) {
	t.Parallel()

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	// addr is confirmed checked with zero reward — zero-reward reporting
	// should still fire even though the lifecycle diff below is skipped.
	now := time.Now()
	require.NoError(t, cache.SaveAccountFetchChunkProgress(
		"preview", 500, "chunkA", nil, []string{"addr1"}, now,
	))

	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	mismatches := accountLifecycleMismatches(
		context.Background(), cache, source, "preview", 500, 499, nil, now,
	)

	var sawZeroReward bool
	for _, m := range mismatches {
		require.NotEqual(
			t,
			CategoryAcctNewlyRegistered,
			m.Category,
			"the lifecycle diff must never run at all for a prunable source",
		)
		require.NotEqual(t, CategoryAcctDeregistered, m.Category)
		if m.Category == CategoryAcctZeroReward {
			sawZeroReward = true
		}
	}
	require.True(
		t,
		sawZeroReward,
		"zero-reward reporting must still work for a prunable source",
	)
}

// TestAccountLifecycleMismatchesPropagatesCacheErrorAsDBError proves a
// genuine cache failure while looking up zero-reward accounts is reported as
// CategoryDBError, never silently swallowed.
func TestAccountLifecycleMismatchesPropagatesCacheErrorAsDBError(t *testing.T) {
	t.Parallel()

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	// Force a genuine query error (not "no rows") for
	// GetZeroRewardAccountsForEpoch by dropping the table its SELECT reads
	// from — mirrors the "break the schema, not just leave it empty"
	// technique check_test.go's own DB-error tests already use.
	_, err = cache.db.Exec("DROP TABLE koios_account_checked")
	require.NoError(t, err)

	mismatches := accountLifecycleMismatches(
		context.Background(), cache, nil, "preview", 500, 0, nil, time.Now(),
	)
	require.Len(t, mismatches, 1)
	require.Equal(t, CategoryDBError, mismatches[0].Category)
}

// TestDetermineStatusAccountLifecycleCategoriesAreInformational proves the
// three categories never affect Status: alone they must PASS,
// and alongside a genuine FAIL-triggering mismatch they must not mask or
// alter that FAIL.
func TestDetermineStatusAccountLifecycleCategoriesAreInformational(
	t *testing.T,
) {
	t.Parallel()

	now := time.Now()
	onlyInformational := []CheckMismatch{
		{Category: CategoryAcctZeroReward, CheckedAt: now},
		{Category: CategoryAcctNewlyRegistered, CheckedAt: now},
		{Category: CategoryAcctDeregistered, CheckedAt: now},
	}
	require.Equal(t, StatusPass, DetermineStatus(onlyInformational))

	withRealFailure := append(
		append([]CheckMismatch{}, onlyInformational...),
		CheckMismatch{Category: CategoryValueMismatch, CheckedAt: now},
	)
	require.Equal(t, StatusFail, DetermineStatus(withRealFailure))
}

// newAccountListServer serves a one-page /account_list and counts the requests
// it answers, so a test can assert how many times the universe was crawled.
func newAccountListServer(
	t *testing.T,
	calls *atomic.Int32,
	addrs ...string,
) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/account_list" {
				http.NotFound(w, r)
				return
			}
			calls.Add(1)
			var sb strings.Builder
			sb.WriteByte('[')
			for i, addr := range addrs {
				if i > 0 {
					sb.WriteByte(',')
				}
				fmt.Fprintf(&sb, `{"stake_address":%q}`, addr)
			}
			sb.WriteByte(']')
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(sb.String()))
		}),
	)
	t.Cleanup(srv.Close)
	return srv
}

func newUniverseTestClient(srv *httptest.Server) *KoiosClient {
	return &KoiosClient{
		baseURL: srv.URL,
		http:    &http.Client{Timeout: 2 * time.Second},
		limiter: newBurstLimiter(0, koiosBurstWindow),
	}
}

// TestResolveKoiosAccountUniverseCachedReusesCrawlAcrossEpochs is the point of
// the cache. The crawl is 304 sequential /account_list requests on Preview, and
// paying it once per epoch is why the in-process observer could not keep pace
// with a syncing node. A second epoch whose end time the cached
// crawl already covers must not touch Koios again.
func TestResolveKoiosAccountUniverseCachedReusesCrawlAcrossEpochs(
	t *testing.T,
) {
	t.Parallel()

	var calls atomic.Int32
	srv := newAccountListServer(t, &calls, "stake_test1a", "stake_test1b")
	koios := newUniverseTestClient(srv)
	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	epochEnd := time.Now().Add(-time.Hour)
	logger := slog.New(slog.DiscardHandler)

	first, err := ResolveKoiosAccountUniverseCached(
		context.Background(), koios, cache, "preview", epochEnd, logger,
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"stake_test1a", "stake_test1b"}, first)
	require.Equal(t, int32(1), calls.Load())

	// A later epoch that also ended before the crawl is fully covered by it.
	second, err := ResolveKoiosAccountUniverseCached(
		context.Background(), koios, cache, "preview",
		epochEnd.Add(30*time.Minute), logger,
	)
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Equal(t, int32(1), calls.Load(),
		"a cached crawl covering the epoch must not be re-fetched")

	// Control: the uncached resolver the observer used to call directly does
	// crawl again, so the counter above is measuring what it claims to.
	_, err = ResolveKoiosAccountUniverse(context.Background(), koios)
	require.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load())
}

// TestResolveKoiosAccountUniverseCachedRefreshesForNewerEpoch is the other
// half. An account that earned a reward in an epoch registered before that
// epoch ended, so a crawl taken before the epoch closed may be missing one and
// cannot be reused — a short universe silently skips accounts, which reads as
// a pass.
func TestResolveKoiosAccountUniverseCachedRefreshesForNewerEpoch(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	srv := newAccountListServer(t, &calls, "stake_test1a")
	koios := newUniverseTestClient(srv)
	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	logger := slog.New(slog.DiscardHandler)
	_, err = ResolveKoiosAccountUniverseCached(
		context.Background(), koios, cache, "preview",
		time.Now().Add(-time.Hour), logger,
	)
	require.NoError(t, err)
	require.Equal(t, int32(1), calls.Load())

	// An epoch that closed after the crawl was taken.
	_, err = ResolveKoiosAccountUniverseCached(
		context.Background(), koios, cache, "preview",
		time.Now().Add(time.Hour), logger,
	)
	require.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load(),
		"a crawl older than the epoch's close must be refreshed")
}

// TestAccountUniverseCacheRoundTrip pins the storage contract the resolver
// relies on: a save replaces the previous set wholesale rather than merging
// into it, so a shrinking universe cannot leave a stale address behind.
func TestAccountUniverseCacheRoundTrip(t *testing.T) {
	t.Parallel()

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	addrs, fetchedAt, cached, err := cache.GetAccountUniverse("preview")
	require.NoError(t, err)
	assert.Empty(t, addrs)
	assert.False(t, cached, "nothing cached yet")
	assert.True(t, fetchedAt.IsZero())

	first := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	require.NoError(t, cache.SaveAccountUniverse(
		"preview", []string{"stake_test1a", "stake_test1b"}, first,
	))
	require.NoError(t, cache.SaveAccountUniverse(
		"preprod", []string{"stake_test1z"}, first,
	))

	addrs, fetchedAt, cached, err = cache.GetAccountUniverse("preview")
	require.NoError(t, err)
	assert.True(t, cached)
	assert.Equal(t, []string{"stake_test1a", "stake_test1b"}, addrs)
	assert.WithinDuration(t, first, fetchedAt, time.Second)

	second := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, cache.SaveAccountUniverse(
		"preview", []string{"stake_test1b"}, second,
	))
	addrs, fetchedAt, cached, err = cache.GetAccountUniverse("preview")
	require.NoError(t, err)
	assert.True(t, cached)
	assert.Equal(t, []string{"stake_test1b"}, addrs,
		"a save replaces the set rather than merging into it")
	assert.WithinDuration(t, second, fetchedAt, time.Second)

	other, _, cached, err := cache.GetAccountUniverse("preprod")
	require.NoError(t, err)
	assert.True(t, cached)
	assert.Equal(t, []string{"stake_test1z"}, other,
		"networks are stored independently")
}

// TestResolveKoiosAccountUniverseCachedWithEmptyCrawl covers a network whose
// /account_list is legitimately empty. Presence is recorded separately from the
// address rows, so an empty crawl is still a cached crawl and a later epoch it
// covers does not pay for it again.
func TestResolveKoiosAccountUniverseCachedWithEmptyCrawl(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	srv := newAccountListServer(t, &calls)
	koios := newUniverseTestClient(srv)
	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	epochEnd := time.Now().Add(-time.Hour)
	logger := slog.New(slog.DiscardHandler)
	for range 2 {
		addrs, err := ResolveKoiosAccountUniverseCached(
			context.Background(), koios, cache, "preview", epochEnd, logger,
		)
		require.NoError(t, err)
		assert.Empty(t, addrs)
	}
	assert.Equal(t, int32(1), calls.Load(),
		"an empty crawl is still a cached crawl")
}

// TestResolveKoiosAccountUniverseCachedRefusesUnboundedReuse covers an epoch
// whose end time the cache does not carry. There is then nothing to measure the
// crawl against, and reusing it anyway could skip an account that registered
// between the crawl and the epoch's close — a short universe reads as a pass.
func TestResolveKoiosAccountUniverseCachedRefusesUnboundedReuse(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	srv := newAccountListServer(t, &calls, "stake_test1a")
	koios := newUniverseTestClient(srv)
	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck
	logger := slog.New(slog.DiscardHandler)

	for range 2 {
		_, err = ResolveKoiosAccountUniverseCached(
			context.Background(), koios, cache, "preview", time.Time{}, logger,
		)
		require.NoError(t, err)
	}
	assert.Equal(t, int32(2), calls.Load(),
		"with no bound to measure against, the crawl is not reused")

	assert.False(t, accountUniverseFresh(time.Time{}, time.Now()),
		"no crawl is never fresh")
	assert.False(t, accountUniverseFresh(time.Now(), time.Time{}),
		"no bound is never fresh")
	assert.True(t, accountUniverseFresh(
		time.Now(), time.Now().Add(-time.Minute),
	))
}

// TestAccountUniverseStateBackfilledOnUpgrade covers a cache written before
// koios_account_universe_state existed: the crawl's rows are there but the
// state row is not, which would read as "never crawled" and pay for a full
// /account_list walk on first use. The schema migration backfills it.
func TestAccountUniverseStateBackfilledOnUpgrade(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "cache.db")
	cache, err := openTestCache(path, nil)
	require.NoError(t, err)

	fetchedAt := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	require.NoError(t, cache.SaveAccountUniverse(
		"preview", []string{"stake_test1a", "stake_test1b"}, fetchedAt,
	))
	// Drop the state row to reproduce the older layout, then reopen so the
	// schema pass runs against it.
	_, err = cache.db.Exec(`DELETE FROM koios_account_universe_state`)
	require.NoError(t, err)
	require.NoError(t, cache.Close())

	reopened, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer reopened.Close() //nolint:errcheck

	addrs, got, cached, err := reopened.GetAccountUniverse("preview")
	require.NoError(t, err)
	assert.True(t, cached, "the existing crawl must survive the upgrade")
	assert.Equal(t, []string{"stake_test1a", "stake_test1b"}, addrs)
	assert.WithinDuration(t, fetchedAt, got, time.Second)
}

// TestNewKoiosClientBaseURLOverride covers pointing the client at a self-hosted
// Koios instance. The requests have to actually reach that host, not the public
// one, so the assertion is a served request rather than a field comparison.
func TestNewKoiosClientBaseURLOverride(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/v1/tip" {
				http.NotFound(w, r)
				return
			}
			hits.Add(1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"epoch_no":42}]`))
		}),
	)
	defer srv.Close()

	// httptest serves plain HTTP, so this is the one case that needs the
	// insecure escape hatch.
	client, err := NewKoiosClient("preview", "", srv.URL+"/api/v1", true, true)
	require.NoError(t, err)

	epoch, err := client.GetTipEpoch(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(42), epoch)
	assert.Equal(t, int32(1), hits.Load(),
		"the request must reach the configured host")
}

func TestNewKoiosClientAllowsPrivateAddressOnlyWhenExplicit(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"epoch_no":42}]`))
		}),
	)
	defer srv.Close()

	_, err := NewKoiosClient("preview", "", srv.URL+"/api/v1", true, false)
	require.Error(t, err)

	client, err := NewKoiosClient(
		"preview", "", srv.URL+"/api/v1", true, true,
	)
	require.NoError(t, err)
	_, err = client.GetTipEpoch(context.Background())
	require.NoError(t, err)
}

func TestNewKoiosClientRejectsPrivateAddressByDefault(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		"https://127.0.0.1/api/v1",
		"https://[::1]/api/v1",
		"https://169.254.169.254/latest/meta-data",
		"https://localhost/api/v1",
		"https://192.0.2.1/api/v1",
		"https://0.0.0.1/api/v1",
		"https://[64:ff9b::7f00:1]/api/v1",
		"https://[100:0:0:1::1]/api/v1",
		"https://[2001:db8::1]/api/v1",
		"https://[2002:7f00:1::]/api/v1",
		"https://[2620:4f:8000::1]/api/v1",
	} {
		_, err := NewKoiosClient("preview", "", raw, false, false)
		require.Error(t, err, "base URL %q must be rejected", raw)
	}
}

func TestNewKoiosClientRestrictsRedirectsAndDisablesProxy(t *testing.T) {
	t.Parallel()

	client, err := NewKoiosClient("preview", "", "", false, false)
	require.NoError(t, err)
	require.NotNil(t, client.http.CheckRedirect)

	req, err := http.NewRequest(
		http.MethodGet,
		"https://127.0.0.1/api/v1/tip",
		nil,
	)
	require.NoError(t, err)
	require.Error(t, client.http.CheckRedirect(req, nil))

	publicReq, err := http.NewRequest(
		http.MethodGet,
		"https://mirror.example/api/v1/tip",
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, client.http.CheckRedirect(publicReq, nil))

	transport, ok := client.http.Transport.(*http.Transport)
	require.True(t, ok)
	assert.Nil(t, transport.Proxy,
		"ambient proxy settings must not bypass destination validation")
}

// TestNewKoiosClientBaseURLTrimsTrailingSlash pins the ergonomics: an operator
// pasting a root with a trailing slash must not produce doubled separators.
func TestNewKoiosClientBaseURLTrimsTrailingSlash(t *testing.T) {
	client, err := NewKoiosClient(
		"preview",
		"",
		"https://host.example/api/v1/",
		false,
		false,
	)
	require.NoError(t, err)
	assert.Equal(t, "https://host.example/api/v1", client.baseURL)

	spaced, err := NewKoiosClient(
		"preview",
		"",
		"  https://host.example/api/v1  ",
		false,
		false,
	)
	require.NoError(t, err)
	assert.Equal(t, "https://host.example/api/v1", spaced.baseURL)
}

// TestNewKoiosClientDefaultsToPublicHost pins that an empty override changes
// nothing, including the burst cap that koios.rest's tiers require.
func TestNewKoiosClientDefaultsToPublicHost(t *testing.T) {
	client, err := NewKoiosClient("preview", "", "", false, false)
	require.NoError(t, err)
	assert.Equal(t, koiosBaseURLs["preview"], client.baseURL)
	require.NotNil(t, client.limiter)
	assert.Equal(t, koiosBurstLimitSafe, client.limiter.limit,
		"the public host keeps the published tier cap")
}

// TestNewKoiosClientCustomHostDropsBurstCap covers the reason the override
// exists. koiosBurstLimitSafe describes koios.rest's own Public/Free window and
// says nothing about another deployment, so throttling a self-hosted instance
// against it would enforce a limit that does not exist.
func TestNewKoiosClientCustomHostDropsBurstCap(t *testing.T) {
	client, err := NewKoiosClient(
		"preview",
		"",
		"https://host.example/api/v1",
		false,
		false,
	)
	require.NoError(t, err)
	require.NotNil(t, client.limiter)
	assert.LessOrEqual(t, client.limiter.limit, 0,
		"a self-hosted host is not subject to the public tier cap")

	// And an unlimited limiter really does not block.
	for range koiosBurstLimitSafe + 5 {
		require.NoError(t, client.limiter.wait(context.Background()))
	}
}

// TestNewKoiosClientRejectsUnsupportedNetworkWithOverride pins that supplying a
// host does not bypass network validation: StakeAddressFromCredential hardcodes
// the testnet address network ID, so an unvalidated "mainnet" would silently
// generate wrong-network stake addresses.
func TestNewKoiosClientRejectsUnsupportedNetworkWithOverride(t *testing.T) {
	_, err := NewKoiosClient(
		"mainnet",
		"",
		"https://host.example/api/v1",
		false,
		false,
	)
	require.Error(t, err)
}

// TestNewKoiosClientRejectsPlainHTTPByDefault covers the transport guard. get
// and post attach the API key as a Bearer token to every request, so a
// plain-HTTP host would put it on the wire in cleartext — and forged reference
// data can make a parity comparison report a false PASS, the one outcome this
// tool must never produce.
func TestNewKoiosClientRejectsPlainHTTPByDefault(t *testing.T) {
	_, err := NewKoiosClient(
		"preview", "secret-token", "http://host.example/api/v1", false, false,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plain HTTP")
	assert.NotContains(t, err.Error(), "secret-token",
		"the error must not echo the API key")
}

// TestNewKoiosClientAllowsPlainHTTPWithEscapeHatch pins the local dev/test
// opt-out, mirroring Mithril.AllowInsecureHTTP.
func TestNewKoiosClientAllowsPlainHTTPWithEscapeHatch(t *testing.T) {
	client, err := NewKoiosClient(
		"preview", "", "http://host.example/api/v1", true, false,
	)
	require.NoError(t, err)
	assert.Equal(t, "http://host.example/api/v1", client.baseURL)
}

// TestNewKoiosClientRejectsMalformedBaseURL covers the shapes an operator can
// plausibly paste: a bare host with no scheme, and a scheme this client cannot
// speak. Neither may fall through to the public host silently.
func TestNewKoiosClientRejectsMalformedBaseURL(t *testing.T) {
	for _, raw := range []string{
		"preview-koios.example.com/api/v1",
		"ftp://host.example/api/v1",
		"://broken",
	} {
		_, err := NewKoiosClient("preview", "", raw, false, false)
		require.Error(t, err, "base URL %q must be rejected", raw)
	}
}

// TestNewKoiosClientPlainHTTPGuardDoesNotAffectPublicHost pins that the guard
// only looks at a custom URL: the built-in hosts are https already, and an
// empty override must not be able to trip it.
func TestNewKoiosClientPlainHTTPGuardDoesNotAffectPublicHost(t *testing.T) {
	client, err := NewKoiosClient("preview", "", "", false, false)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(client.baseURL, "https://"))
}

// TestNewKoiosClientKeepsBurstCapForPublicHostOverride covers an override that
// names koios.rest explicitly. The cap is dropped for a custom deployment, not
// for a custom spelling of the public one — that host's published window
// applies however the URL was written, and ignoring it earns 429 cooldowns.
func TestNewKoiosClientKeepsBurstCapForPublicHostOverride(t *testing.T) {
	for _, raw := range []string{
		"https://preview.koios.rest/api/v1",
		"https://PREPROD.KOIOS.REST/api/v1",
		"https://koios.rest/api/v1",
	} {
		client, err := NewKoiosClient("preview", "", raw, false, false)
		require.NoError(t, err)
		require.NotNil(t, client.limiter)
		assert.Equal(t, koiosBurstLimitSafe, client.limiter.limit,
			"override %q names the public host and keeps its cap", raw)
	}

	// A host that merely mentions the string is not the public host.
	client, err := NewKoiosClient(
		"preview", "", "https://koios.rest.example.com/api/v1", false, false,
	)
	require.NoError(t, err)
	assert.LessOrEqual(t, client.limiter.limit, 0)
}

// TestNewKoiosClientRejectsQueryOrFragment covers a root that already carries a
// delimiter. get and post append an endpoint path and their own query to this
// value, so a root ending in "?x=1" or "#frag" would put the appended path
// after that delimiter and silently reach a different endpoint.
func TestNewKoiosClientRejectsQueryOrFragment(t *testing.T) {
	for _, raw := range []string{
		"https://host.example/api/v1?token=abc",
		"https://host.example/api/v1#frag",
		"https://host.example/api/v1?",
	} {
		_, err := NewKoiosClient("preview", "", raw, false, false)
		require.Error(t, err, "base URL %q must be rejected", raw)
	}
}

// TestValidateKoiosBaseURLErrorsOmitTheURL covers the errors themselves. An
// operator can put credentials in the URL as userinfo or as a
// credential-shaped query parameter, and a validation error is written to the
// same log that logURIConfigFields exists to protect — so the raw value must
// never appear in it.
func TestValidateKoiosBaseURLErrorsOmitTheURL(t *testing.T) {
	const secret = "SENTINEL-URL-PASSWORD"
	for _, raw := range []string{
		"http://dingo:" + secret + "@host.example/api/v1",
		"ftp://dingo:" + secret + "@host.example/api/v1",
		"https://host.example/api/v1?api_key=" + secret,
		"://dingo:" + secret + "@broken",
	} {
		err := validateKoiosBaseURL(raw, false, false)
		require.Error(t, err, "base URL %q must be rejected", raw)
		assert.NotContains(t, err.Error(), secret,
			"validation error must not echo the URL's credentials")
	}
}

// TestNewKoiosClientPublicHostSpellings covers DNS spellings of the public host
// that must keep its published burst cap. A single terminal dot is a valid,
// fully-qualified spelling of the same name.
func TestNewKoiosClientPublicHostSpellings(t *testing.T) {
	for _, raw := range []string{
		"https://preview.koios.rest./api/v1",
		"https://PREVIEW.KOIOS.REST./api/v1",
	} {
		client, err := NewKoiosClient("preview", "", raw, false, false)
		require.NoError(t, err)
		assert.Equal(t, koiosBurstLimitSafe, client.limiter.limit,
			"%q is the public host and keeps its cap", raw)
	}
}

// TestNewKoiosClientRejectsBareFragment covers a root ending in "#". url.URL
// has no ForceFragment counterpart to ForceQuery, so it parses to an empty
// Fragment — but get and post would still append the endpoint path after the
// delimiter and reach the base path instead.
func TestNewKoiosClientRejectsBareFragment(t *testing.T) {
	_, err := NewKoiosClient(
		"preview",
		"",
		"https://host.example/api/v1#",
		false,
		false,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fragment")
}

// TestResolvedBaseURLDropsUserinfo covers the value that gets logged and
// persisted. validateKoiosBaseURL already rejects a query and a fragment, so
// userinfo is the only place a credential survives into a validated root, and
// both the log line and the cache row must be safe to read.
func TestResolvedBaseURLDropsUserinfo(t *testing.T) {
	c, err := NewKoiosClient(
		"preview",
		"key",
		"https://dingo:hunter2@koios.example/api/v1",
		false,
		false,
	)
	require.NoError(t, err)
	resolved := c.ResolvedBaseURL()
	assert.Equal(t, "https://koios.example/api/v1", resolved)
	assert.NotContains(t, resolved, "hunter2")
	assert.NotContains(t, resolved, "dingo:")
}

// TestResolvedBaseURLReportsTheDefaultHost pins that the accessor names the
// host actually queried when no override is given, rather than reporting the
// empty override back.
func TestResolvedBaseURLReportsTheDefaultHost(t *testing.T) {
	c, err := NewKoiosClient("preview", "", "", false, false)
	require.NoError(t, err)
	assert.Equal(t, koiosBaseURLs["preview"], c.ResolvedBaseURL())
}

// TestCreditedAccountRewardsSkipsUncredited pins that the per-account
// comparison sees only the rewards the ledger actually credited.
//
// reward_account_output holds every reward the calculation produced, credited
// or not. The ledger's own application skips a row that is not spendable and
// one whose reward account is guarded by CIP-0163 expiry, so Koios never
// reports either — feeding them to the comparison makes Dingo look like it
// paid a reward nobody received.
func TestCreditedAccountRewardsSkipsUncredited(t *testing.T) {
	t.Parallel()

	// Real credentials from Preview epoch 197, where all three of the epoch's
	// unspendable rows were reported as acct_only_dingo.
	unspendable := mustDecodeHex(
		t,
		"72A4EA5A1B4B170052E279055B6C2B75773006B1062C749376C9D68B",
	)
	guarded := mustDecodeHex(
		t,
		"E392F348B98E66A84389463BA547C2C551586D9973B3DE8C8B044388",
	)
	credited := mustDecodeHex(
		t,
		"F8ADA2B9A94FDD95D35D482BDDDF5A66FFA5B330B539B4613255C1DC",
	)

	rows, credentialErrs, poolErrs := creditedAccountRewards([]*models.RewardAccountOutput{
		{
			StakingKey: unspendable,
			RewardType: "member",
			Amount:     69019,
			Spendable:  false,
		},
		{
			StakingKey: guarded,
			RewardType: "member",
			Amount:     1409915,
			Spendable:  true,
			Guarded:    true,
		},
		{
			StakingKey: credited,
			RewardType: "member",
			Amount:     500,
			Spendable:  true,
		},
	})
	require.Empty(t, credentialErrs)
	require.Empty(t, poolErrs)
	require.Len(t, rows, 1,
		"only the credited row belongs in the comparison")
	assert.Equal(t, "500", rows[0].Amount)
}

// TestCreditedAccountRewardsKeepsLeaderRewards guards the obvious overreach.
// A leader reward is credited to the pool's reward account and Koios reports
// it, so the filter must be about crediting, not about reward type — unlike
// the pool-level member-total path, which filters by type because it is
// summing member stake rewards specifically.
func TestCreditedAccountRewardsKeepsLeaderRewards(t *testing.T) {
	t.Parallel()

	key := mustDecodeHex(
		t,
		"F8ADA2B9A94FDD95D35D482BDDDF5A66FFA5B330B539B4613255C1DC",
	)
	rows, credentialErrs, poolErrs := creditedAccountRewards([]*models.RewardAccountOutput{
		{
			StakingKey: key,
			RewardType: "leader",
			Amount:     1515378117,
			Spendable:  true,
		},
	})
	require.Empty(t, credentialErrs)
	require.Empty(t, poolErrs)
	require.Len(t, rows, 1)
	assert.Equal(t, "leader", rows[0].RewardType)
}

// TestCreditedAccountRewardsReportsDecodeFailure keeps the decode error
// surfacing that the inline loop had: a credential that cannot be turned into
// a stake address is a database problem worth reporting, not a row to drop.
func TestCreditedAccountRewardsReportsDecodeFailure(t *testing.T) {
	t.Parallel()

	rows, credentialErrs, poolErrs := creditedAccountRewards([]*models.RewardAccountOutput{
		{
			StakingKey: []byte{0x01, 0x02},
			RewardType: "member",
			Amount:     1,
			Spendable:  true,
		},
	})
	assert.Empty(t, rows)
	require.Len(t, credentialErrs, 1)
	require.Empty(t, poolErrs,
		"a credential failure is not a pool failure")
}

// An uncredited row with an undecodable credential is still reported. The
// narrowing is about what the comparison sees, not about what gets reported:
// a credential that cannot be turned into a stake address is a storage
// problem whichever row carries it.
//
// It also has to be reported here specifically.
// accountLifecycleMismatches decodes the same rows through
// dingoRewardAddressSet, but only emits a CategoryDBError for the *previous*
// stake epoch's failures — for the current epoch it merely suppresses the
// lifecycle diff and returns, on the stated assumption that this function
// already reported it. Dropping the row before decoding would break that
// assumption and take the lifecycle diff down silently with it.
func TestCreditedAccountRewardsReportsUncreditedDecodeFailure(t *testing.T) {
	t.Parallel()

	rows, credentialErrs, poolErrs := creditedAccountRewards([]*models.RewardAccountOutput{
		{
			StakingKey: []byte{0x01, 0x02},
			RewardType: "member",
			Amount:     1,
			Spendable:  false,
		},
	})
	assert.Empty(t, rows, "an uncredited row still never enters the comparison")
	require.Len(t, credentialErrs, 1,
		"but its corrupt credential is still reported")
	require.Empty(t, poolErrs)
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

// TestCreditedAccountRewardsCarriesPoolID pins the source pool the shared
// reward-account aggregation groups on, and the two ways a row can have no
// usable one.
//
// A credited row's pool key hash becomes the bech32 pool ID the fold treats
// as one contribution. An absent hash is not a failure: it names the "no
// pool" contribution, the same thing Koios's null pool_id_bech32 names. A
// present but malformed hash is a failure, reported rather than dropped, and
// only for a row the comparison would otherwise have used — an uncredited
// row is filtered out before the pool is ever decoded, so a hash it carries
// cannot turn an epoch it has no part in into an error.
func TestCreditedAccountRewardsCarriesPoolID(t *testing.T) {
	t.Parallel()

	key := mustDecodeHex(
		t,
		"F8ADA2B9A94FDD95D35D482BDDDF5A66FFA5B330B539B4613255C1DC",
	)
	poolHash := mustDecodeHex(
		t,
		"00000000000000000000000000000000000000000000000000000001",
	)
	wantPoolID, err := PoolKeyHashHexToBech32(hex.EncodeToString(poolHash))
	require.NoError(t, err)

	t.Run("credited row carries its pool", func(t *testing.T) {
		t.Parallel()

		rows, credentialErrs, poolErrs := creditedAccountRewards([]*models.RewardAccountOutput{
			{
				StakingKey:  key,
				PoolKeyHash: poolHash,
				RewardType:  "member",
				Amount:      500,
				Spendable:   true,
			},
		})
		require.Empty(t, credentialErrs)
		require.Empty(t, poolErrs)
		require.Len(t, rows, 1)
		assert.Equal(t, wantPoolID, rows[0].PoolIDBech32)
	})

	t.Run("absent pool key hash is not a failure", func(t *testing.T) {
		t.Parallel()

		rows, credentialErrs, poolErrs := creditedAccountRewards([]*models.RewardAccountOutput{
			{
				StakingKey: key,
				RewardType: "member",
				Amount:     500,
				Spendable:  true,
			},
		})
		require.Empty(t, credentialErrs)
		require.Empty(t, poolErrs)
		require.Len(t, rows, 1)
		assert.Empty(t, rows[0].PoolIDBech32)
	})

	t.Run("malformed pool key hash is reported", func(t *testing.T) {
		t.Parallel()

		rows, credentialErrs, poolErrs := creditedAccountRewards([]*models.RewardAccountOutput{
			{
				StakingKey:  key,
				PoolKeyHash: []byte{0x01, 0x02},
				RewardType:  "member",
				Amount:      500,
				Spendable:   true,
			},
		})
		require.Len(t, poolErrs, 1)
		require.Empty(t, credentialErrs,
			"a pool failure is not reported as a credential failure")
		assert.Empty(t, rows,
			"a row whose pool cannot be decoded is reported, not compared")
	})

	t.Run("uncredited row's pool is never decoded", func(t *testing.T) {
		t.Parallel()

		rows, credentialErrs, poolErrs := creditedAccountRewards([]*models.RewardAccountOutput{
			{
				StakingKey:  key,
				PoolKeyHash: []byte{0x01, 0x02},
				RewardType:  "member",
				Amount:      500,
				Spendable:   false,
			},
		})
		require.Empty(t, poolErrs,
			"a row the comparison never sees cannot fail the epoch")
		require.Empty(t, credentialErrs)
		assert.Empty(t, rows)
	})
}

func newSourceTestCache(t *testing.T) *Cache {
	t.Helper()
	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })
	return cache
}

// seedOracleRows writes one row into every table RecordKoiosSource is
// responsible for invalidating, so a test can tell "discarded" from "never
// written" without depending on the shape of any particular fetch path.
func seedOracleRows(t *testing.T, c *Cache, network string) {
	t.Helper()
	now := time.Now().UTC()
	// CommitEpochData covers koios_epoch_info, koios_pool_epoch and
	// koios_totals in one call.
	require.NoError(t, c.CommitEpochData(
		KoiosEpochInfo{
			Network:      network,
			Epoch:        7,
			ActiveStake:  "1",
			Fees:         "1",
			TotalRewards: "1",
			EpochEndTime: now,
			FetchedAt:    now,
		},
		[]KoiosPoolEpoch{{
			Network:     network,
			Epoch:       7,
			PoolBech32:  "pool1seeded",
			ActiveStake: "1",
			FetchedAt:   now,
		}},
		&KoiosTotals{
			Network:   network,
			Epoch:     7,
			Treasury:  "1",
			Reserves:  "1",
			Fees:      "1",
			Reward:    "1",
			FetchedAt: now,
		},
	))
	// Staged chunk progress covers koios_account_fetch_staged_rows and
	// koios_account_checked.
	require.NoError(t, c.SaveAccountFetchChunkProgress(
		network,
		7,
		"chunkseed",
		[]KoiosAccountRewards{{
			StakeAddress: "stake_test1seeded",
			RewardType:   "member",
			Earned:       "1",
			FetchedAt:    now,
		}},
		[]string{"stake_test1seeded"},
		now,
	))
	require.NoError(t, c.CommitAccountRewardsForEpoch(
		network,
		7,
		[]KoiosAccountRewards{{
			StakeAddress: "stake_test1seeded",
			RewardType:   "member",
			Earned:       "1",
			FetchedAt:    now,
		}},
		1,
		true,
		now,
	))
	require.NoError(t, c.CommitEpochMismatches(network, 7, []CheckMismatch{{
		Network:    network,
		Epoch:      7,
		Field:      "active_stake",
		DingoValue: "1",
		KoiosValue: "2",
		Category:   CategoryValueMismatch,
		CheckedAt:  now,
	}}))
	require.NoError(t, c.UpsertCheckEpochStatus(CheckEpochStatus{
		Network:       network,
		Epoch:         7,
		LastCheckedAt: now,
		Status:        StatusFail,
		MismatchCount: 1,
	}))
	require.NoError(t, c.InsertCheckRun(CheckRun{
		Network:       network,
		RunAt:         now,
		EpochsChecked: 1,
		MismatchCount: 1,
	}))
	require.NoError(t, c.SaveAccountUniverse(
		network, []string{"stake_test1seeded"}, now,
	))
	// koios_epoch_params is Koios-sourced too, so the discard has to be
	// exercised there as well.
	require.NoError(t, c.UpsertEpochParams(KoiosEpochParams{
		Network: network,
		Epoch:   7,
		Era:     "conway",
	}))
	// koios_tx_info holds another oracle's /tx_info answers, so it is
	// discarded on a source change for the same reason every other fetched
	// table is.
	require.NoError(t, c.UpsertTxInfos(
		network,
		[]KoiosTxInfoItem{{TxHash: "txseeded"}},
		now,
	))
}

func countOracleRows(t *testing.T, c *Cache, network string) int {
	t.Helper()
	total := 0
	for _, table := range koiosSourcedTables {
		var n int
		require.NoError(t, c.db.QueryRow(
			"SELECT COUNT(*) FROM "+table+" WHERE network = ?", network,
		).Scan(&n))
		total += n
	}
	return total
}

// TestRecordKoiosSourceFirstRunKeepsRows pins that recording a source is not
// itself destructive: a cache written before this column existed is
// unattributed, not wrong, and must be claimed rather than thrown away.
func TestRecordKoiosSourceFirstRunKeepsRows(t *testing.T) {
	cache := newSourceTestCache(t)
	seedOracleRows(t, cache, "preview")
	before := countOracleRows(t, cache, "preview")
	require.Positive(t, before, "fixture must seed rows")

	change, err := cache.RecordKoiosSource(
		"preview", "https://preview.koios.rest/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)
	assert.False(t, change.Changed)
	assert.Empty(t, change.Previous)
	assert.Equal(t, before, countOracleRows(t, cache, "preview"))

	got, ok, err := cache.GetKoiosSource("preview")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "https://preview.koios.rest/api/v1", got)
}

// TestRecordKoiosSourceSameHostKeepsRows is the case that must not regress
// into "invalidate on every run": an unchanged source is the normal path, and
// discarding there would refetch the whole history on every start.
func TestRecordKoiosSourceSameHostKeepsRows(t *testing.T) {
	cache := newSourceTestCache(t)
	const url = "https://preview.koios.rest/api/v1"
	_, err := cache.RecordKoiosSource("preview", url, time.Now().UTC())
	require.NoError(t, err)
	seedOracleRows(t, cache, "preview")
	before := countOracleRows(t, cache, "preview")

	change, err := cache.RecordKoiosSource("preview", url, time.Now().UTC())
	require.NoError(t, err)
	assert.False(t, change.Changed)
	assert.Zero(t, change.RowsDiscarded)
	assert.Equal(t, before, countOracleRows(t, cache, "preview"))
}

// TestRecordKoiosSourceChangedHostDiscardsRows is the finding itself: without
// this, rows fetched from a self-hosted mirror and rows fetched from the
// public host are the same rows, and a run against the wrong oracle produces
// output indistinguishable from a run against the right one.
func TestRecordKoiosSourceChangedHostDiscardsRows(t *testing.T) {
	cache := newSourceTestCache(t)
	_, err := cache.RecordKoiosSource(
		"preview", "https://koios.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)
	seedOracleRows(t, cache, "preview")
	require.Positive(t, countOracleRows(t, cache, "preview"))

	change, err := cache.RecordKoiosSource(
		"preview", "https://preview.koios.rest/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)
	assert.True(t, change.Changed)
	assert.Equal(t, "https://koios.example/api/v1", change.Previous)
	assert.Positive(t, change.RowsDiscarded)
	assert.Zero(t, countOracleRows(t, cache, "preview"),
		"no row fetched from the previous oracle may survive")

	got, _, err := cache.GetKoiosSource("preview")
	require.NoError(t, err)
	assert.Equal(t, "https://preview.koios.rest/api/v1", got)
}

// TestRecordKoiosSourceChangeIsScopedToItsNetwork keeps the invalidation from
// becoming a bigger hammer than the problem: the base URL is resolved per
// network, so another network's rows were fetched from their own oracle and
// are not implicated by this one changing.
func TestRecordKoiosSourceChangeIsScopedToItsNetwork(t *testing.T) {
	cache := newSourceTestCache(t)
	_, err := cache.RecordKoiosSource(
		"preview", "https://koios.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)
	seedOracleRows(t, cache, "preview")
	seedOracleRows(t, cache, "preprod")
	other := countOracleRows(t, cache, "preprod")
	require.Positive(t, other)

	_, err = cache.RecordKoiosSource(
		"preview", "https://preview.koios.rest/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)
	assert.Zero(t, countOracleRows(t, cache, "preview"))
	assert.Equal(t, other, countOracleRows(t, cache, "preprod"))
}

// TestRecordKoiosSourceCoversEveryKoiosTable stops koiosSourcedTables from
// silently falling behind the schema. A table added later that holds Koios
// answers but is missing from the list would survive a host change and put
// the mixed-oracle report back within reach — the exact failure this guard
// exists to prevent, reintroduced quietly.
func TestRecordKoiosSourceCoversEveryKoiosTable(t *testing.T) {
	cache := newSourceTestCache(t)
	rows, err := cache.db.Query(
		`SELECT name FROM sqlite_master
		WHERE type = 'table' AND (name LIKE 'koios_%' OR name LIKE 'check_%')`,
	)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck

	listed := make(map[string]struct{}, len(koiosSourcedTables))
	for _, t := range koiosSourcedTables {
		listed[t] = struct{}{}
	}
	seen := 0
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		// koios_source is the record itself, not a row it invalidates.
		if name == "koios_source" {
			continue
		}
		seen++
		_, ok := listed[name]
		assert.True(
			t,
			ok,
			"%s holds Koios-sourced rows but is not invalidated on a source change",
			name,
		)
	}
	require.NoError(t, rows.Err())
	// Without this the test passes vacuously if the query ever stops
	// matching, which would retire the guard rather than satisfy it.
	require.Equal(t, len(koiosSourcedTables), seen,
		"every listed table must exist in the schema, and vice versa")
}

// TestKoiosSourcedTablesAreBareIdentifiers backs the G202 suppression on the
// DELETE in RecordKoiosSource. That annotation is only honest while every
// entry is a plain table identifier, so this pins the property rather than
// leaving the suppression to be quietly outgrown by a later entry.
func TestKoiosSourcedTablesAreBareIdentifiers(t *testing.T) {
	identifier := regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	for _, table := range koiosSourcedTables {
		assert.Regexp(
			t,
			identifier,
			table,
			"%q is not a bare identifier, so it must not be concatenated into SQL",
			table,
		)
	}
}

// TestSeedOracleRowsTouchesEveryInvalidatedTable keeps the fixture honest.
// The discard tests assert the row count reaches zero, so any table the
// fixture never writes is only ever "invalidated" while already empty, and a
// regression that stopped discarding it would still pass.
func TestSeedOracleRowsTouchesEveryInvalidatedTable(t *testing.T) {
	cache := newSourceTestCache(t)
	seedOracleRows(t, cache, "preview")
	for _, table := range koiosSourcedTables {
		var n int
		require.NoError(t, cache.db.QueryRow(
			"SELECT COUNT(*) FROM "+table+" WHERE network = ?", "preview",
		).Scan(&n))
		assert.Positive(
			t,
			n,
			"%s is invalidated on a source change but never seeded, so the discard is untested there",
			table,
		)
	}
}

// TestRecordKoiosSourceFirstRunWithCustomRootDiscards is the upgrade path.
// A cache written before koios_source existed can only hold public-host rows,
// because no build without the column had an override to apply. Claiming them
// for a custom root would adopt the public host's answers as the mirror's —
// mixing two oracles on the exact path this guard exists to close.
func TestRecordKoiosSourceFirstRunWithCustomRootDiscards(t *testing.T) {
	cache := newSourceTestCache(t)
	seedOracleRows(t, cache, "preview")
	require.Positive(t, countOracleRows(t, cache, "preview"))

	change, err := cache.RecordKoiosSource(
		"preview", "https://koios.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)
	assert.True(t, change.Changed)
	assert.Equal(t, koiosBaseURLs["preview"], change.Previous,
		"an unattributed cache is attributed to the built-in public root")
	assert.Zero(t, countOracleRows(t, cache, "preview"),
		"public-host rows must not be adopted by a custom root")
}

// TestPendingKoiosSourceChangeMatchesRecord pins the two against each other.
// The probe gate reads Pending and the destruction happens in Record, so a
// disagreement would either skip the probe before a discard or demand one
// where nothing changes.
func TestPendingKoiosSourceChangeMatchesRecord(t *testing.T) {
	for _, tc := range []struct {
		name     string
		recorded string
		next     string
		want     bool
	}{
		{"unrecorded, public root", "", koiosBaseURLs["preview"], false},
		{"unrecorded, custom root", "", "https://koios.example/api/v1", true},
		{"recorded, same root", "https://a.example/api/v1", "https://a.example/api/v1", false},
		{"recorded, different root", "https://a.example/api/v1", "https://b.example/api/v1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := newSourceTestCache(t)
			if tc.recorded != "" {
				_, err := cache.RecordKoiosSource(
					"preview", tc.recorded, time.Now().UTC(),
				)
				require.NoError(t, err)
			}
			pending, _, err := cache.PendingKoiosSourceChange(
				"preview",
				tc.next,
			)
			require.NoError(t, err)
			assert.Equal(t, tc.want, pending)

			seedOracleRows(t, cache, "preview")
			change, err := cache.RecordKoiosSource(
				"preview", tc.next, time.Now().UTC(),
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				pending,
				change.Changed,
				"PendingKoiosSourceChange must predict what RecordKoiosSource does",
			)
		})
	}
}

// TestRecordKoiosSourceProbeFailureKeepsCache is the cost guard on the
// destructive path: a mistyped or unreachable new host must not discard the
// old host's rows, because recovering from that costs a full historical
// refetch — the expense that made the override worth guarding at all.
func TestRecordKoiosSourceProbeFailureKeepsCache(t *testing.T) {
	// 404, not 500: get() classifies 4xx as ErrKoiosPermanent and returns
	// immediately, while a 5xx would be retried three times with a 2s, 4s, 6s
	// backoff and put ~12s of sleep in every run of this package.
	dead := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}),
	)
	defer dead.Close()

	cache := newSourceTestCache(t)
	_, err := cache.RecordKoiosSource(
		"preview", "https://koios.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)
	seedOracleRows(t, cache, "preview")
	before := countOracleRows(t, cache, "preview")
	require.Positive(t, before)

	// httptest serves plain HTTP, so this needs the insecure escape hatch.
	client, err := NewKoiosClient("preview", "", dead.URL+"/api/v1", true, true)
	require.NoError(t, err)

	err = recordKoiosSource(
		context.Background(), cache, "preview", client,
		slog.New(slog.DiscardHandler),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not answer")
	assert.Equal(t, before, countOracleRows(t, cache, "preview"),
		"a host that does not answer must not cost the cached reference data")

	got, _, err := cache.GetKoiosSource("preview")
	require.NoError(t, err)
	assert.Equal(t, "https://koios.example/api/v1", got,
		"the source must not move to a host that never answered")
}

// TestRecordKoiosSourceProbeSuccessSwitches is the other half: once the new
// host answers, the switch goes through and the old oracle's rows go with it.
func TestRecordKoiosSourceProbeSuccessSwitches(t *testing.T) {
	live := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/v1/tip" {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write([]byte(`[{"epoch_no":42}]`))
		}),
	)
	defer live.Close()

	cache := newSourceTestCache(t)
	_, err := cache.RecordKoiosSource(
		"preview", "https://koios.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)
	seedOracleRows(t, cache, "preview")
	require.Positive(t, countOracleRows(t, cache, "preview"))

	client, err := NewKoiosClient("preview", "", live.URL+"/api/v1", true, true)
	require.NoError(t, err)
	require.NoError(t, recordKoiosSource(
		context.Background(), cache, "preview", client,
		slog.New(slog.DiscardHandler),
	))
	assert.Zero(t, countOracleRows(t, cache, "preview"))

	got, _, err := cache.GetKoiosSource("preview")
	require.NoError(t, err)
	assert.Equal(t, client.ResolvedBaseURL(), got)
}

// TestRecordKoiosSourceUnchangedMakesNoRequest keeps the probe off the
// ordinary start. Every run would otherwise pay a network round-trip before
// doing anything, and a transient blip on the common path would fail startup
// for a source that did not change.
func TestRecordKoiosSourceUnchangedMakesNoRequest(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			_, _ = w.Write([]byte(`[{"epoch_no":42}]`))
		}),
	)
	defer srv.Close()

	cache := newSourceTestCache(t)
	client, err := NewKoiosClient("preview", "", srv.URL+"/api/v1", true, true)
	require.NoError(t, err)

	// First call switches away from the attributed public root and probes.
	require.NoError(t, recordKoiosSource(
		context.Background(), cache, "preview", client,
		slog.New(slog.DiscardHandler),
	))
	require.Equal(t, int32(1), hits.Load())

	// Second call changes nothing, so it must not probe again.
	require.NoError(t, recordKoiosSource(
		context.Background(), cache, "preview", client,
		slog.New(slog.DiscardHandler),
	))
	assert.Equal(t, int32(1), hits.Load(),
		"an unchanged source must not cost a request")
}

// TestClaimedSourceRefusesWritesAfterAnotherWriterRepoints is the concurrent
// case RecordKoiosSource alone cannot cover: it invalidates only the rows
// present when it runs, so a client already fetching from the old host would
// otherwise go on appending that host's answers under the new host's marker —
// reassembling the mixed-oracle state the marker exists to make impossible.
//
// The default cache path is shared across the standalone commands and the
// in-process observer, so an observer on one host and a `fetch --koios-url` on
// another are a reachable pair rather than a hypothetical one.
func TestClaimedSourceRefusesWritesAfterAnotherWriterRepoints(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")

	first, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer first.Close() //nolint:errcheck
	_, err = first.RecordKoiosSource(
		"preview", "https://first.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)

	// A write from the first handle is fine while it still owns the cache.
	require.NoError(t, first.CommitAccountRewardsForEpoch(
		"preview", 7, nil, 0, true, time.Now().UTC(),
	))

	// A second process re-points the same cache at another host.
	second, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer second.Close() //nolint:errcheck
	_, err = second.RecordKoiosSource(
		"preview", "https://second.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)

	// The first handle must now refuse rather than mix.
	now := time.Now().UTC()
	err = first.CommitAccountRewardsForEpoch("preview", 7, nil, 0, true, now)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to write")

	require.Error(t, first.CommitEpochData(KoiosEpochInfo{
		Network:      "preview",
		Epoch:        7,
		ActiveStake:  "1",
		Fees:         "1",
		TotalRewards: "1",
		EpochEndTime: now,
		FetchedAt:    now,
	}, nil, nil))
	require.Error(t, first.SaveAccountUniverse(
		"preview", []string{"stake_test1x"}, now,
	))
	require.Error(t, first.SaveAccountFetchChunkProgress(
		"preview", 7, "chunk", nil, []string{"stake_test1x"}, now,
	))

	// The handle that owns the cache is unaffected.
	assert.NoError(t, second.CommitAccountRewardsForEpoch(
		"preview", 7, nil, 0, true, now,
	))
}

// TestUnclaimedCacheStillWrites keeps the guard off every read-only command
// and every existing caller: a handle that never recorded a source must not
// start refusing writes to a cache someone else stamped.
func TestUnclaimedCacheStillWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	owner, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer owner.Close() //nolint:errcheck
	_, err = owner.RecordKoiosSource(
		"preview", "https://first.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)

	bystander, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer bystander.Close() //nolint:errcheck
	assert.NoError(t, bystander.CommitAccountRewardsForEpoch(
		"preview", 7, nil, 0, true, time.Now().UTC(),
	))
}

// TestPreviousInferredDistinguishesAttributionFromRecord keeps the discard log
// honest. "The host we recorded" and "the host a legacy cache must have used"
// are different strengths of claim, and Previous alone cannot tell them apart.
func TestPreviousInferredDistinguishesAttributionFromRecord(t *testing.T) {
	t.Run("legacy cache is an inference", func(t *testing.T) {
		cache := newSourceTestCache(t)
		change, err := cache.RecordKoiosSource(
			"preview", "https://koios.example/api/v1", time.Now().UTC(),
		)
		require.NoError(t, err)
		require.True(t, change.Changed)
		assert.True(t, change.PreviousInferred)
		assert.Equal(t, koiosBaseURLs["preview"], change.Previous)
	})
	t.Run("a recorded source is a record", func(t *testing.T) {
		cache := newSourceTestCache(t)
		_, err := cache.RecordKoiosSource(
			"preview", "https://first.example/api/v1", time.Now().UTC(),
		)
		require.NoError(t, err)
		change, err := cache.RecordKoiosSource(
			"preview", "https://second.example/api/v1", time.Now().UTC(),
		)
		require.NoError(t, err)
		require.True(t, change.Changed)
		assert.False(t, change.PreviousInferred)
		assert.Equal(t, "https://first.example/api/v1", change.Previous)
	})
}

// TestPinnedSourceRefusesCheckWrites covers the derived half. RecordKoiosSource
// discards check evidence too, so a check already in flight would otherwise
// repopulate mismatches and status under a source its verdicts were never
// computed against — the same mixing, one layer up.
func TestPinnedSourceRefusesCheckWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	owner, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer owner.Close() //nolint:errcheck
	_, err = owner.RecordKoiosSource(
		"preview", "https://first.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)

	// A checker pins whatever is recorded; it has no client to name a source.
	checker, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer checker.Close() //nolint:errcheck
	require.NoError(t, checker.PinRecordedSource("preview"))

	now := time.Now().UTC()
	mismatch := []CheckMismatch{{
		Network: "preview", Epoch: 7, Field: "active_stake",
		DingoValue: "1", KoiosValue: "2",
		Category: CategoryValueMismatch, CheckedAt: now,
	}}
	status := CheckEpochStatus{
		Network: "preview", Epoch: 7, LastCheckedAt: now,
		Status: StatusFail, MismatchCount: 1,
	}
	run := CheckRun{Network: "preview", RunAt: now, EpochsChecked: 1}

	// While it still owns the pin, all three writes go through.
	require.NoError(t, checker.CommitEpochMismatches("preview", 7, mismatch))
	require.NoError(t, checker.UpsertCheckEpochStatus(status))
	require.NoError(t, checker.InsertCheckRun(run))

	// Another process re-points the cache mid-run.
	_, err = owner.RecordKoiosSource(
		"preview", "https://second.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)

	assert.Error(t, checker.CommitEpochMismatches("preview", 7, mismatch))
	assert.Error(t, checker.UpsertCheckEpochStatus(status))
	assert.Error(t, checker.InsertCheckRun(run))

	// The discarded evidence stays discarded rather than being rewritten.
	rows, err := owner.GetMismatches("preview", 7, "")
	require.NoError(t, err)
	assert.Empty(t, rows)
}

// TestPinnedUnstampedCacheWritesUntilItIsStamped covers the legacy cache. It
// has to hold both halves at once: a check against a cache nothing has ever
// stamped must keep working exactly as before, and must still stop if another
// process stamps it with a different host mid-run — the case that slips
// through if an unstamped cache pins nothing at all.
func TestPinnedUnstampedCacheWritesUntilItIsStamped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	checker, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer checker.Close() //nolint:errcheck
	require.NoError(t, checker.PinRecordedSource("preview"))

	status := CheckEpochStatus{
		Network: "preview", Epoch: 7,
		LastCheckedAt: time.Now().UTC(), Status: StatusPass,
	}
	require.NoError(t, checker.UpsertCheckEpochStatus(status),
		"an unstamped cache must behave as it did before this guard existed")

	// Another process records the public root explicitly: same oracle, so the
	// attribution still matches and the check carries on.
	stamper, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer stamper.Close() //nolint:errcheck
	_, err = stamper.RecordKoiosSource(
		"preview", koiosBaseURLs["preview"], time.Now().UTC(),
	)
	require.NoError(t, err)
	assert.NoError(
		t,
		checker.UpsertCheckEpochStatus(status),
		"recording the root the cache was already attributed to changes nothing",
	)

	// Switching it to a custom host does end the run.
	_, err = stamper.RecordKoiosSource(
		"preview", "https://koios.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)
	assert.Error(t, checker.UpsertCheckEpochStatus(status),
		"a legacy cache switched to another host mid-check must stop the check")
}

// TestClaimedSourceRefusesEpochParamsAfterAnotherWriterRepoints is the
// parameter backfill's member of the class
// TestClaimedSourceRefusesWritesAfterAnotherWriterRepoints covers.
//
// fetchEpochParamsOnly reaches UpsertEpochParams without going through
// CommitEpochData, so it is a second way Koios answers enter the cache. A
// backfill that started against one host and finished after another process
// re-pointed the cache would otherwise repopulate koios_epoch_params — a table
// RecordKoiosSource had just discarded — with the old host's answers, under
// the new host's marker. That is the mixed-oracle state the marker exists to
// make impossible.
//
// The assertion is on the rows, not on the call: the first host's parameters
// must not be readable from the cache afterwards.
func TestClaimedSourceRefusesEpochParamsAfterAnotherWriterRepoints(
	t *testing.T,
) {
	const network = "preview"
	path := filepath.Join(t.TempDir(), "cache.db")

	first, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer first.Close() //nolint:errcheck
	_, err = first.RecordKoiosSource(
		network, "https://first.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)

	// The write lands while the first handle still owns the cache.
	require.NoError(t, first.UpsertEpochParams(KoiosEpochParams{
		Network: network,
		Epoch:   7,
		Era:     "alonzo",
	}))
	owned, err := first.GetEpochParams(network, 7)
	require.NoError(t, err)
	require.Equal(t, "alonzo", owned.Era)

	// A second process re-points the same cache, discarding the first host's
	// rows including this parameter row.
	second, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer second.Close() //nolint:errcheck
	_, err = second.RecordKoiosSource(
		network, "https://second.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)
	_, err = second.GetEpochParams(network, 7)
	require.ErrorIs(t, err, sql.ErrNoRows,
		"re-pointing must discard the first host's parameter row")

	// The first handle must refuse rather than repopulate.
	err = first.UpsertEpochParams(KoiosEpochParams{
		Network: network,
		Epoch:   7,
		Era:     "conway",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to write")

	// The rows are what matter: the first host's parameters must not be
	// readable from a cache now attributed to the second host.
	_, err = second.GetEpochParams(network, 7)
	assert.ErrorIs(
		t,
		err,
		sql.ErrNoRows,
		"the first host's parameters reappeared in a cache re-pointed at another host",
	)
}

// TestUnclaimedCacheStillWritesEpochParams keeps the new gate off the
// read-only and legacy callers, matching TestUnclaimedCacheStillWrites.
func TestUnclaimedCacheStillWritesEpochParams(t *testing.T) {
	const network = "preview"
	path := filepath.Join(t.TempDir(), "cache.db")

	owner, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer owner.Close() //nolint:errcheck
	_, err = owner.RecordKoiosSource(
		network, "https://first.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)

	bystander, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer bystander.Close() //nolint:errcheck
	assert.NoError(t, bystander.UpsertEpochParams(KoiosEpochParams{
		Network: network,
		Epoch:   7,
		Era:     "conway",
	}))
}

// gatedCacheWrite is one Cache writer the claimed-source gate covers, kept
// callable so the refusal and rollback assertions below run over the whole
// set rather than over a list that drifts from cache.go.
type gatedCacheWrite struct {
	name string
	call func(*Cache) error
}

// gatedCacheWrites covers every gated writer, chosen so that running the set
// once populates all of koiosSourcedTables.
func gatedCacheWrites(network string, now time.Time) []gatedCacheWrite {
	const epoch = uint64(7)
	const addr = "stake_test1first"
	reward := []KoiosAccountRewards{{
		StakeAddress: addr, RewardType: "member", Earned: "1", FetchedAt: now,
	}}
	return []gatedCacheWrite{
		{"CommitEpochData", func(c *Cache) error {
			return c.CommitEpochData(
				KoiosEpochInfo{
					Network:      network,
					Epoch:        epoch,
					ActiveStake:  "1",
					Fees:         "1",
					TotalRewards: "1",
					EpochEndTime: now,
					FetchedAt:    now,
				},
				[]KoiosPoolEpoch{{
					PoolBech32: "pool1first", ActiveStake: "1", FetchedAt: now,
				}},
				&KoiosTotals{
					Treasury:  "1",
					Reserves:  "1",
					Fees:      "1",
					Reward:    "1",
					FetchedAt: now,
				},
			)
		}},
		{"UpsertEpochParams", func(c *Cache) error {
			return c.UpsertEpochParams(KoiosEpochParams{
				Network: network, Epoch: epoch, Era: "alonzo", FetchedAt: now,
			})
		}},
		{"UpsertTxInfos", func(c *Cache) error {
			return c.UpsertTxInfos(
				network, []KoiosTxInfoItem{{TxHash: "txfirst"}}, now,
			)
		}},
		{"SaveAccountUniverse", func(c *Cache) error {
			return c.SaveAccountUniverse(network, []string{addr}, now)
		}},
		{"SaveAccountFetchChunkProgress", func(c *Cache) error {
			return c.SaveAccountFetchChunkProgress(
				network, epoch, "chunk-first", reward, []string{addr}, now,
			)
		}},
		{"CommitAccountRewardsForEpoch", func(c *Cache) error {
			return c.CommitAccountRewardsForEpoch(
				network, epoch, reward, 1, true, now,
			)
		}},
		{"CommitEpochMismatches", func(c *Cache) error {
			return c.CommitEpochMismatches(network, epoch, []CheckMismatch{{
				PoolBech32: "pool1first",
				Field:      "active_stake",
				DingoValue: "1",
				KoiosValue: "2",
				Category:   "pool",
				CheckedAt:  now,
			}})
		}},
		{"UpsertCheckEpochStatus", func(c *Cache) error {
			return c.UpsertCheckEpochStatus(CheckEpochStatus{
				Network:       network,
				Epoch:         epoch,
				LastCheckedAt: now,
				Status:        "FAIL",
			})
		}},
		{"InsertCheckRun", func(c *Cache) error {
			return c.InsertCheckRun(CheckRun{Network: network, RunAt: now})
		}},
	}
}

// sourcedTableCounts is network's row count in every table
// RecordKoiosSource discards, keyed by table.
func sourcedTableCounts(
	t *testing.T,
	c *Cache,
	network string,
) map[string]int {
	t.Helper()
	counts := make(map[string]int, len(koiosSourcedTables))
	for _, table := range koiosSourcedTables {
		var n int
		// #nosec G202 -- table comes from koiosSourcedTables, a package-level
		// literal slice; TestKoiosSourcedTablesAreBareIdentifiers keeps it so.
		require.NoError(t, c.db.QueryRow(
			"SELECT COUNT(*) FROM "+table+" WHERE network = ?", network,
		).Scan(&n), table)
		counts[table] = n
	}
	return counts
}

// TestRefusedWriteLeavesNoRows is the rollback half of the claimed-source
// gate. Every gated writer runs its statements before assertClaimedSource, so
// a refusal now depends on the transaction rolling those statements back
// rather than on their never having been issued. A statement that survived a
// refusal would be exactly the mixed-oracle row the gate exists to prevent —
// the first host's answer, committed under the second host's marker.
//
// The assertion is over koiosSourcedTables as a set, so a table added to the
// discard list is covered here without editing this test.
func TestRefusedWriteLeavesNoRows(t *testing.T) {
	const network = "preview"
	path := filepath.Join(t.TempDir(), "cache.db")
	now := time.Now().UTC()
	writes := gatedCacheWrites(network, now)

	first, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer first.Close() //nolint:errcheck
	_, err = first.RecordKoiosSource(
		network, "https://first.example/api/v1", now,
	)
	require.NoError(t, err)

	// While the first handle still owns the cache every gated write lands,
	// so the emptiness asserted at the end is a rollback rather than a test
	// that never wrote anything.
	for _, w := range writes {
		require.NoErrorf(t, w.call(first), "%s while still the owner", w.name)
	}
	for table, n := range sourcedTableCounts(t, first, network) {
		require.NotZerof(t, n, "%s holds no rows to roll back", table)
	}

	// A second process re-points the cache, discarding those rows.
	second, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer second.Close() //nolint:errcheck
	_, err = second.RecordKoiosSource(
		network, "https://second.example/api/v1", now,
	)
	require.NoError(t, err)
	for table, n := range sourcedTableCounts(t, second, network) {
		require.Zerof(t, n, "%s survived the re-point", table)
	}

	// The first handle must refuse every gated write...
	for _, w := range writes {
		err := w.call(first)
		require.Errorf(t, err, "%s must refuse after the re-point", w.name)
		assert.Containsf(
			t, err.Error(), "refusing to write", "%s", w.name,
		)
	}

	// ...and must leave nothing behind when it does.
	for table, n := range sourcedTableCounts(t, second, network) {
		assert.Zerof(t, n,
			"%s holds rows a refused write left behind: the first host's "+
				"answers are readable from a cache re-pointed at another host",
			table,
		)
	}
}

// realCollateralOutputShowForm is the verbatim collateral_output.asset_list
// string returned by the live preview Koios mirror
// (https://preview-koios.tosidrop.me/api/v1) for transaction
// c2c84d18534c49ef8a383f7ff24d62c0b9bb7cf887ea42e1e9b1bf24807f00bf, captured
// while diagnosing the ~50%-of-epochs UTxO-check skip.
//
// It is cardano-ledger's Haskell `Show` rendering of the output's MultiAsset,
// NOT JSON -- feeding it to json.Unmarshal fails on the first "(" with
// "invalid character '(' looking for beginning of value", which is the error
// that was failing whole 40-transaction chunks in production.
const realCollateralOutputShowForm = `[(PolicyID {policyID = ScriptHash "09e56a1dcecb140f4416b48f5ca475aab638d64fcb2f76e9d6496c0e"},[("474f565f4e4654",1)]),(PolicyID {policyID = ScriptHash "65a938b778af9a654b2a005d8ea902485f16a878ff81c458eaa7bdbb"},[("494e4459",32200000000000)])]`

// TestAssetListDecodesLedgerShowMultiAsset is the regression test for the
// collateral_output.asset_list decode failure. Every input here is a verbatim
// capture from the live preview API.
//
// Before the fix this test fails on every non-empty case with
// "invalid character '(' looking for beginning of value".
func TestAssetListDecodesLedgerShowMultiAsset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want KoiosTxInfoAssetList
	}{
		{
			// Empty-but-non-nil, exactly as before the fix: this shape is
			// what gets marshalled back into a koios_tx_info cache row, so
			// it must not change (see
			// TestAssetListRoundTripsThroughCachePayload).
			name: "empty list (the case that always worked)",
			in:   `"[]"`,
			want: KoiosTxInfoAssetList{},
		},
		{
			name: "single policy, single asset",
			in:   `"[(PolicyID {policyID = ScriptHash \"ed133dc2813622728057b951e4c4567d72bb1d78dba55c3a37184247\"},[(\"494e4459\",32200000000000)])]"`,
			want: KoiosTxInfoAssetList{{
				PolicyID:  "ed133dc2813622728057b951e4c4567d72bb1d78dba55c3a37184247",
				AssetName: "494e4459",
				Quantity:  "32200000000000",
			}},
		},
		{
			name: "two policies (real tx c2c84d18...)",
			in:   mustJSONString(t, realCollateralOutputShowForm),
			want: KoiosTxInfoAssetList{
				{
					PolicyID:  "09e56a1dcecb140f4416b48f5ca475aab638d64fcb2f76e9d6496c0e",
					AssetName: "474f565f4e4654",
					Quantity:  "1",
				},
				{
					PolicyID:  "65a938b778af9a654b2a005d8ea902485f16a878ff81c458eaa7bdbb",
					AssetName: "494e4459",
					Quantity:  "32200000000000",
				},
			},
		},
		{
			name: "six policies (real tx e36966d1...)",
			in:   mustJSONString(t, `[(PolicyID {policyID = ScriptHash "3cc8d01846f7288fb42b750bb2afaabe2db04c71a53ab5a5ed4be90f"},[("55504752414445",1)]),(PolicyID {policyID = ScriptHash "4d3b6f594725489d9d5f6d167b2fbe1cf39c4138befc6b0a76b7ac56"},[("504f4c4c5f4d414e41474552",1)]),(PolicyID {policyID = ScriptHash "55a68b2630c4f47523e369a87a8263074fd5349fed89f294d1b9b2d2"},[("474f565f4e4654",1)]),(PolicyID {policyID = ScriptHash "961c251cf3560569c9dc25342d9687af84f1ac46728b7a7c820cd3d7"},[("494153534554",1)]),(PolicyID {policyID = ScriptHash "bfc00a0a8626cde875f237e621482d04cbf9d67849874d98666d9855"},[("53544142494c4954595f504f4f4c",1)]),(PolicyID {policyID = ScriptHash "ed133dc2813622728057b951e4c4567d72bb1d78dba55c3a37184247"},[("494e4459",32200000000000)])]`),
			want: KoiosTxInfoAssetList{
				{PolicyID: "3cc8d01846f7288fb42b750bb2afaabe2db04c71a53ab5a5ed4be90f", AssetName: "55504752414445", Quantity: "1"},
				{PolicyID: "4d3b6f594725489d9d5f6d167b2fbe1cf39c4138befc6b0a76b7ac56", AssetName: "504f4c4c5f4d414e41474552", Quantity: "1"},
				{PolicyID: "55a68b2630c4f47523e369a87a8263074fd5349fed89f294d1b9b2d2", AssetName: "474f565f4e4654", Quantity: "1"},
				{PolicyID: "961c251cf3560569c9dc25342d9687af84f1ac46728b7a7c820cd3d7", AssetName: "494153534554", Quantity: "1"},
				{PolicyID: "bfc00a0a8626cde875f237e621482d04cbf9d67849874d98666d9855", AssetName: "53544142494c4954595f504f4f4c", Quantity: "1"},
				{PolicyID: "ed133dc2813622728057b951e4c4567d72bb1d78dba55c3a37184247", AssetName: "494e4459", Quantity: "32200000000000"},
			},
		},
		{
			name: "multiple assets under one policy",
			in:   mustJSONString(t, `[(PolicyID {policyID = ScriptHash "aa"},[("01",5),("02",7)])]`),
			want: KoiosTxInfoAssetList{
				{PolicyID: "aa", AssetName: "01", Quantity: "5"},
				{PolicyID: "aa", AssetName: "02", Quantity: "7"},
			},
		},
		{
			name: "empty asset name",
			in:   mustJSONString(t, `[(PolicyID {policyID = ScriptHash "aa"},[("",3)])]`),
			want: KoiosTxInfoAssetList{
				{PolicyID: "aa", AssetName: "", Quantity: "3"},
			},
		},
		{
			name: "array form still decodes (inputs/outputs/cached rows)",
			in:   `[{"policy_id":"aa","asset_name":"01","quantity":"9"}]`,
			want: KoiosTxInfoAssetList{
				{PolicyID: "aa", AssetName: "01", Quantity: "9"},
			},
		},
		{
			name: "null",
			in:   `null`,
			want: nil,
		},
		{
			name: "empty string",
			in:   `""`,
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var got KoiosTxInfoAssetList
			require.NoError(t, json.Unmarshal([]byte(tt.in), &got))
			require.Equal(t, tt.want, got)
		})
	}
}

// TestAssetListShowFormMatchesArrayFormForTheSameTokens cross-checks the two
// serialisations against each other using real data for transaction
// c2c84d18...: its collateral_inputs report these exact tokens in the JSON
// array form, and its collateral_output reports them in the Show form. Both
// must produce identical KoiosTxInfoAssets, since CanonicalKoiosUTxOEntry
// compares them against Dingo's own decode of the same UTxO.
func TestAssetListShowFormMatchesArrayFormForTheSameTokens(t *testing.T) {
	t.Parallel()

	arrayForm := `[
		{"decimals":0,"quantity":"1","policy_id":"09e56a1dcecb140f4416b48f5ca475aab638d64fcb2f76e9d6496c0e","asset_name":"474f565f4e4654","fingerprint":"asset1sy4k02hdjqcdl0kq309ujqtnt3d0mjzqzlrzy5"},
		{"decimals":0,"quantity":"32200000000000","policy_id":"65a938b778af9a654b2a005d8ea902485f16a878ff81c458eaa7bdbb","asset_name":"494e4459","fingerprint":"asset17pxgv9fap9zfqmykvlk8s643z2wd9u78lw88a0"}
	]`

	var fromArray, fromShow KoiosTxInfoAssetList
	require.NoError(t, json.Unmarshal([]byte(arrayForm), &fromArray))
	require.NoError(t, json.Unmarshal(
		[]byte(mustJSONString(t, realCollateralOutputShowForm)), &fromShow,
	))
	require.Equal(t, fromArray, fromShow)
}

// TestAssetListRoundTripsThroughCachePayload proves the fix needs no
// koiosTxInfoPayloadVersion bump: what is STORED is unchanged. A decoded
// asset list marshals to the plain array form, and reading that back yields
// the same assets, so every koios_tx_info row already written at version 2
// stays valid. (The bug only ever prevented rows from being written at all --
// a chunk containing an undecodable transaction cached nothing -- so no
// stored row can hold a mis-parsed collateral asset list.)
func TestAssetListRoundTripsThroughCachePayload(t *testing.T) {
	t.Parallel()

	var decoded KoiosTxInfoAssetList
	require.NoError(t, json.Unmarshal(
		[]byte(mustJSONString(t, realCollateralOutputShowForm)), &decoded,
	))
	require.Len(t, decoded, 2)

	stored, err := json.Marshal(decoded)
	require.NoError(t, err)
	require.JSONEq(t,
		`[{"policy_id":"09e56a1dcecb140f4416b48f5ca475aab638d64fcb2f76e9d6496c0e","asset_name":"474f565f4e4654","quantity":"1"},`+
			`{"policy_id":"65a938b778af9a654b2a005d8ea902485f16a878ff81c458eaa7bdbb","asset_name":"494e4459","quantity":"32200000000000"}]`,
		string(stored),
	)

	var reread KoiosTxInfoAssetList
	require.NoError(t, json.Unmarshal(stored, &reread))
	require.Equal(t, decoded, reread)
}

// TestAssetListRejectsUnrecognisedStringFormLoudly preserves the safety
// property. A string form this parser genuinely cannot read must still be an
// error -- never silently an empty asset list, which would drop tokens from
// the reconstructed UTxO and turn a parser gap into a phantom Dingo
// mismatch. The error must carry the offending text.
func TestAssetListRejectsUnrecognisedStringFormLoudly(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{
		`"[(SomethingElse {x = 1},[(\"01\",2)])]"`,
		`"[(PolicyID {policyID = ScriptHash \"aa\"},[(\"01\",)])]"`,
		`"[(PolicyID {policyID = ScriptHash \"aa\"},[(\"01\",2)])"`,
		`"not a list at all"`,
	} {
		var got KoiosTxInfoAssetList
		err := json.Unmarshal([]byte(bad), &got)
		require.Error(t, err, "input %s must not decode silently", bad)
		require.Contains(t, err.Error(), "asset_list")
		require.Nil(t, got)
	}
}

// TestGetTxInfosNamesOffendingTransactionOnDecodeFailure pins the error
// context improvement: an undecodable transaction must be identified by hash
// in the error, instead of the bare "koios /tx_info decode: invalid character
// ..." that gave no clue which of 40 transactions (or which field) was at
// fault. The chunk still fails -- see describeTxInfoDecodeFailure for why
// that is deliberate.
func TestGetTxInfosNamesOffendingTransactionOnDecodeFailure(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			// Second transaction carries an asset_list string this parser
			// cannot read.
			_, _ = w.Write([]byte(`[
				{"tx_hash":"aaaa","outputs":[],"inputs":[]},
				{"tx_hash":"bbbb","collateral_output":{"tx_hash":"bbbb","tx_index":0,"value":"1","asset_list":"[(Mystery {})]"}}
			]`))
		},
	))
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	_, err := k.GetTxInfos(t.Context(), []string{"aaaa", "bbbb"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "bbbb",
		"the error must name the offending transaction")
	require.Contains(t, err.Error(), "asset_list",
		"the error must name the offending field")
	require.Contains(t, err.Error(), "Mystery",
		"the error must include the text that failed to parse")
}

// TestGetTxInfosDecodesRealCollateralChunk is the end-to-end proof: a chunk
// containing a transaction with a token-bearing collateral return decodes
// completely, where before the fix the whole chunk failed -- which is what
// tainted the epoch and skipped its UTxO comparison.
func TestGetTxInfosDecodesRealCollateralChunk(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			body := `[
				{"tx_hash":"aaaa","inputs":[],"outputs":[{"tx_hash":"aaaa","tx_index":0,"value":"1000000","asset_list":[]}]},
				{"tx_hash":"bbbb","inputs":[],"outputs":[],
				 "collateral_output":{"tx_hash":"bbbb","tx_index":2,"value":"956015319","asset_list":` +
				mustJSONString(t, realCollateralOutputShowForm) + `},
				 "plutus_contracts":[{"valid_contract":false}]}
			]`
			_, _ = w.Write([]byte(body))
		},
	))
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	items, err := k.GetTxInfos(t.Context(), []string{"aaaa", "bbbb"})
	require.NoError(t, err)
	require.Len(t, items, 2)

	collateral := items[1].CollateralOutput
	require.NotNil(t, collateral)
	require.Len(t, collateral.AssetList, 2)
	require.Equal(t, "32200000000000", collateral.AssetList[1].Quantity)

	// The phase-2-invalid transaction's produced UTxO is its collateral
	// return, and its canonical encoding must carry the tokens -- the whole
	// reason this field has to decode correctly.
	produced := items[1].Produced()
	require.Len(t, produced, 1)
	require.Contains(t,
		CanonicalKoiosUTxOEntry(produced[0]),
		"65a938b778af9a654b2a005d8ea902485f16a878ff81c458eaa7bdbb.494e4459=32200000000000",
	)
}

func mustJSONString(t *testing.T, s string) string {
	t.Helper()
	b, err := json.Marshal(s)
	require.NoError(t, err)
	return string(b)
}

// TestKoiosTxInfoItem_DecodesCollateralAndValidity pins the decode against
// the real response shape. A top-level "valid_contract" key does not exist
// in /tx_info at all, and collateral_output's asset_list is a string; a
// decoder that assumed either otherwise would read every transaction as
// valid, or fail outright on any transaction with a collateral return.
func TestKoiosTxInfoItem_DecodesCollateralAndValidity(t *testing.T) {
	const payload = `[{
	  "tx_hash": "aa",
	  "inputs": [{"tx_hash": "in0", "tx_index": 0, "asset_list": []}],
	  "outputs": [
	    {"tx_hash": "aa", "tx_index": 0, "value": "1000000",
	     "payment_addr": {"bech32": "addr_test1body0"},
	     "datum_hash": null, "inline_datum": null,
	     "reference_script": null, "asset_list": []},
	    {"tx_hash": "aa", "tx_index": 1, "value": "2000000",
	     "payment_addr": {"bech32": "addr_test1body1"},
	     "datum_hash": null, "inline_datum": null,
	     "reference_script": null, "asset_list": []}
	  ],
	  "collateral_inputs": [{"tx_hash": "col0", "tx_index": 3, "asset_list": []}],
	  "collateral_output": {
	    "tx_hash": "aa", "tx_index": 2, "value": "4277281629",
	    "payment_addr": {"bech32": "addr_test1collateralreturn"},
	    "datum_hash": null, "inline_datum": null, "reference_script": null,
	    "asset_list": "[]"
	  },
	  "plutus_contracts": [{"valid_contract": false, "script_hash": "ss"}]
	}]`

	var items []KoiosTxInfoItem
	require.NoError(t, json.Unmarshal([]byte(payload), &items))
	require.Len(t, items, 1)
	item := items[0]

	require.Len(t, item.PlutusContracts, 1)
	require.NotNil(t, item.PlutusContracts[0].ValidContract)
	assert.False(t, *item.PlutusContracts[0].ValidContract)
	assert.False(t, item.IsValid())

	require.NotNil(t, item.CollateralOutput)
	assert.Equal(t, 2, item.CollateralOutput.TxIndex)
	assert.Equal(t, "4277281629", item.CollateralOutput.Value)
	assert.Empty(
		t, item.CollateralOutput.AssetList,
		`collateral_output's "[]" string form must decode to an empty list`,
	)
	require.Len(t, item.CollateralInputs, 1)
	assert.Equal(t, "col0", item.CollateralInputs[0].TxHash)
}

// TestKoiosTxInfoAssetList_BothForms pins the two serialisations Koios uses
// for the same content, plus the null case.
func TestKoiosTxInfoAssetList_BothForms(t *testing.T) {
	const arrayForm = `[{"policy_id": "pp", "asset_name": "nn", "quantity": "5"}]`
	const stringForm = `"[{\"policy_id\": \"pp\", \"asset_name\": \"nn\", \"quantity\": \"5\"}]"`

	var fromArray, fromString, fromNull, fromEmptyString KoiosTxInfoAssetList
	require.NoError(t, json.Unmarshal([]byte(arrayForm), &fromArray))
	require.NoError(t, json.Unmarshal([]byte(stringForm), &fromString))
	require.NoError(t, json.Unmarshal([]byte(`null`), &fromNull))
	require.NoError(t, json.Unmarshal([]byte(`""`), &fromEmptyString))

	assert.Equal(t, fromArray, fromString)
	require.Len(t, fromArray, 1)
	assert.Equal(t, "pp", fromArray[0].PolicyID)
	assert.Equal(t, "5", fromArray[0].Quantity)
	assert.Empty(t, fromNull)
	assert.Empty(t, fromEmptyString)

	// Marshalling is always the array form, so a cached row written from
	// this struct reads back through the array branch.
	encoded, err := json.Marshal(fromString)
	require.NoError(t, err)
	assert.JSONEq(t, arrayForm, string(encoded))
}

// TestKoiosTxInfoItem_ConsumedProduced pins the semantics against gouroboros'
// Transaction.Consumed()/Produced(): body inputs and outputs when phase-2
// validation passed, collateral inputs and the collateral return when it
// failed.
func TestKoiosTxInfoItem_ConsumedProduced(t *testing.T) {
	body := KoiosTxInfoItem{
		TxHash:           "aa",
		Inputs:           []KoiosTxInfoUtxoRef{{TxHash: "in0", TxIndex: 0}},
		Outputs:          []KoiosTxInfoOutput{{TxHash: "aa", TxIndex: 0}},
		CollateralInputs: []KoiosTxInfoUtxoRef{{TxHash: "col0", TxIndex: 3}},
		CollateralOutput: &KoiosTxInfoOutput{TxHash: "aa", TxIndex: 1},
	}

	t.Run("no plutus contracts is valid", func(t *testing.T) {
		assert.True(t, body.IsValid())
		assert.Equal(t, body.Inputs, body.Consumed())
		assert.Equal(t, body.Outputs, body.Produced())
	})

	t.Run("valid_contract true is valid", func(t *testing.T) {
		item := body
		item.PlutusContracts = []KoiosTxInfoPlutusContract{
			{ValidContract: new(true)},
			{ValidContract: new(true)},
		}
		assert.True(t, item.IsValid())
		assert.Equal(t, item.Inputs, item.Consumed())
		assert.Equal(t, item.Outputs, item.Produced())
	})

	t.Run("no verdict reported is valid", func(t *testing.T) {
		item := body
		item.PlutusContracts = []KoiosTxInfoPlutusContract{{ValidContract: nil}}
		assert.True(
			t, item.IsValid(),
			"a null valid_contract is an unreported verdict, not an invalid one",
		)
	})

	t.Run("valid_contract false consumes collateral", func(t *testing.T) {
		item := body
		item.PlutusContracts = []KoiosTxInfoPlutusContract{
			{ValidContract: new(false)},
		}
		assert.False(t, item.IsValid())
		assert.Equal(t, item.CollateralInputs, item.Consumed())
		require.Len(t, item.Produced(), 1)
		assert.Equal(t, 1, item.Produced()[0].TxIndex)
	})

	t.Run("invalid with no collateral return produces nothing", func(t *testing.T) {
		item := body
		item.CollateralOutput = nil
		item.PlutusContracts = []KoiosTxInfoPlutusContract{
			{ValidContract: new(false)},
		}
		assert.False(t, item.IsValid())
		assert.Empty(t, item.Produced())
		assert.Equal(t, item.CollateralInputs, item.Consumed())
	})
}

// TestComparePoolEpochMemberRewardsBeforeApplication is the
// regression, built from the divergence a Preview replay reported at epoch 96.
//
// Dingo computed 4006269 in member rewards for the pool and Koios reported
// 4004412. The 1857 difference was a reward computed for a stake credential
// that deregistered before the boundary which applies it, so the ledger never
// credited it. Dingo was right; the comparison simply ran 72 seconds before
// that boundary, while the per-account spendable flags were still provisional.
//
// Recomputed after the boundary the two agree exactly, so the difference has to
// be classified by whether the rewards have been applied, not by its size.
func TestComparePoolEpochMemberRewardsBeforeApplication(t *testing.T) {
	const (
		koiosPaid     = "4004412"
		dingoComputed = "4006269"
	)
	koios := &KoiosPoolEpoch{
		PoolBech32:    "pool1l5u4zh84na80xr56d342d32rsdw62qycwaw97hy9wwsc6axdwla",
		MemberRewards: koiosPaid,
	}
	base := func() *DingoPoolEpochData {
		return &DingoPoolEpochData{
			MemberRewardPresent:          true,
			MemberRewardTotal:            dingoComputed,
			SpendableMemberRewardPresent: true,
			SpendableMemberRewardTotal:   dingoComputed,
			PoolUnspendable:              1857,
		}
	}
	now := time.Now()

	find := func(t *testing.T, ms []CheckMismatch) CheckMismatch {
		t.Helper()
		for _, m := range ms {
			if m.Field == "member_rewards" {
				return m
			}
		}
		require.FailNow(t, "no member_rewards mismatch produced")
		return CheckMismatch{}
	}

	t.Run("before application it is a timing statement", func(t *testing.T) {
		d := base()
		d.RewardsPending = true
		m := find(t, ComparePoolEpoch(
			"preview", 96, koios, d, now, 0, time.Time{}, false,
			false,
		))
		assert.Equal(t, CategoryReferenceLag, m.Category,
			"a pending forfeiture must not be reported as a divergence")
	})

	t.Run("after application it is a real divergence", func(t *testing.T) {
		d := base()
		d.RewardsPending = false
		m := find(t, ComparePoolEpoch(
			"preview", 96, koios, d, now, 0, time.Time{}, false,
			false,
		))
		assert.Equal(t, CategoryValueMismatch, m.Category,
			"once applied, a difference is a genuine mismatch")
	})

	t.Run("agreement after application reports nothing", func(t *testing.T) {
		d := base()
		d.RewardsPending = false
		d.SpendableMemberRewardTotal = koiosPaid
		for _, m := range ComparePoolEpoch(
			"preview", 96, koios, d, now, 0, time.Time{}, false,
			false,
		) {
			assert.NotEqual(t, "member_rewards", m.Field,
				"the spendable sum equals Koios, so nothing to report")
		}
	})
}

// TestComparePoolEpochMissingRewardsBeforeApplication is the
// regression.
//
// A reward_pool_output row for a stake epoch is not written until well after
// that epoch closes, so an observer running near the tip asks about epochs
// Dingo has not computed yet. The grace window that exists for exactly this is
// measured in wall-clock time against the epoch's real close time, so during a
// from-genesis replay -- where every epoch closed years ago -- it can never
// fire, and the absence was reported as dingo_db_missing against a node that
// was simply not there yet.
func TestComparePoolEpochMissingRewardsBeforeApplication(t *testing.T) {
	koios := &KoiosPoolEpoch{
		PoolBech32:    "pool1l5u4zh84na80xr56d342d32rsdw62qycwaw97hy9wwsc6axdwla",
		MemberRewards: "4004412",
	}
	// No reward_pool_output row: MemberRewardPresent is false.
	missing := func(pending bool) *DingoPoolEpochData {
		return &DingoPoolEpochData{RewardsPending: pending}
	}
	now := time.Now()
	// An epoch that closed long ago, as every epoch of a replay has.
	longClosed := now.Add(-1388 * 24 * time.Hour)

	find := func(t *testing.T, ms []CheckMismatch) CheckMismatch {
		t.Helper()
		for _, m := range ms {
			if m.Field == "member_rewards" {
				return m
			}
		}
		require.FailNow(t, "no member_rewards mismatch produced")
		return CheckMismatch{}
	}

	t.Run(
		"not computed yet is a lag even long after the epoch closed",
		func(t *testing.T) {
			m := find(t, ComparePoolEpoch(
				"preview", 44, koios, missing(true), now, 24, longClosed, false,
				false,
			))
			assert.Equal(
				t,
				CategoryReferenceLag,
				m.Category,
				"the wall-clock window cannot fire in a replay; chain position must",
			)
		},
	)

	t.Run("past the boundary a missing row is a real gap", func(t *testing.T) {
		m := find(t, ComparePoolEpoch(
			"preview", 44, koios, missing(false), now, 24, longClosed, false,
			false,
		))
		assert.Equal(t, CategoryDBMissing, m.Category,
			"once Dingo has had its chance, absence is a genuine gap")
	})

	t.Run("the wall-clock window still works at the tip", func(t *testing.T) {
		m := find(t, ComparePoolEpoch(
			"preview", 44, koios, missing(false), now, 24,
			now.Add(-1*time.Hour), false,
			false,
		))
		assert.Equal(t, CategoryReferenceLag, m.Category,
			"a recently closed epoch keeps the existing grace behaviour")
	})
}

// TestCompareAccountEpochPendingRewardsAreALag is the account-granularity half
// failure mode.
//
// When Dingo has not computed an epoch's rewards yet, every account Koios
// reports a reward for is absent on the Dingo side. That is timing, not
// divergence, and at replay speed it dominates everything else: a Preview run
// produced 15879 acct_only_koios entries, and the epochs carrying them were
// exactly the epochs with no reward row.
//
// The Dingo-only direction is the same statement read the other way (dingo
// ): before the boundary a reward computed for a credential that
// deregisters in the meantime is still marked spendable, so Dingo holds a row
// Koios will never publish. That is timing too, and the branch claimed to be
// symmetric with the Koios-only one while omitting the guard.
func TestCompareAccountEpochPendingRewardsAreALag(t *testing.T) {
	koios := []KoiosAccountRewards{
		{StakeAddress: "stake_test1a", RewardType: "member", Earned: "1000000"},
		{StakeAddress: "stake_test1b", RewardType: "member", Earned: "2000000"},
	}
	now := time.Now()
	// An epoch that closed long ago, as every epoch of a replay has, so the
	// wall-clock grace window cannot fire.
	longClosed := now.Add(-1388 * 24 * time.Hour)

	categories := func(ms []CheckMismatch) []string {
		var out []string
		for _, m := range ms {
			if m.Field == "account_reward_presence" {
				out = append(out, m.Category)
			}
		}
		return out
	}

	t.Run("not computed yet is a lag", func(t *testing.T) {
		ms := CompareAccountEpoch(
			"preview", 100, koios, nil, now, 24, longClosed, true,
		)
		got := categories(ms)
		require.Len(t, got, 2)
		for _, c := range got {
			assert.Equal(t, CategoryReferenceLag, c,
				"an epoch Dingo has not computed cannot be a divergence")
		}
	})

	t.Run("a differing amount while pending is also a lag", func(t *testing.T) {
		dingoRows := []DingoAccountReward{
			{
				StakeAddress: "stake_test1a",
				RewardType:   "member",
				Amount:       "999999",
			},
		}
		ms := CompareAccountEpoch(
			"preview", 100, koios[:1], dingoRows, now, 24, longClosed, true,
		)
		found := false
		for _, m := range ms {
			if m.Field == "account_reward_amount" {
				found = true
				assert.Equal(t, CategoryReferenceLag, m.Category,
					"an amount that can still change is not a divergence")
			}
		}
		// Without this the assertions above never run if the comparison stops
		// emitting the mismatch at all, and the case passes vacuously.
		require.True(t, found,
			"the differing amount must still be reported, as a lag")
	})

	t.Run("a differing amount once applied is a mismatch", func(t *testing.T) {
		dingoRows := []DingoAccountReward{
			{
				StakeAddress: "stake_test1a",
				RewardType:   "member",
				Amount:       "999999",
			},
		}
		ms := CompareAccountEpoch(
			"preview", 100, koios[:1], dingoRows, now, 24, longClosed, false,
		)
		found := false
		for _, m := range ms {
			if m.Field == "account_reward_amount" {
				found = true
				assert.Equal(t, CategoryValueMismatch, m.Category)
			}
		}
		assert.True(t, found, "a differing amount must still be reported")
	})

	t.Run("a Dingo-only row while pending is a lag", func(t *testing.T) {
		dingoRows := []DingoAccountReward{
			{
				StakeAddress: "stake_test1c",
				RewardType:   "member",
				Amount:       "1857",
			},
		}
		ms := CompareAccountEpoch(
			"preview", 100, nil, dingoRows, now, 24, longClosed, true,
		)
		got := categories(ms)
		require.Len(t, got, 1,
			"the Dingo-only row must still be reported, as a lag")
		assert.Equal(t, CategoryReferenceLag, got[0],
			"a reward whose spendable flag is still provisional is timing")
	})

	t.Run(
		"a Dingo-only row once applied is a real finding",
		func(t *testing.T) {
			dingoRows := []DingoAccountReward{
				{
					StakeAddress: "stake_test1c",
					RewardType:   "member",
					Amount:       "1857",
				},
			}
			ms := CompareAccountEpoch(
				"preview", 100, nil, dingoRows, now, 24, longClosed, false,
			)
			got := categories(ms)
			require.Len(t, got, 1)
			assert.Equal(t, CategoryAcctOnlyDingo, got[0],
				"once applied, a row Koios never credited is worth reporting")
		},
	)

	t.Run("computed and still absent is a real finding", func(t *testing.T) {
		ms := CompareAccountEpoch(
			"preview", 100, koios, nil, now, 24, longClosed, false,
		)
		got := categories(ms)
		require.Len(t, got, 2)
		for _, c := range got {
			assert.Equal(t, CategoryAcctOnlyKoios, c,
				"once Dingo has had its chance, absence is worth reporting")
		}
	})
}

// TestAccountRewardsPendingFold pins the decision checkEpoch makes about
// whether the whole epoch's account comparison may be downgraded.
//
// tests_test.go's other cases hand rewardsPending to
// CompareAccountEpoch directly, so they never reach this fold — replacing it
// with a bare `true` left the entire package green. It is the code that
// decides whether every account-level presence *and* amount mismatch in an
// epoch is waived, and the two guards that keep it narrow (every entry, not
// any; an empty or nil-bearing map claims nothing) are what needed pinning.
func TestAccountRewardsPendingFold(t *testing.T) {
	pending := func(v bool) *DingoPoolEpochData {
		return &DingoPoolEpochData{RewardsPending: v}
	}
	for _, tc := range []struct {
		name string
		in   map[string]*DingoPoolEpochData
		want bool
	}{{
		name: "every pool pending",
		in: map[string]*DingoPoolEpochData{
			"aa": pending(true), "bb": pending(true),
		},
		want: true,
	}, {
		name: "one pool past its boundary keeps the epoch strict",
		in: map[string]*DingoPoolEpochData{
			"aa": pending(true), "bb": pending(false),
		},
		want: false,
	}, {
		// The map is the only evidence available, so having none of it is not
		// evidence that the rewards are pending.
		name: "an empty map claims nothing",
		in:   map[string]*DingoPoolEpochData{},
		want: false,
	}, {
		name: "a nil map claims nothing",
		in:   nil,
		want: false,
	}, {
		// A nil entry is an absence of information about that pool, and one
		// pool with no answer is enough to keep the epoch strict.
		name: "a nil entry claims nothing",
		in: map[string]*DingoPoolEpochData{
			"aa": pending(true), "bb": nil,
		},
		want: false,
	}, {
		name: "a single pending pool is enough when it is the only one",
		in:   map[string]*DingoPoolEpochData{"aa": pending(true)},
		want: true,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, accountRewardsPending(tc.in))
		})
	}
}

// TestDingoDBPropagatesApplyingEpochLookupError prevents a failed E+3 lookup
// from being treated as evidence that rewards are pending. The pending flag is
// only valid for a missing epoch row; a database failure must reach the caller.
func TestDingoDBPropagatesApplyingEpochLookupError(t *testing.T) {
	t.Parallel()

	db, gdb := openTestDingoDB(t)
	pool := testPoolKeyHash(t, 0x41)
	require.NoError(t, gdb.Exec(
		`INSERT INTO reward_pool_input (pool_key_hash, epoch, delegated_stake, delegator_count)
		 VALUES (?, ?, ?, ?)`,
		pool,
		9,
		"1000",
		1,
	).Error)
	require.NoError(t, gdb.Exec(
		`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
		[]byte{0x01}, 100, 1,
	).Error)
	require.NoError(t, gdb.Exec(`DROP TABLE epoch`).Error)

	_, err := db.GetPoolEpochDataMap(context.Background(), 9, 10)
	require.ErrorContains(t, err, "epoch lookup")
}

// TestDatabaseSourcePropagatesApplyingEpochLookupError covers the same
// contract through the in-process source. Both RewardParitySource
// implementations must fail closed when their E+3 lookup cannot run.
func TestDatabaseSourcePropagatesApplyingEpochLookupError(t *testing.T) {
	t.Parallel()

	db := newTestDatabaseSourceDB(t)
	sqlDB := sourceSQLDB(t, db)
	pool := testPoolKeyHash(t, 0x42)
	require.NoError(t, sqlDB.Create(&models.RewardPoolInput{
		PoolKeyHash:    pool,
		Epoch:          9,
		DelegatedStake: types.Uint64(1000),
		DelegatorCount: 1,
	}).Error)
	require.NoError(t, sqlDB.Exec(
		`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
		[]byte{0x01}, 100, 1,
	).Error)
	require.NoError(t, sqlDB.Exec(`DROP TABLE epoch`).Error)

	source, err := NewDatabaseSource(db)
	require.NoError(t, err)
	_, err = source.GetPoolEpochDataMap(context.Background(), 9, 10)
	require.ErrorContains(t, err, "epoch lookup 12")
}

func TestMissingApplyingEpochIsThePendingCase(t *testing.T) {
	t.Parallel()

	t.Run("standalone source", func(t *testing.T) {
		db, gdb := openTestDingoDB(t)
		pool := testPoolKeyHash(t, 0x43)
		require.NoError(t, gdb.Exec(
			`INSERT INTO reward_pool_input (pool_key_hash, epoch, delegated_stake, delegator_count)
			 VALUES (?, ?, ?, ?)`,
			pool,
			9,
			"1000",
			1,
		).Error)
		require.NoError(t, gdb.Exec(
			`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
			[]byte{0x01}, 100, 1,
		).Error)

		m, err := db.GetPoolEpochDataMap(context.Background(), 9, 10)
		require.NoError(t, err)
		data, ok := m[hex.EncodeToString(pool)]
		require.True(t, ok)
		require.True(t, data.RewardsPending)
	})

	t.Run("in-process source", func(t *testing.T) {
		db := newTestDatabaseSourceDB(t)
		sqlDB := sourceSQLDB(t, db)
		pool := testPoolKeyHash(t, 0x44)
		require.NoError(t, sqlDB.Create(&models.RewardPoolInput{
			PoolKeyHash:    pool,
			Epoch:          9,
			DelegatedStake: types.Uint64(1000),
			DelegatorCount: 1,
		}).Error)
		require.NoError(t, sqlDB.Exec(
			`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
			[]byte{0x01}, 100, 1,
		).Error)

		source, err := NewDatabaseSource(db)
		require.NoError(t, err)
		m, err := source.GetPoolEpochDataMap(context.Background(), 9, 10)
		require.NoError(t, err)
		data, ok := m[hex.EncodeToString(pool)]
		require.True(t, ok)
		require.True(t, data.RewardsPending)
	})
}

func TestPositiveSlotWithoutTipHashIsNotPending(t *testing.T) {
	t.Parallel()

	t.Run("standalone source", func(t *testing.T) {
		db, gdb := openTestDingoDB(t)
		pool := testPoolKeyHash(t, 0x45)
		require.NoError(t, gdb.Exec(
			`INSERT INTO reward_pool_input (pool_key_hash, epoch, delegated_stake, delegator_count)
			 VALUES (?, ?, ?, ?)`,
			pool,
			9,
			"1000",
			1,
		).Error)
		require.NoError(t, gdb.Exec(
			`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
			[]byte{}, 100, 1,
		).Error)

		m, err := db.GetPoolEpochDataMap(context.Background(), 9, 10)
		require.NoError(t, err)
		require.False(t, m[hex.EncodeToString(pool)].RewardsPending)
	})

	t.Run("in-process source", func(t *testing.T) {
		db := newTestDatabaseSourceDB(t)
		sqlDB := sourceSQLDB(t, db)
		pool := testPoolKeyHash(t, 0x46)
		require.NoError(t, sqlDB.Create(&models.RewardPoolInput{
			PoolKeyHash: pool, Epoch: 9, DelegatedStake: types.Uint64(1000), DelegatorCount: 1,
		}).Error)
		require.NoError(t, sqlDB.Exec(
			`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
			[]byte{}, 100, 1,
		).Error)

		source, err := NewDatabaseSource(db)
		require.NoError(t, err)
		m, err := source.GetPoolEpochDataMap(context.Background(), 9, 10)
		require.NoError(t, err)
		require.False(t, m[hex.EncodeToString(pool)].RewardsPending)
	})
}

func TestZeroSlotWithTipHashIsNotPending(t *testing.T) {
	t.Parallel()

	t.Run("standalone source", func(t *testing.T) {
		db, gdb := openTestDingoDB(t)
		pool := testPoolKeyHash(t, 0x47)
		require.NoError(t, gdb.Exec(
			`INSERT INTO reward_pool_input (pool_key_hash, epoch, delegated_stake, delegator_count)
			 VALUES (?, ?, ?, ?)`,
			pool,
			9,
			"1000",
			1,
		).Error)
		require.NoError(t, gdb.Exec(
			`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
			[]byte{0x01}, 0, 1,
		).Error)
		m, err := db.GetPoolEpochDataMap(context.Background(), 9, 10)
		require.NoError(t, err)
		require.False(t, m[hex.EncodeToString(pool)].RewardsPending)
	})

	t.Run("in-process source", func(t *testing.T) {
		db := newTestDatabaseSourceDB(t)
		sqlDB := sourceSQLDB(t, db)
		pool := testPoolKeyHash(t, 0x48)
		require.NoError(t, sqlDB.Create(&models.RewardPoolInput{
			PoolKeyHash: pool, Epoch: 9, DelegatedStake: types.Uint64(1000), DelegatorCount: 1,
		}).Error)
		require.NoError(t, sqlDB.Exec(
			`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
			[]byte{0x01}, 0, 1,
		).Error)
		source, err := NewDatabaseSource(db)
		require.NoError(t, err)
		m, err := source.GetPoolEpochDataMap(context.Background(), 9, 10)
		require.NoError(t, err)
		require.False(t, m[hex.EncodeToString(pool)].RewardsPending)
	})
}

func TestComparePoolEpochUsesRewardsPending(t *testing.T) {
	t.Parallel()

	memberRewardMismatch := func(mismatches []CheckMismatch) CheckMismatch {
		for _, mismatch := range mismatches {
			if mismatch.Field == "member_rewards" {
				return mismatch
			}
		}
		t.Fatal("member_rewards mismatch not found")
		return CheckMismatch{}
	}

	koios := &KoiosPoolEpoch{MemberRewards: "1"}
	dingo := &DingoPoolEpochData{
		MemberRewardPresent:          true,
		SpendableMemberRewardPresent: true,
		SpendableMemberRewardTotal:   "2",
	}

	dingo.RewardsPending = true
	mismatches := ComparePoolEpoch(
		"preview", 96, koios, dingo, time.Now(), 0, time.Time{}, false,
		false,
	)
	require.Equal(
		t,
		CategoryReferenceLag,
		memberRewardMismatch(mismatches).Category,
	)

	dingo.RewardsPending = false
	mismatches = ComparePoolEpoch(
		"preview", 96, koios, dingo, time.Now(), 0, time.Time{}, false,
		false,
	)
	require.Equal(
		t,
		CategoryValueMismatch,
		memberRewardMismatch(mismatches).Category,
	)
}

// TestCountSignificantExcludesInformational pins that the number reported with
// a parity failure counts the mismatches that caused it.
//
// DetermineStatus deliberately treats the lifecycle and pool-departure
// categories as no-ops, so an epoch can hold many of them and still pass.
// Counting them in the failure message points the reader at rows that are by
// definition never the reason. Preview epoch 198 failed on 3 mismatches and
// reported 12.
func TestCountSignificantExcludesInformational(t *testing.T) {
	t.Parallel()

	mismatches := []CheckMismatch{
		{Category: CategoryAcctOnlyDingo},
		{Category: CategoryAcctOnlyDingo},
		{Category: CategoryAcctOnlyDingo},
		{Category: CategoryAcctZeroReward},
	}
	for range 8 {
		mismatches = append(
			mismatches,
			CheckMismatch{Category: CategoryPoolDeparted},
		)
	}
	require.Len(t, mismatches, 12)
	assert.Equal(t, 3, CountSignificant(mismatches))
	assert.Equal(t, StatusFail, DetermineStatus(mismatches))
}

// TestCountSignificantCountsErrors keeps the error categories significant:
// they drive StatusError, so they are a reason too.
func TestCountSignificantCountsErrors(t *testing.T) {
	t.Parallel()

	mismatches := []CheckMismatch{
		{Category: CategoryDBError},
		{Category: CategoryReferenceLag},
		{Category: CategoryPoolDeparted},
	}
	assert.Equal(t, 2, CountSignificant(mismatches))
	assert.Equal(t, StatusError, DetermineStatus(mismatches))
}

// TestReferenceLagOnly pins the one non-pass shape strict mode does not treat
// as fatal: at least one significant mismatch, all of them
// reference_lag. The other ERROR-severity categories must not qualify, or a
// row Dingo never wrote would stop failing a strict node once the grace
// window closes.
func TestReferenceLagOnly(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		categories []string
		want       bool
	}{
		{"empty", nil, false},
		{"informational only", []string{CategoryPoolDeparted}, false},
		{
			"lag only",
			[]string{CategoryReferenceLag, CategoryReferenceLag},
			true,
		},
		{
			"lag plus informational",
			[]string{CategoryReferenceLag, CategoryPoolDeparted},
			true,
		},
		{
			"lag plus db missing",
			[]string{CategoryReferenceLag, CategoryDBMissing},
			false,
		},
		{
			"lag plus db error",
			[]string{CategoryReferenceLag, CategoryDBError},
			false,
		},
		{
			"lag plus coverage incomplete",
			[]string{CategoryReferenceLag, CategoryAcctCoverageIncomplete},
			false,
		},
		{
			"lag plus value mismatch",
			[]string{CategoryReferenceLag, CategoryValueMismatch},
			false,
		},
		{"db missing only", []string{CategoryDBMissing}, false},
	}
	for _, tc := range cases {
		mismatches := make([]CheckMismatch, 0, len(tc.categories))
		for _, c := range tc.categories {
			mismatches = append(mismatches, CheckMismatch{Category: c})
		}
		assert.Equal(t, tc.want, referenceLagOnly(mismatches), tc.name)
	}
}

// TestSeverityLabelCoversAllThreeTiers pins severityLabel's string mapping
// against severityOf's classification of one category from each of the three
// tiers, so the label dingo_koiosparity_mismatch_total exports (metrics.go)
// cannot silently disagree with what DetermineStatus/CountSignificant treat
// that category as.
func TestSeverityLabelCoversAllThreeTiers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		category string
		want     string
	}{
		{"fail tier", CategoryValueMismatch, "fail"},
		{"error tier", CategoryDBError, "error"},
		{"informational tier", CategoryPoolDeparted, "informational"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(
				t,
				tc.want,
				severityLabel(severityOf(tc.category)),
				"category %q", tc.category,
			)
		})
	}
}

// TestCountSignificantAgreesWithDetermineStatus is the invariant that matters
// more than either number: a status of PASS and a non-zero significant count
// cannot coexist, in either direction. The two must read the same
// classification, or a future category added to one will silently disagree
// with the other.
//
// The loop is AllCategories itself, not a copy of it: a hand-maintained list
// here would omit exactly the category a contributor also forgot to add to
// severityOf, and the guard would pass on the case it exists to catch.
func TestCountSignificantAgreesWithDetermineStatus(t *testing.T) {
	t.Parallel()

	for _, cat := range AllCategories {
		t.Run(cat, func(t *testing.T) {
			ms := []CheckMismatch{{Category: cat}}
			passed := DetermineStatus(ms) == StatusPass
			assert.Equal(t, passed, CountSignificant(ms) == 0,
				"status and significant count must classify %q the same way",
				cat)
		})
	}
}

// TestAllCategoriesCoversEveryConstant keeps AllCategories honest.
//
// AllCategories is what makes the classification guard above load-bearing, so
// it must not itself be a list that can fall behind. This reads the Category*
// constants straight out of the package source and requires the two to be the
// same set: a new category constant that never reaches AllCategories fails
// here rather than sliding past a guard that simply never sees it.
func TestAllCategoriesCoversEveryConstant(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	require.NoError(t, err)
	pkg, ok := pkgs["koiosparity"]
	require.True(t, ok, "package koiosparity must be parseable from .")

	declared := map[string]string{} // constant name -> literal value
	for _, file := range pkg.Files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, name := range vs.Names {
					if !strings.HasPrefix(name.Name, "Category") ||
						i >= len(vs.Values) {
						continue
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					value, err := strconv.Unquote(lit.Value)
					require.NoError(t, err)
					declared[name.Name] = value
				}
			}
		}
	}
	require.NotEmpty(t, declared, "no Category* constants found to check")

	listed := map[string]bool{}
	for _, cat := range AllCategories {
		require.False(t, listed[cat],
			"AllCategories lists %q more than once", cat)
		listed[cat] = true
	}
	for name, value := range declared {
		assert.True(t, listed[value],
			"constant %s (%q) is missing from AllCategories, so severityOf's "+
				"classification of it is unguarded", name, value)
		delete(listed, value)
	}
	for cat := range listed {
		assert.Fail(t, "unknown category listed",
			"AllCategories contains %q, which is not a Category* constant",
			cat)
	}
}

type testDB struct{ db *sql.DB }
type testResult struct{ Error error }

func (d *testDB) DB() (*sql.DB, error) { return d.db, nil }
func (d *testDB) Close() error         { return d.db.Close() }

func (d *testDB) Create(value any) testResult {
	var query string
	var args []any
	switch v := value.(type) {
	case *models.EpochSummary:
		query = `INSERT INTO epoch_summary (epoch,total_active_stake,total_pool_count,total_delegators,epoch_nonce,boundary_slot,snapshot_ready) VALUES (?,?,?,?,?,?,?)`
		args = []any{v.Epoch, v.TotalActiveStake, v.TotalPoolCount, v.TotalDelegators, v.EpochNonce, v.BoundarySlot, v.SnapshotReady}
	case *models.RewardAdaPots:
		query = `INSERT INTO reward_ada_pots (epoch,treasury,reserves,fees,rewards,captured_slot) VALUES (?,?,?,?,?,?)`
		args = []any{v.Epoch, v.Treasury, v.Reserves, v.Fees, v.Rewards, v.CapturedSlot}
	case *models.RewardPoolInput:
		query = `INSERT INTO reward_pool_input (margin,pool_key_hash,reward_account,blocks_produced,total_blocks_in_epoch,epoch,pledge,delegated_stake,owner_stake,cost,delegator_count,reward_account_credential_tag,captured_slot,boundary_slot) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
		args = []any{v.Margin, v.PoolKeyHash, v.RewardAccount, v.BlocksProduced, v.TotalBlocksInEpoch, v.Epoch, v.Pledge, v.DelegatedStake, v.OwnerStake, v.Cost, v.DelegatorCount, v.RewardAccountCredentialTag, v.CapturedSlot, v.BoundarySlot}
	case *models.RewardPoolOutput:
		query = `INSERT INTO reward_pool_output (apparent_performance,pool_key_hash,epoch,optimal_reward,total_reward,leader_reward,member_reward_total,owner_stake,undistributed,unspendable,captured_slot,boundary_slot) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`
		args = []any{v.ApparentPerformance, v.PoolKeyHash, v.Epoch, v.OptimalReward, v.TotalReward, v.LeaderReward, v.MemberRewardTotal, v.OwnerStake, v.Undistributed, v.Unspendable, v.CapturedSlot, v.BoundarySlot}
	case *models.RewardAccountOutput:
		query = `INSERT INTO reward_account_output (staking_key,pool_key_hash,reward_type,epoch,credential_tag,amount,spendable,guarded,captured_slot,boundary_slot) VALUES (?,?,?,?,?,?,?,?,?,?)`
		args = []any{v.StakingKey, v.PoolKeyHash, v.RewardType, v.Epoch, v.CredentialTag, v.Amount, v.Spendable, v.Guarded, v.CapturedSlot, v.BoundarySlot}
	case *models.PoolStakeSnapshot:
		query = `INSERT INTO pool_stake_snapshot (epoch,snapshot_type,pool_key_hash,total_stake,stake_denominator,delegator_count,captured_slot) VALUES (?,?,?,?,?,?,?)`
		args = []any{v.Epoch, v.SnapshotType, v.PoolKeyHash, v.TotalStake, v.StakeDenominator, v.DelegatorCount, v.CapturedSlot}
	case *models.RewardSnapshot:
		query = `INSERT INTO reward_snapshot (epoch,snapshot_type,total_active_stake,total_pool_count,total_delegators,captured_slot,boundary_slot,epoch_nonce,protocol_version,authoritative,calculation_version,excluded_active_stake) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`
		// ExcludedActiveStake is *types.Uint64: nil means "unknown" (a
		// snapshot captured before added the tracking), and its
		// Value() has a value receiver, so passing a nil pointer straight
		// through would panic dereferencing it. Convert nil to a real SQL
		// NULL instead of a driver.Valuer that can't be called.
		var excludedActiveStake any
		if v.ExcludedActiveStake != nil {
			excludedActiveStake = *v.ExcludedActiveStake
		}
		args = []any{
			v.Epoch, v.SnapshotType, v.TotalActiveStake, v.TotalPoolCount,
			v.TotalDelegators, v.CapturedSlot, v.BoundarySlot, v.EpochNonce,
			v.ProtocolVersion, v.Authoritative, v.CalculationVersion,
			excludedActiveStake,
		}
	default:
		return testResult{Error: fmt.Errorf("unsupported test row %T", value)}
	}
	_, err := d.db.Exec(query, args...)
	return testResult{Error: err}
}

// Exec runs an arbitrary write statement against the underlying database,
// for seeding/mutation patterns testDB.Create's fixed type switch doesn't
// cover (e.g. a targeted UPDATE simulating a rollback+replay of a single
// column).
func (d *testDB) Exec(query string, args ...any) testResult {
	_, err := d.db.Exec(query, args...)
	return testResult{Error: err}
}

func openTestSQLDB(t testingT, dir string, includePools bool) *testDB {
	t.Helper()
	path := filepath.Join(dir, "metadata.sqlite")
	db, err := sql.Open(
		"sqlite",
		"file:"+path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(OFF)",
	)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	for _, stmt := range testSchema(includePools) {
		if _, err := db.Exec(stmt); err != nil {
			_ = db.Close()
			t.Fatalf("create sqlite schema: %v", err)
		}
	}
	t.Cleanup(func() { _ = db.Close() })
	return &testDB{db: db}
}

type testingT interface {
	Helper()
	Fatalf(string, ...any)
	Cleanup(func())
}

func testSchema(includePools bool) []string {
	ret := []string{
		`CREATE TABLE pool_stake_snapshot (id INTEGER PRIMARY KEY AUTOINCREMENT, epoch INTEGER NOT NULL, snapshot_type TEXT NOT NULL, pool_key_hash BLOB NOT NULL, total_stake TEXT NOT NULL, stake_denominator TEXT NOT NULL DEFAULT '0', delegator_count INTEGER NOT NULL DEFAULT 0, captured_slot INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE epoch_summary (id INTEGER PRIMARY KEY AUTOINCREMENT, epoch INTEGER NOT NULL UNIQUE, total_active_stake TEXT NOT NULL, total_pool_count INTEGER NOT NULL DEFAULT 0, total_delegators INTEGER NOT NULL DEFAULT 0, epoch_nonce BLOB, boundary_slot INTEGER NOT NULL DEFAULT 0, snapshot_ready NUMERIC NOT NULL DEFAULT 0)`,
		`CREATE TABLE reward_ada_pots (id INTEGER PRIMARY KEY AUTOINCREMENT, epoch INTEGER NOT NULL UNIQUE, treasury TEXT NOT NULL, reserves TEXT NOT NULL, fees TEXT NOT NULL, rewards TEXT NOT NULL, captured_slot INTEGER NOT NULL DEFAULT 0)`,
		// reward_account_output is created unconditionally (not gated behind
		// includePools) — it's a cheap CREATE TABLE and is independent of the
		// per-pool reward tables; TestDingoDBGetRewardAccountOutputs seeds no
		// pool rows at all, so gating this behind includePools would force
		// that test to opt into unrelated schema it doesn't use.
		// epoch and pparams mirror Dingo's real metadata schema (column
		// names and types copied from a synced preview metadata.sqlite) so
		// GetProtocolParams is exercised against the same shape it reads in
		// production, including the nullable integer columns.
		`CREATE TABLE epoch (nonce BLOB, evolving_nonce BLOB, candidate_nonce BLOB, last_epoch_block_nonce BLOB, id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_id INTEGER, start_slot INTEGER, era_id INTEGER, slot_length INTEGER, length_in_slots INTEGER)`,
		`CREATE UNIQUE INDEX idx_epoch_epoch_id ON epoch(epoch_id)`,
		`CREATE TABLE pparams (cbor BLOB, id INTEGER PRIMARY KEY AUTOINCREMENT, added_slot INTEGER, epoch INTEGER, era_id INTEGER)`,
		// sync_state backs DingoDB.GetEarliestAvailableEpoch's read of the
		// mithril_ledger_slot boundary -- created
		// unconditionally like epoch/pparams above, since a check run reads
		// it regardless of whether the test seeds a boundary row.
		`CREATE TABLE sync_state (sync_key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`CREATE TABLE reward_account_output (staking_key BLOB NOT NULL, pool_key_hash BLOB NOT NULL, reward_type TEXT NOT NULL, id INTEGER PRIMARY KEY, epoch INTEGER NOT NULL, credential_tag INTEGER NOT NULL DEFAULT 0, amount TEXT NOT NULL, spendable BOOLEAN NOT NULL, guarded BOOLEAN NOT NULL DEFAULT FALSE, captured_slot INTEGER NOT NULL, boundary_slot INTEGER NOT NULL, UNIQUE (epoch, credential_tag, staking_key, pool_key_hash, reward_type))`,
		// The pool certificate tables back DingoDB.GetPoolsRetiredByEpoch.
		// Created unconditionally for the same reason as
		// reward_account_output: they are cheap, independent of the reward
		// tables, and a checkEpoch run reads them for every epoch whether
		// or not the test seeds pool reward rows.
		`CREATE TABLE pool (id INTEGER PRIMARY KEY AUTOINCREMENT, pool_key_hash BLOB NOT NULL UNIQUE)`,
		`CREATE TABLE pool_registration (id INTEGER PRIMARY KEY AUTOINCREMENT, pool_id INTEGER NOT NULL, pool_key_hash BLOB NOT NULL, certificate_id INTEGER, added_slot INTEGER NOT NULL)`,
		`CREATE TABLE pool_retirement (id INTEGER PRIMARY KEY AUTOINCREMENT, pool_id INTEGER NOT NULL, pool_key_hash BLOB NOT NULL, certificate_id INTEGER, epoch INTEGER NOT NULL, added_slot INTEGER NOT NULL)`,
		`CREATE TABLE "transaction" (id INTEGER PRIMARY KEY AUTOINCREMENT, hash BLOB, slot INTEGER, block_index INTEGER)`,
		`CREATE TABLE certs (id INTEGER PRIMARY KEY AUTOINCREMENT, transaction_id INTEGER, slot INTEGER, cert_index INTEGER)`,
	}
	if includePools {
		ret = append(
			ret,
			`CREATE TABLE reward_pool_input (margin TEXT, pool_key_hash BLOB NOT NULL, reward_account BLOB, blocks_produced INTEGER, total_blocks_in_epoch INTEGER, id INTEGER PRIMARY KEY AUTOINCREMENT, epoch INTEGER NOT NULL, pledge TEXT NOT NULL DEFAULT '0', delegated_stake TEXT NOT NULL DEFAULT '0', owner_stake TEXT NOT NULL DEFAULT '0', cost TEXT NOT NULL DEFAULT '0', delegator_count INTEGER NOT NULL DEFAULT 0, reward_account_credential_tag INTEGER NOT NULL DEFAULT 0, captured_slot INTEGER NOT NULL DEFAULT 0, boundary_slot INTEGER NOT NULL DEFAULT 0)`,
			// epoch itself is created unconditionally above (with the
			// richer, nonce-carrying column set every includePools=true
			// test's columns are already a subset of) -- a second
			// CREATE TABLE epoch here duplicated it, failing every
			// includePools=true test with "table epoch already exists".
			`CREATE TABLE tip (hash BLOB, id INTEGER PRIMARY KEY AUTOINCREMENT, slot INTEGER, block_number INTEGER)`,
			`CREATE TABLE reward_pool_output (apparent_performance TEXT, pool_key_hash BLOB NOT NULL, id INTEGER PRIMARY KEY AUTOINCREMENT, epoch INTEGER NOT NULL, optimal_reward TEXT NOT NULL DEFAULT '0', total_reward TEXT NOT NULL DEFAULT '0', leader_reward TEXT NOT NULL DEFAULT '0', member_reward_total TEXT NOT NULL DEFAULT '0', owner_stake TEXT NOT NULL DEFAULT '0', undistributed TEXT NOT NULL DEFAULT '0', unspendable TEXT NOT NULL DEFAULT '0', captured_slot INTEGER NOT NULL DEFAULT 0, boundary_slot INTEGER NOT NULL DEFAULT 0)`,
			// Column set mirrors the production schema
			// (database/plugin/metadata/sqlstore/queries/sqlite/schema.sql)
			// exactly, since testDB.Create's *models.RewardSnapshot case is
			// shared with source_test.go's sourceSQLDB, which seeds the real
			// production schema through the same INSERT statement.
			`CREATE TABLE reward_snapshot (id INTEGER PRIMARY KEY, epoch INTEGER NOT NULL, snapshot_type TEXT NOT NULL, total_active_stake TEXT NOT NULL, total_pool_count INTEGER NOT NULL, total_delegators INTEGER NOT NULL, captured_slot INTEGER NOT NULL, boundary_slot INTEGER NOT NULL, epoch_nonce BLOB, protocol_version INTEGER NOT NULL, authoritative BOOLEAN NOT NULL DEFAULT FALSE, calculation_version INTEGER NOT NULL DEFAULT 0, excluded_active_stake TEXT, UNIQUE (epoch, snapshot_type))`,
		)
	}
	return ret
}

const zeroRewardAddr = "stake_test1uzf5lwsf37wsxmq9rdpq0v9tepk0g36vqmxr974lenzwchcszrsss"

// TestZeroEarnedKoiosRowIsNotDivergence pins that a Koios reward row worth
// zero, with no Dingo counterpart, is agreement rather than acct_only_koios.
//
// The category doc comment asserted that "Koios never emits a row for zero
// reward". Preview disproves it: Koios publishes zero-earned leader rows, and
// Dingo writes no reward_account_output row at all for a zero reward. Nothing
// was credited on either side, so no lovelace differs — but the presence test
// read the row as a reward Dingo had missed and failed epoch 222.
func TestZeroEarnedKoiosRowIsNotDivergence(t *testing.T) {
	t.Parallel()

	now := time.Now()
	out := CompareAccountEpoch(
		"preview", 222,
		[]KoiosAccountRewards{
			{
				StakeAddress: zeroRewardAddr,
				RewardType:   "leader",
				Earned:       "0",
			},
		},
		nil,
		now, 0, time.Time{}, false,
	)
	require.Len(t, out, 1, "the row should still be reported, not dropped")
	assert.Equal(t, CategoryAcctZeroRewardRow, out[0].Category)
	assert.Equal(t, StatusPass, DetermineStatus(out),
		"a zero-earned row must never fail an epoch")
}

// TestZeroAmountDingoRowIsNotDivergence is the mirror. Dingo emits no
// zero-amount rows today, but the two presence branches are deliberately
// symmetric and a future zero row must not fail an epoch for the same reason.
func TestZeroAmountDingoRowIsNotDivergence(t *testing.T) {
	t.Parallel()

	now := time.Now()
	out := CompareAccountEpoch(
		"preview", 222,
		nil,
		[]DingoAccountReward{
			{
				StakeAddress: zeroRewardAddr,
				RewardType:   "leader",
				Amount:       "0",
			},
		},
		now, 0, time.Time{}, false,
	)
	require.Len(t, out, 1)
	assert.Equal(t, CategoryAcctZeroRewardRow, out[0].Category)
	assert.Equal(t, StatusPass, DetermineStatus(out))
}

// TestNonZeroKoiosOnlyRowStillFails is the discrimination check: the change
// must turn off only the zero case, never one-sided rows generally.
func TestNonZeroKoiosOnlyRowStillFails(t *testing.T) {
	t.Parallel()

	now := time.Now()
	out := CompareAccountEpoch(
		"preview", 222,
		[]KoiosAccountRewards{
			{
				StakeAddress: zeroRewardAddr,
				RewardType:   "leader",
				Earned:       "1",
			},
		},
		nil,
		now, 0, time.Time{}, false,
	)
	require.Len(t, out, 1)
	assert.Equal(t, CategoryAcctOnlyKoios, out[0].Category)
	assert.Equal(t, StatusFail, DetermineStatus(out))
}

// A zero-earned row on both sides is an ordinary match and reports nothing —
// the zero handling must not start manufacturing rows for agreeing pairs.
func TestZeroOnBothSidesReportsNothing(t *testing.T) {
	t.Parallel()

	now := time.Now()
	out := CompareAccountEpoch(
		"preview", 222,
		[]KoiosAccountRewards{
			{StakeAddress: zeroRewardAddr, RewardType: "leader", Earned: "0"},
		},
		[]DingoAccountReward{
			{StakeAddress: zeroRewardAddr, RewardType: "leader", Amount: "0"},
		},
		now, 0, time.Time{}, false,
	)
	assert.Empty(t, out)
}

// TestZeroRewardRowAmountSpellings pins both halves of isZeroRewardAmount's
// contract at the level that matters — CompareAccountEpoch's verdict on a
// one-sided row — rather than on the helper in isolation.
//
// Without these, replacing the helper's body with
// `strings.TrimSpace(amount) == "0"` leaves the whole package green: every
// other case in this file spells the amount "0" or "1", so neither "parsed,
// not compared to the literal" nor "an unparseable amount is not zero" is
// pinned. A waived row is the one outcome this category can produce that a
// parity checker must never produce by accident.
func TestZeroRewardRowAmountSpellings(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		earned   string
		category string
		status   string
	}{
		// Zero however it is spelled: the two sides format independently.
		{"zero", "0", CategoryAcctZeroRewardRow, StatusPass},
		{"leading zeros", "00", CategoryAcctZeroRewardRow, StatusPass},
		// Not zero, and not waivable: each of these is malformed data, and a
		// malformed amount is a real value the comparison must keep
		// reporting.
		{"empty", "", CategoryAcctOnlyKoios, StatusFail},
		{"non-numeric", "abc", CategoryAcctOnlyKoios, StatusFail},
		{"negative zero", "-0", CategoryAcctOnlyKoios, StatusFail},
		{"signed zero", "+0", CategoryAcctOnlyKoios, StatusFail},
		{"padded zero", " 0", CategoryAcctOnlyKoios, StatusFail},
		{"nonzero", "1", CategoryAcctOnlyKoios, StatusFail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := CompareAccountEpoch(
				"preview", 222,
				[]KoiosAccountRewards{{
					StakeAddress: zeroRewardAddr,
					RewardType:   "leader",
					Earned:       tc.earned,
				}},
				nil,
				time.Now(), 0, time.Time{}, false,
			)
			require.Len(t, out, 1)
			assert.Equal(t, tc.category, out[0].Category)
			assert.Equal(t, tc.status, DetermineStatus(out))
		})
	}
}

// TestZeroRewardRowAgreesWithValueComparison is the property behind
// parseLovelace: the presence path and the value path must not read the same
// string two different ways.
//
// A spelling isZeroRewardAmount waives on a one-sided row is a spelling
// lovelaceEqual must also call zero when both sides carry it, and one it
// rejects must be rejected there too. Before the shared parse, " 0" was
// agreement one-sided and value_mismatch two-sided, and "+0" was the reverse
// — the same input, two verdicts, depending only on whether the other side
// happened to have a row.
func TestZeroRewardRowAgreesWithValueComparison(t *testing.T) {
	t.Parallel()

	for _, amount := range []string{
		"0", "00", "", "abc", "-0", "+0", " 0", "0 ", "1",
	} {
		t.Run(amount, func(t *testing.T) {
			waivedOneSided := isZeroRewardAmount(amount)
			agreesWithZero := lovelaceEqual(amount, "0")
			assert.Equal(t, waivedOneSided, agreesWithZero,
				"presence and value paths must read %q the same way", amount)
		})
	}
}
