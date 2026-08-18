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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// TestFetchAccountRewardsForEpochChunksAndCommits proves the address universe
// is split into koiosAccountChunkSize-sized requests and every returned row
// is committed atomically, with coverage marked complete.
func TestFetchAccountRewardsForEpochChunksAndCommits(t *testing.T) {
	var reqCount atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqCount.Add(1)
			var body struct {
				StakeAddresses []string `json:"_stake_addresses"`
				EpochNo        uint64   `json:"_epoch_no"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.EqualValues(t, 100, body.EpochNo)
			var sb strings.Builder
			sb.WriteByte('[')
			for i, addr := range body.StakeAddresses {
				if i > 0 {
					sb.WriteByte(',')
				}
				fmt.Fprintf(
					&sb,
					`{"stake_address":%q,"earned_epoch":100,"amount":"1000000","type":"member"}`,
					addr,
				)
			}
			sb.WriteByte(']')
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(sb.String()))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	// koiosAccountChunkSize addresses + 1 extra to force two chunks/requests.
	addrs := make([]string, koiosAccountChunkSize+1)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("stake1addr%d", i)
	}

	fetched, err := FetchAccountRewardsForEpoch(
		context.Background(),
		k,
		cache,
		"preview",
		100,
		addrs,
		nil,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		len(addrs),
		fetched,
	) // one reward row per requested address
	require.EqualValues(t, 2, reqCount.Load())

	cov, err := cache.GetAccountCoverage("preview", 100)
	require.NoError(t, err)
	require.True(t, cov.Complete)
	require.Equal(t, len(addrs), cov.RequestedCount)
	require.Equal(t, len(addrs), cov.FetchedCount)
}

// TestFetchAccountRewardsForEpochEmptyUniverseCommitsComplete proves an empty
// address universe still commits a complete coverage record rather than
// leaving the epoch perpetually "not fetched".
func TestFetchAccountRewardsForEpochEmptyUniverseCommitsComplete(t *testing.T) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	k := newTestKoiosClient("http://unused.invalid")
	fetched, err := FetchAccountRewardsForEpoch(
		context.Background(),
		k,
		cache,
		"preview",
		100,
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 0, fetched)

	cov, err := cache.GetAccountCoverage("preview", 100)
	require.NoError(t, err)
	require.True(t, cov.Complete)
	require.Equal(t, 0, cov.RequestedCount)
}

// TestFetchAccountRewardsForEpochTransientChunkFailureCommitsNothing proves a
// single failed chunk aborts the whole fetch (nothing committed, coverage
// left absent) rather than committing a partial, silently-"complete" result.
func TestFetchAccountRewardsForEpochTransientChunkFailureCommitsNothing(
	t *testing.T,
) {
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("no server available"))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	_, err = FetchAccountRewardsForEpoch(
		context.Background(),
		k,
		cache,
		"preview",
		100,
		[]string{"stake1a"},
		nil,
	)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrKoiosPermanent))

	_, covErr := cache.GetAccountCoverage("preview", 100)
	require.Error(t, covErr) // no coverage row committed at all
}

// TestFetchAccountRewardsForEpochPermanentErrorAbortsImmediately proves a
// permanent Koios error (daily quota/auth) is returned unwrapped (still
// classified permanent) so callers abort rather than retry.
func TestFetchAccountRewardsForEpochPermanentErrorAbortsImmediately(
	t *testing.T,
) {
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("unauthorized"))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	_, err = FetchAccountRewardsForEpoch(
		context.Background(),
		k,
		cache,
		"preview",
		100,
		[]string{"stake1a"},
		nil,
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrKoiosPermanent))
}

// TestBuildAccountAddressUniverseUnionsKoiosAndDingo proves the address
// universe is the union of Koios's list and Dingo's own committed
// reward_account_output addresses, not either alone — using a real DingoDB
// (RewardParitySource) against the same glebarez/sqlite fixture schema
// dingo_db_test.go/check_test.go use, per this tool's "no local mocks"
// testing convention.
func TestBuildAccountAddressUniverseUnionsKoiosAndDingo(t *testing.T) {
	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck

	stakingKey := testPoolKeyHash(t, 0x33)
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch:         99,
		StakingKey:    stakingKey,
		PoolKeyHash:   testPoolKeyHash(t, 0x22),
		RewardType:    "member",
		CredentialTag: 0,
		Amount:        types.Uint64(1000000),
		Spendable:     true,
	}).Error)

	wantAddr, err := StakeAddressFromCredential(stakingKey, 0)
	require.NoError(t, err)

	koiosAddrs := []string{"stake1koiosonly"}
	universe, err := BuildAccountAddressUniverse(
		context.Background(),
		dingo,
		99,
		koiosAddrs,
	)
	require.NoError(t, err)
	require.Len(t, universe, 2)
	require.Contains(t, universe, "stake1koiosonly")
	require.Contains(t, universe, wantAddr)
}

// TestBuildAccountAddressUniverseNilSourceIsKoiosOnly proves a nil source
// (no Dingo DB access configured) still returns Koios's list alone rather
// than erroring.
func TestBuildAccountAddressUniverseNilSourceIsKoiosOnly(t *testing.T) {
	universe, err := BuildAccountAddressUniverse(
		context.Background(),
		nil,
		99,
		[]string{"stake1a", "stake1b"},
	)
	require.NoError(t, err)
	require.Len(t, universe, 2)
}

func TestChunkAddresses(t *testing.T) {
	chunks := chunkAddresses([]string{"a", "b", "c", "d", "e"}, 2)
	require.Len(t, chunks, 3)
	require.Equal(t, []string{"a", "b"}, chunks[0])
	require.Equal(t, []string{"c", "d"}, chunks[1])
	require.Equal(t, []string{"e"}, chunks[2])
	require.Nil(t, chunkAddresses(nil, 2))
}
