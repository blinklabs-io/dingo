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

package bark

import (
	"bytes"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	archive "github.com/blinklabs-io/bark/proto/v1alpha1/archive"
	archiveconnect "github.com/blinklabs-io/bark/proto/v1alpha1/archive/archivev1alpha1connect"
	"github.com/stretchr/testify/require"
)

func missingBatch(n int) []*archive.BlockRef {
	refs := make([]*archive.BlockRef, 0, n)
	for i := range n {
		refs = append(refs, &archive.BlockRef{
			Hash: new(hex.EncodeToString(
				bytes.Repeat([]byte{byte(i + 1)}, blockHashLength),
			)),
			Slot: new(uint64(5000 + i)),
		})
	}
	return refs
}

// newBudgetedArchiveClient serves handler over HTTP with a frozen budget
// clock. The caller identity is taken from the X-Test-Caller header, which
// the wrapper turns into the connection's remote address.
func newBudgetedArchiveClient(
	t *testing.T,
	handler *archiveServiceHandler,
) func(caller string, refs []*archive.BlockRef) error {
	t.Helper()
	frozen := time.Now()
	handler.bark.archiveBudget.now = func() time.Time { return frozen }
	path, h := archiveconnect.NewArchiveServiceHandler(handler)
	mux := http.NewServeMux()
	mux.Handle(path, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			r.RemoteAddr = r.Header.Get("X-Test-Caller") + ":4000"
			h.ServeHTTP(w, r)
		},
	))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	client := archiveconnect.NewArchiveServiceClient(
		srv.Client(), srv.URL,
	)
	return func(caller string, refs []*archive.BlockRef) error {
		req := connect.NewRequest(&archive.FetchBlockRequest{Blocks: refs})
		req.Header().Set("X-Test-Caller", caller)
		_, err := client.FetchBlock(t.Context(), req)
		return err
	}
}

// TestArchiveFetchBlockCallerAllowanceIsPerCaller shows one caller exhausting
// its own allowance without refusing another, and that a refused request
// reaches the blob store zero times.
func TestArchiveFetchBlockCallerAllowanceIsPerCaller(t *testing.T) {
	t.Parallel()

	counted := make(chan struct{}, 10*DefaultMaxFetchBlockRefs)
	handler, _ := newArchiveTestHandlerWithOptions(
		t, 1, archiveTestOptions{
			onGetBlockURL: func() { counted <- struct{}{} },
		},
	)
	handler.bark.archiveBudget = newArchiveBudget(1000, 1)
	fetch := newBudgetedArchiveClient(t, handler)
	batch := missingBatch(DefaultMaxFetchBlockRefs)

	require.NoError(t, fetch("192.0.2.1", batch))

	require.Len(t, counted, DefaultMaxFetchBlockRefs)

	err := fetch("192.0.2.1", batch)
	require.Error(t, err)
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
	require.Len(t, counted, DefaultMaxFetchBlockRefs, "refused call looked up")

	require.NoError(t, fetch("192.0.2.2", batch))
	require.Len(t, counted, 2*DefaultMaxFetchBlockRefs)
}

// TestArchiveFetchBlockGlobalBudgetBoundsAllCallers shows distinct callers
// sharing one global budget that no single caller's allowance can exceed.
func TestArchiveFetchBlockGlobalBudgetBoundsAllCallers(t *testing.T) {
	t.Parallel()

	handler, _ := newArchiveTestHandlerWithOptions(
		t, 1, archiveTestOptions{},
	)
	handler.bark.archiveBudget = newArchiveBudget(1, 1000)
	fetch := newBudgetedArchiveClient(t, handler)
	batch := missingBatch(DefaultMaxFetchBlockRefs)

	require.NoError(t, fetch("192.0.2.1", batch))
	err := fetch("192.0.2.2", batch)
	require.Error(t, err)
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
}

// TestArchiveBudgetGlobalRefusalKeepsCallerAllowance pins that a request the
// global budget refuses is not charged to the caller.
func TestArchiveBudgetGlobalRefusalKeepsCallerAllowance(t *testing.T) {
	t.Parallel()

	b := newArchiveBudget(1, 1)
	frozen := time.Now()
	b.now = func() time.Time { return frozen }
	n := DefaultMaxFetchBlockRefs

	require.True(t, b.allow("a", n))
	require.False(t, b.allow("b", n), "global budget is spent")
	require.Greater(
		t,
		b.callers["b"].TokensAt(frozen),
		float64(n)-1,
		"refused request drained the caller's allowance",
	)
}

func TestArchiveBudgetBoundsCallerTable(t *testing.T) {
	t.Parallel()

	b := newArchiveBudget(1000000, 1)
	frozen := time.Now()
	b.now = func() time.Time { return frozen }
	for i := range maxArchiveCallers + 10 {
		b.allow(hex.EncodeToString([]byte{byte(i), byte(i >> 8)}), 1)
	}
	require.LessOrEqual(t, len(b.callers), maxArchiveCallers)
}

// TestArchiveBudgetThrottlesCallerTableSweep pins that a full table is scanned
// for evictable entries at most once per archiveCallerSweepInterval, so a
// client rotating caller keys cannot make every request an O(table) scan
// under the budget mutex.
func TestArchiveBudgetThrottlesCallerTableSweep(t *testing.T) {
	t.Parallel()

	b := newArchiveBudget(1000000, 10)
	start := time.Now()
	now := start
	b.now = func() time.Time { return now }
	for i := range maxArchiveCallers {
		require.True(
			t,
			b.allow(hex.EncodeToString([]byte{byte(i), byte(i >> 8)}), 1),
		)
	}
	require.True(t, b.allow("first-new", 1))
	_, ok := b.callers["first-new"]
	require.False(t, ok, "a table of active entries must send it to overflow")

	// Every entry has refilled, but the previous sweep is too recent.
	now = start.Add(archiveCallerSweepInterval / 2)
	require.True(t, b.allow("second-new", 1))
	_, ok = b.callers["second-new"]
	require.False(t, ok, "swept again before the interval elapsed")

	now = start.Add(archiveCallerSweepInterval)
	require.True(t, b.allow("third-new", 1))
	_, ok = b.callers["third-new"]
	require.True(t, ok, "idle entries were not evicted once the interval elapsed")
}

func TestNewBarkRejectsNegativeArchiveLookupRates(t *testing.T) {
	t.Parallel()

	db := newSigningTestDB(t, archiveTestOptions{})
	_, err := NewBark(BarkConfig{DB: db, ArchiveLookupsPerSecond: -1})
	require.ErrorContains(t, err, "ArchiveLookupsPerSecond")
	_, err = NewBark(BarkConfig{DB: db, ArchiveCallerLookupsPerSecond: -1})
	require.ErrorContains(t, err, "ArchiveCallerLookupsPerSecond")
}

// TestArchiveCallerKey pins how a connection's remote address maps to an
// allowance: the port never matters, an IPv6 caller is keyed by its /64 so
// one subscriber prefix cannot mint an allowance per address, and an address
// that does not parse still maps to a single bounded entry.
func TestArchiveCallerKey(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, a, b string
		same       bool
	}{
		{"ipv4 port ignored", "192.0.2.1:1", "192.0.2.1:2", true},
		{"ipv4 hosts differ", "192.0.2.1:1", "192.0.2.2:1", false},
		{"ipv6 same /64", "[2001:db8:1:2::1]:1", "[2001:db8:1:2:ffff::9]:2", true},
		{"ipv6 other /64", "[2001:db8:1:2::1]:1", "[2001:db8:1:3::1]:1", false},
		{"ipv6 zone ignored", "[fe80::1%eth0]:1", "[fe80::2%eth1]:1", true},
		{"ipv4-mapped is ipv4", "[::ffff:192.0.2.1]:1", "192.0.2.1:1", true},
		{"ipv4-mapped hosts differ", "[::ffff:192.0.2.1]:1", "192.0.2.2:1", false},
		{"unparseable is stable", "not-an-address", "not-an-address", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ka, kb := archiveCallerKey(tc.a), archiveCallerKey(tc.b)
			if tc.same {
				require.Equal(t, ka, kb)
			} else {
				require.NotEqual(t, ka, kb)
			}
		})
	}
}
