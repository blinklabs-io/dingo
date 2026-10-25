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

package mithril

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

// An IPv6 client rotating through its /64 must not gain a fresh download
// quota per address.
func TestArtifactDownloadCapSharedAcrossIPv6Prefix(t *testing.T) {
	t.Parallel()

	store := &blockingOpenStore{
		entered: make(chan struct{}, artifactDownloadsPerClient+8),
		release: make(chan struct{}),
	}
	t.Cleanup(func() { close(store.release) })
	handler := newServerHandler(
		ServerConfig{Store: store, PublicBaseURL: "http://127.0.0.1"},
		publicRequestLimit,
		time.Minute,
	)
	hash := strings.Repeat("a", 64)
	request := func(remoteAddr string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(
			http.MethodGet, "/download/"+hash+"/00000.tar.zst", nil,
		)
		req.RemoteAddr = remoteAddr
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	for i := range artifactDownloadsPerClient {
		addr := fmt.Sprintf("[2001:db8:0:1::%x]:4444", i+1)
		go func() { request(addr) }()
	}
	for range artifactDownloadsPerClient {
		testutil.RequireReceive(
			t, store.entered, testutil.AsyncWait, "admitted download",
		)
	}
	// A request the cap fails to reject blocks in the store, so wait for it
	// rather than calling it inline.
	codes := make(chan *httptest.ResponseRecorder, 1)
	go func() { codes <- request("[2001:db8:0:1:ffff::1]:5555") }()
	rec := testutil.RequireReceive(
		t, codes, 5*time.Second, "rejected download",
	)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
}

func TestArtifactDownloadClientKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a, b string
		same bool
	}{
		{"same IPv6 /64", "[2001:db8::1]:1", "[2001:db8::ffff:2]:2", true},
		{"different IPv6 /64", "[2001:db8::1]:1", "[2001:db8:0:1::1]:1", false},
		{"IPv4-mapped IPv4", "[::ffff:10.0.0.7]:1", "10.0.0.7:2", true},
		{"distinct IPv4", "10.0.0.7:1", "10.0.0.8:1", false},
		{"zoned link-local", "[fe80::1%eth0]:1", "[fe80::2]:1", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			admission := newClientAdmission(1)
			release, ok := admission.acquire(tc.a)
			require.True(t, ok)
			defer release()
			second, ok := admission.acquire(tc.b)
			if ok {
				second()
			}
			assert.Equal(t, tc.same, !ok)
		})
	}
}
