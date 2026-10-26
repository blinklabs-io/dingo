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
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The claimed rule is a 30-second transfer grace plus a 64 KiB/s minimum
// write rate: with 20 seconds already spent, one 64 KiB write may not be
// given the plain 15-second idle deadline.
func TestArtifactTransferRateDeadline(t *testing.T) {
	t.Parallel()

	rec := &deadlineResponseWriter{header: make(http.Header)}
	started := time.Now().Add(-20 * time.Second)
	w := &progressResponseWriter{
		ResponseWriter:        rec,
		controller:            http.NewResponseController(rec),
		timeout:               15 * time.Second,
		minimumBytesPerSecond: artifactMinimumBytesPerSecond,
		transferStarted:       started,
		transferGrace:         artifactTransferGrace,
	}
	n, err := w.Write(make([]byte, 64<<10))
	require.NoError(t, err)
	require.Equal(t, 64<<10, n)
	require.Len(t, rec.deadlines, 1)
	// transferStarted + 30s grace + 1s for 64 KiB at 64 KiB/s.
	want := started.Add(31 * time.Second)
	require.WithinDuration(
		t, want, rec.deadlines[0], 2*time.Second,
	)
	// The idle deadline alone would have been 15s from now, later than the
	// transfer deadline, so the rate bound is the binding one.
	require.True(t, rec.deadlines[0].Before(time.Now().Add(15*time.Second)))
}

// The download route must arm the rate deadline itself: with a one-minute
// idle timeout, a download's write deadline is bounded by the transfer start
// plus the grace plus its bytes at the minimum rate.
func TestDownloadRouteArmsTransferRateDeadline(t *testing.T) {
	t.Parallel()

	store, _ := newLocalStore(t)
	hash := strings.Repeat("a", 64)
	const size = 2 * artifactMinimumBytesPerSecond
	require.NoError(t, store.Put(
		context.Background(),
		hash+"/00000.tar.zst",
		bytes.NewReader(make([]byte, size)),
	))
	handler := newServerHandler(
		ServerConfig{Store: store, PublicBaseURL: "http://127.0.0.1"},
		publicRequestLimit,
		time.Minute,
	)
	rec := &deadlineResponseWriter{header: make(http.Header)}
	started := time.Now()
	handler.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/download/"+hash+"/00000.tar.zst", nil,
	))
	require.Equal(t, size, rec.written)
	require.NotEmpty(t, rec.deadlines)
	// The idle deadline alone would be a minute after the last write.
	want := started.Add(artifactTransferGrace + 2*time.Second)
	require.WithinDuration(
		t, want, rec.deadlines[len(rec.deadlines)-1], 5*time.Second,
	)
}
