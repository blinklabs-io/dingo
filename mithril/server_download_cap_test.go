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
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

// blockingOpenStore holds every artifact open until it is released.
type blockingOpenStore struct {
	ArtifactStore
	entered chan struct{}
	release chan struct{}
}

func (s *blockingOpenStore) Open(
	ctx context.Context,
	_ string,
) (io.ReadSeekCloser, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return nil, ErrArtifactNotFound
}

// The per-client download cap is claimed to answer 429 beyond one
// bootstrap's download concurrency from one host.
func TestArtifactDownloadCapAnswers429(t *testing.T) {
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
	request := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(
			http.MethodGet, "/download/"+hash+"/00000.tar.zst", nil,
		)
		req.RemoteAddr = "10.0.0.7:4444"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	for range artifactDownloadsPerClient {
		go func() { request() }()
	}
	for range artifactDownloadsPerClient {
		testutil.RequireReceive(
			t, store.entered, testutil.AsyncWait, "admitted download",
		)
	}
	rec := request()
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
}
