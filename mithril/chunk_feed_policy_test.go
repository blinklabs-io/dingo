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
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A feed that is not a bootstrap's own download fetches through a client that
// holds redirects to the artifact location policy and charges every request
// to a budget, and it drops the cross-archive extraction budget its window
// makes unnecessary.
func TestChunkFeedDownloadsThroughArtifactPolicyClient(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 3, nil)
	cfg := feedConfig(fixture, t.TempDir(), 0, 2, 2)
	extraction, err := newBootstrapExtractionBudget(t.TempDir())
	require.NoError(t, err)
	cfg.Bootstrap.extractionBudget = extraction
	feed := openFeed(t, t.Context(), cfg)

	require.NotNil(t, feed.cfg.httpClient)
	assert.NotNil(t, feed.cfg.httpClient.CheckRedirect,
		"feed downloads must follow only redirects the location policy admits")
	require.NotNil(t, feed.cfg.downloadBudget)
	assert.Nil(t, feed.cfg.extractionBudget,
		"a windowed feed must not carry a running extraction total")

	for {
		chunk, err := feed.Next(nextCtx(t))
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		require.NoError(t, feed.Release(chunk.Num))
	}
	// The digest list plus four immutable archives.
	assert.GreaterOrEqual(t, feed.cfg.downloadBudget.requests.Load(), int64(5))
}

// A kept feed is a bootstrap's immutable download and must use the client the
// bootstrap built, so its archives stay under the bootstrap's redirect policy
// and download budget.
func TestKeptChunkFeedSharesBootstrapClient(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 1, nil)
	dir := t.TempDir()
	bcfg, url, err := prepareFeedConfig(feedConfig(fixture, dir, 0, 0, 1).Bootstrap)
	require.NoError(t, err)
	client := newMithrilClient(url, bcfg.AllowInsecureHTTP)
	artifact, err := selectVerifiedArtifact(t.Context(), bcfg, client)
	require.NoError(t, err)
	digests, err := fetchVerifiedDigests(t.Context(), client, bcfg, artifact, dir)
	require.NoError(t, err)
	budget, err := newArtifactDownloadBudget(8, 1<<30, 1<<20, 0)
	require.NoError(t, err)
	shared, transport, err := newArtifactDownloadClient(
		url, bcfg.AllowInsecureHTTP, 1, budget,
	)
	require.NoError(t, err)
	t.Cleanup(transport.CloseIdleConnections)
	bcfg.httpClient = shared
	bcfg.downloadBudget = budget

	feed, err := newVerifiedChunkFeed(t.Context(), ChunkFeedConfig{
		Bootstrap: bcfg,
		keep: &feedKeep{
			extractDir: filepath.Join(dir, "extract"),
			archiveDir: filepath.Join(dir, "archives"),
		},
	}, bcfg, client, artifact, digests)
	require.NoError(t, err)
	t.Cleanup(func() { _ = feed.Close() })
	assert.Same(t, shared, feed.cfg.httpClient)
	for {
		_, err := feed.Next(nextCtx(t))
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}
	assert.GreaterOrEqual(t, budget.requests.Load(), int64(2),
		"the kept feed's archives must be charged to the bootstrap budget")
}

// The digest list and ancillary archive of a feed-based run share one client
// held to the location policy, a budget sized for those two objects, and an
// extraction budget.
func TestOneShotDownloadClient(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 1, nil)
	dir := t.TempDir()
	bcfg, url, err := prepareFeedConfig(feedConfig(fixture, dir, 0, 0, 1).Bootstrap)
	require.NoError(t, err)
	dcfg, closeClient, err := withOneShotDownloadClient(bcfg, url, dir, true)
	require.NoError(t, err)
	t.Cleanup(closeClient)
	require.NotNil(t, dcfg.httpClient)
	assert.NotNil(t, dcfg.httpClient.CheckRedirect)
	assert.NotNil(t, dcfg.extractionBudget)
	require.NotNil(t, dcfg.downloadBudget)
	assert.Equal(t, int64(2*artifactRequestsPerObject), dcfg.downloadBudget.maxRequests)
	assert.Nil(t, bcfg.httpClient, "the caller's config must stay unchanged")

	client := newMithrilClient(url, bcfg.AllowInsecureHTTP)
	artifact, err := selectVerifiedArtifact(t.Context(), bcfg, client)
	require.NoError(t, err)
	_, err = fetchVerifiedDigests(t.Context(), client, dcfg, artifact, dir)
	require.NoError(t, err)
	assert.Equal(t, int64(1), dcfg.downloadBudget.requests.Load(),
		"the digest list must be fetched through the one-shot client")
}
