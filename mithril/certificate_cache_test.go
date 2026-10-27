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
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// certificateRequests returns the certificate hashes among paths.
func certificateRequests(paths []string) []string {
	var hashes []string
	for _, p := range paths {
		if hash, ok := strings.CutPrefix(p, "/certificate/"); ok {
			hashes = append(hashes, hash)
		}
	}
	return hashes
}

// stakeDistributionRequests counts the stake distribution requests in paths.
func stakeDistributionRequests(paths []string) int {
	n := 0
	for _, p := range paths {
		if strings.Contains(p, "stake-distribution") {
			n++
		}
	}
	return n
}

func TestLatestCertifiedTargetReusesVerifiedCertificates(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 2, nil)
	cfg := fixture.bootstrapConfig(t.TempDir())
	cfg.Logger = slog.New(slog.DiscardHandler)
	cfg.CertificateCache = NewCertificateCache()
	genesis := fixture.certs[fixture.leafHash].PreviousHash
	firstLeaf := fixture.leafHash

	target, err := LatestCertifiedTarget(t.Context(), cfg)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), target.Immutable)
	assert.Equal(t, []string{firstLeaf, genesis},
		certificateRequests(fixture.requestedPaths()),
		"the first poll walks the chain to genesis")

	// Same leaf: nothing to verify, so only the artifact is fetched.
	_, err = LatestCertifiedTarget(t.Context(), cfg)
	require.NoError(t, err)
	paths := fixture.requestedPaths()
	assert.Empty(t, certificateRequests(paths), "unchanged leaf re-walked")
	assert.Zero(t, stakeDistributionRequests(paths))

	// A new leaf: only it is fetched, and the walk stops at the leaf the
	// cache already verified.
	nextLeaf := fixture.publishNextLeaf(t)
	_, err = LatestCertifiedTarget(t.Context(), cfg)
	require.NoError(t, err)
	assert.Equal(t, []string{nextLeaf},
		certificateRequests(fixture.requestedPaths()),
		"only the new certificate is requested")

	// Without a cache every poll walks to genesis.
	cfg.CertificateCache = nil
	_, err = LatestCertifiedTarget(t.Context(), cfg)
	require.NoError(t, err)
	assert.Equal(t, []string{nextLeaf, firstLeaf, genesis},
		certificateRequests(fixture.requestedPaths()))
}

func TestCertificateCacheChecksLinkToAnchor(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 2, nil)
	cfg := fixture.bootstrapConfig(t.TempDir())
	cfg.Logger = slog.New(slog.DiscardHandler)
	cfg.CertificateCache = NewCertificateCache()
	_, err := LatestCertifiedTarget(t.Context(), cfg)
	require.NoError(t, err)

	// A new leaf in the same epoch signed under another AVK does not chain
	// to the verified one, however valid its own signature.
	prevKey := fixture.stmKey
	fixture.stmKey = testSTMSigningKey(t)
	fixture.publishNextLeafUnchecked(t)
	fixture.stmKey = prevKey
	_, err = LatestCertifiedTarget(t.Context(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(),
		"aggregate verification key mismatch within epoch")
}

func TestCertificateCacheIgnoredUnderAnotherGenesisKey(t *testing.T) {
	t.Parallel()
	fixture := newFeedFixture(t, 2, nil)
	cache := NewCertificateCache()
	cfg := fixture.bootstrapConfig(t.TempDir())
	cfg.Logger = slog.New(slog.DiscardHandler)
	cfg.CertificateCache = cache
	_, err := LatestCertifiedTarget(t.Context(), cfg)
	require.NoError(t, err)
	fixture.requestedPaths()

	// The same certificates under another trust anchor are verified again,
	// and fail against it.
	other := newFeedFixture(t, 2, nil)
	cfg.GenesisVerificationKey = other.genesisVKey
	_, err = LatestCertifiedTarget(t.Context(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "genesis certificate verification failed")
	assert.NotEmpty(t, certificateRequests(fixture.requestedPaths()))
}
