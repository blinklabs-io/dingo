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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedactLocationURI(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		{
			name: "strips a pre-signed query",
			in: "https://cdn.example.org/mainnet/immutable/05471.tar.zst" +
				"?X-Amz-Credential=AKIAEXAMPLE&X-Amz-Signature=deadbeef",
			want: "https://cdn.example.org/mainnet/immutable/05471.tar.zst",
		},
		{
			name: "strips userinfo",
			in:   "https://user:secret@cdn.example.org/a/05471.tar.zst",
			want: "https://cdn.example.org/a/05471.tar.zst",
		},
		{
			name: "keeps scheme host and path",
			in:   "https://cdn.example.org/a/05471.tar.zst",
			want: "https://cdn.example.org/a/05471.tar.zst",
		},
		{
			name: "unparsable input keeps nothing after the query marker",
			in:   "::not a url::?token=secret",
			want: "::not a url::",
		},
		{
			name: "empty input",
			in:   "",
			want: "unparsable location",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := redactLocationURI(tc.in)
			assert.Equal(t, tc.want, got)
			assert.NotContains(t, got, "secret")
			assert.NotContains(t, got, "deadbeef")
		})
	}
}

// TestBootstrapV2ReportsIdenticalBadBytesAcrossLocations covers the reported
// failure mode: every published location serves the same archive and its bytes
// do not match the artifact's certified digest list.
//
// The refusal is the correct outcome and is asserted first — the digest list is
// merkle-verified against the certificate before any archive is fetched, so
// bytes that disagree with it are never imported. What the test pins is that
// the refusal carries the evidence an operator needs: the artifact hash and
// beacon, the immutable file, both digests, each location that was tried, and
// the finding that the locations agreed with each other and disagreed with the
// certificate, which is what separates a mis-published archive from a stale
// replica.
func TestBootstrapV2ReportsIdenticalBadBytesAcrossLocations(t *testing.T) {
	tampered := uint64(1)
	fixture := newV2Fixture(t, v2FixtureOptions{
		immutableFileNumber:  2,
		tamperImmutable:      &tampered,
		immutableBadMirror:   true,
		signedImmutableQuery: true,
	})

	_, err := Bootstrap(
		context.Background(),
		fixture.bootstrapConfig(t.TempDir()),
	)
	require.Error(t, err, "a mis-certified archive must still be refused")

	var archiveErr *ImmutableArchiveError
	require.ErrorAs(t, err, &archiveErr)
	assert.Equal(t, fixture.artifact.Hash, archiveErr.ArtifactHash)
	assert.Equal(t, fixture.artifact.Beacon.Epoch, archiveErr.Epoch)
	assert.Equal(t, tampered, archiveErr.ImmutableFileNumber)
	assert.Equal(t, 2, archiveErr.Locations)

	require.Len(t, archiveErr.Attempts, 2,
		"both published locations must be recorded")
	var observed string
	for i, attempt := range archiveErr.Attempts {
		assert.Equal(t, i+1, attempt.Location)
		mismatch := attempt.Mismatch()
		require.NotNil(t, mismatch, "attempt %d", i+1)
		assert.Equal(
			t, fmt.Sprintf("%05d.chunk", tampered), mismatch.FileName,
		)
		assert.NotEmpty(t, mismatch.Expected)
		assert.NotEqual(t, mismatch.Expected, mismatch.Observed)
		if i == 0 {
			observed = mismatch.Observed
			continue
		}
		assert.Equal(t, observed, mismatch.Observed,
			"both locations served the same bytes in this fixture")
	}

	msg := err.Error()
	assert.Contains(t, msg, fixture.artifact.Hash)
	assert.Contains(t, msg, "all 2 published locations served identical bytes")
	assert.Contains(t, msg, observed)
	assert.Contains(t, msg, "/files/imm-bad/00001.tar.zst")
	assert.Contains(t, msg, "/files/imm/00001.tar.zst")
	// Cloud-storage locations are pre-signed; the credentials in the query
	// string must not travel in an error an operator is asked to report.
	assert.NotContains(t, msg, "X-Amz-Signature")
	assert.NotContains(t, msg, "AKIAFIXTURE")
}

// TestBootstrapV2ReportsSingleLocationMismatchWithoutBlamingReplicas is the
// counterpart: with only one published location there is nothing to compare, so
// the "every location agreed" finding must not be claimed. The per-location
// evidence is still recorded.
func TestBootstrapV2ReportsSingleLocationMismatchWithoutBlamingReplicas(
	t *testing.T,
) {
	tampered := uint64(0)
	fixture := newV2Fixture(t, v2FixtureOptions{
		immutableFileNumber: 1,
		tamperImmutable:     &tampered,
	})

	_, err := Bootstrap(
		context.Background(),
		fixture.bootstrapConfig(t.TempDir()),
	)
	require.Error(t, err)

	var archiveErr *ImmutableArchiveError
	require.ErrorAs(t, err, &archiveErr)
	assert.Equal(t, 1, archiveErr.Locations)
	require.Len(t, archiveErr.Attempts, 1)
	require.NotNil(t, archiveErr.Attempts[0].Mismatch())
	assert.NotContains(t, err.Error(), "served identical bytes")
}

// TestBootstrapV2AttributesACorruptCacheToTheCache covers a trio left extracted
// by an earlier run whose bytes do not match this artifact's certified digest
// list. The rejection must be attributed to the local cache rather than to a
// published location, and the trio must be gone afterwards so no later run can
// reuse it.
func TestBootstrapV2AttributesACorruptCacheToTheCache(t *testing.T) {
	tampered := uint64(1)
	fixture := newV2Fixture(t, v2FixtureOptions{
		immutableFileNumber: 2,
		tamperImmutable:     &tampered,
		immutableBadMirror:  true,
	})
	downloadDir := t.TempDir()
	immutableDir := filepath.Join(
		downloadDir, "immutable-"+fixture.artifact.Hash, "immutable",
	)
	require.NoError(t, os.MkdirAll(immutableDir, 0o750))
	for _, ext := range immutableFileExtensions {
		require.NoError(t, os.WriteFile(
			filepath.Join(
				immutableDir, fmt.Sprintf("%05d.%s", tampered, ext),
			),
			[]byte("stale bytes from an earlier artifact"),
			0o640,
		))
	}

	_, err := Bootstrap(
		context.Background(),
		fixture.bootstrapConfig(downloadDir),
	)
	require.Error(t, err)

	var archiveErr *ImmutableArchiveError
	require.ErrorAs(t, err, &archiveErr)
	require.Len(t, archiveErr.Attempts, 3,
		"the cache and both locations must each be recorded")
	assert.Equal(t, 0, archiveErr.Attempts[0].Location)
	assert.Equal(t, immutableSourceCache, archiveErr.Attempts[0].Source)
	cacheMismatch := archiveErr.Attempts[0].Mismatch()
	require.NotNil(t, cacheMismatch)
	assert.Equal(
		t, fmt.Sprintf("%05d.chunk", tampered), cacheMismatch.FileName,
	)
	// The cache's bytes are its own, so they must not be counted as evidence
	// about what the replicas served.
	locationObserved := archiveErr.Attempts[1].Mismatch()
	require.NotNil(t, locationObserved)
	assert.NotEqual(t, cacheMismatch.Observed, locationObserved.Observed)
	assert.Contains(
		t, err.Error(), "all 2 published locations served identical bytes",
	)

	for _, ext := range immutableFileExtensions {
		_, statErr := os.Stat(filepath.Join(
			immutableDir, fmt.Sprintf("%05d.%s", tampered, ext),
		))
		assert.True(t, errors.Is(statErr, os.ErrNotExist),
			"a rejected trio must not be left for a later run to reuse")
	}
}
