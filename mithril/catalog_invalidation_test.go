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
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// The public listing is served from the server's own catalog, which is a
// different instance from the aggregator's catalog. Certification invalidates
// only the aggregator's, so a newly certified snapshot stays unlisted until
// the server's cached view expires.
func TestCertifiedSnapshotListedAfterCertification(
	t *testing.T,
) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	signers := testSigners(4, 100)
	snap := f.newSnapshot(2)

	// Populates the server's catalog with the uncertified snapshot.
	resp := get(t, f.url+"/artifact/cardano-database")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var before []CardanoDatabaseSnapshotListItem
	require.NoError(t, json.Unmarshal(getBody(t, resp), &before))
	require.Empty(t, before, "uncertified snapshots are not listed")

	f.registerAll(signers)
	certHash := f.signUntilCertified(snap.Hash, signers)
	require.NotEmpty(t, certHash)

	resp = get(t, f.url+"/artifact/cardano-database")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var after []CardanoDatabaseSnapshotListItem
	require.NoError(t, json.Unmarshal(getBody(t, resp), &after))
	require.Len(t, after, 1, "the certified snapshot must be listed")
	require.Equal(t, snap.Hash, after[0].Hash)
}
