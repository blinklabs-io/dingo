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
	"testing"

	"github.com/stretchr/testify/require"
)

// A snapshot pruned while it is open for signing must be dropped from
// signing: certifying it spends a certificate on a snapshot that no longer
// exists and advances the certificate chain head onto it.
func TestPrunedOpenSnapshotMustNotSpendCertificate(t *testing.T) {
	t.Parallel()
	f := newAggregatorFixture(t, lotteryParams)
	signers := testSigners(4, 100)
	f.registerAll(signers)
	older := f.newSnapshot(2)
	_, stale := f.pending()
	require.NotNil(t, stale)
	f.newSnapshot(3)
	removed, err := PruneSnapshots(context.Background(), f.store, 1)
	require.NoError(t, err)
	require.Equal(t, []string{older.Hash}, removed)

	var before aggregatorState
	found, err := getJSON(context.Background(), f.store, aggregatorStateKey, &before)
	require.NoError(t, err)
	require.True(t, found)
	t.Logf("chain head before: %s", before.LastCertificateHash)

	for _, s := range signers {
		if req, ok := f.signature(s, stale); ok {
			code, body := f.post("/register-signatures", req)
			t.Logf("signature submission -> %d %s", code, body)
		}
	}

	var after aggregatorState
	found, err = getJSON(context.Background(), f.store, aggregatorStateKey, &after)
	require.NoError(t, err)
	require.True(t, found)
	t.Logf("chain head after: %s", after.LastCertificateHash)
	require.Equal(
		t,
		before.LastCertificateHash,
		after.LastCertificateHash,
		"the pruned snapshot spent a certificate and moved the chain head",
	)
}
