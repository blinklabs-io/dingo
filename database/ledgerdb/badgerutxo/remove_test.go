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

package badgerutxo_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

// TestRemoveOutputsLeavesNoJournalRecord pins that RemoveOutputs deletes live
// outputs, keeps the count and sums equal to the live outputs and leaves the
// tip alone, and that a later rollback does not make a removed output live
// again: the removal belongs to no block, so no journal record may hold it.
func TestRemoveOutputsLeavesNoJournalRecord(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := badgerutxo.Open(dir, badgerutxo.WithAggregator(testAgg))
	require.NoError(t, err)
	bootstrap(t, s, point(1, 1), out(1, 0), out(2, 1), out(4, 2))
	tip := point(2, 2)
	_, err = s.Apply(tip, []badgerutxo.Output{out(5, 0)}, nil)
	require.NoError(t, err)

	removed, err := s.RemoveOutputs([]badgerutxo.Ref{
		ref(1, 0), ref(9, 9), ref(2, 1),
	}, func([]byte) bool { return true })
	require.NoError(t, err)
	require.Equal(t, 2, removed, "the output that is not live is not counted")
	requireLive(t, s, false, ref(1, 0))
	requireLive(t, s, false, ref(2, 1))
	requireLive(t, s, true, ref(4, 2))
	requireLive(t, s, true, ref(5, 0))
	require.Len(t, contents(t, s), 2)
	requireSums(t, s)
	requireTip(t, s, tip)

	require.NoError(t, s.Close())
	s, err = badgerutxo.Open(dir, badgerutxo.WithAggregator(testAgg))
	require.NoError(t, err)
	defer s.Close()
	requireLive(t, s, false, ref(1, 0))
	requireLive(t, s, false, ref(2, 1))

	res, err := s.Rollback(point(1, 1))
	require.NoError(t, err)
	require.Equal(t, []badgerutxo.Ref{ref(5, 0)}, res.Removed)
	require.Empty(t, res.Added)
	requireLive(t, s, false, ref(1, 0))
	requireLive(t, s, false, ref(2, 1))
	requireLive(t, s, true, ref(4, 2))
	require.Len(t, contents(t, s), 1)
	requireSums(t, s)
}

// TestRemoveOutputsKeepsRejectedOutput pins that an output whose CBOR the
// predicate rejects stays live and counted.
func TestRemoveOutputsKeepsRejectedOutput(t *testing.T) {
	t.Parallel()
	s, err := badgerutxo.Open(t.TempDir(), badgerutxo.WithAggregator(testAgg))
	require.NoError(t, err)
	defer s.Close()
	bootstrap(t, s, point(1, 1), out(1, 0), out(2, 1), out(4, 2))

	removed, err := s.RemoveOutputs(
		[]badgerutxo.Ref{ref(1, 0), ref(2, 1)},
		func(cbor []byte) bool { return cbor[0] != 2 },
	)
	require.NoError(t, err)
	require.Equal(t, 1, removed)
	requireLive(t, s, false, ref(1, 0))
	requireLive(t, s, true, ref(2, 1))
	requireLive(t, s, true, ref(4, 2))
	require.Len(t, contents(t, s), 2)
	requireSums(t, s)
}
