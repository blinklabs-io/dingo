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

// pruneNonceCount is above the entries one transaction holds at the 8 MiB
// memtable the store opens with here (about 13,000), as the nonces an epoch
// rollover prunes are above the 104,857 a default memtable holds.
const pruneNonceCount = 15000

// seedNonces writes count non-checkpoint nonces at slots 1..count through
// batches that fit one transaction, with a checkpoint at every 5000th slot,
// and returns the slot after the last.
func seedNonces(t *testing.T, s *badgerutxo.Store, count int) uint64 {
	t.Helper()
	const perBatch = 2500
	for first := 1; first <= count; first += perBatch {
		var ns []badgerutxo.Nonce
		for slot := first; slot < min(first+perBatch, count+1); slot++ {
			ns = append(ns, nonce(uint64(slot), 0x77, slot%5000 == 0))
		}
		applyNonces(t, s, point(uint64(count), 0x77), 0, ns...)
	}
	return uint64(count) + 1
}

func countNonces(t *testing.T, s *badgerutxo.Store) (checkpoints, others int) {
	t.Helper()
	all, err := s.NoncesInSlotRange(0, ^uint64(0))
	require.NoError(t, err)
	for _, n := range all {
		if n.Checkpoint {
			checkpoints++
		} else {
			others++
		}
	}
	return checkpoints, others
}

// A prune of more nonces than one transaction holds is a batch of nothing
// else, as the epoch rollover commits it: no tip, change or row.
func TestPruneNoncesOverOneTransaction(t *testing.T) {
	t.Parallel()
	s, err := badgerutxo.Open(
		t.TempDir(), badgerutxo.WithMemTableSize(8<<20),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	next := seedNonces(t, s, pruneNonceCount)
	checkpoints, others := countNonces(t, s)
	require.Equal(t, pruneNonceCount/5000, checkpoints)
	require.Equal(t, pruneNonceCount-checkpoints, others)

	_, err = s.ApplyLedgerBatch(
		nil, badgerutxo.LedgerBatch{PruneNoncesBefore: next},
	)
	require.NoError(t, err)

	checkpoints, others = countNonces(t, s)
	require.Equal(t, pruneNonceCount/5000, checkpoints,
		"checkpoint nonces survive pruning")
	require.Zero(t, others, "every non-checkpoint nonce before the cutoff goes")
}

// The batch's own nonces before the cutoff are pruned with the rest, as when
// the prune shares its transaction with them, and those at or after it stay.
func TestPruneNoncesOverOneTransactionWithBatchNonces(t *testing.T) {
	t.Parallel()
	s, err := badgerutxo.Open(
		t.TempDir(), badgerutxo.WithMemTableSize(8<<20),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	next := seedNonces(t, s, pruneNonceCount)

	tip := point(next+1, 0x78)
	_, err = s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{
		Nonces: []badgerutxo.Nonce{
			nonce(next-1, 0x78, false),
			nonce(next+1, 0x78, false),
		},
		PruneNoncesBefore: next,
	})
	require.NoError(t, err)

	_, others := countNonces(t, s)
	require.Equal(t, 1, others)
	_, ok, err := s.Nonce(point(next+1, 0x78))
	require.NoError(t, err)
	require.True(t, ok, "a nonce at the cutoff or after stays")
	_, ok, err = s.Nonce(point(next-1, 0x78))
	require.NoError(t, err)
	require.False(t, ok, "the batch's own nonce before the cutoff is pruned")
}
