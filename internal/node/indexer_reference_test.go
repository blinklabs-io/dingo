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

package node

import (
	"context"
	"io"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

// The reference tables are what an IndexerDB indexed from genesis holds over
// the fixture chains. They depend only on the immutable test data, so each is
// computed once per test binary instead of once per test.

// anchorReferenceTables is the reference of the anchor fixture.
type anchorReferenceTables struct {
	atAnchor  map[string][]string
	atTip     map[string][]string
	anchorTip ochainsync.Tip
}

var (
	anchorReferenceOnce   sync.Once
	anchorReferenceBuilds atomic.Int32
	anchorReferenceResult *anchorReferenceTables

	chainReferenceOnce   sync.Once
	chainReferenceBuilds atomic.Int32
	chainReferenceResult map[string][]string
)

// cloneTables copies the tables so a test may edit its view of the reference
// without another test observing the edit.
func cloneTables(in map[string][]string) map[string][]string {
	out := make(map[string][]string, len(in))
	for name, rows := range in {
		out[name] = slices.Clone(rows)
	}
	return out
}

// anchorReference returns the tables of an IndexerDB indexed from genesis at
// the anchor and at the end of the fixture chain, and the anchor's tip.
func anchorReference(t *testing.T) *anchorReferenceTables {
	t.Helper()
	anchorReferenceOnce.Do(func() {
		anchorReferenceResult = buildAnchorReference(t)
	})
	require.NotNil(
		t,
		anchorReferenceResult,
		"the reference build failed in the test that ran it",
	)
	return &anchorReferenceTables{
		atAnchor:  cloneTables(anchorReferenceResult.atAnchor),
		atTip:     cloneTables(anchorReferenceResult.atTip),
		anchorTip: anchorReferenceResult.anchorTip,
	}
}

func buildAnchorReference(t *testing.T) *anchorReferenceTables {
	t.Helper()
	anchorReferenceBuilds.Add(1)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	out := &anchorReferenceTables{}

	// A ledger database loaded to the anchor and indexed from genesis, read
	// there and again at the end of the chain.
	ref := newEquivalenceDB(t)
	require.NoError(t, LoadWithDB(
		ctx, equivalenceLoadConfig(), logger,
		copyImmutableChunkPrefix(t, anchorChunks), ref,
	))
	var err error
	out.anchorTip, err = ref.GetTip(nil)
	require.NoError(t, err)
	refLedger, closeRefLedger := openFactLedger(t, ref)
	_, refRaw := indexFromGenesis(t, ref, refLedger, nil)
	out.atAnchor = dumpTables(t, refRaw)
	closeRefLedger()
	require.NoError(t, LoadWithDB(
		ctx, equivalenceLoadConfig(), logger,
		copyImmutableChunkPrefix(t, chainChunks), ref,
	))
	// A second index from genesis, not the first one followed to the tip:
	// the rows of an epoch whose reward round completed past the anchor are
	// then the final ones.
	refLedger, _ = openFactLedger(t, ref)
	_, refRaw = indexFromGenesis(t, ref, refLedger, nil)
	out.atTip = dumpTables(t, refRaw)
	for _, name := range anchorSkipTables {
		delete(out.atAnchor, name)
		delete(out.atTip, name)
	}
	return out
}

// chainReference returns the tables of an IndexerDB indexed from genesis over
// the equivalenceChunks chain.
func chainReference(t *testing.T) map[string][]string {
	t.Helper()
	chainReferenceOnce.Do(func() {
		chainReferenceResult = buildChainReference(t)
	})
	require.NotNil(
		t,
		chainReferenceResult,
		"the reference build failed in the test that ran it",
	)
	return cloneTables(chainReferenceResult)
}

func buildChainReference(t *testing.T) map[string][]string {
	t.Helper()
	chainReferenceBuilds.Add(1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	consensus := newEquivalenceDB(t)
	require.NoError(t, LoadWithDB(
		context.Background(), equivalenceLoadConfig(), logger,
		copyImmutableChunkPrefix(t, equivalenceChunks), consensus,
	))
	ls, _ := openFactLedger(t, consensus)
	_, raw := indexFromGenesis(t, consensus, ls, nil)
	return dumpTables(t, raw)
}

// Each reference is built once per test binary however many tests ask for it,
// and a caller's edits to its copy do not reach the next caller.
func TestIndexerReferencesAreBuiltOncePerBinary(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		get    func(*testing.T) map[string][]string
		builds *atomic.Int32
	}{
		"anchor": {
			get: func(t *testing.T) map[string][]string {
				return anchorReference(t).atTip
			},
			builds: &anchorReferenceBuilds,
		},
		"chain": {get: chainReference, builds: &chainReferenceBuilds},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			first := tc.get(t)
			require.NotEmpty(t, first)
			table := slices.Sorted(maps.Keys(first))[0]
			rows := first[table]
			delete(first, table)
			if len(rows) > 0 {
				rows[0] += " edited"
			}

			second := tc.get(t)
			require.Contains(t, second, table, "an edit reached the next caller")
			if len(rows) > 0 {
				require.NotEqual(t, rows[0], second[table][0])
			}
			require.Equal(
				t,
				int32(1),
				tc.builds.Load(),
				"the reference was rebuilt",
			)
		})
	}
}
