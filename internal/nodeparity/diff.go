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

package nodeparity

import (
	"fmt"
	"sort"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// maxUTxODiffLines caps individual UTxO divergence lines Diff reports, so a
// badly-diverged network with many differing UTxOs still produces readable
// output; TruncatedUTxO records how many were omitted beyond the cap.
const maxUTxODiffLines = 20

// Diff is the result of comparing two Snapshots, broken out by field so a
// caller (a human-readable report, or a Prometheus counter labeled by
// field) can tell which of the three the issue names actually diverged
// without re-parsing message text.
type Diff struct {
	ProtocolParamsDiff string // formatted a-vs-b text; empty when they match
	StakeDistribution  []string
	UTxO               []string
	TruncatedUTxO      int // count of additional UTxO diffs beyond the cap
}

// Empty reports whether the two snapshots matched on every field.
func (d Diff) Empty() bool {
	return d.ProtocolParamsDiff == "" &&
		len(d.StakeDistribution) == 0 &&
		len(d.UTxO) == 0
}

// Count returns the true total number of individual divergences, including
// UTxO entries beyond maxUTxODiffLines that Lines() only reports as a
// single rolled-up summary line -- so a caller reporting "N difference(s)"
// reports the real count rather than the number of printed lines.
func (d Diff) Count() int {
	count := len(d.StakeDistribution) + len(d.UTxO) + d.TruncatedUTxO
	if d.ProtocolParamsDiff != "" {
		count++
	}
	return count
}

// Lines renders the diff as the flat, human-readable report a `check`
// invocation prints or a `watch` cycle logs.
func (d Diff) Lines() []string {
	var lines []string
	if d.ProtocolParamsDiff != "" {
		lines = append(lines, d.ProtocolParamsDiff)
	}
	lines = append(lines, d.StakeDistribution...)
	lines = append(lines, d.UTxO...)
	if d.TruncatedUTxO > 0 {
		lines = append(lines, fmt.Sprintf(
			"... %d more utxo differences omitted", d.TruncatedUTxO,
		))
	}
	return lines
}

// DiffSnapshots compares two Snapshots and returns every divergence found:
// protocol parameter differences (as a single unified-looking JSON diff),
// stake distribution differences per pool, and UTxO set differences per
// output. Diff.Empty() is true when the two snapshots are equal.
func DiffSnapshots(a, b *Snapshot) Diff {
	var d Diff

	if !proto.Equal(a.ProtocolParams, b.ProtocolParams) {
		d.ProtocolParamsDiff = fmt.Sprintf(
			"protocol parameters differ:\n--- a ---\n%s\n--- b ---\n%s",
			protojson.Format(a.ProtocolParams),
			protojson.Format(b.ProtocolParams),
		)
	}

	for _, poolID := range sortedPoolIDs(a.StakeDistribution) {
		aEntry := a.StakeDistribution[poolID]
		bEntry, ok := b.StakeDistribution[poolID]
		switch {
		case !ok:
			d.StakeDistribution = append(d.StakeDistribution, fmt.Sprintf(
				"stake distribution: pool %s present in a, missing in b",
				poolID,
			))
		case aEntry.StakeFraction.Cmp(bEntry.StakeFraction) != 0:
			d.StakeDistribution = append(d.StakeDistribution, fmt.Sprintf(
				"stake distribution: pool %s fraction differs: %s (a) vs %s (b)",
				poolID,
				aEntry.StakeFraction.RatString(),
				bEntry.StakeFraction.RatString(),
			))
		// A pool's registered VRF key is a leader-election input distinct
		// from its stake share: two nodes agreeing on every pool's
		// fraction while disagreeing on a registered VRF key is a real
		// divergence a fraction-only comparison would miss entirely.
		case aEntry.VrfHash != bEntry.VrfHash:
			d.StakeDistribution = append(d.StakeDistribution, fmt.Sprintf(
				"stake distribution: pool %s VRF key differs: %s (a) vs %s (b)",
				poolID,
				aEntry.VrfHash.String(),
				bEntry.VrfHash.String(),
			))
		}
	}
	for _, poolID := range sortedPoolIDs(b.StakeDistribution) {
		if _, ok := a.StakeDistribution[poolID]; !ok {
			d.StakeDistribution = append(d.StakeDistribution, fmt.Sprintf(
				"stake distribution: pool %s present in b, missing in a",
				poolID,
			))
		}
	}

	utxoDiffCount := 0
	report := func(format string, args ...any) {
		utxoDiffCount++
		if utxoDiffCount <= maxUTxODiffLines {
			d.UTxO = append(d.UTxO, fmt.Sprintf(format, args...))
		}
	}
	for _, key := range sortedStringKeys(a.UTxOEntries) {
		aVal := a.UTxOEntries[key]
		bVal, ok := b.UTxOEntries[key]
		switch {
		case !ok:
			report("utxo %s present in a, missing in b: %s", key, aVal)
		case aVal != bVal:
			report("utxo %s differs: %s (a) vs %s (b)", key, aVal, bVal)
		}
	}
	for _, key := range sortedStringKeys(b.UTxOEntries) {
		if _, ok := a.UTxOEntries[key]; !ok {
			report("utxo %s present in b, missing in a: %s", key, b.UTxOEntries[key])
		}
	}
	if utxoDiffCount > maxUTxODiffLines {
		d.TruncatedUTxO = utxoDiffCount - maxUTxODiffLines
	}

	return d
}

// sortedPoolIDs returns m's keys in a stable, deterministic order.
// DiffSnapshots runs against live map iteration order otherwise, which
// would make its report order -- and, past maxUTxODiffLines, which
// specific differences get truncated out of the report -- nondeterministic
// between two runs over the same divergent snapshots.
func sortedPoolIDs[V any](m map[lcommon.PoolId]V) []lcommon.PoolId {
	ids := make([]lcommon.PoolId, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].String() < ids[j].String()
	})
	return ids
}

// sortedStringKeys returns m's keys in a stable, deterministic order; see
// sortedPoolIDs.
func sortedStringKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
