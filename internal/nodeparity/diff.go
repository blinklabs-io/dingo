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

	for poolID, aFrac := range a.StakeDistribution {
		bFrac, ok := b.StakeDistribution[poolID]
		switch {
		case !ok:
			d.StakeDistribution = append(d.StakeDistribution, fmt.Sprintf(
				"stake distribution: pool %s present in a, missing in b",
				poolID,
			))
		case aFrac.Cmp(bFrac) != 0:
			d.StakeDistribution = append(d.StakeDistribution, fmt.Sprintf(
				"stake distribution: pool %s fraction differs: %s (a) vs %s (b)",
				poolID,
				aFrac.RatString(),
				bFrac.RatString(),
			))
		}
	}
	for poolID := range b.StakeDistribution {
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
	for key, aVal := range a.UTxOEntries {
		bVal, ok := b.UTxOEntries[key]
		switch {
		case !ok:
			report("utxo %s present in a, missing in b: %s", key, aVal)
		case aVal != bVal:
			report("utxo %s differs: %s (a) vs %s (b)", key, aVal, bVal)
		}
	}
	for key, bVal := range b.UTxOEntries {
		if _, ok := a.UTxOEntries[key]; !ok {
			report("utxo %s present in b, missing in a: %s", key, bVal)
		}
	}
	if utxoDiffCount > maxUTxODiffLines {
		d.TruncatedUTxO = utxoDiffCount - maxUTxODiffLines
	}

	return d
}
