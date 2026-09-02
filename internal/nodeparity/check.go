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

import "fmt"

// Skip reason codes: stable, low-cardinality values suitable for a
// Prometheus label, as opposed to SkipDetail's free-text message (which
// embeds slot numbers and so is not).
const (
	SkipTipMismatch = "tip_mismatch" // the two nodes never agreed on a tip
	SkipTipAdvanced = "tip_advanced" // a tip moved during the query round trip
)

// CheckResult is the outcome of one Check cycle.
type CheckResult struct {
	// Tip is the block both nodes agreed on when Skipped is false.
	Tip Tip
	// Skipped is true when the cycle could not produce a trustworthy
	// comparison (the two nodes never shared a tip, or one advanced
	// mid-query) rather than a comparison that happened to match. Report
	// this distinctly from "matched": a caller that folds Skipped into "no
	// divergence found" would show a healthy status while the tool is
	// silently never completing a real comparison.
	Skipped bool
	// SkipReason is one of the Skip* constants above, suitable for a metric
	// label. SkipDetail is the human-readable message (slot numbers and
	// all) for logs and CLI output.
	SkipReason string
	SkipDetail string
	// Diff is the comparison result when Skipped is false. Diff.Empty()
	// means the two nodes' ledger state matched at Tip.
	Diff Diff
}

// Check runs one comparison cycle against a Dingo node and a reference
// cardano-node, both already-running and already-synced NtC listeners at
// dingoAddr/cardanoAddr (see Dial for the address forms accepted). It does
// not start, stop, or manage either node.
//
// Because Dingo's LocalStateQuery Acquire cannot pin a specific historical
// block yet (blinklabs-io/dingo#382), Check instead reads both nodes' tips,
// runs the LocalStateQuery session against both while they agree, and
// re-reads both tips afterward -- discarding the cycle (Skipped) if either
// node advanced during the round trip, rather than reporting a comparison
// whose two halves may not describe the same block.
func Check(dingoAddr, cardanoAddr string, magic uint32) (*CheckResult, error) {
	dingoConn, err := Dial(dingoAddr, magic)
	if err != nil {
		return nil, fmt.Errorf("dial dingo %s: %w", dingoAddr, err)
	}
	defer dingoConn.Close() //nolint:errcheck

	cardanoConn, err := Dial(cardanoAddr, magic)
	if err != nil {
		return nil, fmt.Errorf("dial cardano-node %s: %w", cardanoAddr, err)
	}
	defer cardanoConn.Close() //nolint:errcheck

	before1, err := ReadTip(dingoConn)
	if err != nil {
		return nil, fmt.Errorf("dingo tip: %w", err)
	}
	before2, err := ReadTip(cardanoConn)
	if err != nil {
		return nil, fmt.Errorf("cardano-node tip: %w", err)
	}

	if ok, reason, detail := sandwichOK(before1, before2, before1, before2); !ok {
		return &CheckResult{
			Skipped:    true,
			SkipReason: reason,
			SkipDetail: detail,
		}, nil
	}

	dingoSnap, err := QuerySnapshot(dingoConn)
	if err != nil {
		return nil, fmt.Errorf("dingo snapshot: %w", err)
	}
	cardanoSnap, err := QuerySnapshot(cardanoConn)
	if err != nil {
		return nil, fmt.Errorf("cardano-node snapshot: %w", err)
	}

	after1, err := ReadTip(dingoConn)
	if err != nil {
		return nil, fmt.Errorf("dingo tip re-check: %w", err)
	}
	after2, err := ReadTip(cardanoConn)
	if err != nil {
		return nil, fmt.Errorf("cardano-node tip re-check: %w", err)
	}

	if ok, reason, detail := sandwichOK(before1, before2, after1, after2); !ok {
		return &CheckResult{
			Skipped:    true,
			SkipReason: reason,
			SkipDetail: detail,
		}, nil
	}

	return &CheckResult{
		Tip:  before1,
		Diff: DiffSnapshots(dingoSnap, cardanoSnap),
	}, nil
}

// sandwichOK decides whether a tip-sandwich round trip produced a
// trustworthy comparison: the two nodes must have agreed on a tip before
// the query ran, and neither may have advanced past it by the time of the
// second read. Split out from Check as a pure function so this decision is
// unit-testable without a live node. reason is a stable Skip* code suitable
// for a metric label; detail is a human-readable message for logs.
func sandwichOK(
	before1, before2, after1, after2 Tip,
) (ok bool, reason, detail string) {
	if !before1.Equal(before2) {
		return false, SkipTipMismatch, fmt.Sprintf(
			"tips did not match: dingo at slot %d, cardano-node at slot %d",
			before1.Slot, before2.Slot,
		)
	}
	if !before1.Equal(after1) || !before2.Equal(after2) {
		return false, SkipTipAdvanced, "tip advanced during the query round trip"
	}
	return true, "", ""
}
