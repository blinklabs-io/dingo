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
	"context"
	"fmt"
)

// Skip reason codes: stable, low-cardinality values suitable for a
// Prometheus label, as opposed to SkipDetail's free-text message (which
// embeds slot numbers and so is not).
const (
	SkipTipMismatch = "tip_mismatch" // the two nodes never agreed on a tip
)

// CheckResult is the outcome of one Check cycle.
type CheckResult struct {
	// Tip is the block both nodes agreed on when Skipped is false.
	Tip Tip
	// Skipped is true when the cycle could not produce a trustworthy
	// comparison (the two nodes never shared a tip to acquire) rather than a
	// comparison that happened to match. Report this distinctly from
	// "matched": a caller that folds Skipped into "no divergence found"
	// would show a healthy status while the tool is silently never
	// completing a real comparison.
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
// When at is nil, Check reads both nodes' tips and, once they agree on one,
// acquires that exact point on both connections before running the
// LocalStateQuery session against each -- rather than the live tip each
// query happens to see when it runs. This matters because Dingo's half of
// that session can take minutes (the paginated whole-UTxO walk against its
// disk-backed store; see QuerySnapshot), long enough that a live testnet's
// tip advances many blocks before it finishes: an unpinned comparison would
// silently mix pages describing different, inconsistent moments in history.
//
// When at is non-nil, Check skips the live-tip-agreement step entirely and
// acquires the caller-named point directly on both connections instead --
// explicit historical mode (blinklabs-io/dingo#382). This is what lets a
// caller fall behind the live chain and still walk through specific past
// blocks (N, N+1, N+2, ...) one at a time rather than only ever comparing
// "whatever the two nodes currently agree on." Neither node needs to be
// anywhere near its own live tip for this to work; each independently
// either can or cannot Acquire the named point, and Acquire's own error
// (e.g. a real cardano-node's "point too old"/"not on chain" failures, or
// Dingo's ErrPointNotOnChain/ErrHistoricalStateUnavailable) surfaces
// directly as this call's error rather than a Skipped result -- there is no
// live-tip race left to discard a cycle over once a point is explicitly
// named.
//
// See QuerySnapshot's doc comment for the one accepted gap pinning doesn't
// close in either mode (Dingo's protocol-params/stake-distribution queries
// only honor a pinned point within the live tip's current epoch).
//
// ctx bounds the whole cycle: every query below is a synchronous protocol
// call with no timeout of its own, so cancelling ctx (e.g. on SIGINT) is
// what lets a caller stuck against an unresponsive peer actually return,
// rather than the process hanging until forcibly killed. See Dial.
func Check(
	ctx context.Context, dingoAddr, cardanoAddr string, magic uint32, at *Tip,
) (*CheckResult, error) {
	dingoConn, err := Dial(ctx, dingoAddr, magic)
	if err != nil {
		return nil, fmt.Errorf("dial dingo %s: %w", dingoAddr, err)
	}
	defer dingoConn.Close() //nolint:errcheck

	// cardanoTipConn is used only to read cardano-node's tip below (when
	// at is nil) and is closed well before cardano-node is actually
	// queried -- see the comment above the re-dial further down for why.
	cardanoTipConn, err := Dial(ctx, cardanoAddr, magic)
	if err != nil {
		return nil, fmt.Errorf("dial cardano-node %s: %w", cardanoAddr, err)
	}

	var targetTip Tip
	if at != nil {
		targetTip = *at
	} else {
		dingoTip, err := ReadTip(dingoConn)
		if err != nil {
			cardanoTipConn.Close() //nolint:errcheck
			return nil, fmt.Errorf("dingo tip: %w", err)
		}
		cardanoTip, err := ReadTip(cardanoTipConn)
		if err != nil {
			cardanoTipConn.Close() //nolint:errcheck
			return nil, fmt.Errorf("cardano-node tip: %w", err)
		}
		if ok, reason, detail := tipsAgree(dingoTip, cardanoTip); !ok {
			cardanoTipConn.Close() //nolint:errcheck
			return &CheckResult{
				Skipped:    true,
				SkipReason: reason,
				SkipDetail: detail,
			}, nil
		}
		targetTip = dingoTip
	}

	point, err := targetTip.point()
	if err != nil {
		cardanoTipConn.Close() //nolint:errcheck
		return nil, fmt.Errorf("target point: %w", err)
	}

	// Dingo first: its UTxO walk against its disk-backed store can take
	// minutes, and its result's refs are what drive the cardano-node query
	// below (see QueryReferenceUTxOSnapshot's doc comment for why
	// cardano-node is no longer asked for its own whole UTxO set at all).
	// cardanoTipConn is closed now, before Dingo's walk runs, rather than
	// held open and reused afterward: a real cardano-node closes an NtC
	// session that sits idle as long as Dingo's walk can take ("protocol is
	// shutting down"), observed live against Preview, so a fresh connection
	// is dialed right before cardano-node is actually queried instead.
	if err := cardanoTipConn.Close(); err != nil {
		return nil, fmt.Errorf("close cardano-node tip connection: %w", err)
	}
	dingoSnap, utxoRefs, err := querySnapshot(dingoConn, &point)
	if err != nil {
		return nil, fmt.Errorf("dingo snapshot: %w", err)
	}

	cardanoConn, err := Dial(ctx, cardanoAddr, magic)
	if err != nil {
		return nil, fmt.Errorf("re-dial cardano-node %s: %w", cardanoAddr, err)
	}
	defer cardanoConn.Close() //nolint:errcheck

	cardanoSnap, err := QueryReferenceUTxOSnapshot(
		cardanoConn,
		&point,
		utxoRefs,
	)
	if err != nil {
		return nil, fmt.Errorf("cardano-node snapshot: %w", err)
	}

	return &CheckResult{
		Tip:  targetTip,
		Diff: DiffSnapshots(dingoSnap, cardanoSnap),
	}, nil
}

// tipsAgree reports whether the two nodes named the same point on chain, the
// only requirement for picking a point to Acquire on both connections. Split
// out from Check as a pure function so this decision is unit-testable
// without a live node. reason is a stable Skip* code suitable for a metric
// label; detail is a human-readable message for logs.
func tipsAgree(dingoTip, cardanoTip Tip) (ok bool, reason, detail string) {
	if !dingoTip.Equal(cardanoTip) {
		return false, SkipTipMismatch, fmt.Sprintf(
			"tips did not match: dingo at slot %d, cardano-node at slot %d",
			dingoTip.Slot, cardanoTip.Slot,
		)
	}
	return true, "", ""
}
