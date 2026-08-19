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

package ledger

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A skipped reward round is not a benign no-op. The reference node credits
// the round regardless, so every skip leaves this node's reward balances --
// and the leadership stake distribution derived from them -- permanently
// short by that epoch's rewards, with nothing to backfill it later.
//
// That shortfall is what rejects canonical blocks: leader eligibility
// compares a VRF value against a stake-derived threshold, so a sigma
// shortfall of eps flips a decision with probability about eps per block.
// Measured on preview for issue #3165, the shortfall was ~3 epochs of reward
// accrual, sigma was 0.042% short, and the rejected block's leader value sat
// between this node's threshold and the reference's.
//
// Both skip paths logged at Debug before this, invisible at the default
// level, which is why three separate field reports were investigated without
// anyone seeing the cause. The level is the fix: a node quietly diverging
// from the network has to say so before it wedges, not after.
func TestSkippedStakeRewardsIsReportedLoudly(t *testing.T) {
	var buf bytes.Buffer
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
				// Deliberately Warn: the point of the change is that this
				// survives the default level. A Debug-level report would
				// produce no output here.
				Level: slog.LevelWarn,
			})),
		},
	}

	ls.reportSkippedStakeRewards(1386, "missing ADA pots", "pots_epoch", 1385)

	logs := buf.String()
	require.NotEmpty(t, logs,
		"a skipped reward round must be visible at the default log level; "+
			"at Debug it stays hidden until the node rejects a block")
	assert.Contains(t, logs, "level=WARN")
	assert.Contains(t, logs, "missing ADA pots")
	assert.Contains(t, logs, "new_epoch=1386")
	assert.Contains(t, logs, "pots_epoch=1385")
	// The consequence, not just the event: whoever reads this needs to know
	// the balances stay short rather than catching up on their own.
	assert.Contains(t, logs, "permanently")
}

// The reporting path must tolerate a LedgerState with no logger and no
// metrics, since it runs on the epoch-boundary hot path where a nil
// dereference would take down block application.
func TestSkippedStakeRewardsSurvivesNilDependencies(t *testing.T) {
	ls := &LedgerState{}
	require.NotPanics(t, func() {
		ls.reportSkippedStakeRewards(
			1386,
			"missing reward snapshot",
			"reward_snapshot_epoch",
			1383,
		)
	})
}
