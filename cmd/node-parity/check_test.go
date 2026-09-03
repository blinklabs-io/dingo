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

package main

import (
	"testing"

	"github.com/blinklabs-io/dingo/internal/nodeparity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReportResult_ExitCodeSignaling guards the property checkRun/watchRun
// depend on: a caller (CI, an operator's cron) must be able to tell "clean
// match" apart from both "diverged" and "discarded" purely from the process
// exit code, since a skipped cycle read as success would hide the fact that
// no real comparison ever ran.
func TestReportResult_ExitCodeSignaling(t *testing.T) {
	t.Run("matched returns nil", func(t *testing.T) {
		result := &nodeparity.CheckResult{
			Tip:  nodeparity.Tip{Slot: 100, Hash: "aa"},
			Diff: nodeparity.Diff{},
		}
		require.NoError(t, reportResult(result))
	})

	t.Run("diverged returns an error", func(t *testing.T) {
		result := &nodeparity.CheckResult{
			Tip: nodeparity.Tip{Slot: 100, Hash: "aa"},
			Diff: nodeparity.Diff{
				ProtocolParamsDiff: "protocol parameters differ",
			},
		}
		require.Error(t, reportResult(result))
	})

	t.Run("skipped returns an error, not a silent match", func(t *testing.T) {
		result := &nodeparity.CheckResult{
			Skipped:    true,
			SkipReason: nodeparity.SkipTipMismatch,
			SkipDetail: "tips did not match: dingo at slot 100, cardano-node at slot 105",
		}
		require.Error(t, reportResult(result))
	})
}

// TestCheckCommand_RejectsPositionalArgs covers a typo after 'check' (e.g.
// 'node-parity check now'): checkCommand must reject any positional
// argument up front via Cobra's own validation, rather than silently
// ignoring the typo and running a full comparison cycle against two live
// nodes as if nothing were wrong.
func TestCheckCommand_RejectsPositionalArgs(t *testing.T) {
	cmd := checkCommand()
	require.Error(t, cmd.Args(cmd, []string{"now"}))
	require.NoError(t, cmd.Args(cmd, nil))
}

// TestReportResult_DivergedErrorReportsTrueCountNotLineCount covers a
// truncated UTxO diff: Diff.Lines() rolls every UTxO entry beyond the cap
// into a single "... N more omitted" summary line, so len(lines) undercounts
// the real number of divergences whenever TruncatedUTxO is nonzero. The
// error reportResult returns must state the true total (StakeDistribution +
// UTxO + TruncatedUTxO, +1 if protocol params also differ), not the number
// of printed lines, so an operator reading only the exit-code/error message
// (not the full report) is not misled into thinking fewer things diverged
// than actually did.
func TestReportResult_DivergedErrorReportsTrueCountNotLineCount(t *testing.T) {
	result := &nodeparity.CheckResult{
		Tip: nodeparity.Tip{Slot: 100, Hash: "aa"},
		Diff: nodeparity.Diff{
			ProtocolParamsDiff: "protocol parameters differ",
			UTxO:               []string{"utxo a differs", "utxo b differs"},
			TruncatedUTxO:      5,
		},
	}
	// Lines(): 1 (protocol params) + 2 (UTxO) + 1 (summary) = 4 lines, but
	// the true count is 1 + 2 + 5 = 8 divergences.
	err := reportResult(result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "8 difference(s)")
	assert.NotContains(t, err.Error(), "4 difference(s)")
}
