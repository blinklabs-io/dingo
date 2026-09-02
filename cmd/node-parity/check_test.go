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
