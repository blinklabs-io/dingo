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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestManualProcessingRecoveryKeepsRejectionCause pins that, under manual
// block processing, a pass that rewinds a rejected block returns the
// rejection alongside the restart sentinel. Manual processing has no pipeline
// loop to restart, so its caller receives this error directly and the
// sentinel alone would hide why the block was refused.
func TestManualProcessingRecoveryKeepsRejectionCause(t *testing.T) {
	t.Parallel()

	t.Run("epoch boundary refusal", func(t *testing.T) {
		t.Parallel()
		_, err := runBoundaryEraCase(t, boundaryEraCase{pparamsMajor: 8})
		require.ErrorIs(t, err, errRestartLedgerPipeline)
		var hve *headerValidationError
		require.ErrorAs(t, err, &hve)
	})

	t.Run("transaction validation recovery", func(t *testing.T) {
		t.Parallel()
		fx := newDijkstraHardForkReplayFixture(t, true)
		err := replayDijkstraHardForkBlock(t, fx)
		require.ErrorIs(t, err, errRestartLedgerPipeline)
		var tve *txValidationError
		require.ErrorAs(t, err, &tve)
	})
}
