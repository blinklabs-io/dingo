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
	"errors"
	"fmt"
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// A deferred header-validation failure has to be distinguishable from every
// other pipeline error, because it is the one class that is *deterministic*:
// the block is already in the chain store, so restarting the pipeline reads
// the identical block and fails identically, forever. Transaction validation
// failures already carry a type that routes them into recovery; header
// validation carried a bare fmt.Errorf, which is why it looped instead.
//
// Rejecting the block is correct and stays correct -- the point of the type
// is to reject the *chain* rather than to spin on it.
func TestHeaderValidationErrorIsIdentifiable(t *testing.T) {
	point := ocommon.Point{Slot: 119799023, Hash: []byte{0xab, 0xcd}}
	cause := errors.New("VRF leader value exceeds stake-derived threshold")
	err := &headerValidationError{BlockPoint: point, Cause: cause}

	var target *headerValidationError
	require.True(t, errors.As(err, &target),
		"the pipeline must be able to recognise this error class")
	require.Equal(t, point.Slot, target.BlockPoint.Slot)

	require.ErrorIs(t, err, cause,
		"the underlying validation error must stay inspectable")
	require.Contains(t, err.Error(), "119799023")
	require.Contains(t, err.Error(), "exceeds stake-derived threshold")

	// Still identifiable once the pipeline has wrapped it.
	wrapped := fmt.Errorf("process block batch: %w", err)
	require.True(t, errors.As(wrapped, &target))
	require.ErrorIs(t, wrapped, cause)
}

// Recovery must not fire for unrelated failures, or an ordinary transient
// error would start rewinding the chain.
func TestHeaderValidationRecoveryIgnoresOtherErrors(t *testing.T) {
	ls := &LedgerState{}
	for _, err := range []error{
		errors.New("some unrelated failure"),
		errStaleChainIterator,
		&txValidationError{},
	} {
		recovered, recoverErr := ls.tryRecoverFromHeaderValidationError(err)
		require.NoError(t, recoverErr)
		require.False(t, recovered,
			"only a header-validation failure may trigger this recovery")
	}
}

// Without a chain manager there is nothing to rewind, so recovery declines
// rather than reporting a rewind it did not perform -- a false "recovered"
// would send the pipeline straight back into the same block.
func TestHeaderValidationRecoveryDeclinesWithoutChainManager(t *testing.T) {
	ls := &LedgerState{}
	err := &headerValidationError{
		BlockPoint: ocommon.Point{Slot: 42},
		Cause:      errors.New("rejected"),
	}
	recovered, recoverErr := ls.tryRecoverFromHeaderValidationError(err)
	require.NoError(t, recoverErr)
	require.False(t, recovered)
}
