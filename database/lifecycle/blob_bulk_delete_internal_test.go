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

package lifecycle

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBatchEndDoesNotWrapNearMaxUint64 guards against the regression this
// function was extracted to fix: start+uint64(batchSize)-1 overflowing past
// math.MaxUint64 and wrapping to a value below start, which would corrupt
// the batch's iteration range instead of clamping to tipID.
func TestBatchEndDoesNotWrapNearMaxUint64(t *testing.T) {
	tipID := uint64(math.MaxUint64)
	start := tipID - 5
	end := batchEnd(start, tipID, 10_000)
	assert.Equal(t, tipID, end)
	assert.GreaterOrEqual(t, end, start)
}

func TestBatchEndClampsToTipIDWhenBatchSizeExceedsRemainder(t *testing.T) {
	end := batchEnd(100, 105, 10_000)
	assert.Equal(t, uint64(105), end)
}

func TestBatchEndStopsAtBatchSizeWhenTipIDIsFarther(t *testing.T) {
	end := batchEnd(100, 1_000_000, 10_000)
	assert.Equal(t, uint64(100+10_000-1), end)
}

func TestBatchEndSingleBlockBatch(t *testing.T) {
	end := batchEnd(42, 1_000, 1)
	assert.Equal(t, uint64(42), end)
}
