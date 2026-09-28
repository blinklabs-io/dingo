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

package praos

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStakeSnapshotEpochUsesPraosRotation(t *testing.T) {
	// Leader election in epoch E is validated against stake as of the end of
	// epoch E-2. dingo's mark[K] holds stake as of the end of K-1, so that
	// distribution is mark[E-1]; StakeSnapshotEpoch(E) therefore returns E-1
	// (saturating to the genesis snapshot for the earliest epochs).
	for _, tc := range []struct {
		epoch uint64
		want  uint64
	}{
		{epoch: 0, want: 0},
		{epoch: 1, want: 0},
		{epoch: 2, want: 1},
		{epoch: 3, want: 2},
		{epoch: 10, want: 9},
	} {
		assert.Equal(
			t,
			tc.want,
			StakeSnapshotEpoch(tc.epoch),
			fmt.Sprintf("epoch %d", tc.epoch),
		)
	}
}
