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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// The store ranks a pool certificate row with no certificate as block 0,
// certificate 0 of its slot, so a snapshot import's retirement precedes the
// certificates of its slot and never supersedes them.
func TestPoolReapedRanksAnImportRetirementAsTheStoreDoes(t *testing.T) {
	t.Parallel()
	b := &boundaryStake{
		slot:   200,
		epochs: []models.Epoch{{EpochId: 5, StartSlot: 150}},
	}
	registration := PoolEvent{
		Slot: 100, Position: 3, Registration: &models.PoolRegistration{},
	}
	import_ := PoolEvent{
		Slot: 100, Position: PoolEventSyntheticPosition, RetiringEpoch: 5,
	}
	certified := PoolEvent{Slot: 100, Position: 3, RetiringEpoch: 5}

	t.Run("a registration of its slot supersedes it", func(t *testing.T) {
		t.Parallel()
		require.False(t, b.poolReaped(
			[]PoolEvent{import_, registration}, 10,
		))
	})
	t.Run("it does not supersede a certified retirement", func(t *testing.T) {
		t.Parallel()
		require.True(t, b.poolReaped(
			[]PoolEvent{certified, import_}, 10,
		))
	})
	t.Run("alone it reaps", func(t *testing.T) {
		t.Parallel()
		require.True(t, b.poolReaped([]PoolEvent{import_}, 10))
	})
}
