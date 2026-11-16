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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// A registration the set serves has no row ID, so the registration in effect
// and the newest one are told apart by content when either lacks one, and by
// ID when both have one.
func TestSameRegistrationRowWithoutRowIDs(t *testing.T) {
	t.Parallel()
	pool := bytes.Repeat([]byte{0x01}, 28)
	reg := func(id uint, slot, cost uint64) *models.PoolRegistration {
		return &models.PoolRegistration{
			ID: id, PoolKeyHash: pool, AddedSlot: slot, Cost: types.Uint64(cost),
			VrfKeyHash: bytes.Repeat([]byte{0x02}, 32),
		}
	}
	require.True(t, sameRegistrationRow(reg(7, 10, 5), reg(0, 10, 5)),
		"the store's row and the set's copy of one registration")
	require.False(t, sameRegistrationRow(reg(7, 10, 5), reg(0, 20, 5)),
		"a later registration is another row")
	require.False(t, sameRegistrationRow(reg(0, 10, 5), reg(0, 10, 6)))
	require.True(t, sameRegistrationRow(reg(3, 10, 5), reg(3, 10, 5)))
	require.False(t, sameRegistrationRow(reg(3, 10, 5), reg(4, 10, 5)),
		"two rows with IDs are told apart by ID")
}
