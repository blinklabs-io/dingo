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

package sqlstore

import (
	"bytes"
	"testing"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestGetActivePoolKeyHashesPageOrdersAndBoundsInStorage(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 1)
	for _, seed := range []byte{0x30, 0x10, 0x20} {
		seedPoolWithSingleRegistration(
			t, store, bytes.Repeat([]byte{seed}, 28), 100,
		)
	}
	require.NoError(t, store.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(200, make([]byte, 32)),
	}, nil))

	page, total, err := store.GetActivePoolKeyHashesPage(1, 1, false, nil)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	require.Equal(t, [][]byte{bytes.Repeat([]byte{0x20}, 28)}, page)

	page, total, err = store.GetActivePoolKeyHashesPage(2, 0, true, nil)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	require.Equal(t, [][]byte{
		bytes.Repeat([]byte{0x30}, 28),
		bytes.Repeat([]byte{0x20}, 28),
	}, page)

	page, total, err = store.GetActivePoolKeyHashesPage(10, 3, false, nil)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	require.Empty(t, page)
}
