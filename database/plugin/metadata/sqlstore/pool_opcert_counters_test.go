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

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// The counters carry the sequence of the row at each pool's latest slot beside
// its highest one, so a pool that ended on a lower counter is told from one
// that repeated its highest.
func TestLatestPoolOpCertCountersCarryTheLastSequence(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	dropped := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x21}, 28))
	steady := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x22}, 28))
	for _, w := range []struct {
		pool           lcommon.PoolKeyHash
		sequence, slot uint64
	}{
		{dropped, 6, 10}, {dropped, 5, 20},
		{steady, 7, 11}, {steady, 7, 21},
	} {
		require.NoError(t, store.UpdatePoolOpCertSequence(
			w.pool, w.sequence, w.slot, nil,
		))
	}

	counters, err := store.LatestPoolOpCertCounters(nil)

	require.NoError(t, err)
	type counter struct{ seq, slot, last uint64 }
	got := map[string]counter{}
	for _, c := range counters {
		got[string(c.PoolKeyHash)] = counter{c.Sequence, c.Slot, c.LastSequence}
	}
	require.Equal(t, map[string]counter{
		string(dropped.Bytes()): {6, 20, 5},
		string(steady.Bytes()):  {7, 21, 7},
	}, got)
}
