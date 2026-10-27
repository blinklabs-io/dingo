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

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// poolParamsNodeFirstRegistration is the GetStakePoolParams reply cardano-node
// 11.1.3 sent for the devnet pool while its first registration was in effect
// and a re-registration was still future parameters.
const poolParamsNodeFirstRegistration = "" +
	"81a1581c6340ad239fad55d94f415adeb861889076889cf5bf11a8248b7a8469" +
	"89581c6340ad239fad55d94f415adeb861889076889cf5bf11a8248b7a846958" +
	"2018a7f015b6b2f09f00178c09c1230b02a85aa0614fe9bf982e5985e06634ab" +
	"c21a000f42401a1443fd00d81e82010a581de06b7cc75fe8d4062466ddaad438" +
	"358b6a0f7f2b6b971679f87504b8b7d9010281581c6b7cc75fe8d4062466ddaa" +
	"d438358b6a0f7f2b6b971679f87504b8b7828400190bb9440101a8c0f6830119" +
	"0bb97172656c61792e6578616d706c652e636f6d82781a68747470733a2f2f65" +
	"78616d706c652e636f6d2f702e6a736f6e582067208fc064dff23c8311c14f62" +
	"712cc572cad299f4d363eaae4080fda36a16d7"

// poolParamsNodeReregistered is the reply once the re-registration took
// effect; it is unchanged while a retirement is pending.
const poolParamsNodeReregistered = "" +
	"81a1581c6340ad239fad55d94f415adeb861889076889cf5bf11a8248b7a8469" +
	"89581c6340ad239fad55d94f415adeb861889076889cf5bf11a8248b7a846958" +
	"2018a7f015b6b2f09f00178c09c1230b02a85aa0614fe9bf982e5985e06634ab" +
	"c21a000f42401a14dc9380d81e82010a581de06b7cc75fe8d4062466ddaad438" +
	"358b6a0f7f2b6b971679f87504b8b7d9010281581c6b7cc75fe8d4062466ddaa" +
	"d438358b6a0f7f2b6b971679f87504b8b7828400190bb9440101a8c0f6830119" +
	"0bb97172656c61792e6578616d706c652e636f6d82781a68747470733a2f2f65" +
	"78616d706c652e636f6d2f702e6a736f6e582067208fc064dff23c8311c14f62" +
	"712cc572cad299f4d363eaae4080fda36a16d7"

// TestQueryStakePoolParams_NodeReplyAtEachPoint compares the reply with the
// bytes cardano-node sent for the devnet pool at the matching point of its
// history: the first registration in effect, the second after the boundary,
// the same while a retirement is pending. The live query at the timeline's
// tip answers like the point inside the retirement stage.
func TestQueryStakePoolParams_NodeReplyAtEachPoint(t *testing.T) {
	t.Parallel()
	ls := newPoolStateTimeline(t)
	query := stakePoolParamsQuery(
		poolStateDevnetBytes(t, poolStateDevnetPoolHex),
	)
	tests := map[string]struct {
		at   QueryPoint
		want string
	}{
		"first registration in effect": {
			at: poolStateAt(60), want: poolParamsNodeFirstRegistration,
		},
		"re-registration in effect": {
			at: poolStateAt(110), want: poolParamsNodeReregistered,
		},
		"retirement pending": {
			at: poolStateAt(130), want: poolParamsNodeReregistered,
		},
		"live": {at: QueryPoint{}, want: poolParamsNodeReregistered},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tt.want, runPoolStateHex(t, ls, query, tt.at))
		})
	}
}

// TestQueryStakePoolParams_PinnedPointBeforeRegistration omits a pool at a
// point before its registration, although it is registered at the tip.
func TestQueryStakePoolParams_PinnedPointBeforeRegistration(t *testing.T) {
	t.Parallel()
	ls := newPoolStateTimeline(t)
	seedBlockAtSlot(t, ls, 3, repeatedBytes(32, 3))
	query := stakePoolParamsQuery(
		poolStateDevnetBytes(t, poolStateDevnetPoolHex),
	)
	require.Equal(t, "81a0", runPoolStateHex(t, ls, query, poolStateAt(3)))
}

// TestQueryStakePoolParams_PinnedPointWithoutEpochRejected refuses a point no
// epoch record covers rather than answering from the live epoch.
func TestQueryStakePoolParams_PinnedPointWithoutEpochRejected(t *testing.T) {
	t.Parallel()
	ls := newPoolDistr2Ledger(t, newTestDB(t))
	ls.config.CardanoNodeConfig = newTestEraHistoryCfg(t)
	seedEpochs(t, ls, map[uint64]uint64{100: 1})
	require.NoError(t, ls.db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(160, repeatedBytes(32, 0x0B)),
	}, nil))
	setStakePoolParamsLiveState(ls, 1, 100, 160)
	seedBlockAtSlot(t, ls, 60, repeatedBytes(32, 60))
	_, err := ls.Query(
		t.Context(),
		stakePoolParamsQuery(repeatedBytes(28, 0x11)),
		poolStateAt(60),
	)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}
