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

package ouroboros

import (
	"testing"

	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestNormalizeIntersectPoints(t *testing.T) {
	points := []ocommon.Point{
		ocommon.NewPoint(20, []byte("b")),
		ocommon.NewPoint(30, []byte("c")),
		ocommon.NewPoint(20, []byte("b")),
		ocommon.NewPointOrigin(),
		ocommon.NewPointOrigin(),
	}

	normalized := normalizeIntersectPoints(points)

	require.Equal(
		t,
		[]ocommon.Point{
			ocommon.NewPoint(20, []byte("b")),
			ocommon.NewPoint(30, []byte("c")),
			ocommon.NewPointOrigin(),
		},
		normalized,
	)
}

func TestShouldRestartChainsyncOnSwitch(t *testing.T) {
	tests := []struct {
		name          string
		localTip      ocommon.Point
		trackedClient *dchainsync.TrackedClient
		want          bool
	}{
		{
			name:     "nil tracked client",
			localTip: ocommon.NewPoint(100, []byte("tip")),
			want:     false,
		},
		{
			name:          "origin local tip",
			localTip:      ocommon.NewPointOrigin(),
			trackedClient: &dchainsync.TrackedClient{Cursor: ocommon.NewPoint(5, []byte("a"))},
			want:          false,
		},
		{
			name:          "tracked cursor behind local tip",
			localTip:      ocommon.NewPoint(100, []byte("tip")),
			trackedClient: &dchainsync.TrackedClient{Cursor: ocommon.NewPoint(90, []byte("a"))},
			want:          false,
		},
		{
			name:          "tracked cursor equal to local tip",
			localTip:      ocommon.NewPoint(100, []byte("tip")),
			trackedClient: &dchainsync.TrackedClient{Cursor: ocommon.NewPoint(100, []byte("tip"))},
			want:          false,
		},
		{
			name:          "tracked cursor ahead of local tip",
			localTip:      ocommon.NewPoint(100, []byte("tip")),
			trackedClient: &dchainsync.TrackedClient{Cursor: ocommon.NewPoint(110, []byte("ahead"))},
			want:          true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(
				t,
				test.want,
				shouldRestartChainsyncOnSwitch(
					test.localTip,
					test.trackedClient,
				),
			)
		})
	}
}
