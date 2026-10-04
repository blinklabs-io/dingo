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

package eras

import (
	"math"
	"strconv"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

// TestPParamsUpdateRejectsOutOfDomainUpdate applies updates built in Go,
// which never passed the CBOR decoder's checks, through the era hook that
// governance enactment uses. An out-of-domain update must come back as an
// error and leave the current parameters untouched.
func TestPParamsUpdateRejectsOutOfDomainUpdate(t *testing.T) {
	t.Parallel()

	maxWord32 := uint(math.MaxUint32)
	overWord32 := maxWord32 + 1
	maxWord16 := uint(math.MaxUint16)
	overWord16 := maxWord16 + 1

	conwayCase := func(
		name string,
		set func(*conway.ConwayProtocolParameterUpdate),
		wantErr string,
	) {
		t.Run("Conway/"+name, func(t *testing.T) {
			t.Parallel()
			current := &conway.ConwayProtocolParameters{MaxTxSize: 16384}
			before := *current
			var update conway.ConwayProtocolParameterUpdate
			set(&update)
			var (
				got lcommon.ProtocolParameters
				err error
			)
			require.NotPanics(t, func() {
				got, err = PParamsUpdateConway(current, update)
			})
			if wantErr == "" {
				require.NoError(t, err)
				require.NotNil(t, got)
				return
			}
			require.ErrorContains(t, err, wantErr)
			require.Equal(t, before, *current)
		})
	}
	conwayCase("Word32 maximum", func(u *conway.ConwayProtocolParameterUpdate) {
		u.MaxTxSize = &maxWord32
	}, "")
	if strconv.IntSize > 32 {
		conwayCase("Word32 above maximum", func(u *conway.ConwayProtocolParameterUpdate) {
			u.MaxTxSize = &overWord32
		}, "maxTxSize")
	}
	conwayCase("Word16 maximum", func(u *conway.ConwayProtocolParameterUpdate) {
		u.CollateralPercentage = &maxWord16
	}, "")
	conwayCase("Word16 above maximum", func(u *conway.ConwayProtocolParameterUpdate) {
		u.CollateralPercentage = &overWord16
	}, "collateralPercentage")
	conwayCase("cost model language above Word8", func(u *conway.ConwayProtocolParameterUpdate) {
		u.CostModels = map[uint][]int64{256: {1}}
	}, "256")
	conwayCase("cost model language at Word8 maximum", func(u *conway.ConwayProtocolParameterUpdate) {
		u.CostModels = map[uint][]int64{255: {1}}
	}, "")

	if strconv.IntSize > 32 {
		t.Run("Dijkstra/Word32 above maximum", func(t *testing.T) {
			t.Parallel()
			current := &gdijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: conway.ConwayProtocolParameters{MaxTxSize: 16384},
			}
			before := *current
			update := gdijkstra.DijkstraProtocolParameterUpdate{MaxTxSize: &overWord32}
			var err error
			require.NotPanics(t, func() {
				_, err = PParamsUpdateDijkstra(current, update)
			})
			require.ErrorContains(t, err, "maxTxSize")
			require.Equal(t, before, *current)
		})
	}
}
