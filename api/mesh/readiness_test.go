// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package mesh

import (
	"net/http"
	"testing"

	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestHandlerGatesIndexerRoutesUntilReady(t *testing.T) {
	t.Parallel()
	deps := newTestDeps()
	ready := false
	h := newTestServer(t, deps, func(c *ServerConfig) {
		c.Ready = func(indexerdb.Kind) (bool, error) { return ready, nil }
	}).handler()

	for _, path := range []string{
		"/block", "/block/transaction", "/account/balance",
		"/account/coins", "/network/status",
	} {
		rec := postJSON(t, h, path, map[string]any{
			"network_identifier": testNetworkID(),
		})
		require.Equal(t, http.StatusServiceUnavailable, rec.Code, path)
	}
	rec := postJSON(t, h, "/network/list", map[string]any{})
	require.NotEqual(t, http.StatusServiceUnavailable, rec.Code,
		"network list is served without the IndexerDB")

	ready = true
	rec = postJSON(t, h, "/network/status", map[string]any{
		"network_identifier": testNetworkID(),
	})
	require.NotEqual(t, http.StatusServiceUnavailable, rec.Code)
}

func TestBoundedDatabaseHidesBlocksAboveTheTip(t *testing.T) {
	t.Parallel()
	tip := ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("tip"))}
	inner := &fakeDatabase{
		blockByHash: func([]byte) (models.Block, error) {
			return models.Block{Slot: 150, Number: 15}, nil
		},
		blockByIndex: func(idx uint64) (models.Block, error) {
			return models.Block{Slot: idx * 10, Number: idx}, nil
		},
	}
	db := NewBoundedDatabase(inner, func() (ochainsync.Tip, error) {
		return tip, nil
	})

	_, err := db.BlockByHash(t.Context(), []byte("h"))
	require.ErrorIs(t, err, models.ErrBlockNotFound)
	_, err = db.BlockByIndex(t.Context(), 11)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
	block, err := db.BlockByIndex(t.Context(), 10)
	require.NoError(t, err)
	require.EqualValues(t, 10, block.Number)
}
