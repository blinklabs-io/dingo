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

package blockfrost

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// A rollback deletes blocks before the ledger publishes the lower tip, so the
// first reads of the tip name a block that is gone; the reread finds the new
// tip.
func TestRetryTipBlockFollowsTipOutOfRollbackWindow(t *testing.T) {
	t.Parallel()
	reads := 0
	tipHash := func() []byte {
		reads++
		if reads < 3 {
			return []byte("rolled-back")
		}
		return []byte("new-tip")
	}
	get := func(hash []byte) (models.Block, error) {
		if string(hash) == "rolled-back" {
			return models.Block{}, models.ErrBlockNotFound
		}
		return models.Block{Hash: hash}, nil
	}
	block, err := retryTipBlock(
		context.Background(), time.Millisecond, tipHash, get,
	)
	require.NoError(t, err)
	require.Equal(t, []byte("new-tip"), block.Hash)
	require.Equal(t, 3, reads)
}

func TestRetryTipBlockGivesUpAsUnavailable(t *testing.T) {
	t.Parallel()
	_, err := retryTipBlock(
		context.Background(),
		time.Millisecond,
		func() []byte { return []byte("gone") },
		func([]byte) (models.Block, error) {
			return models.Block{}, models.ErrBlockNotFound
		},
	)
	require.ErrorIs(t, err, ErrTipBlockUnavailable)
}

func TestRetryTipBlockDoesNotRetryOtherErrors(t *testing.T) {
	t.Parallel()
	boom := errors.New("backend closed")
	calls := 0
	_, err := retryTipBlock(
		context.Background(),
		time.Millisecond,
		func() []byte { return []byte("tip") },
		func([]byte) (models.Block, error) {
			calls++
			return models.Block{}, boom
		},
	)
	require.ErrorIs(t, err, boom)
	require.NotErrorIs(t, err, ErrTipBlockUnavailable)
	require.Equal(t, 1, calls)
}

func TestLatestBlockTipUnavailableIs503(t *testing.T) {
	t.Parallel()
	for _, path := range []string{
		"/api/v0/blocks/latest",
		"/api/v0/blocks/latest/txs",
		"/api/v0/blocks/latest/txs/cbor",
	} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			err := fmt.Errorf("get latest block: %w", ErrTipBlockUnavailable)
			node := &mockNode{blockErr: err, txHashesErr: err}
			node.navErr = err
			b := newTestBlockfrost(node)
			w := httptest.NewRecorder()
			b.handler().ServeHTTP(
				w, httptest.NewRequest(http.MethodGet, path, nil),
			)
			require.Equal(t, http.StatusServiceUnavailable, w.Code)
			require.Equal(t, "1", w.Header().Get("Retry-After"))
		})
	}
}

// A tip naming a block the store no longer holds is the state a rollback
// leaves before the ledger publishes the lower tip. It exercises the real
// block lookup, so the not-found mapping the retry depends on is checked
// against the store, not a stub.
func TestNodeAdapterLatestBlockTipOutsideStore(t *testing.T) {
	t.Parallel()
	nodeConfig, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		cardano.EmbeddedConfigPath("preview"),
	)
	require.NoError(t, err)
	adapter, raw, db := newDBBackedAdapter(t, nodeConfig)
	f, _ := seedCertifiedEndorserBlock(t, db)
	ctx := context.Background()
	_, err = raw.Exec(
		`INSERT INTO epoch (epoch_id, start_slot, length_in_slots, slot_length, era_id) VALUES (0, 0, 86400, 1000, 6)`,
	)
	require.NoError(t, err)
	require.NoError(t, adapter.ledgerState.PrepareEpochCacheForStartup(ctx))

	adapter.ledgerState.SetTipForTesting(ochainsync.Tip{
		Point:       ocommon.NewPoint(f.Certifier.SlotNumber()+1, []byte("rolled-back-block-hash-00000")),
		BlockNumber: f.Certifier.BlockNumber() + 1,
	})
	_, err = adapter.LatestBlock(ctx)
	require.ErrorIs(t, err, ErrTipBlockUnavailable)
	_, err = adapter.LatestBlockTxHashes(ctx)
	require.ErrorIs(t, err, ErrTipBlockUnavailable)

	adapter.ledgerState.SetTipForTesting(ochainsync.Tip{
		Point: ocommon.NewPoint(
			f.Certifier.SlotNumber(),
			f.Certifier.Hash().Bytes(),
		),
		BlockNumber: f.Certifier.BlockNumber(),
	})
	_, err = adapter.LatestBlock(ctx)
	require.NoError(t, err)
}
