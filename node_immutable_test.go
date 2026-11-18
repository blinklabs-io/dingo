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

package dingo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// fakeImmutablePoint reports a fixed immutable point, as the ledger does k
// blocks behind its applied tip.
type fakeImmutablePoint struct {
	point ocommon.Point
	found bool
	err   error
}

func (f fakeImmutablePoint) ImmutablePoint(
	context.Context,
) (ocommon.Point, bool, error) {
	return f.point, f.found, f.err
}

func TestImmutableCeilingIsTheLedgerImmutablePoint(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	hashes := make([][]byte, 31)
	for id := uint64(1); id <= 30; id++ {
		hashes[id] = bytes.Repeat([]byte{byte(id)}, 32)
		require.NoError(t, db.BlockCreate(models.Block{
			ID: id, Slot: id * 10, Number: id, Type: 1, Hash: hashes[id],
			Cbor: []byte{0x82, 0x41, byte(id), 0x41, 0x02},
		}, nil))
	}
	at := func(id uint64) fakeImmutablePoint {
		return fakeImmutablePoint{
			point: ocommon.NewPoint(id*10, hashes[id]), found: true,
		}
	}
	ceiling, ok, err := immutableCeilingFor(context.Background(), db, at(15))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(15), ceiling)

	// No immutable point yet, an unconfigured ledger, or a point the block
	// index does not hold permits nothing.
	for name, src := range map[string]fakeImmutablePoint{
		"not found": {},
		"unconfigured": {
			err: fmt.Errorf("%w: test", ledger.ErrImmutablePointUnconfigured),
		},
		"unknown point": {
			point: ocommon.NewPoint(999, bytes.Repeat([]byte{0xEE}, 32)),
			found: true,
		},
	} {
		_, ok, err = immutableCeilingFor(context.Background(), db, src)
		require.NoError(t, err, name)
		require.False(t, ok, name)
	}

	boom := errors.New("read chain")
	_, _, err = immutableCeilingFor(
		context.Background(), db, fakeImmutablePoint{err: boom},
	)
	require.ErrorIs(t, err, boom)
}

func TestValidateImmutableDBRefusals(t *testing.T) {
	t.Parallel()
	enabled := ImmutableDBConfig{Enabled: true}

	require.NoError(t, validateImmutableDB(Config{}))
	require.NoError(t, validateImmutableDB(Config{immutableDB: enabled}))
	require.NoError(t, validateImmutableDB(Config{
		immutableDB: enabled,
		pluginSelections: map[hostplugin.Capability]hostplugin.Selection{
			hostplugin.CapabilityStorageBlob: {Provider: "badger"},
		},
	}))

	for _, provider := range []string{"s3", "gcs"} {
		require.NoError(t, validateImmutableDB(Config{
			immutableDB: enabled,
			pluginSelections: map[hostplugin.Capability]hostplugin.Selection{
				hostplugin.CapabilityStorageBlob: {Provider: provider},
			},
		}))
	}
	err := validateImmutableDB(Config{
		immutableDB: enabled,
		pluginSelections: map[hostplugin.Capability]hostplugin.Selection{
			hostplugin.CapabilityStorageBlob: {Provider: "unsupported"},
		},
	})
	require.ErrorContains(t, err, "blob plugin")

	err = validateImmutableDB(Config{
		immutableDB:   enabled,
		historyExpiry: HistoryExpiryConfig{Enabled: true},
	})
	require.ErrorContains(t, err, "historyExpiry")

	// Disabled, the same selections are fine.
	require.NoError(t, validateImmutableDB(Config{
		historyExpiry: HistoryExpiryConfig{Enabled: true},
	}))
}

func TestHistoryExpiryRefusedOverRecordedImmutableState(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: dir,
		Immutable: database.ImmutableConfig{
			Enabled: true, ChunkSlots: 4320, CanContainEBB: true,
		},
	})
	require.NoError(t, err)
	expiry := Config{historyExpiry: HistoryExpiryConfig{Enabled: true}}
	// The configuration no longer enables the ImmutableDB; the recorded state
	// is what matters.
	err = refuseHistoryExpiryOverImmutable(expiry, db)
	require.ErrorContains(t, err, "ImmutableDB")
	require.NoError(t, refuseHistoryExpiryOverImmutable(Config{}, db))

	plain, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	require.NoError(t, refuseHistoryExpiryOverImmutable(expiry, plain))
}

// The promotion ceiling is read from the ledger's applied tip at the call
// site: a chain that is ahead of the ledger, as during catch-up, must not let
// blocks the ledger has not applied leave the VolatileDB.
func TestNodeImmutableCeilingUsesLedgerAppliedTip(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	// The chain holds 600 blocks and the ledger has applied the first 500.
	// The manager reads its tip at construction, so seed the blocks first.
	const total, appliedID = 600, 500
	hashes := make([][]byte, total+1)
	for id := uint64(1); id <= total; id++ {
		hashes[id] = bytes.Repeat([]byte{byte(id), byte(id >> 8)}, 16)
		require.NoError(t, db.BlockCreate(models.Block{
			ID: id, Slot: id * 10, Number: id, Type: 1, Hash: hashes[id],
			Cbor: []byte{0x82, 0x41, byte(id), 0x41, 0x02},
		}, nil))
	}
	cm, err := chain.NewManager(t.Context(), db, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(total*10), cm.PrimaryChain().Tip().Point.Slot)
	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		Logger:            logger,
		CardanoNodeConfig: nodeCfg,
		Network:           "preview",
		PromRegistry:      prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	k := uint64(ls.SecurityParam())
	require.Positive(t, k)
	require.Less(t, k, uint64(appliedID), "the applied tip must be past k")
	ls.SetTipForTesting(ochainsync.Tip{
		Point:       ocommon.NewPoint(appliedID*10, hashes[appliedID]),
		BlockNumber: appliedID,
	})

	n := &Node{db: db, ledgerState: ls, chainManager: cm}
	ceiling, ok, err := n.immutableCeiling(t.Context())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, appliedID-k, ceiling,
		"k behind the ledger's applied tip, not behind the chain's")
}
