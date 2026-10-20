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

package nodemithril

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mithril"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func preprodNodeConfig(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preprod"),
		"preprod",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	require.NotEmpty(t, cfg.MithrilGenesisVerificationKey)
	return cfg
}

func preprodParams(t *testing.T) dingo.MithrilCatchUpParams {
	t.Helper()
	return dingo.MithrilCatchUpParams{
		Mithril: internalconfig.MithrilConfig{
			Enabled:         true,
			WindowChunks:    16,
			BatchChunks:     4,
			DownloadWorkers: 3,
		},
		StorageMode:       dingo.StorageModeCore,
		CardanoNodeConfig: preprodNodeConfig(t),
		Network:           "preprod",
		DataDir:           "/data/dir",
		Logger:            slog.New(slog.DiscardHandler),
	}
}

func newSource(t *testing.T, p dingo.MithrilCatchUpParams) *source {
	t.Helper()
	src, err := New(p)
	require.NoError(t, err)
	require.NotNil(t, src)
	s, ok := src.(*source)
	require.True(t, ok)
	return s
}

func TestNewSelectsMode(t *testing.T) {
	t.Parallel()
	p := preprodParams(t)
	p.Mithril.Enabled = false
	src, err := New(p)
	require.NoError(t, err)
	assert.Nil(t, src, "mithril.enabled false must leave Mithril manual")

	p.Mithril.Enabled = true
	assert.False(t, newSource(t, p).SnapshotMode())
	p.HistoryExpiry = true
	assert.True(t, newSource(t, p).SnapshotMode())
}

// Immutable files span one Byron epoch, 10*k slots from the Byron genesis:
// 21600 on preprod. The network name comes from the magic when the config
// names none.
func TestNewFromNetworkConfig(t *testing.T) {
	t.Parallel()
	p := preprodParams(t)
	p.Network = ""
	p.NetworkMagic = 1
	p.Mithril.DownloadIdleTimeout = "45s"
	s := newSource(t, p)
	assert.Equal(t, uint64(21_600), s.chunkSlots)
	assert.Equal(t, uint64(1), s.ImmutableForSlot(43_199))
	assert.Equal(t, uint64(2), s.ImmutableForSlot(43_200))
	assert.Equal(t, "preprod", s.bootstrap.Network)
	assert.Equal(t, p.CardanoNodeConfig.MithrilGenesisVerificationKey,
		s.bootstrap.GenesisVerificationKey)
	assert.Equal(t, 45*time.Second, s.bootstrap.DownloadIdleTimeout)
}

func TestNewRejectsUnverifiableConfig(t *testing.T) {
	t.Parallel()
	p := preprodParams(t)
	p.CardanoNodeConfig = nil
	_, err := New(p)
	require.Error(t, err)

	p = preprodParams(t)
	keyless := *p.CardanoNodeConfig
	keyless.MithrilGenesisVerificationKey = ""
	p.CardanoNodeConfig = &keyless
	_, err = New(p)
	require.ErrorContains(t, err, "genesis verification key")

	p = preprodParams(t)
	p.Network = ""
	p.NetworkMagic = 987654321
	_, err = New(p)
	require.ErrorContains(t, err, "network magic")

	p = preprodParams(t)
	p.Mithril.DownloadIdleTimeout = "soon"
	_, err = New(p)
	require.ErrorContains(t, err, "downloadIdleTimeout")
}

// An aggregator that refuses connections fails the probe promptly instead of
// stalling the check.
func TestCertifiedImmutableAggregatorDown(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	url := "http://" + ln.Addr().String()
	require.NoError(t, ln.Close())
	p := preprodParams(t)
	p.Mithril.AggregatorURL = url
	p.Mithril.AllowInsecureHTTP = true
	s := newSource(t, p)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err = s.CertifiedImmutable(ctx)
	require.Error(t, err)
	require.NoError(t, ctx.Err(), "the probe must fail, not hang")
}

type staleApplier struct{ err error }

func (staleApplier) Tip() ocommon.Point { return ocommon.Point{} }

func (a staleApplier) ApplyTrustedBatch(
	context.Context, ocommon.Point, []gledger.Block,
) (ocommon.Point, error) {
	return ocommon.Point{}, a.err
}

func TestCatchUpStreamConfig(t *testing.T) {
	t.Parallel()
	p := preprodParams(t)
	p.Mithril.DownloadDir = "/downloads"
	s := newSource(t, p)
	var seen mithril.StreamConfig
	errStop := errors.New("stop")
	s.runStream = func(
		_ context.Context, cfg mithril.StreamConfig,
	) (mithril.StreamResult, error) {
		seen = cfg
		return mithril.StreamResult{}, errStop
	}
	applier := &staleApplier{err: ledger.ErrStaleBatch}
	holds := 0
	hold := func() func() {
		holds++
		return func() {}
	}
	require.ErrorIs(t, s.CatchUp(t.Context(), nil, applier, hold), errStop)
	// The runner takes the hold itself, once prepared.
	require.NotNil(t, seen.Hold)
	assert.Zero(t, holds)
	seen.Hold()()
	assert.Equal(t, 1, holds)
	assert.Zero(t, seen.NoProgressTimeout, "the default watchdog applies")

	assert.Equal(t, ledger.MaxTrustedBatchBlocks, seen.MaxBatchBlocks)
	assert.Equal(t, uint64(21_600), seen.ChunkSlots)
	assert.Equal(t, "/data/dir", seen.DataDir)
	assert.Equal(t, 4, seen.BatchChunks)
	assert.Equal(t, 16, seen.Feed.WindowChunks)
	assert.Equal(t, 3, seen.Feed.Workers)
	assert.Equal(t, "/downloads", seen.Feed.Bootstrap.DownloadDir)
	assert.Equal(t, "preprod", seen.Feed.Bootstrap.Network)

	// The runner replays only on its own sentinel; the ledger's must map
	// to it without losing the ledger's.
	_, err := seen.Applier.ApplyTrustedBatch(t.Context(), ocommon.Point{}, nil)
	require.ErrorIs(t, err, mithril.ErrStaleBatch)
	require.ErrorIs(t, err, ledger.ErrStaleBatch)

	applier.err = errors.New("disk full")
	_, err = seen.Applier.ApplyTrustedBatch(t.Context(), ocommon.Point{}, nil)
	require.Error(t, err)
	require.NotErrorIs(t, err, mithril.ErrStaleBatch)
}

func TestUseMithrilAppliesDecide(t *testing.T) {
	t.Parallel()
	s := newSource(t, preprodParams(t))
	d := dingo.MithrilDecision{
		LocalImmutable: 10, TargetImmutable: 500, MinGapChunks: 4,
		OuroborosBPS: 100, MithrilBPS: 120,
	}
	assert.False(t, s.UseMithril(d))
	d.MithrilBPS = 130
	assert.True(t, s.UseMithril(d))
	d.TargetImmutable = 12
	assert.False(t, s.UseMithril(d))
}

// Dev mode forces API storage inside node startup, after config validation
// refused mithril.enabled with API storage, so New checks the mode the node
// actually runs in.
func TestNewRequiresCoreStorage(t *testing.T) {
	t.Parallel()
	p := preprodParams(t)
	p.StorageMode = dingo.StorageModeAPI
	src, err := New(p)
	require.NoError(t, err)
	assert.Nil(t, src, "API storage must leave the node on Ouroboros")
	p.StorageMode = dingo.StorageModeCore
	assert.NotNil(t, newSource(t, p))
}

// Snapshot mode keeps what history expiry keeps: 3k/f slots, from the Shelley
// genesis, which is 129600 on preprod (k=2160, f=1/20).
func TestSnapshotRestoreConfig(t *testing.T) {
	t.Parallel()
	p := preprodParams(t)
	p.HistoryExpiry = true
	s := newSource(t, p)
	require.True(t, s.SnapshotMode())
	assert.Equal(t, uint64(129_600), s.stabilityWindow)

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	state, err := s.SnapshotState(t.Context(), db)
	require.NoError(t, err)
	assert.Equal(t, dingo.MithrilSnapshotFresh, state)

	want := ocommon.NewPoint(42, []byte{4, 2})
	var got mithril.SnapshotConfig
	s.runSnapshot = func(
		_ context.Context, cfg mithril.SnapshotConfig,
	) (ocommon.Point, error) {
		got = cfg
		return want, nil
	}
	tip, err := s.RestoreSnapshot(t.Context(), db)
	require.NoError(t, err)
	assert.Equal(t, want, tip)
	assert.Same(t, db, got.DB)
	assert.Equal(t, uint64(129_600), got.StabilityWindow)
	assert.Equal(t, 16, got.Feed.WindowChunks)
	assert.Equal(t, 3, got.Feed.Workers)
	assert.Equal(t, "/data/dir", got.DataDir)
	assert.Same(t, p.CardanoNodeConfig, got.CardanoNodeConfig)
	assert.Equal(t, "preprod", got.Feed.Bootstrap.Network)
	assert.NotEmpty(t, got.Feed.Bootstrap.AncillaryVerificationKey)

	// A restore the mithril half cannot finish is one the node must not
	// retry.
	s.runSnapshot = func(
		context.Context, mithril.SnapshotConfig,
	) (ocommon.Point, error) {
		return ocommon.Point{}, fmt.Errorf(
			"pinned artifact gone: %w",
			mithril.ErrSnapshotRestoreUnrecoverable,
		)
	}
	_, err = s.RestoreSnapshot(t.Context(), db)
	assert.ErrorIs(t, err, dingo.ErrMithrilRestoreUnrecoverable)
	assert.ErrorIs(t, err, mithril.ErrSnapshotRestoreUnrecoverable)

	// An unavailable aggregator is one the node waits out without counting
	// it against the restore.
	s.runSnapshot = func(
		context.Context, mithril.SnapshotConfig,
	) (ocommon.Point, error) {
		return ocommon.Point{}, fmt.Errorf(
			"aggregator down: %w", mithril.ErrSnapshotRestoreUnavailable,
		)
	}
	_, err = s.RestoreSnapshot(t.Context(), db)
	assert.ErrorIs(t, err, dingo.ErrMithrilRestoreUnavailable)
	assert.NotErrorIs(t, err, dingo.ErrMithrilRestoreUnrecoverable)
}

// The node's registry reaches every feed and run the source configures; with
// none, the source carries no metrics.
func TestNewPipelineMetrics(t *testing.T) {
	t.Parallel()
	p := preprodParams(t)
	require.Nil(t, newSource(t, p).bootstrap.Metrics)

	reg := prometheus.NewRegistry()
	p.PromRegistry = reg
	s := newSource(t, p)
	require.NotNil(t, s.bootstrap.Metrics)
	require.Same(
		t,
		s.bootstrap.Metrics,
		s.streamConfig(nil, nil).Feed.Bootstrap.Metrics,
	)
	require.Same(
		t,
		s.bootstrap.Metrics,
		s.snapshotConfig(nil).Feed.Bootstrap.Metrics,
	)
	families, err := reg.Gather()
	require.NoError(t, err)
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	assert.Contains(t, names, "dingo_mithril_chunks_downloaded_total")
}

// One certificate cache serves the poll, every stream run and the snapshot
// restore for the source's lifetime, so a poll after a run verifies only the
// certificates the aggregator added since.
func TestNewSharesCertificateCache(t *testing.T) {
	t.Parallel()
	s := newSource(t, preprodParams(t))
	require.NotNil(t, s.bootstrap.CertificateCache)
	require.Same(
		t,
		s.bootstrap.CertificateCache,
		s.streamConfig(nil, nil).Feed.Bootstrap.CertificateCache,
	)
	require.Same(
		t,
		s.bootstrap.CertificateCache,
		s.snapshotConfig(nil).Feed.Bootstrap.CertificateCache,
	)
}
