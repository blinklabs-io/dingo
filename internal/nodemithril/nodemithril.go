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

// Package nodemithril implements dingo.MithrilCatchUp on the mithril
// package. The root dingo package cannot import mithril, which depends on
// internal/node, so the serve command passes New to the node through
// dingo.WithMithrilCatchUp.
package nodemithril

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mithril"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type source struct {
	mode       mithril.Mode
	cfg        internalconfig.MithrilConfig
	bootstrap  mithril.BootstrapConfig
	chunkSlots uint64
	// stabilityWindow is the history-expiry retention, in snapshot mode.
	stabilityWindow uint64
	nodeConfig      *cardano.CardanoNodeConfig
	dataDir         string
	logger          *slog.Logger

	latestTarget func(
		context.Context, mithril.BootstrapConfig,
	) (mithril.CertifiedTarget, error)
	runStream func(
		context.Context, mithril.StreamConfig,
	) (mithril.StreamResult, error)
	runSnapshot func(
		context.Context, mithril.SnapshotConfig,
	) (ocommon.Point, error)
}

// New is a dingo.MithrilCatchUpFactory. It returns nil when mithril.enabled
// is false or the node does not run in core storage mode, and an error when
// the node cannot verify Mithril data: no Cardano network config, no Mithril
// genesis verification key, no Byron genesis to size immutable files by, or a
// network with no known name.
func New(p dingo.MithrilCatchUpParams) (dingo.MithrilCatchUp, error) {
	mode := mithril.SelectMode(p.Mithril.Enabled, p.HistoryExpiry)
	if mode == mithril.ModeManual {
		return nil, nil
	}
	// Config validation refuses mithril.enabled with API storage, but dev
	// mode switches to API storage after it, so the effective mode decides.
	if p.StorageMode != dingo.StorageModeCore {
		logger := p.Logger
		if logger == nil {
			logger = slog.Default()
		}
		logger.Warn(
			"mithril: node-managed catch-up needs core storage, "+
				"syncing from Ouroboros only",
			"component", "mithril",
			"storage_mode", string(p.StorageMode),
		)
		return nil, nil
	}
	if p.CardanoNodeConfig == nil {
		return nil, errors.New("no Cardano network config")
	}
	key := p.CardanoNodeConfig.MithrilGenesisVerificationKey
	if strings.TrimSpace(key) == "" {
		return nil, errors.New(
			"the Cardano network config has no Mithril genesis verification key",
		)
	}
	// The ImmutableDB gives every file one Byron epoch, 10*k slots from the
	// Byron genesis, on every Cardano network (ledger/envelope_validation.go
	// byronEpochSlots reads k the same way).
	byronGenesis := p.CardanoNodeConfig.ByronGenesis()
	if byronGenesis == nil || byronGenesis.ProtocolConsts.K <= 0 {
		return nil, errors.New(
			"the Cardano network config has no Byron genesis security parameter",
		)
	}
	// Snapshot mode keeps the history the history-expiry pruner keeps,
	// which is measured by the ledger's stability window. A restored ledger
	// state is always Shelley or later.
	var stabilityWindow uint64
	if mode == mithril.ModeSnapshot {
		var err error
		stabilityWindow, err = ledger.ShelleyStabilityWindow(
			p.CardanoNodeConfig.ShelleyGenesis(),
		)
		if err != nil {
			return nil, fmt.Errorf(
				"sizing the history a snapshot restore keeps: %w", err,
			)
		}
	}
	network := p.Network
	if network == "" {
		known, ok := ouroboros.NetworkByNetworkMagic(p.NetworkMagic)
		if !ok {
			return nil, fmt.Errorf(
				"no Mithril network name for network magic %d", p.NetworkMagic,
			)
		}
		network = known.Name
	}
	var idle time.Duration
	if p.Mithril.DownloadIdleTimeout != "" {
		var err error
		idle, err = time.ParseDuration(p.Mithril.DownloadIdleTimeout)
		if err != nil {
			return nil, fmt.Errorf(
				"parsing mithril.downloadIdleTimeout: %w",
				err,
			)
		}
	}
	logger := p.Logger
	if logger == nil {
		logger = slog.Default()
	}
	// Metrics are an aid, not a reason to leave Mithril off.
	metrics, err := mithril.NewPipelineMetrics(p.PromRegistry)
	if err != nil {
		logger.Warn(
			"mithril: pipeline metrics unavailable",
			"component", "mithril",
			"error", err,
		)
	}
	return &source{
		mode: mode,
		cfg:  p.Mithril,
		bootstrap: mithril.BootstrapConfig{
			Network:                  network,
			Backend:                  p.Mithril.Backend,
			AggregatorURL:            p.Mithril.AggregatorURL,
			AllowInsecureHTTP:        p.Mithril.AllowInsecureHTTP,
			DownloadDir:              p.Mithril.DownloadDir,
			GenesisVerificationKey:   key,
			AncillaryVerificationKey: p.CardanoNodeConfig.MithrilGenesisAncillaryVerificationKey,
			Logger:                   logger,
			DownloadIdleTimeout:      idle,
			DownloadMaxIdleRetries:   p.Mithril.DownloadMaxIdleRetries,
			DownloadMaxBytes:         p.Mithril.DownloadMaxBytes,
			Metrics:                  metrics,
			// Polls, runs and restores share it for the process lifetime,
			// so each verifies only the certificates added since.
			CertificateCache: mithril.NewCertificateCache(),
		},
		chunkSlots:      10 * uint64(byronGenesis.ProtocolConsts.K),
		stabilityWindow: stabilityWindow,
		nodeConfig:      p.CardanoNodeConfig,
		dataDir:         p.DataDir,
		logger:          logger,
		latestTarget:    mithril.LatestCertifiedTarget,
		runStream:       mithril.RunStreamCatchUp,
		runSnapshot:     mithril.RunSnapshotBackfill,
	}, nil
}

func (s *source) SnapshotMode() bool {
	return s.mode == mithril.ModeSnapshot
}

func (s *source) CertifiedImmutable(ctx context.Context) (uint64, error) {
	target, err := s.latestTarget(ctx, s.bootstrap)
	if err != nil {
		return 0, err
	}
	return target.Immutable, nil
}

// ImmutableForSlot uses the estimate the stream runner starts from, so the
// decision and the run agree on where the tip is.
func (s *source) ImmutableForSlot(slot uint64) uint64 {
	file, _ := mithril.ImmutableFileForSlot(slot, s.chunkSlots)
	return file
}

func (s *source) UseMithril(d dingo.MithrilDecision) bool {
	return mithril.Decide(mithril.DecisionInput{
		LocalImmutable: d.LocalImmutable,
		Target:         mithril.CertifiedTarget{Immutable: d.TargetImmutable},
		OuroborosBPS:   d.OuroborosBPS,
		MithrilBPS:     d.MithrilBPS,
		MinGapChunks:   d.MinGapChunks,
	})
}

func (s *source) CatchUp(
	ctx context.Context,
	db *database.Database,
	applier dingo.TrustedBatchApplier,
	hold func() (release func()),
) error {
	cfg := s.streamConfig(db, applier)
	cfg.Hold = hold
	res, err := s.runStream(ctx, cfg)
	s.logger.Info(
		"mithril: stream catch-up stopped",
		"component", "mithril",
		"immutable", res.Immutable,
		"tip_slot", res.Tip.Slot,
		"restarts", res.Restarts,
		"error", err,
	)
	return err
}

func (s *source) streamConfig(
	db *database.Database,
	applier dingo.TrustedBatchApplier,
) mithril.StreamConfig {
	return mithril.StreamConfig{
		Feed: mithril.ChunkFeedConfig{
			Bootstrap:    s.bootstrap,
			WindowChunks: s.cfg.WindowChunks,
			Workers:      s.cfg.DownloadWorkers,
		},
		DataDir:        s.dataDir,
		ChunkSlots:     s.chunkSlots,
		BatchChunks:    s.cfg.BatchChunks,
		MaxBatchBlocks: ledger.MaxTrustedBatchBlocks,
		DB:             db,
		Applier:        staleBatchApplier{applier},
		Logger:         s.logger,
	}
}

func (s *source) SnapshotState(
	ctx context.Context,
	db *database.Database,
) (dingo.MithrilSnapshotState, error) {
	state, err := mithril.SnapshotStartupState(ctx, db)
	if err != nil {
		return 0, err
	}
	switch state {
	case mithril.SnapshotStartupFresh:
		return dingo.MithrilSnapshotFresh, nil
	case mithril.SnapshotStartupResume:
		return dingo.MithrilSnapshotInterrupted, nil
	case mithril.SnapshotStartupPopulated:
		return dingo.MithrilSnapshotPopulated, nil
	default:
		return 0, fmt.Errorf("unknown snapshot startup state %d", state)
	}
}

func (s *source) RestoreSnapshot(
	ctx context.Context,
	db *database.Database,
) (ocommon.Point, error) {
	tip, err := s.runSnapshot(ctx, s.snapshotConfig(db))
	switch {
	case errors.Is(err, mithril.ErrSnapshotRestoreUnrecoverable):
		err = fmt.Errorf("%w: %w", dingo.ErrMithrilRestoreUnrecoverable, err)
	case errors.Is(err, mithril.ErrSnapshotRestoreUnavailable):
		err = fmt.Errorf("%w: %w", dingo.ErrMithrilRestoreUnavailable, err)
	}
	return tip, err
}

func (s *source) SnapshotProgress(
	ctx context.Context,
	db *database.Database,
) (string, error) {
	return mithril.SnapshotRestoreProgress(ctx, db)
}

func (s *source) snapshotConfig(db *database.Database) mithril.SnapshotConfig {
	return mithril.SnapshotConfig{
		Feed: mithril.ChunkFeedConfig{
			Bootstrap:    s.bootstrap,
			WindowChunks: s.cfg.WindowChunks,
			Workers:      s.cfg.DownloadWorkers,
		},
		DB:                db,
		StabilityWindow:   s.stabilityWindow,
		CardanoNodeConfig: s.nodeConfig,
		DataDir:           s.dataDir,
		Logger:            s.logger,
	}
}

// staleBatchApplier reports the ledger's stale-tip refusal as the runner's,
// so the runner replays from the new tip instead of falling back.
type staleBatchApplier struct {
	dingo.TrustedBatchApplier
}

func (a staleBatchApplier) ApplyTrustedBatch(
	ctx context.Context,
	expected ocommon.Point,
	blocks []gledger.Block,
) (ocommon.Point, error) {
	tip, err := a.TrustedBatchApplier.ApplyTrustedBatch(ctx, expected, blocks)
	if errors.Is(err, ledger.ErrStaleBatch) &&
		!errors.Is(err, mithril.ErrStaleBatch) {
		err = fmt.Errorf("%w: %w", mithril.ErrStaleBatch, err)
	}
	return tip, err
}
