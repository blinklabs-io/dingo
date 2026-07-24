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

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"time"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/dingo/mithril"
	"github.com/spf13/cobra"
)

const (
	syncStatusInProgress = "in_progress"
	syncStatusBackfill   = "backfill"
)

func mithrilCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mithril",
		Short: "Mithril snapshot management commands",
	}

	cmd.AddCommand(mithrilListCommand())
	cmd.AddCommand(mithrilShowCommand())
	cmd.AddCommand(mithrilSyncCommand())

	return cmd
}

// resolveAggregatorURL returns the Mithril aggregator URL from
// config, falling back to the network default.
func resolveAggregatorURL(
	cfgURL string,
	network string,
) (string, error) {
	if cfgURL != "" {
		return cfgURL, nil
	}
	url, err := mithril.AggregatorURLForNetwork(network)
	if err != nil {
		return "", fmt.Errorf(
			"resolving aggregator URL for network %s: %w",
			network, err,
		)
	}
	return url, nil
}

// resolveMithrilBackend normalizes the configured Mithril artifact
// backend, applying the node default (v2) when unset. The accepted set
// comes from mithril.AcceptedBackends so a backend added there is
// recognized here without a manual update.
func resolveMithrilBackend(backend string) (string, error) {
	if backend == "" {
		return mithril.BackendV2, nil
	}
	if slices.Contains(mithril.AcceptedBackends(), backend) {
		return backend, nil
	}
	return "", fmt.Errorf(
		"unsupported Mithril backend %q (expected one of %q)",
		backend,
		mithril.AcceptedBackends(),
	)
}

func mithrilListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List available Mithril snapshots",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				return errors.New("no config found in context")
			}

			network := cfg.Network
			if network == "" {
				network = "preview"
			}

			aggregatorURL, err := resolveAggregatorURL(
				cfg.Mithril.AggregatorURL, network,
			)
			if err != nil {
				return err
			}

			backend, err := resolveMithrilBackend(
				cfg.Mithril.Backend,
			)
			if err != nil {
				return err
			}

			client := mithril.NewClient(aggregatorURL)
			if backend == mithril.BackendV2 {
				return runMithrilListV2(cmd.Context(), client)
			}
			snapshots, err := client.ListSnapshots(
				cmd.Context(),
			)
			if err != nil {
				return fmt.Errorf("listing snapshots: %w", err)
			}

			if len(snapshots) == 0 {
				fmt.Println("No snapshots available.")
				return nil
			}

			fmt.Printf(
				"%-16s  %-8s  %-8s  %12s  %s\n",
				"DIGEST",
				"EPOCH",
				"IMMUT#",
				"SIZE",
				"CREATED",
			)
			for _, s := range snapshots {
				digest := s.Digest
				if len(digest) > 16 {
					digest = digest[:16]
				}
				created := s.CreatedAt
				if len(created) > 19 {
					created = created[:19]
				}
				fmt.Printf(
					"%-16s  %-8d  %-8d  %12s  %s\n",
					digest,
					s.Beacon.Epoch,
					s.Beacon.ImmutableFileNumber,
					mithril.HumanBytes(s.Size),
					created,
				)
			}

			return nil
		},
	}
	return cmd
}

// runMithrilListV2 lists Cardano database (v2) artifacts.
func runMithrilListV2(ctx context.Context, client *mithril.Client) error {
	items, err := client.ListCardanoDatabaseSnapshots(ctx)
	if err != nil {
		return fmt.Errorf("listing Cardano database snapshots: %w", err)
	}
	if len(items) == 0 {
		fmt.Println("No Cardano database snapshots available.")
		return nil
	}
	fmt.Printf(
		"%-16s  %-8s  %-8s  %12s  %s\n",
		"HASH",
		"EPOCH",
		"IMMUT#",
		"SIZE",
		"CREATED",
	)
	for _, s := range items {
		hash := s.Hash
		if len(hash) > 16 {
			hash = hash[:16]
		}
		created := s.CreatedAt
		if len(created) > 19 {
			created = created[:19]
		}
		fmt.Printf(
			"%-16s  %-8d  %-8d  %12s  %s\n",
			hash,
			s.Beacon.Epoch,
			s.Beacon.ImmutableFileNumber,
			mithril.HumanBytes(s.TotalDbSizeUncompressed),
			created,
		)
	}
	return nil
}

func mithrilShowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show <hash>",
		Short: "Show details of a specific Mithril snapshot",
		Long: "Show details of a specific Mithril snapshot.\n\n" +
			"With the default v2 backend, pass the Cardano database artifact hash. " +
			"With the legacy v1 backend, pass the snapshot digest.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				return errors.New("no config found in context")
			}

			network := cfg.Network
			if network == "" {
				network = "preview"
			}

			aggregatorURL, err := resolveAggregatorURL(
				cfg.Mithril.AggregatorURL, network,
			)
			if err != nil {
				return err
			}

			backend, err := resolveMithrilBackend(
				cfg.Mithril.Backend,
			)
			if err != nil {
				return err
			}

			client := mithril.NewClient(aggregatorURL)
			if backend == mithril.BackendV2 {
				return runMithrilShowV2(
					cmd.Context(), client, args[0],
				)
			}
			snapshot, err := client.GetSnapshot(
				cmd.Context(),
				args[0],
			)
			if err != nil {
				return fmt.Errorf("getting snapshot: %w", err)
			}

			fmt.Printf("Digest:                %s\n", snapshot.Digest)
			fmt.Printf("Network:               %s\n", snapshot.Network)
			fmt.Printf(
				"Epoch:                 %d\n",
				snapshot.Beacon.Epoch,
			)
			fmt.Printf(
				"Immutable File Number: %d\n",
				snapshot.Beacon.ImmutableFileNumber,
			)
			fmt.Printf("Size:                  %s\n", mithril.HumanBytes(snapshot.Size))
			fmt.Printf(
				"Certificate Hash:      %s\n",
				snapshot.CertificateHash,
			)
			fmt.Printf(
				"Compression:           %s\n",
				snapshot.CompressionAlgorithm,
			)
			fmt.Printf(
				"Cardano Node Version:  %s\n",
				snapshot.CardanoNodeVersion,
			)
			fmt.Printf("Created:               %s\n", snapshot.CreatedAt)
			fmt.Println("Locations:")
			for _, loc := range snapshot.Locations {
				fmt.Printf("  - %s\n", loc)
			}

			return nil
		},
	}
	return cmd
}

// runMithrilShowV2 shows the details of a Cardano database (v2)
// artifact identified by its hash.
func runMithrilShowV2(
	ctx context.Context,
	client *mithril.Client,
	hash string,
) error {
	snapshot, err := client.GetCardanoDatabaseSnapshot(ctx, hash)
	if err != nil {
		return fmt.Errorf("getting Cardano database snapshot: %w", err)
	}

	fmt.Printf("Hash:                  %s\n", snapshot.Hash)
	fmt.Printf("Merkle Root:           %s\n", snapshot.MerkleRoot)
	fmt.Printf("Network:               %s\n", snapshot.Network)
	fmt.Printf(
		"Epoch:                 %d\n",
		snapshot.Beacon.Epoch,
	)
	fmt.Printf(
		"Immutable File Number: %d\n",
		snapshot.Beacon.ImmutableFileNumber,
	)
	fmt.Printf(
		"Total DB Size:         %s\n",
		mithril.HumanBytes(snapshot.TotalDbSizeUncompressed),
	)
	fmt.Printf(
		"Certificate Hash:      %s\n",
		snapshot.CertificateHash,
	)
	fmt.Printf(
		"Cardano Node Version:  %s\n",
		snapshot.CardanoNodeVersion,
	)
	fmt.Printf("Created:               %s\n", snapshot.CreatedAt)
	printLocations := func(label string, locs []mithril.CardanoDatabaseLocation) {
		fmt.Printf("%s Locations:\n", label)
		for _, loc := range locs {
			uri := loc.URI
			if uri == "" {
				uri = loc.URITemplate
			}
			fmt.Printf("  - [%s] %s\n", loc.Type, uri)
		}
	}
	printLocations("Digest", snapshot.Digests.Locations)
	printLocations("Immutable", snapshot.Immutables.Locations)
	printLocations("Ancillary", snapshot.Ancillary.Locations)

	return nil
}

func mithrilSyncCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Download latest Mithril snapshot and load into database",
		Long: `Download the latest Mithril snapshot from the aggregator,
extract it, and load the ImmutableDB blocks into the local database.
This is the fastest way to bootstrap a new node.`,
		RunE: mithrilSyncRunE,
	}
	return cmd
}

// mithrilSyncRunE is the shared RunE for both the "mithril sync"
// subcommand and the "sync --mithril" convenience command.
func mithrilSyncRunE(
	cmd *cobra.Command,
	_ []string,
) error {
	cfg := config.FromContext(cmd.Context())
	if cfg == nil {
		return errors.New("no config found in context")
	}
	// CIP-0163: a Mithril bootstrap cannot carry reward-account expiration
	// state (see errMithrilInactivityIncompatible), so refuse before touching
	// the network or the database.
	if cfg.DelegatorInactivityEnabled {
		return errMithrilInactivityIncompatible()
	}
	logger := commonRun(cfg)
	network := cfg.Network
	if network == "" {
		network = "preview"
	}
	return runMithrilSync(cmd.Context(), cfg, logger, network)
}

// errMithrilInactivityIncompatible reports why Mithril bootstrap and the
// CIP-0163 delegator-inactivity gate cannot be combined on the same node.
// Account.ExpirationEpoch is dingo-only ledger state with no representation in
// the cardano-ledger Mithril snapshot dingo imports, and it cannot be
// reconstructed from post-import witness history (a long-inactive account --
// exactly what CIP-0163 targets -- may have last witnessed before the import
// point). A Mithril-bootstrapped node would therefore compute different
// expiry-dependent stake, rewards, and governance than a genesis-synced node.
func errMithrilInactivityIncompatible() error {
	return errors.New(
		"cannot use Mithril bootstrap when delegatorInactivityEnabled " +
			"(CIP-0163) is set: reward-account expiration state is absent from " +
			"the cardano-ledger Mithril snapshot and cannot be reconstructed " +
			"after import, so a Mithril-bootstrapped node would diverge from a " +
			"genesis-synced one; sync from genesis instead",
	)
}

func runMithrilSync(
	ctx context.Context,
	cfg *config.Config,
	logger *slog.Logger,
	network string,
) (err error) {
	metrics, metricsHandler := newMithrilSyncMetricsHandler(network)
	metricsServer, err := startPrometheusMetricsServerWithHandler(
		logger,
		cfg.BindAddr,
		cfg.MetricsPort,
		"mithril",
		metricsHandler,
	)
	if err != nil {
		metrics.recordError()
		logger.Warn(
			"failed to start prometheus metrics server; continuing",
			"component", "mithril",
			"port", cfg.MetricsPort,
			"error", err,
		)
		err = nil
	}
	defer func() {
		if err != nil {
			metrics.recordError()
		}
		if metricsServer == nil {
			return
		}
		shutdownCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			5*time.Second,
		)
		defer cancel()
		if shutdownErr := metricsServer.Shutdown(shutdownCtx); shutdownErr != nil {
			logger.Warn(
				"failed to stop prometheus metrics server",
				"component", "mithril",
				"error", shutdownErr,
			)
		}
	}()
	if metricsServer != nil {
		go func() {
			if serverErr := <-metricsServer.Err(); serverErr != nil {
				logger.Error(
					"prometheus metrics server stopped",
					"component", "mithril",
					"error", serverErr,
				)
			}
		}()
	}
	debugServer, debugErr := startDebugPprofServer(
		logger,
		cfg.BindAddr,
		cfg.DebugPort,
		"mithril",
	)
	if debugErr != nil {
		logger.Warn(
			"failed to start pprof debug server; continuing",
			"component", "mithril",
			"port", cfg.DebugPort,
			"error", debugErr,
		)
	}
	defer func() {
		if debugServer == nil {
			return
		}
		shutdownCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			5*time.Second,
		)
		defer cancel()
		if shutdownErr := debugServer.Shutdown(shutdownCtx); shutdownErr != nil {
			logger.Warn(
				"failed to stop pprof debug server",
				"component", "mithril",
				"error", shutdownErr,
			)
		}
	}()
	if debugServer != nil {
		go func() {
			if serverErr := <-debugServer.Err(); serverErr != nil {
				logger.Error(
					"pprof debug server stopped",
					"component", "mithril",
					"error", serverErr,
				)
			}
		}()
	}

	// Adapt SyncProgress onto the Prometheus metrics.
	onProgress := func(p mithril.SyncProgress) {
		metrics.setPhaseActive(string(p.Phase), p.Active)
		switch p.Phase {
		case mithril.PhaseBootstrap:
			metrics.recordDownloadProgress(mithril.DownloadProgress{
				BytesDownloaded: p.BytesDownloaded,
				TotalBytes:      p.TotalBytes,
				BytesPerSecond:  p.BytesPerSecond,
			})
		case mithril.PhaseImmutableCopy:
			metrics.recordImmutableProgress(node.LoadBlobsProgress{
				BlocksCopied:    p.Count,
				CurrentSlot:     p.CurrentSlot,
				TipSlot:         p.TipSlot,
				Percent:         p.Percent,
				BlocksPerSecond: p.BytesPerSecond,
			})
		case mithril.PhaseGapBlocks:
			metrics.recordGapBlocks(p.Count)
		case mithril.PhaseBackfill:
			metrics.recordBackfillProgress(node.BackfillProgress{
				Slot:            p.CurrentSlot,
				TipSlot:         p.TipSlot,
				ProgressPercent: p.Percent,
				BlocksPerSecond: p.BytesPerSecond,
				ProcessedBlocks: p.Count,
			})
		case mithril.PhaseComplete:
			metrics.markComplete()
		case mithril.PhaseLedgerImport:
			if p.CurrentSlot > 0 {
				metrics.recordLedgerStateSlot(p.CurrentSlot)
			}
			if p.Description != "" || p.Percent > 0 {
				metrics.recordLedgerImportProgress(ledgerstate.ImportProgress{
					Stage:   p.Description,
					Current: p.Count,
					Total:   p.Total,
					Percent: p.Percent,
				})
			}
		case mithril.PhaseIndexRebuild:
			// SyncProgress carries the rebuild duration as Description = d.String(),
			// which is guaranteed round-trippable by time.ParseDuration.
			if d, err := time.ParseDuration(p.Description); err == nil && d > 0 {
				metrics.recordIndexRebuildDuration(d)
			}
		case mithril.PhasePostLedger:
			// Phase-active gauge is updated above; no additional
			// per-phase Prometheus counter for this phase in the CLI.
		}
	}

	backend, err := resolveMithrilBackend(cfg.Mithril.Backend)
	if err != nil {
		return err
	}

	res, err := mithril.Sync(ctx, mithril.SyncConfig{
		Network:                network,
		DataDir:                cfg.DatabasePath,
		StorageMode:            cfg.StorageMode,
		CardanoConfigPath:      cfg.CardanoConfig,
		Backend:                backend,
		AggregatorURL:          cfg.Mithril.AggregatorURL,
		DownloadDir:            cfg.Mithril.DownloadDir,
		DownloadIdleTimeout:    cfg.Mithril.DownloadIdleTimeout,
		DownloadMaxIdleRetries: cfg.Mithril.DownloadMaxIdleRetries,
		VerifyCertChain:        cfg.Mithril.VerifyCertificates,
		CleanupAfterLoad:       cfg.Mithril.CleanupAfterLoad,
		StoragePlugins: mithril.StoragePlugins{
			Blob:     cfg.Plugins.Storage.Blob,
			Metadata: cfg.Plugins.Storage.Metadata,
		},
		RunMode:           string(config.RunModeLoad),
		BackfillBatchSize: cfg.BackfillBatchSize,
		DatabaseWorkers:   cfg.DatabaseWorkers,
		Logger:            logger,
		OnProgress:        onProgress,
	})
	if err != nil {
		return err
	}
	if res.Snapshot != nil {
		metrics.recordSnapshot(res.Snapshot)
	}
	return nil
}

// syncCommand creates the "sync" command with --mithril flag at the
// root level for convenience.
func syncCommand() *cobra.Command {
	var useMithril bool
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Synchronize node data",
		Long: `Synchronize the node database. Use --mithril to bootstrap
from a Mithril snapshot for fast initial sync.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !useMithril {
				fmt.Fprintln(
					os.Stderr,
					"Please specify a sync method."+
						" Use --mithril for"+
						" Mithril snapshot sync.",
				)
				_ = cmd.Usage()
				return errors.New("no sync method specified")
			}
			return mithrilSyncRunE(cmd, args)
		},
	}
	cmd.Flags().BoolVar(
		&useMithril,
		"mithril",
		false,
		"use Mithril snapshot for fast initial sync",
	)
	return cmd
}
