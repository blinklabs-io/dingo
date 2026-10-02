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
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/version"
	"github.com/blinklabs-io/dingo/mithril"
	"github.com/spf13/cobra"
)

func mithrilSnapshotCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "snapshot",
		Short: "Produce Mithril snapshot artifacts",
	}
	cmd.AddCommand(mithrilSnapshotCreateCommand())
	return cmd
}

func mithrilSnapshotCreateCommand() *cobra.Command {
	var dbDir string
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a Mithril snapshot from a cardano-node database",
		Long: `Create a Mithril Cardano database (v2) artifact from a sealed
cardano-node database directory (immutable/ and ledger/) and write it to
mithril.server.artifactStore. The output is deterministic: running it again
over the same directory produces identical archives and the same hash.
When mithril.server.keepSnapshots is set, older snapshots are then removed.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				return errors.New("no config found in context")
			}
			logger, err := commonRun(cfg)
			if err != nil {
				return err
			}
			artifact, err := runMithrilSnapshotCreate(
				cmd.Context(), cfg, dbDir, logger,
			)
			if err != nil {
				return err
			}
			fmt.Println(artifact.Hash)
			return nil
		},
	}
	cmd.Flags().StringVar(
		&dbDir, "db-dir", "", "cardano-node database directory (required)",
	)
	_ = cmd.MarkFlagRequired("db-dir")
	return cmd
}

// runMithrilSnapshotCreate produces a snapshot of dbDir into the configured
// artifact store and applies the configured retention.
func runMithrilSnapshotCreate(
	ctx context.Context,
	cfg *config.Config,
	dbDir string,
	logger *slog.Logger,
) (*mithril.CardanoDatabaseSnapshot, error) {
	server := cfg.Mithril.Server
	if server.AncillarySigningKeyFile == "" {
		return nil, errors.New(
			"mithril.server.ancillarySigningKeyFile is required",
		)
	}
	keyData, err := os.ReadFile(server.AncillarySigningKeyFile)
	if err != nil {
		return nil, fmt.Errorf("reading ancillary signing key: %w", err)
	}
	key, err := mithril.ParseAncillarySigningKey(string(keyData))
	if err != nil {
		return nil, err
	}
	store, err := openMithrilArtifactStore(ctx, cfg)
	if err != nil {
		return nil, err
	}
	network := cfg.Network
	if network == "" {
		network = "preview"
	}
	artifact, err := mithril.CreateSnapshot(ctx, mithril.CreateSnapshotConfig{
		Network:             network,
		DBDir:               dbDir,
		AncillarySigningKey: key,
		CardanoNodeVersion:  "dingo " + version.GetVersionString(),
		Store:               store,
		Logger:              logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating snapshot: %w", err)
	}
	removed, err := mithril.PruneSnapshots(ctx, store, server.KeepSnapshots)
	if err != nil {
		return nil, fmt.Errorf("applying snapshot retention: %w", err)
	}
	for _, hash := range removed {
		logger.Info(
			"expired snapshot removed",
			"component", "mithril",
			"hash", hash,
		)
	}
	return artifact, nil
}

func openMithrilArtifactStore(
	ctx context.Context,
	cfg *config.Config,
) (mithril.ArtifactStore, error) {
	location := cfg.Mithril.Server.ArtifactStore
	if location == "" {
		return nil, errors.New("mithril.server.artifactStore is required")
	}
	return mithril.OpenArtifactStore(ctx, location)
}

func mithrilServeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Serve Mithril snapshot artifacts over HTTP",
		Long: `Serve the snapshots in mithril.server.artifactStore through the
Mithril aggregator artifact API, with range request support, on the shared
bindAddr and mithril.server.port. The endpoint is public and unauthenticated.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				return errors.New("no config found in context")
			}
			logger, err := commonRun(cfg)
			if err != nil {
				return err
			}
			srv, err := newMithrilServer(cmd.Context(), cfg, logger)
			if err != nil {
				return err
			}
			ln, err := net.Listen("tcp", srv.Addr)
			if err != nil {
				return fmt.Errorf("listening on %s: %w", srv.Addr, err)
			}
			logger.Info(
				"serving Mithril snapshots on "+ln.Addr().String(),
				"component", "mithril",
			)
			return serveMithril(
				cmd.Context(), srv, ln, cfg.Mithril.Server.TLSEnabled,
				cfg.TlsCertFilePath, cfg.TlsKeyFilePath,
			)
		},
	}
}

// newMithrilServer builds the artifact server, bound to the shared bindAddr.
func newMithrilServer(
	ctx context.Context,
	cfg *config.Config,
	logger *slog.Logger,
) (*http.Server, error) {
	server := cfg.Mithril.Server
	if server.Port == 0 {
		return nil, errors.New("mithril.server.port must be set")
	}
	if server.TLSEnabled &&
		(cfg.TlsCertFilePath == "" || cfg.TlsKeyFilePath == "") {
		return nil, errors.New(
			"mithril.server.tlsEnabled requires tlsCertFilePath and " +
				"tlsKeyFilePath",
		)
	}
	store, err := openMithrilArtifactStore(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &http.Server{
		Addr: net.JoinHostPort(
			cfg.BindAddr, strconv.FormatUint(uint64(server.Port), 10),
		),
		Handler: mithril.NewServerHandler(mithril.ServerConfig{
			Store:           store,
			RedirectBaseURL: server.RedirectBaseURL,
			Logger:          logger,
		}),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}, nil
}

// serveMithril serves srv on ln until ctx is done, then shuts it down. The
// listener is passed in so the bound address is known before serving starts.
func serveMithril(
	ctx context.Context,
	srv *http.Server,
	ln net.Listener,
	useTLS bool,
	certFile, keyFile string,
) error {
	errCh := make(chan error, 1)
	go func() {
		if useTLS {
			errCh <- srv.ServeTLS(ln, certFile, keyFile)
			return
		}
		errCh <- srv.Serve(ln)
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), 30*time.Second,
	)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutting down Mithril server: %w", err)
	}
	if err := <-errCh; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
