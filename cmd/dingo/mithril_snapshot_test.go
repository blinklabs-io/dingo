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
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/mithril"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var discardLogger = slog.New(slog.DiscardHandler)

// snapshotTestConfig returns a config producing into a fresh store with a
// freshly generated signing key.
func snapshotTestConfig(t *testing.T) (*config.Config, string) {
	t.Helper()
	dir := t.TempDir()
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	keyFile := filepath.Join(dir, "ancillary.skey")
	require.NoError(t, os.WriteFile(
		keyFile, []byte(hex.EncodeToString(key.Seed())), 0o600,
	))
	cfg := &config.Config{Network: "preprod", BindAddr: "127.0.0.1"}
	cfg.Mithril.Server = config.MithrilServerConfig{
		Port:                    8080,
		ArtifactStore:           filepath.Join(dir, "store"),
		AncillarySigningKeyFile: keyFile,
	}
	return cfg, dir
}

// cardanoDB writes a database directory with the given number of immutable
// trios and a real ledger state snapshot.
func cardanoDB(t *testing.T, trios int) string {
	t.Helper()
	db := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(db, "immutable"), 0o750))
	for n := range trios {
		for _, ext := range []string{"chunk", "primary", "secondary"} {
			require.NoError(t, os.WriteFile(
				filepath.Join(db, "immutable", fmt.Sprintf("%05d.%s", n, ext)),
				fmt.Appendf(nil, "trio %d %s", n, ext),
				0o640,
			))
		}
	}
	state, err := os.ReadFile(
		"../../ledgerstate/testdata/devnet-ledger-snapshot-epoch4.cbor",
	)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Join(db, "ledger", "100"), 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(db, "ledger", "100", "state"), state, 0o640,
	))
	return db
}

func TestRunMithrilSnapshotCreateAppliesRetention(t *testing.T) {
	t.Parallel()

	cfg, _ := snapshotTestConfig(t)
	cfg.Mithril.Server.KeepSnapshots = 1

	first, err := runMithrilSnapshotCreate(
		t.Context(), cfg, cardanoDB(t, 1), discardLogger,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), first.Beacon.Epoch, "epoch from ledger state")
	assert.Equal(t, "preprod", first.Network)

	second, err := runMithrilSnapshotCreate(
		t.Context(), cfg, cardanoDB(t, 2), discardLogger,
	)
	require.NoError(t, err)

	store, err := mithril.OpenArtifactStore(
		t.Context(), cfg.Mithril.Server.ArtifactStore,
	)
	require.NoError(t, err)
	snapshots, err := mithril.ListSnapshots(t.Context(), store)
	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	assert.Equal(t, second.Hash, snapshots[0].Hash)
}

func TestRunMithrilSnapshotCreateRequiresSettings(t *testing.T) {
	t.Parallel()

	for name, mutate := range map[string]func(*config.Config){
		"signing key": func(c *config.Config) {
			c.Mithril.Server.AncillarySigningKeyFile = ""
		},
		"unreadable signing key": func(c *config.Config) {
			c.Mithril.Server.AncillarySigningKeyFile = "/nonexistent/key"
		},
		"artifact store": func(c *config.Config) {
			c.Mithril.Server.ArtifactStore = ""
		},
	} {
		cfg, _ := snapshotTestConfig(t)
		mutate(cfg)
		_, err := runMithrilSnapshotCreate(
			t.Context(), cfg, cardanoDB(t, 1), discardLogger,
		)
		assert.Error(t, err, name)
	}
}

func TestNewMithrilServerBindsSharedBindAddr(t *testing.T) {
	t.Parallel()

	for bindAddr, want := range map[string]string{
		"0.0.0.0":   "0.0.0.0:8080",
		"127.0.0.1": "127.0.0.1:8080",
		"::":        "[::]:8080",
	} {
		cfg, _ := snapshotTestConfig(t)
		cfg.BindAddr = bindAddr
		srv, err := newMithrilServer(t.Context(), cfg, discardLogger)
		require.NoError(t, err)
		assert.Equal(t, want, srv.Addr)
	}
}

func TestNewMithrilServerRejectsIncompleteConfig(t *testing.T) {
	t.Parallel()

	for name, mutate := range map[string]func(*config.Config){
		"no port": func(c *config.Config) { c.Mithril.Server.Port = 0 },
		"no store": func(c *config.Config) {
			c.Mithril.Server.ArtifactStore = ""
		},
		"tls without certificate": func(c *config.Config) {
			c.Mithril.Server.TLSEnabled = true
		},
	} {
		cfg, _ := snapshotTestConfig(t)
		mutate(cfg)
		_, err := newMithrilServer(t.Context(), cfg, discardLogger)
		assert.Error(t, err, name)
	}
}

// TestServeMithrilServesSnapshotsUntilCancelled runs the served surface: a
// produced snapshot is listed over HTTP, and cancelling the context shuts the
// server down cleanly.
func TestServeMithrilServesSnapshotsUntilCancelled(t *testing.T) {
	t.Parallel()

	cfg, _ := snapshotTestConfig(t)
	artifact, err := runMithrilSnapshotCreate(
		t.Context(), cfg, cardanoDB(t, 1), discardLogger,
	)
	require.NoError(t, err)
	srv, err := newMithrilServer(t.Context(), cfg, discardLogger)
	require.NoError(t, err)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- serveMithril(ctx, srv, ln, false, "", "") }()

	req, err := http.NewRequestWithContext(
		t.Context(), http.MethodGet,
		"http://"+ln.Addr().String()+"/artifact/cardano-database", nil,
	)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var items []mithril.CardanoDatabaseSnapshotListItem
	require.NoError(t, json.Unmarshal(body, &items))
	require.Len(t, items, 1)
	assert.Equal(t, artifact.Hash, items[0].Hash)

	cancel()
	require.NoError(t, <-done)

	// Shutdown released the listener: the address no longer answers.
	req, err = http.NewRequestWithContext(
		t.Context(), http.MethodGet,
		"http://"+ln.Addr().String()+"/artifact/cardano-database", nil,
	)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	if err == nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)
}

func TestMithrilSnapshotCommandsAreRegistered(t *testing.T) {
	t.Parallel()

	root := mithrilCommand()
	create, _, err := root.Find([]string{"snapshot", "create"})
	require.NoError(t, err)
	assert.Equal(t, "create", create.Name())
	assert.NotNil(t, create.Flags().Lookup("db-dir"))
	serve, _, err := root.Find([]string{"serve"})
	require.NoError(t, err)
	assert.Equal(t, "serve", serve.Name())
}

// TestServeMithrilServesTLSWhenEnabled covers the optional TLS mode: with
// tlsEnabled the same handler answers HTTPS using the shared certificate, and
// the plain-HTTP default is unchanged (see the test above).
func TestServeMithrilServesTLSWhenEnabled(t *testing.T) {
	t.Parallel()

	cfg, _ := snapshotTestConfig(t)
	cfg.Mithril.Server.TLSEnabled = true
	cfg.TlsCertFilePath, cfg.TlsKeyFilePath = testutil.GenerateTestTLSCertKey(t)
	srv, err := newMithrilServer(t.Context(), cfg, discardLogger)
	require.NoError(t, err)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- serveMithril(
			ctx, srv, ln, true, cfg.TlsCertFilePath, cfg.TlsKeyFilePath,
		)
	}()

	req, err := http.NewRequestWithContext(
		t.Context(), http.MethodGet,
		"https://"+ln.Addr().String()+"/artifact/cardano-database", nil,
	)
	require.NoError(t, err)
	resp, err := testutil.InsecureHTTPClient().Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	cancel()
	require.NoError(t, <-done)
}

// aggregatorTestConfig enables the aggregator on a snapshot test config with
// a freshly generated genesis signing key.
func aggregatorTestConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg, dir := snapshotTestConfig(t)
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	keyFile := filepath.Join(dir, "genesis.skey")
	require.NoError(t, os.WriteFile(
		keyFile, []byte(hex.EncodeToString(key.Seed())), 0o600,
	))
	cfg.Mithril.Server.Aggregator = config.MithrilAggregatorConfig{
		Enabled:               true,
		Epoch:                 10,
		K:                     5,
		M:                     40,
		PhiF:                  0.5,
		GenesisSigningKeyFile: keyFile,
	}
	return cfg
}

func TestNewMithrilServerMountsAggregatorOnlyWhenEnabled(t *testing.T) {
	t.Parallel()

	endpoint := "/certificate-pending"
	status := func(cfg *config.Config) int {
		srv, err := newMithrilServer(t.Context(), cfg, discardLogger)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		srv.Handler.ServeHTTP(
			rec, httptest.NewRequest(http.MethodGet, endpoint, nil),
		)
		return rec.Code
	}

	cfg, _ := snapshotTestConfig(t)
	assert.Equal(t, http.StatusNotFound, status(cfg), "disabled")
	assert.Equal(t, http.StatusNoContent, status(aggregatorTestConfig(t)),
		"enabled")
}

func TestNewMithrilServerRejectsUnusableAggregatorKey(t *testing.T) {
	t.Parallel()

	missing := aggregatorTestConfig(t)
	missing.Mithril.Server.Aggregator.GenesisSigningKeyFile = filepath.Join(
		t.TempDir(), "absent.skey",
	)
	_, err := newMithrilServer(t.Context(), missing, discardLogger)
	assert.ErrorContains(t, err, "reading genesis signing key")

	malformed := aggregatorTestConfig(t)
	require.NoError(t, os.WriteFile(
		malformed.Mithril.Server.Aggregator.GenesisSigningKeyFile,
		[]byte("not a key"), 0o600,
	))
	_, err = newMithrilServer(t.Context(), malformed, discardLogger)
	assert.Error(t, err)
}
