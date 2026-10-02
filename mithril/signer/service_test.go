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

package signer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	dingo "github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// serviceConfig configures an enabled signer on the generated pool keys,
// pointed at endpoint.
func serviceConfig(
	keys poolKeys,
	endpoint string,
) *config.Config {
	cfg := &config.Config{Network: "devnet"}
	cfg.Mithril.AllowInsecureHTTP = true
	cfg.Mithril.Signer = config.MithrilSignerConfig{
		Enabled:            true,
		KESKey:             keys.kes,
		OperationalCert:    keys.opCert,
		ColdVKey:           keys.coldVKey,
		STMKey:             keys.stmKey,
		AggregatorEndpoint: endpoint,
	}
	return cfg
}

func serviceEnv(slot func() (uint64, bool, error)) dingo.NodeServiceEnv {
	return dingo.NodeServiceEnv{
		Logger:         slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry:   prometheus.NewRegistry(),
		ShelleyGenesis: testGenesis(),
		LedgerView:     fakeLedger{},
		WallClockSlot:  slot,
	}
}

// registrationAggregator is an aggregator that is empty apart from recording
// the signer's registration.
func registrationAggregator(
	t *testing.T,
) (*httptest.Server, <-chan map[string]any) {
	t.Helper()
	registrations := make(chan map[string]any, 4)
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/epoch-settings":
				writeJSON(w, map[string]any{
					"epoch":           testEpoch,
					"current_signers": []any{},
					"next_signers":    []any{},
				})
			case "/register-signer":
				registrations <- decodeBody(t, r)
				w.WriteHeader(http.StatusCreated)
			default:
				// Stake lookups after registration; the signer retries.
				http.NotFound(w, r)
			}
		},
	))
	t.Cleanup(srv.Close)
	return srv, registrations
}

func TestServiceRegistersWithAggregator(t *testing.T) {
	t.Parallel()
	srv, registrations := registrationAggregator(t)
	cfg := serviceConfig(newPoolKeys(t, testOpCertStartPeriod), srv.URL)

	stop, err := Service(cfg)(
		t.Context(),
		serviceEnv(func() (uint64, bool, error) { return testSlot, true, nil }),
	)
	require.NoError(t, err)
	got := testutil.RequireReceive(t, registrations, testWait, "registration")
	assert.Equal(t, float64(testEpoch+1), got["epoch"])
	assert.Equal(t, float64(testRelativeKESPeriod), got["kes_period"])
	assert.Contains(t, got["party_id"], "pool1")
	stop()
	// Stop is idempotent.
	stop()
}

func TestServiceFailsOnInvalidCredentials(t *testing.T) {
	t.Parallel()
	keys := newPoolKeys(t, testOpCertStartPeriod)
	cfg := serviceConfig(keys, "https://aggregator.example/aggregator")
	cfg.Mithril.Signer.ColdVKey = filepath.Join(t.TempDir(), "absent.vkey")

	_, err := Service(cfg)(
		t.Context(),
		serviceEnv(func() (uint64, bool, error) { return testSlot, true, nil }),
	)
	require.ErrorContains(t, err, "load cold verification key")
}

func TestServiceDefersWhenSlotCannotBePlaced(t *testing.T) {
	t.Parallel()
	keys := newPoolKeys(t, testOpCertStartPeriod)
	cfg := serviceConfig(keys, "https://aggregator.example/aggregator")
	cfg.Mithril.AllowInsecureHTTP = false

	// Credentials are valid but the wall-clock slot is not known yet: the
	// service starts, and its first registration waits for the slot.
	stop, err := Service(cfg)(
		t.Context(),
		serviceEnv(func() (uint64, bool, error) { return 0, false, nil }),
	)
	require.NoError(t, err)
	stop()

	// A failing slot clock is not the same as one that is merely not ready.
	_, err = Service(cfg)(
		t.Context(),
		serviceEnv(func() (uint64, bool, error) {
			return 0, false, errors.New("history unreadable")
		}),
	)
	require.ErrorContains(t, err, "history unreadable")
}

func TestSignerEndpoint(t *testing.T) {
	t.Parallel()
	var cfg config.MithrilConfig

	// Nothing configured on a network with a published aggregator.
	preprod, err := signerEndpoint(cfg, "preprod")
	require.NoError(t, err)
	assert.Contains(t, preprod, "preprod")

	cfg.AggregatorURL = "https://bootstrap.example/aggregator"
	endpoint, err := signerEndpoint(cfg, "preprod")
	require.NoError(t, err)
	assert.Equal(t, cfg.AggregatorURL, endpoint)

	cfg.Signer.AggregatorEndpoint = "https://signer.example/aggregator"
	endpoint, err = signerEndpoint(cfg, "preprod")
	require.NoError(t, err)
	assert.Equal(t, cfg.Signer.AggregatorEndpoint, endpoint)

	// A network without a published aggregator needs an explicit endpoint.
	_, err = signerEndpoint(config.MithrilConfig{}, "devnet")
	require.ErrorContains(
		t,
		err,
		"mithril.signer.aggregatorEndpoint is required",
	)
}

// TestNodeRunsTheSigner starts a node with the signer service enabled against
// a stub aggregator: the signer registers once the node's slot clock is
// usable, and the node stops it on shutdown.
func TestNodeRunsTheSigner(t *testing.T) {
	t.Parallel()
	srv, registrations := registrationAggregator(t)
	keys := newPoolKeys(t, 0)

	cardanoCfg, err := cardano.NewCardanoNodeConfigFromFile(
		filepath.Join("..", "..", "config", "cardano", "devnet", "config.json"),
	)
	require.NoError(t, err)
	// A recent start keeps the wall clock inside the confirmed era history
	// of a node that has applied nothing yet.
	cardanoCfg.ShelleyGenesis().SystemStart = time.Now().Add(-time.Second)
	node, err := dingo.New(dingo.NewConfig(
		dingo.WithDatabasePath(t.TempDir()),
		dingo.WithNetwork("devnet"),
		dingo.WithCardanoNodeConfig(cardanoCfg),
		dingo.WithNetworkMagic(cardanoCfg.ShelleyGenesis().NetworkMagic),
		dingo.WithPrometheusRegistry(prometheus.NewRegistry()),
		dingo.WithStorageMode(dingo.StorageModeCore),
		dingo.WithListeners(dingo.ListenerConfig{
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:0",
		}),
		dingo.WithMidnightConfig(dingo.MidnightConfig{Port: 0}),
		dingo.WithShutdownTimeout(5*time.Second),
		dingo.WithNodeService(Service(serviceConfig(keys, srv.URL))),
	))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- node.Run(ctx) }()

	got := testutil.RequireReceive(t, registrations, testWait, "registration")
	assert.Equal(t, float64(testEpoch+1), got["epoch"])
	assert.Contains(t, got["party_id"], "pool1")

	require.NoError(t, node.Stop())
	cancel()
	require.NoError(
		t,
		testutil.RequireReceive(t, done, testWait, "Run returns"),
	)
}
