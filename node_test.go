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
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/dingo/api/blockfrost"
	"github.com/blinklabs-io/dingo/api/mcp"
	"github.com/blinklabs-io/dingo/api/mesh"
	"github.com/blinklabs-io/dingo/api/utxorpc"
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/kesagent"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/leios"
	"github.com/blinklabs-io/dingo/mempool"
	ouroborosPkg "github.com/blinklabs-io/dingo/ouroboros"
	"github.com/blinklabs-io/dingo/peergov"
	"github.com/blinklabs-io/dingo/plugin"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/kes"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAPIProviderConfigMergesTopLevelDefault asserts a provider selection
// with no tls of its own inherits the shared api.tls
// policy.
func TestAPIProviderConfigMergesTopLevelDefault(t *testing.T) {
	t.Parallel()

	cfg := Config{
		apiConfig: internalconfig.APIConfig{
			TLS: apiconfig.TLSPolicy{
				Mode:         new("server"),
				CertFilePath: new("/shared/cert.pem"),
				KeyFilePath:  new("/shared/key.pem"),
			},
		},
	}
	selection := plugin.Selection{
		Provider: "builtin",
		Config:   map[string]any{"port": uint(3000)},
	}

	merged, err := cfg.apiProviderConfig(
		plugin.CapabilityAPIBlockfrost, selection,
	)
	require.NoError(t, err)

	tlsPolicy, err := apiconfig.DecodeTLSPolicy(merged.Config)
	require.NoError(t, err)
	effective, err := tlsPolicy.Resolve("test")
	require.NoError(t, err)
	assert.True(t, effective.Enabled)
	assert.Equal(t, "/shared/cert.pem", effective.CertFilePath)
	assert.Equal(t, "/shared/key.pem", effective.KeyFilePath)
}

// TestAPIProviderConfigProviderOverrideWins asserts an explicit provider
// field beats the shared top-level default for that field only.
func TestAPIProviderConfigProviderOverrideWins(t *testing.T) {
	t.Parallel()

	cfg := Config{
		apiConfig: internalconfig.APIConfig{
			TLS: apiconfig.TLSPolicy{
				Mode:         new("server"),
				CertFilePath: new("/shared/cert.pem"),
				KeyFilePath:  new("/shared/key.pem"),
			},
		},
	}
	selection := plugin.Selection{
		Provider: "builtin",
		Config: map[string]any{
			"port": uint(3000),
			"tls": map[string]any{
				"certFilePath": "/provider/cert.pem",
			},
		},
	}

	merged, err := cfg.apiProviderConfig(
		plugin.CapabilityAPIBlockfrost, selection,
	)
	require.NoError(t, err)

	tlsPolicy, err := apiconfig.DecodeTLSPolicy(merged.Config)
	require.NoError(t, err)
	effective, err := tlsPolicy.Resolve("test")
	require.NoError(t, err)
	assert.True(t, effective.Enabled)
	assert.Equal(t, "/provider/cert.pem", effective.CertFilePath)
	// keyFilePath falls through to the shared default.
	assert.Equal(t, "/shared/key.pem", effective.KeyFilePath)
}

// TestLegacyUtxorpcTLSPolicyIsUtxorpcOnly asserts the legacy root
// tlsCertFilePath/tlsKeyFilePath compatibility fields feed only UTxORPC's
// default TLS policy, never Blockfrost's or Mesh's -- promoting them to
// every API provider would silently switch previously-plaintext listeners
// to TLS on upgrade for any deployment that had set them (see
// legacyUtxorpcTLSPolicy's own doc comment).
func TestLegacyUtxorpcTLSPolicyIsUtxorpcOnly(t *testing.T) {
	t.Parallel()

	cfg := Config{
		tlsCertFilePath: "/legacy/cert.pem",
		tlsKeyFilePath:  "/legacy/key.pem",
	}
	selection := plugin.Selection{
		Provider: "builtin",
		Config:   map[string]any{"port": uint(9090)},
	}

	for _, tc := range []struct {
		capability   plugin.Capability
		wantEnabled  bool
		wantCertPath string
	}{
		{plugin.CapabilityAPIUtxorpc, true, "/legacy/cert.pem"},
		{plugin.CapabilityAPIBlockfrost, false, ""},
		{plugin.CapabilityAPIMesh, false, ""},
	} {
		merged, err := cfg.apiProviderConfig(tc.capability, selection)
		require.NoErrorf(t, err, "capability %s", tc.capability)
		tlsPolicy, err := apiconfig.DecodeTLSPolicy(merged.Config)
		require.NoErrorf(t, err, "capability %s", tc.capability)
		effective, err := tlsPolicy.Resolve("test")
		require.NoErrorf(t, err, "capability %s", tc.capability)
		assert.Equalf(
			t, tc.wantEnabled, effective.Enabled,
			"capability %s", tc.capability,
		)
		assert.Equalf(
			t, tc.wantCertPath, effective.CertFilePath,
			"capability %s", tc.capability,
		)
	}
}

// TestLegacyUtxorpcTLSPolicyYieldsToExplicitPolicy asserts the shared
// api.tls default and any provider-level override both still take
// precedence over the legacy compatibility fields for UTxORPC, matching
// the canonical-over-compatibility precedence used elsewhere (e.g.
// applyAPIPortCompatibilityEnvironment).
func TestLegacyUtxorpcTLSPolicyYieldsToExplicitPolicy(t *testing.T) {
	t.Parallel()

	cfg := Config{
		tlsCertFilePath: "/legacy/cert.pem",
		tlsKeyFilePath:  "/legacy/key.pem",
		apiConfig: internalconfig.APIConfig{
			TLS: apiconfig.TLSPolicy{Mode: new("disabled")},
		},
	}
	selection := plugin.Selection{
		Provider: "builtin",
		Config:   map[string]any{"port": uint(9090)},
	}

	merged, err := cfg.apiProviderConfig(
		plugin.CapabilityAPIUtxorpc, selection,
	)
	require.NoError(t, err)
	tlsPolicy, err := apiconfig.DecodeTLSPolicy(merged.Config)
	require.NoError(t, err)
	effective, err := tlsPolicy.Resolve("test")
	require.NoError(t, err)
	assert.False(t, effective.Enabled)
}

// TestNewRejectsInvalidMergedAPITLSPolicy asserts a partial certificate/
// key pair in the shared api.tls default is rejected at New(), before any
// listener starts -- not merely logged or deferred to Start() time.
func TestNewRejectsInvalidMergedAPITLSPolicy(t *testing.T) {
	t.Parallel()

	cardanoCfg := newNodeTestCardanoNodeCfg(t)
	_, err := New(NewConfig(
		WithDatabasePath(t.TempDir()),
		WithCardanoNodeConfig(cardanoCfg),
		WithNetworkMagic(cardanoCfg.ShelleyGenesis().NetworkMagic),
		WithPrometheusRegistry(prometheus.NewRegistry()),
		WithStorageMode(StorageModeAPI),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:0",
		}),
		WithMidnightConfig(MidnightConfig{Port: 0}),
		WithShutdownTimeout(5*time.Second),
		WithAPIConfig(internalconfig.APIConfig{
			TLS: apiconfig.TLSPolicy{
				Mode:         new("server"),
				CertFilePath: new("/only/cert.pem"),
			},
		}),
	))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "config.tls")
	assert.Contains(t, err.Error(), "must both be set")
}

// TestNewAllowsUnauthenticatedPublicAPI verifies the shared Node constructor
// permits an intentionally public API without requiring authentication.
func TestNewAllowsUnauthenticatedPublicAPI(t *testing.T) {
	t.Parallel()
	cardanoCfg := newNodeTestCardanoNodeCfg(t)
	node, err := New(NewConfig(
		WithDatabasePath(t.TempDir()),
		WithCardanoNodeConfig(cardanoCfg),
		WithNetworkMagic(cardanoCfg.ShelleyGenesis().NetworkMagic),
		WithPrometheusRegistry(prometheus.NewRegistry()),
		WithStorageMode(StorageModeAPI),
		WithBindAddr("0.0.0.0"),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:0",
		}),
		WithMidnightConfig(MidnightConfig{Port: 0}),
		WithShutdownTimeout(5*time.Second),
	))

	require.NoError(t, err)
	require.NotNil(t, node)
	t.Cleanup(func() {
		assert.NoError(t, node.Stop())
	})
}

// TestChainsyncConfigCarriesLimitOnPatience pins the composition the live
// restore/truncate rebuild shares with Run: the configured Limit on Patience
// and the Genesis gate that decides when it applies.
func TestChainsyncConfigCarriesLimitOnPatience(t *testing.T) {
	t.Parallel()
	n := &Node{config: NewConfig(WithGenesisLimitOnPatience(true, 7, 3))}

	cfg := n.chainsyncConfig()
	assert.Equal(t, chainsync.PatienceConfig{
		Enabled:  true,
		Capacity: 7,
		Rate:     3,
	}, cfg.Patience)
	require.NotNil(t, cfg.PatienceActiveFunc)
	require.NotNil(t, cfg.ObservedHeaderLimitFunc)
	assert.False(t, cfg.PatienceActiveFunc(), "no chain selector yet")

	n.chainSelector = chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{
			GenesisMode:        true,
			GenesisWindowSlots: 30,
		},
	)
	assert.True(t, cfg.PatienceActiveFunc())
	assert.Equal(t, 30, cfg.ObservedHeaderLimitFunc())

	n.chainSelector = chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{},
	)
	assert.False(t, cfg.PatienceActiveFunc(), "Praos selection")
}

func TestChainsyncConfigLimitOnPatienceDefaults(t *testing.T) {
	t.Parallel()
	n := &Node{config: NewConfig()}
	assert.Equal(t, chainsync.PatienceConfig{Enabled: true},
		n.chainsyncConfig().Patience,
		"enabled by default; zero capacity and rate select package defaults")
}

// TestLiveTruncateKeepsLimitOnPatience pins that the chainsync state rebuilt
// by a live truncate uses the configured Limit on Patience rather than the
// package defaults.
func TestLiveTruncateKeepsLimitOnPatience(t *testing.T) {
	t.Parallel()
	n, points := newLiveLifecycleTestNode(t, 25)
	require.NotNil(t, n.config.cfg)
	n.config.cfg.GenesisBootstrap.LimitOnPatienceEnabled = true
	n.config.cfg.GenesisBootstrap.LimitOnPatienceCapacity = 7

	targetSlot := points[10].Slot
	_, err := n.Truncate(context.Background(), dblifecycle.TruncateTarget{
		Slot: &targetSlot,
	})
	require.NoError(t, err)

	conn := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4001},
	}
	require.True(t, n.chainsyncState.AddClientConnId(conn))
	tc := n.chainsyncState.GetTrackedClient(conn)
	require.NotNil(t, tc)
	assert.InDelta(t, 7, tc.Patience.Tokens, 1e-9)
}

func TestNewInvalidConfigPreservesMetricsRegistry(t *testing.T) {
	for _, tc := range []struct {
		name      string
		option    ConfigOptionFunc
		wantError string
	}{
		{"storage", WithStorageMode("invalid"), "invalid storage mode"},
		{"era", WithStartEra("invalid"), "invalid start era"},
		{"margin", WithMinPoolMargin(10001), "min pool margin"},
		{"leverage", WithPledgeLeverage(true, 0), "pledge leverage"},
		{"listeners", func(c *Config) { c.listeners = nil }, "no listeners defined"},
		{"listener address", WithListeners(ListenerConfig{}), "listener must provide"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			control := prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "caller_registry_control", Help: "Collector owned by the caller.",
			})
			registry.MustRegister(control)
			control.Set(1)
			before, err := registry.Gather()
			require.NoError(t, err)
			baseOptions := []ConfigOptionFunc{
				WithNetworkMagic(1), WithPrometheusRegistry(registry),
				WithListeners(ListenerConfig{
					ListenNetwork: "tcp", ListenAddress: "127.0.0.1:0",
				}),
			}
			for range 2 {
				cfg := NewConfig(append(baseOptions, tc.option)...)
				var node *Node
				require.NotPanics(t, func() { node, err = New(cfg) })
				require.Nil(t, node)
				require.ErrorContains(t, err, tc.wantError)
				after, err := registry.Gather()
				require.NoError(t, err)
				require.Len(
					t,
					after,
					len(before),
					"failed construction changed caller metrics",
				)
				assert.Equal(t, before[0], after[0], "caller collector changed")
			}
			var node *Node
			require.NotPanics(
				t,
				func() { node, err = New(NewConfig(baseOptions...)) },
			)
			require.NoError(t, err)
			require.NotNil(t, node)
			t.Cleanup(func() { require.NoError(t, node.Stop()) })
			after, err := registry.Gather()
			require.NoError(t, err)
			require.Greater(
				t,
				len(after),
				len(before),
				"valid retry must register node metrics",
			)
		})
	}
}

func TestNodeEventSubscriptionPoliciesAreExplicit(t *testing.T) {
	t.Parallel()

	type counts struct {
		required               int
		detachable             int
		chainsync              int
		connectionRecycle      int
		ledgerRecycleTranslate int
	}
	want := map[string]map[string]counts{
		"node.go": {
			"Run": {
				required:  3,
				chainsync: 1,
			},
			"subscribeChainsyncClientRemoveRequests": {required: 1},
			"subscribeConnectionEvents": {
				required:               3,
				connectionRecycle:      1,
				ledgerRecycleTranslate: 1,
			},
			"subscribeChainSelectorEvents": {
				required:   8,
				detachable: 1,
			},
		},
		"node_lifecycle.go": {
			"reinitializeNetworkingCore": {
				chainsync:         1,
				connectionRecycle: 1,
			},
		},
		"node_leios.go": {
			"initLeiosVoteManager": {required: 2},
		},
		"node_koiosparity.go": {
			"startKoiosParityObserver": {required: 1},
		},
	}

	policyHelpers := map[string]string{
		"subscribeRequiredEvent":                      "SubscriberBackpressureBlock",
		"subscribeDetachableEvent":                    "SubscriberBackpressureDetach",
		"subscribeConnectionRecycleRequests":          "SubscriberBackpressureBlock",
		"subscribeLedgerConnectionRecycleTranslation": "SubscriberBackpressureBlock",
	}
	seenPolicyHelpers := make(map[string]bool)
	for fileName, functions := range want {
		file, err := parser.ParseFile(
			token.NewFileSet(),
			filepath.Clean(fileName),
			nil,
			parser.SkipObjectResolution,
		)
		require.NoError(t, err)

		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			wantCounts, isRegistration := functions[fn.Name.Name]
			got := counts{}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				name := nodeCallName(call.Fun)
				switch name {
				case "subscribeRequiredEvent":
					got.required++
				case "subscribeDetachableEvent":
					got.detachable++
				case "subscribeChainsyncClientRemoveRequests":
					got.chainsync++
				case "subscribeConnectionRecycleRequests":
					got.connectionRecycle++
				case "subscribeLedgerConnectionRecycleTranslation":
					got.ledgerRecycleTranslate++
				case "SubscribeFunc", "SubscribeFuncWithBuffer", "SubscribeFuncStrict":
					t.Errorf("%s uses unclassified EventBus registration %s", fn.Name.Name, name)
				case "SubscribeFuncWithBufferPolicy":
					policy, allowed := policyHelpers[fn.Name.Name]
					if !allowed {
						t.Errorf("%s registers an EventBus callback outside a policy helper", fn.Name.Name)
						return true
					}
					seenPolicyHelpers[fn.Name.Name] = true
					if len(call.Args) != 4 || nodeCallName(call.Args[2]) != policy {
						t.Errorf("%s must register with %s", fn.Name.Name, policy)
					}
				}
				return true
			})
			if isRegistration {
				require.Equalf(t, wantCounts, got,
					"subscription classification changed in %s:%s", fileName, fn.Name.Name)
			}
		}
	}
	for helper := range policyHelpers {
		require.Truef(t, seenPolicyHelpers[helper],
			"missing explicit policy implementation %s", helper)
	}
}

func nodeCallName(expr ast.Expr) string {
	switch value := expr.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		return value.Sel.Name
	default:
		return ""
	}
}

func TestCancelForFatalMakesShutdownReturnError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{ctx: ctx, cancel: cancel}
	want := errors.New("strict parity mismatch")

	n.cancelForFatal(want)

	require.ErrorIs(t, n.waitForShutdown(), want)
}

func TestParentCancellationRemainsCleanShutdown(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{ctx: ctx, cancel: cancel}

	cancel()

	require.NoError(t, n.waitForShutdown())
}

func TestFatalDuringStartupOverridesCancellationError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{ctx: ctx, cancel: cancel}
	want := errors.New("strict parity mismatch during startup")

	n.cancelForFatal(want)

	require.ErrorIs(t, n.resolveRunError(context.Canceled), want)
}

func TestLedgerFatalCallbackPreservesShutdownCause(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := &Node{
		ctx:    ctx,
		cancel: cancel,
		config: Config{
			cfg:    &internalconfig.Config{},
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	callback := n.ledgerStateConfig().FatalErrorFunc
	want := errors.New("ledger component failure")
	callback(want)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.ErrorIs(t, n.waitForShutdown(), want)
	require.ErrorIs(t, n.resolveRunError(context.Canceled), want)

	callback(errors.New("later ledger failure"))
	require.ErrorIs(t, n.waitForShutdown(), want)
}

// writeKesAgentFrame/readKesAgentFrame speak the bursa KES agent wire format
// (4-byte big-endian length prefix + JSON payload) directly, independent of
// the kesagent package's own unexported framing helpers -- exactly what any
// other implementation of the protocol (this fake agent included) has to do.
func writeKesAgentFrame(t testing.TB, conn net.Conn, v any) {
	t.Helper()
	payload, err := json.Marshal(v)
	require.NoError(t, err)
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	_, err = conn.Write(hdr[:])
	require.NoError(t, err)
	_, err = conn.Write(payload)
	require.NoError(t, err)
}

func readKesAgentFrame(conn net.Conn, v any) error {
	var hdr [4]byte
	if _, err := readFullFrame(conn, hdr[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	buf := make([]byte, n)
	if _, err := readFullFrame(conn, buf); err != nil {
		return err
	}
	return json.Unmarshal(buf, v)
}

func readFullFrame(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// devnetOpCertCBOR extracts the raw CBOR bytes backing the devnet opcert
// text envelope, which is what KeyPush.OpCert carries directly on the wire.
func devnetOpCertCBOR(t testing.TB) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(devnetKeysDir, "opcert.cert"))
	require.NoError(t, err)
	var envelope struct {
		CborHex string `json:"cborHex"`
	}
	require.NoError(t, json.Unmarshal(data, &envelope))
	raw, err := hex.DecodeString(envelope.CborHex)
	require.NoError(t, err)
	return raw
}

// devnetKESKey parses the devnet KES signing key the fake agent pushes. It
// reads the envelope bytes rather than calling bursa.LoadKeyFromFile: that
// function applies bursa's secret-key file policy, which rejects the checked-out
// fixture under any umask on Unix and rejects the Windows runner's file owner
// outright. What these cases need is the key material, not a check of how the
// repository stores it; PoolCredentials.LoadFromFiles remains the path that
// enforces the policy on an operator's own key.
func devnetKESKey(t testing.TB) *bursa.LoadedKey {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(devnetKeysDir, "kes.skey"))
	require.NoError(t, err)
	key, err := bursa.LoadKeyFromBytes(data)
	require.NoError(t, err)
	return key
}

func newTestNodeForBPWithAgent(
	t *testing.T,
	vrf, opcert string,
	mode string,
	socketPath string,
	cardanoCfg *cardano.CardanoNodeConfig,
) *Node {
	t.Helper()
	n := newTestNodeForBP(t, true, vrf, "", opcert, cardanoCfg)
	n.config.shelleyKESKey = ""
	n.config.shelleyKESAgentSocket = socketPath
	n.config.shelleyKESAgentMode = mode
	return n
}

// TestValidateBlockProducerStartup_KESAgentServeKeyMode proves
// validateBlockProducerStartupAtSlot installs credentials sourced entirely
// from a fake bursa KES agent in serve-key mode, using the real wire
// protocol against real devnet KES key material.
func TestValidateBlockProducerStartup_KESAgentServeKeyMode(t *testing.T) {
	t.Parallel()

	vrf, _, opcert := devnetCredPaths(t)
	kesKeyData := devnetKESKey(t)
	opCertCBOR := devnetOpCertCBOR(t)

	testutil.SkipIfBlockProducerUnsupported(t)
	sockPath := testutil.UnixSocketPath(t)
	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		writeKesAgentFrame(t, conn, kesagent.Hello{
			Protocol: kesagent.ProtocolID,
			Mode:     kesagent.ModeServeKey,
		})
		writeKesAgentFrame(t, conn, kesagent.KeyPush{
			Type:       "key_push",
			Period:     0,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: kesKeyData.SKey,
			KESVKey:    kesKeyData.VKey,
			OpCert:     opCertCBOR,
		})
		// Keep the connection open for the background Run loop until the
		// test closes the client.
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	}()

	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBPWithAgent(
		t,
		vrf,
		opcert,
		kesagent.ModeServeKey,
		sockPath,
		cardanoCfg,
	)
	t.Cleanup(n.closeKESAgentClient)

	creds, err := n.validateBlockProducerStartupAtSlot(0)
	require.NoError(t, err)
	require.True(t, creds.IsLoaded())
	require.NotNil(t, n.kesAgentClient)
}

// TestValidateBlockProducerStartup_KESAgentSignMode proves
// validateBlockProducerStartupAtSlot installs sign-mode credentials backed by
// a fake agent that signs real requests with the real devnet KES key,
// exercising the client's response verification against genuine signatures.
func TestValidateBlockProducerStartup_KESAgentSignMode(t *testing.T) {
	t.Parallel()

	vrf, _, opcert := devnetCredPaths(t)
	kesKeyData := devnetKESKey(t)

	testutil.SkipIfBlockProducerUnsupported(t)
	sockPath := testutil.UnixSocketPath(t)
	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		writeKesAgentFrame(t, conn, kesagent.Hello{
			Protocol: kesagent.ProtocolID,
			Mode:     kesagent.ModeSign,
		})
		var req kesagent.SignRequest
		if err := readKesAgentFrame(conn, &req); err != nil {
			return
		}
		sk := &kes.SecretKey{
			Depth:  kes.CardanoKesDepth,
			Period: req.Period, // devnet opcert KESPeriod is 0
			Data:   append([]byte(nil), kesKeyData.SKey...),
		}
		sig, signErr := kes.Sign(sk, req.Period, req.Message)
		if signErr != nil {
			return
		}
		writeKesAgentFrame(t, conn, kesagent.SignResponse{
			Type:      "sign_response",
			Period:    req.Period,
			Signature: sig,
		})
	}()

	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBPWithAgent(
		t,
		vrf,
		opcert,
		kesagent.ModeSign,
		sockPath,
		cardanoCfg,
	)
	t.Cleanup(n.closeKESAgentClient)

	creds, err := n.validateBlockProducerStartupAtSlot(0)
	require.NoError(t, err)
	require.True(t, creds.IsLoaded())
	require.NotNil(t, n.kesAgentClient)
}

// TestValidateBlockProducerStartup_KESAgentServeKeyRotationStaysValidated
// covers the KES rotation the serve-key background loop exists to handle.
//
// Installing a push goes through the same identity/generation-bump path as
// LoadFromFiles, and that path deliberately clears the validated KES protocol
// lifetime (opCertValidated, maxKESEvolutions, opCertExpiryKES) so no
// credential inherits a policy that was never checked against the material
// now installed. Startup re-establishes it for the first push. A push
// arriving later -- a KES evolution, an opcert rotation, or a re-push after a
// reconnect -- is not covered by startup, so the loop re-establishes it
// itself; without that, credentialGeneration.kesSign refuses every subsequent
// signature with "operational certificate is not validated" and the node
// stops forging until it is restarted.
//
// OpCertExpiryPeriod is the observable: it returns opCertExpiryKES, the value
// validatedKESProtocolLifetime requires to be non-zero before kesSign will
// sign at all. The two assertions are one WaitForCondition because a push is
// installed and re-validated by a background goroutine, so reading them
// separately would race the install itself.
func TestValidateBlockProducerStartup_KESAgentServeKeyRotationStaysValidated(
	t *testing.T,
) {
	t.Parallel()

	vrf, _, opcert := devnetCredPaths(t)
	kesKeyData := devnetKESKey(t)
	opCertCBOR := devnetOpCertCBOR(t)
	decodedOpCert, err := bursa.DecodeOpCert(opCertCBOR)
	require.NoError(t, err)

	// The second push carries the same key evolved one KES period forward,
	// which is what a real agent pushes when the period rolls over. It has to
	// be genuinely evolved: kesagent.Client self-sign-probes every push
	// before installing it, so a key that does not actually sign at its
	// declared period is rejected by the client and never reaches the node.
	evolved, err := kes.Update(&kes.SecretKey{
		Depth:  kes.CardanoKesDepth,
		Period: 0,
		Data:   append([]byte(nil), kesKeyData.SKey...),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), evolved.Period)

	testutil.SkipIfBlockProducerUnsupported(t)
	sockPath := testutil.UnixSocketPath(t)
	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		writeKesAgentFrame(t, conn, kesagent.Hello{
			Protocol: kesagent.ProtocolID,
			Mode:     kesagent.ModeServeKey,
		})
		writeKesAgentFrame(t, conn, kesagent.KeyPush{
			Type:       "key_push",
			Period:     decodedOpCert.KESPeriod,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: kesKeyData.SKey,
			KESVKey:    kesKeyData.VKey,
			OpCert:     opCertCBOR,
		})
		writeKesAgentFrame(t, conn, kesagent.KeyPush{
			Type:       "key_push",
			Period:     decodedOpCert.KESPeriod + 1,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: evolved.Data,
			KESVKey:    kesKeyData.VKey,
			OpCert:     opCertCBOR,
		})
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	}()

	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBPWithAgent(
		t,
		vrf,
		opcert,
		kesagent.ModeServeKey,
		sockPath,
		cardanoCfg,
	)
	t.Cleanup(n.closeKESAgentClient)

	creds, err := n.validateBlockProducerStartupAtSlot(0)
	require.NoError(t, err)
	require.NotZero(
		t,
		creds.OpCertExpiryPeriod(),
		"startup must leave a validated KES protocol lifetime",
	)

	testutil.WaitForCondition(
		t,
		func() bool {
			return creds.GetKESPeriod() == 1 &&
				creds.OpCertExpiryPeriod() != 0
		},
		5*time.Second,
		"rotated KES key must be installed and still carry a validated KES protocol lifetime",
	)
}

// TestValidateBlockProducerStartup_KESAgentClosedWhenValidationFails covers
// the cleanup an agent-backed startup owes when a later step rejects the
// credentials.
//
// The agent is dialled and its serve-key loop is running before the opcert
// and KES-period checks run, so a rejection there used to return with the
// client still connected and the loop still installing pushes into
// credentials the node had refused to start on.
//
// Driven by validating against a slot past the operational certificate's
// expiry: the agent's push installs normally and the KES-period check is what
// fails, which is exactly the ordering the cleanup exists for.
func TestValidateBlockProducerStartup_KESAgentClosedWhenValidationFails(
	t *testing.T,
) {
	t.Parallel()

	vrf, _, opcert := devnetCredPaths(t)
	kesKeyData := devnetKESKey(t)
	opCertCBOR := devnetOpCertCBOR(t)

	testutil.SkipIfBlockProducerUnsupported(t)
	sockPath := testutil.UnixSocketPath(t)
	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		writeKesAgentFrame(t, conn, kesagent.Hello{
			Protocol: kesagent.ProtocolID,
			Mode:     kesagent.ModeServeKey,
		})
		writeKesAgentFrame(t, conn, kesagent.KeyPush{
			Type:       "key_push",
			Period:     0,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: kesKeyData.SKey,
			KESVKey:    kesKeyData.VKey,
			OpCert:     opCertCBOR,
		})
		_, _ = conn.Read(make([]byte, 1))
	}()

	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBPWithAgent(
		t,
		vrf,
		opcert,
		kesagent.ModeServeKey,
		sockPath,
		cardanoCfg,
	)
	t.Cleanup(n.closeKESAgentClient)

	// slotsPerKESPeriod 129600 x maxKESEvolutions 62 is the first slot past
	// the certificate's protocol lifetime.
	_, err = n.validateBlockProducerStartupAtSlot(62 * 129600)
	require.Error(t, err)
	require.Nil(
		t, n.kesAgentClient,
		"a startup rejected after the agent was dialled must not leave its "+
			"client and serve-key loop running",
	)
}

func TestValidateBlockProducerStartupForClock_KESAgentDeferredPath(
	t *testing.T,
) {
	t.Parallel()

	vrf, _, opcert := devnetCredPaths(t)
	kesKeyData := devnetKESKey(t)
	opCertCBOR := devnetOpCertCBOR(t)

	testutil.SkipIfBlockProducerUnsupported(t)
	sockPath := testutil.UnixSocketPath(t)
	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		writeKesAgentFrame(t, conn, kesagent.Hello{
			Protocol: kesagent.ProtocolID,
			Mode:     kesagent.ModeServeKey,
		})
		writeKesAgentFrame(t, conn, kesagent.KeyPush{
			Type:       "key_push",
			Period:     0,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: kesKeyData.SKey,
			KESVKey:    kesKeyData.VKey,
			OpCert:     opCertCBOR,
		})
		_, _ = conn.Read(make([]byte, 1))
	}()

	n := newTestNodeForBPWithAgent(
		t,
		vrf,
		opcert,
		kesagent.ModeServeKey,
		sockPath,
		shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour)),
	)
	registry := prometheus.NewRegistry()
	n.config.promRegistry = registry
	t.Cleanup(n.closeKESAgentClient)

	creds, err := n.validateBlockProducerStartupForClock(0, false)
	require.NoError(t, err)
	require.True(t, creds.IsLoaded())
	require.NotZero(t, creds.OpCertExpiryPeriod())
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Contains(
		t,
		metricFamilyNames(families),
		"dingo_kes_agent_connected",
	)
}

func TestValidateBlockProducerStartup_KESAgentPinsLocalColdKey(t *testing.T) {
	t.Parallel()

	vrf, _, opcert := devnetCredPaths(t)
	kesKeyData := devnetKESKey(t)
	opCertCBOR := devnetOpCertCBOR(t)
	localEnvelope, err := os.ReadFile(opcert)
	require.NoError(t, err)
	var envelope struct {
		Type        string `json:"type"`
		Description string `json:"description"`
		CborHex     string `json:"cborHex"`
	}
	require.NoError(t, json.Unmarshal(localEnvelope, &envelope))
	localCBOR, err := hex.DecodeString(envelope.CborHex)
	require.NoError(t, err)
	localCBOR[len(localCBOR)-1] ^= 0xff
	envelope.CborHex = hex.EncodeToString(localCBOR)
	mismatchedEnvelope, err := json.Marshal(envelope)
	require.NoError(t, err)
	mismatchedOpCert := filepath.Join(t.TempDir(), "opcert.cert")
	require.NoError(
		t,
		os.WriteFile(mismatchedOpCert, mismatchedEnvelope, 0o600),
	)

	testutil.SkipIfBlockProducerUnsupported(t)
	sockPath := testutil.UnixSocketPath(t)
	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		writeKesAgentFrame(t, conn, kesagent.Hello{
			Protocol: kesagent.ProtocolID,
			Mode:     kesagent.ModeServeKey,
		})
		writeKesAgentFrame(t, conn, kesagent.KeyPush{
			Type:       "key_push",
			Period:     0,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: kesKeyData.SKey,
			KESVKey:    kesKeyData.VKey,
			OpCert:     opCertCBOR,
		})
	}()

	n := newTestNodeForBPWithAgent(
		t,
		vrf,
		mismatchedOpCert,
		kesagent.ModeServeKey,
		sockPath,
		shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour)),
	)
	t.Cleanup(n.closeKESAgentClient)

	_, err = n.validateBlockProducerStartupAtSlot(0)
	require.ErrorContains(t, err, "cold key does not match local")
	require.Nil(t, n.kesAgentClient)
}

func TestKESAgentMetricsRegisteredOncePerNode(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	n := &Node{config: Config{promRegistry: registry}}
	first := n.kesAgentClientMetrics()
	second := n.kesAgentClientMetrics()
	require.Same(t, first, second)

	families, err := registry.Gather()
	require.NoError(t, err)
	names := metricFamilyNames(families)
	for _, name := range []string{
		"dingo_kes_agent_connected",
		"dingo_kes_agent_reconnect_failures_total",
		"dingo_kes_agent_sign_success_total",
		"dingo_kes_agent_sign_failure_total",
		"dingo_kes_agent_sign_latency_seconds",
	} {
		require.Contains(t, names, name)
	}
}

func metricFamilyNames(families []*dto.MetricFamily) map[string]struct{} {
	names := make(map[string]struct{}, len(families))
	for _, family := range families {
		names[family.GetName()] = struct{}{}
	}
	return names
}

// TestKESAgentStartupWiresClientMetrics covers the wiring rather than the
// collector: kesagent.NewMetrics runs only from kesAgentClientMetrics, and
// that is reached only from the two kesagent.Config literals in
// node_forging.go, so a registry holding dingo_kes_agent_* after startup is
// evidence the literal passes Metrics. TestKESAgentMetricsRegisteredOncePerNode
// calls the helper itself and therefore cannot see a Config literal that
// leaves Metrics nil. Serve-key additionally reads dingo_kes_agent_connected,
// which only the client sets on a completed handshake.
func TestKESAgentStartupWiresClientMetrics(t *testing.T) {
	t.Parallel()

	vrf, _, opcert := devnetCredPaths(t)
	kesKeyData := devnetKESKey(t)
	opCertCBOR := devnetOpCertCBOR(t)

	testutil.SkipIfBlockProducerUnsupported(t)
	sockPath := testutil.UnixSocketPath(t)
	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		writeKesAgentFrame(t, conn, kesagent.Hello{
			Protocol: kesagent.ProtocolID,
			Mode:     kesagent.ModeServeKey,
		})
		writeKesAgentFrame(t, conn, kesagent.KeyPush{
			Type:       "key_push",
			Period:     0,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: kesKeyData.SKey,
			KESVKey:    kesKeyData.VKey,
			OpCert:     opCertCBOR,
		})
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	}()

	registry := prometheus.NewRegistry()
	n := newTestNodeForBPWithAgent(
		t,
		vrf,
		opcert,
		kesagent.ModeServeKey,
		sockPath,
		shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour)),
	)
	n.config.promRegistry = registry
	t.Cleanup(n.closeKESAgentClient)

	_, err = n.validateBlockProducerStartupAtSlot(0)
	require.NoError(t, err)

	families, err := registry.Gather()
	require.NoError(t, err)
	names := metricFamilyNames(families)
	for _, name := range []string{
		"dingo_kes_agent_connected",
		"dingo_kes_agent_reconnect_failures_total",
		"dingo_kes_agent_sign_success_total",
		"dingo_kes_agent_sign_failure_total",
		"dingo_kes_agent_sign_latency_seconds",
	} {
		require.Contains(t, names, name)
	}
	require.Equal(t, 1.0, gaugeValue(t, families, "dingo_kes_agent_connected"))
}

// TestKESAgentSignModeStartupWiresClientMetrics is the sign-mode half of the
// same class: node_forging.go carries exactly two kesagent.Config literals,
// and each needs its own Metrics field.
func TestKESAgentSignModeStartupWiresClientMetrics(t *testing.T) {
	t.Parallel()

	vrf, _, opcert := devnetCredPaths(t)

	testutil.SkipIfBlockProducerUnsupported(t)
	sockPath := testutil.UnixSocketPath(t)
	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	registry := prometheus.NewRegistry()
	n := newTestNodeForBPWithAgent(
		t,
		vrf,
		opcert,
		kesagent.ModeSign,
		sockPath,
		shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour)),
	)
	n.config.promRegistry = registry
	t.Cleanup(n.closeKESAgentClient)

	_, err = n.validateBlockProducerStartupAtSlot(0)
	require.NoError(t, err)

	families, err := registry.Gather()
	require.NoError(t, err)
	names := metricFamilyNames(families)
	for _, name := range []string{
		"dingo_kes_agent_connected",
		"dingo_kes_agent_reconnect_failures_total",
		"dingo_kes_agent_sign_success_total",
		"dingo_kes_agent_sign_failure_total",
		"dingo_kes_agent_sign_latency_seconds",
	} {
		require.Contains(t, names, name)
	}
}

func gaugeValue(
	t testing.TB,
	families []*dto.MetricFamily,
	name string,
) float64 {
	t.Helper()
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		require.Len(t, family.GetMetric(), 1)
		return family.GetMetric()[0].GetGauge().GetValue()
	}
	t.Fatalf("metric family %q not gathered", name)
	return 0
}

func TestMidnightServerActiveRequiresExplicitEnablement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		storageMode StorageMode
		config      MidnightConfig
		want        bool
	}{
		{
			name:        "disabled despite configured default port",
			storageMode: StorageModeAPI,
			config:      MidnightConfig{Port: 50051},
		},
		{
			name:        "indexer enabled without server",
			storageMode: StorageModeAPI,
			config: MidnightConfig{
				Enabled: true,
				Port:    50051,
			},
		},
		{
			name:        "enabled in api mode",
			storageMode: StorageModeAPI,
			config: MidnightConfig{
				ServerEnabled: true,
				Port:          50051,
			},
			want: true,
		},
		{
			name:        "enabled in core mode",
			storageMode: StorageModeCore,
			config: MidnightConfig{
				ServerEnabled: true,
				Port:          50051,
			},
		},
		{
			name:        "enabled with zero port",
			storageMode: StorageModeAPI,
			config:      MidnightConfig{ServerEnabled: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := midnightServerActive(tt.storageMode, tt.config); got != tt.want {
				t.Fatalf("midnightServerActive() = %v, want %v", got, tt.want)
			}
		})
	}
}

// nilIterChainProvider satisfies chainsync.ChainProvider without requiring a
// real database-backed chain. It hands AddClient a nil *chain.ChainIterator,
// which is enough to register server-side (N2C) client state -- the object
// under test here is whether that state is released, not the iterator's own
// Cancel behavior.
type nilIterChainProvider struct{}

func (nilIterChainProvider) GetChainFromPoint(
	_ ocommon.Point,
	_ bool,
) (*chain.ChainIterator, error) {
	return nil, nil
}

func (nilIterChainProvider) StabilityWindow() uint64 { return 0 }

func newHandleConnManagerClosedTestNode(t *testing.T) *Node {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	return &Node{
		chainsyncState: chainsync.NewStateWithConfig(
			bus,
			nilIterChainProvider{},
			chainsync.DefaultConfig(),
		),
	}
}

func newHandleConnManagerClosedOwnerConn(
	t *testing.T,
	o *ouroborosPkg.Ouroboros,
) *ouroboros.Connection {
	t.Helper()
	listener := o.ConfigureListeners([]connmanager.ListenerConfig{{UseNtC: true}})[0]
	localWire, peerWire := newLeiosNotifyTestConnPair(
		&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	)
	t.Cleanup(func() {
		_ = localWire.Close()
		_ = peerWire.Close()
	})
	type result struct {
		conn *ouroboros.Connection
		err  error
	}
	localResult, peerResult := make(chan result, 1), make(chan result, 1)
	go func() {
		conn, err := ouroboros.NewConnection(append(
			[]ouroboros.ConnectionOptionFunc{
				ouroboros.WithConnection(localWire),
			},
			listener.ConnectionOpts...,
		)...)
		localResult <- result{conn: conn, err: err}
	}()
	go func() {
		conn, err := ouroboros.NewConnection(append(
			[]ouroboros.ConnectionOptionFunc{
				ouroboros.WithConnection(peerWire),
				ouroboros.WithServer(true),
			},
			listener.ConnectionOpts...,
		)...)
		peerResult <- result{conn: conn, err: err}
	}()
	local := testutil.RequireReceive(
		t,
		localResult,
		10*time.Second,
		"owner test local handshake",
	)
	peer := testutil.RequireReceive(
		t,
		peerResult,
		10*time.Second,
		"owner test peer handshake",
	)
	t.Cleanup(func() {
		if local.conn != nil {
			_ = local.conn.Close()
		}
		if peer.conn != nil {
			_ = peer.conn.Close()
		}
	})
	require.NoError(t, local.err)
	require.NoError(t, peer.err)
	require.NotNil(t, peer.conn.ChainSync())
	require.NotNil(t, peer.conn.ChainSync().Server)
	return peer.conn
}

// TestHandleConnManagerClosedOwner_NtC_ReleasesChainsyncClientState reproduces a
// leak: NtC connections never received any close notification (the
// EventBus's ConnectionClosedEventType is intentionally NtN-only), so
// chainsync.State.RemoveClient -- which cancels the live chain iterator and
// deletes the per-connection client state -- was never invoked for a closed
// NtC connection. Without handleConnManagerClosedOwner wired as the connection
// manager's ConnClosedOwnerFunc, this assertion fails: the client state
// registered by AddClient is still present after the simulated close.
func TestHandleConnManagerClosedOwner_NtC_ReleasesChainsyncClientState(
	t *testing.T,
) {
	t.Parallel()

	n := newHandleConnManagerClosedTestNode(t)
	conn, err := ouroboros.NewConnection()
	require.NoError(t, err)
	connId := conn.Id()

	_, err = n.chainsyncState.AddClient(connId, ocommon.Point{})
	require.NoError(t, err)
	_, ok := n.chainsyncState.LookupClient(connId)
	require.True(t, ok, "precondition: server-side client state registered")

	n.handleConnManagerClosedOwner(conn, true, nil)

	_, ok = n.chainsyncState.LookupClient(connId)
	require.False(
		t,
		ok,
		"NtC close must release the chainsync server-side client state and its chain iterator",
	)
}

// TestHandleConnManagerClosedOwner_NtN_ReleasesState covers the owner-aware
// connmanager path used for both NtC and NtN. The EventBus path deliberately no
// longer removes server-side state by connection ID because a delayed event
// could delete a replacement connection's state.
func TestHandleConnManagerClosedOwner_NtN_ReleasesState(t *testing.T) {
	t.Parallel()

	n := newHandleConnManagerClosedTestNode(t)
	conn, err := ouroboros.NewConnection()
	require.NoError(t, err)
	connId := conn.Id()

	_, err = n.chainsyncState.AddClient(connId, ocommon.Point{})
	require.NoError(t, err)

	n.handleConnManagerClosedOwner(conn, false, nil)

	_, ok := n.chainsyncState.LookupClient(connId)
	require.False(
		t,
		ok,
		"NtN close must release the chainsync server-side client state",
	)
}

// TestHandleConnManagerClosed_NilChainsyncState guards the shutdown/restore
// window (node_lifecycle.go nils n.chainsyncState while rebuilding it) so a
// late NtC close callback cannot panic.
func TestHandleConnManagerClosedOwner_NilChainsyncState(t *testing.T) {
	t.Parallel()

	n := &Node{}
	require.NotPanics(t, func() {
		conn, err := ouroboros.NewConnection()
		require.NoError(t, err)
		n.handleConnManagerClosedOwner(conn, true, nil)
	})
}

// TestHandleConnManagerClosedOwner_NtC_ReleasesLeiosServeWaiters covers the node
// half of the wiring. The connection manager's ConnClosedOwnerFunc is
// the only close notification an NtC connection gets, and it is what wakes a
// chainsync server callback parked waiting for a certified endorser closure --
// the protocol's own done channel cannot close while that callback is running.
// Without the owner-aware release in handleConnManagerClosedOwner the
// registered waiter survives the close and this fails.
//
// The Ouroboros instance is built through the validating constructor with the
// full dependency set, and the connection is registered with its connection
// manager, so the waiter passes the liveness check the same way a live serve
// does.
func TestHandleConnManagerClosedOwner_NtC_ReleasesLeiosServeWaiters(
	t *testing.T,
) {
	t.Parallel()
	testHandleConnManagerClosedReleasesLeiosServeWaiters(t, true)
}

func TestHandleConnManagerClosedOwner_NtN_ReleasesLeiosServeWaiters(
	t *testing.T,
) {
	t.Parallel()
	testHandleConnManagerClosedReleasesLeiosServeWaiters(t, false)
}

func testHandleConnManagerClosedReleasesLeiosServeWaiters(
	t *testing.T,
	isNtC bool,
) {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	n := newHandleConnManagerClosedTestNode(t)
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Stop)

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })
	chainManager, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	ledgerState, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: chainManager,
		Logger:       logger,
	})
	require.NoError(t, err)
	harnessMempool, err := mempool.NewMempool(mempool.MempoolConfig{
		Logger:          logger,
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       ledgerState,
		MempoolCapacity: 1024 * 1024,
	})
	require.NoError(t, err)
	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{Logger: logger},
	)
	o, err := ouroborosPkg.NewOuroboros(ouroborosPkg.OuroborosConfig{
		Logger:         logger,
		EventBus:       bus,
		LedgerState:    ledgerState,
		NetworkMagic:   ouroboros_mock.MockNetworkMagic,
		Mempool:        &mempool.FIFO{Mempool: harnessMempool},
		ChainsyncState: chainsync.NewState(bus, ledgerState),
		ConnManager:    connManager,
		PeerGov: peergov.NewPeerGovernor(peergov.PeerGovernorConfig{
			Logger:      logger,
			EventBus:    bus,
			ConnManager: connManager,
		}),
	})
	require.NoError(t, err)
	n.ouroborosRef.Store(o)

	// Register a real node-to-client connection so the waiter carries its
	// chainsync server owner, as it does in production.
	conn := newHandleConnManagerClosedOwnerConn(t, o)
	require.True(t, connManager.AddConnection(conn, isNtC, "127.0.0.1:3002"))
	connId := conn.Id()

	done, cancel := o.RegisterLeiosServeWaiterForTesting(connId)
	t.Cleanup(cancel)

	testutil.RequireNoReceive(
		t,
		done,
		50*time.Millisecond,
		"waiter must not be released before the close",
	)

	n.handleConnManagerClosedOwner(conn, isNtC, nil)

	testutil.RequireReceive(
		t,
		done,
		time.Second,
		"connection close must release the parked Leios serving wait",
	)
}

// TestHandleConnManagerClosedOwner_NtC_ReleasesLocalStateQueryAcquiredPoint covers
// the NtC-close half of point-pinning: a client
// that pins a point and then disconnects without a clean Release must not
// leak its map entry, since NtC closes never reach
// Ouroboros.HandleConnClosedEvent (the EventBus's ConnectionClosedEventType
// is intentionally NtN-only) and localstatequeryServerRelease is therefore
// never invoked for it. Without ReleaseLocalStateQueryAcquiredPoint wired
// into handleConnManagerClosedOwner, this assertion fails: the entry
// SetLocalStateQueryAcquiredPointForTesting seeded is still present after
// the simulated close.
func TestHandleConnManagerClosedOwner_NtC_ReleasesLocalStateQueryAcquiredPoint(
	t *testing.T,
) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	n := newHandleConnManagerClosedTestNode(t)
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Stop)

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })
	chainManager, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	ledgerState, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: chainManager,
		Logger:       logger,
	})
	require.NoError(t, err)
	harnessMempool, err := mempool.NewMempool(mempool.MempoolConfig{
		Logger:          logger,
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       ledgerState,
		MempoolCapacity: 1024 * 1024,
	})
	require.NoError(t, err)
	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{Logger: logger},
	)
	o, err := ouroborosPkg.NewOuroboros(ouroborosPkg.OuroborosConfig{
		Logger:         logger,
		EventBus:       bus,
		LedgerState:    ledgerState,
		Mempool:        &mempool.FIFO{Mempool: harnessMempool},
		ChainsyncState: chainsync.NewState(bus, ledgerState),
		ConnManager:    connManager,
		PeerGov: peergov.NewPeerGovernor(peergov.PeerGovernorConfig{
			Logger:      logger,
			EventBus:    bus,
			ConnManager: connManager,
		}),
	})
	require.NoError(t, err)
	n.ouroborosRef.Store(o)

	conn, err := ouroboros.NewConnection()
	require.NoError(t, err)
	connId := conn.Id()
	o.SetLocalStateQueryAcquiredPointForTesting(connId, ledger.QueryPoint{
		Slot: 100,
		Hash: []byte{0xAB},
	})
	require.True(
		t,
		o.HasLocalStateQueryAcquiredPointForTesting(connId),
		"precondition: pinned point recorded",
	)

	n.handleConnManagerClosedOwner(conn, true, nil)

	require.False(
		t,
		o.HasLocalStateQueryAcquiredPointForTesting(connId),
		"NtC close must release the pinned LocalStateQuery point",
	)
}

// TestHandleConnManagerClosedOwner_NilOuroboros guards the same restore window as
// TestHandleConnManagerClosedOwner_NilChainsyncState for the added ouroboros
// dereference: n.ouroboros() is nil before Run wires it.
func TestHandleConnManagerClosedOwner_NilOuroboros(t *testing.T) {
	t.Parallel()

	n := newHandleConnManagerClosedTestNode(t)
	require.Nil(t, n.ouroboros())
	require.NotPanics(t, func() {
		conn, err := ouroboros.NewConnection()
		require.NoError(t, err)
		n.handleConnManagerClosedOwner(conn, true, nil)
	})
}

type apiProbeConfig struct {
	Port uint `yaml:"port"`
}

type apiLifecycleProbe struct {
	host     string
	starts   atomic.Int32
	stops    atomic.Int32
	startErr error
}

func (p *apiLifecycleProbe) instance() plugin.Instance {
	return plugin.Lifecycle{
		StartFunc: func(context.Context) error {
			p.starts.Add(1)
			return p.startErr
		},
		StopFunc: func(context.Context) error {
			p.stops.Add(1)
			return nil
		},
	}
}

func registerAPIProbe(
	t *testing.T,
	host *plugin.Host,
	capability plugin.Capability,
	name string,
	probe *apiLifecycleProbe,
) {
	t.Helper()
	descriptor := plugin.Descriptor{Capability: capability, Name: name}
	var err error
	switch capability {
	case plugin.CapabilityAPIUtxorpc:
		err = plugin.Register(
			host,
			descriptor,
			func() apiProbeConfig { return apiProbeConfig{} },
			func(
				_ context.Context,
				_ apiProbeConfig,
				deps utxorpc.ProviderDependencies,
			) (string, plugin.Instance, error) {
				probe.host = deps.Host
				return name, probe.instance(), nil
			},
		)
	case plugin.CapabilityAPIBlockfrost:
		err = plugin.Register(
			host,
			descriptor,
			func() apiProbeConfig { return apiProbeConfig{} },
			func(
				_ context.Context,
				_ apiProbeConfig,
				deps blockfrost.ProviderDependencies,
			) (string, plugin.Instance, error) {
				probe.host = deps.Host
				return name, probe.instance(), nil
			},
		)
	case plugin.CapabilityAPIMesh:
		err = plugin.Register(
			host,
			descriptor,
			func() apiProbeConfig { return apiProbeConfig{} },
			func(
				_ context.Context,
				_ apiProbeConfig,
				deps mesh.ProviderDependencies,
			) (string, plugin.Instance, error) {
				probe.host = deps.Host
				return name, probe.instance(), nil
			},
		)
	case plugin.CapabilityAPIMcp:
		err = plugin.Register(
			host,
			descriptor,
			func() apiProbeConfig { return apiProbeConfig{} },
			func(
				_ context.Context,
				_ apiProbeConfig,
				deps mcp.ProviderDependencies,
			) (string, plugin.Instance, error) {
				probe.host = deps.Host
				return name, probe.instance(), nil
			},
		)
	default:
		t.Fatalf("unsupported API capability %s", capability)
	}
	require.NoError(t, err)
}

func newAPIPluginRuntimeNode(t *testing.T) *Node {
	t.Helper()
	cardanoCfg := newNodeTestCardanoNodeCfg(t)
	n, err := New(NewConfig(
		WithDatabasePath(t.TempDir()),
		WithCardanoNodeConfig(cardanoCfg),
		WithNetworkMagic(cardanoCfg.ShelleyGenesis().NetworkMagic),
		WithPrometheusRegistry(prometheus.NewRegistry()),
		WithStorageMode(StorageModeAPI),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:0",
		}),
		WithMidnightConfig(MidnightConfig{Port: 0}),
		WithShutdownTimeout(5*time.Second),
	))
	require.NoError(t, err)
	// New validates the production requirement for at least one listener.
	// Runtime plugin tests do not need a network socket, so remove it after
	// validation to keep these tests hermetic in restricted environments.
	n.config.listeners = nil
	return n
}

func selectAPIProbe(
	n *Node,
	capability plugin.Capability,
	name string,
	port uint,
) {
	n.config.pluginSelections[capability] = plugin.Selection{
		Provider: name,
		Config:   map[string]any{"port": port},
	}
}

// newAPISelectionNode builds a Node whose only plugin selection is sel under
// the Blockfrost API capability, for exercising apiPluginSelection.
func newAPISelectionNode(sel plugin.Selection) *Node {
	return &Node{
		config: Config{
			pluginSelections: map[plugin.Capability]plugin.Selection{
				plugin.CapabilityAPIBlockfrost: sel,
			},
		},
	}
}

// TestAPIPluginSelectionPortDecoding covers apiPluginSelection's port decoder.
// The port arrives inside a map[string]any and can be produced by YAML decode
// (int/float64), the environment compatibility shim (uint64), or an in-code
// selection (uint), so every accepted numeric type and every guard is checked.
func TestAPIPluginSelectionPortDecoding(t *testing.T) {
	t.Parallel()

	t.Run("uses capability default when port absent", func(t *testing.T) {
		n := newAPISelectionNode(
			plugin.Selection{Provider: "builtin", Config: map[string]any{}},
		)
		_, port, err := n.apiPluginSelection(plugin.CapabilityAPIBlockfrost)
		require.NoError(t, err)
		assert.Equal(t, uint(3000), port)
	})

	t.Run("uses capability default when config is nil", func(t *testing.T) {
		n := newAPISelectionNode(plugin.Selection{Provider: "builtin"})
		_, port, err := n.apiPluginSelection(plugin.CapabilityAPIBlockfrost)
		require.NoError(t, err)
		assert.Equal(t, uint(3000), port)
	})

	accepted := []struct {
		name  string
		value any
		want  uint
	}{
		{"int", int(3100), 3100},
		{"uint", uint(3101), 3101},
		{"uint64", uint64(3102), 3102},
		{"int64", int64(3103), 3103},
		{"float64", float64(3104), 3104},
		{"zero (disables the API)", int(0), 0},
		{"max port", int(65535), 65535},
		{"int64 max port", int64(65535), 65535},
	}
	for _, tc := range accepted {
		t.Run("accepts port as "+tc.name, func(t *testing.T) {
			n := newAPISelectionNode(plugin.Selection{
				Provider: "builtin",
				Config:   map[string]any{"port": tc.value},
			})
			_, port, err := n.apiPluginSelection(
				plugin.CapabilityAPIBlockfrost,
			)
			require.NoError(t, err)
			assert.Equal(t, tc.want, port)
		})
	}

	rejected := []struct {
		name  string
		value any
	}{
		{"negative int", int(-1)},
		{"negative int64", int64(-1)},
		{"negative float64", float64(-1)},
		{"fractional float64", float64(3000.5)},
		{"int above 65535", int(65536)},
		{"uint64 above 65535", uint64(70000)},
		{"string", "3000"},
		{"bool", true},
	}
	for _, tc := range rejected {
		t.Run("rejects port as "+tc.name, func(t *testing.T) {
			n := newAPISelectionNode(plugin.Selection{
				Provider: "builtin",
				Config:   map[string]any{"port": tc.value},
			})
			_, _, err := n.apiPluginSelection(
				plugin.CapabilityAPIBlockfrost,
			)
			require.Error(t, err)
		})
	}
}

// TestAPIPluginSelectionErrors covers the selection-level guards: an empty
// provider and a capability absent from the selection map are both errors.
func TestAPIPluginSelectionErrors(t *testing.T) {
	t.Parallel()

	t.Run("empty provider is rejected", func(t *testing.T) {
		n := newAPISelectionNode(plugin.Selection{
			Provider: "",
			Config:   map[string]any{"port": 3000},
		})
		_, _, err := n.apiPluginSelection(plugin.CapabilityAPIBlockfrost)
		require.Error(t, err)
	})

	t.Run("missing capability is rejected", func(t *testing.T) {
		n := &Node{
			config: Config{
				pluginSelections: map[plugin.Capability]plugin.Selection{},
			},
		}
		_, _, err := n.apiPluginSelection(plugin.CapabilityAPIBlockfrost)
		require.Error(t, err)
	})
}

// TestAPIPluginSelectionDefaultPortPerCapability verifies each API capability
// falls back to its own default port when no port is configured.
func TestAPIPluginSelectionDefaultPortPerCapability(t *testing.T) {
	t.Parallel()

	want := map[plugin.Capability]uint{
		plugin.CapabilityAPIBlockfrost: 3000,
		plugin.CapabilityAPIMesh:       8080,
		plugin.CapabilityAPIUtxorpc:    9090,
		plugin.CapabilityAPIMcp:        0,
	}
	for capability, wantPort := range want {
		n := &Node{
			config: Config{
				pluginSelections: map[plugin.Capability]plugin.Selection{
					capability: {
						Provider: "builtin",
						Config:   map[string]any{},
					},
				},
			},
		}
		_, port, err := n.apiPluginSelection(capability)
		require.NoErrorf(t, err, "capability %s", capability)
		assert.Equalf(t, wantPort, port, "capability %s", capability)
	}
}

func TestNodeRunSkipsZeroPortAPIProviders(t *testing.T) {
	t.Parallel()

	n := newAPIPluginRuntimeNode(t)
	probes := map[plugin.Capability]*apiLifecycleProbe{
		plugin.CapabilityAPIUtxorpc:    {},
		plugin.CapabilityAPIBlockfrost: {},
		plugin.CapabilityAPIMesh:       {},
		plugin.CapabilityAPIMcp:        {},
	}
	for capability, probe := range probes {
		registerAPIProbe(t, n.pluginHost, capability, "probe", probe)
		selectAPIProbe(n, capability, "probe", 0)
	}
	// Force a deterministic failure after the API startup section so Run
	// returns without requiring an external shutdown signal. Reaching block
	// producer validation proves all four zero-port decisions were exercised.
	n.config.blockProducer = true

	require.ErrorIs(
		t,
		n.Run(context.Background()),
		fs.ErrNotExist,
	)
	for capability, probe := range probes {
		assert.Zero(
			t,
			probe.starts.Load(),
			"capability %s started with port 0",
			capability,
		)
		assert.Zero(
			t,
			probe.stops.Load(),
			"capability %s stopped despite never starting",
			capability,
		)
	}
}

func TestNodeRunAPIStartupFailureCleansUpStartedProviders(t *testing.T) {
	t.Parallel()

	n := newAPIPluginRuntimeNode(t)
	utxorpcProbe := &apiLifecycleProbe{}
	blockfrostProbe := &apiLifecycleProbe{
		startErr: errors.New("injected API startup failure"),
	}
	meshProbe := &apiLifecycleProbe{}
	registerAPIProbe(
		t,
		n.pluginHost,
		plugin.CapabilityAPIUtxorpc,
		"probe",
		utxorpcProbe,
	)
	registerAPIProbe(
		t,
		n.pluginHost,
		plugin.CapabilityAPIBlockfrost,
		"probe",
		blockfrostProbe,
	)
	registerAPIProbe(
		t,
		n.pluginHost,
		plugin.CapabilityAPIMesh,
		"probe",
		meshProbe,
	)
	selectAPIProbe(n, plugin.CapabilityAPIUtxorpc, "probe", 19090)
	selectAPIProbe(n, plugin.CapabilityAPIBlockfrost, "probe", 13000)
	selectAPIProbe(n, plugin.CapabilityAPIMesh, "probe", 0)

	err := n.Run(context.Background())
	require.ErrorContains(t, err, "injected API startup failure")
	assert.Equal(t, int32(1), utxorpcProbe.starts.Load())
	assert.Equal(t, int32(1), utxorpcProbe.stops.Load())
	assert.Equal(t, int32(1), blockfrostProbe.starts.Load())
	assert.Equal(t, int32(1), blockfrostProbe.stops.Load())
	assert.Zero(t, meshProbe.starts.Load())
	assert.Zero(t, meshProbe.stops.Load())
}

func TestNodeRunPublicAPIsUseSharedBindAddress(t *testing.T) {
	t.Parallel()
	for _, bind := range []string{"0.0.0.0", "127.0.0.2"} {
		t.Run(bind, func(t *testing.T) {
			n := newAPIPluginRuntimeNode(t)
			t.Cleanup(func() { require.NoError(t, n.Stop()) })
			if bind != "0.0.0.0" {
				WithBindAddr(bind)(&n.config)
				n.config.syncCompatFields()
			}
			probes := map[plugin.Capability]*apiLifecycleProbe{
				plugin.CapabilityAPIUtxorpc:    {},
				plugin.CapabilityAPIBlockfrost: {},
				plugin.CapabilityAPIMesh:       {},
			}
			for capability, probe := range probes {
				registerAPIProbe(
					t,
					n.pluginHost,
					capability,
					"bind-probe",
					probe,
				)
				selectAPIProbe(n, capability, "bind-probe", 18080)
			}
			// Fail after API composition, using lifecycle-only providers:
			// observe the actual dependency handoff without binding public sockets.
			n.config.blockProducer = true
			require.ErrorIs(t, n.Run(context.Background()), fs.ErrNotExist)
			for capability, probe := range probes {
				assert.Equal(t, bind, probe.host, "provider %s", capability)
			}
		})
	}
}

// TestConnectionRecycleSubscriptionsRemainLossless verifies the production
// wiring of both hops of the connection-recycle stream, not EventBus in
// isolation: the ledger-to-connmanager translation and the connmanager handler
// subscription.
//
// A recycle request cannot be replayed. Each publisher raises exactly one per
// connection and then keeps its own "already asked" flag set -- the leios-fetch
// backfill's markProtocolDead is the clearest case, where a dropped request
// leaves a connection whose leios-fetch protocol can never answer again in the
// pool for the rest of its life. Detaching either subscriber
// under backpressure would silently strip requests out of the stream, so both
// stay attached until they drain.
func TestConnectionRecycleSubscriptionsRemainLossless(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Stop()

	started := make(chan struct{})
	releaseCh := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCh) }) }
	defer release()

	// Enough to fill both hops' buffers, so the ledger-side publisher can only
	// finish if an event was dropped or a subscriber was detached.
	const total = 2*event.DefaultSubscriberBuffer + 8
	allHandled := make(chan struct{})
	var handled atomic.Int32
	n := &Node{eventBus: bus}
	n.subscribeConnectionRecycleRequests(func(evt event.Event) {
		if _, ok := evt.Data.(connmanager.ConnectionRecycleRequestedEvent); !ok {
			return
		}
		if handled.Add(1) == 1 {
			close(started)
			<-releaseCh
		}
		if handled.Load() == total {
			close(allHandled)
		}
	})
	n.subscribeLedgerConnectionRecycleTranslation()

	publishRecycle := func(i int) {
		bus.Publish(
			ledger.ConnectionRecycleRequestedEventType,
			event.NewEvent(
				ledger.ConnectionRecycleRequestedEventType,
				ledger.ConnectionRecycleRequestedEvent{
					Reason: "leios_fetch_request_slot_abandoned_" +
						strconv.Itoa(i),
				},
			),
		)
	}

	publishRecycle(0)
	testutil.RequireReceive(
		t,
		started,
		time.Second,
		"connmanager recycle handler did not begin",
	)

	published := make(chan struct{})
	go func() {
		defer close(published)
		for i := 1; i < total; i++ {
			publishRecycle(i)
		}
	}()

	// The ordinary EventBus subscriber timeout is five seconds. A regression to
	// the detaching policy on either hop would let this publish finish at that
	// bound with the surplus recycle requests discarded.
	testutil.RequireNoReceive(
		t,
		published,
		event.RemoteDeliverTimeout+time.Second,
		"a connection-recycle subscription detached instead of retaining the stream",
	)

	release()
	testutil.RequireReceive(
		t,
		published,
		10*time.Second,
		"recycle publisher did not resume after the handler drained",
	)
	testutil.RequireReceive(
		t,
		allHandled,
		10*time.Second,
		"connmanager recycle handler did not receive every retained request",
	)
	require.Equal(t, int32(total), handled.Load())
}

// TestNodeSettingsGateValuesAssemblesLedgerAndGenesisGates covers the one
// piece of the phase 2 gate-enforcement wiring that is reachable without
// booting a full Node: nodeSettingsGateValues, the assembly function Run
// calls from both the normal-startup call site and the deferred,
// post-recovery call site. Testing it here is exactly what makes "factor
// so both call sites use the same values" a real guarantee rather than an
// aspiration -- a future edit that changes one call site's inputs without
// updating this function would be caught here.
//
// This does not, and cannot without booting a real Node through Run,
// exercise the control flow itself: that dbNeedsRecovery defers the call
// rather than skipping it, and that the deferred call runs immediately
// after RecoverCommitTimestampConflict and before history expiry, the
// Midnight indexer, or any network listener starts. Run is a single large
// method whose body constructs the ledger state, event bus, chain
// selector, and every network listener as a side effect of reaching that
// code, so isolating just the recovery-then-enforce sequence would require
// either duplicating most of Run's setup or refactoring Run to extract a
// narrower seam -- out of scope for this fix. That gap is a known,
// explicitly accepted one (see the coordinator's note deferring a
// DevNet/integration-level test of the real Node.Run wiring), not a gap
// this test is pretending to close.
func TestNodeSettingsGateValuesAssemblesLedgerAndGenesisGates(t *testing.T) {
	t.Parallel()

	n := &Node{
		config: Config{
			validateHistorical:         false, // relaxed: taint "on"
			strictUtxoValidation:       true,  // not relaxed: taint "off"
			historyExpiry:              HistoryExpiryConfig{Enabled: true},
			pledgeLeverageEnabled:      true,
			pledgeLeverage:             3,
			fullPotRewardsEnabled:      true,
			delegatorInactivityEnabled: true,
			delegatorInactivity:        5,
			minPoolMargin:              10,
			cardanoNodeConfig: &cardano.CardanoNodeConfig{
				ByronGenesisHash:    "byronhash",
				ShelleyGenesisHash:  "shelleyhash",
				AlonzoGenesisHash:   "alonzohash",
				ConwayGenesisHash:   "conwayhash",
				DijkstraGenesisHash: "",
			},
		},
	}
	values := n.nodeSettingsGateValues()

	require.Equal(
		t,
		nodesettings.LatchOn,
		values["historical_validation_relaxed"],
	)
	require.Equal(
		t,
		nodesettings.LatchOff,
		values["strict_utxo_validation_relaxed"],
	)
	require.Equal(
		t,
		nodesettings.EncodeLatchBool(true, ""),
		values["history_expiry_active"],
	)
	require.Equal(
		t,
		nodesettings.EncodeLatchBool(true, "3"),
		values["pledge_leverage"],
	)
	require.Equal(
		t,
		nodesettings.EncodeLatchBool(true, ""),
		values["full_pot_rewards"],
	)
	require.Equal(
		t,
		nodesettings.EncodeLatchBool(true, "5"),
		values["delegator_inactivity"],
	)
	require.Equal(
		t,
		nodesettings.EncodeLatchBool(true, "10"),
		values["min_pool_margin"],
	)
	require.Equal(t, "byronhash", values["byron_genesis_hash"])
	require.Equal(t, "shelleyhash", values["shelley_genesis_hash"])
	require.Equal(t, "alonzohash", values["alonzo_genesis_hash"])
	require.Equal(t, "conwayhash", values["conway_genesis_hash"])
	// Left empty by the loaded cardano config, so this is passed through
	// as "" rather than omitted: EnforceNodeSettings's FrozenFillOnce
	// class treats an empty configured value as "not known yet," not a
	// mismatch, which is exactly what an era whose hash an older dingo
	// build didn't know about needs.
	require.Equal(t, "", values["dijkstra_genesis_hash"])
}

// TestNodeSettingsGateValuesOmitsGenesisHashesWithoutCardanoConfig covers
// the guard mirrored from config.go's own nil check
// (`n.config.CardanoNodeConfig() != nil`): a caller with no loaded cardano
// config -- true for every gate-enforcement call before the config is
// parsed, and the reason phase 2 cannot run any earlier than it does --
// must not synthesize genesis-hash keys at all, matching
// nodesettings.Evaluate's "absent from configured is skipped" rule rather
// than passing five empty strings that would incorrectly resolve like a
// config that loaded but left every hash unset.
func TestNodeSettingsGateValuesOmitsGenesisHashesWithoutCardanoConfig(
	t *testing.T,
) {
	t.Parallel()

	n := &Node{config: Config{}}
	values := n.nodeSettingsGateValues()

	for _, gate := range []string{
		"byron_genesis_hash",
		"shelley_genesis_hash",
		"alonzo_genesis_hash",
		"conway_genesis_hash",
		"dijkstra_genesis_hash",
	} {
		_, present := values[gate]
		require.False(t, present, "gate %q should be absent, not empty", gate)
	}
}

// snapshotMgrSetterCall matches a snapshot-manager configuration call on the
// node, capturing the setter name.
var snapshotMgrSetterCall = regexp.MustCompile(
	`n\.snapshotMgr\.(Set[A-Za-z0-9_]+)\(`,
)

// koiosParityRetentionWiring matches the retention setter called with the
// operator's own koios-parity enablement. It binds the argument, not just the
// setter name: a call wired from any other field would leave the operator's
// setting ignored exactly as silently as no call at all.
var koiosParityRetentionWiring = regexp.MustCompile(
	`n\.snapshotMgr\.SetRewardAccountOutputRetentionUnbounded\(\s*` +
		`n\.config\.koiosParity\.Enabled,?\s*\)`,
)

// nodeFuncBodyForSnapshotWiring returns the source of the named function, from
// its declaration to the next top-level declaration.
func nodeFuncBodyForSnapshotWiring(
	t *testing.T,
	path string,
	decl string,
) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	s := string(b)
	start := strings.Index(s, decl)
	if start < 0 {
		t.Fatalf("%s not found in %s", decl, path)
	}
	body := s[start+len(decl):]
	if end := strings.Index(body, "\nfunc "); end >= 0 {
		body = body[:end]
	}
	return body
}

// TestReinitializeBackgroundManagersMirrorsRunSnapshotConfig pins that a live
// Restore/Truncate rebuilds the snapshot manager with every option Run()
// configures. reinitializeBackgroundManagers constructs a second
// snapshot.Manager by hand, so an option added to Run() alone is silently
// dropped after a live lifecycle operation and the node keeps running with
// package defaults instead of the operator's configuration -- which is
// exactly what happened to SetDelegatorInactivity (see
// TestLiveTruncateReinitializationPreservesSnapshotManagerDelegatorInactivityConfig)
// and is the same gap retention setter would leave.
func TestReinitializeBackgroundManagersMirrorsRunSnapshotConfig(t *testing.T) {
	t.Parallel()

	runBody := nodeFuncBodyForSnapshotWiring(
		t,
		"node.go",
		"func (n *Node) Run(ctx context.Context) (runErr error) {",
	)
	reinitBody := nodeFuncBodyForSnapshotWiring(
		t,
		"node_lifecycle.go",
		"func (n *Node) reinitializeBackgroundManagers(",
	)

	runSetters := snapshotMgrSetterCall.FindAllStringSubmatch(runBody, -1)
	if len(runSetters) == 0 {
		t.Fatal("no n.snapshotMgr setter calls found in Run")
	}
	reinitSetters := map[string]struct{}{}
	for _, m := range snapshotMgrSetterCall.FindAllStringSubmatch(
		reinitBody,
		-1,
	) {
		reinitSetters[m[1]] = struct{}{}
	}
	for _, m := range runSetters {
		if _, ok := reinitSetters[m[1]]; !ok {
			t.Errorf(
				"Run configures the snapshot manager with %s but "+
					"reinitializeBackgroundManagers does not; a live "+
					"restore/truncate would silently drop that setting",
				m[1],
			)
		}
	}
}

// TestKoiosParityRetentionWiredFromConfigInBothStartupPaths pins
// wiring itself: both node startup paths must widen reward_account_output
// retention from the operator's koios-parity enablement. Without the call the
// node keeps CORE mode's 4-epoch window, and the observer -- whose network
// -bound epoch validation routinely runs many epochs behind chain progression
// during a catch-up sync -- reads an epoch's rows only after
// cleanupOldSnapshots has already deleted them.
func TestKoiosParityRetentionWiredFromConfigInBothStartupPaths(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		path string
		decl string
	}{
		{
			path: "node.go",
			decl: "func (n *Node) Run(ctx context.Context) (runErr error) {",
		},
		{
			path: "node_lifecycle.go",
			decl: "func (n *Node) reinitializeBackgroundManagers(",
		},
	} {
		body := nodeFuncBodyForSnapshotWiring(t, tc.path, tc.decl)
		if !koiosParityRetentionWiring.MatchString(body) {
			t.Errorf(
				"%s does not call "+
					"n.snapshotMgr.SetRewardAccountOutputRetentionUnbounded("+
					"n.config.koiosParity.Enabled); reward_account_output "+
					"would be pruned to the 4-epoch window with the "+
					"koios-parity observer enabled",
				tc.decl,
			)
		}
	}
}

// forgeCreatedBy and electionCreatedBy identify the goroutines that
// BlockForger.Start and Election.Start launch. Both Stop calls join their
// workers, so these lines disappearing is evidence of a join rather than of
// a cancellation: these tests never cancel the context the components were
// started with.
//
// The match is on the "created by" line rather than on the worker's own
// entry frame, because two renderings of a live goroutine carry no entry
// frame at all: one created by `go` but not yet scheduled has an empty
// stack, and one running on another thread prints "stack unavailable".
// runtime.Stack emits the "created by" line in every case, so matching it is
// not a race against the scheduler. Matching the entry frame is: under a
// loaded test binary the forge loop had reliably not been scheduled by the
// time the assertion ran.
//
// The scan covers every goroutine in the test binary, which is sound because
// these are the only tests in this package that start a forger or an election;
// the rest hold an unstarted forging.BlockForger value.
const (
	forgeCreatedBy = "created by " +
		"github.com/blinklabs-io/dingo/ledger/forging.(*BlockForger).Start"
	electionCreatedBy = "created by " +
		"github.com/blinklabs-io/dingo/ledger/leader.(*Election).start"
)

// goroutineStacksContain reports whether any live goroutine's dump mentions
// marker.
func goroutineStacksContain(marker string) bool {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Contains(string(buf[:n]), marker)
		}
		buf = make([]byte, 2*len(buf))
	}
}

func requireGoroutineGone(t *testing.T, marker string) {
	t.Helper()
	require.Eventually(
		t,
		func() bool { return !goroutineStacksContain(marker) },
		10*time.Second,
		10*time.Millisecond,
		"goroutine %q never exited; its Stop was not run", marker,
	)
}

// newStartupCleanupProducerNode builds the smallest node startBlockProducer
// needs: a real database, chain manager, ledger state and event bus, plus the
// devnet credential fixtures.
func newStartupCleanupProducerNode(t *testing.T) *Node {
	t.Helper()
	vrf, kes, opcert := devnetCredPaths(t)
	// The full devnet config, for the Byron genesis and the genesis hashes
	// LedgerState.Start needs to build the genesis block. Its own Shelley
	// genesis is then replaced with one whose system start is recent, so the
	// devnet opcert fixture (KES period 0) is still current.
	cardanoCfg, err := cardano.NewCardanoNodeConfigFromFile(
		filepath.Join("config", "cardano", "devnet", "config.json"),
	)
	require.NoError(t, err)
	// Moved forward in memory rather than rewritten on disk: the fixture's
	// 2022 system start puts the wall clock about a thousand KES periods past
	// the devnet opcert fixture, which issue 0 at KES period 0 cannot cover,
	// and the initial funds and protocol params the genesis block needs stay
	// exactly as shipped.
	cardanoCfg.ShelleyGenesis().SystemStart = time.Now().Add(-time.Hour)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	eventBus := event.NewEventBus(nil, logger)
	t.Cleanup(eventBus.Close)
	chainManager, err := chain.NewManager(db, eventBus)
	require.NoError(t, err)
	ledgerState, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:          db,
		ChainManager:      chainManager,
		EventBus:          eventBus,
		CardanoNodeConfig: cardanoCfg,
		Logger:            logger,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ledgerState.Close() })
	// Started because Run starts it long before the block-producer section,
	// and validateBlockProducerStartup reads the slot clock it initializes.
	require.NoError(t, ledgerState.Start(context.Background()))

	n := &Node{
		config: Config{
			logger:                        logger,
			blockProducer:                 true,
			shelleyVRFKey:                 vrf,
			shelleyKESKey:                 kes,
			shelleyOperationalCertificate: opcert,
			cardanoNodeConfig:             cardanoCfg,
			network:                       "devnet",
			promRegistry:                  prometheus.NewRegistry(),
		},
		db:           db,
		eventBus:     eventBus,
		chainManager: chainManager,
		ledgerState:  ledgerState,
	}
	// Whatever the outcome, leave no forge loop or election worker behind for
	// the rest of the package; both Stop calls are idempotent.
	t.Cleanup(func() {
		if n.blockForger != nil {
			n.blockForger.Stop()
		}
		if n.leaderElection != nil {
			_ = n.leaderElection.Stop()
		}
	})
	return n
}

func newStartupCleanupRunNode(t *testing.T) *Node {
	t.Helper()
	vrf, kes, opcert := devnetCredPaths(t)
	cardanoCfg, err := cardano.NewCardanoNodeConfigFromFile(
		filepath.Join("config", "cardano", "devnet", "config.json"),
	)
	require.NoError(t, err)
	cardanoCfg.ShelleyGenesis().SystemStart = time.Now().Add(-time.Hour)
	n, err := New(NewConfig(
		WithDatabasePath(t.TempDir()),
		WithNetwork("devnet"),
		WithCardanoNodeConfig(cardanoCfg),
		WithNetworkMagic(cardanoCfg.ShelleyGenesis().NetworkMagic),
		WithPrometheusRegistry(prometheus.NewRegistry()),
		WithStorageMode(StorageModeAPI),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:0",
		}),
		WithMidnightConfig(MidnightConfig{Port: 0}),
		WithShutdownTimeout(5*time.Second),
	))
	require.NoError(t, err)
	// Run reaches its block-producer section without binding public sockets.
	n.config.listeners = nil
	for _, capability := range []plugin.Capability{
		plugin.CapabilityAPIUtxorpc,
		plugin.CapabilityAPIBlockfrost,
		plugin.CapabilityAPIMesh,
	} {
		n.config.pluginSelections[capability] = plugin.Selection{
			Provider: "unused",
			Config:   map[string]any{"port": uint(0)},
		}
	}
	n.config.blockProducer = true
	n.config.shelleyVRFKey = vrf
	n.config.shelleyKESKey = kes
	n.config.shelleyOperationalCertificate = opcert
	n.config.leiosVoteSigningKeyFile = filepath.Join(
		t.TempDir(),
		"absent-vote.skey",
	)
	n.leiosVoteManager = &leios.VoteManager{}
	t.Cleanup(func() {
		if n.blockForger != nil {
			n.blockForger.Stop()
		}
		if n.leaderElection != nil {
			_ = n.leaderElection.Stop()
		}
	})
	return n
}

// runStopsLIFO unwinds a startup-cleanup stack the way cleanupFailedStartup
// does, without cancelling the node context: the components must be stopped by
// the registered closures, not by context cancellation.
func runStopsLIFO(stops []func()) {
	for _, stop := range slices.Backward(stops) {
		stop()
	}
}

// TestStartBlockProducerStopIsRegisteredBeforeLeiosVotingCanFail covers the
// ordering defect in the startup-cleanup stack: initBlockForger returns with
// the forge loop and the election's workers already running, and
// enableLeiosVoting runs after it and can fail. With the stop registered after
// that call, a vote-key failure returned a cleanup stack that never joined
// either component, leaving the forge loop running while the LIFO rollback
// closed ledger state, the database and the plugin host.
func TestStartBlockProducerStopIsRegisteredBeforeLeiosVotingCanFail(
	t *testing.T,
) {
	n := newStartupCleanupProducerNode(t)
	kesStopped := false
	n.kesAgentCancel = func() { kesStopped = true }
	// Non-nil so enableLeiosVoting does not take its no-vote-manager early
	// return; it fails on the key file below without ever calling into it.
	n.leiosVoteManager = &leios.VoteManager{}
	n.config.leiosVoteSigningKeyFile = filepath.Join(
		t.TempDir(),
		"absent-vote.skey",
	)

	ctx := t.Context()

	started, err := n.startBlockProducer(ctx, nil)
	require.ErrorContains(t, err, "failed to enable leios voting")
	require.NotNil(t, n.blockForger)
	require.True(
		t,
		n.blockForger.IsRunning(),
		"forger must be running for this test to mean anything",
	)
	require.True(t, goroutineStacksContain(forgeCreatedBy))
	require.True(
		t,
		goroutineStacksContain(electionCreatedBy),
		"election workers must be running for this test to mean anything",
	)

	runStopsLIFO(started)

	require.True(t, kesStopped, "startup rollback must stop the KES agent loop")
	requireGoroutineGone(t, forgeCreatedBy)
	requireGoroutineGone(t, electionCreatedBy)
	require.False(t, n.blockForger.IsRunning())
	require.Len(
		t,
		started,
		1,
		"the forger and election stop must be registered before enableLeiosVoting",
	)
}

// TestStartBlockProducerStopJoinsBothComponentsOnSuccess is the positive case:
// a startup that completes registers exactly one stop, and running it joins
// the forge loop and the election workers, in that order.
func TestStartBlockProducerStopJoinsBothComponentsOnSuccess(t *testing.T) {
	n := newStartupCleanupProducerNode(t)

	ctx := t.Context()

	started, err := n.startBlockProducer(ctx, nil)
	require.NoError(t, err)
	require.Len(t, started, 1)
	require.True(t, n.blockForger.IsRunning())
	require.True(t, goroutineStacksContain(forgeCreatedBy))
	require.True(
		t,
		goroutineStacksContain(electionCreatedBy),
		"election workers must be running for this test to mean anything",
	)

	runStopsLIFO(started)

	require.False(t, n.blockForger.IsRunning())
	requireGoroutineGone(t, forgeCreatedBy)
	requireGoroutineGone(t, electionCreatedBy)
}

func TestNodeRunRegistersBlockProducerStopBeforeLeiosVotingCanFail(
	t *testing.T,
) {
	n := newStartupCleanupRunNode(t)
	kesStopped := false
	n.kesAgentCancel = func() { kesStopped = true }

	err := n.Run(context.Background())
	require.ErrorContains(t, err, "failed to enable leios voting")
	require.True(
		t,
		kesStopped,
		"Run startup rollback must invoke the registered block producer stop",
	)
	require.Nil(t, n.kesAgentCancel)
	require.NotNil(t, n.blockForger)
	require.False(t, n.blockForger.IsRunning())
	requireGoroutineGone(t, forgeCreatedBy)
	requireGoroutineGone(t, electionCreatedBy)
}

// TestEffectiveBarkHostDefaultsToLoopbackWhenLifecycleEnabled guards a real
// P0 gap: bark.go's own empty-Host default is "0.0.0.0" (all interfaces),
// which would expose the database lifecycle service's unauthenticated,
// destructive Restore/Truncate RPCs on every interface by default. An
// operator's explicit --bark-host must still always win.
func TestEffectiveBarkHostDefaultsToLoopbackWhenLifecycleEnabled(t *testing.T) {
	t.Parallel()

	require.Equal(t, "127.0.0.1", effectiveBarkHost("", true))
	require.Equal(t, "", effectiveBarkHost("", false))
	require.Equal(t, "0.0.0.0", effectiveBarkHost("0.0.0.0", true))
	require.Equal(t, "10.0.0.5", effectiveBarkHost("10.0.0.5", false))
}

func TestBackfillRewardLiveStakeAtStartup(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	stakeKey := make([]byte, 28)
	stakeKey[0] = 0x51
	missingStakeKey := make([]byte, 28)
	missingStakeKey[0] = 0x52
	_, err = raw.Exec(`
INSERT INTO account (staking_key, pool, added_slot, active)
VALUES (?, ?, 50, TRUE), (?, ?, 60, TRUE)`,
		stakeKey, make([]byte, 28),
		missingStakeKey, make([]byte, 28),
	)
	require.NoError(t, err)
	// Simulate a post-upgrade write that populated only one credential. The
	// startup check must detect the missing canonical credential, not merely
	// test whether reward_live_stake is empty.
	_, err = raw.Exec(`
INSERT INTO reward_live_stake
    (staking_key, credential_tag, utxo_stake, reward_stake, total_stake,
     registered, updated_slot)
VALUES (?, 0, '0', '0', '0', TRUE, 75)`,
		stakeKey,
	)
	require.NoError(t, err)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(100, make([]byte, 32)),
	}, nil))
	needed, err := db.Metadata().RewardLiveStakeNeedsBackfill(nil)
	require.NoError(t, err)
	require.True(t, needed)

	n := &Node{
		db: db,
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	require.NoError(t, n.backfillRewardLiveStake())

	needed, err = db.Metadata().RewardLiveStakeNeedsBackfill(nil)
	require.NoError(t, err)
	require.False(t, needed)
	for _, key := range [][]byte{stakeKey, missingStakeKey} {
		var live models.RewardLiveStake
		require.NoError(t, raw.QueryRow(`
SELECT staking_key, credential_tag, registered, updated_slot
FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`,
			0, key,
		).Scan(
			&live.StakingKey,
			&live.CredentialTag,
			&live.Registered,
			&live.UpdatedSlot,
		))
		require.Equal(t, uint64(100), live.UpdatedSlot)
	}
}

func newNodeTestConnId(id uint) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: 6000,
		},
		RemoteAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: int(id),
		},
	}
}

type nodeTestSecurityParamLedger struct {
	securityParam int
}

func (m nodeTestSecurityParamLedger) SecurityParam() int {
	return m.securityParam
}

type nodeTestLogSignalHandler struct {
	message string
	seen    chan struct{}
}

type nodeTestLogCountHandler struct {
	message string
	count   *atomic.Int32
}

func (h nodeTestLogCountHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h nodeTestLogCountHandler) Handle(
	_ context.Context,
	record slog.Record,
) error {
	if record.Message == h.message {
		h.count.Add(1)
	}
	return nil
}

func (h nodeTestLogCountHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h nodeTestLogCountHandler) WithGroup(string) slog.Handler {
	return h
}

func (h nodeTestLogSignalHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h nodeTestLogSignalHandler) Handle(
	_ context.Context,
	record slog.Record,
) error {
	if record.Message == h.message {
		select {
		case h.seen <- struct{}{}:
		default:
		}
	}
	return nil
}

func (h nodeTestLogSignalHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h nodeTestLogSignalHandler) WithGroup(string) slog.Handler {
	return h
}

func newNodeTestCardanoNodeCfg(t testing.TB) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"preview/config.json",
		"preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	return cfg
}

func TestHandleChainSwitchEventUpdatesActiveConnection(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	state := chainsync.NewStateWithConfig(
		bus,
		nil,
		chainsync.DefaultConfig(),
	)
	connA := newNodeTestConnId(3001)
	connB := newNodeTestConnId(3002)
	state.AddClientConnId(connA)
	state.AddClientConnId(connB)
	pointA := ocommon.NewPoint(100, []byte("hash-a"))
	pointB := ocommon.NewPoint(200, []byte("hash-b"))
	tipA := ochainsync.Tip{Point: pointA, BlockNumber: 10}
	tipB := ochainsync.Tip{Point: pointB, BlockNumber: 20}
	state.UpdateClientTip(connA, pointA, tipA)
	state.UpdateClientTip(connB, pointB, tipB)
	state.SetClientConnId(connA)
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		chainsyncState: state,
	}

	n.handleChainSwitchEvent(
		event.NewEvent(
			chainselection.ChainSwitchEventType,
			chainselection.ChainSwitchEvent{
				PreviousConnectionId: connA,
				NewConnectionId:      connB,
				NewTip:               tipB,
			},
		),
	)

	active := state.GetClientConnId()
	require.NotNil(t, active)
	clientA := state.GetTrackedClient(connA)
	clientB := state.GetTrackedClient(connB)
	require.NotNil(t, clientA)
	require.NotNil(t, clientB)
	assert.Equal(t, connB, *active)
	assert.Equal(t, pointA, clientA.Cursor)
	assert.Equal(t, pointB, clientB.Cursor)
	assert.Equal(t, uint64(1), clientA.HeadersRecv)
	assert.Equal(t, uint64(1), clientB.HeadersRecv)
}

func TestChainSelectionDoesNotPromoteUntrackedFallback(t *testing.T) {
	t.Parallel()

	for _, selectorFirst := range []bool{true, false} {
		name := "state-removal-first"
		if selectorFirst {
			name = "selector-removal-first"
		}
		t.Run(name, func(t *testing.T) {
			state := chainsync.NewStateWithConfig(
				nil,
				nil,
				chainsync.DefaultConfig(),
			)
			selector := chainselection.NewChainSelector(
				chainselection.ChainSelectorConfig{},
			)
			selected := newNodeTestConnId(3101)
			fallback := newNodeTestConnId(3102)
			require.True(t, state.AddClientConnId(selected))
			require.True(t, state.AddClientConnId(fallback))

			selectedPoint := ocommon.NewPoint(100, []byte("selected"))
			selectedTip := ochainsync.Tip{
				Point:       selectedPoint,
				BlockNumber: 10,
			}
			state.UpdateClientTip(selected, selectedPoint, selectedTip)
			require.True(t, selector.UpdatePeerTip(selected, selectedTip, nil))
			state.SetClientConnId(selected)
			best := selector.GetBestPeer()
			require.NotNil(t, best)
			require.Equal(t, selected, *best)
			trackedFallback := state.GetTrackedClient(fallback)
			require.NotNil(t, trackedFallback)
			require.Zero(
				t, trackedFallback.HeadersRecv,
				"fallback must still be connected but untracked by ChainSync",
			)

			n := &Node{
				config: Config{
					logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
				},
				chainsyncState: state,
				chainSelector:  selector,
			}
			removeFromSelector := func() {
				selector.RemovePeer(selected)
				require.Nil(t, selector.GetBestPeer())
				n.handleChainSelectedNoneEvent(event.NewEvent(
					chainselection.ChainSelectedNoneEventType,
					chainselection.ChainSelectedNoneEvent{
						PreviousConnectionId: selected,
					},
				))
			}
			if selectorFirst {
				removeFromSelector()
				state.RemoveClientConnId(selected)
			} else {
				state.RemoveClientConnId(selected)
				removeFromSelector()
			}

			require.Nil(
				t,
				state.GetClientConnId(),
				"an untracked fallback must not become the ledger source",
			)

			fallbackPoint := ocommon.NewPoint(110, []byte("fallback"))
			fallbackTip := ochainsync.Tip{
				Point:       fallbackPoint,
				BlockNumber: 11,
			}
			state.UpdateClientTip(fallback, fallbackPoint, fallbackTip)
			require.True(t, selector.UpdatePeerTip(fallback, fallbackTip, nil))
			best = selector.GetBestPeer()
			require.NotNil(t, best)
			require.Equal(t, fallback, *best)
			n.handleChainSwitchEvent(event.NewEvent(
				chainselection.ChainSwitchEventType,
				chainselection.ChainSwitchEvent{
					PreviousConnectionId: selected,
					NewConnectionId:      fallback,
					NewTip:               fallbackTip,
				},
			))
			active := state.GetClientConnId()
			require.NotNil(t, active)
			require.Equal(t, fallback, *active)
		})
	}
}

func TestHandleChainSelectedNoneEventDoesNotClearReselectedConnection(
	t *testing.T,
) {
	t.Parallel()

	state := chainsync.NewStateWithConfig(
		nil,
		nil,
		chainsync.DefaultConfig(),
	)
	selector := chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{},
	)
	conn := newNodeTestConnId(3103)
	require.True(t, state.AddClientConnId(conn))
	tipPoint := ocommon.NewPoint(120, []byte("reselected"))
	state.UpdateClientTip(conn, tipPoint, ochainsync.Tip{Point: tipPoint})
	require.True(t, state.TrySetClientConnId(conn))
	tip := ochainsync.Tip{
		Point:       tipPoint,
		BlockNumber: 12,
	}
	require.True(t, selector.UpdatePeerTip(conn, tip, nil))
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		chainsyncState: state,
		chainSelector:  selector,
	}

	n.handleChainSelectedNoneEvent(event.NewEvent(
		chainselection.ChainSelectedNoneEventType,
		chainselection.ChainSelectedNoneEvent{
			PreviousConnectionId: conn,
		},
	))

	active := state.GetClientConnId()
	require.NotNil(t, active)
	require.Equal(t, conn, *active)
}

func TestHandleChainSelectedNoneEventCoalescesLifecycleContention(
	t *testing.T,
) {
	t.Parallel()

	state := chainsync.NewStateWithConfig(
		nil,
		nil,
		chainsync.DefaultConfig(),
	)
	conn := newNodeTestConnId(3104)
	newerPrevious := newNodeTestConnId(3106)
	require.True(t, state.AddClientConnId(conn))
	point := ocommon.NewPoint(130, []byte("selected"))
	state.UpdateClientTip(conn, point, ochainsync.Tip{Point: point})
	require.True(t, state.TrySetClientConnId(conn))

	selector := chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{},
	)
	var logCount atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		config: Config{
			logger: slog.New(nodeTestLogCountHandler{
				message: "chain selection stalled: no selectable peer",
				count:   &logCount,
			}),
		},
		chainsyncState: state,
		chainSelector:  selector,
	}
	n.startChainSelectedNoneWorker(ctx)
	t.Cleanup(func() {
		cancel()
		n.waitChainSelectedNoneWorker()
	})

	n.liveLifecycleMu.Lock()
	for i := range 64 {
		previous := conn
		if i == 63 {
			// A newer coalesced transition can name a peer whose intervening
			// switch was skipped while the lifecycle lock was held. Selection is
			// still none, so the older registry-active peer must still be cleared.
			previous = newerPrevious
		}
		n.handleChainSelectedNoneEvent(event.NewEvent(
			chainselection.ChainSelectedNoneEventType,
			chainselection.ChainSelectedNoneEvent{
				PreviousConnectionId: previous,
			},
		))
	}
	n.liveLifecycleMu.Unlock()

	require.Eventually(t, func() bool {
		return logCount.Load() == 1 && state.GetClientConnId() == nil
	}, 5*time.Second, time.Millisecond)
	require.Never(t, func() bool {
		return logCount.Load() > 1
	}, 100*time.Millisecond, time.Millisecond,
		"a contended event burst must be handled by one coalesced worker")
}

func TestChainSelectedNoneWorkerCancelsDuringLifecycleContention(
	t *testing.T,
) {
	t.Parallel()

	state := chainsync.NewStateWithConfig(
		nil,
		nil,
		chainsync.DefaultConfig(),
	)
	conn := newNodeTestConnId(3105)
	require.True(t, state.AddClientConnId(conn))
	point := ocommon.NewPoint(140, []byte("selected"))
	state.UpdateClientTip(conn, point, ochainsync.Tip{Point: point})
	require.True(t, state.TrySetClientConnId(conn))

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		chainsyncState: state,
		chainSelector: chainselection.NewChainSelector(
			chainselection.ChainSelectorConfig{},
		),
	}
	n.startChainSelectedNoneWorker(ctx)
	n.liveLifecycleMu.Lock()
	n.handleChainSelectedNoneEvent(event.NewEvent(
		chainselection.ChainSelectedNoneEventType,
		chainselection.ChainSelectedNoneEvent{
			PreviousConnectionId: conn,
		},
	))
	cancel()
	n.waitChainSelectedNoneWorker()
	n.liveLifecycleMu.Unlock()

	active := state.GetClientConnId()
	require.NotNil(t, active)
	require.Equal(t, conn, *active)
}

func TestChainSelectedNoneRetryBackoffCaps(t *testing.T) {
	t.Parallel()

	delay := chainSelectedNoneInitialRetryInterval
	delays := make([]time.Duration, 0, 10)
	for range 10 {
		delays = append(delays, delay)
		delay = nextChainSelectedNoneRetryInterval(delay, false)
	}
	require.Equal(t, []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
		80 * time.Millisecond,
		160 * time.Millisecond,
		320 * time.Millisecond,
		640 * time.Millisecond,
		time.Second,
		time.Second,
		time.Second,
	}, delays)
	require.Equal(t,
		chainSelectedNoneInitialRetryInterval,
		nextChainSelectedNoneRetryInterval(delay, true),
		"a successful acquisition must restart the next contention ramp",
	)
}

// TestHandleChainSwitchEventNilChainsyncStateDoesNotPanic covers the window
// during a live database restore/truncate where n.chainsyncState is nil
// between closeStorageForLiveLifecycleOp and reinitializeNetworkingCore.
// chainSelector's evaluation loop is never paused during quiesce, so it can
// still emit a ChainSwitchEvent in that window.
func TestHandleChainSwitchEventNilChainsyncStateDoesNotPanic(t *testing.T) {
	t.Parallel()

	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}

	require.NotPanics(t, func() {
		n.handleChainSwitchEvent(
			event.NewEvent(
				chainselection.ChainSwitchEventType,
				chainselection.ChainSwitchEvent{
					NewConnectionId: newNodeTestConnId(3003),
					NewTip: ochainsync.Tip{
						Point: ocommon.NewPoint(100, []byte("hash-a")),
					},
				},
			),
		)
	})
}

// TestHandleChainSwitchEventSkipsUpdateDuringLiveLifecycleOp covers the same
// window from the other side: n.chainsyncState has already been rebuilt to a
// non-nil value, but a live restore/truncate still holds n.liveLifecycleMu
// (held for its entire quiesce-through-reinitialize duration), so the
// handler must not block waiting for it -- it should skip the update rather
// than stall the EventBus dispatch goroutine behind a possibly long-running
// operation.
func TestHandleChainSwitchEventSkipsUpdateDuringLiveLifecycleOp(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	state := chainsync.NewStateWithConfig(
		bus,
		nil,
		chainsync.DefaultConfig(),
	)
	connA := newNodeTestConnId(3001)
	connB := newNodeTestConnId(3002)
	state.AddClientConnId(connA)
	state.AddClientConnId(connB)
	pointA := ocommon.NewPoint(100, []byte("hash-a"))
	state.UpdateClientTipWithoutDedup(
		connA, pointA, ochainsync.Tip{Point: pointA},
	)
	require.True(t, state.TrySetClientConnId(connA))
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		chainsyncState: state,
	}

	n.liveLifecycleMu.Lock()
	defer n.liveLifecycleMu.Unlock()

	n.handleChainSwitchEvent(
		event.NewEvent(
			chainselection.ChainSwitchEventType,
			chainselection.ChainSwitchEvent{
				PreviousConnectionId: connA,
				NewConnectionId:      connB,
				NewTip: ochainsync.Tip{
					Point: ocommon.NewPoint(200, []byte("hash-b")),
				},
			},
		),
	)

	active := state.GetClientConnId()
	require.NotNil(t, active)
	assert.Equal(t, connA, *active)
}

func TestLedgerStateConfigSkipsChainsyncReadDuringLiveLifecycleOp(
	t *testing.T,
) {
	t.Parallel()

	state := chainsync.NewStateWithConfig(
		nil,
		nil,
		chainsync.DefaultConfig(),
	)
	connId := newNodeTestConnId(3001)
	require.True(t, state.AddClientConnId(connId))
	point := ocommon.NewPoint(100, []byte("header"))
	state.UpdateClientTipWithoutDedup(
		connId, point, ochainsync.Tip{Point: point},
	)
	require.True(t, state.TrySetClientConnId(connId))
	n := &Node{
		chainsyncState: state,
		config:         Config{cfg: &internalconfig.Config{}},
	}
	config := n.ledgerStateConfig()

	active := config.GetActiveConnectionFunc()
	require.NotNil(t, active)
	assert.Equal(t, connId, *active)

	n.liveLifecycleMu.Lock()
	active = config.GetActiveConnectionFunc()
	n.liveLifecycleMu.Unlock()
	assert.Nil(t, active)
}

// TestLedgerStateConfigForwardsBlockPipelineFlags is the second half of the
// pipeline-flag regression coverage: it proves that a Config built through the
// public NewConfig/With... option API -- not a hand-built struct literal --
// carries BlockPipelineEnabled and BlockPipelineValidateEnabled all the way
// into the ledger.LedgerStateConfig that ledgerStateConfig() hands to
// NewLedgerState. internal/node/node_test.go's
// TestBuildDingoConfigWiresBlockPipelineFlags covers the other half: the
// internal/config.Config -> dingo.Config hop that was the actual defect.
// Together the two tests span the full path a live serve run takes.
func TestLedgerStateConfigForwardsBlockPipelineFlags(t *testing.T) {
	t.Parallel()

	cfg := NewConfig(
		WithBlockPipelineEnabled(true),
		WithBlockPipelineValidateEnabled(true),
	)
	n := &Node{config: cfg}
	lsCfg := n.ledgerStateConfig()

	assert.True(
		t,
		lsCfg.BlockPipelineEnabled,
		"expected LedgerStateConfig.BlockPipelineEnabled true; the parallel "+
			"block decode pipeline never constructs on the serve path "+
			"otherwise",
	)
	assert.True(
		t,
		lsCfg.BlockPipelineValidateEnabled,
		"expected LedgerStateConfig.BlockPipelineValidateEnabled true; the "+
			"pipeline's parallel VRF/KES validate stage never activates "+
			"otherwise",
	)
}

// The ledger is started, and replays any stored blocks it has not applied,
// before the node creates Ouroboros networking. ledgerStateConfig therefore
// hands the ledger callbacks that run while n.ouroboros() is still nil, and
// each of them must report "unavailable" instead of dereferencing it. A
// restart after a Musashi shutdown exposed this ordering when
// EndorserBlockTxsByHash panicked before networking was initialized.
func TestLedgerStateConfigCallbacksTolerateMissingOuroboros(t *testing.T) {
	t.Parallel()

	newConfig := func(t *testing.T) ledger.LedgerStateConfig {
		t.Helper()
		n := &Node{config: Config{cfg: &internalconfig.Config{}}}
		require.Nil(t, n.ouroboros())
		return n.ledgerStateConfig()
	}

	t.Run("endorser block provider reports unavailable", func(t *testing.T) {
		t.Parallel()
		cfg := newConfig(t)
		var (
			txs []cbor.RawMessage
			ok  bool
		)
		require.NotPanics(t, func() {
			txs, ok = cfg.EndorserBlockProvider([]byte("eb-hash"), 660070)
		})
		assert.False(t, ok, "an endorser block must not be reported present")
		assert.Empty(t, txs)
	})

	t.Run("endorser block fetcher returns an error", func(t *testing.T) {
		t.Parallel()
		cfg := newConfig(t)
		var err error
		require.NotPanics(t, func() {
			err = cfg.EndorserBlockFetcher(
				t.Context(), 660070, []byte("eb-hash"),
			)
		})
		assert.ErrorIs(t, err, errOuroborosNotStarted)
	})

	t.Run("blockfetch range request returns an error", func(t *testing.T) {
		t.Parallel()
		cfg := newConfig(t)
		var err error
		require.NotPanics(t, func() {
			_, err = cfg.BlockfetchRequestRangeFunc(
				newNodeTestConnId(3001),
				ocommon.NewPoint(1, []byte("start")),
				ocommon.NewPoint(2, []byte("end")),
			)
		})
		assert.ErrorIs(t, err, errOuroborosNotStarted)
	})

	t.Run("block decode cache reject is a no-op", func(t *testing.T) {
		t.Parallel()
		cfg := newConfig(t)
		require.NotPanics(t, func() {
			cfg.RejectBlockDecodeCacheFunc(7, []byte("raw-block"))
		})
	})
}

// The "not started" answers above apply only while n.ouroboros() is nil. Once
// networking exists the callbacks must reach it, or the ledger would treat
// every endorser block as permanently unavailable.
func TestLedgerStateConfigCallbacksDelegateOnceOuroborosExists(t *testing.T) {
	t.Parallel()

	n := &Node{config: Config{cfg: &internalconfig.Config{}}}
	n.ouroborosRef.Store(&ouroborosPkg.Ouroboros{})
	cfg := n.ledgerStateConfig()

	var err error
	require.NotPanics(t, func() {
		err = cfg.EndorserBlockFetcher(t.Context(), 660070, []byte("eb-hash"))
	})
	require.Error(t, err)
	assert.NotErrorIs(t, err, errOuroborosNotStarted)
}

func TestLedgerStateConfigUsesMusashiCertificateTrust(t *testing.T) {
	t.Parallel()

	t.Run("Musashi prototype trusts certificate without vote manager", func(t *testing.T) {
		n := &Node{config: Config{cfg: &internalconfig.Config{
			Network:      ouroboros.NetworkCardanoMusashi.Name,
			NetworkMagic: ouroboros.NetworkCardanoMusashi.NetworkMagic,
		}}}
		validate := n.ledgerStateConfig().ValidateLeiosCertificate
		require.NotNil(t, validate)
		require.NoError(t, validate(0, nil, nil, nil))
	})

	t.Run("standard network still requires certificate verifier", func(t *testing.T) {
		n := &Node{config: Config{cfg: &internalconfig.Config{
			Network:      ouroboros.NetworkCardanoPreview.Name,
			NetworkMagic: ouroboros.NetworkCardanoPreview.NetworkMagic,
		}}}
		validate := n.ledgerStateConfig().ValidateLeiosCertificate
		require.NotNil(t, validate)
		require.ErrorContains(t, validate(0, nil, nil, nil), "vote manager is unavailable")
	})
}

func TestChainsyncIngressEligibilityCacheDefaultsAndUpdates(t *testing.T) {
	t.Parallel()

	connId := newNodeTestConnId(3003)
	n := &Node{}

	assert.False(t, n.isChainsyncIngressEligible(connId))

	n.handlePeerEligibilityChangedEvent(event.NewEvent(
		peergov.PeerEligibilityChangedEventType,
		peergov.PeerEligibilityChangedEvent{
			ConnectionId: connId,
			Eligible:     false,
		},
	))
	assert.False(t, n.isChainsyncIngressEligible(connId))

	n.handlePeerEligibilityChangedEvent(event.NewEvent(
		peergov.PeerEligibilityChangedEventType,
		peergov.PeerEligibilityChangedEvent{
			ConnectionId: connId,
			Eligible:     true,
		},
	))
	assert.True(t, n.isChainsyncIngressEligible(connId))

	n.deleteChainsyncIngressEligibility(connId)
	assert.False(t, n.isChainsyncIngressEligible(connId))
}

func TestStopReturnsSameShutdownErrorAfterFirstCall(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("shutdown failed")
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		shutdownFuncs: []func(context.Context) error{
			func(context.Context) error {
				return wantErr
			},
		},
	}

	firstErr := n.Stop()
	secondErr := n.Stop()
	require.ErrorIs(t, firstErr, wantErr)
	require.ErrorIs(t, secondErr, wantErr)
	require.Equal(t, firstErr, secondErr)
}

// TestStartupFailureCleanupCancelsBeforeAllowingShutdown verifies the
// signal-during-startup lifecycle boundary. Run owns startupLifecycleMu while
// it unwinds its LIFO stack; shutdown must wait for that rollback rather than
// closing the same partially initialized resource concurrently.
func TestStartupFailureCleanupCancelsBeforeAllowingShutdown(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	rollbackStarted := make(chan struct{})
	releaseRollback := make(chan struct{})
	var releaseRollbackOnce sync.Once
	release := func() { releaseRollbackOnce.Do(func() { close(releaseRollback) }) }
	defer release()
	rollbackDone := make(chan struct{})
	shutdownFuncStarted := make(chan struct{})
	shutdownDone := make(chan error, 1)

	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		ctx:    ctx,
		cancel: cancel,
		shutdownFuncs: []func(context.Context) error{
			func(context.Context) error {
				close(shutdownFuncStarted)
				return nil
			},
		},
	}

	// Match Run's startup section: cleanupFailedStartup owns the gate until
	// every started component's rollback completes.
	n.startupLifecycleMu.Lock()
	go func() {
		defer close(rollbackDone)
		n.cleanupFailedStartup([]func(){func() {
			close(rollbackStarted)
			<-releaseRollback
		}})
	}()
	testutil.RequireReceive(
		t,
		rollbackStarted,
		time.Second,
		"startup rollback to begin",
	)
	require.ErrorIs(t, ctx.Err(), context.Canceled)

	go func() {
		shutdownDone <- n.shutdown()
	}()
	// If shutdown did not take the same gate, its phase-four callback would
	// run while the startup rollback is intentionally blocked above.
	testutil.RequireNoReceive(
		t,
		shutdownFuncStarted,
		50*time.Millisecond,
		"normal shutdown while startup rollback owns the lifecycle gate",
	)

	release()
	testutil.RequireReceive(
		t,
		rollbackDone,
		time.Second,
		"startup rollback completion",
	)
	testutil.RequireReceive(
		t,
		shutdownFuncStarted,
		time.Second,
		"normal shutdown after startup rollback completion",
	)
	require.NoError(t, <-shutdownDone)
}

// TestStopWaitsForLiveLifecycleOperation protects the shared storage lifecycle
// boundary. Restore and Truncate hold liveLifecycleMu and snapshotMu while
// they stop readers, close the old database, and rebuild its dependents. A
// concurrent Stop must wait for both gates before cancelling those readers or
// closing the database; otherwise the two teardown paths can use and close
// the same storage concurrently under suite load.
func TestStopWaitsForLiveLifecycleOperation(t *testing.T) {
	tests := []struct {
		name   string
		lock   func(*Node)
		unlock func(*Node)
	}{
		{
			name:   "restore or truncate",
			lock:   func(n *Node) { n.liveLifecycleMu.Lock() },
			unlock: func(n *Node) { n.liveLifecycleMu.Unlock() },
		},
		{
			name:   "snapshot",
			lock:   func(n *Node) { n.snapshotMu.Lock() },
			unlock: func(n *Node) { n.snapshotMu.Unlock() },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			phaseStarted := make(chan struct{}, 1)
			n := &Node{
				config: Config{
					logger: slog.New(nodeTestLogSignalHandler{
						message: "shutdown phase 1: stopping new work",
						seen:    phaseStarted,
					}),
				},
			}
			test.lock(n)
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { test.unlock(n) }) }
			t.Cleanup(release)

			stopDone := make(chan error, 1)
			go func() { stopDone <- n.Stop() }()

			// Shutdown must not reach phase 1 until the live operation has
			// released the gate it owns.
			testutil.RequireNoReceive(
				t,
				phaseStarted,
				50*time.Millisecond,
				"shutdown must wait for the live lifecycle gate",
			)

			release()
			testutil.RequireReceive(
				t,
				phaseStarted,
				time.Second,
				"shutdown phase 1 after the live lifecycle gate",
			)
			require.NoError(t, <-stopDone)
		})
	}
}

func TestStopCancelsBeforeLiveLifecycleGateTimeout(t *testing.T) {
	cancelCalled := make(chan struct{})
	var cancelOnce sync.Once
	n := &Node{
		config: NewConfig(
			WithShutdownTimeout(50*time.Millisecond),
			WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		),
		cancel: func() { cancelOnce.Do(func() { close(cancelCalled) }) },
	}

	n.liveLifecycleMu.Lock()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { n.liveLifecycleMu.Unlock() }) }
	defer release()

	err := n.Stop()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	testutil.RequireReceive(
		t,
		cancelCalled,
		time.Second,
		"shutdown must cancel the node even when a lifecycle gate times out",
	)

	release()
	require.NoError(t, n.Stop())
}

func TestShutdownClosesEventBusBeforeFinalCleanup(t *testing.T) {
	t.Parallel()

	const eventType event.EventType = "test.shutdown.order"

	bus := event.NewEventBus(nil, nil)
	_, _ = bus.SubscribeWithBuffer(eventType, 1)
	bus.Publish(eventType, event.NewEvent(eventType, "fill"))

	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		bus.Publish(eventType, event.NewEvent(eventType, "blocked"))
	}()
	testutil.RequireNoReceive(
		t,
		publishDone,
		50*time.Millisecond,
		"event publisher should be backpressured before shutdown",
	)

	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		eventBus: bus,
		shutdownFuncs: []func(context.Context) error{
			func(context.Context) error {
				select {
				case <-publishDone:
					return nil
				case <-time.After(time.Second):
					return errors.New(
						"event bus was not closed before final cleanup",
					)
				}
			},
		},
	}

	require.NoError(t, n.Stop())
	testutil.RequireReceive(
		t,
		publishDone,
		time.Second,
		"backpressured publisher did not exit after node shutdown",
	)
}

func TestCloseWithShutdownTimeoutReturnsTimeoutError(t *testing.T) {
	t.Parallel()

	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	releaseClose := make(chan struct{})
	closeDone := make(chan struct{})

	err := n.closeWithShutdownTimeout(
		context.Background(),
		"test",
		0,
		func() error {
			defer close(closeDone)
			<-releaseClose
			return nil
		},
	)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	close(releaseClose)
	testutil.RequireReceive(
		t,
		closeDone,
		time.Second,
		"close function completion",
	)
}

// TestShutdownDoesNotCloseDatabaseWhenLedgerDrainIsUnconfirmed protects the
// storage safety boundary shared with live Restore/Truncate. LedgerState.Close
// can time out while a database worker is still using the database; normal
// shutdown must not close the database or its provider-owned stores in that
// state.
// Not t.Parallel: swaps ledger.CloseDBWorkerPoolShutdownTimeout, a variable
// in another package that every concurrent LedgerState close would observe.
func TestShutdownDoesNotCloseDatabaseWhenLedgerDrainIsUnconfirmed(
	t *testing.T,
) {
	n, _ := newLiveLifecycleTestNodeWithGenesis(
		t,
		1,
		nil,
		ledger.DatabaseWorkerPoolConfig{WorkerPoolSize: 1, TaskQueueSize: 1},
	)

	origTimeout := ledger.CloseDBWorkerPoolShutdownTimeout
	ledger.CloseDBWorkerPoolShutdownTimeout = 10 * time.Millisecond
	t.Cleanup(func() { ledger.CloseDBWorkerPoolShutdownTimeout = origTimeout })

	started := make(chan struct{})
	release := make(chan struct{})
	workerDone := make(chan struct{})
	defer func() {
		close(release)
		testutil.RequireReceive(
			t,
			workerDone,
			time.Second,
			"database worker drain",
		)
	}()
	go func() {
		defer close(workerDone)
		_ = n.ledgerState.SubmitAsyncDBOperation(
			func(*database.Database) error {
				close(started)
				<-release
				return nil
			},
		)
	}()
	<-started

	shutdownErr := n.shutdown()
	require.Error(t, shutdownErr)
	require.ErrorContains(t, shutdownErr, "database worker pool")
	require.ErrorContains(t, shutdownErr, "database close skipped")

	// The ledger worker is still blocked, so the database must remain usable.
	require.NoError(t, n.db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1, make([]byte, 32)),
	}, nil))
}

// TestCleanupFailedStartupSkipsDatabaseCloseWhenLedgerDrainIsUnconfirmed
// covers the startup-failure LIFO rollback path with the same guard
// TestShutdownDoesNotCloseDatabaseWhenLedgerDrainIsUnconfirmed covers for the
// normal signal-driven path: cleanupFailedStartup runs the same ledgerState
// timeout Run() registers, and the earlier-registered (so later-run) db.Close
// and pluginHost.Stop LIFO stops must skip closing storage a still-running
// background goroutine may be using, not silently discard the drain failure.
//
// Unlike that shutdown() test, this one hand-builds the rollback slice
// rather than driving Run() to a real startup failure: shutdown()'s phase
// ordering is hard-coded directly in that function, so calling it exercises
// the real order; cleanupFailedStartup's ordering is purely a property of
// which `started = append(started, ...)` calls Run() happens to reach before
// failing, assembled across ~30 such calls interleaved through Run()'s
// startup sequence, each registered immediately after the resource it tears
// down becomes available -- so driving the real path here would mean
// injecting a failure at a specific point inside that sequence rather than
// calling one self-contained function. This test therefore only proves the
// guard logic is correct given the order Run() is documented (here and in
// ARCHITECTURE.md) to register it in; it cannot catch a future edit to Run()
// that reorders the db.Close/pluginHost.Stop/ledgerState.Close registrations
// relative to each other. Matches this file's existing convention for
// exercising cleanupFailedStartup with a hand-built `started` (see the
// startup-lifecycle-gate test above) and newLiveLifecycleTestNode's own
// documented pattern of wiring a real Node without going through Run().
// Not t.Parallel: swaps ledger.CloseDBWorkerPoolShutdownTimeout, a variable
// in another package that every concurrent LedgerState close would observe.
func TestCleanupFailedStartupSkipsDatabaseCloseWhenLedgerDrainIsUnconfirmed(
	t *testing.T,
) {
	n, _ := newLiveLifecycleTestNodeWithGenesis(
		t,
		1,
		nil,
		ledger.DatabaseWorkerPoolConfig{WorkerPoolSize: 1, TaskQueueSize: 1},
	)

	origTimeout := ledger.CloseDBWorkerPoolShutdownTimeout
	ledger.CloseDBWorkerPoolShutdownTimeout = 10 * time.Millisecond
	t.Cleanup(func() { ledger.CloseDBWorkerPoolShutdownTimeout = origTimeout })

	started := make(chan struct{})
	release := make(chan struct{})
	workerDone := make(chan struct{})
	defer func() {
		close(release)
		testutil.RequireReceive(
			t,
			workerDone,
			time.Second,
			"database worker drain",
		)
	}()
	go func() {
		defer close(workerDone)
		_ = n.ledgerState.SubmitAsyncDBOperation(
			func(*database.Database) error {
				close(started)
				<-release
				return nil
			},
		)
	}()
	<-started

	// Mirror Run's exact registration order and skip logic: ledgerState.Close
	// registered last (so run first in LIFO) sets the flag; db.Close and
	// pluginHost.Stop, registered earlier (so run later), check it.
	ledgerStateDrainConfirmed := true
	var pluginHostStopped, dbClosed bool
	rollback := []func(){
		func() {
			if !ledgerStateDrainConfirmed {
				return
			}
			dbClosed = true
			_ = n.db.Close()
		},
		func() {
			if !ledgerStateDrainConfirmed {
				return
			}
			pluginHostStopped = true
			_ = n.pluginHost.Stop(context.Background())
		},
		func() {
			if err := n.ledgerState.Close(); err != nil {
				ledgerStateDrainConfirmed = false
			}
		},
	}
	n.startupLifecycleMu.Lock()
	n.cleanupFailedStartup(rollback)

	assert.False(
		t,
		dbClosed,
		"db.Close must be skipped when the ledger drain is unconfirmed",
	)
	assert.False(
		t,
		pluginHostStopped,
		"pluginHost.Stop must be skipped when the ledger drain is unconfirmed",
	)
	// The ledger worker is still blocked, so the database must remain usable.
	require.NoError(t, n.db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1, make([]byte, 32)),
	}, nil))
}

// newChainSelectorSubscriptionTestNode builds the minimal node
// subscribeChainSelectorEvents needs, so tests can register the production
// subscriptions instead of reimplementing them.
func newChainSelectorSubscriptionTestNode(
	t *testing.T,
	bus *event.EventBus,
	cs *chainselection.ChainSelector,
) *Node {
	t.Helper()
	return &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		eventBus:      bus,
		chainSelector: cs,
	}
}

type blockingNodeTestLogHandler struct {
	entered     chan struct{}
	release     chan struct{}
	calls       atomic.Int32
	once        sync.Once
	releaseOnce sync.Once
}

func (h *blockingNodeTestLogHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *blockingNodeTestLogHandler) Handle(
	_ context.Context,
	record slog.Record,
) error {
	if record.Level < slog.LevelWarn {
		return nil
	}
	h.calls.Add(1)
	h.once.Do(func() {
		close(h.entered)
		<-h.release
	})
	return nil
}

func (h *blockingNodeTestLogHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *blockingNodeTestLogHandler) WithGroup(string) slog.Handler {
	return h
}

func (h *blockingNodeTestLogHandler) unblock() {
	h.releaseOnce.Do(func() { close(h.release) })
}

var nodeRequiredSubscriptionGroups = []struct {
	function string
	count    int
}{
	{function: "Run", count: 3},
	{function: "subscribeChainsyncClientRemoveRequests", count: 1},
	{function: "subscribeConnectionEvents", count: 3},
	{function: "subscribeChainSelectorEvents", count: 8},
	{function: "initLeiosVoteManager", count: 2},
	{function: "startKoiosParityObserver", count: 1},
}

func TestNodeEventSubscriptionClassifications(t *testing.T) {
	t.Parallel()

	expectedRequired := make(map[string]int, len(nodeRequiredSubscriptionGroups))
	for _, group := range nodeRequiredSubscriptionGroups {
		expectedRequired[group.function] = group.count
	}
	expectedDetachable := map[string]int{
		"subscribeChainSelectorEvents": 1,
	}
	expectedPolicies := map[string]string{
		"subscribeRequiredEvent":                      "SubscriberBackpressureBlock",
		"subscribeDetachableEvent":                    "SubscriberBackpressureDetach",
		"subscribeConnectionRecycleRequests":          "SubscriberBackpressureBlock",
		"subscribeLedgerConnectionRecycleTranslation": "SubscriberBackpressureBlock",
	}
	expectedChainsyncRegistrations := map[string]int{
		"Run":                        1,
		"reinitializeNetworkingCore": 1,
	}

	files, err := filepath.Glob("node*.go")
	require.NoError(t, err)

	fset := token.NewFileSet()
	actualRequired := make(map[string]int)
	actualDetachable := make(map[string]int)
	actualPolicies := make(map[string]string)
	actualChainsyncRegistrations := make(map[string]int)
	var unclassified []string
	for _, filename := range files {
		if strings.HasSuffix(filename, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filename, nil, 0)
		require.NoError(t, err)
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch selector.Sel.Name {
				case "subscribeRequiredEvent":
					actualRequired[function.Name.Name]++
				case "subscribeDetachableEvent":
					actualDetachable[function.Name.Name]++
				case "subscribeChainsyncClientRemoveRequests":
					actualChainsyncRegistrations[function.Name.Name]++
				case "Subscribe", "SubscribeWithBuffer", "SubscribeFunc",
					"SubscribeFuncWithBuffer", "SubscribeFuncStrict":
					unclassified = append(
						unclassified,
						fmt.Sprintf("%s:%s", filename, function.Name.Name),
					)
				case "SubscribeFuncWithBufferPolicy":
					_, ok := actualPolicies[function.Name.Name]
					if ok {
						unclassified = append(
							unclassified,
							fmt.Sprintf("duplicate policy helper: %s", function.Name.Name),
						)
						return true
					}
					if len(call.Args) < 3 {
						unclassified = append(
							unclassified,
							fmt.Sprintf("policy missing: %s:%s", filename, function.Name.Name),
						)
						return true
					}
					policySelector, ok := call.Args[2].(*ast.SelectorExpr)
					if !ok {
						unclassified = append(
							unclassified,
							fmt.Sprintf("policy not explicit: %s:%s", filename, function.Name.Name),
						)
						return true
					}
					actualPolicies[function.Name.Name] = policySelector.Sel.Name
				}
				return true
			})
		}
	}

	require.Empty(t, unclassified,
		"node-owned EventBus function subscriptions must use a policy helper")
	require.Equal(t, expectedRequired, actualRequired)
	require.Equal(t, expectedDetachable, actualDetachable)
	require.Equal(t, expectedPolicies, actualPolicies)
	require.Equal(t, expectedChainsyncRegistrations, actualChainsyncRegistrations)
}

func TestNodeRequiredSubscriptionsKeepPublishersBlocked(t *testing.T) {
	t.Parallel()

	type subscriberCase struct {
		name        string
		eventType   event.EventType
		logger      *blockingNodeTestLogHandler
		queueFilled chan struct{}
		published   chan struct{}
	}
	var cases []subscriberCase
	for _, group := range nodeRequiredSubscriptionGroups {
		for i := range group.count {
			cases = append(cases, subscriberCase{
				name: fmt.Sprintf("%s %d", group.function, i+1),
				eventType: event.EventType(fmt.Sprintf(
					"node.required.test.%d", len(cases),
				)),
				logger: &blockingNodeTestLogHandler{
					entered: make(chan struct{}),
					release: make(chan struct{}),
				},
				queueFilled: make(chan struct{}),
				published:   make(chan struct{}),
			})
		}
	}

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() {
		for _, testCase := range cases {
			testCase.logger.unblock()
		}
		bus.Stop()
	})
	n := &Node{eventBus: bus}
	for _, testCase := range cases {
		logger := slog.New(testCase.logger)
		n.subscribeRequiredEvent(testCase.eventType, func(event.Event) {
			logger.Warn("blocked test subscriber")
		})
		bus.Publish(testCase.eventType, event.NewEvent(testCase.eventType, nil))
		testutil.RequireReceive(
			t,
			testCase.logger.entered,
			time.Second,
			"required subscriber callback should enter",
		)
	}

	for _, testCase := range cases {
		go func(testCase subscriberCase) {
			defer close(testCase.published)
			for i := range event.DefaultSubscriberBuffer + 2 {
				bus.Publish(
					testCase.eventType,
					event.NewEvent(testCase.eventType, nil),
				)
				if i == event.DefaultSubscriberBuffer-1 {
					close(testCase.queueFilled)
				}
			}
		}(testCase)
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for _, testCase := range cases {
		select {
		case <-testCase.queueFilled:
		case <-deadline.C:
			t.Fatalf("%s subscriber queue did not fill", testCase.name)
		}
	}

	allPublished := make(chan struct{})
	go func() {
		for _, testCase := range cases {
			<-testCase.published
		}
		close(allPublished)
	}()
	select {
	case <-allPublished:
		t.Fatal("a required subscriber detached while its handler was stalled")
	case <-time.After(event.RemoteDeliverTimeout + 250*time.Millisecond):
	}

	for _, testCase := range cases {
		testCase.logger.unblock()
	}
	select {
	case <-allPublished:
	case <-time.After(5 * time.Second):
		t.Fatal("publishers did not resume after required subscribers drained")
	}
	for _, testCase := range cases {
		require.Eventually(t, func() bool {
			return testCase.logger.calls.Load() == event.DefaultSubscriberBuffer+3
		}, time.Second, 5*time.Millisecond,
			"subscriber queue should drain: %s", testCase.name)
	}
}

func TestNodeRequiredChainSelectorSubscriberRecoversAfterSaturation(t *testing.T) {
	t.Parallel()

	loggerHandler := &blockingNodeTestLogHandler{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	defer loggerHandler.unblock()
	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		Logger:        slog.New(loggerHandler),
		SecurityParam: 1,
	})
	referenceConn := newNodeTestConnId(5101)
	cs.UpdatePeerTip(referenceConn, ochainsync.Tip{
		Point:       ocommon.NewPoint(10, []byte("reference")),
		BlockNumber: 1,
	}, nil)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	n := newChainSelectorSubscriptionTestNode(t, bus, cs)
	n.subscribeChainSelectorEvents()

	// The first impossible advertised tip blocks in the real selector callback's
	// warning logger. Further publications fill the production subscriber queue.
	stalled := chainselection.PeerTipUpdateEvent{
		ConnectionId: newNodeTestConnId(5102),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(1000, []byte("untrusted")),
			BlockNumber: 1000,
		},
	}
	bus.Publish(
		chainselection.PeerTipUpdateEventType,
		event.NewEvent(chainselection.PeerTipUpdateEventType, stalled),
	)
	select {
	case <-loggerHandler.entered:
	case <-time.After(time.Second):
		t.Fatal("production chain-selector callback did not enter the blocking logger")
	}

	queueFilled := make(chan struct{})
	published := make(chan struct{})
	go func() {
		for i := range event.DefaultSubscriberBuffer + 2 {
			bus.Publish(
				chainselection.PeerTipUpdateEventType,
				event.NewEvent(chainselection.PeerTipUpdateEventType, stalled),
			)
			if i == event.DefaultSubscriberBuffer-1 {
				close(queueFilled)
			}
		}
		close(published)
	}()
	select {
	case <-queueFilled:
	case <-time.After(time.Second):
		t.Fatal("production chain-selector callback queue did not fill")
	}
	select {
	case <-published:
		t.Fatal("required Node subscription stopped applying back-pressure")
	case <-time.After(event.RemoteDeliverTimeout + time.Second):
	}
	loggerHandler.unblock()
	select {
	case <-published:
	case <-time.After(time.Second):
		t.Fatal("required Node subscription did not resume publishing after callback recovery")
	}

	bus.Publish(
		chainselection.PeerTipUpdateEventType,
		event.NewEvent(chainselection.PeerTipUpdateEventType,
			chainselection.PeerTipUpdateEvent{
				ConnectionId: referenceConn,
				Tip: ochainsync.Tip{
					Point:       ocommon.NewPoint(20, []byte("recovered")),
					BlockNumber: 2,
				},
			}),
	)
	require.Eventually(t, func() bool {
		got := cs.GetPeerTip(referenceConn)
		return got != nil && got.Tip.BlockNumber == 2
	}, time.Second, 5*time.Millisecond,
		"required Node subscription must process events after the callback drains")
}

func TestNodeChainForkDiagnosticSubscriberDetachesAfterSaturation(t *testing.T) {
	t.Parallel()

	loggerHandler := &blockingNodeTestLogHandler{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	defer loggerHandler.unblock()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	n := &Node{
		config:   Config{logger: slog.New(loggerHandler)},
		eventBus: bus,
	}
	n.subscribeChainSelectorEvents()

	const forkEvent = chain.ChainForkEventType
	fork := event.NewEvent(forkEvent, chain.ChainForkEvent{})
	bus.Publish(forkEvent, fork)
	select {
	case <-loggerHandler.entered:
	case <-time.After(time.Second):
		t.Fatal("production fork diagnostic callback did not enter the blocking logger")
	}

	queueFilled := make(chan struct{})
	published := make(chan struct{})
	go func() {
		for i := range event.DefaultSubscriberBuffer + 2 {
			bus.Publish(forkEvent, fork)
			if i == event.DefaultSubscriberBuffer-1 {
				close(queueFilled)
			}
		}
		close(published)
	}()
	select {
	case <-queueFilled:
	case <-time.After(time.Second):
		t.Fatal("production fork diagnostic callback queue did not fill")
	}
	select {
	case <-published:
	case <-time.After(event.RemoteDeliverTimeout + 2*time.Second):
		t.Fatal("detachable fork diagnostic subscriber held publishers past its timeout")
	}
	loggerHandler.unblock()
	// Detachment preserves events accepted before the timeout, so wait for that
	// backlog to drain before checking that later publications have no observer.
	acceptedCallbacks := int32(event.DefaultSubscriberBuffer + 1)
	require.Eventually(t, func() bool {
		return loggerHandler.calls.Load() == acceptedCallbacks
	}, time.Second, 5*time.Millisecond,
		"accepted diagnostic events should drain after the callback returns")
	bus.Publish(forkEvent, fork)
	require.Never(t, func() bool {
		return loggerHandler.calls.Load() > acceptedCallbacks
	}, 100*time.Millisecond, 5*time.Millisecond,
		"detached diagnostic observer must not receive a later event")
}

// TestNodePeerEligibilityEventUpdatesChainSelector verifies the node wiring:
// a PeerEligibilityChangedEvent published on the event bus must be forwarded
// to the ChainSelector so that the now-ineligible peer is no longer selected.
func TestNodePeerEligibilityEventUpdatesChainSelector(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })

	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		EvaluationInterval: time.Hour, // driven by trigger, not ticker
	})
	require.NoError(t, cs.Start(t.Context()))

	connId := newNodeTestConnId(5001)
	cs.UpdatePeerTip(connId, ochainsync.Tip{
		Point:       ocommon.NewPoint(100, []byte("tip")),
		BlockNumber: 50,
	}, nil)
	require.NotNil(
		t,
		cs.GetBestPeer(),
		"peer should be selected before ineligibility",
	)

	// Exercise the real node wiring rather than a copy of it.
	newChainSelectorSubscriptionTestNode(t, bus, cs).
		subscribeChainSelectorEvents()

	bus.Publish(
		peergov.PeerEligibilityChangedEventType,
		event.NewEvent(
			peergov.PeerEligibilityChangedEventType,
			peergov.PeerEligibilityChangedEvent{
				ConnectionId: connId,
				Eligible:     false,
			},
		),
	)

	require.Eventually(t, func() bool {
		return cs.GetBestPeer() == nil
	}, time.Second, 5*time.Millisecond,
		"ineligible peer must not be selected after eligibility event")
}

// TestNodePeerPriorityEventUpdatesChainSelector verifies the node wiring:
// a PeerPriorityChangedEvent published on the event bus must be forwarded
// to the ChainSelector so that the higher-priority peer wins equal-tip
// selection.
func TestNodePeerPriorityEventUpdatesChainSelector(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })

	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{})
	lowPrioConn := newNodeTestConnId(5002)
	highPrioConn := newNodeTestConnId(5003)

	equalTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, []byte("equal")),
		BlockNumber: 50,
	}
	cs.UpdatePeerTip(lowPrioConn, equalTip, nil)
	cs.UpdatePeerTip(highPrioConn, equalTip, nil)

	// Exercise the real node wiring rather than a copy of it.
	newChainSelectorSubscriptionTestNode(t, bus, cs).
		subscribeChainSelectorEvents()

	bus.Publish(
		peergov.PeerPriorityChangedEventType,
		event.NewEvent(
			peergov.PeerPriorityChangedEventType,
			peergov.PeerPriorityChangedEvent{
				ConnectionId: highPrioConn,
				Priority:     50,
			},
		),
	)

	// SelectBestChain does a pure comparison with no incumbent bias, so once
	// the priority event has been processed the higher-priority peer wins.
	require.Eventually(
		t,
		func() bool {
			best := cs.SelectBestChain()
			return best != nil && *best == highPrioConn
		},
		time.Second,
		5*time.Millisecond,
		"higher-priority peer must win equal-tip selection after priority event",
	)
}

// A close/stop failure surfaced during the startup-cleanup unwind must
// actually reach the log, not just be swallowed by the caller's `_ =`.
func TestLogErrIfNotNilLogsOnError(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	logErrIfNotNil(
		logger,
		"failed to stop leader election during cleanup",
		errors.New("epoch transition in flight"),
	)

	out := buf.String()
	if !strings.Contains(out, "failed to stop leader election during cleanup") {
		t.Fatalf("expected log message in output, got: %s", out)
	}
	if !strings.Contains(out, "epoch transition in flight") {
		t.Fatalf("expected error detail in output, got: %s", out)
	}
}

// The common case -- a clean stop -- must stay silent, or every successful
// shutdown would log a spurious error line.
func TestLogErrIfNotNilStaysQuietOnNil(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	logErrIfNotNil(logger, "failed to stop leader election during cleanup", nil)

	if buf.Len() != 0 {
		t.Fatalf(
			"expected no log output for a nil error, got: %s",
			buf.String(),
		)
	}
}

// seedIncompleteRewardLiveStake reproduces a post-upgrade database whose
// reward_live_stake aggregate covers only one of two registered credentials,
// which is the state RewardLiveStakeNeedsBackfill is meant to detect.
func seedIncompleteRewardLiveStake(
	t *testing.T,
	db *database.Database,
) {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	stakeKey := make([]byte, 28)
	stakeKey[0] = 0x51
	missingStakeKey := make([]byte, 28)
	missingStakeKey[0] = 0x52
	_, err = raw.Exec(`
INSERT INTO account (staking_key, pool, added_slot, active)
VALUES (?, ?, 50, TRUE), (?, ?, 60, TRUE)`,
		stakeKey, make([]byte, 28),
		missingStakeKey, make([]byte, 28),
	)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO reward_live_stake
    (staking_key, credential_tag, utxo_stake, reward_stake, total_stake,
     registered, updated_slot)
VALUES (?, 0, '0', '0', '0', TRUE, 75)`,
		stakeKey,
	)
	require.NoError(t, err)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(100, make([]byte, 32)),
	}, nil))
}

// TestBackfillRewardLiveStakeSkipsScanWhenConfigured pins the opt-out: with
// the flag set the whole-UTxO consistency scan must not run, so a database
// that genuinely needs a backfill is left untouched rather than rebuilt.
// Without the flag the sibling test above rebuilds the same fixture, so this
// fails if the flag ever stops being honored.
func TestBackfillRewardLiveStakeSkipsScanWhenConfigured(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	seedIncompleteRewardLiveStake(t, db)

	needed, err := db.Metadata().RewardLiveStakeNeedsBackfill(nil)
	require.NoError(t, err)
	require.True(t, needed)

	n := &Node{
		db: db,
		config: Config{
			logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
			skipRewardLiveStakeBackfillCheck: true,
		},
	}
	require.NoError(t, n.backfillRewardLiveStake())

	// Still needed: the scan was skipped, so no rebuild happened.
	needed, err = db.Metadata().RewardLiveStakeNeedsBackfill(nil)
	require.NoError(t, err)
	require.True(t, needed)
}

// TestBackfillRewardLiveStakeChecksProvenanceWhenSkipping pins the boundary
// of the opt-out: the flag suppresses only the reward_live_stake scan, never
// the stake-snapshot provenance probe, which fails closed because such a
// database cannot be safely reconstructed.
func TestBackfillRewardLiveStakeChecksProvenanceWhenSkipping(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO pool_stake_snapshot
    (epoch, snapshot_type, pool_key_hash, total_stake, stake_denominator,
     delegator_count, captured_slot, calculation_version)
VALUES (?, 'mark', ?, '0', '0', 0, 100, ?)`,
		650, make([]byte, 28), models.RewardStakeCalculationVersion-1,
	)
	require.NoError(t, err)

	n := &Node{
		db: db,
		config: Config{
			logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
			skipRewardLiveStakeBackfillCheck: true,
		},
	}
	err = n.backfillRewardLiveStake()
	require.Error(t, err)
	require.Contains(t, err.Error(), "older accounting")
}
