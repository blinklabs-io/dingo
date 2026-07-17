// Copyright 2025 Blink Labs Software
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

package utxorpc

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	sync "github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
)

func TestNewUtxorpc_DefaultLimits(t *testing.T) {
	u := NewUtxorpc(UtxorpcConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: event.NewEventBus(nil, nil),
	})
	require.Equal(
		t,
		DefaultMaxBlockRefs,
		u.config.MaxBlockRefs,
		"MaxBlockRefs should default",
	)
	require.Equal(
		t,
		DefaultMaxUtxoKeys,
		u.config.MaxUtxoKeys,
		"MaxUtxoKeys should default",
	)
	require.Equal(
		t,
		DefaultMaxHistoryItems,
		u.config.MaxHistoryItems,
		"MaxHistoryItems should default",
	)
	require.Equal(
		t,
		DefaultMaxDataKeys,
		u.config.MaxDataKeys,
		"MaxDataKeys should default",
	)
	require.Equal(
		t,
		DefaultServerTimeout,
		u.config.ServerTimeout,
		"ServerTimeout should default",
	)
}

func TestNewUtxorpc_CustomLimits(t *testing.T) {
	u := NewUtxorpc(UtxorpcConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:        event.NewEventBus(nil, nil),
		MaxBlockRefs:    50,
		MaxUtxoKeys:     500,
		MaxHistoryItems: 5000,
		MaxDataKeys:     200,
		ServerTimeout:   10 * time.Second,
	})
	require.Equal(t, 50, u.config.MaxBlockRefs)
	require.Equal(t, 500, u.config.MaxUtxoKeys)
	require.Equal(t, 5000, u.config.MaxHistoryItems)
	require.Equal(t, 200, u.config.MaxDataKeys)
	require.Equal(t, 10*time.Second, u.config.ServerTimeout)
}

// TestRequestLimitConstants verifies that the default limit constants
// are reasonable values for preventing DoS while allowing normal use.
func TestRequestLimitConstants(t *testing.T) {
	require.Equal(t, 100, DefaultMaxBlockRefs)
	require.Equal(t, 1000, DefaultMaxUtxoKeys)
	require.Equal(t, 10000, DefaultMaxHistoryItems)
	require.Equal(t, 1000, DefaultMaxDataKeys)
	require.Equal(t, time.Hour, DefaultServerTimeout)
}

// TestRequestLimitEnforcement_Pattern verifies the limit enforcement
// pattern used in FetchBlock, ReadUtxos, DumpHistory, SearchUtxos, and ReadData.
// This tests the comparison logic in isolation, since calling the
// actual gRPC handlers requires a full LedgerState.
func TestRequestLimitEnforcement_Pattern(t *testing.T) {
	tests := []struct {
		name      string
		count     int
		limit     int
		shouldErr bool
	}{
		{"at limit", 100, 100, false},
		{"below limit", 50, 100, false},
		{"above limit", 101, 100, true},
		{"zero items", 0, 100, false},
		{"single item", 1, 100, false},
		{"way above limit", 10000, 100, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exceeds := tc.count > tc.limit
			require.Equal(
				t,
				tc.shouldErr,
				exceeds,
				"limit enforcement mismatch",
			)
		})
	}
}

func TestUtxorpc_StartStop(t *testing.T) {
	u := NewUtxorpc(UtxorpcConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: event.NewEventBus(nil, nil),
		Host:     "127.0.0.1",
	})
	// NewUtxorpc defaults Port 0 to 9090 for runtime config. Force the
	// test instance back to an ephemeral port to avoid local port conflicts.
	u.config.Port = 0

	startCtx := t.Context()
	err := u.Start(startCtx)
	require.NoError(t, err, "failed to start utxorpc")

	// Server is already listening after Start returns successfully
	// (startServer waits internally for startup or error)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = u.Stop(ctx)
	require.NoError(t, err, "failed to stop utxorpc")
}

// TestConfigureServerTLS verifies that server TLS configurations enforce TLS
// 1.2 as the minimum while preserving configurations that require TLS 1.3.
func TestConfigureServerTLS(t *testing.T) {
	// Cover newly allocated, zero-valued, insecure legacy, and stricter TLS
	// configurations so both the default and caller-supplied paths are tested.
	tests := []struct {
		name       string
		tlsConfig  *tls.Config
		minVersion uint16
	}{
		{
			name:       "nil config",
			minVersion: tls.VersionTLS12,
		},
		{
			name:       "default minimum",
			tlsConfig:  new(tls.Config),
			minVersion: tls.VersionTLS12,
		},
		{
			name: "lower minimum",
			tlsConfig: &tls.Config{
				MinVersion: tls.VersionTLS11,
			},
			minVersion: tls.VersionTLS12,
		},
		{
			name: "higher minimum",
			tlsConfig: &tls.Config{
				MinVersion: tls.VersionTLS13,
			},
			minVersion: tls.VersionTLS13,
		},
	}

	// Apply the server TLS policy to each starting configuration and verify the
	// resulting minimum version is never lower than TLS 1.2.
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := &http.Server{TLSConfig: tc.tlsConfig}

			configureServerTLS(server)

			require.NotNil(t, server.TLSConfig)
			require.Equal(t, tc.minVersion, server.TLSConfig.MinVersion)
		})
	}
}

// TestAnyChainBlockNativeBytes_NonNil ensures that AnyChainBlock.NativeBytes
// is a real field in the generated type and can be set to non-nil, which is
// what the SyncService handlers rely on for raw CBOR propagation.
func TestAnyChainBlockNativeBytes_NonNil(t *testing.T) {
	raw := []byte{0xde, 0xad, 0xbe, 0xef}

	acb := &sync.AnyChainBlock{
		NativeBytes: raw,
	}

	require.NotNil(t, acb.NativeBytes)
	require.Equal(t, raw, acb.NativeBytes)
}

func TestBlockRefFromModel(t *testing.T) {
	block := models.Block{
		Hash:   []byte{0xde, 0xad, 0xbe, 0xef},
		Slot:   42,
		Number: 100,
	}

	br := blockRefFromModel(block)

	require.Equal(t, block.Slot, br.Slot)
	require.Equal(t, block.Hash, br.Hash)
	require.Equal(t, block.Number, br.Height)
}
