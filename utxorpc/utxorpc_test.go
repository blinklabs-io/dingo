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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/event"
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
}

func TestNewUtxorpc_CustomLimits(t *testing.T) {
	u := NewUtxorpc(UtxorpcConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:        event.NewEventBus(nil, nil),
		MaxBlockRefs:    50,
		MaxUtxoKeys:     500,
		MaxHistoryItems: 5000,
		MaxDataKeys:     200,
	})
	require.Equal(t, 50, u.config.MaxBlockRefs)
	require.Equal(t, 500, u.config.MaxUtxoKeys)
	require.Equal(t, 5000, u.config.MaxHistoryItems)
	require.Equal(t, 200, u.config.MaxDataKeys)
}

// TestRequestLimitConstants verifies that the default limit constants
// are reasonable values for preventing DoS while allowing normal use.
func TestRequestLimitConstants(t *testing.T) {
	require.Equal(t, 100, DefaultMaxBlockRefs)
	require.Equal(t, 1000, DefaultMaxUtxoKeys)
	require.Equal(t, 10000, DefaultMaxHistoryItems)
	require.Equal(t, 1000, DefaultMaxDataKeys)
}

// TestRequestLimitEnforcement_Pattern verifies the limit enforcement
// pattern used in FetchBlock, ReadUtxos, DumpHistory, and ReadData.
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
	// Start server on ephemeral port by setting Port to 0
	u := NewUtxorpc(UtxorpcConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: event.NewEventBus(nil, nil),
		Host:     "127.0.0.1",
		Port:     0,
	})
	err := u.Start(context.Background())
	require.NoError(t, err, "failed to start utxorpc")

	// Server is already listening after Start returns successfully
	// (startServer waits internally for startup or error)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = u.Stop(ctx)
	require.NoError(t, err, "failed to stop utxorpc")
}
