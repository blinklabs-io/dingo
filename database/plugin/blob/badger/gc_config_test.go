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

package badger

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	badgerdb "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestProviderGCPolicyDefaults(t *testing.T) {
	t.Parallel()
	store := resolveBadgerProvider(t, nil, blob.ProviderDependencies{})
	require.Equal(t, DefaultGCInterval, store.gcInterval)
	require.Equal(t, DefaultGCDiscardRatio, store.gcDiscardRatio)
}

func TestProviderGCPolicyFromConfig(t *testing.T) {
	t.Parallel()
	store := resolveBadgerProvider(t, map[string]any{
		"gcInterval":     "7m",
		"gcDiscardRatio": 0.25,
	}, blob.ProviderDependencies{})
	require.Equal(t, 7*time.Minute, store.gcInterval)
	require.InDelta(t, 0.25, store.gcDiscardRatio, 0)
}

func TestProviderRejectsInvalidGCPolicy(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		config map[string]any
	}{
		{"negative interval", map[string]any{"gcInterval": "-1m"}},
		{"negative ratio", map[string]any{"gcDiscardRatio": -0.1}},
		{"ratio one", map[string]any{"gcDiscardRatio": 1.0}},
		{"ratio above one", map[string]any{"gcDiscardRatio": 1.5}},
		{"ratio zero", map[string]any{"gcDiscardRatio": 0.0}},
		{"ratio NaN", map[string]any{"gcDiscardRatio": math.NaN()}},
		{"ratio positive infinity", map[string]any{"gcDiscardRatio": math.Inf(1)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			host := plugin.NewHost()
			require.NoError(t, RegisterProvider(host))
			t.Cleanup(func() {
				require.NoError(t, host.Stop(context.Background()))
			})
			_, err := plugin.Resolve[*BlobStoreBadger](
				context.Background(), host,
				plugin.CapabilityStorageBlob, "badger", tt.config,
				blob.ProviderDependencies{DataDir: t.TempDir()},
			)
			require.Error(t, err)
		})
	}
}

// TestGCUsesConfiguredPolicy starts a store with a short interval and a
// non-default ratio and waits for the real ticker to drive the GC worker.
func TestGCUsesConfiguredPolicy(t *testing.T) {
	t.Parallel()
	store, err := New(
		WithDataDir(t.TempDir()),
		WithGc(true),
		WithGcInterval(10*time.Millisecond),
		WithGcDiscardRatio(0.25),
		WithDeferOpen(),
	)
	require.NoError(t, err)
	ratios := make(chan float64, 16)
	store.runValueLogGC = func(ratio float64) error {
		select {
		case ratios <- ratio:
		default:
		}
		return badgerdb.ErrNoRewrite
	}
	require.NoError(t, store.Start())
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	got := testutil.RequireReceive(
		t, ratios, 30*time.Second, "GC did not run at the configured interval",
	)
	require.InDelta(t, 0.25, got, 0)
}
