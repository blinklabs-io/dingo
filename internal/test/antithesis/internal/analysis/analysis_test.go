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

package analysis

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAnalyzerRunSignalsSetupCompleteWithoutForgedBlocks(t *testing.T) {
	called := make(chan struct{}, 1)
	analyzer := NewAnalyzer(&Config{
		InitialWait:   0,
		CheckInterval: time.Hour,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	analyzer.setupComplete = func() {
		called <- struct{}{}
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- analyzer.Run(ctx)
	}()

	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("setup complete was not signaled")
	}

	cancel()
	select {
	case err := <-runDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("analyzer did not stop after context cancellation")
	}
}

func TestChainTipRange_Empty(t *testing.T) {
	min, max, ok := chainTipRange(map[string]uint64{})
	require.False(t, ok)
	require.Zero(t, min)
	require.Zero(t, max)
}

func TestChainTipRange_NonEmpty(t *testing.T) {
	min, max, ok := chainTipRange(map[string]uint64{
		"p1": 42,
		"p2": 7,
		"p3": 99,
	})
	require.True(t, ok)
	require.Equal(t, uint64(7), min)
	require.Equal(t, uint64(99), max)
}
