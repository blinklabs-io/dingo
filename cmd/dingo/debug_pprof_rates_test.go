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
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/config"
)

// profilingSetterCalls records the rates passed to the runtime profile
// setters; nil means the setter was not called.
type profilingSetterCalls struct {
	mutex *int
	block *int
}

// recordProfilingSetters replaces the runtime profile setters for the test.
// Callers must not use t.Parallel: the setters are package globals.
func recordProfilingSetters(t *testing.T) *profilingSetterCalls {
	t.Helper()
	calls := &profilingSetterCalls{}
	origMutex, origBlock := setMutexProfileFraction, setBlockProfileRate
	t.Cleanup(func() {
		setMutexProfileFraction, setBlockProfileRate = origMutex, origBlock
	})
	setMutexProfileFraction = func(rate int) int {
		calls.mutex = &rate
		return 0
	}
	setBlockProfileRate = func(rate int) {
		calls.block = &rate
	}
	return calls
}

func intPtr(v int) *int { return &v }

// Not t.Parallel: recordProfilingSetters swaps package-level seams.
func TestApplyDebugProfilingRates(t *testing.T) {
	tests := []struct {
		name       string
		cfg        config.Config
		wantMutex  *int
		wantBlock  *int
		wantLog    bool
		wantStderr bool
	}{
		{
			name: "default leaves profiling off",
			cfg:  config.Config{DebugPort: 6060},
		},
		{
			name: "rates without listener are ignored",
			cfg: config.Config{
				DebugMutexProfileFraction: 5,
				DebugBlockProfileRate:     1000,
			},
			wantStderr: true,
		},
		{
			name: "mutex only",
			cfg: config.Config{
				DebugPort:                 6060,
				DebugMutexProfileFraction: 5,
			},
			wantMutex: intPtr(5),
			wantLog:   true,
		},
		{
			name: "block only",
			cfg: config.Config{
				DebugPort:             6060,
				DebugBlockProfileRate: 1000,
			},
			wantBlock: intPtr(1000),
			wantLog:   true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			calls := recordProfilingSetters(t)
			var logBuf, stderr bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logBuf, nil))
			applyDebugProfilingRates(logger, &stderr, &tc.cfg)
			require.Equal(t, tc.wantMutex, calls.mutex)
			require.Equal(t, tc.wantBlock, calls.block)
			require.Equal(t, tc.wantLog, logBuf.Len() > 0, logBuf.String())
			require.Equal(t, tc.wantStderr, stderr.Len() > 0, stderr.String())
		})
	}
}

// The ignored-rates warning must survive a logger configured above warn,
// as the config-validation warnings in commonRun do.
// Not t.Parallel: recordProfilingSetters swaps package-level seams.
func TestApplyDebugProfilingRatesWarningIgnoresLogLevel(t *testing.T) {
	recordProfilingSetters(t)
	var logBuf, stderr bytes.Buffer
	logger := slog.New(slog.NewTextHandler(
		&logBuf, &slog.HandlerOptions{Level: slog.LevelError},
	))
	applyDebugProfilingRates(logger, &stderr, &config.Config{
		DebugMutexProfileFraction: 5,
	})
	require.Contains(t, stderr.String(), "profile rates ignored")
	require.Empty(t, logBuf.String())
}

// commonRun also serves load, database and mithril snapshot, which never
// start the pprof listener, so it must not apply the rates. Not t.Parallel:
// it swaps package-level seams and replaces the slog default.
func TestCommonRunLeavesProfilingRatesUnset(t *testing.T) {
	calls := recordProfilingSetters(t)
	origDefault := slog.Default()
	t.Cleanup(func() { slog.SetDefault(origDefault) })
	_, err := commonRun(context.Background(), &config.Config{
		DebugPort:                 6060,
		DebugMutexProfileFraction: 5,
		DebugBlockProfileRate:     1000,
	})
	require.NoError(t, err)
	require.Nil(t, calls.mutex)
	require.Nil(t, calls.block)
}

// mithril sync starts the pprof listener, so it applies the rates before the
// sync runs. Not t.Parallel: it swaps package-level seams and replaces the
// slog default.
func TestMithrilSyncAppliesProfilingRates(t *testing.T) {
	calls := recordProfilingSetters(t)
	origDefault := slog.Default()
	t.Cleanup(func() { slog.SetDefault(origDefault) })
	origRun := runMithrilSyncForCommand
	t.Cleanup(func() { runMithrilSyncForCommand = origRun })
	var atRun profilingSetterCalls
	runMithrilSyncForCommand = func(
		context.Context, *config.Config, *slog.Logger, string,
		*boundHealthProbe,
	) error {
		atRun = *calls
		return nil
	}
	cmd := &cobra.Command{}
	cmd.SetContext(config.WithContext(context.Background(), &config.Config{
		DebugPort:                 6060,
		DebugMutexProfileFraction: 5,
		DebugBlockProfileRate:     1000,
	}))
	require.NoError(t, mithrilSyncRunE(cmd, nil))
	require.Equal(t, intPtr(5), atRun.mutex)
	require.Equal(t, intPtr(1000), atRun.block)
}
