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
	"log/slog"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/config"
)

// Not t.Parallel: the runtime mutex and block profile rates are process-global.
func TestApplyDebugProfilingRates(t *testing.T) {
	// SetMutexProfileFraction(-1) reads the current value without changing it.
	// The block rate has no getter, so the test observes it through recorded
	// block events and restores it to 0, the process default.
	origMutex := runtime.SetMutexProfileFraction(-1)
	t.Cleanup(func() {
		runtime.SetMutexProfileFraction(origMutex)
		runtime.SetBlockProfileRate(0)
	})

	tests := []struct {
		name      string
		cfg       config.Config
		wantMutex int
		wantBlock bool
		wantLog   bool
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
			wantLog: true,
		},
		{
			name: "mutex only",
			cfg: config.Config{
				DebugPort:                 6060,
				DebugMutexProfileFraction: 5,
			},
			wantMutex: 5,
			wantLog:   true,
		},
		{
			name: "block only",
			cfg: config.Config{
				DebugPort:             6060,
				DebugBlockProfileRate: 1000,
			},
			wantBlock: true,
			wantLog:   true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runtime.SetMutexProfileFraction(0)
			runtime.SetBlockProfileRate(0)
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			applyDebugProfilingRates(logger, &tc.cfg)
			require.Equal(
				t, tc.wantMutex, runtime.SetMutexProfileFraction(-1),
			)
			require.Equal(t, tc.wantLog, buf.Len() > 0, buf.String())
			// A blocked receive far longer than the 1000 ns rate is always
			// recorded once the rate is set, and never at rate 0.
			before := blockProfileEvents()
			<-time.After(5 * time.Millisecond)
			require.Equal(t, tc.wantBlock, blockProfileEvents() > before)
		})
	}
}

// blockProfileEvents sums event counts across every block profile record, so
// it grows even when the blocking stack was already recorded.
func blockProfileEvents() int64 {
	n, _ := runtime.BlockProfile(nil)
	records := make([]runtime.BlockProfileRecord, n+16)
	n, ok := runtime.BlockProfile(records)
	for !ok {
		records = make([]runtime.BlockProfileRecord, n+16)
		n, ok = runtime.BlockProfile(records)
	}
	var total int64
	for _, r := range records[:n] {
		total += r.Count
	}
	return total
}
