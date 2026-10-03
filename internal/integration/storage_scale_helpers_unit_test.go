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

package integration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseScales(t *testing.T) {
	t.Parallel()

	got, err := parseScales("1000, 50k,2m,1b")
	require.NoError(t, err)
	require.Equal(t, []int{1000, 50_000, 2_000_000, 1_000_000_000}, got)

	for _, bad := range []string{
		"", "0", "-5", "abc", "1x", "5k,", "k", "9223372037b",
	} {
		_, err := parseScales(bad)
		require.Errorf(t, err, "input %q", bad)
	}
}

func TestEnvScalesDefaultsOnlyWhenUnset(t *testing.T) {
	t.Parallel()

	def := []int{7}
	env := func(m map[string]string) func(string) string {
		return func(k string) string { return m[k] }
	}
	got, err := envScales(env(nil), envBenchScale, def)
	require.NoError(t, err)
	require.Equal(t, def, got)

	got, err = envScales(env(map[string]string{envBenchScale: "3k"}), envBenchScale, def)
	require.NoError(t, err)
	require.Equal(t, []int{3000}, got)

	// A set-but-invalid value is an error, never a silent fall back to the
	// small default that would make a mainnet-scale run look successful.
	_, err = envScales(env(map[string]string{envBenchScale: "oops"}), envBenchScale, def)
	require.Error(t, err)
}

func TestEnvInt(t *testing.T) {
	t.Parallel()

	env := func(v string) func(string) string {
		return func(string) string { return v }
	}
	n, err := envInt(env(""), envBenchBlockBytes, 9)
	require.NoError(t, err)
	require.Equal(t, 9, n)
	n, err = envInt(env("4096"), envBenchBlockBytes, 9)
	require.NoError(t, err)
	require.Equal(t, 4096, n)
	for _, bad := range []string{"0", "-1", "x"} {
		_, err = envInt(env(bad), envBenchBlockBytes, 9)
		require.Errorf(t, err, "input %q", bad)
	}
}

func TestEnvIntAtLeast(t *testing.T) {
	t.Parallel()

	env := func(v string) func(string) string {
		return func(string) string { return v }
	}
	n, err := envIntAtLeast(env(""), envBenchBlockBytes, 8, 8)
	require.NoError(t, err)
	require.Equal(t, 8, n)
	n, err = envIntAtLeast(env("12"), envBenchBlockBytes, 8, 8)
	require.NoError(t, err)
	require.Equal(t, 12, n)
	_, err = envIntAtLeast(env("7"), envBenchBlockBytes, 8, 8)
	require.Error(t, err)
}

func TestPercentileNearestRank(t *testing.T) {
	t.Parallel()

	var s []time.Duration
	for i := 100; i >= 1; i-- { // unsorted on purpose
		s = append(s, time.Duration(i)*time.Millisecond)
	}
	require.Equal(t, 50*time.Millisecond, percentile(s, 50))
	require.Equal(t, 95*time.Millisecond, percentile(s, 95))
	require.Equal(t, 99*time.Millisecond, percentile(s, 99))
	require.Equal(t, 100*time.Millisecond, percentile(s, 100))
	// A rank that is not whole rounds up: the 95th of ten samples is the tenth.
	ten := []time.Duration{3, 1, 2, 5, 4, 7, 6, 9, 8, 10}
	require.Equal(t, time.Duration(10), percentile(ten, 95))
	require.Equal(t, time.Duration(5), percentile(ten, 50))
	require.Equal(t, time.Duration(0), percentile(nil, 50))
	require.Equal(t, 7*time.Millisecond, percentile([]time.Duration{7 * time.Millisecond}, 1))
}

func TestLatencyRecorderDropsSamplesPastLimit(t *testing.T) {
	t.Parallel()

	r := newLatencyRecorder(3)
	for i := 1; i <= 10; i++ {
		r.record(time.Duration(i))
	}
	require.Equal(t, []time.Duration{1, 2, 3}, r.samples)
}

func TestParseVmRSS(t *testing.T) {
	t.Parallel()

	n, ok := parseVmRSS("Name:\tx\nVmPeak:\t 9 kB\nVmRSS:\t  2048 kB\nThreads:\t3\n")
	require.True(t, ok)
	require.Equal(t, uint64(2048*1024), n)

	_, ok = parseVmRSS("Name:\tx\n")
	require.False(t, ok)
	_, ok = parseVmRSS("VmRSS:\tnope kB\n")
	require.False(t, ok)
	_, ok = parseVmRSS("VmRSS:\t5 MB\n")
	require.False(t, ok)
}
