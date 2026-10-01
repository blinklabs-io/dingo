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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/soak"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeCSV(t *testing.T, goroutines func(i int) float64) string {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, soak.WriteCSVHeader(&buf))
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 100; i++ {
		require.NoError(t, soak.WriteCSVRow(&buf, soak.Sample{
			Time:       base.Add(time.Duration(i) * time.Minute),
			Goroutines: goroutines(i),
			RSSBytes:   1e9,
			GCCount:    float64(i),
		}))
	}
	p := filepath.Join(t.TempDir(), "soak.csv")
	require.NoError(t, os.WriteFile(p, buf.Bytes(), 0o600))
	return p
}

func TestAnalyseExitCodes(t *testing.T) {
	t.Parallel()
	flat := writeCSV(t, func(int) float64 { return 500 })
	leak := writeCSV(t, func(i int) float64 { return 500 + float64(i)*5 })

	var out, errb bytes.Buffer
	assert.Equal(t, 0, run(context.Background(), []string{"analyse", "-csv", flat}, &out, &errb))
	assert.Contains(t, out.String(), "goroutines")

	out.Reset()
	errb.Reset()
	assert.Equal(t, 1, run(context.Background(), []string{"analyse", "-csv", leak}, &out, &errb))
	assert.Contains(t, out.String(), "GROWING")

	assert.Equal(t, 2, run(context.Background(), []string{"analyse"}, &out, &errb))
	assert.Equal(t, 2, run(context.Background(), nil, &out, &errb))
}

func TestLogsSubcommand(t *testing.T) {
	t.Parallel()
	p := filepath.Join(t.TempDir(), "node.log")
	require.NoError(t, os.WriteFile(p, []byte(strings.Repeat(
		`{"level":"WARN","msg":"slow peer 10.0.0.1"}`+"\n", 3)), 0o600))
	var out, errb bytes.Buffer
	assert.Equal(t, 0, run(context.Background(), []string{"logs", "-min-count", "2", p}, &out, &errb))
	assert.Contains(t, out.String(), "slow peer <addr>")
}
