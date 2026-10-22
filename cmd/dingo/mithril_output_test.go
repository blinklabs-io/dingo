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
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// captureStdout runs fn with os.Stdout redirected and returns what it wrote.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	orig := os.Stdout
	os.Stdout = w
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	func() {
		defer func() { os.Stdout = orig }()
		fn()
	}()
	require.NoError(t, w.Close())
	return <-done
}

func requireNoControlBytes(t *testing.T, out string) {
	t.Helper()
	for _, r := range strings.TrimSuffix(out, "\n") {
		if r == '\n' {
			continue
		}
		require.False(
			t, r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0),
			"raw control character %U in output %q", r, out,
		)
	}
}

func runMithrilCommand(
	t *testing.T, cmd *cobra.Command, backend, url string, args ...string,
) string {
	t.Helper()
	cfg := &config.Config{Network: "preview"}
	cfg.Mithril.AggregatorURL = url
	cfg.Mithril.AllowInsecureHTTP = true
	cfg.Mithril.Backend = backend
	cmd.SetArgs(args)
	cmd.SetContext(config.WithContext(context.Background(), cfg))
	var runErr error
	out := captureStdout(t, func() { runErr = cmd.Execute() })
	require.NoError(t, runErr)
	return out
}

func hostileAggregator(t *testing.T) string {
	t.Helper()
	snap := `{"digest":"` + `d\u001b[2J` + `","hash":"h\u001b[2J",` +
		`"merkle_root":"m\u001b[2J","network":"n\u001b]0;pwned\u0007",` +
		`"beacon":{"epoch":1,"immutable_file_number":2},` +
		`"certificate_hash":"c\u009b","size":1,` +
		`"total_db_size_uncompressed":1,` +
		`"created_at":"t\u001b[2J","compression_algorithm":"z\u001b[2J",` +
		`"cardano_node_version":"v\u001b[2J",` +
		`"locations":["l\u001b[2J"],` +
		`"digests":{"locations":[{"type":"ty\u001b[2J","uri":"u\u001b[2J"}]},` +
		`"immutables":{"locations":[]},"ancillary":{"locations":[]}}`
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "cardano-database") {
				_, _ = w.Write([]byte("[" + snap + "]"))
				return
			}
			_, _ = w.Write([]byte(snap))
		},
	))
	t.Cleanup(server.Close)
	return server.URL
}

// Not t.Parallel: captureStdout swaps the process-global os.Stdout.
func TestMithrilListShowEscapeAggregatorText(t *testing.T) {
	url := hostileAggregator(t)
	for _, tc := range []struct {
		name string
		cmd  func() *cobra.Command
		args []string
	}{
		{"list", mithrilListCommand, nil},
		{"show", mithrilShowCommand, []string{"abc"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := runMithrilCommand(
				t, tc.cmd(), "v2", url, tc.args...,
			)
			require.Contains(t, out, `\x1b[2J`)
			requireNoControlBytes(t, out)
		})
	}
}
