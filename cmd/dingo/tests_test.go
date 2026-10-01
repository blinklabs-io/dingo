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
	"encoding/json"
	"log/slog"
	"maps"
	"os"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/mithril"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestRootCmd wires the real subcommands under a root command,
// mirroring main() so the command-classification helpers see the same
// tree. The built-in help and completion commands, which cobra normally
// adds during Execute, are initialized explicitly so the tree includes
// them too.
func newTestRootCmd() *cobra.Command {
	root := &cobra.Command{Use: "dingo"}
	root.AddCommand(
		serveCommand(),
		loadCommand(),
		listCommand(),
		versionCommand(),
		mithrilCommand(),
		syncCommand(),
		databaseCommand(),
	)
	root.InitDefaultHelpCmd()
	root.InitDefaultCompletionCmd()
	return root
}

func findCmd(t *testing.T, root *cobra.Command, path ...string) *cobra.Command {
	t.Helper()
	if len(path) == 0 {
		return root
	}
	cmd, _, err := root.Find(path)
	require.NoError(t, err)
	require.Equal(t, path[len(path)-1], cmd.Name())
	return cmd
}

func TestEffectiveRunMode(t *testing.T) {
	t.Parallel()

	root := newTestRootCmd()
	tests := []struct {
		name    string
		path    []string
		runMode config.RunMode
		want    config.RunMode
	}{
		{"bare serve", nil, config.RunModeServe, config.RunModeServe},
		{"bare load", nil, config.RunModeLoad, config.RunModeLoad},
		{"bare dev", nil, config.RunModeDev, config.RunModeDev},
		{"bare empty defaults to serve", nil, "", config.RunModeServe},
		// An invalid configured runMode falls through to serve (matching
		// rootCmd's dispatch default) so serving-listener checks still
		// apply; the invalid mode is reported separately by Validate.
		{"bare invalid falls back to serve", nil, "batch", config.RunModeServe},
		// Subcommands run a fixed operation regardless of configured runMode.
		{
			"serve subcommand ignores load config",
			[]string{"serve"},
			config.RunModeLoad,
			config.RunModeServe,
		},
		{
			"load subcommand",
			[]string{"load"},
			config.RunModeServe,
			config.RunModeLoad,
		},
		{
			"sync uses the sync operation mode",
			[]string{"sync"},
			config.RunModeServe,
			config.RunModeSync,
		},
		// `mithril sync` starts the metrics/debug listeners, so it uses the
		// sync operation mode; the read-only mithril subcommands do not.
		{
			"mithril sync uses the sync operation mode",
			[]string{"mithril", "sync"},
			config.RunModeServe,
			config.RunModeSync,
		},
		{
			"mithril list is a read-only mithril utility",
			[]string{"mithril", "list"},
			config.RunModeServe,
			config.RunModeMithril,
		},
		{
			"mithril show is a read-only mithril utility",
			[]string{"mithril", "show"},
			config.RunModeServe,
			config.RunModeMithril,
		},
		{
			"bare mithril is a read-only mithril utility",
			[]string{"mithril"},
			config.RunModeServe,
			config.RunModeMithril,
		},
		// `dingo database snapshot|restore|truncate` are offline maintenance
		// commands: they must resolve to
		// RunModeDatabase regardless of the configured runMode, the same way
		// `load`/`sync`/`mithril` ignore it, since main.go uses this to skip
		// topology resolution (database never opens a peer connection).
		{
			"database snapshot is an offline maintenance command",
			[]string{"database", "snapshot"},
			config.RunModeServe,
			config.RunModeDatabase,
		},
		{
			"database restore is an offline maintenance command",
			[]string{"database", "restore"},
			config.RunModeServe,
			config.RunModeDatabase,
		},
		{
			"database truncate is an offline maintenance command",
			[]string{"database", "truncate"},
			config.RunModeServe,
			config.RunModeDatabase,
		},
		{
			"bare database is an offline maintenance command",
			[]string{"database"},
			config.RunModeServe,
			config.RunModeDatabase,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := findCmd(t, root, tt.path...)
			got := effectiveRunMode(cmd, &config.Config{RunMode: tt.runMode})
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIsInformationalCommand(t *testing.T) {
	t.Parallel()

	root := newTestRootCmd()
	tests := []struct {
		name string
		path []string
		want bool
	}{
		{"bare root", nil, false},
		{"version", []string{"version"}, true},
		{"list", []string{"list"}, true},
		// cobra runs the root PersistentPreRunE for its built-in help and
		// completion commands, so they must be exempt from validation.
		{"help", []string{"help"}, true},
		{"completion", []string{"completion"}, true},
		{"completion zsh", []string{"completion", "zsh"}, true},
		{"serve", []string{"serve"}, false},
		{"sync", []string{"sync"}, false},
		// Nested `mithril list` must not be mistaken for top-level `list`.
		{"mithril list", []string{"mithril", "list"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := findCmd(t, root, tt.path...)
			require.Equal(
				t,
				tt.want,
				isInformationalCommand(topLevelCommand(cmd)),
			)
		})
	}
}

// internal/config cannot import chainsync or mithril without pulling
// node subsystems into the config package, so the accepted-value sets
// it validates against (config.AcceptedChainsyncStrategies and
// config.AcceptedMithrilBackends) are duplicated from those downstream
// parsers. These parity tests live in cmd/dingo, which can import all
// three. The canonical sets are not hand-maintained: they come from
// the parser packages' exported accepted-value lists
// (chainsync.AcceptedHeaderSyncStrategyNames, mithril.AcceptedBackends),
// which the parsers themselves derive from, so a value added to a
// parser fails these tests until config's duplicated list is updated.
// The tests guard drift in both directions:
//
//   - forward: every value config accepts must be accepted by the real
//     parser, so a config never passes Validate() only to fail at
//     startup;
//   - reverse: config's accepted set must match the parser's contract
//     exactly, so a value added to the parser without updating config
//     (which would spuriously reject a valid config) is caught too.

// TestChainsyncStrategyWhitelistParity checks config.AcceptedChainsyncStrategies
// against chainsync.ParseHeaderSyncStrategy.
func TestChainsyncStrategyWhitelistParity(t *testing.T) {
	t.Parallel()

	assertWhitelistParity(
		t,
		"chainsync.strategy",
		config.AcceptedChainsyncStrategies,
		chainsync.AcceptedHeaderSyncStrategyNames(),
		func(v string) error {
			_, err := chainsync.ParseHeaderSyncStrategy(v)
			return err
		},
	)
}

// TestMithrilBackendWhitelistParity checks config.AcceptedMithrilBackends
// against resolveMithrilBackend.
func TestMithrilBackendWhitelistParity(t *testing.T) {
	t.Parallel()

	// resolveMithrilBackend accepts mithril.AcceptedBackends plus the
	// empty string, which selects the default (v2).
	canonical := append([]string{""}, mithril.AcceptedBackends()...)
	assertWhitelistParity(
		t,
		"mithril.backend",
		config.AcceptedMithrilBackends,
		canonical,
		func(v string) error {
			_, err := resolveMithrilBackend(v)
			return err
		},
	)
}

// assertWhitelistParity verifies that configList (the values
// internal/config accepts) and canonical (the downstream parser's
// contract) describe the same set, and that both are actually accepted
// by the real parser via accepts.
func assertWhitelistParity(
	t *testing.T,
	name string,
	configList, canonical []string,
	accepts func(string) error,
) {
	t.Helper()
	// Forward: everything config accepts must parse.
	for _, v := range configList {
		if err := accepts(v); err != nil {
			t.Errorf(
				"%s: config accepts %q but the parser rejects it: %v",
				name, v, err,
			)
		}
	}
	// Reverse: every value in the parser's contract must actually parse
	// (anchoring the exported accepted list to the real parser) and
	// config's set must match the contract exactly.
	for _, v := range canonical {
		if err := accepts(v); err != nil {
			t.Errorf(
				"%s: canonical value %q is rejected by the parser; "+
					"the parser package's exported accepted list "+
					"disagrees with its parser: %v",
				name, v, err,
			)
		}
	}
	if got, want := toStringSet(configList), toStringSet(canonical); !maps.Equal(
		got,
		want,
	) {
		t.Errorf(
			"%s: config accepted set %v does not match parser contract %v",
			name, configList, canonical,
		)
	}
}

func toStringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, v := range values {
		set[v] = struct{}{}
	}
	return set
}

func TestNewLogger_JSONFormatProducesValidJSON(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, levelOK, formatOK := newLogger(&buf, "json", "info", false)
	require.True(t, levelOK)
	require.True(t, formatOK)

	logger.Info("hello", "component", "test", "k", "v")

	line := strings.TrimSpace(buf.String())
	require.NotEmpty(t, line)
	require.True(
		t,
		json.Valid([]byte(line)),
		"expected valid JSON, got: %s",
		line,
	)
	var rec map[string]any
	require.NoError(t, json.Unmarshal([]byte(line), &rec))
	assert.Equal(t, "hello", rec["msg"])
	assert.Equal(t, "test", rec["component"])
}

func TestNewLogger_TextFormatIsNotJSON(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, _, formatOK := newLogger(&buf, "text", "info", false)
	require.True(t, formatOK)

	logger.Info("hello", "component", "test")

	line := strings.TrimSpace(buf.String())
	require.NotEmpty(t, line)
	assert.False(
		t, json.Valid([]byte(line)),
		"text output should not be valid JSON: %s", line,
	)
	assert.Contains(t, line, "component=test")
}

func TestNewLogger_EmptyFormatDefaultsToText(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, _, formatOK := newLogger(&buf, "", "info", false)
	require.True(t, formatOK)

	logger.Info("hello")
	assert.False(t, json.Valid(bytes.TrimSpace(buf.Bytes())))
}

func TestNewLogger_UnknownFormatFallsBackToTextAndReportsNotOK(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, _, formatOK := newLogger(&buf, "xml", "info", false)
	assert.False(t, formatOK)

	logger.Info("hello")
	assert.False(t, json.Valid(bytes.TrimSpace(buf.Bytes())))
}

func TestNewLogger_LevelFiltersBelowThreshold(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, levelOK, _ := newLogger(&buf, "text", "warn", false)
	require.True(t, levelOK)

	logger.Info("info-msg")
	logger.Warn("warn-msg")

	out := buf.String()
	assert.NotContains(t, out, "info-msg")
	assert.Contains(t, out, "warn-msg")
}

func TestNewLogger_UnknownLevelReportsNotOKAndUsesInfo(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, levelOK, _ := newLogger(&buf, "text", "bogus", false)
	assert.False(t, levelOK)

	logger.Debug("debug-msg") // below info => suppressed
	logger.Info("info-msg")
	out := buf.String()
	assert.NotContains(t, out, "debug-msg")
	assert.Contains(t, out, "info-msg")
}

func TestNewLogger_DebugFlagOverridesLevel(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	// level=error, but --debug must force debug level.
	logger, _, _ := newLogger(&buf, "text", "error", true)

	logger.Debug("debug-msg")
	assert.Contains(t, buf.String(), "debug-msg")
}

func TestParseLogLevel(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in     string
		want   slog.Level
		wantOK bool
	}{
		{"debug", slog.LevelDebug, true},
		{"info", slog.LevelInfo, true},
		{"", slog.LevelInfo, true},
		{"WARN", slog.LevelWarn, true},
		{"warning", slog.LevelWarn, true},
		{"error", slog.LevelError, true},
		{"bogus", slog.LevelInfo, false},
	}
	for _, c := range cases {
		got, ok := parseLogLevel(c.in)
		assert.Equalf(t, c.want, got, "level %q", c.in)
		assert.Equalf(t, c.wantOK, ok, "ok %q", c.in)
	}
}

// A profile file that fails to close must be reported to stderr, not
// silently dropped -- a truncated CPU/memory profile is otherwise
// indistinguishable from a clean one until someone tries to load it. The
// second Close() is forced to fail by closing f once already, ahead of the
// call under test, which deterministically exhausts the file descriptor.
func TestCloseProfileFileReportsCloseError(t *testing.T) {
	t.Parallel()

	f, err := os.CreateTemp(t.TempDir(), "profile-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("unexpected error on first close: %v", err)
	}

	var buf bytes.Buffer
	closeProfileFile(&buf, f, "CPU")

	out := buf.String()
	if !strings.Contains(out, "could not close CPU profile file") {
		t.Fatalf("expected close error to be reported, got: %q", out)
	}
}

// The common case -- a clean close -- must stay silent. Reporting on every
// successful profile write would bury the genuine failures this exists to
// surface.
func TestCloseProfileFileStaysQuietOnSuccess(t *testing.T) {
	t.Parallel()

	f, err := os.CreateTemp(t.TempDir(), "profile-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	var buf bytes.Buffer
	closeProfileFile(&buf, f, "memory")

	if buf.Len() != 0 {
		t.Fatalf(
			"expected no output on successful close, got: %q",
			buf.String(),
		)
	}
}

func TestMithrilPprofServerUsesDedicatedBindAddress(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{
		BindAddr:      "0.0.0.0",
		DebugBindAddr: "127.0.0.1",
		DebugPort:     6060,
	}
	srv := newDebugPprofHTTPServer(cfg)
	if srv == nil {
		t.Fatal("expected enabled pprof server")
	}
	if got, want := srv.Addr, "127.0.0.1:6060"; got != want {
		t.Fatalf("pprof address = %q, want %q", got, want)
	}

	cfg.DebugBindAddr = "0.0.0.0"
	srv = newDebugPprofHTTPServer(cfg)
	if srv == nil {
		t.Fatal("expected explicitly exposed pprof server")
	}
	if got, want := srv.Addr, "0.0.0.0:6060"; got != want {
		t.Fatalf("explicit wildcard pprof address = %q, want %q", got, want)
	}
}
