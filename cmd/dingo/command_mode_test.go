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
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/config"
)

// newTestRootCmd wires the real subcommands under a root command,
// mirroring main() so the command-classification helpers see the same
// tree.
func newTestRootCmd() *cobra.Command {
	root := &cobra.Command{Use: "dingo"}
	root.AddCommand(
		serveCommand(),
		loadCommand(),
		listCommand(),
		versionCommand(),
		mithrilCommand(),
		syncCommand(),
	)
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
	root := newTestRootCmd()
	tests := []struct {
		name string
		path []string
		want bool
	}{
		{"bare root", nil, false},
		{"version", []string{"version"}, true},
		{"list", []string{"list"}, true},
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
