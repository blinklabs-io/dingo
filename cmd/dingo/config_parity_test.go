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
	"maps"
	"testing"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/mithril"
)

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
