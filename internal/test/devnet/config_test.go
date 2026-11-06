//go:build linux

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

package devnet

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// TestCheckedInSpecsAreValid parses every network spec in this directory
// and enforces the consensus-timing invariants on it. This is the guard
// that keeps an accelerated configuration internally valid: shortening
// the epoch without also shrinking k would put the candidate-nonce freeze
// outside the epoch, which fails here rather than as a mystifying DevNet
// stall.
func TestCheckedInSpecsAreValid(t *testing.T) {
	t.Parallel()
	poolCounts := map[string]int{
		"testnet.yaml":                   2,
		"testnet-dingo.yaml":             3,
		"testnet-dingo-tachys.yaml":      3,
		"testnet-accelerated.yaml":       2,
		"testnet-accelerated-pv11.yaml":  2,
		"testnet-accelerated-pv12.yaml":  2,
		"testnet-dingo-accelerated.yaml": 3,
		"testnet-dingo-leios.yaml":       3,
	}
	// The table is keyed by file so that a new spec cannot be added to
	// this directory without also being validated here.
	for _, file := range checkedInSpecs(t, "testnet*.yaml") {
		t.Run(file, func(t *testing.T) {
			t.Parallel()
			poolCount, ok := poolCounts[file]
			require.True(t, ok, "%s has no expected pool count in this test", file)
			cfg, err := LoadDevNetConfigFrom(file)
			require.NoError(t, err)
			require.NoError(t, cfg.Validate())
			require.Equal(t, poolCount, cfg.PoolCount)
			wantMagic := uint32(42)
			if file == "testnet-accelerated-pv12.yaml" {
				// Dingo only enables the Dijkstra era for the Dijkstra magic.
				wantMagic = 6
			}
			require.Equal(t, wantMagic, cfg.NetworkMagic)
		})
	}
}

// checkedInSpecs returns the network spec files in this directory that
// match pattern.
func checkedInSpecs(t *testing.T, pattern string) []string {
	t.Helper()
	files, err := filepath.Glob(pattern)
	require.NoError(t, err)
	require.NotEmpty(t, files, "no spec matches %s", pattern)
	return files
}

// The accelerated specs exist to make a full scenario fit the reference
// runner budget; if someone relaxes their timing back toward canonical,
// this fails before CI spends five minutes discovering it.
func TestAcceleratedSpecsMeetTheRunnerBudget(t *testing.T) {
	t.Parallel()
	for _, file := range checkedInSpecs(t, "testnet*accelerated*.yaml") {
		t.Run(file, func(t *testing.T) {
			t.Parallel()
			cfg, err := LoadDevNetConfigFrom(file)
			require.NoError(t, err)

			plan, err := NewScenarioPlan(cfg)
			require.NoError(t, err)
			require.LessOrEqual(t, plan.Total(), ReferenceRunnerBudget)
		})
	}
}

// The configured quarantine must expire before the accelerated run does;
// otherwise submitted outputs cannot fund a later round in that scenario.
func TestAcceleratedTxPumpQuarantineFitsRunnerBudget(t *testing.T) {
	t.Parallel()
	environments := loadComposeTxPumpEnvironments(t)
	for _, profile := range []struct {
		service string
		spec    string
	}{
		{"txpump", "testnet-accelerated.yaml"},
		{"txpump-dingo", "testnet-dingo-accelerated.yaml"},
	} {
		for _, script := range []string{"run-tests.sh", "start.sh"} {
			t.Run(profile.service+"/"+script, func(t *testing.T) {
				t.Parallel()
				cfg, err := LoadDevNetConfigFrom(profile.spec)
				require.NoError(t, err)
				plan, err := NewScenarioPlan(cfg)
				require.NoError(t, err)
				args := []string{"--accelerated"}
				if profile.service == "txpump" {
					args = append(args, "--conformance")
				}
				slots := launchedTxPumpWindow(t, script, args...)
				require.GreaterOrEqual(
					t,
					slots,
					cfg.BlockFetchStabilityWindowSlots(),
					"quarantine must cover 3k/f slots, the window an early fork can still remove outputs in",
				)
				require.Equal(t, "${DEVNET_TXPUMP_CONFIRMATION_SLOTS:-600}",
					environments[profile.service]["TXPUMP_CONFIRMATION_SLOTS"],
					"Compose must consume the launcher's selected window")
				quarantine := SlotsDuration(slots, cfg.SlotDuration())
				t.Logf(
					"profile=%s slots=%d slotLength=%s quarantine=%s hardTimeout=%s",
					profile.spec,
					slots,
					cfg.SlotDuration(),
					quarantine,
					plan.HardTimeout,
				)
				require.Less(
					t,
					quarantine,
					plan.HardTimeout,
					"configured output quarantine must expire before the accelerated scenario ends",
				)
				propagation, ok := plan.Phase(PhasePropagation)
				require.True(t, ok)
				cooldown, err := strconv.Atoi(
					environments[profile.service]["TXPUMP_COOLDOWN_MAX"],
				)
				require.NoError(t, err)
				require.Less(
					t,
					quarantine+time.Duration(cooldown)*time.Millisecond,
					propagation.Deadline,
					"quarantine and a cooldown must fit before propagation ends",
				)
			})
		}
	}
}

func launchedTxPumpWindow(t *testing.T, script string, args ...string) uint64 {
	t.Helper()
	result := runFakeDevnetScript(t, script, 0, false, map[string]string{
		"DEVNET_TXPUMP_CONFIRMATION_SLOTS": "999999",
	}, args...)
	require.Equal(t, 0, result.exitCode, result.output)
	matches := regexp.MustCompile(`(?m)^TXPUMP_WINDOW=(\d+)$`).
		FindAllStringSubmatch(result.dockerLog, -1)
	require.NotEmpty(t, matches, "launcher never invoked Compose with a window")
	var slots uint64
	for i, match := range matches {
		got, err := strconv.ParseUint(match[1], 10, 64)
		require.NoError(t, err)
		if i == 0 {
			slots = got
		}
		require.Equal(
			t,
			slots,
			got,
			"Compose invocations must agree on the window",
		)
	}
	return slots
}

func TestCanonicalTxPumpWindowIsUnchanged(t *testing.T) {
	t.Parallel()
	for _, script := range []string{"run-tests.sh", "start.sh"} {
		for _, conformance := range []bool{false, true} {
			args := []string{}
			if conformance {
				args = append(args, "--conformance")
			}
			require.Equal(
				t,
				uint64(600),
				launchedTxPumpWindow(t, script, args...),
			)
		}
	}
}

func loadComposeTxPumpEnvironments(t *testing.T) map[string]map[string]string {
	t.Helper()
	composeData, err := os.ReadFile("docker-compose.yml")
	require.NoError(t, err)

	var compose struct {
		Services map[string]struct {
			Environment map[string]string `yaml:"environment"`
		} `yaml:"services"`
	}
	require.NoError(t, yaml.Unmarshal(composeData, &compose))

	environments := make(map[string]map[string]string, 2)
	for _, service := range []string{"txpump-dingo", "txpump"} {
		definition, ok := compose.Services[service]
		require.True(t, ok, "Compose service %s must exist", service)
		require.NotNil(t, definition.Environment,
			"Compose service %s must define an environment", service)
		environments[service] = definition.Environment
	}
	return environments
}

func TestLoadDevNetConfigHonoursEnvOverride(t *testing.T) {
	t.Setenv("DEVNET_TESTNET_YAML", "testnet-dingo-accelerated.yaml")
	cfg, err := LoadDevNetConfig()
	require.NoError(t, err)
	require.Equal(t, 3, cfg.PoolCount)
	require.Equal(t, 500*time.Millisecond, cfg.SlotDuration())
}
