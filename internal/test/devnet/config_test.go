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
	"fmt"
	"os"
	"regexp"
	"slices"
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
	for _, spec := range []struct {
		file      string
		poolCount int
	}{
		{"testnet.yaml", 2},
		{"testnet-dingo.yaml", 3},
		{"testnet-accelerated.yaml", 2},
		{"testnet-dingo-accelerated.yaml", 3},
	} {
		t.Run(spec.file, func(t *testing.T) {
			cfg, err := LoadDevNetConfigFrom(spec.file)
			require.NoError(t, err)
			require.NoError(t, cfg.Validate())
			require.Equal(t, spec.poolCount, cfg.PoolCount)
			require.Equal(t, uint32(42), cfg.NetworkMagic)
		})
	}
}

// The accelerated specs exist to make a full scenario fit the reference
// runner budget; if someone relaxes their timing back toward canonical,
// this fails before CI spends five minutes discovering it.
func TestAcceleratedSpecsMeetTheRunnerBudget(t *testing.T) {
	for _, file := range []string{
		"testnet-accelerated.yaml",
		"testnet-dingo-accelerated.yaml",
	} {
		t.Run(file, func(t *testing.T) {
			cfg, err := LoadDevNetConfigFrom(file)
			require.NoError(t, err)

			plan, err := NewScenarioPlan(cfg)
			require.NoError(t, err)
			require.LessOrEqual(t, plan.Total(), ReferenceRunnerBudget)
		})
	}
}

// TestAcceleratedTxPumpConfirmationWindowFitsBudget guards the fix for
// dingo#4215: TXPUMP_CONFIRMATION_SLOTS's checked-in Compose default (600)
// is tuned for the canonical profile's 1s slot length, giving a 600s
// confirmation window. Both accelerated specs use a 0.5s slot length, so
// the same slot count reaches a 300s window -- exactly
// ReferenceRunnerBudget, the accelerated scenario's hard timeout. Once
// txpump's funded outputs are all inside that window it stops submitting,
// silently starving the propagation phase.
//
// run-tests.sh overrides TXPUMP_CONFIRMATION_SLOTS for --accelerated runs;
// this test converts that override with each accelerated spec's own slot
// length and fails if the resulting window is not well inside the hard
// timeout, regardless of what slot length a future accelerated spec picks.
func TestAcceleratedTxPumpConfirmationWindowFitsBudget(t *testing.T) {
	data, err := os.ReadFile("run-tests.sh")
	require.NoError(t, err)

	overrideRe := regexp.MustCompile(
		`(?m)^\s*export TXPUMP_CONFIRMATION_SLOTS=(\d+)\s*$`)
	match := overrideRe.FindStringSubmatch(string(data))
	require.NotEmpty(t, match,
		"run-tests.sh must export an accelerated TXPUMP_CONFIRMATION_SLOTS"+
			" override; without one, docker-compose.yml's checked-in 600"+
			" default (tuned for the canonical 1s slot) applies to the"+
			" accelerated profile too, reaching ReferenceRunnerBudget")
	overrideSlots, err := strconv.ParseUint(match[1], 10, 64)
	require.NoError(t, err)

	for _, file := range []string{
		"testnet-accelerated.yaml",
		"testnet-dingo-accelerated.yaml",
	} {
		t.Run(file, func(t *testing.T) {
			cfg, err := LoadDevNetConfigFrom(file)
			require.NoError(t, err)

			window := SlotsDuration(overrideSlots, cfg.SlotDuration())
			require.Less(t, window, ReferenceRunnerBudget,
				"%s: a %d-slot confirmation window at a %s slot length is"+
					" %s, which reaches the %s accelerated hard timeout;"+
					" txpump goes silent once every funded output is"+
					" quarantined",
				file, overrideSlots, cfg.SlotDuration(), window,
				ReferenceRunnerBudget)
			require.LessOrEqual(t, window, ReferenceRunnerBudget/2,
				"%s: confirmation window %s should stay well inside the"+
					" %s hard timeout, not merely under it",
				file, window, ReferenceRunnerBudget)
		})
	}
}

// The canonical specs must stay on canonical timing: they are what the
// soak and canary runs use, and quietly accelerating them would remove
// the long-wall-clock coverage the fast scenario deliberately does not
// provide.
func TestCanonicalSpecsKeepCanonicalTiming(t *testing.T) {
	for _, file := range []string{"testnet.yaml", "testnet-dingo.yaml"} {
		t.Run(file, func(t *testing.T) {
			cfg, err := LoadDevNetConfigFrom(file)
			require.NoError(t, err)
			require.Equal(t, uint64(500), cfg.EpochLength)
			require.Equal(t, 1.0, cfg.SlotLength)
			require.Equal(t, uint64(40), cfg.SecurityParam)
			require.Equal(t, time.Second, cfg.SlotDuration())
		})
	}
}

// run-tests.sh and start.sh each map a mode to a network spec, and
// docker-compose.yml supplies the defaults. Nothing makes them agree, so a
// rename that updates one and not another would leave the Go harness
// deriving its timings from a different spec than the configurator
// generated genesis from — the scenario would still run, just against a
// network whose parameters it has wrong.
//
// Rather than route all three through a shared resolver (indirection
// across a shell/compose/Go boundary for two filenames), assert that every
// spec they name exists and that the two scripts agree on the accelerated
// pair.
func TestScriptsAndComposeAgreeOnSpecFiles(t *testing.T) {
	specRe := regexp.MustCompile(`testnet[a-z-]*\.yaml`)
	acceleratedRe := regexp.MustCompile(`testnet[a-z-]*accelerated\.yaml`)

	referenced := map[string][]string{}
	for _, file := range []string{
		"run-tests.sh", "start.sh", "docker-compose.yml",
	} {
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		for _, name := range specRe.FindAllString(string(data), -1) {
			referenced[file] = append(referenced[file], name)
		}
	}

	for file, names := range referenced {
		require.NotEmpty(t, names, "%s names no network spec", file)
		for _, name := range names {
			require.FileExists(t, name,
				"%s references %s, which does not exist", file, name)
		}
	}

	accelerated := func(file string) []string {
		var out []string
		for _, name := range referenced[file] {
			if acceleratedRe.MatchString(name) && !slices.Contains(out, name) {
				out = append(out, name)
			}
		}
		slices.Sort(out)
		return out
	}
	runTests := accelerated("run-tests.sh")
	require.Len(t, runTests, 2,
		"run-tests.sh should name both accelerated specs")
	require.Equal(t, runTests, accelerated("start.sh"),
		"start.sh and run-tests.sh must select the same accelerated specs;"+
			" if they drift, the harness and the running network resolve"+
			" different timings")
}

// TestComposeTxPumpCooldownUsesMilliseconds keeps the DevNet load profile in
// agreement with txpump's millisecond-valued configuration contract and the
// 5-15 second cadence documented in README.md. A value copied as seconds
// makes txpump open a fresh NtC connection and submit each configured batch
// every 5-15 milliseconds. That saturates the mempool and can starve the
// persistent ChainSync observers which drive the accelerated scenario.
func TestComposeTxPumpCooldownUsesMilliseconds(t *testing.T) {
	environments := loadComposeTxPumpEnvironments(t)

	for _, service := range []string{"txpump-dingo", "txpump"} {
		t.Run(service, func(t *testing.T) {
			environment := environments[service]
			for _, tc := range []struct {
				name string
				want int
			}{
				{name: "MIN", want: 5_000},
				{name: "MAX", want: 15_000},
			} {
				key := "TXPUMP_COOLDOWN_" + tc.name
				requireComposeEnvInt(t, service, environment, key, tc.want,
					"txpump cooldown values are milliseconds; the DevNet"+
						" profile documents a 5-15 second cadence")
			}
		})
	}
}

func requireComposeEnvInt(
	t *testing.T,
	service string,
	environment map[string]string,
	key string,
	want int,
	message string,
) {
	t.Helper()
	raw, ok := environment[key]
	require.True(t, ok, "Compose service %s must define %s", service, key)
	got, err := strconv.Atoi(raw)
	require.NoError(t, err,
		"Compose service %s setting %s must be an integer", service, key)
	require.Equal(t, want, got, message)
}

// requireComposeEnvDefaultInt asserts that a Compose environment value uses
// the "${key:-N}" substitution form and that its fallback N equals want.
// TXPUMP_CONFIRMATION_SLOTS uses this form, rather than a bare literal, so
// run-tests.sh can override it for the accelerated profile without moving
// the checked-in canonical default; see
// TestAcceleratedTxPumpConfirmationWindowFitsBudget.
func requireComposeEnvDefaultInt(
	t *testing.T,
	service string,
	environment map[string]string,
	key string,
	want int,
	message string,
) {
	t.Helper()
	raw, ok := environment[key]
	require.True(t, ok, "Compose service %s must define %s", service, key)

	pattern := fmt.Sprintf(`^\$\{%s:-(-?\d+)\}$`, regexp.QuoteMeta(key))
	match := regexp.MustCompile(pattern).FindStringSubmatch(raw)
	require.NotEmpty(t, match,
		`Compose service %s setting %s must be "${%s:-N}", got %q`,
		service, key, key, raw)

	got, err := strconv.Atoi(match[1])
	require.NoError(t, err,
		"Compose service %s setting %s default must be an integer",
		service, key)
	require.Equal(t, want, got, message)
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

// TestComposeTxPumpSubmitsOneTransactionPerBatch prevents the DevNet load
// generator from immediately spending outputs created earlier in the same
// batch. Those dependent transactions can become invalid when an early fork
// removes their parent, leaving the accelerated scenario without a stable
// transaction-bearing block.
func TestComposeTxPumpSubmitsOneTransactionPerBatch(t *testing.T) {
	environments := loadComposeTxPumpEnvironments(t)

	for _, service := range []string{"txpump-dingo", "txpump"} {
		t.Run(service, func(t *testing.T) {
			environment := environments[service]
			for _, bound := range []string{"MIN", "MAX"} {
				key := "TXPUMP_TX_COUNT_" + bound
				requireComposeEnvInt(
					t,
					service,
					environment,
					key,
					1,
					"DevNet txpump batches must not create unconfirmed dependency chains",
				)
			}
			requireComposeEnvDefaultInt(t, service, environment,
				"TXPUMP_CONFIRMATION_SLOTS", 600,
				"submitted outputs must remain quarantined across early"+
					" forks on the canonical profile; the accelerated"+
					" profile overrides this via run-tests.sh, checked by"+
					" TestAcceleratedTxPumpConfirmationWindowFitsBudget")
		})
	}
}

// TestComposeTxPumpWaitsForProfileReadiness verifies that each txpump service
// starts only after every node in its active profile is healthy, preventing
// genesis-backed transactions from being submitted during early convergence.
func TestComposeTxPumpWaitsForProfileReadiness(t *testing.T) {
	composeData, err := os.ReadFile("docker-compose.yml")
	require.NoError(t, err)

	var compose struct {
		Services map[string]struct {
			DependsOn map[string]struct {
				Condition string `yaml:"condition"`
			} `yaml:"depends_on"`
		} `yaml:"services"`
	}
	require.NoError(t, yaml.Unmarshal(composeData, &compose))

	for service, dependencies := range map[string][]string{
		"txpump-dingo": {"dingo-1", "dingo-2", "dingo-3", "dingo-relay"},
		"txpump":       {"dingo-producer", "cardano-producer", "cardano-relay"},
	} {
		t.Run(service, func(t *testing.T) {
			for _, dependency := range dependencies {
				condition, ok := compose.Services[service].DependsOn[dependency]
				require.True(t, ok, "%s must depend on %s", service, dependency)
				require.Equal(t, "service_healthy", condition.Condition,
					"%s must wait for %s to be healthy", service, dependency)
			}
		})
	}
}

func TestLoadDevNetConfigFromMissingFile(t *testing.T) {
	_, err := LoadDevNetConfigFrom("no-such-testnet.yaml")
	require.Error(t, err)
}

func TestLoadDevNetConfigHonoursEnvOverride(t *testing.T) {
	t.Setenv("DEVNET_TESTNET_YAML", "testnet-dingo-accelerated.yaml")
	cfg, err := LoadDevNetConfig()
	require.NoError(t, err)
	require.Equal(t, 3, cfg.PoolCount)
	require.Equal(t, 500*time.Millisecond, cfg.SlotDuration())
}
