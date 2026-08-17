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
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
