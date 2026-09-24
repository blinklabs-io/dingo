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

package analysis

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/antithesis/internal/genesis"
)

// Analysis runs are bounded to avoid accidentally configuring a test that
// waits for an impractical amount of time while still leaving ample room for
// long-running Antithesis environments.
const maxAnalysisDurationSeconds int64 = 24 * 60 * 60

// Config holds all runtime configuration for the analysis binary.
// Values are read from environment variables; defaults apply when a variable
// is absent or empty.
type Config struct {
	// LogDir is the directory containing node log files (p*.log).
	// Default: /logs
	LogDir string

	// InitialWait is how long to wait before the first analysis pass,
	// allowing nodes to start up and produce some blocks.
	// Default: 30s
	InitialWait time.Duration

	// CheckInterval is the period between successive analysis passes.
	// Default: 5s
	CheckInterval time.Duration

	// MaxForkDepth is the maximum number of slots that may separate two
	// nodes' chain tips before a fork-depth safety violation is reported.
	// Default: 40
	MaxForkDepth int

	// Pools is the expected number of stake pools (and dingo nodes) in the
	// test network.
	// Default: 5
	Pools int

	// MinBlocksSample is the minimum number of forged blocks that must be
	// observed before chain-quality assertions are evaluated.
	// Default: 50
	MinBlocksSample int

	// GenesisFile is the path to the testnet.yaml genesis configuration.
	// When set, EpochLength, Pools, and MaxForkDepth are read from the
	// genesis config (overriding env defaults). Default: empty (use env defaults).
	GenesisFile string

	// EpochLength is the number of slots per epoch. Read from genesis config
	// or defaults to 500.
	EpochLength uint64
}

// LoadConfig reads configuration from environment variables and returns a
// fully-populated Config struct.
func LoadConfig() (*Config, error) {
	cfg := &Config{
		LogDir:          envStringA("ANALYSIS_LOG_DIR", "/logs"),
		InitialWait:     30 * time.Second,
		CheckInterval:   5 * time.Second,
		MaxForkDepth:    40,
		Pools:           5,
		MinBlocksSample: 50,
		GenesisFile:     envStringA("ANALYSIS_GENESIS_FILE", ""),
		EpochLength:     500,
	}

	if v := os.Getenv("ANALYSIS_INITIAL_WAIT"); v != "" {
		var err error
		cfg.InitialWait, err = parseAnalysisDuration(
			"ANALYSIS_INITIAL_WAIT", v, true,
		)
		if err != nil {
			return nil, err
		}
	}

	if v := os.Getenv("ANALYSIS_CHECK_INTERVAL"); v != "" {
		var err error
		cfg.CheckInterval, err = parseAnalysisDuration(
			"ANALYSIS_CHECK_INTERVAL", v, false,
		)
		if err != nil {
			return nil, err
		}
	}

	if v := os.Getenv("ANALYSIS_MAX_FORK_DEPTH"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return nil, fmt.Errorf(
				"ANALYSIS_MAX_FORK_DEPTH: must be a positive integer, got %q",
				v,
			)
		}
		cfg.MaxForkDepth = n
	}

	if v := os.Getenv("ANALYSIS_POOLS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return nil, fmt.Errorf(
				"ANALYSIS_POOLS: must be a positive integer, got %q",
				v,
			)
		}
		cfg.Pools = n
	}

	if v := os.Getenv("ANALYSIS_MIN_BLOCKS_SAMPLE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return nil, fmt.Errorf(
				"ANALYSIS_MIN_BLOCKS_SAMPLE: must be a positive integer, got %q",
				v,
			)
		}
		cfg.MinBlocksSample = n
	}

	// If a genesis file is provided, read network params from it.
	// Genesis values override the hardcoded defaults but env vars
	// still take precedence (they were parsed above).
	if cfg.GenesisFile != "" {
		gcfg, loadErr := genesis.Load(cfg.GenesisFile)
		if loadErr != nil {
			return nil, fmt.Errorf("ANALYSIS_GENESIS_FILE: %w", loadErr)
		}
		// Only apply genesis values when the env var was NOT set.
		if os.Getenv("ANALYSIS_POOLS") == "" {
			if gcfg.PoolCount <= 0 {
				return nil, fmt.Errorf(
					"ANALYSIS_GENESIS_FILE: poolCount must be > 0, got %d",
					gcfg.PoolCount,
				)
			}
			cfg.Pools = gcfg.PoolCount
		}
		if os.Getenv("ANALYSIS_MAX_FORK_DEPTH") == "" &&
			gcfg.SecurityParam > 0 {
			// SecurityParam always fits in int.
			cfg.MaxForkDepth = int(gcfg.SecurityParam) //nolint:gosec
		}
		if gcfg.EpochLength == 0 {
			return nil, errors.New(
				"ANALYSIS_GENESIS_FILE: epochLength must be > 0",
			)
		}
		cfg.EpochLength = gcfg.EpochLength
	}

	return cfg, nil
}

func parseAnalysisDuration(name, value string, allowZero bool) (time.Duration, error) {
	seconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil || seconds < 0 || (!allowZero && seconds == 0) ||
		seconds > maxAnalysisDurationSeconds {
		qualifier := "a positive"
		if allowZero {
			qualifier = "a non-negative"
		}
		return 0, fmt.Errorf(
			"%s: must be %s integer (seconds) <= %d, got %q",
			name, qualifier, maxAnalysisDurationSeconds, value,
		)
	}
	return time.Duration(seconds) * time.Second, nil
}

// envStringA returns the value of the named environment variable, or the
// provided default if the variable is absent or empty.
func envStringA(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
