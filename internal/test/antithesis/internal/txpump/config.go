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

package txpump

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/internal/test/antithesis/internal/genesis"
)

// Config holds all runtime configuration for txpump.
// Values are read from environment variables; defaults apply when a variable
// is absent or empty.
type Config struct {
	// NodeAddr is the primary N2C socket or TCP address to connect to.
	// Default: /ipc/node.socket
	NodeAddr string

	// NetworkMagic is the Cardano network magic number.
	// Default: 42 (devnet)
	NetworkMagic uint32

	// TxCountMin / TxCountMax bound the random batch size.
	// Default: 1 / 10
	TxCountMin int
	TxCountMax int

	// CooldownMin / CooldownMax (milliseconds) bound the inter-batch pause.
	// Default: 500 / 2000
	CooldownMin int
	CooldownMax int

	// Types is the set of transaction types to generate.
	// Recognised values: "payment", "delegation", "governance", "plutus".
	// Default: ["payment","delegation","governance","plutus"]
	Types []string

	// LogDir is the directory for structured log files.
	// Default: /logs
	LogDir string

	// FallbackAddr is a secondary N2C address tried when the primary fails.
	// Empty string means no fallback.
	FallbackAddr string

	// GenesisUTxOFile is an optional path to a JSON file containing initial
	// UTxO entries (used when the wallet is empty on first start).
	GenesisUTxOFile string

	// GenesisFile is the path to the testnet.yaml genesis configuration.
	// When set, EpochLength and NetworkMagic are read from the genesis.
	GenesisFile string

	// EpochLength is the number of slots per epoch. Default: 500.
	EpochLength uint64
}

// LoadConfig reads configuration from environment variables and returns a
// fully-populated Config struct.
func LoadConfig() (*Config, error) {
	cfg := &Config{
		NodeAddr:     envString("TXPUMP_NODE_ADDR", "/ipc/node.socket"),
		NetworkMagic: 42,
		TxCountMin:   1,
		TxCountMax:   10,
		CooldownMin:  500,
		CooldownMax:  2000,
		Types:        []string{"payment", "delegation", "governance", "plutus"},
		LogDir:       envString("TXPUMP_LOG_DIR", "/logs"),
		FallbackAddr: envString("TXPUMP_FALLBACK_ADDR", ""),
		GenesisUTxOFile: envString(
			"TXPUMP_GENESIS_UTXO_FILE", "",
		),
		GenesisFile: envString("TXPUMP_GENESIS_FILE", ""),
		EpochLength: 500,
	}

	if v := os.Getenv("TXPUMP_NETWORK_MAGIC"); v != "" {
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return nil, fmt.Errorf(
				"TXPUMP_NETWORK_MAGIC: invalid uint32 %q: %w", v, err,
			)
		}
		cfg.NetworkMagic = uint32(n)
	}

	if v := os.Getenv("TXPUMP_TX_COUNT_MIN"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return nil, fmt.Errorf(
				"TXPUMP_TX_COUNT_MIN: must be a positive integer, got %q", v,
			)
		}
		cfg.TxCountMin = n
	}

	if v := os.Getenv("TXPUMP_TX_COUNT_MAX"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return nil, fmt.Errorf(
				"TXPUMP_TX_COUNT_MAX: must be a positive integer, got %q", v,
			)
		}
		cfg.TxCountMax = n
	}

	if v := os.Getenv("TXPUMP_COOLDOWN_MIN"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return nil, fmt.Errorf(
				"TXPUMP_COOLDOWN_MIN: must be a non-negative integer, got %q",
				v,
			)
		}
		cfg.CooldownMin = n
	}

	if v := os.Getenv("TXPUMP_COOLDOWN_MAX"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return nil, fmt.Errorf(
				"TXPUMP_COOLDOWN_MAX: must be a non-negative integer, got %q",
				v,
			)
		}
		cfg.CooldownMax = n
	}

	if v := os.Getenv("TXPUMP_TYPES"); v != "" {
		cfg.Types = splitComma(v)
	}

	// Load genesis config if provided. Genesis values override defaults
	// but explicit env vars take precedence.
	if cfg.GenesisFile != "" {
		gcfg, loadErr := genesis.Load(cfg.GenesisFile)
		if loadErr != nil {
			return nil, fmt.Errorf("TXPUMP_GENESIS_FILE: %w", loadErr)
		}
		cfg.EpochLength = gcfg.EpochLength
		if os.Getenv("TXPUMP_NETWORK_MAGIC") == "" {
			cfg.NetworkMagic = gcfg.NetworkMagic
		}
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// validate checks that the config values are internally consistent.
func (c *Config) validate() error {
	if c.TxCountMin > c.TxCountMax {
		return fmt.Errorf(
			"TXPUMP_TX_COUNT_MIN (%d) must be <= TXPUMP_TX_COUNT_MAX (%d)",
			c.TxCountMin, c.TxCountMax,
		)
	}
	if c.CooldownMin > c.CooldownMax {
		return fmt.Errorf(
			"TXPUMP_COOLDOWN_MIN (%d) must be <= TXPUMP_COOLDOWN_MAX (%d)",
			c.CooldownMin, c.CooldownMax,
		)
	}
	if len(c.Types) == 0 {
		return errors.New("TXPUMP_TYPES must not be empty")
	}
	validTypes := map[string]bool{
		"payment":    true,
		"delegation": true,
		"governance": true,
		"plutus":     true,
	}
	for _, t := range c.Types {
		if !validTypes[t] {
			return fmt.Errorf(
				"unknown transaction type: %q (valid: payment, delegation, governance, plutus)",
				t,
			)
		}
	}
	return nil
}

// envString returns the value of the named environment variable, or the
// provided default if the variable is absent or empty.
func envString(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// splitComma splits a comma-separated string, trimming whitespace from each
// element, and discards empty elements.
func splitComma(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
