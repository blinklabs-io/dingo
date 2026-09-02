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
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// defaultTestnetYAMLPath is the fallback path to the network spec,
// relative to the scenarios test package directory
// (internal/test/devnet/scenarios/). It is only a fallback: run-tests.sh
// exports DEVNET_TESTNET_YAML for the mode it actually brought up, so
// the harness reads the same spec the configurator generated genesis
// from. Tests outside scenarios/ (e.g. in internal/test/devnet/ itself)
// must set DEVNET_TESTNET_YAML or call LoadDevNetConfigFrom with an
// explicit path, otherwise LoadDevNetConfig will fail to find the file.
const defaultTestnetYAMLPath = "../testnet.yaml"

// testnetParams holds the required testnet parameters from document 1.
type testnetParams struct {
	PoolCount    int    `yaml:"poolCount"`
	NetworkMagic uint32 `yaml:"networkMagic"`
}

// shelleyGenesisOverride holds the Shelley genesis parameters from document 3.
type shelleyGenesisOverride struct {
	EpochLength      uint64  `yaml:"epochLength"`
	SlotLength       float64 `yaml:"slotLength"`
	ActiveSlotsCoeff float64 `yaml:"activeSlotsCoeff"`
	SecurityParam    uint64  `yaml:"securityParam"`
}

// DevNetConfig holds the parsed configuration values from testnet.yaml.
type DevNetConfig struct {
	PoolCount        int
	NetworkMagic     uint32
	EpochLength      uint64
	SlotLength       float64
	ActiveSlotsCoeff float64
	SecurityParam    uint64
}

// SlotDuration returns the wall-clock duration of a single slot.
func (c *DevNetConfig) SlotDuration() time.Duration {
	return time.Duration(c.SlotLength * float64(time.Second))
}

// ExpectedBlocksPerSlot returns the approximate probability that any
// given slot produces a block (i.e. activeSlotsCoeff, ignoring per-pool
// stake fraction).
func (c *DevNetConfig) ExpectedBlocksPerSlot() float64 {
	return c.ActiveSlotsCoeff
}

// ExpectedBlockTime returns the average wall-clock time between blocks.
// This is slotDuration / activeSlotsCoeff (e.g. 2.5s with 1s slots and f=0.4).
// Panics if ActiveSlotsCoeff is zero or negative (invalid configuration).
func (c *DevNetConfig) ExpectedBlockTime() time.Duration {
	if c.ActiveSlotsCoeff <= 0 {
		panic(fmt.Sprintf(
			"DevNetConfig.ExpectedBlockTime: invalid ActiveSlotsCoeff %.4f",
			c.ActiveSlotsCoeff,
		))
	}
	return time.Duration(
		float64(c.SlotDuration()) / c.ActiveSlotsCoeff,
	)
}

// LoadDevNetConfig reads the active network spec and returns the parsed
// DevNetConfig. The path is taken from the DEVNET_TESTNET_YAML environment
// variable; if unset, it defaults to defaultTestnetYAMLPath (relative to
// the test package).
func LoadDevNetConfig() (*DevNetConfig, error) {
	path := os.Getenv("DEVNET_TESTNET_YAML")
	if path == "" {
		path = defaultTestnetYAMLPath
	}
	return LoadDevNetConfigFrom(path)
}

// LoadDevNetConfigFrom parses the network spec at an explicit path. It is
// the form used by tests that check the checked-in specs directly, without
// depending on which mode is currently exported to the environment.
func LoadDevNetConfigFrom(path string) (*DevNetConfig, error) {
	//nolint:gosec // network spec path comes from the harness or a test,
	// never from network input
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf(
			"LoadDevNetConfigFrom: reading %s: %w", path, err,
		)
	}

	// Split on "\n---" (line-leading document separator) to avoid matching
	// "---" inside comments or values. The YAML file has 6 documents.
	docs := bytes.Split(data, []byte("\n---"))

	// docs[0] is the content before the first "---" (the file comment header).
	// docs[1] is document 1 (required testnet params).
	// docs[2] is document 2 (Byron genesis overrides).
	// docs[3] is document 3 (Shelley genesis overrides).
	if len(docs) < 4 {
		return nil, fmt.Errorf(
			"LoadDevNetConfigFrom: expected at least 4 YAML documents in %s, got %d",
			path,
			len(docs),
		)
	}

	var params testnetParams
	if err := yaml.Unmarshal(docs[1], &params); err != nil {
		return nil, fmt.Errorf(
			"LoadDevNetConfigFrom: parsing testnet params (doc 1): %w", err,
		)
	}

	var shelley shelleyGenesisOverride
	if err := yaml.Unmarshal(docs[3], &shelley); err != nil {
		return nil, fmt.Errorf(
			"LoadDevNetConfigFrom: parsing shelley genesis overrides (doc 3): %w",
			err,
		)
	}

	return &DevNetConfig{
		PoolCount:        params.PoolCount,
		NetworkMagic:     params.NetworkMagic,
		EpochLength:      shelley.EpochLength,
		SlotLength:       shelley.SlotLength,
		ActiveSlotsCoeff: shelley.ActiveSlotsCoeff,
		SecurityParam:    shelley.SecurityParam,
	}, nil
}

// NonceStabilityWindowSlots returns 4k/f, the window cardano-node uses
// for the candidate-nonce freeze. The candidate nonce for the next epoch
// stops evolving this many slots before the epoch ends, so an epoch
// shorter than the window can never freeze it.
func (c *DevNetConfig) NonceStabilityWindowSlots() uint64 {
	if c.ActiveSlotsCoeff <= 0 {
		return 0
	}
	return uint64(4 * float64(c.SecurityParam) / c.ActiveSlotsCoeff)
}

// BlockFetchStabilityWindowSlots returns 3k/f, the shorter window used
// for blockfetch stability.
func (c *DevNetConfig) BlockFetchStabilityWindowSlots() uint64 {
	if c.ActiveSlotsCoeff <= 0 {
		return 0
	}
	return uint64(3 * float64(c.SecurityParam) / c.ActiveSlotsCoeff)
}

// EpochDuration returns the wall-clock length of one epoch.
func (c *DevNetConfig) EpochDuration() time.Duration {
	return SlotsDuration(c.EpochLength, c.SlotDuration())
}

// SlotsDuration returns the wall-clock time n slots take. DevNet slot
// counts come from a checked-in spec and are small, but the conversion is
// clamped rather than trusted so a nonsensical spec cannot wrap a
// duration into the past.
func SlotsDuration(slots uint64, slotDuration time.Duration) time.Duration {
	if slots > math.MaxInt32 {
		slots = math.MaxInt32
	}
	return time.Duration(slots) * slotDuration //nolint:gosec // clamped above
}

// NextEpochBoundary returns the first epoch-start slot strictly after
// slot. Deriving the target from the observed tip rather than assuming
// slot == epochLength lets a scenario cross a real transition even when
// it attaches to a network that has been running for a while.
func (c *DevNetConfig) NextEpochBoundary(slot uint64) uint64 {
	if c.EpochLength == 0 {
		return 0
	}
	return (slot/c.EpochLength + 1) * c.EpochLength
}

// Validate checks that the spec describes an internally consistent
// network. Every violation is reported, not just the first, so shrinking
// a network's timing tells you everything that has to move with it.
func (c *DevNetConfig) Validate() error {
	var errs []error
	if c.PoolCount <= 0 {
		errs = append(errs, fmt.Errorf("poolCount %d must be positive",
			c.PoolCount))
	}
	if c.NetworkMagic == 0 {
		errs = append(errs, errors.New("networkMagic must be non-zero"))
	}
	if c.SlotLength <= 0 {
		errs = append(errs, fmt.Errorf("slotLength %.4f must be positive",
			c.SlotLength))
	}
	if c.SecurityParam == 0 {
		errs = append(errs, errors.New("securityParam must be non-zero"))
	}
	if c.EpochLength == 0 {
		errs = append(errs, errors.New("epochLength must be non-zero"))
	}
	if c.ActiveSlotsCoeff <= 0 || c.ActiveSlotsCoeff > 1 {
		errs = append(errs, fmt.Errorf(
			"activeSlotsCoeff %.4f must be in (0, 1]", c.ActiveSlotsCoeff,
		))
		// The stability windows below divide by f; without a usable
		// coefficient they carry no information.
		return errors.Join(errs...)
	}
	if nonce := c.NonceStabilityWindowSlots(); nonce >= c.EpochLength {
		errs = append(errs, fmt.Errorf(
			"nonce stability window 4k/f = %d slots must be shorter than"+
				" epochLength %d (k=%d, f=%.4f)",
			nonce, c.EpochLength, c.SecurityParam, c.ActiveSlotsCoeff,
		))
	}
	if fetch := c.BlockFetchStabilityWindowSlots(); fetch >= c.EpochLength {
		errs = append(errs, fmt.Errorf(
			"blockfetch stability window 3k/f = %d slots must be shorter"+
				" than epochLength %d (k=%d, f=%.4f)",
			fetch, c.EpochLength, c.SecurityParam, c.ActiveSlotsCoeff,
		))
	}
	return errors.Join(errs...)
}
