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

//go:build devnet

package devnet

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// defaultTestnetYAMLPath is the default path to testnet.yaml, relative
// to the scenarios test package directory.
const defaultTestnetYAMLPath = "../../test/devnet/testnet.yaml"

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

// LoadDevNetConfig reads testnet.yaml and returns the parsed DevNetConfig.
// The path is taken from the DEVNET_TESTNET_YAML environment variable; if
// unset, it defaults to defaultTestnetYAMLPath (relative to the test package).
func LoadDevNetConfig() (*DevNetConfig, error) {
	path := os.Getenv("DEVNET_TESTNET_YAML")
	if path == "" {
		path = defaultTestnetYAMLPath
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf(
			"LoadDevNetConfig: reading %s: %w", path, err,
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
			"LoadDevNetConfig: expected at least 4 YAML documents in %s, got %d",
			path, len(docs),
		)
	}

	var params testnetParams
	if err := yaml.Unmarshal(docs[1], &params); err != nil {
		return nil, fmt.Errorf(
			"LoadDevNetConfig: parsing testnet params (doc 1): %w", err,
		)
	}

	var shelley shelleyGenesisOverride
	if err := yaml.Unmarshal(docs[3], &shelley); err != nil {
		return nil, fmt.Errorf(
			"LoadDevNetConfig: parsing shelley genesis overrides (doc 3): %w",
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
