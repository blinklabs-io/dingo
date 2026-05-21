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

//go:build erastest

package erastest

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"gopkg.in/yaml.v3"
)

// defaultTestnetYAMLPath is the relative path used when ERASTEST_TESTNET_YAML
// is unset. Resolved from the test package directory
// (internal/test/erastest/), which is where testnet.yaml lives.
const defaultTestnetYAMLPath = "testnet.yaml"

// testnetParams holds the required testnet parameters from document 1.
type testnetParams struct {
	PoolCount    int    `yaml:"poolCount"`
	NetworkMagic uint32 `yaml:"networkMagic"`
}

// shelleyGenesisOverride holds the Shelley genesis parameters from
// document 3, including the starting protocol version.
type shelleyGenesisOverride struct {
	EpochLength      uint64  `yaml:"epochLength"`
	SlotLength       float64 `yaml:"slotLength"`
	ActiveSlotsCoeff float64 `yaml:"activeSlotsCoeff"`
	SecurityParam    uint64  `yaml:"securityParam"`
	ProtocolParams   struct {
		ProtocolVersion struct {
			Major uint `yaml:"major"`
			Minor uint `yaml:"minor"`
		} `yaml:"protocolVersion"`
	} `yaml:"protocolParams"`
}

// nodeConfigOverride holds the fields read from document 6 (the node
// config.json overrides). All hard-fork-epoch fields are pointers so the
// zero value is distinguishable from "configured to fork at epoch 0."
type nodeConfigOverride struct {
	ExperimentalHardForksEnabled *bool   `yaml:"ExperimentalHardForksEnabled"`
	TestShelleyHardForkAtEpoch   *uint64 `yaml:"TestShelleyHardForkAtEpoch"`
	TestAllegraHardForkAtEpoch   *uint64 `yaml:"TestAllegraHardForkAtEpoch"`
	TestMaryHardForkAtEpoch      *uint64 `yaml:"TestMaryHardForkAtEpoch"`
	TestAlonzoHardForkAtEpoch    *uint64 `yaml:"TestAlonzoHardForkAtEpoch"`
	TestBabbageHardForkAtEpoch   *uint64 `yaml:"TestBabbageHardForkAtEpoch"`
	TestConwayHardForkAtEpoch    *uint64 `yaml:"TestConwayHardForkAtEpoch"`
}

// Config holds the parsed configuration values from the eras testnet.yaml.
type Config struct {
	PoolCount        int
	NetworkMagic     uint32
	EpochLength      uint64
	SlotLength       float64
	ActiveSlotsCoeff float64
	SecurityParam    uint64
	// StartingMajorVersion is the protocolVersion.major from the Shelley
	// genesis override. Selects the era at slot 0: e.g. major=2 → Shelley,
	// major=10 → Conway.
	StartingMajorVersion uint
	// hardForkEpochs maps successor era ID → scheduled fork epoch from
	// the Test{Era}HardForkAtEpoch overrides. Populated only when
	// ExperimentalHardForksEnabled is explicitly true; an absent key
	// means "no scheduled fork into that era was configured."
	hardForkEpochs map[uint]uint64
}

// SlotDuration returns the wall-clock duration of a single slot.
func (c *Config) SlotDuration() time.Duration {
	return time.Duration(c.SlotLength * float64(time.Second))
}

// ExpectedBlocksPerSlot returns the approximate probability that any
// given slot produces a block (i.e. activeSlotsCoeff, ignoring per-pool
// stake fraction).
func (c *Config) ExpectedBlocksPerSlot() float64 {
	return c.ActiveSlotsCoeff
}

// ExpectedBlockTime returns the average wall-clock time between blocks.
// This is slotDuration / activeSlotsCoeff (e.g. 2.5s with 1s slots and
// f=0.4). Panics if ActiveSlotsCoeff is zero or negative.
func (c *Config) ExpectedBlockTime() time.Duration {
	if c.ActiveSlotsCoeff <= 0 {
		panic(fmt.Sprintf(
			"erastest.Config.ExpectedBlockTime: invalid ActiveSlotsCoeff %.4f",
			c.ActiveSlotsCoeff,
		))
	}
	return time.Duration(
		float64(c.SlotDuration()) / c.ActiveSlotsCoeff,
	)
}

// LoadConfig reads testnet.yaml and returns the parsed Config. The path
// is taken from ERASTEST_TESTNET_YAML; if unset, defaults to
// defaultTestnetYAMLPath relative to the test package directory.
func LoadConfig() (*Config, error) {
	path := os.Getenv("ERASTEST_TESTNET_YAML")
	if path == "" {
		path = defaultTestnetYAMLPath
	}

	data, err := os.ReadFile(path) //nolint:gosec // trusted config path supplied by the test runner
	if err != nil {
		return nil, fmt.Errorf(
			"erastest.LoadConfig: reading %s: %w", path, err,
		)
	}

	// Split on a line-leading "---" so a "---" inside a comment or value
	// is not misinterpreted as a document separator. The eras
	// testnet.yaml has 6 documents.
	docs := bytes.Split(data, []byte("\n---"))

	// docs[0] is the file header (content before the first "---").
	// docs[1] is document 1 (required testnet params).
	// docs[2] is document 2 (Byron overrides).
	// docs[3] is document 3 (Shelley overrides).
	// docs[6] is document 6 (Node config overrides).
	if len(docs) < 4 {
		return nil, fmt.Errorf(
			"erastest.LoadConfig: expected at least 4 YAML documents in %s, got %d",
			path, len(docs),
		)
	}

	var params testnetParams
	if err := yaml.Unmarshal(docs[1], &params); err != nil {
		return nil, fmt.Errorf(
			"erastest.LoadConfig: parsing testnet params (doc 1): %w", err,
		)
	}

	var shelley shelleyGenesisOverride
	if err := yaml.Unmarshal(docs[3], &shelley); err != nil {
		return nil, fmt.Errorf(
			"erastest.LoadConfig: parsing shelley genesis overrides (doc 3): %w",
			err,
		)
	}

	cfg := &Config{
		PoolCount:            params.PoolCount,
		NetworkMagic:         params.NetworkMagic,
		EpochLength:          shelley.EpochLength,
		SlotLength:           shelley.SlotLength,
		ActiveSlotsCoeff:     shelley.ActiveSlotsCoeff,
		SecurityParam:        shelley.SecurityParam,
		StartingMajorVersion: shelley.ProtocolParams.ProtocolVersion.Major,
		hardForkEpochs:       map[uint]uint64{},
	}

	// Document 6 (node config override) is optional but, when present
	// and ExperimentalHardForksEnabled is true, supplies the scheduled
	// fork epochs we want to assert against.
	if len(docs) >= 7 {
		var nodeCfg nodeConfigOverride
		if err := yaml.Unmarshal(docs[6], &nodeCfg); err != nil {
			return nil, fmt.Errorf(
				"erastest.LoadConfig: parsing node config override (doc 6): %w",
				err,
			)
		}
		experimental := nodeCfg.ExperimentalHardForksEnabled != nil &&
			*nodeCfg.ExperimentalHardForksEnabled
		if experimental {
			pairs := []struct {
				era   *eras.EraDesc
				epoch *uint64
			}{
				{&eras.ShelleyEraDesc, nodeCfg.TestShelleyHardForkAtEpoch},
				{&eras.AllegraEraDesc, nodeCfg.TestAllegraHardForkAtEpoch},
				{&eras.MaryEraDesc, nodeCfg.TestMaryHardForkAtEpoch},
				{&eras.AlonzoEraDesc, nodeCfg.TestAlonzoHardForkAtEpoch},
				{&eras.BabbageEraDesc, nodeCfg.TestBabbageHardForkAtEpoch},
				{&eras.ConwayEraDesc, nodeCfg.TestConwayHardForkAtEpoch},
			}
			for _, p := range pairs {
				if p.epoch != nil {
					cfg.hardForkEpochs[p.era.Id] = *p.epoch
				}
			}
		}
	}

	return cfg, nil
}

// StartingEra returns the era the chain runs in at genesis, derived from
// StartingMajorVersion. Returns (nil, false) when the major version maps
// to no known era (which would indicate a misconfigured testnet.yaml).
func (c *Config) StartingEra() (*eras.EraDesc, bool) {
	return eras.EraForVersion(c.StartingMajorVersion)
}

// HardForkEpoch returns the scheduled epoch at which the chain
// transitions into the given era, and whether the schedule was
// configured.
func (c *Config) HardForkEpoch(era *eras.EraDesc) (uint64, bool) {
	if era == nil {
		return 0, false
	}
	epoch, ok := c.hardForkEpochs[era.Id]
	return epoch, ok
}

// HardForkSlot returns the slot at which the given era is scheduled to
// become active (epoch * epochLength). Returns (0, false) if the fork is
// not scheduled or epochLength is zero.
func (c *Config) HardForkSlot(era *eras.EraDesc) (uint64, bool) {
	epoch, ok := c.HardForkEpoch(era)
	if !ok || c.EpochLength == 0 {
		return 0, false
	}
	return epoch * c.EpochLength, true
}

// ScheduledTransitions returns the configured era transitions in chain
// order, skipping any era already active at genesis. The returned slice
// is suitable for "iterate the forks the test is expected to witness."
func (c *Config) ScheduledTransitions() []ScheduledTransition {
	var out []ScheduledTransition
	startMajor := c.StartingMajorVersion
	for _, era := range eras.Eras {
		epoch, ok := c.hardForkEpochs[era.Id]
		if !ok {
			continue
		}
		// Skip eras already active at genesis: their MaxMajorVersion is
		// at or below the configured starting major version.
		if era.MaxMajorVersion <= startMajor {
			continue
		}
		out = append(out, ScheduledTransition{
			Era:   era,
			Epoch: epoch,
			Slot:  epoch * c.EpochLength,
		})
	}
	return out
}

// ScheduledTransition describes one configured fork: the target era it
// transitions into, the configured epoch, and the corresponding slot.
type ScheduledTransition struct {
	Era   eras.EraDesc
	Epoch uint64
	Slot  uint64
}

// String renders the transition for log output.
func (t ScheduledTransition) String() string {
	return fmt.Sprintf(
		"%s@epoch=%d(slot=%d)",
		strings.ToLower(t.Era.Name), t.Epoch, t.Slot,
	)
}
