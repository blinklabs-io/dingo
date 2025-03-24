// Copyright 2025 Blink Labs Software
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

package cardano

import (
	"io"
	"os"
	"path"
	"strings"

	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"gopkg.in/yaml.v3"
)

// CardanoNodeConfig represents the config.json/yaml file used by cardano-node
type CardanoNodeConfig struct {
	path               string
	alonzoGenesis      *alonzo.AlonzoGenesis
	AlonzoGenesisFile  string `yaml:"AlonzoGenesisFile"`
	AlonzoGenesisHash  string `yaml:"AlonzoGenesisHash"`
	byronGenesis       *byron.ByronGenesis
	ByronGenesisFile   string `yaml:"ByronGenesisFile"`
	ByronGenesisHash   string `yaml:"ByronGenesisHash"`
	conwayGenesis      *conway.ConwayGenesis
	ConwayGenesisFile  string `yaml:"ConwayGenesisFile"`
	ConwayGenesisHash  string `yaml:"ConwayGenesisHash"`
	shelleyGenesis     *shelley.ShelleyGenesis
	ShelleyGenesisFile string `yaml:"ShelleyGenesisFile"`
	ShelleyGenesisHash string `yaml:"ShelleyGenesisHash"`
}

func NewCardanoNodeConfigFromReader(r io.Reader) (*CardanoNodeConfig, error) {
	var ret CardanoNodeConfig
	dec := yaml.NewDecoder(r)
	if err := dec.Decode(&ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

func NewCardanoNodeConfigFromFile(file string) (*CardanoNodeConfig, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	c, err := NewCardanoNodeConfigFromReader(f)
	if err != nil {
		return nil, err
	}
	c.path = path.Dir(file)
	if err := c.loadGenesisConfigs(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *CardanoNodeConfig) loadGenesisConfigs() error {
	prefix := "/"
	// Load Byron genesis
	if c.ByronGenesisFile != "" {
		byronGenesisPath := c.ByronGenesisFile
		if !strings.HasPrefix(byronGenesisPath, prefix) {
			byronGenesisPath = path.Join(c.path, byronGenesisPath)
		}
		// TODO: check genesis file hash (#399)
		byronGenesis, err := byron.NewByronGenesisFromFile(byronGenesisPath)
		if err != nil {
			return err
		}
		c.byronGenesis = &byronGenesis
	}
	// Load Shelley genesis
	if c.ShelleyGenesisFile != "" {
		shelleyGenesisPath := c.ShelleyGenesisFile
		if !strings.HasPrefix(shelleyGenesisPath, prefix) {
			shelleyGenesisPath = path.Join(c.path, shelleyGenesisPath)
		}
		// TODO: check genesis file hash (#399)
		shelleyGenesis, err := shelley.NewShelleyGenesisFromFile(
			shelleyGenesisPath,
		)
		if err != nil {
			return err
		}
		c.shelleyGenesis = &shelleyGenesis
	}
	// Load Alonzo genesis
	if c.AlonzoGenesisFile != "" {
		alonzoGenesisPath := c.AlonzoGenesisFile
		if !strings.HasPrefix(alonzoGenesisPath, prefix) {
			alonzoGenesisPath = path.Join(c.path, alonzoGenesisPath)
		}
		// TODO: check genesis file hash (#399)
		alonzoGenesis, err := alonzo.NewAlonzoGenesisFromFile(alonzoGenesisPath)
		if err != nil {
			return err
		}
		c.alonzoGenesis = &alonzoGenesis
	}
	// Load Conway genesis
	if c.ConwayGenesisFile != "" {
		conwayGenesisPath := c.ConwayGenesisFile
		if !strings.HasPrefix(conwayGenesisPath, prefix) {
			conwayGenesisPath = path.Join(c.path, conwayGenesisPath)
		}
		// TODO: check genesis file hash (#399)
		conwayGenesis, err := conway.NewConwayGenesisFromFile(conwayGenesisPath)
		if err != nil {
			return err
		}
		c.conwayGenesis = &conwayGenesis
	}
	return nil
}

// ByronGenesis returns the Byron genesis config specified in the cardano-node config
func (c *CardanoNodeConfig) ByronGenesis() *byron.ByronGenesis {
	return c.byronGenesis
}

// ShelleyGenesis returns the Shelley genesis config specified in the cardano-node config
func (c *CardanoNodeConfig) ShelleyGenesis() *shelley.ShelleyGenesis {
	return c.shelleyGenesis
}

// AlonzoGenesis returns the Alonzo genesis config specified in the cardano-node config
func (c *CardanoNodeConfig) AlonzoGenesis() *alonzo.AlonzoGenesis {
	return c.alonzoGenesis
}

// ConwayGenesis returns the Conway genesis config specified in the cardano-node config
func (c *CardanoNodeConfig) ConwayGenesis() *conway.ConwayGenesis {
	return c.conwayGenesis
}
