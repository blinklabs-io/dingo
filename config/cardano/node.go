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
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"gopkg.in/yaml.v3"
)

// CardanoNodeConfig represents the config.json/yaml file used by cardano-node.
type CardanoNodeConfig struct {
	// Embedded filesystem for loading genesis files
	embedFS            embed.FS
	alonzoGenesis      *alonzo.AlonzoGenesis
	byronGenesis       *byron.ByronGenesis
	conwayGenesis      *conway.ConwayGenesis
	shelleyGenesis     *shelley.ShelleyGenesis
	path               string
	AlonzoGenesisFile  string `yaml:"AlonzoGenesisFile"`
	AlonzoGenesisHash  string `yaml:"AlonzoGenesisHash"`
	ByronGenesisFile   string `yaml:"ByronGenesisFile"`
	ByronGenesisHash   string `yaml:"ByronGenesisHash"`
	ConwayGenesisFile  string `yaml:"ConwayGenesisFile"`
	ConwayGenesisHash  string `yaml:"ConwayGenesisHash"`
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

// NewCardanoNodeConfigFromEmbedFS creates a CardanoNodeConfig from an embedded filesystem.
// It loads the main config file and all referenced genesis files from the embedded FS.
// The file parameter should be a path relative to the root of the embedded filesystem.
func NewCardanoNodeConfigFromEmbedFS(
	fs embed.FS,
	file string,
) (*CardanoNodeConfig, error) {
	f, err := fs.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	c, err := NewCardanoNodeConfigFromReader(f)
	if err != nil {
		return nil, err
	}
	c.path = path.Dir(file)
	c.embedFS = fs // Store reference to embedded FS
	if err := c.loadGenesisConfigsFromEmbed(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *CardanoNodeConfig) loadGenesisConfigs() error {
	// Load Byron genesis
	if c.ByronGenesisFile != "" {
		byronGenesisPath := c.ByronGenesisFile
		if !filepath.IsAbs(byronGenesisPath) {
			byronGenesisPath = path.Join(c.path, byronGenesisPath)
		}
		byronGenesisBytes, err := os.ReadFile(byronGenesisPath)
		if err != nil {
			return err
		}
		byronGenesisHashBytes, err := canonicalizeByronGenesisJSON(
			byronGenesisBytes,
		)
		if err != nil {
			return err
		}
		byronHash, err := validateGenesisHash(
			"Byron",
			c.ByronGenesisHash,
			byronGenesisHashBytes,
		)
		if err != nil {
			return err
		}
		//Store computed hash if config does not contain hash
		c.ByronGenesisHash = byronHash
		byronGenesis, err := byron.NewByronGenesisFromFile(byronGenesisPath)
		if err != nil {
			return err
		}
		c.byronGenesis = &byronGenesis
	}
	// Load Shelley genesis
	if c.ShelleyGenesisFile != "" {
		shelleyGenesisPath := c.ShelleyGenesisFile
		if !filepath.IsAbs(shelleyGenesisPath) {
			shelleyGenesisPath = path.Join(c.path, shelleyGenesisPath)
		}
		shelleyGenesisBytes, err := os.ReadFile(shelleyGenesisPath)
		if err != nil {
			return err
		}
		shelleyHash, err := validateGenesisHash(
			"Shelley",
			c.ShelleyGenesisHash,
			shelleyGenesisBytes,
		)
		if err != nil {
			return err
		}
		c.ShelleyGenesisHash = shelleyHash
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
		if !filepath.IsAbs(alonzoGenesisPath) {
			alonzoGenesisPath = path.Join(c.path, alonzoGenesisPath)
		}
		alonzoGenesisBytes, err := os.ReadFile(alonzoGenesisPath)
		if err != nil {
			return err
		}
		alonzoHash, err := validateGenesisHash(
			"Alonzo",
			c.AlonzoGenesisHash,
			alonzoGenesisBytes,
		)
		if err != nil {
			return err
		}
		c.AlonzoGenesisHash = alonzoHash
		alonzoGenesis, err := alonzo.NewAlonzoGenesisFromFile(alonzoGenesisPath)
		if err != nil {
			return err
		}
		c.alonzoGenesis = &alonzoGenesis
	}
	// Load Conway genesis
	if c.ConwayGenesisFile != "" {
		conwayGenesisPath := c.ConwayGenesisFile
		if !filepath.IsAbs(conwayGenesisPath) {
			conwayGenesisPath = path.Join(c.path, conwayGenesisPath)
		}
		conwayGenesisBytes, err := os.ReadFile(conwayGenesisPath)
		if err != nil {
			return err
		}
		conwayHash, err := validateGenesisHash(
			"Conway",
			c.ConwayGenesisHash,
			conwayGenesisBytes,
		)
		if err != nil {
			return err
		}
		c.ConwayGenesisHash = conwayHash
		conwayGenesis, err := conway.NewConwayGenesisFromFile(conwayGenesisPath)
		if err != nil {
			return err
		}
		c.conwayGenesis = &conwayGenesis
	}
	return nil
}

// loadGenesisFromEmbedFS loads a single genesis file from the embedded filesystem.
// It handles path resolution and file opening/closing automatically.
func (c *CardanoNodeConfig) loadGenesisFromEmbedFS(
	filename string,
) (io.ReadCloser, error) {
	if filename == "" {
		return nil, nil
	}

	genesisPath := filename
	genesisPath = path.Join(c.path, genesisPath)

	f, err := c.embedFS.Open(genesisPath)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// loadGenesisConfigsFromEmbed loads all genesis configuration files from the embedded filesystem.
// This method mirrors loadGenesisConfigs but reads from embed.FS instead of the regular filesystem.
func (c *CardanoNodeConfig) loadGenesisConfigsFromEmbed() error {
	// Load Byron genesis
	if f, err := c.loadGenesisFromEmbedFS(c.ByronGenesisFile); err != nil {
		return err
	} else if f != nil {
		defer f.Close()
		byronGenesisBytes, err := io.ReadAll(f)
		if err != nil {
			return err
		}
		byronGenesisHashBytes, err := canonicalizeByronGenesisJSON(
			byronGenesisBytes,
		)
		if err != nil {
			return err
		}
		byronHash, err := validateGenesisHash(
			"Byron",
			c.ByronGenesisHash,
			byronGenesisHashBytes,
		)
		if err != nil {
			return err
		}
		c.ByronGenesisHash = byronHash
		byronGenesis, err := byron.NewByronGenesisFromReader(
			bytes.NewReader(byronGenesisBytes),
		)
		if err != nil {
			return err
		}
		c.byronGenesis = &byronGenesis
	}

	// Load Shelley genesis
	if f, err := c.loadGenesisFromEmbedFS(c.ShelleyGenesisFile); err != nil {
		return err
	} else if f != nil {
		defer f.Close()
		shelleyGenesisBytes, err := io.ReadAll(f)
		if err != nil {
			return err
		}
		shelleyHash, err := validateGenesisHash(
			"Shelley",
			c.ShelleyGenesisHash,
			shelleyGenesisBytes,
		)
		if err != nil {
			return err
		}
		c.ShelleyGenesisHash = shelleyHash
		shelleyGenesis, err := shelley.NewShelleyGenesisFromReader(
			bytes.NewReader(shelleyGenesisBytes),
		)
		if err != nil {
			return err
		}
		c.shelleyGenesis = &shelleyGenesis
	}

	// Load Alonzo genesis
	if f, err := c.loadGenesisFromEmbedFS(c.AlonzoGenesisFile); err != nil {
		return err
	} else if f != nil {
		defer f.Close()
		alonzoGenesisBytes, err := io.ReadAll(f)
		if err != nil {
			return err
		}
		alonzoHash, err := validateGenesisHash(
			"Alonzo",
			c.AlonzoGenesisHash,
			alonzoGenesisBytes,
		)
		if err != nil {
			return err
		}
		c.AlonzoGenesisHash = alonzoHash
		alonzoGenesis, err := alonzo.NewAlonzoGenesisFromReader(
			bytes.NewReader(alonzoGenesisBytes),
		)
		if err != nil {
			return err
		}
		c.alonzoGenesis = &alonzoGenesis
	}

	// Load Conway genesis
	if f, err := c.loadGenesisFromEmbedFS(c.ConwayGenesisFile); err != nil {
		return err
	} else if f != nil {
		defer f.Close()
		conwayGenesisBytes, err := io.ReadAll(f)
		if err != nil {
			return err
		}
		conwayHash, err := validateGenesisHash(
			"Conway",
			c.ConwayGenesisHash,
			conwayGenesisBytes,
		)
		if err != nil {
			return err
		}
		c.ConwayGenesisHash = conwayHash
		conwayGenesis, err := conway.NewConwayGenesisFromReader(
			bytes.NewReader(conwayGenesisBytes),
		)
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

// LoadByronGenesisFromReader loads a Byron genesis config from an io.Reader
// This is useful mostly for tests
func (c *CardanoNodeConfig) LoadByronGenesisFromReader(r io.Reader) error {
	byronGenesis, err := byron.NewByronGenesisFromReader(r)
	if err != nil {
		return err
	}
	c.byronGenesis = &byronGenesis
	return nil
}

// ShelleyGenesis returns the Shelley genesis config specified in the cardano-node config
func (c *CardanoNodeConfig) ShelleyGenesis() *shelley.ShelleyGenesis {
	return c.shelleyGenesis
}

// LoadShelleyGenesisFromReader loads a Shelley genesis config from an io.Reader
// This is useful mostly for tests
func (c *CardanoNodeConfig) LoadShelleyGenesisFromReader(r io.Reader) error {
	shelleyGenesis, err := shelley.NewShelleyGenesisFromReader(r)
	if err != nil {
		return err
	}
	c.shelleyGenesis = &shelleyGenesis
	return nil
}

// AlonzoGenesis returns the Alonzo genesis config specified in the cardano-node config
func (c *CardanoNodeConfig) AlonzoGenesis() *alonzo.AlonzoGenesis {
	return c.alonzoGenesis
}

// LoadAlonzoGenesisFromReader loads a Alonzo genesis config from an io.Reader
// This is useful mostly for tests
func (c *CardanoNodeConfig) LoadAlonzoGenesisFromReader(r io.Reader) error {
	alonzoGenesis, err := alonzo.NewAlonzoGenesisFromReader(r)
	if err != nil {
		return err
	}
	c.alonzoGenesis = &alonzoGenesis
	return nil
}

// ConwayGenesis returns the Conway genesis config specified in the cardano-node config
func (c *CardanoNodeConfig) ConwayGenesis() *conway.ConwayGenesis {
	return c.conwayGenesis
}

// LoadConwayGenesisFromReader loads a Conway genesis config from an io.Reader
// This is useful mostly for tests
func (c *CardanoNodeConfig) LoadConwayGenesisFromReader(r io.Reader) error {
	conwayGenesis, err := conway.NewConwayGenesisFromReader(r)
	if err != nil {
		return err
	}
	c.conwayGenesis = &conwayGenesis
	return nil
}

func validateGenesisHash(
	genesisName string,
	expectedHash string,
	genesisBytes []byte,
) (string, error) {
	actualHash := lcommon.Blake2b256Hash(genesisBytes).String()
	if expectedHash != "" && expectedHash != actualHash {
		return "", fmt.Errorf(
			"%s genesis hash mismatch: expected %s, computed %s",
			genesisName,
			expectedHash,
			actualHash,
		)
	}
	return actualHash, nil
}

func canonicalizeByronGenesisJSON(genesisBytes []byte) ([]byte, error) {
	var payload any
	if err := json.Unmarshal(genesisBytes, &payload); err != nil {
		return nil, err
	}
	return json.Marshal(payload)
}
