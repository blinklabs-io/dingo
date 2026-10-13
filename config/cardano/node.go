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
	"io/fs"
	"math"
	"math/big"
	"os"
	"path"
	"path/filepath"

	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"gopkg.in/yaml.v3"
)

// CardanoNodeConfig represents the config.json/yaml file used by cardano-node.
type CardanoNodeConfig struct {
	// Embedded filesystem for loading genesis files
	embedFS                                    embed.FS
	alonzoGenesis                              *alonzo.AlonzoGenesis
	byronGenesis                               *byron.ByronGenesis
	conwayGenesis                              *conway.ConwayGenesis
	dijkstraGenesis                            *dijkstra.DijkstraGenesis
	shelleyGenesis                             *shelley.ShelleyGenesis
	checkpoints                                map[uint64]string
	path                                       string
	AlonzoGenesisFile                          string `yaml:"AlonzoGenesisFile"`
	AlonzoGenesisHash                          string `yaml:"AlonzoGenesisHash"`
	ByronGenesisFile                           string `yaml:"ByronGenesisFile"`
	ByronGenesisHash                           string `yaml:"ByronGenesisHash"`
	ConwayGenesisFile                          string `yaml:"ConwayGenesisFile"`
	ConwayGenesisHash                          string `yaml:"ConwayGenesisHash"`
	DijkstraGenesisFile                        string `yaml:"DijkstraGenesisFile"`
	DijkstraGenesisHash                        string `yaml:"DijkstraGenesisHash"`
	MithrilGenesisVerificationKey              string `yaml:"MithrilGenesisVerificationKey"`
	MithrilGenesisVerificationKeyFile          string `yaml:"MithrilGenesisVerificationKeyFile"`
	MithrilGenesisAncillaryVerificationKey     string `yaml:"MithrilGenesisAncillaryVerificationKey"`
	MithrilGenesisAncillaryVerificationKeyFile string `yaml:"MithrilGenesisAncillaryVerificationKeyFile"`
	ShelleyGenesisFile                         string `yaml:"ShelleyGenesisFile"`
	ShelleyGenesisHash                         string `yaml:"ShelleyGenesisHash"`
	CheckpointsFile                            string `yaml:"CheckpointsFile"`
	CheckpointsFileHash                        string `yaml:"CheckpointsFileHash"`
	// PBftSignatureThreshold is the optional Byron PBFT signature threshold,
	// the maximum share of the last k blocks one genesis key may sign.
	PBftSignatureThreshold *CardanoNodeDouble `yaml:"PBftSignatureThreshold"`

	// Hard fork epoch configuration. Pointer types distinguish
	// "not set" (nil) from "set to 0" (*0), which is critical
	// because setting these to 0 instructs the node to perform
	// the hard fork at genesis (epoch 0), starting directly in
	// that era. This is used by devnets and testnets.
	ExperimentalHardForksEnabled *bool   `yaml:"ExperimentalHardForksEnabled"`
	TestShelleyHardForkAtEpoch   *uint64 `yaml:"TestShelleyHardForkAtEpoch"`
	TestAllegraHardForkAtEpoch   *uint64 `yaml:"TestAllegraHardForkAtEpoch"`
	TestMaryHardForkAtEpoch      *uint64 `yaml:"TestMaryHardForkAtEpoch"`
	TestAlonzoHardForkAtEpoch    *uint64 `yaml:"TestAlonzoHardForkAtEpoch"`
	TestBabbageHardForkAtEpoch   *uint64 `yaml:"TestBabbageHardForkAtEpoch"`
	TestConwayHardForkAtEpoch    *uint64 `yaml:"TestConwayHardForkAtEpoch"`
	TestDijkstraHardForkAtEpoch  *uint64 `yaml:"TestDijkstraHardForkAtEpoch"`

	// P2P peer target configuration from cardano-node config.json.
	// These are read but only used as fallback defaults when the
	// Dingo-native config (dingo.yaml / env) does not specify them.
	TargetNumberOfRootPeers        int  `yaml:"TargetNumberOfRootPeers"`
	TargetNumberOfKnownPeers       int  `yaml:"TargetNumberOfKnownPeers"`
	TargetNumberOfEstablishedPeers int  `yaml:"TargetNumberOfEstablishedPeers"`
	TargetNumberOfActivePeers      int  `yaml:"TargetNumberOfActivePeers"`
	EnableP2P                      bool `yaml:"EnableP2P"`

	// PeerSharing in cardano-node config.json. Pointer distinguishes
	// "field absent" (nil) from "explicitly false". Only consulted as
	// a fallback default for non-block-producing nodes when the
	// Dingo-native peerSharing value is unset.
	PeerSharing *bool `yaml:"PeerSharing"`
}

// CardanoNodeDouble is a numeric node-config value that cardano-node reads as
// a Double.
type CardanoNodeDouble float64

// UnmarshalYAML accepts finite numeric scalars only.
func (d *CardanoNodeDouble) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode ||
		(node.Tag != "!!float" && node.Tag != "!!int") {
		return fmt.Errorf("expected a numeric scalar, got %s", node.Tag)
	}
	var value float64
	if err := node.Decode(&value); err != nil {
		return err
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Errorf("expected a finite number, got %s", node.Value)
	}
	*d = CardanoNodeDouble(value)
	return nil
}

const (
	defaultMithrilGenesisVerificationKeyFile          = "genesis.vkey"
	defaultMithrilGenesisAncillaryVerificationKeyFile = "ancillary.vkey"
)

func NewCardanoNodeConfigFromReader(r io.Reader) (*CardanoNodeConfig, error) {
	var ret CardanoNodeConfig
	dec := yaml.NewDecoder(r)
	if err := dec.Decode(&ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

// PBFTSignatureLimit returns the most blocks one genesis key may sign in a
// window of the last securityParam Byron blocks. ouroboros-consensus computes
// floor(threshold * k) in Double arithmetic and stores it as a Word64, so a
// product just below an integer rounds down and a negative product wraps.
// configured is false when PBftSignatureThreshold is absent, in which case
// cardano-node's default of 0.22 applies.
func (c *CardanoNodeConfig) PBFTSignatureLimit(
	securityParam uint64,
) (limit uint64, configured bool, err error) {
	if c == nil || c.PBftSignatureThreshold == nil {
		return 0, false, nil
	}
	product := float64(*c.PBftSignatureThreshold) * float64(securityParam)
	if math.IsNaN(product) || math.IsInf(product, 0) {
		return 0, true, fmt.Errorf(
			"PBftSignatureThreshold %v times k %d is not finite",
			float64(*c.PBftSignatureThreshold),
			securityParam,
		)
	}
	floor, _ := big.NewFloat(math.Floor(product)).Int(nil)
	modulus := new(big.Int).Lsh(big.NewInt(1), 64)
	return floor.Mod(floor, modulus).Uint64(), true, nil
}

func NewCardanoNodeConfigFromFile(file string) (*CardanoNodeConfig, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	c, err := NewCardanoNodeConfigFromReader(f)
	if err != nil {
		return nil, err
	}
	c.path = filepath.Dir(file)
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
	err := c.loadGenesisDocuments(func(name string) ([]byte, error) {
		if !filepath.IsAbs(name) {
			name = filepath.Join(c.path, name)
		}
		return os.ReadFile(name)
	})
	if err != nil {
		return err
	}
	if err := c.loadMithrilVerificationKeysFromDisk(); err != nil {
		return err
	}
	if err := c.validateGenesisConsistency(); err != nil {
		return err
	}
	if err := c.loadCheckpoints(); err != nil {
		return err
	}
	return nil
}

// loadGenesisDocuments reads each configured genesis document once, then
// hashes and parses those same bytes. Reading again to parse would let a file
// replaced after the read make the parsed configuration differ from the bytes
// whose hash was accepted.
func (c *CardanoNodeConfig) loadGenesisDocuments(
	read func(name string) ([]byte, error),
) error {
	if c.ByronGenesisFile != "" {
		data, err := readHashedGenesis(
			read, "Byron", c.ByronGenesisFile, &c.ByronGenesisHash,
			canonicalizeByronGenesisJSON,
		)
		if err != nil {
			return err
		}
		genesis, err := loadByronGenesisFromBytes(data)
		if err != nil {
			return err
		}
		c.byronGenesis = &genesis
	}
	if c.ShelleyGenesisFile != "" {
		data, err := readHashedGenesis(
			read, "Shelley", c.ShelleyGenesisFile, &c.ShelleyGenesisHash,
			normalizeGenesisLineEndings,
		)
		if err != nil {
			return err
		}
		genesis, err := shelley.NewShelleyGenesisFromReader(
			bytes.NewReader(data),
		)
		if err != nil {
			return err
		}
		c.shelleyGenesis = &genesis
	}
	if c.AlonzoGenesisFile != "" {
		data, err := readHashedGenesis(
			read, "Alonzo", c.AlonzoGenesisFile, &c.AlonzoGenesisHash,
			normalizeGenesisLineEndings,
		)
		if err != nil {
			return err
		}
		genesis, err := alonzo.NewAlonzoGenesisFromReader(
			bytes.NewReader(data),
		)
		if err != nil {
			return err
		}
		c.alonzoGenesis = &genesis
	}
	if c.ConwayGenesisFile != "" {
		data, err := readHashedGenesis(
			read, "Conway", c.ConwayGenesisFile, &c.ConwayGenesisHash,
			normalizeGenesisLineEndings,
		)
		if err != nil {
			return err
		}
		genesis, err := loadConwayGenesisFromBytes(data)
		if err != nil {
			return err
		}
		c.conwayGenesis = &genesis
	}
	if c.DijkstraGenesisFile != "" {
		data, err := readHashedGenesis(
			read, "Dijkstra", c.DijkstraGenesisFile, &c.DijkstraGenesisHash,
			normalizeGenesisLineEndings,
		)
		if err != nil {
			return err
		}
		genesis, err := dijkstra.NewDijkstraGenesisFromReader(
			bytes.NewReader(data),
		)
		if err != nil {
			return err
		}
		c.dijkstraGenesis = &genesis
	}
	return nil
}

// readHashedGenesis reads a genesis document, validates its hash against
// *hash (storing the computed hash there when none is configured), and
// returns the bytes that were hashed. hashInput derives the bytes the hash
// covers from the document.
func readHashedGenesis(
	read func(name string) ([]byte, error),
	era string,
	file string,
	hash *string,
	hashInput func([]byte) ([]byte, error),
) ([]byte, error) {
	data, err := read(file)
	if err != nil {
		return nil, err
	}
	input, err := hashInput(data)
	if err != nil {
		return nil, err
	}
	computed, err := validateGenesisHash(era, *hash, input)
	if err != nil {
		return nil, err
	}
	*hash = computed
	return input, nil
}

func normalizeGenesisLineEndings(data []byte) ([]byte, error) {
	return replaceGenesisLineEndings(data), nil
}

func (c *CardanoNodeConfig) loadOptionalConfigFile(
	filename string,
) ([]byte, error) {
	if filename == "" {
		return nil, nil
	}
	filePath := filename
	if !filepath.IsAbs(filePath) {
		filePath = filepath.Join(c.path, filePath)
	}
	ret, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf(
			"reading config file %q: %w", filePath, err,
		)
	}
	return ret, nil
}

func (c *CardanoNodeConfig) resolveOptionalConfigFile(
	configuredFilename string,
	defaultFilename string,
) string {
	if configuredFilename != "" {
		return configuredFilename
	}
	if defaultFilename == "" {
		return ""
	}
	filePath := defaultFilename
	if !filepath.IsAbs(filePath) {
		filePath = filepath.Join(c.path, filePath)
	}
	if _, err := os.Stat(filePath); err == nil {
		return defaultFilename
	}
	return ""
}

func (c *CardanoNodeConfig) loadMithrilVerificationKeysFromDisk() error {
	c.MithrilGenesisVerificationKeyFile = c.resolveOptionalConfigFile(
		c.MithrilGenesisVerificationKeyFile,
		defaultMithrilGenesisVerificationKeyFile,
	)
	if c.MithrilGenesisVerificationKeyFile != "" &&
		c.MithrilGenesisVerificationKey == "" {
		keyBytes, err := c.loadOptionalConfigFile(
			c.MithrilGenesisVerificationKeyFile,
		)
		if err != nil {
			return err
		}
		c.MithrilGenesisVerificationKey = string(keyBytes)
	}
	c.MithrilGenesisAncillaryVerificationKeyFile = c.resolveOptionalConfigFile(
		c.MithrilGenesisAncillaryVerificationKeyFile,
		defaultMithrilGenesisAncillaryVerificationKeyFile,
	)
	if c.MithrilGenesisAncillaryVerificationKeyFile != "" &&
		c.MithrilGenesisAncillaryVerificationKey == "" {
		keyBytes, err := c.loadOptionalConfigFile(
			c.MithrilGenesisAncillaryVerificationKeyFile,
		)
		if err != nil {
			return err
		}
		c.MithrilGenesisAncillaryVerificationKey = string(keyBytes)
	}
	return nil
}

func (c *CardanoNodeConfig) loadOptionalConfigFileFromEmbedFS(
	filename string,
) ([]byte, error) {
	if filename == "" {
		return nil, nil
	}
	filePath := path.Join(c.path, filename)
	f, err := c.embedFS.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", filePath, err)
	}
	defer f.Close()
	ret, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", filePath, err)
	}
	return ret, nil
}

func (c *CardanoNodeConfig) resolveOptionalConfigFileFromEmbedFS(
	configuredFilename string,
	defaultFilename string,
) string {
	if configuredFilename != "" {
		return configuredFilename
	}
	if defaultFilename == "" {
		return ""
	}
	filePath := path.Join(c.path, defaultFilename)
	if f, err := c.embedFS.Open(filePath); err == nil {
		f.Close()
		return defaultFilename
	}
	return ""
}

// loadGenesisConfigsFromEmbed loads all genesis configuration files from the embedded filesystem.
// This method mirrors loadGenesisConfigs but reads from embed.FS instead of the regular filesystem.
func (c *CardanoNodeConfig) loadGenesisConfigsFromEmbed() error {
	err := c.loadGenesisDocuments(func(name string) ([]byte, error) {
		return fs.ReadFile(c.embedFS, path.Join(c.path, name))
	})
	if err != nil {
		return err
	}
	c.MithrilGenesisVerificationKeyFile = c.resolveOptionalConfigFileFromEmbedFS(
		c.MithrilGenesisVerificationKeyFile,
		defaultMithrilGenesisVerificationKeyFile,
	)
	if c.MithrilGenesisVerificationKey == "" &&
		c.MithrilGenesisVerificationKeyFile != "" {
		keyBytes, err := c.loadOptionalConfigFileFromEmbedFS(
			c.MithrilGenesisVerificationKeyFile,
		)
		if err != nil {
			return err
		}
		c.MithrilGenesisVerificationKey = string(keyBytes)
	}
	c.MithrilGenesisAncillaryVerificationKeyFile = c.resolveOptionalConfigFileFromEmbedFS(
		c.MithrilGenesisAncillaryVerificationKeyFile,
		defaultMithrilGenesisAncillaryVerificationKeyFile,
	)
	if c.MithrilGenesisAncillaryVerificationKey == "" &&
		c.MithrilGenesisAncillaryVerificationKeyFile != "" {
		keyBytes, err := c.loadOptionalConfigFileFromEmbedFS(
			c.MithrilGenesisAncillaryVerificationKeyFile,
		)
		if err != nil {
			return err
		}
		c.MithrilGenesisAncillaryVerificationKey = string(keyBytes)
	}
	if err := c.validateGenesisConsistency(); err != nil {
		return err
	}
	if err := c.loadCheckpointsFromEmbed(); err != nil {
		return err
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
	// Decode one value rather than io.ReadAll: the reader need not reach EOF.
	var conwayGenesisBytes json.RawMessage
	if err := json.NewDecoder(r).Decode(&conwayGenesisBytes); err != nil {
		return err
	}
	conwayGenesis, err := loadConwayGenesisFromBytes(conwayGenesisBytes)
	if err != nil {
		return err
	}
	c.conwayGenesis = &conwayGenesis
	return nil
}

// DijkstraGenesis returns the Dijkstra genesis config specified in the
// cardano-node config.
func (c *CardanoNodeConfig) DijkstraGenesis() *dijkstra.DijkstraGenesis {
	return c.dijkstraGenesis
}

// LoadDijkstraGenesisFromReader loads a Dijkstra genesis config from an
// io.Reader. This is useful mostly for tests.
func (c *CardanoNodeConfig) LoadDijkstraGenesisFromReader(r io.Reader) error {
	dijkstraGenesis, err := dijkstra.NewDijkstraGenesisFromReader(r)
	if err != nil {
		return err
	}
	c.dijkstraGenesis = &dijkstraGenesis
	return nil
}

// P2PTargets returns the peer target values from the cardano-node
// config.json. Values of 0 mean the field was not present.
func (c *CardanoNodeConfig) P2PTargets() (
	rootPeers, knownPeers, establishedPeers, activePeers int,
) {
	return c.TargetNumberOfRootPeers,
		c.TargetNumberOfKnownPeers,
		c.TargetNumberOfEstablishedPeers,
		c.TargetNumberOfActivePeers
}

// HardForkEpoch returns the epoch at which the named era's hard fork is
// scheduled to occur, and whether it is scheduled at all.
//
// This matches cardano-node, which honours TestShelleyHardForkAtEpoch through
// TestConwayHardForkAtEpoch whatever ExperimentalHardForksEnabled says, and
// reads TestDijkstraHardForkAtEpoch only when that flag is explicitly true.
//
// Callers asking what the configuration *declares*, rather than what is
// scheduled, want DeclaredHardForkEpoch. Both read the same field through the
// same switch so the two questions cannot drift apart.
func (c *CardanoNodeConfig) HardForkEpoch(era string) (uint64, bool) {
	if era == "dijkstra" &&
		(c.ExperimentalHardForksEnabled == nil ||
			!*c.ExperimentalHardForksEnabled) {
		return 0, false
	}
	return c.DeclaredHardForkEpoch(era)
}

// DeclaredHardForkEpoch returns the epoch the configuration declares for the
// named era's hard fork, and whether the setting is present, ignoring
// ExperimentalHardForksEnabled. It differs from HardForkEpoch only for
// Dijkstra, whose override is not scheduled unless that flag is set.
func (c *CardanoNodeConfig) DeclaredHardForkEpoch(
	era string,
) (uint64, bool) {
	var p *uint64
	switch era {
	case "shelley":
		p = c.TestShelleyHardForkAtEpoch
	case "allegra":
		p = c.TestAllegraHardForkAtEpoch
	case "mary":
		p = c.TestMaryHardForkAtEpoch
	case "alonzo":
		p = c.TestAlonzoHardForkAtEpoch
	case "babbage":
		p = c.TestBabbageHardForkAtEpoch
	case "conway":
		p = c.TestConwayHardForkAtEpoch
	case "dijkstra":
		p = c.TestDijkstraHardForkAtEpoch
	default:
		return 0, false
	}
	if p == nil {
		return 0, false
	}
	return *p, true
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
	parsed, err := parseByronCanonicalJSON(genesisBytes)
	if err != nil {
		return nil, err
	}
	return renderByronCanonicalHash(parsed), nil
}

// loadConwayGenesisFromBytes parses a Conway genesis, tolerating members that
// cardano-node's lenient JSON parser ignores but gouroboros's strict decoder
// rejects: the top-level genDelegs and the legacy committee.quorum, both
// present in Vector Testnet's genesis. Dingo consumes neither. quorum is
// stripped only beside threshold. Any other unknown member is still rejected.
func loadConwayGenesisFromBytes(
	genesisBytes []byte,
) (conway.ConwayGenesis, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(genesisBytes, &doc); err != nil {
		return conway.ConwayGenesis{}, err
	}
	stripped := false
	if _, ok := doc["genDelegs"]; ok {
		delete(doc, "genDelegs")
		stripped = true
	}
	if rawCommittee, ok := doc["committee"]; ok {
		var committee map[string]json.RawMessage
		if err := json.Unmarshal(rawCommittee, &committee); err == nil {
			// cardano-ledger requires threshold and ignores quorum; stripping
			// quorum beside a missing or null threshold would decode to a nil
			// threshold and silently fall back to the default committee quorum.
			_, hasQuorum := committee["quorum"]
			threshold, hasThreshold := committee["threshold"]
			if hasQuorum && hasThreshold &&
				!bytes.Equal(bytes.TrimSpace(threshold), []byte("null")) {
				// Re-encoding the map keeps only the last of duplicate
				// members, hiding an earlier one from the strict decoder.
				if err := rejectDuplicateJSONMembers(rawCommittee); err != nil {
					return conway.ConwayGenesis{}, fmt.Errorf(
						"conway genesis committee: %w", err,
					)
				}
				delete(committee, "quorum")
				out, err := json.Marshal(committee)
				if err != nil {
					return conway.ConwayGenesis{}, err
				}
				doc["committee"] = out
				stripped = true
			}
		}
	}
	if stripped {
		if err := rejectDuplicateJSONMembers(genesisBytes); err != nil {
			return conway.ConwayGenesis{}, fmt.Errorf("conway genesis: %w", err)
		}
		out, err := json.Marshal(doc)
		if err != nil {
			return conway.ConwayGenesis{}, err
		}
		genesisBytes = out
	}
	return conway.NewConwayGenesisFromReader(bytes.NewReader(genesisBytes))
}

// rejectDuplicateJSONMembers returns an error if the JSON object in raw names
// any member more than once. Only the object's own members are checked.
func rejectDuplicateJSONMembers(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if _, err := dec.Token(); err != nil {
		return err
	}
	seen := make(map[string]struct{})
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		key, ok := tok.(string)
		if !ok {
			return fmt.Errorf("unexpected JSON token %v", tok)
		}
		if _, dup := seen[key]; dup {
			return fmt.Errorf("duplicate member %q", key)
		}
		seen[key] = struct{}{}
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			return err
		}
	}
	return nil
}

// loadByronGenesisFromBytes decodes a Byron genesis document into the
// gouroboros schema type using the Byron reference's first-occurrence
// semantics for duplicate object keys, then rejects genesis fields that are
// unsigned in the reference schema but were parsed as negative.
//
// gouroboros's byron.ByronGenesis decoder (like encoding/json generally)
// resolves duplicate JSON object keys last-occurrence-wins, which disagrees
// with the Byron reference's first-occurrence rule. Rather than changing
// that decoder (an upstream gouroboros concern), this
// pre-filters the parsed document down to one member per key, keeping
// whichever occurred first, before handing it to the decoder.
func loadByronGenesisFromBytes(
	genesisBytes []byte,
) (byron.ByronGenesis, error) {
	parsed, err := parseByronCanonicalJSON(genesisBytes)
	if err != nil {
		return byron.ByronGenesis{}, err
	}
	deduped := renderByronFirstOccurrenceJSON(parsed)
	genesis, err := byron.NewByronGenesisFromReader(bytes.NewReader(deduped))
	if err != nil {
		return byron.ByronGenesis{}, err
	}
	if err := validateByronGenesisUnsignedFields(&genesis); err != nil {
		return byron.ByronGenesis{}, err
	}
	return genesis, nil
}

// validateByronGenesisUnsignedFields rejects Byron genesis fields that the
// reference schema declares unsigned but which gouroboros parses into a
// signed Go int, allowing a negative value such as slotDuration "-1" through
// genesis loading undetected. Left unchecked, a negative SlotDuration reaches
// a bare uint conversion in the Byron era-shape calculation and wraps to a
// very large duration instead of failing here, at the point the bad value
// was introduced.
func validateByronGenesisUnsignedFields(genesis *byron.ByronGenesis) error {
	if genesis.BlockVersionData.SlotDuration < 0 {
		return fmt.Errorf(
			"byron genesis: slotDuration must not be negative, got %d",
			genesis.BlockVersionData.SlotDuration,
		)
	}
	return nil
}

func replaceGenesisLineEndings(genesisBytes []byte) []byte {
	// Normalize line endings so hashes to get rid of the hash mismatch on different environments
	genesisBytes = bytes.ReplaceAll(genesisBytes, []byte("\r\n"), []byte("\n"))
	return bytes.ReplaceAll(genesisBytes, []byte("\r"), []byte("\n"))
}
