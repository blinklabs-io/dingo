// Copyright 2024 Blink Labs Software
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

package topology

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
)

// TopologyConfig represents a cardano-node topology config
type TopologyConfig struct {
	LocalRoots         []TopologyConfigP2PLocalRoot     `json:"localRoots"`
	PublicRoots        []TopologyConfigP2PPublicRoot    `json:"publicRoots"`
	BootstrapPeers     []TopologyConfigP2PBootstrapPeer `json:"bootstrapPeers"`
	UseLedgerAfterSlot int64                            `json:"useLedgerAfterSlot"`
	PeerSnapshotFile   string                           `json:"peerSnapshotFile"`
	PeerSnapshot       *PeerSnapshotConfig              `json:"-"`
}

type TopologyConfigP2PAccessPoint struct {
	Address string `json:"address"`
	Port    uint   `json:"port"`
}

type TopologyConfigP2PLocalRoot struct {
	AccessPoints []TopologyConfigP2PAccessPoint `json:"accessPoints"`
	Advertise    bool                           `json:"advertise"`
	Trustable    bool                           `json:"trustable"`
	Valency      uint                           `json:"valency"`
	WarmValency  uint                           `json:"warmValency"`
}

type TopologyConfigP2PPublicRoot struct {
	AccessPoints []TopologyConfigP2PAccessPoint `json:"accessPoints"`
	Advertise    bool                           `json:"advertise"`
	Valency      uint                           `json:"valency"`
	WarmValency  uint                           `json:"warmValency"`
}

type TopologyConfigP2PBootstrapPeer = TopologyConfigP2PAccessPoint

// PeerSnapshotConfig represents cardano-node's peer-snapshot.json format.
type PeerSnapshotConfig struct {
	NetworkMagic        uint32                   `json:"NetworkMagic"`
	NodeToClientVersion uint16                   `json:"NodeToClientVersion"`
	Point               PeerSnapshotPoint        `json:"Point"`
	BigLedgerPools      []PeerSnapshotLedgerPool `json:"bigLedgerPools"`
	LedgerPools         []PeerSnapshotLedgerPool `json:"ledgerPools"`
}

type PeerSnapshotPoint struct {
	BlockPointHash string `json:"blockPointHash"`
	BlockPointSlot uint64 `json:"blockPointSlot"`
}

type PeerSnapshotLedgerPool struct {
	AccumulatedStake float64                        `json:"accumulatedStake"`
	RelativeStake    float64                        `json:"relativeStake"`
	Relays           []TopologyConfigP2PAccessPoint `json:"relays"`
}

func NewTopologyConfigFromFile(path string) (*TopologyConfig, error) {
	dataFile, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer dataFile.Close()
	ret, err := NewTopologyConfigFromReader(dataFile)
	if err != nil {
		return nil, err
	}
	if err := ret.loadPeerSnapshotFromDisk(filepath.Dir(path)); err != nil {
		return nil, err
	}
	return ret, nil
}

func NewTopologyConfigFromFS(
	fsys fs.FS,
	file string,
) (*TopologyConfig, error) {
	dataFile, err := fsys.Open(file)
	if err != nil {
		return nil, err
	}
	defer dataFile.Close()
	ret, err := NewTopologyConfigFromReader(dataFile)
	if err != nil {
		return nil, err
	}
	if err := ret.loadPeerSnapshotFromFS(fsys, path.Dir(file)); err != nil {
		return nil, err
	}
	return ret, nil
}

// maxTopologySize is the maximum allowed size for a topology config file
// (10 MB). This prevents unbounded memory allocation from untrusted readers.
const maxTopologySize = 10 * 1024 * 1024

func NewTopologyConfigFromReader(r io.Reader) (*TopologyConfig, error) {
	t := &TopologyConfig{}
	data, err := io.ReadAll(io.LimitReader(r, maxTopologySize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxTopologySize {
		return nil, fmt.Errorf(
			"topology file exceeds maximum size of %d bytes",
			maxTopologySize,
		)
	}
	if err := json.Unmarshal(data, t); err != nil {
		return nil, err
	}
	return t, nil
}

// maxPeerSnapshotSize is the maximum allowed size for peer-snapshot.json
// (10 MB). This matches topology config's defensive read limit.
const maxPeerSnapshotSize = 10 * 1024 * 1024

func NewPeerSnapshotConfigFromReader(
	r io.Reader,
) (*PeerSnapshotConfig, error) {
	s := &PeerSnapshotConfig{}
	data, err := io.ReadAll(io.LimitReader(r, maxPeerSnapshotSize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxPeerSnapshotSize {
		return nil, fmt.Errorf(
			"peer snapshot file exceeds maximum size of %d bytes",
			maxPeerSnapshotSize,
		)
	}
	if err := json.Unmarshal(data, s); err != nil {
		return nil, err
	}
	return s, nil
}

func NewPeerSnapshotConfigFromFile(
	file string,
) (*PeerSnapshotConfig, error) {
	dataFile, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer dataFile.Close()
	return NewPeerSnapshotConfigFromReader(dataFile)
}

func (t *TopologyConfig) loadPeerSnapshotFromDisk(baseDir string) error {
	if t == nil || t.PeerSnapshotFile == "" {
		return nil
	}
	snapshotPath := t.PeerSnapshotFile
	if !filepath.IsAbs(snapshotPath) {
		snapshotPath = filepath.Join(baseDir, snapshotPath)
	}
	snapshot, err := NewPeerSnapshotConfigFromFile(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to load peer snapshot file: %w", err)
	}
	t.PeerSnapshot = snapshot
	return nil
}

func (t *TopologyConfig) loadPeerSnapshotFromFS(
	fsys fs.FS,
	baseDir string,
) error {
	if t == nil || t.PeerSnapshotFile == "" {
		return nil
	}
	snapshotPath := t.PeerSnapshotFile
	if !path.IsAbs(snapshotPath) {
		snapshotPath = path.Join(baseDir, snapshotPath)
	}
	dataFile, err := fsys.Open(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to load peer snapshot file: %w", err)
	}
	defer dataFile.Close()
	snapshot, err := NewPeerSnapshotConfigFromReader(dataFile)
	if err != nil {
		return err
	}
	t.PeerSnapshot = snapshot
	return nil
}

func (t *TopologyConfig) WithoutBootstrapPeers() *TopologyConfig {
	if t == nil {
		return nil
	}
	ret := *t
	ret.BootstrapPeers = nil
	return &ret
}

func (s *PeerSnapshotConfig) RelayAccessPoints() []TopologyConfigP2PAccessPoint {
	if s == nil {
		return nil
	}
	ret := make(
		[]TopologyConfigP2PAccessPoint,
		0,
		s.peerSnapshotRelayCount(),
	)
	for _, pool := range s.BigLedgerPools {
		ret = append(ret, pool.Relays...)
	}
	for _, pool := range s.LedgerPools {
		ret = append(ret, pool.Relays...)
	}
	return ret
}

func (s *PeerSnapshotConfig) HasRelays() bool {
	return s != nil && s.peerSnapshotRelayCount() > 0
}

func (s *PeerSnapshotConfig) peerSnapshotRelayCount() int {
	if s == nil {
		return 0
	}
	count := 0
	for _, pool := range s.BigLedgerPools {
		count += len(pool.Relays)
	}
	for _, pool := range s.LedgerPools {
		count += len(pool.Relays)
	}
	return count
}
