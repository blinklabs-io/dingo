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
	"io"
	"os"
)

// TopologyConfig represents a cardano-node topology config
type TopologyConfig struct {
	LocalRoots         []TopologyConfigP2PLocalRoot     `json:"localRoots"`
	PublicRoots        []TopologyConfigP2PPublicRoot    `json:"publicRoots"`
	BootstrapPeers     []TopologyConfigP2PBootstrapPeer `json:"bootstrapPeers"`
	UseLedgerAfterSlot int64                            `json:"useLedgerAfterSlot"`
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

func NewTopologyConfigFromFile(path string) (*TopologyConfig, error) {
	dataFile, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return NewTopologyConfigFromReader(dataFile)
}

func NewTopologyConfigFromReader(r io.Reader) (*TopologyConfig, error) {
	t := &TopologyConfig{}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, t); err != nil {
		return nil, err
	}
	return t, nil
}
