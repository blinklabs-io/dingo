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

package dmq

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/topology"
)

// Peer is one DMQ peer's address.
type Peer struct {
	Host string
	Port uint
}

// Address returns the peer as a host:port string, bracketing IPv6 literals.
func (p Peer) Address() string {
	return net.JoinHostPort(p.Host, strconv.FormatUint(uint64(p.Port), 10))
}

// RootGroup is a set of peers that share valency and advertising settings.
type RootGroup struct {
	Peers []Peer
	// Valency is how many of Peers to keep connections to.
	Valency uint
	// WarmValency is how many of Peers to keep warm. It is never below
	// Valency.
	WarmValency uint
	// Trustable and Advertise are set from a local root only.
	Trustable bool
	Advertise bool
}

// Topology is the peer set a dmq-node topology file names. It is separate
// from the Cardano topology, so DMQ peer selection never reads or changes
// Cardano peer governance.
type Topology struct {
	LocalRoots  []RootGroup
	PublicRoots []RootGroup
}

// LoadTopology reads a dmq-node topology file. The format is cardano-node's
// P2P topology; bootstrapPeers and peerSnapshotFile are not supported by
// dmq-node, so they are ignored with a warning and a peer snapshot is never
// read from disk. useLedgerAfterSlot is likewise ignored: DMQ discovers
// ledger peers through its own configuration.
func LoadTopology(path string, logger *slog.Logger) (*Topology, error) {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("dmq: open topology file: %w", err)
	}
	defer f.Close()
	// Read the file here rather than through NewTopologyConfigFromFile,
	// which also loads a peerSnapshotFile relative to the topology file.
	cfg, err := topology.NewTopologyConfigFromReader(f)
	if err != nil {
		return nil, fmt.Errorf("dmq: parse topology file %q: %w", path, err)
	}
	if len(cfg.BootstrapPeers) > 0 {
		logger.Warn(
			"ignoring bootstrapPeers in dmq topology: not supported",
			"file", path,
		)
	}
	if cfg.PeerSnapshotFile != "" {
		logger.Warn(
			"ignoring peerSnapshotFile in dmq topology: not supported",
			"file", path,
		)
	}
	t := &Topology{}
	for _, root := range cfg.LocalRoots {
		t.LocalRoots = append(t.LocalRoots, RootGroup{
			Peers:       peersFromAccessPoints(root.AccessPoints),
			Valency:     root.Valency,
			WarmValency: root.WarmValency,
			Trustable:   root.Trustable,
			Advertise:   root.Advertise,
		})
	}
	for _, root := range cfg.PublicRoots {
		t.PublicRoots = append(t.PublicRoots, RootGroup{
			Peers:       peersFromAccessPoints(root.AccessPoints),
			Valency:     root.Valency,
			WarmValency: root.WarmValency,
			Advertise:   root.Advertise,
		})
	}
	return t, nil
}

func peersFromAccessPoints(
	points []topology.TopologyConfigP2PAccessPoint,
) []Peer {
	peers := make([]Peer, 0, len(points))
	for _, ap := range points {
		peers = append(peers, Peer{Host: ap.Address, Port: ap.Port})
	}
	return peers
}

// Peers returns every distinct peer, local roots first and each in file
// order. A peer named in several groups is listed once, and hostnames compare
// case-insensitively.
func (t *Topology) Peers() []Peer {
	seen := make(map[string]struct{})
	var peers []Peer
	for _, groups := range [][]RootGroup{t.LocalRoots, t.PublicRoots} {
		for _, group := range groups {
			for _, p := range group.Peers {
				key := strings.ToLower(p.Address())
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				peers = append(peers, p)
			}
		}
	}
	return peers
}
