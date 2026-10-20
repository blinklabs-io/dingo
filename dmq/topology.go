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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/topology"
)

// maxTopologyBytes bounds the topology file read into memory.
const maxTopologyBytes = 10 << 20

// Peer is one DMQ peer's address.
type Peer struct {
	Host string
	Port uint
}

// Address returns the peer as a host:port string, bracketing IPv6 literals.
func (p Peer) Address() string {
	return net.JoinHostPort(p.Host, strconv.FormatUint(uint64(p.Port), 10))
}

// key identifies a peer for deduplication: the host is lowercased, stripped
// of a trailing dot, and an IP literal is written in its canonical form, so
// "2001:DB8::1" and "2001:db8:0::1" name the same peer.
func (p Peer) key() string {
	host := strings.ToLower(strings.TrimSpace(p.Host))
	host = strings.TrimSuffix(host, ".")
	literal := strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	if addr, err := netip.ParseAddr(literal); err == nil {
		host = addr.Unmap().String()
	}
	return net.JoinHostPort(host, strconv.FormatUint(uint64(p.Port), 10))
}

// RootGroup is a set of peers that share valency and advertising settings.
type RootGroup struct {
	Peers []Peer
	// Valency is how many of Peers to keep connections to.
	Valency uint
	// WarmValency is how many of Peers to keep warm. A file that leaves it
	// out gets Valency, so it is never below Valency.
	WarmValency uint
	// Trustable is set from a local root only; a public root cannot be
	// trusted.
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

// ignoredTopologyFields are cardano-node topology keys that dmq-node does not
// support. They are removed before the file is parsed, so their contents are
// never validated or followed.
var ignoredTopologyFields = []string{
	"bootstrapPeers",
	"peerSnapshotFile",
	"useLedgerAfterSlot",
}

// LoadTopology reads a dmq-node topology file. The format is cardano-node's
// P2P topology, except that the fields in ignoredTopologyFields are not
// supported: they are dropped before parsing, so even a malformed one does
// not fail the load, and a peer snapshot is never read from disk. Configured
// bootstrapPeers and peerSnapshotFile are reported with a warning.
//
// Public roots read valency and warmValency only, as the Cardano topology
// parser does; hotValency is understood for local roots alone.
//
// The file is read here, whatever configuration validation already checked of
// it, so a file that changes or disappears after validation still fails the
// load with a clear error.
func LoadTopology(path string, logger *slog.Logger) (*Topology, error) {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("dmq: open topology file: %w", err)
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxTopologyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("dmq: read topology file %q: %w", path, err)
	}
	if len(data) > maxTopologyBytes {
		return nil, fmt.Errorf(
			"dmq: topology file %q exceeds maximum size of %d bytes",
			path, maxTopologyBytes,
		)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, fmt.Errorf("dmq: parse topology file %q: %w", path, err)
	}
	// Go matches JSON keys to struct fields without regard to case, so the
	// ignored keys are matched the same way.
	for key, value := range fields {
		for _, ignored := range ignoredTopologyFields {
			if !strings.EqualFold(key, ignored) {
				continue
			}
			warnIgnoredTopologyField(logger, path, ignored, value)
			delete(fields, key)
		}
	}
	stripped, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("dmq: parse topology file %q: %w", path, err)
	}
	cfg, err := topology.NewTopologyConfigFromReader(
		bytes.NewReader(stripped),
	)
	if err != nil {
		return nil, fmt.Errorf("dmq: parse topology file %q: %w", path, err)
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
		warm := root.WarmValency
		if warm == 0 {
			warm = root.Valency
		}
		t.PublicRoots = append(t.PublicRoots, RootGroup{
			Peers:       peersFromAccessPoints(root.AccessPoints),
			Valency:     root.Valency,
			WarmValency: warm,
			Advertise:   root.Advertise,
		})
	}
	return t, nil
}

// warnIgnoredTopologyField logs an ignored field that carries a value worth
// noticing: a non-empty bootstrapPeers or a peerSnapshotFile. A
// useLedgerAfterSlot is dropped silently.
func warnIgnoredTopologyField(
	logger *slog.Logger,
	path, field string,
	value json.RawMessage,
) {
	switch field {
	case "bootstrapPeers":
		var peers []json.RawMessage
		if err := json.Unmarshal(value, &peers); err != nil || len(peers) > 0 {
			logger.Warn(
				"ignoring bootstrapPeers in dmq topology: not supported",
				"file", path,
			)
		}
	case "peerSnapshotFile":
		var name string
		if err := json.Unmarshal(value, &name); err != nil || name != "" {
			logger.Warn(
				"ignoring peerSnapshotFile in dmq topology: not supported",
				"file", path,
			)
		}
	}
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
// order. A peer named in several groups is listed once. Hostnames compare
// without regard to case or a trailing dot, and an IP literal by its
// canonical form; a hostname is never resolved, so two names for one address
// stay distinct.
func (t *Topology) Peers() []Peer {
	seen := make(map[string]struct{})
	var peers []Peer
	for _, groups := range [][]RootGroup{t.LocalRoots, t.PublicRoots} {
		for _, group := range groups {
			for _, p := range group.Peers {
				key := p.key()
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
