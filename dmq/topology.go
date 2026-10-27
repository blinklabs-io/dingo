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
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
)

// maxTopologyBytes bounds the topology file read into memory.
const maxTopologyBytes = 10 << 20

// maxPort is the highest valid TCP port.
const maxPort = 65535

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
	// Valency is how many peers to keep connections to. It is a target, not
	// a bound: it may exceed len(Peers), as in the topology the Mithril
	// documentation publishes.
	Valency uint
	// WarmValency is how many peers to keep warm. A file that leaves it out
	// gets Valency, so it is never below Valency.
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

// topologyFile is the part of a dmq-node topology file that is read. Fields
// dmq-node does not support (bootstrapPeers, peerSnapshotFile,
// useLedgerAfterSlot) are not declared, so they are never decoded or
// validated and may hold anything.
type topologyFile struct {
	LocalRoots  []topologyRoot `json:"localRoots"`
	PublicRoots []topologyRoot `json:"publicRoots"`
}

type topologyRoot struct {
	AccessPoints []topologyAccessPoint `json:"accessPoints"`
	Advertise    bool                  `json:"advertise"`
	Trustable    bool                  `json:"trustable"`
	Valency      *uint                 `json:"valency"`
	HotValency   *uint                 `json:"hotValency"`
	WarmValency  *uint                 `json:"warmValency"`
}

type topologyAccessPoint struct {
	Address string `json:"address"`
	Port    uint   `json:"port"`
}

// LoadTopology reads a dmq-node topology file. The format is cardano-node's
// P2P topology, except that bootstrapPeers, peerSnapshotFile and
// useLedgerAfterSlot are not supported: they are never decoded, so even a
// malformed one does not fail the load, and a peer snapshot is never read
// from disk. A configured bootstrapPeers or peerSnapshotFile is reported with
// a warning.
//
// The shared Cardano topology reader is not used, because it refuses a
// valency above the number of access points and the DMQ topology the Mithril
// documentation publishes has a local root with valency 2 and one access
// point. Here a valency is a target that may exceed the peers listed. The
// other rules hold: an access point has a non-empty host and a port from 1 to
// 65535, and a warmValency is not below the valency. Both local and public
// roots accept hotValency as a spelling of valency, and valency wins when
// both are present.
//
// Peer hosts are returned without surrounding whitespace or IPv6 brackets. A
// file that names no peers loads as an empty topology, because ledger peer
// discovery can supply peers without one; the caller decides whether that is
// worth a warning. A file that is not a JSON object, or that has two keys
// differing only in case, is an error.
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
	// A JSON null unmarshals into a nil map without error.
	if fields == nil {
		return nil, fmt.Errorf(
			"dmq: parse topology file %q: must be a JSON object", path,
		)
	}
	if err := rejectCaseDuplicateKeys(fields); err != nil {
		return nil, fmt.Errorf("dmq: parse topology file %q: %w", path, err)
	}
	warnIgnoredTopologyFields(logger, path, fields)
	// Go matches keys to struct fields without regard to case.
	var file topologyFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("dmq: parse topology file %q: %w", path, err)
	}
	t := &Topology{}
	for i, root := range file.LocalRoots {
		group, err := newRootGroup(root, fmt.Sprintf("localRoots[%d]", i))
		if err != nil {
			return nil, fmt.Errorf(
				"dmq: parse topology file %q: %w", path, err,
			)
		}
		group.Trustable = root.Trustable
		t.LocalRoots = append(t.LocalRoots, group)
	}
	for i, root := range file.PublicRoots {
		group, err := newRootGroup(root, fmt.Sprintf("publicRoots[%d]", i))
		if err != nil {
			return nil, fmt.Errorf(
				"dmq: parse topology file %q: %w", path, err,
			)
		}
		t.PublicRoots = append(t.PublicRoots, group)
	}
	return t, nil
}

// newRootGroup validates one root and converts it. field names the root in
// errors, for example "localRoots[0]".
func newRootGroup(root topologyRoot, field string) (RootGroup, error) {
	peers := make([]Peer, 0, len(root.AccessPoints))
	for i, ap := range root.AccessPoints {
		host := cleanHost(ap.Address)
		if host == "" {
			return RootGroup{}, fmt.Errorf(
				"%s.accessPoints[%d].address must not be empty", field, i,
			)
		}
		if ap.Port == 0 || ap.Port > maxPort {
			return RootGroup{}, fmt.Errorf(
				"%s.accessPoints[%d].port must be in range 1-%d",
				field, i, maxPort,
			)
		}
		peers = append(peers, Peer{Host: host, Port: ap.Port})
	}
	var valency uint
	switch {
	case root.Valency != nil:
		valency = *root.Valency
	case root.HotValency != nil:
		valency = *root.HotValency
	}
	warm := valency
	if root.WarmValency != nil {
		warm = *root.WarmValency
		if warm < valency {
			return RootGroup{}, fmt.Errorf(
				"%s.warmValency must be >= %s.valency", field, field,
			)
		}
	}
	return RootGroup{
		Peers:       peers,
		Valency:     valency,
		WarmValency: warm,
		Advertise:   root.Advertise,
	}, nil
}

// cleanHost trims whitespace and the brackets around an IPv6 literal, so a
// dial and Peer.Address, which brackets an IPv6 host itself, both see the bare
// host.
func cleanHost(host string) string {
	host = strings.TrimSpace(host)
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = strings.TrimSpace(host[1 : len(host)-1])
	}
	return host
}

// rejectCaseDuplicateKeys refuses an object that has two keys differing only
// in case. Go matches keys to struct fields without regard to case, so the
// decoder would otherwise pick one of them by position and silently drop the
// other.
func rejectCaseDuplicateKeys(fields map[string]json.RawMessage) error {
	seen := make(map[string]string, len(fields))
	for key := range fields {
		folded := strings.ToLower(key)
		if first, ok := seen[folded]; ok {
			first, second := min(first, key), max(first, key)
			return fmt.Errorf(
				"keys %q and %q differ only in case", first, second,
			)
		}
		seen[folded] = key
	}
	return nil
}

// warnIgnoredTopologyFields logs an unsupported field that carries a value
// worth noticing: a non-empty bootstrapPeers or a peerSnapshotFile.
// useLedgerAfterSlot is dropped silently.
func warnIgnoredTopologyFields(
	logger *slog.Logger,
	path string,
	fields map[string]json.RawMessage,
) {
	for key, value := range fields {
		switch strings.ToLower(key) {
		case "bootstrappeers":
			var peers []json.RawMessage
			if err := json.Unmarshal(value, &peers); err != nil ||
				len(peers) > 0 {
				logger.Warn(
					"ignoring bootstrapPeers in dmq topology: not supported",
					"file", path,
				)
			}
		case "peersnapshotfile":
			var name string
			if string(value) == "null" {
				continue
			}
			if err := json.Unmarshal(value, &name); err != nil ||
				name != "" {
				logger.Warn(
					"ignoring peerSnapshotFile in dmq topology: not supported",
					"file", path,
				)
			}
		}
	}
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
