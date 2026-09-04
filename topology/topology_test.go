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

package topology_test

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/topology"
	"github.com/stretchr/testify/require"
)

type topologyTestDefinition struct {
	jsonData       string
	expectedObject *topology.TopologyConfig
}

var topologyTests = []topologyTestDefinition{
	{
		jsonData: `
{
  "localRoots": [
    {
      "accessPoints": [],
      "advertise": false,
      "valency": 1
    }
  ],
  "publicRoots": [
    {
      "accessPoints": [
        {
          "address": "backbone.cardano.iog.io",
          "port": 3001
        }
      ],
      "advertise": false
    },
    {
      "accessPoints": [
        {
          "address": "backbone.mainnet.emurgornd.com",
          "port": 3001
        }
      ],
      "advertise": false
    }
  ],
  "useLedgerAfterSlot": 99532743
}
`,
		expectedObject: &topology.TopologyConfig{
			LocalRoots: []topology.TopologyConfigP2PLocalRoot{
				{
					AccessPoints: []topology.TopologyConfigP2PAccessPoint{},
					Advertise:    false,
					Valency:      1,
				},
			},
			PublicRoots: []topology.TopologyConfigP2PPublicRoot{
				{
					AccessPoints: []topology.TopologyConfigP2PAccessPoint{
						{
							Address: "backbone.cardano.iog.io",
							Port:    3001,
						},
					},
					Advertise: false,
				},
				{
					AccessPoints: []topology.TopologyConfigP2PAccessPoint{
						{
							Address: "backbone.mainnet.emurgornd.com",
							Port:    3001,
						},
					},
					Advertise: false,
				},
			},
			UseLedgerAfterSlot: 99532743,
		},
	},
	{
		jsonData: `
{
  "bootstrapPeers": [
    {
      "address": "backbone.cardano.iog.io",
      "port": 3001
    },
    {
      "address": "backbone.mainnet.emurgornd.com",
      "port": 3001
    },
    {
      "address": "backbone.mainnet.cardanofoundation.org",
      "port": 3001
    }
  ],
  "localRoots": [
    {
      "accessPoints": [],
      "advertise": false,
      "trustable": false,
      "valency": 1
    }
  ],
  "publicRoots": [
    {
      "accessPoints": [],
      "advertise": false
    }
  ],
  "useLedgerAfterSlot": 128908821
}
`,
		expectedObject: &topology.TopologyConfig{
			LocalRoots: []topology.TopologyConfigP2PLocalRoot{
				{
					AccessPoints: []topology.TopologyConfigP2PAccessPoint{},
					Advertise:    false,
					Trustable:    false,
					Valency:      1,
				},
			},
			PublicRoots: []topology.TopologyConfigP2PPublicRoot{
				{
					AccessPoints: []topology.TopologyConfigP2PAccessPoint{},
					Advertise:    false,
				},
			},
			BootstrapPeers: []topology.TopologyConfigP2PBootstrapPeer{
				{
					Address: "backbone.cardano.iog.io",
					Port:    3001,
				},
				{
					Address: "backbone.mainnet.emurgornd.com",
					Port:    3001,
				},
				{
					Address: "backbone.mainnet.cardanofoundation.org",
					Port:    3001,
				},
			},
			UseLedgerAfterSlot: 128908821,
		},
	},
}

func TestParseTopologyConfig(t *testing.T) {
	for _, test := range topologyTests {
		topology, err := topology.NewTopologyConfigFromReader(
			strings.NewReader(test.jsonData),
		)
		if err != nil {
			t.Fatalf("failed to load TopologyConfig from JSON data: %s", err)
		}
		if !reflect.DeepEqual(topology, test.expectedObject) {
			t.Fatalf(
				"did not get expected object\n  got:\n    %#v\n  wanted:\n    %#v",
				topology,
				test.expectedObject,
			)
		}
	}
}

func TestNewTopologyConfigFromFile_ClosesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "topology.json")
	err := os.WriteFile(
		path,
		[]byte(topologyTests[0].jsonData),
		0o600,
	)
	require.NoError(t, err)

	cfg, err := topology.NewTopologyConfigFromFile(path)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(
		t,
		int64(99532743),
		cfg.UseLedgerAfterSlot,
	)
}

func TestNewTopologyConfigFromFile_LoadsPeerSnapshot(t *testing.T) {
	dir := t.TempDir()
	topologyPath := filepath.Join(dir, "topology.json")
	snapshotPath := filepath.Join(dir, "peer-snapshot.json")

	require.NoError(
		t,
		os.WriteFile(
			topologyPath,
			[]byte(`{"peerSnapshotFile":"peer-snapshot.json"}`),
			0o600,
		),
	)
	require.NoError(
		t,
		os.WriteFile(
			snapshotPath,
			[]byte(`{
  "NetworkMagic": 1,
  "NodeToClientVersion": 23,
  "Point": {
    "blockPointHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "blockPointSlot": 42
  },
  "bigLedgerPools": [
    {
      "relativeStake": 0.5,
      "accumulatedStake": 0.5,
      "relays": [
        {"address": "relay.example.com", "port": 3001}
      ]
    }
  ]
}`),
			0o600,
		),
	)

	cfg, err := topology.NewTopologyConfigFromFile(topologyPath)
	require.NoError(t, err)
	require.Equal(t, "peer-snapshot.json", cfg.PeerSnapshotFile)
	require.NotNil(t, cfg.PeerSnapshot)
	require.Equal(t, uint64(42), cfg.PeerSnapshot.Point.BlockPointSlot)
	require.Equal(
		t,
		[]topology.TopologyConfigP2PAccessPoint{
			{Address: "relay.example.com", Port: 3001},
		},
		cfg.PeerSnapshot.RelayAccessPoints(),
	)
	require.NoError(t, cfg.PeerSnapshot.Validate(1))
}

func TestNewTopologyConfigFromFS_LoadsPeerSnapshot(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "preview"), 0o700))
	require.NoError(
		t,
		os.WriteFile(
			filepath.Join(dir, "preview", "topology.json"),
			[]byte(`{"peerSnapshotFile":"peer-snapshot.json"}`),
			0o600,
		),
	)
	require.NoError(
		t,
		os.WriteFile(
			filepath.Join(dir, "preview", "peer-snapshot.json"),
			[]byte(`{
  "NetworkMagic": 2,
  "NodeToClientVersion": 23,
  "Point": {
    "blockPointHash": "def0000000000000000000000000000000000000000000000000000000000000",
    "blockPointSlot": 77
  },
  "bigLedgerPools": [
    {
      "relays": [
        {"address": "44.0.0.1", "port": 3001}
      ]
    }
  ]
}`),
			0o600,
		),
	)

	cfg, err := topology.NewTopologyConfigFromFS(
		os.DirFS(dir),
		"preview/topology.json",
	)
	require.NoError(t, err)
	require.NotNil(t, cfg.PeerSnapshot)
	require.Equal(t, uint64(77), cfg.PeerSnapshot.Point.BlockPointSlot)
	require.True(t, cfg.PeerSnapshot.HasRelays())
	require.NoError(t, cfg.PeerSnapshot.Validate(2))
}

func TestNewTopologyConfigFromFile_NotFound(t *testing.T) {
	_, err := topology.NewTopologyConfigFromFile(
		"/nonexistent/topology.json",
	)
	require.Error(t, err)
}

func TestNewTopologyConfigFromReader_OversizedInput(t *testing.T) {
	// maxTopologySize is 10 MB. Create input that exceeds it.
	const maxTopologySize = 10 * 1024 * 1024
	oversized := bytes.NewReader(make([]byte, maxTopologySize+1))
	_, err := topology.NewTopologyConfigFromReader(oversized)
	require.Error(t, err)
	require.Contains(
		t,
		err.Error(),
		"topology file exceeds maximum size",
	)
}

// TestPeerSnapshotConfigValidate verifies that snapshot identity, format,
// point, pool mode, and relay endpoints are accepted or rejected together.
func TestPeerSnapshotConfigValidate(t *testing.T) {
	valid := func() topology.PeerSnapshotConfig {
		return topology.PeerSnapshotConfig{
			NetworkMagic:        2,
			NodeToClientVersion: 23,
			Point: topology.PeerSnapshotPoint{
				BlockPointHash: "d6792f8031323804b7ac44a67747de78ed70fd307bb5ffddc5147844d9363b30",
				BlockPointSlot: 0,
			},
			BigLedgerPools: []topology.PeerSnapshotLedgerPool{{
				Relays: []topology.TopologyConfigP2PAccessPoint{{
					Address: "relay.example.com",
					Port:    1,
				}},
			}},
		}
	}

	tests := []struct {
		name    string
		mutate  func(*topology.PeerSnapshotConfig)
		wantErr string
	}{
		{name: "valid minimum TCP port and slot zero"},
		{
			name: "valid maximum TCP port and all-pool mode",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.AllLedgerPools = s.BigLedgerPools
				s.AllLedgerPools[0].Relays[0].Port = 65535
				s.BigLedgerPools = nil
			},
		},
		{
			name: "network mismatch",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.NetworkMagic = 1
			},
			wantErr: "does not match configured network magic",
		},
		{
			name: "missing network",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.NetworkMagic = 0
			},
			wantErr: "network magic must be specified",
		},
		{
			name: "unsupported legacy version",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.NodeToClientVersion = 2
			},
			wantErr: "unsupported node-to-client version 2",
		},
		{
			name: "missing version",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.NodeToClientVersion = 0
			},
			wantErr: "unsupported node-to-client version 0",
		},
		{
			name: "short point hash",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.Point.BlockPointHash = "00"
			},
			wantErr: "64 hexadecimal characters",
		},
		{
			name: "non-hex point hash",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.Point.BlockPointHash = strings.Repeat("z", 64)
			},
			wantErr: "is not hexadecimal",
		},
		{
			name: "mixed pool modes",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.AllLedgerPools = []topology.PeerSnapshotLedgerPool{{
					Relays: []topology.TopologyConfigP2PAccessPoint{{
						Address: "other.example.com",
						Port:    3001,
					}},
				}}
			},
			wantErr: "mutually exclusive",
		},
		{
			name: "no pool mode",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.BigLedgerPools = nil
			},
			wantErr: "contains no ledger pools",
		},
		{
			name: "pool without relays",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.BigLedgerPools[0].Relays = nil
			},
			wantErr: "contains no relays",
		},
		{
			name: "empty relay address",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.BigLedgerPools[0].Relays[0].Address = ""
			},
			wantErr: "address must not be empty",
		},
		{
			name: "unsupported SRV relay",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.BigLedgerPools[0].Relays[0].Port = 0
			},
			wantErr: "SRV relay mode is not supported",
		},
		{
			name: "port above TCP range",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.BigLedgerPools[0].Relays[0].Port = 65536
			},
			wantErr: "outside the TCP port range",
		},
		{
			name: "unspecified IPv4 relay",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.BigLedgerPools[0].Relays[0].Address = "0.0.0.0"
			},
			wantErr: "is not a relay endpoint",
		},
		{
			name: "unspecified IPv6 relay",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.BigLedgerPools[0].Relays[0].Address = "::"
			},
			wantErr: "is not a relay endpoint",
		},
		{
			name: "valid IPv4 relay",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.BigLedgerPools[0].Relays[0].Address = "192.0.2.1"
			},
		},
		{
			name: "malformed IPv4 is not a hostname",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.BigLedgerPools[0].Relays[0].Address = "192.0.2.999"
			},
			wantErr: "is not a valid DNS hostname",
		},
		{
			name: "valid IPv6 relay",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.BigLedgerPools[0].Relays[0].Address = "2001:db8::1"
			},
		},
		{
			name: "valid fully-qualified hostname",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.BigLedgerPools[0].Relays[0].Address = "relay.example.com."
			},
		},
		{
			name: "valid hostname with numeric label",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.BigLedgerPools[0].Relays[0].Address = "123.relay.example.com"
			},
		},
		{
			name: "hostname includes port",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.BigLedgerPools[0].Relays[0].Address = "relay.example:3001"
			},
			wantErr: "is not a valid DNS hostname",
		},
		{
			name: "bracketed IPv6",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.BigLedgerPools[0].Relays[0].Address = "[2001:db8::1]"
			},
			wantErr: "is not a valid DNS hostname",
		},
		{
			name: "whitespace hostname",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.BigLedgerPools[0].Relays[0].Address = " relay.example.com "
			},
			wantErr: "is not a valid DNS hostname",
		},
		{
			name: "hostname with empty label",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.BigLedgerPools[0].Relays[0].Address = "relay..example.com"
			},
			wantErr: "is not a valid DNS hostname",
		},
		{
			name: "hostname label exceeds boundary",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.BigLedgerPools[0].Relays[0].Address =
					strings.Repeat("a", 64) + ".example.com"
			},
			wantErr: "is not a valid DNS hostname",
		},
		{
			name: "hostname has invalid character",
			mutate: func(s *topology.PeerSnapshotConfig) {
				s.BigLedgerPools[0].Relays[0].Address = "relay_name.example.com"
			},
			wantErr: "is not a valid DNS hostname",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := valid()
			if tt.mutate != nil {
				tt.mutate(&snapshot)
			}
			err := snapshot.Validate(2)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
