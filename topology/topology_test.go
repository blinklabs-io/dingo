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
    "blockPointHash": "abc",
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
    "blockPointHash": "def",
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
