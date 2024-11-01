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
	"reflect"
	"strings"
	"testing"

	"github.com/blinklabs-io/node/topology"
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
