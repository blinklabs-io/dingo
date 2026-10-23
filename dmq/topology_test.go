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
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeTopology(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dmq-topology.json")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestLoadTopologyGroups(t *testing.T) {
	t.Parallel()
	path := writeTopology(t, `{
		"localRoots": [{
			"accessPoints": [
				{"address": "10.0.0.1", "port": 3141},
				{"address": "relay.example.com", "port": 3142}
			],
			"advertise": true,
			"trustable": true,
			"hotValency": 2,
			"warmValency": 2
		}],
		"publicRoots": [{
			"accessPoints": [{"address": "2001:db8::1", "port": 3141}],
			"advertise": false,
			"valency": 1
		}]
	}`)
	topo, err := LoadTopology(path, nil)
	require.NoError(t, err)
	require.Len(t, topo.LocalRoots, 1)
	assert.Equal(t, RootGroup{
		Peers: []Peer{
			{Host: "10.0.0.1", Port: 3141},
			{Host: "relay.example.com", Port: 3142},
		},
		Valency:     2,
		WarmValency: 2,
		Trustable:   true,
		Advertise:   true,
	}, topo.LocalRoots[0])
	require.Len(t, topo.PublicRoots, 1)
	// A public root that names no warmValency gets its valency.
	assert.Equal(t, RootGroup{
		Peers:       []Peer{{Host: "2001:db8::1", Port: 3141}},
		Valency:     1,
		WarmValency: 1,
	}, topo.PublicRoots[0])
	assert.Equal(
		t,
		"[2001:db8::1]:3141",
		topo.PublicRoots[0].Peers[0].Address(),
	)
}

// The topology the Mithril documentation publishes for a DMQ node: one access
// point under a local root with valency 2, an access point of its own valency
// 1, a null peerSnapshotFile and a public root with no access points.
const mithrilDocTopology = `{
	"bootstrapPeers": [],
	"localRoots": [{
		"accessPoints": [
			{"address": "dmq.example.com", "port": 6161, "valency": 1}
		],
		"advertise": true,
		"trustable": false,
		"valency": 2
	}],
	"peerSnapshotFile": null,
	"publicRoots": [{
		"accessPoints": [],
		"advertise": false
	}]
}`

func TestLoadTopologyMithrilDocumentedFile(t *testing.T) {
	t.Parallel()
	topo, err := LoadTopology(writeTopology(t, mithrilDocTopology), nil)
	require.NoError(t, err)
	assert.Equal(t, []RootGroup{{
		Peers:       []Peer{{Host: "dmq.example.com", Port: 6161}},
		Valency:     2,
		WarmValency: 2,
		Advertise:   true,
	}}, topo.LocalRoots)
	assert.Equal(t, []RootGroup{{Peers: []Peer{}}}, topo.PublicRoots)
	assert.Equal(
		t,
		[]Peer{{Host: "dmq.example.com", Port: 6161}},
		topo.Peers(),
	)
}

// A valency is a target, not a bound on the access points, so a root may ask
// for more peers than it lists, in either group and for either valency.
func TestLoadTopologyValencyMayExceedAccessPoints(t *testing.T) {
	t.Parallel()
	topo, err := LoadTopology(writeTopology(t, `{
		"localRoots": [{
			"accessPoints": [{"address": "10.0.0.1", "port": 3141}],
			"hotValency": 3,
			"warmValency": 5
		}],
		"publicRoots": [{
			"accessPoints": [{"address": "10.0.0.2", "port": 3141}],
			"valency": 4
		}]
	}`), nil)
	require.NoError(t, err)
	assert.Equal(t, uint(3), topo.LocalRoots[0].Valency)
	assert.Equal(t, uint(5), topo.LocalRoots[0].WarmValency)
	assert.Equal(t, uint(4), topo.PublicRoots[0].Valency)
	assert.Equal(t, uint(4), topo.PublicRoots[0].WarmValency)
}

func TestLoadTopologyValencyWinsOverHotValency(t *testing.T) {
	t.Parallel()
	topo, err := LoadTopology(writeTopology(t, `{"localRoots": [{
		"accessPoints": [{"address": "10.0.0.1", "port": 3141}],
		"valency": 1,
		"hotValency": 2
	}]}`), nil)
	require.NoError(t, err)
	assert.Equal(t, uint(1), topo.LocalRoots[0].Valency)
}

func TestLoadTopologyPublicRootWarmValency(t *testing.T) {
	t.Parallel()
	topo, err := LoadTopology(writeTopology(t, `{"publicRoots": [{
		"accessPoints": [
			{"address": "10.0.0.1", "port": 3141},
			{"address": "10.0.0.2", "port": 3141},
			{"address": "10.0.0.3", "port": 3141}
		],
		"valency": 1,
		"warmValency": 3
	}]}`), nil)
	require.NoError(t, err)
	assert.Equal(t, uint(1), topo.PublicRoots[0].Valency)
	assert.Equal(t, uint(3), topo.PublicRoots[0].WarmValency)
}

func TestLoadTopologyPublicRootIsNeverTrustable(t *testing.T) {
	t.Parallel()
	topo, err := LoadTopology(writeTopology(t, `{"publicRoots": [{
		"accessPoints": [{"address": "10.0.0.1", "port": 3141}],
		"trustable": true,
		"valency": 1
	}]}`), nil)
	require.NoError(t, err)
	assert.False(t, topo.PublicRoots[0].Trustable)
}

func TestLoadTopologyIgnoresUnsupportedFields(t *testing.T) {
	t.Parallel()
	const roots = `"localRoots": [{
		"accessPoints": [{"address": "10.0.0.1", "port": 3141}],
		"valency": 1
	}]`
	tests := []struct {
		name     string
		fields   string
		wantWarn []string
		noWarn   []string
	}{
		{
			name: "valid unsupported fields",
			fields: `"bootstrapPeers": [{"address": "boot.example.com", "port": 3001}],
				"peerSnapshotFile": "missing-snapshot.json",
				"useLedgerAfterSlot": 100,`,
			wantWarn: []string{
				"ignoring bootstrapPeers", "ignoring peerSnapshotFile",
			},
		},
		{
			name: "malformed bootstrap peers",
			fields: `"bootstrapPeers": [
				{"address": "", "port": 0},
				{"address": "boot.example.com", "port": 70000}
			],`,
			wantWarn: []string{"ignoring bootstrapPeers"},
		},
		{
			name:     "bootstrap peers of the wrong type",
			fields:   `"bootstrapPeers": "not a list",`,
			wantWarn: []string{"ignoring bootstrapPeers"},
		},
		{
			name:     "key in another case",
			fields:   `"BootstrapPeers": [{"address": "", "port": 0}],`,
			wantWarn: []string{"ignoring bootstrapPeers"},
		},
		{
			name:   "negative useLedgerAfterSlot",
			fields: `"useLedgerAfterSlot": -1,`,
			noWarn: []string{"ignoring"},
		},
		{
			name:   "huge useLedgerAfterSlot",
			fields: `"useLedgerAfterSlot": 18446744073709551615,`,
			noWarn: []string{"ignoring"},
		},
		{
			name:   "non-numeric useLedgerAfterSlot",
			fields: `"useLedgerAfterSlot": "soon",`,
			noWarn: []string{"ignoring"},
		},
		{
			name: "empty bootstrap peers and snapshot file",
			fields: `"bootstrapPeers": [],
				"peerSnapshotFile": "",`,
			noWarn: []string{"ignoring"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			path := writeTopology(t, "{"+tt.fields+roots+"}")
			var logs bytes.Buffer
			topo, err := LoadTopology(
				path,
				slog.New(slog.NewTextHandler(&logs, nil)),
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				[]Peer{{Host: "10.0.0.1", Port: 3141}},
				topo.Peers(),
			)
			for _, want := range tt.wantWarn {
				assert.Contains(t, logs.String(), want)
			}
			for _, not := range tt.noWarn {
				assert.NotContains(t, logs.String(), not)
			}
		})
	}
}

func TestLoadTopologyPeersDeduplicates(t *testing.T) {
	t.Parallel()
	path := writeTopology(t, `{
		"localRoots": [{
			"accessPoints": [
				{"address": "Relay.Example.com", "port": 3141},
				{"address": "10.0.0.1", "port": 3141},
				{"address": "2001:DB8::1", "port": 3141}
			],
			"valency": 1
		}],
		"publicRoots": [{
			"accessPoints": [
				{"address": "relay.example.com", "port": 3141},
				{"address": "relay.example.com.", "port": 3141},
				{"address": "relay.example.com", "port": 3142},
				{"address": "2001:db8:0::1", "port": 3141},
				{"address": "::ffff:10.0.0.1", "port": 3141}
			],
			"valency": 1
		}]
	}`)
	topo, err := LoadTopology(path, nil)
	require.NoError(t, err)
	assert.Equal(t, []Peer{
		{Host: "Relay.Example.com", Port: 3141},
		{Host: "10.0.0.1", Port: 3141},
		{Host: "2001:DB8::1", Port: 3141},
		{Host: "relay.example.com", Port: 3142},
	}, topo.Peers())
}

func TestLoadTopologyCleansHosts(t *testing.T) {
	t.Parallel()
	topo, err := LoadTopology(writeTopology(t, `{"localRoots": [{
		"accessPoints": [
			{"address": "[2001:db8::1]", "port": 3141},
			{"address": "  relay.example.com  ", "port": 3142},
			{"address": " [ ::1 ] ", "port": 3143}
		],
		"valency": 1
	}]}`), nil)
	require.NoError(t, err)
	peers := topo.Peers()
	assert.Equal(t, []Peer{
		{Host: "2001:db8::1", Port: 3141},
		{Host: "relay.example.com", Port: 3142},
		{Host: "::1", Port: 3143},
	}, peers)
	assert.Equal(t, "[2001:db8::1]:3141", peers[0].Address())
	assert.Equal(t, "relay.example.com:3142", peers[1].Address())
	assert.Equal(t, "[::1]:3143", peers[2].Address())
}

func TestLoadTopologyBracketedAndBareHostAreOnePeer(t *testing.T) {
	t.Parallel()
	topo, err := LoadTopology(writeTopology(t, `{"localRoots": [{
		"accessPoints": [
			{"address": "[::1]", "port": 3141},
			{"address": "::1", "port": 3141}
		],
		"valency": 1
	}]}`), nil)
	require.NoError(t, err)
	assert.Equal(t, []Peer{{Host: "::1", Port: 3141}}, topo.Peers())
}

func TestLoadTopologyEmpty(t *testing.T) {
	t.Parallel()
	topo, err := LoadTopology(writeTopology(t, `{}`), nil)
	require.NoError(t, err)
	assert.Empty(t, topo.Peers())
}

func TestLoadTopologyErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		path    func(*testing.T) string
		wantErr string
	}{
		{
			name: "missing file",
			path: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "absent.json")
			},
			wantErr: "open topology file",
		},
		{
			name:    "directory",
			path:    func(t *testing.T) string { return t.TempDir() },
			wantErr: "read topology file",
		},
		{
			name: "empty file",
			path: func(t *testing.T) string {
				return writeTopology(t, ``)
			},
			wantErr: "parse topology file",
		},
		{
			name: "malformed json",
			path: func(t *testing.T) string {
				return writeTopology(t, `{"localRoots": [`)
			},
			wantErr: "parse topology file",
		},
		{
			name: "not an object",
			path: func(t *testing.T) string {
				return writeTopology(t, `[]`)
			},
			wantErr: "parse topology file",
		},
		{
			name: "null",
			path: func(t *testing.T) string {
				return writeTopology(t, `null`)
			},
			wantErr: "must be a JSON object",
		},
		{
			name: "keys differing only in case",
			path: func(t *testing.T) string {
				return writeTopology(t, `{
					"localRoots": [{
						"accessPoints": [{"address": "10.0.0.1", "port": 3141}],
						"valency": 1
					}],
					"LocalRoots": []
				}`)
			},
			wantErr: `"LocalRoots" and "localRoots" differ only in case`,
		},
		{
			name: "port out of range",
			path: func(t *testing.T) string {
				return writeTopology(t, `{"localRoots": [{
					"accessPoints": [{"address": "10.0.0.1", "port": 70000}],
					"valency": 1
				}]}`)
			},
			wantErr: "localRoots[0].accessPoints[0].port",
		},
		{
			name: "port zero",
			path: func(t *testing.T) string {
				return writeTopology(t, `{"localRoots": [{
					"accessPoints": [{"address": "10.0.0.1", "port": 0}],
					"valency": 1
				}]}`)
			},
			wantErr: "localRoots[0].accessPoints[0].port",
		},
		{
			name: "empty address",
			path: func(t *testing.T) string {
				return writeTopology(t, `{"publicRoots": [{
					"accessPoints": [{"address": "", "port": 3141}],
					"valency": 1
				}]}`)
			},
			wantErr: "publicRoots[0].accessPoints[0].address",
		},
		{
			name: "warm valency below the valency",
			path: func(t *testing.T) string {
				return writeTopology(t, `{"localRoots": [{
					"accessPoints": [{"address": "10.0.0.1", "port": 3141}],
					"valency": 2,
					"warmValency": 1
				}]}`)
			},
			wantErr: "localRoots[0].warmValency must be >= localRoots[0].valency",
		},
		{
			name: "public root warm valency below the valency",
			path: func(t *testing.T) string {
				return writeTopology(t, `{"publicRoots": [{
					"accessPoints": [{"address": "10.0.0.1", "port": 3141}],
					"valency": 1,
					"warmValency": 0
				}]}`)
			},
			wantErr: "publicRoots[0].warmValency must be >= publicRoots[0].valency",
		},
		{
			name: "address of only brackets",
			path: func(t *testing.T) string {
				return writeTopology(t, `{"localRoots": [{
					"accessPoints": [{"address": " [] ", "port": 3141}],
					"valency": 1
				}]}`)
			},
			wantErr: "localRoots[0].accessPoints[0].address must not be empty",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := LoadTopology(tt.path(t), nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestLoadTopologyRejectsOversizedFile(t *testing.T) {
	t.Parallel()
	path := writeTopology(t, `{"padding": "`+string(
		bytes.Repeat([]byte("a"), maxTopologyBytes),
	)+`"}`)
	_, err := LoadTopology(path, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum size")
}
