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
	assert.Equal(t, uint(1), topo.PublicRoots[0].Valency)
	assert.False(t, topo.PublicRoots[0].Trustable)
	assert.Equal(
		t,
		"[2001:db8::1]:3141",
		topo.PublicRoots[0].Peers[0].Address(),
	)
}

func TestLoadTopologyIgnoresUnsupportedFields(t *testing.T) {
	t.Parallel()
	// The snapshot file does not exist: a loader that followed it would fail.
	path := writeTopology(t, `{
		"bootstrapPeers": [{"address": "boot.example.com", "port": 3001}],
		"peerSnapshotFile": "missing-snapshot.json",
		"useLedgerAfterSlot": 100,
		"localRoots": [{
			"accessPoints": [{"address": "10.0.0.1", "port": 3141}],
			"valency": 1
		}],
		"publicRoots": []
	}`)
	var logs bytes.Buffer
	topo, err := LoadTopology(
		path,
		slog.New(slog.NewTextHandler(&logs, nil)),
	)
	require.NoError(t, err)
	assert.Equal(t, []Peer{{Host: "10.0.0.1", Port: 3141}}, topo.Peers())
	assert.Contains(t, logs.String(), "ignoring bootstrapPeers")
	assert.Contains(t, logs.String(), "ignoring peerSnapshotFile")
}

func TestLoadTopologyPeersDeduplicates(t *testing.T) {
	t.Parallel()
	path := writeTopology(t, `{
		"localRoots": [{
			"accessPoints": [
				{"address": "Relay.Example.com", "port": 3141},
				{"address": "10.0.0.1", "port": 3141}
			],
			"valency": 1
		}],
		"publicRoots": [{
			"accessPoints": [
				{"address": "relay.example.com", "port": 3141},
				{"address": "relay.example.com", "port": 3142}
			],
			"valency": 1
		}]
	}`)
	topo, err := LoadTopology(path, nil)
	require.NoError(t, err)
	assert.Equal(t, []Peer{
		{Host: "Relay.Example.com", Port: 3141},
		{Host: "10.0.0.1", Port: 3141},
		{Host: "relay.example.com", Port: 3142},
	}, topo.Peers())
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
			name: "malformed json",
			path: func(t *testing.T) string {
				return writeTopology(t, `{"localRoots": [`)
			},
			wantErr: "parse topology file",
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
			name: "valency above the access points",
			path: func(t *testing.T) string {
				return writeTopology(t, `{"publicRoots": [{
					"accessPoints": [{"address": "10.0.0.1", "port": 3141}],
					"valency": 2
				}]}`)
			},
			wantErr: "publicRoots[0].valency",
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
