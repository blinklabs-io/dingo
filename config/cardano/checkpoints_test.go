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

package cardano

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCardanoNodeConfigLoadsCheckpoints(t *testing.T) {
	cfg, err := NewCardanoNodeConfigFromFile(
		filepath.Join(testDataDir, "config.json"),
	)
	require.NoError(t, err)
	cps := cfg.Checkpoints()
	require.NotNil(t, cps)
	require.Len(t, cps, 401)
	// First entry from testdata/checkpoints.json.
	require.Equal(
		t,
		"3e065fa887f09f5d1275f7d2c42b3a92d74e53535244aff5dd500f8968e3ee5e",
		cps[3788847],
	)
}

func TestParseCheckpointsNormalizesHashCase(t *testing.T) {
	data := []byte(
		`{"checkpoints":[{"blockNo":1,"hash":"AABB"},{"blockNo":2,"hash":"ccdd"}]}`,
	)
	cps, err := parseCheckpoints(data, "")
	require.NoError(t, err)
	require.Equal(t, "aabb", cps[1])
	require.Equal(t, "ccdd", cps[2])
}

func TestParseCheckpointsRejectsInvalidHash(t *testing.T) {
	tests := []struct {
		name        string
		hash        string
		errContains string
	}{
		{
			name:        "empty",
			hash:        " ",
			errContains: "empty hash",
		},
		{
			name:        "non-hex",
			hash:        "00xz",
			errContains: "non-hex hash",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := []byte(
				`{"checkpoints":[{"blockNo":1,"hash":"` + tt.hash + `"}]}`,
			)
			_, err := parseCheckpoints(data, "")
			require.ErrorContains(t, err, tt.errContains)
		})
	}
}

func TestParseCheckpointsRejectsWrongHash(t *testing.T) {
	data := []byte(`{"checkpoints":[{"blockNo":1,"hash":"aa"}]}`)
	_, err := parseCheckpoints(data, "deadbeef")
	require.EqualError(
		t,
		err,
		"checkpoints file hash mismatch: expected deadbeef, computed "+blake2b256Hex(data),
	)
}

func TestParseCheckpointsAcceptsCorrectHash(t *testing.T) {
	data := []byte(`{"checkpoints":[{"blockNo":1,"hash":"aa"}]}`)
	// blake2b256 of the exact bytes above.
	correct := blake2b256Hex(data)
	cps, err := parseCheckpoints(data, correct)
	require.NoError(t, err)
	require.Equal(t, "aa", cps[1])
}

func TestParseCheckpointsRejectsConflictingDuplicate(t *testing.T) {
	data := []byte(
		`{"checkpoints":[{"blockNo":1,"hash":"aa"},{"blockNo":1,"hash":"bb"}]}`,
	)
	_, err := parseCheckpoints(data, "")
	require.Error(t, err)
}
