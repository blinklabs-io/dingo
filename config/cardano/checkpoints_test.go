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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCardanoNodeConfigLoadsCheckpoints(t *testing.T) {
	t.Parallel()

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

const (
	testCheckpointHashA = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	testCheckpointHashB = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
)

func TestParseCheckpointsNormalizesHashCase(t *testing.T) {
	t.Parallel()

	data := []byte(
		`{"checkpoints":[{"blockNo":1,"hash":" ` +
			strings.ToUpper(testCheckpointHashA) +
			` "},{"blockNo":2,"hash":"` + testCheckpointHashB + `"}]}`,
	)
	cps, err := parseCheckpoints(data, "")
	require.NoError(t, err)
	require.Equal(t, testCheckpointHashA, cps[1])
	require.Equal(t, testCheckpointHashB, cps[2])
}

func TestParseCheckpointsRejectsInvalidHash(t *testing.T) {
	t.Parallel()

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
		{
			name:        "non-hex at full width",
			hash:        strings.Repeat("a", 63) + "g",
			errContains: "non-hex hash",
		},
		{
			name:        "truncated",
			hash:        testCheckpointHashA[:63],
			errContains: "must be 64 hex characters",
		},
		{
			name:        "odd length",
			hash:        "abc",
			errContains: "must be 64 hex characters",
		},
		{
			name:        "overlong",
			hash:        testCheckpointHashA + "a",
			errContains: "must be 64 hex characters",
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
	t.Parallel()

	data := []byte(`{"checkpoints":[{"blockNo":1,"hash":"` + testCheckpointHashA + `"}]}`)
	_, err := parseCheckpoints(data, "deadbeef")
	require.EqualError(
		t,
		err,
		"checkpoints file hash mismatch: expected deadbeef, computed "+blake2b256Hex(
			data,
		),
	)
}

func TestParseCheckpointsAcceptsCorrectHash(t *testing.T) {
	t.Parallel()

	data := []byte(`{"checkpoints":[{"blockNo":1,"hash":"` + testCheckpointHashA + `"}]}`)
	// blake2b256 of the exact bytes above.
	correct := blake2b256Hex(data)
	cps, err := parseCheckpoints(data, correct)
	require.NoError(t, err)
	require.Equal(t, testCheckpointHashA, cps[1])
}

func TestParseCheckpointsRejectsConflictingDuplicate(t *testing.T) {
	t.Parallel()

	data := []byte(
		`{"checkpoints":[{"blockNo":1,"hash":"` + testCheckpointHashA +
			`"},{"blockNo":1,"hash":"` + testCheckpointHashB + `"}]}`,
	)
	_, err := parseCheckpoints(data, "")
	require.Error(t, err)
}
