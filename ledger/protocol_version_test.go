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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ledger

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetProtocolVersion_Shelley(t *testing.T) {
	pp := &shelley.ShelleyProtocolParameters{
		ProtocolMajor: 2,
		ProtocolMinor: 0,
	}
	pv, err := GetProtocolVersion(pp)
	require.NoError(t, err)
	assert.Equal(t, uint(2), pv.Major)
	assert.Equal(t, uint(0), pv.Minor)
}

func TestGetProtocolVersion_Allegra(t *testing.T) {
	// allegra.AllegraProtocolParameters is a type alias for
	// shelley.ShelleyProtocolParameters, so Allegra pparams
	// are handled by the Shelley case in the type switch.
	pp := &allegra.AllegraProtocolParameters{
		ProtocolMajor: 3,
		ProtocolMinor: 0,
	}
	pv, err := GetProtocolVersion(pp)
	require.NoError(t, err)
	assert.Equal(t, uint(3), pv.Major)
	assert.Equal(t, uint(0), pv.Minor)
}

func TestGetProtocolVersion_Mary(t *testing.T) {
	pp := &mary.MaryProtocolParameters{
		ProtocolMajor: 4,
		ProtocolMinor: 0,
	}
	pv, err := GetProtocolVersion(pp)
	require.NoError(t, err)
	assert.Equal(t, uint(4), pv.Major)
	assert.Equal(t, uint(0), pv.Minor)
}

func TestGetProtocolVersion_Alonzo(t *testing.T) {
	pp := &alonzo.AlonzoProtocolParameters{
		ProtocolMajor: 6,
		ProtocolMinor: 0,
	}
	pv, err := GetProtocolVersion(pp)
	require.NoError(t, err)
	assert.Equal(t, uint(6), pv.Major)
	assert.Equal(t, uint(0), pv.Minor)
}

func TestGetProtocolVersion_Babbage(t *testing.T) {
	pp := &babbage.BabbageProtocolParameters{
		ProtocolMajor: 8,
		ProtocolMinor: 0,
	}
	pv, err := GetProtocolVersion(pp)
	require.NoError(t, err)
	assert.Equal(t, uint(8), pv.Major)
	assert.Equal(t, uint(0), pv.Minor)
}

func TestGetProtocolVersion_Conway(t *testing.T) {
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 9,
			Minor: 0,
		},
	}
	pv, err := GetProtocolVersion(pp)
	require.NoError(t, err)
	assert.Equal(t, uint(9), pv.Major)
	assert.Equal(t, uint(0), pv.Minor)
}

func TestGetProtocolVersion_Nil(t *testing.T) {
	_, err := GetProtocolVersion(nil)
	require.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"protocol parameters are nil",
	)
}

func TestEraForVersion(t *testing.T) {
	tests := []struct {
		name          string
		majorVersion  uint
		expectedEraId uint
		expectedOk    bool
	}{
		{
			name:          "Byron version 0",
			majorVersion:  0,
			expectedEraId: 0,
			expectedOk:    true,
		},
		{
			name:          "Byron version 1",
			majorVersion:  1,
			expectedEraId: 0,
			expectedOk:    true,
		},
		{
			name:          "Shelley version 2",
			majorVersion:  2,
			expectedEraId: 1,
			expectedOk:    true,
		},
		{
			name:          "Allegra version 3",
			majorVersion:  3,
			expectedEraId: 2,
			expectedOk:    true,
		},
		{
			name:          "Mary version 4",
			majorVersion:  4,
			expectedEraId: 3,
			expectedOk:    true,
		},
		{
			name:          "Alonzo version 5",
			majorVersion:  5,
			expectedEraId: 4,
			expectedOk:    true,
		},
		{
			name:          "Alonzo version 6",
			majorVersion:  6,
			expectedEraId: 4,
			expectedOk:    true,
		},
		{
			name:          "Babbage version 7",
			majorVersion:  7,
			expectedEraId: 5,
			expectedOk:    true,
		},
		{
			name:          "Babbage version 8",
			majorVersion:  8,
			expectedEraId: 5,
			expectedOk:    true,
		},
		{
			name:          "Conway version 9",
			majorVersion:  9,
			expectedEraId: 6,
			expectedOk:    true,
		},
		{
			name:          "Conway version 10",
			majorVersion:  10,
			expectedEraId: 6,
			expectedOk:    true,
		},
		{
			name:         "Unknown version 99",
			majorVersion: 99,
			expectedOk:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eraId, ok := EraForVersion(tt.majorVersion)
			assert.Equal(t, tt.expectedOk, ok)
			if tt.expectedOk {
				assert.Equal(
					t,
					tt.expectedEraId,
					eraId,
				)
			}
		})
	}
}

func TestIsHardForkTransition(t *testing.T) {
	tests := []struct {
		name     string
		old      ProtocolVersion
		new      ProtocolVersion
		expected bool
	}{
		{
			name:     "same version no transition",
			old:      ProtocolVersion{Major: 8, Minor: 0},
			new:      ProtocolVersion{Major: 8, Minor: 0},
			expected: false,
		},
		{
			name:     "minor version change only",
			old:      ProtocolVersion{Major: 8, Minor: 0},
			new:      ProtocolVersion{Major: 8, Minor: 1},
			expected: false,
		},
		{
			name:     "Babbage to Conway transition",
			old:      ProtocolVersion{Major: 8, Minor: 0},
			new:      ProtocolVersion{Major: 9, Minor: 0},
			expected: true,
		},
		{
			name:     "Alonzo to Babbage transition",
			old:      ProtocolVersion{Major: 6, Minor: 0},
			new:      ProtocolVersion{Major: 7, Minor: 0},
			expected: true,
		},
		{
			name:     "intra-era Alonzo version bump",
			old:      ProtocolVersion{Major: 5, Minor: 0},
			new:      ProtocolVersion{Major: 6, Minor: 0},
			expected: false,
		},
		{
			name:     "intra-era Babbage version bump",
			old:      ProtocolVersion{Major: 7, Minor: 0},
			new:      ProtocolVersion{Major: 8, Minor: 0},
			expected: false,
		},
		{
			name:     "intra-era Conway version bump",
			old:      ProtocolVersion{Major: 9, Minor: 0},
			new:      ProtocolVersion{Major: 10, Minor: 0},
			expected: false,
		},
		{
			name:     "Shelley to Allegra transition",
			old:      ProtocolVersion{Major: 2, Minor: 0},
			new:      ProtocolVersion{Major: 3, Minor: 0},
			expected: true,
		},
		{
			name:     "unknown old version",
			old:      ProtocolVersion{Major: 99, Minor: 0},
			new:      ProtocolVersion{Major: 9, Minor: 0},
			expected: false,
		},
		{
			name:     "unknown new version",
			old:      ProtocolVersion{Major: 9, Minor: 0},
			new:      ProtocolVersion{Major: 99, Minor: 0},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsHardForkTransition(tt.old, tt.new)
			assert.Equal(t, tt.expected, result)
		})
	}
}
