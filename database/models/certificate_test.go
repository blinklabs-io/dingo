// Copyright 2025 Blink Labs Software
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

package models

import (
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCertificateType_Value(t *testing.T) {
	tests := []struct {
		name     string
		ct       CertificateType
		expected driver.Value
	}{
		{
			name:     "StakeRegistration",
			ct:       CertificateTypeStakeRegistration,
			expected: int64(0),
		},
		{
			name:     "StakeDeregistration",
			ct:       CertificateTypeStakeDeregistration,
			expected: int64(1),
		},
		{
			name:     "Zero value",
			ct:       0,
			expected: int64(0),
		},
		{
			name:     "Large value",
			ct:       999,
			expected: int64(999),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test value receiver
			val, err := tt.ct.Value()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, val)

			// Test pointer receiver also works
			ctPtr := &tt.ct
			valPtr, err := ctPtr.Value()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, valPtr)
		})
	}
}

func TestCertificateType_Scan(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected CertificateType
		wantErr  bool
	}{
		{
			name:     "int64",
			input:    int64(5),
			expected: 5,
			wantErr:  false,
		},
		{
			name:     "int",
			input:    int(10),
			expected: 10,
			wantErr:  false,
		},
		{
			name:     "uint",
			input:    uint(15),
			expected: 15,
			wantErr:  false,
		},
		{
			name:     "uint64",
			input:    uint64(20),
			expected: 20,
			wantErr:  false,
		},
		{
			name:     "[]byte",
			input:    []byte("25"),
			expected: 25,
			wantErr:  false,
		},
		{
			name:     "string",
			input:    "30",
			expected: 30,
			wantErr:  false,
		},
		{
			name:     "nil",
			input:    nil,
			expected: 0,
			wantErr:  false,
		},
		{
			name:     "invalid type",
			input:    "invalid",
			expected: 0,
			wantErr:  true,
		},
		{
			name:     "negative value",
			input:    int64(-1),
			expected: 0,
			wantErr:  true,
		},
		{
			name:     "too large value",
			input:    int64(1001),
			expected: 0,
			wantErr:  true,
		},
		{
			name:     "uint overflow",
			input:    uint(1<<63 + 1), // Larger than max int64
			expected: 0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ct CertificateType
			err := ct.Scan(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, ct)
			}
		})
	}
}

func TestCertificateType_ImplementsInterfaces(t *testing.T) {
	var ct CertificateType
	var ctPtr *CertificateType

	// Test that both CertificateType and *CertificateType implement driver.Valuer
	_, ok := interface{}(ct).(driver.Valuer)
	assert.True(t, ok, "CertificateType should implement driver.Valuer")

	_, ok = interface{}(ctPtr).(driver.Valuer)
	assert.True(t, ok, "*CertificateType should implement driver.Valuer")

	// Test that *CertificateType implements sql.Scanner
	_, ok = interface{}(ctPtr).(interface{ Scan(interface{}) error })
	assert.True(t, ok, "*CertificateType should implement sql.Scanner")
}
