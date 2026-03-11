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

package mithril

import (
	"encoding/hex"
	"testing"

	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/stretchr/testify/require"
)

func testBLSG1Hex(t *testing.T) string {
	t.Helper()
	_, _, g1, _ := bls12381.Generators()
	return hex.EncodeToString(g1.Marshal())
}

func testBLSG2Hex(t *testing.T) string {
	t.Helper()
	_, _, _, g2 := bls12381.Generators()
	return hex.EncodeToString(g2.Marshal())
}

func TestDecodeBLSG1Point(t *testing.T) {
	point, err := decodeBLSG1Point(testBLSG1Hex(t))
	require.NoError(t, err)
	require.NotNil(t, point)
}

func TestDecodeBLSG2Point(t *testing.T) {
	point, err := decodeBLSG2Point(testBLSG2Hex(t))
	require.NoError(t, err)
	require.NotNil(t, point)
}

func TestDecodeBLSG1PointErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"odd hex", "abc"},
		{"not hex", "zzzz"},
		{
			"wrong length",
			"deadbeef",
		},
		{
			"all zeros (infinity)",
			hex.EncodeToString(make([]byte, 48)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeBLSG1Point(tt.input)
			require.Error(t, err)
		})
	}
}

func TestDecodeBLSG2PointErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"odd hex", "abc"},
		{"not hex", "zzzz"},
		{
			"wrong length",
			"deadbeef",
		},
		{
			"all zeros (infinity)",
			hex.EncodeToString(make([]byte, 96)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeBLSG2Point(tt.input)
			require.Error(t, err)
		})
	}
}
