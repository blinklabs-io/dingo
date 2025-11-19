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
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/btcsuite/btcd/btcutil/bech32"
)

func TestAccount_String(t *testing.T) {
	tests := []struct {
		name         string
		wantErrMsg   string
		expectedHRP  string
		stakingKey   []byte
		isTestnet    bool
		wantErr      bool
		validateAddr bool
	}{
		{
			name: "valid staking key - 28 bytes - mainnet",
			// Sample 28-byte staking key hash
			stakingKey: hexDecode(
				"e1cbb80db89e292d1175088b0ce5cd27857d5f1e53e41a754d566a6b",
			),
			isTestnet:    false,
			wantErr:      false,
			validateAddr: true,
			expectedHRP:  "stake",
		},
		{
			name: "valid staking key - 28 bytes - testnet",
			// Sample 28-byte staking key hash
			stakingKey: hexDecode(
				"e1cbb80db89e292d1175088b0ce5cd27857d5f1e53e41a754d566a6b",
			),
			isTestnet:    true,
			wantErr:      false,
			validateAddr: true,
			expectedHRP:  "stake_test",
		},
		{
			name: "valid staking key - 32 bytes - mainnet",
			// Sample 32-byte staking key hash
			stakingKey: hexDecode(
				"e1cbb80db89e292d1175088b0ce5cd27857d5f1e53e41a754d566a6b12345678",
			),
			isTestnet:    false,
			wantErr:      false,
			validateAddr: true,
			expectedHRP:  "stake",
		},
		{
			name: "valid staking key - 32 bytes - testnet",
			// Sample 32-byte staking key hash
			stakingKey: hexDecode(
				"e1cbb80db89e292d1175088b0ce5cd27857d5f1e53e41a754d566a6b12345678",
			),
			isTestnet:    true,
			wantErr:      false,
			validateAddr: true,
			expectedHRP:  "stake_test",
		},
		{
			name:       "empty staking key",
			stakingKey: []byte{},
			wantErr:    true,
			wantErrMsg: "staking key is empty",
		},
		{
			name:       "nil staking key",
			stakingKey: nil,
			wantErr:    true,
			wantErrMsg: "staking key is empty",
		},
		{
			name: "single byte staking key - mainnet",
			// Edge case with minimal data
			stakingKey:   []byte{0x01},
			isTestnet:    false,
			wantErr:      false,
			validateAddr: true,
			expectedHRP:  "stake",
		},
		{
			name: "single byte staking key - testnet",
			// Edge case with minimal data
			stakingKey:   []byte{0x01},
			isTestnet:    true,
			wantErr:      false,
			validateAddr: true,
			expectedHRP:  "stake_test",
		},
		{
			name: "all zeros staking key - mainnet",
			// Edge case with all zeros
			stakingKey:   make([]byte, 28),
			isTestnet:    false,
			wantErr:      false,
			validateAddr: true,
			expectedHRP:  "stake",
		},
		{
			name: "all zeros staking key - testnet",
			// Edge case with all zeros
			stakingKey:   make([]byte, 28),
			isTestnet:    true,
			wantErr:      false,
			validateAddr: true,
			expectedHRP:  "stake_test",
		},
		{
			name: "all ones staking key - mainnet",
			// Edge case with all 0xFF
			stakingKey: []byte{
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff,
			},
			isTestnet:    false,
			wantErr:      false,
			validateAddr: true,
			expectedHRP:  "stake",
		},
		{
			name: "all ones staking key - testnet",
			// Edge case with all 0xFF
			stakingKey: []byte{
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff,
			},
			isTestnet:    true,
			wantErr:      false,
			validateAddr: true,
			expectedHRP:  "stake_test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Account{
				StakingKey: tt.stakingKey,
			}
			got, err := a.StringWithNetwork(tt.isTestnet)
			if (err != nil) != tt.wantErr {
				t.Errorf(
					"Account.StringWithNetwork() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)
				return
			}
			if tt.wantErr {
				if err == nil {
					t.Errorf(
						"Account.StringWithNetwork() expected error but got none",
					)
					return
				}
				if tt.wantErrMsg != "" &&
					!strings.Contains(err.Error(), tt.wantErrMsg) {
					t.Errorf(
						"Account.StringWithNetwork() error = %v, want error containing %q",
						err,
						tt.wantErrMsg,
					)
				}
				return
			}
			if err != nil {
				t.Errorf(
					"Account.StringWithNetwork() unexpected error: %v",
					err,
				)
				return
			}
			if got == "" {
				t.Errorf(
					"Account.StringWithNetwork() returned empty string for valid input",
				)
				return
			}
			if tt.validateAddr {
				// Validate the returned bech32 address
				hrp, data, err := bech32.Decode(got)
				if err != nil {
					t.Errorf(
						"Account.StringWithNetwork() returned invalid bech32 address: %v",
						err,
					)
					return
				}
				if hrp != tt.expectedHRP {
					t.Errorf(
						"Account.StringWithNetwork() HRP = %v, want %v",
						hrp,
						tt.expectedHRP,
					)
				}
				// Convert back and verify we get the original data
				converted, err := bech32.ConvertBits(data, 5, 8, false)
				if err != nil {
					t.Errorf("Failed to convert address back to bytes: %v", err)
					return
				}
				if !bytes.Equal(converted, tt.stakingKey) {
					t.Errorf(
						"Account.StringWithNetwork() round-trip failed: got %x, want %x",
						converted,
						tt.stakingKey,
					)
				}
			}
		})
	}
}

func TestAccount_String_Idempotent(t *testing.T) {
	// Test that calling String() multiple times returns the same result
	stakingKey := hexDecode(
		"e1cbb80db89e292d1175088b0ce5cd27857d5f1e53e41a754d566a6b",
	)
	a := &Account{
		StakingKey: stakingKey,
	}

	result1, err := a.String()
	if err != nil {
		t.Fatalf("Account.String() first call error: %v", err)
	}

	result2, err := a.String()
	if err != nil {
		t.Fatalf("Account.String() second call error: %v", err)
	}

	if result1 != result2 {
		t.Errorf(
			"Account.String() not idempotent: first=%s, second=%s",
			result1,
			result2,
		)
	}
}

func TestAccount_String_DifferentKeys(t *testing.T) {
	// Test that different staking keys produce different addresses
	key1 := hexDecode(
		"e1cbb80db89e292d1175088b0ce5cd27857d5f1e53e41a754d566a6b",
	)
	key2 := hexDecode(
		"a1cbb80db89e292d1175088b0ce5cd27857d5f1e53e41a754d566a6b",
	)

	a1 := &Account{StakingKey: key1}
	a2 := &Account{StakingKey: key2}

	addr1, err := a1.String()
	if err != nil {
		t.Fatalf("Account.String() for key1 error: %v", err)
	}

	addr2, err := a2.String()
	if err != nil {
		t.Fatalf("Account.String() for key2 error: %v", err)
	}

	if addr1 == addr2 {
		t.Errorf(
			"Account.String() produced same address for different keys: %s",
			addr1,
		)
	}
}

func TestAccount_String_Bech32Format(t *testing.T) {
	// Test that the output follows bech32 format conventions
	stakingKey := hexDecode(
		"e1cbb80db89e292d1175088b0ce5cd27857d5f1e53e41a754d566a6b",
	)
	a := &Account{
		StakingKey: stakingKey,
	}

	addr, err := a.String()
	if err != nil {
		t.Fatalf("Account.String() error: %v", err)
	}

	// Check that address starts with "stake"
	if !strings.HasPrefix(addr, "stake") {
		t.Errorf(
			"Account.String() address doesn't start with 'stake': %s",
			addr,
		)
	}

	// Check that address contains the separator '1'
	if !strings.Contains(addr, "1") {
		t.Errorf(
			"Account.String() address doesn't contain separator '1': %s",
			addr,
		)
	}

	// Check that address only contains valid bech32 characters (lowercase alphanumeric, no '1', 'b', 'i', 'o')
	separatorIdx := strings.Index(addr, "1")
	datapart := addr[separatorIdx+1:]
	validChars := "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
	for _, c := range datapart {
		if !strings.ContainsRune(validChars, c) {
			t.Errorf(
				"Account.String() address contains invalid bech32 character: %c in %s",
				c,
				addr,
			)
		}
	}
}

// Helper functions

func hexDecode(s string) []byte {
	data, err := hex.DecodeString(s)
	if err != nil {
		panic("invalid hex in test data: " + err.Error())
	}
	return data
}
