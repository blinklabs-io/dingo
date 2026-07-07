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

package leios

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseVoteSigningKey(t *testing.T) {
	key, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 42))
	require.NoError(t, err)
	require.NotNil(t, key)
	pubBytes := key.PublicKeyBytes()
	assert.Len(t, pubBytes, VotePublicKeySize)
	assert.False(t, key.PublicKey().IsInfinity())
}

func TestParseVoteSigningKeyRejectsZero(t *testing.T) {
	_, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 0))
	assert.ErrorIs(t, err, ErrInvalidSigningKey)
}

func TestParseVoteSigningKeyRejectsAboveModulus(t *testing.T) {
	// The scalar field modulus itself is out of range (no silent
	// reduction)
	modulusHex := hex.EncodeToString(fr.Modulus().Bytes())
	_, err := ParseVoteSigningKey(modulusHex)
	assert.ErrorIs(t, err, ErrInvalidSigningKey)
}

func TestParseVoteSigningKeyRejectsMalformed(t *testing.T) {
	for _, input := range []string{
		"",
		"zz",
		"0102",                     // too short
		fmt.Sprintf("%066x", 1234), // too long
	} {
		_, err := ParseVoteSigningKey(input)
		assert.ErrorIs(t, err, ErrInvalidSigningKey, "input=%q", input)
	}
}

func TestLoadVoteSigningKeyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vote.skey")
	// Trailing whitespace must be tolerated
	content := fmt.Sprintf("%064x\n", 42)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	key, err := LoadVoteSigningKeyFile(path)
	require.NoError(t, err)
	expected, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 42))
	require.NoError(t, err)
	assert.Equal(t, expected.PublicKeyBytes(), key.PublicKeyBytes())
}

func TestLoadVoteSigningKeyFileRejectsLoosePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("file permission checks are not enforced on windows")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "vote.skey")
	require.NoError(
		t,
		os.WriteFile(path, fmt.Appendf(nil, "%064x", 42), 0o644),
	)
	_, err := LoadVoteSigningKeyFile(path)
	assert.Error(t, err)
}

func TestLoadVoteSigningKeyFileMissing(t *testing.T) {
	_, err := LoadVoteSigningKeyFile(
		filepath.Join(t.TempDir(), "does-not-exist"),
	)
	assert.Error(t, err)
}

func TestParseVoterPublicKey(t *testing.T) {
	key, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 42))
	require.NoError(t, err)
	pubHex := hex.EncodeToString(key.PublicKeyBytes())
	pub, err := ParseVoterPublicKey(pubHex)
	require.NoError(t, err)
	assert.True(t, pub.Equal(key.PublicKey()))
}

func TestParseVoterPublicKeyRejectsMalformed(t *testing.T) {
	for _, input := range []string{
		"",
		"zz",
		"0102", // too short
	} {
		_, err := ParseVoterPublicKey(input)
		assert.ErrorIs(t, err, ErrInvalidPublicKey, "input=%q", input)
	}
	// Point at infinity (compressed G2: 0xc0 then zeros)
	infinity := make([]byte, VotePublicKeySize)
	infinity[0] = 0xc0
	_, err := ParseVoterPublicKey(hex.EncodeToString(infinity))
	assert.ErrorIs(t, err, ErrInvalidPublicKey)
	// Corrupted point bytes
	garbage := make([]byte, VotePublicKeySize)
	for i := range garbage {
		garbage[i] = 0xff
	}
	_, err = ParseVoterPublicKey(hex.EncodeToString(garbage))
	assert.ErrorIs(t, err, ErrInvalidPublicKey)
}

func TestNewVoterRegistry(t *testing.T) {
	key1, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 11))
	require.NoError(t, err)
	key2, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 22))
	require.NoError(t, err)
	registry, err := NewVoterRegistry(map[string]string{
		testPoolHash(1): hex.EncodeToString(key1.PublicKeyBytes()),
		testPoolHash(2): hex.EncodeToString(key2.PublicKeyBytes()),
	})
	require.NoError(t, err)
	require.Equal(t, 2, registry.Size())

	poolHash1, err := hex.DecodeString(testPoolHash(1))
	require.NoError(t, err)
	pub, ok := registry.PublicKeyFor(poolHash1)
	require.True(t, ok)
	assert.True(t, pub.Equal(key1.PublicKey()))

	unknown := make([]byte, 28)
	unknown[0] = 99
	_, ok = registry.PublicKeyFor(unknown)
	assert.False(t, ok)
}

func TestNewVoterRegistryEmpty(t *testing.T) {
	registry, err := NewVoterRegistry(nil)
	require.NoError(t, err)
	assert.Equal(t, 0, registry.Size())
	_, ok := registry.PublicKeyFor(make([]byte, 28))
	assert.False(t, ok)
}

func TestNewVoterRegistryRejectsInvalidEntries(t *testing.T) {
	key, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 11))
	require.NoError(t, err)
	pubHex := hex.EncodeToString(key.PublicKeyBytes())

	// Invalid public key
	_, err = NewVoterRegistry(map[string]string{
		testPoolHash(1): "zz",
	})
	assert.Error(t, err)

	// Invalid pool key hash
	_, err = NewVoterRegistry(map[string]string{
		"not-hex": pubHex,
	})
	assert.Error(t, err)
}

func TestNewVoterRegistryNormalizesPoolHashCase(t *testing.T) {
	key, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 11))
	require.NoError(t, err)
	poolHashHex := "ABCDEF" + testPoolHash(0)[6:]
	registry, err := NewVoterRegistry(map[string]string{
		poolHashHex: hex.EncodeToString(key.PublicKeyBytes()),
	})
	require.NoError(t, err)
	poolHash, err := hex.DecodeString(poolHashHex)
	require.NoError(t, err)
	_, ok := registry.PublicKeyFor(poolHash)
	assert.True(t, ok)
}

func TestNewVoterRegistryRejectsWrongLengthPoolHash(t *testing.T) {
	key, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 11))
	require.NoError(t, err)
	// Valid hex, but not a 28-byte pool key hash
	_, err = NewVoterRegistry(map[string]string{
		"aabbccdd": hex.EncodeToString(key.PublicKeyBytes()),
	})
	assert.Error(t, err)
}

func TestNewVoterRegistryRejectsDuplicateNormalizedEntries(t *testing.T) {
	key, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 11))
	require.NoError(t, err)
	pubHex := hex.EncodeToString(key.PublicKeyBytes())
	lower := "abcdef" + testPoolHash(0)[6:]
	upper := "ABCDEF" + testPoolHash(0)[6:]
	// Distinct map keys that normalize to the same pool key hash must
	// be rejected instead of silently picking one entry.
	_, err = NewVoterRegistry(map[string]string{
		lower: pubHex,
		upper: pubHex,
	})
	assert.Error(t, err)
}

func TestLoadVoteSigningKeyFileRejectsOversized(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vote.skey")
	content := fmt.Sprintf("%064x", 42) + strings.Repeat(" ", 2048)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	_, err := LoadVoteSigningKeyFile(path)
	assert.Error(t, err)
}
