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
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseVoteSigningKeyRejectsMalformed(t *testing.T) {
	t.Parallel()

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

// TestParseVoteSigningKeyStillRejectsUnreducedScalars guards the strict
// path: operator key files must never be silently reduced mod the scalar
// field modulus.
func TestParseVoteSigningKeyStillRejectsUnreducedScalars(t *testing.T) {
	t.Parallel()

	// An out-of-range scalar.
	padded := make([]byte, voteSigningKeySize)
	padded[0] = 0xf3
	_, err := ParseVoteSigningKey(hex.EncodeToString(padded))
	assert.ErrorIs(t, err, ErrInvalidSigningKey)
	// r itself, r+1, and zero
	_, err = ParseVoteSigningKey(fmt.Sprintf("%064x", fr.Modulus()))
	assert.ErrorIs(t, err, ErrInvalidSigningKey)
	_, err = ParseVoteSigningKey(
		fmt.Sprintf("%064x", new(big.Int).Add(fr.Modulus(), big.NewInt(1))),
	)
	assert.ErrorIs(t, err, ErrInvalidSigningKey)
	_, err = ParseVoteSigningKey(fmt.Sprintf("%064x", 0))
	assert.ErrorIs(t, err, ErrInvalidSigningKey)
}

func TestLoadVoteSigningKeyFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "vote.skey")
	// Trailing whitespace must be tolerated
	content := fmt.Sprintf("%064x\n", 42)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	testutil.RestrictFileToCurrentUser(t, path)
	key, err := LoadVoteSigningKeyFile(path)
	require.NoError(t, err)
	expected, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 42))
	require.NoError(t, err)
	assert.Equal(t, expected.PublicKeyBytes(), key.PublicKeyBytes())
}

func TestLoadVoteSigningKeyFileCardanoTextEnvelope(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "bls.skey")
	scalar := make([]byte, voteSigningKeySize)
	scalar[len(scalar)-1] = 42
	cborBytes := append([]byte{0x58, voteSigningKeySize}, scalar...)
	content := fmt.Sprintf(
		`{"type":"BLS12-381 Signing Key","description":"Leios BLS signing key","cborHex":"%s"}`,
		hex.EncodeToString(cborBytes),
	)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	testutil.RestrictFileToCurrentUser(t, path)

	key, err := LoadVoteSigningKeyFile(path)
	require.NoError(t, err)
	expected, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 42))
	require.NoError(t, err)
	assert.Equal(t, expected.PublicKeyBytes(), key.PublicKeyBytes())
}

func TestLoadVoteSigningKeyFileRejectsTrailingCbor(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "bls.skey")
	scalar := make([]byte, voteSigningKeySize)
	scalar[len(scalar)-1] = 42
	cborBytes := append([]byte{0x58, voteSigningKeySize}, scalar...)
	cborBytes = append(cborBytes, 0x00)
	content := fmt.Sprintf(
		`{"type":"BLS12-381 Signing Key","cborHex":"%s"}`,
		hex.EncodeToString(cborBytes),
	)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	testutil.RestrictFileToCurrentUser(t, path)

	_, err := LoadVoteSigningKeyFile(path)
	assert.ErrorIs(t, err, ErrInvalidSigningKey)
}

func TestLoadVoteSigningKeyFileRejectsUnsupportedEnvelope(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "node.skey")
	content := `{"type":"Node KES Signing Key","description":"","cborHex":"582001"}`
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	testutil.RestrictFileToCurrentUser(t, path)

	_, err := LoadVoteSigningKeyFile(path)
	assert.ErrorIs(t, err, ErrInvalidSigningKey)
}

func TestLoadVoteSigningKeyFileRejectsLoosePermissions(t *testing.T) {
	t.Parallel()

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

func TestParseVoterPublicKeyRejectsMalformed(t *testing.T) {
	t.Parallel()

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

func TestVoterRegistryZeroValueRegister(t *testing.T) {
	t.Parallel()

	key, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 11))
	require.NoError(t, err)
	var registry VoterRegistry
	poolHash, err := hex.DecodeString(testPoolHash(1))
	require.NoError(t, err)
	require.NoError(t, registry.RegisterPublicKey(poolHash, key.PublicKey()))
	pub, ok := registry.PublicKeyFor(poolHash)
	require.True(t, ok)
	assert.True(t, pub.Equal(key.PublicKey()))
}

func TestNewVoterRegistryRejectsInvalidEntries(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	key, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 11))
	require.NoError(t, err)
	// Valid hex, but not a 28-byte pool key hash
	_, err = NewVoterRegistry(map[string]string{
		"aabbccdd": hex.EncodeToString(key.PublicKeyBytes()),
	})
	assert.Error(t, err)
}

func TestNewVoterRegistryRejectsDuplicateNormalizedEntries(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "vote.skey")
	content := fmt.Sprintf("%064x", 42) + strings.Repeat(" ", 2048)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	testutil.RestrictFileToCurrentUser(t, path)
	_, err := LoadVoteSigningKeyFile(path)
	assert.Error(t, err)
}

func testVoteKeyEnvelope(t *testing.T, scalar uint64) string {
	t.Helper()
	raw := make([]byte, voteSigningKeySize)
	new(big.Int).SetUint64(scalar).FillBytes(raw)
	cborBytes := append([]byte{0x58, voteSigningKeySize}, raw...)
	return fmt.Sprintf(
		`{"type":"BLS12-381 Signing Key","description":"Leios BLS signing key","cborHex":"%s"}`,
		hex.EncodeToString(cborBytes),
	)
}

func testVoteKeyArray(t *testing.T, scalars ...uint64) string {
	t.Helper()
	envelopes := make([]string, 0, len(scalars))
	for _, scalar := range scalars {
		envelopes = append(envelopes, testVoteKeyEnvelope(t, scalar))
	}
	return "[" + strings.Join(envelopes, ",") + "]"
}

func requireKeysMatchScalars(
	t *testing.T,
	keys []*VoteSigningKey,
	scalars ...uint64,
) {
	t.Helper()
	require.Len(t, keys, len(scalars))
	for i, scalar := range scalars {
		expected, err := ParseVoteSigningKey(fmt.Sprintf("%064x", scalar))
		require.NoError(t, err)
		assert.Equal(
			t,
			expected.PublicKeyBytes(),
			keys[i].PublicKeyBytes(),
			"key %d keeps its file order",
			i,
		)
	}
}

func TestParseVoteSigningKeysFile(t *testing.T) {
	t.Parallel()

	t.Run("array of envelopes keeps file order", func(t *testing.T) {
		t.Parallel()
		keys, err := parseVoteSigningKeysFile(
			[]byte(testVoteKeyArray(t, 42, 7, 9)),
		)
		require.NoError(t, err)
		requireKeysMatchScalars(t, keys, 42, 7, 9)
	})
	t.Run("single envelope is one key", func(t *testing.T) {
		t.Parallel()
		keys, err := parseVoteSigningKeysFile(
			[]byte(testVoteKeyEnvelope(t, 42)),
		)
		require.NoError(t, err)
		requireKeysMatchScalars(t, keys, 42)
	})
	t.Run("one-element array equals a bare envelope", func(t *testing.T) {
		t.Parallel()
		keys, err := parseVoteSigningKeysFile(
			[]byte(testVoteKeyArray(t, 42)),
		)
		require.NoError(t, err)
		requireKeysMatchScalars(t, keys, 42)
	})
	t.Run("legacy raw hex scalar is one key", func(t *testing.T) {
		t.Parallel()
		keys, err := parseVoteSigningKeysFile(
			[]byte(fmt.Sprintf("%064x\n", 42)),
		)
		require.NoError(t, err)
		requireKeysMatchScalars(t, keys, 42)
	})
	t.Run("empty array holds no keys", func(t *testing.T) {
		t.Parallel()
		keys, err := parseVoteSigningKeysFile([]byte(" [ ] \n"))
		require.NoError(t, err)
		assert.Empty(t, keys)
	})
	t.Run("whitespace around the array is tolerated", func(t *testing.T) {
		t.Parallel()
		keys, err := parseVoteSigningKeysFile(
			[]byte("\n  " + testVoteKeyArray(t, 3, 4) + "\n"),
		)
		require.NoError(t, err)
		requireKeysMatchScalars(t, keys, 3, 4)
	})
	t.Run("one invalid element fails the whole file", func(t *testing.T) {
		t.Parallel()
		bad := `{"type":"Node KES Signing Key","description":"","cborHex":"582001"}`
		_, err := parseVoteSigningKeysFile([]byte(
			"[" + testVoteKeyEnvelope(t, 42) + "," + bad + "]",
		))
		require.ErrorIs(t, err, ErrInvalidSigningKey)
		assert.ErrorContains(t, err, "key 1")
	})
	t.Run(
		"a hex string is not an envelope inside an array",
		func(t *testing.T) {
			t.Parallel()
			_, err := parseVoteSigningKeysFile([]byte(
				fmt.Sprintf(`[%q]`, fmt.Sprintf("%064x", 42)),
			))
			assert.Error(t, err)
		},
	)
	t.Run("null is not an empty array", func(t *testing.T) {
		t.Parallel()
		_, err := parseVoteSigningKeysFile([]byte("null"))
		assert.Error(t, err)
	})
	t.Run("array element with trailing cbor is rejected", func(t *testing.T) {
		t.Parallel()
		raw := make([]byte, voteSigningKeySize)
		raw[len(raw)-1] = 42
		cborBytes := append([]byte{0x58, voteSigningKeySize}, raw...)
		cborBytes = append(cborBytes, 0x00)
		bad := fmt.Sprintf(
			`{"type":"BLS12-381 Signing Key","cborHex":"%s"}`,
			hex.EncodeToString(cborBytes),
		)
		_, err := parseVoteSigningKeysFile([]byte("[" + bad + "]"))
		assert.ErrorIs(t, err, ErrInvalidSigningKey)
	})
}

func TestLoadVoteSigningKeysFile(t *testing.T) {
	t.Parallel()

	write := func(t *testing.T, content string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "bls.skey")
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
		testutil.RestrictFileToCurrentUser(t, path)
		return path
	}

	t.Run("loads every key in the array", func(t *testing.T) {
		t.Parallel()
		keys, err := LoadVoteSigningKeysFile(
			write(t, testVoteKeyArray(t, 42, 43)),
		)
		require.NoError(t, err)
		requireKeysMatchScalars(t, keys, 42, 43)
	})
	t.Run("a bundle larger than one envelope fits", func(t *testing.T) {
		t.Parallel()
		scalars := make([]uint64, 0, 64)
		for i := range uint64(64) {
			scalars = append(scalars, i+1)
		}
		content := testVoteKeyArray(t, scalars...)
		require.Greater(t, len(content), voteSigningKeyFileMaxSize)
		keys, err := LoadVoteSigningKeysFile(write(t, content))
		require.NoError(t, err)
		requireKeysMatchScalars(t, keys, scalars...)
	})
	t.Run("legacy single key files still load", func(t *testing.T) {
		t.Parallel()
		keys, err := LoadVoteSigningKeysFile(
			write(t, fmt.Sprintf("%064x\n", 42)),
		)
		require.NoError(t, err)
		requireKeysMatchScalars(t, keys, 42)
	})
	t.Run("error names the file", func(t *testing.T) {
		t.Parallel()
		path := write(t, `[{"type":"Node KES Signing Key","cborHex":"582001"}]`)
		_, err := LoadVoteSigningKeysFile(path)
		require.ErrorIs(t, err, ErrInvalidSigningKey)
		assert.ErrorContains(t, err, path)
	})
	t.Run("a file beyond the bundle bound is refused", func(t *testing.T) {
		t.Parallel()
		_, err := LoadVoteSigningKeysFile(write(
			t,
			fmt.Sprintf(
				"%064x",
				42,
			)+strings.Repeat(
				" ",
				voteSigningKeysFileMaxSize,
			),
		))
		assert.ErrorContains(t, err, "exceeds")
	})
	t.Run("loose permissions are refused", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("file permission checks are not enforced on windows")
		}
		t.Parallel()
		path := filepath.Join(t.TempDir(), "bls.skey")
		require.NoError(
			t,
			os.WriteFile(path, []byte(testVoteKeyArray(t, 42, 43)), 0o644),
		)
		_, err := LoadVoteSigningKeysFile(path)
		assert.Error(t, err)
	})
}
