//go:build devnet

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

package devnet

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStakeKeyToCredentialHashesKey verifies StakeKeyToCredential produces
// a Tag-0 (key-hash) credential whose Bytes are the blake2b-224 digest of
// the raw stake verification key.
//
// Known vector: blake2b-224 of 32 zero bytes.
func TestStakeKeyToCredentialHashesKey(t *testing.T) {
	vkey := make([]byte, 32) // all zeros
	cred := StakeKeyToCredential(vkey)
	require.Equal(t, uint64(0), cred.Tag, "key-hash credential tag must be 0")
	// blake2b-224 (28-byte) digest of 32 zero bytes, computed independently
	// via `go run` (golang.org/x/crypto/blake2b) and cross-checked with
	// Python's hashlib.blake2b(digest_size=28).
	want := "f9dca21a6c826ec8acb4cf395cbc24351937bfe6560b2683ab8b415f"
	require.Equal(t, want, hex.EncodeToString(cred.Bytes.Bytes()))
}
