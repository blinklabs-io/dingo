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
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestVoteManagerValidateVotingKeyErrorsNamePoolInSingleHex pins the pool id in
// the ValidateVotingKey rejections to its 56-character hex form.
//
// lcommon.PoolKeyHash is an alias for Blake2b224, which implements fmt.Stringer
// with a hex String method, and fmt routes the x verb through String for such
// operands. Formatting the value itself with %x therefore hex-encoded its hex
// string, so an operator whose voting key was rejected could not match the
// logged pool id against the id they registered.
func TestVoteManagerValidateVotingKeyErrorsNamePoolInSingleHex(t *testing.T) {
	fixture := newManagerFixture(t)
	member := fixture.members[3]

	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	registeredHex := hex.EncodeToString(member.PoolKeyHash)
	require.Len(t, registeredHex, 2*lcommon.Blake2b224Size)

	wrongKey, err := ParseVoteSigningKey(fmt.Sprintf("%064x", 999))
	require.NoError(t, err)

	// Key mismatch for a pool that does resolve.
	err = fixture.mgr.ValidateVotingKey(poolKeyHash, wrongKey)
	require.Error(t, err)
	require.Contains(t, err.Error(), "public key for pool "+registeredHex)
	require.NotContains(
		t,
		err.Error(),
		hex.EncodeToString([]byte(registeredHex)),
		"pool id must not be hex-encoded twice",
	)

	// Pool with no resolvable key at all.
	var missingPool lcommon.PoolKeyHash
	missingPool[0] = 0xff
	missingHex := hex.EncodeToString(missingPool[:])

	err = fixture.mgr.ValidateVotingKey(missingPool, wrongKey)
	require.Error(t, err)
	require.Contains(t, err.Error(), "for pool "+missingHex)
	require.NotContains(
		t,
		err.Error(),
		hex.EncodeToString([]byte(missingHex)),
		"pool id must not be hex-encoded twice",
	)
}
