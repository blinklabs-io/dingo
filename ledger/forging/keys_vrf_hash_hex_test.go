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

package forging

import (
	"encoding/hex"
	"strings"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestValidateAgainstLedgerVRFMismatchPrintsBothHashesInSingleHex pins both
// VRF key hashes in the startup mismatch error to their 64-character hex form.
//
// The message exists to be read as a comparison, but its two operands had
// different types: the registered hash arrives from the ledger view as a plain
// [32]byte, while the loaded key is hashed to an lcommon.Blake2b256, which
// implements fmt.Stringer with a hex String method. fmt routes the x verb
// through String for such operands, so %x hex-encoded that hex string and the
// loaded hash printed at 128 characters beside a 64-character registered hash.
// An operator checking whether they had started the node with the wrong VRF
// key was shown two ids that cannot be compared, in the one message whose only
// purpose is comparing them.
func TestValidateAgainstLedgerVRFMismatchPrintsBothHashesInSingleHex(
	t *testing.T,
) {
	pc := newCredsForLedger(t)
	registeredHash := lcommon.Blake2b256Hash(bytes32(0xDD))
	view := &fakeLedgerView{
		registered: true,
		regVRFHash: registeredHash,
	}

	_, _, err := pc.ValidateAgainstLedger(view)
	require.Error(t, err)

	registeredHex := hex.EncodeToString(registeredHash[:])
	loadedHex := lcommon.Blake2b256Hash(pc.vrfVKey).String()
	require.Len(t, registeredHex, 2*lcommon.Blake2b256Size)
	require.Len(t, loadedHex, 2*lcommon.Blake2b256Size)
	require.NotEqual(t, registeredHex, loadedHex)

	msg := err.Error()
	require.Contains(t, msg, "pool registration has "+registeredHex)
	require.Contains(t, msg, "loaded VRF key hashes to "+loadedHex)
	require.NotContains(
		t,
		msg,
		hex.EncodeToString([]byte(loadedHex)),
		"loaded VRF key hash must not be hex-encoded twice",
	)

	// Both hashes must be the same width, or the message cannot be read as
	// the comparison it is written as.
	for _, field := range []string{
		"pool registration has ",
		"loaded VRF key hashes to ",
	} {
		idx := strings.Index(msg, field)
		require.GreaterOrEqual(t, idx, 0)
		rest := msg[idx+len(field):]
		if space := strings.IndexAny(rest, " "); space >= 0 {
			rest = rest[:space]
		}
		require.Len(
			t,
			rest,
			2*lcommon.Blake2b256Size,
			"hash after %q must be a single-hex-encoded 32-byte hash",
			field,
		)
	}
}
