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

package utxoset_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// A block's issuer need not be a registered pool: a TPraos genesis delegate
// issues the overlay slots and is never in the stake distribution, and a node
// replaying history applies blocks of pools that have since retired. The
// ledger records the issuer's op-cert counter all the same, so the set takes
// the counter without any pool row and keeps following the tip.
func TestSetFollowsAnOpCertOfAnIssuerWithNoPool(t *testing.T) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{})
	issuer := bytes.Repeat([]byte{0x6e}, 28)
	for slot := uint64(1); slot <= 2; slot++ {
		tip := slotPoint(slot)
		require.NoError(t, set.ApplyLedgerChanges(&tip, ledger.LedgerChanges{
			OpCerts: []ledger.OpCertChange{{
				Slot: slot,
				State: ledger.OpCertState{
					Pool: issuer, Sequence: slot, Slot: slot,
					LastSequence: slot,
				},
			}},
		}))
	}
	st, err := set.OpCertState(issuer)
	require.NoError(t, err)
	require.NotNil(t, st)
	require.Equal(t, uint64(2), st.Sequence)
	require.Equal(t, uint64(2), st.Slot)
}
