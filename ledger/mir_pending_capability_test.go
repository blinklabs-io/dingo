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

package ledger

import (
	"encoding/hex"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// mirNegativeDeltaTxCbor encodes a transaction whose only certificate is a
// reserves MIR distribution of delta to the key credential 0x4e..4e. delta is
// a single-byte CBOR integer head.
func mirNegativeDeltaTxCbor(t *testing.T, delta string) []byte {
	t.Helper()
	mirCert, err := hex.DecodeString(
		"82068200a18200581c" + strings.Repeat("4e", 28) + delta,
	)
	require.NoError(t, err)
	certsCbor, err := cbor.Encode([]any{cbor.RawMessage(mirCert)})
	require.NoError(t, err)
	txCbor, err := cbor.Encode([]any{
		map[uint]any{
			0: []any{},
			1: []any{},
			2: uint64(0),
			4: cbor.RawMessage(certsCbor),
		},
		map[uint]any{},
		true,
		nil,
	})
	require.NoError(t, err)
	return txCbor
}

// TestLedgerViewAnswersPendingInstantaneousRewards proves the upstream DELEG
// rule dingo runs for Alonzo and Babbage sees the MIR distributions committed
// earlier in the epoch. Without that, a negative delta covered by an earlier
// transaction is rejected as undecidable, and one exceeding the cover is
// reported as undecidable rather than as a negative update.
func TestLedgerViewAnswersPendingInstantaneousRewards(t *testing.T) {
	t.Parallel()

	type era struct {
		name     string
		decode   func([]byte) (lcommon.Transaction, error)
		validate lcommon.UtxoValidationRuleFunc
		pparams  lcommon.ProtocolParameters
	}
	eraCases := []era{
		{
			name: "alonzo",
			decode: func(b []byte) (lcommon.Transaction, error) {
				return alonzo.NewAlonzoTransactionFromCbor(b)
			},
			validate: alonzo.UtxoValidateDelegation,
			pparams:  &alonzo.AlonzoProtocolParameters{ProtocolMajor: 6},
		},
		{
			name: "babbage",
			decode: func(b []byte) (lcommon.Transaction, error) {
				return babbage.NewBabbageTransactionFromCbor(b)
			},
			validate: babbage.UtxoValidateDelegation,
			pparams:  &babbage.BabbageProtocolParameters{ProtocolMajor: 7},
		},
	}
	deltaCases := []struct {
		name      string
		seed      bool
		delta     string
		wantError bool
	}{
		// -10 is 0x29 and -11 is 0x2a.
		{name: "covered_by_earlier_tx", seed: true, delta: "29"},
		{name: "exceeds_earlier_tx", seed: true, delta: "2a", wantError: true},
		{name: "no_earlier_tx", delta: "29", wantError: true},
	}
	for _, e := range eraCases {
		for _, dc := range deltaCases {
			t.Run(e.name+"/"+dc.name, func(t *testing.T) {
				t.Parallel()

				ls, db, gdb := newMIRTestLedger(t)
				withMIRCutoffEpoch(
					t,
					ls,
					models.Epoch{StartSlot: 100, LengthInSlots: 432_000},
				)
				if dc.seed {
					// The other pot holds a larger credit for the same
					// credential, which must not count toward reserves.
					seedMIRDistribution(t, gdb, mirPotReserves, 150,
						[]models.MoveInstantaneousRewardsReward{
							{
								Credential: mirCred28(0x4e),
								Amount:     big.NewInt(10),
							},
						})
					seedMIRDistribution(t, gdb, mirPotTreasury, 160,
						[]models.MoveInstantaneousRewardsReward{
							{
								Credential: mirCred28(0x4e),
								Amount:     big.NewInt(100),
							},
						})
				}
				// Before the epoch, so it must not count either.
				seedMIRDistribution(t, gdb, mirPotReserves, 50,
					[]models.MoveInstantaneousRewardsReward{
						{
							Credential: mirCred28(0x4e),
							Amount:     big.NewInt(1_000),
						},
					})
				tx, err := e.decode(mirNegativeDeltaTxCbor(t, dc.delta))
				require.NoError(t, err)

				txn := db.Transaction(false)
				err = txn.Do(func(txn *database.Txn) error {
					lv := &LedgerView{ls: ls, txn: txn, epochStartSlot: 100}
					return e.validate(tx, 200, lv, e.pparams)
				})
				if !dc.wantError {
					require.NoError(t, err)
					return
				}
				var negErr shelley.MIRProducesNegativeUpdateError
				require.ErrorAs(t, err, &negErr)
			})
		}
	}
}
