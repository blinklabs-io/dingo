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
	"bytes"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/stretchr/testify/require"
)

// maxUint32AsInt64 and overMaxUint32AsInt64 are variables rather than
// constants so that converting them to int below is a runtime conversion.
// A constant conversion of either one does not compile on a 32-bit target,
// which is the whole reason these guards needed widening.
var (
	maxUint32AsInt64     = int64(math.MaxUint32)
	overMaxUint32AsInt64 = int64(math.MaxUint32) + 1
)

// requireWideInt skips a case whose input cannot be represented as an int on
// this target. Neither math.MaxUint32 nor the value above it fits a 32-bit
// int, so there is no input that can drive the rejecting side of these
// guards there.
func requireWideInt(t *testing.T, v int64) int {
	t.Helper()
	n := int(v)
	if int64(n) != v {
		t.Skipf("int is too narrow to represent %d on this target", v)
	}
	return n
}

func TestFitsUint32(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		n    int64
		want bool
	}{
		{name: "zero", n: 0, want: true},
		{name: "one below max", n: maxUint32AsInt64 - 1, want: true},
		{name: "max", n: maxUint32AsInt64, want: true},
		{name: "one past max", n: overMaxUint32AsInt64, want: false},
		{name: "negative", n: -1, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, fitsUint32(requireWideInt(t, tc.n)))
		})
	}
}

// TestWriteCborMajorTypeUint32Boundary pins the header width chosen on
// either side of 2^32, the point where writeCborMajorType switches from the
// four-byte to the eight-byte argument encoding.
func TestWriteCborMajorTypeUint32Boundary(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		n      int64
		header byte
		length int
	}{
		{name: "max uint32", n: maxUint32AsInt64, header: 0x1a, length: 5},
		{
			name:   "one past max uint32",
			n:      overMaxUint32AsInt64,
			header: 0x1b,
			length: 9,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			n := requireWideInt(t, tc.n)
			var buf bytes.Buffer
			writeCborMajorType(&buf, 0, n)
			require.Len(t, buf.Bytes(), tc.length)
			require.Equal(t, tc.header, buf.Bytes()[0])
		})
	}
}

// TestDeltaApplyTransactionIndexWidth drives the delta apply path either
// side of the uint32 transaction-index bound its database column carries.
func TestDeltaApplyTransactionIndexWidth(t *testing.T) {
	t.Parallel()

	t.Run("max uint32 index is applied", func(t *testing.T) {
		t.Parallel()
		index := requireWideInt(t, maxUint32AsInt64)
		ls, db, _ := newTransactionEventTestLedger(t)
		delta := newTransactionEventTestDelta(t, 6, index)
		defer delta.Release()

		txn := db.Transaction(true)
		require.NoError(t, delta.apply(ls, txn))
		require.NoError(t, txn.Rollback())
	})

	t.Run("index past uint32 is rejected", func(t *testing.T) {
		t.Parallel()
		index := requireWideInt(t, overMaxUint32AsInt64)
		ls, db, _ := newTransactionEventTestLedger(t)
		delta := newTransactionEventTestDelta(t, 7, index)
		defer delta.Release()

		txn := db.Transaction(true)
		require.ErrorContains(
			t,
			delta.apply(ls, txn),
			"transaction index out of range",
		)
		require.NoError(t, txn.Rollback())
	})
}

// TestByronProtocolMagicWidth drives ByronProtocolMagic either side of the
// uint32 bound its return type carries. Byron genesis declares protocolMagic
// as a JSON number decoded into an int, so a genesis naming a value above
// uint32 must be rejected rather than truncated.
func TestByronProtocolMagicWidth(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		magic   int64
		wantErr string
	}{
		{name: "max uint32", magic: maxUint32AsInt64},
		{
			name:    "one past max uint32",
			magic:   overMaxUint32AsInt64,
			wantErr: "byron protocol magic exceeds uint32",
		},
		{
			name:    "negative",
			magic:   -1,
			wantErr: "byron protocol magic is negative",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireWideInt(t, tc.magic)
			nodeCfg := &cardano.CardanoNodeConfig{}
			require.NoError(t, nodeCfg.LoadByronGenesisFromReader(
				strings.NewReader(
					`{
						"avvmDistr": {},
						"blockVersionData": {
							"heavyDelThd":"300000000000","maxBlockSize":"2000000",
							"maxHeaderSize":"2000000","maxProposalSize":"700",
							"maxTxSize":"4096","mpcThd":"20000000000000",
							"scriptVersion":0,"slotDuration":"20000",
							"softforkRule":{"initThd":"900000000000000","minThd":"600000000000000","thdDecrement":"50000000000000"},
							"txFeePolicy":{"multiplier":"43946000000","summand":"155381000000000"},
							"unlockStakeEpoch":"18446744073709551615","updateImplicit":"10000",
							"updateProposalThd":"100000000000000","updateVoteThd":"1000000000000"
						},
						"startTime": 1666656000,
						"bootStakeholders": {}, "heavyDelegation": {}, "nonAvvmBalances": {},
						"protocolConsts": {"k": 108, "protocolMagic": `+
						strconv.FormatInt(tc.magic, 10)+`}
					}`,
				),
			))
			ls := &LedgerState{
				config: LedgerStateConfig{CardanoNodeConfig: nodeCfg},
			}

			magic, err := ls.ByronProtocolMagic()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, uint32(tc.magic), magic)
		})
	}
}
