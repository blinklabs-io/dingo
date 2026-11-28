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

package ouroboros

import (
	"testing"

	"github.com/blinklabs-io/dingo/ledger"
	sdkcardano "github.com/blinklabs-io/dingo/sdk/cardano"
	sdkchain "github.com/blinklabs-io/dingo/sdk/chain"
	"github.com/stretchr/testify/require"
)

var _ ProtocolLedger = (*ledger.LedgerState)(nil)
var _ interface {
	LedgerState() *ledger.LedgerState
} = (*Ouroboros)(nil)

type sdkLedgerStub struct {
	securityParam int
	tip           sdkchain.Tip
}

func (s sdkLedgerStub) SecurityParam() int { return s.securityParam }
func (s sdkLedgerStub) Tip() sdkchain.Tip  { return s.tip }
func (sdkLedgerStub) ValidateTx(sdkchain.Tx) error {
	return nil
}

func TestOuroborosReadsSDKLedgerRole(t *testing.T) {
	t.Parallel()

	state := newTestLedgerState(t)
	wantTip := sdkchain.Tip{
		Point:       sdkchain.Point{Slot: 27, Hash: []byte{0xaa, 0xbb}},
		BlockNumber: 19,
	}
	o := newOuroboros(OuroborosConfig{
		LedgerState: state,
		ChainLedger: sdkLedgerStub{securityParam: 7, tip: wantTip},
	})

	if got := o.ledgerSecurityParam(); got != 7 {
		t.Fatalf("SDK ledger role security parameter = %d, want 7", got)
	}
	require.Equal(t, wantTip.Point.Slot, o.ledgerTip().Point.Slot)
	require.Equal(t, wantTip.Point.Hash, o.ledgerTip().Point.Hash)
	require.Equal(t, wantTip.BlockNumber, o.ledgerTip().BlockNumber)
}

func newTestChainLedgerRole(
	t testing.TB,
	state *ledger.LedgerState,
) sdkchain.Ledger {
	t.Helper()
	role, err := sdkcardano.NewLedgerRole(state)
	require.NoError(t, err)
	return role
}

func installTestLedger(o *Ouroboros, state *ledger.LedgerState) {
	role, err := sdkcardano.NewLedgerRole(state)
	if err != nil {
		panic("test ledger role: " + err.Error())
	}
	o.ledgerState = state
	o.chainLedger = role
}

func concreteLedgerState(
	t testing.TB,
	role ProtocolLedger,
) *ledger.LedgerState {
	t.Helper()
	state, ok := role.(*ledger.LedgerState)
	require.True(t, ok, "test requires the built-in ledger role")
	return state
}

func testLedgerState(t testing.TB, o *Ouroboros) *ledger.LedgerState {
	t.Helper()
	return concreteLedgerState(t, o.ledgerState)
}

func builtInLedgerState(role ProtocolLedger) *ledger.LedgerState {
	state, ok := role.(*ledger.LedgerState)
	if !ok {
		panic("test requires the built-in ledger role")
	}
	return state
}
