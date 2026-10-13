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

package testutil

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// Protocol-parameter update keys from the Conway CDDL, shared by Dijkstra.
const (
	PParamUpdateKeyMaxTxSize          = 3
	PParamUpdateKeyPoolDeposit        = 6
	PParamUpdateKeyNOpt               = 8
	PParamUpdateKeyAdaPerUtxoByte     = 17
	PParamUpdateKeyCostModels         = 18
	PParamUpdateKeyCollateralPercent  = 23
	PParamUpdateKeyGovActionDeposit   = 30
	PParamUpdateKeyDRepDeposit        = 31
	PParamUpdateKeyCommitteeTermLimit = 28
	PParamUpdateKeyGovActionPeriod    = 29
)

// ParameterChangeTxCbor returns transaction CBOR carrying one ParameterChange
// governance proposal whose update map is ppu. The transaction is otherwise
// minimal: it is meant for tests that assert how a proposal is judged, not
// for tests that need an otherwise valid transaction. The anchor URL is part
// of the transaction body, so distinct urls yield distinct transaction ids.
func ParameterChangeTxCbor(
	t testing.TB,
	ppu map[uint]any,
	anchorURL string,
) []byte {
	t.Helper()
	inputHash := make([]byte, 32)
	inputHash[0] = 0xaa
	addr := make([]byte, 29)
	addr[0] = 0x60
	rewardAccount := make([]byte, 29)
	rewardAccount[0] = 0xe0
	body := map[uint]any{
		0: cbor.Tag{
			Number:  258,
			Content: []any{[]any{inputHash, uint64(0)}},
		},
		1: []any{[]any{addr, uint64(1_000_000)}},
		2: uint64(200_000),
		20: []any{
			[]any{
				uint64(1_000_000_000),
				rewardAccount,
				[]any{
					uint64(lcommon.GovActionTypeParameterChange),
					nil,
					ppu,
					nil,
				},
				[]any{anchorURL, make([]byte, 32)},
			},
		},
	}
	txCbor, err := cbor.Encode([]any{body, map[uint]any{}, true, nil})
	require.NoError(t, err)
	return txCbor
}
