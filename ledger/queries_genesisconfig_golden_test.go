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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/protocol"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// consensusGenesisJSON is the Shelley genesis behind the consensus golden
// reply genesisConfigNoExtraConfigGolden: system start 2020 day 135, network
// magic 0, a 0.9 active slot coefficient, k 10, 100-slot epochs, a 2 second
// slot, an update quorum of 5, 17 protocol parameters of which only the
// maximum transaction size (2048), n-opt (100) and protocol version 2.0 are
// non-zero, and no genesis delegates, funds or staking.
const consensusGenesisJSON = `{
	"systemStart": "2020-05-14T00:00:00Z",
	"networkMagic": 0,
	"networkid": "Testnet",
	"activeSlotsCoeff": 0.9,
	"securityParam": 10,
	"epochLength": 100,
	"slotsPerKESPeriod": 20,
	"maxKESEvolutions": 10,
	"slotLength": 2,
	"updateQuorum": 5,
	"maxLovelaceSupply": 45000000000000000,
	"protocolParams": {
		"minFeeA": 0,
		"minFeeB": 0,
		"maxBlockBodySize": 0,
		"maxTxSize": 2048,
		"maxBlockHeaderSize": 0,
		"keyDeposit": 0,
		"poolDeposit": 0,
		"eMax": 0,
		"nOpt": 100,
		"a0": 0,
		"rho": 0,
		"tau": 0,
		"decentralisationParam": 0,
		"extraEntropy": {"tag": "NeutralNonce"},
		"protocolVersion": {"major": 2, "minor": 0},
		"minUTxOValue": 0,
		"minPoolCost": 0
	},
	"genDelegs": {},
	"initialFunds": {},
	"staking": {"pools": {}, "stake": {}}
}`

// TestGenesisConfigMatchesConsensusGoldenAtV21To23 requires the whole reply,
// all 15 fields, to equal the consensus golden's bytes for node-to-client
// versions 21 to 23, from a genesis holding the golden's values, through both
// the handler and the query entry point.
func TestGenesisConfigMatchesConsensusGoldenAtV21To23(t *testing.T) {
	t.Parallel()

	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(
			strings.NewReader(consensusGenesisJSON),
		),
	)
	ls := &LedgerState{config: LedgerStateConfig{CardanoNodeConfig: cfg}}
	query := &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyGenesisConfigQuery{},
		},
	}
	for _, version := range []uint16{21, 22, 23} {
		ntc := version + protocol.ProtocolVersionNtCOffset
		direct, err := ls.queryShelleyGenesisConfig(t.Context(), ntc)
		require.NoError(t, err)
		viaQuery, err := ls.QueryWithProtocolVersion(
			t.Context(), query, QueryPoint{}, ntc,
		)
		require.NoError(t, err)
		for name, result := range map[string]any{
			"direct": direct,
			"query":  viaQuery,
		} {
			values, ok := result.([]any)
			require.True(t, ok, "v%d %s", version, name)
			require.Len(t, values, 1)
			wire, err := cbor.Encode(values[0])
			require.NoError(t, err)
			require.Equal(
				t,
				genesisConfigNoExtraConfigGolden,
				hex.EncodeToString(wire),
				"v%d %s",
				version,
				name,
			)
		}
	}
}
