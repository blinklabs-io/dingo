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

package dingo

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestConfigValidateRejectsByronMagicAboveUint32 covers the width half of
// configValidate's Byron protocol-magic check, which the mismatch test above
// it does not reach: a genesis naming a magic wider than the uint32 the
// network magic is compared as must be rejected, not truncated into a
// spurious match.
func TestConfigValidateRejectsByronMagicAboveUint32(t *testing.T) {
	for _, tc := range []struct {
		name  string
		magic int64
	}{
		{name: "one past max uint32", magic: int64(math.MaxUint32) + 1},
		{name: "negative", magic: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if int64(int(tc.magic)) != tc.magic {
				t.Skipf(
					"int is too narrow to represent %d on this target",
					tc.magic,
				)
			}
			nodeCfg := &cardano.CardanoNodeConfig{}
			require.NoError(t, nodeCfg.LoadShelleyGenesisFromReader(
				strings.NewReader(`{
					"networkMagic": 42,
					"activeSlotsCoeff": 0.05,
					"securityParam": 432,
					"slotsPerKESPeriod": 129600,
					"maxKESEvolutions": 62,
					"systemStart": "2022-10-25T00:00:00Z"
				}`),
			))
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

			_, err := New(NewConfig(
				WithPrometheusRegistry(prometheus.NewRegistry()),
				WithListeners(ListenerConfig{
					ListenNetwork: "tcp",
					ListenAddress: "127.0.0.1:0",
				}),
				WithNetworkMagic(42),
				WithCardanoNodeConfig(nodeCfg),
			))
			require.ErrorContains(t, err, "is out of uint32 range")
		})
	}
}
