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

package eras

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/stretchr/testify/require"
)

func testByronGenesis(slotDuration string, k int) []byte {
	return []byte(fmt.Sprintf(`{
		"avvmDistr": {},
		"blockVersionData": {
			"heavyDelThd":"300000000000","maxBlockSize":"2000000",
			"maxHeaderSize":"2000000","maxProposalSize":"700",
			"maxTxSize":"4096","mpcThd":"20000000000000",
			"scriptVersion":0,"slotDuration":%q,
			"softforkRule":{"initThd":"900000000000000","minThd":"600000000000000","thdDecrement":"50000000000000"},
			"txFeePolicy":{"multiplier":"43946000000","summand":"155381000000000"},
			"unlockStakeEpoch":"18446744073709551615","updateImplicit":"10000",
			"updateProposalThd":"100000000000000","updateVoteThd":"1000000000000"
		},
		"protocolConsts":{"k":%d,"protocolMagic":164},"startTime":1506203091,
		"bootStakeholders":{"e551ed0645140f2fc9975a7cee7dd89380d918e091869b19332bcb54":1},
		"heavyDelegation":{"e551ed0645140f2fc9975a7cee7dd89380d918e091869b19332bcb54":{"cert":"0f28871316b43f19773332984976f5d5838ed55b165c5afef4668b9efaee83c45744af47c41cda4ca3579443e3438bc6c6443106a5b2e8f820ab569bc7c1a907","delegatePk":"4yJc1LKn25BXdt5RFiMPKymb2P+V6qnKxbdi8CzHePekiNiMtPIupsmS+TbnZ43NMP6M7QfOEsLou5730/0Dsg==","issuerPk":"U3i0cs1QPT6ajXHJpZj1Aqj0Nkh2bhqkOOEXkd/MmcyH8XDJVZ2TchvZyt2m5PKsrgnqQWIv/dWmWQGwBB065Q==","omega":0}},
		"nonAvvmBalances":{}
	}`, slotDuration, k))
}

// TestEpochLengthByronRejectsNegativeSlotDuration covers dingo#4427: the
// upstream gouroboros parser accepts a signed slotDuration, so a genesis
// carrying slotDuration "-1" must not silently wrap to a huge unsigned
// duration at the uint(...) conversion in EpochLengthByron. This uses
// LoadByronGenesisFromReader, the config package's own test-only bypass of
// the CardanoNodeConfig.loadGenesisConfigs load path, so it exercises the
// guard at EpochLengthByron itself rather than the earlier guard the load
// path now also has (config/cardano.TestLoadGenesisConfigsRejectsNegativeByronSlotDuration).
func TestEpochLengthByronRejectsNegativeSlotDuration(t *testing.T) {
	t.Parallel()

	cfg := &cardano.CardanoNodeConfig{}
	err := cfg.LoadByronGenesisFromReader(
		bytes.NewReader(testByronGenesis("20000", 2160)),
	)
	require.NoError(t, err)
	cfg.ByronGenesis().BlockVersionData.SlotDuration = -1

	_, _, err = EpochLengthByron(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "slotDuration")
	require.Contains(t, err.Error(), "negative")
}

// TestEpochLengthByronRejectsNonPositiveK covers the same signed-to-unsigned
// conversion class of bug as TestEpochLengthByronRejectsNegativeSlotDuration,
// but for ProtocolConsts.K: EpochLengthByron's uint(K*10) had no local guard
// of its own, even though three other call sites (config/cardano's
// validateSecurityParameters, internal/node/load.go's
// loadSecurityParamForConfig, and this package's own StabilityWindowForEra)
// already reject a non-positive k before it would reach a production call
// here.
func TestEpochLengthByronRejectsNonPositiveK(t *testing.T) {
	t.Parallel()

	cfg := &cardano.CardanoNodeConfig{}
	err := cfg.LoadByronGenesisFromReader(
		bytes.NewReader(testByronGenesis("20000", 2160)),
	)
	require.NoError(t, err)
	cfg.ByronGenesis().ProtocolConsts.K = -1

	_, _, err = EpochLengthByron(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "protocolConsts.k")
}

// TestEpochLengthByronAcceptsNonNegativeSlotDuration is the companion
// positive case: an ordinary genesis must still compute an epoch length, so
// the new guard only rejects negative values.
func TestEpochLengthByronAcceptsNonNegativeSlotDuration(t *testing.T) {
	t.Parallel()

	cfg := &cardano.CardanoNodeConfig{}
	err := cfg.LoadByronGenesisFromReader(
		bytes.NewReader(testByronGenesis("20000", 2160)),
	)
	require.NoError(t, err)

	slotDuration, epochLength, err := EpochLengthByron(cfg)
	require.NoError(t, err)
	require.Equal(t, uint(20000), slotDuration)
	require.Equal(t, uint(21600), epochLength)
}
