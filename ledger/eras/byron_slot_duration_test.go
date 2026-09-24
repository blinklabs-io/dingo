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
	"encoding/json"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/stretchr/testify/require"
)

func byronGenesisForEpochTest(
	t *testing.T,
	slotDuration *string,
	k *int,
) *cardano.CardanoNodeConfig {
	t.Helper()
	genesis, err := cardano.EmbeddedConfigFS.ReadFile(
		"mainnet/byron-genesis.json",
	)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(genesis, &fields))
	if slotDuration != nil {
		var blockVersionData map[string]json.RawMessage
		require.NoError(
			t,
			json.Unmarshal(fields["blockVersionData"], &blockVersionData),
		)
		blockVersionData["slotDuration"], err = json.Marshal(*slotDuration)
		require.NoError(t, err)
		fields["blockVersionData"], err = json.Marshal(blockVersionData)
		require.NoError(t, err)
	}
	if k != nil {
		var protocolConsts map[string]json.RawMessage
		require.NoError(
			t,
			json.Unmarshal(fields["protocolConsts"], &protocolConsts),
		)
		protocolConsts["k"], err = json.Marshal(*k)
		require.NoError(t, err)
		fields["protocolConsts"], err = json.Marshal(protocolConsts)
		require.NoError(t, err)
	}
	genesis, err = json.Marshal(fields)
	require.NoError(t, err)
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadByronGenesisFromReader(strings.NewReader(string(genesis))))
	return cfg
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

	_, _, err := epochLengthByronGenesis(&byron.ByronGenesis{
		BlockVersionData: byron.ByronGenesisBlockVersionData{
			SlotDuration: -1,
		},
	})
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

	k := -1
	cfg := byronGenesisForEpochTest(t, nil, &k)

	_, _, err := EpochLengthByron(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "protocolConsts.k")
}

// TestEpochLengthByronAcceptsNonNegativeSlotDuration is the companion
// positive case: an ordinary genesis must still compute an epoch length, so
// the new guard only rejects negative values.
func TestEpochLengthByronAcceptsNonNegativeSlotDuration(t *testing.T) {
	t.Parallel()

	duration := "20000"
	k := 2160
	cfg := byronGenesisForEpochTest(t, &duration, &k)

	slotDuration, epochLength, err := EpochLengthByron(cfg)
	require.NoError(t, err)
	require.Equal(t, uint(20000), slotDuration)
	require.Equal(t, uint(21600), epochLength)
}
