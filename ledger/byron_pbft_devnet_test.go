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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewByronPBFTCacheDevnetEmptyGenesisIssuers reproduces
// blinklabs-io/dingo's devnet startup regression: a Byron genesis with no
// boot stakeholders (and therefore no heavy delegation, since a heavy
// delegation certificate must name an existing boot stakeholder as its
// issuer) has no possible PBFT signer, so no valid Byron main block can ever
// be produced on that chain. internal/test/devnet/configurator.sh generates
// exactly this genesis shape for a network that hard-forks away from Byron
// at genesis (testnet.yaml sets every TestXHardForkAtEpoch to 0). Ledger
// state construction must tolerate it rather than failing node startup.
func TestNewByronPBFTCacheDevnetEmptyGenesisIssuers(t *testing.T) {
	t.Parallel()

	const byronGenesisJSON = `{
		"protocolConsts": {"k": 60, "protocolMagic": 42},
		"blockVersionData": {"slotDuration": "1000"},
		"bootStakeholders": {},
		"heavyDelegation": {}
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)),
	)

	cache, err := newByronPBFTCache(LedgerStateConfig{CardanoNodeConfig: cfg})
	require.NoError(
		t,
		err,
		"a Byron genesis with no possible PBFT issuers must not fail ledger "+
			"state construction",
	)
	assert.Nil(
		t,
		cache.config,
		"no PBFT config should be cached when Byron has no eligible issuers",
	)
}

// TestNewByronPBFTCacheRealByronGenesis is the control: a Byron genesis that
// does declare a boot stakeholder (every real chain -- mainnet, preprod,
// preview -- always has at least one) must still build and cache a full PBFT
// config, so this fix does not relax validation for a chain with real Byron
// history.
func TestNewByronPBFTCacheRealByronGenesis(t *testing.T) {
	t.Parallel()

	const byronGenesisJSON = `{
		"protocolConsts": {"k": 60, "protocolMagic": 42},
		"blockVersionData": {"slotDuration": "1000"}
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)),
	)

	cache, err := newByronPBFTCache(LedgerStateConfig{CardanoNodeConfig: cfg})
	require.NoError(t, err)
	require.NotNil(
		t,
		cache.config,
		"a genesis with a real boot stakeholder must still build a PBFT config",
	)
	assert.Len(t, cache.config.GenesisKeyHashes, 1)
}
