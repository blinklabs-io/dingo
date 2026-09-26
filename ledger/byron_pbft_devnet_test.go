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
	"context"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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

const noByronIssuersRule = "declares no boot stakeholders"

// newNoByronIssuersTestLedger builds a LedgerState over the devnet genesis
// shape: a Byron genesis with no boot stakeholders, its PBFT cache built the
// way NewLedgerState builds it, a real primary chain and a working slot clock.
// The Byron genesis hash is set explicitly because LoadByronGenesisFromReader
// does not derive it; production config loading does.
func newNoByronIssuersTestLedger(
	t *testing.T,
	genesisHash lcommon.Blake2b256,
) (*LedgerState, *chain.Chain) {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, loadByronGenesisForTest(t, cfg, strings.NewReader(`{
		"protocolConsts": {"k": 60, "protocolMagic": 42},
		"blockVersionData": {"slotDuration": "1000"},
		"bootStakeholders": {},
		"heavyDelegation": {}
	}`)))
	cfg.ByronGenesisHash = genesisHash.String()
	lsConfig := LedgerStateConfig{CardanoNodeConfig: cfg}
	cache, err := newByronPBFTCache(lsConfig)
	require.NoError(t, err)

	cm, err := chain.NewManager(newTestDB(t), nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 10}),
	)
	ls := &LedgerState{
		chain:     cm.PrimaryChain(),
		config:    lsConfig,
		byronPBFT: cache,
	}
	ls.slotClock = NewSlotClock(
		newMockSlotTimeProvider(
			time.Now().Add(-100*time.Second),
			time.Second,
			100,
		),
		DefaultSlotClockConfig(),
	)
	return ls, cm.PrimaryChain()
}

// TestByronHeaderRejectedWithoutGenesisIssuers feeds peer-shaped Byron
// headers and blocks to a node whose Byron genesis has no boot stakeholders,
// through every header entry point that routes Byron to
// validateByronPBFTHeaderCrypto. An epoch-boundary block carries no PBFT
// signature, so without a rule of its own a genesis-anchored EBB at origin,
// or any EBB at or below the current slot after it, would pass every
// entry point on this chain.
func TestByronHeaderRejectedWithoutGenesisIssuers(t *testing.T) {
	t.Parallel()

	genesisHash := lcommon.Blake2b256Hash([]byte("devnet byron genesis"))
	entryPoints := []struct {
		name   string
		verify func(*LedgerState, gledger.Block) error
	}{
		{"chainsync header", func(ls *LedgerState, b gledger.Block) error {
			return ls.verifyBlockHeaderOnlyCrypto(b.Header())
		}},
		{"chain selection header", func(ls *LedgerState, b gledger.Block) error {
			return ls.ValidateChainSelectionHeaderCrypto(b.Header())
		}},
		{"announced header", func(ls *LedgerState, b gledger.Block) error {
			return ls.ValidateBlockHeaderCrypto(b.Header())
		}},
		{"fetched block", func(ls *LedgerState, b gledger.Block) error {
			return ls.verifyBlockHeaderCryptoBeforeApply(b)
		}},
	}
	inputs := []struct {
		name     string
		atOrigin bool
		block    gledger.Block
	}{
		{
			name:     "genesis-anchored EBB at origin",
			atOrigin: true,
			block: &byron.ByronEpochBoundaryBlock{
				BlockHeader: &byron.ByronEpochBoundaryBlockHeader{
					PrevBlock: genesisHash,
				},
			},
		},
		{
			name: "EBB after a post-Byron tip",
			block: &byron.ByronEpochBoundaryBlock{
				BlockHeader: &byron.ByronEpochBoundaryBlockHeader{
					PrevBlock: lcommon.Blake2b256Hash([]byte("conway tip")),
				},
			},
		},
		{
			name: "main block after a post-Byron tip",
			block: &byron.ByronMainBlock{
				BlockHeader: &byron.ByronMainBlockHeader{},
			},
		},
	}
	for _, input := range inputs {
		for _, entry := range entryPoints {
			t.Run(input.name+"/"+entry.name, func(t *testing.T) {
				t.Parallel()
				ls, primaryChain := newNoByronIssuersTestLedger(
					t,
					genesisHash,
				)
				if !input.atOrigin {
					require.NoError(t, primaryChain.AddRawBlocks(
						[]chain.RawBlock{{
							Slot:        1,
							Hash:        bytes.Repeat([]byte{0xcc}, 32),
							BlockNumber: 0,
							Type:        gledger.BlockTypeConway,
							Cbor:        []byte{0x80},
						}},
					))
				}
				require.Equal(
					t,
					input.atOrigin,
					len(primaryChain.Tip().Point.Hash) == 0,
				)
				err := entry.verify(ls, input.block)
				require.ErrorContains(t, err, noByronIssuersRule)
			})
		}
	}
}

// TestByronPBFTStateAtTipRejectsChainWithoutGenesisIssuers pins the ledger
// apply path: reconstructing Byron PBFT state for a batch containing a Byron
// block must fail on the same rule rather than on missing configuration.
func TestByronPBFTStateAtTipRejectsChainWithoutGenesisIssuers(t *testing.T) {
	t.Parallel()

	ls, _ := newNoByronIssuersTestLedger(
		t,
		lcommon.Blake2b256Hash([]byte("devnet byron genesis")),
	)
	_, err := ls.byronPBFTStateAtTip(
		context.Background(),
		ocommon.Tip{},
	)
	require.ErrorContains(t, err, noByronIssuersRule)
}

// TestByronHeaderGateReadsOnlyConstructionTimeFields runs the no-issuer
// header gate while another goroutine commits Byron PBFT state under the
// ledger lock, as ledger apply does concurrently with chainsync header
// validation. The gate takes no lock, so under -race it must read only the
// fields NewLedgerState sets once.
func TestByronHeaderGateReadsOnlyConstructionTimeFields(t *testing.T) {
	t.Parallel()

	ls, _ := newNoByronIssuersTestLedger(
		t,
		lcommon.Blake2b256Hash([]byte("devnet byron genesis")),
	)
	ebb := &byron.ByronEpochBoundaryBlock{
		BlockHeader: &byron.ByronEpochBoundaryBlockHeader{},
	}
	const iterations = 200
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range iterations {
			ls.Lock()
			ls.byronPBFT.tip = ocommon.NewPoint(uint64(i), nil)
			ls.byronPBFT.initialized = true
			ls.Unlock()
		}
	}()
	for range iterations {
		require.ErrorContains(
			t,
			ls.validateByronPBFTHeaderCrypto(ebb),
			noByronIssuersRule,
		)
	}
	<-done
}
