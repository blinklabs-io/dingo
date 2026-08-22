// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	byronconsensus "github.com/blinklabs-io/gouroboros/consensus/byron"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func newByronPBFTTestNodeConfig(
	t *testing.T,
	block gledger.Block,
	securityParam uint64,
) *cardano.CardanoNodeConfig {
	t.Helper()
	header, ok := block.Header().(*byron.ByronMainBlockHeader)
	require.True(t, ok)
	proxySignature, ok := header.ConsensusData.BlockSig[1].([]any)
	require.True(t, ok)
	certificate, ok := proxySignature[0].([]any)
	require.True(t, ok)
	delegateKey, ok := certificate[2].([]byte)
	require.True(t, ok)
	certificateSignature, ok := certificate[3].([]byte)
	require.True(t, ok)
	omega, ok := certificate[0].(uint64)
	require.True(t, ok)
	issuerHash, err := byronconsensus.PBFTVerificationKeyHash(
		header.ConsensusData.PubKey,
	)
	require.NoError(t, err)

	nodeConfig, err := cardano.NewCardanoNodeConfigFromEmbedFS(
		cardano.EmbeddedConfigFS,
		"mainnet/config.json",
	)
	require.NoError(t, err)
	require.NoError(t, nodeConfig.LoadByronGenesisFromReader(strings.NewReader(
		fmt.Sprintf(`{
			"avvmDistr": {},
			"blockVersionData": {"slotDuration": "20000"},
			"ftsSeed": null,
			"protocolConsts": {"k": %d, "protocolMagic": %d},
			"startTime": 1506203091,
			"bootStakeholders": {},
			"heavyDelegation": {
				%q: {"cert": %q, "delegatePk": %q, "issuerPk": %q, "omega": %d}
			},
			"nonAvvmBalances": {},
			"vssCerts": {}
		}`,
			securityParam,
			header.ProtocolMagic,
			issuerHash.String(),
			hex.EncodeToString(certificateSignature),
			base64.StdEncoding.EncodeToString(delegateKey),
			base64.StdEncoding.EncodeToString(header.ConsensusData.PubKey),
			omega,
		),
	)))
	return nodeConfig
}

func TestAdvanceByronPBFTStateEnforcesIssuerWindow(t *testing.T) {
	stored := loadRealByronMainBlock(t)
	block, err := stored.Decode()
	require.NoError(t, err)
	const securityParam = 10
	ls := &LedgerState{
		config: LedgerStateConfig{
			CardanoNodeConfig: newByronPBFTTestNodeConfig(
				t,
				block,
				securityParam,
			),
		},
	}
	ls.slotClock = NewSlotClock(
		newMockSlotTimeProvider(time.Unix(0, 0), time.Second, 100),
		DefaultSlotClockConfig(),
	)
	state, err := byronconsensus.NewPBFTState(nil, securityParam)
	require.NoError(t, err)

	state, err = ls.advanceByronPBFTState(state, block, true)
	require.NoError(t, err)
	state, err = ls.advanceByronPBFTState(state, block, true)
	require.NoError(t, err)
	_, err = ls.advanceByronPBFTState(state, block, true)
	require.ErrorContains(t, err, "signature threshold")
	require.Len(t, state.SignatureHistory(), 2)
}

func TestByronPBFTStateAtTipRebuildsAfterRestartAndRollback(t *testing.T) {
	stored := loadRealByronMainBlock(t)
	block, err := stored.Decode()
	require.NoError(t, err)
	header, ok := block.Header().(*byron.ByronMainBlockHeader)
	require.True(t, ok)
	const securityParam = 3
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{
		securityParam: securityParam,
	}))

	rawBlocks := make([]chain.RawBlock, 4)
	expectedIssuers := make([]lcommon.Blake2b224, 4)
	var prevHash []byte
	for i := range rawBlocks {
		hash := bytes.Repeat([]byte{byte(i + 1)}, 32)
		issuerKey := bytes.Repeat([]byte{byte(0x40 + i)}, 64)
		modifiedCbor := bytes.Replace(
			stored.Cbor,
			header.ConsensusData.PubKey,
			issuerKey,
			1,
		)
		require.NotEqual(t, stored.Cbor, modifiedCbor)
		expectedIssuers[i], err = byronconsensus.PBFTVerificationKeyHash(
			issuerKey,
		)
		require.NoError(t, err)
		rawBlocks[i] = chain.RawBlock{
			Slot:        uint64(i + 1),
			Hash:        hash,
			PrevHash:    prevHash,
			BlockNumber: uint64(i + 1),
			Type:        gledger.BlockTypeByronMain,
			Cbor:        modifiedCbor,
		}
		prevHash = hash
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(rawBlocks))
	ls := &LedgerState{
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newByronPBFTTestNodeConfig(
				t,
				block,
				securityParam,
			),
		},
	}
	tip4 := ochainsync.Tip{
		Point: ocommon.NewPoint(
			rawBlocks[3].Slot,
			rawBlocks[3].Hash,
		),
		BlockNumber: rawBlocks[3].BlockNumber,
	}
	state, err := ls.byronPBFTStateAtTip(context.Background(), tip4)
	require.NoError(t, err)
	require.Equal(
		t,
		expectedIssuers[1:],
		state.SignatureHistory(),
		"restart reconstruction must keep exactly k issuers in canonical order",
	)

	ls.Lock()
	ls.byronPBFT = byronPBFTCache{
		state:       state,
		tip:         tip4.Point,
		initialized: true,
	}
	ls.Unlock()
	point2 := ocommon.NewPoint(rawBlocks[1].Slot, rawBlocks[1].Hash)
	state, err = ls.byronPBFTStateAtTip(context.Background(), ochainsync.Tip{
		Point:       point2,
		BlockNumber: rawBlocks[1].BlockNumber,
	})
	require.NoError(t, err)
	require.Equal(
		t,
		expectedIssuers[:2],
		state.SignatureHistory(),
		"rollback reconstruction must ignore a cache from the abandoned tip",
	)
}

func TestValidateByronPBFTSlotRejectsFuture(t *testing.T) {
	require.NoError(t, validateByronPBFTSlot(42, 42))
	require.ErrorContains(t, validateByronPBFTSlot(43, 42), "current slot")
}
