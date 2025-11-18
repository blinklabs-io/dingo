// Copyright 2025 Blink Labs Software
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

package ouroboros_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/ouroboros"
	oblockfetch "github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	olocaltxmonitor "github.com/blinklabs-io/gouroboros/protocol/localtxmonitor"
	olocaltxsubmission "github.com/blinklabs-io/gouroboros/protocol/localtxsubmission"
	opeersharing "github.com/blinklabs-io/gouroboros/protocol/peersharing"
	otxsubmission "github.com/blinklabs-io/gouroboros/protocol/txsubmission"
	"github.com/stretchr/testify/assert"
)

func TestGetNetworkMagic(t *testing.T) {
	tests := []struct {
		name        string
		networkName string
		expected    uint32
		expectError bool
	}{
		{"Mainnet", ouroboros.NetworkMainnet, 764824073, false},
		{"Preview", ouroboros.NetworkPreview, 2, false},
		{"Preprod", ouroboros.NetworkPreprod, 1, false},
		{"Devnet", ouroboros.NetworkDevnet, 42, false},
		{"Testnet", ouroboros.NetworkTestnet, 0, true},
		{"Sancho", ouroboros.NetworkSancho, 0, true},
		{"Unknown", "unknown", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			magic, err := ouroboros.GetNetworkMagic(tt.networkName)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, magic)
			}
		})
	}
}

func TestGetNetwork(t *testing.T) {
	tests := []struct {
		name        string
		networkName string
		expectError bool
	}{
		{"Mainnet", ouroboros.NetworkMainnet, false},
		{"Preview", ouroboros.NetworkPreview, false},
		{"Preprod", ouroboros.NetworkPreprod, false},
		{"Devnet", ouroboros.NetworkDevnet, false},
		{"Testnet", ouroboros.NetworkTestnet, true},
		{"Sancho", ouroboros.NetworkSancho, true},
		{"Unknown", "unknown", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			network, err := ouroboros.GetNetwork(tt.networkName)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, network)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, network)
				assert.Equal(t, tt.networkName, network.Name)
			}
		})
	}
}

func TestNodeToClientServerOpts(t *testing.T) {
	networkMagic := uint32(764824073)
	chainsyncOpts := []ochainsync.ChainSyncOptionFunc{}
	localstatequeryOpts := []olocalstatequery.LocalStateQueryOptionFunc{}
	localtxmonitorOpts := []olocaltxmonitor.LocalTxMonitorOptionFunc{}
	localtxsubmissionOpts := []olocaltxsubmission.LocalTxSubmissionOptionFunc{}

	opts := ouroboros.NodeToClientServerOpts(
		networkMagic,
		chainsyncOpts,
		localstatequeryOpts,
		localtxmonitorOpts,
		localtxsubmissionOpts,
	)

	assert.NotNil(t, opts)
	assert.True(t, len(opts) > 0)
}

func TestNodeToNodeServerOpts(t *testing.T) {
	peerSharing := true
	networkMagic := uint32(764824073)
	peersharingOpts := []opeersharing.PeerSharingOptionFunc{}
	txsubmissionOpts := []otxsubmission.TxSubmissionOptionFunc{}
	chainsyncOpts := []ochainsync.ChainSyncOptionFunc{}
	blockfetchOpts := []oblockfetch.BlockFetchOptionFunc{}

	opts := ouroboros.NodeToNodeServerOpts(
		peerSharing,
		networkMagic,
		peersharingOpts,
		txsubmissionOpts,
		chainsyncOpts,
		blockfetchOpts,
	)

	assert.NotNil(t, opts)
	assert.True(t, len(opts) > 0)

	// Test with peerSharing disabled to ensure different option count
	optsDisabled := ouroboros.NodeToNodeServerOpts(
		false,
		networkMagic,
		peersharingOpts,
		txsubmissionOpts,
		chainsyncOpts,
		blockfetchOpts,
	)

	assert.NotNil(t, optsDisabled)
	assert.True(t, len(optsDisabled) > 0)
	// Enabling peer sharing should only add options, not remove them
	assert.True(t, len(opts) > len(optsDisabled))
}

func TestOutboundOpts(t *testing.T) {
	networkMagic := uint32(764824073)
	enablePeerSharing := true
	peersharingClientOpts := []opeersharing.PeerSharingOptionFunc{}
	peersharingServerOpts := []opeersharing.PeerSharingOptionFunc{}
	txsubmissionClientOpts := []otxsubmission.TxSubmissionOptionFunc{}
	txsubmissionServerOpts := []otxsubmission.TxSubmissionOptionFunc{}
	chainsyncClientOpts := []ochainsync.ChainSyncOptionFunc{}
	chainsyncServerOpts := []ochainsync.ChainSyncOptionFunc{}
	blockfetchClientOpts := []oblockfetch.BlockFetchOptionFunc{}
	blockfetchServerOpts := []oblockfetch.BlockFetchOptionFunc{}

	opts := ouroboros.OutboundOpts(
		networkMagic,
		enablePeerSharing,
		peersharingClientOpts,
		peersharingServerOpts,
		txsubmissionClientOpts,
		txsubmissionServerOpts,
		chainsyncClientOpts,
		chainsyncServerOpts,
		blockfetchClientOpts,
		blockfetchServerOpts,
	)

	assert.NotNil(t, opts)
	assert.True(t, len(opts) > 0)

	// Test with peer sharing disabled to ensure different option count
	optsDisabled := ouroboros.OutboundOpts(
		networkMagic,
		false,
		peersharingClientOpts,
		peersharingServerOpts,
		txsubmissionClientOpts,
		txsubmissionServerOpts,
		chainsyncClientOpts,
		chainsyncServerOpts,
		blockfetchClientOpts,
		blockfetchServerOpts,
	)

	assert.NotNil(t, optsDisabled)
	assert.True(t, len(optsDisabled) > 0)
	// Enabling peer sharing should only add options, not remove them
	assert.True(t, len(opts) > len(optsDisabled))
}
