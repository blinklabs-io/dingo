package ouroboros

import (
	"fmt"

	"github.com/blinklabs-io/dingo/connmanager"
	gouroboros "github.com/blinklabs-io/gouroboros"
	oblockfetch "github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	olocaltxmonitor "github.com/blinklabs-io/gouroboros/protocol/localtxmonitor"
	olocaltxsubmission "github.com/blinklabs-io/gouroboros/protocol/localtxsubmission"
	opeersharing "github.com/blinklabs-io/gouroboros/protocol/peersharing"
	otxsubmission "github.com/blinklabs-io/gouroboros/protocol/txsubmission"
)

const (
	// NetworkMainnet represents the Cardano mainnet network.
	NetworkMainnet = "mainnet"
	// NetworkTestnet represents the Cardano testnet network.
	NetworkTestnet = "testnet"
	// NetworkPreview represents the Cardano preview testnet.
	NetworkPreview = "preview"
	// NetworkPreprod represents the Cardano preprod testnet.
	NetworkPreprod = "preprod"
	// NetworkSancho represents the Sancho testnet.
	NetworkSancho = "sancho"
	// NetworkDevnet represents a local development network.
	NetworkDevnet = "devnet"
)

// NodeToClientServerOpts returns server options for node-to-client connections.
func NodeToClientServerOpts(
	networkMagic uint32,
	chainsyncOpts []ochainsync.ChainSyncOptionFunc,
	localstatequeryOpts []olocalstatequery.LocalStateQueryOptionFunc,
	localtxmonitorOpts []olocaltxmonitor.LocalTxMonitorOptionFunc,
	localtxsubmissionOpts []olocaltxsubmission.LocalTxSubmissionOptionFunc,
) []gouroboros.ConnectionOptionFunc {
	return []gouroboros.ConnectionOptionFunc{
		gouroboros.WithNetworkMagic(networkMagic),
		gouroboros.WithNodeToNode(false),
		gouroboros.WithChainSyncConfig(
			ochainsync.NewConfig(chainsyncOpts...),
		),
		gouroboros.WithLocalStateQueryConfig(
			olocalstatequery.NewConfig(localstatequeryOpts...),
		),
		gouroboros.WithLocalTxMonitorConfig(
			olocaltxmonitor.NewConfig(localtxmonitorOpts...),
		),
		gouroboros.WithLocalTxSubmissionConfig(
			olocaltxsubmission.NewConfig(localtxsubmissionOpts...),
		),
	}
}

// NodeToNodeServerOpts returns server options for node-to-node connections.
func NodeToNodeServerOpts(
	peerSharing bool,
	networkMagic uint32,
	peersharingOpts []opeersharing.PeerSharingOptionFunc,
	txsubmissionOpts []otxsubmission.TxSubmissionOptionFunc,
	chainsyncOpts []ochainsync.ChainSyncOptionFunc,
	blockfetchOpts []oblockfetch.BlockFetchOptionFunc,
) []gouroboros.ConnectionOptionFunc {
	opts := []gouroboros.ConnectionOptionFunc{
		gouroboros.WithNetworkMagic(networkMagic),
		gouroboros.WithNodeToNode(true),
		gouroboros.WithTxSubmissionConfig(
			otxsubmission.NewConfig(txsubmissionOpts...),
		),
		gouroboros.WithChainSyncConfig(
			ochainsync.NewConfig(chainsyncOpts...),
		),
		gouroboros.WithBlockFetchConfig(
			oblockfetch.NewConfig(blockfetchOpts...),
		),
	}
	if peerSharing {
		opts = append(
			opts,
			gouroboros.WithPeerSharingConfig(
				opeersharing.NewConfig(peersharingOpts...),
			),
		)
	}
	return opts
}

// OutboundOpts returns connection options for outbound connections.
func OutboundOpts(
	networkMagic uint32,
	enablePeerSharing bool,
	peersharingClientOpts []opeersharing.PeerSharingOptionFunc,
	peersharingServerOpts []opeersharing.PeerSharingOptionFunc,
	txsubmissionClientOpts []otxsubmission.TxSubmissionOptionFunc,
	txsubmissionServerOpts []otxsubmission.TxSubmissionOptionFunc,
	chainsyncClientOpts []ochainsync.ChainSyncOptionFunc,
	chainsyncServerOpts []ochainsync.ChainSyncOptionFunc,
	blockfetchClientOpts []oblockfetch.BlockFetchOptionFunc,
	blockfetchServerOpts []oblockfetch.BlockFetchOptionFunc,
) []gouroboros.ConnectionOptionFunc {
	opts := []gouroboros.ConnectionOptionFunc{
		gouroboros.WithNetworkMagic(networkMagic),
		gouroboros.WithNodeToNode(true),
		gouroboros.WithTxSubmissionConfig(
			otxsubmission.NewConfig(
				append(
					txsubmissionClientOpts,
					txsubmissionServerOpts...,
				)...,
			),
		),
		gouroboros.WithChainSyncConfig(
			ochainsync.NewConfig(
				append(
					chainsyncClientOpts,
					chainsyncServerOpts...,
				)...,
			),
		),
		gouroboros.WithBlockFetchConfig(
			oblockfetch.NewConfig(
				append(
					blockfetchClientOpts,
					blockfetchServerOpts...,
				)...,
			),
		),
	}
	if enablePeerSharing {
		opts = append(
			opts,
			gouroboros.WithPeerSharingConfig(
				opeersharing.NewConfig(
					append(
						peersharingClientOpts,
						peersharingServerOpts...,
					)...,
				),
			),
		)
	}
	return opts
}

// ConfigureListeners configures listeners with the appropriate connection options.
func ConfigureListeners(
	listeners []connmanager.ListenerConfig,
	nodeToClientOpts []gouroboros.ConnectionOptionFunc,
	nodeToNodeOpts []gouroboros.ConnectionOptionFunc,
) []connmanager.ListenerConfig {
	tmpListeners := make([]connmanager.ListenerConfig, len(listeners))
	for idx, l := range listeners {
		if l.UseNtC {
			l.ConnectionOpts = nodeToClientOpts
		} else {
			l.ConnectionOpts = nodeToNodeOpts
		}
		tmpListeners[idx] = l
	}
	return tmpListeners
}

// GetNetworkMagic returns the network magic for the given network name.
func GetNetworkMagic(networkName string) (uint32, error) {
	network, ok := gouroboros.NetworkByName(networkName)
	if !ok {
		return 0, fmt.Errorf("unknown network name: %s", networkName)
	}
	return network.NetworkMagic, nil
}

// GetNetwork returns the network configuration for the given network name.
func GetNetwork(networkName string) (*gouroboros.Network, error) {
	network, ok := gouroboros.NetworkByName(networkName)
	if !ok {
		return nil, fmt.Errorf("unknown network name: %s", networkName)
	}
	return &network, nil
}
