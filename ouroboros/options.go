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
	NetworkMainnet = "mainnet"
	NetworkTestnet = "testnet"
	NetworkPreview = "preview"
	NetworkPreprod = "preprod"
	NetworkSancho  = "sancho"
	NetworkDevnet  = "devnet"
)

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

func NodeToNodeServerOpts(
	peerSharing bool,
	networkMagic uint32,
	peersharingOpts []opeersharing.PeerSharingOptionFunc,
	txsubmissionOpts []otxsubmission.TxSubmissionOptionFunc,
	chainsyncOpts []ochainsync.ChainSyncOptionFunc,
	blockfetchOpts []oblockfetch.BlockFetchOptionFunc,
) []gouroboros.ConnectionOptionFunc {
	return []gouroboros.ConnectionOptionFunc{
		gouroboros.WithNetworkMagic(networkMagic),
		gouroboros.WithNodeToNode(true),
		gouroboros.WithPeerSharingConfig(
			opeersharing.NewConfig(peersharingOpts...),
		),
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
}

func OutboundOpts(
	networkMagic uint32,
	peersharingClientOpts []opeersharing.PeerSharingOptionFunc,
	peersharingServerOpts []opeersharing.PeerSharingOptionFunc,
	txsubmissionClientOpts []otxsubmission.TxSubmissionOptionFunc,
	txsubmissionServerOpts []otxsubmission.TxSubmissionOptionFunc,
	chainsyncClientOpts []ochainsync.ChainSyncOptionFunc,
	chainsyncServerOpts []ochainsync.ChainSyncOptionFunc,
	blockfetchClientOpts []oblockfetch.BlockFetchOptionFunc,
	blockfetchServerOpts []oblockfetch.BlockFetchOptionFunc,
) []gouroboros.ConnectionOptionFunc {
	return []gouroboros.ConnectionOptionFunc{
		gouroboros.WithNetworkMagic(networkMagic),
		gouroboros.WithNodeToNode(true),
		gouroboros.WithPeerSharingConfig(
			opeersharing.NewConfig(
				append(
					peersharingClientOpts,
					peersharingServerOpts...,
				)...,
			),
		),
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
}

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

func GetNetworkMagic(networkName string) (uint32, error) {
	network, ok := gouroboros.NetworkByName(networkName)
	if !ok {
		return 0, fmt.Errorf("unknown network name: %s", networkName)
	}
	return network.NetworkMagic, nil
}

func GetNetwork(networkName string) (*gouroboros.Network, error) {
	network, ok := gouroboros.NetworkByName(networkName)
	if !ok {
		return nil, fmt.Errorf("unknown network name: %s", networkName)
	}
	return &network, nil
}
