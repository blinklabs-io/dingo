// Copyright 2024 Blink Labs Software
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

package node

import (
	"fmt"
	"net"
	"strconv"

	opeersharing "github.com/blinklabs-io/gouroboros/protocol/peersharing"
)

func (n *Node) peersharingServerConnOpts() []opeersharing.PeerSharingOptionFunc {
	return []opeersharing.PeerSharingOptionFunc{
		opeersharing.WithShareRequestFunc(n.peersharingShareRequest),
	}
}

func (n *Node) peersharingClientConnOpts() []opeersharing.PeerSharingOptionFunc {
	return []opeersharing.PeerSharingOptionFunc{
		// TODO
	}
}

func (n *Node) peersharingShareRequest(
	ctx opeersharing.CallbackContext,
	amount int,
) ([]opeersharing.PeerAddress, error) {
	peers := []opeersharing.PeerAddress{}
	var cnt int
	for _, peer := range n.outboundConns {
		cnt++
		if cnt > amount {
			break
		}
		if cnt > len(n.outboundConns) {
			break
		}
		if peer.sharable {
			host, port, err := net.SplitHostPort(peer.Address)
			if err != nil {
				// Skip on error
				n.config.logger.Debug("failed to split peer address, skipping")
				continue
			}
			portNum, err := strconv.ParseUint(port, 10, 16)
			if err != nil {
				// Skip on error
				n.config.logger.Debug("failed to parse peer port, skipping")
				continue
			}
			n.config.logger.Debug(
				fmt.Sprintf("adding peer for sharing: %s", peer.Address),
			)
			peers = append(peers, opeersharing.PeerAddress{
				IP:   net.ParseIP(host),
				Port: uint16(portNum),
			},
			)
		}
	}
	return peers, nil
}
