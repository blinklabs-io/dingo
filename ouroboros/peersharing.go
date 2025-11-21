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

package ouroboros

import (
	"net"
	"strconv"

	opeersharing "github.com/blinklabs-io/gouroboros/protocol/peersharing"
)

func (o *Ouroboros) peersharingServerConnOpts() []opeersharing.PeerSharingOptionFunc {
	return []opeersharing.PeerSharingOptionFunc{
		opeersharing.WithShareRequestFunc(o.peersharingShareRequest),
	}
}

func (o *Ouroboros) peersharingClientConnOpts() []opeersharing.PeerSharingOptionFunc {
	// We don't provide any client options, but we have this here for consistency
	return []opeersharing.PeerSharingOptionFunc{}
}

func (o *Ouroboros) peersharingShareRequest(
	ctx opeersharing.CallbackContext,
	amount int,
) ([]opeersharing.PeerAddress, error) {
	// If PeerGov isn't wired yet, don't share any peers rather than panic
	if o.PeerGov == nil {
		return []opeersharing.PeerAddress{}, nil
	}

	peers := []opeersharing.PeerAddress{}
	shared := 0
	for _, peer := range o.PeerGov.GetPeers() {
		if !peer.Sharable {
			continue
		}
		if shared >= amount {
			break
		}
		host, port, err := net.SplitHostPort(peer.Address)
		if err != nil {
			// Skip on error
			o.config.Logger.Debug("failed to split peer address, skipping")
			continue
		}
		portNum, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			// Skip on error
			o.config.Logger.Debug("failed to parse peer port, skipping")
			continue
		}
		o.config.Logger.Debug(
			"adding peer for sharing: " + peer.Address,
		)
		peers = append(peers, opeersharing.PeerAddress{
			IP:   net.ParseIP(host),
			Port: uint16(portNum),
		},
		)
		shared++
	}
	return peers, nil
}
