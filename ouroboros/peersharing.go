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
	"time"

	"github.com/blinklabs-io/dingo/peergov"
	opeersharing "github.com/blinklabs-io/gouroboros/protocol/peersharing"
)

const defaultPeersToRequest = 5

func (o *Ouroboros) peerSharingConfig() opeersharing.Config {
	return opeersharing.NewConfig(o.peersharingConnOpts()...)
}

func (o *Ouroboros) peersharingConnOpts() []opeersharing.PeerSharingOptionFunc {
	opts := append(
		[]opeersharing.PeerSharingOptionFunc{},
		o.peersharingClientConnOpts()...,
	)
	opts = append(opts, o.peersharingServerConnOpts()...)
	opts = append(opts, opeersharing.WithLocalDisabled(!o.config.PeerSharing))
	return opts
}

func (o *Ouroboros) peersharingServerConnOpts() []opeersharing.PeerSharingOptionFunc {
	return []opeersharing.PeerSharingOptionFunc{
		opeersharing.WithShareRequestFunc(
			o.instrumentPeersharingShareRequest(o.peersharingShareRequest),
		),
	}
}

func (o *Ouroboros) peersharingClientConnOpts() []opeersharing.PeerSharingOptionFunc {
	return []opeersharing.PeerSharingOptionFunc{
		opeersharing.WithShareRequestFunc(
			o.instrumentPeersharingShareRequest(o.peersharingClientRequest),
		),
	}
}

func (o *Ouroboros) instrumentPeersharingShareRequest(
	fn func(opeersharing.CallbackContext, int) ([]opeersharing.PeerAddress, error),
) func(opeersharing.CallbackContext, int) ([]opeersharing.PeerAddress, error) {
	return func(
		ctx opeersharing.CallbackContext,
		amount int,
	) ([]opeersharing.PeerAddress, error) {
		start := time.Now()
		addrs, err := fn(ctx, amount)
		o.recordProtocolMessage("peersharing", err, time.Since(start))
		return addrs, err
	}
}

func (o *Ouroboros) peersharingClientRequest(
	ctx opeersharing.CallbackContext,
	amount int,
) ([]opeersharing.PeerAddress, error) {
	// This callback is intentionally a no-op stub.
	// Peer requests are driven explicitly by the reconcile loop via RequestPeersFromPeer,
	// not through the protocol's automatic peer sharing callbacks.
	return []opeersharing.PeerAddress{}, nil
}

func (o *Ouroboros) peersharingShareRequest(
	ctx opeersharing.CallbackContext,
	amount int,
) ([]opeersharing.PeerAddress, error) {
	// If PeerGov isn't wired yet, don't share any peers rather than panic
	if o.peerGov == nil {
		return []opeersharing.PeerAddress{}, nil
	}
	if amount <= 0 {
		return []opeersharing.PeerAddress{}, nil
	}

	peers := make([]opeersharing.PeerAddress, 0, amount)
	for _, peer := range o.peerGov.GetPeers() {
		if !peer.Sharable {
			continue
		}
		if len(peers) >= amount {
			break
		}
		address := peer.NormalizedAddress
		if address == "" {
			address = peer.Address
		}
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			o.config.Logger.Debug(
				"failed to split peer address, skipping",
				"address", address,
				"error", err,
			)
			continue
		}
		ip := net.ParseIP(host)
		if ip == nil {
			o.config.Logger.Debug(
				"peer address has no serializable IP, skipping",
				"address", address,
			)
			continue
		}
		portNum, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			o.config.Logger.Debug(
				"failed to parse peer port, skipping",
				"address", address,
				"error", err,
			)
			continue
		}
		o.config.Logger.Debug(
			"adding peer for sharing: " + peer.Address,
		)
		peers = append(peers, opeersharing.PeerAddress{
			IP:   ip,
			Port: uint16(portNum),
		},
		)
	}
	return peers, nil
}

func (o *Ouroboros) RequestPeersFromPeer(peer *peergov.Peer) []string {
	if peer == nil || peer.Connection == nil {
		return nil
	}
	if o.connManager == nil {
		o.config.Logger.Debug("ConnManager not available")
		return nil
	}
	conn := o.connManager.GetConnectionById(peer.Connection.Id)
	if conn == nil {
		return nil
	}
	// Skip peers that didn't advertise willingness to share. Sending
	// MsgShareRequest to a remote with PeerSharing disabled (or on a
	// negotiated version that doesn't carry mini-protocol 10) triggers
	// UnknownMiniProtocol on the remote muxer and resets the connection.
	_, versionData := conn.ProtocolVersion()
	if versionData == nil || !versionData.PeerSharing() {
		return nil
	}
	// Get the peer sharing client
	ps := conn.PeerSharing()
	if ps == nil || ps.Client == nil {
		o.config.Logger.Debug(
			"peer sharing client not available",
			"peer",
			peer.Address,
		)
		return nil
	}
	// Request 5 peers
	peers, err := ps.Client.GetPeers(defaultPeersToRequest)
	if err != nil {
		o.config.Logger.Debug(
			"failed to request peers",
			"error",
			err,
			"peer",
			peer.Address,
		)
		return nil
	}
	return o.peerSharingReplyAddresses(peers, defaultPeersToRequest)
}

// peerSharingReplyAddresses converts a peer-sharing reply into peer-governor
// candidate addresses.
//
// At most `requested` entries are examined. A reply longer than that violates
// the request, and walking it would let a remote turn one 5-peer request into
// an arbitrary number of peergov.AddPeer calls, each of which resolves the
// address (a DNS lookup for a non-literal host) and scans the peer list.
// Bounding the entries examined, rather than collecting `requested` valid ones
// from an unbounded reply, keeps the work constant; a peer that pads its reply
// with unusable entries spends its own slots doing so.
//
// Entries that cannot serve as a peer address are dropped. peergov.AddPeer
// applies the same routability policy, but it treats a host that does not
// parse as an IP as a routable hostname, so a malformed entry rendered as
// "<nil>:3001" would be accepted there and sent to a DNS lookup. Rejecting it
// here keeps every emitted candidate an IP literal.
func (o *Ouroboros) peerSharingReplyAddresses(
	peers []opeersharing.PeerAddress,
	requested int,
) []string {
	if requested <= 0 {
		return nil
	}
	if len(peers) > requested {
		o.config.Logger.Debug(
			"peer sharing reply exceeds requested count, truncating",
			"requested", requested,
			"received", len(peers),
		)
		peers = peers[:requested]
	}
	addrs := make([]string, 0, len(peers))
	for _, p := range peers {
		// Reject before net.IP.String(), which renders a nil or
		// wrong-length address as "<nil>" or "?<hex>" rather than failing.
		if p.IP.To16() == nil {
			o.config.Logger.Debug(
				"shared peer has no usable IP address, skipping",
				"len", len(p.IP),
			)
			continue
		}
		if p.Port == 0 {
			o.config.Logger.Debug(
				"shared peer has no usable port, skipping",
				"ip", p.IP.String(),
			)
			continue
		}
		if !peergov.IsRoutableIP(p.IP) {
			o.config.Logger.Debug(
				"shared peer address is not globally routable, skipping",
				"ip", p.IP.String(),
			)
			continue
		}
		addr := net.JoinHostPort(p.IP.String(), strconv.Itoa(int(p.Port)))
		addrs = append(addrs, addr)
		o.config.Logger.Debug("collected peer from sharing", "addr", addr)
	}
	return addrs
}
