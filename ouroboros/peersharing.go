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
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/peergov"
	opeersharing "github.com/blinklabs-io/gouroboros/protocol/peersharing"
)

const defaultPeersToRequest = 5

const (
	// peerShareStickyTime is how long one salt orders the peers offered to
	// ShareRequests before it is regenerated. It matches the reference
	// node's ps_POLICY_PEER_SHARE_STICKY_TIME.
	peerShareStickyTime = 823 * time.Second
	// peerShareMaxPeers caps a reply regardless of the requested amount. It
	// matches the reference node's ps_POLICY_PEER_SHARE_MAX_PEERS.
	peerShareMaxPeers = 10
)

// peerShareSalter holds the salt that orders sharable peers for
// ShareRequest replies. The same salt is used until peerShareStickyTime has
// elapsed, so repeated requests inside a window see a stable answer, and a
// fresh salt then rotates which peers are offered. The zero value is ready
// for use; now and newSalt are test seams that default to time.Now and
// crypto/rand.
type peerShareSalter struct {
	mu       sync.Mutex
	salt     [16]byte
	resaltAt time.Time
	now      func() time.Time
	newSalt  func() [16]byte
}

// current returns the salt for the window containing the present time,
// regenerating it when the previous window has ended.
func (s *peerShareSalter) current() [16]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now
	if s.now != nil {
		now = s.now
	}
	t := now()
	if s.resaltAt.IsZero() || !t.Before(s.resaltAt) {
		if s.newSalt != nil {
			s.salt = s.newSalt()
		} else {
			// crypto/rand.Read never returns an error and always fills
			// the buffer.
			_, _ = rand.Read(s.salt[:])
		}
		s.resaltAt = t.Add(peerShareStickyTime)
	}
	return s.salt
}

// saltedRank orders an address under a salt.
func saltedRank(salt [16]byte, address string) uint64 {
	h := sha256.New()
	_, _ = h.Write(salt[:])
	_, _ = h.Write([]byte(address))
	var sum [sha256.Size]byte
	return binary.BigEndian.Uint64(h.Sum(sum[:0]))
}

func (o *Ouroboros) peerSharingConfig() opeersharing.Config {
	return opeersharing.NewConfig(o.peersharingConnOpts()...)
}

// peersharingConnOpts wires the single response-side callback the
// PeerSharing protocol exposes: opeersharing.Config carries exactly one
// ShareRequestFunc slot, which answers an incoming ShareRequest from a
// remote peer. There is no separate outbound-request slot to configure here;
// this node's own requests for peers are driven explicitly by the reconcile
// loop via RequestPeersFromPeer, not through a protocol callback. Registering
// WithShareRequestFunc more than once would silently make the last
// registration win, so ownership of that single slot stays explicit here
// rather than split across a client/server pair that both target it.
func (o *Ouroboros) peersharingConnOpts() []opeersharing.PeerSharingOptionFunc {
	return []opeersharing.PeerSharingOptionFunc{
		opeersharing.WithShareRequestFunc(
			o.instrumentPeersharingShareRequest(o.peersharingShareRequest),
		),
		opeersharing.WithLocalDisabled(!o.config.PeerSharing),
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

	// Collect every valid sharable candidate, order by a salted hash, and
	// then cap. Taking the first peers in list order would answer every
	// request identically and let a requester whose only source is this
	// node never learn the rest.
	type candidate struct {
		key  string
		rank uint64
		addr opeersharing.PeerAddress
	}
	salt := o.peerShareSalt.current()
	knownPeers := o.peerGov.GetPeers()
	candidates := make([]candidate, 0, len(knownPeers))
	for _, peer := range knownPeers {
		if !peer.Sharable {
			continue
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
		if !peergov.IsRoutableIP(ip) {
			o.config.Logger.Debug(
				"peer address is not globally routable, skipping",
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
		if portNum == 0 {
			o.config.Logger.Debug(
				"peer address has no usable port, skipping",
				"address", address,
			)
			continue
		}
		candidates = append(candidates, candidate{
			key:  address,
			rank: saltedRank(salt, address),
			addr: opeersharing.PeerAddress{
				IP:   ip,
				Port: uint16(portNum),
			},
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].rank != candidates[j].rank {
			return candidates[i].rank < candidates[j].rank
		}
		return candidates[i].key < candidates[j].key
	})
	limit := min(amount, peerShareMaxPeers, len(candidates))
	peers := make([]opeersharing.PeerAddress, 0, limit)
	for _, c := range candidates[:limit] {
		o.config.Logger.Debug("adding peer for sharing: " + c.key)
		peers = append(peers, c.addr)
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
// candidate addresses. The reply is bounded to the request so a remote cannot
// amplify one request into unbounded peer-admission work. Invalid, private,
// reserved, and zero-port addresses are discarded before they reach DNS or
// peergov.
func (o *Ouroboros) peerSharingReplyAddresses(
	peers []opeersharing.PeerAddress,
	requested int,
) []string {
	if requested <= 0 {
		return nil
	}
	addrs := make([]string, 0, min(len(peers), requested))
	for _, p := range peers {
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
		if len(addrs) == requested {
			break
		}
	}
	return addrs
}
