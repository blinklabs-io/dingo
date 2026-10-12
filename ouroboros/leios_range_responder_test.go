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

package ouroboros

import (
	"testing"
	"time"

	gouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/stretchr/testify/require"
)

// newLeiosFetchServerPeer builds a muxerServerPeer driving Dingo's real
// leios-fetch server config (leiosfetchServerConnOpts, instrumentation
// wrappers included), so the assertions are about what Dingo actually puts
// on the wire rather than about what a callback returns.
func newLeiosFetchServerPeer(t *testing.T, o *Ouroboros) *muxerServerPeer {
	t.Helper()
	opts, peer := newMuxerServerPeer(t)
	cfg := oleiosfetch.NewConfig(o.leiosfetchServerConnOpts()...)
	server := oleiosfetch.NewServer(opts, &cfg)
	peer.start(t, server)
	return peer
}

// TestLeiosFetchBlockRangeRequestIsDeclined is the Dingo-owned half of the
// backfill stall fix. Dingo registers a BlockRangeRequestFunc but does not
// serve ranges. gouroboros reads a nil return from that callback as "an async
// process was started that will send NextBlockAndTxsInRange /
// LastBlockAndTxsInRange", so returning nil without sending anything left this
// server holding leios-fetch agency in StateBlockRange forever.
//
// A peer in that state is wedged permanently: its protocol send loop waits on
// agency the state map only returns when the missing response arrives, so it
// can never issue another leios-fetch request on the connection and has no way
// to detect the condition. Dingo must decline observably instead.
//
// There is no absence reply for a range request, so declining means a
// connection-level protocol error. That error is what this asserts, bounded by
// a timeout because the defect being fixed is a park.
func TestLeiosFetchBlockRangeRequestIsDeclined(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	peer := newLeiosFetchServerPeer(t, o)

	peer.send(t, oleiosfetch.ProtocolId, oleiosfetch.NewMsgBlockRangeRequest(
		ocommon.NewPoint(3623, make([]byte, lcommon.Blake2b256Size)),
		ocommon.NewPoint(3700, make([]byte, lcommon.Blake2b256Size)),
	))

	select {
	case err := <-peer.errChan:
		require.Error(t, err)
		require.Contains(t, err.Error(), "block range")
	case <-time.After(5 * time.Second):
		t.Fatal(
			"leios-fetch BlockRangeRequest was left pending instead of declined",
		)
	}
}

// TestLeiosFetchUnavailableBlockTxsFailsBearer is the absence case for the
// test above. The leios-fetch protocol has no absence reply for a
// BlockTxsRequest, so when Dingo's server callback
// (leiosfetchServerBlockTxsRequest) reports ErrBlockTxsNotFound, gouroboros's
// server returns the error and fails the bearer, as for the undeclined
// BlockRangeRequest above.
func TestLeiosFetchUnavailableBlockTxsFailsBearer(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	peer := newLeiosFetchServerPeer(t, o)

	// No endorser block is stored, so the callback reports
	// ErrBlockTxsNotFound and gouroboros's server fails the bearer.
	peer.send(t, oleiosfetch.ProtocolId, oleiosfetch.NewMsgBlockTxsRequest(
		ocommon.NewPoint(3623, make([]byte, lcommon.Blake2b256Size)),
		map[uint16]uint64{0: 1 << 63},
	))

	select {
	case err := <-peer.errChan:
		require.Error(t, err)
		require.Contains(t, err.Error(), "endorser block")
	case <-time.After(5 * time.Second):
		t.Fatal(
			"leios-fetch BlockTxsRequest for an unavailable endorser block " +
				"was left pending instead of failing the bearer",
		)
	}
}

// TestLeiosFetchBlockRangeRequestTearsDownConnection drives a block-range
// request through a real gouroboros connection built with Dingo's leios-fetch
// configuration. The decline must surface as a protocol error on the shared
// bearer and the connection must then shut down, so the peer is not left
// wedged and can be replaced.
func TestLeiosFetchBlockRangeRequestTearsDownConnection(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	mockConn := ouroboros_mock.NewConnection(
		ouroboros_mock.ProtocolRoleServer,
		[]ouroboros_mock.ConversationEntry{
			ouroboros_mock.ConversationEntryHandshakeRequestOutput,
			ouroboros_mock.ConversationEntryHandshakeNtNResponseInput,
			ouroboros_mock.ConversationEntryOutput{
				ProtocolId: oleiosfetch.ProtocolId,
				Messages: []protocol.Message{
					oleiosfetch.NewMsgBlockRangeRequest(
						ocommon.NewPoint(
							3412,
							make([]byte, lcommon.Blake2b256Size),
						),
						ocommon.NewPoint(
							3500,
							make([]byte, lcommon.Blake2b256Size),
						),
					),
				},
			},
		},
	)
	conn, err := gouroboros.New(
		gouroboros.WithConnection(mockConn),
		gouroboros.WithServer(true),
		gouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
		gouroboros.WithNodeToNode(true),
		gouroboros.WithLeiosFetchConfig(
			oleiosfetch.NewConfig(o.leiosfetchServerConnOpts()...),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	timeout := time.After(5 * time.Second)
	select {
	case err := <-conn.ErrorChan():
		require.Error(t, err)
		require.Contains(t, err.Error(), "block range")
	case <-timeout:
		t.Fatal("block range request did not fail the connection")
	}
	// The error channel closes once the connection has shut down.
	for {
		select {
		case _, open := <-conn.ErrorChan():
			if !open {
				return
			}
		case <-timeout:
			t.Fatal("connection stayed open after the block range failure")
		}
	}
}
