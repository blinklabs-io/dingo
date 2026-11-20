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
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/stretchr/testify/require"
)

// A receiver ends the connection on a second offer of the same body or
// closure, so the server offers each part of an endorser block at most once
// per connection however many times it is queued.
func TestLeiosNotifyServerOffersEachPartOncePerConnection(t *testing.T) {
	t.Parallel()

	point := ocommon.NewPoint(42, make([]byte, lcommon.Blake2b256Size))
	log := newLeiosForgedEBLog()
	log.registerConn("downstream", nil, nil)

	blockPoint, txPoint := point, point
	log.append(leiosForgedEBEntry{point: &blockPoint, size: 100})
	log.append(leiosForgedEBEntry{txOffer: &txPoint})
	for _, want := range []uint8{
		oleiosnotify.MessageTypeBlockOffer,
		oleiosnotify.MessageTypeBlockTxsOffer,
	} {
		entry, _ := log.next("downstream")
		require.NotNil(t, entry)
		require.Equal(t, want, leiosForgedEBOffer(entry).Type())
		log.complete("downstream", nil, true)
	}

	repeatBlock, repeatTx := point, point
	log.append(leiosForgedEBEntry{point: &repeatBlock, size: 100})
	log.append(leiosForgedEBEntry{txOffer: &repeatTx})
	entry, _ := log.next("downstream")
	require.Nil(
		t,
		entry,
		"a part already offered on this connection was offered again",
	)

	log.registerConn("late", nil, nil)
	lateBlock := point
	log.append(leiosForgedEBEntry{point: &lateBlock, size: 100})
	entry, _ = log.next("late")
	require.NotNil(t, entry, "another connection has not been offered it yet")
}

// A forged endorser block that a peer then offers back marks it for relay.
// The relay must not offer it again on a connection the forger already
// offered it on.
func TestLeiosForgedEndorserBlockOfferedBackIsNotReoffered(t *testing.T) {
	t.Parallel()

	txRaw := testDijkstraTx(t, 0)
	ebRaw, err := cbor.Encode(&lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{{
			TransactionHash: lcommon.Blake2b256Hash(txRaw),
			TransactionSize: uint32(
				len(txRaw),
			), //nolint:gosec // short test transaction
		}},
	})
	require.NoError(t, err)
	ebHash := lcommon.Blake2b256Hash(ebRaw)
	point := ocommon.NewPoint(42, ebHash.Bytes())

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	o.leiosEBLog.registerConn("downstream", nil, nil)

	announceTestEndorserBlock(t, o, point.Slot, ebHash, len(ebRaw))
	require.NoError(t, o.BroadcastEndorserBlock(
		point.Slot, point.Hash, ebRaw, [][]byte{txRaw},
	))
	offered := 0
	for {
		entry, _ := o.leiosEBLog.next("downstream")
		if entry == nil {
			break
		}
		if entry.isEndorserBlockOffer() {
			offered++
		}
		o.leiosEBLog.complete("downstream", nil, true)
	}
	require.Equal(t, 2, offered, "the forged body and closure offers")

	o.markLeiosEndorserBlockRelayOffer(point)
	entry, _ := o.leiosEBLog.next("downstream")
	require.Nil(t, entry, "the forged endorser block was offered again")
}

// leiosNotifyPipe connects a receiver's LeiosNotify client to a sender's
// LeiosNotify server over an in-memory connection and starts the client.
func leiosNotifyPipe(
	t *testing.T,
	sender, receiver *Ouroboros,
	receiverConnections *connmanager.ConnectionManager,
	port int,
) (client, server *gouroboros.Connection) {
	t.Helper()
	clientCfg := oleiosnotify.NewConfig(receiver.leiosnotifyClientConnOpts()...)
	serverCfg := oleiosnotify.NewConfig(sender.leiosnotifyServerConnOpts()...)
	// Every net.Pipe end reports the same address, so two pipes would share
	// a connection ID without their own.
	pipeLeft, pipeRight := net.Pipe()
	receiverAddr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}
	senderAddr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 2), Port: 3001}
	left := leiosFixedAddrConn{
		Conn:   pipeLeft,
		local:  receiverAddr,
		remote: senderAddr,
	}
	right := leiosFixedAddrConn{
		Conn:   pipeRight,
		local:  senderAddr,
		remote: receiverAddr,
	}
	type result struct {
		conn *gouroboros.Connection
		err  error
	}
	clientCh := make(chan result, 1)
	serverCh := make(chan result, 1)
	go func() {
		conn, err := gouroboros.NewConnection(
			gouroboros.WithConnection(left),
			gouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
			gouroboros.WithNodeToNode(true),
			gouroboros.WithLeiosNotifyConfig(clientCfg),
		)
		clientCh <- result{conn: conn, err: err}
	}()
	go func() {
		conn, err := gouroboros.NewConnection(
			gouroboros.WithConnection(right),
			gouroboros.WithServer(true),
			gouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
			gouroboros.WithNodeToNode(true),
			gouroboros.WithLeiosNotifyConfig(serverCfg),
		)
		serverCh <- result{conn: conn, err: err}
	}()
	clientResult := testutil.RequireReceive(
		t, clientCh, 5*time.Second, "LeiosNotify client handshake",
	)
	serverResult := testutil.RequireReceive(
		t, serverCh, 5*time.Second, "LeiosNotify server handshake",
	)
	require.NoError(t, clientResult.err)
	require.NoError(t, serverResult.err)
	client, server = clientResult.conn, serverResult.conn
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
		_ = left.Close()
		_ = right.Close()
	})
	require.True(t, receiverConnections.AddConnection(client, false, ""))
	require.NoError(t, receiver.leiosnotifyClientStart(client.Id()))
	serverKey := leiosConnectionIdString(server.Id())
	testutil.WaitForCondition(t, func() bool {
		sender.leiosEBLog.mu.Lock()
		defer sender.leiosEBLog.mu.Unlock()
		_, registered := sender.leiosEBLog.cursors[serverKey]
		return registered
	}, 2*time.Second, "notification request to register the server cursor")
	return client, server
}

// An invalid offer received on a live connection closes that connection and
// leaves the receiver's other LeiosNotify connections serving.
func TestLeiosInvalidOfferClosesOnlyItsLiveConnection(t *testing.T) {
	t.Parallel()

	receiverConnections := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	receiver := newOuroboros(OuroborosConfig{ConnManager: receiverConnections})
	offender := newOuroboros(OuroborosConfig{})
	bystander := newOuroboros(OuroborosConfig{})
	offenderClient, _ := leiosNotifyPipe(
		t, offender, receiver, receiverConnections, 40001,
	)
	bystanderClient, _ := leiosNotifyPipe(
		t, bystander, receiver, receiverConnections, 40002,
	)

	// Never announced over this connection. Without the offer check the
	// receiver would only remember it, its election being unobserved.
	point := ocommon.NewPoint(42, make([]byte, lcommon.Blake2b256Size))
	offender.leiosEBLog.append(leiosForgedEBEntry{point: &point, size: 100})
	// The connection manager is the only reader of a connection's error
	// channel and removes the connection once it fails.
	testutil.WaitForCondition(t, func() bool {
		return receiverConnections.GetConnectionById(offenderClient.Id()) == nil
	}, 5*time.Second, "the offending connection to close")

	for voterID := range uint64(2) {
		bystander.EnqueueLeiosPrototypeVote(lcommon.LeiosPrototypeVote{
			VoterId:       voterID,
			VoteSignature: make([]byte, lcommon.LeiosBlsSignatureSize),
		})
	}
	testutil.WaitForCondition(t, func() bool {
		bystander.leiosEBLog.mu.Lock()
		defer bystander.leiosEBLog.mu.Unlock()
		return bystander.leiosEBLog.base == 2 &&
			len(bystander.leiosEBLog.items) == 0
	}, 2*time.Second, "the other connection to keep consuming notifications")
	require.NotNil(
		t,
		receiverConnections.GetConnectionById(bystanderClient.Id()),
		"the other connection closed",
	)
}
