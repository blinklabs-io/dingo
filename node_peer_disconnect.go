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

package dingo

import (
	"context"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

// queuePeerDisconnect hands a Genesis disconnect to the node-owned worker and
// returns at once. The hooks run on a chain-sync callback, which must not
// wait for networkingCoreMu: Restore and Truncate hold it while stopping the
// connection manager waits for callbacks to return, and a hook's Close can
// wait for the very callback that reported the peer.
//
// locked runs under networkingCoreMu and returns the close to run after the
// worker releases it, or nil. A report already queued under key is replaced,
// so the queue stays bounded by the number of connections.
func (n *Node) queuePeerDisconnect(key string, locked func() func()) {
	n.peerDisconnectMu.Lock()
	if n.peerDisconnectPending == nil {
		n.peerDisconnectPending = make(map[string]func() func())
	}
	if _, queued := n.peerDisconnectPending[key]; !queued {
		n.peerDisconnectOrder = append(n.peerDisconnectOrder, key)
	}
	n.peerDisconnectPending[key] = locked
	wake := n.peerDisconnectWake
	n.peerDisconnectMu.Unlock()
	if wake == nil {
		// Not started yet: startPeerDisconnectWorker wakes for what is queued.
		return
	}
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (n *Node) nextPeerDisconnect() (func() func(), bool) {
	n.peerDisconnectMu.Lock()
	defer n.peerDisconnectMu.Unlock()
	if len(n.peerDisconnectOrder) == 0 {
		return nil, false
	}
	key := n.peerDisconnectOrder[0]
	n.peerDisconnectOrder = n.peerDisconnectOrder[1:]
	locked := n.peerDisconnectPending[key]
	delete(n.peerDisconnectPending, key)
	return locked, true
}

// startPeerDisconnectWorker starts the worker that runs queued Genesis
// disconnects until ctx is cancelled.
func (n *Node) startPeerDisconnectWorker(ctx context.Context) {
	n.peerDisconnectMu.Lock()
	defer n.peerDisconnectMu.Unlock()
	if n.peerDisconnectWorkerDone != nil {
		return
	}
	wake := make(chan struct{}, 1)
	if len(n.peerDisconnectOrder) > 0 {
		wake <- struct{}{}
	}
	done := make(chan struct{})
	n.peerDisconnectWake = wake
	n.peerDisconnectWorkerDone = done
	go n.runPeerDisconnectWorker(ctx, wake, done)
}

// waitPeerDisconnectWorker waits for the worker to exit. The caller must
// cancel the worker's context first.
func (n *Node) waitPeerDisconnectWorker() {
	n.peerDisconnectMu.Lock()
	done := n.peerDisconnectWorkerDone
	n.peerDisconnectMu.Unlock()
	if done != nil {
		<-done
	}
}

func (n *Node) runPeerDisconnectWorker(
	ctx context.Context,
	wake <-chan struct{},
	done chan<- struct{},
) {
	defer close(done)
	for {
		select {
		case <-ctx.Done():
			return
		case <-wake:
		}
		for ctx.Err() == nil {
			locked, ok := n.nextPeerDisconnect()
			if !ok {
				break
			}
			// The worker is no connection's callback, so it may wait here for
			// a Restore and its Close may wait for the target's callbacks:
			// those only queue, and never wait for the worker.
			n.networkingCoreMu.Lock()
			closeConn := locked()
			n.networkingCoreMu.Unlock()
			if closeConn != nil {
				closeConn()
			}
		}
	}
}

// peerConnCloser looks up the connection with id in the current connection
// manager and returns the close to run once networkingCoreMu is released, or
// nil when there is nothing to close. A non-empty reason is recorded as the
// connection's local close reason. The caller holds networkingCoreMu.
func (n *Node) peerConnCloser(id ouroboros.ConnectionId, reason string) func() {
	if n.closePeerConnOverride != nil {
		closeConn := n.closePeerConnOverride
		return func() { closeConn(id, reason) }
	}
	cm := n.connManager
	if cm == nil {
		return nil
	}
	conn := cm.GetConnectionById(id)
	if conn == nil {
		return nil
	}
	if reason != "" {
		return func() { _ = cm.CloseConnectionWithReason(conn, reason) }
	}
	return func() { _ = conn.Close() }
}
