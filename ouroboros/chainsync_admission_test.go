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
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/muxer"
	"github.com/blinklabs-io/gouroboros/protocol"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// sendChainsyncResponse writes msg to a chainsync client as a responder
// segment, the direction a server's replies travel.
func sendChainsyncResponse(
	t *testing.T,
	peer *muxerServerPeer,
	msg protocol.Message,
) {
	t.Helper()
	data, err := cbor.Encode(msg)
	require.NoError(t, err)
	segment := muxer.NewSegment(ochainsync.ProtocolIdNtN, data, true)
	require.NotNil(t, segment)
	buf := &bytes.Buffer{}
	require.NoError(
		t,
		binary.Write(buf, binary.BigEndian, segment.SegmentHeader),
	)
	_, err = buf.Write(segment.Payload)
	require.NoError(t, err)
	_, err = peer.peerConn.Write(buf.Bytes())
	require.NoError(t, err)
}

// A roll-forward callback runs on the protocol's receive loop, and the
// protocol's DoneChan closes only after that loop returns. An admission wait
// bounded by DoneChan therefore cannot be released by stopping the client
// while the wait holds the callback. Stopping must cancel the admission
// context, so the callback returns and the client finishes stopping.
func TestChainsyncAdmissionContextCancelledByClientStopDuringCallback(
	t *testing.T,
) {
	t.Parallel()
	opts, peer := newMuxerServerPeer(t)
	opts.Mode = protocol.ProtocolModeNodeToNode
	opts.Role = protocol.ProtocolRoleClient

	entered := make(chan struct{})
	waitResult := make(chan error, 1)
	release := make(chan struct{})
	defer close(release)
	cfg := ochainsync.NewConfig(
		ochainsync.WithPipelineLimit(1),
		ochainsync.WithRollForwardRawFunc(func(
			ctx ochainsync.CallbackContext,
			_ uint,
			_ []byte,
			_ ochainsync.Tip,
		) error {
			admissionCtx, cancel := chainsyncAdmissionContext(ctx)
			defer cancel()
			close(entered)
			select {
			case <-admissionCtx.Done():
				waitResult <- admissionCtx.Err()
				return admissionCtx.Err()
			case <-release:
				return nil
			}
		}),
	)
	client := ochainsync.NewClient(opts, &cfg)
	client.Start()
	peer.muxer.Start()

	go func() {
		_ = client.Sync([]ocommon.Point{ocommon.NewPointOrigin()})
	}()
	peer.readMessage(t, 5*time.Second) // FindIntersect
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, bytes.Repeat([]byte{0xaa}, 32)),
		BlockNumber: 1,
	}
	sendChainsyncResponse(
		t,
		peer,
		ochainsync.NewMsgIntersectFound(ocommon.NewPointOrigin(), tip),
	)
	peer.readMessage(t, 5*time.Second) // RequestNext
	rollForward, err := ochainsync.NewMsgRollForwardNtN(
		gledger.BlockHeaderTypeBabbage,
		0,
		[]byte{0x82, 0x01, 0x02},
		tip,
	)
	require.NoError(t, err)
	sendChainsyncResponse(t, peer, rollForward)

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("roll-forward callback was not invoked")
	}

	stopped := make(chan error, 1)
	go func() { stopped <- client.Stop() }()

	select {
	case err := <-waitResult:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("stopping the client did not cancel the held admission wait")
	}
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("client stop did not complete after the wait was cancelled")
	}
}

// The owning connection's shutdown signal also cancels the admission context.
func TestChainsyncAdmissionContextCancelledByConnectionShutdown(
	t *testing.T,
) {
	t.Parallel()
	connDone := make(chan any)
	admissionCtx, cancel := chainsyncAdmissionContext(
		ochainsync.CallbackContext{ConnectionDoneChan: connDone},
	)
	defer cancel()
	require.NoError(t, admissionCtx.Err())
	close(connDone)
	select {
	case <-admissionCtx.Done():
		require.ErrorIs(t, admissionCtx.Err(), context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("connection shutdown did not cancel the admission context")
	}
}
