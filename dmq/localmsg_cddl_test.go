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

package dmq

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localmessagenotification"
	"github.com/blinklabs-io/gouroboros/protocol/localmessagesubmission"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The encodings below are written out by hand from dmq-node's
// local-msg-notification.cddl and its codecs in
// DMQ/Protocol/LocalMsgNotification/Codec.hs and
// DMQ/Protocol/LocalMsgSubmission/Codec.hs (release 0.7.2.1), not produced by
// the code under test. The messages travel on the node-to-client socket that
// Dingo serves, so a client built on dmq-node's codecs reads exactly these
// bytes. In CBOR, 0x82 and 0x83 open arrays of two and three, 0x9f opens an
// indefinite array that 0xff closes, 0xf4 is false and 0xf5 is true.

// TestLocalNotificationEncodesAsDmqNodeDoes pins the replies Dingo's
// notification server sends. The blocking reply has no hasMore flag: the
// codec's two-element form is what dmq-node's client decodes today, and a
// three-element one is refused.
func TestLocalNotificationEncodesAsDmqNodeDoes(t *testing.T) {
	t.Parallel()
	golden, goldenBytes := goldenMessage()
	cases := []struct {
		name string
		msg  protocol.Message
		want []byte
	}{
		{
			name: "request blocking",
			msg:  localmessagenotification.NewMsgRequestMessages(true),
			want: []byte{0x82, 0x00, 0xf5},
		},
		{
			name: "request non-blocking",
			msg:  localmessagenotification.NewMsgRequestMessages(false),
			want: []byte{0x82, 0x00, 0xf4},
		},
		{
			name: "non-blocking reply with more",
			msg: localmessagenotification.NewMsgReplyMessagesNonBlocking(
				[]ocommon.DmqMessage{golden}, true,
			),
			want: concat(
				[]byte{0x83, 0x01, 0x9f}, goldenBytes, []byte{0xff, 0xf5},
			),
		},
		{
			name: "non-blocking reply without more",
			msg: localmessagenotification.NewMsgReplyMessagesNonBlocking(
				[]ocommon.DmqMessage{golden}, false,
			),
			want: concat(
				[]byte{0x83, 0x01, 0x9f}, goldenBytes, []byte{0xff, 0xf4},
			),
		},
		{
			name: "empty non-blocking reply",
			msg: localmessagenotification.NewMsgReplyMessagesNonBlocking(
				nil, false,
			),
			want: []byte{0x83, 0x01, 0x9f, 0xff, 0xf4},
		},
		{
			name: "blocking reply",
			msg: localmessagenotification.NewMsgReplyMessagesBlocking(
				[]ocommon.DmqMessage{golden},
			),
			want: concat([]byte{0x82, 0x02, 0x9f}, goldenBytes, []byte{0xff}),
		},
		{
			name: "client done",
			msg:  localmessagenotification.NewMsgClientDone(),
			want: []byte{0x81, 0x03},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := cbor.Encode(tc.msg)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestLocalNotificationDecodesWhatDmqNodeSends feeds Dingo's decoder the
// bytes dmq-node's client writes, so a request dmq-node sends is understood.
func TestLocalNotificationDecodesWhatDmqNodeSends(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		msgType uint
		data    []byte
		check   func(t *testing.T, msg protocol.Message)
	}{
		{
			name:    "request blocking",
			msgType: localmessagenotification.MessageTypeRequestMessages,
			data:    []byte{0x82, 0x00, 0xf5},
			check: func(t *testing.T, msg protocol.Message) {
				got, ok := msg.(*localmessagenotification.MsgRequestMessages)
				require.True(t, ok)
				assert.True(t, got.IsBlocking)
			},
		},
		{
			name:    "request non-blocking",
			msgType: localmessagenotification.MessageTypeRequestMessages,
			data:    []byte{0x82, 0x00, 0xf4},
			check: func(t *testing.T, msg protocol.Message) {
				got, ok := msg.(*localmessagenotification.MsgRequestMessages)
				require.True(t, ok)
				assert.False(t, got.IsBlocking)
			},
		},
		{
			name:    "client done",
			msgType: localmessagenotification.MessageTypeClientDone,
			data:    []byte{0x81, 0x03},
			check: func(t *testing.T, msg protocol.Message) {
				_, ok := msg.(*localmessagenotification.MsgClientDone)
				assert.True(t, ok)
			},
		},
		{
			name:    "non-blocking reply with more",
			msgType: localmessagenotification.MessageTypeReplyMessagesNonBlocking,
			data: concat(
				[]byte{0x83, 0x01, 0x9f}, goldenMessageBytes(),
				[]byte{0xff, 0xf5},
			),
			check: func(t *testing.T, msg protocol.Message) {
				got, ok := msg.(*localmessagenotification.MsgReplyMessagesNonBlocking)
				require.True(t, ok)
				assert.Len(t, got.Messages, 1)
				assert.True(t, got.HasMore)
			},
		},
		{
			name:    "blocking reply",
			msgType: localmessagenotification.MessageTypeReplyMessagesBlocking,
			data: concat(
				[]byte{0x82, 0x02, 0x9f}, goldenMessageBytes(), []byte{0xff},
			),
			check: func(t *testing.T, msg protocol.Message) {
				got, ok := msg.(*localmessagenotification.MsgReplyMessagesBlocking)
				require.True(t, ok)
				assert.Len(t, got.Messages, 1)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg, err := localmessagenotification.NewMsgFromCbor(
				tc.msgType, tc.data,
			)
			require.NoError(t, err)
			tc.check(t, msg)
		})
	}
}

// TestLocalSubmissionRejectReasonsEncodeAsDmqNodeDoes pins the reject
// replies Dingo sends to a local submitter. dmq-node's decodeReject accepts
// [1] for a duplicate and [2] for an expired message (list length one), [3,
// text] for another reason and [0, text] for an invalid message, and fails
// on any other pairing of tag and length.
func TestLocalSubmissionRejectReasonsEncodeAsDmqNodeDoes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		reason ocommon.RejectReason
		want   []byte
	}{
		{
			name:   "already received",
			reason: ocommon.AlreadyReceivedReason{},
			want:   []byte{0x82, 0x02, 0x81, 0x01},
		},
		{
			name:   "expired",
			reason: ocommon.ExpiredReason{},
			want:   []byte{0x82, 0x02, 0x81, 0x02},
		},
		{
			name:   "other",
			reason: ocommon.OtherReason{Message: "no"},
			want:   []byte{0x82, 0x02, 0x82, 0x03, 0x62, 'n', 'o'},
		},
		{
			name:   "invalid",
			reason: ocommon.InvalidReason{Message: "no"},
			want:   []byte{0x82, 0x02, 0x82, 0x00, 0x62, 'n', 'o'},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg, err := localmessagesubmission.NewMsgRejectMessage(tc.reason)
			require.NoError(t, err)
			got, err := cbor.Encode(msg)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestLocalSubmissionDecodesDmqNodeRejectReasons reads the reasons dmq-node
// itself writes.
func TestLocalSubmissionDecodesDmqNodeRejectReasons(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		data []byte
		want ocommon.RejectReason
	}{
		{
			name: "already received",
			data: []byte{0x82, 0x02, 0x81, 0x01},
			want: ocommon.AlreadyReceivedReason{},
		},
		{
			name: "expired",
			data: []byte{0x82, 0x02, 0x81, 0x02},
			want: ocommon.ExpiredReason{},
		},
		{
			name: "other",
			data: []byte{0x82, 0x02, 0x82, 0x03, 0x62, 'n', 'o'},
			want: ocommon.OtherReason{Message: "no"},
		},
		{
			name: "invalid",
			data: []byte{0x82, 0x02, 0x82, 0x00, 0x62, 'n', 'o'},
			want: ocommon.InvalidReason{Message: "no"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			parsed, err := localmessagesubmission.NewMsgFromCbor(
				localmessagesubmission.MessageTypeRejectMessage, tc.data,
			)
			require.NoError(t, err)
			reject, ok := parsed.(*localmessagesubmission.MsgRejectMessage)
			require.True(t, ok)
			got, err := ocommon.FromRejectReasonData(reject.Reason)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
