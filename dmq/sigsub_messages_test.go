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
	"bytes"
	"encoding/hex"
	"runtime"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The encodings below are written out by hand from dmq-node's
// sig-submission-v2.cddl and sig.cddl and its codec in
// DMQ/Protocol/SigSubmissionV2/Codec.hs (release 0.7.2.1), not produced by the
// code under test, so an in-process pair of Dingo stacks cannot agree on a
// format dmq-node would refuse. In CBOR, 0x84 and 0x82 open arrays of four and
// two, 0x9f opens an indefinite array that 0xff closes, 0x58 0x20 is a 32-byte
// string, and 0x59 0x01 0xc0 a 448-byte one.

func fill(b byte, n int) []byte { return bytes.Repeat([]byte{b}, n) }

// goldenMessageBytes is goldenMessage's encoding alone.
func goldenMessageBytes() []byte {
	_, encoded := goldenMessage()
	return encoded
}

func concat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	out, err := hex.DecodeString(s)
	require.NoError(t, err)
	return out
}

// goldenMessage is one message with fixed contents, and its hand-written
// encoding in the shape of sig.cddl's message:
//
//	[messageId, [body, kesPeriod, expiresAt], kesSignature,
//	 [kesVerificationKey, issueNumber, kesPeriod, coldSignature],
//	 coldVerificationKey]
func goldenMessage() (ocommon.DmqMessage, []byte) {
	msg := ocommon.DmqMessage{
		MessageID: fill(0xaa, 32),
		Payload: ocommon.DmqMessagePayload{
			MessageID:   fill(0xaa, 32),
			MessageBody: []byte("ab"),
			KESPeriod:   5,
			ExpiresAt:   0x65000000,
		},
		KESSignature: fill(0x00, 448),
		OperationalCertificate: ocommon.OperationalCertificate{
			KESVerificationKey: fill(0xbb, 32),
			IssueNumber:        1,
			KESPeriod:          2,
			ColdSignature:      fill(0xcc, 64),
		},
		ColdVerificationKey: fill(0xdd, 32),
	}
	encoded := concat(
		[]byte{0x85},
		[]byte{0x58, 0x20}, fill(0xaa, 32),
		[]byte{0x83, 0x42, 'a', 'b', 0x05, 0x1a, 0x65, 0x00, 0x00, 0x00},
		[]byte{0x59, 0x01, 0xc0}, fill(0x00, 448),
		[]byte{0x84, 0x58, 0x20}, fill(0xbb, 32),
		[]byte{0x01, 0x02, 0x58, 0x40}, fill(0xcc, 64),
		[]byte{0x58, 0x20}, fill(0xdd, 32),
	)
	return msg, encoded
}

func TestSigMessagesEncodeAsDmqNodeDoes(t *testing.T) {
	t.Parallel()
	id1, id2 := fill(0x11, 32), fill(0x22, 32)
	tuple1 := concat([]byte{0x82, 0x58, 0x20}, id1, []byte{0x19, 0x01, 0x90})
	tuple2 := concat([]byte{0x82, 0x58, 0x20}, id2, []byte{0x17})
	msg, msgBytes := goldenMessage()
	tests := []struct {
		name string
		msg  protocol.Message
		want []byte
	}{
		{
			"request IDs, blocking",
			newMsgRequestIDs(true, 0, 20),
			[]byte{0x84, 0x01, 0xf5, 0x00, 0x14},
		},
		{
			"request IDs, non-blocking, counts above 23",
			newMsgRequestIDs(false, 1000, 65535),
			[]byte{
				0x84, 0x01, 0xf4, 0x19, 0x03, 0xe8, 0x19, 0xff, 0xff,
			},
		},
		{
			"reply IDs",
			newMsgReplyIDs([]ocommon.MessageIDAndSize{
				{MessageID: id1, SizeInBytes: 400},
				{MessageID: id2, SizeInBytes: 23},
			}),
			concat(
				[]byte{0x82, 0x02, 0x9f}, tuple1, tuple2, []byte{0xff},
			),
		},
		{
			"reply IDs, empty, which only a non-blocking request may get",
			newMsgReplyIDs(nil),
			[]byte{0x82, 0x02, 0x9f, 0xff},
		},
		{"reply no IDs", newMsgReplyNoIDs(), []byte{0x81, 0x03}},
		{
			"request messages",
			newMsgRequestSigs([][]byte{id1, id2}),
			concat(
				[]byte{0x82, 0x04, 0x9f},
				[]byte{0x58, 0x20}, id1,
				[]byte{0x58, 0x20}, id2,
				[]byte{0xff},
			),
		},
		{
			"request messages, none",
			newMsgRequestSigs(nil),
			[]byte{0x82, 0x04, 0x9f, 0xff},
		},
		{
			"reply messages",
			newMsgReplySigs([]ocommon.DmqMessage{msg}),
			concat([]byte{0x82, 0x05, 0x9f}, msgBytes, []byte{0xff}),
		},
		{"done", newMsgDone(), []byte{0x81, 0x06}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := cbor.Encode(tt.msg)
			require.NoError(t, err)
			assert.Equal(
				t, hex.EncodeToString(tt.want), hex.EncodeToString(got),
			)
		})
	}
}

func TestSigMessagesDecodeWhatDmqNodeSends(t *testing.T) {
	t.Parallel()
	id1, id2 := fill(0x11, 32), fill(0x22, 32)
	msg, msgBytes := goldenMessage()

	decode := func(t *testing.T, b []byte) protocol.Message {
		t.Helper()
		var parts []cbor.RawMessage
		_, err := cbor.Decode(b, &parts)
		require.NoError(t, err)
		var kind uint8
		_, err = cbor.Decode(parts[0], &kind)
		require.NoError(t, err)
		got, err := newSigMsgFromCbor(uint(kind), b)
		require.NoError(t, err)
		return got
	}

	t.Run("request IDs", func(t *testing.T) {
		t.Parallel()
		got, ok := decode(t, []byte{0x84, 0x01, 0xf5, 0x00, 0x14}).(*msgRequestIDs)
		require.True(t, ok)
		assert.True(t, got.Blocking)
		assert.Equal(t, uint16(0), got.Ack)
		assert.Equal(t, uint16(20), got.Req)
	})
	t.Run("reply IDs in an indefinite list", func(t *testing.T) {
		t.Parallel()
		b := concat(
			[]byte{0x82, 0x02, 0x9f},
			[]byte{0x82, 0x58, 0x20}, id1, []byte{0x19, 0x01, 0x90},
			[]byte{0x82, 0x58, 0x20}, id2, []byte{0x17},
			[]byte{0xff},
		)
		got, ok := decode(t, b).(*msgReplyIDs)
		require.True(t, ok)
		require.Len(t, got.IDs, 2)
		assert.Equal(t, id1, got.IDs[0].MessageID)
		assert.Equal(t, uint32(400), got.IDs[0].SizeInBytes)
		assert.Equal(t, uint32(23), got.IDs[1].SizeInBytes)
	})
	t.Run("empty reply IDs", func(t *testing.T) {
		t.Parallel()
		got, ok := decode(t, []byte{0x82, 0x02, 0x9f, 0xff}).(*msgReplyIDs)
		require.True(t, ok)
		assert.Empty(t, got.IDs)
	})
	t.Run("reply no IDs", func(t *testing.T) {
		t.Parallel()
		_, ok := decode(t, []byte{0x81, 0x03}).(*msgReplyNoIDs)
		assert.True(t, ok)
	})
	t.Run("request messages", func(t *testing.T) {
		t.Parallel()
		b := concat(
			[]byte{0x82, 0x04, 0x9f},
			[]byte{0x58, 0x20}, id1, []byte{0x58, 0x20}, id2,
			[]byte{0xff},
		)
		got, ok := decode(t, b).(*msgRequestSigs)
		require.True(t, ok)
		assert.Equal(t, [][]byte{id1, id2}, got.IDs)
	})
	t.Run("reply messages", func(t *testing.T) {
		t.Parallel()
		b := concat([]byte{0x82, 0x05, 0x9f}, msgBytes, []byte{0xff})
		got, ok := decode(t, b).(*msgReplySigs)
		require.True(t, ok)
		require.Len(t, got.Msgs, 1)
		assert.Equal(t, msg.ID(), got.Msgs[0].ID())
		assert.Equal(t, []byte("ab"), got.Msgs[0].Payload.MessageBody)
		assert.Equal(t, uint64(5), got.Msgs[0].Payload.KESPeriod)
		assert.Equal(t, uint32(0x65000000), got.Msgs[0].Payload.ExpiresAt)
		assert.Equal(t, msg.KESSignature, got.Msgs[0].KESSignature)
		assert.Equal(
			t,
			uint64(1),
			got.Msgs[0].OperationalCertificate.IssueNumber,
		)
		assert.Equal(
			t,
			msg.ColdVerificationKey,
			got.Msgs[0].ColdVerificationKey,
		)
	})
	t.Run("done", func(t *testing.T) {
		t.Parallel()
		_, ok := decode(t, []byte{0x81, 0x06}).(*msgDone)
		assert.True(t, ok)
	})
}

func TestSigMessagesRejectWhatDmqNodeWouldNot(t *testing.T) {
	t.Parallel()
	id := fill(0x11, 32)
	tests := []struct {
		name    string
		kind    uint
		encoded []byte
	}{
		// Version 1's Init was number 0 in the gouroboros package and 6 in
		// dmq-node's TxSubmission2 codec; version 2 has neither.
		{"an Init message from version 1", 0, []byte{0x81, 0x00}},
		{"unknown message number", 7, []byte{0x81, 0x07}},
		{"reply IDs as a definite list", 2, concat(
			[]byte{0x82, 0x02, 0x81},
			[]byte{0x82, 0x58, 0x20}, id, []byte{0x19, 0x01, 0x90},
		)},
		{"request messages as a definite list", 4, concat(
			[]byte{0x82, 0x04, 0x81}, []byte{0x58, 0x20}, id,
		)},
		{"reply messages as a definite list", 5, concat(
			[]byte{0x82, 0x05, 0x81}, goldenMessageBytes(),
		)},
		{"a message in the older shape with its ID in the payload", 5, concat(
			[]byte{0x82, 0x05, 0x9f},
			// [[id, body, kesPeriod, expiresAt], kesSignature,
			//  [kesKey, n, period, sig], coldKey]
			[]byte{0x84, 0x84, 0x58, 0x20}, fill(0xaa, 32),
			[]byte{0x42, 'a', 'b', 0x05, 0x1a, 0x65, 0x00, 0x00, 0x00},
			[]byte{0x59, 0x01, 0xc0}, fill(0x00, 448),
			[]byte{0x84, 0x58, 0x20}, fill(0xbb, 32),
			[]byte{0x01, 0x02, 0x58, 0x40}, fill(0xcc, 64),
			[]byte{0x58, 0x20}, fill(0xdd, 32),
			[]byte{0xff},
		)},
		{"a message whose payload has four fields", 5, concat(
			[]byte{0x82, 0x05, 0x9f, 0x85, 0x58, 0x20},
			fill(0xaa, 32),
			[]byte{
				0x84,
				0x42,
				'a',
				'b',
				0x05,
				0x1a,
				0x65,
				0x00,
				0x00,
				0x00,
				0x00,
			},
			[]byte{0x59, 0x01, 0xc0},
			fill(0x00, 448),
			[]byte{0x84, 0x58, 0x20},
			fill(0xbb, 32),
			[]byte{0x01, 0x02, 0x58, 0x40},
			fill(0xcc, 64),
			[]byte{0x58, 0x20},
			fill(0xdd, 32),
			[]byte{0xff},
		)},
		{"a number that wraps to a real message", 257, []byte{
			0x84, 0x01, 0xf5, 0x00, 0x14,
		}},
		{"request IDs with a missing field", 1, []byte{0x83, 0x01, 0xf5, 0x00}},
		{"request IDs with an extra field", 1, []byte{
			0x85, 0x01, 0xf5, 0x00, 0x14, 0x00,
		}},
		{"done with a field", 6, []byte{0x82, 0x06, 0x00}},
		{"reply no IDs with a field", 3, []byte{0x82, 0x03, 0x00}},
		{"reply IDs with a short ID", 2, concat(
			[]byte{0x82, 0x02, 0x9f},
			[]byte{0x82, 0x58, 0x1f}, fill(0x11, 31), []byte{0x01},
			[]byte{0xff},
		)},
		{"reply IDs entry with three fields", 2, concat(
			[]byte{0x82, 0x02, 0x9f},
			[]byte{0x83, 0x58, 0x20}, id, []byte{0x01, 0x01},
			[]byte{0xff},
		)},
		{"request messages with a short ID", 4, concat(
			[]byte{0x82, 0x04, 0x9f},
			[]byte{0x58, 0x1f}, fill(0x11, 31),
			[]byte{0xff},
		)},
		{"request messages that is not a list", 4, concat(
			[]byte{0x82, 0x04, 0x58, 0x20}, id,
		)},
		{"reply messages with a malformed message", 5, []byte{
			0x82, 0x05, 0x9f, 0x81, 0x00, 0xff,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := newSigMsgFromCbor(tt.kind, tt.encoded)
			require.Error(t, err)
		})
	}
}

// A message that does not fit CDDL's sizes is one dmq-node refuses to decode.
func TestCheckMessageShape(t *testing.T) {
	t.Parallel()
	good := func() ocommon.DmqMessage {
		msg, _ := goldenMessage()
		msg.Payload.MessageBody = fill(0x01, 400)
		return msg
	}
	tests := []struct {
		name    string
		mutate  func(*ocommon.DmqMessage)
		wantErr string
	}{
		{"a conforming message", func(*ocommon.DmqMessage) {}, ""},
		{"body at the minimum", func(m *ocommon.DmqMessage) {
			m.Payload.MessageBody = fill(1, 90)
		}, ""},
		{"body at the maximum", func(m *ocommon.DmqMessage) {
			m.Payload.MessageBody = fill(1, 2000)
		}, ""},
		{"body below the minimum", func(m *ocommon.DmqMessage) {
			m.Payload.MessageBody = fill(1, 89)
		}, "body is 89 bytes"},
		{"body above the maximum", func(m *ocommon.DmqMessage) {
			m.Payload.MessageBody = fill(1, 2001)
		}, "body is 2001 bytes"},
		{"empty body", func(m *ocommon.DmqMessage) {
			m.Payload.MessageBody = nil
		}, "body is 0 bytes"},
		{"short KES signature", func(m *ocommon.DmqMessage) {
			m.KESSignature = fill(0, 447)
		}, "KES signature is 447 bytes"},
		{"short KES verification key", func(m *ocommon.DmqMessage) {
			m.OperationalCertificate.KESVerificationKey = fill(0, 31)
		}, "KES verification key is 31 bytes"},
		{"short cold signature", func(m *ocommon.DmqMessage) {
			m.OperationalCertificate.ColdSignature = fill(0, 63)
		}, "cold signature is 63 bytes"},
		{"short cold verification key", func(m *ocommon.DmqMessage) {
			m.ColdVerificationKey = fill(0, 33)
		}, "cold verification key is 33 bytes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			msg := good()
			tt.mutate(&msg)
			err := checkMessageShape(
				&msg, DefaultMessageBodyMin, DefaultMessageBodyMax,
			)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, errMessageShape)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// replyIDsOf is a message 2 naming n IDs, every one of them valid.
func replyIDsOf(n int) []byte {
	entry := concat([]byte{0x82, 0x58, 0x20}, fill(0x11, 32), []byte{0x01})
	return concat(
		[]byte{0x82, 0x02, 0x9f},
		bytes.Repeat(entry, n),
		[]byte{0xff},
	)
}

func TestSigMessagesBoundWhatAPeerSends(t *testing.T) {
	t.Parallel()

	t.Run(
		"a request over its limit is refused before it is decoded",
		func(t *testing.T) {
			t.Parallel()
			// Not valid CBOR: only a size check run first reports the limit.
			_, err := newSigMsgFromCbor(
				uint(sigMsgRequestSigs), make([]byte, sigRequestByteLimit+1),
			)
			require.ErrorContains(t, err, "byte limit")
		},
	)
	t.Run(
		"a reply over its limit is refused before it is decoded",
		func(t *testing.T) {
			t.Parallel()
			_, err := newSigMsgFromCbor(
				uint(sigMsgReplySigs), make([]byte, sigReplyByteLimit+1),
			)
			require.ErrorContains(t, err, "byte limit")
		},
	)
	t.Run(
		"a reply may be larger than a request is allowed to be",
		func(t *testing.T) {
			t.Parallel()
			encoded := replyIDsOf(8000)
			require.Greater(t, len(encoded), sigRequestByteLimit)
			got, err := newSigMsgFromCbor(uint(sigMsgReplyIDs), encoded)
			require.NoError(t, err)
			require.Len(t, got.(*msgReplyIDs).IDs, 8000)
		},
	)
	t.Run("a list of the most items is accepted", func(t *testing.T) {
		t.Parallel()
		got, err := newSigMsgFromCbor(
			uint(sigMsgReplyIDs), replyIDsOf(maxListItems),
		)
		require.NoError(t, err)
		require.Len(t, got.(*msgReplyIDs).IDs, maxListItems)
	})
	t.Run("a list of one more is refused", func(t *testing.T) {
		t.Parallel()
		_, err := newSigMsgFromCbor(
			uint(sigMsgReplyIDs), replyIDsOf(maxListItems+1),
		)
		require.ErrorContains(t, err, "more than 65535 items")
	})
	t.Run("an unterminated list is refused", func(t *testing.T) {
		t.Parallel()
		_, err := newSigMsgFromCbor(
			uint(sigMsgRequestSigs),
			concat([]byte{0x82, 0x04, 0x9f, 0x58, 0x20}, fill(0x11, 32)),
		)
		require.Error(t, err)
	})
}

// A request padded with empty entries is refused at the first one, and the
// list is never built.
//
// Not t.Parallel: it reads the process-wide allocation counter.
func TestSigMessagesDoNotBuildAListBeforeCheckingIt(t *testing.T) {
	const entries = 200_000
	encoded := concat(
		[]byte{0x82, 0x04, 0x9f},
		bytes.Repeat([]byte{0x40}, entries),
		[]byte{0xff},
	)
	require.LessOrEqual(t, len(encoded), sigRequestByteLimit)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := newSigMsgFromCbor(uint(sigMsgRequestSigs), encoded)
	runtime.ReadMemStats(&after)

	require.ErrorContains(t, err, "message ID is 0 bytes")
	// Building the list of 200,000 entries takes about ten megabytes.
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(2<<20))
}
