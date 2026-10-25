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
	"errors"
	"fmt"
	"math"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// The messages of the node-to-node message submission protocol, version 2,
// as dmq-node encodes them. They differ from the gouroboros messagesubmission
// package, which dmq-node cannot read: the numbers after the first reply are
// shifted by one to make room for MsgReplyNoIDs, and the item list in
// messages 2, 4 and 5 is an indefinite-length CBOR array.
//
//	msgRequestIDs  = [1, blocking, ack, req]
//	msgReplyIDs    = [2, [*[messageId, messageSize]]]
//	msgReplyNoIDs  = [3]
//	msgRequestSigs = [4, [*messageId]]
//	msgReplySigs   = [5, [*message]]
//	msgDone        = [6]
const (
	sigMsgRequestIDs  uint8 = 1
	sigMsgReplyIDs    uint8 = 2
	sigMsgReplyNoIDs  uint8 = 3
	sigMsgRequestSigs uint8 = 4
	sigMsgReplySigs   uint8 = 5
	sigMsgDone        uint8 = 6
)

// msgRequestIDs asks for up to Req message IDs after acknowledging Ack of the
// ones already announced. It encodes as the array its fields make.
type msgRequestIDs struct {
	protocol.MessageBase
	Blocking bool
	Ack      uint16
	Req      uint16
}

func newMsgRequestIDs(blocking bool, ack, req uint16) *msgRequestIDs {
	return &msgRequestIDs{
		MessageBase: protocol.MessageBase{MessageType: sigMsgRequestIDs},
		Blocking:    blocking,
		Ack:         ack,
		Req:         req,
	}
}

// msgReplyIDs announces message IDs with their encoded sizes. The reply to a
// non-blocking request may announce none.
type msgReplyIDs struct {
	protocol.MessageBase
	IDs []ocommon.MessageIDAndSize
}

func newMsgReplyIDs(ids []ocommon.MessageIDAndSize) *msgReplyIDs {
	return &msgReplyIDs{
		MessageBase: protocol.MessageBase{MessageType: sigMsgReplyIDs},
		IDs:         ids,
	}
}

func (m *msgReplyIDs) MarshalCBOR() ([]byte, error) {
	items := make([]any, 0, len(m.IDs))
	for _, id := range m.IDs {
		items = append(items, id)
	}
	return cbor.Encode([]any{sigMsgReplyIDs, cbor.IndefLengthList(items)})
}

// msgReplyNoIDs answers a blocking request when there is nothing to announce.
type msgReplyNoIDs struct {
	protocol.MessageBase
}

func newMsgReplyNoIDs() *msgReplyNoIDs {
	return &msgReplyNoIDs{
		MessageBase: protocol.MessageBase{MessageType: sigMsgReplyNoIDs},
	}
}

// msgRequestSigs asks for the bodies of announced messages.
type msgRequestSigs struct {
	protocol.MessageBase
	IDs [][]byte
}

func newMsgRequestSigs(ids [][]byte) *msgRequestSigs {
	return &msgRequestSigs{
		MessageBase: protocol.MessageBase{MessageType: sigMsgRequestSigs},
		IDs:         ids,
	}
}

func (m *msgRequestSigs) MarshalCBOR() ([]byte, error) {
	items := make([]any, 0, len(m.IDs))
	for _, id := range m.IDs {
		items = append(items, id)
	}
	return cbor.Encode(
		[]any{sigMsgRequestSigs, cbor.IndefLengthList(items)},
	)
}

// msgReplySigs carries the requested message bodies.
type msgReplySigs struct {
	protocol.MessageBase
	Msgs []ocommon.DmqMessage
}

func newMsgReplySigs(msgs []ocommon.DmqMessage) *msgReplySigs {
	return &msgReplySigs{
		MessageBase: protocol.MessageBase{MessageType: sigMsgReplySigs},
		Msgs:        msgs,
	}
}

func (m *msgReplySigs) MarshalCBOR() ([]byte, error) {
	items := make([]any, 0, len(m.Msgs))
	for _, msg := range m.Msgs {
		items = append(items, msg)
	}
	return cbor.Encode([]any{sigMsgReplySigs, cbor.IndefLengthList(items)})
}

// msgDone ends the protocol. Only the pulling side sends it, from StIdle.
type msgDone struct {
	protocol.MessageBase
}

func newMsgDone() *msgDone {
	return &msgDone{
		MessageBase: protocol.MessageBase{MessageType: sigMsgDone},
	}
}

// newSigMsgFromCbor decodes one message and checks each item's shape. The CIP
// says only indefinite-length lists of IDs, sizes and messages are supported,
// so a list in any other form is refused, as is a message in the older shape
// that kept its ID inside the payload. How many items a message may carry, and
// whether it fits the state, is checked by the protocol that receives it.
func newSigMsgFromCbor(
	msgType uint,
	data []byte,
) (protocol.Message, error) {
	if limit := inboundByteLimit(msgType); len(data) > limit {
		return nil, fmt.Errorf(
			"%s: message %d is %d bytes, over the %d byte limit",
			sigProtocolName, msgType, len(data), limit,
		)
	}
	parts := make([]cbor.RawMessage, 0, 4)
	if _, err := cbor.Decode(data, &parts); err != nil {
		return nil, fmt.Errorf("%s: decode error: %w", sigProtocolName, err)
	}
	msg, err := decodeSigMsgParts(msgType, parts)
	if err != nil {
		return nil, fmt.Errorf("%s: decode error: %w", sigProtocolName, err)
	}
	msg.SetCbor(data)
	return msg, nil
}

func decodeSigMsgParts(
	msgType uint,
	parts []cbor.RawMessage,
) (protocol.Message, error) {
	want := func(n int) error {
		if len(parts) != n {
			return fmt.Errorf(
				"message %d has %d elements, expected %d",
				msgType, len(parts), n,
			)
		}
		return nil
	}
	// A number beyond a byte is not a message, and must not be read as the
	// one its low byte names.
	if msgType > math.MaxUint8 {
		return nil, fmt.Errorf("unknown message type %d", msgType)
	}
	// #nosec G115 -- checked against MaxUint8 just above
	switch uint8(msgType) {
	case sigMsgRequestIDs:
		if err := want(4); err != nil {
			return nil, err
		}
		var blocking bool
		var ack, req uint16
		for i, dst := range []any{&blocking, &ack, &req} {
			if _, err := cbor.Decode(parts[i+1], dst); err != nil {
				return nil, fmt.Errorf("request IDs field %d: %w", i, err)
			}
		}
		return newMsgRequestIDs(blocking, ack, req), nil
	case sigMsgReplyIDs:
		if err := want(2); err != nil {
			return nil, err
		}
		if err := wantIndefiniteList(parts[1]); err != nil {
			return nil, err
		}
		var ids []ocommon.MessageIDAndSize
		if err := eachListItem(parts[1], func(item cbor.RawMessage) error {
			var id ocommon.MessageIDAndSize
			if _, err := cbor.Decode(item, &id); err != nil {
				return fmt.Errorf("reply IDs entry: %w", err)
			}
			if err := checkMessageID(id.MessageID); err != nil {
				return err
			}
			ids = append(ids, id)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("reply IDs list: %w", err)
		}
		return newMsgReplyIDs(ids), nil
	case sigMsgReplyNoIDs:
		if err := want(1); err != nil {
			return nil, err
		}
		return newMsgReplyNoIDs(), nil
	case sigMsgRequestSigs:
		if err := want(2); err != nil {
			return nil, err
		}
		if err := wantIndefiniteList(parts[1]); err != nil {
			return nil, err
		}
		var ids [][]byte
		if err := eachListItem(parts[1], func(item cbor.RawMessage) error {
			var id []byte
			if _, err := cbor.Decode(item, &id); err != nil {
				return fmt.Errorf("request messages entry: %w", err)
			}
			if err := checkMessageID(id); err != nil {
				return err
			}
			ids = append(ids, id)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("request messages list: %w", err)
		}
		return newMsgRequestSigs(ids), nil
	case sigMsgReplySigs:
		if err := want(2); err != nil {
			return nil, err
		}
		if err := wantIndefiniteList(parts[1]); err != nil {
			return nil, err
		}
		var msgs []ocommon.DmqMessage
		if err := eachListItem(parts[1], func(item cbor.RawMessage) error {
			msg, err := decodeWireMessage(item)
			if err != nil {
				return fmt.Errorf("reply messages entry: %w", err)
			}
			msgs = append(msgs, msg)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("reply messages list: %w", err)
		}
		return newMsgReplySigs(msgs), nil
	case sigMsgDone:
		if err := want(1); err != nil {
			return nil, err
		}
		return newMsgDone(), nil
	default:
		return nil, fmt.Errorf("unknown message type %d", msgType)
	}
}

// indefiniteArrayHeader is the CBOR byte that opens an indefinite-length
// array.
const indefiniteArrayHeader = 0x9f

// wantIndefiniteList refuses a list that is not an indefinite-length array.
func wantIndefiniteList(raw cbor.RawMessage) error {
	if len(raw) == 0 || raw[0] != indefiniteArrayHeader {
		return errors.New("list is not an indefinite-length array")
	}
	return nil
}

const cborBreak = 0xff

// maxListItems is the most items a message may carry. The counts in a request
// are 16 bits wide, so no valid list is longer.
const maxListItems = math.MaxUint16

// inboundByteLimit is the most bytes a received message of this type may
// take, checked before any of it is decoded. A request names at most a window
// of IDs and a reply at most a few thousand items, and a peer's message never
// needs more.
func inboundByteLimit(msgType uint) int {
	switch msgType {
	case uint(sigMsgReplyIDs), uint(sigMsgReplyNoIDs), uint(sigMsgReplySigs):
		return sigReplyByteLimit
	default:
		return sigRequestByteLimit
	}
}

// eachListItem calls fn on each item of an indefinite-length list in turn. It
// refuses a list of more than maxListItems items as soon as the next one
// starts, and never holds more than the item in hand, so a list of empty
// entries costs nothing beyond what fn keeps.
func eachListItem(
	raw cbor.RawMessage,
	fn func(item cbor.RawMessage) error,
) error {
	if err := wantIndefiniteList(raw); err != nil {
		return err
	}
	pos := 1
	for count := 0; ; count++ {
		if pos >= len(raw) {
			return errors.New("list is not terminated")
		}
		if raw[pos] == cborBreak {
			return nil
		}
		if count >= maxListItems {
			return fmt.Errorf("list has more than %d items", maxListItems)
		}
		var item cbor.RawMessage
		used, err := cbor.Decode(raw[pos:], &item)
		if err != nil {
			return err
		}
		if err := fn(item); err != nil {
			return err
		}
		pos += used
	}
}

// wireMessage is a message in the only shape CIP-0137 defines for version 2:
//
//	[messageId, [messageBody, kesPeriod, expiresAt], kesSignature,
//	 operationalCertificate, coldVerificationKey]
//
// The gouroboros decoder also takes an older shape, which kept the ID inside
// the payload; this stack does not, because dmq-node would not.
type wireMessage struct {
	cbor.StructAsArray
	MessageID              []byte
	Payload                wirePayload
	KESSignature           []byte
	OperationalCertificate ocommon.OperationalCertificate
	ColdVerificationKey    []byte
}

type wirePayload struct {
	cbor.StructAsArray
	MessageBody []byte
	KESPeriod   uint64
	ExpiresAt   uint32
}

func decodeWireMessage(raw cbor.RawMessage) (ocommon.DmqMessage, error) {
	var wire wireMessage
	if _, err := cbor.Decode(raw, &wire); err != nil {
		return ocommon.DmqMessage{}, err
	}
	return ocommon.DmqMessage{
		MessageID: wire.MessageID,
		Payload: ocommon.DmqMessagePayload{
			MessageID:   wire.MessageID,
			MessageBody: wire.Payload.MessageBody,
			KESPeriod:   wire.Payload.KESPeriod,
			ExpiresAt:   wire.Payload.ExpiresAt,
		},
		KESSignature:           wire.KESSignature,
		OperationalCertificate: wire.OperationalCertificate,
		ColdVerificationKey:    wire.ColdVerificationKey,
	}, nil
}

// messageIDSize is the length of a message ID.
const messageIDSize = 32

func checkMessageID(id []byte) error {
	if len(id) != messageIDSize {
		return fmt.Errorf(
			"message ID is %d bytes, expected %d", len(id), messageIDSize,
		)
	}
	return nil
}

// The size limits CIP-0137's CDDL puts on a message, which dmq-node enforces
// when it decodes one.
const (
	// DefaultMessageBodyMin and DefaultMessageBodyMax bound messageBody. The
	// CIP's version 2 CDDL says 90 to 2000, and dmq-node's decoder uses the
	// same limits (Policy.minSigBodySize and maxSigBodySize); the sig.cddl file
	// in its repository says 360, which its decoder does not enforce.
	DefaultMessageBodyMin = 90
	DefaultMessageBodyMax = 2000

	kesSignatureSize    = 448
	kesVerificationSize = 32
	coldSignatureSize   = 64
	coldVerificationKey = 32
)

var errMessageShape = errors.New("message does not fit the CIP-0137 sizes")

// checkMessageShape reports why msg breaks the sizes CIP-0137's CDDL fixes: a
// body of bodyMin to bodyMax bytes, a 448-byte KES signature, and the key and
// signature lengths of the operational certificate and cold key. A message
// outside them is one dmq-node refuses to decode, so it is never accepted or
// passed on.
func checkMessageShape(
	msg *ocommon.DmqMessage,
	bodyMin, bodyMax int,
) error {
	switch {
	case len(msg.Payload.MessageBody) < bodyMin ||
		len(msg.Payload.MessageBody) > bodyMax:
		return fmt.Errorf(
			"%w: body is %d bytes, expected %d to %d",
			errMessageShape, len(msg.Payload.MessageBody), bodyMin, bodyMax,
		)
	case len(msg.KESSignature) != kesSignatureSize:
		return fmt.Errorf(
			"%w: KES signature is %d bytes, expected %d",
			errMessageShape, len(msg.KESSignature), kesSignatureSize,
		)
	case len(msg.OperationalCertificate.KESVerificationKey) !=
		kesVerificationSize:
		return fmt.Errorf(
			"%w: KES verification key is %d bytes, expected %d",
			errMessageShape,
			len(msg.OperationalCertificate.KESVerificationKey),
			kesVerificationSize,
		)
	case len(msg.OperationalCertificate.ColdSignature) != coldSignatureSize:
		return fmt.Errorf(
			"%w: cold signature is %d bytes, expected %d",
			errMessageShape,
			len(msg.OperationalCertificate.ColdSignature),
			coldSignatureSize,
		)
	case len(msg.ColdVerificationKey) != coldVerificationKey:
		return fmt.Errorf(
			"%w: cold verification key is %d bytes, expected %d",
			errMessageShape, len(msg.ColdVerificationKey), coldVerificationKey,
		)
	}
	return nil
}
