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

// Package kesagent implements the client half of bursa's KES agent wire
// protocol. It connects to a running agent's service socket and either
// receives the evolving KES signing key (serve-key mode) or forwards header
// bodies for the agent to sign (sign mode), presenting a forging.KESSigner to
// the block producer either way.
//
// The wire format mirrors bursa internal/kesagent (protocol
// "bursa-kes-agent/1") byte-for-byte:
//
//	frame   = uint32(len, big-endian) || payload
//	payload = JSON object (UTF-8), []byte fields base64-encoded
//
// The maximum payload length is maxFrameLen (1 MiB). Immediately after a
// client connects the server sends a Hello frame; a client MUST verify the
// protocol string and mode before proceeding.
//
//	{"protocol":"bursa-kes-agent/1","mode":"serve-key"|"sign"}
//
// serve-key: the server pushes a KeyPush frame whenever the active key
// becomes available or evolves; the client consumes frames and sends nothing.
//
//	{"type":"key_push","period":<abs KES period>,"depth":6,
//	 "kes_sign_key":<b64>,"kes_vkey":<b64>,"opcert":<b64 CBOR>}
//
// sign: the client sends SignRequest frames and the server replies with a
// SignResponse for each; the KES key never leaves the agent.
//
//	-> {"type":"sign_request","period":<abs>,"message":<b64>}
//	<- {"type":"sign_response","period":<abs>,"signature":<b64>,"error":""}
package kesagent

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	// ProtocolID is the handshake protocol/version string. It must match
	// bursa internal/kesagent.ProtocolID exactly.
	ProtocolID = "bursa-kes-agent/1"

	// maxFrameLen caps a single frame payload (matches the agent).
	maxFrameLen = 1 << 20 // 1 MiB
)

// Socket modes for the service socket.
const (
	ModeServeKey = "serve-key"
	ModeSign     = "sign"
)

// Hello is the first frame the server sends after a client connects.
type Hello struct {
	Protocol string `json:"protocol"`
	Mode     string `json:"mode"`
}

// Frame type identifiers. A frame's declared type is required to match before
// its contents are trusted: applyKeyPush refuses a frame whose type is anything
// other than KeyPushType, absent included.
const (
	KeyPushType      = "key_push"
	SignRequestType  = "sign_request"
	SignResponseType = "sign_response"
)

// KeyPush is sent by the server in serve-key mode to deliver the current KES
// signing key, its verification key, its absolute KES period, and the
// operational certificate.
type KeyPush struct {
	Type   string `json:"type"` // "key_push"; required
	Period uint64 `json:"period"`
	// Depth is the KES tree depth. Shelley fixes it at kes.CardanoKesDepth,
	// which is the only value accepted; an omitted (zero) value means that
	// depth rather than a depth-0 key.
	Depth      uint64 `json:"depth"`
	KESSignKey []byte `json:"kes_sign_key"`
	KESVKey    []byte `json:"kes_vkey"`
	OpCert     []byte `json:"opcert"`
}

// SignRequest is sent by the client in sign mode.
type SignRequest struct {
	Type    string `json:"type"` // "sign_request"
	Period  uint64 `json:"period"`
	Message []byte `json:"message"`
}

// SignResponse is the server's reply to a SignRequest.
type SignResponse struct {
	Type      string `json:"type"` // "sign_response"
	Period    uint64 `json:"period"`
	Signature []byte `json:"signature"`
	Error     string `json:"error,omitempty"`
}

// writeFrame writes a single length-prefixed JSON frame.
func writeFrame(w io.Writer, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("kesagent: marshal frame: %w", err)
	}
	if len(payload) > maxFrameLen {
		return fmt.Errorf("kesagent: frame too large (%d bytes)", len(payload))
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(
		hdr[:],
		uint32(len(payload)),
	) // #nosec G115 -- bounded by maxFrameLen
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("kesagent: write frame header: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("kesagent: write frame payload: %w", err)
	}
	return nil
}

// framePayloadBuffer allocates a frame's payload buffer. It is a variable so a
// test can retain the exact buffer readFrame wipes; the wipe is otherwise
// unobservable, because the buffer is local and becomes unreachable garbage the
// moment readFrame returns.
var framePayloadBuffer = func(n uint32) []byte { return make([]byte, n) }

// readFrame reads a single length-prefixed JSON frame into v.
//
// The payload buffer is wiped before returning on every path. A serve-key frame
// holds the base64-encoded KES signing key, so without the wipe every push --
// accepted or rejected -- leaves a trivially decodable copy of the pool's
// signing key in freed heap memory, and a push recurs on every evolve and every
// reconnect. json.Unmarshal base64-decodes into freshly allocated fields, so
// nothing in v aliases this buffer.
func readFrame(r io.Reader, v any) error {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return err // may be io.EOF; callers distinguish
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 {
		return errors.New("kesagent: zero-length frame")
	}
	if n > maxFrameLen {
		return fmt.Errorf("kesagent: frame too large (%d bytes)", n)
	}
	payload := framePayloadBuffer(n)
	defer wipe(payload)
	if _, err := io.ReadFull(r, payload); err != nil {
		return fmt.Errorf("kesagent: read frame payload: %w", err)
	}
	if err := json.Unmarshal(payload, v); err != nil {
		return fmt.Errorf("kesagent: unmarshal frame: %w", err)
	}
	return nil
}
