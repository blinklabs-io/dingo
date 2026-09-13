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

// Package kesagent implements a client for the bursa KES agent wire protocol
// (blinklabs-io/bursa, internal/kesagent and docs/signer/kes-agent-protocol.md),
// so a dingo block producer can source its KES signing key -- or delegate KES
// signing entirely -- to an external agent process over a Unix-domain socket
// instead of reading a local kes.skey file.
//
// # Wire format
//
// Both directions of the service socket speak the same framing:
//
//	frame   = uint32(len, big-endian) || payload
//	payload = JSON object (UTF-8)
//
// A client MUST bound both the declared frame length and the time it is
// willing to wait for one: the socket is attacker-reachable whenever the
// filesystem path granting access to it is, so nothing here trusts an
// unbounded length prefix or an unresponsive peer. See MaxHelloFrameLen,
// MaxKeyPushFrameLen, MaxSignFrameLen, and Client's HelloTimeout/SignTimeout.
//
// # Handshake
//
// Immediately after connecting, the agent sends a Hello frame:
//
//	{"protocol":"bursa-kes-agent/1","mode":"serve-key"|"sign"}
//
// A client MUST verify the protocol string and the mode before proceeding.
//
// # serve-key mode
//
// After the Hello, and whenever the active key becomes available, evolves, or
// is (re)installed, the agent pushes a KeyPush frame; the client only reads.
//
// # sign mode
//
// After the Hello the client sends SignRequest frames and the agent replies
// with one SignResponse per request, over the same persistent connection; the
// KES key never leaves the agent.
package kesagent

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	// ProtocolID is the handshake protocol/version string a bursa KES agent
	// reports. A client MUST verify the server's Hello carries exactly this
	// value before trusting anything else on the connection.
	ProtocolID = "bursa-kes-agent/1"

	// ModeServeKey and ModeSign name the two service-socket modes the wire
	// protocol supports.
	ModeServeKey = "serve-key"
	ModeSign     = "sign"

	// MaxHelloFrameLen bounds the initial handshake frame. A real Hello is a
	// few dozen bytes; this is generous headroom without trusting an
	// unbounded length prefix from an unauthenticated-by-transport peer (P1:
	// "unbounded Hello handshake").
	MaxHelloFrameLen = 1 << 12 // 4 KiB

	// MaxKeyPushFrameLen bounds a serve-key push: a 608-byte KES secret key,
	// a 32-byte verification key, and a CBOR operational certificate
	// (a few hundred bytes), all base64-encoded in JSON (~1.4x expansion).
	MaxKeyPushFrameLen = 1 << 13 // 8 KiB

	// MaxSignFrameLen bounds a sign-mode request or response: a Cardano
	// block header body (at most a few KiB, even with Leios extensions) plus
	// a fixed 448-byte signature.
	MaxSignFrameLen = 1 << 16 // 64 KiB
)

// Hello is the handshake frame the agent sends immediately after accept.
type Hello struct {
	Protocol string `json:"protocol"`
	Mode     string `json:"mode"`
}

// KeyPush is sent by the agent in serve-key mode to deliver the current KES
// signing key, its verification key, its absolute KES period, and the
// operational certificate. Field names and shapes mirror
// blinklabs-io/bursa's internal/kesagent.KeyPush exactly: this is the same
// wire message, not a reinterpretation of it.
type KeyPush struct {
	Type       string `json:"type"` // "key_push"
	Period     uint64 `json:"period"`
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

// SignResponse is the agent's reply to a SignRequest.
type SignResponse struct {
	Type      string `json:"type"` // "sign_response"
	Period    uint64 `json:"period"`
	Signature []byte `json:"signature"`
	Error     string `json:"error,omitempty"`
}

// errFrameTooLarge is wrapped into every frame-size rejection so callers can
// distinguish it from a transport or decode error with errors.Is.
var errFrameTooLarge = errors.New("kesagent: frame exceeds configured maximum size")

// writeFrame writes a single length-prefixed JSON frame, rejecting a payload
// larger than maxSize before ever writing to w.
func writeFrame(w io.Writer, maxSize int, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("kesagent: marshal frame: %w", err)
	}
	if len(payload) > maxSize {
		return fmt.Errorf(
			"%w: %d bytes > %d",
			errFrameTooLarge,
			len(payload),
			maxSize,
		)
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload))) // #nosec G115 -- bounded by maxSize above
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("kesagent: write frame header: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("kesagent: write frame payload: %w", err)
	}
	return nil
}

// readFrame reads a single length-prefixed JSON frame into v, refusing to
// read more than maxSize bytes of payload regardless of what the length
// prefix declares.
func readFrame(r io.Reader, maxSize int, v any) error {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return err // may be io.EOF; callers distinguish
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 {
		return errors.New("kesagent: zero-length frame")
	}
	if n > uint32(maxSize) { // #nosec G115 -- maxSize is always a small positive constant
		return fmt.Errorf(
			"%w: declared %d bytes > %d",
			errFrameTooLarge,
			n,
			maxSize,
		)
	}
	var buf bytes.Buffer
	if _, err := io.CopyN(&buf, r, int64(n)); err != nil {
		return fmt.Errorf("kesagent: read frame payload: %w", err)
	}
	if err := json.Unmarshal(buf.Bytes(), v); err != nil {
		return fmt.Errorf("kesagent: unmarshal frame: %w", err)
	}
	return nil
}
