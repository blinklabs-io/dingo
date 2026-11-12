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
// (blinklabs-io/bursa, kesagent/wire and docs/signer/kes-agent-protocol.md),
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
// The two halves of a frame need separate bounds. Waiting for a header can be
// legitimately unbounded -- a serve-key subscriber idles between pushes with
// nothing to read -- but once a peer has declared a length, the body must
// arrive promptly or the read has to give up, or a peer that announces a
// frame and then stalls parks the reader forever. bursa's own server bounds
// the same direction for the same reason (frameBodyReadTimeout); Client
// mirrors it with Config.FrameBodyTimeout.
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
// After the Hello the client sends a SignRequest and the agent replies with one
// SignResponse. A client may authenticate a fresh connection for each request;
// the KES key never leaves the agent.
package kesagent

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"

	"github.com/blinklabs-io/bursa/kesagent/wire"
)

const (
	// ProtocolID is the handshake protocol/version string a bursa KES agent
	// reports. A client MUST verify the server's Hello carries exactly this
	// value before trusting anything else on the connection.
	ProtocolID = wire.ProtocolID

	// ModeServeKey and ModeSign name the two service-socket modes the wire
	// protocol supports.
	ModeServeKey = wire.ModeServeKey
	ModeSign     = wire.ModeSign

	// MaxHelloFrameLen bounds the initial handshake frame. A real Hello is a
	// few dozen bytes; this is generous headroom without trusting an
	// unbounded length prefix from an unauthenticated-by-transport peer (P1:
	// "unbounded Hello handshake").
	MaxHelloFrameLen = wire.MaxHelloFrameLen // 4 KiB

	// MaxKeyPushFrameLen bounds a serve-key push: a 608-byte KES secret key,
	// a 32-byte verification key, and a CBOR operational certificate
	// (a few hundred bytes), all base64-encoded in JSON (~1.4x expansion).
	MaxKeyPushFrameLen = wire.MaxKeyPushFrameLen // 8 KiB

	// MaxSignFrameLen bounds a sign-mode request or response: a Cardano
	// block header body (at most a few KiB, even with Leios extensions) plus
	// a fixed 448-byte signature.
	MaxSignFrameLen = wire.MaxSignFrameLen // 64 KiB
)

// The frame types are bursa's own (kesagent/wire), aliased so this package's
// API is unchanged and the two ends cannot drift apart.

// Hello is the handshake frame the agent sends immediately after accept.
type Hello = wire.Hello

// KeyPush is sent by the agent in serve-key mode to deliver the current KES
// signing key, its verification key, its absolute KES period, and the
// operational certificate.
type KeyPush = wire.KeyPush

// SignRequest is sent by the client in sign mode.
type SignRequest = wire.SignRequest

// SignResponse is the agent's reply to a SignRequest.
type SignResponse = wire.SignResponse

// errFrameTooLarge is wrapped into every frame-size rejection so callers can
// distinguish it from a transport or decode error with errors.Is.
var errFrameTooLarge = errors.New(
	"kesagent: frame exceeds configured maximum size",
)

// errFrameBodyStalled wraps every failure to read a frame body whose length
// the peer already declared, so a peer that announces a frame and then stops
// sending is greppable in a node log and distinguishable from a clean EOF
// between frames.
var errFrameBodyStalled = errors.New(
	"kesagent: frame body did not arrive after its length header",
)

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
	binary.BigEndian.PutUint32(
		hdr[:],
		uint32(len(payload)),
	) // #nosec G115 -- bounded by maxSize above
	n, err := w.Write(hdr[:])
	if err != nil {
		return fmt.Errorf("kesagent: write frame header: %w", err)
	}
	if n != len(hdr) {
		return fmt.Errorf("kesagent: write frame header: %w", io.ErrShortWrite)
	}
	n, err = w.Write(payload)
	if err != nil {
		return fmt.Errorf("kesagent: write frame payload: %w", err)
	}
	if n != len(payload) {
		return fmt.Errorf("kesagent: write frame payload: %w", io.ErrShortWrite)
	}
	return nil
}

// readFrame reads a single length-prefixed JSON frame into v, refusing to
// read more than maxSize bytes of payload regardless of what the length
// prefix declares.
func readFrame(r io.Reader, maxSize int, v any) error {
	return readFrameGuarded(r, maxSize, nil, v)
}

// readFrameGuarded is readFrame with a hook that runs after the length header
// has been read and before any of the body is: the one point at which a
// caller whose header wait is legitimately unbounded can still put a deadline
// on the body. A nil bodyGuard means the caller has already bounded the whole
// read (Client.connectLocked and Client.Sign each set a whole-operation conn
// deadline) or is reading from something that cannot stall (a test buffer).
//
// The framing is bursa's wire.ReadFrameLimit, which sets no deadlines and
// reads the body into one exact-size buffer it wipes. This wrapper counts the
// bytes it hands over, so it can still tell a declared length over maxSize
// (errFrameTooLarge) and a body that stopped arriving (errFrameBodyStalled)
// apart from other failures.
func readFrameGuarded(
	r io.Reader,
	maxSize int,
	bodyGuard func() error,
	v any,
) error {
	cr := &frameCountingReader{r: r}
	guard := func() error {
		cr.headerDone = true
		if bodyGuard == nil {
			return nil
		}
		if err := bodyGuard(); err != nil {
			cr.guardFailed = true
			return fmt.Errorf("kesagent: bound frame body read: %w", err)
		}
		return nil
	}
	err := wire.ReadFrameLimit(cr, maxSize, guard, v)
	if err == nil {
		return nil
	}
	if !cr.headerDone {
		// The header arrived but no body read began: wire refused the
		// declared length.
		if cr.read == len(cr.hdr) {
			n := binary.BigEndian.Uint32(cr.hdr[:])
			if n != 0 && int64(n) > int64(maxSize) {
				return fmt.Errorf(
					"%w: declared %d bytes > %d",
					errFrameTooLarge,
					n,
					maxSize,
				)
			}
		}
		return err
	}
	if cr.guardFailed {
		return err
	}
	// The body is short of its declared length: it stalled or the
	// connection ended. A complete body that failed to decode is not.
	declared := int64(binary.BigEndian.Uint32(cr.hdr[:]))
	if int64(cr.read) < int64(len(cr.hdr))+declared {
		return fmt.Errorf("%w: %w", errFrameBodyStalled, err)
	}
	return err
}

// frameCountingReader passes reads through to r, keeping the frame's 4-byte
// length header and a count of the bytes delivered, so readFrameGuarded can
// tell where in a frame wire.ReadFrameLimit stopped.
type frameCountingReader struct {
	r           io.Reader
	hdr         [4]byte
	read        int
	headerDone  bool
	guardFailed bool
}

func (c *frameCountingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if c.read < len(c.hdr) {
		copy(c.hdr[c.read:], p[:n])
	}
	c.read += n
	return n, err
}

// wipeBytes zeroes b in place. It is used on every buffer that has held KES
// signing key material, so a pushed key does not outlive its use in a heap
// array nobody owns any more. Best-effort by nature: Go offers no way to
// guarantee a wiped page was never swapped out first, and unlike bursa's
// agent side this process does not mlock the pages it decodes into.
func wipeBytes(b []byte) {
	clear(b)
	runtime.KeepAlive(b)
}
