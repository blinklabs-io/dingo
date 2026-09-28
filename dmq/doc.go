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

// Package dmq implements phase 1 of CIP-0137's Decentralized Message Queue:
// the generic message pool shared by every DMQ topic instance. It holds no
// opinion about authentication, KES/opcert validation, network wiring, or
// peer selection -- those belong to later CIP-0137 phases (issues #1949-#1954).
//
// The wire types -- Message, MessagePayload, and OperationalCertificate, along
// with their CBOR encode/decode and message-ID computation -- already exist
// upstream as github.com/blinklabs-io/gouroboros/protocol/common's
// DmqMessage, DmqMessagePayload, and OperationalCertificate. This package
// reuses them rather than redefining CIP-0137's CDDL a second time.
//
// # MessageMempool
//
// MessageMempool de-duplicates messages by their 32-byte message ID, retains
// admitted messages in arrival order behind a size bound, and expires them
// once their CIP-0137 expiresAt timestamp passes. A background goroutine
// sweeps expired messages on a fixed interval; Start launches it and Stop
// tears it down.
//
// # Per-peer diffusion cursor
//
// Each connected peer gets its own FIFO cursor over the arrival-ordered
// message log, obtained by calling NextForPeer with that peer's identifier.
// A peer new to the pool starts at the beginning of the retained log; each
// call advances that peer's cursor past the message it returns. This mirrors
// CIP-0137's per-peer outstanding-message-ids queue for the node-to-node
// message-submission mini-protocol (protocol 18), without implementing the
// protocol's blocking/non-blocking request state machine itself -- that is
// phase 3's job (issue #1950).
//
// # Size limits and backpressure
//
// Config.Capacity and Config.MaxMessages bound the pool's total CBOR-encoded
// size and message count. Add rejects a message that would exceed either
// bound with ErrFull, and rejects an already-expired message with ErrExpired,
// so callers can apply their own backpressure or reply with a CIP-0137 reject
// reason without the pool growing unbounded between TTL sweeps.
package dmq
