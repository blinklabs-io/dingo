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

// Package dmq implements CIP-0137's Decentralized Message Queue: the generic
// message pool shared by every DMQ topic instance, and the Stack that serves
// it to local producers and consumers over a Unix socket alongside the Cardano
// stack. The pool itself holds no opinion about authentication, KES/opcert
// validation, network wiring, or peer selection.
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
// phase 3's job.
//
// # Size limits and backpressure
//
// Config.Capacity and Config.MaxMessages bound the pool's total CBOR-encoded
// size and message count. Add rejects a message that would exceed either
// bound with ErrFull, and rejects an already-expired message with ErrExpired,
// so callers can apply their own backpressure or reply with a CIP-0137 reject
// reason without the pool growing unbounded between TTL sweeps.
//
// # Local submission and notification
//
// A Stack owns one topic instance: a MessageMempool, a connection manager of
// its own, and a Unix socket speaking the DMQ node-to-client handshake under
// the topic's network magic. Each connection carries both local mini-protocols.
//
// Local message submission validates a message in order -- already expired,
// the current KES period when one is configured, expiry beyond the configured
// message TTL, CIP-0137 authentication -- and then admits it to the pool. It
// replies accept, or rejects with the current CIP-0137 reason: expired,
// invalid (with the validation error), alreadyReceived for a duplicate ID, or
// other (with the error) when the current KES period cannot be resolved or the
// pool is full.
//
// Local message notification gives each connection its own cursor over the
// pool, so every consumer receives each unexpired message once. A feeder
// goroutine moves messages from the cursor into the connection's notification
// queue and holds back any message the queue refuses, so a slow consumer
// delays messages but loses none except those that expire before delivery. Blocking and non-blocking requests are answered by the
// gouroboros notification server.
package dmq
