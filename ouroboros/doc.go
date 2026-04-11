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

// Package ouroboros hosts Dingo's handlers for the Ouroboros mini-
// protocols: chainsync, blockfetch, txsubmission, keepalive,
// peer-sharing, handshake, and the Leios prototype protocols
// (LeiosFetch, LeiosNotify) when enabled.
//
// The Ouroboros type in this package is the integration surface
// between Dingo and the gouroboros library. It does not open
// sockets itself — that is connmanager's job. Instead, it receives
// connection lifecycle events from the connmanager via the
// EventBus (InboundConnectionEvent, OutboundConnectionEvent,
// ConnectionClosedEvent), attaches mini-protocol handlers to each
// new connection, and routes inbound messages to the appropriate
// downstream component:
//
//   - chainsync headers → chainsync.State and chainselection
//   - blockfetch blocks → ledger (validation) → chain (storage)
//   - txsubmission txs  → mempool
//
// Two modes are supported on a single Ouroboros instance:
//
//   - N2N (node-to-node) on the public listener
//   - N2C (node-to-client) on the private listener and Unix socket
//
// The handshake handler negotiates protocol versions per connection.
// N2C sockets are intended for local wallets and tooling, so there
// is no authentication — reverse proxies or socket permissions are
// the boundary. N2N connections are gated by peergov eligibility.
package ouroboros
