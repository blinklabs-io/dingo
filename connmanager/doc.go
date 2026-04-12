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

// Package connmanager owns the lifecycle of network connections between
// Dingo and its peers and clients. It accepts inbound connections on
// configured listeners (TCP, Unix sockets, Windows named pipes), opens
// outbound connections on demand, and enforces per-IP and global
// connection caps.
//
// ConnectionManager is the top-level type. It publishes events on the
// EventBus for every connection lifecycle transition — inbound,
// outbound, closed, recycle-requested — which the ouroboros, peergov,
// and chainselection packages subscribe to.
//
// This package intentionally does not know anything about Ouroboros
// mini-protocols. It hands accepted connections over to the ouroboros
// package, which sets up the chainsync, blockfetch, keepalive, and
// other mini-protocol handlers.
//
// The connection manager is also the recipient of
// ConnectionRecycleRequestedEvent from the node's stall recycler: when
// a peer is selected for recycling, this package is the one that
// actually closes the underlying socket.
package connmanager
