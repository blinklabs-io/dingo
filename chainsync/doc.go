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

// Package chainsync tracks the state of Dingo's block-synchronization
// sessions with connected peers. It does not speak the Ouroboros
// chainsync mini-protocol directly — that lives in the ouroboros
// package — but holds the per-peer tracking state the rest of the node
// queries and mutates.
//
// The State type tracks which connection is currently the "primary"
// chainsync client, observed headers per peer, stall detection
// timestamps, and ingress-eligibility flags. It is thread-safe and is
// shared by the chain selector, the ouroboros handlers, and the node's
// stall recycler.
//
// Chainsync clients can enter a stalled state when a peer stops
// delivering headers while still holding the connection open. The
// node's stall recycler reads tracked-client status from this package
// and issues ConnectionRecycleRequestedEvent to recover.
package chainsync
