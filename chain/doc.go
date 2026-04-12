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

// Package chain manages Dingo's blockchain state: the primary chain, any
// alternate (candidate) chains, fork detection, and rollback orchestration.
//
// ChainManager is the top-level type. It owns the primary Chain, tracks
// alternate chains observed from peers, and emits events on the global
// EventBus when a fork is detected, when a rollback occurs, or when a new
// block is added.
//
// Blocks are persisted through the database package; this package stores
// only the structural relationships between blocks and the metadata needed
// to compare competing chains. Chain selection itself (deciding which
// candidate becomes primary) lives in the chainselection package.
//
// Key event types emitted from this package:
//
//   - ChainUpdateEventType      — a block was added to the primary chain, or
//     the primary chain was rolled back to a point
//   - ChainForkEventType        — a fork was observed against the primary chain
package chain
