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

// Package chainselection implements Ouroboros Praos multi-peer chain
// selection. It tracks the tip reported by each connected peer and picks
// the best candidate chain to follow using the standard Praos rules:
// longer chain wins, then density within the security window, then VRF
// tie-break.
//
// Genesis selection mode can be enabled at startup to prefer observed
// density during bootstrap before automatically falling back to Praos once
// the local tip is close enough to the best observed peer tip.
//
// ChainSelector is the top-level type. It consumes PeerTipUpdateEvent
// events from chainsync, compares peer tips against the local tip, and
// emits ChainSwitchEvent on the EventBus when the selected peer changes.
// The node's Run loop listens for those events and repoints the active
// chainsync stream at the new best peer.
//
// This package does not perform any block validation; it only selects
// which peer's chain to follow. Validation happens downstream in the
// ledger package after blocks are fetched from the selected peer.
package chainselection
