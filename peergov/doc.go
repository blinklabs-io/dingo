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

// Package peergov implements Dingo's peer governance: it decides who
// the node connects to, how many peers to maintain in each tier, and
// when to churn inactive peers out of the active set.
//
// PeerGovernor is the top-level type. It draws peer candidates from
// three sources in priority order:
//
//  1. Topology file (operator-configured peers)
//  2. Gossip via peer-sharing
//  3. Ledger peers (stake pool relays discovered from on-chain
//     registration data, enabled by UseLedgerAfterSlot)
//
// Peers progress through known → established → active tiers. Target
// counts and per-source quotas for the active tier are configurable
// (TargetNumberOfActivePeers, ActivePeersTopologyQuota,
// ActivePeersGossipQuota, ActivePeersLedgerQuota).
//
// # Churn
//
// A reconcile loop periodically compares the current active set to
// targets and opens new outbound connections or closes inactive ones
// to converge. Activity is tracked via TouchPeerByConnId, called
// from chainselection and the ouroboros handlers whenever a peer
// does useful work. Peers that go InactivityTimeout without touching
// are eligible for churn.
//
// # Chainsync ingress eligibility
//
// Not every connected peer should feed chainsync. PeerGovernor
// publishes PeerEligibilityChangedEvent to tell the node which
// connections are allowed to provide header/block data, preventing
// low-quality peers from backpressuring the primary chainsync
// stream.
package peergov
