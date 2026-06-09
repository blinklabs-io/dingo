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

// Package leios implements the CIP-0164 stake-truncated voting committee,
// stake-quorum vote tallying, and endorser-block certificate construction
// and validation.
//
// The voting committee for an epoch is computed deterministically from the
// active stake distribution: pools are ordered by stake descending and
// selected until the cumulative-stake coverage target (the Dijkstra
// CommitteeStakeCoverage protocol parameter, sigma_c in CIP-0164) is
// reached. Each selected pool is assigned a stable voter_id equal to its
// position in the selected order. Votes carry a BLS12-381 MinSig signature
// (48-byte signatures in G1, 96-byte public keys in G2) over the voted
// endorser block. Collected votes meet quorum when their combined stake is
// at least the QuorumStakeThreshold protocol parameter (tau) times total
// active stake. Quorum produces a certificate of the signers bitfield plus
// one aggregated BLS signature.
//
// All state is in-memory: the committee is a pure function of the persisted
// stake snapshots and is recomputed on demand; votes live in a TTL-bounded
// store.
package leios
