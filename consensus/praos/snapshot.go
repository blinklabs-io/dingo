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

package praos

// StakeSnapshotEpoch returns the epoch whose mark stake snapshot is active for
// Praos leader election in the given epoch. The first epochs saturate to the
// genesis snapshot epoch.
//
// Timing (matches cardano-ledger): leader election in epoch E is validated
// against the stake distribution as of the end of epoch E-2 (the reference
// node's "set" snapshot / nesPd; the canonical rule that a delegation in epoch N
// becomes active for leadership in N+2). dingo captures mark[K] at the boundary
// into epoch K, i.e. as of the end of epoch K-1 (SnapshotSlot = boundary-1), so
// mark[K] = stake_end(K-1) — the same as the reference "mark at epoch K". The
// stake_end(E-2) distribution the leader check needs is therefore mark[E-1], so
// this returns E-1 (previously E-2, which used stake_end(E-3) — one epoch stale;
// harmless on networks with a stable pool set but on a churny chain it drops
// pools that first delegated in E-2, spuriously rejecting their epoch-E blocks).
func StakeSnapshotEpoch(epoch uint64) uint64 {
	if epoch >= 1 {
		return epoch - 1
	}
	return 0
}
