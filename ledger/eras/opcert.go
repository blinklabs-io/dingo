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

package eras

import "fmt"

// ValidateOpCertCounter enforces the operational-certificate issue-number
// counter rule shared by block application (ledger/verify_opcert.go) and
// the forge loop's pre-flight check (ledger/forging/keys.go): the single
// source of truth for the rule, so the two call sites cannot drift apart.
//
// A counter below the last-seen value is always rejected (stale or stolen
// hot key), in every era. A counter that skips ahead of it is rejected
// only when enforceNoGap is set: the over-increment (no-gap) rule is
// Praos-only (Babbage onward); TPraos eras (Shelley-Alonzo) accept any
// candidate at or above stored. When there is no recorded counter (found
// is false) there is no baseline to compare against, so the candidate is
// accepted.
//
// The gap comparison checks candidate > stored before subtracting, rather
// than comparing candidate > stored+1, so a stored value of
// math.MaxUint64 cannot wrap the addition to zero and misclassify an
// equal candidate as gapped.
func ValidateOpCertCounter(
	stored uint64,
	found bool,
	candidate uint64,
	enforceNoGap bool,
) error {
	if !found {
		return nil
	}
	if candidate < stored {
		return fmt.Errorf(
			"opcert counter %d is below last seen %d (stale or stolen hot key)",
			candidate,
			stored,
		)
	}
	if enforceNoGap && candidate > stored && candidate-stored > 1 {
		return fmt.Errorf(
			"opcert counter %d skips ahead of last seen %d (gapped rotation)",
			candidate,
			stored,
		)
	}
	return nil
}
