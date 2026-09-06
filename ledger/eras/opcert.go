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

import (
	"fmt"
	"math"
)

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

// MaxPersistableOpCertCounter is the highest operational-certificate issue
// number dingo can record for a pool.
//
// The reference imposes no bound: cardano-ledger decodes the counter as
// Word64 (Cardano.Protocol.TPraos.OCert) and the CDDL declares it uint .size
// 8, so every value up to math.MaxUint64 is a well-formed counter. dingo's
// bound is narrower because it persists what cardano-node only holds in
// memory. pool_opcert_sequence.sequence and pool.latest_op_cert_sequence are
// signed engine integers that carry both the value and the ordering the
// monotonicity rule reads -- MAX(sequence) per pool, the
// latest_op_cert_sequence < ? guard on the denormalized maximum, and the
// (pool_key_hash, sequence) index GetChainDepState's aggregate is served
// from. A counter above math.MaxInt64 stored in those columns as two's
// complement would order below every smaller counter and silently invert
// each of those three reads.
//
// The bound is unreachable from Babbage onward: Praos rejects a counter more
// than one past the last seen (CounterOverIncrementedOCERT) and a registered
// pool with no recorded counter has a baseline of zero (currentIssueNo), so
// reaching it would take 2^63 rotations. Only the TPraos eras
// (Shelley-Alonzo), which enforce monotonicity alone, admit an arbitrary
// first counter -- so this is the one place where dingo is narrower than the
// reference, and it says so at the boundary rather than failing part-way
// through block application.
const MaxPersistableOpCertCounter = uint64(math.MaxInt64)

// ValidateOpCertPersistableCounter rejects an operational-certificate issue
// number dingo cannot record, naming the bound and why it exists.
//
// It is deliberately separate from ValidateOpCertCounter: that rule is the
// era-scoped chain rule and is only evaluated for blocks being validated,
// while this bound governs every counter that reaches the metadata store --
// an unvalidated replay and a locally forged block included.
func ValidateOpCertPersistableCounter(candidate uint64) error {
	if candidate > MaxPersistableOpCertCounter {
		return fmt.Errorf(
			"opcert counter %d exceeds the highest counter dingo records (%d); the reference accepts it but the pool_opcert_sequence ordering cannot represent it",
			candidate,
			MaxPersistableOpCertCounter,
		)
	}
	return nil
}
