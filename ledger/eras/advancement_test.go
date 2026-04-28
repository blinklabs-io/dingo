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

package eras_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
)

// TestIsValidEraAdvancement_SameEra: a block claiming the same era as
// the ledger is valid (in-era epoch rollover).
func TestIsValidEraAdvancement_SameEra(t *testing.T) {
	for _, era := range eras.Eras {
		if !eras.IsValidEraAdvancement(era.Id, era.Id) {
			t.Errorf("same era %d (%s) must be a valid advancement",
				era.Id, era.Name)
		}
	}
}

// TestIsValidEraAdvancement_OneStepForward: a block claiming the era
// immediately after the ledger's era is valid (normal era boundary).
func TestIsValidEraAdvancement_OneStepForward(t *testing.T) {
	for i := 0; i < len(eras.Eras)-1; i++ {
		current := eras.Eras[i]
		next := eras.Eras[i+1]
		if !eras.IsValidEraAdvancement(current.Id, next.Id) {
			t.Errorf("one-step %d (%s) → %d (%s) must be valid",
				current.Id, current.Name, next.Id, next.Name)
		}
	}
}

// TestIsValidEraAdvancement_MultiStepForward_Rejected: a block claiming
// an era ≥2 ahead of the ledger is rejected. Each era boundary must be
// crossed by a block from that era; multi-step advancement would skip
// per-era epoch-based events that should have fired during the omitted
// span.
func TestIsValidEraAdvancement_MultiStepForward_Rejected(t *testing.T) {
	for i := 0; i < len(eras.Eras); i++ {
		for j := i + 2; j < len(eras.Eras); j++ {
			current := eras.Eras[i]
			next := eras.Eras[j]
			if eras.IsValidEraAdvancement(current.Id, next.Id) {
				t.Errorf("multi-step %d (%s) → %d (%s) must be rejected",
					current.Id, current.Name, next.Id, next.Name)
			}
		}
	}
}

// TestIsValidEraAdvancement_Backwards_Rejected: a block claiming an
// earlier era than the ledger's current era is rejected. The chain
// only advances forward through the era list.
func TestIsValidEraAdvancement_Backwards_Rejected(t *testing.T) {
	for i := 1; i < len(eras.Eras); i++ {
		for j := 0; j < i; j++ {
			current := eras.Eras[i]
			next := eras.Eras[j]
			if eras.IsValidEraAdvancement(current.Id, next.Id) {
				t.Errorf("backwards %d (%s) → %d (%s) must be rejected",
					current.Id, current.Name, next.Id, next.Name)
			}
		}
	}
}

// TestIsValidEraAdvancement_UnknownNextEra_Rejected: a next era ID
// outside the known era set (e.g. a block from a future era dingo
// hasn't been updated for) is rejected. Accepting it would be silently
// wrong: dingo cannot apply a HardForkFunc for an unknown era, so
// pparams translation would be skipped.
func TestIsValidEraAdvancement_UnknownNextEra_Rejected(t *testing.T) {
	currentEraId := eras.Eras[len(eras.Eras)-1].Id
	if eras.IsValidEraAdvancement(currentEraId, currentEraId+5) {
		t.Errorf("advancement to unknown era %d must be rejected",
			currentEraId+5)
	}
}

// TestIsValidEraAdvancement_UnknownCurrentEra_Rejected: a current era
// ID outside the known era set has no defined "next era" so any
// advancement is rejected. This guards against caller bugs where the
// snapshot era was zero or otherwise corrupt.
func TestIsValidEraAdvancement_UnknownCurrentEra_Rejected(t *testing.T) {
	unknownEraId := uint(99)
	if eras.IsValidEraAdvancement(unknownEraId, unknownEraId) {
		t.Errorf("advancement from unknown era %d must be rejected",
			unknownEraId)
	}
	if eras.IsValidEraAdvancement(unknownEraId, eras.ConwayEraDesc.Id) {
		t.Errorf("advancement from unknown era %d to known era must be rejected",
			unknownEraId)
	}
}
