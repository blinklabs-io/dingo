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

package dingo

import (
	"testing"

	"github.com/blinklabs-io/dingo/ledger/leader"
)

// TestEpochInfoAdapterProvidesExactActiveSlotCoeff pins the wiring that makes
// the leader schedule use the exact Shelley genesis active slot coefficient.
//
// leader.ActiveSlotCoeffRatProvider is an optional interface: computeSchedule
// type-asserts it and silently falls back to the float64 accessor when it is not
// satisfied. Without this assertion, dropping or renaming
// epochInfoAdapter.ActiveSlotCoeffRat would compile cleanly and quietly restore
// the float64 approximation, which yields a strictly larger leadership threshold
// than the reference node's (dingo #2798).
func TestEpochInfoAdapterProvidesExactActiveSlotCoeff(t *testing.T) {
	var adapter any = &epochInfoAdapter{}
	if _, ok := adapter.(leader.EpochInfoProvider); !ok {
		t.Fatal("epochInfoAdapter must satisfy leader.EpochInfoProvider")
	}
	if _, ok := adapter.(leader.ActiveSlotCoeffRatProvider); !ok {
		t.Fatal(
			"epochInfoAdapter must satisfy " +
				"leader.ActiveSlotCoeffRatProvider so the leader schedule " +
				"uses the exact genesis coefficient",
		)
	}
}
