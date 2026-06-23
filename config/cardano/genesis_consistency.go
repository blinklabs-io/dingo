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

package cardano

import (
	"fmt"
	"time"
)

// validateGenesisConsistency cross-checks invariants that must hold
// between genesis files but are not captured by any single file's hash.
//
// Currently it asserts that, when both a Byron and a Shelley genesis are
// present, the Byron startTime (Unix seconds) equals the Shelley
// systemStart. On every real Cardano network these are the same instant:
// the Shelley systemStart is the chain start time inherited from Byron.
// All slot-to-time and time-to-slot conversion (ledger/hardfork_summary.go,
// ledger/slot.go) is anchored on the Shelley systemStart, so a config
// whose two genesis files disagree would compute wrong wall-clock times
// for Byron-era slots. Failing closed at load time surfaces such a
// misconfiguration immediately rather than as silent time drift later.
func (c *CardanoNodeConfig) validateGenesisConsistency() error {
	if c.byronGenesis == nil || c.shelleyGenesis == nil {
		return nil
	}
	byronStart := int64(c.byronGenesis.StartTime)
	shelleyStart := c.shelleyGenesis.SystemStart.Unix()
	if byronStart != shelleyStart {
		return fmt.Errorf(
			"genesis system start mismatch: Byron startTime %d does not match Shelley systemStart %d (%s); slot-to-time conversion is anchored on the Shelley systemStart and would be wrong for Byron-era slots",
			byronStart,
			shelleyStart,
			c.shelleyGenesis.SystemStart.UTC().Format(time.RFC3339),
		)
	}
	return nil
}
