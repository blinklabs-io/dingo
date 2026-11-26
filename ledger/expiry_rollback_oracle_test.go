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

package ledger_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func expireDrive(t *testing.T) setDrive {
	t.Helper()
	for _, d := range setDrives {
		if d.name == "CIP-0163 expiry" {
			return d
		}
	}
	require.FailNow(t, "no CIP-0163 expiry drive")
	return setDrive{}
}

// TestExpiryRollbackMatchesAFreshRunStoppedAtTheRollbackPoint requires the
// account and DRep reads after a rollback across the CIP-0163 activation to
// equal those of a run that applied the chain to the rollback point and
// stopped.
func TestExpiryRollbackMatchesAFreshRunStoppedAtTheRollbackPoint(t *testing.T) {
	t.Parallel()
	expire := expireDrive(t)
	idle := setDrive{
		name: "no activity",
		drive: func(*testing.T, *setHarness) setTouched {
			return setTouched{dreps: setDRepKeys[:2]}
		},
	}
	fresh, _ := accountsRun(t, idle)
	want := fresh["applied"]
	require.NotEmpty(t, want)

	got, _ := accountsRun(t, expire)
	require.NotEqual(t, want, got["applied"], "the expiry changes the reads")
	require.Equal(t, want, got["rolled back"])
}
