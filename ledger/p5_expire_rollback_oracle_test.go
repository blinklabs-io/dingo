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

func p5ExpireDrive(t *testing.T) p5Drive {
	t.Helper()
	for _, d := range p5Drives {
		if d.name == "CIP-0163 expiry" {
			return d
		}
	}
	require.FailNow(t, "no CIP-0163 expiry drive")
	return p5Drive{}
}

// TestExpiryRollbackMatchesAFreshRunStoppedAtTheRollbackPoint requires the
// account and DRep reads after a rollback across the CIP-0163 activation to
// equal those of a run that applied the chain to the rollback point and
// stopped.
func TestExpiryRollbackMatchesAFreshRunStoppedAtTheRollbackPoint(t *testing.T) {
	t.Parallel()
	expire := p5ExpireDrive(t)
	idle := p5Drive{
		name: "no activity",
		drive: func(*testing.T, *p5Harness) p5Touched {
			return p5Touched{dreps: p5DRepKeys[:2]}
		},
	}
	fresh, _ := p5AccountsRun(t, nil, nil, idle)
	want := fresh["applied"]
	require.NotEmpty(t, want)

	got, _ := p5AccountsRun(t, nil, nil, expire)
	require.NotEqual(t, want, got["applied"], "the expiry changes the reads")
	require.Equal(t, want, got["rolled back"])
}
