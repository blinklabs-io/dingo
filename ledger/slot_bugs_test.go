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

package ledger

import (
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// futureSystemStartCfg returns a CardanoNodeConfig whose Shelley
// SystemStart is in the future, simulating a node that booted before the
// configured genesis time (clock skew, misconfig, or early bring-up).
func futureSystemStartCfg(t *testing.T, future time.Time) *cardano.CardanoNodeConfig {
	t.Helper()
	var sb strings.Builder
	sb.WriteString(`{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"slotLength": 1,
		"epochLength": 432000,
		"systemStart": "`)
	sb.WriteString(future.UTC().Format(time.RFC3339))
	sb.WriteString(`"
	}`)
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(sb.String())))
	return cfg
}

// TestTimeToSlot_FutureTimeWithEmptyCacheReturnsError pins that when the
// epoch cache is empty, TimeToSlot rejects arbitrary future times instead of
// silently returning a "now"-ish approximation.
//
// The implementation falls through to nearNowSlot whenever `time.Since(t) <
// 5*time.Second`. Because `time.Since` is `now - t`, that is NEGATIVE (and
// therefore always `< 5*time.Second`) for any t in the future — so a caller
// asking about a time one day ahead gets the current slot, not an error.
func TestTimeToSlot_FutureTimeWithEmptyCacheReturnsError(t *testing.T) {
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"slotLength": 1,
		"epochLength": 432000,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))

	ls := &LedgerState{
		// epochCache empty — HardForkSummary will error.
		config: LedgerStateConfig{CardanoNodeConfig: cfg},
	}

	// Far-future time; well past any "near now" tolerance.
	future := time.Now().Add(24 * time.Hour)
	_, err := ls.TimeToSlot(future)
	assert.Error(t, err,
		"TimeToSlot must reject far-future times when the epoch cache is empty; "+
			"the nearNowSlot fallback is only for times within ±5s of now")
}

// TestNearNowSlot_FutureSystemStartReturnsZero pins that nearNowSlot does
// not silently wrap a negative `time.Since` to a huge uint64. Under clock
// skew or node-before-genesis boot, `time.Since(SystemStart)` is negative,
// and `uint64(negative)` produces a near-MaxUint value — bogus and
// indistinguishable from a valid slot.
func TestNearNowSlot_FutureSystemStartReturnsZero(t *testing.T) {
	cfg := futureSystemStartCfg(t, time.Now().Add(time.Hour))
	got := nearNowSlot(cfg.ShelleyGenesis())
	assert.Equal(t, uint64(0), got,
		"nearNowSlot with SystemStart in the future must return 0, not a huge wrapped uint64")
}
