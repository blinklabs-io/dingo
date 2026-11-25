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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func statusReg(slot, position, effective uint64) PoolEvent {
	return PoolEvent{
		Slot: slot, Position: position, EffectiveSlot: effective,
		Registration: &models.PoolRegistration{},
	}
}

func statusRet(slot, position, epoch, effective uint64) PoolEvent {
	return PoolEvent{
		Slot: slot, Position: position, RetiringEpoch: epoch,
		EffectiveSlot: effective,
	}
}

// The cases mirror the metadata store's GetPoolStatus tests of the same
// scenarios, and must give the answers those assert.
func TestPoolStatusFromEventsMatchesTheMetadataStore(t *testing.T) {
	t.Parallel()
	epoch := func(e uint64) *uint64 { return &e }
	tests := []struct {
		name    string
		events  []PoolEvent
		current uint64
		want    models.PoolStatus
	}{
		{
			name:   "no registration",
			events: []PoolEvent{statusRet(10, 0, 3, 0)},
			want:   models.PoolStatus{Exists: true},
		},
		{
			name:   "registered",
			events: []PoolEvent{statusReg(10, 0, 0)},
			want:   models.PoolStatus{Exists: true, Active: true},
		},
		{
			name: "pending retirement",
			events: []PoolEvent{
				statusReg(10, 0, 0), statusRet(20, 0, 7, 0),
			},
			current: 5,
			want: models.PoolStatus{
				Exists: true, Active: true, RetirementEpoch: epoch(7),
			},
		},
		{
			name: "retirement reached",
			events: []PoolEvent{
				statusReg(10, 0, 0), statusRet(20, 0, 7, 0),
			},
			current: 7,
			want:    models.PoolStatus{Exists: true},
		},
		{
			name: "re-registration cancels the retirement",
			events: []PoolEvent{
				statusReg(10, 0, 0), statusRet(20, 0, 7, 0),
				statusReg(30, 0, 0),
			},
			current: 9,
			want:    models.PoolStatus{Exists: true, Active: true},
		},
		{
			// A snapshot import retires pools at their registration's slot
			// without a certificate; the retirement follows the registration.
			name: "synthetic retirement wins a slot tie",
			events: []PoolEvent{
				statusReg(1000, 0, 0),
				statusRet(1000, PoolEventSyntheticPosition, 7, 0),
			},
			current: 5,
			want: models.PoolStatus{
				Exists: true, Active: true, RetirementEpoch: epoch(7),
			},
		},
		{
			name: "registration after a synthetic retirement cancels it",
			events: []PoolEvent{
				statusReg(1000, 0, 0),
				statusRet(1000, PoolEventSyntheticPosition, 7, 0),
				statusReg(1001, 0, 0),
			},
			current: 5,
			want:    models.PoolStatus{Exists: true, Active: true},
		},
		{
			name: "retirement executes before the registration it follows",
			events: []PoolEvent{
				statusReg(1000, 0, 0), statusRet(2000, 0, 3, 900),
			},
			current: 2,
			want:    models.PoolStatus{Exists: true, Active: true},
		},
		{
			name: "registration executes before the retirement it follows",
			events: []PoolEvent{
				statusReg(1000, 0, 0), statusRet(1500, 0, 3, 0),
				statusReg(2000, 0, 1400),
			},
			current: 2,
			want: models.PoolStatus{
				Exists: true, Active: true, RetirementEpoch: epoch(3),
			},
		},
		{
			name: "registration executes before the retirement, epoch reached",
			events: []PoolEvent{
				statusReg(1000, 0, 0), statusRet(1500, 0, 3, 0),
				statusReg(2000, 0, 1400),
			},
			current: 3,
			want:    models.PoolStatus{Exists: true},
		},
		{
			// Chain order has the registration last; effective order puts
			// it before the retirement at 110.
			name: "late registration with an earlier effective slot",
			events: []PoolEvent{
				statusReg(50, 0, 0), statusRet(110, 0, 5, 0),
				statusReg(120, 0, 100),
			},
			current: 6,
			want:    models.PoolStatus{Exists: true},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := poolStatusFromEvents(tc.events, tc.current, true)
			require.Equal(t, tc.want, got)
		})
	}
	t.Run("no events", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, models.PoolStatus{}, poolStatusFromEvents(nil, 0, true))
	})
	t.Run("no epoch row leaves the retirement pending", func(t *testing.T) {
		t.Parallel()
		got := poolStatusFromEvents([]PoolEvent{
			statusReg(10, 0, 0), statusRet(20, 0, 7, 0),
		}, 0, false)
		require.Equal(t, models.PoolStatus{
			Exists: true, Active: true, RetirementEpoch: epoch(7),
		}, got)
	})
}
