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

package hardfork_test

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/stretchr/testify/assert"
)

func TestBound_Zero(t *testing.T) {
	var b hardfork.Bound
	assert.Equal(t, time.Duration(0), b.RelativeTime)
	assert.Equal(t, uint64(0), b.Slot)
	assert.Equal(t, uint64(0), b.Epoch)
}

func TestEraParams_Zero(t *testing.T) {
	var p hardfork.EraParams
	assert.Equal(t, uint64(0), p.EpochSize)
	assert.Equal(t, time.Duration(0), p.SlotLength)
	assert.Equal(t, uint64(0), p.SafeZoneSlots)
	assert.Equal(t, uint64(0), p.GenesisWindow)
}

func TestEraParams_Valid(t *testing.T) {
	p := hardfork.EraParams{
		EpochSize:     432_000,
		SlotLength:    time.Second,
		SafeZoneSlots: 25_920,
		GenesisWindow: 25_920,
	}
	assert.NoError(t, p.Validate())
}

func TestEraParams_ValidateRejectsZeroEpochSize(t *testing.T) {
	p := hardfork.EraParams{
		EpochSize:  0,
		SlotLength: time.Second,
	}
	assert.Error(t, p.Validate())
}

func TestEraParams_ValidateRejectsZeroSlotLength(t *testing.T) {
	p := hardfork.EraParams{
		EpochSize:  432_000,
		SlotLength: 0,
	}
	assert.Error(t, p.Validate())
}
