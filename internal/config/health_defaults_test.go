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

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHealthPortDefaultsToDefaultHealthPort pins the health listener's
// defaults on newDefaultConfig, which is the only place they come from.
// NewHealthServer skips the listener entirely at HealthPort 0 and
// ApplyDefaults has no fill-in step for that field, so losing the literal
// disables the probe outright -- and resetGlobalConfig's separately
// maintained copy (config_test.go) seeds every other test in this package,
// so none of them would notice. Same blind spot as
// TestValidateForgedBlockDefaultsToTrue (flags_test.go).
func TestHealthPortDefaultsToDefaultHealthPort(t *testing.T) {
	defaults := newDefaultConfig()
	require.Equal(
		t,
		uint(DefaultHealthPort),
		defaults.HealthPort,
		"newDefaultConfig is the only source of the health listener port",
	)
	require.Equal(
		t,
		uint(DefaultHealthReadyGapSlots),
		defaults.HealthReadyGapSlots,
	)

	// ApplyDefaults refills HealthReadyGapSlots but deliberately not
	// HealthPort, so it cannot stand in as a second source.
	zeroed := newDefaultConfig()
	zeroed.HealthPort = 0
	zeroed.HealthReadyGapSlots = 0
	zeroed.ApplyDefaults()
	assert.Zero(t, zeroed.HealthPort)
	assert.Equal(
		t,
		uint(DefaultHealthReadyGapSlots),
		zeroed.HealthReadyGapSlots,
	)
}
