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
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStorageModeValid(t *testing.T) {
	tests := []struct {
		mode  StorageMode
		valid bool
	}{
		{StorageModeCore, true},
		{StorageModeAPI, true},
		{"", false},
		{"invalid", false},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.valid, tt.mode.Valid(), "mode=%q", tt.mode)
	}
}

func TestStorageModeIsAPI(t *testing.T) {
	assert.False(t, StorageModeCore.IsAPI())
	assert.True(t, StorageModeAPI.IsAPI())
}

func TestWithStorageMode(t *testing.T) {
	cfg := &Config{}

	// Default should be zero value (empty string)
	assert.Equal(t, StorageMode(""), cfg.storageMode)

	// Apply API mode
	WithStorageMode(StorageModeAPI)(cfg)
	assert.Equal(t, StorageModeAPI, cfg.storageMode)

	// Apply core mode
	WithStorageMode(StorageModeCore)(cfg)
	assert.Equal(t, StorageModeCore, cfg.storageMode)
}

func TestPeerGovernorOptionsIgnoreNonPositiveValues(t *testing.T) {
	cfg := &Config{}

	WithMinHotPeers(-1)(cfg)
	WithReconcileInterval(-1 * time.Minute)(cfg)
	WithInactivityTimeout(-5 * time.Minute)(cfg)
	WithMaxConnectionsPerIP(-2)(cfg)
	WithMaxInboundConns(0)(cfg)

	assert.Zero(t, cfg.minHotPeers)
	assert.Zero(t, cfg.reconcileInterval)
	assert.Zero(t, cfg.inactivityTimeout)
	assert.Zero(t, cfg.maxConnectionsPerIP)
	assert.Zero(t, cfg.maxInboundConns)
}

func TestPeerGovernorOptionsApplyPositiveValues(t *testing.T) {
	cfg := &Config{}

	WithMinHotPeers(3)(cfg)
	WithReconcileInterval(30 * time.Second)(cfg)
	WithInactivityTimeout(2 * time.Minute)(cfg)
	WithMaxConnectionsPerIP(4)(cfg)
	WithMaxInboundConns(25)(cfg)

	assert.Equal(t, 3, cfg.minHotPeers)
	assert.Equal(t, 30*time.Second, cfg.reconcileInterval)
	assert.Equal(t, 2*time.Minute, cfg.inactivityTimeout)
	assert.Equal(t, 4, cfg.maxConnectionsPerIP)
	assert.Equal(t, 25, cfg.maxInboundConns)
}
