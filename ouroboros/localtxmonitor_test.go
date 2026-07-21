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

package ouroboros

import (
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/mempool"
	olocaltxmonitor "github.com/blinklabs-io/gouroboros/protocol/localtxmonitor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocaltxmonitorServerGetMempoolReportsConfiguredCapacity(t *testing.T) {
	tests := []struct {
		name     string
		capacity int64
	}{
		{name: "praos default", capacity: 1024 * 1024},
		{name: "leios default", capacity: 25 * 1024 * 1024},
		{name: "custom", capacity: 7 * 1024 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o, _ := newTxSubmissionTestOuroboros(t, func(cfg *mempool.MempoolConfig) {
				cfg.MempoolCapacity = tt.capacity
			})
			o.LedgerState = newTestLedgerState(t)

			_, capacity, _, err := o.localtxmonitorServerGetMempool(
				olocaltxmonitor.CallbackContext{},
			)

			require.NoError(t, err)
			assert.Equal(t, uint32(tt.capacity), capacity) // #nosec G115 -- test values fit
		})
	}
}

func TestLocaltxmonitorServerGetMempoolRejectsUnrepresentableCapacity(t *testing.T) {
	o, _ := newTxSubmissionTestOuroboros(t, func(cfg *mempool.MempoolConfig) {
		cfg.MempoolCapacity = int64(math.MaxUint32) + 1
	})
	o.LedgerState = newTestLedgerState(t)

	_, _, _, err := o.localtxmonitorServerGetMempool(
		olocaltxmonitor.CallbackContext{},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be represented")
}
