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

package txpump

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testPump(genesisTime time.Time, startupTimeout time.Duration) *Pump {
	return NewPump(
		&Config{StartupTimeout: startupTimeout},
		NewWallet(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		genesisTime,
	)
}

// TestWaitForGenesis proves that node connectivity cannot release transaction
// generation before the network start boundary.
func TestWaitForGenesis(t *testing.T) {
	pump := testPump(time.Now().Add(25*time.Millisecond), time.Second)
	started := time.Now()
	require.NoError(t, pump.waitForGenesis(context.Background(), nil))
	require.GreaterOrEqual(t, time.Since(started), 20*time.Millisecond)
}

// TestWaitForGenesisHonorsStartupTimeout ensures a malformed future runtime
// start surfaces as a readiness failure instead of hanging the workload.
func TestWaitForGenesisHonorsStartupTimeout(t *testing.T) {
	pump := testPump(time.Now().Add(time.Hour), time.Millisecond)
	startup := make(chan time.Time, 1)
	startup <- time.Now()
	err := pump.waitForGenesis(context.Background(), startup)
	require.ErrorContains(t, err, "genesis has not started")
}

// TestCurrentSlotBeforeGenesisIsZero guards the signed-duration to uint64
// conversion that previously produced an architecture-independent wrap.
func TestCurrentSlotBeforeGenesisIsZero(t *testing.T) {
	pump := testPump(time.Now().Add(time.Hour), time.Second)
	require.Zero(t, pump.currentSlot())
}
