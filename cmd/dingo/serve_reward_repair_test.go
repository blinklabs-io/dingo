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

package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/mithril"
	"github.com/stretchr/testify/require"
)

func TestCheckSyncStateAllowsInterruptedRewardRepairToResume(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &config.Config{
		RunMode:      config.RunModeServe,
		StorageMode:  "core",
		Network:      "preview",
		DatabasePath: t.TempDir(),
		Plugins:      testStoragePlugins(),
	}
	runtime, err := openConfiguredDatabase(context.Background(), cfg, logger, 1)
	require.NoError(t, err)
	require.NoError(t, runtime.Database.SetSyncState(
		"sync_status", syncStatusInProgress, nil,
	))
	require.NoError(t, runtime.Database.SetSyncState(
		mithril.RewardStateRepairPendingKey, "1", nil,
	))
	require.NoError(t, runtime.Close(context.Background()))

	require.NoError(t, checkSyncState(cfg, logger),
		"serve must resume an interrupted in-place repair before node startup")
}

func TestRetryMithrilRewardStateRepairWaitsForNewSnapshot(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	attempts := 0
	err := retryMithrilRewardStateRepair(
		context.Background(),
		logger,
		time.Nanosecond,
		func() error {
			attempts++
			if attempts == 1 {
				return mithril.ErrRewardStateRepairWaitingForSnapshot
			}
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, 2, attempts)
}

func TestRetryMithrilRewardStateRepairStopsOnOtherErrors(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	wantErr := context.DeadlineExceeded
	attempts := 0
	err := retryMithrilRewardStateRepair(
		context.Background(), logger, time.Nanosecond,
		func() error {
			attempts++
			return wantErr
		},
	)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 1, attempts)
}
