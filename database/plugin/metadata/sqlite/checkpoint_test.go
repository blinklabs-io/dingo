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

package sqlite

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckpointWALDoesNotTruncateBusyPassiveCheckpoint(t *testing.T) {
	var modes []string
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

	err := checkpointWALWith(
		context.Background(),
		logger,
		func(_ context.Context, mode string) (int, int, int, error) {
			modes = append(modes, mode)
			if mode == "PASSIVE" {
				return 1, 10, 5, nil
			}
			t.Fatal("TRUNCATE must not run after an incomplete PASSIVE checkpoint")
			return 0, 0, 0, nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"PASSIVE"}, modes)
}

func TestCheckpointWALTruncatesOnlyAfterPassiveDrainsWAL(t *testing.T) {
	var modes []string
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

	err := checkpointWALWith(
		context.Background(),
		logger,
		func(_ context.Context, mode string) (int, int, int, error) {
			modes = append(modes, mode)
			return 0, 10, 10, nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"PASSIVE", "TRUNCATE"}, modes)
}
