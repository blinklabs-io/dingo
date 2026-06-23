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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ledger

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateCheckpointNoCheckpointsConfigured(t *testing.T) {
	require.NoError(t, ValidateCheckpoint(nil, 100, "abc"))
	require.NoError(t, ValidateCheckpoint(map[uint64]string{}, 100, "abc"))
}

func TestValidateCheckpointNoCheckpointAtHeight(t *testing.T) {
	cps := map[uint64]string{2000: "aabb"}
	// A block at a height with no configured checkpoint is always accepted,
	// regardless of its hash.
	require.NoError(t, ValidateCheckpoint(cps, 100, "ffff"))
}

func TestValidateCheckpointMatch(t *testing.T) {
	cps := map[uint64]string{2000: "aabbcc"}
	require.NoError(t, ValidateCheckpoint(cps, 2000, "aabbcc"))
}

func TestValidateCheckpointMatchIsCaseInsensitive(t *testing.T) {
	cps := map[uint64]string{2000: "AABBCC"}
	require.NoError(t, ValidateCheckpoint(cps, 2000, "aabbcc"))
}

func TestValidateCheckpointMismatch(t *testing.T) {
	cps := map[uint64]string{2000: "aabbcc"}
	err := ValidateCheckpoint(cps, 2000, "ddeeff")
	require.Error(t, err)
	var cpErr *CheckpointMismatchError
	require.True(t, errors.As(err, &cpErr))
	require.Equal(t, uint64(2000), cpErr.BlockNo)
	require.Equal(t, "aabbcc", cpErr.Expected)
	require.Equal(t, "ddeeff", cpErr.Actual)
}
