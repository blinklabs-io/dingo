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

package stakequery

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPoolQueryChunkSizeDefault(t *testing.T) {
	chunkSize := poolQueryChunkSize(nil)
	require.Equal(t, 800, chunkSize)

	_, cteArgs := activeDelegationCTE(nil, 0)
	const stakeQueryArgs = 2
	require.LessOrEqual(t, len(cteArgs)+stakeQueryArgs+chunkSize, 999)
}

func TestPreEpochPoolQueryChunkSizeSQLite(t *testing.T) {
	chunkSize := preEpochPoolQueryChunkSize(
		"sqlite",
		defaultPoolQueryChunkSize,
	)
	require.Equal(t, defaultPoolQueryChunkSize/2, chunkSize)
	// The query binds the pool chunk twice plus three scalar arguments.
	require.LessOrEqual(t, 2*chunkSize+3, 999)
}
