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

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/stretchr/testify/require"
)

func TestEraTransitionPathAllowsPrimeBoundaryPair(t *testing.T) {
	ls := &LedgerState{}
	path, ok := ls.eraTransitionPath(
		eras.MaryEraDesc.Id,
		eras.BabbageEraDesc.Id,
	)
	require.True(t, ok)
	require.Equal(
		t,
		[]uint{eras.AlonzoEraDesc.Id, eras.BabbageEraDesc.Id},
		path,
	)
}

func TestEraTransitionPathRejectsLargerJump(t *testing.T) {
	ls := &LedgerState{}
	path, ok := ls.eraTransitionPath(
		eras.MaryEraDesc.Id,
		eras.ConwayEraDesc.Id,
	)
	require.False(t, ok)
	require.Nil(t, path)
}

func TestBoundaryEraForBlockUsesSuccessorHeaderEra(t *testing.T) {
	ls := &LedgerState{}
	target := ls.boundaryEraForBlock(
		eras.MaryEraDesc.Id,
		eras.AlonzoEraDesc.Id,
		7,
		true,
	)
	require.Equal(t, eras.BabbageEraDesc.Id, target)
}

func TestBoundaryEraForBlockRejectsNonAdjacentHeaderEra(t *testing.T) {
	ls := &LedgerState{}
	target := ls.boundaryEraForBlock(
		eras.MaryEraDesc.Id,
		eras.AlonzoEraDesc.Id,
		12,
		true,
	)
	require.Equal(t, eras.AlonzoEraDesc.Id, target)
}
