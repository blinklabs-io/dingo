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

package eras_test

import (
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/stretchr/testify/require"
)

func TestLegacyDecentralizationUpdateUsesSourceEraShape(t *testing.T) {
	// Preview's epoch-1 proposal: {12: tag(30, [0, 1])}.
	legacyUpdate := []byte{0xa1, 0x0c, 0xd8, 0x1e, 0x82, 0x00, 0x01}

	decoded, err := eras.DecodePParamsUpdateAlonzo(legacyUpdate)
	require.NoError(t, err)
	update, ok := decoded.(alonzo.AlonzoProtocolParameterUpdate)
	require.True(t, ok)
	require.NotNil(t, update.Decentralization)
	require.Equal(t, 0, update.Decentralization.Cmp(big.NewRat(0, 1)))

	_, err = eras.DecodePParamsUpdateBabbage(legacyUpdate)
	require.Error(
		t,
		err,
		"Babbage must reject the removed legacy decentralization field; the update must be enacted before the era transition",
	)
}
