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

package types_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

func TestBlockPayloadLocatorNamesTheBytesItWasGiven(t *testing.T) {
	t.Parallel()
	hash := bytes.Repeat([]byte{0xab}, 32)
	block := []byte("header|datum-one|datum-two|datum-one")
	locator, err := types.NewBlockPayloadLocator(77, hash, block)
	require.NoError(t, err)

	encoded, err := locator.LocatePayload([]byte("datum-two"))
	require.NoError(t, err)
	offset, err := types.DecodeCborOffset(encoded)
	require.NoError(t, err)
	require.EqualValues(t, 77, offset.BlockSlot)
	require.Equal(t, hash, offset.BlockHash[:])
	require.Equal(
		t,
		"datum-two",
		string(block[offset.ByteOffset:offset.ByteOffset+offset.ByteLength]),
	)

	// A repeated payload is named by its first occurrence.
	encoded, err = locator.LocatePayload([]byte("datum-one"))
	require.NoError(t, err)
	offset, err = types.DecodeCborOffset(encoded)
	require.NoError(t, err)
	require.EqualValues(t, indexOf(block, "datum-one"), offset.ByteOffset)
}

func indexOf(block []byte, payload string) int {
	return bytes.Index(block, []byte(payload))
}

func TestBlockPayloadLocatorRefusesWhatTheBlockDoesNotHold(t *testing.T) {
	t.Parallel()
	hash := bytes.Repeat([]byte{0xab}, 32)
	locator, err := types.NewBlockPayloadLocator(1, hash, []byte("block"))
	require.NoError(t, err)

	_, err = locator.LocatePayload([]byte("absent"))
	require.Error(t, err)
	_, err = locator.LocatePayload(nil)
	require.Error(t, err)

	_, err = types.NewBlockPayloadLocator(1, hash[:31], []byte("block"))
	require.Error(t, err, "a block hash is 32 bytes")
}
