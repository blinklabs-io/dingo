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

package database

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTxBodyProducedOutputRangeIgnoresNonOutputMatch(t *testing.T) {
	bodyCbor := []byte{
		0xa2,
		0x00, 0x43, 0x82, 0x01, 0x02,
		0x01, 0x82,
		0x82, 0x01, 0x02,
		0x82, 0x03, 0x04,
	}

	offset, length, found, err := txBodyProducedOutputRange(
		bodyCbor,
		0,
		0,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint32(8), offset)
	require.Equal(t, uint32(3), length)
	require.Equal(t, []byte{0x82, 0x01, 0x02}, bodyCbor[offset:offset+length])
}

func TestTxBodyProducedOutputRangeDuplicateOutputs(t *testing.T) {
	bodyCbor := []byte{
		0xa1,
		0x01, 0x82,
		0x82, 0x01, 0x02,
		0x82, 0x01, 0x02,
	}

	offset0, length0, found0, err := txBodyProducedOutputRange(
		bodyCbor,
		0,
		0,
	)
	require.NoError(t, err)
	require.True(t, found0)
	require.Equal(t, uint32(3), offset0)
	require.Equal(t, uint32(3), length0)
	require.Equal(
		t,
		[]byte{0x82, 0x01, 0x02},
		bodyCbor[offset0:offset0+length0],
	)

	offset1, length1, found1, err := txBodyProducedOutputRange(
		bodyCbor,
		0,
		1,
	)
	require.NoError(t, err)
	require.True(t, found1)
	require.Equal(t, uint32(6), offset1)
	require.Equal(t, uint32(3), length1)
	require.Equal(
		t,
		[]byte{0x82, 0x01, 0x02},
		bodyCbor[offset1:offset1+length1],
	)
}

func TestTxBodyProducedOutputRangeCollateralReturn(t *testing.T) {
	bodyCbor := []byte{
		0xa2,
		0x01, 0x81,
		0x82, 0x01, 0x02,
		0x10,
		0x82, 0x03, 0x04,
	}

	// 42 is an arbitrary base offset of bodyCbor inside an enclosing
	// block envelope; the returned offset is base + position-within-body.
	offset, length, found, err := txBodyProducedOutputRange(
		bodyCbor,
		42,
		1,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint32(49), offset)
	require.Equal(t, uint32(3), length)
	relativeOffset := offset - 42
	require.Equal(
		t,
		[]byte{0x82, 0x03, 0x04},
		bodyCbor[relativeOffset:relativeOffset+length],
	)
}
