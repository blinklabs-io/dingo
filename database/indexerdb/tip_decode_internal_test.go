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

package indexerdb

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeTipRejectsPositionsBeyondInt64(t *testing.T) {
	t.Parallel()
	tooBig := strconv.FormatUint(math.MaxInt64+1, 10)
	maxOK := strconv.FormatUint(math.MaxInt64, 10)

	tip, err := decodeTip(maxOK + ":" + maxOK + ":ab")
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxInt64), tip.BlockNumber)

	_, err = decodeTip("1:" + tooBig + ":ab")
	require.Error(t, err, "block number above int64")
	_, err = decodeTip(tooBig + ":1:ab")
	require.Error(t, err, "slot above int64")
}
