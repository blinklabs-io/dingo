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

	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

func TestMaxBlockSizeFollowsCurrentProtocolParameters(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		currentPParams: &conway.ConwayProtocolParameters{
			MaxBlockBodySize:   90112,
			MaxBlockHeaderSize: 1100,
		},
	}
	ls.publishSnapshotsLocked()
	require.Equal(
		t,
		uint64(90112+1100+blockFramingAllowance),
		ls.MaxBlockSize(),
	)

	ls.currentPParams = &conway.ConwayProtocolParameters{
		MaxBlockBodySize:   180000,
		MaxBlockHeaderSize: 1100,
	}
	ls.publishSnapshotsLocked()
	require.Equal(
		t,
		uint64(180000+1100+blockFramingAllowance),
		ls.MaxBlockSize(),
	)
}

func TestMaxBlockSizeUnknownWithoutProtocolParameters(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	ls.publishSnapshotsLocked()
	require.Zero(t, ls.MaxBlockSize())
}
