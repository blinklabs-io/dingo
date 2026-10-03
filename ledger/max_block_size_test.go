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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
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

// Blocks served from an archive were admitted under their own era's limits.
// Byron main blocks of up to 148187 bytes and Byron epoch boundary blocks of
// about 648 KB exist on mainnet, both far above the Conway limits, so the
// bound must still admit the Byron genesis block size.
func TestMaxBlockSizeAdmitsByronHistory(t *testing.T) {
	t.Parallel()

	nodeConfig := &cardano.CardanoNodeConfig{}
	require.NoError(t, loadByronGenesisForTest(t, nodeConfig, strings.NewReader(
		`{"blockVersionData": {"slotDuration": "20000", `+
			`"maxBlockSize": "2000000", "maxHeaderSize": "2000000"}, `+
			`"protocolConsts": {"k": 2160, "protocolMagic": 764824073}}`,
	)))
	ls := &LedgerState{
		config: LedgerStateConfig{CardanoNodeConfig: nodeConfig},
		currentPParams: &conway.ConwayProtocolParameters{
			MaxBlockBodySize:   90112,
			MaxBlockHeaderSize: 1100,
		},
	}
	ls.publishSnapshotsLocked()
	require.Equal(t, uint64(2000000), ls.MaxBlockSize())

	ls.currentPParams = &conway.ConwayProtocolParameters{
		MaxBlockBodySize:   4000000,
		MaxBlockHeaderSize: 1100,
	}
	ls.publishSnapshotsLocked()
	require.Equal(
		t,
		uint64(4000000+1100+blockFramingAllowance),
		ls.MaxBlockSize(),
	)

	ls.currentPParams = nil
	ls.publishSnapshotsLocked()
	require.Equal(t, uint64(2000000), ls.MaxBlockSize())
}

func TestMaxBlockSizeRetainsPersistedHistoricalLimit(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, nil)
	require.NoError(t, err)
	historical := mithrilRewardConwayPParams()
	historical.MaxBlockBodySize = 4000000
	historical.MaxBlockHeaderSize = 1100
	encoded, err := cbor.Encode(historical)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(encoded, 100, 1, gledger.EraIdConway, nil))
	ls := &LedgerState{db: db, currentPParams: &conway.ConwayProtocolParameters{MaxBlockBodySize: 90112, MaxBlockHeaderSize: 1100}}
	ls.publishSnapshotsLocked()
	require.Equal(t, uint64(4000000+1100+blockFramingAllowance), ls.MaxBlockSize())
}
