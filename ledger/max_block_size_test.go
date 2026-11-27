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
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
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
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	historical := mithrilRewardConwayPParams()
	historical.MaxBlockBodySize = 4000000
	historical.MaxBlockHeaderSize = 1100
	encoded, err := cbor.Encode(historical)
	require.NoError(t, err)
	require.NoError(t, seedDB(t, db).SetPParams(encoded, 100, 1, gledger.EraIdConway, nil))
	ls := &LedgerState{db: db, currentPParams: &conway.ConwayProtocolParameters{MaxBlockBodySize: 90112, MaxBlockHeaderSize: 1100}}
	fxAttachLedgerDB(t, ls)
	ls.publishSnapshotsLocked()
	require.Equal(t, uint64(4000000+1100+blockFramingAllowance), ls.MaxBlockSize())
}

// MaxBlockSize runs on every archive download, so the persisted history is
// read once rather than per call, and a later raise in the current limits
// still widens the bound.
func TestMaxBlockSizeReadsPersistedLimitsOnce(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	ls := &LedgerState{db: db, currentPParams: &conway.ConwayProtocolParameters{MaxBlockBodySize: 90112, MaxBlockHeaderSize: 1100}}
	reads := &atomic.Int32{}
	fxFaultedLedger(t, ls, SetFaultsForTest{PParamsReads: reads})
	historical := mithrilRewardConwayPParams()
	historical.MaxBlockBodySize = 4000000
	historical.MaxBlockHeaderSize = 1100
	encoded, err := cbor.Encode(historical)
	require.NoError(t, err)
	require.NoError(t, seedDB(t, db).SetPParams(encoded, 100, 1, gledger.EraIdConway, nil))
	ls.publishSnapshotsLocked()
	want := uint64(4000000 + 1100 + blockFramingAllowance)
	require.Equal(t, want, ls.MaxBlockSize())
	lists := reads.Load()
	require.Positive(t, lists)
	for range 3 {
		require.Equal(t, want, ls.MaxBlockSize())
	}
	require.Equal(t, lists, reads.Load())

	ls.currentPParams = &conway.ConwayProtocolParameters{MaxBlockBodySize: 5000000, MaxBlockHeaderSize: 1100}
	ls.publishSnapshotsLocked()
	require.Equal(t, uint64(5000000+1100+blockFramingAllowance), ls.MaxBlockSize())
	require.Equal(t, lists, reads.Load())
}

func TestMaxBlockSizeCachesPartialHistoryAfterMalformedRow(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	ls := &LedgerState{
		db: db,
		currentPParams: &conway.ConwayProtocolParameters{
			MaxBlockBodySize: 90112, MaxBlockHeaderSize: 1100,
		},
	}
	reads := &atomic.Int32{}
	fxFaultedLedger(t, ls, SetFaultsForTest{PParamsReads: reads})
	historical := mithrilRewardConwayPParams()
	historical.MaxBlockBodySize = 4000000
	historical.MaxBlockHeaderSize = 1100
	encoded, err := cbor.Encode(historical)
	require.NoError(t, err)
	require.NoError(t, seedDB(t, db).SetPParams(encoded, 100, 1, gledger.EraIdConway, nil))
	require.NoError(t, seedDB(t, db).SetPParams([]byte{0xff}, 101, 1, gledger.EraIdConway, nil))
	ls.publishSnapshotsLocked()
	want := uint64(4000000 + 1100 + blockFramingAllowance)
	require.Equal(t, want, ls.MaxBlockSize())
	lists := reads.Load()
	require.Positive(t, lists)
	for range 3 {
		require.Equal(t, want, ls.MaxBlockSize())
	}
	require.Equal(t, lists, reads.Load())
}

// A failed history read must not drop the bound to zero, which archive callers
// read as unknown and replace with a default far below the current limits.
func TestMaxBlockSizeKeepsCurrentLimitsWhenHistoryReadFails(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	ls := &LedgerState{db: db, currentPParams: &conway.ConwayProtocolParameters{MaxBlockBodySize: 180000, MaxBlockHeaderSize: 1100}}
	faults := fxFaultedLedger(t, ls, SetFaultsForTest{
		PParams: errors.New("set unavailable"),
	})
	ls.publishSnapshotsLocked()
	require.Equal(t, uint64(180000+1100+blockFramingAllowance), ls.MaxBlockSize())

	// The failed read is retried rather than remembered as an empty history.
	historical := mithrilRewardConwayPParams()
	historical.MaxBlockBodySize = 4000000
	historical.MaxBlockHeaderSize = 1100
	encoded, err := cbor.Encode(historical)
	require.NoError(t, err)
	require.NoError(t, seedDB(t, db).SetPParams(encoded, 100, 1, gledger.EraIdConway, nil))
	faults.PParams = nil
	require.Equal(t, uint64(4000000+1100+blockFramingAllowance), ls.MaxBlockSize())
}
