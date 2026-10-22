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

package sqlstore

import (
	"encoding/hex"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// Genesis pools are registered by the ledger's initial-state construction with
// mkStakePoolState (nesEL nes) SNothing (cardano-ledger 9df9f98c,
// eras/shelley/impl/src/Cardano/Ledger/Shelley/Transition.hs, the staking
// injection at the lines using nesELL), and the initial nesEL is epoch 0
// (initialEpochNo there). A genesis pool's BLS key is therefore registered in
// epoch 0, and a later same-key re-registration keeps it.

func genesisKeyedPool(
	t *testing.T,
	store *Store,
	pool lcommon.PoolKeyHash,
) {
	t.Helper()
	cert := leiosKeyEpochCert(pool, 0x11, 0x21)
	require.NoError(t, store.SetGenesisStakingWithDeposits(
		map[string]lcommon.PoolRegistrationCertificate{
			hex.EncodeToString(pool.Bytes()): *cert,
		},
		map[string]string{},
		2_000_000,
		500,
		nil,
		nil,
	))
}

func TestPoolLeiosKeyEpochGenesisKeyIsEpochZero(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 10)
	pool := depositHeldPoolKey(0xc1)

	genesisKeyedPool(t, store, pool)

	requireLeiosKeyEpoch(t, store, pool, 0, 0)
}

// A same-key re-registration of a genesis pool keeps the genesis epoch; it is
// not an imported key of unknown age.
func TestPoolLeiosKeyEpochGenesisSameKeyReRegistrationKeepsEpochZero(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 12)
	pool := depositHeldPoolKey(0xc2)

	genesisKeyedPool(t, store, pool)
	writeDepositHeldCert(t, store, 6_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 0)

	requireLeiosKeyEpoch(t, store, pool, 6_100, 0)
}

// A rotated key on a genesis pool is adopted at the next boundary.
func TestPoolLeiosKeyEpochGenesisRotatedKeyTakesNextBoundary(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 12)
	pool := depositHeldPoolKey(0xc3)

	genesisKeyedPool(t, store, pool)
	writeDepositHeldCert(t, store, 6_100, 0, leiosKeyEpochCert(pool, 0x12, 0x22), 0)

	requireLeiosKeyEpoch(t, store, pool, 6_100, 7)
}
