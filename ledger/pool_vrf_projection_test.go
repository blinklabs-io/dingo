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
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// newPoolVrfHeaderLedger is a ledger holding the registration and the stake
// snapshot that make block's issuer eligible in epoch 5.
func newPoolVrfHeaderLedger(
	t *testing.T,
	block *testBlockResult,
) (*LedgerState, *database.Database) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	ls := newEligibilityTestLedgerOnDB(t, block.epochNonce, db)
	key := block.block.IssuerVkey().Hash()
	seedBlockPoolRegistration(t, db, block.block)
	seedPoolStakeSnapshot(t, db, 4, key[:], 1_000_000_000)
	return ls, db
}

func TestHeaderStateResolvesRegisteredPoolVrfKey(t *testing.T) {
	t.Parallel()
	block := createTestBlock(t, [32]byte{72}, 0, tamperNone)
	ls, db := newPoolVrfHeaderLedger(t, block)
	require.NoError(t, ls.verifyBlockHeaderState(block.block, 5, false))
	_, err := seedDB(t, db).GetPoolVrfState(
		context.Background(),
		lcommon.PoolKeyHash{0xff},
		nil,
	)
	require.ErrorIs(t, err, models.ErrPoolNotFound)
}

func TestPoolVrfReadFailuresRemainLocalStateErrors(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{"registration hash", "storage failure"} {
		t.Run(fault, func(t *testing.T) {
			t.Parallel()
			block := createTestBlock(t, [32]byte{73}, 0, tamperNone)
			ls, _ := newPoolVrfHeaderLedger(t, block)
			require.NoError(t, ls.verifyBlockHeaderState(block.block, 5, false))
			key := block.block.IssuerVkey().Hash()
			faults := fxFaultedLedger(t, ls, SetFaultsForTest{})
			switch fault {
			case "registration hash":
				faults.ExtraPoolHistory = map[string][]PoolEvent{
					string(key[:]): {{
						Slot: 1,
						Registration: &models.PoolRegistration{
							PoolKeyHash: key[:],
							VrfKeyHash:  []byte{1},
							AddedSlot:   1,
						},
					}},
				}
			case "storage failure":
				faults.Pool = errInjectedStorage
			}
			err := ls.verifyBlockHeaderState(block.block, 5, false)
			require.ErrorIs(t, err, errHeaderStateLookupFailed)
			require.False(t, headerFailureBlamesPeer(err))
			if faults.Pool != nil {
				require.ErrorIs(t, err, errInjectedStorage)
			}
		})
	}
}
