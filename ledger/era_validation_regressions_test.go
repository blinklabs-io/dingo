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
	"bytes"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// Regressions in this file drive Dingo's era validators with a real
// *LedgerView over a real database, the way block and mempool validation reach
// the upstream rules. A mock ledger state would satisfy optional capabilities
// the real view might not, which is how an inert rule goes unnoticed.

const retirementTestMaxEpoch = 18

// TestValidateTxConwayPoolRetirementEpochBound pins the retirement-epoch bound
// (current, current+eMax] through eras.ValidateTxConway with a real
// *LedgerView. The bound reads the current epoch through the optional
// common.EpochState capability and is skipped when the ledger state lacks it,
// so a view that stops providing the capability turns every out-of-range
// retirement into an accepted one.
func TestValidateTxConwayPoolRetirementEpochBound(t *testing.T) {
	t.Parallel()

	const (
		currentEpoch = uint64(5)
		epochLength  = uint(100)
		slot         = uint64(550)
	)
	ls, db := newRewardCalculationTestLedger(t)
	ls.epochCache = []models.Epoch{{
		EpochId:       currentEpoch,
		StartSlot:     500,
		LengthInSlots: epochLength,
	}}
	ls.publishSnapshotsLocked()
	lv := &LedgerView{ls: ls}

	pool := bytes.Repeat([]byte{0x3c}, lcommon.Blake2b224Size)
	vrfKeyHash := bytes.Repeat([]byte{0x3d}, lcommon.Blake2b256Size)
	rewardAccount := bytes.Repeat([]byte{0x3e}, lcommon.Blake2b224Size)
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash:   pool,
			VrfKeyHash:    vrfKeyHash,
			RewardAccount: rewardAccount,
		},
		&models.PoolRegistration{
			PoolKeyHash:   pool,
			VrfKeyHash:    vrfKeyHash,
			RewardAccount: rewardAccount,
			AddedSlot:     10,
		},
		nil,
	))
	pp := stakeRefundTestPparams()
	pp.MaxEpoch = retirementTestMaxEpoch

	for _, tc := range []struct {
		name    string
		epoch   uint64
		allowed bool
	}{
		{"the current epoch", currentEpoch, false},
		{"an earlier epoch", currentEpoch - 1, false},
		{"the next epoch", currentEpoch + 1, true},
		{"the last epoch within eMax", currentEpoch + retirementTestMaxEpoch, true},
		{"one epoch beyond eMax", currentEpoch + retirementTestMaxEpoch + 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cert := &lcommon.PoolRetirementCertificate{
				CertType:    uint(lcommon.CertificateTypePoolRetirement),
				PoolKeyHash: lcommon.PoolKeyHash(pool),
				Epoch:       tc.epoch,
			}
			tx := &conway.ConwayTransaction{
				TxIsValid: true,
				Body: conway.ConwayTransactionBody{
					TxFee: 200_000,
					TxCertificates: []lcommon.CertificateWrapper{{
						Type:        uint(lcommon.CertificateTypePoolRetirement),
						Certificate: cert,
					}},
				},
			}
			err := eras.ValidateTxConway(tx, slot, lv, pp)
			wrong, rejected := errors.AsType[shelley.StakePoolRetirementWrongEpochError](err)
			if tc.allowed {
				require.False(t, rejected,
					"retirement epoch %d must satisfy the bound: %v", tc.epoch, err)
				return
			}
			require.True(t, rejected,
				"retirement epoch %d must be rejected by the bound, got: %v", tc.epoch, err)
			require.Equal(t, tc.epoch, wrong.Supplied)
			require.Equal(t, currentEpoch, wrong.CurrentEpoch)
			require.Equal(t, currentEpoch+retirementTestMaxEpoch, wrong.LimitEpoch)
		})
	}
}
