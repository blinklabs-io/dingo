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

package sqlite

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// gapDepositFixture drives one pool through register / re-register / retire so
// the deposit POOLREAP refunds can be read back from the certificate rows.
type gapDepositFixture struct {
	store      *sqlstore.Store
	poolKey    []byte
	vrfKey     []byte
	rewardKey  []byte
	stakeKey   []byte
	nextTxByte byte
}

func newGapDepositFixture(t *testing.T) *gapDepositFixture {
	t.Helper()
	store, _ := newSharedSQLStore(t)
	return &gapDepositFixture{
		store:      store,
		poolKey:    bytes.Repeat([]byte{0xA1}, 28),
		vrfKey:     bytes.Repeat([]byte{0xA2}, 32),
		rewardKey:  bytes.Repeat([]byte{0xA3}, 28),
		stakeKey:   bytes.Repeat([]byte{0xA4}, 28),
		nextTxByte: 1,
	}
}

func (f *gapDepositFixture) hash() lcommon.Blake2b256 {
	var h lcommon.Blake2b256
	h[0] = f.nextTxByte
	f.nextTxByte++
	return h
}

func (f *gapDepositFixture) registration() lcommon.Certificate {
	return &lcommon.PoolRegistrationCertificate{
		CertType:      uint(lcommon.CertificateTypePoolRegistration),
		Operator:      lcommon.PoolKeyHash(f.poolKey),
		VrfKeyHash:    lcommon.VrfKeyHash(f.vrfKey),
		Pledge:        1_000_000,
		Cost:          340_000_000,
		Margin:        gcbor.Rat{Rat: big.NewRat(1, 100)},
		RewardAccount: lcommon.AddrKeyHash(f.rewardKey),
		PoolOwners: []lcommon.AddrKeyHash{
			lcommon.AddrKeyHash(f.stakeKey),
		},
	}
}

func (f *gapDepositFixture) retirement(epoch uint64) lcommon.Certificate {
	return &lcommon.PoolRetirementCertificate{
		CertType:    uint(lcommon.CertificateTypePoolRetirement),
		PoolKeyHash: lcommon.PoolKeyHash(f.poolKey),
		Epoch:       epoch,
	}
}

// applyLive writes a transaction through the normal block-apply path, which
// always carries calculated deposits.
func (f *gapDepositFixture) applyLive(
	t *testing.T,
	slot uint64,
	certificates []lcommon.Certificate,
	deposits map[int]uint64,
) {
	t.Helper()
	require.NoError(t, f.store.SetTransaction(
		&mockTransaction{
			hash:         f.hash(),
			isValid:      true,
			certificates: certificates,
		},
		ocommon.Point{Slot: slot, Hash: bytes.Repeat([]byte{0xb1}, 32)},
		0,
		deposits,
		false,
		nil,
	))
}

// applyGap writes a transaction through the Mithril gap path. deposits is what
// mithril's gapCertDeposits derives from the gap block's era and the epoch's
// protocol parameters.
func (f *gapDepositFixture) applyGap(
	t *testing.T,
	slot uint64,
	certificates []lcommon.Certificate,
	deposits map[int]uint64,
) {
	t.Helper()
	require.NoError(t, f.store.SetGapBlockTransaction(
		&mockTransaction{
			hash:         f.hash(),
			isValid:      true,
			certificates: certificates,
		},
		ocommon.Point{Slot: slot, Hash: bytes.Repeat([]byte{0xb2}, 32)},
		0,
		deposits,
		nil,
	))
}

// TestGapBlockPoolRegistrationRefundsItsDeposit pins the refund POOLREAP pays a
// pool whose most recent registration was ingested from a Mithril gap block.
//
// GetPoolsRetiringAtEpoch takes the retiring pool's latest pool_registration
// row and applyPoolRetirements credits that row's deposit_amount as the refund.
// A gap block replays from raw CBOR with no ledger delta, so nothing upstream
// calculates its deposits; mithril's gapCertDeposits derives them from the
// block's era and the epoch's protocol parameters and passes them in here.
//
// Without them the gap registration is the latest row and records no deposit,
// which parseNullUint64 reads back as 0 -- indistinguishable from a pool that
// genuinely paid nothing -- and the operator's real deposit is never refunded.
func TestGapBlockPoolRegistrationRefundsItsDeposit(t *testing.T) {
	t.Parallel()
	const deposit = uint64(500_000_000)

	for _, test := range []struct {
		name string
		// gapDeposits is what the gap path supplies for the
		// re-registration at slot 200.
		gapDeposits map[int]uint64
		want        uint64
	}{
		{
			// Control: no gap re-registration at all, so the live
			// registration at slot 100 stays the latest row. This is the
			// refund the same pool must still receive once a gap block
			// re-registers it.
			name: "live registration only",
			want: deposit,
		},
		{
			name:        "gap re-registration carrying its deposit",
			gapDeposits: map[int]uint64{0: deposit},
			want:        deposit,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newGapDepositFixture(t)
			f.applyLive(
				t, 100,
				[]lcommon.Certificate{f.registration()},
				map[int]uint64{0: deposit},
			)
			if test.gapDeposits != nil {
				f.applyGap(
					t, 200,
					[]lcommon.Certificate{f.registration()},
					test.gapDeposits,
				)
			}
			f.applyLive(
				t, 300,
				[]lcommon.Certificate{f.retirement(5)},
				map[int]uint64{},
			)

			refunds, err := f.store.GetPoolsRetiringAtEpoch(5, 400, nil)
			require.NoError(t, err)
			require.Len(t, refunds, 1,
				"the pool must be found retiring at epoch 5")
			require.Equal(t, f.poolKey, refunds[0].PoolKeyHash)
			require.Equal(t,
				test.want,
				uint64(refunds[0].DepositAmount),
				"the refund must be the deposit the pool actually paid",
			)
		})
	}
}
