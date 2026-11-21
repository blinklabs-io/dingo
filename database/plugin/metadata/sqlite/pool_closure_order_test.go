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
	"math"
	"math/big"
	"testing"

	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A certifying ranking block applies its endorser-block closure first and then
// its own body, all under the ranking block's point. The ledger stores the
// closure at its endorser-block indexes and reserves that range, so the body's
// block indexes follow it. Position-ranked readers must then resolve a pool
// re-registered in the closure (index 3) and again in the body (index 4) to the
// body's parameters, the later application.
func TestPoolRegistrationOrderAcrossClosureAndBodyInOneRankingBlock(
	t *testing.T,
) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	poolKey := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0x51}, 28)),
	)
	rewardKey := bytes.Repeat([]byte{0x52}, 28)
	closureVrf := bytes.Repeat([]byte{0xC1}, 32)
	bodyVrf := bytes.Repeat([]byte{0xB0}, 32)
	register := func(vrf []byte) lcommon.Certificate {
		return &lcommon.PoolRegistrationCertificate{
			CertType:      uint(lcommon.CertificateTypePoolRegistration),
			Operator:      poolKey,
			VrfKeyHash:    lcommon.VrfKeyHash(lcommon.NewBlake2b256(vrf)),
			Pledge:        1_000_000,
			Cost:          340_000_000,
			Margin:        gcbor.Rat{Rat: big.NewRat(1, 100)},
			RewardAccount: lcommon.AddrKeyHash(lcommon.NewBlake2b224(rewardKey)),
		}
	}
	rbPoint := ocommon.Point{Slot: 500, Hash: bytes.Repeat([]byte{0xAB}, 32)}
	apply := func(txHashByte byte, blockIndex uint32, vrf []byte) {
		hash := lcommon.Blake2b256{}
		hash[0] = txHashByte
		require.NoError(t, store.SetTransaction(
			&mockTransaction{
				hash:         hash,
				isValid:      true,
				certificates: []lcommon.Certificate{register(vrf)},
			},
			rbPoint,
			blockIndex,
			map[int]uint64{0: 0},
			false,
			nil, 0,
		))
	}
	// Ledger application order: closure (endorser index 3), then body, whose
	// indexes start after the closure's reserved range.
	apply(0x01, 3, closureVrf)
	apply(0x02, 4, bodyVrf)

	got, ok, err := store.GetPoolVrfKeyHashAtSlot(poolKey[:], rbPoint.Slot, nil)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, bodyVrf, got,
		"GetPoolVrfKeyHashAtSlot must resolve the registration applied last")

	regs, err := store.GetPoolRegistrationsEffectiveForEpoch(
		[]lcommon.PoolKeyHash{poolKey}, 1_000, 0, math.MaxInt64, nil,
	)
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Equal(t, bodyVrf, regs[0].VrfKeyHash,
		"GetPoolRegistrationsEffectiveForEpoch must resolve the registration applied last")
}
