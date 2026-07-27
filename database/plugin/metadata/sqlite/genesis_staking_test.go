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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestSetGenesisStakingRestartable(t *testing.T) {
	t.Parallel()

	store := setupFileTestStore(t)
	hostname := "relay.example.com"
	port := uint32(3001)
	operator := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224([]byte("genesis pool")),
	)
	cert := lcommon.PoolRegistrationCertificate{
		CertType:   uint(lcommon.CertificateTypePoolRegistration),
		Operator:   operator,
		VrfKeyHash: lcommon.VrfKeyHash(lcommon.NewBlake2b256([]byte("vrf"))),
		Pledge:     1_000_000,
		Cost:       340_000_000,
		Margin:     cbor.Rat{Rat: big.NewRat(1, 20)},
		RewardAccount: lcommon.AddrKeyHash(
			lcommon.NewBlake2b224([]byte("reward account")),
		),
		PoolOwners: []lcommon.AddrKeyHash{
			lcommon.AddrKeyHash(lcommon.NewBlake2b224([]byte("owner"))),
		},
		Relays: []lcommon.PoolRelay{{
			Type:     lcommon.PoolRelayTypeSingleHostName,
			Port:     &port,
			Hostname: &hostname,
		}},
	}
	pools := map[string]lcommon.PoolRegistrationCertificate{
		operator.String(): cert,
	}
	blockHash := bytes.Repeat([]byte{0xab}, 32)

	require.NoError(t, store.SetGenesisStaking(pools, nil, blockHash, nil))

	// Model an interrupted/partial prior attempt: the parent registration and
	// owner are durable, but its relay is missing and an incomplete duplicate
	// owner was left behind.
	var storedReg models.PoolRegistration
	require.NoError(
		t,
		store.DB().Where("added_slot = ?", 0).First(&storedReg).Error,
	)
	require.NoError(
		t,
		store.DB().Where(
			"pool_registration_id = ?",
			storedReg.ID,
		).Delete(&models.PoolRegistrationRelay{}).Error,
	)
	require.NoError(
		t,
		store.DB().Create(&models.PoolRegistrationOwner{
			KeyHash:            bytes.Repeat([]byte{0xff}, 28),
			PoolRegistrationID: storedReg.ID,
			PoolID:             storedReg.PoolID,
		}).Error,
	)

	require.NoError(
		t,
		store.SetGenesisStaking(pools, nil, blockHash, nil),
		"replaying genesis staking after a restart must be idempotent",
	)

	var registrationCount, ownerCount, relayCount int64
	require.NoError(
		t,
		store.DB().Model(&models.PoolRegistration{}).
			Where("added_slot = ?", 0).
			Count(&registrationCount).Error,
	)
	require.NoError(
		t,
		store.DB().Model(&models.PoolRegistrationOwner{}).
			Count(&ownerCount).Error,
	)
	require.NoError(
		t,
		store.DB().Model(&models.PoolRegistrationRelay{}).
			Count(&relayCount).Error,
	)
	require.Equal(t, int64(1), registrationCount)
	require.Equal(t, int64(1), ownerCount)
	require.Equal(t, int64(1), relayCount)
}
