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

package lifecycle_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// seedCip163Certificate records a certificate through the public database
// ingestion boundary, mirroring ledger's own account_expiry_rollback_test.go
// seedRollbackCertificate helper (a different package, so not directly
// reusable) -- this keeps the test independent of a concrete metadata
// plugin and its SQL schema.
func seedCip163Certificate(
	t *testing.T,
	db *database.Database,
	slot uint64,
	cert lcommon.Certificate,
) {
	t.Helper()
	txID := make([]byte, 32)
	binary.BigEndian.PutUint64(txID[len(txID)-8:], slot)
	tx := mockledger.NewTransactionBuilder()
	tx.WithId(txID)
	tx.WithCertificates(cert)
	require.NoError(t, db.SetTransactionMetadataOnly(
		tx,
		ocommon.NewPoint(slot, txID),
		0,
		map[int]uint64{0: 0},
		nil,
	))
}

// TestTruncateRecomputesCip163ExpirationForWitnessAfterTruncatePoint
// guards the actual bug behind this fix: database/lifecycle.Truncate
// (the offline and live CIP-0135 disaster-recovery truncate path) used to
// call database.TruncateAfterSlot directly, bypassing the CIP-0163
// pre/post hooks ledger.LedgerState.rollback applies for a normal
// (security-parameter-bounded) rollback -- so a delegation witness in a
// truncated-away block could leave expiration_epoch renewed past what the
// surviving chain actually witnessed, producing incorrect stake/reward/
// DRep calculations after any offline or live truncate on a CIP-0163-
// enabled network.
func TestTruncateRecomputesCip163ExpirationForWitnessAfterTruncatePoint(
	t *testing.T,
) {
	const (
		inactivity = uint64(90)
		// 100 slots per epoch: epoch 0 = [0,100), epoch 1 = [100,200), ...
		epochLength = uint64(100)
	)
	db := newTestDB(t)
	for epoch := range uint64(3) {
		require.NoError(t, db.SetEpoch(
			epoch*epochLength, epoch,
			nil, nil, nil, nil,
			1, 1000, uint(epochLength),
			nil,
		))
	}

	cred := bytes.Repeat([]byte{0x07}, 28)

	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:      cred,
		CredentialTag:   0,
		Active:          true,
		AddedSlot:       50,
		CreatedSlot:     50,
		ExpirationEpoch: 1 + inactivity, // as block application would stamp it
	}))

	// Surviving registration witness at slot 50 (epoch 0).
	survivingBlock := testBlock(1, 0xA1)
	survivingBlock.Slot = 50
	require.NoError(t, db.BlockCreate(survivingBlock, nil))
	seedCip163Certificate(t, db, 50, &lcommon.StakeRegistrationCertificate{
		StakeCredential: lcommon.Credential{
			CredType:   0,
			Credential: lcommon.NewBlake2b224(cred),
		},
	})

	// To-be-truncated-away delegation witness at slot 150 (epoch 1), which
	// renews the expiration to a value the surviving chain never actually
	// witnessed.
	truncatedBlock := testBlock(2, 0xA2)
	truncatedBlock.Slot = 150
	truncatedBlock.PrevHash = survivingBlock.Hash
	require.NoError(t, db.BlockCreate(truncatedBlock, nil))
	seedCip163Certificate(t, db, 150, &lcommon.StakeDelegationCertificate{
		StakeCredential: &lcommon.Credential{
			CredType:   0,
			Credential: lcommon.NewBlake2b224(cred),
		},
		PoolKeyHash: lcommon.NewBlake2b224(bytes.Repeat([]byte{0x0A}, 28)),
	})

	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.Point{
			Slot: truncatedBlock.Slot,
			Hash: truncatedBlock.Hash,
		},
		BlockNumber: truncatedBlock.Number,
	}, nil))

	// Truncate to the surviving block, discarding the slot-150 witness --
	// with delegatorInactivityEnabled=true, matching a CIP-0163-enabled
	// network.
	_, err := lifecycle.Truncate(
		context.Background(), db, survivingBlock, 0, true, inactivity,
	)
	require.NoError(t, err)

	acct, err := db.GetAccountByCredential(0, cred, true, nil)
	require.NoError(t, err)
	require.Equal(
		t, uint64(0)+inactivity, acct.ExpirationEpoch,
		"expiration must be recomputed from the surviving epoch-0 witness "+
			"(0+inactivity), not left at the truncated-away epoch-1 "+
			"witness's stamp (1+inactivity) or the CIP-0163 hooks being "+
			"skipped entirely (leaving it unchanged)",
	)
}
