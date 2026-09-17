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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestLedgerProcessBlockRejectsOpCertCounterAbovePersistableBound is the
// apply-path half of #3991's third acceptance criterion: a counter above
// 2^63-1 must fail through ledgerProcessBlock, and the test must fail
// without eras.ValidateOpCertPersistableCounter's guard at state.go. That
// guard was unreachable at the gouroboros pin #3991 fixed against, because
// every opCertFromHeader path decoded the counter as uint32; the module is
// now at a release past gouroboros #2256, so a counter this wide is
// representable and the guard is finally exercisable.
//
// The guard runs unconditionally, ahead of the era-scoped monotonicity/no-gap
// rule, so shouldValidate is false here to isolate it: this proves the width
// bound alone rejects the block, not the separate stateful check.
func TestLedgerProcessBlockRejectsOpCertCounterAbovePersistableBound(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{db: db}

	var issuerVkey lcommon.IssuerVkey
	for i := range issuerVkey {
		issuerVkey[i] = byte(i + 1)
	}
	pkh := lcommon.PoolKeyHash(issuerVkey.Hash())
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  make([]byte, 32),
		},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  make([]byte, 32),
			AddedSlot:   1,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))

	block := &babbage.BabbageBlock{
		BlockHeader: &babbage.BabbageBlockHeader{
			Body: babbage.BabbageBlockHeaderBody{
				Slot:       10,
				IssuerVkey: issuerVkey,
				OpCert: babbage.BabbageOpCert{
					SequenceNumber: eras.MaxPersistableOpCertCounter + 1,
				},
			},
		},
	}

	err := db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(
			txn,
			ocommon.Point{Slot: 10},
			block,
			false,
			false,
			false,
			nil,
			envelopeParent{},
			nil,
			eras.BabbageEraDesc,
			nil,
			nil,
			0,
			false,
		)
		return err
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "pool_opcert_sequence")

	var poolID [28]byte
	copy(poolID[:], pkh.Bytes())
	_, found, err := ls.LatestOpCertSequence(poolID)
	require.NoError(t, err)
	require.False(
		t,
		found,
		"a counter above the persistable bound must not be recorded",
	)
}
