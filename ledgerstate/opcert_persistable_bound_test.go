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

package ledgerstate

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/stretchr/testify/require"
)

// TestPersistableOpCertBoundMatchesStore ties eras.MaxPersistableOpCertCounter
// to the check that enforces it. The constant exists to name the limit
// sqlstore.checkedInt64 imposes on pool_opcert_sequence.sequence and
// pool.latest_op_cert_sequence before a caller reaches it, so an assertion
// comparing the constant against its own definition proves nothing: it stays
// green if either side moves. This one drives the value through the real
// store write in both directions, so a change to the constant or to the
// store's accepted range fails it.
//
// It is not next to checkedInt64 because checkedInt64 is unexported and the
// reviewed import direction in internal/architecture/import_boundary_test.go
// forbids anything under database/ from importing ledger/, test files
// included. ledgerstate owns the write path the bound was found missing from
// (importOpCertCounters) and already imports ledger/eras, so it is the
// package that can see both sides.
func TestPersistableOpCertBoundMatchesStore(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	poolKeyHash := testPoolKeyHash(bytes.Repeat([]byte{0x7a}, 28))

	// The store records the highest counter the bound admits.
	require.NoError(t, db.Metadata().UpdatePoolOpCertSequence(
		poolKeyHash, eras.MaxPersistableOpCertCounter, 100, nil,
	))
	sequence, found, err := db.LatestPoolOpCertSequence(poolKeyHash, nil)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, eras.MaxPersistableOpCertCounter, sequence)

	// The store refuses the next one, which is what makes the bound the
	// bound rather than an arbitrary constant.
	require.Error(t, db.Metadata().UpdatePoolOpCertSequence(
		poolKeyHash, eras.MaxPersistableOpCertCounter+1, 101, nil,
	))
}
