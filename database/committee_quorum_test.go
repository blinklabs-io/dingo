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

package database_test

import (
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

func TestCommitteeQuorumZeroPersistsAndClearRemainsAbsent(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	require.NoError(t, db.SetCommitteeQuorum(big.NewRat(0, 1), 10, nil))
	quorum, err := db.GetCommitteeQuorum(nil)
	require.NoError(t, err)
	require.NotNil(t, quorum)
	require.Zero(t, quorum.Sign())

	require.NoError(t, db.ClearCommitteeQuorum(20, nil))
	quorum, err = db.GetCommitteeQuorum(nil)
	require.NoError(t, err)
	require.Nil(t, quorum)
}
