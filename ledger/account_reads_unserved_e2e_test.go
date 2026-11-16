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

package ledger_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// With accounts drained and no set serving them, the account lookups and
// lists fail as unserved rather than fall through to the emptied table, the
// empty-credential DRep delegator read included.
func TestAccountReadsFailWhenTheDrainedFamilyHasNoServingSet(t *testing.T) {
	t.Parallel()
	h := newLiveStakeHarnessWithReads(t, false, ledger.UtxoSetReadsOff)
	h.scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyAccounts),
	)
	ldb := h.scenario.LedgerDB()
	ctx := t.Context()
	pools := []lcommon.PoolKeyHash{lcommon.PoolKeyHash(h.pool)}
	ref := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x72}, 28))
	for name, err := range map[string]error{
		"account": func() error {
			_, err := ldb.GetAccountByCredential(ctx, 0, ref.Key, true, nil)
			return err
		}(),
		"accounts": func() error {
			_, err := ldb.GetAccountsByCredential(
				ctx, []models.StakeCredentialRef{ref}, true, nil,
			)
			return err
		}(),
		"active": func() error {
			_, err := ldb.GetActiveAccountCredentials(nil)
			return err
		}(),
		"counts": func() error {
			_, err := ldb.GetActiveDelegatorCountsByPool(pools, nil)
			return err
		}(),
		"credentials": func() error {
			_, err := ldb.GetActiveDelegatorCredentialsByPool(pools, nil)
			return err
		}(),
		"drep delegators": func() error {
			_, err := ldb.GetDRepDelegators(ctx, 0, ref.Key, nil)
			return err
		}(),
		"predefined drep delegators": func() error {
			_, err := ldb.GetDRepDelegators(ctx, 2, nil, nil)
			return err
		}(),
	} {
		require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved, name)
	}
}
