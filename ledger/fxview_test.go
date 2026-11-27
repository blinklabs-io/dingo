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
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledgerstate"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// SetFaultsForTest are the errors a fixture's LedgerDB set returns in place
// of the read each field names. A nil field leaves that read alone.
type SetFaultsForTest struct {
	// Account fails a current account read.
	Account error
	// Pool fails a read of a pool's current state or certificate history.
	Pool error
	// ExtraPoolHistory is certificate history the set reports besides its
	// own, by pool key, as a store holding a damaged row does.
	ExtraPoolHistory map[string][]PoolEvent
	// Proposal fails a governance proposal read.
	Proposal error
	// Committee fails a committee and constitution state read.
	Committee error
	// TamperCommittee changes the committee and constitution state a read
	// returns, as a store holding a damaged row does.
	TamperCommittee func(*CommitteeState)
	// NetworkState fails a treasury and reserves read.
	NetworkState error
	// PParams fails a protocol parameter history read.
	PParams error
	// PParamsReads, when set, counts the protocol parameter history reads.
	PParamsReads *atomic.Int32
	// PParamsEra, when set, limits PParams to the history of that era.
	PParamsEra *uint
	// Epochs fails an epoch record read.
	Epochs error
	// TamperEpochs changes the epoch records a read returns, as a store
	// holding a damaged row does.
	TamperEpochs func([]models.Epoch) []models.Epoch
	// SyncMarker fails the read of each marker it names.
	SyncMarker map[string]error
	// LatestBlockNonce fails a read of the newest block nonce.
	LatestBlockNonce error
	// AfterRollback, when set, runs each time the set has rolled back, so a
	// fixture can fail only the reads that follow a committed rollback.
	AfterRollback func()
}

// FaultLiveSetForTest is installed by the external tests. It returns set
// with the reads faults names failing, every other read and every write
// reaching set.
var FaultLiveSetForTest func(
	tb testing.TB, set UtxoSetWriter, faults *SetFaultsForTest,
) UtxoSetWriter

// fxFaultedLedger makes ls read its LedgerDB set through faults, as a
// ledger whose store fails mid-read does, opening the set as
// fxAttachLedgerDB does when ls has none yet. The returned faults may be
// changed while the fixture runs.
func fxFaultedLedger(
	t testing.TB,
	ls *LedgerState,
	faults SetFaultsForTest,
) *SetFaultsForTest {
	t.Helper()
	if ls.config.UtxoSet == nil {
		fxAttachLedgerDB(t, ls)
	}
	live := &faults
	ls.config.UtxoSet = FaultLiveSetForTest(t, ls.config.UtxoSet, live)
	return live
}

// OpenImportSetForTest is installed by the external tests. It opens an empty
// LedgerDB set that belongs to no database yet, the set a Mithril import
// loads before the node starts on it.
var OpenImportSetForTest func(tb testing.TB) UtxoSetWriter

// fxImportThenOpen imports cfg into db the way a Mithril bootstrap does and
// then opens db's LedgerDB set as the node's next start does, so the set
// takes over everything the import wrote. db's set must not be open yet. A
// snapshot built without a UTxO map or certificate state gets empty ones.
func fxImportThenOpen(
	t testing.TB,
	db *database.Database,
	cfg ledgerstate.ImportConfig,
) {
	t.Helper()
	_, open := LiveSetOfForTest(db)
	require.False(t, open, "the database's set is already open")
	set := OpenImportSetForTest(t)
	loader, ok := set.(ledgerstate.UtxoSetLoader)
	require.True(t, ok, "the import set must load a UTxO set")
	fxCompleteSnapshot(t, cfg.State)
	cfg.Database = db
	cfg.UtxoSet = loader
	cfg.LedgerDB = NewImportLedgerDB(db, set, cfg.Logger)
	require.NoError(t, ledgerstate.ImportLedgerState(t.Context(), cfg))
	AdoptLiveSetForTest(t, db, set)
}

// fxApplyTx commits tx's withdrawals and certificates to db's open LedgerDB
// set as the block at point applies them, tx being the block's transaction
// at index. The set journals them under point, so a rollback below it undoes
// them. certDeposits are the deposits the certificates paid, by index.
func fxApplyTx(
	t testing.TB,
	db *database.Database,
	tx lcommon.Transaction,
	point ocommon.Point,
	index uint32,
	certDeposits map[int]uint64,
) {
	t.Helper()
	ls := seedLedgerOf(t, db)
	fxBlock(t, ls, point, func(_ ledgerdb.LedgerDB, txn ledgerdb.Txn) error {
		return ls.stageAccountTransaction(
			txn, tx, point.Slot, index, point.Hash,
			database.BatchedTxIngestOpts{}, certDeposits,
		)
	})
}

// fxImportAccount puts account in db's open LedgerDB set with the import
// baseline a ledger-state import records for it: the registration, deposit
// and delegations the snapshot held, dated at account.AddedSlot.
func fxImportAccount(
	t testing.TB,
	db *database.Database,
	account *models.Account,
) {
	t.Helper()
	ref := models.NewStakeCredentialRef(
		account.CredentialTag, account.StakingKey,
	)
	baseline := ImportBaseline{
		Ref:       ref,
		Active:    account.Active,
		AddedSlot: account.AddedSlot,
	}
	if account.ImportDeposit != nil {
		deposit := uint64(*account.ImportDeposit)
		baseline.Deposit = &deposit
	}
	fxSeed(t, db, LedgerChanges{
		Accounts: []AccountChange{{
			Slot: account.AddedSlot, Account: *account,
		}},
		ImportBaselines: []ImportBaselineChange{{
			Slot: account.AddedSlot, Baseline: baseline,
		}},
	})
}

// fxGenesisDelegation records a genesis key delegation in db's open LedgerDB
// set, as its certificate applied in a block does.
func fxGenesisDelegation(
	t testing.TB,
	db *database.Database,
	row models.GenesisDelegation,
) {
	t.Helper()
	fxSeed(t, db, LedgerChanges{
		GenesisDelegations: []models.GenesisDelegation{row},
	})
}

// fxStageTx stages tx's withdrawals and certificates in db's open LedgerDB
// set as the block at point applies them, without committing a block: the
// set dates each change at point's slot and keeps it under no journal
// record, so fixtures may stage slots in any order. protocolMajor is the
// protocol version the certificates apply under.
func fxStageTx(
	t testing.TB,
	db *database.Database,
	tx lcommon.Transaction,
	point ocommon.Point,
	certDeposits map[int]uint64,
	protocolMajor uint64,
) {
	t.Helper()
	ls := seedLedgerOf(t, db)
	require.NoError(t, ls.stageAccountTransaction(
		nil, tx, point.Slot, 0, point.Hash,
		database.BatchedTxIngestOpts{ProtocolMajor: protocolMajor},
		certDeposits,
	))
	require.False(t, ls.utxoSetFailed.Load(), "the set did not take the transaction")
}
