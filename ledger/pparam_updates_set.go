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
	"fmt"
	"maps"
	"slices"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// stagePParamUpdates stages the proposals the metadata store stores for a
// transaction: only a valid transaction's, each at the transaction's slot
// and target epoch.
func (ls *LedgerState) stagePParamUpdates(
	txn ledgerdb.Txn,
	tx lcommon.Transaction,
	slot uint64,
	index uint32,
	updateEpoch uint64,
	updates map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
) {
	if len(updates) == 0 || !tx.IsValid() {
		return
	}
	changes := make([]PParamUpdateChange, 0, len(updates))
	for genesis, update := range updates {
		changes = append(changes, PParamUpdateChange{
			Index: index,
			Update: models.PParamUpdate{
				GenesisHash: bytes.Clone(genesis.Bytes()),
				Cbor:        slices.Clone(update.Cbor()),
				AddedSlot:   slot,
				Epoch:       updateEpoch,
			},
		})
	}
	ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		PParamUpdates: changes,
	})
}

// PParamUpdateReader serves protocol parameter update proposals from a live
// UTxO set that keeps them.
type PParamUpdateReader interface {
	PParamUpdatesLoaded() (bool, error)
	// PParamUpdates calls fn with every proposal in the order stored.
	PParamUpdates(fn func(models.PParamUpdate) error) error
}

// pparamUpdateVerifyReader returns the set's proposal reader when proposal
// reads compare.
func (ls *LedgerState) pparamUpdateVerifyReader(
	txn ledgerdb.Txn,
) PParamUpdateReader {
	if _, mode := ls.utxoSetReader(); mode == UtxoSetReadsOff {
		return nil
	}
	if ls.accountReader(txn) == nil || ls.utxoSetFailed.Load() {
		return nil
	}
	r, ok := ls.readSet(txn).(PParamUpdateReader)
	if !ok {
		return nil
	}
	return r
}

// pparamUpdateView returns the proposals the store's GetPParamUpdates gives
// for epoch, those targeting it or the epoch before it, as txn sees the set:
// the committed proposals under the rollbacks and writes of the batches txn
// sees. They are in the order stored; the store's answer is in reverse.
func (ls *LedgerState) pparamUpdateView(
	r PParamUpdateReader,
	txn ledgerdb.Txn,
	epoch uint64,
) ([]models.PParamUpdate, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	ops := ls.stagedEpochStateOps(txn)
	var rows []models.PParamUpdate
	err := r.PParamUpdates(func(u models.PParamUpdate) error {
		u.GenesisHash = bytes.Clone(u.GenesisHash)
		u.Cbor = bytes.Clone(u.Cbor)
		rows = append(rows, u)
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, o := range ops {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			rows = slices.DeleteFunc(rows, func(u models.PParamUpdate) bool {
				return u.AddedSlot > to
			})
		}
		for _, c := range o.pparamUpdates {
			rows = append(rows, c.Update)
		}
	}
	previous := epoch
	if epoch > 0 {
		previous--
	}
	return slices.DeleteFunc(rows, func(u models.PParamUpdate) bool {
		return u.Epoch != epoch && u.Epoch != previous
	}), nil
}

// pparamUpdateServing returns the set's proposal reader when proposal reads
// are answered from it: reads are in set mode and the set holds every
// proposal.
func (ls *LedgerState) pparamUpdateServing(
	txn ledgerdb.Txn,
) PParamUpdateReader {
	if ls.accountReader(txn) == nil {
		return nil
	}
	r, ok := ls.readSet(txn).(PParamUpdateReader)
	if !ok || !ls.setServes(&ls.pparamUpdatesSetLoaded, r.PParamUpdatesLoaded) {
		return nil
	}
	return r
}

// GetPParamUpdates is answered from the set in set mode. The set's rows have
// no row IDs, and the store's order between genesis keys is the order of a
// map within one transaction, so the answer is the newest proposal first.
func (s stagingLedgerDB) GetPParamUpdates(
	epoch uint64,
	txn ledgerdb.Txn,
) ([]models.PParamUpdate, error) {
	if r := s.ls.pparamUpdateServing(txn); r != nil {
		rows, err := s.ls.pparamUpdateView(r, txn, epoch)
		if err != nil {
			return nil, err
		}
		slices.Reverse(rows)
		return rows, nil
	}
	if s.pparamsDrained() {
		return nil, drainedUnserved(nodesettings.FamilyPParamsUpdates)
	}
	return s.LedgerDB.GetPParamUpdates(epoch, txn)
}

type pparamProposal struct {
	slot, epoch uint64
	cbor        string
}

// samePParamUpdates compares the store's proposals, newest first, with the
// set's, oldest first: the same proposals for each genesis key in the same
// order. The order between keys is not compared; the store gives it by row
// ID, which within one transaction is the order of a map, and the proposal
// selection reads only each key's latest.
func samePParamUpdates(want, got []models.PParamUpdate) bool {
	group := func(rows []models.PParamUpdate, newestFirst bool) map[string][]pparamProposal {
		out := make(map[string][]pparamProposal)
		for i := range rows {
			u := rows[i]
			if !newestFirst {
				u = rows[len(rows)-1-i]
			}
			out[string(u.GenesisHash)] = append(out[string(u.GenesisHash)],
				pparamProposal{u.AddedSlot, u.Epoch, string(u.Cbor)})
		}
		return out
	}
	return maps.EqualFunc(
		group(want, true), group(got, false),
		func(a, b []pparamProposal) bool { return slices.Equal(a, b) },
	)
}

func (ls *LedgerState) comparePParamUpdateRead(
	g verifyReadGuard,
	what string,
	same bool,
	setErr error,
) {
	if !ls.verifying() {
		return
	}
	if setErr == nil && !same && ls.verifyReadRaced(g) {
		return
	}
	ls.metrics.pparamUpdateReadsVerified.Inc()
	if same && setErr == nil {
		return
	}
	n := ls.pparamUpdateReadMismatches.Add(1)
	ls.metrics.pparamUpdateReadMismatches.Inc()
	if n > 20 && n%1000 != 0 {
		return
	}
	ls.config.Logger.Warn(
		"live UTxO set protocol parameter update read differs from the metadata store",
		"component", "ledger",
		"read", what,
		"set_error", setErr,
		"mismatches", n,
	)
}

// verifyPParamUpdates compares the proposals the store reads to select the
// update enacted for epoch, which it reads from the epoch before, with the
// set's. The selection is a function of those rows and the epoch records,
// so the rows are what the set can check; the store's answer to the caller
// is its own.
func (s stagingLedgerDB) verifyPParamUpdates(
	g verifyReadGuard,
	txn ledgerdb.Txn,
	epoch uint64,
) {
	r := s.ls.pparamUpdateVerifyReader(txn)
	if r == nil || epoch == 0 {
		return
	}
	submission := epoch - 1
	want, err := s.GetPParamUpdates(submission, txn)
	if err != nil {
		return
	}
	got, setErr := s.ls.pparamUpdateView(r, txn, submission)
	s.ls.comparePParamUpdateRead(
		g, fmt.Sprintf("proposals for epoch %d", submission),
		setErr == nil && samePParamUpdates(want, got), setErr,
	)
}

// ForecastPParamUpdates is compared on the proposals it selects from, when
// it reads any.
func (s stagingLedgerDB) ForecastPParamUpdates(
	epoch uint64,
	quorum int,
	currentPParams lcommon.ProtocolParameters,
	decodeFunc func([]byte) (any, error),
	updateFunc func(lcommon.ProtocolParameters, any) (lcommon.ProtocolParameters, error),
	cloneFunc func(lcommon.ProtocolParameters) (lcommon.ProtocolParameters, error),
	txn ledgerdb.Txn,
) (lcommon.ProtocolParameters, error) {
	if r := s.ls.pparamUpdateServing(txn); r != nil {
		if epoch == 0 || currentPParams == nil || decodeFunc == nil ||
			updateFunc == nil || cloneFunc == nil {
			return currentPParams, nil
		}
		{
			rows, err := s.ls.pparamUpdateView(r, txn, epoch-1)
			if err != nil {
				return nil, err
			}
			// The proposals' order within a slot is the order stored.
			for i := range rows {
				rows[i].ID = uint(i) + 1
			}
			return database.ForecastPParamUpdatesFromRows(
				rows, epoch, quorum, currentPParams,
				decodeFunc, updateFunc, cloneFunc,
				func(id uint64) (*models.Epoch, error) {
					return s.GetEpoch(id, txn)
				},
			)
		}
	}
	if s.pparamsDrained() {
		if epoch == 0 || currentPParams == nil || decodeFunc == nil ||
			updateFunc == nil || cloneFunc == nil {
			return currentPParams, nil
		}
		return nil, drainedUnserved(nodesettings.FamilyPParamsUpdates)
	}
	g := s.ls.verifyGuard(txn)
	res, err := s.LedgerDB.ForecastPParamUpdates(
		epoch, quorum, currentPParams, decodeFunc, updateFunc, cloneFunc, txn,
	)
	if err != nil {
		return res, err
	}
	if currentPParams != nil && decodeFunc != nil && updateFunc != nil &&
		cloneFunc != nil {
		s.verifyPParamUpdates(g, txn, epoch)
	}
	return res, nil
}
