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
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// PParamsReader serves protocol parameter rows from a live UTxO set that
// keeps them.
type PParamsReader interface {
	// PParamsLoaded reports whether the reader holds every protocol
	// parameter row; until it does, reads stay on the metadata store.
	PParamsLoaded() (bool, error)
	// PParamsForEra returns era's rows in the order they were written.
	PParamsForEra(era uint) ([]models.PParams, error)
}

// pparamsReader returns the set's protocol parameter reader and the read
// mode, or UtxoSetReadsOff when these reads stay on the metadata store.
func (ls *LedgerState) pparamsReader(txn ledgerdb.Txn) (PParamsReader, string) {
	_, mode := ls.utxoSetReader()
	if mode == UtxoSetReadsOff {
		return nil, UtxoSetReadsOff
	}
	r, ok := ls.readSet(txn).(PParamsReader)
	if !ok {
		return nil, UtxoSetReadsOff
	}
	if !ls.pparamsSetLoaded.Load() {
		loaded, err := r.PParamsLoaded()
		if err != nil || !loaded {
			return nil, UtxoSetReadsOff
		}
		ls.pparamsSetLoaded.Store(true)
	}
	return r, mode
}

// pparamsView is era's rows as txn sees the set, in write order.
func (ls *LedgerState) pparamsView(
	r PParamsReader,
	txn ledgerdb.Txn,
	era uint,
) ([]models.PParams, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	ops := ls.stagedEpochStateOps(txn)
	view, err := r.PParamsForEra(era)
	if err != nil {
		return nil, err
	}
	for _, o := range ops {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			view = slices.DeleteFunc(view, func(p models.PParams) bool {
				return p.AddedSlot > to
			})
		}
		for _, p := range o.pparams {
			if p.EraId == era {
				view = append(view, p)
			}
		}
	}
	return view, nil
}

// latestPParams picks from rows, in write order, as the metadata store's
// GetPParams does: the latest epoch at or before epoch, and of its rows the
// last written.
func latestPParams(rows []models.PParams, epoch uint64) *models.PParams {
	var best *models.PParams
	for i := range rows {
		p := &rows[i]
		if p.Epoch <= epoch && (best == nil || p.Epoch >= best.Epoch) {
			best = p
		}
	}
	return best
}

func samePParamsRows(a, b []models.PParams) bool {
	return slices.EqualFunc(a, b, func(x, y models.PParams) bool {
		return x.AddedSlot == y.AddedSlot && x.Epoch == y.Epoch &&
			x.EraId == y.EraId && bytes.Equal(x.Cbor, y.Cbor)
	})
}

func (ls *LedgerState) comparePParamsRead(
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
	ls.metrics.pparamsReadsVerified.Inc()
	if same && setErr == nil {
		return
	}
	n := ls.pparamsReadMismatches.Add(1)
	ls.metrics.pparamsReadMismatches.Inc()
	if n > 20 && n%1000 != 0 {
		return
	}
	ls.config.Logger.Warn(
		"live UTxO set protocol parameter read differs from the metadata store",
		"component", "ledger",
		"read", what,
		"set_error", setErr,
		"mismatches", n,
	)
}

func (s stagingLedgerDB) GetPParams(
	epoch uint64,
	eraId uint,
	decodeFunc func([]byte) (lcommon.ProtocolParameters, error),
	txn ledgerdb.Txn,
) (lcommon.ProtocolParameters, error) {
	r, mode := s.ls.pparamsReader(txn)
	if mode == UtxoSetReadsOff || r == nil {
		if s.pparamsDrained() {
			return nil, drainedUnserved(nodesettings.FamilyPParamsUpdates)
		}
		return s.LedgerDB.GetPParams(epoch, eraId, decodeFunc, txn)
	}
	g := s.ls.beginVerifyRead(txn)
	view, setErr := s.ls.pparamsView(r, txn, eraId)
	var fromSet []byte
	if setErr == nil {
		if best := latestPParams(view, epoch); best != nil {
			fromSet = best.Cbor
		}
	}
	if mode == UtxoSetReadsSet {
		if setErr != nil || fromSet == nil {
			return nil, setErr
		}
		return decodeFunc(fromSet)
	}
	// The metadata store decodes the row it picks; capture its bytes to
	// compare with the set's pick.
	var stored []byte
	want, err := s.LedgerDB.GetPParams(epoch, eraId,
		func(b []byte) (lcommon.ProtocolParameters, error) {
			stored = b
			return decodeFunc(b)
		},
		txn,
	)
	if err != nil {
		return nil, err
	}
	s.ls.comparePParamsRead(
		g, fmt.Sprintf("pparams of era %d at epoch %d", eraId, epoch),
		(stored == nil) == (fromSet == nil) && bytes.Equal(stored, fromSet),
		setErr,
	)
	return want, nil
}

func (s stagingLedgerDB) ListPParamsForEra(
	eraID uint,
	txn ledgerdb.Txn,
) ([]models.PParams, error) {
	r, mode := s.ls.pparamsReader(txn)
	if mode == UtxoSetReadsOff || r == nil {
		if s.pparamsDrained() {
			return nil, drainedUnserved(nodesettings.FamilyPParamsUpdates)
		}
		return s.LedgerDB.ListPParamsForEra(eraID, txn)
	}
	g := s.ls.beginVerifyRead(txn)
	fromSet, setErr := s.ls.pparamsView(r, txn, eraID)
	if mode == UtxoSetReadsSet {
		return fromSet, setErr
	}
	want, err := s.LedgerDB.ListPParamsForEra(eraID, txn)
	if err != nil {
		return nil, err
	}
	s.ls.comparePParamsRead(
		g, fmt.Sprintf("pparams of era %d", eraID),
		samePParamsRows(want, fromSet), setErr,
	)
	return want, nil
}
