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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/livesettest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// faultedSet is a LedgerDB set whose named reads fail.
type faultedSet struct {
	*utxoset.Set
	faults *ledger.SetFaultsForTest
}

func (s faultedSet) Inner() *utxoset.Set { return s.Set }

func (s faultedSet) Account(tag uint8, key []byte) (*models.Account, error) {
	if err := s.faults.Account; err != nil {
		return nil, err
	}
	return s.Set.Account(tag, key)
}

func (s faultedSet) PoolState(pool []byte) (*ledger.PoolState, error) {
	if err := s.faults.Pool; err != nil {
		return nil, err
	}
	return s.Set.PoolState(pool)
}

func (s faultedSet) PoolHistoryPools() ([][]byte, error) {
	if err := s.faults.Pool; err != nil {
		return nil, err
	}
	pools, err := s.Set.PoolHistoryPools()
	if err != nil {
		return nil, err
	}
	for pool := range s.faults.ExtraPoolHistory {
		pools = append(pools, []byte(pool))
	}
	return pools, nil
}

func (s faultedSet) PoolHistory(pool []byte) ([]ledger.PoolEvent, error) {
	if err := s.faults.Pool; err != nil {
		return nil, err
	}
	if events, ok := s.faults.ExtraPoolHistory[string(pool)]; ok {
		return events, nil
	}
	return s.Set.PoolHistory(pool)
}

func (s faultedSet) ProposalAt(
	txHash []byte,
	actionIndex uint32,
	at *uint64,
) (*models.GovernanceProposal, error) {
	if err := s.faults.Proposal; err != nil {
		return nil, err
	}
	return s.Set.ProposalAt(txHash, actionIndex, at)
}

func (s faultedSet) ProposalsAt(
	at *uint64,
) ([]models.GovernanceProposal, error) {
	if err := s.faults.Proposal; err != nil {
		return nil, err
	}
	return s.Set.ProposalsAt(at)
}

func (s faultedSet) CommitteeStateAt(
	at *uint64,
) (*ledger.CommitteeState, error) {
	if err := s.faults.Committee; err != nil {
		return nil, err
	}
	st, err := s.Set.CommitteeStateAt(at)
	if err != nil || s.faults.TamperCommittee == nil {
		return st, err
	}
	if st == nil {
		st = &ledger.CommitteeState{}
	}
	s.faults.TamperCommittee(st)
	return st, nil
}

func (s faultedSet) NetworkStateAt(
	slot uint64,
) (*models.NetworkState, error) {
	if err := s.faults.NetworkState; err != nil {
		return nil, err
	}
	return s.Set.NetworkStateAt(slot)
}

func (s faultedSet) PParamsForEra(era uint) ([]models.PParams, error) {
	if s.faults.PParamsReads != nil {
		s.faults.PParamsReads.Add(1)
	}
	if err := s.faults.PParams; err != nil &&
		(s.faults.PParamsEra == nil || *s.faults.PParamsEra == era) {
		return nil, err
	}
	return s.Set.PParamsForEra(era)
}

func (s faultedSet) Epochs() ([]models.Epoch, error) {
	if err := s.faults.Epochs; err != nil {
		return nil, err
	}
	epochs, err := s.Set.Epochs()
	if err != nil || s.faults.TamperEpochs == nil {
		return epochs, err
	}
	return s.faults.TamperEpochs(epochs), nil
}

func (s faultedSet) SyncMarker(name string) (string, bool, error) {
	if err := s.faults.SyncMarker[name]; err != nil {
		return "", false, err
	}
	return s.Set.SyncMarker(name)
}

func (s faultedSet) LatestBlockNonce() (models.BlockNonce, bool, error) {
	if err := s.faults.LatestBlockNonce; err != nil {
		return models.BlockNonce{}, false, err
	}
	return s.Set.LatestBlockNonce()
}

func (s faultedSet) RollbackUtxoSet(
	point ocommon.Point,
) (ledger.UtxoEvent, error) {
	evt, err := s.Set.RollbackUtxoSet(point)
	if err == nil && s.faults.AfterRollback != nil {
		s.faults.AfterRollback()
	}
	return evt, err
}

func init() {
	ledger.OpenImportSetForTest = func(t testing.TB) ledger.UtxoSetWriter {
		t.Helper()
		return livesettest.Open(t)
	}
	ledger.FaultLiveSetForTest = func(
		t testing.TB, set ledger.UtxoSetWriter, faults *ledger.SetFaultsForTest,
	) ledger.UtxoSetWriter {
		t.Helper()
		inner, ok := set.(*utxoset.Set)
		if !ok {
			t.Fatalf("cannot fault a %T", set)
		}
		return faultedSet{Set: inner, faults: faults}
	}
}
