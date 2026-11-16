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
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// accountReadSource returns the source for comparing a read of the metadata
// store's account table, or false when reads do not compare or the set
// cannot answer.
func (ls *LedgerState) accountReadSource(
	txn ledgerdb.Txn,
) (accountSource, bool) {
	if _, mode := ls.utxoSetReader(); mode == UtxoSetReadsOff {
		return accountSource{}, false
	}
	ar := ls.accountReader(txn)
	if ar == nil || ls.utxoSetFailed.Load() {
		return accountSource{}, false
	}
	return accountSource{ls: ls, ar: ar, txn: txn}, true
}

// accountListSource returns the source answering a list read from the set,
// or false when the read stays on the metadata store: reads are not in set
// mode, or the set does not hold every account.
func (ls *LedgerState) accountListSource(
	txn ledgerdb.Txn,
) (accountSource, bool) {
	ar := ls.accountServing(txn)
	if ar == nil {
		return accountSource{}, false
	}
	return accountSource{ls: ls, ar: ar, txn: txn}, true
}

// activeRefs returns the credential of every active account.
func (src accountSource) activeRefs() ([]models.StakeCredentialRef, error) {
	refs, err := src.ar.ActiveAccountRefs()
	if err != nil {
		return nil, err
	}
	refs, err = src.stagedAccountRefs(refs, refSet(refs))
	if err != nil {
		return nil, err
	}
	out := make([]models.StakeCredentialRef, 0, len(refs))
	for _, ref := range refs {
		a, err := src.ls.accountView(src.ar, src.txn, ref.Tag, ref.Key)
		if err != nil {
			return nil, err
		}
		if a != nil && a.Active {
			out = append(out, ref)
		}
	}
	return out, nil
}

// poolDelegators returns the credentials of the active accounts delegating
// to each of pools, keyed by pool. A pool with no delegator is absent.
func (src accountSource) poolDelegators(
	pools []lcommon.PoolKeyHash,
) (map[string][]models.StakeCredentialRef, error) {
	out := make(map[string][]models.StakeCredentialRef, len(pools))
	for _, pool := range pools {
		if _, done := out[string(pool[:])]; done {
			continue
		}
		err := src.eachPoolDelegator(pool[:], func(a *models.Account) error {
			out[string(pool[:])] = append(
				out[string(pool[:])],
				models.NewStakeCredentialRef(a.CredentialTag, a.StakingKey),
			)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// drepDelegators returns the credentials of the active accounts delegating
// to the DRep of typ and drep.
func (src accountSource) drepDelegators(
	typ uint64,
	drep []byte,
) ([]models.StakeCredentialRef, error) {
	refs, err := src.ls.accountsByDrepCandidates(src.ar, src.txn, typ, drep)
	if err != nil {
		return nil, err
	}
	var out []models.StakeCredentialRef
	for _, ref := range refs {
		a, err := src.ls.accountView(src.ar, src.txn, ref.Tag, ref.Key)
		if err != nil {
			return nil, err
		}
		if a != nil && a.Active && a.DrepType == typ &&
			bytes.Equal(a.Drep, drep) {
			out = append(out, ref)
		}
	}
	return out, nil
}

// compareRefs orders credentials by tag, then key.
func compareRefs(a, b models.StakeCredentialRef) int {
	if c := cmp.Compare(a.Tag, b.Tag); c != 0 {
		return c
	}
	return bytes.Compare(a.Key, b.Key)
}

// compareRefsScriptFirst is the ledger's credential order, which places a
// script hash (tag 1) before a key hash (tag 0).
func compareRefsScriptFirst(a, b models.StakeCredentialRef) int {
	if c := cmp.Compare(b.Tag, a.Tag); c != 0 {
		return c
	}
	return bytes.Compare(a.Key, b.Key)
}

func sameRefs(a, b []models.StakeCredentialRef) bool {
	return slices.EqualFunc(a, b, func(x, y models.StakeCredentialRef) bool {
		return x.Tag == y.Tag && bytes.Equal(x.Key, y.Key)
	})
}

func (s stagingLedgerDB) GetActiveAccountCredentials(
	txn ledgerdb.Txn,
) ([]models.StakeCredentialRef, error) {
	if src, ok := s.ls.accountListSource(txn); ok {
		got, err := src.activeRefs()
		if err != nil {
			return nil, err
		}
		slices.SortFunc(got, compareRefs)
		return got, nil
	}
	if s.accountsDrained() {
		return nil, drainedAccountsRefusal()
	}
	g := s.ls.verifyGuard(txn)
	want, err := s.LedgerDB.GetActiveAccountCredentials(txn)
	if err != nil {
		return want, err
	}
	src, ok := s.ls.accountReadSource(txn)
	if !ok {
		return want, nil
	}
	got, setErr := src.activeRefs()
	// The store gives no order.
	slices.SortFunc(got, compareRefs)
	s.ls.compareAccountRead(
		g,
		"active account credentials",
		setErr == nil &&
			sameRefs(slices.SortedFunc(slices.Values(want), compareRefs), got),
		setErr,
	)
	return want, nil
}

func (s stagingLedgerDB) GetActiveDelegatorCountsByPool(
	poolKeyHashes []lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	if src, ok := s.ls.accountListSource(txn); ok {
		delegators, err := src.poolDelegators(poolKeyHashes)
		if err != nil {
			return nil, err
		}
		out := make(map[string]uint64, len(delegators))
		for pool, refs := range delegators {
			out[pool] = uint64(len(refs))
		}
		return out, nil
	}
	if s.accountsDrained() {
		return nil, drainedAccountsRefusal()
	}
	g := s.ls.verifyGuard(txn)
	want, err := s.LedgerDB.GetActiveDelegatorCountsByPool(poolKeyHashes, txn)
	if err != nil {
		return want, err
	}
	src, ok := s.ls.accountReadSource(txn)
	if !ok {
		return want, nil
	}
	got, setErr := src.poolDelegators(poolKeyHashes)
	same := setErr == nil && len(got) == len(want)
	for pool, refs := range got {
		same = same && want[pool] == uint64(len(refs))
	}
	s.ls.compareAccountRead(g, "active delegator counts by pool", same, setErr)
	return want, nil
}

// GetActiveDelegatorCredentialsByPool is compared in the order the store
// gives, which is the ledger's credential order.
func (s stagingLedgerDB) GetActiveDelegatorCredentialsByPool(
	poolKeyHashes []lcommon.PoolKeyHash,
	txn ledgerdb.Txn,
) (map[string][]models.StakeCredentialRef, error) {
	if src, ok := s.ls.accountListSource(txn); ok {
		out, err := src.poolDelegators(poolKeyHashes)
		if err != nil {
			return nil, err
		}
		for _, refs := range out {
			slices.SortFunc(refs, compareRefsScriptFirst)
		}
		return out, nil
	}
	if s.accountsDrained() {
		return nil, drainedAccountsRefusal()
	}
	g := s.ls.verifyGuard(txn)
	want, err := s.LedgerDB.GetActiveDelegatorCredentialsByPool(
		poolKeyHashes,
		txn,
	)
	if err != nil {
		return want, err
	}
	src, ok := s.ls.accountReadSource(txn)
	if !ok {
		return want, nil
	}
	got, setErr := src.poolDelegators(poolKeyHashes)
	same := setErr == nil && len(got) == len(want)
	for pool, refs := range got {
		slices.SortFunc(refs, compareRefsScriptFirst)
		same = same && sameRefs(want[pool], refs)
	}
	s.ls.compareAccountRead(
		g,
		"active delegator credentials by pool",
		same,
		setErr,
	)
	return want, nil
}

// GetDRepDelegators is not answered from the set, nor compared, for an empty
// credential: the store's DRep column holds no value for a predefined DRep,
// which an empty credential cannot be matched against in the set.
func (s stagingLedgerDB) GetDRepDelegators(
	ctx context.Context,
	credentialTag uint8,
	drepCredential []byte,
	txn ledgerdb.Txn,
) ([]models.StakeCredentialRef, error) {
	if len(drepCredential) > 0 {
		if src, ok := s.ls.accountListSource(txn); ok {
			got, err := src.drepDelegators(
				uint64(credentialTag),
				drepCredential,
			)
			if err != nil {
				return nil, err
			}
			slices.SortFunc(got, compareRefs)
			return got, nil
		}
	}
	if s.accountsDrained() {
		return nil, drainedAccountsRefusal()
	}
	g := s.ls.verifyGuard(txn)
	want, err := s.LedgerDB.GetDRepDelegators(
		ctx,
		credentialTag,
		drepCredential,
		txn,
	)
	if err != nil || len(drepCredential) == 0 {
		return want, err
	}
	src, ok := s.ls.accountReadSource(txn)
	if !ok {
		return want, nil
	}
	got, setErr := src.drepDelegators(uint64(credentialTag), drepCredential)
	// The store orders by credential tag, then staking key.
	slices.SortFunc(got, compareRefs)
	s.ls.compareAccountRead(
		g,
		fmt.Sprintf("delegators of DRep %d/%x", credentialTag, drepCredential),
		setErr == nil && sameRefs(want, got),
		setErr,
	)
	return want, nil
}
