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

package indexer

import (
	"context"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// movingAnchorState is an AnchorStateSource whose ledger keeps committing
// credits while it is read. Read through the source itself, a hook commits one
// between the journal and the accounts; read through a view, the point is
// fixed when the view is taken.
type movingAnchorState struct {
	metadata.AnchorStateSource
	mu      sync.Mutex
	key     []byte
	balance uint64
	journal []metadata.RewardJournalEntry
	// afterJournal runs once the live journal has been read.
	afterJournal func(*movingAnchorState)
}

func (m *movingAnchorState) credit(amount uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.balance += amount
	m.journal = append(m.journal, metadata.RewardJournalEntry{
		StakingKey: m.key, Amount: amount,
	})
}

func (m *movingAnchorState) each(
	fn func(metadata.RewardJournalEntry) error,
) error {
	m.mu.Lock()
	entries := append([]metadata.RewardJournalEntry(nil), m.journal...)
	m.mu.Unlock()
	for _, e := range entries {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

func (m *movingAnchorState) accounts() []metadata.ImportedAccount {
	m.mu.Lock()
	defer m.mu.Unlock()
	return []metadata.ImportedAccount{{
		StakingKey: m.key, AddedSlot: 100, Active: true,
		Reward: types.Uint64(m.balance),
	}}
}

func (m *movingAnchorState) EachAnchorRewardJournal(
	_ context.Context, _ uint64, fn func(metadata.RewardJournalEntry) error,
) error {
	err := m.each(fn)
	if m.afterJournal != nil {
		m.afterJournal(m)
	}
	return err
}

func (m *movingAnchorState) AnchorImportedAccounts(
	context.Context, uint64, []byte, int,
) ([]metadata.ImportedAccount, []byte, error) {
	return m.accounts(), nil, nil
}

func (m *movingAnchorState) AnchorStateView(
	context.Context,
) (metadata.AnchorStateSource, func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &movingAnchorState{
		key: m.key, balance: m.balance,
		journal: append([]metadata.RewardJournalEntry(nil), m.journal...),
	}, func() {}, nil
}

// The balance an account had at the anchor is its balance less the journal
// after it, read at one point of the ledger. A credit that lands between the
// journal read and the account read is in the balance and not the journal, so
// it must not reach the copied balance.
func TestCopyAnchorStateReadsJournalAndBalancesAtOnePoint(t *testing.T) {
	t.Parallel()
	src, _ := newFactsDB(t)
	dst, dstRaw := newFactsDB(t)
	f, err := NewMetadataFacts(src, dst)
	require.NoError(t, err)

	key := []byte("stake-credential-hash-0123456789")
	_, err = dstRaw.Exec(`INSERT INTO account
		(staking_key, credential_tag, added_slot, created_slot, reward, active)
		VALUES (?, 0, 10, 10, '0', 1)`, key)
	require.NoError(t, err)

	// 100 at the anchor, then a credit of 30 the journal records.
	moving := &movingAnchorState{key: key, balance: 100}
	moving.credit(30)
	moving.afterJournal = func(m *movingAnchorState) {
		m.afterJournal = nil
		m.credit(5)
	}
	f.anchorState = moving

	txn := dst.Transaction(context.Background(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return f.CopyAnchorState(context.Background(), txn, 90)
	}))
	require.Equal(t, "100", scalar[string](t, dstRaw,
		`SELECT reward FROM account WHERE staking_key = ?`, key),
		"the balance at the anchor, not off by the credit that landed mid-copy")
}
