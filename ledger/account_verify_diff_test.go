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
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// memAccountSet is a live set that answers account reads with one account.
type memAccountSet struct {
	memNonceSet
	account *models.Account
}

func (*memAccountSet) AccountsLoaded() (bool, error) { return true, nil }

func (s *memAccountSet) Account(uint8, []byte) (*models.Account, error) {
	return s.account, nil
}

func (s *memAccountSet) AccountAt(uint8, []byte, uint64) (*models.Account, error) {
	return s.account, nil
}

func (*memAccountSet) HasRewardRecord(RewardRecord) (bool, error) { return false, nil }

func (*memAccountSet) AccountsByPool([]byte) ([]models.StakeCredentialRef, error) {
	return nil, nil
}

func (*memAccountSet) ActiveAccountRefs() ([]models.StakeCredentialRef, error) {
	return nil, nil
}

func (*memAccountSet) AccountsByDrep(uint64, []byte) ([]models.StakeCredentialRef, error) {
	return nil, nil
}

func (*memAccountSet) AccountsWithDrep() ([]models.StakeCredentialRef, error) {
	return nil, nil
}

// logRecords collects every record a logger receives.
type logRecords struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (l *logRecords) Enabled(context.Context, slog.Level) bool { return true }
func (l *logRecords) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.recs = append(l.recs, r)
	return nil
}
func (l *logRecords) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *logRecords) WithGroup(string) slog.Handler      { return l }

func (l *logRecords) attrs(msg string) map[string]string {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range l.recs {
		if r.Message != msg {
			continue
		}
		out := map[string]string{}
		r.Attrs(func(a slog.Attr) bool {
			out[a.Key] = a.Value.String()
			return true
		})
		return out
	}
	return nil
}

// A mismatching account read names the fields that differ, with the store's
// and the set's value, and what the set's answer was staged from.
func TestAccountMismatchReportNamesTheDifferingFields(t *testing.T) {
	t.Parallel()
	ls, _ := newDiskTestLedger(t)
	ls.metrics.init(prometheus.NewRegistry())
	logs := &logRecords{}
	ls.config.Logger = slog.New(logs)
	key := bytes.Repeat([]byte{0xc2}, 28)
	setAccount := &models.Account{
		StakingKey: key, Active: true, Reward: 100, AddedSlot: 7, CreatedSlot: 5,
		Pool: bytes.Repeat([]byte{0x24}, 28),
	}
	ls.config.UtxoSet = &memAccountSet{
		memNonceSet: memNonceSet{nonces: map[string]models.BlockNonce{}},
		account:     setAccount,
	}
	ls.config.UtxoSetReads = UtxoSetReadsVerify

	storeAccount := *setAccount
	storeAccount.Reward = types.Uint64(250)
	storeAccount.AddedSlot = 9
	ls.verifyAccount(nil, 0, key, false, &storeAccount, ls.verifyGuard(nil))
	require.EqualValues(t, 1, ls.accountReadMismatches.Load())
	attrs := logs.attrs("live UTxO set account read differs from the metadata store")
	require.NotNil(t, attrs, "the mismatch is logged")
	require.Equal(t,
		"reward store=250 set=100; added_slot store=9 set=7", attrs["diff"])
	require.Equal(t, "batches=0 account_changes=0 rollbacks=0", attrs["staged"])

	ls.verifyAccount(nil, 0, key, false, nil, ls.verifyGuard(nil))
	require.EqualValues(t, 2, ls.accountReadMismatches.Load())
}

func TestAccountDiffNamesAbsence(t *testing.T) {
	t.Parallel()
	a := &models.Account{StakingKey: []byte{1}}
	require.Empty(t, accountDiff(nil, nil))
	require.Equal(t, "store=absent set=present", accountDiff(nil, a))
	require.Equal(t, "store=present set=absent", accountDiff(a, nil))
	require.Empty(t, accountDiff(a, a))
}
