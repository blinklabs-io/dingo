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
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// memNonceSet is a live set that keeps only block nonces in memory.
type memNonceSet struct {
	mu     sync.Mutex
	nonces map[string]models.BlockNonce
	onRead func()
}

func (*memNonceSet) Get([]byte, uint32) ([]byte, bool, error) { return nil, false, nil }
func (*memNonceSet) Sum([]byte) (uint64, error)               { return 0, nil }
func (*memNonceSet) ApplyUtxoChanges(*ocommon.Point, []UtxoEvent) error {
	return nil
}

func (s *memNonceSet) ApplyLedgerChanges(_ *ocommon.Point, lc LedgerChanges) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range lc.Nonces {
		s.nonces[nonceKeyOf(n.Slot, n.Hash)] = n
	}
	return nil
}

func (*memNonceSet) NoncesLoaded() (bool, error) { return true, nil }
func (*memNonceSet) BlockNonce(ocommon.Point) ([]byte, bool, error) {
	return nil, false, nil
}

func (*memNonceSet) LatestBlockNonce() (models.BlockNonce, bool, error) {
	return models.BlockNonce{}, false, nil
}

func (s *memNonceSet) BlockNoncesInSlotRange(start, end uint64) ([]models.BlockNonce, error) {
	s.mu.Lock()
	var out []models.BlockNonce
	for _, n := range s.nonces {
		if n.Slot >= start && n.Slot < end {
			out = append(out, n)
		}
	}
	hook := s.onRead
	s.onRead = nil
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	return out, nil
}

func testNonces(from, count uint64) []models.BlockNonce {
	var out []models.BlockNonce
	for i := range count {
		slot := from + i*20
		out = append(out, models.BlockNonce{
			Slot:  slot,
			Hash:  bytes.Repeat([]byte{byte(slot)}, 32),
			Nonce: bytes.Repeat([]byte{byte(slot) ^ 0x5a}, 32),
		})
	}
	return out
}

// A verify read without a transaction (the header path's epoch-nonce
// forecast reads this way) reads the set first and the metadata store
// second. A block-apply commit landing between the two shows its nonces in
// the store only; the read must retry rather than count a disagreement.
func TestNonceVerifyReadRetriesAcrossAConcurrentCommit(t *testing.T) {
	t.Parallel()
	ls, _ := newDiskTestLedger(t)
	ls.metrics.init(prometheus.NewRegistry())
	set := &memNonceSet{nonces: map[string]models.BlockNonce{}}
	ls.config.UtxoSet = set
	ls.config.UtxoSetReads = UtxoSetReadsVerify

	earlier := testNonces(1000, 20)
	require.NoError(t, ls.ledgerDB().Run(
		ls.ledgerDB().Begin(t.Context(), true),
		func(txn ledgerdb.Txn) error {
			if err := ls.ledgerDB().SetBlockNonces(earlier, txn); err != nil {
				return err
			}
			ls.stageBlockNonces(txn, earlier, 0)
			return nil
		},
	))
	_, err := ls.blockNoncesInSlotRange(0, 100_000, nil)
	require.NoError(t, err)
	require.Zero(t, ls.nonceSetReadMismatches.Load(), "seeded stores agree")

	// The epoch's last blocks, applied while the forecast reads.
	last := testNonces(1400, 8)
	applied := make(chan error, 1)
	set.onRead = func() {
		txn := ls.ledgerDB().Begin(t.Context(), true)
		metadataCommitted := make(chan struct{})
		txn.OnFinish(func() { close(metadataCommitted) })
		go func() {
			applied <- ls.ledgerDB().Run(txn, func(txn ledgerdb.Txn) error {
				if err := ls.ledgerDB().SetBlockNonces(last, txn); err != nil {
					return err
				}
				ls.stageBlockNonces(txn, last, 0)
				return nil
			})
		}()
		// The commit is visible to new metadata reads now; its set apply
		// waits for this reader's utxoSetApplyMu read lock.
		<-metadataCommitted
	}
	stored, err := ls.blockNoncesInSlotRange(0, 100_000, nil)
	require.NoError(t, err)
	require.Len(t, stored, len(earlier)+len(last))
	require.Zero(t, ls.nonceSetReadMismatches.Load(),
		"a commit between the two reads is not a disagreement")

	require.NoError(t, <-applied)
	_, err = ls.blockNoncesInSlotRange(0, 100_000, nil)
	require.NoError(t, err)
	require.Zero(t, ls.nonceSetReadMismatches.Load())
	set.mu.Lock()
	defer set.mu.Unlock()
	for _, n := range last {
		_, ok := set.nonces[nonceKeyOf(n.Slot, n.Hash)]
		require.True(t, ok)
	}
}

// Every verify comparison skips a disagreement that a commit between its two
// reads explains, and still counts one that no commit explains.
func TestVerifyComparisonsSkipARacedDisagreement(t *testing.T) {
	t.Parallel()
	ls, _ := newDiskTestLedger(t)
	ls.metrics.init(prometheus.NewRegistry())
	ls.config.UtxoSet = &memNonceSet{nonces: map[string]models.BlockNonce{}}
	ls.config.UtxoSetReads = UtxoSetReadsVerify

	compares := map[string]struct {
		compare    func(verifyReadGuard)
		mismatches func() uint64
	}{
		"account": {
			func(g verifyReadGuard) { ls.compareAccountRead(g, "a", false, nil) },
			ls.accountReadMismatches.Load,
		},
		"drep": {
			func(g verifyReadGuard) { ls.compareDrepRead(g, "d", false, nil) },
			ls.drepReadMismatches.Load,
		},
		"epoch state": {
			func(g verifyReadGuard) { ls.compareEpochStateRead(g, "e", false, nil) },
			ls.epochStateReadMismatches.Load,
		},
		"epoch summary": {
			func(g verifyReadGuard) { ls.compareEpochSummaryRead(g, "es", false, nil) },
			ls.epochSummaryReadMismatches.Load,
		},
		"network state": {
			func(g verifyReadGuard) { ls.compareNetworkStateRead(g, "n", false, nil) },
			ls.networkStateReadMismatches.Load,
		},
		"pool": {
			func(g verifyReadGuard) { ls.comparePoolRead(g, "p", false, nil) },
			ls.poolReadMismatches.Load,
		},
		"registration": {
			func(g verifyReadGuard) { ls.compareRegistrationRead(g, "r", false, nil) },
			ls.registrationReadMismatches.Load,
		},
		"pparams": {
			func(g verifyReadGuard) { ls.comparePParamsRead(g, "pp", false, nil) },
			ls.pparamsReadMismatches.Load,
		},
		"cert history": {
			func(g verifyReadGuard) { ls.compareCertHistoryRead(g, "c", false, nil) },
			ls.certHistoryReadMismatches.Load,
		},
		"op-cert": {
			func(g verifyReadGuard) { ls.compareOpCertRead(g, "o", false, nil) },
			ls.opCertReadMismatches.Load,
		},
		"pparam updates": {
			func(g verifyReadGuard) { ls.comparePParamUpdateRead(g, "pu", false, nil) },
			ls.pparamUpdateReadMismatches.Load,
		},
		"live stake": {
			func(g verifyReadGuard) { ls.compareLiveStakeRead(g, "ls", false, nil) },
			ls.liveStakeReadMismatches.Load,
		},
		"reward output": {
			func(g verifyReadGuard) { ls.compareRewardOutputRead(g, "r", false, nil) },
			ls.rewardOutputReadMismatches.Load,
		},
		"utxo": {
			func(g verifyReadGuard) {
				ls.compareUtxoSetRead(g, []byte{1}, 0, []byte{1}, true, nil, false, nil)
			},
			ls.utxoSetReadMismatches.Load,
		},
	}
	commit := func(slot uint64) {
		require.NoError(t, ls.ledgerDB().Run(
			ls.ledgerDB().Begin(t.Context(), true),
			func(txn ledgerdb.Txn) error {
				ls.stageBlockNonces(txn, testNonces(slot, 1), 0)
				return nil
			},
		))
	}
	slot := uint64(1000)
	for name, c := range compares {
		raced := ls.verifyGuard(nil)
		commit(slot)
		slot += 100
		c.compare(raced)
		require.Zero(t, c.mismatches(), "%s: a raced disagreement", name)

		c.compare(ls.verifyGuard(nil))
		require.Equal(t, uint64(1), c.mismatches(), "%s: an unraced disagreement", name)
	}

	// Set mode trusts the set and holds no guard, so nothing it compares
	// could tell a disagreement from a racing write: it compares nothing.
	ls.config.UtxoSetReads = UtxoSetReadsSet
	for name, c := range compares {
		c.compare(ls.beginVerifyRead(nil))
		require.Equal(t, uint64(1), c.mismatches(), "%s: set mode compared", name)
	}
}
