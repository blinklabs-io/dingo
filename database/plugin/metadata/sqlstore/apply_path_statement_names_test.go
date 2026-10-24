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

package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// sqlDurationCounts returns the total number of observations in
// dingo_database_sql_query_duration_seconds and how many of them carry
// query="unknown".
func sqlDurationCounts(
	t *testing.T,
	reg *prometheus.Registry,
) (total, unknown uint64) {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "dingo_database_sql_query_duration_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			count := metric.GetHistogram().GetSampleCount()
			total += count
			for _, label := range metric.GetLabel() {
				if label.GetName() == "query" && label.GetValue() == "unknown" {
					unknown += count
				}
			}
		}
	}
	return total, unknown
}

// TestApplyPathHelpersRecordNamedStatements runs block-apply helpers that
// issue hand-written statements outside the hot-statement cache and requires
// every statement they issue to be recorded under a name, not "unknown".
func TestApplyPathHelpersRecordNamedStatements(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	store := newMigratedSQLiteStoreWithRegistry(t, reg)
	drep := bytes.Repeat([]byte{0x11}, 28)
	stake := bytes.Repeat([]byte{0x22}, 28)
	ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(3))
	proposal, err := store.writeDB.ExecContext(t.Context(), `
INSERT INTO governance_proposal (
    tx_hash, action_index, action_type, proposed_epoch, expires_epoch,
    deposit, added_slot
) VALUES (?, 0, 6, 1, 10, 1, 10)`, bytes.Repeat([]byte{0x01}, 32))
	require.NoError(t, err)
	proposalID, err := proposal.LastInsertId()
	require.NoError(t, err)

	inWrite := func(
		fn func(ctx context.Context, db queryer) error,
	) func() error {
		return func() error {
			return store.withWriteTransaction(
				nil,
				func(db queryer, ctx context.Context) error {
					return fn(ctx, db)
				},
			)
		}
	}
	for _, step := range []struct {
		name string
		run  func() error
	}{
		{"storeScriptContent", inWrite(func(ctx context.Context, db queryer) error {
			return storeScriptContent(
				ctx, db, lcommon.PlutusV2Script{0x01, 0x02}, 3, 10,
			)
		})},
		{"addDrepDelegator", inWrite(func(ctx context.Context, db queryer) error {
			return addDrepDelegator(ctx, db, 0, drep, 0, stake, 10)
		})},
		{"removeDrepDelegator", inWrite(func(ctx context.Context, db queryer) error {
			return removeDrepDelegator(ctx, db, 0, drep, 0, stake, 11)
		})},
		{"removeAllDrepDelegators", inWrite(func(ctx context.Context, db queryer) error {
			return removeAllDrepDelegators(ctx, db, 0, drep, 12)
		})},
		{"recordDrepExpiry", inWrite(func(ctx context.Context, db queryer) error {
			return recordDrepExpiry(ctx, db, 0, drep, 10, 1, 21)
		})},
		{"applyRewardLiveStakeAggregate", inWrite(func(ctx context.Context, db queryer) error {
			return store.applyRewardLiveStakeAggregate(ctx, db, ref, 10, 0)
		})},
		{"SetGovernanceVote cast", func() error {
			return store.SetGovernanceVote(&models.GovernanceVote{
				ProposalID:      uint(proposalID),
				VoterType:       models.VoterTypeDRep,
				VoterCredential: drep,
				Vote:            models.VoteYes,
				AddedSlot:       10,
				CastTxHash:      bytes.Repeat([]byte{0x55}, 32),
				CastCertIndex:   0,
			}, nil)
		}},
		{"SetBlockNonces", func() error {
			return store.SetBlockNonces([]models.BlockNonce{{
				Hash:  bytes.Repeat([]byte{0x33}, 32),
				Nonce: bytes.Repeat([]byte{0x44}, 32),
				Slot:  10,
			}}, nil)
		}},
	} {
		totalBefore, unknownBefore := sqlDurationCounts(t, reg)
		require.NoError(t, step.run(), step.name)
		totalAfter, unknownAfter := sqlDurationCounts(t, reg)
		require.Greater(
			t, totalAfter, totalBefore,
			"%s issued no measured statement", step.name,
		)
		require.Equal(
			t, unknownBefore, unknownAfter,
			"%s recorded a statement under query=\"unknown\"", step.name,
		)
	}
}

// TestPoolAndCommitteeApplyStatementsAreNamed covers the statements a block
// apply reaches through pool registration, committee hot authorization,
// DRep deregistration and header validation, none of which go through the
// hot-statement cache.
func TestPoolAndCommitteeApplyStatementsAreNamed(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	store := newMigratedSQLiteStoreWithRegistry(t, reg)
	depositHeldEpochs(t, store, 6)
	pool := depositHeldPoolKey(0xa7)
	cold := credentialHash(0xc7)
	at := poolCertPosition{slot: 300}

	inWrite := func(
		fn func(ctx context.Context, db queryer) error,
	) func() error {
		return func() error {
			return store.withWriteTransaction(
				nil,
				func(db queryer, ctx context.Context) error {
					return fn(ctx, db)
				},
			)
		}
	}
	certAt := func(slot uint64, cert lcommon.Certificate) func() error {
		return func() error {
			writeDepositHeldCert(t, store, slot, 0, cert, 500)
			return nil
		}
	}
	for _, step := range []struct {
		name string
		run  func() error
	}{
		{"pool registration", certAt(100, depositHeldRegistration(pool))},
		{"pool retirement", certAt(200, depositHeldRetirement(pool, 3))},
		{"pool re-registration", certAt(300, depositHeldRegistration(pool))},
		{"poolRegistrationID", inWrite(func(ctx context.Context, db queryer) error {
			_, err := poolRegistrationID(ctx, db, 1, 100, 1, 0)
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		})},
		{"latestPoolRetirementBefore", inWrite(func(ctx context.Context, db queryer) error {
			_, _, err := latestPoolRetirementBefore(ctx, db, 1, at)
			return err
		})},
		{"epochAtSlot", inWrite(func(ctx context.Context, db queryer) error {
			_, _, err := epochAtSlot(ctx, db, 300)
			return err
		})},
		{"pruneCommitteeHotAuthorizations", inWrite(func(ctx context.Context, db queryer) error {
			_, err := store.pruneCommitteeHotAuthorizations(
				ctx, db, 0, cold, preprodTipSlot,
			)
			return err
		})},
		{"DeleteGovernanceVotesForDrep", func() error {
			_, err := store.DeleteGovernanceVotesForDrep(0, cold, 1, 300, nil)
			return err
		}},
		{"LatestPoolOpCertSequence", func() error {
			_, _, err := store.LatestPoolOpCertSequence(pool, nil)
			return err
		}},
		{"LatestPoolOpCertSequenceAfter", func() error {
			_, _, err := store.LatestPoolOpCertSequenceAfter(pool, 100, nil)
			return err
		}},
		{"LatestPoolOpCertSequenceAtOrBefore", func() error {
			_, _, err := store.LatestPoolOpCertSequenceAtOrBefore(pool, 100, nil)
			return err
		}},
		{"LatestPoolOpCertSequenceInRange", func() error {
			_, _, err := store.LatestPoolOpCertSequenceInRange(
				pool,
				50,
				100,
				nil,
			)
			return err
		}},
		{"PoolOpCertSequencesExistAtSlot", func() error {
			_, err := store.PoolOpCertSequencesExistAtSlot(100, nil)
			return err
		}},
		{"GetPoolVrfKeyHashAtSlot", func() error {
			_, _, err := store.GetPoolVrfKeyHashAtSlot(pool.Bytes(), 300, nil)
			return err
		}},
		{"GetPoolByVrfKeyHash", func() error {
			_, err := store.GetPoolByVrfKeyHash(
				bytes.Repeat([]byte{0x5b}, 32), 0, nil,
			)
			return err
		}},
	} {
		totalBefore, unknownBefore := sqlDurationCounts(t, reg)
		require.NoError(t, step.run(), step.name)
		totalAfter, unknownAfter := sqlDurationCounts(t, reg)
		require.Greater(
			t, totalAfter, totalBefore,
			"%s issued no measured statement", step.name,
		)
		require.Equal(
			t, unknownBefore, unknownAfter,
			"%s recorded a statement under query=\"unknown\"", step.name,
		)
	}
}
