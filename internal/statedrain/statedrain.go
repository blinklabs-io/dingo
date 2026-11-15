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

// Package statedrain deletes the SQL copy of a ledger-state family once the
// live UTxO set is proven to hold it, and records the family in the persisted
// latch so no later start reads or rewrites those rows.
package statedrain

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// DefaultChunkRows is how many rows one delete statement removes.
const DefaultChunkRows = 5000

// MarkerPrefix prefixes the sync_state key that marks a family's drain as
// begun. Its presence means the gate already passed, so a resumed drain
// finishes the deletes without re-running the gate against rows it has
// partly deleted.
const MarkerPrefix = "ledger.state_drain."

// ErrGateRefused is wrapped by every error that stops a drain because a
// precondition does not hold. The SQL rows are untouched.
var ErrGateRefused = errors.New("ledger-state drain gate refused")

// Table is one SQL table of a family. Key is a unique column the chunked
// delete orders by.
type Table struct {
	Name string
	Key  string
}

// Entry is one family's drain: the tables whose rows go, and the checks that
// must pass first.
type Entry struct {
	Family nodesettings.Family
	// Tables are deleted in order, so a child table precedes the table its
	// foreign key references.
	Tables []Table
	// SyncKeys are sync_state keys the family owns; they are deleted after
	// the tables, under the same marker. sync_state itself is never drained,
	// so a family names its keys rather than a table.
	SyncKeys []string
	// SyncKeyPrefixes name sync_state keys by prefix, for keys a family
	// writes per epoch or per slot and so cannot list in advance. Every key
	// found under a prefix is deleted after SyncKeys, under the same marker.
	// The drain's own marker keys are never deleted this way, and an empty
	// prefix is refused. The Store must implement SyncKeyLister.
	SyncKeyPrefixes []string
	// Loaded reports whether every set marker the family reads is loaded.
	Loaded func() (bool, error)
	// Verify compares the set's copy of the family with the SQL rows and
	// returns an error on any difference. It runs only when the operator
	// asked to verify before draining.
	Verify func(ctx context.Context) error
}

// Store is the metadata store as the drain uses it.
type Store interface {
	// SQLTip returns the metadata store's tip, the zero point when empty.
	SQLTip(ctx context.Context) (ocommon.Point, error)
	DrainedFamilies(ctx context.Context) (nodesettings.FamilyMask, error)
	RecordDrainedFamily(ctx context.Context, f nodesettings.Family) error
	DeleteLedgerStateRows(
		ctx context.Context,
		table string,
		key string,
		limit int,
	) (int64, error)
	GetSyncState(ctx context.Context, key string) (string, error)
	SetSyncState(ctx context.Context, key, value string) error
	DeleteSyncState(ctx context.Context, key string) error
}

// SyncKeyLister is the Store capability an Entry with SyncKeyPrefixes needs.
type SyncKeyLister interface {
	// ListSyncStateKeys returns every sync_state key beginning with prefix.
	ListSyncStateKeys(ctx context.Context, prefix string) ([]string, error)
}

// SetState is the live UTxO set as the drain reads it.
type SetState interface {
	// Tip returns the point the set last recorded; ok is false when it has
	// recorded none.
	Tip() (ocommon.Point, bool, error)
}

// Guard learns which tables a drained family owns, so a read of one can be
// caught.
type Guard interface {
	Arm(family nodesettings.Family, tables []string)
}

// Env carries what Run needs.
type Env struct {
	Store Store
	Set   SetState
	// Guard, when non-nil, is armed for every drained family.
	Guard Guard
	// VerifyBeforeDrain runs each entry's Verify before its first delete.
	VerifyBeforeDrain bool
	// ChunkRows overrides DefaultChunkRows.
	ChunkRows int
	// Known is the set of families this binary understands; a latch bit
	// outside it stops the run. Zero means every family of this release.
	Known  nodesettings.FamilyMask
	Logger *slog.Logger
}

// Source is what the graduated entries read: the set's tip, whether it holds
// a family's markers, and a comparison of a family's rows in the set with the
// SQL rows.
type Source interface {
	SetState
	Loaded(family nodesettings.Family) (bool, error)
	Verify(ctx context.Context, family nodesettings.Family) error
}

// graduated lists the families this release drains, in drain order, with the
// tables each one owns. A family is added only after every read of its SQL
// rows has moved to the set and its SQL writes stop once drained.
var graduated = []struct {
	family   nodesettings.Family
	tables   []Table
	syncKeys []string
}{
	{nodesettings.FamilyNonces, []Table{{"block_nonce", "id"}}, nil},
	{nodesettings.FamilyEraSwitch, nil, []string{EraSwitchSyncKey}},
	{
		nodesettings.FamilyNetworkDonations,
		[]Table{{"network_donation", "id"}, {"network_state", "id"}},
		nil,
	},
	{nodesettings.FamilyLiveStake, []Table{{"reward_live_stake", "id"}}, nil},
	{
		nodesettings.FamilyCommitteeConstitution,
		[]Table{
			{"auth_committee_hot", "id"},
			{"resign_committee_cold", "id"},
			{"committee_member", "id"},
			{"committee_quorum", "id"},
			{"constitution", "id"},
		},
		nil,
	},
	{
		nodesettings.FamilyGovernance,
		[]Table{
			{"governance_vote_history", "id"},
			{"governance_vote", "id"},
			{"governance_proposal_ratification_history", "id"},
			{"governance_proposal_order", "proposal_id"},
			{"governance_proposal_drop", "proposal_id"},
			{"governance_proposal", "id"},
		},
		nil,
	},
	{
		nodesettings.FamilyPParamsUpdates,
		[]Table{{"pparam_update", "id"}, {"pparams", "id"}},
		[]string{SyntheticV2SyncKey, SyntheticV2ClearedEpochSyncKey},
	},
	// The MIR certificate tables stay: the account MIR history API joins
	// them with the certificate's transaction, which the set does not keep.
	{nodesettings.FamilyMIRGenesis, []Table{{"genesis_delegation", "id"}}, nil},
	// Neither table has a unique column, so each is ordered by epoch, and one
	// delete statement removes every row of the epochs it reaches, which is
	// bounded by the epochs a bootstrap imports. The family drains before
	// reward-inputs and op-certs: its Verify rebuilds the window from the
	// imported ADA pots and the op-cert rows.
	{
		nodesettings.FamilyRewardBlocksFees,
		[]Table{
			{"imported_pool_block_count", "epoch"},
			{"imported_epoch_block_total", "epoch"},
		},
		nil,
	},
	// reward_seed_failure has no unique column, so it is ordered by epoch and
	// its few rows go in one delete.
	{
		nodesettings.FamilyRewardInputs,
		[]Table{
			{"reward_stake_input", "id"},
			{"reward_pool_input", "id"},
			{"reward_seed_failure", "epoch"},
			{"reward_snapshot", "id"},
			{"reward_ada_pots", "id"},
		},
		[]string{RewardStakeInputsPendingSyncKey},
	},
	{nodesettings.FamilyOpCerts, []Table{{"pool_opcert_sequence", "id"}}, nil},
	// The two drain as a pair, DReps first: a certificate that changes a
	// DRep delegation reads the account's prior delegation, and the store
	// refuses to write while exactly one of them is drained, so no write runs
	// between the two drains. The tables each name are children first.
	{
		nodesettings.FamilyDReps,
		[]Table{
			{"drep_delegator", "id"},
			{"drep_expiry_history", "id"},
			{"drep_dormancy_history", "id"},
			{"drep_dormancy_state", "id"},
			{"drep_expiry_epoch_event", "added_slot"},
			{"account_drep_clear", "staking_key"},
			{"drep", "id"},
		},
		nil,
	},
	{
		nodesettings.FamilyAccounts,
		[]Table{
			{"account_reward_delta", "id"},
			{"account_withdrawal_witness", "id"},
			{"account_import_baseline", "staking_key"},
			{"registration", "id"},
			{"deregistration", "id"},
			{"stake_registration", "id"},
			{"stake_deregistration", "id"},
			{"account", "id"},
		},
		nil,
	},
	// The account rollback reads the pool retirement history to tell a
	// delegation a reap removed from one a certificate set, and only the
	// accounts drain removes that rollback, so pools-history drains after it.
	{
		nodesettings.FamilyPoolsHistory,
		[]Table{
			{"pool_registration_owner", "id"},
			{"pool_registration_relay", "id"},
			{"pool_retirement", "id"},
			{"pool_registration", "id"},
			{"pool", "id"},
		},
		nil,
	},
	// reward_credit_round has no id column, so it is ordered by epoch. The
	// family drains after accounts: an account rollback records the
	// credentials it restored under the recheck key this entry deletes.
	{
		nodesettings.FamilyRewardOutputs,
		[]Table{
			{"reward_account_output", "id"},
			{"reward_pool_output", "id"},
			{"reward_credit_round", "snapshot_epoch"},
		},
		[]string{RewardEligibilityRecheckSyncKey},
	},
	// The two drain last. Summaries go first because their Verify bounds its
	// scan by the newest epoch row of the metadata store, which a drained
	// epochs family would answer from the set it is being compared with.
	{nodesettings.FamilySummaries, []Table{{"epoch_summary", "id"}}, nil},
	{nodesettings.FamilyEpochs, []Table{{"epoch", "id"}}, nil},
}

// SyntheticV2SyncKey and SyntheticV2ClearedEpochSyncKey are the sync_state
// keys of the synthetic PlutusV2 cost model marker and the epoch real data
// was first confirmed at, the sync state of the pparams-updates family.
const (
	SyntheticV2SyncKey             = "synthetic_v2_cost_model"
	SyntheticV2ClearedEpochSyncKey = "synthetic_v2_cost_model_cleared_epoch"
)

// RewardStakeInputsPendingSyncKey is the sync_state key listing the stake
// input snapshots whose deferred write has not finished, the only sync state
// of the reward-inputs family. The set writes a snapshot's stake rows in the
// boundary's own apply, so a drained family has no gap for it to describe.
const RewardStakeInputsPendingSyncKey = "dingo:stake-reward:stake-inputs-pending"

// RewardPrecomputeCursorSyncPrefix starts the sync_state key of each epoch's
// reward precompute cursor, and RewardEligibilityRecheckSyncKey is the key
// listing the credentials a rollback restored: the sync state of the
// reward-outputs family.
const (
	RewardPrecomputeCursorSyncPrefix = "dingo:stake-reward:precompute-cursor:"
	RewardEligibilityRecheckSyncKey  = "dingo:reward-credit:eligibility-recheck"
)

// EraSwitchSyncKey is the sync_state key of the recorded mid-epoch era
// switch, the only state of the era-switch family.
const EraSwitchSyncKey = "mid_epoch_era_switch"

// syncKeyPrefixes lists, per family, the sync_state key prefixes its drain
// deletes.
var syncKeyPrefixes = map[nodesettings.Family][]string{
	nodesettings.FamilyRewardOutputs: {RewardPrecomputeCursorSyncPrefix},
}

func entryFor(
	src Source,
	f nodesettings.Family,
	syncKeys []string,
	tables ...Table,
) Entry {
	return Entry{
		Family:          f,
		Tables:          tables,
		SyncKeys:        syncKeys,
		SyncKeyPrefixes: slices.Clone(syncKeyPrefixes[f]),
		Loaded:          func() (bool, error) { return src.Loaded(f) },
		Verify: func(ctx context.Context) error {
			return src.Verify(ctx, f)
		},
	}
}

// Tables returns the SQL tables of a graduated family in delete order, nil
// for a family this release does not drain.
func Tables(family nodesettings.Family) []Table {
	for _, g := range graduated {
		if g.family == family {
			return slices.Clone(g.tables)
		}
	}
	return nil
}

// TableNames returns the names of Tables(family).
func TableNames(family nodesettings.Family) []string {
	tables := Tables(family)
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.Name)
	}
	return names
}

// Graduated returns the families this release drains, in drain order.
func Graduated(src Source) []Entry {
	entries := make([]Entry, 0, len(graduated))
	for _, g := range graduated {
		entries = append(
			entries, entryFor(src, g.family, g.syncKeys, Tables(g.family)...),
		)
	}
	return entries
}

// Staged returns the entries of families whose ledger reads and writes have
// moved to the set but that no release drains yet. Graduated never includes
// them. Every family is graduated, so the list is empty; a family that is
// implemented ahead of its graduation is listed here until it moves.
func Staged(Source) []Entry {
	return nil
}

// Run drains every entry whose bit is not yet in the latch, in order. It is
// idempotent: a family already latched is skipped, and one interrupted after
// its gate passed resumes its deletes.
func Run(ctx context.Context, env Env, entries []Entry) error {
	known := env.Known
	if known == 0 {
		known = nodesettings.AllFamilies()
	}
	latch, err := env.Store.DrainedFamilies(ctx)
	if err != nil {
		return err
	}
	if unknown := nodesettings.UnknownFamilies(latch, known); unknown != 0 {
		return fmt.Errorf(
			"this database drained ledger-state families a newer release "+
				"graduated (mask %s); run that release or a later one",
			nodesettings.EncodeFamilyMask(unknown),
		)
	}
	for _, entry := range entries {
		if latch.Has(entry.Family) {
			// A latched family whose marker is present has had its rows
			// written back by an import into the set, which marks the family
			// before it deletes them again.
			begun, err := env.Store.GetSyncState(
				ctx, MarkerPrefix+entry.Family.String(),
			)
			if err != nil {
				return fmt.Errorf("read drain marker: %w", err)
			}
			if begun != "" {
				if err := finish(ctx, env, entry); err != nil {
					return fmt.Errorf("drain %s: %w", entry.Family, err)
				}
				continue
			}
			armGuard(env, entry)
			continue
		}
		if err := drain(ctx, env, entry); err != nil {
			return fmt.Errorf("drain %s: %w", entry.Family, err)
		}
		latch = latch.With(entry.Family)
	}
	return nil
}

// BegunFamilies returns the families named by drain marker keys: those whose
// drain began, so some of their rows may be deleted. A marker naming a family
// this binary does not know is an error, since its tables cannot be named.
func BegunFamilies(keys []string) (nodesettings.FamilyMask, error) {
	var mask nodesettings.FamilyMask
	for _, key := range keys {
		name, ok := strings.CutPrefix(key, MarkerPrefix)
		if !ok {
			continue
		}
		found := false
		for _, f := range nodesettings.Families() {
			if f.String() == name {
				mask = mask.With(f)
				found = true
				break
			}
		}
		if !found {
			return 0, fmt.Errorf(
				"drain marker %q names a family this release does not "+
					"know; run the release that began it or a later one", key,
			)
		}
	}
	return mask, nil
}

func armGuard(env Env, entry Entry) {
	if env.Guard == nil {
		return
	}
	names := make([]string, 0, len(entry.Tables))
	for _, table := range entry.Tables {
		names = append(names, table.Name)
	}
	env.Guard.Arm(entry.Family, names)
}

func drain(ctx context.Context, env Env, entry Entry) error {
	marker := MarkerPrefix + entry.Family.String()
	begun, err := env.Store.GetSyncState(ctx, marker)
	if err != nil {
		return fmt.Errorf("read drain marker: %w", err)
	}
	if begun == "" {
		if err := checkGate(ctx, env, entry); err != nil {
			return err
		}
		if err := env.Store.SetSyncState(ctx, marker, "1"); err != nil {
			return fmt.Errorf("record drain marker: %w", err)
		}
	}
	return finish(ctx, env, entry)
}

// MarkRewritten records, for every family in mask, that its SQL rows were
// written again and must be deleted: the same marker a drain writes before its
// first delete, so Run or DeleteMarked resumes the deletes after a crash.
func MarkRewritten(
	ctx context.Context,
	store Store,
	mask nodesettings.FamilyMask,
) error {
	for _, g := range graduated {
		if !mask.Has(g.family) {
			continue
		}
		if err := store.SetSyncState(
			ctx, MarkerPrefix+g.family.String(), "1",
		); err != nil {
			return fmt.Errorf("record drain marker: %w", err)
		}
	}
	return nil
}

// DeleteMarked deletes the SQL rows of every family in mask whose drain marker
// is present, the finishing half of a drain.
func DeleteMarked(
	ctx context.Context,
	env Env,
	mask nodesettings.FamilyMask,
) error {
	for _, g := range graduated {
		if !mask.Has(g.family) {
			continue
		}
		begun, err := env.Store.GetSyncState(
			ctx, MarkerPrefix+g.family.String(),
		)
		if err != nil {
			return fmt.Errorf("read drain marker: %w", err)
		}
		if begun == "" {
			continue
		}
		entry := entryFor(nil, g.family, g.syncKeys, Tables(g.family)...)
		if err := finish(ctx, env, entry); err != nil {
			return fmt.Errorf("drain %s: %w", g.family, err)
		}
	}
	return nil
}

// finish deletes a family's rows and sync keys, latches it and clears its
// marker. Every step is idempotent, so it resumes after a crash.
func finish(ctx context.Context, env Env, entry Entry) error {
	marker := MarkerPrefix + entry.Family.String()
	chunk := env.ChunkRows
	if chunk <= 0 {
		chunk = DefaultChunkRows
	}
	for _, table := range entry.Tables {
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			n, err := env.Store.DeleteLedgerStateRows(
				ctx, table.Name, table.Key, chunk,
			)
			if err != nil {
				return err
			}
			if n < int64(chunk) {
				break
			}
		}
	}
	for _, key := range entry.SyncKeys {
		if err := env.Store.DeleteSyncState(ctx, key); err != nil {
			return fmt.Errorf("delete sync key %s: %w", key, err)
		}
	}
	if err := deleteSyncKeyPrefixes(ctx, env, entry); err != nil {
		return err
	}
	if err := env.Store.RecordDrainedFamily(ctx, entry.Family); err != nil {
		return err
	}
	armGuard(env, entry)
	if err := env.Store.DeleteSyncState(ctx, marker); err != nil {
		return fmt.Errorf("clear drain marker: %w", err)
	}
	if env.Logger != nil {
		env.Logger.Info(
			"drained ledger-state family from the metadata store",
			"component", "database",
			"family", entry.Family.String(),
		)
	}
	return nil
}

func deleteSyncKeyPrefixes(ctx context.Context, env Env, entry Entry) error {
	if len(entry.SyncKeyPrefixes) == 0 {
		return nil
	}
	lister, ok := env.Store.(SyncKeyLister)
	if !ok {
		return fmt.Errorf(
			"%w: the store cannot list sync_state keys", ErrGateRefused,
		)
	}
	for _, prefix := range entry.SyncKeyPrefixes {
		if prefix == "" {
			return fmt.Errorf(
				"%w: an empty sync_state key prefix", ErrGateRefused,
			)
		}
		keys, err := lister.ListSyncStateKeys(ctx, prefix)
		if err != nil {
			return fmt.Errorf("list sync keys under %q: %w", prefix, err)
		}
		for _, key := range keys {
			if strings.HasPrefix(key, MarkerPrefix) {
				continue
			}
			if err := env.Store.DeleteSyncState(ctx, key); err != nil {
				return fmt.Errorf("delete sync key %s: %w", key, err)
			}
		}
	}
	return nil
}

func checkGate(ctx context.Context, env Env, entry Entry) error {
	if entry.Loaded == nil {
		return fmt.Errorf("%w: no set markers declared", ErrGateRefused)
	}
	loaded, err := entry.Loaded()
	if err != nil {
		return fmt.Errorf("read set markers: %w", err)
	}
	if !loaded {
		return fmt.Errorf("%w: set markers not loaded", ErrGateRefused)
	}
	setTip, ok, err := env.Set.Tip()
	if err != nil {
		return fmt.Errorf("read set tip: %w", err)
	}
	sqlTip, err := env.Store.SQLTip(ctx)
	if err != nil {
		return fmt.Errorf("read SQL tip: %w", err)
	}
	if !ok {
		setTip = ocommon.Point{}
	}
	if setTip.Slot != sqlTip.Slot ||
		string(setTip.Hash) != string(sqlTip.Hash) {
		return fmt.Errorf(
			"%w: set tip %d differs from SQL tip %d",
			ErrGateRefused, setTip.Slot, sqlTip.Slot,
		)
	}
	if env.VerifyBeforeDrain {
		if entry.Verify == nil {
			return fmt.Errorf("%w: no verification declared", ErrGateRefused)
		}
		if err := entry.Verify(ctx); err != nil {
			return fmt.Errorf("%w: verification: %w", ErrGateRefused, err)
		}
	}
	return nil
}
