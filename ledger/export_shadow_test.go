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
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"math/big"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// ShadowScenario is the result of RunShadowScenarioForTest.
type ShadowScenario struct {
	// Points are the applied blocks' points, in chain order.
	Points []ocommon.Point
	// Rollback rolls the ledger back to point.
	Rollback func(point ocommon.Point) error
	// Resume is the ledger's ResumeUtxoSetFrom.
	Resume func(ctx context.Context, point ocommon.Point) (bool, error)
	// UtxoSetFailed reports whether the ledger stopped following the set.
	UtxoSetFailed func() bool
	// UtxoSetReads reports the verify-mode reads and how many of them
	// differed from the metadata store.
	UtxoSetReads func() (verified, mismatches float64)
	// ReadsForValidation resolves refs the way validation's block
	// prefetch does, outside any transaction.
	ReadsForValidation func(refs []models.UtxoId) ([]models.Utxo, error)
	// UtxosByAddress and UtxoIncludingSpent are the ledger's lookups.
	UtxosByAddress     func(addrs []gledger.Address) ([]models.Utxo, error)
	UtxoIncludingSpent func(txID []byte, index uint32) (*models.Utxo, error)
	// UtxoWhole answers LocalStateQuery GetUTxOWhole as of slot (zero for
	// live) and returns its encoded reply.
	UtxoWhole func(slot uint64, hash []byte) ([]byte, error)
	// UtxosByAddressAt and UtxosByTxInAt answer LocalStateQuery
	// GetUTxOByAddress and GetUTxOByTxIn as of point and return the
	// reply's outputs, CBOR-encoded, by "txid#index".
	UtxosByAddressAt func(point ocommon.Point, addrs []gledger.Address) ([]models.Utxo, error)
	UtxosByTxInAt    func(point ocommon.Point, refs []models.UtxoId) (map[string][]byte, error)
	// BlockNonce, BlockNoncesInSlotRange and LatestBlockNonce are the
	// ledger's nonce reads, outside any transaction.
	BlockNonce             func(point ocommon.Point) ([]byte, error)
	BlockNoncesInSlotRange func(start, end uint64) ([]models.BlockNonce, error)
	LatestBlockNonce       func() (models.BlockNonce, bool, error)
	// NonceSetReads reports the verify-mode nonce reads and how many of
	// them differed from the metadata store.
	NonceSetReads func() (verified, mismatches float64)
}

// RunShadowScenarioForTest applies the external-ingest blocks, which include
// an invalid transaction whose collateral return a later block spends, with
// bus as the ledger's event bus. attach runs on the database before any block
// is applied.
func RunShadowScenarioForTest(
	t *testing.T,
	storageMode string,
	bus *event.EventBus,
	utxoSet UtxoSetWriter,
	attach func(*database.Database),
) ShadowScenario {
	return RunUtxoSetScenarioForTest(
		t, storageMode, bus, utxoSet, UtxoSetReadsOff, attach,
	)
}

// RunUtxoSetScenarioForTest is RunShadowScenarioForTest with validation
// reading its inputs as reads selects.
func RunUtxoSetScenarioForTest(
	t *testing.T,
	storageMode string,
	bus *event.EventBus,
	utxoSet UtxoSetWriter,
	reads string,
	attach func(*database.Database),
) ShadowScenario {
	t.Helper()
	return runUtxoSetScenario(
		t,
		storageMode,
		bus,
		utxoSet,
		reads,
		false,
		nil,
		attach,
	)
}

// RunUtxoSetFailureScenarioForTest is RunUtxoSetScenarioForTest with fatal as
// the ledger's FatalErrorFunc, for a utxoSet that fails an update. A setOnly
// run is in core mode, as RunUtxoSetOnlyScenarioForTest's.
func RunUtxoSetFailureScenarioForTest(
	t *testing.T,
	utxoSet UtxoSetWriter,
	reads string,
	setOnly bool,
	fatal func(error),
	attach func(*database.Database),
) ShadowScenario {
	t.Helper()
	return runUtxoSetScenario(
		t,
		types.StorageModeCore,
		nil,
		utxoSet,
		reads,
		setOnly,
		fatal,
		attach,
	)
}

func runUtxoSetScenario(
	t *testing.T,
	storageMode string,
	bus *event.EventBus,
	utxoSet UtxoSetWriter,
	reads string,
	setOnly bool,
	fatal FatalErrorFunc,
	attach func(*database.Database),
	configure ...func(*LedgerState),
) ShadowScenario {
	t.Helper()
	blocks, _ := externalIngestBlocks(t, 0)
	run := runRowBatchingScenarioTolerating(
		t,
		blocks,
		storageMode,
		true,
		2,
		fatal != nil,
		func(ls *LedgerState) {
			ls.config.EventBus = bus
			ls.config.UtxoSet = utxoSet
			ls.config.UtxoSetReads = reads
			ls.config.FatalErrorFunc = fatal
			if setOnly {
				require.NoError(t, ls.EnableUtxoSetOnly())
			}
			attach(ls.db)
			for _, fn := range configure {
				fn(ls)
			}
		},
	)
	points := make([]ocommon.Point, 0, len(blocks))
	for _, b := range blocks {
		points = append(points, blockPoint(b))
	}
	return ShadowScenario{
		Points: points,
		Rollback: func(point ocommon.Point) error {
			return run.ls.rollback(t.Context(), point)
		},
		Resume: run.ls.ResumeUtxoSetFrom,
		UtxoWhole: func(slot uint64, hash []byte) ([]byte, error) {
			res, err := run.ls.queryShelleyUtxoWhole(
				t.Context(), QueryPoint{Slot: slot, Hash: hash}, nil,
			)
			if err != nil {
				return nil, err
			}
			return res.(*utxoWholeResult).encoded, nil
		},
		UtxosByAddress: func(addrs []gledger.Address) ([]models.Utxo, error) {
			return run.ls.UtxosByAddress(t.Context(), addrs)
		},
		UtxosByAddressAt: func(p ocommon.Point, addrs []gledger.Address) ([]models.Utxo, error) {
			res, err := run.ls.queryShelleyUtxoByAddress(
				t.Context(), addrs, QueryPoint{Slot: p.Slot, Hash: p.Hash}, nil,
			)
			if err != nil {
				return nil, err
			}
			return lsqReplyUtxos(res)
		},
		UtxosByTxInAt: func(p ocommon.Point, refs []models.UtxoId) (map[string][]byte, error) {
			ins := make([]gledger.ShelleyTransactionInput, 0, len(refs))
			for _, r := range refs {
				ins = append(ins, gledger.MustNewShelleyTransactionInput(
					hex.EncodeToString(r.Hash), int(r.Idx),
				))
			}
			res, err := run.ls.queryShelleyUtxoByTxIn(
				t.Context(), ins, QueryPoint{Slot: p.Slot, Hash: p.Hash}, nil,
			)
			if err != nil {
				return nil, err
			}
			utxos, err := lsqReplyUtxos(res)
			if err != nil {
				return nil, err
			}
			ret := make(map[string][]byte, len(utxos))
			for _, u := range utxos {
				ret[fmt.Sprintf("%x#%d", u.TxId, u.OutputIdx)] = u.Cbor
			}
			return ret, nil
		},
		UtxoIncludingSpent: func(txID []byte, index uint32) (*models.Utxo, error) {
			return run.ls.UtxoByRefIncludingSpent(txID, index)
		},
		ReadsForValidation: func(refs []models.UtxoId) ([]models.Utxo, error) {
			return run.ls.utxosByRefsForValidation(t.Context(), refs, nil)
		},
		UtxoSetFailed: func() bool { return run.ls.utxoSetFailed.Load() },
		UtxoSetReads: func() (float64, float64) {
			return promtestutil.ToFloat64(run.ls.metrics.utxoSetReadsVerified),
				promtestutil.ToFloat64(run.ls.metrics.utxoSetReadMismatches)
		},
		BlockNonce: func(point ocommon.Point) ([]byte, error) {
			return run.ls.blockNonce(point, nil)
		},
		BlockNoncesInSlotRange: func(start, end uint64) ([]models.BlockNonce, error) {
			return run.ls.blockNoncesInSlotRange(start, end, nil)
		},
		LatestBlockNonce: func() (models.BlockNonce, bool, error) {
			return run.ls.latestBlockNonce(nil)
		},
		NonceSetReads: func() (float64, float64) {
			return promtestutil.ToFloat64(run.ls.metrics.nonceSetReadsVerified),
				promtestutil.ToFloat64(run.ls.metrics.nonceSetReadMismatches)
		},
	}
}

// RunUtxoSetOnlyScenarioForTest is RunShadowScenarioForTest in core mode with
// utxoSet as the only copy of the live UTxO set.
func RunUtxoSetOnlyScenarioForTest(
	t *testing.T,
	utxoSet UtxoSetWriter,
	attach func(*database.Database),
) ShadowScenario {
	t.Helper()
	return runUtxoSetScenario(
		t,
		types.StorageModeCore,
		nil,
		utxoSet,
		UtxoSetReadsOff,
		true,
		nil,
		attach,
	)
}

// RunUtxoSetOnlyDrainedScenarioForTest is RunUtxoSetOnlyScenarioForTest on a
// database that has drained the families of mask before its first block.
func RunUtxoSetOnlyDrainedScenarioForTest(
	t *testing.T,
	utxoSet UtxoSetWriter,
	mask nodesettings.FamilyMask,
	attach func(*database.Database),
) ShadowScenario {
	t.Helper()
	return runUtxoSetScenario(
		t,
		types.StorageModeCore,
		nil,
		utxoSet,
		UtxoSetReadsOff,
		true,
		nil,
		attach,
		func(ls *LedgerState) {
			for _, f := range mask.Families() {
				require.NoError(t, ls.db.RecordDrainedFamily(t.Context(), f))
			}
			ls.SetDrainedFamilies(mask)
		},
	)
}

// MetadataDigestOutsideUtxoSetForTest digests the metadata store without what
// UtxoSetOnly moves to the live UTxO set: the utxo table and the tables keyed
// by its rows.
func MetadataDigestOutsideUtxoSetForTest(
	t *testing.T,
	db *database.Database,
) string {
	t.Helper()
	return metadataDigest(
		t,
		db,
		map[string]bool{
			"utxo":                  true,
			"asset":                 true,
			"utxo_pointer":          true,
			"utxo_collateral_input": true,
			"utxo_reference_input":  true,
			"utxo_address":          true,
			"utxo_reference_script": true,
		},
		nil,
		nil,
	)
}

// lsqReplyUtxos unpacks a GetUTxOByAddress or GetUTxOByTxIn reply into its
// outputs, each CBOR-encoded.
func lsqReplyUtxos(res any) ([]models.Utxo, error) {
	m := res.([]any)[0].(map[olocalstatequery.UtxoId]gledger.TransactionOutput)
	ret := make([]models.Utxo, 0, len(m))
	for id, out := range m {
		c, err := cbor.Encode(out)
		if err != nil {
			return nil, err
		}
		ret = append(ret, models.Utxo{
			TxId: id.Hash.Bytes(), OutputIdx: uint32(id.Idx), Cbor: c, //nolint:gosec // an output index
		})
	}
	return ret, nil
}

// BoundaryEpochStateScenario crosses an epoch boundary with the snapshot
// manager writing through the ledger's LedgerDB, and rolls it back.
type BoundaryEpochStateScenario struct {
	DB                     *database.Database
	ApplyBoundary          func()
	RollbackBeforeBoundary func()
	// ApplyBoundaryQuiet is ApplyBoundary without reading the metadata
	// store afterwards.
	ApplyBoundaryQuiet func()
	// WriteNonces writes nonces the way block processing does: to the
	// metadata store and staged for the set, in one commit.
	WriteNonces func(rows []models.BlockNonce)
	// StageNoncesInSet gives the set nonces the harness wrote only to the
	// metadata store.
	StageNoncesInSet func(rows []models.BlockNonce)
	// PruneNonces runs the ledger's nonce retention for slot.
	PruneNonces func(slot uint64)
	// ReadNonces and ReadNetwork render the family's rows as the ledger's
	// LedgerDB answers them.
	ReadNonces  func() []string
	ReadNetwork func() []string
	// SetNetworkState commits a network state through the LedgerDB.
	SetNetworkState func(treasury, reserves, slot uint64)
	// ReadEpochs renders the epoch and summary rows as the ledger's LedgerDB
	// answers them.
	ReadEpochs func() []string
	// SaveSummary commits an epoch summary through the LedgerDB.
	SaveSummary func(summary models.EpochSummary)
	// AncestorPoint is the rollback target and CurrentPoint the tip above it.
	AncestorPoint func() ocommon.Point
	CurrentPoint  func() ocommon.Point
	// LedgerDB is the LedgerDB the ledger reads through.
	LedgerDB func() ledgerdb.LedgerDB
	// ReadFactRows reads a fact table through the ledger, from the live UTxO
	// set when the table's family is drained and from the metadata store
	// otherwise. UnservedFactTables names the drained tables it cannot read.
	ReadFactRows func(
		table metadata.FactTable, from, through uint64, cursor int64,
		limit int,
	) (metadata.FactRows, int64, error)
	UnservedFactTables func() []string
	// AnchorState is the ledger's source of the state a snapshot import
	// established.
	AnchorState metadata.AnchorStateSource
	// CertifyLeios moves the ledger to the Dijkstra era with a Leios
	// certificate validator, as a node that certifies endorser blocks is.
	CertifyLeios func()
	// ConfigureLeiosValidator sets a certificate validator, as every node
	// does, without moving the ledger to an era that certifies.
	ConfigureLeiosValidator func()
	// DeferredBoundaryRows captures the fixture's boundary at its SNAP
	// point, applies the boundary, and returns the mark rows the
	// post-commit job prepares from the committed boundary, as a Conway
	// node does.
	DeferredBoundaryRows func(f BoundaryStakeFixture) []*models.PoolStakeSnapshot
	// HideStoreReads makes the named LedgerDB reads fail against the
	// metadata store from now on, and returns the count of reads that
	// reached it and a switch for the hiding. A read the live UTxO set
	// serves never reaches the store.
	HideStoreReads func(names ...string) (reached func() int64, hide func(on bool))
	// LedgerStateSetOnly turns the set-only switch on or off and, when on,
	// also drains every family: the metadata store then holds no copy of
	// the ledger state the set computes.
	LedgerStateSetOnly func(on bool)
	// SetOnlySwitch turns the set-only switch on or off without draining
	// any family.
	SetOnlySwitch func(on bool)
	// DrainFamilies adds mask to the families the ledger treats as drained.
	DrainFamilies func(mask nodesettings.FamilyMask)
	// InstallDrainedSources gives the database the sources its reads of a
	// drained family come from, which a ledger built for set reads installs
	// when it is created.
	InstallDrainedSources func()
	// ComputedResults reports the verify-mode results of kind result
	// (epoch, epoch_summary, pparam_enactment) the set computed and how
	// many of them differed from the metadata store's.
	ComputedResults func(result string) (verified, mismatches float64)
	// CorruptEpochWrites makes the metadata store keep a different nonce
	// than each epoch write gives it.
	CorruptEpochWrites func()
	// EpochStateReads reports the verify-mode epoch state reads and how
	// many of them differed from the metadata store.
	EpochStateReads func() (verified, mismatches float64)
	// EpochSummaryReads is EpochStateReads for epoch summary and total
	// active stake reads.
	EpochSummaryReads func() (verified, mismatches float64)
	// SummaryInsideRollback reads epoch's summary and mark total active
	// stake in an abandoned write transaction that stages the rollback.
	SummaryInsideRollback func(epoch uint64) (*models.EpochSummary, uint64)
	// SaveSummariesInsideTxn saves epoch's summary once for each readiness
	// in one abandoned write transaction, and returns the summary the
	// ledger reads back from it.
	SaveSummariesInsideTxn func(epoch uint64, ready ...bool) *models.EpochSummary
	// SummaryInsideTxn saves summary in an abandoned write transaction and
	// reads it and its epoch's mark total active stake back inside it.
	SummaryInsideTxn func(summary models.EpochSummary) (*models.EpochSummary, uint64)
	// NetworkStateReads is EpochStateReads for network state and
	// donation reads.
	NetworkStateReads func() (verified, mismatches float64)
	// AccountReads is EpochStateReads for account reads.
	AccountReads func() (verified, mismatches float64)
	// DrepReads is EpochStateReads for DRep reads.
	DrepReads func() (verified, mismatches float64)
	// PoolReads is EpochStateReads for pool reads.
	PoolReads func() (verified, mismatches float64)
	// RegistrationReads is EpochStateReads for registration history and
	// DRep deposit reads.
	RegistrationReads func() (verified, mismatches float64)
	// OpCertReads is EpochStateReads for op-cert counter reads.
	OpCertReads func() (verified, mismatches float64)
	// LiveStakeReads is EpochStateReads for live stake reads.
	LiveStakeReads func() (verified, mismatches float64)
	// RewardOutputReads is EpochStateReads for reward output, credited round
	// and post-snapshot credit reads.
	RewardOutputReads func() (verified, mismatches float64)
	// EpochStartSlot is the current epoch's first slot, and RollbackSlot the
	// slot RollbackBeforeBoundary rolls back to.
	EpochStartSlot func() uint64
	RollbackSlot   func() uint64
	// FoldForWithdrawal runs the ledger's fold of ref's pending reward
	// credits for a transaction withdrawing from ref, in a write transaction
	// it commits.
	FoldForWithdrawal func(ref models.StakeCredentialRef)
	// RewardViewsInsideRollback stages a rollback to RollbackSlot in an
	// abandoned write transaction, runs after in it, and returns what the
	// set's reward views give there for ref and the credits at slot.
	RewardViewsInsideRollback func(
		ref models.StakeCredentialRef,
		slot uint64,
		after func(ledgerdb.LedgerDB, ledgerdb.Txn),
	) RewardViews
	// CompactRewardCredits runs one chunk of the credit compaction job, in
	// its own write transaction, and reports whether no round was due.
	CompactRewardCredits func(limit int) bool
	// PinnedReadTxn begins a read transaction, fixes its snapshot with a
	// read and pins it for verify reads as the ratification job pins its own
	// after the boundary commits.
	PinnedReadTxn func() ledgerdb.Txn
	// AccountReadMismatches counts the account reads whose answer differed
	// from the metadata store's.
	AccountReadMismatches func() uint64
	// RelayProviderStake is the stake the pool relay provider's default
	// lookup reports for pools.
	RelayProviderStake func(pools [][]byte) (map[string]uint64, error)
	// AddSetStake adds an output of amount lovelace naming the key-hash
	// stake credential key to the live UTxO set alone, as the harness's own
	// rows hold outputs the set never saw.
	AddSetStake func(key []byte, amount uint64)
	// VoteDelegate commits a vote delegation of ref through the ledger's
	// LedgerDB, at the epoch's start slot plus offset, which the rollback
	// undoes: to the credential DRep drepKey, registered first, or to the
	// predefined DRep type when drepKey is nil. Writes must come in slot
	// order.
	VoteDelegate func(ref models.StakeCredentialRef, drepKey []byte, predefined uint64, offset uint64)
	// CreditInsideTxn credits ref in an abandoned write transaction and
	// returns the stake the LedgerDB reports for ref's pool delegation
	// inside it, through every live stake read.
	CreditInsideTxn func(ref models.StakeCredentialRef, pool []byte, amount uint64) uint64
	// RegisterInsideTxn registers a new key-hash stake credential and
	// delegates it to pool in an abandoned write transaction, and returns
	// the credentials the LedgerDB reports as the pool's live stake inside
	// it.
	RegisterInsideTxn func(key []byte, pool []byte) [][]byte
	// RegisterCredentialInsideTxn registers the stake credential of tag
	// and key and delegates it to pool in an abandoned write transaction,
	// and returns the credentials and the count the LedgerDB reports as
	// pool's active delegators inside it.
	RegisterCredentialInsideTxn func(
		tag uint8, key []byte, pool []byte,
	) ([]models.StakeCredentialRef, uint64)
	// RetireInsideTxn retires pool for epoch in an abandoned write
	// transaction and returns what the LedgerDB reports as retiring at
	// epoch, as of boundarySlot, inside it.
	RetireInsideTxn func(
		pool []byte,
		epoch, boundarySlot uint64,
	) []models.PoolRetirementRefund
	// ReapRetiredPool registers a pool and its reward account, deregisters
	// the account, retires the pool for the next epoch, and applies the
	// refunds of the pools retiring then at the next boundary as the epoch
	// boundary does, from a network state of 1000 treasury and 5000
	// reserves.
	ReapRetiredPool func() ReapOutcome
	// SetFailed reports whether the live UTxO set stopped following the
	// ledger.
	SetFailed func() bool
	// OpCerts commits op-cert counters through the ledger's LedgerDB: one
	// at the rollback target, then a raise, the same counter again, a
	// lower counter and a batched raise at slots the rollback undoes, and
	// one for a pool that is not registered.
	OpCerts func() OpCertSlots
	// AnchorOpCertCounters is the ledger's export of the counters observed
	// at exactly slot.
	AnchorOpCertCounters func(slot uint64) ([]metadata.OpCertCounter, error)
	// OpCertsInsideTxn writes a counter in an abandoned write transaction
	// and reads the pool's counter through the LedgerDB inside it.
	OpCertsInsideTxn func(pool []byte, sequence, slot uint64)
	// OpCertObservedInsideTxn writes a counter in an abandoned write
	// transaction and reports whether the LedgerDB sees a counter observed
	// at its slot inside it.
	OpCertObservedInsideTxn func(pool []byte, sequence, slot uint64) bool
	// OpCertsLater commits counters for Pool and Other at from, from+1 and
	// from+2, each higher than any before it.
	OpCertsLater func(pool, other []byte, from uint64)
	// CommitteeLater seats cold credential cold at from with quorum 3/4,
	// authorizes hot key hot at from+1, resigns the member at from+2,
	// authorizes hot2 at from+3 and changes the constitution at from+4.
	CommitteeLater func(cold, hot, hot2 []byte, from uint64)
	// CommitteeReads is EpochStateReads for committee and constitution reads.
	CommitteeReads func() (verified, mismatches float64)
	// Committee commits the committee, quorum and constitution writes and
	// the committee certificates described by CommitteeSlots, through the
	// ledger's LedgerDB.
	Committee func() CommitteeSlots
	// CommitteeInsideTxn writes a committee member, a quorum and a
	// constitution in an abandoned write transaction and reads them through
	// the LedgerDB inside it.
	CommitteeInsideTxn func(slot uint64)
	// GovernanceReads is EpochStateReads for governance proposal and vote
	// reads.
	GovernanceReads func() (verified, mismatches float64)
	// Proposals commits the proposal and vote writes described by
	// GovernanceSlots through the ledger's LedgerDB.
	Proposals func() GovernanceSlots
	// SnapshotReadsAfterLaterCommit commits a DRep, a proposal and a vote,
	// pins a read transaction, commits a DRep activity renewal, a changed
	// vote and a new vote after it, and then reads the active DReps and the
	// proposal's votes through the pinned transaction, as the deferred
	// ratification does. It returns the verify-mode counts of the DRep and
	// governance reads after the pinned reads.
	SnapshotReadsAfterLaterCommit func() SnapshotReadCounts
	// PinInsideCommit commits two DRep activity updates in one write
	// transaction and a third in another, and pins a read transaction in
	// the first transaction's AfterCommit, ahead of the callback that applies
	// its batch to the set, so the pin finds the batch committed but not
	// applied. It reads the DRep's expiry through the pin and without it.
	PinInsideCommit func() PinInsideCommitResult
	// SnapshotReadOfADivergence writes a DRep around the ledger's LedgerDB,
	// so the set never sees it, pins a read transaction after it with no
	// later commit, and reads the active DReps through that transaction.
	// It returns the verify-mode counts of the DRep reads.
	SnapshotReadOfADivergence func() SnapshotReadCounts
	// OnProposalWrite sets f to run after each committed write of Proposals
	// and the reads that follow it, with the write's number from 1.
	OnProposalWrite func(f func(step int))
	// ProposalsInsideTxn submits a proposal and a vote in an abandoned write
	// transaction and reads them through the LedgerDB inside it.
	ProposalsInsideTxn func(slot uint64)
	// Donate commits a donation through the ledger's LedgerDB, in the
	// boundary's epoch, at a slot the rollback undoes.
	Donate func(amount uint64)
	// NetworkInsideRollback reads, in an abandoned write transaction that
	// stages the rollback, the latest network state's slot and the current
	// epoch's donations.
	NetworkInsideRollback func() (slot, donations uint64)
	// Credit commits reward credits to ref through the ledger's LedgerDB at
	// a slot the rollback undoes: one credit, then a batch that repeats it
	// and adds a second source, which must apply once each.
	Credit func(ref models.StakeCredentialRef, amount uint64)
	// Withdraw commits a transaction withdrawing amount from ref's key-hash
	// reward account through the ledger's LedgerDB, at a slot the rollback
	// undoes, and returns the error the write reports.
	Withdraw func(ref models.StakeCredentialRef, amount uint64) error
	// Reap clears every delegation to pool through the ledger's LedgerDB,
	// at a slot the rollback undoes.
	Reap func(pool []byte)
	// Expire turns CIP-0163 on, runs its one-time activation stamp and then
	// renews ref's expiration, at slots the rollback undoes.
	Expire func(ref models.StakeCredentialRef)
	// ExpireAtBoundary is Expire with the activation stamp where production
	// puts it: at the first slot of the epoch it enters, which it records as
	// an epoch row, so a rollback to before that slot is always in an earlier
	// epoch than the activation marker's. It runs no other boundary rule.
	ExpireAtBoundary func(ref models.StakeCredentialRef)
	// Governance drives DRep state through the ledger's LedgerDB at slots
	// the rollback undoes: it registers a DRep and vote-delegates ref to
	// it, records DRep activity, bumps and resets the dormancy counter,
	// deregisters the DRep under protocolMajor, and then delegates ref to
	// an unregistered DRep and clears dangling delegations.
	Governance func(ref models.StakeCredentialRef, protocolMajor uint64)
	// Pools registers a new pool and re-registers it, retires the
	// harness's pool for the next epoch, then writes the next epoch's row
	// and applies its boundary to the pools, at slots the rollback undoes.
	// beforeBoundary runs between the certificates and the boundary.
	// The re-registration rotates the pool's VRF key.
	Pools func(beforeBoundary func()) PoolSlots
	// CertifiedPools writes pool certificates as Leios closure
	// transactions, each certified at a slot after the context slot it takes
	// effect at; call it after CertifyLeios. See CertifiedPoolSlots.
	// beforeBoundary runs between the last certificate of the first epoch
	// and its boundary.
	CertifiedPools func(beforeBoundary func()) CertifiedPoolSlots
	// Registrations commits the stake registration and DRep certificates
	// described by RegistrationSlots through the ledger's LedgerDB, at
	// slots the rollback undoes.
	Registrations func() RegistrationSlots
	// CertifiedStake writes stake registrations, a deregistration and a zero
	// withdrawal as Leios closure transactions, each certified at a slot
	// after the context slot it takes effect at; call it after CertifyLeios.
	// See CertifiedStakeSlots.
	CertifiedStake func() CertifiedStakeSlots
	// CertifiedUtxos writes staked outputs as Leios closure transactions
	// and ordinary ones, each closure certified at a slot after the context
	// slot it takes effect at; call it after CertifyLeios. See
	// CertifiedUtxoSlots.
	CertifiedUtxos func() CertifiedUtxoSlots
	// CertifiedDelegations delegates stake credentials to pools in Leios
	// closure transactions certified after the context slot they take effect
	// at, and in an ordinary transaction; call it after CertifyLeios. See
	// CertifiedDelegationSlots.
	CertifiedDelegations func() CertifiedDelegationSlots
	// CertifiedBoundaryStake writes credentials that register, delegate,
	// deregister, withdraw and hold outputs through Leios closures applied
	// after the context slot they take effect at, each transaction through
	// the ledger's delta application; call it after CertifyLeios. See
	// CertifiedBoundaryStakeSlots.
	CertifiedBoundaryStake func(withdrawAcrossSnapshot bool) CertifiedBoundaryStakeSlots
	// CertifiedBoundaryStakeWith is CertifiedBoundaryStake with the scenarios
	// CertifiedBoundaryOptions selects.
	CertifiedBoundaryStakeWith func(CertifiedBoundaryOptions) CertifiedBoundaryStakeSlots
	// Witnesses commits the certificates and withdrawals described by
	// WitnessSlots, in slot order, through the ledger's LedgerDB, or around
	// it, straight to the metadata store, when around is set.
	Witnesses func(around bool) WitnessSlots
	// WitnessesInsideTxn registers a new credential in an abandoned write
	// transaction at slot and returns the greatest witness slot of it at or
	// below slot the LedgerDB reports inside it.
	WitnessesInsideTxn func(key []byte, slot uint64) (uint64, bool)
	// WitnessesInsideRollback returns, from an abandoned write transaction
	// that stages the rollback, the greatest witness slot of each of refs the
	// LedgerDB reports inside it.
	WitnessesInsideRollback func(refs []models.StakeCredentialRef) map[string]uint64
	// RegistrationsInsideTxn registers a new credential and DRep in an
	// abandoned write transaction and returns the registration history rows
	// and DRep deposit the LedgerDB reports inside it.
	RegistrationsInsideTxn func(slot uint64) (rows []models.AccountRegistrationHistoryRow, deposit *uint64)
	// GenesisDrep runs the ledger's genesis governance write for a DRep
	// registered at slot 0 with deposit, which has no certificate.
	GenesisDrep func(cred []byte, deposit uint64)
	// RegistrationsInsideRollback returns, from an abandoned write
	// transaction that stages the rollback, the set's view of key's
	// registration events and of drep's deposit.
	RegistrationsInsideRollback func(key, drep []byte) (events []StakeEvent, deposit *uint64)
	// StagedRollbackAccountReads renders the account list reads of pool and of
	// the credential DRep drep inside an abandoned write transaction that
	// stages the rollback.
	StagedRollbackAccountReads func(pool, drep []byte) []string
	// PoolKeysInsideRollback returns the pools the boundary's pool walk
	// takes as candidates inside an abandoned write transaction that stages
	// the rollback.
	PoolKeysInsideRollback func() [][]byte
	// CommittedAccountReads renders the same reads outside any transaction.
	CommittedAccountReads func(pool, drep []byte) []string
	// RegistrationCredentialsInsideTxn registers a new credential in an
	// abandoned write transaction at slot and returns the credentials with a
	// registration event in [from, to] the LedgerDB reports inside it.
	RegistrationCredentialsInsideTxn func(slot, from, to uint64) []models.StakeCredentialRef
	// RegistrationCredentialsInsideRollback returns the credentials with a
	// registration event in [from, to] the LedgerDB reports inside an
	// abandoned write transaction that stages the rollback.
	RegistrationCredentialsInsideRollback func(from, to uint64) []models.StakeCredentialRef
	// CertHistoryReads is EpochStateReads for genesis delegation and MIR
	// certificate reads.
	CertHistoryReads func() (verified, mismatches float64)
	// GenesisMIR commits Babbage transactions carrying genesis key
	// delegation and MIR certificates described by GenesisMIRSlots, through
	// the ledger's LedgerDB, at slots the rollback undoes, and then applies
	// the epoch boundary that settles the MIR certificates. beforeBoundary
	// runs between the certificates and the boundary.
	GenesisMIR func(beforeBoundary func()) GenesisMIRSlots
	// CertHistoryInsideRollback reads, in an abandoned write transaction
	// that stages the rollback, the genesis key delegations and the MIR
	// certificates.
	CertHistoryInsideRollback func() (delegations, mirCerts int)
	// BoundaryStake commits, at the slots of the epoch before the boundary,
	// the registrations, delegations, pointer-address outputs, reward
	// credits and pool retirement described by BoundaryStakeFixture,
	// through the ledger's LedgerDB, and the same outputs to the metadata
	// store's utxo rows. retireEpoch, when non-zero, retires the
	// harness's pool for that epoch.
	BoundaryStake func(retireEpoch uint64) BoundaryStakeFixture
	// AfterBoundaryStake commits state after the fixture's snapshot slot, an
	// account registered and delegated and stake at a base address, and
	// moves the live UTxO set's tip past it, so a read of the fixture's
	// boundary is of a past one.
	AfterBoundaryStake func(f BoundaryStakeFixture)
	// WithdrawAtSlot commits a withdrawal of amount from ref in a block at
	// slot, through the ledger's LedgerDB.
	WithdrawAtSlot func(ref models.StakeCredentialRef, slot, amount uint64)
	// CertsAtSlot commits certificates in a block at slot, as transaction
	// idx, through the ledger's LedgerDB.
	CertsAtSlot func(slot uint64, idx uint32, seed byte, deposits map[int]uint64, certs ...[]any)
	// DijkstraBoundary makes the epoch starting at the fixture's boundary a
	// Dijkstra one, whose SNAP follows the boundary's enactment.
	DijkstraBoundary func(f BoundaryStakeFixture)
	// ReadBoundaryStake makes the five epoch-boundary stake reads for the
	// fixture's boundary, through the metadata store when viaStore is set
	// and through the ledger's LedgerDB otherwise.
	ReadBoundaryStake func(f BoundaryStakeFixture, viaStore bool) BoundaryStakeAnswers
}

// BoundaryStakeFixture names what BoundaryEpochStateScenario.BoundaryStake
// wrote. Pointed registers at (10, 0, 0) and delegates to Pool; Dereg
// registers at (10, 1, 0), delegates, deregisters at slot 11 and registers and
// delegates again after it, so the pointer naming its first registration
// dangles while the account counts. Outputs hold Pointed 4000000 at a base
// address and 7000000 at a pointer address naming its registration, Dereg
// 5000000 at a base address and 3000000 at a pointer address naming its first
// registration, 2000000 at a pointer address naming no certificate and
// 9000000 at an enterprise address. Gone registers and delegates at slot 10
// and deregisters at slot 11, with 6000000 at a base address; AfterBoundaryStake
// registers and delegates it again. Pointed has a reward
// balance of 13, pending credits of 17 and a credit of 6 applied after the
// snapshot. Slot is the boundary's snapshot slot.
type BoundaryStakeFixture struct {
	Pool, Idle                             []byte
	Pointed, Dereg, Late, Gone             models.StakeCredentialRef
	Slot, Boundary                         uint64
	PointedBase, PointedPointer, PointedRw uint64
}

// BoundaryStakeAnswers are the answers of the five epoch-boundary stake reads
// for the fixture's pool and an idle pool.
type BoundaryStakeAnswers struct {
	Active, Delegated  [][]byte
	Stakes, Delegators map[string]uint64
	Inputs, Pointers   []*models.RewardStakeInput
}

// GenesisMIRSlots names the keys and slots BoundaryEpochStateScenario.GenesisMIR
// wrote, which are the two slots of the epoch before the boundary. The
// genesis key Genesis delegates to DelegateA with VrfA at DelegA, the epoch's
// first slot, which the rollback keeps. The rest are in the second slot,
// which it undoes: transaction 0 moves ReservesToOwner from reserves to Owner,
// ReservesToDelegator from reserves to Delegator and TreasuryToOwner from the
// treasury to Owner, in that certificate order; transaction 1 moves
// ToTreasuryAmount from reserves to the treasury; transaction 2 moves
// ToReservesAmount from the treasury to reserves; and transaction 3 delegates
// Genesis to DelegateB with VrfB. The boundary into NextEpoch is at Boundary.
type GenesisMIRSlots struct {
	Genesis, DelegateA, DelegateB, VrfA, VrfB, Owner, Delegator []byte
	DelegA, DelegB, Distribute, ToTreasury, ToReserves          uint64
	EpochStart, Boundary, NextEpoch                             uint64
	ReservesToOwner, ReservesToDelegator, TreasuryToOwner       uint64
	ToTreasuryAmount, ToReservesAmount                          uint64
}

// RegistrationSlots names the credentials and slots
// BoundaryEpochStateScenario.Registrations wrote. K and L register with a
// stake registration certificate at S1, K with deposit 2000000 and L with
// none known, and M with a conway registration of 1000000. K deregisters
// with a stake deregistration at S2, registers again with a conway
// registration of 3000000 at S3, deregisters with a refund of 3000000 at S4
// and registers at S5 with 4000000. D and E register at DRegister with
// 500000000 and 700000000, D updates at DUpdate, both retire at DRetire,
// and D registers again at DReRegister with 600000000. Imported is an
// account the harness imported with no certificate.
type RegistrationSlots struct {
	K, L, M, D, E                       []byte
	Imported                            models.StakeCredentialRef
	S1, S2, S3, S4, S5                  uint64
	DRegister, DUpdate, DRetire, DReReg uint64
}

// WitnessSlots names the credentials and slots
// BoundaryEpochStateScenario.Witnesses wrote. Payee delegates to Pool at
// Kept, the rollback target. C registers at Register, delegates to Pool at
// Delegate, vote-delegates to the predefined abstain DRep at VoteDelegate and
// deregisters at Deregister. Payee is credited at Credit, withdraws a
// positive amount at Withdraw, withdraws nothing at ZeroSkipped with the
// withdrawal witness write elided, and withdraws nothing at ZeroRecorded with
// it written.
type WitnessSlots struct {
	Payee, C                                    models.StakeCredentialRef
	Pool                                        []byte
	Kept, Register, Delegate, VoteDelegate      uint64
	Credit, Withdraw, ZeroSkipped, ZeroRecorded uint64
	Deregister                                  uint64
}

// renderAccountReads renders the reads that list accounts by pool and DRep.
func renderAccountReads(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
	pool, drep []byte,
) []string {
	t.Helper()
	pools := []lcommon.PoolKeyHash{lcommon.PoolKeyHash(lcommon.NewBlake2b224(pool))}
	creds, err := ldb.GetActiveDelegatorCredentialsByPool(pools, txn)
	require.NoError(t, err)
	var keys []string
	for _, c := range creds[string(pool)] {
		keys = append(keys, c.MapKey())
	}
	slices.Sort(keys)
	counts, err := ldb.GetActiveDelegatorCountsByPool(pools, txn)
	require.NoError(t, err)
	delegated, err := ldb.GetDelegatedPoolKeyHashes(txn)
	require.NoError(t, err)
	var dp []string
	for _, p := range delegated {
		dp = append(dp, string(p))
	}
	slices.Sort(dp)
	stake, delegators, err := ldb.GetStakeByPools(
		t.Context(), [][]byte{pool}, txn,
	)
	require.NoError(t, err)
	dd, err := ldb.GetDRepDelegators(t.Context(), 0, drep, txn)
	require.NoError(t, err)
	return []string{
		fmt.Sprint("creds ", keys),
		fmt.Sprint("count ", counts[string(pool)]),
		fmt.Sprintf("delegated %x", dp),
		fmt.Sprint("stake ", stake[string(pool)], " ", delegators[string(pool)]),
		fmt.Sprint("drep delegators ", len(dd)),
	}
}

// submitCertificates commits a transaction of certs through ldb at slot,
// with deposits as its computed certificate deposits.
func submitCertificates(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	slot uint64,
	seed byte,
	deposits map[int]uint64,
	certs ...[]any,
) {
	t.Helper()
	txn := ldb.Begin(t.Context(), true)
	defer txn.Release()
	writeCertificates(t, ldb, txn, slot, seed, deposits, certs...)
	require.NoError(t, txn.Commit())
}

func writeCertificates(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
	slot uint64,
	seed byte,
	deposits map[int]uint64,
	certs ...[]any,
) {
	t.Helper()
	body := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)}},
		1: []any{},
		2: uint64(100_000),
		4: certs,
	})
	tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
	require.NoError(t, err)
	var txHash [32]byte
	copy(txHash[:], tx.Hash().Bytes())
	require.NoError(t, ldb.SetTransactionWithOpts(
		t.Context(), tx,
		ocommon.NewPoint(slot, bytes.Repeat([]byte{seed}, 32)),
		0, 0, nil, deposits,
		&database.BlockIngestionResult{
			TxOffsets: map[[32]byte]database.CborOffset{txHash: {}},
		},
		txn, database.BatchedTxIngestOpts{ProtocolMajor: 10},
	))
}

// ReapOutcome is what BoundaryEpochStateScenario.ReapRetiredPool observed.
// Refund is the reaped pool's refund, Registered whether its reward account
// was registered before the harness deregistered it, Network the rendered
// network state after the reap and Reward the account's balance after it.
// SetFailed reports whether the live UTxO set stopped following the ledger.
type ReapOutcome struct {
	Refund     models.PoolRetirementRefund
	Registered bool
	Network    []string
	Reward     uint64
	SetFailed  bool
}

// PoolSlots names the pool and slots BoundaryEpochStateScenario.Pools wrote.
// Pool registers at Register with VrfA and re-registers at Rotate with VrfB;
// the harness's pool Retired retires at Retire for epoch RetireEpoch, and the
// boundary into NextEpoch is at Boundary.
type PoolSlots struct {
	Pool, Retired                         []byte
	VrfA, VrfB                            []byte
	Register, Rotate, Retire, RetireEpoch uint64
	EpochStart, Boundary, NextEpoch       uint64
}

// CertifiedPoolSlots names the pools and slots
// BoundaryEpochStateScenario.CertifiedPools wrote. A certificate is applied at
// its certifying slot and takes effect at its context slot.
//
// Pool registers at RegA (context CtxA), then re-registers at RegB with VrfB
// at a context slot CtxB before CtxA, so the effective order is the reverse of
// the apply order. The harness's pool Retired retires for epoch RetireEpoch at
// RetP (context CtxRetP). The boundary into NextEpoch is at Boundary. Pool
// then retires for NextEpoch+1 at RetN, applied in the next epoch with a
// context slot CtxRetN inside the epoch before the boundary.
type CertifiedPoolSlots struct {
	Pool, Retired                   []byte
	VrfA, VrfB                      []byte
	RegA, CtxA, RegB, CtxB          uint64
	RetP, CtxRetP, RetN, CtxRetN    uint64
	RetireEpoch                     uint64
	EpochStart, Boundary, NextEpoch uint64
}

// CertifiedStakeSlots names the credentials and slots
// BoundaryEpochStateScenario.CertifiedStake wrote. A certificate is applied at
// its certifying slot and takes effect at its context slot.
//
// C registers at Reg1 (context Ctx1), deregisters at Dereg (CtxDereg) and
// registers again at Reg2 (Ctx2), where Ctx1 < Ctx2 < CtxDereg, so the
// effective order ends on the deregistration while the apply order ends on
// the registration. Reg2 is applied after Boundary while Ctx2 lies before it.
// D registers in an ordinary transaction at Plain. Payee is withdrawn from
// with a zero amount at Withdraw, effective at CtxWithdraw.
type CertifiedStakeSlots struct {
	C, D, Payee                             models.StakeCredentialRef
	Reg1, Ctx1, Dereg, CtxDereg, Reg2, Ctx2 uint64
	Plain, Withdraw, CtxWithdraw, Boundary  uint64
}

// CertifiedUtxoSlots names the credentials, outputs and slots
// BoundaryEpochStateScenario.CertifiedUtxos wrote. Closure C1, applied at A1
// and effective at E1, creates OA1 (credential A) and OB1 (credential B).
// Ordinary transaction C3, applied at A3, creates OB2. Closure C2 (A2, E2)
// spends OA1 and creates OA2; closure C4 (A4, E4, with E4 before E2) spends OB1
// and creates OA3. Every slot is between Start and End, and Boundary lies
// between the effective and the applied slot of C2.
type CertifiedUtxoSlots struct {
	A, B                       models.StakeCredentialRef
	OA1, OB1, OB2, OA2, OA3    models.UtxoId
	A1, E1, A3, A2, E2, A4, E4 uint64
	Start, Boundary, End       uint64
}

// CertifiedDelegationSlots names the credentials, pools and slots
// BoundaryEpochStateScenario.CertifiedDelegations wrote. X registers at Reg,
// then delegates to PoolA in a closure applied at D1 and effective at E1, and
// to PoolB in a closure applied later, at D2, but effective at E2 before E1, so
// the effective order is the reverse of the apply order. D2 is applied after
// Boundary while E2 lies before it. Y delegates to PoolA in an ordinary
// transaction at Plain.
type CertifiedDelegationSlots struct {
	X, Y                                 models.StakeCredentialRef
	PoolA, PoolB                         []byte
	Reg, D1, E1, D2, E2, Plain, Boundary uint64
}

// RewardViews are the answers of the set's reward views inside a
// transaction.
type RewardViews struct {
	Pending    []models.RewardAccountOutput
	HasPending bool
	Rounds     []models.RewardCreditRound
	Credits    []models.AccountRewardDelta
}

// GovernanceSlots names the proposals and slots
// BoundaryEpochStateScenario.Proposals wrote, in Epoch. P1 is submitted at
// Base and P2 at Submit. D1 votes on P1 at Vote and again at Revote; D2 votes
// at Vote and has its votes deleted at Deregister, then votes again at
// Revote2. P1 is ratified at Ratify, has its ratification cleared at
// ClearRatify, has its action CBOR translated at Translate, and is ratified
// at RatifyAgain and enacted at Enact in one write. P2 expires at Expire and
// is dropped at Drop.
type SnapshotReadCounts struct {
	DrepVerified, DrepMismatches             float64
	GovernanceVerified, GovernanceMismatches float64
}

// PinInsideCommitResult is the DRep expiry epoch the pinned transaction reads
// and the latest one, with the epoch the activity updates start from.
type PinInsideCommitResult struct {
	Epoch, Pinned, Latest uint64
}

type GovernanceSlots struct {
	P1, P2, D1, D2 []byte
	Epoch          uint64
	Base, Submit, Vote, Revote, Ratify, ClearRatify, Deregister,
	Revote2, Expire, Translate, Drop, RatifyAgain, Enact uint64
}

// CommitteeSlots names the credentials and slots
// BoundaryEpochStateScenario.Committee wrote. At Base, members A and B are
// seated with quorum 1/2 and a constitution. A and B authorize hot keys at
// Auth, B resigns at Resign and A authorizes another hot key at Reauth. At
// Update B is removed, C is seated with quorum 2/3; a constitution follows
// at Constitution, and at NoConfidence every member is removed and the
// quorum cleared.
type CommitteeSlots struct {
	A, B, C                                                        []byte
	Base, Auth, Resign, Reauth, Update, Constitution, NoConfidence uint64
}

// OpCertSlots names the pools and slots BoundaryEpochStateScenario.OpCerts
// wrote. Pool has counter 3 at Base, 5 at Raised, 5 again at Repeated, 4 at
// Lowered and 6 at Batched; Other, which is not registered, has 2 at
// Repeated.
type OpCertSlots struct {
	Pool, Other                              []byte
	Base, Raised, Repeated, Lowered, Batched uint64
}

// NewBoundaryEpochStateScenarioForTest builds the boundary replay harness
// with set attached.
func NewBoundaryEpochStateScenarioForTest(
	t *testing.T,
	set UtxoSetWriter,
	reads string,
) BoundaryEpochStateScenario {
	t.Helper()
	return NewGuardedBoundaryEpochStateScenarioForTest(t, set, reads, nil)
}

// NewGuardedBoundaryEpochStateScenarioForTest is
// NewBoundaryEpochStateScenarioForTest over a metadata store whose
// statements pass through guard.
func NewGuardedBoundaryEpochStateScenarioForTest(
	t *testing.T,
	set UtxoSetWriter,
	reads string,
	guard metadata.StatementGuard,
) BoundaryEpochStateScenario {
	t.Helper()
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config:         &database.Config{DataDir: t.TempDir()},
		StatementGuard: guard,
	})
	require.NoError(t, err)
	h := newBoundaryReplayHarnessWithSet(t, db, set)
	h.ls.config.UtxoSetReads = reads
	// A treasury and reserves baseline, and a donation in the epoch before
	// the boundary, for the boundary's pots to start from.
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 9_000, 0, nil))
	require.NoError(t, db.Metadata().AddNetworkDonation(
		h.fixture.ancestorTip.Point.Slot, h.ls.currentEpoch.EpochId, 25, nil,
	))
	// The harness creates its accounts with no certificate history; an
	// import baseline gives a rollback the delegation to restore, as a
	// Mithril import's accounts have.
	require.NoError(
		t,
		db.Metadata().IterateAccounts(nil, func(a *models.Account) error {
			c := *a
			return db.Metadata().ImportAccount(&c, nil)
		}),
	)
	var setOutputs byte
	var onProposalWrite func(step int)
	proposalWrites := 0
	return BoundaryEpochStateScenario{
		DB:                 db,
		UnservedFactTables: h.ls.UnservedFactTables,
		AnchorState:        h.ls,
		ReadFactRows: func(
			table metadata.FactTable, from, through uint64, cursor int64,
			limit int,
		) (metadata.FactRows, int64, error) {
			return h.ls.ReadFactRows(
				t.Context(), table, from, through, cursor, limit, nil,
			)
		},
		OnProposalWrite:    func(f func(int)) { onProposalWrite = f },
		ApplyBoundaryQuiet: func() { h.applyBoundaryQuiet() },
		WriteNonces: func(rows []models.BlockNonce) {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			require.NoError(t, ldb.SetBlockNonces(rows, txn))
			h.ls.stageBlockNonces(txn, rows, 0)
			require.NoError(t, txn.Commit())
		},
		StageNoncesInSet: func(rows []models.BlockNonce) {
			h.ls.stageBlockNonces(nil, rows, 0)
		},
		PruneNonces: func(slot uint64) { h.ls.cleanupBlockNoncesBefore(t.Context(), slot) },
		ReadNonces: func() []string {
			ldb := h.ls.LedgerDB()
			var out []string
			rows, err := ldb.GetBlockNoncesInSlotRange(0, math.MaxInt64, nil)
			require.NoError(t, err)
			for _, n := range rows {
				out = append(
					out,
					fmt.Sprintf(
						"nonce %d %x %x %t",
						n.Slot,
						n.Hash,
						n.Nonce,
						n.IsCheckpoint,
					),
				)
				one, err := ldb.GetBlockNonce(
					ocommon.NewPoint(n.Slot, n.Hash),
					nil,
				)
				require.NoError(t, err)
				out = append(out, fmt.Sprintf("at %d %x", n.Slot, one))
			}
			latest, ok, err := ldb.GetLatestBlockNonce(nil)
			require.NoError(t, err)
			out = append(out, fmt.Sprintf("latest %t %d", ok, latest.Slot))
			return out
		},
		ReadNetwork: func() []string {
			ldb := h.ls.LedgerDB()
			var out []string
			state, err := ldb.GetNetworkState(nil)
			require.NoError(t, err)
			if state != nil {
				out = append(
					out,
					fmt.Sprintf(
						"network %d %d %d",
						state.Slot,
						state.Treasury,
						state.Reserves,
					),
				)
			}
			donations, err := ldb.ListNetworkDonations(nil)
			require.NoError(t, err)
			for _, d := range donations {
				out = append(
					out,
					fmt.Sprintf("donation %d %d %d", d.Slot, d.Epoch, d.Amount),
				)
				sum, err := ldb.SumNetworkDonationsForEpoch(d.Epoch, nil)
				require.NoError(t, err)
				out = append(out, fmt.Sprintf("sum %d %d", d.Epoch, sum))
			}
			return out
		},
		SetNetworkState: func(treasury, reserves, slot uint64) {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			require.NoError(
				t,
				ldb.SetNetworkState(treasury, reserves, slot, txn),
			)
			require.NoError(t, txn.Commit())
		},
		ReadEpochs: func() []string { return renderEpochRows(t, h.ls.LedgerDB()) },
		SaveSummary: func(summary models.EpochSummary) {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			require.NoError(t, ldb.SaveEpochSummary(&summary, txn))
			require.NoError(t, txn.Commit())
		},
		AncestorPoint: func() ocommon.Point { return h.fixture.ancestorTip.Point },
		CurrentPoint:  func() ocommon.Point { return h.fixture.currentTip.Point },
		BoundaryStake: func(retireEpoch uint64) BoundaryStakeFixture {
			ldb := h.ls.LedgerDB()
			store := dbadapter.New(h.ls.db)
			cur := h.ls.currentEpoch
			f := BoundaryStakeFixture{
				Pool: h.poolHash, Idle: bytes.Repeat([]byte{0xee}, 28),
				Pointed: models.NewStakeCredentialRef(
					0,
					bytes.Repeat([]byte{0x74}, 28),
				),
				Dereg: models.NewStakeCredentialRef(
					0,
					bytes.Repeat([]byte{0x75}, 28),
				),
				Late: models.NewStakeCredentialRef(
					0,
					bytes.Repeat([]byte{0x76}, 28),
				),
				Gone: models.NewStakeCredentialRef(
					0,
					bytes.Repeat([]byte{0x77}, 28),
				),
				Slot:           cur.StartSlot + uint64(cur.LengthInSlots) - 1,
				Boundary:       cur.StartSlot + uint64(cur.LengthInSlots),
				PointedBase:    4_000_000,
				PointedPointer: 7_000_000,
				PointedRw:      13 + 17,
			}
			require.Equal(t, uint64(10), cur.StartSlot)
			cred := func(r models.StakeCredentialRef) []any { return []any{uint64(r.Tag), r.Key} }
			submit := func(slot uint64, idx uint32, seed byte, deposits map[int]uint64, certs ...[]any) {
				body := newRowBatchingTx(t, map[uint]any{
					0: []any{[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)}},
					1: []any{},
					2: uint64(100_000),
					4: certs,
				})
				tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
				require.NoError(t, err)
				var txHash [32]byte
				copy(txHash[:], tx.Hash().Bytes())
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				require.NoError(t, ldb.SetTransactionWithOpts(
					t.Context(), tx,
					ocommon.NewPoint(slot, bytes.Repeat([]byte{seed}, 32)),
					idx, 0, nil, deposits,
					&database.BlockIngestionResult{
						TxOffsets: map[[32]byte]database.CborOffset{txHash: {}},
					},
					txn, database.BatchedTxIngestOpts{ProtocolMajor: 10},
				))
				require.NoError(t, txn.Commit())
			}
			deposit := map[int]uint64{0: 2_000_000}
			submit(10, 0, 0xb1, deposit,
				[]any{uint64(0), cred(f.Pointed)},
				[]any{uint64(2), cred(f.Pointed), h.poolHash})
			submit(10, 1, 0xb2, deposit,
				[]any{uint64(0), cred(f.Dereg)},
				[]any{uint64(2), cred(f.Dereg), h.poolHash})
			submit(10, 3, 0xb7, deposit,
				[]any{uint64(0), cred(f.Gone)},
				[]any{uint64(2), cred(f.Gone), h.poolHash})
			submit(11, 0, 0xb3, nil, []any{uint64(1), cred(f.Dereg)})
			submit(11, 3, 0xb8, nil, []any{uint64(1), cred(f.Gone)})
			submit(11, 2, 0xb6, deposit,
				[]any{uint64(0), cred(f.Dereg)},
				[]any{uint64(2), cred(f.Dereg), h.poolHash})
			if retireEpoch > 0 {
				submit(
					11,
					1,
					0xb4,
					nil,
					[]any{uint64(4), h.poolHash, retireEpoch},
				)
			}

			varint := func(n uint64) []byte {
				out := []byte{byte(n & 0x7f)}
				for n >>= 7; n > 0; n >>= 7 {
					out = append([]byte{byte(n&0x7f) | 0x80}, out...)
				}
				return out
			}
			payment := bytes.Repeat([]byte{0xaa}, 28)
			pointerAddr := func(slot, tx, cert uint64) []byte {
				raw := append([]byte{0x40}, payment...)
				raw = append(raw, varint(slot)...)
				raw = append(raw, varint(tx)...)
				return append(raw, varint(cert)...)
			}
			baseAddr := func(r models.StakeCredentialRef) []byte {
				addr, err := lcommon.NewAddressFromParts(
					lcommon.AddressTypeKeyKey, lcommon.AddressNetworkTestnet,
					payment, r.Key,
				)
				require.NoError(t, err)
				raw, err := addr.Bytes()
				require.NoError(t, err)
				return raw
			}
			enterprise := append([]byte{0x60}, payment...)
			type output struct {
				addr    []byte
				amount  uint64
				staking []byte
				pointer *models.UtxoPointer
			}
			outputs := []output{
				{baseAddr(f.Pointed), f.PointedBase, f.Pointed.Key, nil},
				{
					pointerAddr(10, 0, 0),
					f.PointedPointer,
					nil,
					&models.UtxoPointer{Slot: 10},
				},
				{baseAddr(f.Dereg), 5_000_000, f.Dereg.Key, nil},
				{baseAddr(f.Gone), 6_000_000, f.Gone.Key, nil},
				{
					pointerAddr(10, 1, 0),
					3_000_000,
					nil,
					&models.UtxoPointer{Slot: 10, TxIndex: 1},
				},
				{
					pointerAddr(10, 5, 3),
					2_000_000,
					nil,
					&models.UtxoPointer{Slot: 10, TxIndex: 5, CertIndex: 3},
				},
				{enterprise, 9_000_000, nil, nil},
			}
			var added []UtxoOutput
			for i, o := range outputs {
				id := bytes.Repeat([]byte{0xc0 + byte(i)}, 32)
				out, err := cbor.Encode(map[uint]any{0: o.addr, 1: o.amount})
				require.NoError(t, err)
				added = append(
					added,
					UtxoOutput{Ref: models.UtxoId{Hash: id}, Cbor: out},
				)
				require.NoError(
					t,
					h.ls.db.CreateUtxo(t.Context(), nil, &models.Utxo{
						TxId: id, OutputIdx: 0, StakingKey: o.staking,
						Amount: types.Uint64(
							o.amount,
						), AddedSlot: 11, Pointer: o.pointer,
					}),
				)
			}
			point := ocommon.NewPoint(11, bytes.Repeat([]byte{0xcf}, 32))
			require.NoError(t, set.ApplyUtxoChanges(&point, []UtxoEvent{{
				Point: point, Added: added,
			}}))

			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			require.NoError(t, ldb.AddAccountRewardByCredential(
				t.Context(),
				f.Pointed.Tag,
				f.Pointed.Key,
				13,
				11,
				[]byte{0x31},
				txn,
			))
			require.NoError(
				t,
				ldb.SaveRewardAccountOutputs([]*models.RewardAccountOutput{{
					Epoch: 3, CredentialTag: f.Pointed.Tag, StakingKey: f.Pointed.Key,
					PoolKeyHash: h.poolHash, RewardType: "member", Amount: 17,
					Spendable: true, CapturedSlot: 11, BoundarySlot: 11,
				}}, txn),
			)
			require.NoError(t, ldb.AddAppliedRewardCreditRound(
				models.RewardCreditRound{
					SnapshotEpoch: 3,
					BoundarySlot:  11,
				},
				txn,
			))
			require.NoError(t, ldb.AddPostSnapshotAccountRewardByCredential(
				t.Context(), f.Pointed.Tag, f.Pointed.Key, 6, f.Boundary,
				[]byte{0x32}, txn,
			))
			require.NoError(t, txn.Commit())
			_ = store
			return f
		},
		AfterBoundaryStake: func(f BoundaryStakeFixture) {
			ldb := h.ls.LedgerDB()
			late := f.Boundary + 1
			cred := []any{uint64(f.Late.Tag), f.Late.Key}
			body := newRowBatchingTx(t, map[uint]any{
				0: []any{[]any{bytes.Repeat([]byte{0xb5}, 32), uint64(0)}},
				1: []any{},
				2: uint64(100_000),
				4: [][]any{
					{uint64(0), cred}, {uint64(2), cred, h.poolHash},
					{uint64(0), []any{uint64(f.Gone.Tag), f.Gone.Key}},
					{
						uint64(2),
						[]any{uint64(f.Gone.Tag), f.Gone.Key},
						h.poolHash,
					},
				},
			})
			tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
			require.NoError(t, err)
			var txHash [32]byte
			copy(txHash[:], tx.Hash().Bytes())
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			require.NoError(t, ldb.SetTransactionWithOpts(
				t.Context(), tx,
				ocommon.NewPoint(late, bytes.Repeat([]byte{0xb5}, 32)),
				0, 0, nil, map[int]uint64{0: 2_000_000, 2: 2_000_000},
				&database.BlockIngestionResult{
					TxOffsets: map[[32]byte]database.CborOffset{txHash: {}},
				},
				txn, database.BatchedTxIngestOpts{ProtocolMajor: 10},
			))
			require.NoError(t, ldb.AddAccountRewardByCredential(
				t.Context(),
				f.Pointed.Tag,
				f.Pointed.Key,
				1_000,
				late,
				[]byte{0x33},
				txn,
			))
			require.NoError(t, txn.Commit())
			addr, err := lcommon.NewAddressFromParts(
				lcommon.AddressTypeKeyKey, lcommon.AddressNetworkTestnet,
				bytes.Repeat([]byte{0xaa}, 28), f.Pointed.Key,
			)
			require.NoError(t, err)
			raw, err := addr.Bytes()
			require.NoError(t, err)
			id := bytes.Repeat([]byte{0xd0}, 32)
			out, err := cbor.Encode(map[uint]any{0: raw, 1: uint64(1_000_000)})
			require.NoError(t, err)
			require.NoError(
				t,
				h.ls.db.CreateUtxo(t.Context(), nil, &models.Utxo{
					TxId: id, OutputIdx: 0, StakingKey: f.Pointed.Key,
					Amount: 1_000_000, AddedSlot: late,
				}),
			)
			point := ocommon.NewPoint(late+1, bytes.Repeat([]byte{0xd1}, 32))
			require.NoError(t, set.ApplyUtxoChanges(&point, []UtxoEvent{{
				Point: point,
				Added: []UtxoOutput{{Ref: models.UtxoId{Hash: id}, Cbor: out}},
			}}))
		},
		WithdrawAtSlot: func(ref models.StakeCredentialRef, slot, amount uint64) {
			body := newRowBatchingTx(t, map[uint]any{
				0: []any{[]any{bytes.Repeat([]byte{0x7a}, 32), uint64(0)}},
				1: []any{},
				2: uint64(100_000),
				5: map[cbor.ByteString]uint64{
					cbor.NewByteString(append([]byte{0xe0}, ref.Key...)): amount,
				},
			})
			tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
			require.NoError(t, err)
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			var txHash [32]byte
			copy(txHash[:], tx.Hash().Bytes())
			require.NoError(t, ldb.SetTransactionWithOpts(
				t.Context(), tx,
				ocommon.NewPoint(slot, bytes.Repeat([]byte{0x7b}, 32)),
				0, 0, nil, nil, &database.BlockIngestionResult{
					TxOffsets: map[[32]byte]database.CborOffset{txHash: {}},
				}, txn, database.BatchedTxIngestOpts{ProtocolMajor: 10},
			))
			require.NoError(t, txn.Commit())
		},
		CertsAtSlot: func(slot uint64, idx uint32, seed byte, deposits map[int]uint64, certs ...[]any) {
			body := newRowBatchingTx(t, map[uint]any{
				0: []any{[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)}},
				1: []any{},
				2: uint64(100_000),
				4: certs,
			})
			tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
			require.NoError(t, err)
			var txHash [32]byte
			copy(txHash[:], tx.Hash().Bytes())
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			require.NoError(t, ldb.SetTransactionWithOpts(
				t.Context(), tx,
				ocommon.NewPoint(slot, bytes.Repeat([]byte{seed}, 32)),
				idx, 0, nil, deposits,
				&database.BlockIngestionResult{
					TxOffsets: map[[32]byte]database.CborOffset{txHash: {}},
				},
				txn, database.BatchedTxIngestOpts{ProtocolMajor: 10},
			))
			require.NoError(t, txn.Commit())
		},
		DijkstraBoundary: func(f BoundaryStakeFixture) {
			ldb := h.ls.LedgerDB()
			cur := h.ls.currentEpoch
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			require.NoError(t, ldb.SetEpoch(
				f.Boundary, cur.EpochId+1, nil, nil, nil, nil,
				gdijkstra.EraIdDijkstra, cur.SlotLength, cur.LengthInSlots, txn,
			))
			require.NoError(t, txn.Commit())
		},
		ReadBoundaryStake: func(f BoundaryStakeFixture, viaStore bool) BoundaryStakeAnswers {
			var db ledgerdb.LedgerDB = dbadapter.New(h.ls.db)
			if !viaStore {
				db = h.ls.LedgerDB()
			}
			pools := [][]byte{f.Pool, f.Idle}
			var a BoundaryStakeAnswers
			var err error
			a.Active, err = db.GetEpochBoundaryActivePoolKeyHashes(
				f.Slot,
				f.Boundary,
				nil,
			)
			require.NoError(t, err)
			a.Delegated, err = db.GetEpochBoundaryDelegatedPoolKeyHashes(
				f.Slot,
				f.Boundary,
				nil,
			)
			require.NoError(t, err)
			a.Stakes, a.Delegators, err = db.GetEpochBoundaryStakeByPools(
				pools, f.Slot, f.Boundary, nil, 0, 0, nil,
			)
			require.NoError(t, err)
			a.Inputs, err = db.GetEpochBoundaryRewardStakeInputsForPools(
				pools, f.Slot, f.Boundary, nil, 0, 0, nil,
			)
			require.NoError(t, err)
			a.Pointers, err = db.GetPointerStakeInputsForPools(
				pools, f.Slot, f.Boundary, 0, nil,
			)
			require.NoError(t, err)
			return a
		},
		ApplyBoundary:          func() { h.applyBoundary() },
		RollbackBeforeBoundary: h.rollbackBeforeBoundary,
		LedgerDB:               h.ls.LedgerDB,
		CertifyLeios: func() {
			h.ls.config.ValidateLeiosCertificate = func(uint64, []byte, []byte, []byte) error {
				return nil
			}
			// Certificates exist only from the Dijkstra era on.
			h.ls.Lock()
			h.ls.currentEra = eras.DijkstraEraDesc
			h.ls.publishSnapshotsLocked()
			h.ls.Unlock()
		},
		ConfigureLeiosValidator: func() {
			h.ls.config.ValidateLeiosCertificate = func(uint64, []byte, []byte, []byte) error {
				return nil
			}
		},
		DeferredBoundaryRows: func(f BoundaryStakeFixture) []*models.PoolStakeSnapshot {
			evt := event.EpochTransitionEvent{
				PreviousEpoch: f.Boundary/2 - 1,
				NewEpoch:      f.Boundary / 2,
				BoundarySlot:  f.Boundary,
				SnapshotSlot:  f.Slot,
			}
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			require.NoError(t, h.mgr.DeferEpochBoundaryCapture(txn, evt))
			txn.Release()
			h.applyBoundary()
			txn = ldb.Begin(t.Context(), false)
			defer txn.Release()
			prepared, err := h.mgr.PrepareEpochBoundarySnapshot(
				t.Context(), txn, evt,
			)
			require.NoError(t, err)
			return prepared.SPOStakeRows()
		},
		HideStoreReads: func(names ...string) (func() int64, func(bool)) {
			blind := h.blind
			if blind == nil {
				blind = &blindStore{
					LedgerDB: dbadapter.New(h.ls.db),
					denied:   map[string]bool{},
				}
				h.ls.ldb = stagingLedgerDB{LedgerDB: blind, ls: h.ls}
			}
			clear(blind.denied)
			for _, n := range names {
				blind.denied[n] = true
			}
			blind.off.Store(false)
			return blind.hits.Load, func(on bool) { blind.off.Store(!on) }
		},
		LedgerStateSetOnly: func(on bool) {
			h.ls.config.LedgerStateSetOnly = on
			if on {
				h.ls.SetDrainedFamilies(nodesettings.AllFamilies())
			}
		},
		SetOnlySwitch: func(on bool) { h.ls.config.LedgerStateSetOnly = on },
		DrainFamilies: h.ls.SetDrainedFamilies,
		InstallDrainedSources: func() {
			h.ls.installNonceSource()
			h.ls.installEpochSource()
			h.ls.installPParamsSource()
			h.ls.installPoolSource()
		},
		ComputedResults: computedResultCounts(h.ls),
		CorruptEpochWrites: func() {
			h.ls.ldb = stagingLedgerDB{
				LedgerDB: corruptEpochStore{dbadapter.New(h.ls.db)},
				ls:       h.ls,
			}
		},
		EpochStateReads: func() (float64, float64) {
			return promtestutil.ToFloat64(h.ls.metrics.epochStateReadsVerified),
				promtestutil.ToFloat64(h.ls.metrics.epochStateReadMismatches)
		},
		EpochSummaryReads: func() (float64, float64) {
			return promtestutil.ToFloat64(
					h.ls.metrics.epochSummaryReadsVerified,
				),
				promtestutil.ToFloat64(
					h.ls.metrics.epochSummaryReadMismatches,
				)
		},
		SummaryInsideRollback: func(epoch uint64) (*models.EpochSummary, uint64) {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			require.NoError(t, h.ls.stageUtxoRollbackEvent(
				t.Context(), txn, h.fixture.ancestorTip.Point,
			))
			sm, err := ldb.GetEpochSummary(epoch, txn)
			require.NoError(t, err)
			total, err := ldb.GetTotalActiveStake(epoch, "mark", txn)
			require.NoError(t, err)
			return sm, total
		},
		SaveSummariesInsideTxn: func(epoch uint64, ready ...bool) *models.EpochSummary {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			for _, r := range ready {
				require.NoError(t, ldb.SaveEpochSummary(&models.EpochSummary{
					Epoch: epoch, TotalActiveStake: 5, SnapshotReady: r,
				}, txn))
			}
			sm, err := ldb.GetEpochSummary(epoch, txn)
			require.NoError(t, err)
			return sm
		},
		SummaryInsideTxn: func(summary models.EpochSummary) (*models.EpochSummary, uint64) {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			require.NoError(t, ldb.SaveEpochSummary(&summary, txn))
			sm, err := ldb.GetEpochSummary(summary.Epoch, txn)
			require.NoError(t, err)
			total, err := ldb.GetTotalActiveStake(summary.Epoch, "mark", txn)
			require.NoError(t, err)
			return sm, total
		},
		NetworkStateReads: func() (float64, float64) {
			return promtestutil.ToFloat64(
					h.ls.metrics.networkStateReadsVerified,
				),
				promtestutil.ToFloat64(
					h.ls.metrics.networkStateReadMismatches,
				)
		},
		AccountReads: func() (float64, float64) {
			return promtestutil.ToFloat64(h.ls.metrics.accountReadsVerified),
				promtestutil.ToFloat64(h.ls.metrics.accountReadMismatches)
		},
		DrepReads: func() (float64, float64) {
			return promtestutil.ToFloat64(h.ls.metrics.drepReadsVerified),
				promtestutil.ToFloat64(h.ls.metrics.drepReadMismatches)
		},
		PoolReads: func() (float64, float64) {
			return promtestutil.ToFloat64(h.ls.metrics.poolReadsVerified),
				promtestutil.ToFloat64(h.ls.metrics.poolReadMismatches)
		},
		RegistrationReads: func() (float64, float64) {
			return promtestutil.ToFloat64(
					h.ls.metrics.registrationReadsVerified,
				),
				promtestutil.ToFloat64(
					h.ls.metrics.registrationReadMismatches,
				)
		},
		OpCertReads: func() (float64, float64) {
			return promtestutil.ToFloat64(h.ls.metrics.opCertReadsVerified),
				promtestutil.ToFloat64(h.ls.metrics.opCertReadMismatches)
		},
		LiveStakeReads: func() (float64, float64) {
			return promtestutil.ToFloat64(h.ls.metrics.liveStakeReadsVerified),
				promtestutil.ToFloat64(h.ls.metrics.liveStakeReadMismatches)
		},
		RewardOutputReads: func() (float64, float64) {
			return promtestutil.ToFloat64(
					h.ls.metrics.rewardOutputReadsVerified,
				),
				promtestutil.ToFloat64(
					h.ls.metrics.rewardOutputReadMismatches,
				)
		},
		EpochStartSlot: func() uint64 { return h.ls.currentEpoch.StartSlot },
		RollbackSlot:   func() uint64 { return h.fixture.ancestorTip.Point.Slot },
		FoldForWithdrawal: func(ref models.StakeCredentialRef) {
			address, err := lcommon.NewAddressFromParts(
				lcommon.AddressTypeNoneKey, lcommon.AddressNetworkTestnet,
				nil, ref.Key,
			)
			require.NoError(t, err)
			tx := &conway.ConwayTransaction{Body: conway.ConwayTransactionBody{
				TxWithdrawals: map[*lcommon.Address]uint64{&address: 1},
			}}
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			require.NoError(
				t,
				h.ls.foldRewardCreditsForWithdrawals(t.Context(), tx, txn),
			)
			require.NoError(t, txn.Commit())
		},
		RewardViewsInsideRollback: func(
			ref models.StakeCredentialRef,
			slot uint64,
			after func(ledgerdb.LedgerDB, ledgerdb.Txn),
		) RewardViews {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			require.NoError(t, h.ls.stageUtxoRollbackEvent(
				t.Context(), txn, h.fixture.ancestorTip.Point,
			))
			if after != nil {
				after(ldb, txn)
			}
			r := h.ls.rewardOutputReader(txn)
			require.NotNil(t, r)
			var v RewardViews
			var err error
			v.Pending, err = h.ls.pendingRewardOutputsView(
				r,
				txn,
				ref.Tag,
				ref.Key,
			)
			require.NoError(t, err)
			v.HasPending, err = h.ls.hasCreditedRewardOutputs(r, txn, nil)
			require.NoError(t, err)
			v.Rounds, err = h.ls.rewardRoundsView(r, txn)
			require.NoError(t, err)
			v.Credits, err = h.ls.postSnapshotCreditsView(r, txn, slot)
			require.NoError(t, err)
			return v
		},
		PinnedReadTxn: func() ledgerdb.Txn {
			txn := h.ls.ledgerDB().Begin(t.Context(), false)
			t.Cleanup(txn.Release)
			// A set-served read would leave the store's snapshot unfixed.
			_, err := dbadapter.New(h.ls.db).GetPendingRewardCreditRounds(txn)
			require.NoError(t, err)
			h.ls.pinSetReads(txn)
			return txn
		},
		AccountReadMismatches: func() uint64 {
			return h.ls.accountReadMismatches.Load()
		},
		CompactRewardCredits: func(limit int) bool {
			done, err := h.ls.compactRewardCreditChunk(t.Context(), limit)
			require.NoError(t, err)
			return done
		},
		RelayProviderStake: func(pools [][]byte) (map[string]uint64, error) {
			p, err := NewPoolRelayProvider(h.ls, h.ls.db, nil)
			require.NoError(t, err)
			return p.stakeByPools(t.Context(), pools)
		},
		AddSetStake: func(key []byte, amount uint64) {
			addr, err := lcommon.NewAddressFromParts(
				lcommon.AddressTypeKeyKey, lcommon.AddressNetworkTestnet,
				rowBatchingKey(0xaa), key,
			)
			require.NoError(t, err)
			raw, err := addr.Bytes()
			require.NoError(t, err)
			out, err := cbor.Encode(map[uint]any{0: raw, 1: amount})
			require.NoError(t, err)
			setOutputs++
			id := bytes.Repeat([]byte{0xf0 + setOutputs}, 32)
			require.NoError(t, set.ApplyUtxoChanges(nil, []UtxoEvent{{
				Added: []UtxoOutput{{Ref: models.UtxoId{Hash: id}, Cbor: out}},
			}}))
		},
		VoteDelegate: func(ref models.StakeCredentialRef, drepKey []byte, predefined uint64, offset uint64) {
			ldb := h.ls.LedgerDB()
			start := h.ls.currentEpoch.StartSlot
			stake := []any{uint64(ref.Tag), ref.Key}
			submit := func(slot uint64, seed byte, deposits map[int]uint64, certs ...[]any) {
				body := newRowBatchingTx(t, map[uint]any{
					0: []any{[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)}},
					1: []any{},
					2: uint64(100_000),
					4: certs,
				})
				tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
				require.NoError(t, err)
				var txHash [32]byte
				copy(txHash[:], tx.Hash().Bytes())
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				require.NoError(t, ldb.SetTransactionWithOpts(
					t.Context(), tx,
					ocommon.NewPoint(slot, bytes.Repeat([]byte{seed}, 32)),
					0, 0, nil, deposits,
					&database.BlockIngestionResult{
						TxOffsets: map[[32]byte]database.CborOffset{txHash: {}},
					},
					txn, database.BatchedTxIngestOpts{ProtocolMajor: 10},
				))
				require.NoError(t, txn.Commit())
			}
			if drepKey == nil {
				submit(start+offset, 0x81, nil,
					[]any{uint64(9), stake, []any{predefined}})
				return
			}
			submit(
				start+offset,
				0x82,
				map[int]uint64{0: 500_000_000},
				[]any{
					uint64(16),
					[]any{uint64(0), drepKey},
					uint64(500_000_000),
					nil,
				},
				[]any{uint64(9), stake, []any{uint64(0), drepKey}},
			)
		},
		CreditInsideTxn: func(ref models.StakeCredentialRef, pool []byte, amount uint64) uint64 {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			require.NoError(t, ldb.AddAccountRewardByCredential(
				t.Context(), ref.Tag, ref.Key, amount,
				h.ls.currentEpoch.StartSlot+15, []byte{9}, txn,
			))
			in, err := ldb.GetLiveStakeInputsForPools([][]byte{pool}, 0, txn)
			require.NoError(t, err)
			_, _, err = ldb.GetStakeByPools(t.Context(), [][]byte{pool}, txn)
			require.NoError(t, err)
			_, err = ldb.GetDelegatedPoolKeyHashes(txn)
			require.NoError(t, err)
			_, err = ldb.GetDRepVotingPowerByType(
				t.Context(),
				[]uint64{2, 3},
				0,
				txn,
			)
			require.NoError(t, err)
			for _, i := range in {
				if i.CredentialTag == ref.Tag &&
					bytes.Equal(i.StakingKey, ref.Key) {
					return uint64(i.Stake)
				}
			}
			require.FailNow(t, "ref has no live stake input")
			return 0
		},
		RegisterInsideTxn: func(key []byte, pool []byte) [][]byte {
			ldb := h.ls.LedgerDB()
			stake := []any{uint64(0), key}
			body := newRowBatchingTx(t, map[uint]any{
				0: []any{[]any{bytes.Repeat([]byte{0x91}, 32), uint64(0)}},
				1: []any{},
				2: uint64(100_000),
				4: [][]any{{uint64(0), stake}, {uint64(2), stake, pool}},
			})
			tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
			require.NoError(t, err)
			var txHash [32]byte
			copy(txHash[:], tx.Hash().Bytes())
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			require.NoError(t, ldb.SetTransactionWithOpts(
				t.Context(), tx,
				ocommon.NewPoint(
					h.ls.currentEpoch.StartSlot+16,
					bytes.Repeat([]byte{0x91}, 32),
				),
				0, 0, nil, map[int]uint64{0: 2_000_000},
				&database.BlockIngestionResult{
					TxOffsets: map[[32]byte]database.CborOffset{txHash: {}},
				},
				txn, database.BatchedTxIngestOpts{ProtocolMajor: 10},
			))
			in, err := ldb.GetLiveStakeInputsForPools([][]byte{pool}, 0, txn)
			require.NoError(t, err)
			var keys [][]byte
			for _, i := range in {
				keys = append(keys, i.StakingKey)
			}
			return keys
		},
		RegisterCredentialInsideTxn: func(
			tag uint8, key []byte, pool []byte,
		) ([]models.StakeCredentialRef, uint64) {
			ldb := h.ls.LedgerDB()
			stake := []any{uint64(tag), key}
			body := newRowBatchingTx(t, map[uint]any{
				0: []any{[]any{bytes.Repeat([]byte{0x93}, 32), uint64(0)}},
				1: []any{},
				2: uint64(100_000),
				4: [][]any{{uint64(0), stake}, {uint64(2), stake, pool}},
			})
			tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
			require.NoError(t, err)
			var txHash [32]byte
			copy(txHash[:], tx.Hash().Bytes())
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			require.NoError(t, ldb.SetTransactionWithOpts(
				t.Context(), tx,
				ocommon.NewPoint(
					h.ls.currentEpoch.StartSlot+18,
					bytes.Repeat([]byte{0x93}, 32),
				),
				0, 0, nil, map[int]uint64{0: 2_000_000},
				&database.BlockIngestionResult{
					TxOffsets: map[[32]byte]database.CborOffset{txHash: {}},
				},
				txn, database.BatchedTxIngestOpts{ProtocolMajor: 10},
			))
			pools := []lcommon.PoolKeyHash{
				lcommon.PoolKeyHash(lcommon.NewBlake2b224(pool)),
			}
			creds, err := ldb.GetActiveDelegatorCredentialsByPool(pools, txn)
			require.NoError(t, err)
			counts, err := ldb.GetActiveDelegatorCountsByPool(pools, txn)
			require.NoError(t, err)
			_, err = ldb.GetActiveAccountCredentials(txn)
			require.NoError(t, err)
			return creds[string(pool)], counts[string(pool)]
		},
		RetireInsideTxn: func(
			pool []byte,
			epoch, boundarySlot uint64,
		) []models.PoolRetirementRefund {
			ldb := h.ls.LedgerDB()
			body := newRowBatchingTx(t, map[uint]any{
				0: []any{[]any{bytes.Repeat([]byte{0x92}, 32), uint64(0)}},
				1: []any{},
				2: uint64(100_000),
				4: [][]any{{uint64(4), pool, epoch}},
			})
			tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
			require.NoError(t, err)
			var txHash [32]byte
			copy(txHash[:], tx.Hash().Bytes())
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			require.NoError(t, ldb.SetTransactionWithOpts(
				t.Context(), tx,
				ocommon.NewPoint(
					h.ls.currentEpoch.StartSlot+17,
					bytes.Repeat([]byte{0x92}, 32),
				),
				0, 0, nil, nil,
				&database.BlockIngestionResult{
					TxOffsets: map[[32]byte]database.CborOffset{txHash: {}},
				},
				txn, database.BatchedTxIngestOpts{ProtocolMajor: 10},
			))
			refunds, err := ldb.GetPoolsRetiringAtEpoch(
				t.Context(), epoch, boundarySlot, txn,
			)
			require.NoError(t, err)
			return refunds
		},
		SetFailed: func() bool { return h.ls.utxoSetFailed.Load() },
		ReapRetiredPool: func() ReapOutcome {
			ldb := h.ls.LedgerDB()
			cur := h.ls.currentEpoch
			start := cur.StartSlot
			epoch := cur.EpochId + 1
			// The harness's epochs are two slots long; the certificates need
			// room before the boundary they retire into.
			boundarySlot := start + 30
			pool := bytes.Repeat([]byte{0x95}, 28)
			key := bytes.Repeat([]byte{0x96}, 28)
			const tag = uint8(0)
			for _, slot := range []uint64{start + 20, start + 21, start + 22} {
				h.coverSlot(slot)
			}
			submitCertificates(t, ldb, start+20, 0x97,
				map[int]uint64{0: 2_000_000, 1: 500_000_000},
				[]any{uint64(0), []any{uint64(tag), key}},
				[]any{
					uint64(3), pool, bytes.Repeat([]byte{0x98}, 32),
					uint64(1_000_000), uint64(340_000_000),
					cbor.Tag{Number: 30, Content: []uint64{1, 10}},
					append([]byte{0xe0}, key...), []any{key},
					[]any{[]any{uint64(1), uint64(3001), "relay.example"}}, nil,
				})
			account := func(includeInactive bool) *models.Account {
				txn := ldb.Begin(t.Context(), false)
				defer txn.Release()
				a, err := ldb.GetAccountByCredential(
					t.Context(), tag, key, includeInactive, txn,
				)
				if errors.Is(err, models.ErrAccountNotFound) {
					return nil
				}
				require.NoError(t, err)
				return a
			}
			var out ReapOutcome
			out.Registered = account(false) != nil
			submitCertificates(t, ldb, start+21, 0x99, nil,
				[]any{uint64(1), []any{uint64(tag), key}})
			submitCertificates(t, ldb, start+22, 0x9a, nil,
				[]any{uint64(4), pool, epoch})
			txn := ldb.Begin(t.Context(), false)
			refunds, err := ldb.GetPoolsRetiringAtEpoch(
				t.Context(), epoch, boundarySlot, txn,
			)
			txn.Release()
			require.NoError(t, err)
			for _, r := range refunds {
				if bytes.Equal(r.PoolKeyHash, pool) {
					out.Refund = r
				}
			}
			require.NotNil(t, out.Refund.PoolKeyHash, "the pool must be retiring")
			txn = ldb.Begin(t.Context(), true)
			require.NoError(t, ldb.SetNetworkState(1_000, 5_000, start+23, txn))
			require.NoError(t, txn.Commit())
			txn = ldb.Begin(t.Context(), true)
			defer txn.Release()
			require.NoError(t, h.ls.applyPoolReapRefunds(
				t.Context(), txn, epoch, boundarySlot,
			))
			require.NoError(t, txn.Commit())
			out.SetFailed = h.ls.utxoSetFailed.Load()
			if out.SetFailed {
				// A set that stopped following the ledger serves no read.
				return out
			}
			if after := account(true); after != nil {
				out.Reward = uint64(after.Reward)
			}
			state, err := ldb.GetNetworkState(nil)
			require.NoError(t, err)
			out.Network = []string{fmt.Sprintf(
				"network %d %d %d", state.Slot, state.Treasury, state.Reserves,
			)}
			return out
		},
		AnchorOpCertCounters: func(slot uint64) ([]metadata.OpCertCounter, error) {
			return h.ls.AnchorOpCertCounters(t.Context(), slot)
		},
		OpCerts: func() OpCertSlots {
			ldb := h.ls.LedgerDB()
			start := h.ls.currentEpoch.StartSlot
			slots := OpCertSlots{
				Pool:     h.poolHash,
				Other:    bytes.Repeat([]byte{0x6e}, 28),
				Base:     h.fixture.ancestorTip.Point.Slot,
				Raised:   start + 30,
				Repeated: start + 31,
				Lowered:  start + 32,
				Batched:  start + 33,
			}
			write := func(pool []byte, sequence, slot uint64) {
				h.coverSlot(slot)
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				require.NoError(t, ldb.UpdatePoolOpCertSequence(
					t.Context(), lcommon.PoolKeyHash(pool), sequence, slot, txn,
				))
				require.NoError(t, txn.Commit())
			}
			write(slots.Pool, 3, slots.Base)
			write(slots.Pool, 5, slots.Raised)
			write(slots.Pool, 5, slots.Repeated)
			write(slots.Other, 2, slots.Repeated)
			write(slots.Pool, 4, slots.Lowered)
			// The same observation again, at its earlier slot, changes nothing.
			write(slots.Pool, 5, slots.Raised)
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			acc := ldb.NewBatchAccumulator()
			require.NoError(t, ldb.UpdatePoolOpCertSequenceBatched(
				t.Context(), lcommon.PoolKeyHash(slots.Pool), 6, slots.Batched,
				acc, txn,
			))
			require.NoError(t, ldb.FlushBatch(acc, txn))
			require.NoError(t, txn.Commit())
			return slots
		},
		OpCertsInsideTxn: func(pool []byte, sequence, slot uint64) {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			pkh := lcommon.PoolKeyHash(pool)
			require.NoError(t, ldb.UpdatePoolOpCertSequence(
				t.Context(), pkh, sequence, slot, txn,
			))
			got, found, err := ldb.LatestPoolOpCertSequence(
				t.Context(),
				pkh,
				txn,
			)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, sequence, got)
			at, found, err := ldb.LatestPoolOpCertSequenceAtOrBefore(
				t.Context(), pkh, slot, txn,
			)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, sequence, at)
			_, err = ldb.LatestPoolOpCertSequences(t.Context(), txn)
			require.NoError(t, err)
		},
		OpCertObservedInsideTxn: func(pool []byte, sequence, slot uint64) bool {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			require.NoError(t, ldb.UpdatePoolOpCertSequence(
				t.Context(), lcommon.PoolKeyHash(pool), sequence, slot, txn,
			))
			observed, err := ldb.PoolOpCertSequencesExistAtSlot(
				t.Context(),
				slot,
				txn,
			)
			require.NoError(t, err)
			return observed
		},
		CommitteeReads: func() (float64, float64) {
			return promtestutil.ToFloat64(h.ls.metrics.committeeReadsVerified),
				promtestutil.ToFloat64(h.ls.metrics.committeeReadMismatches)
		},
		Committee: func() CommitteeSlots {
			ldb := h.ls.LedgerDB()
			start := h.ls.currentEpoch.StartSlot
			slots := CommitteeSlots{
				A:            bytes.Repeat([]byte{0xa1}, 28),
				B:            bytes.Repeat([]byte{0xb1}, 28),
				C:            bytes.Repeat([]byte{0xc1}, 28),
				Base:         h.fixture.ancestorTip.Point.Slot,
				Auth:         start + 30,
				Resign:       start + 31,
				Reauth:       start + 32,
				Update:       start + 33,
				Constitution: start + 34,
				NoConfidence: start + 35,
			}
			member := func(cold []byte, expires, slot uint64) *models.CommitteeMember {
				return &models.CommitteeMember{
					ColdCredHash: cold, ExpiresEpoch: expires, AddedSlot: slot,
				}
			}
			write := func(fn func(txn ledgerdb.Txn)) {
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				fn(txn)
				require.NoError(t, txn.Commit())
			}
			cert := func(slot uint64, seed byte, certs ...[]any) {
				body := newRowBatchingTx(t, map[uint]any{
					0: []any{[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)}},
					1: []any{},
					2: uint64(100_000),
					4: certs,
				})
				tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
				require.NoError(t, err)
				var txHash [32]byte
				copy(txHash[:], tx.Hash().Bytes())
				write(func(txn ledgerdb.Txn) {
					require.NoError(t, ldb.SetTransactionWithOpts(
						t.Context(), tx,
						ocommon.NewPoint(slot, bytes.Repeat([]byte{seed}, 32)),
						0, 0, nil, nil,
						&database.BlockIngestionResult{
							TxOffsets: map[[32]byte]database.CborOffset{
								txHash: {},
							},
						},
						txn, database.BatchedTxIngestOpts{ProtocolMajor: 10},
					))
				})
			}
			auth := func(cold, hot []byte) []any {
				return []any{
					uint64(14),
					[]any{uint64(0), cold},
					[]any{uint64(0), hot},
				}
			}
			hotA := bytes.Repeat([]byte{0xa2}, 28)
			write(func(txn ledgerdb.Txn) {
				require.NoError(t, ldb.SetConstitution(&models.Constitution{
					AnchorURL: "https://example.test/c1", AnchorHash: bytes.Repeat([]byte{1}, 32),
					AddedSlot: slots.Base,
				}, txn))
				require.NoError(
					t,
					ldb.SetCommitteeMembers(
						t.Context(),
						[]*models.CommitteeMember{
							member(
								slots.A,
								100,
								slots.Base,
							), member(slots.B, 100, slots.Base),
						},
						txn,
					),
				)
				require.NoError(
					t,
					ldb.SetCommitteeQuorum(
						t.Context(),
						big.NewRat(1, 2),
						slots.Base,
						txn,
					),
				)
			})
			cert(slots.Auth, 0x81,
				auth(slots.A, bytes.Repeat([]byte{0xa3}, 28)),
				auth(slots.B, bytes.Repeat([]byte{0xb3}, 28)))
			cert(slots.Resign, 0x82,
				[]any{uint64(15), []any{uint64(0), slots.B}, nil})
			cert(slots.Reauth, 0x83, auth(slots.A, hotA))
			write(func(txn ledgerdb.Txn) {
				require.NoError(t, ldb.SoftDeleteCommitteeMembers(
					t.Context(),
					[]models.CommitteeCredential{
						{Credential: slots.B},
					},
					slots.Update,
					txn,
				))
				require.NoError(
					t,
					ldb.SetCommitteeMembers(
						t.Context(),
						[]*models.CommitteeMember{
							member(
								slots.C,
								120,
								slots.Update,
							), member(slots.A, 130, slots.Update),
						},
						txn,
					),
				)
				require.NoError(
					t,
					ldb.SetCommitteeQuorum(
						t.Context(),
						big.NewRat(2, 3),
						slots.Update,
						txn,
					),
				)
			})
			write(func(txn ledgerdb.Txn) {
				require.NoError(t, ldb.SetConstitution(&models.Constitution{
					AnchorURL: "https://example.test/c2", AnchorHash: bytes.Repeat([]byte{2}, 32),
					PolicyHash: bytes.Repeat(
						[]byte{3},
						28,
					), AddedSlot: slots.Constitution,
				}, txn))
			})
			write(func(txn ledgerdb.Txn) {
				require.NoError(
					t,
					ldb.SoftDeleteAllCommitteeMembers(
						t.Context(),
						slots.NoConfidence,
						txn,
					),
				)
				require.NoError(
					t,
					ldb.ClearCommitteeQuorum(
						t.Context(),
						slots.NoConfidence,
						txn,
					),
				)
			})
			return slots
		},
		OpCertsLater: func(pool, other []byte, from uint64) {
			h.coverSlot(from)
			h.coverSlot(from + 2)
			ldb := h.ls.LedgerDB()
			for _, w := range []struct {
				pool     []byte
				sequence uint64
				slot     uint64
			}{
				{pool, 7, from}, {other, 4, from + 1}, {pool, 9, from + 2},
			} {
				txn := ldb.Begin(t.Context(), true)
				require.NoError(t, ldb.UpdatePoolOpCertSequence(
					t.Context(),
					lcommon.PoolKeyHash(w.pool),
					w.sequence,
					w.slot,
					txn,
				))
				require.NoError(t, txn.Commit())
			}
		},
		CommitteeLater: func(cold, hot, hot2 []byte, from uint64) {
			ldb := h.ls.LedgerDB()
			write := func(fn func(txn ledgerdb.Txn)) {
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				fn(txn)
				require.NoError(t, txn.Commit())
			}
			cert := func(slot uint64, seed byte, certs ...[]any) {
				h.coverSlot(slot)
				body := newRowBatchingTx(t, map[uint]any{
					0: []any{[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)}},
					1: []any{},
					2: uint64(100_000),
					4: certs,
				})
				tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
				require.NoError(t, err)
				var txHash [32]byte
				copy(txHash[:], tx.Hash().Bytes())
				write(func(txn ledgerdb.Txn) {
					require.NoError(t, ldb.SetTransactionWithOpts(
						t.Context(), tx,
						ocommon.NewPoint(slot, bytes.Repeat([]byte{seed}, 32)),
						0, 0, nil, nil,
						&database.BlockIngestionResult{
							TxOffsets: map[[32]byte]database.CborOffset{
								txHash: {},
							},
						},
						txn, database.BatchedTxIngestOpts{ProtocolMajor: 10},
					))
				})
			}
			auth := func(hotKey []byte) []any {
				return []any{
					uint64(14),
					[]any{uint64(0), cold},
					[]any{uint64(0), hotKey},
				}
			}
			write(func(txn ledgerdb.Txn) {
				require.NoError(
					t,
					ldb.SetCommitteeMembers(
						t.Context(),
						[]*models.CommitteeMember{
							{
								ColdCredHash: cold,
								ExpiresEpoch: 200,
								AddedSlot:    from,
							},
						},
						txn,
					),
				)
				require.NoError(
					t,
					ldb.SetCommitteeQuorum(
						t.Context(),
						big.NewRat(3, 4),
						from,
						txn,
					),
				)
			})
			cert(from+1, 0x91, auth(hot))
			cert(from+2, 0x92, []any{uint64(15), []any{uint64(0), cold}, nil})
			cert(from+3, 0x93, auth(hot2))
			write(func(txn ledgerdb.Txn) {
				require.NoError(t, ldb.SetConstitution(&models.Constitution{
					AnchorURL: "https://example.test/c4", AnchorHash: bytes.Repeat([]byte{4}, 32),
					AddedSlot: from + 4,
				}, txn))
			})
		},
		CommitteeInsideTxn: func(slot uint64) {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			cold := bytes.Repeat([]byte{0xd1}, 28)
			require.NoError(
				t,
				ldb.SetCommitteeMembers(t.Context(), []*models.CommitteeMember{
					{ColdCredHash: cold, ExpiresEpoch: 9, AddedSlot: slot},
				}, txn),
			)
			require.NoError(
				t,
				ldb.SetCommitteeQuorum(
					t.Context(),
					big.NewRat(3, 4),
					slot,
					txn,
				),
			)
			require.NoError(t, ldb.SetConstitution(&models.Constitution{
				AnchorURL: "https://example.test/c3", AnchorHash: bytes.Repeat([]byte{5}, 32), AddedSlot: slot,
			}, txn))
			members, err := ldb.GetCommitteeMembers(t.Context(), txn)
			require.NoError(t, err)
			require.NotEmpty(t, members)
			q, err := ldb.GetCommitteeQuorum(t.Context(), txn)
			require.NoError(t, err)
			require.Equal(t, big.NewRat(3, 4), q)
			c, err := ldb.GetConstitution(txn)
			require.NoError(t, err)
			require.Equal(t, "https://example.test/c3", c.AnchorURL)
			_, err = ldb.GetCommitteeMembersIncludeDeleted(t.Context(), txn)
			require.NoError(t, err)
			_, err = ldb.GetActiveCommitteeMembers(t.Context(), txn)
			require.NoError(t, err)
		},
		GovernanceReads: func() (float64, float64) {
			return promtestutil.ToFloat64(h.ls.metrics.governanceReadsVerified),
				promtestutil.ToFloat64(h.ls.metrics.governanceReadMismatches)
		},
		PinInsideCommit: func() PinInsideCommitResult {
			ldb := h.ls.LedgerDB()
			start := h.ls.currentEpoch.StartSlot
			epoch := h.ls.currentEpoch.EpochId
			d := bytes.Repeat([]byte{0xe5}, 28)
			write := func(fn func(txn ledgerdb.Txn)) {
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				fn(txn)
				require.NoError(t, txn.Commit())
			}
			activity := func(txn ledgerdb.Txn, at, slot uint64) {
				require.NoError(t, ldb.UpdateDRepActivity(
					t.Context(), 0, d, at, 20, slot, txn,
				))
			}
			write(func(txn ledgerdb.Txn) {
				require.NoError(t, ldb.InsertDrepIfAbsent(
					t.Context(), 0, d, start+1, "", nil, true, txn,
				))
				activity(txn, epoch, start+1)
			})
			var pin ledgerdb.Txn
			write(func(txn ledgerdb.Txn) {
				txn.AfterCommit(func() {
					pin = ldb.Begin(t.Context(), false)
					_, err := ldb.GetPendingRewardCreditRounds(pin)
					require.NoError(t, err)
					h.ls.pinSetReads(pin)
				})
				activity(txn, epoch+1, start+2)
			})
			require.NotNil(t, pin)
			defer pin.Release()
			write(func(txn ledgerdb.Txn) { activity(txn, epoch+2, start+3) })
			expiry := func(txn ledgerdb.Txn) uint64 {
				dreps, err := ldb.GetActiveDreps(t.Context(), txn)
				require.NoError(t, err)
				for _, r := range dreps {
					if bytes.Equal(r.Credential, d) {
						return r.ExpiryEpoch
					}
				}
				require.FailNow(t, "the DRep is not active")
				return 0
			}
			return PinInsideCommitResult{
				Epoch: epoch, Pinned: expiry(pin), Latest: expiry(nil),
			}
		},
		SnapshotReadOfADivergence: func() SnapshotReadCounts {
			ldb := h.ls.LedgerDB()
			require.NoError(t, db.Metadata().InsertDrepIfAbsent(
				0, bytes.Repeat([]byte{0xe3}, 28),
				h.ls.currentEpoch.StartSlot+1, "", nil, true, nil,
			))
			snap := ldb.Begin(t.Context(), false)
			defer snap.Release()
			dreps, err := ldb.GetActiveDreps(t.Context(), snap)
			require.NoError(t, err)
			h.ls.pinSetReads(snap)
			require.NotEmpty(
				t,
				dreps,
				"the read answers from the metadata store",
			)
			return SnapshotReadCounts{
				DrepVerified: promtestutil.ToFloat64(
					h.ls.metrics.drepReadsVerified,
				),
				DrepMismatches: promtestutil.ToFloat64(
					h.ls.metrics.drepReadMismatches,
				),
			}
		},
		SnapshotReadsAfterLaterCommit: func() SnapshotReadCounts {
			ldb := h.ls.LedgerDB()
			start := h.ls.currentEpoch.StartSlot
			epoch := h.ls.currentEpoch.EpochId
			hash := bytes.Repeat([]byte{0x93}, 32)
			d1 := bytes.Repeat([]byte{0xe1}, 28)
			d2 := bytes.Repeat([]byte{0xe2}, 28)
			write := func(fn func(txn ledgerdb.Txn)) {
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				fn(txn)
				require.NoError(t, txn.Commit())
			}
			index := uint32(0)
			p := &models.GovernanceProposal{
				TxHash: hash, ActionType: 1, ProposedEpoch: epoch,
				ExpiresEpoch: epoch + 5, AnchorURL: "https://example.test/s",
				AnchorHash: bytes.Repeat([]byte{7}, 32), Deposit: 1000,
				ReturnAddress: bytes.Repeat([]byte{0xe0}, 29),
				GovActionCbor: []byte{
					0x01,
				}, AddedSlot: start + 1, TxIndex: &index,
			}
			vote := func(voter []byte, choice uint8, slot uint64, updated *uint64) *models.GovernanceVote {
				return &models.GovernanceVote{
					ProposalID: p.ID, VoterType: models.VoterTypeDRep,
					VoterCredential: voter, Vote: choice, AddedSlot: slot,
					VoteUpdatedSlot: updated,
				}
			}
			write(func(txn ledgerdb.Txn) {
				for _, d := range [][]byte{d1, d2} {
					require.NoError(t, ldb.InsertDrepIfAbsent(
						t.Context(), 0, d, start+1, "", nil, true, txn,
					))
					require.NoError(t, ldb.UpdateDRepActivity(
						t.Context(), 0, d, epoch, 20, start+1, txn,
					))
				}
				require.NoError(
					t,
					ldb.SetGovernanceProposal(t.Context(), p, txn),
				)
				require.NoError(t, ldb.SetGovernanceVote(
					t.Context(), vote(d1, 1, start+2, nil), txn,
				))
			})
			snap := ldb.Begin(t.Context(), false)
			defer snap.Release()
			// A read of the metadata store fixes its snapshot, as the job's
			// read of its pending record does; a set-served read would not.
			_, err := ldb.GetPendingRewardCreditRounds(snap)
			require.NoError(t, err)
			h.ls.pinSetReads(snap)
			write(func(txn ledgerdb.Txn) {
				require.NoError(t, ldb.UpdateDRepActivity(
					t.Context(), 0, d1, epoch+1, 20, start+3, txn,
				))
				updated := start + 3
				require.NoError(t, ldb.SetGovernanceVote(
					t.Context(), vote(d1, 0, start+2, &updated), txn,
				))
				require.NoError(t, ldb.SetGovernanceVote(
					t.Context(), vote(d2, 1, start+3, nil), txn,
				))
			})
			dreps, err := ldb.GetActiveDreps(t.Context(), snap)
			require.NoError(t, err)
			require.Len(t, dreps, 2)
			for _, d := range dreps {
				if bytes.Equal(d.Credential, d1) {
					require.Equal(t, epoch+20, d.ExpiryEpoch,
						"the pinned read still sees the earlier expiry")
				}
			}
			votes, err := ldb.GetGovernanceVotes(t.Context(), p.ID, snap)
			require.NoError(t, err)
			require.Len(t, votes, 1, "the pinned read still sees one vote")
			var c SnapshotReadCounts
			c.DrepVerified = promtestutil.ToFloat64(
				h.ls.metrics.drepReadsVerified,
			)
			c.DrepMismatches = promtestutil.ToFloat64(
				h.ls.metrics.drepReadMismatches,
			)
			c.GovernanceVerified = promtestutil.ToFloat64(
				h.ls.metrics.governanceReadsVerified,
			)
			c.GovernanceMismatches = promtestutil.ToFloat64(
				h.ls.metrics.governanceReadMismatches,
			)
			return c
		},
		Proposals: func() GovernanceSlots {
			ldb := h.ls.LedgerDB()
			start := h.ls.currentEpoch.StartSlot
			slots := GovernanceSlots{
				P1:          bytes.Repeat([]byte{0x91}, 32),
				P2:          bytes.Repeat([]byte{0x92}, 32),
				D1:          bytes.Repeat([]byte{0xd1}, 28),
				D2:          bytes.Repeat([]byte{0xd2}, 28),
				Epoch:       h.ls.currentEpoch.EpochId,
				Base:        h.fixture.ancestorTip.Point.Slot,
				Submit:      start + 30,
				Vote:        start + 31,
				Revote:      start + 32,
				Ratify:      start + 33,
				ClearRatify: start + 34,
				Deregister:  start + 35,
				Revote2:     start + 36,
				Expire:      start + 37,
				Translate:   start + 38,
				Drop:        start + 39,
				RatifyAgain: start + 40,
				Enact:       start + 41,
			}
			write := func(fn func(txn ledgerdb.Txn)) {
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				fn(txn)
				require.NoError(t, txn.Commit())
			}
			u64 := func(v uint64) *uint64 { return &v }
			proposal := func(hash []byte, slot uint64, index uint32) *models.GovernanceProposal {
				return &models.GovernanceProposal{
					TxHash: hash, ActionType: 1, ProposedEpoch: slots.Epoch,
					ExpiresEpoch: slots.Epoch + 5, AnchorURL: "https://example.test/p",
					AnchorHash: bytes.Repeat([]byte{7}, 32), Deposit: 1000,
					ReturnAddress: bytes.Repeat([]byte{0xe0}, 29),
					GovActionCbor: []byte{0x01}, AddedSlot: slot,
					TxIndex: &index,
				}
			}
			vote := func(proposalID uint, voter []byte, choice uint8, added uint64, updated *uint64) *models.GovernanceVote {
				return &models.GovernanceVote{
					ProposalID: proposalID, VoterType: models.VoterTypeDRep,
					VoterCredential: voter, Vote: choice, AddedSlot: added,
					VoteUpdatedSlot: updated,
				}
			}
			var p1, p2 *models.GovernanceProposal
			// Each write is followed by reads the set answers from what it
			// staged, which verify mode compares with the metadata store's.
			readAll := func() {
				_, err := ldb.GetGovernanceProposalSet(t.Context(), nil)
				require.NoError(t, err)
				_, err = ldb.GetRatifiedGovernanceProposals(t.Context(), nil)
				require.NoError(t, err)
				_, err = ldb.GetActiveGovernanceProposals(
					t.Context(),
					slots.Epoch,
					nil,
				)
				require.NoError(t, err)
				if p1 != nil {
					_, err = ldb.GetGovernanceVotes(t.Context(), p1.ID, nil)
					require.NoError(t, err)
				}
			}
			commit := write
			write = func(fn func(txn ledgerdb.Txn)) {
				commit(fn)
				readAll()
				proposalWrites++
				if onProposalWrite != nil {
					onProposalWrite(proposalWrites)
				}
			}
			write(func(txn ledgerdb.Txn) {
				p1 = proposal(slots.P1, slots.Base, 0)
				require.NoError(
					t,
					ldb.SetGovernanceProposal(t.Context(), p1, txn),
				)
			})
			write(func(txn ledgerdb.Txn) {
				p2 = proposal(slots.P2, slots.Submit, 1)
				require.NoError(
					t,
					ldb.SetGovernanceProposal(t.Context(), p2, txn),
				)
			})
			write(func(txn ledgerdb.Txn) {
				require.NoError(t, ldb.SetGovernanceVote(
					t.Context(),
					vote(
						p1.ID,
						slots.D1,
						models.VoteYes,
						slots.Vote,
						nil,
					),
					txn,
				))
				require.NoError(t, ldb.SetGovernanceVote(t.Context(),
					vote(p1.ID, slots.D2, models.VoteNo, slots.Vote, nil), txn))
			})
			write(func(txn ledgerdb.Txn) {
				require.NoError(t, ldb.SetGovernanceVote(
					t.Context(),
					vote(
						p1.ID,
						slots.D1,
						models.VoteAbstain,
						slots.Vote,
						&slots.Revote,
					),
					txn,
				))
			})
			ratified := func(slot uint64) *models.GovernanceProposal {
				p := proposal(slots.P1, slots.Base, 0)
				p.RatifiedEpoch, p.RatifiedSlot = u64(slots.Epoch), u64(slot)
				return p
			}
			write(func(txn ledgerdb.Txn) {
				require.NoError(
					t,
					ldb.SetGovernanceProposal(
						t.Context(),
						ratified(slots.Ratify),
						txn,
					),
				)
			})
			write(func(txn ledgerdb.Txn) {
				require.NoError(t, ldb.ClearGovernanceProposalRatification(
					t.Context(), slots.P1, 0, slots.ClearRatify, txn))
			})
			write(func(txn ledgerdb.Txn) {
				n, err := ldb.DeleteGovernanceVotesForDrep(
					t.Context(),
					0,
					slots.D2,
					slots.Epoch,
					slots.Deregister,
					txn,
				)
				require.NoError(t, err)
				require.Equal(t, 1, n)
			})
			write(func(txn ledgerdb.Txn) {
				require.NoError(t, ldb.SetGovernanceVote(
					t.Context(),
					vote(
						p1.ID,
						slots.D2,
						models.VoteYes,
						slots.Revote2,
						nil,
					),
					txn,
				))
			})
			write(func(txn ledgerdb.Txn) {
				p := proposal(slots.P2, slots.Submit, 1)
				p.ExpiredEpoch, p.ExpiredSlot = u64(
					slots.Epoch,
				), u64(
					slots.Expire,
				)
				require.NoError(
					t,
					ldb.SetGovernanceProposal(t.Context(), p, txn),
				)
			})
			write(func(txn ledgerdb.Txn) {
				p := proposal(slots.P1, slots.Base, 0)
				p.GovActionCbor = []byte{0x02, 0x03}
				require.NoError(
					t,
					ldb.SetGovernanceProposal(t.Context(), p, txn),
				)
			})
			write(func(txn ledgerdb.Txn) {
				p := proposal(slots.P2, slots.Submit, 1)
				p.DroppedEpoch, p.DroppedSlot = u64(
					slots.Epoch,
				), u64(
					slots.Drop,
				)
				require.NoError(
					t,
					ldb.SetGovernanceProposal(t.Context(), p, txn),
				)
			})
			write(func(txn ledgerdb.Txn) {
				p := ratified(slots.RatifyAgain)
				p.GovActionCbor = nil
				p.EnactedEpoch, p.EnactedSlot = u64(
					slots.Epoch,
				), u64(
					slots.Enact,
				)
				require.NoError(
					t,
					ldb.SetGovernanceProposal(t.Context(), p, txn),
				)
			})
			return slots
		},
		ProposalsInsideTxn: func(slot uint64) {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			index := uint32(2)
			hash := bytes.Repeat([]byte{0x93}, 32)
			p := &models.GovernanceProposal{
				TxHash: hash, ActionType: 1, ExpiresEpoch: h.ls.currentEpoch.EpochId + 5,
				AnchorURL: "https://example.test/p3", AnchorHash: bytes.Repeat([]byte{8}, 32),
				ReturnAddress: bytes.Repeat([]byte{0xe0}, 29), AddedSlot: slot,
				TxIndex: &index,
			}
			require.NoError(t, ldb.SetGovernanceProposal(t.Context(), p, txn))
			require.NoError(
				t,
				ldb.SetGovernanceVote(t.Context(), &models.GovernanceVote{
					ProposalID: p.ID, VoterType: models.VoterTypeDRep,
					VoterCredential: bytes.Repeat(
						[]byte{0xd3},
						28,
					), Vote: models.VoteYes,
					AddedSlot: slot,
				}, txn),
			)
			got, err := ldb.GetGovernanceProposal(t.Context(), hash, 0, txn)
			require.NoError(t, err)
			require.NotNil(t, got)
			votes, err := ldb.GetGovernanceVotes(t.Context(), p.ID, txn)
			require.NoError(t, err)
			require.Len(t, votes, 1)
			set, err := ldb.GetGovernanceProposalSet(t.Context(), txn)
			require.NoError(t, err)
			require.NotEmpty(t, set)
		},
		Donate: func(amount uint64) {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			require.NoError(t, ldb.AddNetworkDonation(
				h.ls.currentEpoch.StartSlot+1, h.ls.currentEpoch.EpochId,
				amount, txn,
			))
			require.NoError(t, txn.Commit())
		},
		Credit: func(ref models.StakeCredentialRef, amount uint64) {
			ldb := h.ls.LedgerDB()
			slot := h.ls.currentEpoch.StartSlot + 2
			txn := ldb.Begin(t.Context(), true)
			require.NoError(t, ldb.AddAccountRewardByCredential(
				t.Context(), ref.Tag, ref.Key, amount, slot, []byte{1}, txn,
			))
			require.NoError(t, ldb.AddAccountRewardsByCredential(
				t.Context(), []models.AccountRewardCredit{
					{
						StakingKey:    ref.Key,
						CredentialTag: ref.Tag,
						Amount:        amount,
						Slot:          slot,
						SourceHash:    []byte{1},
					},
					{
						StakingKey:    ref.Key,
						CredentialTag: ref.Tag,
						Amount:        amount,
						Slot:          slot,
						SourceHash:    []byte{2},
					},
				}, txn,
			))
			require.NoError(t, txn.Commit())
		},
		Withdraw: func(ref models.StakeCredentialRef, amount uint64) error {
			body := newRowBatchingTx(t, map[uint]any{
				0: []any{[]any{bytes.Repeat([]byte{0x77}, 32), uint64(0)}},
				1: []any{},
				2: uint64(100_000),
				5: map[cbor.ByteString]uint64{
					cbor.NewByteString(append([]byte{0xe0}, ref.Key...)): amount,
				},
			})
			tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
			require.NoError(t, err)
			ldb := h.ls.LedgerDB()
			slot := h.ls.currentEpoch.StartSlot + 3
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			var txHash [32]byte
			copy(txHash[:], tx.Hash().Bytes())
			offsets := &database.BlockIngestionResult{
				TxOffsets: map[[32]byte]database.CborOffset{txHash: {}},
			}
			if err := ldb.SetTransactionWithOpts(
				t.Context(), tx,
				ocommon.NewPoint(slot, bytes.Repeat([]byte{0x33}, 32)),
				0, 0, nil, nil, offsets, txn, database.BatchedTxIngestOpts{},
			); err != nil {
				return err
			}
			return txn.Commit()
		},
		Reap: func(pool []byte) {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			require.NoError(t, ldb.ClearDelegationsToRetiredPool(
				pool, h.ls.currentEpoch.StartSlot+4, txn,
			))
			require.NoError(t, txn.Commit())
		},
		Expire: func(ref models.StakeCredentialRef) {
			h.ls.config.DelegatorInactivityEnabled = true
			h.ls.config.DelegatorInactivity = 20
			ldb := h.ls.LedgerDB()
			epoch := h.ls.currentEpoch.EpochId
			start := h.ls.currentEpoch.StartSlot
			txn := ldb.Begin(t.Context(), true)
			require.NoError(t, h.ls.activateDelegatorInactivityIfNeeded(
				t.Context(), txn, epoch, start+5,
			))
			require.NoError(t, ldb.RenewAccountExpirations(
				t.Context(), []models.StakeCredentialRef{ref}, epoch+30, txn,
			))
			h.ls.stageAccountExpirations(
				txn, []models.StakeCredentialRef{ref}, epoch+30, start+6,
			)
			require.NoError(t, txn.Commit())
		},
		ExpireAtBoundary: func(ref models.StakeCredentialRef) {
			h.ls.config.DelegatorInactivityEnabled = true
			h.ls.config.DelegatorInactivity = 20
			ldb := h.ls.LedgerDB()
			cur := h.ls.currentEpoch
			next := cur.StartSlot + uint64(cur.LengthInSlots)
			entered := cur.EpochId + 1
			txn := ldb.Begin(t.Context(), true)
			require.NoError(t, ldb.SetEpoch(
				next, entered, nil, nil, nil, nil,
				cur.EraId, cur.SlotLength, cur.LengthInSlots, txn,
			))
			require.NoError(t, h.ls.activateDelegatorInactivityIfNeeded(
				t.Context(), txn, entered, next,
			))
			require.NoError(t, ldb.RenewAccountExpirations(
				t.Context(), []models.StakeCredentialRef{ref}, entered+30, txn,
			))
			h.ls.stageAccountExpirations(
				txn, []models.StakeCredentialRef{ref}, entered+30, next+1,
			)
			require.NoError(t, txn.Commit())
		},
		Governance: func(ref models.StakeCredentialRef, protocolMajor uint64) {
			ldb := h.ls.LedgerDB()
			start := h.ls.currentEpoch.StartSlot
			drepKey := bytes.Repeat([]byte{0x5d}, 28)
			otherKey := bytes.Repeat([]byte{0x5e}, 28)
			stake := []any{uint64(ref.Tag), ref.Key}
			submit := func(slot uint64, seed byte, deposits map[int]uint64, certs ...[]any) {
				body := newRowBatchingTx(t, map[uint]any{
					0: []any{[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)}},
					1: []any{},
					2: uint64(100_000),
					4: certs,
				})
				tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
				require.NoError(t, err)
				var txHash [32]byte
				copy(txHash[:], tx.Hash().Bytes())
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				require.NoError(t, ldb.SetTransactionWithOpts(
					t.Context(),
					tx,
					ocommon.NewPoint(slot, bytes.Repeat([]byte{seed}, 32)),
					0,
					0,
					nil,
					deposits,
					&database.BlockIngestionResult{
						TxOffsets: map[[32]byte]database.CborOffset{txHash: {}},
					},
					txn,
					database.BatchedTxIngestOpts{ProtocolMajor: protocolMajor},
				))
				require.NoError(t, txn.Commit())
			}
			inTxn := func(fn func(txn ledgerdb.Txn)) {
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				fn(txn)
				require.NoError(t, txn.Commit())
			}
			submit(
				start+7,
				0x61,
				map[int]uint64{0: 500_000_000},
				[]any{
					uint64(16),
					[]any{uint64(0), drepKey},
					uint64(500_000_000),
					nil,
				},
				[]any{uint64(9), stake, []any{uint64(0), drepKey}},
			)
			inTxn(func(txn ledgerdb.Txn) {
				require.NoError(t, ldb.UpdateDRepActivity(
					t.Context(), 0, drepKey, h.ls.currentEpoch.EpochId, 20,
					start+8, txn,
				))
				_, err := ldb.BumpDormantDRepExpiries(t.Context(), start+8, txn)
				require.NoError(t, err)
			})
			inTxn(func(txn ledgerdb.Txn) {
				require.NoError(
					t,
					ldb.ResetDormantDRepEpochs(t.Context(), start+9, txn),
				)
				require.NoError(t, ldb.UpdateDRepActivity(
					t.Context(), 0, drepKey, h.ls.currentEpoch.EpochId+1, 20,
					start+9, txn,
				))
			})
			submit(
				start+10,
				0x62,
				map[int]uint64{0: 500_000_000},
				[]any{
					uint64(17),
					[]any{uint64(0), drepKey},
					uint64(500_000_000),
				},
			)
			submit(start+11, 0x63, nil,
				[]any{uint64(9), stake, []any{uint64(0), otherKey}},
			)
			inTxn(func(txn ledgerdb.Txn) {
				_, err := ldb.ClearDanglingDRepDelegations(
					t.Context(),
					start+12,
					txn,
				)
				require.NoError(t, err)
			})
		},
		Pools: func(beforeBoundary func()) PoolSlots {
			ldb := h.ls.LedgerDB()
			cur := h.ls.currentEpoch
			start := cur.StartSlot
			newPool := bytes.Repeat([]byte{0x6a}, 28)
			reward := append([]byte{0xe0}, bytes.Repeat([]byte{0x6b}, 28)...)
			vrfA := bytes.Repeat([]byte{0x6c}, 32)
			vrfB := bytes.Repeat([]byte{0x6d}, 32)
			poolCert := func(pledge uint64, vrf []byte) []any {
				return []any{
					uint64(3), newPool, vrf,
					pledge, uint64(340_000_000),
					cbor.Tag{Number: 30, Content: []uint64{1, 10}},
					reward, []any{bytes.Repeat([]byte{0x6b}, 28)},
					[]any{[]any{uint64(1), uint64(3001), "relay.example"}}, nil,
				}
			}
			submit := func(slot uint64, seed byte, deposits map[int]uint64, certs ...[]any) {
				h.coverSlot(slot)
				body := newRowBatchingTx(t, map[uint]any{
					0: []any{[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)}},
					1: []any{},
					2: uint64(100_000),
					4: certs,
				})
				tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
				require.NoError(t, err)
				var txHash [32]byte
				copy(txHash[:], tx.Hash().Bytes())
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				require.NoError(t, ldb.SetTransactionWithOpts(
					t.Context(), tx,
					ocommon.NewPoint(slot, bytes.Repeat([]byte{seed}, 32)),
					0, 0, nil, deposits,
					&database.BlockIngestionResult{
						TxOffsets: map[[32]byte]database.CborOffset{txHash: {}},
					},
					txn, database.BatchedTxIngestOpts{ProtocolMajor: 10},
				))
				require.NoError(t, txn.Commit())
			}
			submit(
				start+20,
				0x71,
				map[int]uint64{0: 500_000_000},
				poolCert(1_000_000, vrfA),
			)
			submit(
				start+21,
				0x72,
				map[int]uint64{0: 500_000_000},
				poolCert(2_000_000, vrfB),
			)
			submit(start+22, 0x73, nil,
				[]any{uint64(4), h.poolHash, cur.EpochId + 1})
			beforeBoundary()
			next := start + uint64(cur.LengthInSlots)
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			require.NoError(t, ldb.SetEpoch(
				next, cur.EpochId+1, nil, nil, nil, nil,
				cur.EraId, cur.SlotLength, cur.LengthInSlots, txn,
			))
			h.ls.stagePoolBoundary(txn, cur.EpochId+1, next)
			require.NoError(t, txn.Commit())
			return PoolSlots{
				Pool: newPool, Retired: h.poolHash, VrfA: vrfA, VrfB: vrfB,
				Register: start + 20, Rotate: start + 21, Retire: start + 22,
				RetireEpoch: cur.EpochId + 1,
				EpochStart:  start, Boundary: next, NextEpoch: cur.EpochId + 1,
			}
		},
		CertifiedPools: func(beforeBoundary func()) CertifiedPoolSlots {
			ldb := h.ls.LedgerDB()
			cur := h.ls.currentEpoch
			// The harness's own epochs are two slots long; the certificates
			// need room, so two long epochs follow them.
			const epochLength = 1000
			start := cur.StartSlot + 10_000
			next := start + epochLength
			newPool := bytes.Repeat([]byte{0x7a}, 28)
			reward := append([]byte{0xe0}, bytes.Repeat([]byte{0x7b}, 28)...)
			vrfA := bytes.Repeat([]byte{0x7c}, 32)
			vrfB := bytes.Repeat([]byte{0x7d}, 32)
			poolCert := func(pledge uint64, vrf []byte) []any {
				return []any{
					uint64(3), newPool, vrf,
					pledge, uint64(340_000_000),
					cbor.Tag{Number: 30, Content: []uint64{1, 10}},
					reward, []any{bytes.Repeat([]byte{0x7b}, 28)},
					[]any{[]any{uint64(1), uint64(3001), "relay.example"}}, nil,
				}
			}
			submit := func(slot, ctxSlot uint64, seed byte, deposits map[int]uint64, certs ...[]any) {
				h.coverSlot(ctxSlot)
				h.coverSlot(slot)
				body := newRowBatchingTx(t, map[uint]any{
					0: []any{[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)}},
					1: []any{},
					2: uint64(100_000),
					4: certs,
				})
				tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
				require.NoError(t, err)
				var txHash [32]byte
				copy(txHash[:], tx.Hash().Bytes())
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				require.NoError(t, ldb.SetTransactionWithOpts(
					t.Context(), tx,
					ocommon.NewPoint(slot, bytes.Repeat([]byte{seed}, 32)),
					0, 0, nil, deposits,
					&database.BlockIngestionResult{
						TxOffsets: map[[32]byte]database.CborOffset{txHash: {}},
					},
					txn, database.BatchedTxIngestOpts{
						ProtocolMajor:             10,
						SkipConsumedInputRecovery: true,
						LedgerContextSlot:         &ctxSlot,
					},
				))
				require.NoError(t, txn.Commit())
			}
			s := CertifiedPoolSlots{
				Pool: newPool, Retired: h.poolHash, VrfA: vrfA, VrfB: vrfB,
				RegA: start + 20, CtxA: start + 10,
				RegB: start + 22, CtxB: start + 8,
				RetP: start + 30, CtxRetP: start + 12,
				RetireEpoch: cur.EpochId + 2,
				EpochStart:  start, Boundary: next, NextEpoch: cur.EpochId + 2,
				RetN: next + 5, CtxRetN: next - 3,
			}
			openEpoch := func(slot, id uint64) {
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				require.NoError(t, ldb.SetEpoch(
					slot, id, nil, nil, nil, nil,
					cur.EraId, cur.SlotLength, epochLength, txn,
				))
				h.ls.stagePoolBoundary(txn, id, slot)
				require.NoError(t, txn.Commit())
			}
			openEpoch(start, cur.EpochId+1)
			submit(s.RegA, s.CtxA, 0x81, map[int]uint64{0: 500_000_000},
				poolCert(1_000_000, vrfA))
			submit(s.RegB, s.CtxB, 0x82, map[int]uint64{0: 500_000_000},
				poolCert(2_000_000, vrfB))
			submit(s.RetP, s.CtxRetP, 0x83, nil,
				[]any{uint64(4), h.poolHash, s.RetireEpoch})
			if beforeBoundary != nil {
				beforeBoundary()
			}
			openEpoch(next, cur.EpochId+2)
			submit(s.RetN, s.CtxRetN, 0x84, nil,
				[]any{uint64(4), newPool, s.RetireEpoch + 1})
			return s
		},
		CertifiedBoundaryStake: func(withdraw bool) CertifiedBoundaryStakeSlots {
			return h.certifiedBoundaryStake(
				CertifiedBoundaryOptions{WithdrawAcrossSnapshot: withdraw},
			)
		},
		CertifiedBoundaryStakeWith: h.certifiedBoundaryStake,
		CertifiedDelegations: func() CertifiedDelegationSlots {
			ldb := h.ls.LedgerDB()
			start := h.ls.currentEpoch.StartSlot
			s := CertifiedDelegationSlots{
				X:     models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0xf1}, 28)),
				Y:     models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0xf2}, 28)),
				PoolA: h.poolHash, PoolB: bytes.Repeat([]byte{0xf3}, 28),
				Reg: start + 45, Plain: start + 50,
				D1: start + 60, E1: start + 44, D2: start + 70, E2: start + 41,
				Boundary: start + 66,
			}
			cred := func(ref models.StakeCredentialRef) []any {
				return []any{uint64(ref.Tag), ref.Key}
			}
			closure := func(slot, ctxSlot uint64, seed byte, certs ...[]any) {
				h.coverSlot(ctxSlot)
				h.coverSlot(slot)
				body := newRowBatchingTx(t, map[uint]any{
					0: []any{[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)}},
					1: []any{},
					2: uint64(100_000),
					4: certs,
				})
				tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
				require.NoError(t, err)
				var txHash [32]byte
				copy(txHash[:], tx.Hash().Bytes())
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				require.NoError(t, ldb.SetTransactionWithOpts(
					t.Context(), tx,
					ocommon.NewPoint(slot, bytes.Repeat([]byte{seed}, 32)),
					0, 0, nil, nil,
					&database.BlockIngestionResult{
						TxOffsets: map[[32]byte]database.CborOffset{txHash: {}},
					},
					txn, database.BatchedTxIngestOpts{
						ProtocolMajor:             10,
						SkipConsumedInputRecovery: true,
						LedgerContextSlot:         &ctxSlot,
					},
				))
				require.NoError(t, txn.Commit())
			}
			h.coverSlot(s.Reg)
			h.coverSlot(s.Plain)
			submitCertificates(t, ldb, s.Reg, 0xc0,
				map[int]uint64{0: 2_000_000, 1: 2_000_000},
				[]any{uint64(0), cred(s.X)}, []any{uint64(0), cred(s.Y)})
			submitCertificates(t, ldb, s.Plain, 0xc1, nil,
				[]any{uint64(2), cred(s.Y), s.PoolA})
			closure(s.D1, s.E1, 0xc2, []any{uint64(2), cred(s.X), s.PoolA})
			closure(s.D2, s.E2, 0xc3, []any{uint64(2), cred(s.X), s.PoolB})
			return s
		},
		CertifiedUtxos: func() CertifiedUtxoSlots {
			ldb := h.ls.LedgerDB()
			start := h.ls.currentEpoch.StartSlot
			credA := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0xd1}, 28))
			credB := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0xd2}, 28))
			s := CertifiedUtxoSlots{
				A: credA, B: credB,
				A1: start + 50, E1: start + 40,
				A3: start + 55,
				A2: start + 60, E2: start + 45,
				A4: start + 70, E4: start + 42,
				Start: start, Boundary: start + 52, End: start + 70,
			}
			address := func(ref models.StakeCredentialRef) []byte {
				a := []byte{0x00}
				a = append(a, bytes.Repeat([]byte{0x55}, 28)...)
				return append(a, ref.Key...)
			}
			type in struct {
				hash []byte
				idx  uint64
			}
			type out struct {
				ref    models.StakeCredentialRef
				amount uint64
			}
			submit := func(slot uint64, ctxSlot *uint64, seed byte, ins []in, outs ...out) []byte {
				h.coverSlot(slot)
				if ctxSlot != nil {
					h.coverSlot(*ctxSlot)
				}
				inputs := []any{}
				for _, i := range ins {
					inputs = append(inputs, []any{i.hash, i.idx})
				}
				if len(inputs) == 0 {
					inputs = []any{[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)}}
				}
				outputs := []any{}
				for _, o := range outs {
					outputs = append(outputs, []any{address(o.ref), o.amount})
				}
				body := newRowBatchingTx(t, map[uint]any{
					0: inputs, 1: outputs, 2: uint64(100_000),
				})
				tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
				require.NoError(t, err)
				var txHash [32]byte
				copy(txHash[:], tx.Hash().Bytes())
				utxoOffsets := map[database.UtxoRef]database.CborOffset{}
				for i := range outs {
					utxoOffsets[database.UtxoRef{
						TxId: txHash, OutputIdx: uint32(i), //nolint:gosec // a test index
					}] = database.CborOffset{}
				}
				opts := database.BatchedTxIngestOpts{ProtocolMajor: 10}
				if ctxSlot != nil {
					opts.SkipConsumedInputRecovery = true
					opts.LedgerContextSlot = ctxSlot
				}
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				require.NoError(t, ldb.SetTransactionWithOpts(
					t.Context(), tx,
					ocommon.NewPoint(slot, bytes.Repeat([]byte{seed}, 32)),
					0, 0, nil, nil,
					&database.BlockIngestionResult{
						TxOffsets:   map[[32]byte]database.CborOffset{txHash: {}},
						UtxoOffsets: utxoOffsets,
					},
					txn, opts,
				))
				require.NoError(t, txn.Commit())
				point := ocommon.NewPoint(slot, bytes.Repeat([]byte{seed}, 32))
				evt, err := utxoEventFor(point, 0, tx)
				require.NoError(t, err)
				if ctxSlot != nil {
					evt.EffectiveSlot = *ctxSlot
				}
				require.NoError(t, set.ApplyUtxoChanges(&point, []UtxoEvent{evt}))
				return txHash[:]
			}
			// A tip before every transaction gives the set's journal a floor
			// the as-of reads can reach.
			baseline := ocommon.NewPoint(start+30, bytes.Repeat([]byte{0xe0}, 32))
			require.NoError(t, set.ApplyUtxoChanges(&baseline, nil))
			e1, e2, e4 := s.E1, s.E2, s.E4
			c1 := submit(s.A1, &e1, 0xe1, nil, out{credA, 5_000_000}, out{credB, 3_000_000})
			c3 := submit(s.A3, nil, 0xe3, nil, out{credB, 2_000_000})
			c2 := submit(s.A2, &e2, 0xe2, []in{{c1, 0}}, out{credA, 4_000_000})
			c4 := submit(s.A4, &e4, 0xe4, []in{{c1, 1}}, out{credA, 1_000_000})
			s.OA1 = models.UtxoId{Hash: c1, Idx: 0}
			s.OB1 = models.UtxoId{Hash: c1, Idx: 1}
			s.OB2 = models.UtxoId{Hash: c3, Idx: 0}
			s.OA2 = models.UtxoId{Hash: c2, Idx: 0}
			s.OA3 = models.UtxoId{Hash: c4, Idx: 0}
			return s
		},
		CertifiedStake: func() CertifiedStakeSlots {
			ldb := h.ls.LedgerDB()
			start := h.ls.currentEpoch.StartSlot
			s := CertifiedStakeSlots{
				C:    models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0xc1}, 28)),
				D:    models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0xc2}, 28)),
				Reg1: start + 50, Ctx1: start + 40,
				Dereg: start + 60, CtxDereg: start + 45,
				Reg2: start + 70, Ctx2: start + 42,
				Plain: start + 55, Withdraw: start + 80, CtxWithdraw: start + 47,
				Boundary: start + 66,
			}
			require.NoError(t, db.Metadata().IterateAccounts(nil, func(a *models.Account) error {
				if s.Payee.Key == nil {
					s.Payee = models.NewStakeCredentialRef(a.CredentialTag, a.StakingKey)
				}
				return nil
			}))
			require.NotNil(t, s.Payee.Key)
			cred := func(ref models.StakeCredentialRef) []any {
				return []any{uint64(ref.Tag), ref.Key}
			}
			closure := func(slot, ctxSlot uint64, seed byte, deposits map[int]uint64, withdrawals map[cbor.ByteString]uint64, certs ...[]any) {
				h.coverSlot(ctxSlot)
				h.coverSlot(slot)
				fields := map[uint]any{
					0: []any{[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)}},
					1: []any{},
					2: uint64(100_000),
				}
				if len(certs) > 0 {
					fields[4] = certs
				}
				if withdrawals != nil {
					fields[5] = withdrawals
				}
				body := newRowBatchingTx(t, fields)
				tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
				require.NoError(t, err)
				var txHash [32]byte
				copy(txHash[:], tx.Hash().Bytes())
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				require.NoError(t, ldb.SetTransactionWithOpts(
					t.Context(), tx,
					ocommon.NewPoint(slot, bytes.Repeat([]byte{seed}, 32)),
					0, 0, nil, deposits,
					&database.BlockIngestionResult{
						TxOffsets: map[[32]byte]database.CborOffset{txHash: {}},
					},
					txn, database.BatchedTxIngestOpts{
						ProtocolMajor:             10,
						SkipConsumedInputRecovery: true,
						LedgerContextSlot:         &ctxSlot,
					},
				))
				require.NoError(t, txn.Commit())
			}
			closure(s.Reg1, s.Ctx1, 0xb1, map[int]uint64{0: 2_000_000}, nil,
				[]any{uint64(0), cred(s.C)})
			closure(s.Dereg, s.CtxDereg, 0xb2, nil, nil,
				[]any{uint64(1), cred(s.C)})
			closure(s.Reg2, s.Ctx2, 0xb3, map[int]uint64{0: 2_000_000}, nil,
				[]any{uint64(0), cred(s.C)})
			submitCertificates(t, ldb, s.Plain, 0xb4,
				map[int]uint64{0: 2_000_000}, []any{uint64(0), cred(s.D)})
			closure(s.Withdraw, s.CtxWithdraw, 0xb5, nil,
				map[cbor.ByteString]uint64{
					cbor.NewByteString(append([]byte{0xe0}, s.Payee.Key...)): 0,
				})
			return s
		},
		Registrations: func() RegistrationSlots {
			ldb := h.ls.LedgerDB()
			start := h.ls.currentEpoch.StartSlot
			slots := RegistrationSlots{
				K: bytes.Repeat(
					[]byte{0x94},
					28,
				), L: bytes.Repeat([]byte{0x95}, 28),
				M: bytes.Repeat(
					[]byte{0x96},
					28,
				), D: bytes.Repeat([]byte{0x97}, 28),
				E:  bytes.Repeat([]byte{0x98}, 28),
				S1: start + 30, S2: start + 31, S3: start + 32, S4: start + 33,
				S5: start + 34, DRegister: start + 35, DUpdate: start + 36,
				DRetire: start + 37, DReReg: start + 38,
			}
			// Through the LedgerDB, so a drained accounts family is read from
			// the set.
			active, err := ldb.GetActiveAccountCredentials(nil)
			require.NoError(t, err)
			slices.SortFunc(active, compareRefs)
			require.NotEmpty(t, active)
			slots.Imported = active[0]
			cred := func(key []byte) []any { return []any{uint64(0), key} }
			submitCertificates(t, ldb, slots.S1, 0xa1,
				map[int]uint64{0: 2_000_000, 2: 1_000_000},
				[]any{uint64(0), cred(slots.K)},
				[]any{uint64(0), cred(slots.L)},
				[]any{uint64(7), cred(slots.M), uint64(1_000_000)},
			)
			submitCertificates(t, ldb, slots.S2, 0xa2, nil,
				[]any{uint64(1), cred(slots.K)})
			submitCertificates(t, ldb, slots.S3, 0xa3,
				map[int]uint64{0: 3_000_000},
				[]any{uint64(7), cred(slots.K), uint64(3_000_000)})
			submitCertificates(t, ldb, slots.S4, 0xa4, nil,
				[]any{uint64(8), cred(slots.K), uint64(3_000_000)})
			submitCertificates(t, ldb, slots.S5, 0xa5,
				map[int]uint64{0: 4_000_000},
				[]any{uint64(7), cred(slots.K), uint64(4_000_000)})
			submitCertificates(t, ldb, slots.DRegister, 0xa6,
				map[int]uint64{0: 500_000_000, 1: 700_000_000},
				[]any{uint64(16), cred(slots.D), uint64(500_000_000), nil},
				[]any{uint64(16), cred(slots.E), uint64(700_000_000), nil})
			submitCertificates(t, ldb, slots.DUpdate, 0xa7, nil,
				[]any{uint64(18), cred(slots.D), nil})
			submitCertificates(t, ldb, slots.DRetire, 0xa8, nil,
				[]any{uint64(17), cred(slots.D), uint64(500_000_000)},
				[]any{uint64(17), cred(slots.E), uint64(700_000_000)})
			submitCertificates(t, ldb, slots.DReReg, 0xa9,
				map[int]uint64{0: 600_000_000},
				[]any{uint64(16), cred(slots.D), uint64(600_000_000), nil})
			return slots
		},
		Witnesses: func(around bool) WitnessSlots {
			var ldb ledgerdb.LedgerDB = h.ls.LedgerDB()
			if around {
				ldb = dbadapter.New(db)
			}
			start := h.ls.currentEpoch.StartSlot
			s := WitnessSlots{
				C: models.NewStakeCredentialRef(
					0,
					bytes.Repeat([]byte{0xe1}, 28),
				),
				Pool:     bytes.Repeat([]byte{0xe3}, 28),
				Kept:     h.fixture.ancestorTip.Point.Slot,
				Register: start + 40, Delegate: start + 41, VoteDelegate: start + 42,
				Credit: start + 43, Withdraw: start + 44, ZeroSkipped: start + 45,
				ZeroRecorded: start + 46, Deregister: start + 47,
			}
			require.NoError(
				t,
				db.Metadata().
					IterateAccounts(nil, func(a *models.Account) error {
						if s.Payee.Key == nil {
							s.Payee = models.NewStakeCredentialRef(
								a.CredentialTag,
								a.StakingKey,
							)
						}
						return nil
					}),
			)
			require.NotNil(t, s.Payee.Key)
			cred := func(ref models.StakeCredentialRef) []any {
				return []any{uint64(ref.Tag), ref.Key}
			}
			withdraw := func(slot uint64, seed byte, amount uint64, skipWitness bool) {
				body := newRowBatchingTx(t, map[uint]any{
					0: []any{[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)}},
					1: []any{},
					2: uint64(100_000),
					5: map[cbor.ByteString]uint64{
						cbor.NewByteString(append([]byte{0xe0}, s.Payee.Key...)): amount,
					},
				})
				tx, err := conway.NewConwayTransactionFromCbor(body.cbor)
				require.NoError(t, err)
				var txHash [32]byte
				copy(txHash[:], tx.Hash().Bytes())
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				require.NoError(t, ldb.SetTransactionWithOpts(
					t.Context(),
					tx,
					ocommon.NewPoint(slot, bytes.Repeat([]byte{seed}, 32)),
					0,
					0,
					nil,
					nil,
					&database.BlockIngestionResult{
						TxOffsets: map[[32]byte]database.CborOffset{txHash: {}},
					},
					txn,
					database.BatchedTxIngestOpts{
						SkipWithdrawalWitnessWrite: skipWitness,
					},
				))
				require.NoError(t, txn.Commit())
			}
			submitCertificates(t, ldb, s.Kept, 0xb1, nil,
				[]any{uint64(2), cred(s.Payee), s.Pool})
			submitCertificates(t, ldb, s.Register, 0xb2,
				map[int]uint64{0: 2_000_000},
				[]any{uint64(0), cred(s.C)})
			submitCertificates(t, ldb, s.Delegate, 0xb3, nil,
				[]any{uint64(2), cred(s.C), s.Pool})
			submitCertificates(t, ldb, s.VoteDelegate, 0xb4, nil,
				[]any{uint64(9), cred(s.C), []any{uint64(2)}})
			txn := ldb.Begin(t.Context(), true)
			require.NoError(t, ldb.AddAccountRewardByCredential(
				t.Context(),
				s.Payee.Tag,
				s.Payee.Key,
				10,
				s.Credit,
				[]byte{0xb5},
				txn,
			))
			require.NoError(t, txn.Commit())
			withdraw(s.Withdraw, 0xb6, 4, true)
			withdraw(s.ZeroSkipped, 0xb7, 0, true)
			withdraw(s.ZeroRecorded, 0xb8, 0, false)
			submitCertificates(t, ldb, s.Deregister, 0xb9, nil,
				[]any{uint64(1), cred(s.C)})
			return s
		},
		WitnessesInsideRollback: func(refs []models.StakeCredentialRef) map[string]uint64 {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			require.NoError(t, h.ls.stageUtxoRollbackEvent(
				t.Context(), txn, h.fixture.ancestorTip.Point,
			))
			got, err := ldb.AccountLastWitnessSlots(
				t.Context(), refs, math.MaxInt64, txn,
			)
			require.NoError(t, err)
			return got
		},
		WitnessesInsideTxn: func(key []byte, slot uint64) (uint64, bool) {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			writeCertificates(t, ldb, txn, slot, 0xba,
				map[int]uint64{0: 2_000_000},
				[]any{uint64(0), []any{uint64(0), key}})
			ref := models.NewStakeCredentialRef(0, key)
			got, err := ldb.AccountLastWitnessSlots(
				t.Context(), []models.StakeCredentialRef{ref}, slot, txn,
			)
			require.NoError(t, err)
			last, ok := got[ref.MapKey()]
			return last, ok
		},
		RegistrationsInsideTxn: func(slot uint64) ([]models.AccountRegistrationHistoryRow, *uint64) {
			ldb := h.ls.LedgerDB()
			key := bytes.Repeat([]byte{0x9a}, 28)
			drep := bytes.Repeat([]byte{0x9b}, 28)
			cred := func(key []byte) []any { return []any{uint64(0), key} }
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			writeCertificates(t, ldb, txn, slot, 0xaa,
				map[int]uint64{0: 2_000_000, 1: 500_000_000},
				[]any{uint64(0), cred(key)},
				[]any{uint64(16), cred(drep), uint64(500_000_000), nil})
			rows, err := ldb.GetAccountRegistrationHistoryByCredential(
				t.Context(), 0, key, 0, 0, "asc", txn,
			)
			require.NoError(t, err)
			deposit, err := ldb.GetDrepLastRegistrationDeposit(
				t.Context(),
				0,
				drep,
				txn,
			)
			require.NoError(t, err)
			return rows, deposit
		},
		GenesisDrep: func(cred []byte, deposit uint64) {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			require.NoError(t, ldb.SetGenesisGovernance(
				conway.ConwayGenesisInitialDReps{
					&lcommon.Credential{
						CredType:   lcommon.CredentialTypeAddrKeyHash,
						Credential: lcommon.CredentialHash(cred),
					}: {Deposit: deposit},
				},
				conway.ConwayGenesisDelegs{}, nil, txn,
			))
			require.NoError(t, txn.Commit())
		},
		RegistrationsInsideRollback: func(key, drep []byte) ([]StakeEvent, *uint64) {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			require.NoError(t, h.ls.stageUtxoRollbackEvent(
				t.Context(), txn, h.fixture.ancestorTip.Point,
			))
			r := h.ls.registrationHistoryReader(txn)
			require.NotNil(t, r)
			events, err := h.ls.stakeHistoryView(
				r, txn, models.NewStakeCredentialRef(0, key),
			)
			require.NoError(t, err)
			deposit, err := h.ls.drepDepositView(r, txn, 0, drep)
			require.NoError(t, err)
			return events, deposit
		},
		StagedRollbackAccountReads: func(pool, drep []byte) []string {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			require.NoError(t, h.ls.stageUtxoRollbackEvent(
				t.Context(), txn, h.fixture.ancestorTip.Point,
			))
			return renderAccountReads(t, ldb, txn, pool, drep)
		},
		PoolKeysInsideRollback: func() [][]byte {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			require.NoError(t, h.ls.stageUtxoRollbackEvent(
				t.Context(), txn, h.fixture.ancestorTip.Point,
			))
			r := h.ls.poolReader(txn)
			require.NotNil(t, r)
			pools, err := h.ls.poolKeys(r, txn)
			require.NoError(t, err)
			return pools
		},
		CommittedAccountReads: func(pool, drep []byte) []string {
			return renderAccountReads(t, h.ls.LedgerDB(), nil, pool, drep)
		},
		RegistrationCredentialsInsideTxn: func(slot, from, to uint64) []models.StakeCredentialRef {
			ldb := h.ls.LedgerDB()
			key := bytes.Repeat([]byte{0x9a}, 28)
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			writeCertificates(t, ldb, txn, slot, 0xaa,
				map[int]uint64{0: 2_000_000},
				[]any{uint64(0), []any{uint64(0), key}})
			refs, err := ldb.GetStakeCredentialsWithRegistrationEvents(
				from, to, txn,
			)
			require.NoError(t, err)
			return refs
		},
		RegistrationCredentialsInsideRollback: func(from, to uint64) []models.StakeCredentialRef {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			require.NoError(t, h.ls.stageUtxoRollbackEvent(
				t.Context(), txn, h.fixture.ancestorTip.Point,
			))
			refs, err := ldb.GetStakeCredentialsWithRegistrationEvents(
				from, to, txn,
			)
			require.NoError(t, err)
			return refs
		},
		CertHistoryReads: func() (float64, float64) {
			return promtestutil.ToFloat64(
					h.ls.metrics.certHistoryReadsVerified,
				),
				promtestutil.ToFloat64(
					h.ls.metrics.certHistoryReadMismatches,
				)
		},
		GenesisMIR: func(beforeBoundary func()) GenesisMIRSlots {
			ldb := h.ls.LedgerDB()
			cur := h.ls.currentEpoch
			start := cur.StartSlot
			s := GenesisMIRSlots{
				Genesis:   bytes.Repeat([]byte{0x81}, 28),
				DelegateA: bytes.Repeat([]byte{0x82}, 28),
				DelegateB: bytes.Repeat([]byte{0x83}, 28),
				VrfA:      bytes.Repeat([]byte{0x84}, 32),
				VrfB:      bytes.Repeat([]byte{0x85}, 32),
				Owner:     bytes.Repeat([]byte{0x71}, 28),
				Delegator: bytes.Repeat([]byte{0x72}, 28),
				DelegA:    start, DelegB: start + 1,
				Distribute: start + 1, ToTreasury: start + 1,
				ToReserves:      start + 1,
				EpochStart:      start,
				Boundary:        start + uint64(cur.LengthInSlots),
				NextEpoch:       cur.EpochId + 1,
				ReservesToOwner: 300, ReservesToDelegator: 100,
				TreasuryToOwner: 50, ToTreasuryAmount: 120,
				ToReservesAmount: 40,
			}
			certBytes := func(cert any) cbor.RawMessage {
				raw, err := cbor.Encode(cert)
				require.NoError(t, err)
				return raw
			}
			credential := func(key []byte) *lcommon.Credential {
				return &lcommon.Credential{
					CredType: lcommon.CredentialTypeAddrKeyHash,
					Credential: lcommon.CredentialHash(
						lcommon.NewBlake2b224(key),
					),
				}
			}
			genesisCert := func(delegate, vrf []byte) cbor.RawMessage {
				return certBytes(&lcommon.GenesisKeyDelegationCertificate{
					CertType: uint(
						lcommon.CertificateTypeGenesisKeyDelegation,
					),
					GenesisHash:         s.Genesis,
					GenesisDelegateHash: delegate,
					VrfKeyHash:          lcommon.NewBlake2b256(vrf),
				})
			}
			distribute := func(source uint, rewards map[*lcommon.Credential]*big.Int) cbor.RawMessage {
				return certBytes(&lcommon.MoveInstantaneousRewardsCertificate{
					CertType: uint(
						lcommon.CertificateTypeMoveInstantaneousRewards,
					),
					Reward: lcommon.MoveInstantaneousRewardsCertificateReward{
						Source: source, Rewards: rewards,
					},
				})
			}
			transfer := func(source uint, amount uint64) cbor.RawMessage {
				return certBytes(&lcommon.MoveInstantaneousRewardsCertificate{
					CertType: uint(
						lcommon.CertificateTypeMoveInstantaneousRewards,
					),
					Reward: lcommon.MoveInstantaneousRewardsCertificateReward{
						Source: source, OtherPot: amount,
					},
				})
			}
			submit := func(slot uint64, seed byte, index uint32, certs ...cbor.RawMessage) {
				txCbor, err := cbor.Encode([]any{
					map[uint]any{
						0: []any{
							[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)},
						},
						1: []any{},
						2: uint64(100_000),
						4: certs,
					},
					map[uint]any{}, true, nil,
				})
				require.NoError(t, err)
				tx, err := babbage.NewBabbageTransactionFromCbor(txCbor)
				require.NoError(t, err)
				require.Len(t, tx.Certificates(), len(certs))
				var txHash [32]byte
				copy(txHash[:], tx.Hash().Bytes())
				txn := ldb.Begin(t.Context(), true)
				defer txn.Release()
				require.NoError(t, ldb.SetTransactionWithOpts(
					t.Context(),
					tx,
					ocommon.NewPoint(
						slot,
						bytes.Repeat([]byte{byte(slot)}, 32),
					),
					index,
					0,
					nil,
					nil,
					&database.BlockIngestionResult{
						TxOffsets: map[[32]byte]database.CborOffset{txHash: {}},
					},
					txn,
					database.BatchedTxIngestOpts{ProtocolMajor: 6},
				))
				require.NoError(t, txn.Commit())
			}
			submit(s.DelegA, 0x91, 0, genesisCert(s.DelegateA, s.VrfA))
			submit(s.Distribute, 0x92, 0,
				distribute(0, map[*lcommon.Credential]*big.Int{
					credential(s.Owner): new(
						big.Int,
					).SetUint64(s.ReservesToOwner),
					credential(s.Delegator): new(
						big.Int,
					).SetUint64(s.ReservesToDelegator),
				}),
				distribute(1, map[*lcommon.Credential]*big.Int{
					credential(s.Owner): new(
						big.Int,
					).SetUint64(s.TreasuryToOwner),
				}),
			)
			submit(s.ToTreasury, 0x93, 1, transfer(0, s.ToTreasuryAmount))
			submit(s.ToReserves, 0x94, 2, transfer(1, s.ToReservesAmount))
			submit(s.DelegB, 0x95, 3, genesisCert(s.DelegateB, s.VrfB))
			beforeBoundary()
			h.applyBoundary()
			return s
		},
		CertHistoryInsideRollback: func() (int, int) {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			require.NoError(t, h.ls.stageUtxoRollbackEvent(
				t.Context(), txn, h.fixture.ancestorTip.Point,
			))
			delegs, err := ldb.GetGenesisDelegationsInSlotRange(
				0,
				math.MaxInt64,
				txn,
			)
			require.NoError(t, err)
			mirs, err := ldb.GetMIRCertsInSlotRange(
				t.Context(),
				0,
				math.MaxInt64,
				txn,
			)
			require.NoError(t, err)
			return len(delegs), len(mirs)
		},
		NetworkInsideRollback: func() (uint64, uint64) {
			ldb := h.ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			require.NoError(t, h.ls.stageUtxoRollbackEvent(
				t.Context(), txn, h.fixture.ancestorTip.Point,
			))
			state, err := ldb.GetNetworkState(txn)
			require.NoError(t, err)
			require.NotNil(t, state)
			sum, err := ldb.SumNetworkDonationsForEpoch(
				h.ls.currentEpoch.EpochId, txn,
			)
			require.NoError(t, err)
			return state.Slot, sum
		},
	}
}

// renderPParamState renders every protocol parameter row, the proposals
// targeting each early epoch and the synthetic PlutusV2 markers as ldb
// answers them, through each of its reads.
func renderPParamState(t *testing.T, ldb ledgerdb.LedgerDB) []string {
	t.Helper()
	var out []string
	for era := range uint(8) {
		rows, err := ldb.ListPParamsForEra(era, nil)
		require.NoError(t, err)
		for _, p := range rows {
			out = append(out, fmt.Sprintf(
				"pparams era %d epoch %d slot %d %x",
				p.EraId, p.Epoch, p.AddedSlot, p.Cbor,
			))
		}
		for epoch := range uint64(6) {
			var picked []byte
			_, err := ldb.GetPParams(
				epoch, era,
				func(b []byte) (lcommon.ProtocolParameters, error) {
					picked = b
					return nil, nil
				},
				nil,
			)
			require.NoError(t, err)
			out = append(out, fmt.Sprintf(
				"pick era %d epoch %d %x", era, epoch, picked,
			))
		}
	}
	for epoch := range uint64(6) {
		updates, err := ldb.GetPParamUpdates(epoch, nil)
		require.NoError(t, err)
		for _, u := range updates {
			out = append(out, fmt.Sprintf(
				"update for %d genesis %x epoch %d slot %d %x",
				epoch, u.GenesisHash, u.Epoch, u.AddedSlot, u.Cbor,
			))
		}
	}
	for _, key := range []string{
		database.SyntheticV2CostModelSyncKey,
		database.SyntheticV2CostModelClearedEpochSyncKey,
	} {
		v, err := ldb.GetSyncState(key, nil)
		require.NoError(t, err)
		out = append(out, fmt.Sprintf("marker %s %q", key, v))
	}
	return out
}

// renderEpochRows renders every epoch and summary row a LedgerDB answers,
// through each of its epoch and summary reads.
func renderEpochRows(t *testing.T, ldb ledgerdb.LedgerDB) []string {
	t.Helper()
	var out []string
	epochs, err := ldb.GetEpochs(nil)
	require.NoError(t, err)
	var hi uint64
	eraIDs := map[uint]bool{}
	for _, e := range epochs {
		out = append(out, fmt.Sprintf(
			"epoch %d %d %d %d %d %x %x %x %x",
			e.EpochId, e.StartSlot, e.EraId, e.SlotLength, e.LengthInSlots,
			e.Nonce, e.EvolvingNonce, e.CandidateNonce, e.LastEpochBlockNonce,
		))
		one, err := ldb.GetEpoch(e.EpochId, nil)
		require.NoError(t, err)
		require.NotNil(t, one)
		out = append(out, fmt.Sprintf("get %d era %d", one.EpochId, one.EraId))
		bySlot, err := ldb.GetEpochBySlot(e.StartSlot, nil)
		require.NoError(t, err)
		require.NotNil(t, bySlot)
		out = append(out, fmt.Sprintf(
			"slot %d epoch %d", e.StartSlot, bySlot.EpochId,
		))
		hi = max(hi, e.EpochId)
		eraIDs[e.EraId] = true
	}
	for _, era := range slices.Sorted(maps.Keys(eraIDs)) {
		inEra, err := ldb.GetEpochsByEra(era, nil)
		require.NoError(t, err)
		out = append(out, fmt.Sprintf("era %d epochs %d", era, len(inEra)))
	}
	for epoch := uint64(0); epoch <= hi+3; epoch++ {
		sm, err := ldb.GetEpochSummary(epoch, nil)
		require.NoError(t, err)
		if sm == nil {
			continue
		}
		out = append(out, fmt.Sprintf(
			"summary %d %d %d %d %x %d %t", sm.Epoch, sm.TotalActiveStake,
			sm.TotalPoolCount, sm.TotalDelegators, sm.EpochNonce,
			sm.BoundarySlot, sm.SnapshotReady,
		))
		total, err := ldb.GetTotalActiveStake(epoch, "mark", nil)
		require.NoError(t, err)
		out = append(out, fmt.Sprintf("total %d %d", epoch, total))
	}
	latest, err := ldb.GetLatestEpochSummary(nil)
	require.NoError(t, err)
	if latest != nil {
		out = append(out, fmt.Sprintf("latest %d", latest.Epoch))
	}
	return out
}

// MidEpochEraSwitchScenario drives a ledger through a mid-epoch switch from
// Babbage to Conway and a rollback to the block before it.
type MidEpochEraSwitchScenario struct {
	DB *database.Database
	// ApplySwitch applies the blocks up to and including the first Conway
	// block.
	ApplySwitch func()
	// RollbackBeforeSwitch rolls back to the last Babbage block.
	RollbackBeforeSwitch func()
	// PParamsReads is EpochStateReads for protocol parameter reads.
	PParamsReads func() (verified, mismatches float64)
	// EraInsideRollback stages a rollback to the last Babbage block in a
	// write transaction, reads the switched epoch's era inside it, and
	// abandons the transaction.
	EraInsideRollback func() uint
	// LedgerDB is the LedgerDB the ledger reads through.
	LedgerDB func() ledgerdb.LedgerDB
	// DrainFamilies adds mask to the families the ledger treats as drained.
	DrainFamilies func(mask nodesettings.FamilyMask)
	// ReadEpochs renders the epoch and summary rows as the ledger's LedgerDB
	// answers them.
	ReadEpochs func() []string
	// ReadPParams is ClassicUpdateBoundaryScenario.ReadPParams.
	ReadPParams func() []string
	// EpochStateReads reports the verify-mode epoch state reads and how
	// many of them differed from the metadata store.
	EpochStateReads func() (verified, mismatches float64)
	// ComputedResults is BoundaryEpochStateScenario.ComputedResults.
	ComputedResults func(result string) (verified, mismatches float64)
}

// NewMidEpochEraSwitchScenarioForTest returns a MidEpochEraSwitchScenario
// whose ledger writes to set.
func NewMidEpochEraSwitchScenarioForTest(
	t *testing.T,
	set UtxoSetWriter,
	reads string,
) MidEpochEraSwitchScenario {
	t.Helper()
	return NewGuardedMidEpochEraSwitchScenarioForTest(t, set, reads, nil)
}

// NewGuardedMidEpochEraSwitchScenarioForTest is
// NewMidEpochEraSwitchScenarioForTest over a metadata store whose statements
// pass through guard.
func NewGuardedMidEpochEraSwitchScenarioForTest(
	t *testing.T,
	set UtxoSetWriter,
	reads string,
	guard metadata.StatementGuard,
) MidEpochEraSwitchScenario {
	t.Helper()
	past := uint64(midEpochEraDeadline)
	tc := midEpochEraCase(past,
		boundaryEraFollower{babbageBody: true, slot: past + 1},
		boundaryEraFollower{slot: past + 2},
	)
	tc.utxoSet = set
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config:         &database.Config{DataDir: t.TempDir()},
		StatementGuard: guard,
	})
	require.NoError(t, err)
	tc.db = db
	ls, blocks := newBoundaryEraLedgerChain(t, tc)
	ls.config.UtxoSetReads = reads
	return MidEpochEraSwitchScenario{
		DB:              ls.db,
		LedgerDB:        ls.LedgerDB,
		DrainFamilies:   ls.SetDrainedFamilies,
		ReadPParams:     func() []string { return renderPParamState(t, ls.LedgerDB()) },
		ReadEpochs:      func() []string { return renderEpochRows(t, ls.LedgerDB()) },
		ComputedResults: computedResultCounts(ls),
		EpochStateReads: func() (float64, float64) {
			return promtestutil.ToFloat64(ls.metrics.epochStateReadsVerified),
				promtestutil.ToFloat64(ls.metrics.epochStateReadMismatches)
		},
		ApplySwitch: func() {
			require.NoError(t, processMidEpochEraBlocks(t, ls, blocks))
			require.Equal(t, eras.ConwayEraDesc.Id, ls.currentEra.Id)
		},
		PParamsReads: func() (float64, float64) {
			return promtestutil.ToFloat64(ls.metrics.pparamsReadsVerified),
				promtestutil.ToFloat64(ls.metrics.pparamsReadMismatches)
		},
		EraInsideRollback: func() uint {
			before := blocks[1]
			ldb := ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			require.NoError(t, ls.stageUtxoRollbackEvent(
				t.Context(), txn,
				ocommon.NewPoint(before.SlotNumber(), before.Hash().Bytes()),
			))
			e, err := ldb.GetEpoch(ls.currentEpoch.EpochId, txn)
			require.NoError(t, err)
			require.NotNil(t, e)
			return e.EraId
		},
		RollbackBeforeSwitch: func() {
			before := blocks[1]
			require.NoError(t, ls.rollback(
				t.Context(),
				ocommon.NewPoint(before.SlotNumber(), before.Hash().Bytes()),
			))
			require.Equal(t, eras.BabbageEraDesc.Id, ls.currentEra.Id)
			// The switch queues a protocol-version evaluation; let it
			// finish before the test closes the ledger.
			ls.protocolVersionEvalWG.Wait()
		},
	}
}

// computedResultCounts reports the verify-mode results the set computed
// for result and how many differed from the metadata store's.
func computedResultCounts(
	ls *LedgerState,
) func(result string) (float64, float64) {
	return func(result string) (float64, float64) {
		return promtestutil.ToFloat64(
				ls.metrics.setComputedVerified.WithLabelValues(result),
			),
			promtestutil.ToFloat64(
				ls.metrics.setComputedMismatches.WithLabelValues(result),
			)
	}
}

// corruptEpochStore is the metadata store's LedgerDB storing every epoch
// with a different nonce than it is given.
type corruptEpochStore struct {
	ledgerdb.LedgerDB
}

func (c corruptEpochStore) SetEpoch(
	slot, epoch uint64,
	nonce, evolvingNonce, candidateNonce, lastEpochBlockNonce []byte,
	era, slotLength, lengthInSlots uint,
	txn ledgerdb.Txn,
) error {
	return c.LedgerDB.SetEpoch(
		slot, epoch, append([]byte{0xff}, nonce...), evolvingNonce,
		candidateNonce, lastEpochBlockNonce, era, slotLength, lengthInSlots,
		txn,
	)
}

// ClassicUpdateBoundaryScenario crosses into Conway at an epoch boundary
// that enacts a quorum-met classic major bump, and rolls it back.
type ClassicUpdateBoundaryScenario struct {
	DB *database.Database
	// ApplyBoundary applies the boundary block.
	ApplyBoundary func()
	// RollbackBeforeBoundary rolls back to the last block of epoch 0.
	RollbackBeforeBoundary func()
	// LedgerDB is the LedgerDB the ledger reads through.
	LedgerDB func() ledgerdb.LedgerDB
	// PParamsReads reports the verify-mode protocol parameter reads and
	// how many of them differed from the metadata store.
	PParamsReads func() (verified, mismatches float64)
	// ConwayRowsInsideTxn counts the Conway rows an abandoned write
	// transaction sees after it writes one, after it stages a rollback to
	// the last block of epoch 0, and after that rollback and a write.
	ConwayRowsInsideTxn func() (afterWrite, afterRollback, afterBoth int)
	// RollbackTarget is the point RollbackBeforeBoundary rolls back to.
	RollbackTarget ocommon.Point
	// Propose commits a classic transaction carrying update proposals
	// through the ledger's LedgerDB.
	Propose func(p UpdateProposal)
	// ProposeAroundTheLedger commits the same through the metadata store
	// alone, so the live UTxO set never sees the proposals.
	ProposeAroundTheLedger func(p UpdateProposal)
	// PParamUpdateReads reports the verify-mode proposal reads and how many
	// of them differed from the metadata store.
	PParamUpdateReads func() (verified, mismatches float64)
	// Forecast forecasts the parameters the boundary into epoch enacts,
	// reading through the ledger's LedgerDB as header verification does.
	Forecast func(epoch uint64) error
	// EnactInsideTxn enacts the boundary into epoch in an abandoned write
	// transaction, through the ledger's LedgerDB.
	EnactInsideTxn func(epoch uint64) error
	// ForecastInsideTxn forecasts the boundary into epoch inside an
	// abandoned write transaction that has committed p.
	ForecastInsideTxn func(p UpdateProposal, epoch uint64) error
	// ForecastRendered is Forecast returning the forecast parameters
	// rendered as text.
	ForecastRendered func(epoch uint64) (string, error)
	// Updates reads the proposals for epoch through the ledger's LedgerDB.
	Updates func(epoch uint64) ([]models.PParamUpdate, error)
	// HideStoreReads makes the named LedgerDB reads fail against the
	// metadata store from now on, and returns the count of reads that
	// reached it.
	HideStoreReads func(names ...string) (reached func() int64)
	// ComputedResults is BoundaryEpochStateScenario.ComputedResults.
	ComputedResults func(result string) (verified, mismatches float64)
	// DrainFamilies adds mask to the families the ledger treats as drained.
	DrainFamilies func(mask nodesettings.FamilyMask)
	// ReadPParams renders the protocol parameter rows, the update proposals
	// and the synthetic PlutusV2 markers as the ledger's LedgerDB answers
	// them.
	ReadPParams func() []string
	// ObserveRealV2 records, in a committed transaction, that real
	// PlutusV2 cost model data was confirmed at epoch.
	ObserveRealV2 func(epoch uint64)
	// MarkSyntheticV2 records, in a committed transaction, that the
	// PlutusV2 cost model in force is the fabricated default.
	MarkSyntheticV2 func()
}

// UpdateProposal is a classic transaction carrying one max-tx-size update
// proposal from each of the genesis keys Keys names, each key 28 bytes of
// that value.
type UpdateProposal struct {
	Slot, Epoch uint64
	// Index is the transaction's index in its block.
	Index     uint32
	Keys      []byte
	MaxTxSize uint
	// Invalid makes the transaction phase-2 invalid, so the metadata store
	// stores none of its proposals.
	Invalid bool
	// Batched writes the transaction through the batched path.
	Batched bool
}

// NewClassicUpdateBoundaryScenarioForTest returns a
// ClassicUpdateBoundaryScenario whose ledger writes to set.
func NewClassicUpdateBoundaryScenarioForTest(
	t *testing.T,
	set UtxoSetWriter,
	reads string,
) ClassicUpdateBoundaryScenario {
	t.Helper()
	return NewGuardedClassicUpdateBoundaryScenarioForTest(t, set, reads, nil)
}

// NewGuardedClassicUpdateBoundaryScenarioForTest is
// NewClassicUpdateBoundaryScenarioForTest over a file-backed metadata store
// whose statements pass through guard, when guard is not nil.
func NewGuardedClassicUpdateBoundaryScenarioForTest(
	t *testing.T,
	set UtxoSetWriter,
	reads string,
	guard metadata.StatementGuard,
) ClassicUpdateBoundaryScenario {
	t.Helper()
	tc := boundaryEraCase{
		pparamsMajor:       8,
		classicUpdateMajor: 9,
		votingBlocks:       uint64(boundaryEraTestSecurityParam),
		utxoSet:            set,
	}
	if guard != nil {
		db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
			Config:         &database.Config{DataDir: t.TempDir()},
			StatementGuard: guard,
		})
		require.NoError(t, err)
		tc.db = db
	}
	ls, block := newBoundaryEraLedger(t, tc)
	ls.config.UtxoSetReads = reads
	before := ls.currentTip.Point
	// The setup's blocks start forecasts that read proposals; let them
	// finish so a test counts only its own reads.
	ls.protocolVersionEvalWG.Wait()
	propose := func(ldb ledgerdb.LedgerDB, txn ledgerdb.Txn, p UpdateProposal) {
		updates := make(map[lcommon.Blake2b224]cbor.RawMessage)
		for _, k := range p.Keys {
			// Key 3 is maxTxSize in every classic update map.
			u, err := cbor.Encode(map[uint]uint{3: p.MaxTxSize})
			require.NoError(t, err)
			updates[lcommon.NewBlake2b224(bytes.Repeat([]byte{k}, 28))] = u
		}
		// The body is raw CBOR because the update structs encode every
		// absent field as null, which the decoder rejects.
		body := map[uint]any{
			0: []any{[]any{
				binary.BigEndian.AppendUint64(
					bytes.Repeat([]byte{0xb1}, 24), p.Slot<<16|uint64(p.Index),
				),
				uint(0),
			}},
			1: []any{},
			2: uint(0),
			3: p.Slot + 1000,
			6: []any{updates, p.Epoch},
		}
		var tx lcommon.Transaction
		if p.Invalid {
			raw, err := cbor.Encode([]any{body, map[uint]any{}, false, nil})
			require.NoError(t, err)
			tx, err = alonzo.NewAlonzoTransactionFromCbor(raw)
			require.NoError(t, err)
		} else {
			raw, err := cbor.Encode([]any{body, map[uint]any{}, nil})
			require.NoError(t, err)
			tx, err = mary.NewMaryTransactionFromCbor(raw)
			require.NoError(t, err)
		}
		updateEpoch, proposals := tx.ProtocolParameterUpdates()
		require.Equal(t, p.Epoch, updateEpoch)
		require.Len(t, proposals, len(p.Keys))
		var txHash [32]byte
		copy(txHash[:], tx.Hash().Bytes())
		offsets := &database.BlockIngestionResult{
			TxOffsets: map[[32]byte]database.CborOffset{txHash: {}},
		}
		point := ocommon.NewPoint(p.Slot, bytes.Repeat([]byte{0x34}, 32))
		if p.Batched {
			acc := ldb.NewBatchAccumulator()
			require.NoError(t, ldb.SetTransactionBatchedWithOpts(
				t.Context(), tx, point, p.Index, updateEpoch, proposals,
				nil, offsets, acc, txn, database.BatchedTxIngestOpts{},
			))
			require.NoError(t, ldb.FlushBatch(acc, txn))
		} else {
			require.NoError(t, ldb.SetTransactionWithOpts(
				t.Context(), tx, point, p.Index, updateEpoch, proposals,
				nil, offsets, txn, database.BatchedTxIngestOpts{},
			))
		}
	}
	return ClassicUpdateBoundaryScenario{
		DB:              ls.db,
		LedgerDB:        ls.LedgerDB,
		DrainFamilies:   ls.SetDrainedFamilies,
		ReadPParams:     func() []string { return renderPParamState(t, ls.LedgerDB()) },
		ComputedResults: computedResultCounts(ls),
		ObserveRealV2: func(epoch uint64) {
			ldb := ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			require.NoError(t, ls.markRealV2CostModelObserved(epoch, txn))
			require.NoError(t, txn.Commit())
		},
		MarkSyntheticV2: func() {
			ldb := ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			require.NoError(t, ls.persistSyntheticV2CostModel(true, txn))
			require.NoError(t, txn.Commit())
		},
		ApplyBoundary: func() {
			require.NoError(t, processMidEpochEraBlocks(
				t, ls, []gledger.Block{block},
			))
			require.Equal(t, eras.ConwayEraDesc.Id, ls.currentEra.Id)
		},
		RollbackBeforeBoundary: func() {
			require.NoError(t, ls.rollback(t.Context(), before))
			ls.protocolVersionEvalWG.Wait()
		},
		PParamsReads: func() (float64, float64) {
			return promtestutil.ToFloat64(ls.metrics.pparamsReadsVerified),
				promtestutil.ToFloat64(ls.metrics.pparamsReadMismatches)
		},
		RollbackTarget: before,
		Propose: func(p UpdateProposal) {
			ldb := ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			propose(ldb, txn, p)
			require.NoError(t, txn.Commit())
		},
		ProposeAroundTheLedger: func(p UpdateProposal) {
			ldb := dbadapter.New(ls.db)
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			propose(ldb, txn, p)
			require.NoError(t, txn.Commit())
		},
		PParamUpdateReads: func() (float64, float64) {
			return promtestutil.ToFloat64(ls.metrics.pparamUpdateReadsVerified),
				promtestutil.ToFloat64(ls.metrics.pparamUpdateReadMismatches)
		},
		Forecast: func(epoch uint64) error {
			_, err := ls.forecastPendingPParamUpdate(
				ls.currentEra, epoch, ls.currentPParams,
			)
			return err
		},
		ForecastRendered: func(epoch uint64) (string, error) {
			pp, err := ls.forecastPendingPParamUpdate(
				ls.currentEra, epoch, ls.currentPParams,
			)
			return fmt.Sprintf("%+v", pp), err
		},
		Updates: func(epoch uint64) ([]models.PParamUpdate, error) {
			return ls.LedgerDB().GetPParamUpdates(epoch, nil)
		},
		HideStoreReads: func(names ...string) func() int64 {
			blind := &blindStore{
				LedgerDB: dbadapter.New(ls.db),
				denied:   map[string]bool{},
			}
			for _, n := range names {
				blind.denied[n] = true
			}
			ls.ldb = stagingLedgerDB{LedgerDB: blind, ls: ls}
			return blind.hits.Load
		},
		EnactInsideTxn: func(epoch uint64) error {
			ldb := ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			current, err := cloneProtocolParametersForEra(
				ls.currentEra, ls.currentPParams,
			)
			require.NoError(t, err)
			_, _, err = ls.computeClassicPParamUpdates(
				t.Context(), txn, ls.currentTip.Point.Slot, epoch,
				ls.currentEra, current,
			)
			return err
		},
		ForecastInsideTxn: func(p UpdateProposal, epoch uint64) error {
			ldb := ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer func() { require.NoError(t, txn.Rollback()) }()
			propose(ldb, txn, p)
			_, err := ls.forecastPendingPParamUpdateIn(
				ls.currentEra, epoch, ls.currentPParams, txn,
			)
			return err
		},
		ConwayRowsInsideTxn: func() (int, int, int) {
			ldb := ls.LedgerDB()
			era := eras.ConwayEraDesc.Id
			inTxn := func(rollback, write bool) int {
				txn := ldb.Begin(t.Context(), true)
				defer func() { require.NoError(t, txn.Rollback()) }()
				if rollback {
					require.NoError(t, ls.stageUtxoRollbackEvent(
						t.Context(), txn, before,
					))
				}
				if write {
					require.NoError(t, ldb.SetPParams(
						[]byte{0x80}, ls.currentTip.Point.Slot,
						ls.currentEpoch.EpochId, era, txn,
					))
				}
				rows, err := ldb.ListPParamsForEra(era, txn)
				require.NoError(t, err)
				return len(rows)
			}
			return inTxn(false, true), inTxn(true, false), inTxn(true, true)
		},
	}
}

// GenesisScenario creates a Musashi genesis, whose Shelley genesis delegates
// one stake key to one pool, with a live UTxO set attached.
type GenesisScenario struct {
	DB *database.Database
	// CreateGenesis applies the genesis block.
	CreateGenesis func()
	// LedgerDB is the LedgerDB the ledger reads through.
	LedgerDB func() ledgerdb.LedgerDB
	// HideStoreReads makes the named LedgerDB reads fail against the
	// metadata store from now on, and returns the count of reads that
	// reached it.
	HideStoreReads func(names ...string) (reached func() int64)
	// SetGenesisGovernance writes initial DReps and delegations through the
	// ledger's LedgerDB, as the genesis block does for the Conway genesis.
	SetGenesisGovernance func(
		initialDReps conway.ConwayGenesisInitialDReps,
		delegs conway.ConwayGenesisDelegs,
	)
	// RegistrationReads and AccountReads are the verify-mode read counts of
	// the registration and account families.
	RegistrationReads func() (verified, mismatches float64)
	AccountReads      func() (verified, mismatches float64)
}

// NewGenesisScenarioForTest returns a GenesisScenario whose ledger writes to
// set.
func NewGenesisScenarioForTest(
	t *testing.T,
	set UtxoSetWriter,
) GenesisScenario {
	t.Helper()
	return NewGenesisScenarioWithReadsForTest(t, set, UtxoSetReadsOff)
}

// NewGenesisScenarioWithReadsForTest is NewGenesisScenarioForTest with the
// ledger's reads in the given mode.
func NewGenesisScenarioWithReadsForTest(
	t *testing.T,
	set UtxoSetWriter,
	reads string,
) GenesisScenario {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json", "musashi", cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Database:          db,
			CardanoNodeConfig: nodeCfg,
			UtxoSet:           set,
			UtxoSetReads:      reads,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(nil)
	return GenesisScenario{
		DB: db,
		CreateGenesis: func() {
			require.NoError(t, ls.createGenesisBlock(t.Context()))
		},
		LedgerDB: ls.LedgerDB,
		HideStoreReads: func(names ...string) func() int64 {
			blind := &blindStore{
				LedgerDB: dbadapter.New(ls.db),
				denied:   map[string]bool{},
			}
			for _, n := range names {
				blind.denied[n] = true
			}
			ls.ldb = stagingLedgerDB{LedgerDB: blind, ls: ls}
			return blind.hits.Load
		},
		SetGenesisGovernance: func(
			initialDReps conway.ConwayGenesisInitialDReps,
			delegs conway.ConwayGenesisDelegs,
		) {
			ldb := ls.LedgerDB()
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			require.NoError(t, ldb.SetGenesisGovernance(
				initialDReps, delegs, nil, txn,
			))
			require.NoError(t, txn.Commit())
		},
		RegistrationReads: func() (float64, float64) {
			return promtestutil.ToFloat64(ls.metrics.registrationReadsVerified),
				promtestutil.ToFloat64(ls.metrics.registrationReadMismatches)
		},
		AccountReads: func() (float64, float64) {
			return promtestutil.ToFloat64(ls.metrics.accountReadsVerified),
				promtestutil.ToFloat64(ls.metrics.accountReadMismatches)
		},
	}
}

// coverSlot publishes an epoch covering slot when the published epoch cache
// holds none: a block or transaction is applied only after the ledger has
// published an epoch covering its slot, and the reward block and fee staging
// resolves that epoch from the published cache.
func (h *boundaryReplayHarness) coverSlot(slot uint64) {
	snap := h.ls.loadConsensusSnapshot()
	if snap == nil {
		return
	}
	for _, e := range snap.epochCache {
		if e.LengthInSlots != 0 && slot >= e.StartSlot &&
			slot < e.StartSlot+uint64(e.LengthInSlots) {
			return
		}
	}
	cp := *snap
	cp.epochCache = append(slices.Clone(snap.epochCache), models.Epoch{
		EpochId: uint64(len(snap.epochCache)), StartSlot: slot,
		SlotLength: 1000, LengthInSlots: 10, EraId: snap.currentEra.Id,
	})
	h.ls.consensus.Store(&cp)
}
