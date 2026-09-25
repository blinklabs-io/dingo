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

// Package conformance provides a DingoStateManager that implements the
// ouroboros-mock conformance.StateManager interface using dingo's ledger
// state models.
package conformance

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"os"
	"sort"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/blinklabs-io/plutigo/data"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// conformanceSlotsPerEpoch is the slots-per-epoch constant used by the
// conformance state manager to translate between epochs and the slot
// values stored in deleted_slot / added_slot columns. Conformance tests
// use it purely as a monotonic marker -- the real slot count for a
// given network is irrelevant here -- so a single fixed value is fine.
const conformanceSlotsPerEpoch uint64 = 432000

// defaultGovActionLifetime and defaultDRepInactivityPeriod are the
// fallback epoch counts used when the active protocol parameters aren't
// Conway (governance is a Conway-only concern, but ApplyTransaction must
// still produce a value before the era check happens deeper in the call).
const (
	defaultGovActionLifetime    uint64 = 6
	defaultDRepInactivityPeriod uint64 = 20
)

// syntheticTxSeedAddress is a syntactically valid mainnet address used only
// to satisfy MockTransaction.Build()'s "at least one input/output" rule for
// synthetic seed transactions built purely to drive real certificate
// persistence (see seedAuthCommitteeHot). It is never read back by
// anything: SetTransactionMetadataOnly discards inputs/outputs entirely.
const syntheticTxSeedAddress = "addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd"

func conformanceCredentialTag(credential common.Credential) uint8 {
	credentialTag, err := models.CredentialTagFromUint(
		credential.CredType,
	)
	if err != nil {
		return 0
	}
	return credentialTag
}

// DingoStateManager implements conformance.StateManager against a real
// Dingo database.Database (sqlite/postgres/mysql metadata store plus a
// local Badger blob store), composed the same way the production node
// composes its storage plugins at startup. UTxOs, certificates, and
// governance state are all read from and written to this real backend --
// see ApplyTransaction, ProcessEpochBoundary, and state_provider.go --
// reusing dingo's own production persistence code
// (database.SetTransactionMetadataOnly, ledger/governance) rather than
// hand-rolling a second implementation against the raw metadata.MetadataStore
// interface.
//
// govState mirrors the subset of state (proposal votes/thresholds,
// pending-committee bookkeeping) the upstream conformance harness reads
// via GetGovernanceState to pre-validate the *next* event before this
// manager applies it; it is kept from drifting out of step with the real
// backend by being driven from the exact same certificate/proposal/vote
// processing calls that also write to the real database (see
// updateGovStateForCertificate, recordProposalsInGovState,
// recordVotesInGovState), and epoch-boundary ratification/enactment
// decisions are persisted back to the real database via
// governance.EnactProposal / Database.SetGovernanceProposal rather than
// mutating only this in-memory mirror.
type DingoStateManager struct {
	db   *database.Database
	host *hostplugin.Host

	// dataDir is the backend's local data directory (Badger blob files,
	// plus the sqlite metadata file for the default backend). reopen
	// recreates a fresh backend at the same opts (used by Reset for
	// backends -- i.e. sqlite -- whose metadata.Resettable.Reset is a
	// documented no-op). ownsDataDir controls whether Close removes it.
	dataDir     string
	ownsDataDir bool
	reopen      func() (*database.Database, *hostplugin.Host, error)

	// protocolParams holds the current protocol parameters
	protocolParams common.ProtocolParameters

	// govState tracks governance-related state for harness pre-validation;
	// see the type doc comment above.
	govState *conformance.GovernanceState

	// currentEpoch tracks the current epoch
	currentEpoch uint64

	// committeeRemovals tracks the remove-set of pending UpdateCommittee
	// proposals, keyed by gov action id. The upstream conformance
	// GovActionInfo only carries the add-set (ProposedMembers), so this
	// stashes the removed cold credentials locally and consumes them on
	// enactment to honor CIP-1694 incremental committee updates in the
	// pre-validation govState mirror (the real committee row removal is
	// handled by governance.EnactProposal itself, decoding the same
	// UpdateCommittee action from the persisted GovActionCbor).
	committeeRemovals map[string]map[common.Blake2b224]struct{}

	// committeeQuorums tracks the new quorum of pending UpdateCommittee
	// proposals, keyed by gov action id, consumed at enactment.
	committeeQuorums map[string]*big.Rat
	utxoIDs          map[string]struct{}
	stakeDeposits    map[mockledger.RewardAccountKey]uint64

	// lastSlot/blockIndex give each transaction within the same slot a
	// distinct, increasing block index, matching production's
	// disambiguation requirement for cert ordering (see AGENTS.md's "Cert
	// ordering" invariant).
	lastSlot   uint64
	blockIndex uint32

	// wipeMetadata, when set, destructively clears the remote metadata
	// store's own state (schema/database) before Reset reopens the
	// backend. The local sqlite backend has no need for it: its metadata
	// lives in the same dataDir the reopen path already wipes. postgres
	// and mysql set it because their metadata lives on a remote server
	// dataDir wiping never touches -- see state_manager_postgres.go and
	// state_manager_mysql.go.
	wipeMetadata func() error

	// wipeBlob, when set, empties the local blob store in place. It pairs
	// with wipeMetadata: database.New checks that the blob store's commit
	// timestamp and the metadata store's agree, so a Reset that emptied one
	// side and not the other would leave a pairing neither side caused.
	// Only the sqlite backend sets it -- the remote backends deliberately
	// share one process-wide blob directory across every vector (see
	// state_manager_postgres.go's postgresProcessBlobDir).
	wipeBlob func() error

	// closeExtra, when set, releases backend-scoped resources the manager
	// owns beyond its database -- currently the long-lived admin connection
	// backendResetter holds so Reset does not reconnect per vector (see
	// reset_cost.go). Close joins its error.
	closeExtra func() error
}

// newDingoStateManager opens a real backend per opts and wraps it in a
// fresh DingoStateManager. Shared by NewDingoStateManager,
// NewDingoPostgresStateManager, and NewDingoMysqlStateManager.
func newDingoStateManager(opts realBackendOptions) (*DingoStateManager, error) {
	db, host, err := openRealDatabase(opts)
	if err != nil {
		return nil, err
	}
	return &DingoStateManager{
		db:      db,
		host:    host,
		dataDir: opts.dataDir,
		reopen: func() (*database.Database, *hostplugin.Host, error) {
			return openRealDatabase(opts)
		},
		govState:          conformance.NewGovernanceState(),
		committeeRemovals: make(map[string]map[common.Blake2b224]struct{}),
		committeeQuorums:  make(map[string]*big.Rat),
		utxoIDs:           make(map[string]struct{}),
		stakeDeposits:     make(map[mockledger.RewardAccountKey]uint64),
	}, nil
}

// NewDingoStateManager creates a DingoStateManager backed by a real, local
// SQLite metadata store (plus a local Badger blob store), composed through
// the same plugin.Resolve path the production node uses at startup.
func NewDingoStateManager() (*DingoStateManager, error) {
	dataDir, err := os.MkdirTemp("", "dingo-conformance-sqlite-*")
	if err != nil {
		return nil, fmt.Errorf(
			"create sqlite conformance data dir: %w",
			err,
		)
	}
	m, err := newDingoStateManagerAt(dataDir)
	if err != nil {
		_ = os.RemoveAll(dataDir)
		return nil, err
	}
	m.ownsDataDir = true
	return m, nil
}

// newDingoStateManagerAt creates a sqlite-backed DingoStateManager rooted at
// an explicit, caller-owned data directory. Used directly by tests that
// close one manager and open a second at the same path to prove state
// survives a restart (see state_manager_backend_test.go); NewDingoStateManager
// uses it with a manager-owned temp directory.
func newDingoStateManagerAt(dataDir string) (*DingoStateManager, error) {
	m, err := newDingoStateManager(realBackendOptions{
		dataDir:          dataDir,
		metadataName:     "sqlite",
		registerMetadata: sqlite.RegisterProvider,
	})
	if err != nil {
		return nil, err
	}
	if err := installSqliteResetHooks(m, dataDir); err != nil {
		return nil, errors.Join(err, m.Close())
	}
	return m, nil
}

// Close releases state-manager resources: the database, its provider host,
// and -- for a manager-owned data directory (the plain NewDingoStateManager
// constructor) -- the directory itself. It never drops a remote schema or
// database: NewDingoPostgresStateManager/NewDingoMysqlStateManager share one
// schema/database across every call in their process (see
// postgresProcessSchema's doc comment in state_manager_postgres.go and
// mysqlProcessDatabase's in state_manager_mysql.go), so an individual
// manager's Close must not drop a resource a sibling manager elsewhere in
// the same process may still be using -- that cleanup belongs to TestMain
// (conformance_main_test.go), once, after every test in the process has
// finished.
func (m *DingoStateManager) Close() error {
	err := closeRealDatabase(m.db, m.host)
	if m.closeExtra != nil {
		err = errors.Join(err, m.closeExtra())
	}
	if m.ownsDataDir && m.dataDir != "" {
		if rmErr := os.RemoveAll(m.dataDir); rmErr != nil {
			err = errors.Join(err, rmErr)
		}
	}
	return err
}

// Reset implements conformance.StateManager.Reset. It clears the
// pre-validation govState mirror and empties the real backend so the next
// vector starts from a genuinely clean database, not just clean bookkeeping.
func (m *DingoStateManager) Reset() error {
	m.protocolParams = nil
	m.currentEpoch = 0
	m.govState = conformance.NewGovernanceState()
	m.committeeRemovals = make(map[string]map[common.Blake2b224]struct{})
	m.committeeQuorums = make(map[string]*big.Rat)
	m.utxoIDs = make(map[string]struct{})
	m.stakeDeposits = make(map[mockledger.RewardAccountKey]uint64)
	m.lastSlot = 0
	m.blockIndex = 0

	// Deliberately not using metadata.Resettable.Reset here: every
	// backend's Reset callback (see database/plugin/metadata/{postgres,
	// mysql}/backup.go's resetDatabase) drops its tables outright rather
	// than clearing rows -- it exists to prepare a target for
	// RestoreFrom, not to clear-and-continue against the same live store
	// -- and postgres's version scans every non-system schema, not just
	// this suite's own, so calling it here would also destroy
	// database/plugin/metadata/postgres's own concurrently running
	// tests' tables in the shared dingo_test database.
	//
	// wipeMetadata truncates every dirty table in this suite's own
	// schema/database in place, over the live connection pool, and does not
	// close/reopen the store: a full close-and-reopen re-runs real
	// migrations, and at one Reset per vector across the whole corpus that
	// is far too slow. For postgres/mysql each migration statement is a real
	// network round trip; for sqlite it is ~260 local DDL statements (80
	// CREATE TABLE, 180 CREATE INDEX). Measured under -race before this
	// path existed, sqlite's Reset averaged 712ms with a CPU profile
	// attributing 76.9% to migrations.Run/execDDL; in place it averages
	// 12ms, and the package fell from 656s to 178s.
	//
	// sqlite pairs wipeMetadata with wipeBlob because it owns its blob
	// store: the two must be emptied together or database.New's
	// blob-versus-metadata commit-timestamp check sees a pairing neither
	// side caused. The remote backends set no wipeBlob -- they share one
	// process-wide blob directory across every vector by design (see
	// state_manager_postgres.go).
	//
	// reopenBackend remains the fallback for any future backend that sets
	// neither hook.
	// The two hooks are checked independently: a backend that sets only one
	// still gets that half emptied, rather than silently skipping it because
	// its partner is nil.
	if m.wipeMetadata != nil || m.wipeBlob != nil {
		var errs []error
		if m.wipeMetadata != nil {
			errs = append(errs, m.wipeMetadata())
		}
		// Runs even when wipeMetadata failed: leaving the blob store
		// populated as well would compound a half-cleared backend.
		if m.wipeBlob != nil {
			errs = append(errs, m.wipeBlob())
		}
		return errors.Join(errs...)
	}
	return m.reopenBackend()
}

// reopenBackend closes the current backend, destructively clears the
// remote metadata store's own state via wipeMetadata (if set -- see its
// field doc comment), clears the local data directory, and opens a fresh
// backend at the same location/DSN via reopen (which re-runs real
// migrations, since it goes through the same plugin.Resolve path
// construction did).
func (m *DingoStateManager) reopenBackend() error {
	if m.reopen == nil {
		return errors.New(
			"conformance: state manager has no backend reopen hook",
		)
	}
	if err := closeRealDatabase(m.db, m.host); err != nil {
		return fmt.Errorf("close backend for reset: %w", err)
	}
	if m.wipeMetadata != nil {
		if err := m.wipeMetadata(); err != nil {
			return fmt.Errorf("wipe metadata store for reset: %w", err)
		}
	}
	if m.dataDir != "" {
		if err := os.RemoveAll(m.dataDir); err != nil {
			return fmt.Errorf("clear data dir for reset: %w", err)
		}
		if err := os.MkdirAll(m.dataDir, 0o700); err != nil {
			return fmt.Errorf("recreate data dir for reset: %w", err)
		}
	}
	db, host, err := m.reopen()
	if err != nil {
		return fmt.Errorf("reopen backend for reset: %w", err)
	}
	m.db = db
	m.host = host
	return nil
}

// LoadInitialState implements conformance.StateManager.LoadInitialState.
func (m *DingoStateManager) LoadInitialState(
	state *conformance.ParsedInitialState,
	pp common.ProtocolParameters,
) error {
	m.protocolParams = pp
	m.currentEpoch = state.CurrentEpoch
	m.committeeRemovals = make(map[string]map[common.Blake2b224]struct{})
	m.committeeQuorums = make(map[string]*big.Rat)

	// LoadFromParsedState populates every field of the pre-validation
	// govState mirror generically from the parsed vector state (stake/pool/
	// drep/committee registrations, hot key auths, proposals). The loop
	// below drives the same seed data through real backend writes.
	m.govState = conformance.NewGovernanceState()
	m.govState.LoadFromParsedState(state)
	for id, parsedUtxo := range state.Utxos {
		if parsedUtxo.Output != nil {
			m.utxoIDs[id] = struct{}{}
		}
	}
	maps.Copy(m.stakeDeposits, state.StakeCredentialDeposits)
	m.syncRewardBalanceMirrors()

	txn := m.db.Transaction(true)
	defer txn.Release()

	// A vector's initial-state registration has no registration certificate
	// in this database, which is exactly what an import baseline stands in
	// for. Seeding it through ImportAccount rather than CreateAccount records
	// the deposit alongside the account row, so
	// DingoStateProvider.StakeCredentialDeposit can report the deposit the
	// credential registered with instead of returning absence and sending
	// UtxoValidateValueNotConservedUtxo down its KeyDeposit fallback for
	// every vector.
	//
	// The vector's parsed initial state does not carry a per-credential
	// deposit (ouroboros-mock's state parser keeps only the reward half of
	// the UMap pair), so the deposit is the KeyDeposit in effect for this
	// vector -- what a registration at that initial state would have paid,
	// and the value the corpus documents for these credentials.
	initialDeposit := m.initialStakeDepositLocked()
	for credential, balance := range resolveInitialStakeRegistrations(state) {
		account := &models.Account{
			StakingKey:    credential.Credential[:],
			CredentialTag: conformanceCredentialTag(credential.AsCredential()),
			Active:        true,
			Reward:        types.Uint64(balance),
			ImportDeposit: initialDeposit,
		}
		if drep, delegated := state.DRepDelegationsByCredential[credential]; delegated {
			account.Drep = append([]byte(nil), drep.Credential...)
			account.DrepType = uint64(drep.Type) //nolint:gosec // conformance DRep types are bounded ledger enums.
		}
		if err := m.db.Metadata().ImportAccount(
			account,
			txn.Metadata(),
		); err != nil {
			return fmt.Errorf("seed account: %w", err)
		}
	}

	for hash, registered := range state.PoolRegistrations {
		if !registered {
			continue
		}
		if err := m.db.ImportPool(
			txn,
			&models.Pool{PoolKeyHash: hash[:]},
			&models.PoolRegistration{PoolKeyHash: hash[:]},
		); err != nil {
			return fmt.Errorf("seed pool: %w", err)
		}
	}

	for credential, registered := range state.DRepRegistrationsByCredential {
		if !registered {
			continue
		}
		credentialTag, err := models.CredentialTagFromUint(credential.CredType)
		if err != nil {
			return fmt.Errorf("seed drep credential tag: %w", err)
		}
		drep := &models.Drep{
			Credential:    credential.Credential[:],
			CredentialTag: credentialTag,
			Active:        true,
		}
		if err := m.db.CreateDrep(txn, drep); err != nil {
			return fmt.Errorf("seed drep: %w", err)
		}
	}
	for _, hash := range state.DRepRegistrations {
		if hasDRepCredentialHash(state.DRepRegistrationsByCredential, hash) {
			continue
		}
		drep := &models.Drep{Credential: hash[:], Active: true}
		if err := m.db.CreateDrep(txn, drep); err != nil {
			return fmt.Errorf("seed legacy drep: %w", err)
		}
	}

	if len(state.CommitteeMembers) > 0 {
		members := make(
			[]*models.CommitteeMember,
			0,
			len(state.CommitteeMembers),
		)
		for coldKey, expiry := range state.CommitteeMembers {
			members = append(members, &models.CommitteeMember{
				ColdCredHash:     coldKey[:],
				ExpiresEpoch:     expiry,
				TermStartSlotSet: true,
			})
		}
		if err := m.db.SetCommitteeMembers(members, txn); err != nil {
			return fmt.Errorf("seed committee members: %w", err)
		}
	}

	if err := m.seedAuthCommitteeHot(txn, state.HotKeyAuthorizations); err != nil {
		return err
	}

	// The enacted constitution is real backend state that the read side
	// (DingoStateProvider.Constitution) reads back through
	// database.Database.GetConstitution, the same way production does.
	// Without this seed a vector whose initial state already carries a
	// constitution -- which is where its guardrails policy hash comes
	// from -- would leave the backend with no constitution row at all, and
	// the read side would (correctly) fail closed on every
	// parameter-change and treasury-withdrawal proposal in the vector.
	// Slot 0 matches the other seeds here, so a rollback to any vector
	// slot never prunes it.
	if state.Constitution != nil {
		if err := m.db.SetConstitution(
			&models.Constitution{
				AnchorURL:  state.Constitution.AnchorURL,
				AnchorHash: state.Constitution.AnchorHash,
				PolicyHash: state.Constitution.PolicyHash,
				AddedSlot:  0,
			},
			txn,
		); err != nil {
			return fmt.Errorf("seed constitution: %w", err)
		}
	}

	for id, proposal := range state.Proposals {
		govProposal := m.proposalToModel(id, proposal)
		if err := m.db.SetGovernanceProposal(&govProposal, txn); err != nil {
			return fmt.Errorf("seed governance proposal: %w", err)
		}
	}

	for utxoId, parsedUtxo := range state.Utxos {
		if parsedUtxo.Output == nil {
			continue
		}
		var txHash common.Blake2b256
		copy(txHash[:], parsedUtxo.TxHash)
		input := &dingoTransactionInput{
			txId:  txHash,
			index: parsedUtxo.Index,
		}
		utxo := common.Utxo{Id: input, Output: parsedUtxo.Output}
		if err := m.createUtxo(txn, utxo, 0); err != nil {
			return fmt.Errorf("seed utxo %s: %w", utxoId, err)
		}
	}

	return txn.Commit()
}

// resolveInitialStakeRegistrations mirrors the original
// LoadInitialState's credential-identity preference: full credential
// identity first, then reward-balance-only, then legacy hash-only
// registrations representing key credentials.
func resolveInitialStakeRegistrations(
	state *conformance.ParsedInitialState,
) map[mockledger.RewardAccountKey]uint64 {
	regs := make(map[mockledger.RewardAccountKey]uint64)
	switch {
	case len(state.StakeRegistrationsByCredential) > 0:
		for credential, registered := range state.StakeRegistrationsByCredential {
			if !registered {
				continue
			}
			balance, exists := state.RewardAccountBalances[credential]
			if !exists {
				balance = state.RewardAccounts[credential.Credential]
			}
			regs[credential] = balance
		}
	case len(state.RewardAccountBalances) > 0:
		maps.Copy(regs, state.RewardAccountBalances)
	default:
		for hash, registered := range state.StakeRegistrations {
			if !registered {
				continue
			}
			regs[mockledger.RewardAccountKey{
				CredType:   common.CredentialTypeAddrKeyHash,
				Credential: hash,
			}] = state.RewardAccounts[hash]
		}
	}
	return regs
}

// seedAuthCommitteeHot persists pre-existing hot-key authorizations from a
// vector's initial state through the real certificate-application path
// (there is no direct metadata.GovernanceStore write for AuthCommitteeHot
// outside of certificate processing), by building a synthetic, otherwise
// inert transaction carrying one AuthCommitteeHotCertificate per
// authorization and running it through SetTransactionMetadataOnly.
func (m *DingoStateManager) seedAuthCommitteeHot(
	txn *database.Txn,
	auths map[common.Blake2b224]common.Blake2b224,
) error {
	if len(auths) == 0 {
		return nil
	}
	certs := make([]common.Certificate, 0, len(auths))
	for coldKey, hotKey := range auths {
		certs = append(certs, &common.AuthCommitteeHotCertificate{
			CertType: uint(common.CertificateTypeAuthCommitteeHot),
			ColdCredential: common.Credential{
				CredType:   common.CredentialTypeAddrKeyHash,
				Credential: coldKey,
			},
			HotCredential: common.Credential{
				CredType:   common.CredentialTypeAddrKeyHash,
				Credential: hotKey,
			},
		})
	}
	tx, err := syntheticTransaction("seed-auth-committee-hot", certs)
	if err != nil {
		return fmt.Errorf("build synthetic auth-committee-hot tx: %w", err)
	}
	point := ocommon.Point{Slot: 0, Hash: syntheticBlockHash(0)}
	if err := m.db.SetTransactionMetadataOnly(
		tx, point, 0, map[int]uint64{}, txn,
	); err != nil {
		return fmt.Errorf("seed auth committee hot: %w", err)
	}
	return nil
}

// syntheticTransaction builds a minimal, otherwise-inert lcommon.Transaction
// carrying certs, for driving real certificate persistence
// (SetTransactionMetadataOnly) outside of a decoded vector transaction --
// e.g. seeding pre-existing state that a vector's initial_state describes
// but has no originating transaction bytes for. The dummy input/output only
// satisfy MockTransaction.Build()'s non-empty requirement;
// SetTransactionMetadataOnly discards both.
func syntheticTransaction(
	seed string,
	certs []common.Certificate,
) (common.Transaction, error) {
	txId := sha256.Sum256([]byte(seed))
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(syntheticTxSeedAddress).
		WithLovelace(1_000_000).
		Build()
	if err != nil {
		return nil, err
	}
	inputId := sha256.Sum256([]byte(seed + "-input"))
	input, err := mockledger.NewSimpleTransactionInput(inputId[:], 0)
	if err != nil {
		return nil, err
	}
	builder := mockledger.NewTransactionBuilder()
	builder.WithId(txId[:])
	builder.WithType(int(conway.EraIdConway))
	builder.WithValid(true)
	builder.WithInputs(input)
	builder.WithOutputs(output)
	if len(certs) > 0 {
		builder.WithCertificates(certs...)
	}
	return builder.Build()
}

// syntheticBlockHash derives a deterministic, unique-enough 32-byte block
// hash from slot for the ocommon.Point passed to real persistence calls.
// Conformance vectors don't model real blocks, so any well-formed,
// slot-derived value is sufficient here.
func syntheticBlockHash(slot uint64) []byte {
	seed := binary.BigEndian.AppendUint64(
		[]byte("dingo-conformance-block"),
		slot,
	)
	hash := sha256.Sum256(seed)
	return hash[:]
}

// pointForSlot builds the ocommon.Point passed to real persistence calls.
func (m *DingoStateManager) pointForSlot(slot uint64) ocommon.Point {
	return ocommon.Point{Slot: slot, Hash: syntheticBlockHash(slot)}
}

// nextBlockIndex returns a monotonically increasing index for transactions
// within the same slot, and resets to zero when the slot advances --
// mirroring production's per-slot block-index disambiguation for cert
// ordering (see AGENTS.md's "Cert ordering" invariant).
func (m *DingoStateManager) nextBlockIndex(slot uint64) uint32 {
	if slot != m.lastSlot {
		m.lastSlot = slot
		m.blockIndex = 0
	} else {
		m.blockIndex++
	}
	return m.blockIndex
}

// createUtxo persists a produced UTxO through the real backend: the
// metadata row via Database.CreateUtxo (reusing
// models.UtxoLedgerToModel, the same conversion the production
// block-application path uses), and the output's raw CBOR via the blob
// store's existing legacy (non-offset) UTxO format, so a later read can
// reconstruct a full common.Utxo{Output: ...} for ledger-rule validation
// -- not just prove metadata existence. See database/utxo.go's loadCbor
// for the non-offset fallback this relies on.
func (m *DingoStateManager) createUtxo(
	txn *database.Txn,
	utxo common.Utxo,
	slot uint64,
) error {
	if utxo.Output == nil {
		return nil
	}
	utxoModel, err := models.UtxoLedgerToModel(utxo, slot)
	if err != nil {
		return fmt.Errorf("convert utxo to model: %w", err)
	}
	if err := m.db.CreateUtxo(txn, &utxoModel); err != nil {
		return fmt.Errorf("create utxo metadata: %w", err)
	}
	// The transaction's own store, so the handle and the store it is used
	// with always come from the same installation.
	if blobStore := txn.BlobStore(); blobStore != nil {
		if err := blobStore.SetUtxo(
			txn.Blob(), utxoModel.TxId, utxoModel.OutputIdx,
			utxo.Output.Cbor(),
		); err != nil {
			return fmt.Errorf("create utxo blob: %w", err)
		}
	}
	return nil
}

// spendUtxos marks the referenced UTxOs deleted at slot (the real store's
// spend representation -- see Database.MarkUtxosDeletedAtSlot), mirroring
// what production does when a transaction consumes inputs/collateral.
func (m *DingoStateManager) spendUtxos(
	txn *database.Txn,
	inputs []common.TransactionInput,
	slot uint64,
) error {
	if len(inputs) == 0 {
		return nil
	}
	refs := make([]types.UtxoKey, 0, len(inputs))
	for _, input := range inputs {
		if input == nil {
			continue
		}
		inputId := input.Id()
		refs = append(refs, types.UtxoKey{
			TxId:      inputId.Bytes(),
			OutputIdx: input.Index(),
		})
	}
	if len(refs) == 0 {
		return nil
	}
	return m.db.MarkUtxosDeletedAtSlot(txn, refs, slot)
}

// certDepositsFor builds the per-certificate-index deposit map
// SetTransactionMetadataOnly requires for deposit-bearing certificate
// types. Conway-era certs that carry an explicit deposit amount
// (RegistrationCertificate and its delegation/DRep variants) use that
// amount; the legacy StakeRegistrationCertificate and
// PoolRegistrationCertificate have no amount field and use the
// corresponding protocol parameter instead.
func (m *DingoStateManager) certDepositsFor(
	certs []common.Certificate,
) map[int]uint64 {
	deposits := make(map[int]uint64, len(certs))
	var keyDeposit, poolDeposit uint64
	if conwayPP, ok := m.protocolParams.(*conway.ConwayProtocolParameters); ok {
		keyDeposit = uint64(conwayPP.KeyDeposit)
		poolDeposit = uint64(conwayPP.PoolDeposit)
	}
	for i, cert := range certs {
		switch c := cert.(type) {
		case *common.StakeRegistrationCertificate:
			deposits[i] = keyDeposit
		case *common.RegistrationCertificate:
			deposits[i] = depositAmount(c.Amount)
		case *common.StakeRegistrationDelegationCertificate:
			deposits[i] = depositAmount(c.Amount)
		case *common.StakeVoteRegistrationDelegationCertificate:
			deposits[i] = depositAmount(c.Amount)
		case *common.VoteRegistrationDelegationCertificate:
			deposits[i] = depositAmount(c.Amount)
		case *common.RegistrationDrepCertificate:
			deposits[i] = depositAmount(c.Amount)
		case *common.PoolRegistrationCertificate:
			deposits[i] = poolDeposit
		}
	}
	return deposits
}

// initialStakeDepositLocked returns the deposit to record for a credential a
// vector declares as already registered, as *types.Uint64 for
// models.Account.ImportDeposit. It is the KeyDeposit from the vector's own
// protocol parameters, or nil when those parameters do not expose one -- nil
// meaning the recorded deposit is unknown, which correctly sends value
// conservation back to its KeyDeposit fallback rather than inventing a zero.
func (m *DingoStateManager) initialStakeDepositLocked() *types.Uint64 {
	// A typed-nil *conway.ConwayProtocolParameters satisfies the assertion,
	// so guard the pointer as well -- the same "!ok || nil" shape
	// ledger/eras uses before dereferencing era parameters. Reporting nil
	// here is the correct answer anyway: with no usable parameters the
	// deposit is unknown.
	conwayPP, ok := m.protocolParams.(*conway.ConwayProtocolParameters)
	if !ok || conwayPP == nil {
		return nil
	}
	deposit := types.Uint64(conwayPP.KeyDeposit)
	return &deposit
}

// depositAmount converts a certificate's signed deposit amount (gouroboros
// models it as int64) to the unsigned form the metadata store expects.
// Deposit amounts are always non-negative on chain.
func depositAmount(amount int64) uint64 {
	if amount < 0 {
		return 0
	}
	return uint64(amount)
}

// ApplyTransaction implements conformance.StateManager.ApplyTransaction.
func (m *DingoStateManager) ApplyTransaction(
	tx common.Transaction,
	slot uint64,
) error {
	point := m.pointForSlot(slot)
	idx := m.nextBlockIndex(slot)

	txn := m.db.Transaction(true)
	defer txn.Release()

	if !tx.IsValid() {
		if err := m.applyInvalidTransaction(txn, tx, slot); err != nil {
			return err
		}
		if err := txn.Commit(); err != nil {
			return err
		}
		m.removeUtxoIDs(tx.Collateral())
		if tx.CollateralReturn() != nil {
			m.addUtxoID(tx.Hash(), len(tx.Outputs()))
		}
		return nil
	}

	if err := m.spendUtxos(txn, tx.Inputs(), slot); err != nil {
		return fmt.Errorf("spend inputs: %w", err)
	}

	txHash := tx.Hash()
	for outIdx, output := range tx.Outputs() {
		input := &dingoTransactionInput{
			txId:  txHash,
			index: uint32(outIdx), //nolint:gosec // idx bounded by tx outputs
		}
		utxo := common.Utxo{Id: input, Output: output}
		if err := m.createUtxo(txn, utxo, slot); err != nil {
			return fmt.Errorf("create output %d: %w", outIdx, err)
		}
	}

	certDeposits := m.certDepositsFor(tx.Certificates())
	protocolMajor := uint64(0)
	if versioned, ok := m.protocolParams.(common.PoolRuleProtocolParameters); ok {
		protocolMajor = uint64(versioned.ProtocolMajorVersion())
	}
	if err := m.db.SetTransactionMetadataOnly(
		tx, point, idx, certDeposits, txn, protocolMajor,
	); err != nil {
		return fmt.Errorf("apply certificates: %w", err)
	}
	for _, cert := range tx.Certificates() {
		m.updateGovStateForCertificate(cert)
	}

	govActionLifetime := defaultGovActionLifetime
	drepInactivityPeriod := defaultDRepInactivityPeriod
	if conwayPP, ok := m.protocolParams.(*conway.ConwayProtocolParameters); ok {
		govActionLifetime = conwayPP.GovActionValidityPeriod
		drepInactivityPeriod = conwayPP.DRepInactivityPeriod
	}

	if votes := tx.VotingProcedures(); len(votes) > 0 {
		if err := governance.ProcessVotes(
			tx, point, m.currentEpoch, drepInactivityPeriod, m.db, txn,
		); err != nil {
			return fmt.Errorf("process votes: %w", err)
		}
		m.recordVotesInGovState(tx)
	}

	if governance.HasDRepActivityCertificates(tx) {
		protocolMajor := uint64(0)
		if conwayPP, ok := m.protocolParams.(*conway.ConwayProtocolParameters); ok {
			protocolMajor = uint64(conwayPP.ProtocolVersion.Major)
		}
		if err := governance.ProcessDRepActivityCertificates(
			tx, point, m.currentEpoch, drepInactivityPeriod, protocolMajor, m.db, txn,
		); err != nil {
			return fmt.Errorf("process drep activity certs: %w", err)
		}
	}
	if proposals := tx.ProposalProcedures(); len(proposals) > 0 {
		if err := governance.ProcessProposals(
			tx, point, m.currentEpoch, govActionLifetime, m.db, txn,
		); err != nil {
			return fmt.Errorf("process proposals: %w", err)
		}
		m.recordProposalsInGovState(tx, govActionLifetime)
	}
	if governance.HasDRepDeregistrationCertificates(tx) {
		if err := governance.ProcessDRepDeregistrationEffects(
			tx, point, m.currentEpoch, m.db, txn,
		); err != nil {
			return fmt.Errorf("process drep deregistration effects: %w", err)
		}
	}

	if err := txn.Commit(); err != nil {
		return err
	}
	for _, cert := range tx.Certificates() {
		m.updateStakeDepositForCertificate(cert)
	}
	m.applyRewardWithdrawals(tx)
	m.removeUtxoIDs(tx.Inputs())
	for outIdx := range tx.Outputs() {
		m.addUtxoID(txHash, outIdx)
	}
	return nil
}

func (m *DingoStateManager) applyRewardWithdrawals(tx common.Transaction) {
	for address, amount := range tx.Withdrawals() {
		credential, err := address.RewardAccountCredential()
		if err != nil || amount.Sign() < 0 || !amount.IsUint64() {
			continue
		}
		key := rewardAccountKey(credential)
		balance, exists := m.govState.RewardAccountBalances[key]
		if exists && amount.Uint64() <= balance {
			m.govState.RewardAccountBalances[key] = balance - amount.Uint64()
		}
	}
	m.govState.RewardAccounts = rewardBalancesByHash(
		m.govState.RewardAccountBalances,
	)
}

// applyInvalidTransaction handles a phase-2-invalid transaction: only
// collateral is consumed, and the collateral-return output (if any) is
// produced. Certificates/proposals/votes on an invalid transaction have no
// on-chain effect and are skipped, matching production
// (SetTransactionMetadataOnly's underlying SetTransaction only applies
// certificates when the transaction IsValid()).
func (m *DingoStateManager) applyInvalidTransaction(
	txn *database.Txn,
	tx common.Transaction,
	slot uint64,
) error {
	if err := m.spendUtxos(txn, tx.Collateral(), slot); err != nil {
		return fmt.Errorf("spend collateral: %w", err)
	}
	collateralReturn := tx.CollateralReturn()
	if collateralReturn == nil {
		return nil
	}
	txHash := tx.Hash()
	outputCount := len(tx.Outputs())
	returnIdx := uint32(outputCount) //nolint:gosec // bounded by max tx size
	input := &dingoTransactionInput{txId: txHash, index: returnIdx}
	utxo := common.Utxo{Id: input, Output: collateralReturn}
	if err := m.createUtxo(txn, utxo, slot); err != nil {
		return fmt.Errorf("create collateral return utxo: %w", err)
	}
	return nil
}

// updateGovStateForCertificate keeps the pre-validation govState mirror in
// step with a certificate that was just persisted to the real backend via
// SetTransactionMetadataOnly above. It performs no database writes itself.
func (m *DingoStateManager) updateGovStateForCertificate(
	cert common.Certificate,
) {
	certType := common.CertificateType(cert.Type())

	//exhaustive:ignore
	switch certType {
	case common.CertificateTypeStakeRegistration:
		if c, ok := cert.(*common.StakeRegistrationCertificate); ok {
			m.govState.RegisterStakeCredential(c.StakeCredential)
		}
	case common.CertificateTypeRegistration:
		if c, ok := cert.(*common.RegistrationCertificate); ok {
			m.govState.RegisterStakeCredential(c.StakeCredential)
		}
	case common.CertificateTypeStakeRegistrationDelegation:
		if c, ok := cert.(*common.StakeRegistrationDelegationCertificate); ok {
			m.govState.RegisterStakeCredential(c.StakeCredential)
			m.govState.SetPoolDelegation(c.StakeCredential, c.PoolKeyHash)
		}
	case common.CertificateTypeVoteRegistrationDelegation:
		if c, ok := cert.(*common.VoteRegistrationDelegationCertificate); ok {
			m.govState.RegisterStakeCredential(c.StakeCredential)
			m.govState.SetDRepDelegation(c.StakeCredential, c.Drep)
		}
	case common.CertificateTypeStakeVoteRegistrationDelegation:
		if c, ok := cert.(*common.StakeVoteRegistrationDelegationCertificate); ok {
			m.govState.RegisterStakeCredential(c.StakeCredential)
			m.govState.SetDRepDelegation(c.StakeCredential, c.Drep)
			m.govState.SetPoolDelegation(c.StakeCredential, c.PoolKeyHash)
		}
	case common.CertificateTypeStakeDelegation:
		if c, ok := cert.(*common.StakeDelegationCertificate); ok && c.StakeCredential != nil {
			m.govState.SetPoolDelegation(*c.StakeCredential, c.PoolKeyHash)
		}
	case common.CertificateTypeVoteDelegation:
		if c, ok := cert.(*common.VoteDelegationCertificate); ok {
			m.govState.SetDRepDelegation(c.StakeCredential, c.Drep)
		}
	case common.CertificateTypeStakeVoteDelegation:
		if c, ok := cert.(*common.StakeVoteDelegationCertificate); ok {
			m.govState.SetDRepDelegation(c.StakeCredential, c.Drep)
			m.govState.SetPoolDelegation(c.StakeCredential, c.PoolKeyHash)
		}
	case common.CertificateTypeStakeDeregistration:
		if c, ok := cert.(*common.StakeDeregistrationCertificate); ok {
			m.govState.DeregisterStakeCredential(c.StakeCredential)
		}
	case common.CertificateTypeDeregistration:
		if c, ok := cert.(*common.DeregistrationCertificate); ok {
			m.govState.DeregisterStakeCredential(c.StakeCredential)
		}
	case common.CertificateTypePoolRegistration:
		if c, ok := cert.(*common.PoolRegistrationCertificate); ok {
			m.govState.RegisterPool(c.Operator)
			credential := c.RewardAccountCredential()
			m.govState.SetPoolRewardAccount(c.Operator, rewardAccountKey(credential))
		}
	case common.CertificateTypePoolRetirement:
		if c, ok := cert.(*common.PoolRetirementCertificate); ok {
			m.govState.RetirePool(c.PoolKeyHash, c.Epoch)
		}
	case common.CertificateTypeRegistrationDrep:
		if c, ok := cert.(*common.RegistrationDrepCertificate); ok {
			m.govState.RegisterDRepCredentialUntil(
				c.DrepCredential,
				m.currentEpoch+m.drepInactivityPeriod(),
			)
		}
	case common.CertificateTypeDeregistrationDrep:
		if c, ok := cert.(*common.DeregistrationDrepCertificate); ok {
			m.govState.DeregisterDRepCredential(c.DrepCredential)
		}
	case common.CertificateTypeUpdateDrep:
		if c, ok := cert.(*common.UpdateDrepCertificate); ok {
			m.govState.RegisterDRepCredentialUntil(
				c.DrepCredential,
				m.currentEpoch+m.drepInactivityPeriod(),
			)
		}
	case common.CertificateTypeAuthCommitteeHot:
		if c, ok := cert.(*common.AuthCommitteeHotCertificate); ok {
			m.govState.AuthorizeHotKey(
				c.ColdCredential.Credential,
				c.HotCredential.Credential,
			)
		}
	case common.CertificateTypeResignCommitteeCold:
		if c, ok := cert.(*common.ResignCommitteeColdCertificate); ok {
			m.govState.ResignCommitteeMember(c.ColdCredential.Credential)
		}
	default:
		// Other certificate types not relevant to governance pre-validation.
	}
}

func hasDRepCredentialHash(
	registrations map[mockledger.RewardAccountKey]bool,
	hash common.Blake2b224,
) bool {
	for credential, registered := range registrations {
		if registered && credential.Credential == hash {
			return true
		}
	}
	return false
}

func (m *DingoStateManager) updateStakeDepositForCertificate(
	cert common.Certificate,
) {
	var credential common.Credential
	var deposit uint64
	var registered bool
	switch c := cert.(type) {
	case *common.StakeRegistrationCertificate:
		credential, registered = c.StakeCredential, true
		if pp, ok := m.protocolParams.(*conway.ConwayProtocolParameters); ok {
			deposit = uint64(pp.KeyDeposit)
		}
	case *common.RegistrationCertificate:
		credential, deposit, registered = c.StakeCredential, depositAmount(c.Amount), true
	case *common.StakeRegistrationDelegationCertificate:
		credential, deposit, registered = c.StakeCredential, depositAmount(c.Amount), true
	case *common.VoteRegistrationDelegationCertificate:
		credential, deposit, registered = c.StakeCredential, depositAmount(c.Amount), true
	case *common.StakeVoteRegistrationDelegationCertificate:
		credential, deposit, registered = c.StakeCredential, depositAmount(c.Amount), true
	case *common.StakeDeregistrationCertificate:
		delete(m.stakeDeposits, rewardAccountKey(c.StakeCredential))
		return
	case *common.DeregistrationCertificate:
		delete(m.stakeDeposits, rewardAccountKey(c.StakeCredential))
		return
	}
	if registered {
		m.stakeDeposits[rewardAccountKey(credential)] = deposit
	}
}

func rewardAccountKey(credential common.Credential) mockledger.RewardAccountKey {
	return mockledger.RewardAccountKey{
		CredType:   credential.CredType,
		Credential: credential.Credential,
	}
}

func (m *DingoStateManager) drepInactivityPeriod() uint64 {
	if pp, ok := m.protocolParams.(*conway.ConwayProtocolParameters); ok {
		return pp.DRepInactivityPeriod
	}
	return defaultDRepInactivityPeriod
}

// recordProposalsInGovState mirrors newly submitted proposals into the
// pre-validation govState cache after governance.ProcessProposals has
// already persisted them to the real backend.
func (m *DingoStateManager) recordProposalsInGovState(
	tx common.Transaction,
	govActionLifetime uint64,
) {
	txHash := tx.Hash()
	txHashStr := hex.EncodeToString(txHash.Bytes())
	for idx, proposal := range tx.ProposalProcedures() {
		govActionId := fmt.Sprintf("%s#%d", txHashStr, idx)
		action := proposal.GovAction()
		if action == nil {
			continue
		}
		info := conformance.GovActionInfo{
			ActionType:      getActionType(action),
			ExpiresAfter:    m.currentEpoch + govActionLifetime,
			SubmittedEpoch:  m.currentEpoch,
			Deposit:         proposal.Deposit(),
			ProposedMembers: make(map[common.Blake2b224]uint64),
		}
		rewardAccount := proposal.RewardAccount()
		if credential, err := rewardAccount.RewardAccountCredential(); err == nil {
			key := rewardAccountKey(credential)
			info.ReturnAccount = &key
		}
		extractActionSpecificData(action, &info)

		// CIP-1694 UpdateCommittee carries a remove-set and a new quorum
		// alongside the add-set. GovActionInfo only tracks the add-set, so
		// stash the remove-set and quorum in local maps keyed by gov
		// action id for use at enactment time (see enactProposal).
		if uca, ok := action.(*common.UpdateCommitteeGovAction); ok {
			removed := make(
				map[common.Blake2b224]struct{},
				len(uca.Credentials),
			)
			for _, cred := range uca.Credentials {
				removed[cred.Credential] = struct{}{}
			}
			m.committeeRemovals[govActionId] = removed
			if uca.Quorum.Rat != nil {
				m.committeeQuorums[govActionId] = new(
					big.Rat,
				).Set(uca.Quorum.Rat)
			}
		}

		m.govState.AddProposal(govActionId, info)
	}
}

// recordVotesInGovState mirrors newly cast votes into the pre-validation
// govState cache after governance.ProcessVotes has already persisted them
// to the real backend.
func (m *DingoStateManager) recordVotesInGovState(tx common.Transaction) {
	for voter, voteMap := range tx.VotingProcedures() {
		for govActionId, votingProc := range voteMap {
			actionKey := fmt.Sprintf(
				"%s#%d",
				hex.EncodeToString(govActionId.TransactionId[:]),
				govActionId.GovActionIdx,
			)
			proposal := m.govState.GetProposal(actionKey)
			if proposal == nil {
				continue
			}
			if proposal.Votes == nil {
				proposal.Votes = make(map[string]uint8)
			}
			voterKey := fmt.Sprintf(
				"%d:%s",
				voter.Type,
				hex.EncodeToString(voter.Hash[:]),
			)
			proposal.Votes[voterKey] = votingProc.Vote
		}
	}
}

// ProcessEpochBoundary implements conformance.StateManager.ProcessEpochBoundary.
//
// Pool retirement has no separate real-database write here: a pool's
// PoolRetirementCertificate is already persisted (pool + pool_retirement
// rows) at certificate-application time via SetTransactionMetadataOnly in
// ApplyTransaction, and DingoStateProvider.PoolCurrentState/IsPoolRegistered
// determine registered-vs-retired status by comparing that stored
// retirement epoch against the current epoch at read time -- so there is
// nothing further to persist at the boundary itself.
//
// Ratification/enactment decisions are made by ratifyProposals/enactProposal
// below, not by invoking the full governance.ProcessEpoch orchestration.
// Most action types still use the vector-validated vote-shape heuristic the
// harness has always used; NoConfidence and UpdateCommittee are the
// exception (see committeeActionRatified), deciding ratification with a
// real stake-weighted DRep/SPO tally against the database's live stake
// distribution via credentialVotingStake/GetControlledAmountByCredential.
// The rest of ProcessEpoch's real orchestration -- committee/SPO/DRep
// denominators computed once per epoch tick, parent-chain and per-purpose
// bookkeeping -- is not invoked here, and synthetic per-vector seed data
// isn't guaranteed to model that fidelity, so a mismatch there would show
// up as vector regressions, not as an isolated persistence gap. Enactment
// side effects that a ratified
// proposal must apply (committee membership, protocol parameters,
// constitution, treasury withdrawal) are instead persisted by calling the
// real governance.EnactProposal directly against the already-persisted
// governance_proposal row once this manager's own heuristic decides to
// enact it -- reusing dingo's production side-effect code without
// re-deriving its ratification math. governance.ProcessEpoch is exercised
// directly, end-to-end, by TestProcessEpochAgainstRealBackend in
// state_manager_backend_test.go.
func (m *DingoStateManager) ProcessEpochBoundary(newEpoch uint64) error {
	m.currentEpoch = newEpoch
	m.govState.CurrentEpoch = newEpoch
	m.govState.ProcessPoolRetirements(newEpoch)

	boundarySlot := newEpoch * conformanceSlotsPerEpoch

	txn := m.db.Transaction(true)
	defer txn.Release()

	// Phase 1: enact proposals that were ratified in previous epochs.
	var toEnact []string
	for id, proposal := range m.govState.Proposals {
		if proposal == nil {
			continue
		}
		if proposal.RatifiedEpoch != nil && newEpoch > *proposal.RatifiedEpoch {
			toEnact = append(toEnact, id)
		}
	}
	for _, id := range toEnact {
		proposal := m.govState.Proposals[id]
		if proposal == nil || proposal.ActionType == common.GovActionTypeInfo {
			continue
		}
		if err := m.enactProposal(txn, id, proposal, boundarySlot); err != nil {
			return fmt.Errorf("enact proposal %s: %w", id, err)
		}
	}

	m.pruneCommitteeResignations()

	// Phase 2: ratify proposals that meet threshold requirements.
	if err := m.ratifyProposals(txn, newEpoch, boundarySlot); err != nil {
		return fmt.Errorf("ratify proposals: %w", err)
	}

	// Phase 3: expire old proposals.
	for id, proposal := range m.govState.Proposals {
		if proposal == nil {
			continue
		}
		if newEpoch <= proposal.ExpiresAfter {
			continue
		}
		delete(m.govState.Proposals, id)
		if err := m.expireGovernanceProposal(txn, id, newEpoch); err != nil {
			return fmt.Errorf("expire proposal %s: %w", id, err)
		}
	}

	return txn.Commit()
}

// pruneCommitteeResignations drops the resignation recorded for a cold
// credential that holds no committee seat in the pre-validation govState
// mirror.
//
// cardano-ledger keeps resignations and hot-key authorizations in one
// committee credential map and intersects that map with the current
// committee at every epoch boundary
// (eras/conway/impl/src/Cardano/Ledger/Conway/Rules/Epoch.hs
// updateCommitteeState), so a resignation lives exactly as long as the seat
// it was filed against: a member an UpdateCommittee action removed carries
// none forward, and a later re-election seats a member that may authorize a
// hot key again. Without this, a cold credential that resigns once is
// treated as resigned for the rest of the vector, which rejects that second
// authorization.
//
// Only the resignation is pruned. Hot-key authorizations are left alone
// because this mirror's simplified ratification (see the ProcessEpochBoundary
// doc comment) does not seat every member the vectors elect, so intersecting
// those as well would withdraw voting rights a vector still exercises.
func (m *DingoStateManager) pruneCommitteeResignations() {
	for coldKey := range m.govState.CommitteeResignations {
		if _, ok := m.govState.CommitteeMembersByCredential[coldKey]; ok {
			continue
		}
		if _, ok := m.govState.CommitteeMembers[coldKey.Credential]; ok {
			continue
		}
		delete(m.govState.CommitteeResignations, coldKey)
	}
}

// ratifyProposals performs the harness's proposal ratification -- a
// vote-shape heuristic for most action types, a real stake-weighted DRep/SPO
// tally for NoConfidence/UpdateCommittee (see committeeActionRatified and
// the ProcessEpochBoundary doc comment for why this still isn't
// governance.ProcessEpoch's full orchestration) -- and persists each
// ratification decision to the real backend row.
func (m *DingoStateManager) ratifyProposals(
	txn *database.Txn,
	currentEpoch uint64,
	boundarySlot uint64,
) error {
	for id, proposal := range m.govState.Proposals {
		if proposal.RatifiedEpoch != nil {
			continue
		}
		if currentEpoch <= proposal.SubmittedEpoch {
			continue
		}

		if proposal.ActionType == common.GovActionTypeInfo {
			epoch := currentEpoch
			proposal.RatifiedEpoch = &epoch
			m.govState.Proposals[id] = proposal
			if err := m.persistRatification(
				txn, id, currentEpoch, boundarySlot,
			); err != nil {
				return err
			}
			continue
		}

		var meetsRequirements bool
		if proposal.ActionType == common.GovActionTypeNoConfidence ||
			proposal.ActionType == common.GovActionTypeUpdateCommittee {
			// cardano-ledger's votingCommitteeThresholdInternal returns
			// NoVotingAllowed for the committee on both of these action
			// types, so a hasCC requirement here means the harness would
			// never ratify either. Vote *shape* also cannot replace a real
			// tally: a vector can carry the same yes-voter shape (one DRep,
			// one SPO) as another vector that must NOT ratify once active
			// proposal deposits are counted as part of the depositor's
			// active voting stake (CIP-1694). See issue #4007.
			//
			// This must run before the zero-explicit-vote guard below: a
			// DRep or silent pool delegated AlwaysNoConfidence casts an
			// automatic yes on a NoConfidence action without ever appearing
			// in proposal.Votes (see drepStakeForCommitteeAction/
			// spoStakeForCommitteeAction), so a proposal backed only by that
			// implicit vote must still reach committeeActionRatified.
			var err error
			meetsRequirements, err = m.committeeActionRatified(
				txn, proposal, currentEpoch,
			)
			if err != nil {
				return fmt.Errorf(
					"evaluate committee-action ratification for %s: %w",
					id, err,
				)
			}
		} else {
			if len(proposal.Votes) == 0 {
				continue
			}

			voterTypesWithYes := make(map[uint8]bool)
			for voterKey, voteValue := range proposal.Votes {
				if voteValue != 1 {
					continue
				}
				if len(voterKey) > 0 {
					voterTypesWithYes[voterKey[0]-'0'] = true
				}
			}

			hasCC := voterTypesWithYes[0] || voterTypesWithYes[1]
			hasDRep := voterTypesWithYes[2] || voterTypesWithYes[3]
			hasSPO := voterTypesWithYes[4] || voterTypesWithYes[5]

			//exhaustive:ignore
			switch proposal.ActionType {
			case common.GovActionTypeHardForkInitiation:
				// Conway bootstrap explicitly admits HardForkInitiation
				// alongside ParameterChange (ledger/governance's
				// ShouldRatify), so no bootstrap gate applies to either of
				// those two.
				meetsRequirements = hasCC && hasDRep && hasSPO
			case common.GovActionTypeParameterChange:
				meetsRequirements = hasCC && hasDRep
			case common.GovActionTypeNewConstitution,
				common.GovActionTypeTreasuryWithdrawal:
				// Unlike ParameterChange/HardForkInitiation, ShouldRatify
				// refuses these two outright during Conway bootstrap
				// regardless of votes. This heuristic path has no tally to
				// hand ShouldRatify (see committeeActionRatified for the
				// pair that does), so the bootstrap ineligibility has to be
				// checked directly here.
				meetsRequirements = hasCC && hasDRep && !m.inConwayBootstrap()
			default:
				meetsRequirements = len(voterTypesWithYes) >= 2
			}
		}

		if !meetsRequirements {
			continue
		}

		epoch := currentEpoch
		proposal.RatifiedEpoch = &epoch
		m.govState.Proposals[id] = proposal
		if err := m.persistRatification(
			txn, id, currentEpoch, boundarySlot,
		); err != nil {
			return err
		}
	}
	return nil
}

// inConwayBootstrap reports whether the active protocol parameters are at
// Conway's bootstrap major version (common.ProtocolVersionConway, 9),
// during which ledger/governance's ShouldRatify refuses several action
// types (NoConfidence, UpdateCommittee, NewConstitution,
// TreasuryWithdrawal, Info) outright, regardless of votes.
func (m *DingoStateManager) inConwayBootstrap() bool {
	conwayPP, ok := m.protocolParams.(*conway.ConwayProtocolParameters)
	return ok && conwayPP.ProtocolVersion.Major == common.ProtocolVersionConway
}

// committeeActionRatified evaluates NoConfidence/UpdateCommittee
// ratification with a real stake-weighted DRep/SPO tally: it builds a
// deposit-inclusive governance.ProposalTally itself (reading UTxO stake from
// the real backend via m.db, since this state manager -- unlike
// MockStateManager -- has no in-memory UTxO set) and hands the actual
// ratification decision to ledger/governance's real ShouldRatify. That keeps
// threshold selection (MotionNoConfidence vs. CommitteeNormal/
// CommitteeNoConfidence), the Conway bootstrap gate, and
// committeeTermsWithinLimit under production's own tests instead of a second,
// hand-maintained copy of each -- an earlier revision duplicated the
// threshold selection and bootstrap gate here, and the duplication itself
// went untested and drifted (see PR #4333 review history).
//
// The one thing this cannot delegate to ShouldRatify is the tally itself:
// production's DRep voting-power query does not yet add a proposal's own
// deposit to its return account's DRep voting power (CIP-1694 counts an
// active proposal's deposit as part of the depositor's active voting stake).
// That gap is tracked separately as issue #4355 -- it affects every
// DRep-gated action type's real ratification, not just these two.
func (m *DingoStateManager) committeeActionRatified(
	txn *database.Txn,
	proposal *conformance.ProposalState,
	currentEpoch uint64,
) (bool, error) {
	conwayPP, ok := m.protocolParams.(*conway.ConwayProtocolParameters)
	if !ok {
		return false, fmt.Errorf(
			"committee action ratification: protocol parameters are %T, want *conway.ConwayProtocolParameters",
			m.protocolParams,
		)
	}

	deposits := m.activeProposalDeposits(currentEpoch)
	drepYes, drepTotal, err := m.drepStakeForCommitteeAction(
		txn, proposal, deposits, currentEpoch,
	)
	if err != nil {
		return false, err
	}
	spoYes, spoTotal, err := m.spoStakeForCommitteeAction(txn, proposal)
	if err != nil {
		return false, err
	}

	committeeNoConfidence, err := m.committeeInNoConfidence(txn)
	if err != nil {
		return false, err
	}

	// ShouldRatify's UpdateCommittee branch requires a decoded GovAction to
	// check committeeTermsWithinLimit; build one directly from the
	// proposed-member expiries already tracked in-memory rather than
	// round-tripping through CBOR. committeeTermsWithinLimit only reads
	// CredEpochs' values, so the keys need only be distinct pointers.
	var govAction common.GovAction
	if proposal.ActionType == common.GovActionTypeUpdateCommittee {
		govAction = syntheticUpdateCommitteeGovAction(proposal)
	}

	decision := governance.ShouldRatify(governance.RatifyInputs{
		Tally: &governance.ProposalTally{
			ActionType:     uint8(proposal.ActionType), //nolint:gosec // bounded by the small fixed set of GovActionType values
			DRepYesStake:   drepYes,
			DRepTotalStake: drepTotal,
			SPOYesStake:    spoYes,
			SPOTotalStake:  spoTotal,
		},
		PParams:               conwayPP,
		GovAction:             govAction,
		CurrentEpoch:          currentEpoch,
		MajorVersion:          conwayPP.ProtocolVersion.Major,
		CommitteeNoConfidence: committeeNoConfidence,
	})
	return decision.Ratified, nil
}

// committeeInNoConfidence reports whether the current committee-purpose root
// (the most recently enacted NoConfidence/UpdateCommittee action) is itself
// a NoConfidence action, mirroring ledger/governance's unexported
// committeeNoConfidenceState. ShouldRatify uses this, not simply whether a
// committee member is currently seated, to select CommitteeNormal vs.
// CommitteeNoConfidence for an UpdateCommittee proposal.
func (m *DingoStateManager) committeeInNoConfidence(
	txn *database.Txn,
) (bool, error) {
	rootID := m.govState.Roots.ConstitutionalCommittee
	if rootID == nil {
		return false, nil
	}
	root, err := m.lookupGovernanceProposal(txn, *rootID)
	if err != nil {
		return false, err
	}
	if root == nil {
		return false, nil
	}
	return common.GovActionType(root.ActionType) ==
		common.GovActionTypeNoConfidence, nil
}

// syntheticUpdateCommitteeGovAction builds just enough of a
// common.UpdateCommitteeGovAction for ShouldRatify's committeeTermsWithinLimit
// check: that function only reads CredEpochs' values (each proposed member's
// expiry epoch), never its keys. ProposedMembers is always a hash-only
// projection of ProposedMembersByCredential (ouroboros-mock's
// committeeMembersByHash only ever drops entries already covered by a full
// credential identity, never adds ones absent from it), so
// ProposedMembersByCredential alone is a complete source -- mirroring
// initialCommitteeProposalCbor, which builds a real UpdateCommitteeGovAction
// the same way for the CBOR-reconstruction path.
func syntheticUpdateCommitteeGovAction(
	proposal *conformance.ProposalState,
) *common.UpdateCommitteeGovAction {
	credEpochs := make(
		map[*common.Credential]uint64,
		len(proposal.ProposedMembersByCredential),
	)
	for credential, expiry := range proposal.ProposedMembersByCredential {
		cred := credential.AsCredential()
		credEpochs[&cred] = expiry
	}
	return &common.UpdateCommitteeGovAction{CredEpochs: credEpochs}
}

// activeProposalDeposits sums, per return-account credential, the deposits
// of every proposal still active at currentEpoch. Per CIP-1694 an active
// proposal's deposit counts as part of the depositor's active voting stake.
func (m *DingoStateManager) activeProposalDeposits(
	currentEpoch uint64,
) map[mockledger.RewardAccountKey]uint64 {
	deposits := make(map[mockledger.RewardAccountKey]uint64)
	for _, proposal := range m.govState.Proposals {
		if proposal == nil || proposal.ReturnAccount == nil ||
			proposal.Deposit == 0 || currentEpoch > proposal.ExpiresAfter {
			continue
		}
		deposits[*proposal.ReturnAccount] += proposal.Deposit
	}
	return deposits
}

// credentialVotingStake returns a stake credential's total voting stake:
// its real, live UTxO stake (read from the backend, since this state
// manager -- unlike MockStateManager -- has no in-memory UTxO set) plus its
// reward balance plus any active-proposal deposits routed to it.
func (m *DingoStateManager) credentialVotingStake(
	txn *database.Txn,
	credential mockledger.RewardAccountKey,
	deposits map[mockledger.RewardAccountKey]uint64,
) (uint64, error) {
	tag := conformanceCredentialTag(credential.AsCredential())
	utxoStake, err := m.db.GetControlledAmountByCredential(
		tag, credential.Credential[:], txn,
	)
	if err != nil {
		return 0, err
	}
	return utxoStake +
		m.govState.RewardAccountBalances[credential] +
		deposits[credential], nil
}

// drepStakeForCommitteeAction tallies DRep-delegated yes/total stake for a
// NoConfidence/UpdateCommittee proposal, for governance.ProposalTally's
// DRepYesStake/DRepTotalStake (threshold comparison is ShouldRatify's job,
// not this function's). AlwaysAbstain delegators are excluded entirely.
// AlwaysNoConfidence delegators count toward the total always, and toward
// yes too when the proposal itself is a NoConfidence action -- mirroring
// production's tallyDRepVotes, which treats the AlwaysNoConfidence virtual
// DRep as an automatic yes specifically on a NoConfidence action and an
// automatic no otherwise. A credential-backed DRep must be active to count,
// and an explicit Abstain vote on this proposal excludes its delegated
// stake from the total.
func (m *DingoStateManager) drepStakeForCommitteeAction(
	txn *database.Txn,
	proposal *conformance.ProposalState,
	deposits map[mockledger.RewardAccountKey]uint64,
	currentEpoch uint64,
) (uint64, uint64, error) {
	isNoConfidence := proposal.ActionType == common.GovActionTypeNoConfidence
	yesStake := new(big.Int)
	totalStake := new(big.Int)
	for credential, delegation := range m.govState.DRepDelegationsByCredential {
		switch delegation.Type {
		case common.DrepTypeAbstain:
			continue
		case common.DrepTypeNoConfidence:
			stake, err := m.credentialVotingStake(txn, credential, deposits)
			if err != nil {
				return 0, 0, err
			}
			stakeInt := new(big.Int).SetUint64(stake)
			totalStake.Add(totalStake, stakeInt)
			if isNoConfidence {
				yesStake.Add(yesStake, stakeInt)
			}
		case common.DrepTypeAddrKeyHash, common.DrepTypeScriptHash:
			if len(delegation.Credential) != common.Blake2b224Size {
				continue
			}
			drepCredential := common.Credential{
				CredType:   common.CredentialTypeAddrKeyHash,
				Credential: common.NewBlake2b224(delegation.Credential),
			}
			voterType := common.VoterTypeDRepKeyHash
			if delegation.Type == common.DrepTypeScriptHash {
				drepCredential.CredType = common.CredentialTypeScriptHash
				voterType = common.VoterTypeDRepScriptHash
			}
			if !m.govState.IsDRepCredentialActive(
				drepCredential, currentEpoch,
			) {
				continue
			}
			stake, err := m.credentialVotingStake(txn, credential, deposits)
			if err != nil {
				return 0, 0, err
			}
			stakeInt := new(big.Int).SetUint64(stake)
			vote, voted := proposal.Votes[fmt.Sprintf(
				"%d:%s",
				voterType,
				hex.EncodeToString(drepCredential.Credential[:]),
			)]
			if voted && vote == 2 {
				continue
			}
			totalStake.Add(totalStake, stakeInt)
			if voted && vote == 1 {
				yesStake.Add(yesStake, stakeInt)
			}
		}
	}
	yes, err := bigIntToUint64(yesStake, "drep yes stake")
	if err != nil {
		return 0, 0, err
	}
	total, err := bigIntToUint64(totalStake, "drep total stake")
	if err != nil {
		return 0, 0, err
	}
	return yes, total, nil
}

// spoStakeForCommitteeAction tallies pool-delegated yes/total stake for a
// NoConfidence/UpdateCommittee proposal, for governance.ProposalTally's
// SPOYesStake/SPOTotalStake. There is no Conway-bootstrap branch here:
// ShouldRatify itself refuses both action types outright during bootstrap
// before ever reading this tally, so a bootstrap-specific adjustment to the
// tally would never run (an earlier revision carried one that PR #4333
// review found was already dead code for exactly this reason).
//
// Active-proposal deposits are deliberately excluded from this tally: a
// deposit raises the return account's DRep voting power, not the delegated
// stake behind a pool. Production reads SPO stake straight from the stake
// distribution snapshot (tallySPOVotes over LoadSPOVotingState's Dist in
// ledger/governance/tally.go), which carries no deposit adjustment, so
// credentialVotingStake is called here with a nil deposit map while
// drepStakeForCommitteeAction keeps the deposit-inclusive one. A silent
// pool whose reward account delegates AlwaysAbstain is excluded; one that
// delegates AlwaysNoConfidence counts as an implicit Yes when the proposal
// is a NoConfidence action (an implicit No otherwise, same as production's
// tallySPOVotes PoolRewardAccountAutoVoteNoConfidence handling); every other
// silent pool counts as an implicit No.
func (m *DingoStateManager) spoStakeForCommitteeAction(
	txn *database.Txn,
	proposal *conformance.ProposalState,
) (uint64, uint64, error) {
	poolStake := make(map[common.PoolKeyHash]*big.Int)
	for credential, pool := range m.govState.PoolDelegationsByCredential {
		if !m.govState.IsPoolRegistered(pool) {
			continue
		}
		stake, err := m.credentialVotingStake(txn, credential, nil)
		if err != nil {
			return 0, 0, err
		}
		if current, ok := poolStake[pool]; ok {
			current.Add(current, new(big.Int).SetUint64(stake))
		} else {
			poolStake[pool] = new(big.Int).SetUint64(stake)
		}
	}

	isNoConfidence := proposal.ActionType == common.GovActionTypeNoConfidence
	yesStake := new(big.Int)
	totalStake := new(big.Int)
	for pool, stake := range poolStake {
		vote, voted := proposal.Votes[fmt.Sprintf(
			"%d:%s",
			common.VoterTypeStakingPoolKeyHash,
			hex.EncodeToString(pool[:]),
		)]
		if voted {
			switch vote {
			case 1:
				yesStake.Add(yesStake, stake)
				totalStake.Add(totalStake, stake)
			case 0:
				totalStake.Add(totalStake, stake)
			}
			continue
		}
		if rewardAccount, ok := m.govState.PoolRewardAccounts[pool]; ok {
			if delegation, ok := m.govState.DRepDelegationsByCredential[rewardAccount]; ok {
				switch delegation.Type {
				case common.DrepTypeAbstain:
					continue
				case common.DrepTypeNoConfidence:
					totalStake.Add(totalStake, stake)
					if isNoConfidence {
						yesStake.Add(yesStake, stake)
					}
					continue
				}
			}
		}
		totalStake.Add(totalStake, stake)
	}
	yes, err := bigIntToUint64(yesStake, "spo yes stake")
	if err != nil {
		return 0, 0, err
	}
	total, err := bigIntToUint64(totalStake, "spo total stake")
	if err != nil {
		return 0, 0, err
	}
	return yes, total, nil
}

// bigIntToUint64 converts a non-negative stake total to uint64, failing
// closed instead of silently truncating if it ever somehow exceeds uint64
// range (real ADA amounts never approach this bound).
func bigIntToUint64(v *big.Int, name string) (uint64, error) {
	if !v.IsUint64() {
		return 0, fmt.Errorf(
			"committee action ratification: %s overflows uint64: %s",
			name, v.String(),
		)
	}
	return v.Uint64(), nil
}

// persistRatification sets the real governance_proposal row's ratification
// epoch and boundary slot as one rollback-safe state transition. A proposal
// id with no matching real row (a synthetic
// initial-state seed with no originating transaction) is left as an
// in-memory-only decision -- there is nothing to persist for it.
func (m *DingoStateManager) persistRatification(
	txn *database.Txn,
	id string,
	epoch uint64,
	boundarySlot uint64,
) error {
	proposal, err := m.lookupGovernanceProposal(txn, id)
	if proposal == nil || err != nil {
		return err
	}
	ratifiedEpoch := epoch
	ratifiedSlot := boundarySlot
	proposal.RatifiedEpoch = &ratifiedEpoch
	proposal.RatifiedSlot = &ratifiedSlot
	return m.db.SetGovernanceProposal(proposal, txn)
}

// enactProposal applies a ratified proposal's effects to the pre-validation
// govState mirror (unchanged decision/bookkeeping logic) and, when the real
// backend row carries a decodable governance action, persists the same
// enactment through the real governance.EnactProposal -- which applies the
// committee/protocol-parameter/constitution/treasury side effects via
// production code, not a second hand-rolled implementation of them.
func (m *DingoStateManager) enactProposal(
	txn *database.Txn,
	id string,
	proposal *conformance.ProposalState,
	boundarySlot uint64,
) error {
	//exhaustive:ignore
	switch proposal.ActionType {
	case common.GovActionTypeNewConstitution:
		m.govState.Roots.Constitution = &id
		if m.govState.Constitution == nil {
			m.govState.Constitution = &conformance.ConstitutionInfo{}
		}
		if len(proposal.PolicyHash) > 0 {
			m.govState.Constitution.PolicyHash = append(
				[]byte(nil),
				proposal.PolicyHash...,
			)
		} else {
			m.govState.Constitution.PolicyHash = nil
		}
	case common.GovActionTypeParameterChange:
		m.govState.Roots.ProtocolParameters = &id
	case common.GovActionTypeHardForkInitiation:
		m.govState.Roots.HardFork = &id
	case common.GovActionTypeNoConfidence:
		m.govState.Roots.ConstitutionalCommittee = &id
		for coldKey := range m.govState.CommitteeMembers {
			delete(m.govState.CommitteeMembers, coldKey)
		}
	case common.GovActionTypeUpdateCommittee:
		m.govState.Roots.ConstitutionalCommittee = &id
		if removed, ok := m.committeeRemovals[id]; ok {
			for coldKey := range removed {
				delete(m.govState.CommitteeMembers, coldKey)
			}
			delete(m.committeeRemovals, id)
		}
		for coldKey, expiry := range proposal.ProposedMembers {
			m.govState.CommitteeMembers[coldKey] = &conformance.CommitteeMemberInfo{
				ColdKey:     coldKey,
				ExpiryEpoch: expiry,
			}
		}
		delete(m.committeeQuorums, id)
	}

	m.govState.EnactedProposals[id] = true
	delete(m.govState.Proposals, id)

	return m.persistEnactment(txn, id, boundarySlot)
}

// persistEnactment loads the real governance_proposal row for id and either
// applies its enactment side effects through the real governance.EnactProposal
// (when the row carries a decodable action) or, failing that, just records
// the enactment epoch for read-side visibility.
func (m *DingoStateManager) persistEnactment(
	txn *database.Txn,
	id string,
	boundarySlot uint64,
) error {
	dbProposal, err := m.lookupGovernanceProposal(txn, id)
	if dbProposal == nil || err != nil {
		return err
	}

	// TreasuryWithdrawal enactment moves real lovelace out of
	// NetworkState.Treasury, which this harness never seeds (treasury/pot
	// accounting is reward/treasury machinery explicitly out of scope --
	// see ProcessEpochBoundary's doc comment). Calling the real side effect
	// against an unseeded (zero) treasury would reject withdrawals the
	// vectors submit successfully, so this action type is persisted the
	// same way as an action with no decodable CBOR: enacted for read-side
	// visibility, without applying the production side effect.
	skipSideEffect := len(dbProposal.GovActionCbor) == 0 ||
		common.GovActionType(dbProposal.ActionType) ==
			common.GovActionTypeTreasuryWithdrawal
	if skipSideEffect {
		enactedEpoch := m.currentEpoch
		dbProposal.EnactedEpoch = &enactedEpoch
		return m.db.SetGovernanceProposal(dbProposal, txn)
	}

	conwayPP, ok := m.protocolParams.(*conway.ConwayProtocolParameters)
	if !ok {
		return fmt.Errorf(
			"enact governance proposal: protocol parameters are %T, want *conway.ConwayProtocolParameters",
			m.protocolParams,
		)
	}
	result, err := governance.EnactProposal(&governance.EnactmentContext{
		DB:       m.db,
		Txn:      txn,
		Epoch:    m.currentEpoch,
		Slot:     boundarySlot,
		PParams:  conwayPP,
		UpdateFn: eras.ConwayEraDesc.PParamsUpdateFunc,
	}, dbProposal)
	if err != nil {
		return fmt.Errorf("enact governance proposal: %w", err)
	}
	if result.PParamsChanged {
		m.protocolParams = result.UpdatedPParams
	}
	return nil
}

// expireGovernanceProposal marks the real governance_proposal row expired
// at newEpoch.
func (m *DingoStateManager) expireGovernanceProposal(
	txn *database.Txn,
	id string,
	newEpoch uint64,
) error {
	dbProposal, err := m.lookupGovernanceProposal(txn, id)
	if dbProposal == nil || err != nil {
		return err
	}
	expiredEpoch := newEpoch
	expiredSlot := newEpoch * conformanceSlotsPerEpoch
	dbProposal.ExpiredEpoch = &expiredEpoch
	dbProposal.ExpiredSlot = &expiredSlot
	return m.db.SetGovernanceProposal(dbProposal, txn)
}

// lookupGovernanceProposal fetches the real governance_proposal row for a
// harness proposal id ("txHash#actionIndex"). A row that genuinely doesn't
// exist (models.ErrGovernanceProposalNotFound) is reported as (nil, nil):
// callers treat that as "nothing to persist for this id" rather than an
// error, since not every harness-tracked proposal id necessarily has a
// corresponding real row (see the synthetic-seed cases above). Any other
// error is a real failure and is propagated.
func (m *DingoStateManager) lookupGovernanceProposal(
	txn *database.Txn,
	id string,
) (*models.GovernanceProposal, error) {
	txHash := parseProposalTxHash(id)
	actionIdx := parseProposalActionIdx(id)
	proposal, err := m.db.GetGovernanceProposal(txHash, actionIdx, txn)
	if errors.Is(err, models.ErrGovernanceProposalNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup governance proposal %s: %w", id, err)
	}
	return proposal, nil
}

// GetStateProvider implements conformance.StateManager.GetStateProvider.
func (m *DingoStateManager) GetStateProvider() conformance.StateProvider {
	return NewDingoStateProvider(m)
}

// GetGovernanceState implements conformance.StateManager.GetGovernanceState.
func (m *DingoStateManager) GetGovernanceState() *conformance.GovernanceState {
	return m.govState
}

// GetStateSnapshot implements conformance.StateSnapshotProvider. The
// conformance harness compares only observable ledger state; the database
// remains authoritative for validation while this projection tracks the
// committed UTxO identities alongside the same writes.
func (m *DingoStateManager) GetStateSnapshot() *conformance.StateSnapshot {
	utxoIDs := make([]string, 0, len(m.utxoIDs))
	for id := range m.utxoIDs {
		utxoIDs = append(utxoIDs, id)
	}
	sort.Strings(utxoIDs)
	return &conformance.StateSnapshot{
		CurrentEpoch:                   m.currentEpoch,
		UtxoIDs:                        utxoIDs,
		StakeRegistrationsByCredential: maps.Clone(m.govState.StakeRegistrationsByCredential),
		RewardAccountBalances:          maps.Clone(m.govState.RewardAccountBalances),
		StakeCredentialDeposits:        maps.Clone(m.stakeDeposits),
		PoolRegistrations:              maps.Clone(m.govState.PoolRegistrations),
		Governance:                     m.govState,
	}
}

func (m *DingoStateManager) addUtxoID(hash common.Blake2b256, index int) {
	m.utxoIDs[fmt.Sprintf("%x#%d", hash[:], index)] = struct{}{}
}

func (m *DingoStateManager) removeUtxoIDs(inputs []common.TransactionInput) {
	for _, input := range inputs {
		delete(m.utxoIDs, input.String())
	}
}

// SetRewardBalances implements conformance.StateManager.SetRewardBalances.
//
// Reward-account balances are injected by the harness itself (precomputed
// from the vector's final_state plus future withdrawals -- see
// ouroboros-mock/conformance's adjustRewardBalances), not derived from
// anything Dingo's ApplyTransaction commits from decoded block data the way
// UTxOs/certificates/governance rows are. There is also no real backend
// primitive for an absolute reward-balance set (only
// Database.Add*AccountRewardByCredential, which credits a delta and can't
// express a decrease). Real reward calculation
// (ledger/chainsync.go's applyStakeRewards) is explicitly out of scope for
// this harness. Balances therefore stay in the govState mirror, matching
// how the upstream harness already treats them as synthetic validation
// input rather than application state.
func (m *DingoStateManager) SetRewardBalances(
	balances map[common.Blake2b224]uint64,
) {
	for credential, registered := range m.govState.StakeRegistrationsByCredential {
		if !registered {
			continue
		}
		balance, exists := balances[credential.Credential]
		if !exists {
			continue
		}
		m.govState.RewardAccountBalances[credential] = balance
	}
	m.syncRewardBalanceMirrors()
}

// SetRewardAccountBalances implements conformance.RewardAccountBalanceSetter.
// It updates registered accounts by full credential identity without
// creating or removing registrations. See SetRewardBalances's doc comment
// for why this stays in the govState mirror.
func (m *DingoStateManager) SetRewardAccountBalances(
	balances map[mockledger.RewardAccountKey]uint64,
) {
	for credential, registered := range m.govState.StakeRegistrationsByCredential {
		if !registered {
			continue
		}
		balance, exists := balances[credential]
		if !exists {
			continue
		}
		m.govState.RewardAccountBalances[credential] = balance
	}
	m.syncRewardBalanceMirrors()
}

func (m *DingoStateManager) syncRewardBalanceMirrors() {
	if m.govState == nil {
		return
	}
	m.govState.RewardAccounts = rewardBalancesByHash(
		m.govState.RewardAccountBalances,
	)
}

func rewardBalancesByHash(
	balances map[mockledger.RewardAccountKey]uint64,
) map[common.Blake2b224]uint64 {
	result := make(map[common.Blake2b224]uint64, len(balances))
	for credential, balance := range balances {
		_, exists := result[credential.Credential]
		if !exists ||
			credential.CredType == common.CredentialTypeAddrKeyHash {
			result[credential.Credential] = balance
		}
	}
	return result
}

// GetProtocolParameters implements conformance.StateManager.GetProtocolParameters.
func (m *DingoStateManager) GetProtocolParameters() common.ProtocolParameters {
	return m.protocolParams
}

// proposalToModel converts a governance proposal to a database model. This
// is the real database.SetGovernanceProposal input type -- not a stale,
// conformance-local shape -- confirmed against
// database/plugin/metadata/store.go's GovernanceStore methods.
func (m *DingoStateManager) proposalToModel(
	id string,
	info conformance.GovActionInfo,
) models.GovernanceProposal {
	txHash := parseProposalTxHash(id)
	actionIdx := parseProposalActionIdx(id)
	actionType := uint8(info.ActionType) //nolint:gosec // GovActionType is 0-6.

	proposal := models.GovernanceProposal{
		TxHash:        txHash,
		ActionIndex:   actionIdx,
		ActionType:    actionType,
		ProposedEpoch: info.SubmittedEpoch,
		ExpiresEpoch:  info.ExpiresAfter,
		PolicyHash:    info.PolicyHash,
	}
	if info.ActionType == common.GovActionTypeUpdateCommittee {
		proposal.GovActionCbor = initialCommitteeProposalCbor(info)
	}

	if info.ParentActionId != nil {
		parentTxHash := parseProposalTxHash(*info.ParentActionId)
		parentIdx := parseProposalActionIdx(*info.ParentActionId)
		proposal.ParentTxHash = parentTxHash
		proposal.ParentActionIdx = &parentIdx
	}

	if info.RatifiedEpoch != nil {
		ratifiedEpoch := *info.RatifiedEpoch
		ratifiedSlot := ratifiedEpoch * conformanceSlotsPerEpoch
		proposal.RatifiedEpoch = &ratifiedEpoch
		proposal.RatifiedSlot = &ratifiedSlot
	}

	return proposal
}

// initialCommitteeProposalCbor reconstructs the action body needed by Dingo's
// committee lookup for a proposal imported from a vector initial_state. The
// vector's canonical projection carries the committee additions/removals and
// parent, but not the original action bytes.
func initialCommitteeProposalCbor(info conformance.GovActionInfo) []byte {
	action := &common.UpdateCommitteeGovAction{
		Type:        uint(common.GovActionTypeUpdateCommittee),
		Credentials: make([]common.Credential, 0, len(info.RemovedMembers)),
		CredEpochs:  make(map[*common.Credential]uint64),
		Quorum:      cbor.Rat{Rat: big.NewRat(0, 1)},
	}
	for credential := range info.RemovedMembers {
		action.Credentials = append(action.Credentials, credential.AsCredential())
	}
	for credential, epoch := range info.ProposedMembersByCredential {
		cred := credential.AsCredential()
		action.CredEpochs[&cred] = epoch
	}
	if info.ParentActionId != nil {
		parent := &common.GovActionId{}
		copy(parent.TransactionId[:], parseProposalTxHash(*info.ParentActionId))
		parent.GovActionIdx = parseProposalActionIdx(*info.ParentActionId)
		action.ActionId = parent
	}
	encoded, err := cbor.Encode(action)
	if err != nil {
		return nil
	}
	return encoded
}

// Helper functions

func parseProposalTxHash(id string) []byte {
	if len(id) < 64 {
		return nil
	}
	hashStr := id[:64]
	hashBytes, _ := hex.DecodeString(hashStr)
	return hashBytes
}

func parseProposalActionIdx(id string) uint32 {
	if len(id) < 66 {
		return 0
	}
	idxStr := id[65:]
	var idx uint32
	_, _ = fmt.Sscanf(idxStr, "%d", &idx)
	return idx
}

func getActionType(action common.GovAction) common.GovActionType {
	// The Conway parameter-change action is a distinct concrete type; it
	// is handled explicitly rather than relying on the default so a
	// future era adds-a-type doesn't silently misclassify.
	switch action.(type) {
	case *conway.ConwayParameterChangeGovAction:
		return common.GovActionTypeParameterChange
	case *common.HardForkInitiationGovAction:
		return common.GovActionTypeHardForkInitiation
	case *common.TreasuryWithdrawalGovAction:
		return common.GovActionTypeTreasuryWithdrawal
	case *common.NoConfidenceGovAction:
		return common.GovActionTypeNoConfidence
	case *common.UpdateCommitteeGovAction:
		return common.GovActionTypeUpdateCommittee
	case *common.NewConstitutionGovAction:
		return common.GovActionTypeNewConstitution
	case *common.InfoGovAction:
		return common.GovActionTypeInfo
	default:
		return common.GovActionTypeParameterChange
	}
}

func extractActionSpecificData(
	action common.GovAction,
	info *conformance.GovActionInfo,
) {
	switch ga := action.(type) {
	case *common.UpdateCommitteeGovAction:
		if ga.ActionId != nil {
			key := fmt.Sprintf(
				"%x#%d",
				ga.ActionId.TransactionId[:],
				ga.ActionId.GovActionIdx,
			)
			info.ParentActionId = &key
		}
		for cred, epoch := range ga.CredEpochs {
			if cred != nil {
				info.ProposedMembers[cred.Credential] = uint64(epoch)
				if info.ProposedMembersByCredential == nil {
					info.ProposedMembersByCredential = make(
						map[mockledger.RewardAccountKey]uint64,
					)
				}
				info.ProposedMembersByCredential[rewardAccountKey(*cred)] = uint64(epoch)
			}
		}
		info.RemovedMembers = make(map[mockledger.RewardAccountKey]bool)
		for _, cred := range ga.Credentials {
			info.RemovedMembers[rewardAccountKey(cred)] = true
		}
	case *common.NoConfidenceGovAction:
		if ga.ActionId != nil {
			key := fmt.Sprintf(
				"%x#%d",
				ga.ActionId.TransactionId[:],
				ga.ActionId.GovActionIdx,
			)
			info.ParentActionId = &key
		}
	case *common.HardForkInitiationGovAction:
		if ga.ActionId != nil {
			key := fmt.Sprintf(
				"%x#%d",
				ga.ActionId.TransactionId[:],
				ga.ActionId.GovActionIdx,
			)
			info.ParentActionId = &key
		}
		info.ProtocolVersion = &conformance.ProtocolVersionInfo{
			Major: ga.ProtocolVersion.Major,
			Minor: ga.ProtocolVersion.Minor,
		}
	case *common.NewConstitutionGovAction:
		if ga.ActionId != nil {
			key := fmt.Sprintf(
				"%x#%d",
				ga.ActionId.TransactionId[:],
				ga.ActionId.GovActionIdx,
			)
			info.ParentActionId = &key
		}
		if len(ga.Constitution.ScriptHash) > 0 {
			info.PolicyHash = make([]byte, len(ga.Constitution.ScriptHash))
			copy(info.PolicyHash, ga.Constitution.ScriptHash)
		}
	case *conway.ConwayParameterChangeGovAction:
		if ga.ActionId != nil {
			key := fmt.Sprintf(
				"%x#%d",
				ga.ActionId.TransactionId[:],
				ga.ActionId.GovActionIdx,
			)
			info.ParentActionId = &key
		}
		info.ParameterUpdate = &ga.ParamUpdate
	}
}

// dingoTransactionInput implements common.TransactionInput for mock UTxOs.
type dingoTransactionInput struct {
	txId  common.Blake2b256
	index uint32
}

func (d *dingoTransactionInput) Id() common.Blake2b256 {
	return d.txId
}

func (d *dingoTransactionInput) Index() uint32 {
	return d.index
}

func (d *dingoTransactionInput) String() string {
	return fmt.Sprintf("%x#%d", d.txId[:], d.index)
}

func (d *dingoTransactionInput) Utxorpc() (*utxorpc.TxInput, error) {
	return &utxorpc.TxInput{
		TxHash:      d.txId[:],
		OutputIndex: d.index,
	}, nil
}

func (d *dingoTransactionInput) ToPlutusData() data.PlutusData {
	return data.NewConstr(0,
		data.NewByteString(d.txId[:]),
		data.NewInteger(big.NewInt(int64(d.index))),
	)
}

// Compile-time interface check
var _ conformance.StateManager = (*DingoStateManager)(nil)
