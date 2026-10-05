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

package conformance

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// TestStateProviderConstitutionExposesStoredShape proves the conformance
// provider reports the same anchor and guardrails policy hash the backend
// holds, in the same shape production's ledger.LedgerView.Constitution
// reports. It previously returned an empty common.Constitution regardless
// of what the backend held.
func TestStateProviderConstitutionExposesStoredShape(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	anchorHash := testHash32(0xc1)
	policyHash := bytes.Repeat([]byte{0xc2}, 28)
	require.NoError(t, m.db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.invalid/conformance",
		AnchorHash: anchorHash,
		PolicyHash: policyHash,
		AddedSlot:  0,
	}, nil))

	got, err := m.GetStateProvider().Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "https://example.invalid/conformance", got.Anchor.Url)
	require.Equal(t, anchorHash, got.Anchor.DataHash[:])
	require.Equal(t, policyHash, got.ScriptHash)
}

// TestStateProviderConstitutionWithoutPolicyHash proves a constitution with
// no guardrails script is reported with a nil ScriptHash, which is what
// gouroboros' guardrails rule reads as "proposals must carry no policy
// hash".
func TestStateProviderConstitutionWithoutPolicyHash(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	anchorHash := testHash32(0xd1)
	require.NoError(t, m.db.SetConstitution(&models.Constitution{
		AnchorURL:  "https://example.invalid/conformance-plain",
		AnchorHash: anchorHash,
		AddedSlot:  0,
	}, nil))

	got, err := m.GetStateProvider().Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, anchorHash, got.Anchor.DataHash[:])
	require.Nil(t, got.ScriptHash)
}

// TestStateProviderConstitutionMissingFailsClosed proves a backend with no
// constitution row is reported as unavailable rather than as a valid
// constitution with no guardrails script, so a vector whose constitution
// never reached the backend fails instead of silently passing guardrails
// validation.
func TestStateProviderConstitutionMissingFailsClosed(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	got, err := m.GetStateProvider().Constitution()
	require.ErrorIs(t, err, governance.ErrConstitutionUnavailable)
	require.Nil(t, got)
}

// TestLoadInitialStateSeedsConstitution proves a vector's initial
// constitution is written to the real backend, so the read side above has
// something to read without consulting the pre-validation govState mirror.
func TestLoadInitialStateSeedsConstitution(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	anchorHash := testHash32(0xe1)
	policyHash := bytes.Repeat([]byte{0xe2}, 28)
	require.NoError(t, m.LoadInitialState(
		&conformance.ParsedInitialState{
			Constitution: &conformance.ConstitutionInfo{
				AnchorURL:  "https://example.invalid/vector",
				AnchorHash: anchorHash,
				PolicyHash: policyHash,
			},
		},
		&conway.ConwayProtocolParameters{},
	))

	stored, err := m.db.GetConstitution(nil)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, "https://example.invalid/vector", stored.AnchorURL)
	require.Equal(t, anchorHash, stored.AnchorHash)
	require.Equal(t, policyHash, stored.PolicyHash)

	got, err := m.GetStateProvider().Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, anchorHash, got.Anchor.DataHash[:])
	require.Equal(t, policyHash, got.ScriptHash)
}

func TestDingoStateProviderEpochForSlotUsesReplayEpoch(t *testing.T) {
	t.Parallel()

	provider := NewDingoStateProvider(&DingoStateManager{
		currentEpoch: 42,
	})

	epoch, err := provider.EpochForSlot(123)
	require.NoError(t, err)
	require.Equal(t, uint64(42), epoch)
}

// The corpus documents keyDeposit=2000000 for its stake vectors, and every
// registration it declares was made at that value, so a corpus vector alone
// cannot tell a recorded refund from the KeyDeposit fallback -- the two
// numbers coincide. These constants deliberately separate them: the state is
// seeded at a recorded deposit that is not the KeyDeposit in effect during
// validation, so the two candidate refunds give opposite value-conservation
// outcomes.
const (
	stakeDepositVectorRecorded   = uint64(5_000_000)
	stakeDepositVectorKeyDeposit = uint64(2_000_000)
)

func stakeDepositVectorPparams(
	keyDeposit uint64,
) *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: common.ProtocolParametersProtocolVersion{
			Major: 9,
		},
		//nolint:gosec // G115: test-scoped constants do not overflow
		KeyDeposit:           uint(keyDeposit),
		MaxTxSize:            16_384,
		MaxValueSize:         5_000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
	}
}

func stakeDepositVectorCredential() common.Credential {
	return common.Credential{
		CredType: common.CredentialTypeAddrKeyHash,
		Credential: common.NewBlake2b224(
			bytes.Repeat([]byte{0xd1}, common.AddressHashSize),
		),
	}
}

// loadStakeDepositVector seeds a vector-shaped initial state declaring the
// credential already registered, with the given KeyDeposit in force. The
// deposit recorded for the credential is derived from these protocol
// parameters, matching how the harness seeds every corpus vector.
func loadStakeDepositVector(
	t *testing.T,
	m *DingoStateManager,
	cred common.Credential,
	keyDeposit uint64,
) {
	t.Helper()
	key := mockledger.NewRewardAccountKey(cred)
	require.NoError(t, m.LoadInitialState(
		&conformance.ParsedInitialState{
			CurrentEpoch: 1,
			StakeRegistrationsByCredential: map[mockledger.RewardAccountKey]bool{
				key: true,
			},
			RewardAccountBalances: map[mockledger.RewardAccountKey]uint64{
				key: 0,
			},
		},
		stakeDepositVectorPparams(keyDeposit),
	))
}

// stakeDepositVectorTx is a legacy stake deregistration with no inputs and no
// outputs, so value conservation reduces to "refund must equal fee" and
// isolates the recorded-deposit lookup from every other term.
func stakeDepositVectorTx(
	cred common.Credential,
	fee uint64,
) *conway.ConwayTransaction {
	return &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxFee: fee,
			TxCertificates: []common.CertificateWrapper{
				{
					Type: uint(
						common.CertificateTypeStakeDeregistration,
					),
					Certificate: &common.StakeDeregistrationCertificate{
						CertType: uint(
							common.CertificateTypeStakeDeregistration,
						),
						StakeCredential: cred,
					},
				},
			},
		},
	}
}

// TestConformanceProviderRefundsRecordedStakeDepositNotKeyDeposit is the
// It runs gouroboros'
// UtxoValidateValueNotConservedUtxo against the conformance state provider
// with a recorded deposit of 5 ADA while the KeyDeposit in force during
// validation is 2 ADA.
//
// Without DingoStateProvider.StakeCredentialDeposit the rule's optional type
// assertion misses, the refund silently becomes the 2 ADA KeyDeposit, and the
// 5 ADA transaction fails value conservation. That is the gap the issue
// describes: the corpus could not distinguish a correct recorded refund from
// the fallback.
func TestConformanceProviderRefundsRecordedStakeDepositNotKeyDeposit(
	t *testing.T,
) {
	require.NotEqual(
		t,
		stakeDepositVectorKeyDeposit,
		stakeDepositVectorRecorded,
		"the recorded deposit must differ from KeyDeposit for this vector to discriminate",
	)

	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	cred := stakeDepositVectorCredential()
	// Seed the registration at the recorded deposit.
	loadStakeDepositVector(t, m, cred, stakeDepositVectorRecorded)
	provider := m.GetStateProvider()
	require.True(t, provider.IsStakeCredentialRegistered(cred))

	// Validate under a *different* KeyDeposit, so the recorded value and the
	// fallback disagree.
	validationPparams := stakeDepositVectorPparams(
		stakeDepositVectorKeyDeposit,
	)

	// Refunded at the recorded 5 ADA: a 5 ADA fee conserves value.
	require.NoError(t, conway.UtxoValidateValueNotConservedUtxo(
		stakeDepositVectorTx(cred, stakeDepositVectorRecorded),
		200,
		provider,
		validationPparams,
	))

	// Refunded at the 2 ADA KeyDeposit instead: rejected. This is the
	// assertion that fails when the provider lacks the capability, because
	// the fallback would make this the accepted case and the one above the
	// rejected one.
	require.ErrorContains(t, conway.UtxoValidateValueNotConservedUtxo(
		stakeDepositVectorTx(cred, stakeDepositVectorKeyDeposit),
		200,
		provider,
		validationPparams,
	), "value not conserved")

	// Supporting evidence, discovered exactly the way the rule discovers it:
	// a runtime type assertion on the value the harness passes as the ledger
	// state. If this assertion misses, the rule takes its silent fallback.
	depositState, ok := provider.(common.StakeCredentialDepositState)
	require.True(
		t,
		ok,
		"the conformance provider must satisfy StakeCredentialDepositState, or the corpus never exercises the recorded refund",
	)
	recorded, err := depositState.StakeCredentialDeposit(cred)
	require.NoError(t, err)
	require.NotNil(t, recorded)
	require.Equal(t, stakeDepositVectorRecorded, *recorded)
}

// TestConformanceProviderStakeDepositAbsentForUnregisteredCredential pins the
// nil contract the rule depends on: an unregistered credential reports
// absence, which is what sends value conservation to its KeyDeposit fallback
// rather than to a refund of zero.
func TestConformanceProviderStakeDepositAbsentForUnregisteredCredential(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	cred := stakeDepositVectorCredential()
	provider := m.GetStateProvider()
	require.False(t, provider.IsStakeCredentialRegistered(cred))

	depositState, ok := provider.(common.StakeCredentialDepositState)
	require.True(t, ok)
	recorded, err := depositState.StakeCredentialDeposit(cred)
	require.NoError(t, err)
	require.Nil(t, recorded)
}

// TestStateProviderTreasuryValueReportsBackendState proves the conformance
// provider reports the treasury the backend holds, in the same shape
// production's ledger.LedgerView.TreasuryValue reports. It previously
// returned a hardcoded zero regardless of backend state.
func TestStateProviderTreasuryValueReportsBackendState(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	const treasury = uint64(87_920_693_660_807)
	require.NoError(
		t,
		m.db.Metadata().SetNetworkState(treasury, 1_000, 42, nil),
	)

	got, err := m.GetStateProvider().TreasuryValue()
	require.NoError(t, err)
	require.Equal(t, treasury, got)
}

// TestStateProviderTreasuryValueMissingFailsClosed proves a backend with no
// network-state row is reported as unavailable rather than as a treasury of
// zero. The upstream current-treasury-value rule compares for equality once a
// transaction body carries key 21, so a synthetic zero would silently reject
// every vector declaring a non-zero treasury and silently accept one
// declaring zero.
func TestStateProviderTreasuryValueMissingFailsClosed(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	got, err := m.GetStateProvider().TreasuryValue()
	require.ErrorContains(t, err, "treasury network state is unavailable")
	require.Zero(t, got)
}

// TestStateProviderTreasuryValueTracksLatestSlot proves the provider reads
// the newest network-state row rather than the first one written, so a
// vector that advances the treasury is validated against the value the
// backend currently holds.
func TestStateProviderTreasuryValueTracksLatestSlot(t *testing.T) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	require.NoError(t, m.db.Metadata().SetNetworkState(100, 900, 10, nil))
	require.NoError(t, m.db.Metadata().SetNetworkState(250, 750, 20, nil))

	got, err := m.GetStateProvider().TreasuryValue()
	require.NoError(t, err)
	require.Equal(t, uint64(250), got)
}

// Stored committee credentials of the wrong length are corrupt state: the
// harness's committee lookups must fail rather than truncate them into a
// 28-byte credential that matches a real hot key or cold credential.
func TestCommitteeVotingStateRejectsMalformedStoredHashes(t *testing.T) {
	hot := testHash28(0x91)
	cold := testHash28(0x92)
	keyCredential := func(hash common.Blake2b224) common.Credential {
		return common.Credential{
			CredType:   common.CredentialTypeAddrKeyHash,
			Credential: hash,
		}
	}
	overlong := func(hash common.Blake2b224) []byte {
		return append(append([]byte(nil), hash[:]...), 0xff)
	}
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, raw *sql.DB)
		lookup func(p *DingoStateProvider) error
	}{
		{
			name: "authorized hot credential",
			mutate: func(t *testing.T, raw *sql.DB) {
				_, err := raw.Exec(
					`UPDATE auth_committee_hot SET host_credential = ?`,
					overlong(hot),
				)
				require.NoError(t, err)
			},
			lookup: func(p *DingoStateProvider) error {
				_, err := p.CommitteeHotCredentialMember(keyCredential(hot))
				if err != nil {
					return err
				}
				_, err = p.CommitteeHotCredentialColdCredentials(
					keyCredential(hot),
				)
				return err
			},
		},
		{
			name: "seated cold credential",
			mutate: func(t *testing.T, raw *sql.DB) {
				_, err := raw.Exec(
					`UPDATE committee_member SET cold_cred_hash = ?`,
					overlong(cold),
				)
				require.NoError(t, err)
			},
			lookup: func(p *DingoStateProvider) error {
				_, err := p.CommitteeCredentialIsElected(keyCredential(cold))
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := NewDingoStateManager()
			require.NoError(t, err)
			defer func() { require.NoError(t, m.Close()) }()
			require.NoError(t, m.LoadInitialState(
				&conformance.ParsedInitialState{
					CurrentEpoch:     5,
					CommitteeMembers: map[common.Blake2b224]uint64{cold: 999},
					HotKeyAuthorizations: map[common.Blake2b224]common.Blake2b224{
						cold: hot,
					},
				},
				&conway.ConwayProtocolParameters{},
			))
			raw, err := dbtest.RawSQLiteMetadata(t, m.db)
			require.NoError(t, err)
			tc.mutate(t, raw)

			require.ErrorContains(
				t,
				tc.lookup(NewDingoStateProvider(m)),
				"invalid blake2b-224 hash",
			)
		})
	}
}

var (
	testdataOnce sync.Once
	testdataDir  string
	testdataRoot string
	testdataErr  error
)

// corpusTestdataRoot extracts the embedded vector corpus once per process.
// It deliberately does not use t.TempDir(): the extraction is shared across
// tests, so tying it to the lifetime of whichever test triggered it first
// would delete it while later tests still name its paths in failure output.
// TestMain removes it (see cleanupCorpusTestdata).
func corpusTestdataRoot() (string, error) {
	testdataOnce.Do(func() {
		dir, err := os.MkdirTemp("", "dingo-conformance-vectors-")
		if err != nil {
			testdataErr = fmt.Errorf("create testdata dir: %w", err)
			return
		}
		// Record the directory before extraction, not after: a failure
		// below still leaves a real directory on disk, and
		// cleanupCorpusTestdata's empty-string early return would skip it.
		testdataDir = dir
		// ExtractEmbeddedTestdata returns the extracted root, which is a
		// "testdata" subdirectory of dir, not dir itself. Use the returned
		// path; dir is only what gets removed on cleanup.
		root, err := conformance.ExtractEmbeddedTestdata(dir)
		if err != nil {
			testdataErr = fmt.Errorf("extract embedded testdata: %w", err)
			return
		}
		testdataRoot = root
	})
	return testdataRoot, testdataErr
}

// validateTxFunc is eras.EraDesc.ValidateTxFunc's signature.
type validateTxFunc = func(
	common.Transaction,
	uint64,
	common.LedgerState,
	common.ProtocolParameters,
) error

// eraEntryPoint is one era's production transaction-validation entry point,
// read from Dingo's era registry rather than restated here. Restating the
// list would defeat the purpose: an era whose ValidateTxFunc is dropped from
// the registry has to show up as a missing entry point, not as a local copy
// that keeps working.
type eraEntryPoint struct {
	Validate        validateTxFunc
	Name            string
	Id              uint
	MinMajorVersion uint
	MaxMajorVersion uint
}

// dingoEraEntryPoints reads the production validation entry point out of each
// era descriptor. An era with no ValidateTxFunc is reported rather than
// skipped: a nil entry point is exactly the "validation path bypassed" state
// these tests exist to catch.
func dingoEraEntryPoints(eraList []eras.EraDesc) ([]eraEntryPoint, error) {
	if len(eraList) == 0 {
		return nil, errors.New("era registry is empty")
	}
	entries := make([]eraEntryPoint, 0, len(eraList))
	var missing []string
	for _, era := range eraList {
		if era.ValidateTxFunc == nil {
			missing = append(missing, era.Name)
			continue
		}
		entries = append(entries, eraEntryPoint{
			Validate:        era.ValidateTxFunc,
			Name:            era.Name,
			Id:              era.Id,
			MinMajorVersion: era.MinMajorVersion,
			MaxMajorVersion: era.MaxMajorVersion,
		})
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf(
			"eras with no production validation entry point: %v",
			missing,
		)
	}
	return entries, nil
}

// entryPointForProtocolVersion resolves the era entry point covering a
// protocol major version, mirroring eras.EraForVersionIn.
func entryPointForProtocolVersion(
	entries []eraEntryPoint,
	majorVersion uint,
) (eraEntryPoint, bool) {
	for _, entry := range entries {
		if majorVersion >= entry.MinMajorVersion &&
			majorVersion <= entry.MaxMajorVersion {
			return entry, true
		}
	}
	return eraEntryPoint{}, false
}

// protocolMajorVersion reports the protocol major version carried by pp. It
// is what selects the era, and therefore which production entry point a
// vector's transactions belong to.
//
// Every parameter type from Shelley onward implements
// common.PoolRuleProtocolParameters; the Utxorpc projection is the fallback
// for anything that does not.
func protocolMajorVersion(pp common.ProtocolParameters) (uint, error) {
	if pp == nil {
		return 0, errors.New("nil protocol parameters")
	}
	if versioned, ok := pp.(common.PoolRuleProtocolParameters); ok {
		return versioned.ProtocolMajorVersion(), nil
	}
	upp, err := pp.Utxorpc()
	if err != nil {
		return 0, fmt.Errorf("project protocol parameters: %w", err)
	}
	version := upp.GetProtocolVersion()
	if version == nil {
		return 0, errors.New("protocol parameters carry no protocol version")
	}
	return uint(version.GetMajor()), nil
}

// observedLedgerState wraps the real DingoStateProvider and records the reads
// a validation entry point performs through it.
//
// It embeds the concrete provider rather than the common.LedgerState
// interface so that the optional capabilities Dingo's era validation asserts
// for -- eras.CommitteeCredentialState among them -- keep resolving. Wrapping
// the interface would silently strip them and change what the entry point
// validates.
type observedLedgerState struct {
	*DingoStateProvider
	utxoLookups map[string]struct{}
	reads       int
}

func newObservedLedgerState(
	provider *DingoStateProvider,
) *observedLedgerState {
	return &observedLedgerState{
		DingoStateProvider: provider,
		utxoLookups:        make(map[string]struct{}),
	}
}

// reset clears the recorded reads so one observer can serve consecutive
// routings without attributing an earlier transaction's lookups to a later
// one.
func (o *observedLedgerState) reset() {
	clear(o.utxoLookups)
	o.reads = 0
}

// utxoLookupKey is the canonical identity of a transaction input, used to
// match what an entry point looked up against what the transaction declared.
// It deliberately does not use TransactionInput.String(), whose format is not
// part of any contract.
func utxoLookupKey(id common.TransactionInput) string {
	txId := id.Id()
	return fmt.Sprintf("%x#%d", txId[:], id.Index())
}

// entryPointRouting is the evidence produced by routing one vector
// transaction through a production era validation entry point.
type entryPointRouting struct {
	// Err is what the entry point returned. A non-nil Err is not a test
	// failure: Dingo's rule set is a strict superset of the corpus rule set
	// (it keeps the fee and max-size rules the corpus excludes because the
	// vectors carry Haskell-computed values), so a vector the corpus accepts
	// may still be rejected here. What is asserted is that the entry point
	// ran and read transaction-derived state, not what it decided.
	Err error

	// EraName is the era whose entry point was used.
	EraName string

	// EntryPoint identifies the production function that ran.
	EntryPoint string

	// EventIndex is the transaction event's index within the vector.
	EventIndex int

	// DeclaredInputs is the number of inputs the transaction declares.
	DeclaredInputs int

	// LookedUpInputs is how many of those declared inputs the entry point
	// resolved through the ledger state. This is the transaction-derived
	// signal: a no-op, bypassed, or fixture-only validator resolves none.
	LookedUpInputs int

	// StateReads is the total number of observed ledger-state reads.
	StateReads int
}

// routeTransaction runs one transaction through a production era validation
// entry point against the observed ledger state and returns the evidence.
func routeTransaction(
	entry eraEntryPoint,
	entryPointName string,
	tx common.Transaction,
	slot uint64,
	ls *observedLedgerState,
	pp common.ProtocolParameters,
	eventIndex int,
) entryPointRouting {
	inputs := tx.Inputs()
	declared := make(map[string]struct{}, len(inputs))
	for _, input := range inputs {
		if input == nil {
			continue
		}
		declared[utxoLookupKey(input)] = struct{}{}
	}

	ls.reset()
	err := entry.Validate(tx, slot, ls, pp)

	lookedUp := 0
	for key := range declared {
		if _, ok := ls.utxoLookups[key]; ok {
			lookedUp++
		}
	}

	return entryPointRouting{
		Err:            err,
		EraName:        entry.Name,
		EntryPoint:     entryPointName,
		EventIndex:     eventIndex,
		DeclaredInputs: len(declared),
		LookedUpInputs: lookedUp,
		StateReads:     ls.reads,
	}
}

// entryPointExecutionFault reports why a routing fails to prove that the
// production validation path executed, or nil when it does prove it.
//
// The predicate is deliberately independent of the validation verdict. It is
// satisfied only by evidence the entry point could not have produced without
// looking at this transaction: the ledger-state lookups of the inputs the
// transaction itself declares. A validator that returns a canned verdict --
// nil, an error, or a value copied from the vector fixture -- performs none
// of those lookups and is reported here.
func entryPointExecutionFault(routing entryPointRouting) error {
	if routing.EraName == "" || routing.EntryPoint == "" {
		return errors.New(
			"no production era validation entry point was resolved for this transaction",
		)
	}
	if routing.DeclaredInputs == 0 {
		// A transaction with no inputs is invalid in every era (Byron's
		// InputSetEmpty rule and the UtxoValidateInputSetEmptyUtxo rule from
		// Shelley onward), so there is nothing to look up and acceptance is
		// itself proof the path did not run.
		if routing.Err == nil {
			return fmt.Errorf(
				"%s accepted a transaction with no inputs; a validating entry point must reject one",
				routing.EntryPoint,
			)
		}
		return nil
	}
	if routing.LookedUpInputs == 0 {
		return fmt.Errorf(
			"%s resolved none of the transaction's %d declared inputs through the ledger state (%d total state reads); the production validation path did not run",
			routing.EntryPoint,
			routing.DeclaredInputs,
			routing.StateReads,
		)
	}
	return nil
}

// entryPointFuncName is the reporting name of an era's production entry
// point, e.g. "eras.ValidateTxConway".
func entryPointFuncName(entry eraEntryPoint) string {
	return "eras.ValidateTx" + entry.Name
}

// collectEntryPointVectors walks the same corpus roots the shared harness
// walks, so this pass and the harness pass see the same vector set.
func collectEntryPointVectors(testdataRoot string) ([]string, error) {
	var all []string
	for _, sub := range []string{"eras", "synthetic"} {
		root := filepath.Join(testdataRoot, sub)
		paths, err := conformance.CollectVectorFiles(root)
		if err != nil {
			// A corpus that ships no synthetic/ directory is legitimate, so
			// that one case is skipped. Every other failure is reported: an
			// unreadable vector or an IO error would otherwise shrink the
			// vector set silently, and a partial corpus that still routes
			// some transactions reports as full entry-point coverage --
			// the exact failure mode these tests exist to catch.
			if sub == "synthetic" && errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("collect %s vectors: %w", sub, err)
		}
		all = append(all, paths...)
	}
	if len(all) == 0 {
		return nil, fmt.Errorf("no vectors found under %s", testdataRoot)
	}
	sort.Strings(all)
	return all, nil
}

// decodeVectorTransaction decodes a vector transaction. The corpus is Conway,
// and the shared harness decodes it the same way; routeVectorTransaction
// cross-checks the decoded era against the era the vector's protocol
// parameters select, so a future corpus in another era cannot pass unnoticed.
func decodeVectorTransaction(txBytes []byte) (common.Transaction, error) {
	tx := &conway.ConwayTransaction{}
	if _, err := cbor.Decode(txBytes, tx); err != nil {
		return nil, err
	}
	return tx, nil
}

// entryPointEraList is the era table these tests cover. Dijkstra is included
// deliberately: it is off by default at runtime but its ValidateTxFunc is a
// production entry point, and per-era rule duplication means an entry point
// that is only covered for Conway proves nothing about the others.
func entryPointEraList() []eras.EraDesc {
	return eras.ActiveEras(true)
}

// TestEntryPointExecutionFaultDetectsBypassedValidator proves the detector
// used by TestConformanceVectorsExerciseDingoEraEntryPoints actually
// discriminates: it accepts the production entry point and rejects both a
// no-op validator and one that returns the vector fixture's own verdict
// without consulting ledger state.
//
// Without this, the coverage assertion above would be unfalsifiable, which is
// the same failure mode as the aggregate pass rate it exists to backstop.
func TestEntryPointExecutionFaultDetectsBypassedValidator(t *testing.T) {
	root, err := corpusTestdataRoot()
	require.NoError(t, err)
	entries, err := dingoEraEntryPoints(entryPointEraList())
	require.NoError(t, err)

	sm, err := NewDingoStateManager()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })

	probe := loadEntryPointProbeTransaction(t, root, sm)
	observer := newObservedLedgerState(NewDingoStateProvider(sm))
	major, err := protocolMajorVersion(probe.pp)
	require.NoError(t, err)
	production, ok := entryPointForProtocolVersion(entries, major)
	require.True(t, ok, "no era covers protocol major version %d", major)

	route := func(validate validateTxFunc) entryPointRouting {
		entry := production
		entry.Validate = validate
		return routeTransaction(
			entry,
			entryPointFuncName(production),
			probe.tx,
			probe.slot,
			observer,
			probe.pp,
			0,
		)
	}

	t.Run("production entry point is accepted", func(t *testing.T) {
		routing := route(production.Validate)
		require.NoError(t, entryPointExecutionFault(routing))
		require.Positive(
			t,
			routing.LookedUpInputs,
			"%s must resolve the transaction's declared inputs",
			entryPointFuncName(production),
		)
	})

	t.Run("no-op validator is detected", func(t *testing.T) {
		routing := route(func(
			common.Transaction,
			uint64,
			common.LedgerState,
			common.ProtocolParameters,
		) error {
			return nil
		})
		require.Error(
			t,
			entryPointExecutionFault(routing),
			"a validator that accepts everything without reading state must "+
				"be reported as a bypassed validation path",
		)
	})

	t.Run("fixture-only verdict is detected", func(t *testing.T) {
		// The worst case for an outcome-based check: a validator that returns
		// exactly the verdict the vector fixture declares. Every accept/reject
		// comparison against the corpus would agree with it.
		routing := route(func(
			common.Transaction,
			uint64,
			common.LedgerState,
			common.ProtocolParameters,
		) error {
			if probe.expectSuccess {
				return nil
			}
			return errors.New("vector fixture says this transaction fails")
		})
		require.Error(
			t,
			entryPointExecutionFault(routing),
			"a verdict copied from the vector fixture must be reported as a "+
				"bypassed validation path",
		)
	})

	t.Run(
		"rejecting validator that reads no state is detected",
		func(t *testing.T) {
			routing := route(func(
				common.Transaction,
				uint64,
				common.LedgerState,
				common.ProtocolParameters,
			) error {
				return errors.New("rejected without looking")
			})
			require.Error(
				t,
				entryPointExecutionFault(routing),
				"returning an error is not evidence the validation path ran",
			)
		},
	)
}

// entryPointProbe is a single real corpus transaction plus the state it was
// loaded against, used to exercise the detector.
type entryPointProbe struct {
	tx            common.Transaction
	pp            common.ProtocolParameters
	path          string
	slot          uint64
	expectSuccess bool
}

// loadEntryPointProbeTransaction loads the first corpus vector carrying a
// transaction with at least one declared input, and leaves sm holding that
// vector's initial state. Using real vector data rather than a constructed
// transaction is deliberate: the detector must be shown to work on the same
// input the coverage assertion runs on.
func loadEntryPointProbeTransaction(
	t *testing.T,
	root string,
	sm *DingoStateManager,
) entryPointProbe {
	t.Helper()
	paths, err := collectEntryPointVectors(root)
	require.NoError(t, err)
	loader := conformance.NewPParamsLoaderFromTestdata(root)

	for _, path := range paths {
		vector, err := conformance.DecodeTestVector(path)
		if err != nil {
			continue
		}
		initialState, err := conformance.ParseInitialState(vector.InitialState)
		if err != nil {
			continue
		}
		pp, err := loader.LoadForVector(vector, initialState)
		if err != nil {
			continue
		}
		for _, event := range vector.Events {
			if event.Type != conformance.EventTypeTransaction {
				continue
			}
			tx, err := decodeVectorTransaction(event.TxBytes)
			if err != nil || len(tx.Inputs()) == 0 {
				continue
			}
			require.NoError(t, sm.Reset())
			require.NoError(t, sm.LoadInitialState(initialState, pp))
			return entryPointProbe{
				tx:            tx,
				pp:            pp,
				path:          filepath.Base(path),
				slot:          event.Slot,
				expectSuccess: event.Success,
			}
		}
	}
	t.Fatal("no corpus vector carries a transaction with declared inputs")
	return entryPointProbe{}
}

// TestDingoEraEntryPointsRejectInputlessTransaction covers every era in the
// registry, not just the era the corpus happens to contain.
//
// The corpus is Conway-only, and validation rules are duplicated per era, so
// Conway coverage says nothing about ValidateTxShelley or ValidateTxDijkstra.
// A transaction with no inputs is invalid in every era (Byron's own
// InputSetEmpty rule, and UtxoValidateInputSetEmptyUtxo from Shelley onward),
// which makes it a rule the whole table can be held to. The paired no-op
// assertion is what makes this a detector rather than a restatement: the same
// input is accepted by a validator that does nothing.
func TestDingoEraEntryPointsRejectInputlessTransaction(t *testing.T) {
	entries, err := dingoEraEntryPoints(entryPointEraList())
	require.NoError(t, err)

	sm, err := NewDingoStateManager()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })
	observer := newObservedLedgerState(NewDingoStateProvider(sm))

	probes := inputlessEraProbes()
	for _, entry := range entries {
		t.Run(entry.Name, func(t *testing.T) {
			probe, ok := probes[entry.Name]
			require.Truef(
				t,
				ok,
				"era %s has no input-less transaction probe; add one so its "+
					"production entry point is covered",
				entry.Name,
			)
			require.Empty(t, probe.tx.Inputs())

			routing := routeTransaction(
				entry,
				entryPointFuncName(entry),
				probe.tx,
				0,
				observer,
				probe.pp,
				0,
			)
			require.NoErrorf(
				t,
				entryPointExecutionFault(routing),
				"%s accepted a transaction with no inputs",
				entryPointFuncName(entry),
			)

			bypassed := entry
			bypassed.Validate = func(
				common.Transaction,
				uint64,
				common.LedgerState,
				common.ProtocolParameters,
			) error {
				return nil
			}
			bypassedRouting := routeTransaction(
				bypassed,
				entryPointFuncName(entry),
				probe.tx,
				0,
				observer,
				probe.pp,
				0,
			)
			require.Errorf(
				t,
				entryPointExecutionFault(bypassedRouting),
				"a no-op replacement for %s must be detected",
				entryPointFuncName(entry),
			)
		})
	}
}

// eraProbe is an era-appropriate transaction and the protocol parameters its
// entry point requires.
type eraProbe struct {
	tx common.Transaction
	pp common.ProtocolParameters
}

// inputlessEraProbes returns one input-less transaction per era, keyed by era
// name.
//
// From Shelley onward each entry point type-asserts its own parameter type,
// so the parameters have to match or the entry point returns
// eras.ErrIncompatibleProtocolParams before reaching any rule -- which would
// satisfy the input-less assertion for the wrong reason.
//
// Byron is the exception and carries nil: ValidateTxByron never asserts on
// pp, and gouroboros has no Byron protocol-parameters type to supply. It runs
// its structural rules unconditionally (byronValidateInputsNotEmpty is what
// rejects the probe) and its UTxO-aware rules whenever a ledger state is
// given, passing pp through to rules that ignore it.
func inputlessEraProbes() map[string]eraProbe {
	return map[string]eraProbe{
		byron.EraNameByron: {
			tx: &byron.ByronTransaction{},
			pp: nil,
		},
		shelley.EraNameShelley: {
			tx: &shelley.ShelleyTransaction{},
			pp: &shelley.ShelleyProtocolParameters{},
		},
		allegra.EraNameAllegra: {
			tx: &allegra.AllegraTransaction{},
			pp: &allegra.AllegraProtocolParameters{},
		},
		mary.EraNameMary: {
			tx: &mary.MaryTransaction{},
			pp: &mary.MaryProtocolParameters{},
		},
		alonzo.EraNameAlonzo: {
			tx: &alonzo.AlonzoTransaction{},
			pp: &alonzo.AlonzoProtocolParameters{},
		},
		babbage.EraNameBabbage: {
			tx: &babbage.BabbageTransaction{},
			pp: &babbage.BabbageProtocolParameters{},
		},
		conway.EraNameConway: {
			tx: &conway.ConwayTransaction{},
			pp: &conway.ConwayProtocolParameters{},
		},
		dijkstra.EraNameDijkstra: {
			tx: &dijkstra.DijkstraTransaction{},
			pp: &dijkstra.DijkstraProtocolParameters{},
		},
	}
}
