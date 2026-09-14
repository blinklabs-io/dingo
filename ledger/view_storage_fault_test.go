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
	"errors"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	omockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// errInjectingMetadataStore wraps a real metadata.MetadataStore and forces
// specific lookups to fail with a caller-supplied error instead of
// delegating to the wrapped store, reproducing a genuine non-not-found
// storage fault (a timeout, a lost connection) without corrupting on-disk
// rows. See blinklabs-io/dingo#1649.
type errInjectingMetadataStore struct {
	metadata.MetadataStore
	getPoolErr                error
	getAccountByCredentialErr error
	getGovernanceProposalErr  error
}

func (s *errInjectingMetadataStore) GetPool(
	pkh lcommon.PoolKeyHash,
	includeInactive bool,
	txn types.Txn,
) (*models.Pool, error) {
	if s.getPoolErr != nil {
		return nil, s.getPoolErr
	}
	return s.MetadataStore.GetPool(pkh, includeInactive, txn)
}

func (s *errInjectingMetadataStore) GetAccountByCredential(
	credentialTag uint8,
	stakeKey []byte,
	includeInactive bool,
	txn types.Txn,
) (*models.Account, error) {
	if s.getAccountByCredentialErr != nil {
		return nil, s.getAccountByCredentialErr
	}
	return s.MetadataStore.GetAccountByCredential(
		credentialTag,
		stakeKey,
		includeInactive,
		txn,
	)
}

func (s *errInjectingMetadataStore) GetGovernanceProposal(
	txHash []byte,
	actionIndex uint32,
	txn types.Txn,
) (*models.GovernanceProposal, error) {
	if s.getGovernanceProposalErr != nil {
		return nil, s.getGovernanceProposalErr
	}
	return s.MetadataStore.GetGovernanceProposal(txHash, actionIndex, txn)
}

// newDiscardLoggerLedgerState builds a bare *LedgerState wired only to db,
// with a non-nil discard logger so the boolean predicates' error-path
// logging (lv.ls.config.Logger.Error) does not panic on a zero-value
// LedgerStateConfig.
func newDiscardLoggerLedgerState(db *database.Database) *LedgerState {
	ls := &LedgerState{
		db:     db,
		config: LedgerStateConfig{Logger: slog.New(slog.DiscardHandler)},
	}
	ls.metrics.init(prometheus.NewRegistry())
	return ls
}

// newStorageFaultTestDB builds a real dingo database (the same badger blob
// and sqlite metadata composition dbtest.NewDatabase uses) with its metadata
// store wrapped by errs, so a specific lookup returns errs's error instead of
// the real store's result.
func newStorageFaultTestDB(
	t *testing.T,
	errs errInjectingMetadataStore,
) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabaseWithMetadataWrapper(
		t,
		dbtest.Options{Config: &database.Config{DataDir: ""}},
		func(store metadata.MetadataStore) metadata.MetadataStore {
			errs.MetadataStore = store
			return &errs
		},
	)
	require.NoError(t, err)
	return db
}

// TestLedgerViewPredicatesRecordStorageFaultNotRuleVerdict is the core
// regression test: each boolean LedgerState predicate must record a genuine
// non-not-found storage error on the view instead of only swallowing it into
// a bare false/does-not-exist return with no trace of the fault.
func TestLedgerViewPredicatesRecordStorageFaultNotRuleVerdict(t *testing.T) {
	t.Parallel()

	t.Run("IsStakeCredentialRegistered", func(t *testing.T) {
		t.Parallel()
		stubErr := errors.New("synthetic account lookup fault")
		db := newStorageFaultTestDB(t, errInjectingMetadataStore{
			getAccountByCredentialErr: stubErr,
		})
		ls := newDiscardLoggerLedgerState(db)
		lv := &LedgerView{ls: ls}
		cred := lcommon.Credential{Credential: lcommon.Blake2b224{0x01}}

		registered := lv.IsStakeCredentialRegistered(cred)

		require.False(t, registered)
		require.ErrorIs(t, lv.StorageErr(), stubErr)
	})

	t.Run(
		"IsStakeCredentialRegistered not-found is not a fault",
		func(t *testing.T) {
			t.Parallel()
			db := newStorageFaultTestDB(t, errInjectingMetadataStore{})
			ls := newDiscardLoggerLedgerState(db)
			lv := &LedgerView{ls: ls}
			cred := lcommon.Credential{Credential: lcommon.Blake2b224{0x01}}

			registered := lv.IsStakeCredentialRegistered(cred)

			require.False(t, registered)
			require.NoError(t, lv.StorageErr())
		},
	)

	t.Run("IsRewardAccountRegistered", func(t *testing.T) {
		t.Parallel()
		stubErr := errors.New("synthetic account lookup fault")
		db := newStorageFaultTestDB(t, errInjectingMetadataStore{
			getAccountByCredentialErr: stubErr,
		})
		ls := newDiscardLoggerLedgerState(db)
		lv := &LedgerView{ls: ls}
		cred := lcommon.Credential{Credential: lcommon.Blake2b224{0x02}}

		registered := lv.IsRewardAccountRegistered(cred)

		require.False(t, registered)
		require.ErrorIs(t, lv.StorageErr(), stubErr)
	})

	t.Run(
		"IsRewardAccountRegistered not-found is not a fault",
		func(t *testing.T) {
			t.Parallel()
			db := newStorageFaultTestDB(t, errInjectingMetadataStore{})
			ls := newDiscardLoggerLedgerState(db)
			lv := &LedgerView{ls: ls}
			cred := lcommon.Credential{Credential: lcommon.Blake2b224{0x02}}

			registered := lv.IsRewardAccountRegistered(cred)

			require.False(t, registered)
			require.NoError(t, lv.StorageErr())
		},
	)

	t.Run("IsPoolRegistered", func(t *testing.T) {
		t.Parallel()
		stubErr := errors.New("synthetic pool lookup fault")
		db := newStorageFaultTestDB(t, errInjectingMetadataStore{
			getPoolErr: stubErr,
		})
		ls := newDiscardLoggerLedgerState(db)
		lv := &LedgerView{ls: ls}

		registered := lv.IsPoolRegistered(lcommon.PoolKeyHash{0x03})

		require.False(t, registered)
		require.ErrorIs(t, lv.StorageErr(), stubErr)
	})

	t.Run("IsPoolRegistered not-found is not a fault", func(t *testing.T) {
		t.Parallel()
		db := newStorageFaultTestDB(t, errInjectingMetadataStore{})
		ls := newDiscardLoggerLedgerState(db)
		lv := &LedgerView{ls: ls}

		registered := lv.IsPoolRegistered(lcommon.PoolKeyHash{0x03})

		require.False(t, registered)
		require.NoError(t, lv.StorageErr())
	})

	t.Run("GovActionExists", func(t *testing.T) {
		t.Parallel()
		stubErr := errors.New("synthetic governance proposal lookup fault")
		db := newStorageFaultTestDB(t, errInjectingMetadataStore{
			getGovernanceProposalErr: stubErr,
		})
		ls := newDiscardLoggerLedgerState(db)
		lv := &LedgerView{ls: ls}

		exists := lv.GovActionExists(lcommon.GovActionId{})

		require.False(t, exists)
		require.ErrorIs(t, lv.StorageErr(), stubErr)
	})

	t.Run("GovActionExists not-found is not a fault", func(t *testing.T) {
		t.Parallel()
		db := newStorageFaultTestDB(t, errInjectingMetadataStore{})
		ls := newDiscardLoggerLedgerState(db)
		lv := &LedgerView{ls: ls}

		exists := lv.GovActionExists(lcommon.GovActionId{})

		require.False(t, exists)
		require.NoError(t, lv.StorageErr())
	})
}

// TestStorageFaultOrErrPrefersRecordedFault pins storageFaultOrErr, the
// helper every ValidateTxFunc/EvaluateTxFunc call site uses after invoking
// the era's rule: a recorded fault always wins, including over a nil rule
// verdict (the case where the false negative caused the rule to wrongly
// accept, e.g. "a stake re-registration today yields nil").
func TestStorageFaultOrErrPrefersRecordedFault(t *testing.T) {
	t.Parallel()

	t.Run("no fault recorded, rule error passes through", func(t *testing.T) {
		t.Parallel()
		lv := &LedgerView{}
		ruleErr := errors.New("rule failure")
		require.Same(t, ruleErr, storageFaultOrErr(lv, ruleErr))
	})

	t.Run(
		"no fault recorded, nil rule verdict passes through",
		func(t *testing.T) {
			t.Parallel()
			lv := &LedgerView{}
			require.NoError(t, storageFaultOrErr(lv, nil))
		},
	)

	t.Run("fault recorded overrides nil rule verdict", func(t *testing.T) {
		t.Parallel()
		lv := &LedgerView{}
		faultErr := errors.New("storage fault")
		lv.recordStorageErr(faultErr)
		err := storageFaultOrErr(lv, nil)
		require.ErrorIs(t, err, ErrLedgerViewStorageFault)
		require.ErrorIs(t, err, faultErr)
	})

	t.Run("fault recorded overrides rule error", func(t *testing.T) {
		t.Parallel()
		lv := &LedgerView{}
		faultErr := errors.New("storage fault")
		lv.recordStorageErr(faultErr)
		ruleErr := errors.New("rule failure")
		err := storageFaultOrErr(lv, ruleErr)
		require.ErrorIs(t, err, ErrLedgerViewStorageFault)
		require.ErrorIs(t, err, faultErr)
		require.NotErrorIs(t, err, ruleErr)
	})

	t.Run("only the first fault is sticky", func(t *testing.T) {
		t.Parallel()
		lv := &LedgerView{}
		firstErr := errors.New("first fault")
		secondErr := errors.New("second fault")
		lv.recordStorageErr(firstErr)
		lv.recordStorageErr(secondErr)
		err := storageFaultOrErr(lv, nil)
		require.ErrorIs(t, err, firstErr)
		require.NotErrorIs(t, err, secondErr)
	})

	t.Run("nil view is a no-op", func(t *testing.T) {
		t.Parallel()
		ruleErr := errors.New("rule failure")
		require.Same(t, ruleErr, storageFaultOrErr(nil, ruleErr))
		require.NoError(t, storageFaultOrErr(nil, nil))
	})
}

// newFakeEraLedgerState builds a minimal LedgerState wired to a single,
// caller-supplied era descriptor so a test can drive the real ValidateTx/
// EvaluateTx call sites without needing a transaction that satisfies every
// gouroboros ledger rule. It mirrors view_governance_test.go's
// governanceTestView, adding the era wiring ValidateTx/EvaluateTx need.
func newFakeEraLedgerState(
	db *database.Database,
	validateTxFunc func(
		lcommon.Transaction,
		uint64,
		lcommon.LedgerState,
		lcommon.ProtocolParameters,
	) error,
	evaluateTxFunc func(
		lcommon.Transaction,
		lcommon.LedgerState,
		lcommon.ProtocolParameters,
	) (uint64, lcommon.ExUnits, map[lcommon.RedeemerKey]lcommon.ExUnits, error),
) *LedgerState {
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger:            slog.New(slog.DiscardHandler),
			CardanoNodeConfig: &cardano.CardanoNodeConfig{},
		},
		currentEra: eras.EraDesc{
			Id:             conway.EraIdConway,
			ValidateTxFunc: validateTxFunc,
			EvaluateTxFunc: evaluateTxFunc,
		},
		currentPParams: &conway.ConwayProtocolParameters{},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.publishSnapshotsLocked()
	return ls
}

// TestValidateTxSurfacesStorageFaultOverNilVerdict drives the real
// LedgerState.ValidateTx call site (validateTxCore) with a rule that mimics
// Conway's certificate-deposit rule: IsStakeCredentialRegistered only
// changes whether a deposit is charged, so the rule accepts (returns nil)
// whether or not the credential looks registered. Without the fault check, a
// swallowed storage error is therefore invisible: ValidateTx returns nil.
// After the fix, it returns the storage fault.
func TestValidateTxSurfacesStorageFaultOverNilVerdict(t *testing.T) {
	t.Parallel()
	stubErr := errors.New("synthetic account lookup fault")
	db := newStorageFaultTestDB(t, errInjectingMetadataStore{
		getAccountByCredentialErr: stubErr,
	})
	cred := lcommon.Credential{Credential: lcommon.Blake2b224{0x04}}
	ls := newFakeEraLedgerState(db, func(
		tx lcommon.Transaction,
		slot uint64,
		view lcommon.LedgerState,
		pp lcommon.ProtocolParameters,
	) error {
		_ = view.IsStakeCredentialRegistered(cred)
		return nil
	}, nil)

	tx := &conway.ConwayTransaction{TxIsValid: true}
	err := ls.ValidateTx(tx)

	require.Error(t, err)
	require.ErrorIs(t, err, stubErr)
	require.ErrorIs(t, err, ErrLedgerViewStorageFault)
}

// TestValidateTxSurfacesStorageFaultOverPoolRetirementVerdict drives
// LedgerState.ValidateTx with gouroboros's real
// shelley.UtxoValidatePoolCertificates rule validating a pool-retirement
// certificate for a pool the metadata store cannot resolve. Before the fix
// this returns shelley.StakePoolNotRegisteredOnKeyError, indistinguishable
// from a genuinely unregistered pool. After the fix it returns the storage
// fault, and the rule's own verdict is not observable.
func TestValidateTxSurfacesStorageFaultOverPoolRetirementVerdict(t *testing.T) {
	t.Parallel()
	stubErr := errors.New("synthetic pool lookup fault")
	db := newStorageFaultTestDB(t, errInjectingMetadataStore{
		getPoolErr: stubErr,
	})
	poolKeyHash := lcommon.PoolKeyHash{0x05}
	ls := newFakeEraLedgerState(
		db,
		shelley.UtxoValidatePoolCertificates,
		nil,
	)

	tx := &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxCertificates: []lcommon.CertificateWrapper{
				{
					Type: uint(lcommon.CertificateTypePoolRetirement),
					Certificate: &lcommon.PoolRetirementCertificate{
						CertType: uint(
							lcommon.CertificateTypePoolRetirement,
						),
						PoolKeyHash: poolKeyHash,
						Epoch:       500,
					},
				},
			},
		},
	}

	err := ls.ValidateTx(tx)

	require.Error(t, err)
	require.ErrorIs(t, err, stubErr)
	require.ErrorIs(t, err, ErrLedgerViewStorageFault)
	var ruleErr shelley.StakePoolNotRegisteredOnKeyError
	require.False(
		t,
		errors.As(err, &ruleErr),
		"the rule's own not-registered verdict must not surface once a storage fault is recorded, got %v",
		err,
	)
}

// TestEvaluateTxSurfacesStorageFaultOverNilVerdict drives the
// LedgerState.EvaluateTx call site (EvaluateTxFunc) the same way
// TestValidateTxSurfacesStorageFaultOverNilVerdict drives ValidateTx.
func TestEvaluateTxSurfacesStorageFaultOverNilVerdict(t *testing.T) {
	t.Parallel()
	stubErr := errors.New("synthetic pool lookup fault")
	db := newStorageFaultTestDB(t, errInjectingMetadataStore{
		getPoolErr: stubErr,
	})
	poolKeyHash := lcommon.PoolKeyHash{0x06}
	ls := newFakeEraLedgerState(db, nil, func(
		tx lcommon.Transaction,
		view lcommon.LedgerState,
		pp lcommon.ProtocolParameters,
	) (uint64, lcommon.ExUnits, map[lcommon.RedeemerKey]lcommon.ExUnits, error) {
		_ = view.IsPoolRegistered(poolKeyHash)
		return 0, lcommon.ExUnits{}, nil, nil
	})

	tx := &conway.ConwayTransaction{TxIsValid: true}
	_, _, _, err := ls.EvaluateTx(tx)

	require.Error(t, err)
	require.ErrorIs(t, err, stubErr)
	require.ErrorIs(t, err, ErrLedgerViewStorageFault)
}

// TestWithTxValidationSessionSurfacesStorageFaultOverNilVerdict drives the
// WithTxValidationSession call site the same way
// TestValidateTxSurfacesStorageFaultOverNilVerdict drives ValidateTx.
func TestWithTxValidationSessionSurfacesStorageFaultOverNilVerdict(
	t *testing.T,
) {
	t.Parallel()
	stubErr := errors.New("synthetic account lookup fault")
	db := newStorageFaultTestDB(t, errInjectingMetadataStore{
		getAccountByCredentialErr: stubErr,
	})
	cred := lcommon.Credential{Credential: lcommon.Blake2b224{0x07}}
	ls := newFakeEraLedgerState(db, func(
		tx lcommon.Transaction,
		slot uint64,
		view lcommon.LedgerState,
		pp lcommon.ProtocolParameters,
	) error {
		_ = view.IsStakeCredentialRegistered(cred)
		return nil
	}, nil)

	tx := &conway.ConwayTransaction{TxIsValid: true}
	err := ls.WithTxValidationSession(func(
		validate func(
			tx lcommon.Transaction,
			consumedUtxos map[string]struct{},
			createdUtxos map[string]lcommon.Utxo,
		) error,
		stillCurrent func() bool,
	) error {
		return validate(tx, nil, nil)
	})

	require.Error(t, err)
	require.ErrorIs(t, err, stubErr)
	require.ErrorIs(t, err, ErrLedgerViewStorageFault)
}

// TestLedgerProcessBlockSurfacesStorageFaultOverRuleVerdict drives the
// chain-sync block-application call site in ledgerProcessBlock. A storage
// fault recorded by a LedgerView predicate must surface as
// ErrLedgerViewStorageFault, not as the rule's verdict: a rejecting rule
// would otherwise classify a canonical block as invalid (txValidationError),
// and an accepting rule would otherwise apply the block on a false negative.
func TestLedgerProcessBlockSurfacesStorageFaultOverRuleVerdict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		rejectOnMiss bool
	}{
		{name: "rule rejects on the false negative", rejectOnMiss: true},
		{name: "rule accepts despite the false negative", rejectOnMiss: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stubErr := errors.New("synthetic pool lookup fault")
			db := newStorageFaultTestDB(t, errInjectingMetadataStore{
				getPoolErr: stubErr,
			})
			verdictErr := errors.New("pool not registered verdict")
			called := false
			testEra := eras.ByronEraDesc
			testEra.ValidateTxFunc = func(
				_ lcommon.Transaction,
				_ uint64,
				view lcommon.LedgerState,
				_ lcommon.ProtocolParameters,
			) error {
				called = true
				if !view.IsPoolRegistered(lcommon.PoolKeyHash{0x08}) &&
					tt.rejectOnMiss {
					return verdictErr
				}
				return nil
			}
			ls := &LedgerState{
				db:         db,
				activeEras: []eras.EraDesc{testEra},
				config: LedgerStateConfig{
					Logger: slog.New(slog.DiscardHandler),
				},
				currentEra: testEra,
			}
			tx := omockledger.NewTransactionBuilder()
			tx.WithId(bytes.Repeat([]byte{0x36}, 32))
			tx.WithType(byron.TxTypeByron)
			tx.WithValid(true)
			block := &validityOutcomeTestBlock{
				header: &byron.ByronMainBlockHeader{},
				txs:    []lcommon.Transaction{tx},
			}

			err := db.Transaction(true).Do(func(txn *database.Txn) error {
				_, err := ls.ledgerProcessBlock(
					txn,
					ocommon.NewPoint(1, block.Hash().Bytes()),
					block,
					true,
					false,
					false,
					nil,
					envelopeParent{origin: true},
					nil,
					testEra,
					nil,
					nil,
					0,
					false,
				)
				return err
			})

			require.True(t, called, "block validation must run the rule")
			require.ErrorIs(t, err, stubErr)
			require.ErrorIs(t, err, ErrLedgerViewStorageFault)
			require.NotErrorIs(t, err, verdictErr)
			var validationErr *txValidationError
			require.False(
				t,
				errors.As(err, &validationErr),
				"a storage fault must not classify the block as invalid, got %v",
				err,
			)
		})
	}
}
