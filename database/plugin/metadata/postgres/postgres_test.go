// Copyright 2025 Blink Labs Software
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

package postgres

import (
	"math/big"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	"gorm.io/gorm"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin"
)

// TestTable is a simple table for testing concurrent transactions
type TestTable struct {
	gorm.Model
}

type mockTransaction struct {
	hash         lcommon.Blake2b256
	certificates []lcommon.Certificate
	isValid      bool
}

func (m *mockTransaction) Hash() lcommon.Blake2b256 {
	return m.hash
}

func (m *mockTransaction) Id() lcommon.Blake2b256 {
	return m.hash
}

func (m *mockTransaction) Type() int {
	return 0 // Shelley transaction
}

func (m *mockTransaction) Fee() *big.Int {
	return big.NewInt(1000)
}

func (m *mockTransaction) TTL() uint64 {
	return 1000000
}

func (m *mockTransaction) IsValid() bool {
	return m.isValid
}

func (m *mockTransaction) Metadata() lcommon.TransactionMetadatum {
	return nil
}

func (m *mockTransaction) AuxiliaryData() lcommon.AuxiliaryData {
	return nil
}

func (m *mockTransaction) RawAuxiliaryData() []byte {
	return nil
}

func (m *mockTransaction) CollateralReturn() lcommon.TransactionOutput {
	return nil
}

func (m *mockTransaction) Produced() []lcommon.Utxo {
	return nil
}

func (m *mockTransaction) Outputs() []lcommon.TransactionOutput {
	return nil
}

func (m *mockTransaction) Inputs() []lcommon.TransactionInput {
	return nil
}

func (m *mockTransaction) Collateral() []lcommon.TransactionInput {
	return nil
}

func (m *mockTransaction) Certificates() []lcommon.Certificate {
	return m.certificates
}

func (m *mockTransaction) ProtocolParameterUpdates() (uint64, map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate) {
	return 0, nil
}

func (m *mockTransaction) AssetMint() *lcommon.MultiAsset[lcommon.MultiAssetTypeMint] {
	return nil
}

func (m *mockTransaction) AuxDataHash() *lcommon.Blake2b256 {
	return nil
}

func (m *mockTransaction) Cbor() []byte {
	return []byte("mock_cbor")
}

func (m *mockTransaction) Consumed() []lcommon.TransactionInput {
	return nil
}

func (m *mockTransaction) Witnesses() lcommon.TransactionWitnessSet {
	return nil
}

func (m *mockTransaction) ValidityIntervalStart() uint64 {
	return 0
}

func (m *mockTransaction) ReferenceInputs() []lcommon.TransactionInput {
	return nil
}

func (m *mockTransaction) TotalCollateral() *big.Int {
	return big.NewInt(0)
}

func (m *mockTransaction) Withdrawals() map[*lcommon.Address]*big.Int {
	return nil
}

func (m *mockTransaction) RequiredSigners() []lcommon.Blake2b224 {
	return nil
}

func (m *mockTransaction) ScriptDataHash() *lcommon.Blake2b256 {
	return nil
}

func (m *mockTransaction) VotingProcedures() lcommon.VotingProcedures {
	return lcommon.VotingProcedures{}
}

func (m *mockTransaction) ProposalProcedures() []lcommon.ProposalProcedure {
	return nil
}

func (m *mockTransaction) CurrentTreasuryValue() *big.Int {
	return big.NewInt(0)
}

func (m *mockTransaction) Donation() *big.Int {
	return big.NewInt(0)
}

func (m *mockTransaction) Utxorpc() (*cardano.Tx, error) {
	return nil, nil
}

func (m *mockTransaction) LeiosHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

// isPostgresConfigured checks if postgres is configured via cmdlineOptions or environment variables.
// It first checks cmdlineOptions (the plugin's configured state), then falls back to environment variables.
// Returns true if a password or DSN is configured, false otherwise.
func isPostgresConfigured() bool {
	// Check if cmdlineOptions has a password or DSN set
	cmdlineOptionsMutex.RLock()
	password := cmdlineOptions.password
	dsn := cmdlineOptions.dsn
	cmdlineOptionsMutex.RUnlock()

	if password != "" || dsn != "" {
		return true
	}

	// Fall back to environment variables
	return os.Getenv("POSTGRES_PASSWORD") != "" ||
		os.Getenv("POSTGRES_DSN") != ""
}

// getTestPostgresOptions returns options for creating a test postgres store.
// It uses cmdlineOptions if configured, otherwise falls back to environment variables.
func getTestPostgresOptions() []PostgresOptionFunc {
	cmdlineOptionsMutex.RLock()
	host := cmdlineOptions.host
	port := uint(cmdlineOptions.port)
	user := cmdlineOptions.user
	password := cmdlineOptions.password
	database := cmdlineOptions.database
	sslMode := cmdlineOptions.sslMode
	timeZone := cmdlineOptions.timeZone
	dsn := cmdlineOptions.dsn
	cmdlineOptionsMutex.RUnlock()

	// Override with environment variables if cmdlineOptions password is not set
	if password == "" {
		password = os.Getenv("POSTGRES_PASSWORD")

		// Also check for other env vars when using env-based config
		if envHost := os.Getenv("POSTGRES_HOST"); envHost != "" {
			host = envHost
		}
		if envPort := os.Getenv("POSTGRES_PORT"); envPort != "" {
			if p, err := strconv.ParseUint(envPort, 10, 32); err == nil {
				port = uint(p)
			}
		}
		if envUser := os.Getenv("POSTGRES_USER"); envUser != "" {
			user = envUser
		}
		if envDB := os.Getenv("POSTGRES_DATABASE"); envDB != "" {
			database = envDB
		} else if database == "postgres" {
			// Use a separate test database by default
			database = "dingo_test"
		}
		if envSSL := os.Getenv("POSTGRES_SSLMODE"); envSSL != "" {
			sslMode = envSSL
		}
		if envDSN := os.Getenv("POSTGRES_DSN"); envDSN != "" {
			dsn = envDSN
		}
	}

	return []PostgresOptionFunc{
		WithHost(host),
		WithPort(port),
		WithUser(user),
		WithPassword(password),
		WithDatabase(database),
		WithSSLMode(sslMode),
		WithTimeZone(timeZone),
		WithDSN(dsn),
	}
}

// newTestPostgresStore creates a new postgres store for testing.
// It skips the test if postgres is not configured (no password in cmdlineOptions or POSTGRES_PASSWORD env var).
func newTestPostgresStore(t *testing.T) *MetadataStorePostgres {
	t.Helper()

	if !isPostgresConfigured() {
		t.Skip(
			"Skipping postgres integration test: postgres not configured (set POSTGRES_PASSWORD or configure via cmdline options)",
		)
	}

	opts := getTestPostgresOptions()
	store, err := NewWithOptions(opts...)
	if err != nil {
		t.Fatalf("failed to create postgres store: %v", err)
	}

	if err := store.Start(); err != nil {
		t.Fatalf("failed to start postgres store: %v", err)
	}

	return store
}

// newTestPostgresStoreFromPlugin creates a postgres store using NewFromCmdlineOptions.
// This tests the plugin registration path. Skips if not configured.
func newTestPostgresStoreFromPlugin(t *testing.T) *MetadataStorePostgres {
	t.Helper()

	if !isPostgresConfigured() {
		t.Skip(
			"Skipping postgres integration test: postgres not configured (set POSTGRES_PASSWORD or configure via cmdline options)",
		)
	}

	// Capture original cmdlineOptions before any modifications
	cmdlineOptionsMutex.RLock()
	originalHost := cmdlineOptions.host
	originalPort := cmdlineOptions.port
	originalUser := cmdlineOptions.user
	originalPassword := cmdlineOptions.password
	originalDatabase := cmdlineOptions.database
	originalSslMode := cmdlineOptions.sslMode
	originalTimeZone := cmdlineOptions.timeZone
	originalDsn := cmdlineOptions.dsn
	cmdlineOptionsMutex.RUnlock()

	// Restore original cmdlineOptions after test setup
	t.Cleanup(func() {
		cmdlineOptionsMutex.Lock()
		cmdlineOptions.host = originalHost
		cmdlineOptions.port = originalPort
		cmdlineOptions.user = originalUser
		cmdlineOptions.password = originalPassword
		cmdlineOptions.database = originalDatabase
		cmdlineOptions.sslMode = originalSslMode
		cmdlineOptions.timeZone = originalTimeZone
		cmdlineOptions.dsn = originalDsn
		cmdlineOptionsMutex.Unlock()
	})

	if originalPassword == "" && originalDsn == "" {
		// Set cmdlineOptions from environment for this test
		cmdlineOptionsMutex.Lock()
		if envHost := os.Getenv("POSTGRES_HOST"); envHost != "" {
			cmdlineOptions.host = envHost
		}
		if envPort := os.Getenv("POSTGRES_PORT"); envPort != "" {
			if p, err := strconv.ParseUint(envPort, 10, 32); err == nil {
				cmdlineOptions.port = p
			}
		}
		if envUser := os.Getenv("POSTGRES_USER"); envUser != "" {
			cmdlineOptions.user = envUser
		}
		cmdlineOptions.password = os.Getenv("POSTGRES_PASSWORD")
		if envDB := os.Getenv("POSTGRES_DATABASE"); envDB != "" {
			cmdlineOptions.database = envDB
		} else {
			cmdlineOptions.database = "dingo_test"
		}
		if envSSL := os.Getenv("POSTGRES_SSLMODE"); envSSL != "" {
			cmdlineOptions.sslMode = envSSL
		}
		if envDSN := os.Getenv("POSTGRES_DSN"); envDSN != "" {
			cmdlineOptions.dsn = envDSN
		}
		cmdlineOptionsMutex.Unlock()
	}

	p := NewFromCmdlineOptions()
	if p == nil {
		t.Fatal("NewFromCmdlineOptions returned nil")
	}

	// Check if it's an error plugin
	if _, ok := p.(*plugin.ErrorPlugin); ok {
		t.Fatal("NewFromCmdlineOptions returned an error plugin")
	}

	store, ok := p.(*MetadataStorePostgres)
	if !ok {
		t.Fatalf("expected *MetadataStorePostgres, got %T", p)
	}

	if err := store.Start(); err != nil {
		t.Fatalf("failed to start postgres store: %v", err)
	}

	return store
}

// TestPostgresMultipleTransaction tests that postgres allows multiple
// concurrent transactions
func TestPostgresMultipleTransaction(t *testing.T) {
	pgStore := newTestPostgresStore(t)
	defer pgStore.Close() //nolint:errcheck

	if err := pgStore.DB().AutoMigrate(&TestTable{}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if result := pgStore.DB().Create(&TestTable{}); result.Error != nil {
		t.Fatalf("unexpected error: %s", result.Error)
	}

	doQuery := func(sleep time.Duration) error {
		txn := pgStore.DB().Begin()
		defer txn.Rollback() //nolint:errcheck
		if result := txn.First(&TestTable{}); result.Error != nil {
			return result.Error
		}
		time.Sleep(sleep)
		if result := txn.Commit(); result.Error != nil {
			return result.Error
		}
		return nil
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- doQuery(5 * time.Second)
	}()
	time.Sleep(1 * time.Second)
	if err := doQuery(0); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("goroutine error: %s", err)
	}
}

// TestPostgresUnifiedCertificateCreation tests that unified certificate records are created
// correctly and linked to specialized certificate records
func TestPostgresUnifiedCertificateCreation(t *testing.T) {
	pgStore := newTestPostgresStore(t)
	defer pgStore.Close() //nolint:errcheck

	// Clean up any existing records from previous test runs to ensure deterministic results
	pgStore.DB().Where("1 = 1").Delete(&models.StakeRegistration{})
	pgStore.DB().Where("1 = 1").Delete(&models.PoolRegistration{})
	pgStore.DB().Where("1 = 1").Delete(&models.AuthCommitteeHot{})
	pgStore.DB().Where("1 = 1").Delete(&models.Certificate{})

	// Create a mock transaction with certificates
	mockTx := &mockTransaction{
		hash: lcommon.NewBlake2b256(
			[]byte("test_hash_1234567890123456789012345678901234567890"),
		),
		isValid: true,
		certificates: []lcommon.Certificate{
			&lcommon.StakeRegistrationCertificate{
				CertType: uint(lcommon.CertificateTypeStakeRegistration),
				StakeCredential: lcommon.Credential{
					CredType: lcommon.CredentialTypeAddrKeyHash,
					Credential: lcommon.CredentialHash(
						[]byte("stake_key_hash_1234567890123456789012345678"),
					),
				},
			},
			&lcommon.PoolRegistrationCertificate{
				CertType: uint(lcommon.CertificateTypePoolRegistration),
				Operator: lcommon.PoolKeyHash(
					[]byte("pool_key_hash_1234567890123456789012345678"),
				),
				VrfKeyHash: lcommon.VrfKeyHash(
					[]byte("vrf_key_hash_12345678901234567890123456789012"),
				),
				Pledge: 1000000,
				Cost:   340000000,
				Margin: cbor.Rat{Rat: big.NewRat(1, 100)},
				RewardAccount: lcommon.AddrKeyHash(
					[]byte("reward_account_1234567890123456789012345678"),
				),
				PoolOwners: []lcommon.AddrKeyHash{
					lcommon.AddrKeyHash(
						[]byte("owner1_1234567890123456789012345678"),
					),
				},
			},
			&lcommon.AuthCommitteeHotCertificate{
				CertType: uint(lcommon.CertificateTypeAuthCommitteeHot),
				ColdCredential: lcommon.Credential{
					CredType: lcommon.CredentialTypeAddrKeyHash,
					Credential: lcommon.CredentialHash(
						[]byte("cold_cred_hash_1234567890123456789012345678"),
					),
				},
				HotCredential: lcommon.Credential{
					CredType: lcommon.CredentialTypeAddrKeyHash,
					Credential: lcommon.CredentialHash(
						[]byte("hot_cred_hash_1234567890123456789012345678"),
					),
				},
			},
		},
	}

	point := ocommon.Point{
		Hash: []byte("block_hash_12345678901234567890123456789012"),
		Slot: 1000000,
	}

	// Process the transaction
	err := pgStore.SetTransaction(
		mockTx,
		point,
		0,
		map[int]uint64{0: 2000000, 1: 500000000},
		nil,
	)
	if err != nil {
		t.Fatalf("failed to set transaction: %v", err)
	}

	// Verify unified certificate records were created
	var unifiedCerts []models.Certificate
	if result := pgStore.DB().Order("cert_index ASC").Find(&unifiedCerts); result.Error != nil {
		t.Fatalf("failed to query unified certificates: %v", result.Error)
	}

	if len(unifiedCerts) != 3 {
		t.Errorf("expected 3 unified certificates, got %d", len(unifiedCerts))
	}

	// Verify the unified certificates have correct data
	for i, cert := range unifiedCerts {
		if cert.TransactionID == 0 {
			t.Errorf("certificate %d has zero transaction ID", i)
		}
		if cert.CertIndex != uint(i) {
			t.Errorf(
				"certificate %d has cert_index %d, expected %d",
				i,
				cert.CertIndex,
				i,
			)
		}
		if cert.Slot != point.Slot {
			t.Errorf(
				"certificate %d has slot %d, expected %d",
				i,
				cert.Slot,
				point.Slot,
			)
		}
		if string(cert.BlockHash) != string(point.Hash) {
			t.Errorf("certificate %d has wrong block hash", i)
		}
	}

	// Verify specialized certificate records were created with correct CertificateID
	var stakeReg models.StakeRegistration
	if result := pgStore.DB().First(&stakeReg); result.Error != nil {
		t.Fatalf("failed to query stake registration: %v", result.Error)
	}

	// Find the unified cert for stake registration (should be index 0)
	var stakeUnified models.Certificate
	if result := pgStore.DB().Where("cert_index = ? AND cert_type = ?", 0, uint(lcommon.CertificateTypeStakeRegistration)).First(&stakeUnified); result.Error != nil {
		t.Fatalf(
			"failed to find unified stake registration cert: %v",
			result.Error,
		)
	}

	if stakeReg.CertificateID != stakeUnified.ID {
		t.Errorf(
			"stake registration CertificateID %d does not match unified cert ID %d",
			stakeReg.CertificateID,
			stakeUnified.ID,
		)
	}

	var poolReg models.PoolRegistration
	if result := pgStore.DB().First(&poolReg); result.Error != nil {
		t.Fatalf("failed to query pool registration: %v", result.Error)
	}

	// Find the unified cert for pool registration (should be index 1)
	var poolUnified models.Certificate
	if result := pgStore.DB().Where("cert_index = ? AND cert_type = ?", 1, uint(lcommon.CertificateTypePoolRegistration)).First(&poolUnified); result.Error != nil {
		t.Fatalf(
			"failed to find unified pool registration cert: %v",
			result.Error,
		)
	}

	if poolReg.CertificateID != poolUnified.ID {
		t.Errorf(
			"pool registration CertificateID %d does not match unified cert ID %d",
			poolReg.CertificateID,
			poolUnified.ID,
		)
	}

	var authHot models.AuthCommitteeHot
	if result := pgStore.DB().First(&authHot); result.Error != nil {
		t.Fatalf("failed to query auth committee hot: %v", result.Error)
	}

	// Find the unified cert for auth committee hot (should be index 2)
	var authUnified models.Certificate
	if result := pgStore.DB().Where("cert_index = ? AND cert_type = ?", 2, uint(lcommon.CertificateTypeAuthCommitteeHot)).First(&authUnified); result.Error != nil {
		t.Fatalf(
			"failed to find unified auth committee hot cert: %v",
			result.Error,
		)
	}

	if authHot.CertificateID != authUnified.ID {
		t.Errorf(
			"auth committee hot CertificateID %d does not match unified cert ID %d",
			authHot.CertificateID,
			authUnified.ID,
		)
	}
}

// TestPostgresSetAccountPreservesCertificateID tests that SetAccount does not
// overwrite the CertificateID field when updating an existing account
func TestPostgresSetAccountPreservesCertificateID(t *testing.T) {
	pgStore := newTestPostgresStore(t)
	defer pgStore.Close() //nolint:errcheck

	stakeKey := []byte("test_stake_key_123456789012345678901234567890")

	// First, create an account with a CertificateID via direct DB access
	account := &models.Account{
		StakingKey:    stakeKey,
		AddedSlot:     1000,
		Pool:          []byte("pool1"),
		Drep:          []byte("drep1"),
		Active:        true,
		CertificateID: 42, // Set a non-zero CertificateID
	}
	if result := pgStore.DB().Create(account); result.Error != nil {
		t.Fatalf("failed to create account: %v", result.Error)
	}

	// Now use SetAccount to update the account (this should NOT overwrite CertificateID)
	err := pgStore.SetAccount(
		stakeKey,
		[]byte("pool2"), // new pool
		[]byte("drep2"), // new drep
		2000,            // new slot
		true,
		nil,
	)
	if err != nil {
		t.Fatalf("failed to set account: %v", err)
	}

	// Fetch the account and verify CertificateID was preserved
	var updatedAccount models.Account
	if result := pgStore.DB().Where("staking_key = ?", stakeKey).First(&updatedAccount); result.Error != nil {
		t.Fatalf("failed to fetch updated account: %v", result.Error)
	}

	if updatedAccount.CertificateID != 42 {
		t.Errorf(
			"CertificateID was overwritten: expected 42, got %d",
			updatedAccount.CertificateID,
		)
	}

	// Verify other fields were updated
	if string(updatedAccount.Pool) != "pool2" {
		t.Errorf(
			"Pool was not updated: expected 'pool2', got '%s'",
			string(updatedAccount.Pool),
		)
	}
	if string(updatedAccount.Drep) != "drep2" {
		t.Errorf(
			"Drep was not updated: expected 'drep2', got '%s'",
			string(updatedAccount.Drep),
		)
	}
	if updatedAccount.AddedSlot != 2000 {
		t.Errorf(
			"AddedSlot was not updated: expected 2000, got %d",
			updatedAccount.AddedSlot,
		)
	}
}

// TestPostgresFeeConversion tests that the Fee field handles nil and large values correctly
func TestPostgresFeeConversion(t *testing.T) {
	pgStore := newTestPostgresStore(t)
	defer pgStore.Close() //nolint:errcheck

	// Test with nil Fee
	mockTxNilFee := &mockTransactionNilFee{
		hash: lcommon.NewBlake2b256(
			[]byte("nil_fee_tx_hash_12345678901234567890"),
		),
		isValid: true,
	}

	point := ocommon.Point{
		Hash: []byte("block_hash_nil_fee_test_1234567890123456"),
		Slot: 2000000,
	}

	err := pgStore.SetTransaction(mockTxNilFee, point, 0, nil, nil)
	if err != nil {
		t.Fatalf("failed to set transaction with nil fee: %v", err)
	}

	// Verify the transaction was created with fee = 0
	var tx models.Transaction
	if result := pgStore.DB().Where("hash = ?", mockTxNilFee.hash.Bytes()).First(&tx); result.Error != nil {
		t.Fatalf("failed to fetch transaction: %v", result.Error)
	}

	if uint64(tx.Fee) != 0 {
		t.Errorf("expected fee to be 0 for nil fee, got %d", tx.Fee)
	}
}

// mockTransactionNilFee is a mock transaction that returns nil for Fee()
type mockTransactionNilFee struct {
	hash    lcommon.Blake2b256
	isValid bool
}

func (m *mockTransactionNilFee) Hash() lcommon.Blake2b256 {
	return m.hash
}

func (m *mockTransactionNilFee) Id() lcommon.Blake2b256 {
	return m.hash
}

func (m *mockTransactionNilFee) Type() int {
	return 0
}

func (m *mockTransactionNilFee) Fee() *big.Int {
	return nil // Return nil to test nil handling
}

func (m *mockTransactionNilFee) TTL() uint64 {
	return 1000000
}

func (m *mockTransactionNilFee) IsValid() bool {
	return m.isValid
}

func (m *mockTransactionNilFee) Metadata() lcommon.TransactionMetadatum {
	return nil
}

func (m *mockTransactionNilFee) AuxiliaryData() lcommon.AuxiliaryData {
	return nil
}

func (m *mockTransactionNilFee) RawAuxiliaryData() []byte {
	return nil
}

func (m *mockTransactionNilFee) CollateralReturn() lcommon.TransactionOutput {
	return nil
}

func (m *mockTransactionNilFee) Produced() []lcommon.Utxo {
	return nil
}

func (m *mockTransactionNilFee) Outputs() []lcommon.TransactionOutput {
	return nil
}

func (m *mockTransactionNilFee) Inputs() []lcommon.TransactionInput {
	return nil
}

func (m *mockTransactionNilFee) Collateral() []lcommon.TransactionInput {
	return nil
}

func (m *mockTransactionNilFee) Certificates() []lcommon.Certificate {
	return nil
}

func (m *mockTransactionNilFee) ProtocolParameterUpdates() (uint64, map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate) {
	return 0, nil
}

func (m *mockTransactionNilFee) AssetMint() *lcommon.MultiAsset[lcommon.MultiAssetTypeMint] {
	return nil
}

func (m *mockTransactionNilFee) AuxDataHash() *lcommon.Blake2b256 {
	return nil
}

func (m *mockTransactionNilFee) Cbor() []byte {
	return []byte("mock_cbor")
}

func (m *mockTransactionNilFee) Consumed() []lcommon.TransactionInput {
	return nil
}

func (m *mockTransactionNilFee) Witnesses() lcommon.TransactionWitnessSet {
	return nil
}

func (m *mockTransactionNilFee) ValidityIntervalStart() uint64 {
	return 0
}

func (m *mockTransactionNilFee) ReferenceInputs() []lcommon.TransactionInput {
	return nil
}

func (m *mockTransactionNilFee) TotalCollateral() *big.Int {
	return big.NewInt(0)
}

func (m *mockTransactionNilFee) Withdrawals() map[*lcommon.Address]*big.Int {
	return nil
}

func (m *mockTransactionNilFee) RequiredSigners() []lcommon.Blake2b224 {
	return nil
}

func (m *mockTransactionNilFee) ScriptDataHash() *lcommon.Blake2b256 {
	return nil
}

func (m *mockTransactionNilFee) VotingProcedures() lcommon.VotingProcedures {
	return lcommon.VotingProcedures{}
}

func (m *mockTransactionNilFee) ProposalProcedures() []lcommon.ProposalProcedure {
	return nil
}

func (m *mockTransactionNilFee) CurrentTreasuryValue() *big.Int {
	return big.NewInt(0)
}

func (m *mockTransactionNilFee) Donation() *big.Int {
	return big.NewInt(0)
}

func (m *mockTransactionNilFee) Utxorpc() (*cardano.Tx, error) {
	return nil, nil
}

func (m *mockTransactionNilFee) LeiosHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

// createTestTransactionPg is a helper to create a Transaction record for FK constraints in postgres tests
func createTestTransactionPg(db *gorm.DB, id uint, slot uint64) error {
	tx := models.Transaction{
		Hash: []byte(
			"tx_hash_" + strconv.FormatUint(
				uint64(id),
				10,
			) + "_123456789012345678901234567890",
		),
		BlockHash: []byte(
			"block_hash_" + strconv.FormatUint(
				slot,
				10,
			) + "_12345678901234567890123456789012",
		),
		Slot:  slot,
		Valid: true,
	}
	tx.ID = id
	return db.Create(&tx).Error
}

// TestPostgresDeleteCertificatesAfterSlot tests that certificates are correctly deleted after a slot
func TestPostgresDeleteCertificatesAfterSlot(t *testing.T) {
	pgStore := newTestPostgresStore(t)
	defer pgStore.Close() //nolint:errcheck

	// Clean up any existing records
	pgStore.DB().Where("1 = 1").Delete(&models.StakeDelegation{})
	pgStore.DB().Where("1 = 1").Delete(&models.Certificate{})
	pgStore.DB().Where("1 = 1").Delete(&models.Transaction{})

	// Create Transaction records for foreign key constraints
	if err := createTestTransactionPg(pgStore.DB(), 1, 1000); err != nil {
		t.Fatalf("failed to create transaction 1: %v", err)
	}
	if err := createTestTransactionPg(pgStore.DB(), 2, 2000); err != nil {
		t.Fatalf("failed to create transaction 2: %v", err)
	}

	// Create certificate at slot 1000
	cert1 := models.Certificate{
		Slot: 1000,
		BlockHash: []byte(
			"block_hash_1000_12345678901234567890123456789012",
		),
		CertType:      uint(lcommon.CertificateTypeStakeDelegation),
		TransactionID: 1,
		CertIndex:     0,
	}
	if result := pgStore.DB().Create(&cert1); result.Error != nil {
		t.Fatalf("failed to create cert1: %v", result.Error)
	}

	stakeReg1 := models.StakeDelegation{
		CertificateID: cert1.ID,
		StakingKey:    []byte("stake_key_1_1234567890123456789012345678"),
		PoolKeyHash:   []byte("pool_hash_1_12345678901234567890123456789012"),
		AddedSlot:     1000,
	}
	if result := pgStore.DB().Create(&stakeReg1); result.Error != nil {
		t.Fatalf("failed to create stakeReg1: %v", result.Error)
	}

	// Create certificate at slot 2000
	cert2 := models.Certificate{
		Slot: 2000,
		BlockHash: []byte(
			"block_hash_2000_12345678901234567890123456789012",
		),
		CertType:      uint(lcommon.CertificateTypeStakeDelegation),
		TransactionID: 2,
		CertIndex:     0,
	}
	if result := pgStore.DB().Create(&cert2); result.Error != nil {
		t.Fatalf("failed to create cert2: %v", result.Error)
	}

	stakeReg2 := models.StakeDelegation{
		CertificateID: cert2.ID,
		StakingKey:    []byte("stake_key_2_1234567890123456789012345678"),
		PoolKeyHash:   []byte("pool_hash_2_12345678901234567890123456789012"),
		AddedSlot:     2000,
	}
	if result := pgStore.DB().Create(&stakeReg2); result.Error != nil {
		t.Fatalf("failed to create stakeReg2: %v", result.Error)
	}

	// Verify we have 2 certificates
	var countBefore int64
	pgStore.DB().Model(&models.Certificate{}).Count(&countBefore)
	if countBefore != 2 {
		t.Fatalf("expected 2 certificates before rollback, got %d", countBefore)
	}

	// Delete certificates after slot 1500 (should delete the one at slot 2000)
	if err := pgStore.DeleteCertificatesAfterSlot(1500, nil); err != nil {
		t.Fatalf("failed to delete certificates: %v", err)
	}

	// Verify only 1 certificate remains
	var countAfter int64
	pgStore.DB().Model(&models.Certificate{}).Count(&countAfter)
	if countAfter != 1 {
		t.Errorf("expected 1 certificate after rollback, got %d", countAfter)
	}

	// Verify the remaining certificate is at slot 1000
	var remainingCert models.Certificate
	if result := pgStore.DB().First(&remainingCert); result.Error != nil {
		t.Fatalf("failed to query remaining certificate: %v", result.Error)
	}
	if remainingCert.Slot != 1000 {
		t.Errorf(
			"expected remaining certificate at slot 1000, got %d",
			remainingCert.Slot,
		)
	}

	// Verify specialized delegation record was also deleted
	var delegationCount int64
	pgStore.DB().Model(&models.StakeDelegation{}).Count(&delegationCount)
	if delegationCount != 1 {
		t.Errorf(
			"expected 1 delegation after rollback, got %d",
			delegationCount,
		)
	}
}

// TestPostgresRestoreAccountStateAtSlot tests account delegation state restoration
func TestPostgresRestoreAccountStateAtSlot(t *testing.T) {
	t.Run("account delegation is restored to prior pool", func(t *testing.T) {
		pgStore := newTestPostgresStore(t)
		defer pgStore.Close() //nolint:errcheck

		// Clean up
		pgStore.DB().Where("1 = 1").Delete(&models.StakeDelegation{})
		pgStore.DB().Where("1 = 1").Delete(&models.StakeRegistration{})
		pgStore.DB().Where("1 = 1").Delete(&models.Certificate{})
		pgStore.DB().Where("1 = 1").Delete(&models.Account{})
		pgStore.DB().Where("1 = 1").Delete(&models.Transaction{})

		// Create transactions
		if err := createTestTransactionPg(pgStore.DB(), 100, 1000); err != nil {
			t.Fatalf("failed to create transaction: %v", err)
		}
		if err := createTestTransactionPg(pgStore.DB(), 101, 2000); err != nil {
			t.Fatalf("failed to create transaction: %v", err)
		}
		if err := createTestTransactionPg(pgStore.DB(), 102, 3000); err != nil {
			t.Fatalf("failed to create transaction: %v", err)
		}

		stakingKey := []byte(
			"staking_key_test_12345678901234567890123456789012",
		)
		pool1 := []byte("pool1_12345678901234567890123456789012")
		pool2 := []byte("pool2_12345678901234567890123456789012")

		// Create registration certificate at slot 1000
		regCert := models.Certificate{
			Slot: 1000,
			BlockHash: []byte(
				"block_hash_1000_12345678901234567890123456789012",
			),
			CertType:      uint(lcommon.CertificateTypeStakeRegistration),
			TransactionID: 100,
			CertIndex:     0,
		}
		pgStore.DB().Create(&regCert)

		stakeReg := models.StakeRegistration{
			CertificateID: regCert.ID,
			StakingKey:    stakingKey,
			AddedSlot:     1000,
		}
		pgStore.DB().Create(&stakeReg)

		// Create delegation to pool1 at slot 2000
		delCert1 := models.Certificate{
			Slot: 2000,
			BlockHash: []byte(
				"block_hash_2000_12345678901234567890123456789012",
			),
			CertType:      uint(lcommon.CertificateTypeStakeDelegation),
			TransactionID: 101,
			CertIndex:     0,
		}
		pgStore.DB().Create(&delCert1)

		stakeDel1 := models.StakeDelegation{
			CertificateID: delCert1.ID,
			StakingKey:    stakingKey,
			PoolKeyHash:   pool1,
			AddedSlot:     2000,
		}
		pgStore.DB().Create(&stakeDel1)

		// Create delegation to pool2 at slot 3000
		delCert2 := models.Certificate{
			Slot: 3000,
			BlockHash: []byte(
				"block_hash_3000_12345678901234567890123456789012",
			),
			CertType:      uint(lcommon.CertificateTypeStakeDelegation),
			TransactionID: 102,
			CertIndex:     0,
		}
		pgStore.DB().Create(&delCert2)

		stakeDel2 := models.StakeDelegation{
			CertificateID: delCert2.ID,
			StakingKey:    stakingKey,
			PoolKeyHash:   pool2,
			AddedSlot:     3000,
		}
		pgStore.DB().Create(&stakeDel2)

		// Create account with current state (pool2, slot 3000)
		account := models.Account{
			StakingKey: stakingKey,
			Pool:       pool2,
			AddedSlot:  3000,
			Active:     true,
		}
		pgStore.DB().Create(&account)

		// Restore to slot 2500 (should restore to pool1)
		if err := pgStore.RestoreAccountStateAtSlot(2500, nil); err != nil {
			t.Fatalf("failed to restore account state: %v", err)
		}

		// Verify account is restored to pool1
		var restoredAccount models.Account
		pgStore.DB().First(&restoredAccount, "staking_key = ?", stakingKey)

		if string(restoredAccount.Pool) != string(pool1) {
			t.Errorf(
				"expected pool to be pool1, got %s",
				string(restoredAccount.Pool),
			)
		}
		if !restoredAccount.Active {
			t.Errorf("expected account to be active")
		}
	})

	t.Run("account with no prior registration is deleted", func(t *testing.T) {
		pgStore := newTestPostgresStore(t)
		defer pgStore.Close() //nolint:errcheck

		// Clean up
		pgStore.DB().Where("1 = 1").Delete(&models.StakeRegistration{})
		pgStore.DB().Where("1 = 1").Delete(&models.Certificate{})
		pgStore.DB().Where("1 = 1").Delete(&models.Account{})
		pgStore.DB().Where("1 = 1").Delete(&models.Transaction{})

		stakingKey := []byte("staking_key_new_12345678901234567890123456789012")

		// Create account registered at slot 2000 (no prior registration)
		account := models.Account{
			StakingKey: stakingKey,
			AddedSlot:  2000,
			Active:     true,
		}
		pgStore.DB().Create(&account)

		// Restore to slot 1500 (before registration)
		if err := pgStore.RestoreAccountStateAtSlot(1500, nil); err != nil {
			t.Fatalf("failed to restore account state: %v", err)
		}

		// Verify account is deleted
		var count int64
		pgStore.DB().
			Model(&models.Account{}).
			Where("staking_key = ?", stakingKey).
			Count(&count)
		if count != 0 {
			t.Errorf("expected account to be deleted, but found %d", count)
		}
	})
}

// TestPostgresDeletePParamsAfterSlot tests protocol parameter deletion
func TestPostgresDeletePParamsAfterSlot(t *testing.T) {
	pgStore := newTestPostgresStore(t)
	defer pgStore.Close() //nolint:errcheck

	// Clean up
	pgStore.DB().Where("1 = 1").Delete(&models.PParams{})

	// Create pparams at different slots
	pparams1 := models.PParams{
		AddedSlot: 1000,
		Epoch:     100,
		EraId:     1,
		Cbor:      []byte("cbor1"),
	}
	pparams2 := models.PParams{
		AddedSlot: 2000,
		Epoch:     101,
		EraId:     1,
		Cbor:      []byte("cbor2"),
	}
	pgStore.DB().Create(&pparams1)
	pgStore.DB().Create(&pparams2)

	// Verify we have 2
	var countBefore int64
	pgStore.DB().Model(&models.PParams{}).Count(&countBefore)
	if countBefore != 2 {
		t.Fatalf("expected 2 pparams before, got %d", countBefore)
	}

	// Delete after slot 1500
	if err := pgStore.DeletePParamsAfterSlot(1500, nil); err != nil {
		t.Fatalf("failed to delete pparams: %v", err)
	}

	// Verify only 1 remains
	var countAfter int64
	pgStore.DB().Model(&models.PParams{}).Count(&countAfter)
	if countAfter != 1 {
		t.Errorf("expected 1 pparam after rollback, got %d", countAfter)
	}
}

// TestPostgresDeleteTransactionsAfterSlot tests transaction deletion and UTXO restoration
func TestPostgresDeleteTransactionsAfterSlot(t *testing.T) {
	pgStore := newTestPostgresStore(t)
	defer pgStore.Close() //nolint:errcheck

	// Clean up
	pgStore.DB().Where("1 = 1").Delete(&models.Utxo{})
	pgStore.DB().Where("1 = 1").Delete(&models.Transaction{})

	// Create transactions at different slots
	tx1Hash := []byte(
		"tx1_hash_12345678901234567890123456789012345678901234567890",
	)
	tx2Hash := []byte(
		"tx2_hash_12345678901234567890123456789012345678901234567890",
	)

	tx1 := models.Transaction{Hash: tx1Hash, Slot: 1000, Valid: true}
	tx2 := models.Transaction{Hash: tx2Hash, Slot: 2000, Valid: true}
	pgStore.DB().Create(&tx1)
	pgStore.DB().Create(&tx2)

	// Create a UTXO that was spent by tx2
	utxo := models.Utxo{
		TxId: []byte(
			"utxo_txid_123456789012345678901234567890123456789012",
		),
		OutputIdx:   0,
		AddedSlot:   500,
		SpentAtTxId: tx2Hash,
		DeletedSlot: 2000,
	}
	pgStore.DB().Create(&utxo)

	// Verify setup
	var txCountBefore int64
	pgStore.DB().Model(&models.Transaction{}).Count(&txCountBefore)
	if txCountBefore != 2 {
		t.Fatalf("expected 2 transactions before, got %d", txCountBefore)
	}

	// Delete transactions after slot 1500
	if err := pgStore.DeleteTransactionsAfterSlot(1500, nil); err != nil {
		t.Fatalf("failed to delete transactions: %v", err)
	}

	// Verify only 1 transaction remains
	var txCountAfter int64
	pgStore.DB().Model(&models.Transaction{}).Count(&txCountAfter)
	if txCountAfter != 1 {
		t.Errorf("expected 1 transaction after rollback, got %d", txCountAfter)
	}

	// Verify UTXO spent_at_tx_id was cleared (UTXO restored to unspent)
	var restoredUtxo models.Utxo
	pgStore.DB().First(&restoredUtxo, "id = ?", utxo.ID)
	if restoredUtxo.SpentAtTxId != nil {
		t.Errorf(
			"expected SpentAtTxId to be nil, got %v",
			restoredUtxo.SpentAtTxId,
		)
	}
	if restoredUtxo.DeletedSlot != 0 {
		t.Errorf(
			"expected DeletedSlot to be 0, got %d",
			restoredUtxo.DeletedSlot,
		)
	}
}

// TestPostgresRestorePoolStateAtSlot tests pool state restoration
func TestPostgresRestorePoolStateAtSlot(t *testing.T) {
	t.Run("pool with no prior registrations is deleted", func(t *testing.T) {
		pgStore := newTestPostgresStore(t)
		defer pgStore.Close() //nolint:errcheck

		// Clean up
		pgStore.DB().Where("1 = 1").Delete(&models.PoolRegistration{})
		pgStore.DB().Where("1 = 1").Delete(&models.Pool{})

		poolKeyHash := []byte("pool_key_hash_12345678901234567890123456789012")

		// Create pool registered at slot 2000
		pool := models.Pool{
			PoolKeyHash: poolKeyHash,
		}
		pgStore.DB().Create(&pool)

		// Create registration at slot 2000 (after rollback point)
		poolReg := models.PoolRegistration{
			PoolID:      pool.ID,
			PoolKeyHash: poolKeyHash,
			AddedSlot:   2000,
		}
		pgStore.DB().Create(&poolReg)

		// Restore to slot 1500 (before registration)
		if err := pgStore.RestorePoolStateAtSlot(1500, nil); err != nil {
			t.Fatalf("failed to restore pool state: %v", err)
		}

		// Verify pool is deleted
		var count int64
		pgStore.DB().
			Model(&models.Pool{}).
			Where("pool_key_hash = ?", poolKeyHash).
			Count(&count)
		if count != 0 {
			t.Errorf("expected pool to be deleted, found %d", count)
		}
	})
}

// TestPostgresRestoreDrepStateAtSlot tests DRep state restoration
func TestPostgresRestoreDrepStateAtSlot(t *testing.T) {
	t.Run("DRep with no prior registrations is deleted", func(t *testing.T) {
		pgStore := newTestPostgresStore(t)
		defer pgStore.Close() //nolint:errcheck

		// Clean up
		pgStore.DB().Where("1 = 1").Delete(&models.RegistrationDrep{})
		pgStore.DB().Where("1 = 1").Delete(&models.Drep{})

		drepCred := []byte("drep_credential_12345678901234567890123456789012")

		// Create DRep registered at slot 2000
		drep := models.Drep{
			Credential: drepCred,
			AddedSlot:  2000,
			Active:     true,
		}
		pgStore.DB().Create(&drep)

		// Restore to slot 1500 (before registration)
		if err := pgStore.RestoreDrepStateAtSlot(1500, nil); err != nil {
			t.Fatalf("failed to restore DRep state: %v", err)
		}

		// Verify DRep is deleted
		var count int64
		pgStore.DB().
			Model(&models.Drep{}).
			Where("credential = ?", drepCred).
			Count(&count)
		if count != 0 {
			t.Errorf("expected DRep to be deleted, found %d", count)
		}
	})

	t.Run(
		"DRep with prior registration has state restored",
		func(t *testing.T) {
			pgStore := newTestPostgresStore(t)
			defer pgStore.Close() //nolint:errcheck

			// Clean up
			pgStore.DB().Where("1 = 1").Delete(&models.RegistrationDrep{})
			pgStore.DB().Where("1 = 1").Delete(&models.UpdateDrep{})
			pgStore.DB().Where("1 = 1").Delete(&models.DeregistrationDrep{})
			pgStore.DB().Where("1 = 1").Delete(&models.Certificate{})
			pgStore.DB().Where("1 = 1").Delete(&models.Drep{})
			pgStore.DB().Where("1 = 1").Delete(&models.Transaction{})

			drepCred := []byte(
				"drep_credential_12345678901234567890123456789012",
			)

			// Create transactions
			if err := createTestTransactionPg(pgStore.DB(), 200, 1000); err != nil {
				t.Fatalf("failed to create transaction: %v", err)
			}
			if err := createTestTransactionPg(pgStore.DB(), 201, 2000); err != nil {
				t.Fatalf("failed to create transaction: %v", err)
			}

			// Create registration at slot 1000
			regCert := models.Certificate{
				Slot: 1000,
				BlockHash: []byte(
					"block_hash_1000_12345678901234567890123456789012",
				),
				CertType:      uint(lcommon.CertificateTypeRegistrationDrep),
				TransactionID: 200,
				CertIndex:     0,
			}
			pgStore.DB().Create(&regCert)

			drepReg := models.RegistrationDrep{
				CertificateID:  regCert.ID,
				DrepCredential: drepCred,
				AnchorUrl:      "https://example.com/drep1",
				AnchorHash: []byte(
					"anchor_hash_1_12345678901234567890123456789012",
				),
				AddedSlot: 1000,
			}
			pgStore.DB().Create(&drepReg)

			// Create update at slot 2000 with different anchor
			updateCert := models.Certificate{
				Slot: 2000,
				BlockHash: []byte(
					"block_hash_2000_12345678901234567890123456789012",
				),
				CertType:      uint(lcommon.CertificateTypeUpdateDrep),
				TransactionID: 201,
				CertIndex:     0,
			}
			pgStore.DB().Create(&updateCert)

			drepUpdate := models.UpdateDrep{
				CertificateID: updateCert.ID,
				Credential:    drepCred,
				AnchorUrl:     "https://example.com/drep2",
				AnchorHash: []byte(
					"anchor_hash_2_12345678901234567890123456789012",
				),
				AddedSlot: 2000,
			}
			pgStore.DB().Create(&drepUpdate)

			// Create DRep with current state (from update at slot 2000)
			drep := models.Drep{
				Credential: drepCred,
				AnchorUrl:  "https://example.com/drep2",
				AnchorHash: []byte(
					"anchor_hash_2_12345678901234567890123456789012",
				),
				AddedSlot: 2000,
				Active:    true,
			}
			pgStore.DB().Create(&drep)

			// Restore to slot 1500 (should restore to registration state)
			if err := pgStore.RestoreDrepStateAtSlot(1500, nil); err != nil {
				t.Fatalf("failed to restore DRep state: %v", err)
			}

			// Verify DRep is restored to registration state
			var restoredDrep models.Drep
			pgStore.DB().First(&restoredDrep, "credential = ?", drepCred)

			if restoredDrep.AnchorUrl != "https://example.com/drep1" {
				t.Errorf(
					"expected anchor URL to be restored to drep1, got %s",
					restoredDrep.AnchorUrl,
				)
			}
			if !restoredDrep.Active {
				t.Errorf("expected DRep to be active")
			}
		},
	)
}
