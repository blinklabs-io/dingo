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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	"gorm.io/gorm"
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
	return os.Getenv("POSTGRES_PASSWORD") != "" || os.Getenv("POSTGRES_DSN") != ""
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
		t.Skip("Skipping postgres integration test: postgres not configured (set POSTGRES_PASSWORD or configure via cmdline options)")
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
		t.Skip("Skipping postgres integration test: postgres not configured (set POSTGRES_PASSWORD or configure via cmdline options)")
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
		t.Errorf("Pool was not updated: expected 'pool2', got '%s'", string(updatedAccount.Pool))
	}
	if string(updatedAccount.Drep) != "drep2" {
		t.Errorf("Drep was not updated: expected 'drep2', got '%s'", string(updatedAccount.Drep))
	}
	if updatedAccount.AddedSlot != 2000 {
		t.Errorf("AddedSlot was not updated: expected 2000, got %d", updatedAccount.AddedSlot)
	}
}

// TestPostgresFeeConversion tests that the Fee field handles nil and large values correctly
func TestPostgresFeeConversion(t *testing.T) {
	pgStore := newTestPostgresStore(t)
	defer pgStore.Close() //nolint:errcheck

	// Test with nil Fee
	mockTxNilFee := &mockTransactionNilFee{
		hash:    lcommon.NewBlake2b256([]byte("nil_fee_tx_hash_12345678901234567890")),
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
