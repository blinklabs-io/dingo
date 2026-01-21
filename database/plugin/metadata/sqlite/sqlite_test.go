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

package sqlite

import (
	"math/big"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	"gorm.io/gorm"
)

type TestTable struct {
	gorm.Model
}

type mockTransaction struct {
	certificates []lcommon.Certificate
	hash         lcommon.Blake2b256
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

// createTestTransaction creates a Transaction record for testing with foreign key constraints.
func createTestTransaction(db *gorm.DB, id uint, slot uint64) error {
	tx := models.Transaction{
		Hash:       []byte{byte(id), byte(id >> 8), byte(id >> 16), byte(id >> 24)},
		Slot:       slot,
		Valid:      true,
		Type:       0,
		BlockIndex: 0,
	}
	// Use raw SQL to insert with specific ID
	return db.Exec(
		"INSERT INTO \"transaction\" (id, hash, slot, valid, type, block_index) VALUES (?, ?, ?, ?, ?, ?)",
		id,
		tx.Hash,
		tx.Slot,
		tx.Valid,
		tx.Type,
		tx.BlockIndex,
	).Error
}

// TestInMemorySqliteMultipleTransaction tests that our sqlite connection allows multiple
// concurrent transactions when using in-memory mode. This requires special URI flags, and
// this is mostly making sure that we don't lose them
func TestInMemorySqliteMultipleTransaction(t *testing.T) {
	sqliteStore, err := New("", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := sqliteStore.Start(); err != nil {
		t.Fatalf("unexpected error starting store: %s", err)
	}
	defer sqliteStore.Close() //nolint:errcheck

	if err := sqliteStore.DB().AutoMigrate(&TestTable{}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if result := sqliteStore.DB().Create(&TestTable{}); result.Error != nil {
		t.Fatalf("unexpected error: %s", result.Error)
	}

	doQuery := func(sleep time.Duration) error {
		txn := sqliteStore.DB().Begin()
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

// TestUnifiedCertificateCreation tests that unified certificate records are created
// correctly and linked to specialized certificate records
func TestUnifiedCertificateCreation(t *testing.T) {
	sqliteStore, err := New("", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := sqliteStore.Start(); err != nil {
		t.Fatalf("unexpected error starting store: %s", err)
	}
	defer sqliteStore.Close() //nolint:errcheck

	// Run auto-migration to ensure tables exist
	if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
		t.Fatalf("failed to auto-migrate: %v", err)
	}

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
	err = sqliteStore.SetTransaction(
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
	if result := sqliteStore.DB().Order("cert_index ASC").Find(&unifiedCerts); result.Error != nil {
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
	if result := sqliteStore.DB().First(&stakeReg); result.Error != nil {
		t.Fatalf("failed to query stake registration: %v", result.Error)
	}

	// Find the unified cert for stake registration (should be index 0)
	var stakeUnified models.Certificate
	if result := sqliteStore.DB().Where("cert_index = ? AND cert_type = ?", 0, uint(lcommon.CertificateTypeStakeRegistration)).First(&stakeUnified); result.Error != nil {
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
	if result := sqliteStore.DB().First(&poolReg); result.Error != nil {
		t.Fatalf("failed to query pool registration: %v", result.Error)
	}

	// Find the unified cert for pool registration (should be index 1)
	var poolUnified models.Certificate
	if result := sqliteStore.DB().Where("cert_index = ? AND cert_type = ?", 1, uint(lcommon.CertificateTypePoolRegistration)).First(&poolUnified); result.Error != nil {
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
	if result := sqliteStore.DB().First(&authHot); result.Error != nil {
		t.Fatalf("failed to query auth committee hot: %v", result.Error)
	}

	// Find the unified cert for auth committee hot (should be index 2)
	var authUnified models.Certificate
	if result := sqliteStore.DB().Where("cert_index = ? AND cert_type = ?", 2, uint(lcommon.CertificateTypeAuthCommitteeHot)).First(&authUnified); result.Error != nil {
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

// TestDeleteCertificatesAfterSlot tests that certificates added after a given slot are deleted
func TestDeleteCertificatesAfterSlot(t *testing.T) {
	sqliteStore, err := New("", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := sqliteStore.Start(); err != nil {
		t.Fatalf("unexpected error starting store: %s", err)
	}
	defer sqliteStore.Close() //nolint:errcheck

	// Run auto-migration
	if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
		t.Fatalf("failed to auto-migrate: %v", err)
	}

	// Create Transaction records for foreign key constraints
	if err := createTestTransaction(sqliteStore.DB(), 1, 1000); err != nil {
		t.Fatalf("failed to create transaction 1: %v", err)
	}
	if err := createTestTransaction(sqliteStore.DB(), 2, 2000); err != nil {
		t.Fatalf("failed to create transaction 2: %v", err)
	}

	// Create certificate at slot 1000 directly
	cert1 := models.Certificate{
		Slot: 1000,
		BlockHash: []byte(
			"block_hash_1000_12345678901234567890123456789012",
		),
		CertType:      uint(lcommon.CertificateTypeStakeDelegation),
		TransactionID: 1,
		CertIndex:     0,
	}
	if result := sqliteStore.DB().Create(&cert1); result.Error != nil {
		t.Fatalf("failed to create cert1: %v", result.Error)
	}

	stakeReg1 := models.StakeDelegation{
		CertificateID: cert1.ID,
		StakingKey:    []byte("stake_key_1_1234567890123456789012345678"),
		PoolKeyHash:   []byte("pool_hash_1_12345678901234567890123456789012"),
		AddedSlot:     1000,
	}
	if result := sqliteStore.DB().Create(&stakeReg1); result.Error != nil {
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
	if result := sqliteStore.DB().Create(&cert2); result.Error != nil {
		t.Fatalf("failed to create cert2: %v", result.Error)
	}

	stakeReg2 := models.StakeDelegation{
		CertificateID: cert2.ID,
		StakingKey:    []byte("stake_key_2_1234567890123456789012345678"),
		PoolKeyHash:   []byte("pool_hash_2_12345678901234567890123456789012"),
		AddedSlot:     2000,
	}
	if result := sqliteStore.DB().Create(&stakeReg2); result.Error != nil {
		t.Fatalf("failed to create stakeReg2: %v", result.Error)
	}

	// Verify we have 2 certificates
	var countBefore int64
	sqliteStore.DB().Model(&models.Certificate{}).Count(&countBefore)
	if countBefore != 2 {
		t.Fatalf("expected 2 certificates before rollback, got %d", countBefore)
	}

	// Delete certificates after slot 1500 (should delete the one at slot 2000)
	if err := sqliteStore.DeleteCertificatesAfterSlot(1500, nil); err != nil {
		t.Fatalf("failed to delete certificates: %v", err)
	}

	// Verify only 1 certificate remains
	var countAfter int64
	sqliteStore.DB().Model(&models.Certificate{}).Count(&countAfter)
	if countAfter != 1 {
		t.Errorf("expected 1 certificate after rollback, got %d", countAfter)
	}

	// Verify the remaining certificate is at slot 1000
	var remainingCert models.Certificate
	if result := sqliteStore.DB().First(&remainingCert); result.Error != nil {
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
	sqliteStore.DB().Model(&models.StakeDelegation{}).Count(&delegationCount)
	if delegationCount != 1 {
		t.Errorf(
			"expected 1 delegation after rollback, got %d",
			delegationCount,
		)
	}
}

// TestRestoreAccountStateAtSlot tests that account delegation state is correctly restored
func TestRestoreAccountStateAtSlot(t *testing.T) {
	t.Run("account delegation is restored to prior pool", func(t *testing.T) {
		sqliteStore, err := New("", nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if err := sqliteStore.Start(); err != nil {
			t.Fatalf("unexpected error starting store: %s", err)
		}
		defer sqliteStore.Close() //nolint:errcheck

		// Run auto-migration
		if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
			t.Fatalf("failed to auto-migrate: %v", err)
		}

		// Create Transaction records for foreign key constraints
		for i := uint(1); i <= 3; i++ {
			if err := createTestTransaction(sqliteStore.DB(), i, uint64(i*500)); err != nil {
				t.Fatalf("failed to create transaction %d: %v", i, err)
			}
		}

		stakingKey := []byte("staking_key_test_1234567890123456789012345678")
		poolHash1 := []byte("pool_hash_1_12345678901234567890123456789012")
		poolHash2 := []byte("pool_hash_2_12345678901234567890123456789012")

		// Create an account with delegation to pool2 at slot 2000
		// (simulating current state after delegation change)
		account := models.Account{
			StakingKey: stakingKey,
			Pool:       poolHash2,
			AddedSlot:  2000,
			Active:     true,
		}
		if result := sqliteStore.DB().Create(&account); result.Error != nil {
			t.Fatalf("failed to create account: %v", result.Error)
		}

		// Create stake registration certificate at slot 500 (registration must exist before delegation)
		regCert := models.Certificate{
			Slot: 500,
			BlockHash: []byte(
				"block_500_1234567890123456789012345678901234",
			),
			CertType:      uint(lcommon.CertificateTypeStakeRegistration),
			TransactionID: 1,
			CertIndex:     0,
		}
		if result := sqliteStore.DB().Create(&regCert); result.Error != nil {
			t.Fatalf("failed to create regCert: %v", result.Error)
		}

		stakeReg := models.StakeRegistration{
			CertificateID: regCert.ID,
			StakingKey:    stakingKey,
			AddedSlot:     500,
		}
		if result := sqliteStore.DB().Create(&stakeReg); result.Error != nil {
			t.Fatalf("failed to create stakeReg: %v", result.Error)
		}

		// Create initial stake delegation certificate at slot 1000 (to pool1)
		cert1 := models.Certificate{
			Slot: 1000,
			BlockHash: []byte(
				"block_1000_12345678901234567890123456789012",
			),
			CertType:      uint(lcommon.CertificateTypeStakeDelegation),
			TransactionID: 2,
			CertIndex:     0,
		}
		if result := sqliteStore.DB().Create(&cert1); result.Error != nil {
			t.Fatalf("failed to create cert1: %v", result.Error)
		}

		delegation1 := models.StakeDelegation{
			CertificateID: cert1.ID,
			StakingKey:    stakingKey,
			PoolKeyHash:   poolHash1,
			AddedSlot:     1000,
		}
		if result := sqliteStore.DB().Create(&delegation1); result.Error != nil {
			t.Fatalf("failed to create delegation1: %v", result.Error)
		}

		// Create stake delegation certificate at slot 2000 (to pool2)
		cert2 := models.Certificate{
			Slot: 2000,
			BlockHash: []byte(
				"block_2000_12345678901234567890123456789012",
			),
			CertType:      uint(lcommon.CertificateTypeStakeDelegation),
			TransactionID: 3,
			CertIndex:     0,
		}
		if result := sqliteStore.DB().Create(&cert2); result.Error != nil {
			t.Fatalf("failed to create cert2: %v", result.Error)
		}

		delegation2 := models.StakeDelegation{
			CertificateID: cert2.ID,
			StakingKey:    stakingKey,
			PoolKeyHash:   poolHash2,
			AddedSlot:     2000,
		}
		if result := sqliteStore.DB().Create(&delegation2); result.Error != nil {
			t.Fatalf("failed to create delegation2: %v", result.Error)
		}

		// Delete certificates after slot 1500 (removes cert2 and delegation2)
		if err := sqliteStore.DeleteCertificatesAfterSlot(1500, nil); err != nil {
			t.Fatalf("failed to delete certificates: %v", err)
		}

		// Restore account state to slot 1500
		if err := sqliteStore.RestoreAccountStateAtSlot(1500, nil); err != nil {
			t.Fatalf("failed to restore account state: %v", err)
		}

		// Verify account is delegated back to pool1
		var restoredAccount models.Account
		if result := sqliteStore.DB().First(&restoredAccount); result.Error != nil {
			t.Fatalf("failed to query restored account: %v", result.Error)
		}
		if string(restoredAccount.Pool) != string(poolHash1) {
			t.Errorf(
				"expected account delegated to pool1, got %x",
				restoredAccount.Pool,
			)
		}
	})

	t.Run(
		"account deregistered after rollback slot is restored to active",
		func(t *testing.T) {
			sqliteStore, err := New("", nil, nil)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if err := sqliteStore.Start(); err != nil {
				t.Fatalf("unexpected error starting store: %s", err)
			}
			defer sqliteStore.Close() //nolint:errcheck

			if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
				t.Fatalf("failed to auto-migrate: %v", err)
			}

			// Create Transaction records for foreign key constraints
			for i := uint(1); i <= 2; i++ {
				if err := createTestTransaction(sqliteStore.DB(), i, uint64(i*1000)); err != nil {
					t.Fatalf("failed to create transaction %d: %v", i, err)
				}
			}

			stakingKey := []byte("staking_key_active_test_12345678901234567890")

			// Create an account that is currently inactive (deregistered at slot 2000)
			account := models.Account{
				StakingKey: stakingKey,
				AddedSlot:  2000,
				Active:     false, // Currently inactive due to deregistration
			}
			if result := sqliteStore.DB().Create(&account); result.Error != nil {
				t.Fatalf("failed to create account: %v", result.Error)
			}

			// Create stake registration certificate at slot 1000
			regCert := models.Certificate{
				Slot: 1000,
				BlockHash: []byte(
					"block_1000_12345678901234567890123456789012",
				),
				CertType:      uint(lcommon.CertificateTypeStakeRegistration),
				TransactionID: 1,
				CertIndex:     0,
			}
			if result := sqliteStore.DB().Create(&regCert); result.Error != nil {
				t.Fatalf("failed to create regCert: %v", result.Error)
			}

			stakeReg := models.StakeRegistration{
				CertificateID: regCert.ID,
				StakingKey:    stakingKey,
				AddedSlot:     1000,
			}
			if result := sqliteStore.DB().Create(&stakeReg); result.Error != nil {
				t.Fatalf("failed to create stakeReg: %v", result.Error)
			}

			// Create stake deregistration certificate at slot 2000 (after rollback point)
			deregCert := models.Certificate{
				Slot: 2000,
				BlockHash: []byte(
					"block_2000_12345678901234567890123456789012",
				),
				CertType:      uint(lcommon.CertificateTypeStakeDeregistration),
				TransactionID: 2,
				CertIndex:     0,
			}
			if result := sqliteStore.DB().Create(&deregCert); result.Error != nil {
				t.Fatalf("failed to create deregCert: %v", result.Error)
			}

			stakeDereg := models.StakeDeregistration{
				CertificateID: deregCert.ID,
				StakingKey:    stakingKey,
				AddedSlot:     2000,
			}
			if result := sqliteStore.DB().Create(&stakeDereg); result.Error != nil {
				t.Fatalf("failed to create stakeDereg: %v", result.Error)
			}

			// Delete certificates after slot 1500 (removes deregistration)
			if err := sqliteStore.DeleteCertificatesAfterSlot(1500, nil); err != nil {
				t.Fatalf("failed to delete certificates: %v", err)
			}

			// Restore account state to slot 1500
			if err := sqliteStore.RestoreAccountStateAtSlot(1500, nil); err != nil {
				t.Fatalf("failed to restore account state: %v", err)
			}

			// Verify account is now active (deregistration was rolled back)
			var restoredAccount models.Account
			if result := sqliteStore.DB().First(&restoredAccount); result.Error != nil {
				t.Fatalf("failed to query restored account: %v", result.Error)
			}
			if !restoredAccount.Active {
				t.Error(
					"expected account to be active after rolling back deregistration",
				)
			}
		},
	)

	t.Run(
		"account deregistered before rollback slot remains inactive",
		func(t *testing.T) {
			sqliteStore, err := New("", nil, nil)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if err := sqliteStore.Start(); err != nil {
				t.Fatalf("unexpected error starting store: %s", err)
			}
			defer sqliteStore.Close() //nolint:errcheck

			if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
				t.Fatalf("failed to auto-migrate: %v", err)
			}

			// Create Transaction records for foreign key constraints
			for i := uint(1); i <= 3; i++ {
				if err := createTestTransaction(sqliteStore.DB(), i, uint64(i*500)); err != nil {
					t.Fatalf("failed to create transaction %d: %v", i, err)
				}
			}

			stakingKey := []byte("staking_key_inactive_test_123456789012345678")

			// Create an account that was re-registered at slot 2000 (currently active)
			account := models.Account{
				StakingKey: stakingKey,
				AddedSlot:  2000,
				Active:     true,
			}
			if result := sqliteStore.DB().Create(&account); result.Error != nil {
				t.Fatalf("failed to create account: %v", result.Error)
			}

			// Create stake registration certificate at slot 500
			regCert1 := models.Certificate{
				Slot: 500,
				BlockHash: []byte(
					"block_500_1234567890123456789012345678901234",
				),
				CertType:      uint(lcommon.CertificateTypeStakeRegistration),
				TransactionID: 1,
				CertIndex:     0,
			}
			if result := sqliteStore.DB().Create(&regCert1); result.Error != nil {
				t.Fatalf("failed to create regCert1: %v", result.Error)
			}

			stakeReg1 := models.StakeRegistration{
				CertificateID: regCert1.ID,
				StakingKey:    stakingKey,
				AddedSlot:     500,
			}
			if result := sqliteStore.DB().Create(&stakeReg1); result.Error != nil {
				t.Fatalf("failed to create stakeReg1: %v", result.Error)
			}

			// Create stake deregistration certificate at slot 1000 (before rollback point)
			deregCert := models.Certificate{
				Slot: 1000,
				BlockHash: []byte(
					"block_1000_12345678901234567890123456789012",
				),
				CertType:      uint(lcommon.CertificateTypeStakeDeregistration),
				TransactionID: 2,
				CertIndex:     0,
			}
			if result := sqliteStore.DB().Create(&deregCert); result.Error != nil {
				t.Fatalf("failed to create deregCert: %v", result.Error)
			}

			stakeDereg := models.StakeDeregistration{
				CertificateID: deregCert.ID,
				StakingKey:    stakingKey,
				AddedSlot:     1000,
			}
			if result := sqliteStore.DB().Create(&stakeDereg); result.Error != nil {
				t.Fatalf("failed to create stakeDereg: %v", result.Error)
			}

			// Create re-registration certificate at slot 2000 (after rollback point)
			regCert2 := models.Certificate{
				Slot: 2000,
				BlockHash: []byte(
					"block_2000_12345678901234567890123456789012",
				),
				CertType:      uint(lcommon.CertificateTypeStakeRegistration),
				TransactionID: 3,
				CertIndex:     0,
			}
			if result := sqliteStore.DB().Create(&regCert2); result.Error != nil {
				t.Fatalf("failed to create regCert2: %v", result.Error)
			}

			stakeReg2 := models.StakeRegistration{
				CertificateID: regCert2.ID,
				StakingKey:    stakingKey,
				AddedSlot:     2000,
			}
			if result := sqliteStore.DB().Create(&stakeReg2); result.Error != nil {
				t.Fatalf("failed to create stakeReg2: %v", result.Error)
			}

			// Delete certificates after slot 1500 (removes re-registration at slot 2000)
			if err := sqliteStore.DeleteCertificatesAfterSlot(1500, nil); err != nil {
				t.Fatalf("failed to delete certificates: %v", err)
			}

			// Restore account state to slot 1500
			if err := sqliteStore.RestoreAccountStateAtSlot(1500, nil); err != nil {
				t.Fatalf("failed to restore account state: %v", err)
			}

			// Verify account is inactive (deregistration at slot 1000 is more recent than registration at slot 500)
			var restoredAccount models.Account
			if result := sqliteStore.DB().First(&restoredAccount); result.Error != nil {
				t.Fatalf("failed to query restored account: %v", result.Error)
			}
			if restoredAccount.Active {
				t.Error(
					"expected account to be inactive (deregistered before rollback slot)",
				)
			}
		},
	)

	t.Run("account with no prior registration is deleted", func(t *testing.T) {
		sqliteStore, err := New("", nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if err := sqliteStore.Start(); err != nil {
			t.Fatalf("unexpected error starting store: %s", err)
		}
		defer sqliteStore.Close() //nolint:errcheck

		if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
			t.Fatalf("failed to auto-migrate: %v", err)
		}

		// Create Transaction for foreign key constraints
		if err := createTestTransaction(sqliteStore.DB(), 1, 2000); err != nil {
			t.Fatalf("failed to create transaction: %v", err)
		}

		stakingKey := []byte("staking_key_test_1234567890123456789012345678")

		// Create an account registered at slot 2000 (after rollback point)
		account := models.Account{
			StakingKey: stakingKey,
			AddedSlot:  2000,
			Active:     true,
		}
		if result := sqliteStore.DB().Create(&account); result.Error != nil {
			t.Fatalf("failed to create account: %v", result.Error)
		}

		// Create stake registration certificate at slot 2000 (after rollback point)
		regCert := models.Certificate{
			Slot: 2000,
			BlockHash: []byte(
				"block_2000_12345678901234567890123456789012",
			),
			CertType:      uint(lcommon.CertificateTypeStakeRegistration),
			TransactionID: 1,
			CertIndex:     0,
		}
		if result := sqliteStore.DB().Create(&regCert); result.Error != nil {
			t.Fatalf("failed to create regCert: %v", result.Error)
		}

		stakeReg := models.StakeRegistration{
			CertificateID: regCert.ID,
			StakingKey:    stakingKey,
			AddedSlot:     2000,
		}
		if result := sqliteStore.DB().Create(&stakeReg); result.Error != nil {
			t.Fatalf("failed to create stakeReg: %v", result.Error)
		}

		// Delete certificates after slot 1500 (removes registration)
		if err := sqliteStore.DeleteCertificatesAfterSlot(1500, nil); err != nil {
			t.Fatalf("failed to delete certificates: %v", err)
		}

		// Restore account state to slot 1500
		if err := sqliteStore.RestoreAccountStateAtSlot(1500, nil); err != nil {
			t.Fatalf("failed to restore account state: %v", err)
		}

		// Verify account is deleted (no registration before rollback slot)
		var count int64
		sqliteStore.DB().Model(&models.Account{}).Count(&count)
		if count != 0 {
			t.Errorf("expected 0 accounts after rollback, got %d", count)
		}
	})
}

// TestDeletePParamsAfterSlot tests that protocol parameters after a slot are deleted
func TestDeletePParamsAfterSlot(t *testing.T) {
	sqliteStore, err := New("", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := sqliteStore.Start(); err != nil {
		t.Fatalf("unexpected error starting store: %s", err)
	}
	defer sqliteStore.Close() //nolint:errcheck

	// Run auto-migration
	if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
		t.Fatalf("failed to auto-migrate: %v", err)
	}

	// Create pparams at slot 1000
	pparams1 := models.PParams{
		AddedSlot: 1000,
		Epoch:     100,
		Cbor:      []byte("pparams_cbor_1"),
	}
	if result := sqliteStore.DB().Create(&pparams1); result.Error != nil {
		t.Fatalf("failed to create pparams1: %v", result.Error)
	}

	// Create pparams at slot 2000
	pparams2 := models.PParams{
		AddedSlot: 2000,
		Epoch:     101,
		Cbor:      []byte("pparams_cbor_2"),
	}
	if result := sqliteStore.DB().Create(&pparams2); result.Error != nil {
		t.Fatalf("failed to create pparams2: %v", result.Error)
	}

	// Verify we have 2 pparams
	var countBefore int64
	sqliteStore.DB().Model(&models.PParams{}).Count(&countBefore)
	if countBefore != 2 {
		t.Fatalf("expected 2 pparams before rollback, got %d", countBefore)
	}

	// Delete pparams after slot 1500
	if err := sqliteStore.DeletePParamsAfterSlot(1500, nil); err != nil {
		t.Fatalf("failed to delete pparams: %v", err)
	}

	// Verify only 1 remains
	var countAfter int64
	sqliteStore.DB().Model(&models.PParams{}).Count(&countAfter)
	if countAfter != 1 {
		t.Errorf("expected 1 pparams after rollback, got %d", countAfter)
	}
}

// TestDeletePParamUpdatesAfterSlot tests that protocol parameter updates after a slot are deleted
func TestDeletePParamUpdatesAfterSlot(t *testing.T) {
	sqliteStore, err := New("", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := sqliteStore.Start(); err != nil {
		t.Fatalf("unexpected error starting store: %s", err)
	}
	defer sqliteStore.Close() //nolint:errcheck

	// Run auto-migration
	if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
		t.Fatalf("failed to auto-migrate: %v", err)
	}

	// Create pparam update at slot 1000
	update1 := models.PParamUpdate{
		AddedSlot:   1000,
		Epoch:       100,
		GenesisHash: []byte("genesis_hash_1"),
		Cbor:        []byte("update_cbor_1"),
	}
	if result := sqliteStore.DB().Create(&update1); result.Error != nil {
		t.Fatalf("failed to create update1: %v", result.Error)
	}

	// Create pparam update at slot 2000
	update2 := models.PParamUpdate{
		AddedSlot:   2000,
		Epoch:       101,
		GenesisHash: []byte("genesis_hash_2"),
		Cbor:        []byte("update_cbor_2"),
	}
	if result := sqliteStore.DB().Create(&update2); result.Error != nil {
		t.Fatalf("failed to create update2: %v", result.Error)
	}

	// Verify we have 2 updates
	var countBefore int64
	sqliteStore.DB().Model(&models.PParamUpdate{}).Count(&countBefore)
	if countBefore != 2 {
		t.Fatalf(
			"expected 2 pparam updates before rollback, got %d",
			countBefore,
		)
	}

	// Delete updates after slot 1500
	if err := sqliteStore.DeletePParamUpdatesAfterSlot(1500, nil); err != nil {
		t.Fatalf("failed to delete pparam updates: %v", err)
	}

	// Verify only 1 remains
	var countAfter int64
	sqliteStore.DB().Model(&models.PParamUpdate{}).Count(&countAfter)
	if countAfter != 1 {
		t.Errorf("expected 1 pparam update after rollback, got %d", countAfter)
	}
}

// TestDeleteTransactionsAfterSlot tests that transactions and their child records are deleted
func TestDeleteTransactionsAfterSlot(t *testing.T) {
	sqliteStore, err := New("", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := sqliteStore.Start(); err != nil {
		t.Fatalf("unexpected error starting store: %s", err)
	}
	defer sqliteStore.Close() //nolint:errcheck

	// Run auto-migration
	if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
		t.Fatalf("failed to auto-migrate: %v", err)
	}

	// Create transaction at slot 1000 (should be kept)
	tx1 := models.Transaction{
		Hash:       []byte("tx_hash_1_123456789012345678901234567890123456"),
		BlockHash:  []byte("block_1000_12345678901234567890123456789012"),
		Slot:       1000,
		BlockIndex: 0,
	}
	if result := sqliteStore.DB().Create(&tx1); result.Error != nil {
		t.Fatalf("failed to create tx1: %v", result.Error)
	}

	// Create child records for tx1
	kw1 := models.KeyWitness{TransactionID: tx1.ID, Type: 1}
	if result := sqliteStore.DB().Create(&kw1); result.Error != nil {
		t.Fatalf("failed to create key witness for tx1: %v", result.Error)
	}

	// Create transaction at slot 2000 (should be deleted)
	tx2 := models.Transaction{
		Hash:       []byte("tx_hash_2_123456789012345678901234567890123456"),
		BlockHash:  []byte("block_2000_12345678901234567890123456789012"),
		Slot:       2000,
		BlockIndex: 0,
	}
	if result := sqliteStore.DB().Create(&tx2); result.Error != nil {
		t.Fatalf("failed to create tx2: %v", result.Error)
	}

	// Create child records for tx2
	kw2 := models.KeyWitness{TransactionID: tx2.ID, Type: 1}
	if result := sqliteStore.DB().Create(&kw2); result.Error != nil {
		t.Fatalf("failed to create key witness for tx2: %v", result.Error)
	}
	ws2 := models.WitnessScripts{TransactionID: tx2.ID, Type: 1}
	if result := sqliteStore.DB().Create(&ws2); result.Error != nil {
		t.Fatalf("failed to create witness script for tx2: %v", result.Error)
	}
	rd2 := models.Redeemer{TransactionID: tx2.ID}
	if result := sqliteStore.DB().Create(&rd2); result.Error != nil {
		t.Fatalf("failed to create redeemer for tx2: %v", result.Error)
	}
	pd2 := models.PlutusData{TransactionID: tx2.ID}
	if result := sqliteStore.DB().Create(&pd2); result.Error != nil {
		t.Fatalf("failed to create plutus data for tx2: %v", result.Error)
	}

	// Verify we have 2 transactions and their child records
	var txCountBefore int64
	sqliteStore.DB().Model(&models.Transaction{}).Count(&txCountBefore)
	if txCountBefore != 2 {
		t.Fatalf(
			"expected 2 transactions before rollback, got %d",
			txCountBefore,
		)
	}

	var kwCountBefore int64
	sqliteStore.DB().Model(&models.KeyWitness{}).Count(&kwCountBefore)
	if kwCountBefore != 2 {
		t.Fatalf(
			"expected 2 key witnesses before rollback, got %d",
			kwCountBefore,
		)
	}

	// Delete transactions after slot 1500
	if err := sqliteStore.DeleteTransactionsAfterSlot(1500, nil); err != nil {
		t.Fatalf("failed to delete transactions: %v", err)
	}

	// Verify only tx1 remains
	var txCountAfter int64
	sqliteStore.DB().Model(&models.Transaction{}).Count(&txCountAfter)
	if txCountAfter != 1 {
		t.Errorf("expected 1 transaction after rollback, got %d", txCountAfter)
	}

	// Verify only kw1 remains (child record of tx1)
	var kwCountAfter int64
	sqliteStore.DB().Model(&models.KeyWitness{}).Count(&kwCountAfter)
	if kwCountAfter != 1 {
		t.Errorf("expected 1 key witness after rollback, got %d", kwCountAfter)
	}

	// Verify all other child records of tx2 are deleted
	var wsCountAfter int64
	sqliteStore.DB().Model(&models.WitnessScripts{}).Count(&wsCountAfter)
	if wsCountAfter != 0 {
		t.Errorf(
			"expected 0 witness scripts after rollback, got %d",
			wsCountAfter,
		)
	}

	var rdCountAfter int64
	sqliteStore.DB().Model(&models.Redeemer{}).Count(&rdCountAfter)
	if rdCountAfter != 0 {
		t.Errorf("expected 0 redeemers after rollback, got %d", rdCountAfter)
	}

	var pdCountAfter int64
	sqliteStore.DB().Model(&models.PlutusData{}).Count(&pdCountAfter)
	if pdCountAfter != 0 {
		t.Errorf("expected 0 plutus data after rollback, got %d", pdCountAfter)
	}
}

// TestRestorePoolStateAtSlot tests that pool state is correctly restored during rollback
func TestRestorePoolStateAtSlot(t *testing.T) {
	t.Run("pool with no prior registrations is deleted", func(t *testing.T) {
		sqliteStore, err := New("", nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if err := sqliteStore.Start(); err != nil {
			t.Fatalf("unexpected error starting store: %s", err)
		}
		defer sqliteStore.Close() //nolint:errcheck

		if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
			t.Fatalf("failed to auto-migrate: %v", err)
		}

		poolHash := []byte("pool_key_hash_1234567890123456789012345678")

		// Create a pool with registration after rollback point
		pool := models.Pool{PoolKeyHash: poolHash}
		if result := sqliteStore.DB().Create(&pool); result.Error != nil {
			t.Fatalf("failed to create pool: %v", result.Error)
		}

		poolReg := models.PoolRegistration{
			PoolID:      pool.ID,
			PoolKeyHash: poolHash,
			AddedSlot:   2000,
		}
		if result := sqliteStore.DB().Create(&poolReg); result.Error != nil {
			t.Fatalf("failed to create pool registration: %v", result.Error)
		}

		// Delete certificates and restore state
		if err := sqliteStore.DeleteCertificatesAfterSlot(1500, nil); err != nil {
			t.Fatalf("failed to delete certificates: %v", err)
		}
		if err := sqliteStore.RestorePoolStateAtSlot(1500, nil); err != nil {
			t.Fatalf("failed to restore pool state: %v", err)
		}

		// Pool should be deleted
		var count int64
		sqliteStore.DB().Model(&models.Pool{}).Count(&count)
		if count != 0 {
			t.Errorf("expected 0 pools after rollback, got %d", count)
		}
	})

	t.Run("pool with prior registration is kept", func(t *testing.T) {
		sqliteStore, err := New("", nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if err := sqliteStore.Start(); err != nil {
			t.Fatalf("unexpected error starting store: %s", err)
		}
		defer sqliteStore.Close() //nolint:errcheck

		if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
			t.Fatalf("failed to auto-migrate: %v", err)
		}

		poolHash := []byte("pool_key_hash_1234567890123456789012345678")

		// Create a pool with registration BEFORE rollback point
		pool := models.Pool{PoolKeyHash: poolHash}
		if result := sqliteStore.DB().Create(&pool); result.Error != nil {
			t.Fatalf("failed to create pool: %v", result.Error)
		}

		// Registration at slot 1000 (before rollback)
		poolReg1 := models.PoolRegistration{
			PoolID:      pool.ID,
			PoolKeyHash: poolHash,
			AddedSlot:   1000,
		}
		if result := sqliteStore.DB().Create(&poolReg1); result.Error != nil {
			t.Fatalf("failed to create pool registration 1: %v", result.Error)
		}

		// Registration at slot 2000 (after rollback)
		poolReg2 := models.PoolRegistration{
			PoolID:      pool.ID,
			PoolKeyHash: poolHash,
			AddedSlot:   2000,
		}
		if result := sqliteStore.DB().Create(&poolReg2); result.Error != nil {
			t.Fatalf("failed to create pool registration 2: %v", result.Error)
		}

		// Delete certificates and restore state
		if err := sqliteStore.DeleteCertificatesAfterSlot(1500, nil); err != nil {
			t.Fatalf("failed to delete certificates: %v", err)
		}
		if err := sqliteStore.RestorePoolStateAtSlot(1500, nil); err != nil {
			t.Fatalf("failed to restore pool state: %v", err)
		}

		// Pool should still exist (has registration before rollback)
		var count int64
		sqliteStore.DB().Model(&models.Pool{}).Count(&count)
		if count != 1 {
			t.Errorf("expected 1 pool after rollback, got %d", count)
		}

		// Only one registration should remain
		var regCount int64
		sqliteStore.DB().Model(&models.PoolRegistration{}).Count(&regCount)
		if regCount != 1 {
			t.Errorf(
				"expected 1 pool registration after rollback, got %d",
				regCount,
			)
		}
	})

	t.Run("pool with retirement after rollback has retirement undone", func(t *testing.T) {
		sqliteStore, err := New("", nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if err := sqliteStore.Start(); err != nil {
			t.Fatalf("unexpected error starting store: %s", err)
		}
		defer sqliteStore.Close() //nolint:errcheck

		if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
			t.Fatalf("failed to auto-migrate: %v", err)
		}

		poolHash := []byte("pool_key_hash_1234567890123456789012345678")

		// Create a pool with registration BEFORE rollback point
		pool := models.Pool{PoolKeyHash: poolHash}
		if result := sqliteStore.DB().Create(&pool); result.Error != nil {
			t.Fatalf("failed to create pool: %v", result.Error)
		}

		// Registration at slot 1000 (before rollback)
		poolReg := models.PoolRegistration{
			PoolID:      pool.ID,
			PoolKeyHash: poolHash,
			AddedSlot:   1000,
		}
		if result := sqliteStore.DB().Create(&poolReg); result.Error != nil {
			t.Fatalf("failed to create pool registration: %v", result.Error)
		}

		// Retirement at slot 2000 (after rollback)
		poolRetirement := models.PoolRetirement{
			PoolID:      pool.ID,
			PoolKeyHash: poolHash,
			AddedSlot:   2000,
			Epoch:       100,
		}
		if result := sqliteStore.DB().Create(&poolRetirement); result.Error != nil {
			t.Fatalf("failed to create pool retirement: %v", result.Error)
		}

		// Verify pool has retirement before rollback
		var retirementCountBefore int64
		sqliteStore.DB().Model(&models.PoolRetirement{}).Count(&retirementCountBefore)
		if retirementCountBefore != 1 {
			t.Fatalf(
				"expected 1 retirement before rollback, got %d",
				retirementCountBefore,
			)
		}

		// Delete certificates and restore state
		if err := sqliteStore.DeleteCertificatesAfterSlot(1500, nil); err != nil {
			t.Fatalf("failed to delete certificates: %v", err)
		}
		if err := sqliteStore.RestorePoolStateAtSlot(1500, nil); err != nil {
			t.Fatalf("failed to restore pool state: %v", err)
		}

		// Pool should still exist
		var count int64
		sqliteStore.DB().Model(&models.Pool{}).Count(&count)
		if count != 1 {
			t.Errorf("expected 1 pool after rollback, got %d", count)
		}

		// Retirement should be removed (CASCADE from DeleteCertificatesAfterSlot)
		var retirementCountAfter int64
		sqliteStore.DB().Model(&models.PoolRetirement{}).Count(&retirementCountAfter)
		if retirementCountAfter != 0 {
			t.Errorf(
				"expected 0 retirements after rollback, got %d",
				retirementCountAfter,
			)
		}

		// Registration should still exist
		var regCount int64
		sqliteStore.DB().Model(&models.PoolRegistration{}).Count(&regCount)
		if regCount != 1 {
			t.Errorf(
				"expected 1 pool registration after rollback, got %d",
				regCount,
			)
		}
	})
}

// TestRestoreDrepStateAtSlot tests that DRep state is correctly restored during rollback
func TestRestoreDrepStateAtSlot(t *testing.T) {
	t.Run("DRep with no prior registrations is deleted", func(t *testing.T) {
		sqliteStore, err := New("", nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if err := sqliteStore.Start(); err != nil {
			t.Fatalf("unexpected error starting store: %s", err)
		}
		defer sqliteStore.Close() //nolint:errcheck

		if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
			t.Fatalf("failed to auto-migrate: %v", err)
		}

		drepCredential := []byte("drep_credential_12345678901234567890123456")

		// Create a DRep registered at slot 2000 (after rollback point)
		drep := models.Drep{
			Credential: drepCredential,
			Active:     true,
			AddedSlot:  2000,
		}
		if result := sqliteStore.DB().Create(&drep); result.Error != nil {
			t.Fatalf("failed to create drep: %v", result.Error)
		}

		// Delete certificates and restore state
		if err := sqliteStore.DeleteCertificatesAfterSlot(1500, nil); err != nil {
			t.Fatalf("failed to delete certificates: %v", err)
		}
		if err := sqliteStore.RestoreDrepStateAtSlot(1500, nil); err != nil {
			t.Fatalf("failed to restore DRep state: %v", err)
		}

		// DRep should be deleted
		var count int64
		sqliteStore.DB().Model(&models.Drep{}).Count(&count)
		if count != 0 {
			t.Errorf("expected 0 DReps after rollback, got %d", count)
		}
	})

	t.Run(
		"DRep with prior registration has state restored",
		func(t *testing.T) {
			sqliteStore, err := New("", nil, nil)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if err := sqliteStore.Start(); err != nil {
				t.Fatalf("unexpected error starting store: %s", err)
			}
			defer sqliteStore.Close() //nolint:errcheck

			if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
				t.Fatalf("failed to auto-migrate: %v", err)
			}

			drepCredential := []byte(
				"drep_credential_12345678901234567890123456",
			)
			anchorUrl1 := "https://example.com/drep1"
			anchorHash1 := []byte("anchor_hash_1_1234567890123456789012345678")
			anchorUrl2 := "https://example.com/drep2"
			anchorHash2 := []byte("anchor_hash_2_1234567890123456789012345678")

			// Create a DRep with current state at slot 2000
			drep := models.Drep{
				Credential: drepCredential,
				Active:     true,
				AnchorUrl:  anchorUrl2,
				AnchorHash: anchorHash2,
				AddedSlot:  2000,
			}
			if result := sqliteStore.DB().Create(&drep); result.Error != nil {
				t.Fatalf("failed to create drep: %v", result.Error)
			}

			// Create Transaction and Certificate records for proper JOIN support
			tx1 := models.Transaction{Hash: []byte("tx_hash_1000"), Slot: 1000}
			if result := sqliteStore.DB().Create(&tx1); result.Error != nil {
				t.Fatalf("failed to create tx1: %v", result.Error)
			}
			cert1 := models.Certificate{
				Slot:          1000,
				CertIndex:     0,
				TransactionID: tx1.ID,
			}
			if result := sqliteStore.DB().Create(&cert1); result.Error != nil {
				t.Fatalf("failed to create cert1: %v", result.Error)
			}
			tx2 := models.Transaction{Hash: []byte("tx_hash_2000"), Slot: 2000}
			if result := sqliteStore.DB().Create(&tx2); result.Error != nil {
				t.Fatalf("failed to create tx2: %v", result.Error)
			}
			cert2 := models.Certificate{
				Slot:          2000,
				CertIndex:     0,
				TransactionID: tx2.ID,
			}
			if result := sqliteStore.DB().Create(&cert2); result.Error != nil {
				t.Fatalf("failed to create cert2: %v", result.Error)
			}

			// Registration at slot 1000 (before rollback) with original anchor data
			reg1 := models.RegistrationDrep{
				DrepCredential: drepCredential,
				AnchorUrl:      anchorUrl1,
				AnchorHash:     anchorHash1,
				AddedSlot:      1000,
				CertificateID:  cert1.ID,
			}
			if result := sqliteStore.DB().Create(&reg1); result.Error != nil {
				t.Fatalf("failed to create registration 1: %v", result.Error)
			}

			// Registration at slot 2000 (after rollback) with new anchor data
			reg2 := models.RegistrationDrep{
				DrepCredential: drepCredential,
				AnchorUrl:      anchorUrl2,
				AnchorHash:     anchorHash2,
				AddedSlot:      2000,
				CertificateID:  cert2.ID,
			}
			if result := sqliteStore.DB().Create(&reg2); result.Error != nil {
				t.Fatalf("failed to create registration 2: %v", result.Error)
			}

			// Delete certificates and restore state
			if err := sqliteStore.DeleteCertificatesAfterSlot(1500, nil); err != nil {
				t.Fatalf("failed to delete certificates: %v", err)
			}
			if err := sqliteStore.RestoreDrepStateAtSlot(1500, nil); err != nil {
				t.Fatalf("failed to restore DRep state: %v", err)
			}

			// DRep should still exist with original anchor data
			var restoredDrep models.Drep
			if result := sqliteStore.DB().First(&restoredDrep); result.Error != nil {
				t.Fatalf("failed to query restored DRep: %v", result.Error)
			}
			if restoredDrep.AnchorUrl != anchorUrl1 {
				t.Errorf(
					"expected anchor URL %s, got %s",
					anchorUrl1,
					restoredDrep.AnchorUrl,
				)
			}
			if string(restoredDrep.AnchorHash) != string(anchorHash1) {
				t.Errorf("expected anchor hash to be restored")
			}
			if !restoredDrep.Active {
				t.Errorf("expected DRep to be active")
			}
		},
	)

	t.Run("DRep deregistered before rollback is inactive", func(t *testing.T) {
		sqliteStore, err := New("", nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if err := sqliteStore.Start(); err != nil {
			t.Fatalf("unexpected error starting store: %s", err)
		}
		defer sqliteStore.Close() //nolint:errcheck

		if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
			t.Fatalf("failed to auto-migrate: %v", err)
		}

		drepCredential := []byte("drep_credential_12345678901234567890123456")

		// DRep is currently active (re-registered at slot 2000)
		drep := models.Drep{
			Credential: drepCredential,
			Active:     true,
			AddedSlot:  2000,
		}
		if result := sqliteStore.DB().Create(&drep); result.Error != nil {
			t.Fatalf("failed to create drep: %v", result.Error)
		}

		// Create Transaction and Certificate records for proper JOIN support
		txReg := models.Transaction{Hash: []byte("tx_hash_500"), Slot: 500}
		if result := sqliteStore.DB().Create(&txReg); result.Error != nil {
			t.Fatalf("failed to create txReg: %v", result.Error)
		}
		certReg := models.Certificate{
			Slot:          500,
			CertIndex:     0,
			TransactionID: txReg.ID,
		}
		if result := sqliteStore.DB().Create(&certReg); result.Error != nil {
			t.Fatalf("failed to create certReg: %v", result.Error)
		}
		txDereg := models.Transaction{Hash: []byte("tx_hash_1000"), Slot: 1000}
		if result := sqliteStore.DB().Create(&txDereg); result.Error != nil {
			t.Fatalf("failed to create txDereg: %v", result.Error)
		}
		certDereg := models.Certificate{
			Slot:          1000,
			CertIndex:     0,
			TransactionID: txDereg.ID,
		}
		if result := sqliteStore.DB().Create(&certDereg); result.Error != nil {
			t.Fatalf("failed to create certDereg: %v", result.Error)
		}
		txReg2 := models.Transaction{Hash: []byte("tx_hash_2000"), Slot: 2000}
		if result := sqliteStore.DB().Create(&txReg2); result.Error != nil {
			t.Fatalf("failed to create txReg2: %v", result.Error)
		}
		certReg2 := models.Certificate{
			Slot:          2000,
			CertIndex:     0,
			TransactionID: txReg2.ID,
		}
		if result := sqliteStore.DB().Create(&certReg2); result.Error != nil {
			t.Fatalf("failed to create certReg2: %v", result.Error)
		}

		// Registration at slot 500
		reg := models.RegistrationDrep{
			DrepCredential: drepCredential,
			AddedSlot:      500,
			CertificateID:  certReg.ID,
		}
		if result := sqliteStore.DB().Create(&reg); result.Error != nil {
			t.Fatalf("failed to create registration: %v", result.Error)
		}

		// Deregistration at slot 1000 (before rollback)
		dereg := models.DeregistrationDrep{
			DrepCredential: drepCredential,
			AddedSlot:      1000,
			CertificateID:  certDereg.ID,
		}
		if result := sqliteStore.DB().Create(&dereg); result.Error != nil {
			t.Fatalf("failed to create deregistration: %v", result.Error)
		}

		// Re-registration at slot 2000 (after rollback)
		reg2 := models.RegistrationDrep{
			DrepCredential: drepCredential,
			AddedSlot:      2000,
			CertificateID:  certReg2.ID,
		}
		if result := sqliteStore.DB().Create(&reg2); result.Error != nil {
			t.Fatalf("failed to create re-registration: %v", result.Error)
		}

		// Delete certificates and restore state
		if err := sqliteStore.DeleteCertificatesAfterSlot(1500, nil); err != nil {
			t.Fatalf("failed to delete certificates: %v", err)
		}
		if err := sqliteStore.RestoreDrepStateAtSlot(1500, nil); err != nil {
			t.Fatalf("failed to restore DRep state: %v", err)
		}

		// DRep should exist but be inactive (deregistered at slot 1000)
		var restoredDrep models.Drep
		if result := sqliteStore.DB().First(&restoredDrep); result.Error != nil {
			t.Fatalf("failed to query restored DRep: %v", result.Error)
		}
		if restoredDrep.Active {
			t.Errorf(
				"expected DRep to be inactive after rollback (was deregistered at slot 1000)",
			)
		}
	})

	t.Run(
		"DRep with update certificate has anchor restored",
		func(t *testing.T) {
			sqliteStore, err := New("", nil, nil)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if err := sqliteStore.Start(); err != nil {
				t.Fatalf("unexpected error starting store: %s", err)
			}
			defer sqliteStore.Close() //nolint:errcheck

			if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
				t.Fatalf("failed to auto-migrate: %v", err)
			}

			drepCredential := []byte(
				"drep_credential_12345678901234567890123456",
			)
			anchorUrl1 := "https://example.com/drep_initial"
			anchorHash1 := []byte("anchor_hash_initial_123456789012345678901")
			anchorUrl2 := "https://example.com/drep_updated"
			anchorHash2 := []byte("anchor_hash_updated_123456789012345678901")
			anchorUrl3 := "https://example.com/drep_final"
			anchorHash3 := []byte("anchor_hash_final_12345678901234567890123")

			// DRep currently has final anchor data
			drep := models.Drep{
				Credential: drepCredential,
				Active:     true,
				AnchorUrl:  anchorUrl3,
				AnchorHash: anchorHash3,
				AddedSlot:  2000,
			}
			if result := sqliteStore.DB().Create(&drep); result.Error != nil {
				t.Fatalf("failed to create drep: %v", result.Error)
			}

			// Create Transaction and Certificate records for proper JOIN support
			txReg := models.Transaction{Hash: []byte("tx_hash_500"), Slot: 500}
			if result := sqliteStore.DB().Create(&txReg); result.Error != nil {
				t.Fatalf("failed to create txReg: %v", result.Error)
			}
			certReg := models.Certificate{
				Slot:          500,
				CertIndex:     0,
				TransactionID: txReg.ID,
			}
			if result := sqliteStore.DB().Create(&certReg); result.Error != nil {
				t.Fatalf("failed to create certReg: %v", result.Error)
			}
			txUpdate1 := models.Transaction{
				Hash: []byte("tx_hash_1000"),
				Slot: 1000,
			}
			if result := sqliteStore.DB().Create(&txUpdate1); result.Error != nil {
				t.Fatalf("failed to create txUpdate1: %v", result.Error)
			}
			certUpdate1 := models.Certificate{
				Slot:          1000,
				CertIndex:     0,
				TransactionID: txUpdate1.ID,
			}
			if result := sqliteStore.DB().Create(&certUpdate1); result.Error != nil {
				t.Fatalf("failed to create certUpdate1: %v", result.Error)
			}
			txUpdate2 := models.Transaction{
				Hash: []byte("tx_hash_2000"),
				Slot: 2000,
			}
			if result := sqliteStore.DB().Create(&txUpdate2); result.Error != nil {
				t.Fatalf("failed to create txUpdate2: %v", result.Error)
			}
			certUpdate2 := models.Certificate{
				Slot:          2000,
				CertIndex:     0,
				TransactionID: txUpdate2.ID,
			}
			if result := sqliteStore.DB().Create(&certUpdate2); result.Error != nil {
				t.Fatalf("failed to create certUpdate2: %v", result.Error)
			}

			// Registration at slot 500
			reg := models.RegistrationDrep{
				DrepCredential: drepCredential,
				AnchorUrl:      anchorUrl1,
				AnchorHash:     anchorHash1,
				AddedSlot:      500,
				CertificateID:  certReg.ID,
			}
			if result := sqliteStore.DB().Create(&reg); result.Error != nil {
				t.Fatalf("failed to create registration: %v", result.Error)
			}

			// Update at slot 1000 (before rollback)
			update := models.UpdateDrep{
				Credential:    drepCredential,
				AnchorUrl:     anchorUrl2,
				AnchorHash:    anchorHash2,
				AddedSlot:     1000,
				CertificateID: certUpdate1.ID,
			}
			if result := sqliteStore.DB().Create(&update); result.Error != nil {
				t.Fatalf("failed to create update: %v", result.Error)
			}

			// Update at slot 2000 (after rollback)
			update2 := models.UpdateDrep{
				Credential:    drepCredential,
				AnchorUrl:     anchorUrl3,
				AnchorHash:    anchorHash3,
				AddedSlot:     2000,
				CertificateID: certUpdate2.ID,
			}
			if result := sqliteStore.DB().Create(&update2); result.Error != nil {
				t.Fatalf("failed to create update 2: %v", result.Error)
			}

			// Delete certificates and restore state
			if err := sqliteStore.DeleteCertificatesAfterSlot(1500, nil); err != nil {
				t.Fatalf("failed to delete certificates: %v", err)
			}
			if err := sqliteStore.RestoreDrepStateAtSlot(1500, nil); err != nil {
				t.Fatalf("failed to restore DRep state: %v", err)
			}

			// DRep should have anchor data from slot 1000 update
			var restoredDrep models.Drep
			if result := sqliteStore.DB().First(&restoredDrep); result.Error != nil {
				t.Fatalf("failed to query restored DRep: %v", result.Error)
			}
			if restoredDrep.AnchorUrl != anchorUrl2 {
				t.Errorf(
					"expected anchor URL %s, got %s",
					anchorUrl2,
					restoredDrep.AnchorUrl,
				)
			}
			if string(restoredDrep.AnchorHash) != string(anchorHash2) {
				t.Errorf("expected anchor hash from update at slot 1000")
			}
			if !restoredDrep.Active {
				t.Errorf("expected DRep to be active")
			}
		},
	)

	t.Run(
		"DRep with update after deregistration stays inactive",
		func(t *testing.T) {
			// This test verifies that an update certificate does NOT reactivate
			// a deregistered DRep. Per CIP-1694, update certificates only modify
			// anchor data and cannot change the active status.
			sqliteStore, err := New("", nil, nil)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if err := sqliteStore.Start(); err != nil {
				t.Fatalf("unexpected error starting store: %s", err)
			}
			defer sqliteStore.Close() //nolint:errcheck

			if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
				t.Fatalf("failed to auto-migrate: %v", err)
			}

			drepCredential := []byte(
				"drep_credential_12345678901234567890123456",
			)
			anchorUrl1 := "https://example.com/drep_reg"
			anchorHash1 := []byte("anchor_hash_reg_123456789012345678901234")
			anchorUrl2 := "https://example.com/drep_update"
			anchorHash2 := []byte("anchor_hash_update_1234567890123456789012")

			// DRep currently shows as active at slot 2000 (after rollback point)
			drep := models.Drep{
				Credential: drepCredential,
				Active:     true,
				AnchorUrl:  anchorUrl2,
				AnchorHash: anchorHash2,
				AddedSlot:  2000,
			}
			if result := sqliteStore.DB().Create(&drep); result.Error != nil {
				t.Fatalf("failed to create drep: %v", result.Error)
			}

			// Create Transaction and Certificate records
			txReg := models.Transaction{Hash: []byte("tx_hash_500"), Slot: 500}
			if result := sqliteStore.DB().Create(&txReg); result.Error != nil {
				t.Fatalf("failed to create txReg: %v", result.Error)
			}
			certReg := models.Certificate{
				Slot:          500,
				CertIndex:     0,
				TransactionID: txReg.ID,
			}
			if result := sqliteStore.DB().Create(&certReg); result.Error != nil {
				t.Fatalf("failed to create certReg: %v", result.Error)
			}

			txDereg := models.Transaction{Hash: []byte("tx_hash_1000"), Slot: 1000}
			if result := sqliteStore.DB().Create(&txDereg); result.Error != nil {
				t.Fatalf("failed to create txDereg: %v", result.Error)
			}
			certDereg := models.Certificate{
				Slot:          1000,
				CertIndex:     0,
				TransactionID: txDereg.ID,
			}
			if result := sqliteStore.DB().Create(&certDereg); result.Error != nil {
				t.Fatalf("failed to create certDereg: %v", result.Error)
			}

			txUpdate := models.Transaction{Hash: []byte("tx_hash_1200"), Slot: 1200}
			if result := sqliteStore.DB().Create(&txUpdate); result.Error != nil {
				t.Fatalf("failed to create txUpdate: %v", result.Error)
			}
			certUpdate := models.Certificate{
				Slot:          1200,
				CertIndex:     0,
				TransactionID: txUpdate.ID,
			}
			if result := sqliteStore.DB().Create(&certUpdate); result.Error != nil {
				t.Fatalf("failed to create certUpdate: %v", result.Error)
			}

			// Registration at slot 500
			reg := models.RegistrationDrep{
				DrepCredential: drepCredential,
				AnchorUrl:      anchorUrl1,
				AnchorHash:     anchorHash1,
				AddedSlot:      500,
				CertificateID:  certReg.ID,
			}
			if result := sqliteStore.DB().Create(&reg); result.Error != nil {
				t.Fatalf("failed to create registration: %v", result.Error)
			}

			// Deregistration at slot 1000
			dereg := models.DeregistrationDrep{
				DrepCredential: drepCredential,
				AddedSlot:      1000,
				CertificateID:  certDereg.ID,
			}
			if result := sqliteStore.DB().Create(&dereg); result.Error != nil {
				t.Fatalf("failed to create deregistration: %v", result.Error)
			}

			// Update at slot 1200 (AFTER deregistration - should be ignored per protocol)
			update := models.UpdateDrep{
				Credential:    drepCredential,
				AnchorUrl:     anchorUrl2,
				AnchorHash:    anchorHash2,
				AddedSlot:     1200,
				CertificateID: certUpdate.ID,
			}
			if result := sqliteStore.DB().Create(&update); result.Error != nil {
				t.Fatalf("failed to create update: %v", result.Error)
			}

			// Rollback to slot 1500 (all certs are within the rollback window)
			if err := sqliteStore.RestoreDrepStateAtSlot(1500, nil); err != nil {
				t.Fatalf("failed to restore DRep state: %v", err)
			}

			// DRep should be INACTIVE because:
			// - Registered at 500 (active)
			// - Deregistered at 1000 (inactive)
			// - Update at 1200 should NOT reactivate (update only changes anchor, not status)
			var restoredDrep models.Drep
			if result := sqliteStore.DB().First(&restoredDrep); result.Error != nil {
				t.Fatalf("failed to query restored DRep: %v", result.Error)
			}
			if restoredDrep.Active {
				t.Errorf(
					"expected DRep to be inactive (update after deregistration should not reactivate)",
				)
			}
		},
	)

	t.Run(
		"DRep update as latest event does not reactivate deregistered DRep",
		func(t *testing.T) {
			// This test verifies the specific bug scenario where the update
			// certificate is the chronologically latest event. The DRep should
			// remain inactive because updates cannot change active status.
			//
			// Scenario: Reg(500) -> Dereg(600) -> Update(700) (update is latest)
			sqliteStore, err := New("", nil, nil)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if err := sqliteStore.Start(); err != nil {
				t.Fatalf("unexpected error starting store: %s", err)
			}
			defer sqliteStore.Close() //nolint:errcheck

			if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
				t.Fatalf("failed to auto-migrate: %v", err)
			}

			drepCredential := []byte(
				"drep_credential_12345678901234567890123456",
			)
			anchorUrl1 := "https://example.com/drep_reg"
			anchorHash1 := []byte("anchor_hash_reg_123456789012345678901234")
			anchorUrl2 := "https://example.com/drep_update"
			anchorHash2 := []byte("anchor_hash_update_1234567890123456789012")

			// DRep currently shows as active at slot 2000 (after rollback point)
			drep := models.Drep{
				Credential: drepCredential,
				Active:     true,
				AnchorUrl:  anchorUrl2,
				AnchorHash: anchorHash2,
				AddedSlot:  2000,
			}
			if result := sqliteStore.DB().Create(&drep); result.Error != nil {
				t.Fatalf("failed to create drep: %v", result.Error)
			}

			// Create Transaction and Certificate records
			txReg := models.Transaction{Hash: []byte("tx_hash_500"), Slot: 500}
			if result := sqliteStore.DB().Create(&txReg); result.Error != nil {
				t.Fatalf("failed to create txReg: %v", result.Error)
			}
			certReg := models.Certificate{
				Slot:          500,
				CertIndex:     0,
				TransactionID: txReg.ID,
			}
			if result := sqliteStore.DB().Create(&certReg); result.Error != nil {
				t.Fatalf("failed to create certReg: %v", result.Error)
			}

			txDereg := models.Transaction{Hash: []byte("tx_hash_600"), Slot: 600}
			if result := sqliteStore.DB().Create(&txDereg); result.Error != nil {
				t.Fatalf("failed to create txDereg: %v", result.Error)
			}
			certDereg := models.Certificate{
				Slot:          600,
				CertIndex:     0,
				TransactionID: txDereg.ID,
			}
			if result := sqliteStore.DB().Create(&certDereg); result.Error != nil {
				t.Fatalf("failed to create certDereg: %v", result.Error)
			}

			// Update is at slot 700 - the latest event chronologically
			txUpdate := models.Transaction{Hash: []byte("tx_hash_700"), Slot: 700}
			if result := sqliteStore.DB().Create(&txUpdate); result.Error != nil {
				t.Fatalf("failed to create txUpdate: %v", result.Error)
			}
			certUpdate := models.Certificate{
				Slot:          700,
				CertIndex:     0,
				TransactionID: txUpdate.ID,
			}
			if result := sqliteStore.DB().Create(&certUpdate); result.Error != nil {
				t.Fatalf("failed to create certUpdate: %v", result.Error)
			}

			// Registration at slot 500
			reg := models.RegistrationDrep{
				DrepCredential: drepCredential,
				AnchorUrl:      anchorUrl1,
				AnchorHash:     anchorHash1,
				AddedSlot:      500,
				CertificateID:  certReg.ID,
			}
			if result := sqliteStore.DB().Create(&reg); result.Error != nil {
				t.Fatalf("failed to create registration: %v", result.Error)
			}

			// Deregistration at slot 600
			dereg := models.DeregistrationDrep{
				DrepCredential: drepCredential,
				AddedSlot:      600,
				CertificateID:  certDereg.ID,
			}
			if result := sqliteStore.DB().Create(&dereg); result.Error != nil {
				t.Fatalf("failed to create deregistration: %v", result.Error)
			}

			// Update at slot 700 - the latest event (AFTER deregistration)
			update := models.UpdateDrep{
				Credential:    drepCredential,
				AnchorUrl:     anchorUrl2,
				AnchorHash:    anchorHash2,
				AddedSlot:     700,
				CertificateID: certUpdate.ID,
			}
			if result := sqliteStore.DB().Create(&update); result.Error != nil {
				t.Fatalf("failed to create update: %v", result.Error)
			}

			// Rollback to slot 1500 (all certs are within the rollback window)
			if err := sqliteStore.RestoreDrepStateAtSlot(1500, nil); err != nil {
				t.Fatalf("failed to restore DRep state: %v", err)
			}

			// DRep should be INACTIVE because:
			// - Registered at 500 (active)
			// - Deregistered at 600 (inactive)
			// - Update at 700 should NOT reactivate (update only changes anchor, not status)
			var restoredDrep models.Drep
			if result := sqliteStore.DB().First(&restoredDrep); result.Error != nil {
				t.Fatalf("failed to query restored DRep: %v", result.Error)
			}
			if restoredDrep.Active {
				t.Errorf(
					"expected DRep to be inactive (update as latest event should not reactivate)",
				)
			}
		},
	)
}

// TestPoolCascadeDelete tests that Pool deletion cascades correctly to child records
func TestPoolCascadeDelete(t *testing.T) {
	t.Run(
		"pool deletion cascades to registrations, owners, relays",
		func(t *testing.T) {
			sqliteStore, err := New("", nil, nil)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if err := sqliteStore.Start(); err != nil {
				t.Fatalf("unexpected error starting store: %s", err)
			}
			defer sqliteStore.Close() //nolint:errcheck

			if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
				t.Fatalf("failed to auto-migrate: %v", err)
			}

			poolHash := []byte("pool_key_hash_1234567890123456789012345678")

			// Create a pool
			pool := models.Pool{PoolKeyHash: poolHash}
			if result := sqliteStore.DB().Create(&pool); result.Error != nil {
				t.Fatalf("failed to create pool: %v", result.Error)
			}

			// Create a registration with owners and relays
			poolReg := models.PoolRegistration{
				PoolID:      pool.ID,
				PoolKeyHash: poolHash,
				AddedSlot:   1000,
				Owners: []models.PoolRegistrationOwner{
					{KeyHash: []byte("owner1"), PoolID: pool.ID},
					{KeyHash: []byte("owner2"), PoolID: pool.ID},
				},
				Relays: []models.PoolRegistrationRelay{
					{Hostname: "relay1.example.com", PoolID: pool.ID},
				},
			}
			if result := sqliteStore.DB().Create(&poolReg); result.Error != nil {
				t.Fatalf("failed to create pool registration: %v", result.Error)
			}

			// Verify records exist
			var regCount, ownerCount, relayCount int64
			sqliteStore.DB().Model(&models.PoolRegistration{}).Count(&regCount)
			sqliteStore.DB().
				Model(&models.PoolRegistrationOwner{}).
				Count(&ownerCount)
			sqliteStore.DB().
				Model(&models.PoolRegistrationRelay{}).
				Count(&relayCount)
			if regCount != 1 || ownerCount != 2 || relayCount != 1 {
				t.Fatalf(
					"expected 1 reg, 2 owners, 1 relay; got %d, %d, %d",
					regCount,
					ownerCount,
					relayCount,
				)
			}

			// Delete the pool
			if result := sqliteStore.DB().Delete(&pool); result.Error != nil {
				t.Fatalf("failed to delete pool: %v", result.Error)
			}

			// All related records should be deleted via cascade
			sqliteStore.DB().Model(&models.PoolRegistration{}).Count(&regCount)
			sqliteStore.DB().
				Model(&models.PoolRegistrationOwner{}).
				Count(&ownerCount)
			sqliteStore.DB().
				Model(&models.PoolRegistrationRelay{}).
				Count(&relayCount)
			if regCount != 0 {
				t.Errorf(
					"expected 0 registrations after pool delete, got %d",
					regCount,
				)
			}
			if ownerCount != 0 {
				t.Errorf(
					"expected 0 owners after pool delete, got %d",
					ownerCount,
				)
			}
			if relayCount != 0 {
				t.Errorf(
					"expected 0 relays after pool delete, got %d",
					relayCount,
				)
			}
		},
	)

	t.Run(
		"registration deletion cascades to its owners/relays only",
		func(t *testing.T) {
			sqliteStore, err := New("", nil, nil)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if err := sqliteStore.Start(); err != nil {
				t.Fatalf("unexpected error starting store: %s", err)
			}
			defer sqliteStore.Close() //nolint:errcheck

			if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
				t.Fatalf("failed to auto-migrate: %v", err)
			}

			poolHash := []byte("pool_key_hash_1234567890123456789012345678")

			// Create a pool
			pool := models.Pool{PoolKeyHash: poolHash}
			if result := sqliteStore.DB().Create(&pool); result.Error != nil {
				t.Fatalf("failed to create pool: %v", result.Error)
			}

			// Create first registration with owners and relays
			poolReg1 := models.PoolRegistration{
				PoolID:      pool.ID,
				PoolKeyHash: poolHash,
				AddedSlot:   1000,
				Owners: []models.PoolRegistrationOwner{
					{KeyHash: []byte("owner1_reg1"), PoolID: pool.ID},
				},
				Relays: []models.PoolRegistrationRelay{
					{Hostname: "relay1_reg1.example.com", PoolID: pool.ID},
				},
			}
			if result := sqliteStore.DB().Create(&poolReg1); result.Error != nil {
				t.Fatalf(
					"failed to create pool registration 1: %v",
					result.Error,
				)
			}

			// Create second registration with different owners and relays
			poolReg2 := models.PoolRegistration{
				PoolID:      pool.ID,
				PoolKeyHash: poolHash,
				AddedSlot:   2000,
				Owners: []models.PoolRegistrationOwner{
					{KeyHash: []byte("owner1_reg2"), PoolID: pool.ID},
					{KeyHash: []byte("owner2_reg2"), PoolID: pool.ID},
				},
				Relays: []models.PoolRegistrationRelay{
					{Hostname: "relay1_reg2.example.com", PoolID: pool.ID},
				},
			}
			if result := sqliteStore.DB().Create(&poolReg2); result.Error != nil {
				t.Fatalf(
					"failed to create pool registration 2: %v",
					result.Error,
				)
			}

			// Verify initial counts
			var regCount, ownerCount, relayCount int64
			sqliteStore.DB().Model(&models.PoolRegistration{}).Count(&regCount)
			sqliteStore.DB().
				Model(&models.PoolRegistrationOwner{}).
				Count(&ownerCount)
			sqliteStore.DB().
				Model(&models.PoolRegistrationRelay{}).
				Count(&relayCount)
			if regCount != 2 || ownerCount != 3 || relayCount != 2 {
				t.Fatalf(
					"expected 2 regs, 3 owners, 2 relays; got %d, %d, %d",
					regCount,
					ownerCount,
					relayCount,
				)
			}

			// Delete the second registration only
			if result := sqliteStore.DB().Delete(&poolReg2); result.Error != nil {
				t.Fatalf(
					"failed to delete pool registration 2: %v",
					result.Error,
				)
			}

			// Pool should still exist
			var poolCount int64
			sqliteStore.DB().Model(&models.Pool{}).Count(&poolCount)
			if poolCount != 1 {
				t.Errorf(
					"expected pool to still exist, got %d pools",
					poolCount,
				)
			}

			// First registration and its owners/relays should still exist
			sqliteStore.DB().Model(&models.PoolRegistration{}).Count(&regCount)
			sqliteStore.DB().
				Model(&models.PoolRegistrationOwner{}).
				Count(&ownerCount)
			sqliteStore.DB().
				Model(&models.PoolRegistrationRelay{}).
				Count(&relayCount)
			if regCount != 1 {
				t.Errorf(
					"expected 1 registration after deleting one, got %d",
					regCount,
				)
			}
			if ownerCount != 1 {
				t.Errorf(
					"expected 1 owner from first registration, got %d",
					ownerCount,
				)
			}
			if relayCount != 1 {
				t.Errorf(
					"expected 1 relay from first registration, got %d",
					relayCount,
				)
			}
		},
	)
}

// TestPoolCrossPoolOwnerRelay tests that owners/relays with the same key hash
// across different pools are stored as separate records and don't affect each other
func TestPoolCrossPoolOwnerRelay(t *testing.T) {
	t.Run(
		"same owner key hash in different pools are independent records",
		func(t *testing.T) {
			sqliteStore, err := New("", nil, nil)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if err := sqliteStore.Start(); err != nil {
				t.Fatalf("unexpected error starting store: %s", err)
			}
			defer sqliteStore.Close() //nolint:errcheck

			if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
				t.Fatalf("failed to auto-migrate: %v", err)
			}

			// Create two different pools
			poolA := models.Pool{
				PoolKeyHash: []byte("pool_A_hash_123456789012345678901234"),
			}
			if result := sqliteStore.DB().Create(&poolA); result.Error != nil {
				t.Fatalf("failed to create pool A: %v", result.Error)
			}
			poolB := models.Pool{
				PoolKeyHash: []byte("pool_B_hash_123456789012345678901234"),
			}
			if result := sqliteStore.DB().Create(&poolB); result.Error != nil {
				t.Fatalf("failed to create pool B: %v", result.Error)
			}

			// Both pools use the SAME owner key hash and relay hostname
			sharedOwnerHash := []byte("shared_owner_key_hash_1234567890123")
			sharedRelayHost := "shared-relay.example.com"

			// Create registration for Pool A
			regA := models.PoolRegistration{
				PoolID:      poolA.ID,
				PoolKeyHash: poolA.PoolKeyHash,
				AddedSlot:   500,
				Owners: []models.PoolRegistrationOwner{
					{KeyHash: sharedOwnerHash, PoolID: poolA.ID},
				},
				Relays: []models.PoolRegistrationRelay{
					{Hostname: sharedRelayHost, PoolID: poolA.ID, Port: 3001},
				},
			}
			if result := sqliteStore.DB().Create(&regA); result.Error != nil {
				t.Fatalf("failed to create registration A: %v", result.Error)
			}

			// Create registration for Pool B with SAME owner hash and relay
			regB := models.PoolRegistration{
				PoolID:      poolB.ID,
				PoolKeyHash: poolB.PoolKeyHash,
				AddedSlot:   1000,
				Owners: []models.PoolRegistrationOwner{
					{
						KeyHash: sharedOwnerHash,
						PoolID:  poolB.ID,
					}, // Same key hash!
				},
				Relays: []models.PoolRegistrationRelay{
					{
						Hostname: sharedRelayHost,
						PoolID:   poolB.ID,
						Port:     3001,
					}, // Same relay!
				},
			}
			if result := sqliteStore.DB().Create(&regB); result.Error != nil {
				t.Fatalf("failed to create registration B: %v", result.Error)
			}

			// Verify we have 2 owner records and 2 relay records (separate records, same data)
			var ownerCount, relayCount int64
			sqliteStore.DB().
				Model(&models.PoolRegistrationOwner{}).
				Count(&ownerCount)
			sqliteStore.DB().
				Model(&models.PoolRegistrationRelay{}).
				Count(&relayCount)
			if ownerCount != 2 {
				t.Fatalf("expected 2 owner records, got %d", ownerCount)
			}
			if relayCount != 2 {
				t.Fatalf("expected 2 relay records, got %d", relayCount)
			}

			// Delete Pool B - this should only delete Pool B's records
			if result := sqliteStore.DB().Delete(&poolB); result.Error != nil {
				t.Fatalf("failed to delete pool B: %v", result.Error)
			}

			// Pool A should still exist with its registration, owner, and relay
			var poolCount int64
			sqliteStore.DB().Model(&models.Pool{}).Count(&poolCount)
			if poolCount != 1 {
				t.Errorf("expected 1 pool remaining, got %d", poolCount)
			}

			var regCount int64
			sqliteStore.DB().Model(&models.PoolRegistration{}).Count(&regCount)
			if regCount != 1 {
				t.Errorf("expected 1 registration remaining, got %d", regCount)
			}

			// Verify Pool A's owner and relay still exist
			sqliteStore.DB().
				Model(&models.PoolRegistrationOwner{}).
				Count(&ownerCount)
			sqliteStore.DB().
				Model(&models.PoolRegistrationRelay{}).
				Count(&relayCount)
			if ownerCount != 1 {
				t.Errorf(
					"expected 1 owner remaining (Pool A's), got %d",
					ownerCount,
				)
			}
			if relayCount != 1 {
				t.Errorf(
					"expected 1 relay remaining (Pool A's), got %d",
					relayCount,
				)
			}

			// Verify the remaining records belong to Pool A
			var remainingOwner models.PoolRegistrationOwner
			sqliteStore.DB().First(&remainingOwner)
			if remainingOwner.PoolID != poolA.ID {
				t.Errorf("remaining owner should belong to Pool A")
			}
		},
	)
}

// TestUtxoCascadeDelete tests that Utxo deletion cascades correctly to Asset records
func TestUtxoCascadeDelete(t *testing.T) {
	t.Run("utxo deletion cascades to assets", func(t *testing.T) {
		sqliteStore, err := New("", nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if err := sqliteStore.Start(); err != nil {
			t.Fatalf("unexpected error starting store: %s", err)
		}
		defer sqliteStore.Close() //nolint:errcheck

		if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
			t.Fatalf("failed to auto-migrate: %v", err)
		}

		txId := []byte("tx_hash_1234567890123456789012345678901234567890")

		// Create a Utxo with multiple assets
		utxo := models.Utxo{
			TxId:       txId,
			OutputIdx:  0,
			AddedSlot:  1000,
			PaymentKey: []byte("payment_key_123456789012345678901234567890"),
			Amount:     1000000,
			Assets: []models.Asset{
				{
					PolicyId: []byte(
						"policy_id_1234567890123456789012345678901234",
					),
					Name:   []byte("asset_name_1"),
					Amount: 100,
				},
				{
					PolicyId: []byte(
						"policy_id_1234567890123456789012345678901234",
					),
					Name:   []byte("asset_name_2"),
					Amount: 200,
				},
			},
		}
		if result := sqliteStore.DB().Create(&utxo); result.Error != nil {
			t.Fatalf("failed to create utxo: %v", result.Error)
		}

		// Verify records exist
		var utxoCount, assetCount int64
		sqliteStore.DB().Model(&models.Utxo{}).Count(&utxoCount)
		sqliteStore.DB().Model(&models.Asset{}).Count(&assetCount)
		if utxoCount != 1 || assetCount != 2 {
			t.Fatalf(
				"expected 1 utxo, 2 assets; got %d, %d",
				utxoCount,
				assetCount,
			)
		}

		// Delete the utxo
		if result := sqliteStore.DB().Delete(&utxo); result.Error != nil {
			t.Fatalf("failed to delete utxo: %v", result.Error)
		}

		// All related assets should be deleted via cascade
		sqliteStore.DB().Model(&models.Utxo{}).Count(&utxoCount)
		sqliteStore.DB().Model(&models.Asset{}).Count(&assetCount)
		if utxoCount != 0 {
			t.Errorf("expected 0 utxos after delete, got %d", utxoCount)
		}
		if assetCount != 0 {
			t.Errorf(
				"expected 0 assets after utxo delete (cascade), got %d",
				assetCount,
			)
		}
	})

	t.Run(
		"deleting one utxo does not affect assets of other utxos",
		func(t *testing.T) {
			sqliteStore, err := New("", nil, nil)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if err := sqliteStore.Start(); err != nil {
				t.Fatalf("unexpected error starting store: %s", err)
			}
			defer sqliteStore.Close() //nolint:errcheck

			if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
				t.Fatalf("failed to auto-migrate: %v", err)
			}

			// Create first utxo with asset
			utxo1 := models.Utxo{
				TxId: []byte(
					"tx_hash_1111111111111111111111111111111111111111",
				),
				OutputIdx: 0,
				AddedSlot: 1000,
				PaymentKey: []byte(
					"payment_key_123456789012345678901234567890",
				),
				Amount: 1000000,
				Assets: []models.Asset{
					{
						PolicyId: []byte(
							"policy_id_1234567890123456789012345678901234",
						),
						Name:   []byte("asset_utxo1"),
						Amount: 100,
					},
				},
			}
			if result := sqliteStore.DB().Create(&utxo1); result.Error != nil {
				t.Fatalf("failed to create utxo1: %v", result.Error)
			}

			// Create second utxo with asset
			utxo2 := models.Utxo{
				TxId: []byte(
					"tx_hash_2222222222222222222222222222222222222222",
				),
				OutputIdx: 0,
				AddedSlot: 1000,
				PaymentKey: []byte(
					"payment_key_123456789012345678901234567890",
				),
				Amount: 2000000,
				Assets: []models.Asset{
					{
						PolicyId: []byte(
							"policy_id_1234567890123456789012345678901234",
						),
						Name:   []byte("asset_utxo2"),
						Amount: 200,
					},
				},
			}
			if result := sqliteStore.DB().Create(&utxo2); result.Error != nil {
				t.Fatalf("failed to create utxo2: %v", result.Error)
			}

			// Verify initial counts
			var utxoCount, assetCount int64
			sqliteStore.DB().Model(&models.Utxo{}).Count(&utxoCount)
			sqliteStore.DB().Model(&models.Asset{}).Count(&assetCount)
			if utxoCount != 2 || assetCount != 2 {
				t.Fatalf(
					"expected 2 utxos, 2 assets; got %d, %d",
					utxoCount,
					assetCount,
				)
			}

			// Delete only the first utxo
			if result := sqliteStore.DB().Delete(&utxo1); result.Error != nil {
				t.Fatalf("failed to delete utxo1: %v", result.Error)
			}

			// Second utxo and its asset should still exist
			sqliteStore.DB().Model(&models.Utxo{}).Count(&utxoCount)
			sqliteStore.DB().Model(&models.Asset{}).Count(&assetCount)
			if utxoCount != 1 {
				t.Errorf(
					"expected 1 utxo after deleting one, got %d",
					utxoCount,
				)
			}
			if assetCount != 1 {
				t.Errorf(
					"expected 1 asset from remaining utxo, got %d",
					assetCount,
				)
			}

			// Verify the remaining asset belongs to utxo2
			var remainingAsset models.Asset
			if result := sqliteStore.DB().First(&remainingAsset); result.Error != nil {
				t.Fatalf("failed to query remaining asset: %v", result.Error)
			}
			if string(remainingAsset.Name) != "asset_utxo2" {
				t.Errorf(
					"expected remaining asset to be 'asset_utxo2', got '%s'",
					string(remainingAsset.Name),
				)
			}
		},
	)
}

// TestCollateralReturnUniqueConstraint verifies that CollateralReturnForTxID has a unique constraint:
//   - Multiple UTXOs with NULL CollateralReturnForTxID are allowed (regular outputs)
//   - Two UTXOs with the same non-NULL CollateralReturnForTxID are rejected (per Cardano protocol,
//     a transaction has at most one collateral return output)
func TestCollateralReturnUniqueConstraint(t *testing.T) {
	t.Run("multiple NULL CollateralReturnForTxID allowed", func(t *testing.T) {
		sqliteStore, err := New("", nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if err := sqliteStore.Start(); err != nil {
			t.Fatalf("unexpected error starting store: %s", err)
		}
		defer sqliteStore.Close() //nolint:errcheck

		if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
			t.Fatalf("failed to auto-migrate: %v", err)
		}

		// Create multiple UTXOs with NULL CollateralReturnForTxID (normal outputs)
		utxo1 := models.Utxo{
			TxId: []byte(
				"tx_hash_1111111111111111111111111111111111111111",
			),
			OutputIdx: 0,
			AddedSlot: 1000,
			Amount:    1000000,
			// CollateralReturnForTxID is nil (NULL)
		}
		if result := sqliteStore.DB().Create(&utxo1); result.Error != nil {
			t.Fatalf("failed to create utxo1: %v", result.Error)
		}

		utxo2 := models.Utxo{
			TxId: []byte(
				"tx_hash_2222222222222222222222222222222222222222",
			),
			OutputIdx: 0,
			AddedSlot: 1000,
			Amount:    2000000,
			// CollateralReturnForTxID is nil (NULL)
		}
		if result := sqliteStore.DB().Create(&utxo2); result.Error != nil {
			t.Fatalf("failed to create utxo2: %v", result.Error)
		}

		// Both should exist
		var count int64
		sqliteStore.DB().Model(&models.Utxo{}).Count(&count)
		if count != 2 {
			t.Errorf(
				"expected 2 UTXOs with NULL CollateralReturnForTxID, got %d",
				count,
			)
		}
	})

	t.Run(
		"duplicate non-NULL CollateralReturnForTxID rejected",
		func(t *testing.T) {
			sqliteStore, err := New("", nil, nil)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if err := sqliteStore.Start(); err != nil {
				t.Fatalf("unexpected error starting store: %s", err)
			}
			defer sqliteStore.Close() //nolint:errcheck

			if err := sqliteStore.DB().AutoMigrate(models.MigrateModels...); err != nil {
				t.Fatalf("failed to auto-migrate: %v", err)
			}

			// Create a transaction first (needed for FK)
			tx := models.Transaction{
				Hash: []byte(
					"tx_hash_for_collateral_test_1234567890123456",
				),
				Slot:       1000,
				BlockIndex: 0,
				Valid:      false, // Invalid tx that used collateral
			}
			if result := sqliteStore.DB().Create(&tx); result.Error != nil {
				t.Fatalf("failed to create transaction: %v", result.Error)
			}

			// Create first UTXO with CollateralReturnForTxID pointing to tx
			txID := tx.ID
			utxo1 := models.Utxo{
				TxId: []byte(
					"collateral_return_1111111111111111111111111111111",
				),
				OutputIdx:               0,
				AddedSlot:               1000,
				Amount:                  1000000,
				CollateralReturnForTxID: &txID,
			}
			if result := sqliteStore.DB().Create(&utxo1); result.Error != nil {
				t.Fatalf(
					"failed to create first collateral return: %v",
					result.Error,
				)
			}

			// Try to create second UTXO with same CollateralReturnForTxID - should fail
			utxo2 := models.Utxo{
				TxId: []byte(
					"collateral_return_2222222222222222222222222222222",
				),
				OutputIdx:               1,
				AddedSlot:               1000,
				Amount:                  2000000,
				CollateralReturnForTxID: &txID, // Same transaction ID - violates unique constraint
			}
			result := sqliteStore.DB().Create(&utxo2)
			if result.Error == nil {
				t.Fatal(
					"expected unique constraint violation for duplicate CollateralReturnForTxID, but insert succeeded",
				)
			}
			// Verify only one UTXO with this CollateralReturnForTxID exists
			var count int64
			sqliteStore.DB().
				Model(&models.Utxo{}).
				Where("collateral_return_for_tx_id = ?", txID).
				Count(&count)
			if count != 1 {
				t.Errorf(
					"expected exactly 1 UTXO with CollateralReturnForTxID=%d, got %d",
					txID,
					count,
				)
			}
		},
	)
}
