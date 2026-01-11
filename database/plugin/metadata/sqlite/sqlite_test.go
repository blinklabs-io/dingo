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

func (m *mockTransaction) Fee() uint64 {
	return 1000
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

func (m *mockTransaction) TotalCollateral() uint64 {
	return 0
}

func (m *mockTransaction) Withdrawals() map[*lcommon.Address]uint64 {
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

func (m *mockTransaction) CurrentTreasuryValue() int64 {
	return 0
}

func (m *mockTransaction) Donation() uint64 {
	return 0
}

func (m *mockTransaction) Utxorpc() (*cardano.Tx, error) {
	return nil, nil
}

func (m *mockTransaction) LeiosHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
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
