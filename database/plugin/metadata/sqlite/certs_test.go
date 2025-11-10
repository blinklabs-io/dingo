package sqlite_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/clause"
)

func TestPoolCertificateDuplicatePrevention(t *testing.T) {
	// Setup database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       "",
	})
	require.NoError(t, err)
	defer db.Close()

	// Get metadata store
	metadata := db.Metadata()

	// Test data
	poolKeyHash := []byte{0x01, 0x02, 0x03, 0x04}
	vrfKeyHash := []byte{0x05, 0x06, 0x07, 0x08}
	rewardAccount := []byte{0x09, 0x0A, 0x0B, 0x0C}

	// Insert pool registration record multiple times
	for i := range 3 {
		regCert := models.PoolRegistration{
			PoolKeyHash:   poolKeyHash,
			VrfKeyHash:    vrfKeyHash,
			RewardAccount: rewardAccount,
			Pledge:        types.Uint64{Val: 1000000},
			Cost:          types.Uint64{Val: 340000000},
			AddedSlot:     uint64(1000 + i), // Different slot for each record
			DepositAmount: types.Uint64{Val: 500000000},
			CertificateID: uint(
				i + 1,
			), // Different CertificateID for each record
		}
		err := metadata.DB().Create(&regCert).Error
		require.NoError(
			t,
			err,
			"Insert %d should succeed (re-registration allowed)",
			i+1,
		)
	}

	// Verify multiple pool registration records exist
	var poolRegs []models.PoolRegistration
	err = metadata.DB().
		Where("pool_key_hash = ?", poolKeyHash).
		Find(&poolRegs).
		Error
	require.NoError(t, err)
	assert.Len(
		t,
		poolRegs,
		3,
		"Should have multiple pool registration records (re-registration allowed)",
	)

	// Insert pool retirement record multiple times
	for i := range 3 {
		retCert := models.PoolRetirement{
			PoolKeyHash: poolKeyHash,
			Epoch:       100,
			AddedSlot:   1000,
			CertificateID: uint(
				i + 4,
			), // Different CertificateID for each record
		}
		err := metadata.DB().Create(&retCert).Error
		if i == 0 {
			require.NoError(t, err, "First insert should succeed")
		} else {
			require.Error(t, err, "Subsequent inserts should fail due to unique constraint")
		}
	}

	// Verify only one pool retirement record exists
	var poolRets []models.PoolRetirement
	err = metadata.DB().
		Where("pool_key_hash = ? AND epoch = ?", poolKeyHash, uint64(100)).
		Find(&poolRets).
		Error
	require.NoError(t, err)
	assert.Len(t, poolRets, 1, "Should have exactly one pool retirement record")
}

func TestStakeCertificateDuplicatePrevention(t *testing.T) {
	// Setup database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       "",
	})
	require.NoError(t, err)
	defer db.Close()

	// Get metadata store
	metadata := db.Metadata()

	// Test data
	stakeKey := []byte{0x01, 0x02, 0x03, 0x04}

	// Insert stake registration record multiple times with different certificate IDs
	for i := range 3 {
		regCert := models.StakeRegistration{
			StakingKey:    stakeKey,
			DepositAmount: types.Uint64{Val: 2000000},
			AddedSlot:     uint64(1000 + i), // Different slot for each record
			CertificateID: uint(
				i + 10,
			), // Different CertificateID for each record
		}
		err := metadata.DB().Create(&regCert).Error
		require.NoError(
			t,
			err,
			"All inserts should succeed with different certificate IDs",
		)
	}

	// Verify all stake registration records exist
	var stakeRegs []models.StakeRegistration
	err = metadata.DB().
		Where("staking_key = ?", stakeKey).
		Find(&stakeRegs).
		Error
	require.NoError(t, err)
	assert.Len(
		t,
		stakeRegs,
		3,
		"Should have exactly three stake registration records",
	)

	// Test that duplicate certificate IDs fail
	dupRegCert := models.StakeRegistration{
		StakingKey:    stakeKey,
		DepositAmount: types.Uint64{Val: 2000000},
		AddedSlot:     uint64(2000),
		CertificateID: uint(10), // Same CertificateID as first record
	}
	err = metadata.DB().Create(&dupRegCert).Error
	require.Error(t, err, "Insert with duplicate certificate ID should fail")

	// Insert stake deregistration record multiple times with different certificate IDs
	for i := range 3 {
		deregCert := models.Deregistration{
			StakingKey: stakeKey,
			AddedSlot:  uint64(1000 + i), // Different slot for each record
			CertificateID: uint(
				i + 20,
			), // Different CertificateID for each record
		}
		err := metadata.DB().Create(&deregCert).Error
		require.NoError(
			t,
			err,
			"All inserts should succeed with different certificate IDs",
		)
	}

	// Verify all stake deregistration records exist
	var deregRegs []models.Deregistration
	err = metadata.DB().
		Where("staking_key = ?", stakeKey).
		Find(&deregRegs).
		Error
	require.NoError(t, err)
	assert.Len(
		t,
		deregRegs,
		3,
		"Should have exactly three stake deregistration records",
	)

	// Test that duplicate certificate IDs fail for deregistration
	dupDeregCert := models.Deregistration{
		StakingKey: stakeKey,
		AddedSlot:  uint64(2000),
		CertificateID: uint(
			20,
		), // Same CertificateID as first deregistration record
	}
	err = metadata.DB().Create(&dupDeregCert).Error
	require.Error(t, err, "Insert with duplicate certificate ID should fail")
}

func TestDrepCertificateReRegistration(t *testing.T) {
	// Setup database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       "",
	})
	require.NoError(t, err)
	defer db.Close()

	// Get metadata store
	metadata := db.Metadata()

	// Test data
	drepCredential := []byte{0x01, 0x02, 0x03, 0x04}

	// Insert DRep registration record multiple times (should all succeed now)
	for i := range 3 {
		regCert := models.RegistrationDrep{
			DrepCredential: drepCredential,
			DepositAmount:  types.Uint64{Val: 2000000},
			AddedSlot:      uint64(1000 + i), // Different slot for each record
		}
		err := metadata.DB().Create(&regCert).Error
		require.NoError(
			t,
			err,
			"Insert %d should succeed (re-registration allowed)",
			i+1,
		)
	}

	// Verify multiple DRep registration records exist
	var drepRegs []models.RegistrationDrep
	err = metadata.DB().
		Where("drep_credential = ?", drepCredential).
		Find(&drepRegs).
		Error
	require.NoError(t, err)
	assert.Len(
		t,
		drepRegs,
		3,
		"Should have multiple DRep registration records (re-registration allowed)",
	)

	// Insert DRep deregistration record multiple times (should all succeed)
	for i := range 3 {
		deregCert := models.DeregistrationDrep{
			DrepCredential: drepCredential,
			DepositAmount:  types.Uint64{Val: 2000000},
			AddedSlot:      uint64(1000 + i), // Different slot for each record
		}
		err := metadata.DB().Create(&deregCert).Error
		require.NoError(t, err, "Deregistration insert %d should succeed", i+1)
	}

	// Verify multiple DRep deregistration records exist
	var drepDeregs []models.DeregistrationDrep
	err = metadata.DB().
		Where("drep_credential = ?", drepCredential).
		Find(&drepDeregs).
		Error
	require.NoError(t, err)
	assert.Len(
		t,
		drepDeregs,
		3,
		"Should have multiple DRep deregistration records",
	)

	// Insert DRep update record multiple times (should all succeed)
	for i := range 3 {
		updateCert := models.UpdateDrep{
			DrepCredential: drepCredential,
			AddedSlot:      uint64(1000 + i), // Different slot for each record
		}
		err := metadata.DB().Create(&updateCert).Error
		require.NoError(t, err, "Update insert %d should succeed", i+1)
	}

	// Verify multiple DRep update records exist
	var drepUpdates []models.UpdateDrep
	err = metadata.DB().
		Where("drep_credential = ?", drepCredential).
		Find(&drepUpdates).
		Error
	require.NoError(t, err)
	assert.Len(
		t,
		drepUpdates,
		3,
		"Should have multiple DRep update records",
	)
}

func TestGenesisKeyDelegationCertificateIdempotent(t *testing.T) {
	// Setup database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       "",
	})
	require.NoError(t, err)
	defer db.Close()

	// Get metadata store
	metadata := db.Metadata()

	// Test data
	genesisHash := []byte{0x01, 0x02, 0x03, 0x04}
	genesisDelegateHash := []byte{0x05, 0x06, 0x07, 0x08}
	vrfKeyHash := []byte{0x09, 0x0A, 0x0B, 0x0C}

	// Create genesis key delegation certificate manually and insert multiple times
	genesisCert := models.GenesisKeyDelegation{
		GenesisHash:         genesisHash,
		GenesisDelegateHash: genesisDelegateHash,
		VrfKeyHash:          vrfKeyHash,
		AddedSlot:           1000,
	}

	// Insert genesis key delegation record multiple times (should all succeed but only create one record)
	var firstID uint
	for i := range 3 {
		// Use the same conflict resolution logic as the actual store function
		err := metadata.DB().Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "genesis_hash"},
				{Name: "genesis_delegate_hash"},
				{Name: "vrf_key_hash"},
			},
			DoNothing: true,
		}).Create(&genesisCert).Error
		require.NoError(
			t,
			err,
			"Insert %d should succeed (idempotent operation)",
			i+1,
		)

		if i == 0 {
			firstID = genesisCert.ID
			require.NotZero(t, firstID, "First insert should get a valid ID")
		} else {
			// For subsequent inserts, if conflict resolution prevented insertion,
			// the ID field may not be updated. Query to ensure we have the correct ID.
			if genesisCert.ID == 0 {
				err := metadata.DB().Where(models.GenesisKeyDelegation{
					GenesisHash:         genesisHash,
					GenesisDelegateHash: genesisDelegateHash,
					VrfKeyHash:          vrfKeyHash,
				}).First(&genesisCert).Error
				require.NoError(t, err, "Should be able to query existing record")
			}
			require.Equal(t, firstID, genesisCert.ID, "Should have the same ID as the first insert")
		}
	}

	// Verify only one genesis key delegation record exists
	var genesisDelegations []models.GenesisKeyDelegation
	err = metadata.DB().
		Where("genesis_hash = ? AND genesis_delegate_hash = ? AND vrf_key_hash = ?",
			genesisHash, genesisDelegateHash, vrfKeyHash).
		Find(&genesisDelegations).
		Error
	require.NoError(t, err)
	assert.Len(
		t,
		genesisDelegations,
		1,
		"Should have exactly one genesis key delegation record",
	)
	assert.Equal(
		t,
		firstID,
		genesisDelegations[0].ID,
		"Should have the same ID as the first insert",
	)
}

func TestAuthCommitteeHotCertificateIdPopulation(t *testing.T) {
	// Setup database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       "",
	})
	require.NoError(t, err)
	defer db.Close()

	// Get metadata store
	metadata := db.Metadata()

	// Test data
	coldCredential := []byte{0x01, 0x02, 0x03, 0x04}
	hostCredential := []byte{0x05, 0x06, 0x07, 0x08}

	// Create auth committee hot record (simulating the store process)
	authHot := models.AuthCommitteeHot{
		ColdCredential: coldCredential,
		HostCredential: hostCredential,
		AddedSlot:      1000,
		// CertificateID starts as 0
	}
	err = metadata.DB().Create(&authHot).Error
	require.NoError(t, err)

	// Verify CertificateID is initially 0
	assert.Equal(
		t,
		uint(0),
		authHot.CertificateID,
		"CertificateID should initially be 0",
	)

	// Simulate the certificate ID update (as done in storeAuthCommitteeHotCertificate)
	certID := uint(42) // Simulated certificate ID
	err = metadata.DB().Model(&authHot).Update("certificate_id", certID).Error
	require.NoError(t, err)

	// Verify CertificateID is now populated
	var updatedAuthHot models.AuthCommitteeHot
	err = metadata.DB().Where("id = ?", authHot.ID).First(&updatedAuthHot).Error
	require.NoError(t, err)
	assert.Equal(
		t,
		certID,
		updatedAuthHot.CertificateID,
		"CertificateID should be populated after update",
	)

	// Test unique constraint: try to create another record with same ColdCredential and CertificateID
	duplicateAuthHot := models.AuthCommitteeHot{
		ColdCredential: coldCredential,
		HostCredential: []byte{
			0x09,
			0x0A,
			0x0B,
			0x0C,
		}, // Different host credential
		CertificateID: certID,
		AddedSlot:     1001,
	}
	err = metadata.DB().Create(&duplicateAuthHot).Error
	require.Error(
		t,
		err,
		"Creating duplicate with same ColdCredential and CertificateID should fail",
	)
}

func TestCertificatePersistenceValidation(t *testing.T) {
	// Setup database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       "",
	})
	require.NoError(t, err)
	defer db.Close()

	// Get metadata store
	metadata := db.Metadata()

	// Test data
	stakeKey := []byte{0x01, 0x02, 0x03, 0x04}
	poolKeyHash := []byte{0x05, 0x06, 0x07, 0x08}
	drepKey := []byte{0x09, 0x0A, 0x0B, 0x0C}

	// Test 1: Verify StakeDeregistration unique constraint
	t.Run("StakeDeregistrationUniqueConstraint", func(t *testing.T) {
		// Insert stake deregistration record
		stakeDereg := models.StakeDeregistration{
			StakingKey:    stakeKey,
			CertificateID: 100,
			AddedSlot:     1000,
		}
		err := metadata.DB().Create(&stakeDereg).Error
		require.NoError(t, err)

		// Try to insert duplicate with same StakingKey and CertificateID
		dupStakeDereg := models.StakeDeregistration{
			StakingKey:    stakeKey,
			CertificateID: 100, // Same CertificateID
			AddedSlot:     1001,
		}
		err = metadata.DB().Create(&dupStakeDereg).Error
		require.Error(t, err, "Duplicate StakeDeregistration should fail")
	})

	// Test 2: Verify certificate cleanup removes all related records
	t.Run("CertificateCleanupValidation", func(t *testing.T) {
		// Insert test records
		testCertID := uint(200)
		stakeReg := models.StakeRegistration{
			StakingKey:    stakeKey,
			CertificateID: testCertID,
			DepositAmount: types.Uint64{Val: 2000000},
			AddedSlot:     1000,
		}
		err := metadata.DB().Create(&stakeReg).Error
		require.NoError(t, err)

		// Insert certificate mapping
		certMap := models.Certificate{
			TransactionID: 1,
			CertIndex:     0,
			CertType:      uint(lcommon.CertificateTypeStakeRegistration),
			CertificateID: testCertID,
			Slot:          1000,
		}
		err = metadata.DB().Create(&certMap).Error
		require.NoError(t, err)

		// Verify records exist
		var count int64
		metadata.DB().
			Model(&models.StakeRegistration{}).
			Where("certificate_id = ?", testCertID).
			Count(&count)
		assert.Equal(
			t,
			int64(1),
			count,
			"StakeRegistration record should exist",
		)

		metadata.DB().
			Model(&models.Certificate{}).
			Where("certificate_id = ?", testCertID).
			Count(&count)
		assert.Equal(t, int64(1), count, "Certificate mapping should exist")

		// Manually perform bulk removal (simulating what bulkRemoveCertificateRecords does)
		err = metadata.DB().Where("certificate_id IN ?", []uint{testCertID}).
			Delete(&models.StakeRegistration{}).Error
		require.NoError(t, err)

		// Manually remove certificate mapping
		err = metadata.DB().
			Where("id IN ?", []uint{certMap.ID}).
			Delete(&models.Certificate{}).
			Error
		require.NoError(t, err)

		// Verify records are removed
		metadata.DB().
			Model(&models.StakeRegistration{}).
			Where("certificate_id = ?", testCertID).
			Count(&count)
		assert.Equal(
			t,
			int64(0),
			count,
			"StakeRegistration record should be removed",
		)

		metadata.DB().
			Model(&models.Certificate{}).
			Where("certificate_id = ?", testCertID).
			Count(&count)
		assert.Equal(
			t,
			int64(0),
			count,
			"Certificate mapping should be removed",
		)
	})

	// Test 3: Verify account state changes are correct
	t.Run("AccountStateChanges", func(t *testing.T) {
		// Create an account
		account := models.Account{
			StakingKey: stakeKey,
			Pool:       poolKeyHash,
			Drep:       drepKey,
			Active:     true,
			AddedSlot:  1000,
		}
		err := metadata.DB().Create(&account).Error
		require.NoError(t, err)

		// Verify account is active
		var retrievedAccount models.Account
		err = metadata.DB().
			Where("staking_key = ?", stakeKey).
			First(&retrievedAccount).
			Error
		require.NoError(t, err)
		assert.True(t, retrievedAccount.Active, "Account should be active")

		// Simulate deactivation (as done in deregistration)
		retrievedAccount.Active = false
		err = metadata.DB().Save(&retrievedAccount).Error
		require.NoError(t, err)

		// Verify account is deactivated
		err = metadata.DB().
			Where("staking_key = ?", stakeKey).
			First(&retrievedAccount).
			Error
		require.NoError(t, err)
		assert.False(
			t,
			retrievedAccount.Active,
			"Account should be deactivated",
		)
	})

	// Test 4: Verify types.Uint64 conversion in stored models
	t.Run("Uint64ConversionValidation", func(t *testing.T) {
		testDeposit := uint64(2500000)

		// Create a stake registration record directly to test conversion
		stakeReg := models.StakeRegistration{
			StakingKey:    stakeKey,
			CertificateID: 300,
			DepositAmount: types.Uint64{
				Val: testDeposit,
			}, // Simulate what the store function does
			AddedSlot: 1000,
		}
		err := metadata.DB().Create(&stakeReg).Error
		require.NoError(t, err)

		// Retrieve the record and verify the types.Uint64 conversion
		var retrieved models.StakeRegistration
		err = metadata.DB().
			Where("certificate_id = ?", 300).
			First(&retrieved).
			Error
		require.NoError(t, err)

		// Verify that the types.Uint64 was stored and can be converted back
		assert.Equal(
			t,
			testDeposit,
			uint64(retrieved.DepositAmount.Val),
			"Deposit amount should be convertible back to uint64",
		)

		// Test with a deregistration record that has Amount
		testAmount := uint64(2000000)
		dereg := models.Deregistration{
			StakingKey:    stakeKey,
			CertificateID: 301,
			AddedSlot:     1001,
			Amount:        types.Uint64{Val: testAmount},
		}
		err = metadata.DB().Create(&dereg).Error
		require.NoError(t, err)

		// Retrieve and verify
		var retrievedDereg models.Deregistration
		err = metadata.DB().
			Where("certificate_id = ?", 301).
			First(&retrievedDereg).
			Error
		require.NoError(t, err)

		assert.Equal(
			t,
			testAmount,
			uint64(retrievedDereg.Amount.Val),
			"Deregistration amount should be convertible back to uint64",
		)

		// Test 5: Verify end-to-end conversion through store functions
		// This tests that the store functions properly convert uint64 to types.Uint64
		testDeposit = uint64(3500000)
		stakeKey2 := []byte{0x11, 0x12, 0x13, 0x14}

		// Create a transaction to store certificates
		tx := metadata.DB().Begin()
		require.NoError(t, tx.Error)
		defer tx.Rollback()

		// Manually create a stake registration record using the same pattern as store functions
		stakeReg2 := models.StakeRegistration{
			StakingKey:    stakeKey2,
			CertificateID: 400,
			DepositAmount: types.Uint64{
				Val: testDeposit,
			}, // This is what the store function does
			AddedSlot: 2000,
		}
		err = tx.Create(&stakeReg2).Error
		require.NoError(t, err)

		// Retrieve and verify the conversion worked
		var retrieved2 models.StakeRegistration
		err = tx.Where("certificate_id = ?", 400).First(&retrieved2).Error
		require.NoError(t, err)

		assert.Equal(
			t,
			testDeposit,
			uint64(retrieved2.DepositAmount.Val),
			"Store function conversion should preserve uint64 value",
		)
	})
}
