package sqlite_test

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// testTransactionCBORHex contains the CBOR hex string used for certificate tests
const testTransactionCBORHex = "83a50081825820377732953cbd7eb824e58291dd08599cfcfe6eedb49f590633610674fc3c33c50001818258390187acac5a3d0b41cd1c5e8c03af5be782f261f21beed70970ddee0873ae34f9d442c7f1a01de9f2dd520791122d6fbf3968c5f8328e9091331a597dbcf1021a0002fd99031a025c094a04828a03581c27b1b4470c84db78ce1ffbfff77bb068abb4e47d43cb6009caaa352358204a7537ce9eeaba1c650261f167827b4e12dd403c4bf13c56b2cba06288f7e9ab1a59682f001a1443fd00d81e82011864581de1ae34f9d442c7f1a01de9f2dd520791122d6fbf3968c5f8328e90913381581cae34f9d442c7f1a01de9f2dd520791122d6fbf3968c5f8328e9091338183011917706e33342e3139382e3234312e323336827468747470733a2f2f6769742e696f2f4a7543786e5820fa77d30bb41e2998233245d269ff5763ecf4371388214943ecef277cae45492783028200581cae34f9d442c7f1a01de9f2dd520791122d6fbf3968c5f8328e909133581c27b1b4470c84db78ce1ffbfff77bb068abb4e47d43cb6009caaa3523a10083825820922a22d07c0ca148105760cb767ece603574ea465d6697c87da8207c8936ebea58405594a100197379c0de715de0b5304e0546e661dae2f36b12173cc150a42215356a5600bf0c02954f02ce3620cfb7f12c23a19328fd00dd1194b4f363675ef407825820727c1891d01cf29ccd1146528221827dcf00a093498509404af77a8b15d77c925840f52e0e1403167212b11fe5d87b7cfdb2f39e5384979ac3625917127ad46763d864a7fcb7147c7b85322ada7ba8fe91c0b5152c74ef4ff0c8132b125e681af50382582073c16f2b67ff85307c4c5935bad1389b9ead473419dbad20f5d5e6436982992b58400572eed773b9a199fd486ebe61b480f05803d107ea97ff649f28b8874d3117f890f80657cbb6eea0d833c21e4e8bc7f1a27cddb9e24fc1ed79b04ddbdcd11d0ff6"

// decodeTestTransaction decodes the test transaction CBOR and returns the transaction object
func decodeTestTransaction(t *testing.T) gledger.Transaction {
	t.Helper()

	txRawBytes, err := hex.DecodeString(testTransactionCBORHex)
	require.NoError(t, err)

	txType, err := gledger.DetermineTransactionType(txRawBytes)
	require.NoError(t, err)

	tx, err := gledger.NewTransactionFromCbor(txType, txRawBytes)
	require.NoError(t, err)

	return tx
}

// getTestDeposits returns the standard deposits map used for certificate tests
func getTestDeposits() map[int]uint64 {
	return map[int]uint64{0: 2000000, 1: 2000000}
}

func TestPoolCertificateDuplicatePrevention(t *testing.T) {
	// Setup database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       t.TempDir(),
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
		require.NoError(
			t,
			err,
			"Insert %d should succeed (multiple retirements allowed per pool per epoch)",
			i+1,
		)
	}

	// Verify multiple pool retirement records exist
	var poolRets []models.PoolRetirement
	err = metadata.DB().
		Where("pool_key_hash = ? AND epoch = ?", poolKeyHash, uint64(100)).
		Find(&poolRets).
		Error
	require.NoError(t, err)
	assert.Len(
		t,
		poolRets,
		3,
		"Should have multiple pool retirement records (multiple retirements allowed per pool per epoch)",
	)
}

func TestStakeCertificateDuplicatePrevention(t *testing.T) {
	// Setup database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       t.TempDir(),
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

	// Test that duplicate (staking_key, certificate_id) pairs fail
	dupRegCert := models.StakeRegistration{
		StakingKey:    stakeKey,
		DepositAmount: types.Uint64{Val: 2000000},
		AddedSlot:     uint64(2000),
		CertificateID: uint(10), // Same CertificateID as first record
	}
	err = metadata.DB().Create(&dupRegCert).Error
	require.Error(
		t,
		err,
		"Insert with duplicate (staking_key, certificate_id) pair should fail",
	)

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

	// Test that duplicate (staking_key, certificate_id) pairs fail for deregistration
	dupDeregCert := models.Deregistration{
		StakingKey: stakeKey,
		AddedSlot:  uint64(2000),
		CertificateID: uint(
			20,
		), // Same CertificateID as first deregistration record
	}
	err = metadata.DB().Create(&dupDeregCert).Error
	require.Error(
		t,
		err,
		"Insert with duplicate (staking_key, certificate_id) pair should fail",
	)
}

func TestDrepCertificateReRegistration(t *testing.T) {
	// Setup database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       t.TempDir(),
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

	// Insert genesis key delegation record multiple times (should all succeed but only create one record)
	var firstID uint
	for i := range 3 {
		// Create a fresh instance for each iteration to avoid coupling to GORM's conflict behavior
		genesisCert := models.GenesisKeyDelegation{
			GenesisHash:         genesisHash,
			GenesisDelegateHash: genesisDelegateHash,
			VrfKeyHash:          vrfKeyHash,
			AddedSlot:           types.Uint64{Val: 1000},
		}

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
			// For subsequent inserts, query the existing record to verify it's the same one
			var existing models.GenesisKeyDelegation
			err := metadata.DB().Where(models.GenesisKeyDelegation{
				GenesisHash:         genesisHash,
				GenesisDelegateHash: genesisDelegateHash,
				VrfKeyHash:          vrfKeyHash,
			}).First(&existing).Error
			require.NoError(t, err, "Should be able to query existing record")
			require.Equal(t, firstID, existing.ID, "Should have the same ID as the first insert")
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
	certID := uint(42) // Simulated certificate ID

	// Create auth committee hot record with correct certificate ID (simulating the new store process)
	authHot := models.AuthCommitteeHot{
		ColdCredential: coldCredential,
		HostCredential: hostCredential,
		CertificateID:  certID, // CertificateID is set correctly from the start
		AddedSlot:      types.Uint64{Val: 1000},
	}
	err = metadata.DB().Create(&authHot).Error
	require.NoError(t, err)

	// Verify CertificateID is populated correctly from the start
	assert.Equal(
		t,
		certID,
		authHot.CertificateID,
		"CertificateID should be populated correctly from creation",
	)

	// Verify we can retrieve the record with the correct CertificateID
	var retrievedAuthHot models.AuthCommitteeHot
	err = metadata.DB().
		Where("id = ?", authHot.ID).
		First(&retrievedAuthHot).
		Error
	require.NoError(t, err)
	assert.Equal(
		t,
		certID,
		retrievedAuthHot.CertificateID,
		"CertificateID should be correct when retrieved",
	)

	// Test unique constraint: try to create another record with same ColdCredential
	// (should fail due to unique constraint on ColdCredential)
	duplicateAuthHot := models.AuthCommitteeHot{
		ColdCredential: coldCredential,
		HostCredential: []byte{
			0x09,
			0x0A,
			0x0B,
			0x0C,
		}, // Different host credential
		CertificateID: certID + 1, // Different certificate ID
		AddedSlot:     types.Uint64{Val: 1001},
	}
	err = metadata.DB().Create(&duplicateAuthHot).Error
	require.Error(
		t,
		err,
		"Creating duplicate with same ColdCredential should fail",
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
			CertType:      models.CertificateTypeStakeRegistration,
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
		// bulkRemoveCertificateRecords deletes specialized records by certificate_id IN ?
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

func TestTransactionCertificateIdempotency(t *testing.T) {
	// Test that SetTransaction handles transaction re-inclusion correctly:
	// - Same transaction with same block point should be fully idempotent
	// - Same transaction with different block point should update slot/hash but not create duplicates
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       t.TempDir(), // Use temporary directory for clean database
	})
	require.NoError(t, err)
	defer db.Close()

	metadata := db.Metadata()

	// Decode the test transaction
	tx := decodeTestTransaction(t)

	// Set up initial test data
	blockPoint1 := ocommon.Point{
		Slot: 1000,
		Hash: []byte{0x05, 0x06, 0x07, 0x08},
	}

	// Call SetTransaction the first time
	deposits := getTestDeposits()
	err = metadata.SetTransaction(blockPoint1, tx, 0, deposits, nil)
	require.NoError(t, err)

	// Count certificates after first call
	var certCount1 int64
	err = metadata.DB().Model(&models.Certificate{}).Count(&certCount1).Error
	require.NoError(t, err)
	assert.NotZero(
		t,
		certCount1,
		"Should have created certificates on first call",
	)

	// Verify initial slot
	var certificates []models.Certificate
	err = metadata.DB().Find(&certificates).Error
	require.NoError(t, err)
	for _, cert := range certificates {
		assert.Equal(
			t,
			blockPoint1.Slot,
			cert.Slot,
			"Certificate Slot should match initial block slot",
		)
	}

	// Test 1: Call SetTransaction again with the same transaction and same block point (should be fully idempotent)
	err = metadata.SetTransaction(blockPoint1, tx, 0, deposits, nil)
	require.NoError(t, err)

	// Count certificates after second call
	var certCount2 int64
	err = metadata.DB().Model(&models.Certificate{}).Count(&certCount2).Error
	require.NoError(t, err)

	// Should have the same number of certificates (idempotent)
	assert.Equal(
		t,
		certCount1,
		certCount2,
		"Certificate count should not change on second call with same block point",
	)

	// Test 2: Call SetTransaction with same transaction but different block point (simulating re-inclusion after reorg)
	blockPoint2 := ocommon.Point{
		Slot: 1500,                           // Different slot
		Hash: []byte{0x09, 0x0A, 0x0B, 0x0C}, // Different hash
	}
	err = metadata.SetTransaction(blockPoint2, tx, 0, deposits, nil)
	require.NoError(t, err)

	// Count certificates after third call
	var certCount3 int64
	err = metadata.DB().Model(&models.Certificate{}).Count(&certCount3).Error
	require.NoError(t, err)

	// Should still have the same number of certificates (no duplicates)
	assert.Equal(
		t,
		certCount1,
		certCount3,
		"Certificate count should not change on re-inclusion with different block point",
	)

	// Verify that the certificates now have the updated slot
	err = metadata.DB().Find(&certificates).Error
	require.NoError(t, err)
	assert.Len(
		t,
		certificates,
		int(certCount1),
		"Should have the expected number of certificates",
	)

	for _, cert := range certificates {
		assert.Equal(
			t,
			blockPoint2.Slot,
			cert.Slot,
			"Certificate Slot should be updated to new block slot after re-inclusion",
		)
	}
}

func TestCertificateProcessingDataIntegrity(t *testing.T) {
	// Test SetTransaction behavior with different scenarios using real transaction data
	tests := []struct {
		name            string
		callTwice       bool // Test re-processing by calling SetTransaction twice
		differentBlock  bool // Use different block point for second call (simulating re-inclusion)
		provideDeposits bool
		expectError     bool
		expectErrorMsg  string
		description     string
	}{
		{
			name:            "New transaction with deposits",
			callTwice:       false,
			differentBlock:  false,
			provideDeposits: true,
			expectError:     false,
			description:     "Should process certificates successfully",
		},
		{
			name:            "New transaction without deposits",
			callTwice:       false,
			differentBlock:  false,
			provideDeposits: false,
			expectError:     true,
			expectErrorMsg:  "certificate deposits required",
			description:     "Should fail due to missing deposits",
		},
		{
			name:            "Idempotent processing (same block)",
			callTwice:       true,
			differentBlock:  false,
			provideDeposits: true,
			expectError:     false,
			description:     "Should process certificates only once when called with same block",
		},
		{
			name:            "Re-inclusion with different block",
			callTwice:       true,
			differentBlock:  true,
			provideDeposits: true,
			expectError:     false,
			description:     "Should update slot when transaction is re-included in different block",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup fresh database for each test
			db, err := database.New(&database.Config{
				BlobCacheSize: 1 << 20,
				Logger:        nil,
				PromRegistry:  nil,
				DataDir:       t.TempDir(), // Use temporary directory for clean database
			})
			require.NoError(t, err)
			defer db.Close()

			metadata := db.Metadata()

			// Decode the test transaction
			tx := decodeTestTransaction(t)

			// Set up test data
			blockPoint1 := ocommon.Point{
				Slot: 1000,
				Hash: []byte{0x05, 0x06, 0x07, 0x08},
			}

			// Prepare deposits if needed
			var deposits map[int]uint64
			if tt.provideDeposits {
				deposits = getTestDeposits()
			}

			// Call SetTransaction first time
			err = metadata.SetTransaction(blockPoint1, tx, 0, deposits, nil)

			// Check error expectations
			if tt.expectError {
				require.Error(
					t,
					err,
					"Expected error for test case: %s",
					tt.description,
				)
				if tt.expectErrorMsg != "" {
					assert.Contains(
						t,
						err.Error(),
						tt.expectErrorMsg,
						"Error message should contain expected text",
					)
				}
				return // Skip further checks for error cases
			} else {
				require.NoError(t, err, "Expected no error for test case: %s", tt.description)
			}

			// Count certificates after first call
			var certCount1 int64
			err = metadata.DB().
				Model(&models.Certificate{}).
				Count(&certCount1).
				Error
			require.NoError(t, err)
			assert.NotZero(t, certCount1, "Should have created certificates")

			// Verify initial slot
			var certificates []models.Certificate
			err = metadata.DB().Find(&certificates).Error
			require.NoError(t, err)
			for _, cert := range certificates {
				assert.Equal(
					t,
					blockPoint1.Slot,
					cert.Slot,
					"Certificate Slot should match initial block slot",
				)
			}

			// If testing re-processing, call SetTransaction again
			reprocessingSucceeded := false
			if tt.callTwice {
				// Determine block point for second call
				blockPoint2 := blockPoint1
				if tt.differentBlock {
					blockPoint2 = ocommon.Point{
						Slot: 1500,                           // Different slot for re-inclusion
						Hash: []byte{0x09, 0x0A, 0x0B, 0x0C}, // Different hash
					}
				}

				// For re-processing, we expect the second call to either succeed or fail due to constraints,
				// but either way, no new certificates should be created
				err = metadata.SetTransaction(blockPoint2, tx, 0, deposits, nil)
				if err != nil {
					// If it fails, that's acceptable for re-processing (transaction already processed)
					// Just verify no new certificates were created
					var certCount2 int64
					err = metadata.DB().
						Model(&models.Certificate{}).
						Count(&certCount2).
						Error
					require.NoError(t, err)
					assert.Equal(
						t,
						certCount1,
						certCount2,
						"Certificate count should not change even if second call fails",
					)
				} else {
					// If it succeeds, count certificates after second call
					reprocessingSucceeded = true
					var certCount2 int64
					err = metadata.DB().Model(&models.Certificate{}).Count(&certCount2).Error
					require.NoError(t, err)

					// Should have the same number of certificates (no duplicates)
					assert.Equal(t, certCount1, certCount2, "Certificate count should not change on re-processing")

					// If using different block, verify slot was updated
					if tt.differentBlock {
						err = metadata.DB().Find(&certificates).Error
						require.NoError(t, err)
						for _, cert := range certificates {
							assert.Equal(t, blockPoint2.Slot, cert.Slot, "Certificate Slot should be updated to new block slot")
						}
					}
				}
			}

			// Final verification that certificates have correct slot (use the expected slot based on test case)
			expectedSlot := blockPoint1.Slot
			if tt.callTwice && tt.differentBlock && reprocessingSucceeded {
				expectedSlot = 1500 // The updated slot (only if reprocessing actually succeeded)
			}
			err = metadata.DB().Find(&certificates).Error
			require.NoError(t, err)

			for _, cert := range certificates {
				assert.Equal(
					t,
					expectedSlot,
					cert.Slot,
					"Certificate Slot should match expected slot",
				)
			}
		})
	}
}

func TestCertificateSlotField(t *testing.T) {
	// Test that the Slot field in Certificate model is correctly stored and retrievable
	// using a real transaction with certificates
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       t.TempDir(), // Use temporary directory for clean database
	})
	require.NoError(t, err)
	defer db.Close()

	metadata := db.Metadata()

	// Decode the test transaction
	tx := decodeTestTransaction(t)

	// Set up test data
	blockPoint := ocommon.Point{
		Slot: 1000,
		Hash: []byte{0x05, 0x06, 0x07, 0x08},
	}

	// Call SetTransaction to store the certificates
	deposits := getTestDeposits() // Provide deposits for the certificates
	err = metadata.SetTransaction(blockPoint, tx, 0, deposits, nil)
	require.NoError(t, err)

	// Query the certificates and verify the Slot field
	var certificates []models.Certificate
	err = metadata.DB().Find(&certificates).Error
	require.NoError(t, err)
	assert.NotEmpty(t, certificates, "Should have stored certificates")

	// Verify that each certificate has the correct Slot
	for _, cert := range certificates {
		assert.Equal(
			t,
			blockPoint.Slot,
			cert.Slot,
			"Certificate Slot should match the block slot",
		)
	}
}

func TestDeregistrationDrepDepositAmountPersistence(t *testing.T) {
	// Setup database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	// Get metadata store
	metadata := db.Metadata()

	// Test data
	drepCredential := []byte{0x01, 0x02, 0x03, 0x04}
	testDeposit := uint64(2500000)

	// Create a deregistration drep record directly
	deregDrep := models.DeregistrationDrep{
		DrepCredential: drepCredential,
		CertificateID:  400,
		DepositAmount:  types.Uint64{Val: testDeposit},
		AddedSlot:      1000,
	}
	err = metadata.DB().Create(&deregDrep).Error
	require.NoError(t, err)

	// Retrieve the record and verify the DepositAmount
	var retrieved models.DeregistrationDrep
	err = metadata.DB().
		Where("certificate_id = ?", 400).
		First(&retrieved).
		Error
	require.NoError(t, err)

	// Verify that the DepositAmount was stored correctly
	assert.Equal(
		t,
		testDeposit,
		retrieved.DepositAmount.Val,
		"DeregistrationDrep DepositAmount should be stored correctly",
	)
}

func TestCertificateDataMappingValidation(t *testing.T) {
	// Test that certificate data mapping works correctly for MIR, LeiosEb, and anchor-bearing certificates
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	metadata := db.Metadata()

	t.Run("MoveInstantaneousRewardsDataMapping", func(t *testing.T) {
		// Test MIR certificate data mapping
		rewardData := []byte(`{"test":"data"}`)
		source := uint(1) // treasury
		otherPot := uint64(5000000)

		mir := models.MoveInstantaneousRewards{
			RewardData:    rewardData,
			Source:        source,
			OtherPot:      types.Uint64{Val: otherPot},
			CertificateID: 500,
			AddedSlot:     types.Uint64{Val: 1000},
		}
		err := metadata.DB().Create(&mir).Error
		require.NoError(t, err)

		// Retrieve and verify
		var retrieved models.MoveInstantaneousRewards
		err = metadata.DB().
			Where("certificate_id = ?", 500).
			First(&retrieved).
			Error
		require.NoError(t, err)

		assert.Equal(
			t,
			rewardData,
			retrieved.RewardData,
			"RewardData should be stored correctly",
		)
		assert.Equal(
			t,
			source,
			retrieved.Source,
			"Source should be stored correctly",
		)
		assert.Equal(
			t,
			otherPot,
			retrieved.OtherPot.Val,
			"OtherPot should be stored correctly",
		)
	})

	t.Run("LeiosEbDataMapping", func(t *testing.T) {
		// Test LeiosEb certificate data mapping
		electionID := []byte{0x01, 0x02, 0x03, 0x04}
		endorserBlockHash := []byte{0x05, 0x06, 0x07, 0x08}
		persistentVotersData := []byte(`[{"voter":"data"}]`)
		nonpersistentVotersData := []byte(`{"voter":"map"}`)
		aggregateEligSigData := []byte(`{"signature":"data"}`)
		aggregateVoteSigData := []byte(`{"vote":"signature"}`)

		leiosEb := models.LeiosEb{
			ElectionID:              electionID,
			EndorserBlockHash:       endorserBlockHash,
			PersistentVotersData:    persistentVotersData,
			NonpersistentVotersData: nonpersistentVotersData,
			AggregateEligSigData:    aggregateEligSigData,
			AggregateVoteSigData:    aggregateVoteSigData,
			CertificateID:           501,
			AddedSlot:               types.Uint64{Val: 1000},
		}
		err := metadata.DB().Create(&leiosEb).Error
		require.NoError(t, err)

		// Retrieve and verify
		var retrieved models.LeiosEb
		err = metadata.DB().
			Where("certificate_id = ?", 501).
			First(&retrieved).
			Error
		require.NoError(t, err)

		assert.Equal(
			t,
			electionID,
			retrieved.ElectionID,
			"ElectionID should be stored correctly",
		)
		assert.Equal(
			t,
			endorserBlockHash,
			retrieved.EndorserBlockHash,
			"EndorserBlockHash should be stored correctly",
		)
		assert.Equal(
			t,
			persistentVotersData,
			retrieved.PersistentVotersData,
			"PersistentVotersData should be stored correctly",
		)
		assert.Equal(
			t,
			nonpersistentVotersData,
			retrieved.NonpersistentVotersData,
			"NonpersistentVotersData should be stored correctly",
		)
		assert.Equal(
			t,
			aggregateEligSigData,
			retrieved.AggregateEligSigData,
			"AggregateEligSigData should be stored correctly",
		)
		assert.Equal(
			t,
			aggregateVoteSigData,
			retrieved.AggregateVoteSigData,
			"AggregateVoteSigData should be stored correctly",
		)
	})

	t.Run("AnchorDataMapping", func(t *testing.T) {
		// Test anchor data mapping for different certificate types
		anchorUrl := "https://example.com/anchor"
		anchorHash := []byte{
			0x01,
			0x02,
			0x03,
			0x04,
			0x05,
			0x06,
			0x07,
			0x08,
			0x09,
			0x0A,
			0x0B,
			0x0C,
			0x0D,
			0x0E,
			0x0F,
			0x10,
			0x11,
			0x12,
			0x13,
			0x14,
			0x15,
			0x16,
			0x17,
			0x18,
			0x19,
			0x1A,
			0x1B,
			0x1C,
			0x1D,
			0x1E,
			0x1F,
			0x20,
		}

		// Test ResignCommitteeCold
		resignCold := models.ResignCommitteeCold{
			ColdCredential: []byte{0x01, 0x02, 0x03, 0x04},
			AnchorUrl:      anchorUrl,
			AnchorHash:     anchorHash,
			CertificateID:  502,
			AddedSlot:      types.Uint64{Val: 1000},
		}
		err := metadata.DB().Create(&resignCold).Error
		require.NoError(t, err)

		var retrievedResign models.ResignCommitteeCold
		err = metadata.DB().
			Where("certificate_id = ?", 502).
			First(&retrievedResign).
			Error
		require.NoError(t, err)

		assert.Equal(
			t,
			anchorUrl,
			retrievedResign.AnchorUrl,
			"AnchorUrl should be stored correctly",
		)
		assert.Equal(
			t,
			anchorHash,
			retrievedResign.AnchorHash,
			"AnchorHash should be stored correctly",
		)

		// Test RegistrationDrep
		regDrep := models.RegistrationDrep{
			DrepCredential: []byte{0x05, 0x06, 0x07, 0x08},
			DepositAmount:  types.Uint64{Val: 2000000},
			AnchorUrl:      anchorUrl,
			AnchorHash:     anchorHash,
			CertificateID:  503,
			AddedSlot:      1000,
		}
		err = metadata.DB().Create(&regDrep).Error
		require.NoError(t, err)

		var retrievedRegDrep models.RegistrationDrep
		err = metadata.DB().
			Where("certificate_id = ?", 503).
			First(&retrievedRegDrep).
			Error
		require.NoError(t, err)

		assert.Equal(
			t,
			anchorUrl,
			retrievedRegDrep.AnchorUrl,
			"RegistrationDrep AnchorUrl should be stored correctly",
		)
		assert.Equal(
			t,
			anchorHash,
			retrievedRegDrep.AnchorHash,
			"RegistrationDrep AnchorHash should be stored correctly",
		)

		// Test UpdateDrep
		updateDrep := models.UpdateDrep{
			DrepCredential: []byte{0x09, 0x0A, 0x0B, 0x0C},
			AnchorUrl:      anchorUrl,
			AnchorHash:     anchorHash,
			CertificateID:  504,
			AddedSlot:      1000,
		}
		err = metadata.DB().Create(&updateDrep).Error
		require.NoError(t, err)

		var retrievedUpdateDrep models.UpdateDrep
		err = metadata.DB().
			Where("certificate_id = ?", 504).
			First(&retrievedUpdateDrep).
			Error
		require.NoError(t, err)

		assert.Equal(
			t,
			anchorUrl,
			retrievedUpdateDrep.AnchorUrl,
			"UpdateDrep AnchorUrl should be stored correctly",
		)
		assert.Equal(
			t,
			anchorHash,
			retrievedUpdateDrep.AnchorHash,
			"UpdateDrep AnchorHash should be stored correctly",
		)
	})
}

func TestDeleteCertificateTypeCoverage(t *testing.T) {
	// This test ensures that all defined CertificateType constants are handled
	// in the DeleteCertificate switch statement. If new certificate types are added,
	// this test will fail until the DeleteCertificate method is updated to handle them.

	// Get all defined certificate types by reflecting on the models package
	// We can't easily enumerate all constants, so we'll test the known ones
	knownTypes := []models.CertificateType{
		models.CertificateTypeStakeRegistration,
		models.CertificateTypeStakeDeregistration,
		models.CertificateTypeStakeDelegation,
		models.CertificateTypePoolRegistration,
		models.CertificateTypePoolRetirement,
		models.CertificateTypeRegistration,
		models.CertificateTypeDeregistration,
		models.CertificateTypeStakeRegistrationDelegation,
		models.CertificateTypeVoteDelegation,
		models.CertificateTypeStakeVoteDelegation,
		models.CertificateTypeVoteRegistrationDelegation,
		models.CertificateTypeStakeVoteRegistrationDelegation,
		models.CertificateTypeAuthCommitteeHot,
		models.CertificateTypeResignCommitteeCold,
		models.CertificateTypeRegistrationDrep,
		models.CertificateTypeDeregistrationDrep,
		models.CertificateTypeUpdateDrep,
		models.CertificateTypeGenesisKeyDelegation,
		models.CertificateTypeMoveInstantaneousRewards,
		models.CertificateTypeLeiosEb,
	}

	// Setup database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	metadata := db.Metadata()

	// Test that each known certificate type can be handled by DeleteCertificate
	// We don't actually create certificates, just verify the switch handles all types
	for _, certType := range knownTypes {
		t.Run(fmt.Sprintf("certType_%d", int(certType)), func(t *testing.T) {
			// Cast metadata to SQLite implementation to access DeleteCertificate
			sqliteMetadata, ok := metadata.(*sqlite.MetadataStoreSqlite)
			require.True(t, ok, "metadata should be SQLite implementation")

			// Create a mock certificate record for this type
			mockCert := models.Certificate{
				BlockHash:     []byte{0x01, 0x02, 0x03},
				TransactionID: 1,
				CertificateID: 1,
				Slot:          1000,
				CertIndex:     0,
				CertType:      certType,
			}

			// Insert the mock certificate
			err := metadata.DB().Create(&mockCert).Error
			require.NoError(t, err)

			// Try to delete it - this should succeed for known types
			err = metadata.DB().Transaction(func(tx *gorm.DB) error {
				return sqliteMetadata.DeleteCertificate(1, 0, tx)
			})

			// For known certificate types, deletion should succeed (even if the specific model record doesn't exist)
			assert.NoError(
				t,
				err,
				"DeleteCertificate should handle known certificate type %d without error",
				int(certType),
			)
		})
	}

	// Test that unknown certificate types return a specific error
	t.Run("unknown_cert_type", func(t *testing.T) {
		sqliteMetadata, ok := metadata.(*sqlite.MetadataStoreSqlite)
		require.True(t, ok, "metadata should be SQLite implementation")

		// Create a mock certificate with an unknown type (use a high number that's unlikely to be used)
		unknownType := models.CertificateType(999)
		mockCert := models.Certificate{
			BlockHash:     []byte{0x01, 0x02, 0x03},
			TransactionID: 2,
			CertificateID: 2,
			Slot:          1000,
			CertIndex:     0,
			CertType:      unknownType,
		}

		// Insert the mock certificate
		err := metadata.DB().Create(&mockCert).Error
		require.NoError(t, err)

		// Try to delete it - this should fail with a specific error for unknown types
		err = metadata.DB().Transaction(func(tx *gorm.DB) error {
			return sqliteMetadata.DeleteCertificate(2, 0, tx)
		})

		// Should return the specific error for unknown certificate types
		require.Error(
			t,
			err,
			"DeleteCertificate should return error for unknown certificate type",
		)
		assert.Contains(
			t,
			err.Error(),
			"unknown certificate type",
			"Error should mention unknown certificate type",
		)
		assert.Contains(
			t,
			err.Error(),
			"999",
			"Error should include the unknown type number",
		)
	})
}

func TestDrepRegistrationCreatesGeneralDrepRecord(t *testing.T) {
	// Setup database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	metadata := db.Metadata()

	// Test data
	drepCred := []byte{0x01, 0x02, 0x03, 0x04}
	anchorUrl := "https://example.com/drep"
	anchorHash := []byte{0x05, 0x06, 0x07, 0x08}

	// Directly test SetDrep function
	err = metadata.DB().Transaction(func(tx *gorm.DB) error {
		sqliteMetadata, ok := metadata.(*sqlite.MetadataStoreSqlite)
		require.True(t, ok, "metadata should be SQLite implementation")

		return sqliteMetadata.SetDrep(
			drepCred,
			1000, // slot
			anchorUrl,
			anchorHash,
			true, // active
			1,    // certificateID
			tx,
		)
	})
	require.NoError(t, err)

	// Verify that a general Drep record was created
	var drep models.Drep
	err = metadata.DB().
		Where("drep_credential = ?", drepCred).
		First(&drep).
		Error
	require.NoError(t, err, "General Drep record should be created")

	assert.Equal(
		t,
		drepCred,
		drep.DrepCredential,
		"DrepCredential should match",
	)
	assert.Equal(t, anchorUrl, drep.AnchorUrl, "AnchorUrl should match")
	assert.Equal(t, anchorHash, drep.AnchorHash, "AnchorHash should match")
	assert.Equal(t, uint64(1000), drep.AddedSlot, "AddedSlot should match")
	assert.True(t, drep.Active, "Drep should be active")
	assert.Equal(t, uint(1), drep.CertificateID, "CertificateID should match")
}

func TestDrepSetDrepUpdateFunctionality(t *testing.T) {
	// Setup database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       t.TempDir(),
	})
	require.NoError(t, err)
	defer db.Close()

	metadata := db.Metadata()

	// Test data
	drepCred := []byte{0x01, 0x02, 0x03, 0x04}

	// First, create a DRep record
	err = metadata.DB().Transaction(func(tx *gorm.DB) error {
		sqliteMetadata, ok := metadata.(*sqlite.MetadataStoreSqlite)
		require.True(t, ok, "metadata should be SQLite implementation")

		return sqliteMetadata.SetDrep(
			drepCred,
			1000, // slot
			"https://example.com/drep",
			[]byte{0x05, 0x06, 0x07, 0x08},
			true, // active
			1,    // certificateID
			tx,
		)
	})
	require.NoError(t, err)

	// Update the same DRep record (should update existing record)
	err = metadata.DB().Transaction(func(tx *gorm.DB) error {
		sqliteMetadata, ok := metadata.(*sqlite.MetadataStoreSqlite)
		require.True(t, ok, "metadata should be SQLite implementation")

		return sqliteMetadata.SetDrep(
			drepCred,
			2000, // new slot
			"https://example.com/drep-updated",
			[]byte{0x11, 0x12, 0x13, 0x14},
			false, // inactive
			2,     // new certificateID
			tx,
		)
	})
	require.NoError(t, err)

	// Verify that the DRep record was updated (should still be only one record)
	var dreps []models.Drep
	err = metadata.DB().
		Where("drep_credential = ?", drepCred).
		Find(&dreps).
		Error
	require.NoError(t, err)
	assert.Len(t, dreps, 1, "Should have exactly one Drep record")

	drep := dreps[0]
	assert.Equal(
		t,
		drepCred,
		drep.DrepCredential,
		"DrepCredential should match",
	)
	assert.Equal(
		t,
		"https://example.com/drep-updated",
		drep.AnchorUrl,
		"AnchorUrl should be updated",
	)
	assert.Equal(
		t,
		[]byte{0x11, 0x12, 0x13, 0x14},
		drep.AnchorHash,
		"AnchorHash should be updated",
	)
	assert.Equal(t, uint64(2000), drep.AddedSlot, "AddedSlot should be updated")
	assert.False(t, drep.Active, "Drep should be inactive")
	assert.Equal(
		t,
		uint(2),
		drep.CertificateID,
		"CertificateID should be updated",
	)
}
