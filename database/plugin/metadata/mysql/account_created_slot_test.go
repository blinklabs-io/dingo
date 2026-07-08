//go:build dingo_extra_plugins

package mysql

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMysqlBackfillAccountCreatedSlotCaseUpdate exercises the non-sqlite CASE
// UPDATE path of models.BackfillAccountCreatedSlot against a real MySQL: a
// pre-existing account with a registration certificate is stamped with the
// earliest registration slot, a genesis-delegated account (no registration)
// keeps created_slot 0, and completion is durably recorded so a rerun is a
// no-op. The shared path inlines the slot literal because Postgres rejects an
// untyped CASE result assigned to the bigint column; this test confirms that
// same path also works against a real MySQL.
func TestMysqlBackfillAccountCreatedSlotCaseUpdate(t *testing.T) {
	store := newTestMysqlStore(t)
	defer store.Close() //nolint:errcheck
	db := store.DB()
	db.Where("1 = 1").Delete(&models.StakeRegistration{})
	db.Where("1 = 1").Delete(&models.Account{})
	db.Where("phase = ?", "account_created_slot").
		Delete(&models.BackfillCheckpoint{})

	reg := bytes.Repeat([]byte{0x71}, 28)
	gen := bytes.Repeat([]byte{0x72}, 28)
	require.NoError(t, db.Create(&[]models.Account{
		{StakingKey: reg, CredentialTag: 0, Active: true, CreatedSlot: 0},
		{StakingKey: gen, CredentialTag: 0, Active: true, CreatedSlot: 0},
	}).Error)
	// Two registrations for reg; the earliest (321) must win.
	require.NoError(t, db.Create(&[]models.StakeRegistration{
		{StakingKey: reg, CredentialTag: 0, AddedSlot: 500},
		{StakingKey: reg, CredentialTag: 0, AddedSlot: 321},
	}).Error)

	require.NoError(t, models.BackfillAccountCreatedSlot(db, nil))

	var got models.Account
	require.NoError(t, db.Where(
		"credential_tag = ? AND staking_key = ?", 0, reg,
	).First(&got).Error)
	assert.Equal(t, uint64(321), got.CreatedSlot)

	var genAcct models.Account
	require.NoError(t, db.Where(
		"credential_tag = ? AND staking_key = ?", 0, gen,
	).First(&genAcct).Error)
	assert.Equal(t, uint64(0), genAcct.CreatedSlot)

	var cp models.BackfillCheckpoint
	require.NoError(t, db.Where(
		"phase = ?", "account_created_slot",
	).First(&cp).Error)
	assert.True(t, cp.Completed)
}
