//go:build dingo_extra_plugins

package mysql

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMysqlBackfillAccountCreatedSlotCaseUpdate(t *testing.T) {
	store := newTestMysqlStore(t)
	defer store.Close() //nolint:errcheck
	db := store.DB()
	db.Where("1 = 1").Delete(&models.StakeRegistration{})
	db.Where("1 = 1").Delete(&models.Account{})
	db.Where("phase = ?", "account_created_slot").
		Delete(&models.BackfillCheckpoint{})

	registered := bytes.Repeat([]byte{0x71}, 28)
	genesis := bytes.Repeat([]byte{0x72}, 28)
	require.NoError(t, db.Create(&[]models.Account{
		{StakingKey: registered, Active: true},
		{StakingKey: genesis, Active: true},
	}).Error)
	require.NoError(t, db.Create(&[]models.StakeRegistration{
		{StakingKey: registered, AddedSlot: 500},
		{StakingKey: registered, AddedSlot: 321},
	}).Error)
	require.NoError(t, models.BackfillAccountCreatedSlot(db, nil))

	var account models.Account
	require.NoError(t, db.Where(
		"credential_tag = ? AND staking_key = ?", 0, registered,
	).First(&account).Error)
	assert.Equal(t, uint64(321), account.CreatedSlot)

	var genesisAccount models.Account
	require.NoError(t, db.Where(
		"credential_tag = ? AND staking_key = ?", 0, genesis,
	).First(&genesisAccount).Error)
	assert.Zero(t, genesisAccount.CreatedSlot)
}
