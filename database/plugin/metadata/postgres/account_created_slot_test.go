//go:build dingo_extra_plugins

package postgres

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresBackfillAccountCreatedSlotCaseUpdate(t *testing.T) {
	store := newTestPostgresStore(t)
	defer store.Close() //nolint:errcheck
	db := store.DB()
	for _, history := range []any{
		&models.StakeRegistration{},
		&models.StakeRegistrationDelegation{},
		&models.StakeVoteRegistrationDelegation{},
		&models.VoteRegistrationDelegation{},
		&models.Registration{},
		&models.StakeDelegation{},
		&models.StakeVoteDelegation{},
		&models.VoteDelegation{},
	} {
		require.NoError(t, db.Where("1 = 1").Delete(history).Error)
	}
	require.NoError(t, db.Where("1 = 1").Delete(&models.Account{}).Error)
	require.NoError(t, db.Where("phase = ?", "account_created_slot").
		Delete(&models.BackfillCheckpoint{}).Error)

	registered := bytes.Repeat([]byte{0x71}, 28)
	genesis := bytes.Repeat([]byte{0x72}, 28)
	require.NoError(t, db.Create(&[]models.Account{
		{StakingKey: registered, CertificateID: 1, Active: true},
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
