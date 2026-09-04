package sqlstore

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The real pointer address that wedged a Preview replay at epoch 111, and the
// base address sharing its payment credential.
const (
	ptrAddrBech32 = "addr_test1gzgj6rad2h398mvgv59zcnrrq0x2adcftl6647ukcp7masupkwaszqgqjupejx"
	baseAddrBech  = "addr_test1qzgj6rad2h398mvgv59zcnrrq0x2adcftl6647ukcp7mascvjpyjh0n7kvlns9ej2hj50hp3jj4ua3wd98xl2p9mfupsl8h34q"
	ptrCredHex    = "0c90492bbe7eb33f38173255e547dc3194abcec5cd29cdf504bb4f03"
	ptrSlot       = 2_940_289
	ptrTxIndex    = 1
	ptrCertIndex  = 0
)

// newPointerStore builds a store with just the three tables the pointer
// lookup joins. newTestStore starts empty, so each test owns its schema.
func newPointerStore(t *testing.T) *Store {
	t.Helper()
	s := newTestStore(t)
	for _, ddl := range []string{
		`CREATE TABLE "transaction" (id INTEGER PRIMARY KEY, slot INTEGER, block_index INTEGER)`,
		`CREATE TABLE certs (id INTEGER PRIMARY KEY, transaction_id INTEGER, certificate_id INTEGER, slot INTEGER, cert_index INTEGER, cert_type INTEGER)`,
		`CREATE TABLE stake_registration (id INTEGER PRIMARY KEY AUTOINCREMENT, staking_key BLOB, credential_tag INTEGER NOT NULL DEFAULT 0, certificate_id INTEGER, added_slot INTEGER)`,
	} {
		_, err := s.writeDB.Exec(ddl)
		require.NoError(t, err)
	}
	return s
}

func seedPointerTarget(
	t *testing.T,
	s *Store,
	txID, certID int64,
	slot, txIndex, certIndex uint64,
	cred []byte,
) {
	t.Helper()
	exec := func(q string, args ...any) {
		_, err := s.writeDB.Exec(q, args...)
		require.NoError(t, err)
	}
	exec(`INSERT INTO "transaction" (id, slot, block_index) VALUES (?, ?, ?)`,
		txID, slot, txIndex)
	exec(`INSERT INTO certs (id, transaction_id, slot, cert_index, cert_type) VALUES (?, ?, ?, ?, ?)`,
		certID, txID, slot, certIndex, 0)
	exec(`INSERT INTO stake_registration (staking_key, credential_tag, certificate_id, added_slot) VALUES (?, ?, ?, ?)`,
		cred, 0, certID, slot)
}

// TestResolvePointerStakeCredential is the dingo #3854 regression, built from
// the account that wedged a Preview replay at epoch 111.
//
// A pointer address carries the position of a stake registration certificate
// rather than a credential, so gouroboros reports an empty StakeKeyHash for it
// and the output was stored with a NULL staking_key. Its value then never
// reached the stake distribution, understating the producing pool's stake and
// tightening its leader threshold until the node rejected a canonical block.
func TestResolvePointerStakeCredential(t *testing.T) {
	cred, err := hex.DecodeString(ptrCredHex)
	require.NoError(t, err)
	ptrAddr, err := lcommon.NewAddress(ptrAddrBech32)
	require.NoError(t, err)
	require.IsType(t, lcommon.AddressPayloadPointer{}, ptrAddr.StakingPayload(),
		"fixture must actually be a pointer address")

	ctx := context.Background()

	t.Run("resolves to the credential registered at that position", func(t *testing.T) {
		s := newPointerStore(t)
		seedPointerTarget(t, s, 286907, 1993, ptrSlot, ptrTxIndex, ptrCertIndex, cred)

		model := &models.Utxo{}
		require.NoError(t, s.resolvePointerStakeCredential(
			ctx, s.writeDB, ptrAddr, model,
		))
		assert.Equal(t, cred, model.StakingKey,
			"pointer stake must reach the credential it designates")
		assert.Equal(t, uint8(0), model.CredentialTag)
	})

	t.Run("a pointer naming no certificate stays unattributed", func(t *testing.T) {
		s := newPointerStore(t)
		// Nothing seeded: the pointer dangles.
		model := &models.Utxo{}
		require.NoError(t, s.resolvePointerStakeCredential(
			ctx, s.writeDB, ptrAddr, model,
		),
			"a dangling pointer must not fail block application")
		assert.Empty(t, model.StakingKey,
			"stake that points at no registration is not counted")
	})

	t.Run("a non-pointer address is not resolved", func(t *testing.T) {
		s := newPointerStore(t)
		// Seed a target that WOULD resolve, so a pass cannot come from the
		// lookup simply finding nothing.
		seedPointerTarget(t, s, 286907, 1993, ptrSlot, ptrTxIndex, ptrCertIndex, cred)
		baseAddr, err := lcommon.NewAddress(baseAddrBech)
		require.NoError(t, err)
		require.NotNil(t, baseAddr.StakingPayload())
		_, isPointer := baseAddr.StakingPayload().(lcommon.AddressPayloadPointer)
		require.False(t, isPointer, "fixture must not be a pointer address")

		// Deliberately empty: the first guard is not what is under test here,
		// so the address payload has to be what stops the resolution.
		model := &models.Utxo{}
		require.NoError(t, s.resolvePointerStakeCredential(
			ctx, s.writeDB, baseAddr, model,
		))
		assert.Empty(t, model.StakingKey,
			"a base address carries its own credential and must not be "+
				"attributed from an unrelated pointer target")
	})

	t.Run("an already-attributed model is left alone", func(t *testing.T) {
		s := newPointerStore(t)
		other, err := hex.DecodeString(
			"bb11223344556677889900aabbccddeeff00112233445566778899bb",
		)
		require.NoError(t, err)
		seedPointerTarget(t, s, 286907, 1993, ptrSlot, ptrTxIndex, ptrCertIndex, cred)

		// A pointer address whose model already has a credential: the guard,
		// not the address, is what must stop this one.
		model := &models.Utxo{StakingKey: other}
		require.NoError(t, s.resolvePointerStakeCredential(
			ctx, s.writeDB, ptrAddr, model,
		))
		assert.Equal(t, other, model.StakingKey,
			"an existing credential must not be overwritten by the pointer")
	})

	t.Run("the tx index disambiguates two registrations in one slot", func(t *testing.T) {
		s := newPointerStore(t)
		other, err := hex.DecodeString(
			"aa11223344556677889900aabbccddeeff00112233445566778899aa",
		)
		require.NoError(t, err)
		// The pointer names tx index 1; seed a decoy at index 0 first.
		seedPointerTarget(t, s, 286906, 1992, ptrSlot, 0, ptrCertIndex, other)
		seedPointerTarget(t, s, 286907, 1993, ptrSlot, ptrTxIndex, ptrCertIndex, cred)

		model := &models.Utxo{}
		require.NoError(t, s.resolvePointerStakeCredential(
			ctx, s.writeDB, ptrAddr, model,
		))
		assert.Equal(t, cred, model.StakingKey,
			"two registrations in one slot are told apart by tx index")
	})
}

// TestUtxoConflictRepairFillsStakeCredential covers the other half of #3854.
//
// Block application inserts produced outputs with ON CONFLICT DO NOTHING, so an
// output that a snapshot import already created keeps whatever it was imported
// with. An imported row carries no credential, and the resolution performed
// when the producing transaction is applied would be discarded by the conflict,
// leaving pointer stake unattributed on exactly the nodes that bootstrap from a
// snapshot.
//
// The repair is a COALESCE, so it fills an absent credential and never
// overwrites one. credential_tag is NOT NULL and cannot be COALESCEd, so it is
// gated on the stake key being absent -- which relies on SQLite evaluating
// every SET expression against the pre-update row.
func TestUtxoConflictRepairFillsStakeCredential(t *testing.T) {
	const repair = `
UPDATE utxo
SET credential_tag = CASE
        WHEN staking_key IS NULL AND ? IS NOT NULL THEN ?
        ELSE credential_tag
    END,
    staking_key = COALESCE(staking_key, ?)
WHERE id = ?`

	newUtxoStore := func(t *testing.T) *Store {
		t.Helper()
		s := newTestStore(t)
		_, err := s.writeDB.Exec(
			`CREATE TABLE utxo (id INTEGER PRIMARY KEY AUTOINCREMENT,
			 staking_key BLOB, credential_tag INTEGER NOT NULL DEFAULT 0)`,
		)
		require.NoError(t, err)
		return s
	}
	resolved, err := hex.DecodeString(ptrCredHex)
	require.NoError(t, err)
	existing, err := hex.DecodeString(
		"cc11223344556677889900aabbccddeeff00112233445566778899cc",
	)
	require.NoError(t, err)

	read := func(t *testing.T, s *Store) ([]byte, uint8) {
		t.Helper()
		var key []byte
		var tag uint8
		require.NoError(t,
			s.writeDB.QueryRow(`SELECT staking_key, credential_tag FROM utxo WHERE id = 1`).
				Scan(&key, &tag))
		return key, tag
	}

	t.Run("an imported row gains the resolved credential", func(t *testing.T) {
		s := newUtxoStore(t)
		_, err := s.writeDB.Exec(
			`INSERT INTO utxo (id, staking_key, credential_tag) VALUES (1, NULL, 0)`)
		require.NoError(t, err)

		_, err = s.writeDB.Exec(repair, resolved, 1, resolved, 1)
		require.NoError(t, err)

		key, tag := read(t, s)
		assert.Equal(t, resolved, key, "an absent credential must be filled")
		assert.Equal(t, uint8(1), tag, "its tag must be filled with it")
	})

	t.Run("an attributed row is not overwritten", func(t *testing.T) {
		s := newUtxoStore(t)
		_, err := s.writeDB.Exec(
			`INSERT INTO utxo (id, staking_key, credential_tag) VALUES (1, ?, 0)`,
			existing)
		require.NoError(t, err)

		_, err = s.writeDB.Exec(repair, resolved, 1, resolved, 1)
		require.NoError(t, err)

		key, tag := read(t, s)
		assert.Equal(t, existing, key, "an existing credential must survive")
		assert.Equal(t, uint8(0), tag,
			"and its tag with it, since the CASE reads the pre-update row")
	})

	t.Run("nothing to fill leaves the row alone", func(t *testing.T) {
		s := newUtxoStore(t)
		_, err := s.writeDB.Exec(
			`INSERT INTO utxo (id, staking_key, credential_tag) VALUES (1, NULL, 0)`)
		require.NoError(t, err)

		_, err = s.writeDB.Exec(repair, nil, 1, nil, 1)
		require.NoError(t, err)

		key, tag := read(t, s)
		assert.Nil(t, key)
		assert.Equal(t, uint8(0), tag)
	})
}
