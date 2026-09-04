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

	t.Run("a base address is left alone", func(t *testing.T) {
		s := newPointerStore(t)
		seedPointerTarget(t, s, 286907, 1993, ptrSlot, ptrTxIndex, ptrCertIndex, cred)
		baseAddr, err := lcommon.NewAddress(baseAddrBech)
		require.NoError(t, err)

		// The base address already carries its credential, so the resolver
		// must not touch it -- and must not spend a query on it.
		model := &models.Utxo{StakingKey: cred}
		require.NoError(t, s.resolvePointerStakeCredential(
			ctx, s.writeDB, baseAddr, model,
		))
		assert.Equal(t, cred, model.StakingKey)
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
