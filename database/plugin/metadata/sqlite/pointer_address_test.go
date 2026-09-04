// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package sqlite

import (
	"bytes"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestSetTransactionAttributesPointerAddressStake(t *testing.T) {
	store, raw := newSharedSQLStore(t)
	stakeKey := lcommon.NewBlake2b224(bytes.Repeat([]byte{0x11}, 28))
	registrationPoint := ocommon.Point{
		Slot: 10,
		Hash: bytes.Repeat([]byte{0x12}, 32),
	}

	// Pointer addresses identify a registration by its block slot, transaction
	// index, and certificate index. Use a non-zero transaction index so the
	// lookup cannot accidentally succeed by matching only the slot.
	registration := &mockTransaction{
		hash:    lcommon.NewBlake2b256(bytes.Repeat([]byte{0x13}, 32)),
		isValid: true,
		certificates: []lcommon.Certificate{
			&lcommon.StakeRegistrationCertificate{
				CertType: uint(lcommon.CertificateTypeStakeRegistration),
				StakeCredential: lcommon.Credential{
					CredType:   0,
					Credential: stakeKey,
				},
			},
		},
	}
	deposits := map[int]uint64{0: 0}
	require.NoError(t, store.SetTransaction(
		registration,
		registrationPoint,
		3,
		deposits,
		false,
		nil,
	))

	paymentKey := bytes.Repeat([]byte{0x21}, lcommon.AddressHashSize)
	pointerAddressBytes := append(
		[]byte{lcommon.AddressTypeKeyPointer << 4},
		paymentKey...,
	)
	pointerAddressBytes = append(pointerAddressBytes, 0x0a, 0x03, 0x00)
	pointerAddress, err := lcommon.NewAddressFromBytes(pointerAddressBytes)
	require.NoError(t, err)
	transactionHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x22}, 32))
	produced := lcommon.Utxo{
		Id: mockTransactionInput{hash: transactionHash, index: 0},
		Output: &shelley.ShelleyTransactionOutput{
			OutputAddress: pointerAddress,
			OutputAmount:  123,
		},
	}
	pointerTransaction := &mockTransaction{
		hash:     transactionHash,
		isValid:  true,
		produced: []lcommon.Utxo{produced},
	}
	require.NoError(t, store.SetTransaction(
		pointerTransaction,
		ocommon.Point{Slot: 20, Hash: bytes.Repeat([]byte{0x23}, 32)},
		0,
		nil,
		false,
		nil,
	))

	stored, err := store.GetUtxo(transactionHash.Bytes(), 0, nil)
	require.NoError(t, err)
	require.Equal(t, stakeKey.Bytes(), stored.StakingKey)
	require.Equal(t, uint8(0), stored.CredentialTag)

	var utxoStake string
	require.NoError(t, raw.QueryRow(
		`SELECT utxo_stake FROM reward_live_stake
         WHERE credential_tag = ? AND staking_key = ?`,
		0,
		stakeKey.Bytes(),
	).Scan(&utxoStake))
	require.Equal(t, "123", utxoStake)
}
