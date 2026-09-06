// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package sqlstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// setPointerAddressStakeCredential resolves the stake credential named by a
// pointer address and adds it to the UTxO model. Pointer addresses carry a
// certificate location, not a credential hash, so the address-level
// StakeKeyHash accessor intentionally returns the zero hash.
func setPointerAddressStakeCredential(
	db queryer,
	addr lcommon.Address,
	utxo *models.Utxo,
) error {
	stakingKey, credentialTag, found, err := pointerAddressStakeCredential(
		db,
		addr,
	)
	if err != nil {
		return err
	}
	if found {
		utxo.StakingKey = stakingKey
		utxo.CredentialTag = credentialTag
	}
	return nil
}

func pointerAddressStakeCredential(
	db queryer,
	addr lcommon.Address,
) ([]byte, uint8, bool, error) {
	pointer, ok := addr.StakingPayload().(lcommon.AddressPayloadPointer)
	if !ok {
		return nil, 0, false, nil
	}

	rows, err := db.QueryContext(context.Background(), `
SELECT sr.staking_key, sr.credential_tag
FROM certs c
JOIN "transaction" tx ON tx.id = c.transaction_id
JOIN stake_registration sr ON sr.certificate_id = c.id
WHERE c.slot = ? AND tx.block_index = ? AND c.cert_index = ?
  AND c.cert_type = ?
UNION ALL
SELECT srd.staking_key, srd.credential_tag
FROM certs c
JOIN "transaction" tx ON tx.id = c.transaction_id
JOIN stake_registration_delegation srd ON srd.certificate_id = c.id
WHERE c.slot = ? AND tx.block_index = ? AND c.cert_index = ?
  AND c.cert_type = ?`,
		pointer.Slot,
		pointer.TxIndex,
		pointer.CertIndex,
		uint(lcommon.CertificateTypeStakeRegistration),
		pointer.Slot,
		pointer.TxIndex,
		pointer.CertIndex,
		uint(lcommon.CertificateTypeStakeRegistrationDelegation),
	)
	if err != nil {
		return nil, 0, false, fmt.Errorf(
			"query pointer address registration: %w",
			err,
		)
	}
	defer rows.Close()

	var (
		stakingKey    []byte
		credentialTag uint8
		found         bool
	)
	for rows.Next() {
		if found {
			return nil, 0, false, errors.New(
				"pointer address resolves to multiple registrations",
			)
		}
		if err := rows.Scan(&stakingKey, &credentialTag); err != nil {
			return nil, 0, false, fmt.Errorf(
				"scan pointer address registration: %w",
				err,
			)
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return nil, 0, false, fmt.Errorf(
			"iterate pointer address registrations: %w",
			err,
		)
	}
	if !found {
		// An unresolved pointer is not evidence for any credential. Leave the
		// UTxO unattributed rather than guessing from its payment credential.
		return nil, 0, false, nil
	}
	if len(stakingKey) != lcommon.AddressHashSize {
		return nil, 0, false, fmt.Errorf(
			"pointer address registration has %d-byte staking key, expected %d",
			len(stakingKey),
			lcommon.AddressHashSize,
		)
	}
	if credentialTag > 1 {
		return nil, 0, false, fmt.Errorf(
			"pointer address registration has invalid credential tag %d",
			credentialTag,
		)
	}
	return bytes.Clone(stakingKey), credentialTag, true, nil
}
