// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eras

import (
	"bytes"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

type dijkstraBlsKeyTx struct {
	lcommon.Transaction
	certs []lcommon.Certificate
}

func (t dijkstraBlsKeyTx) Certificates() []lcommon.Certificate {
	return t.certs
}

func (dijkstraBlsKeyTx) IsValid() bool { return true }

type dijkstraBlsKeyState struct {
	*mockLedgerState
	key   []byte
	owner lcommon.PoolKeyHash
}

func (s dijkstraBlsKeyState) IsBlsKeyInUse(
	key []byte,
) (bool, lcommon.PoolKeyHash, error) {
	return bytes.Equal(key, s.key), s.owner, nil
}

func TestDijkstraPoolRegistrationRejectsAnotherPoolsBlsKey(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x31}, 96)
	owner := lcommon.PoolKeyHash{0x41}
	registrant := lcommon.PoolKeyHash{0x42}
	tx := dijkstraBlsKeyTx{certs: []lcommon.Certificate{
		&lcommon.PoolRegistrationCertificate{
			Operator: registrant,
			LeiosKey: &lcommon.LeiosKey{
				PublicKey:       key,
				PossessionProof: bytes.Repeat([]byte{0x51}, 48),
			},
		},
	}}
	state := dijkstraBlsKeyState{
		mockLedgerState: newMockLedgerState(),
		key:             key,
		owner:           owner,
	}

	err := validateDijkstraPoolBlsKeyUniqueness(tx, state)
	require.ErrorContains(t, err, "BLS key is already registered")
}

func TestDijkstraPoolRegistrationAllowsPoolsOwnBlsKey(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x31}, 96)
	pool := lcommon.PoolKeyHash{0x41}
	tx := dijkstraBlsKeyTx{certs: []lcommon.Certificate{
		&lcommon.PoolRegistrationCertificate{
			Operator: pool,
			LeiosKey: &lcommon.LeiosKey{
				PublicKey:       key,
				PossessionProof: bytes.Repeat([]byte{0x52}, 48),
			},
		},
	}}
	state := dijkstraBlsKeyState{
		mockLedgerState: newMockLedgerState(),
		key:             key,
		owner:           pool,
	}

	require.NoError(t, validateDijkstraPoolBlsKeyUniqueness(tx, state))
}

func TestDijkstraPoolRegistrationRejectsDuplicateBlsKeyInTransaction(
	t *testing.T,
) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x31}, 96)
	tx := dijkstraBlsKeyTx{certs: []lcommon.Certificate{
		&lcommon.PoolRegistrationCertificate{
			Operator: lcommon.PoolKeyHash{0x41},
			LeiosKey: &lcommon.LeiosKey{
				PublicKey:       key,
				PossessionProof: bytes.Repeat([]byte{0x51}, 48),
			},
		},
		&lcommon.PoolRegistrationCertificate{
			Operator: lcommon.PoolKeyHash{0x42},
			LeiosKey: &lcommon.LeiosKey{
				PublicKey:       key,
				PossessionProof: bytes.Repeat([]byte{0x52}, 48),
			},
		},
	}}
	state := dijkstraBlsKeyState{mockLedgerState: newMockLedgerState()}

	err := validateDijkstraPoolBlsKeyUniqueness(tx, state)
	require.ErrorContains(t, err, "BLS key is already registered")
}
