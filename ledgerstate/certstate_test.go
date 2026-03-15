// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ledgerstate

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
)

type testCredentialKey struct {
	cbor.StructAsArray
	Type uint64
	Hash [28]byte
}

type testPtr struct {
	cbor.StructAsArray
	Slot    uint64
	TxIndex uint64
	Cert    uint64
}

func TestParseCredentialMapConwayAccountState(t *testing.T) {
	stakingKey := bytes.Repeat([]byte{0x11}, 28)
	poolHash := bytes.Repeat([]byte{0x22}, 28)
	drepHash := bytes.Repeat([]byte{0x33}, 28)

	data := encodeCredentialMapEntry(
		t,
		testCredentialKey{
			Type: 0,
			Hash: toFixed28(stakingKey),
		},
		[]any{
			uint64(101),
			uint64(202),
			poolHash,
			[]any{uint64(0), drepHash},
		},
	)

	accounts, err := parseCredentialMap(data)
	if err != nil {
		t.Fatalf("parseCredentialMap failed: %v", err)
	}
	if len(accounts) != 1 {
		t.Fatalf("expected 1 account, got %d", len(accounts))
	}

	acct := accounts[0]
	if !bytes.Equal(acct.StakingKey.Hash, stakingKey) {
		t.Fatalf("staking key mismatch: %x", acct.StakingKey.Hash)
	}
	if acct.Reward != 101 {
		t.Fatalf("expected reward 101, got %d", acct.Reward)
	}
	if acct.Deposit != 202 {
		t.Fatalf("expected deposit 202, got %d", acct.Deposit)
	}
	if !bytes.Equal(acct.PoolKeyHash, poolHash) {
		t.Fatalf("pool hash mismatch: %x", acct.PoolKeyHash)
	}
	if acct.DRepCred.Type != CredentialTypeKey {
		t.Fatalf("expected key drep, got type %d", acct.DRepCred.Type)
	}
	if !bytes.Equal(acct.DRepCred.Hash, drepHash) {
		t.Fatalf("drep hash mismatch: %x", acct.DRepCred.Hash)
	}
}

func TestParseCredentialMapShelleyAccountState(t *testing.T) {
	stakingKey := bytes.Repeat([]byte{0x44}, 28)
	poolHash := bytes.Repeat([]byte{0x55}, 28)

	data := encodeCredentialMapEntry(
		t,
		testCredentialKey{
			Type: 0,
			Hash: toFixed28(stakingKey),
		},
		[]any{
			testPtr{Slot: 1, TxIndex: 2, Cert: 3},
			uint64(303),
			uint64(404),
			poolHash,
		},
	)

	accounts, err := parseCredentialMap(data)
	if err != nil {
		t.Fatalf("parseCredentialMap failed: %v", err)
	}
	if len(accounts) != 1 {
		t.Fatalf("expected 1 account, got %d", len(accounts))
	}

	acct := accounts[0]
	if acct.Reward != 303 {
		t.Fatalf("expected reward 303, got %d", acct.Reward)
	}
	if acct.Deposit != 404 {
		t.Fatalf("expected deposit 404, got %d", acct.Deposit)
	}
	if !bytes.Equal(acct.PoolKeyHash, poolHash) {
		t.Fatalf("pool hash mismatch: %x", acct.PoolKeyHash)
	}
}

func TestParseCredentialMapLegacyUMElem(t *testing.T) {
	stakingKey := bytes.Repeat([]byte{0x66}, 28)
	poolHash := bytes.Repeat([]byte{0x77}, 28)

	data := encodeCredentialMapEntry(
		t,
		testCredentialKey{
			Type: 0,
			Hash: toFixed28(stakingKey),
		},
		[]any{
			[]uint64{505, 606},
			poolHash,
			[]any{uint64(2)},
		},
	)

	accounts, err := parseCredentialMap(data)
	if err != nil {
		t.Fatalf("parseCredentialMap failed: %v", err)
	}
	if len(accounts) != 1 {
		t.Fatalf("expected 1 account, got %d", len(accounts))
	}

	acct := accounts[0]
	if acct.Reward != 505 {
		t.Fatalf("expected reward 505, got %d", acct.Reward)
	}
	if acct.Deposit != 606 {
		t.Fatalf("expected deposit 606, got %d", acct.Deposit)
	}
	if !bytes.Equal(acct.PoolKeyHash, poolHash) {
		t.Fatalf("pool hash mismatch: %x", acct.PoolKeyHash)
	}
	if acct.DRepCred.Type != CredentialTypeAbstain {
		t.Fatalf("expected abstain drep, got type %d", acct.DRepCred.Type)
	}
}

func encodeCredentialMapEntry(t *testing.T, key any, value any) []byte {
	t.Helper()

	keyRaw, err := cbor.Encode(key)
	if err != nil {
		t.Fatalf("encoding key: %v", err)
	}
	valueRaw, err := cbor.Encode(value)
	if err != nil {
		t.Fatalf("encoding value: %v", err)
	}

	data := append([]byte{0xa1}, keyRaw...)
	data = append(data, valueRaw...)
	return data
}

func toFixed28(src []byte) [28]byte {
	var dst [28]byte
	copy(dst[:], src)
	return dst
}
