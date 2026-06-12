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

func TestParsePStateSelectsUTxOHDPoolMap(t *testing.T) {
	poolHash := bytes.Repeat([]byte{0x11}, 28)
	vrfHash := bytes.Repeat([]byte{0x22}, 32)
	rewardHash := bytes.Repeat([]byte{0x33}, 28)
	ownerHash := bytes.Repeat([]byte{0x44}, 28)
	metadataHash := bytes.Repeat([]byte{0x55}, 32)

	// UTxO-HD Preview snapshots encode PState with a non-pool map at
	// index 0 and the active pool map at index 1. The active pool map
	// omits the operator from the value because it is already the map key.
	wrongKey := bytes.Repeat([]byte{0x99}, 32)
	wrongMap := encodeCredentialMapEntry(t, wrongKey, uint64(1))
	poolMap := encodeCredentialMapEntry(
		t,
		poolHash,
		[]any{
			vrfHash,
			uint64(500_000_000),
			uint64(340_000_000),
			[]uint64{1, 20},
			[]any{uint64(0), []any{uint64(0), rewardHash}},
			[]any{ownerHash},
			[]any{
				[]any{
					uint64(0),
					uint64(3001),
					[]byte{127, 0, 0, 1},
					nil,
				},
			},
			[]any{[]any{"https://pool.example", metadataHash}},
			uint64(500_000_000),
			[]any{},
		},
	)
	emptyMap, err := cbor.Encode(map[uint64]uint64{})
	if err != nil {
		t.Fatalf("encoding empty map: %v", err)
	}

	pstate, err := cbor.Encode([]any{
		cbor.RawMessage(wrongMap),
		cbor.RawMessage(poolMap),
		cbor.RawMessage(emptyMap),
		cbor.RawMessage(emptyMap),
	})
	if err != nil {
		t.Fatalf("encoding PState: %v", err)
	}

	pools, err := parsePState(pstate)
	if err != nil {
		t.Fatalf("parsePState failed: %v", err)
	}
	if len(pools) != 1 {
		t.Fatalf("expected 1 pool, got %d", len(pools))
	}

	pool := pools[0]
	if !bytes.Equal(pool.PoolKeyHash, poolHash) {
		t.Fatalf("pool hash mismatch: %x", pool.PoolKeyHash)
	}
	if !bytes.Equal(pool.VrfKeyHash, vrfHash) {
		t.Fatalf("vrf hash mismatch: %x", pool.VrfKeyHash)
	}
	if pool.Pledge != 500_000_000 {
		t.Fatalf("pledge mismatch: %d", pool.Pledge)
	}
	if pool.Cost != 340_000_000 {
		t.Fatalf("cost mismatch: %d", pool.Cost)
	}
	if pool.MarginNum != 1 || pool.MarginDen != 20 {
		t.Fatalf("margin mismatch: %d/%d", pool.MarginNum, pool.MarginDen)
	}
	if !bytes.Equal(pool.RewardAccount, rewardHash) {
		t.Fatalf("reward account mismatch: %x", pool.RewardAccount)
	}
	if pool.RewardAccountCredentialTag != 0 {
		t.Fatalf(
			"expected reward account credential tag 0, got %d",
			pool.RewardAccountCredentialTag,
		)
	}
	if len(pool.Owners) != 1 || !bytes.Equal(pool.Owners[0], ownerHash) {
		t.Fatalf("owners mismatch: %#v", pool.Owners)
	}
	if len(pool.Relays) != 1 || pool.Relays[0].Port != 3001 {
		t.Fatalf("relays mismatch: %#v", pool.Relays)
	}
	if pool.MetadataUrl != "https://pool.example" ||
		!bytes.Equal(pool.MetadataHash, metadataHash) {
		t.Fatalf(
			"metadata mismatch: url=%q hash=%x",
			pool.MetadataUrl,
			pool.MetadataHash,
		)
	}
	if pool.Deposit != 500_000_000 {
		t.Fatalf("deposit mismatch: %d", pool.Deposit)
	}
}

// TestParseRewardAccountNormalizesAddressBytes verifies full reward
// addresses are stored as 28-byte hashes plus their credential tag.
func TestParseRewardAccountNormalizesAddressBytes(t *testing.T) {
	rewardHash := bytes.Repeat([]byte{0x62}, 28)

	cases := []struct {
		name    string
		account []byte
		wantTag uint8
	}{
		{
			name:    "key reward address",
			account: append([]byte{0xe0}, rewardHash...),
			wantTag: 0,
		},
		{
			name:    "script reward address",
			account: append([]byte{0xf0}, rewardHash...),
			wantTag: 1,
		},
		{
			name:    "legacy hash only",
			account: rewardHash,
			wantTag: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := cbor.Encode(tc.account)
			if err != nil {
				t.Fatalf("encoding reward account: %v", err)
			}

			gotHash, gotTag, ok := parseRewardAccount(data)
			if !ok {
				t.Fatal("expected reward account to parse")
			}
			if !bytes.Equal(gotHash, rewardHash) {
				t.Fatalf("reward hash mismatch: %x", gotHash)
			}
			if gotTag != tc.wantTag {
				t.Fatalf("expected tag %d, got %d", tc.wantTag, gotTag)
			}
		})
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
