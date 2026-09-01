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

func TestParsePStateDijkstraLeiosKeyField(t *testing.T) {
	poolHash := bytes.Repeat([]byte{0x61}, 28)
	vrfHash := bytes.Repeat([]byte{0x62}, 32)
	rewardHash := bytes.Repeat([]byte{0x63}, 28)
	ownerHash := bytes.Repeat([]byte{0x64}, 28)
	leiosKey := []any{
		bytes.Repeat([]byte{0x65}, 96),
		bytes.Repeat([]byte{0x66}, 48),
	}

	for _, tc := range []struct {
		name      string
		leiosKey  any
		wantError bool
	}{
		{name: "registered key", leiosKey: leiosKey},
		{name: "explicit null", leiosKey: nil},
		{
			name: "malformed key",
			leiosKey: []any{
				bytes.Repeat([]byte{0x65}, 95),
				bytes.Repeat([]byte{0x66}, 48),
			},
			wantError: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			poolMap := encodeCredentialMapEntry(
				t,
				poolHash,
				[]any{
					vrfHash,
					tc.leiosKey,
					uint64(500_000_000),
					uint64(340_000_000),
					[]uint64{1, 20},
					[]any{uint64(0), []any{uint64(0), rewardHash}},
					[]any{ownerHash},
					[]any{},
					nil,
					uint64(500_000_000),
					[]any{},
				},
			)
			pstate, err := cbor.Encode([]any{
				map[uint64]uint64{},
				cbor.RawMessage(poolMap),
				map[uint64]uint64{},
				map[uint64]uint64{},
			})
			if err != nil {
				t.Fatalf("encoding PState: %v", err)
			}

			pools, err := parsePState(pstate)
			if tc.wantError {
				if err == nil {
					t.Fatal("expected malformed Leios key to fail")
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePState failed: %v", err)
			}
			if len(pools) != 1 {
				t.Fatalf("expected 1 pool, got %d", len(pools))
			}

			pool := pools[0]
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
				t.Fatalf(
					"margin mismatch: %d/%d",
					pool.MarginNum,
					pool.MarginDen,
				)
			}
			if !bytes.Equal(pool.RewardAccount, rewardHash) {
				t.Fatalf(
					"reward account mismatch: %x",
					pool.RewardAccount,
				)
			}
			if len(pool.Owners) != 1 ||
				!bytes.Equal(pool.Owners[0], ownerHash) {
				t.Fatalf("owners mismatch: %#v", pool.Owners)
			}
			if pool.Deposit != 500_000_000 {
				t.Fatalf("deposit mismatch: %d", pool.Deposit)
			}
		})
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

func TestParseCommitteeVStatePreservesTaggedAuthorizations(t *testing.T) {
	keyHash := bytes.Repeat([]byte{0x11}, 28)
	scriptHash := bytes.Repeat([]byte{0x22}, 28)
	hotHash := bytes.Repeat([]byte{0x33}, 28)
	keyCredential, err := cbor.Encode([]any{uint64(0), keyHash})
	if err != nil {
		t.Fatal(err)
	}
	scriptCredential, err := cbor.Encode([]any{uint64(1), scriptHash})
	if err != nil {
		t.Fatal(err)
	}
	hotCredential, err := cbor.Encode([]any{uint64(1), hotHash})
	if err != nil {
		t.Fatal(err)
	}
	hotMap := append([]byte{0xa1}, keyCredential...)
	hotMap = append(hotMap, hotCredential...)
	resignMap := append([]byte{0xa1}, scriptCredential...)
	resignMap = append(resignMap, 0xf5)
	hotKeys, resignations := parseCommitteeVState(
		[][]byte{hotMap, resignMap},
	)
	if len(hotKeys) != 1 || len(resignations) != 1 {
		t.Fatalf("unexpected committee state: %d authorizations, %d resignations", len(hotKeys), len(resignations))
	}
	if hotKeys[0].Cold.Type != CredentialTypeKey || hotKeys[0].Hot.Type != CredentialTypeScript {
		t.Fatalf("credential tags were not preserved: %#v", hotKeys[0])
	}
	if resignations[0].Type != CredentialTypeScript {
		t.Fatalf("resignation credential tag was not preserved: %#v", resignations[0])
	}
}

func toFixed28(src []byte) [28]byte {
	var dst [28]byte
	copy(dst[:], src)
	return dst
}

// committeeVStateFixture builds a committee hot-key map and resignation map
// with distinguishable credential tags.
func committeeVStateFixture(t *testing.T) (hotMap, resignMap []byte) {
	t.Helper()
	keyHash := bytes.Repeat([]byte{0x44}, 28)
	scriptHash := bytes.Repeat([]byte{0x55}, 28)
	hotHash := bytes.Repeat([]byte{0x66}, 28)
	keyCredential, err := cbor.Encode([]any{uint64(0), keyHash})
	if err != nil {
		t.Fatal(err)
	}
	scriptCredential, err := cbor.Encode([]any{uint64(1), scriptHash})
	if err != nil {
		t.Fatal(err)
	}
	hotCredential, err := cbor.Encode([]any{uint64(0), hotHash})
	if err != nil {
		t.Fatal(err)
	}
	hotMap = append([]byte{0xa1}, keyCredential...)
	hotMap = append(hotMap, hotCredential...)
	resignMap = append([]byte{0xa1}, scriptCredential...)
	resignMap = append(resignMap, 0xf5)
	return hotMap, resignMap
}

// A nested committee state followed by a dormant-epoch field must still be
// unwrapped; the field count cannot be the signal.
func TestParseCommitteeVStateUnwrapsNestedStateWithTrailingFields(t *testing.T) {
	hotMap, resignMap := committeeVStateFixture(t)
	nested := append([]byte{0x82}, hotMap...)
	nested = append(nested, resignMap...)

	hotKeys, resignations := parseCommitteeVState(
		// [committeeState, dormantEpoch]
		[][]byte{nested, {0x00}},
	)
	if len(hotKeys) != 1 || len(resignations) != 1 {
		t.Fatalf(
			"nested committee state was dropped: %d authorizations, %d resignations",
			len(hotKeys),
			len(resignations),
		)
	}
	if hotKeys[0].Cold.Type != CredentialTypeKey {
		t.Fatalf("cold credential tag not preserved: %#v", hotKeys[0])
	}
	if resignations[0].Type != CredentialTypeScript {
		t.Fatalf("resignation tag not preserved: %#v", resignations[0])
	}
}

// The flattened Conway CertState inlines the VState fields, so committee state
// must be recovered there too rather than silently dropped.
func TestParseCertStateConwayRecoversCommitteeState(t *testing.T) {
	hotMap, resignMap := committeeVStateFixture(t)
	poolState := []byte{0x87, 0xa0, 0xa0, 0xa0, 0xa0, 0xa0, 0xa0, 0xa0}

	drepHash := bytes.Repeat([]byte{0x88}, 28)
	drepCredential, err := cbor.Encode([]any{uint64(0), drepHash})
	if err != nil {
		t.Fatal(err)
	}
	drepMap := append([]byte{0xa1}, drepCredential...)
	drepMap = append(drepMap, 0x80)

	// DState must be the largest credential-keyed map so it is identified
	// ahead of the DRep and committee maps.
	dstate := []byte{0xa2}
	for _, tag := range []byte{0x77, 0x78} {
		delegatorCredential, err := cbor.Encode(
			[]any{uint64(0), bytes.Repeat([]byte{tag}, 28)},
		)
		if err != nil {
			t.Fatal(err)
		}
		dstate = append(dstate, delegatorCredential...)
		dstate = append(dstate, 0x80)
	}

	certState := [][]byte{
		drepMap,
		hotMap,
		resignMap,
		poolState,
		dstate,
		{0x00},
	}
	result, err := parseCertStateConway(certState)
	if err != nil {
		t.Logf("parse warnings: %v", err)
	}
	if result == nil {
		t.Fatal("no parsed cert state")
	}
	if len(result.CommitteeHotKeys) != 1 {
		t.Fatalf(
			"committee authorizations were dropped: %#v",
			result.CommitteeHotKeys,
		)
	}
	if len(result.CommitteeResignations) != 1 {
		t.Fatalf(
			"committee resignations were dropped: %#v",
			result.CommitteeResignations,
		)
	}
	if result.CommitteeHotKeys[0].Cold.Type != CredentialTypeKey {
		t.Fatalf(
			"cold credential tag not preserved: %#v",
			result.CommitteeHotKeys[0],
		)
	}
	if result.CommitteeResignations[0].Type != CredentialTypeScript {
		t.Fatalf(
			"resignation tag not preserved: %#v",
			result.CommitteeResignations[0],
		)
	}
}
