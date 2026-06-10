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

package certutil

import (
	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// PoolRewardAccount extracts the credential tag and pure 28-byte hash from a
// pool registration certificate's reward account.
//
// Background: PoolRegistrationCertificate.RewardAccount is AddrKeyHash
// ([28]byte), but the on-chain CBOR encodes the reward_account field as a
// 29-byte reward address (1 header byte + 28 hash bytes). fxamacker/cbor
// decodes a 29-byte bytestring into [28]byte by taking the first 28 bytes, so
// the struct field contains [header, hash_0..hash_26] — the last hash byte is
// lost and the header byte pollutes the stored value.
//
// This function decodes the raw cert CBOR (available via cert.Cbor()) to
// recover the full 29-byte reward address and returns both the credential type
// (0 = key hash, 1 = script hash) and the pure 28-byte stake credential hash.
//
// Fallback when raw CBOR is unavailable (e.g. genesis/JSON-sourced certs):
// the cert was parsed via UnmarshalJSON which correctly stores the pure
// 28-byte hash in RewardAccount, and only key-hash reward accounts appear in
// genesis, so tag=0 and cert.RewardAccount[:] are returned.
func PoolRewardAccount(
	cert *lcommon.PoolRegistrationCertificate,
) (credentialTag uint8, hash []byte) {
	rawCbor := cert.Cbor()
	if len(rawCbor) > 0 {
		// Pool cert CBOR is a flat array:
		//   [cert_type, operator, vrf_keyhash, pledge, cost, margin,
		//    reward_account, pool_owners, relays, pool_metadata]
		// reward_account is at index 6.
		var raw []gcbor.RawMessage
		if _, err := gcbor.Decode(rawCbor, &raw); err == nil && len(raw) > 6 {
			var rewardAddrBytes []byte
			if _, err := gcbor.Decode(raw[6], &rewardAddrBytes); err == nil &&
				len(rewardAddrBytes) == 29 {
				// Header high nibble: 0xF = script credential, otherwise key.
				if (rewardAddrBytes[0] & 0xF0) == 0xF0 {
					credentialTag = 1
				}
				return credentialTag, rewardAddrBytes[1:] // pure 28-byte hash
			}
		}
	}
	// JSON/genesis path: RewardAccount already holds the pure 28-byte hash
	// and genesis pools always use key-hash reward accounts.
	return 0, cert.RewardAccount[:]
}
