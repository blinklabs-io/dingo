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

package eras

import (
	"bytes"
	"math/big"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/common/script"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newConwayRewardAddress builds a CIP-0019 reward account for the given
// credential type.
func newConwayRewardAddress(
	t *testing.T,
	addrType uint8,
	stakeHash []byte,
) *lcommon.Address {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		addrType,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeHash,
	)
	require.NoError(t, err)
	return &addr
}

// TestConwayWithdrawalOrderPlacesScriptCredentialFirst pins the Rewarding
// redeemer index for a transaction that withdraws from both a key credential
// and a script credential. cardano-ledger keys withdrawals by RewardAccount,
// ordered by Credential, whose constructors are declared ScriptHashObj before
// KeyHashObj, so the script withdrawal takes index 0. Reward address bytes put
// the key-hash header (0xe0) before the script-hash header (0xf0), which is the
// opposite order.
func TestConwayWithdrawalOrderPlacesScriptCredentialFirst(t *testing.T) {
	plutusScript := lcommon.PlutusV2Script([]byte{0x01, 0x02})
	scriptHash := plutusScript.Hash()
	keyHash := make([]byte, lcommon.AddressHashSize)
	keyHash[0] = 0xaa

	scriptReward := newConwayRewardAddress(
		t,
		lcommon.AddressTypeNoneScript,
		scriptHash.Bytes(),
	)
	keyReward := newConwayRewardAddress(
		t,
		lcommon.AddressTypeNoneKey,
		keyHash,
	)

	// The two orderings only disagree when the raw bytes rank the key
	// credential first. Assert that premise so the test cannot pass because
	// both orderings happen to agree.
	scriptBytes, err := scriptReward.Bytes()
	require.NoError(t, err)
	keyBytes, err := keyReward.Bytes()
	require.NoError(t, err)
	require.Negative(t, bytes.Compare(keyBytes, scriptBytes))

	bothWithdrawals := map[*lcommon.Address]*big.Int{
		keyReward:    big.NewInt(1),
		scriptReward: big.NewInt(1),
	}

	newTx := func(
		withdrawals map[*lcommon.Address]*big.Int,
		redeemers lcommon.TransactionWitnessRedeemers,
	) *mockConwayFeeTx {
		return &mockConwayFeeTx{
			mockFeeTx: mockFeeTx{
				txType: txTypeAlonzo,
				witnesses: &mockWitnessSet{
					plutusV2Scripts: []lcommon.PlutusV2Script{plutusScript},
					redeemers:       redeemers,
				},
			},
			withdrawals: withdrawals,
		}
	}

	validate := func(
		withdrawals map[*lcommon.Address]*big.Int,
		redeemers lcommon.TransactionWitnessRedeemers,
	) error {
		return ValidateTxPlutusConway(
			newTx(withdrawals, redeemers),
			0,
			newMockLedgerState(),
			&conway.ConwayProtocolParameters{},
		)
	}

	t.Run("sort order", func(t *testing.T) {
		sorted := sortedConwayWithdrawalAddresses(bothWithdrawals)
		require.Len(t, sorted, 2)
		assert.Same(t, scriptReward, sorted[0])
		assert.Same(t, keyReward, sorted[1])
	})

	t.Run("required redeemer index", func(t *testing.T) {
		err := validate(bothWithdrawals, nil)
		var missing conway.MissingRedeemerForScriptError
		require.ErrorAs(t, err, &missing)
		assert.Equal(t, scriptHash, missing.ScriptHash)
		assert.Equal(t, lcommon.RedeemerTagReward, missing.Tag)
		assert.Equal(t, uint32(0), missing.Index)
	})

	t.Run(
		"every index maps back to the withdrawal it demanded",
		func(t *testing.T) {
			// script.BuildScriptPurpose turns a redeemer key back into the
			// credential the script is evaluated against, so an index the
			// required-redeemer check demands has to resolve to the same
			// script through it. The two use separate orderings, and this is
			// where they have to agree.
			sorted := sortedConwayWithdrawalAddresses(bothWithdrawals)
			checked := 0
			for idx, addr := range sorted {
				cred, ok := addr.StakeCredential()
				require.True(t, ok)
				if cred.CredType != lcommon.CredentialTypeScriptHash {
					continue
				}
				purpose, err := script.BuildScriptPurpose(
					lcommon.RedeemerKey{
						Tag:   lcommon.RedeemerTagReward,
						Index: uint32(idx), // #nosec G115 -- two entries
					},
					nil,
					nil,
					lcommon.MultiAsset[lcommon.MultiAssetTypeMint]{},
					nil,
					bothWithdrawals,
					nil,
					nil,
					nil,
				)
				require.NoError(t, err)
				rewarding, ok := purpose.(script.ScriptPurposeRewarding)
				require.True(t, ok)
				assert.Equal(
					t,
					uint(lcommon.CredentialTypeScriptHash),
					rewarding.StakeCredential.CredType,
				)
				assert.Equal(
					t,
					lcommon.ScriptHash(cred.Credential),
					purpose.ScriptHash(),
				)
				checked++
			}
			require.Equal(t, 1, checked)
		},
	)

	t.Run(
		"supplying the redeemer at that index satisfies the requirement",
		func(t *testing.T) {
			redeemers := &mockRedeemers{
				entries: []struct {
					key lcommon.RedeemerKey
					val lcommon.RedeemerValue
				}{
					{
						key: lcommon.RedeemerKey{
							Tag:   lcommon.RedeemerTagReward,
							Index: 0,
						},
					},
				},
			}
			err := validate(bothWithdrawals, redeemers)
			// Phase-2 still runs and the stub script cannot be evaluated, but
			// the redeemer must no longer be reported missing and the purpose
			// must not resolve to some other hash.
			var missing conway.MissingRedeemerForScriptError
			assert.NotErrorAs(t, err, &missing)
			var missingWitness lcommon.MissingScriptWitnessesError
			assert.NotErrorAs(t, err, &missingWitness)
			var extra conway.ExtraRedeemerError
			assert.NotErrorAs(t, err, &extra)
		},
	)

	t.Run("control: a single script withdrawal", func(t *testing.T) {
		// One withdrawal leaves no ordering choice, so this subtest holds
		// under either comparator.
		err := validate(map[*lcommon.Address]*big.Int{
			scriptReward: big.NewInt(1),
		}, nil)
		var missing conway.MissingRedeemerForScriptError
		require.ErrorAs(t, err, &missing)
		assert.Equal(t, scriptHash, missing.ScriptHash)
		assert.Equal(t, uint32(0), missing.Index)
	})

	t.Run(
		"control: a single key withdrawal needs no redeemer",
		func(t *testing.T) {
			require.NoError(t, validate(map[*lcommon.Address]*big.Int{
				keyReward: big.NewInt(1),
			}, nil))
		},
	)
}
