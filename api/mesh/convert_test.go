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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mesh

import (
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestAdaCurrency(t *testing.T) {
	c := adaCurrency()
	require.Equal(t, "ADA", c.Symbol)
	require.Equal(t, int32(6), c.Decimals)
	require.Nil(t, c.Metadata)
}

func TestNativeAssetCurrency(t *testing.T) {
	policy := hexString(testKeyHash(0xaa))
	name := hexString([]byte("token"))

	c := nativeAssetCurrency(policy, name)

	require.Equal(t, name, c.Symbol)
	require.Equal(t, int32(0), c.Decimals)
	require.Equal(t, policy, c.Metadata["policyId"])
	require.Equal(t, name, c.Metadata["assetName"])
}

func TestConvertAmount(t *testing.T) {
	require.Equal(t, "0", convertAmount(0).Value)
	require.Equal(
		t,
		"18446744073709551615",
		convertAmount(^uint64(0)).Value,
	)
}

func TestConvertAssetAmount(t *testing.T) {
	policy := testKeyHash(0xab)
	name := []byte("tok")

	amt := convertAssetAmount(policy, name, 25)

	require.Equal(t, "25", amt.Value)
	require.Equal(t, hexString(name), amt.Currency.Symbol)
	require.Equal(
		t, hexString(policy), amt.Currency.Metadata["policyId"],
	)
}

func TestTxStatus(t *testing.T) {
	require.Equal(t, StatusSuccess, *txStatus(true))
	require.Equal(t, StatusInvalid, *txStatus(false))
}

// TestUtxoAddressCredentialCombinations covers the CIP-19 address types
// reconstructed from the payment/staking credentials stored with a UTxO.
// A wrong mapping here silently reports coins under the wrong address.
func TestUtxoAddressCredentialCombinations(t *testing.T) {
	paymentKey := testKeyHash(0x30)
	stakingKey := testKeyHash(0x31)

	tests := map[string]struct {
		utxo      models.Utxo
		wantType  uint8
		wantStake bool
	}{
		"key payment, no stake": {
			utxo:     models.Utxo{PaymentKey: paymentKey},
			wantType: lcommon.AddressTypeKeyNone,
		},
		"script payment, no stake": {
			utxo: models.Utxo{
				PaymentKey:    paymentKey,
				PaymentScript: true,
			},
			wantType: lcommon.AddressTypeScriptNone,
		},
		"key payment, key stake": {
			utxo: models.Utxo{
				PaymentKey: paymentKey,
				StakingKey: stakingKey,
			},
			wantType:  lcommon.AddressTypeKeyKey,
			wantStake: true,
		},
		"key payment, script stake": {
			utxo: models.Utxo{
				PaymentKey:    paymentKey,
				StakingKey:    stakingKey,
				CredentialTag: 1,
			},
			wantType:  lcommon.AddressTypeKeyScript,
			wantStake: true,
		},
		"script payment, key stake": {
			utxo: models.Utxo{
				PaymentKey:    paymentKey,
				StakingKey:    stakingKey,
				PaymentScript: true,
			},
			wantType:  lcommon.AddressTypeScriptKey,
			wantStake: true,
		},
		"script payment, script stake": {
			utxo: models.Utxo{
				PaymentKey:    paymentKey,
				StakingKey:    stakingKey,
				PaymentScript: true,
				CredentialTag: 1,
			},
			wantType:  lcommon.AddressTypeScriptScript,
			wantStake: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := utxoAddress(
				&tc.utxo, lcommon.AddressNetworkTestnet,
			)

			var stake []byte
			if tc.wantStake {
				stake = stakingKey
			}
			require.Equal(
				t,
				testAddress(
					t, tc.wantType, paymentKey, stake,
				),
				got,
			)
			// Round-tripping through the parser must recover the
			// same address, proving the encoding is well formed.
			parsed, err := lcommon.NewAddress(got)
			require.NoError(t, err)
			require.Equal(t, got, parsed.String())
		})
	}
}

// TestUtxoAddressByronPlaceholder covers Byron-era outputs, which carry
// no payment key hash: the Mesh spec requires a non-empty address, so a
// distinguishable placeholder is returned instead.
func TestUtxoAddressByronPlaceholder(t *testing.T) {
	txID := testHash(0x32)

	got := utxoAddress(
		&models.Utxo{TxId: txID},
		lcommon.AddressNetworkTestnet,
	)

	require.Equal(t, "byron:"+hexString(txID), got)
	require.True(t, strings.HasPrefix(got, "byron:"))
}

// TestUtxoAddressFallbackOnInvalidCredential covers a malformed payment
// key: the converter must degrade to the hex hash rather than panic.
func TestUtxoAddressFallbackOnInvalidCredential(t *testing.T) {
	short := []byte{0x01, 0x02}

	got := utxoAddress(
		&models.Utxo{PaymentKey: short},
		lcommon.AddressNetworkTestnet,
	)

	require.Equal(t, hexString(short), got)
}

// TestUtxoAddressNetworkPrefix asserts the configured network selects
// the bech32 prefix, so mainnet coins are never reported under testnet
// addresses.
func TestUtxoAddressNetworkPrefix(t *testing.T) {
	utxo := models.Utxo{PaymentKey: testKeyHash(0x33)}

	testnet := utxoAddress(
		&utxo, lcommon.AddressNetworkTestnet,
	)
	mainnet := utxoAddress(
		&utxo, lcommon.AddressNetworkMainnet,
	)

	require.True(t, strings.HasPrefix(testnet, "addr_test1"))
	require.True(t, strings.HasPrefix(mainnet, "addr1"))
	require.NotEqual(t, testnet, mainnet)
}

func TestConvertBlockGenesisParent(t *testing.T) {
	hash := testHash(0x34)

	block := convertBlock(
		models.Block{Hash: hash, Number: 0, Slot: 0},
		nil,
		lcommon.AddressNetworkTestnet,
		func(slot uint64) int64 { return int64(slot) * 1000 },
	)

	require.Equal(
		t,
		&BlockIdentifier{Index: 0, Hash: hexString(hash)},
		block.BlockIdentifier,
	)
	require.Equal(
		t, block.BlockIdentifier, block.ParentBlockIdentifier,
	)
	require.Equal(t, int64(0), block.Timestamp)
	require.Empty(t, block.Transactions)
}

func TestConvertBlockParent(t *testing.T) {
	hash := testHash(0x35)
	prev := testHash(0x36)

	block := convertBlock(
		models.Block{
			Hash: hash, PrevHash: prev, Number: 10, Slot: 20,
		},
		nil,
		lcommon.AddressNetworkTestnet,
		func(slot uint64) int64 { return int64(slot) * 1000 },
	)

	require.Equal(
		t,
		&BlockIdentifier{Index: 9, Hash: hexString(prev)},
		block.ParentBlockIdentifier,
	)
	require.Equal(t, int64(20000), block.Timestamp)
}

// TestConvertTransactionInvalidStatus covers phase-2 failures: every
// operation must carry the "invalid" status so clients do not credit
// balances from a failed transaction.
func TestConvertTransactionInvalidStatus(t *testing.T) {
	txHash := testHash(0x37)
	paymentKey := testKeyHash(0x38)

	tx := convertTransaction(
		models.Transaction{
			Hash:  txHash,
			Valid: false,
			Inputs: []models.Utxo{
				testUtxo(
					testHash(0x39), 0, 1_000_000,
					paymentKey, nil,
				),
			},
			Outputs: []models.Utxo{
				testUtxo(txHash, 0, 900_000, paymentKey, nil),
			},
		},
		lcommon.AddressNetworkTestnet,
	)

	require.Len(t, tx.Operations, 2)
	for _, op := range tx.Operations {
		require.NotNil(t, op.Status)
		require.Equal(t, StatusInvalid, *op.Status)
	}
}

// TestConvertTransactionAssetOperations covers native assets: each
// asset becomes its own operation with a sub-coin identifier, and the
// operation indices stay contiguous across ADA and asset entries.
func TestConvertTransactionAssetOperations(t *testing.T) {
	txHash := testHash(0x3a)
	inputTxID := testHash(0x3b)
	paymentKey := testKeyHash(0x3c)
	policy := testKeyHash(0xcd)

	tx := convertTransaction(
		models.Transaction{
			Hash:  txHash,
			Valid: true,
			Inputs: []models.Utxo{
				testUtxo(
					inputTxID, 1, 5_000_000, paymentKey,
					[]models.Asset{
						testAsset(
							policy, []byte("tok"), 4,
						),
					},
				),
			},
			Outputs: []models.Utxo{
				testUtxo(
					txHash, 0, 4_500_000, paymentKey,
					[]models.Asset{
						testAsset(
							policy, []byte("tok"), 4,
						),
					},
				),
			},
		},
		lcommon.AddressNetworkTestnet,
	)

	require.Len(t, tx.Operations, 4)
	for i, op := range tx.Operations {
		require.Equal(
			t, int64(i), op.OperationIdentifier.Index,
		)
	}
	// Inputs are negated; outputs are not.
	require.Equal(t, "-5000000", tx.Operations[0].Amount.Value)
	require.Equal(t, "-4", tx.Operations[1].Amount.Value)
	require.Equal(t, "4500000", tx.Operations[2].Amount.Value)
	require.Equal(t, "4", tx.Operations[3].Amount.Value)
	// Asset operations reference the owning UTxO as a sub-coin.
	require.Equal(
		t,
		hexString(inputTxID)+":1:"+hexString(policy)+":"+
			hexString([]byte("tok")),
		tx.Operations[1].CoinChange.CoinIdentifier.Identifier,
	)
	require.Equal(t, CoinSpent, tx.Operations[0].CoinChange.CoinAction)
	require.Equal(
		t, CoinCreated, tx.Operations[2].CoinChange.CoinAction,
	)
	// Only the primary ADA input operation is related to outputs, so
	// clients do not double-count asset sub-operations.
	require.Equal(
		t,
		[]*OperationIdentifier{{Index: 0}},
		tx.Operations[2].RelatedOperations,
	)
	require.Nil(t, tx.Operations[0].RelatedOperations)
}

// TestConvertBodyToOpsWithdrawalsAreSorted covers the deterministic
// ordering of withdrawal operations. Go map iteration is randomized, so
// without sorting the operation indices would differ between calls for
// the same transaction.
func TestConvertBodyToOpsWithdrawalsAreSorted(t *testing.T) {
	addrs := make([]*lcommon.Address, 0, 3)
	for _, b := range []byte{0x03, 0x01, 0x02} {
		addr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeKeyNone,
			lcommon.AddressNetworkTestnet,
			testKeyHash(b),
			nil,
		)
		require.NoError(t, err)
		addrs = append(addrs, &addr)
	}
	withdrawals := map[*lcommon.Address]*big.Int{
		addrs[0]: big.NewInt(30),
		addrs[1]: big.NewInt(10),
		addrs[2]: big.NewInt(20),
	}

	var first []string
	for range 8 {
		ops := convertBodyToOps(
			nil, nil, nil, withdrawals, "",
		)
		require.Len(t, ops, 3)
		got := make([]string, 0, len(ops))
		for i, op := range ops {
			require.Equal(t, OpWithdrawal, op.Type)
			require.Equal(
				t, int64(i), op.OperationIdentifier.Index,
			)
			got = append(got, op.Account.Address)
		}
		if first == nil {
			first = got
			require.True(
				t,
				got[0] < got[1] && got[1] < got[2],
				"withdrawals not sorted: %v", got,
			)
			continue
		}
		require.Equal(t, first, got)
	}
}

// certFixtures returns one real certificate value per certificate type
// the ledger defines, so the mapping below is exercised against the
// same types that arrive from a decoded transaction.
func certFixtures() map[lcommon.CertificateType]gledger.Certificate {
	ct := func(t lcommon.CertificateType) uint { return uint(t) }
	return map[lcommon.CertificateType]gledger.Certificate{
		lcommon.CertificateTypeStakeRegistration: &lcommon.StakeRegistrationCertificate{
			CertType: ct(lcommon.CertificateTypeStakeRegistration),
		},
		lcommon.CertificateTypeStakeDeregistration: &lcommon.StakeDeregistrationCertificate{
			CertType: ct(lcommon.CertificateTypeStakeDeregistration),
		},
		lcommon.CertificateTypeStakeDelegation: &lcommon.StakeDelegationCertificate{
			CertType: ct(lcommon.CertificateTypeStakeDelegation),
		},
		lcommon.CertificateTypePoolRegistration: &lcommon.PoolRegistrationCertificate{
			CertType: ct(lcommon.CertificateTypePoolRegistration),
		},
		lcommon.CertificateTypePoolRetirement: &lcommon.PoolRetirementCertificate{
			CertType: ct(lcommon.CertificateTypePoolRetirement),
		},
		lcommon.CertificateTypeGenesisKeyDelegation: &lcommon.GenesisKeyDelegationCertificate{
			CertType: ct(lcommon.CertificateTypeGenesisKeyDelegation),
		},
		lcommon.CertificateTypeMoveInstantaneousRewards: &lcommon.MoveInstantaneousRewardsCertificate{
			CertType: ct(lcommon.CertificateTypeMoveInstantaneousRewards),
		},
		lcommon.CertificateTypeRegistration: &lcommon.RegistrationCertificate{
			CertType: ct(lcommon.CertificateTypeRegistration),
		},
		lcommon.CertificateTypeDeregistration: &lcommon.DeregistrationCertificate{
			CertType: ct(lcommon.CertificateTypeDeregistration),
		},
		lcommon.CertificateTypeVoteDelegation: &lcommon.VoteDelegationCertificate{
			CertType: ct(lcommon.CertificateTypeVoteDelegation),
		},
		lcommon.CertificateTypeStakeVoteDelegation: &lcommon.StakeVoteDelegationCertificate{
			CertType: ct(lcommon.CertificateTypeStakeVoteDelegation),
		},
		lcommon.CertificateTypeStakeRegistrationDelegation: &lcommon.StakeRegistrationDelegationCertificate{
			CertType: ct(lcommon.CertificateTypeStakeRegistrationDelegation),
		},
		lcommon.CertificateTypeVoteRegistrationDelegation: &lcommon.VoteRegistrationDelegationCertificate{
			CertType: ct(lcommon.CertificateTypeVoteRegistrationDelegation),
		},
		lcommon.CertificateTypeStakeVoteRegistrationDelegation: &lcommon.StakeVoteRegistrationDelegationCertificate{
			CertType: ct(
				lcommon.CertificateTypeStakeVoteRegistrationDelegation,
			),
		},
		lcommon.CertificateTypeAuthCommitteeHot: &lcommon.AuthCommitteeHotCertificate{
			CertType: ct(lcommon.CertificateTypeAuthCommitteeHot),
		},
		lcommon.CertificateTypeResignCommitteeCold: &lcommon.ResignCommitteeColdCertificate{
			CertType: ct(lcommon.CertificateTypeResignCommitteeCold),
		},
		lcommon.CertificateTypeRegistrationDrep: &lcommon.RegistrationDrepCertificate{
			CertType: ct(lcommon.CertificateTypeRegistrationDrep),
		},
		lcommon.CertificateTypeDeregistrationDrep: &lcommon.DeregistrationDrepCertificate{
			CertType: ct(lcommon.CertificateTypeDeregistrationDrep),
		},
		lcommon.CertificateTypeUpdateDrep: &lcommon.UpdateDrepCertificate{
			CertType: ct(lcommon.CertificateTypeUpdateDrep),
		},
	}
}

// TestCertToOpType pins the certificate-to-operation mapping. The
// operation type is part of the Mesh contract, and unsupported
// certificates must map to the empty string so they are dropped rather
// than reported under a wrong type.
func TestCertToOpType(t *testing.T) {
	want := map[lcommon.CertificateType]string{
		lcommon.CertificateTypeStakeRegistration:   OpStakeKeyRegistration,
		lcommon.CertificateTypeRegistration:        OpStakeKeyRegistration,
		lcommon.CertificateTypeStakeDeregistration: OpStakeKeyDeregistration,
		lcommon.CertificateTypeDeregistration:      OpStakeKeyDeregistration,
		lcommon.CertificateTypeStakeDelegation:     OpStakeDelegation,
		lcommon.CertificateTypePoolRegistration:    OpPoolRegistration,
		lcommon.CertificateTypePoolRetirement:      OpPoolRetirement,
		lcommon.CertificateTypeVoteDelegation:      OpVoteDRepDelegation,
	}

	for certType, cert := range certFixtures() {
		require.Equal(
			t,
			want[certType],
			certToOpType(cert),
			"certificate type %d", certType,
		)
	}
}

// TestCertToOpTypeUnknown covers a certificate type outside the known
// range, which must not be reported as an operation.
func TestCertToOpTypeUnknown(t *testing.T) {
	require.Equal(
		t,
		"",
		certToOpType(&lcommon.StakeRegistrationCertificate{
			CertType: 250,
		}),
	)
}

// TestOperationTypesCoverCertificateMapping asserts every operation type
// the certificate mapping can produce is advertised by
// /network/options, so clients can recognize it.
func TestOperationTypesCoverCertificateMapping(t *testing.T) {
	advertised := make(map[string]struct{})
	for _, opType := range OperationTypes() {
		advertised[opType] = struct{}{}
	}
	mapped := 0
	for certType, cert := range certFixtures() {
		opType := certToOpType(cert)
		if opType == "" {
			continue
		}
		mapped++
		require.Contains(
			t, advertised, opType,
			"certificate type %d maps to unadvertised "+
				"operation %q", certType, opType,
		)
	}
	require.Positive(t, mapped)
}

// TestConvertBodyToOpsSkipsUnmappedCertificates asserts certificates
// without a Mesh operation type produce no operation and do not consume
// an operation index, keeping indices contiguous.
func TestConvertBodyToOpsSkipsUnmappedCertificates(t *testing.T) {
	fixtures := certFixtures()
	certs := []gledger.Certificate{
		fixtures[lcommon.CertificateTypeStakeRegistration],
		fixtures[lcommon.CertificateTypeAuthCommitteeHot],
		fixtures[lcommon.CertificateTypeStakeDelegation],
	}

	ops := convertBodyToOps(nil, nil, certs, nil, "")

	require.Len(t, ops, 2)
	require.Equal(t, OpStakeKeyRegistration, ops[0].Type)
	require.Equal(t, int64(0), ops[0].OperationIdentifier.Index)
	require.Equal(t, OpStakeDelegation, ops[1].Type)
	require.Equal(t, int64(1), ops[1].OperationIdentifier.Index)
}

func TestDecodeTxCborErrors(t *testing.T) {
	tests := map[string]string{
		"non-hex":     "zz",
		"empty":       "",
		"not a tx":    "a10101",
		"truncated":   "84",
		"wrong shape": "820102",
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			tx, meshErr := decodeTxCbor(input)

			require.Nil(t, tx)
			require.NotNil(t, meshErr)
			require.Equal(
				t, ErrInvalidTransaction.Code, meshErr.Code,
			)
			require.NotEmpty(t, meshErr.Details["error"])
		})
	}
}

func TestDecodeTxCborSuccess(t *testing.T) {
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x3d), nil,
	)
	txCbor, want := testSimpleSignedTx(t, addr)

	tx, meshErr := decodeTxCbor(hexString(txCbor))

	require.Nil(t, meshErr)
	require.NotNil(t, tx)
	require.Equal(t, want.Hash().String(), tx.Hash().String())
}
