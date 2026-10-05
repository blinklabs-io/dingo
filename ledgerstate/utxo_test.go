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
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func FuzzDecodeTxInFromBytes(f *testing.F) {
	f.Add([]byte(nil))
	f.Add(make([]byte, 34))
	addTxInArraySeed(f, bytes.Repeat([]byte{0xab}, 32), 7)

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64*1024 {
			t.Skip("TxIn input is too large for fast fuzzing")
		}

		txHash, outputIndex, err := decodeTxInFromBytes(data)
		if err != nil {
			return
		}
		if len(data) == 34 {
			if !bytes.Equal(txHash, data[:32]) {
				t.Fatalf("binary TxIn hash = %x, want %x", txHash, data[:32])
			}
			expectedIndex := uint32(binary.BigEndian.Uint16(data[32:34]))
			if outputIndex != expectedIndex {
				t.Fatalf(
					"binary TxIn index = %d, want %d",
					outputIndex,
					expectedIndex,
				)
			}
		}
	})
}

func FuzzUnwrapCborBytes(f *testing.F) {
	addByteStringSeed(f, []byte(nil))
	addByteStringSeed(f, []byte{0xde, 0xad, 0xbe, 0xef})
	f.Add([]byte{0xd8, 0x18, 0x42, 0x01, 0x02})
	f.Add([]byte(nil))

	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64*1024 {
			t.Skip("CBOR input is too large for fast fuzzing")
		}

		unwrapped, err := unwrapCborBytes(cbor.RawMessage(raw))
		if err != nil {
			return
		}
		if len(unwrapped) > 64*1024 {
			t.Fatalf(
				"unwrapCborBytes returned %d bytes, want <= 65536",
				len(unwrapped),
			)
		}

		encoded, err := cbor.Encode([]byte(unwrapped))
		if err != nil {
			t.Fatalf("cbor.Encode(unwrapped): %v", err)
		}
		roundTrip, err := unwrapCborBytes(cbor.RawMessage(encoded))
		if err != nil {
			t.Fatalf("unwrapCborBytes(re-encoded unwrapped): %v", err)
		}
		if !bytes.Equal(roundTrip, unwrapped) {
			t.Fatalf("re-encoded unwrap = %x, want %x", roundTrip, unwrapped)
		}
	})
}

func addTxInArraySeed(f *testing.F, txHash []byte, outputIndex uint32) {
	data, err := cbor.Encode([]any{txHash, outputIndex})
	if err != nil {
		f.Fatalf("marshal TxIn seed: %v", err)
	}
	f.Add(data)
}

func addByteStringSeed(f *testing.F, data []byte) {
	encoded, err := cbor.Encode(data)
	if err != nil {
		f.Fatalf("marshal byte string seed: %v", err)
	}
	f.Add(encoded)
}

func TestDecodeTxIn_BinaryKeyUsesBigEndianOutputIndex(t *testing.T) {
	t.Parallel()

	txHash := bytes.Repeat([]byte{0x5a}, 32)
	keyBytes := append(append([]byte{}, txHash...), 0x00, 0x01)
	keyRaw, err := cbor.Encode(keyBytes)
	require.NoError(t, err)

	decodedHash, outputIndex, err := decodeTxIn(cbor.RawMessage(keyRaw))
	require.NoError(t, err)
	require.Equal(t, txHash, decodedHash)
	require.Equal(t, uint32(1), outputIndex)
}

// buildShelleyAddr constructs a minimal Shelley address byte slice:
// header (addrType<<4 | networkId), 28-byte payment hash, 28-byte staking hash (for base addrs).
func buildShelleyAddr(
	addrType, networkID byte,
	paymentHash, stakingHash []byte,
) []byte {
	addr := []byte{(addrType << 4) | networkID}
	addr = append(addr, paymentHash...)
	if stakingHash != nil {
		addr = append(addr, stakingHash...)
	}
	return addr
}

func TestExtractAddressKeys_ScriptPaymentTypes(t *testing.T) {
	t.Parallel()

	payHash := bytes.Repeat([]byte{0x11}, 28)
	stakeHash := bytes.Repeat([]byte{0x22}, 28)

	tests := []struct {
		name         string
		addr         []byte
		wantScript   bool
		wantPayKey   bool
		wantStakeKey bool
		wantStakeTag uint8
	}{
		{
			// Type 0: key payment + key staking — NOT script
			name:         "type0_key_payment",
			addr:         buildShelleyAddr(0, 1, payHash, stakeHash),
			wantScript:   false,
			wantPayKey:   true,
			wantStakeKey: true,
			wantStakeTag: 0,
		},
		{
			// Type 1: script payment + key staking
			name:         "type1_script_payment_key_staking",
			addr:         buildShelleyAddr(1, 1, payHash, stakeHash),
			wantScript:   true,
			wantPayKey:   true,
			wantStakeKey: true,
			wantStakeTag: 0,
		},
		{
			// Type 2: key payment + script staking
			name:         "type2_key_payment_script_staking",
			addr:         buildShelleyAddr(2, 1, payHash, stakeHash),
			wantScript:   false,
			wantPayKey:   true,
			wantStakeKey: true,
			wantStakeTag: 1,
		},
		{
			// Type 3: script payment + script staking
			name:         "type3_script_payment_script_staking",
			addr:         buildShelleyAddr(3, 1, payHash, stakeHash),
			wantScript:   true,
			wantPayKey:   true,
			wantStakeKey: true,
			wantStakeTag: 1,
		},
		{
			// Type 5: script payment + pointer staking
			name:       "type5_script_payment_pointer",
			addr:       append(buildShelleyAddr(5, 1, payHash, nil), 0, 0, 0),
			wantScript: true,
			wantPayKey: true,
		},
		{
			// Type 6: enterprise key payment — NOT script
			name:       "type6_enterprise_key",
			addr:       buildShelleyAddr(6, 1, payHash, nil),
			wantScript: false,
			wantPayKey: true,
		},
		{
			// Type 7: enterprise script payment
			name:       "type7_enterprise_script",
			addr:       buildShelleyAddr(7, 1, payHash, nil),
			wantScript: true,
			wantPayKey: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := &ParsedUTxO{}
			require.NoError(t, extractAddressKeys(tc.addr, result))
			require.Equal(
				t,
				tc.wantScript,
				result.PaymentScript,
				"PaymentScript",
			)
			if tc.wantPayKey {
				require.Equal(t, payHash, result.PaymentKey, "PaymentKey")
			} else {
				require.Empty(t, result.PaymentKey, "PaymentKey should be empty for truncated/unknown address types")
			}
			if tc.wantStakeKey {
				require.Equal(t, stakeHash, result.StakingKey, "StakingKey")
				require.Equal(
					t,
					tc.wantStakeTag,
					result.CredentialTag,
					"CredentialTag",
				)
			} else {
				require.Empty(t, result.StakingKey, "StakingKey should be empty for non-staking-key address types")
			}
		})
	}
}

func TestExtractAddressKeysRejectsMalformedPointer(t *testing.T) {
	addr := append(
		[]byte{lcommon.AddressTypeKeyPointer << 4},
		bytes.Repeat([]byte{0xab}, lcommon.AddressHashSize)...,
	)

	result := &ParsedUTxO{}
	err := extractAddressKeys(addr, result)

	require.Error(t, err)
	require.ErrorContains(t, err, "decoding pointer address")
	require.Empty(t, result.PaymentKey)
	require.Empty(t, result.StakingKey)
}

func TestUTxOToModel_PropagatesPaymentScript(t *testing.T) {
	t.Parallel()

	txHash := bytes.Repeat([]byte{0xab}, 32)

	for _, wantScript := range []bool{false, true} {
		u := &ParsedUTxO{
			TxHash:        txHash,
			OutputIndex:   0,
			Amount:        1_000_000,
			PaymentScript: wantScript,
			CredentialTag: 1,
		}
		m := UTxOToModel(u, 100)
		require.Equal(t, wantScript, m.PaymentScript)
		require.Equal(t, uint8(1), m.CredentialTag)
	}
}

func TestExtractAddressKeys_PreservesPointerPosition(t *testing.T) {
	t.Parallel()

	addr := buildShelleyAddr(
		4,
		1,
		bytes.Repeat([]byte{0x11}, 28),
		nil,
	)
	// Pointer components are CBOR-style unsigned variable-length integers.
	addr = append(addr, 100, 2, 3)

	parsed := &ParsedUTxO{}
	extractAddressKeys(addr, parsed)
	require.Equal(t, &models.UtxoPointer{
		Slot: 100, TxIndex: 2, CertIndex: 3,
	}, parsed.Pointer)

	model := UTxOToModel(parsed, 200)
	require.Equal(t, parsed.Pointer, model.Pointer)
}

func TestParseCborTxOut_PreservesPointerPosition(t *testing.T) {
	t.Parallel()

	addr := buildShelleyAddr(
		4,
		1,
		bytes.Repeat([]byte{0x11}, 28),
		nil,
	)
	addr = append(addr, 100, 2, 3)
	txOut, err := cbor.Encode([]any{addr, uint64(1_000_000)})
	require.NoError(t, err)

	parsed, err := parseCborTxOut(
		bytes.Repeat([]byte{0xab}, 32),
		0,
		cbor.RawMessage(txOut),
		cbor.RawMessage(txOut),
	)
	require.NoError(t, err)
	require.Equal(t, &models.UtxoPointer{
		Slot: 100, TxIndex: 2, CertIndex: 3,
	}, parsed.Pointer)
}
