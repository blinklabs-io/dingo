package ledgerstate

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

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
func buildShelleyAddr(addrType, networkID byte, paymentHash, stakingHash []byte) []byte {
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
			wantPayKey:   false,
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
			wantPayKey:   false,
			wantStakeKey: true,
			wantStakeTag: 1,
		},
		{
			// Type 5: script payment + pointer staking (enterprise-like min length)
			name:       "type5_script_payment_pointer",
			addr:       buildShelleyAddr(5, 1, payHash, nil),
			wantScript: true,
			wantPayKey: false,
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
			wantPayKey: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := &ParsedUTxO{}
			extractAddressKeys(tc.addr, result)
			require.Equal(t, tc.wantScript, result.PaymentScript, "PaymentScript")
			if tc.wantPayKey {
				require.Equal(t, payHash, result.PaymentKey, "PaymentKey")
			} else {
				require.Empty(t, result.PaymentKey, "PaymentKey should be empty for script payment")
			}
			if tc.wantStakeKey {
				require.Equal(t, stakeHash, result.StakingKey, "StakingKey")
				require.Equal(t, tc.wantStakeTag, result.CredentialTag, "CredentialTag")
			} else {
				require.Empty(t, result.StakingKey, "StakingKey should be empty for non-staking-key address types")
			}
		})
	}
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
