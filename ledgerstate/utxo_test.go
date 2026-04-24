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
