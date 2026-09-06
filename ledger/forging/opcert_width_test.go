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

package forging

import (
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// decodeCborArrayElementUint decodes one element of a CBOR array as an
// unsigned integer, without going through either header body struct. The
// point of these tests is what dingo puts on the wire, so reading it back
// through the same struct that wrote it would assert nothing.
func decodeCborArrayElementUint(
	t *testing.T,
	encoded []byte,
	index int,
) uint64 {
	t.Helper()
	var elements []cbor.RawMessage
	_, err := cbor.Decode(encoded, &elements)
	require.NoError(t, err)
	require.Greater(t, len(elements), index)
	var value uint64
	_, err = cbor.Decode(elements[index], &value)
	require.NoError(t, err)
	return value
}

// TestTPraosHeaderBodyEncodesOpCertBeyondUint32 pins the width of the
// operational certificate fields dingo writes into a TPraos-era header body.
//
// cardano-ledger decodes the counter as Word64 and the KES period as
// KESPeriod{Word} with no bound, and the CDDL declares both uint .size 8, so
// a header body whose fields are uint32 truncates a certificate the chain
// accepts. The counter and period here are one past math.MaxUint32, which is
// the first value a uint32 field cannot hold.
func TestTPraosHeaderBodyEncodesOpCertBeyondUint32(t *testing.T) {
	const (
		sequenceNumber = uint64(math.MaxUint32) + 1
		kesPeriod      = uint64(math.MaxUint32) + 7
	)
	body := tpraosHeaderBody{
		BlockNumber:          101,
		Slot:                 1001,
		IssuerVkey:           lcommon.IssuerVkey{},
		VrfKey:               make([]byte, 32),
		BlockBodySize:        4,
		BlockBodyHash:        lcommon.Blake2b256{},
		OpCertHotVkey:        make([]byte, 32),
		OpCertSequenceNumber: sequenceNumber,
		OpCertKesPeriod:      kesPeriod,
		OpCertSignature:      make([]byte, 64),
		ProtoMajorVersion:    2,
		ProtoMinorVersion:    0,
	}
	encoded, err := cbor.Encode(body)
	require.NoError(t, err)

	// The TPraos header body is a flat 15-element array; the operational
	// certificate occupies elements 9 through 12.
	assert.Equal(
		t,
		sequenceNumber,
		decodeCborArrayElementUint(t, encoded, 10),
	)
	assert.Equal(
		t,
		kesPeriod,
		decodeCborArrayElementUint(t, encoded, 11),
	)
}

// TestPraosOpCertEncodesCounterBeyondUint32 is the same contract for the
// nested operational_cert array a Praos-era header body carries.
func TestPraosOpCertEncodesCounterBeyondUint32(t *testing.T) {
	const (
		sequenceNumber = uint64(math.MaxUint32) + 1
		kesPeriod      = uint64(math.MaxUint32) + 7
	)
	encoded, err := cbor.Encode(praosOpCert{
		HotVkey:        make([]byte, 32),
		SequenceNumber: sequenceNumber,
		KesPeriod:      kesPeriod,
		Signature:      make([]byte, 64),
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		sequenceNumber,
		decodeCborArrayElementUint(t, encoded, 1),
	)
	assert.Equal(
		t,
		kesPeriod,
		decodeCborArrayElementUint(t, encoded, 2),
	)
}

// TestBuildBlockDoesNotNarrowOpCertCounterAtUint32 exercises the assignment
// rather than the struct: a counter one past math.MaxUint32 reaches the
// encoder intact, so no bound of dingo's stands between the certificate and
// the forged header.
//
// buildBlock re-decodes the block it encoded, so the outcome depends on the
// width the linked gouroboros release declares, and both outcomes assert the
// counter was not narrowed. A release that decodes the field as uint64
// returns a block whose header carries the full counter. The pinned release
// decodes ShelleyBlockHeaderBody.OpCertSequenceNumber as uint32 and reports
// the overflow from inside gouroboros, naming the untruncated value: had the
// forging path narrowed the counter, the encoded value would have been zero
// and that decode would have succeeded. That remaining failure is upstream's
// and is what a release carrying gouroboros #2256 removes; before this change
// the same certificate was refused earlier, by dingo's own uint32 bound.
func TestBuildBlockDoesNotNarrowOpCertCounterAtUint32(t *testing.T) {
	const counter = uint64(math.MaxUint32) + 1
	creds := setupTestCredentials(t)
	creds.opCert.IssueNumber = counter

	builder := newTPraosTestBuilder(t, creds)
	block, _, err := builder.BuildBlock(1001, 0)
	if err == nil {
		header, ok := block.Header().(*shelley.ShelleyBlockHeader)
		require.True(t, ok, "TPraos forge must return a Shelley header")
		assert.Equal(
			t,
			counter,
			uint64(header.Body.OpCertSequenceNumber),
		)
		return
	}
	assert.NotContains(t, err.Error(), "exceeds uint32 max")
	assert.Contains(t, err.Error(), "4294967296 overflows uint32")
}

// TestBuildBlockRejectsOpCertCounterAbovePersistableBound covers the other
// end: a counter the reference accepts but this node cannot record is
// refused before the leader slot is spent, naming the bound, rather than
// being forged and then failing its own block apply.
func TestBuildBlockRejectsOpCertCounterAbovePersistableBound(t *testing.T) {
	creds := setupTestCredentials(t)
	creds.opCert.IssueNumber = eras.MaxPersistableOpCertCounter + 1

	builder := newTPraosTestBuilder(t, creds)
	_, _, err := builder.BuildBlock(1001, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pool_opcert_sequence")
	assert.NotContains(t, err.Error(), "exceeds uint32 max")
}

// TestValidateOpCertSequenceRejectsCounterAbovePersistableBound pins the
// forge loop's pre-flight against the same bound block application applies,
// so the two cannot disagree about which counters are forgeable.
func TestValidateOpCertSequenceRejectsCounterAbovePersistableBound(
	t *testing.T,
) {
	require.NoError(
		t,
		validateOpCertSequence(5, true, uint64(math.MaxInt64), false),
	)
	err := validateOpCertSequence(
		5,
		true,
		eras.MaxPersistableOpCertCounter+1,
		false,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pool_opcert_sequence")
}

func newTPraosTestBuilder(
	t *testing.T,
	creds *PoolCredentials,
) *DefaultBlockBuilder {
	t.Helper()
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool: &mockMempool{transactions: []MempoolTransaction{}},
		PParamsProvider: &mockPParamsProvider{
			pparams: &shelley.ShelleyProtocolParameters{
				MaxTxSize:        16384,
				MaxBlockBodySize: 90112,
				ProtocolMajor:    2,
			},
		},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: creds,
	})
	require.NoError(t, err)
	return builder
}
