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
	"bytes"
	"testing"

	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPoolRewardAccountCBORLayouts(t *testing.T) {
	rewardHash := bytes.Repeat([]byte{0x42}, lcommon.Blake2b224Size)

	tests := []struct {
		name        string
		certificate []any
		wantTag     uint8
	}{
		{
			name: "legacy key hash",
			certificate: poolRegistrationFields(
				append([]byte{0xE1}, rewardHash...),
				false,
			),
			wantTag: 0,
		},
		{
			name: "Dijkstra script hash",
			certificate: poolRegistrationFields(
				append([]byte{0xF1}, rewardHash...),
				true,
			),
			wantTag: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, err := gcbor.Encode(test.certificate)
			require.NoError(t, err)

			cert := &lcommon.PoolRegistrationCertificate{}
			cert.SetCbor(raw)

			gotTag, gotHash, err := PoolRewardAccount(cert)
			require.NoError(t, err)
			assert.Equal(t, test.wantTag, gotTag)
			assert.Equal(t, rewardHash, gotHash)
		})
	}
}

func TestPoolRewardAccountRejectsInvalidLayout(t *testing.T) {
	fields := poolRegistrationFields(
		append([]byte{0xE1}, bytes.Repeat([]byte{0x42}, lcommon.Blake2b224Size)...),
		false,
	)
	fields = fields[:9]
	raw, err := gcbor.Encode(fields)
	require.NoError(t, err)

	cert := &lcommon.PoolRegistrationCertificate{}
	cert.SetCbor(raw)

	_, _, err = PoolRewardAccount(cert)
	require.EqualError(t, err, "pool cert CBOR: got 9 fields, want 10 or 11")
}

func poolRegistrationFields(rewardAddress []byte, leios bool) []any {
	fields := []any{
		uint(lcommon.CertificateTypePoolRegistration),
		bytes.Repeat([]byte{0x01}, lcommon.Blake2b224Size),
		bytes.Repeat([]byte{0x02}, lcommon.Blake2b256Size),
		uint64(1_000_000),
		uint64(340_000_000),
		[]uint64{1, 20},
		rewardAddress,
		[]any{},
		[]any{},
		nil,
	}
	if leios {
		leiosKey := &lcommon.LeiosKey{
			PublicKey: bytes.Repeat(
				[]byte{0x03},
				lcommon.LeiosBlsPublicKeySize,
			),
			PossessionProof: bytes.Repeat(
				[]byte{0x04},
				lcommon.LeiosBlsPossessionProofSize,
			),
		}
		fields = append(
			fields[:3],
			append([]any{leiosKey}, fields[3:]...)...,
		)
	}
	return fields
}
