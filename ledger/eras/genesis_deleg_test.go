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
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// genesisDelegLedgerState adds the genesis delegation state to the mock.
type genesisDelegLedgerState struct {
	*mockLedgerState
	state GenesisDelegState
}

func (g genesisDelegLedgerState) GenesisDelegState(
	_ uint64,
) (GenesisDelegState, error) {
	return g.state, nil
}

func gdHash224(seed byte) lcommon.Blake2b224 {
	return lcommon.NewBlake2b224(bytes.Repeat([]byte{seed}, 28))
}

func gdHash256(seed byte) lcommon.Blake2b256 {
	return lcommon.NewBlake2b256(bytes.Repeat([]byte{seed}, 32))
}

func genesisKeyDelegation(
	genesis, delegate lcommon.Blake2b224,
	vrf lcommon.Blake2b256,
) lcommon.Certificate {
	return &lcommon.GenesisKeyDelegationCertificate{
		CertType:            uint(lcommon.CertificateTypeGenesisKeyDelegation),
		GenesisHash:         genesis.Bytes(),
		GenesisDelegateHash: delegate.Bytes(),
		VrfKeyHash:          lcommon.VrfKeyHash(vrf),
	}
}

// Two genesis keys, G1 -> D1/V1 in force and G2 -> D2/V2 in force, with G2
// re-delegating to D3/V3 from slot 500.
func genesisDelegFixture() genesisDelegLedgerState {
	return genesisDelegLedgerState{
		mockLedgerState: newMockLedgerState(),
		state: GenesisDelegState{
			StabilityWindow: 400,
			Current: map[lcommon.Blake2b224]GenesisDelegPair{
				gdHash224(0x01): {
					Delegate: gdHash224(0xd1),
					Vrf:      gdHash256(0xe1),
				},
				gdHash224(0x02): {
					Delegate: gdHash224(0xd2),
					Vrf:      gdHash256(0xe2),
				},
			},
			Future: map[FutureGenesisDelegKey]GenesisDelegPair{
				{Slot: 500, Genesis: gdHash224(0x02)}: {
					Delegate: gdHash224(0xd3), Vrf: gdHash256(0xe3),
				},
			},
		},
	}
}

func TestValidateShelleyDelegCerts_GenesisKeyDelegation(t *testing.T) {
	t.Parallel()

	g1, g2 := gdHash224(0x01), gdHash224(0x02)
	tests := []struct {
		name    string
		certs   []lcommon.Certificate
		wantErr any
	}{
		{
			name: "fresh delegate and VRF key",
			certs: []lcommon.Certificate{
				genesisKeyDelegation(g1, gdHash224(0xd9), gdHash256(0xe9)),
			},
		},
		{
			name: "own delegate and VRF key kept",
			certs: []lcommon.Certificate{
				genesisKeyDelegation(g1, gdHash224(0xd1), gdHash256(0xe1)),
			},
		},
		{
			name: "own pending delegate and VRF key reused",
			certs: []lcommon.Certificate{
				genesisKeyDelegation(g2, gdHash224(0xd3), gdHash256(0xe3)),
			},
		},
		{
			name: "unknown genesis root",
			certs: []lcommon.Certificate{
				genesisKeyDelegation(
					gdHash224(0x09), gdHash224(0xd9), gdHash256(0xe9),
				),
			},
			wantErr: GenesisKeyNotInMappingError{},
		},
		{
			name: "delegate in force for another genesis key",
			certs: []lcommon.Certificate{
				genesisKeyDelegation(g1, gdHash224(0xd2), gdHash256(0xe9)),
			},
			wantErr: DuplicateGenesisDelegateError{},
		},
		{
			name: "delegate pending for another genesis key",
			certs: []lcommon.Certificate{
				genesisKeyDelegation(g1, gdHash224(0xd3), gdHash256(0xe9)),
			},
			wantErr: DuplicateGenesisDelegateError{},
		},
		{
			name: "VRF key in force for another genesis key",
			certs: []lcommon.Certificate{
				genesisKeyDelegation(g1, gdHash224(0xd9), gdHash256(0xe2)),
			},
			wantErr: DuplicateGenesisVRFError{},
		},
		{
			name: "VRF key pending for another genesis key",
			certs: []lcommon.Certificate{
				genesisKeyDelegation(g1, gdHash224(0xd9), gdHash256(0xe3)),
			},
			wantErr: DuplicateGenesisVRFError{},
		},
		{
			name: "delegate taken by an earlier certificate of the transaction",
			certs: []lcommon.Certificate{
				genesisKeyDelegation(g1, gdHash224(0xd9), gdHash256(0xe9)),
				genesisKeyDelegation(g2, gdHash224(0xd9), gdHash256(0xe8)),
			},
			wantErr: DuplicateGenesisDelegateError{},
		},
		{
			name: "same genesis key redelegated within the transaction",
			certs: []lcommon.Certificate{
				genesisKeyDelegation(g1, gdHash224(0xd9), gdHash256(0xe9)),
				genesisKeyDelegation(g1, gdHash224(0xd8), gdHash256(0xe8)),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateCerts(
				genesisDelegFixture(),
				lcommon.ProtocolVersionShelley,
				tc.certs...,
			)
			switch tc.wantErr.(type) {
			case nil:
				require.NoError(t, err)
			case GenesisKeyNotInMappingError:
				require.ErrorAs(t, err, &GenesisKeyNotInMappingError{})
			case DuplicateGenesisDelegateError:
				require.ErrorAs(t, err, &DuplicateGenesisDelegateError{})
			case DuplicateGenesisVRFError:
				require.ErrorAs(t, err, &DuplicateGenesisVRFError{})
			}
		})
	}
}

// A ledger state that cannot report genesis delegation state skips the
// stateful predicates rather than rejecting every certificate.
func TestValidateShelleyDelegCerts_GenesisKeyDelegationWithoutProvider(
	t *testing.T,
) {
	t.Parallel()

	require.NoError(t, validateCerts(
		newMockLedgerState(),
		lcommon.ProtocolVersionShelley,
		genesisKeyDelegation(gdHash224(0x09), gdHash224(0xd9), gdHash256(0xe9)),
	))
}
