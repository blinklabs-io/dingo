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

package eras

import (
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

// TestCertDepositRejectsTypedNilParams pins the guard both Conway-era and
// Dijkstra-era deposit functions need.
//
// A typed-nil parameter pointer satisfies the type assertion, so testing only
// the ok result lets every case in the switch dereference nil. Conway has
// always checked for it; Dijkstra checked only ok and panicked instead of
// reporting incompatible parameters. Both are asserted here so the pair cannot
// drift apart again.
func TestCertDepositRejectsTypedNilParams(t *testing.T) {
	certificates := map[string]lcommon.Certificate{
		"drep registration":  &lcommon.RegistrationDrepCertificate{},
		"pool registration":  &lcommon.PoolRegistrationCertificate{},
		"stake registration": &lcommon.StakeRegistrationCertificate{},
	}
	eras := map[string]struct {
		fn     func(lcommon.Certificate, lcommon.ProtocolParameters) (uint64, error)
		params lcommon.ProtocolParameters
	}{
		"conway": {
			fn:     CertDepositConway,
			params: (*conway.ConwayProtocolParameters)(nil),
		},
		"dijkstra": {
			fn:     CertDepositDijkstra,
			params: (*gdijkstra.DijkstraProtocolParameters)(nil),
		},
	}
	for eraName, era := range eras {
		for certName, cert := range certificates {
			t.Run(eraName+"/"+certName, func(t *testing.T) {
				require.NotPanics(t, func() {
					deposit, err := era.fn(cert, era.params)
					require.ErrorIs(t, err, ErrIncompatibleProtocolParams)
					require.Zero(t, deposit)
				})
			})
		}
	}
}
