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

package ledger

import (
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

// dijkstraPoolCertTx builds a real *gdijkstra.DijkstraTransaction carrying a
// single pool registration certificate with the given margin, for exercising
// the CIP-23 pool-margin-floor certificate rule end to end.
func dijkstraPoolCertTx(marginNum, marginDen int64) *gdijkstra.DijkstraTransaction {
	cert := &lcommon.PoolRegistrationCertificate{
		CertType: uint(lcommon.CertificateTypePoolRegistration),
		Margin:   lcommon.GenesisRat{Rat: big.NewRat(marginNum, marginDen)},
	}
	return &gdijkstra.DijkstraTransaction{
		TxIsValid: true,
		Body: gdijkstra.DijkstraTransactionBody{
			TxFee: 200_000,
			TxCertificates: []lcommon.CertificateWrapper{
				{
					Type:        uint(lcommon.CertificateTypePoolRegistration),
					Certificate: cert,
				},
			},
		},
	}
}

func dijkstraTestProtocolParameters() *gdijkstra.DijkstraProtocolParameters {
	return &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
			MaxTxSize:            16_384,
			MaxValueSize:         5_000,
			CollateralPercentage: 150,
			MaxCollateralInputs:  3,
		},
	}
}

// TestValidateTxDijkstraRejectsBelowFloorPoolMarginThroughLedgerView is the
// end-to-end regression test for the CIP-23 pool-margin-floor certificate
// rule through the exact link used at runtime: a real *ledger.LedgerView
// (built from a *LedgerState whose config.MinPoolMargin is nonzero) passed to
// eras.ValidateTxDijkstra as the lcommon.LedgerState argument. This is the
// path that depends on *LedgerView satisfying eras.MinPoolMarginProvider —
// fix #1's compile-time assertion guards the interface signature, but only
// this test proves the value actually reaches checkPoolMarginFloor at
// runtime. Other Dijkstra UTxO validation rules may also error on this
// deliberately minimal transaction; that's fine, since ValidateTxDijkstra
// joins all rule errors and this test only asserts on the CIP-23 substring.
func TestValidateTxDijkstraRejectsBelowFloorPoolMarginThroughLedgerView(t *testing.T) {
	ls, _ := newRewardCalculationTestLedger(t)
	ls.config.MinPoolMargin = 150 // 1.5%
	lv := &LedgerView{ls: ls}

	tx := dijkstraPoolCertTx(1, 1000) // 0.1%, below the 1.5% floor
	err := eras.ValidateTxDijkstra(tx, 0, lv, dijkstraTestProtocolParameters())
	require.Error(t, err)
	require.Contains(t, err.Error(), "below minimum pool margin")
}

// TestValidateTxDijkstraAcceptsAtOrAboveFloorPoolMarginThroughLedgerView is
// the companion negative assertion: a certificate at (or above, or under a
// disabled floor) the configured minimum must never trip the CIP-23 rule
// through the same real *LedgerView path.
func TestValidateTxDijkstraAcceptsAtOrAboveFloorPoolMarginThroughLedgerView(
	t *testing.T,
) {
	t.Run("at floor", func(t *testing.T) {
		ls, _ := newRewardCalculationTestLedger(t)
		ls.config.MinPoolMargin = 150 // 1.5%
		lv := &LedgerView{ls: ls}

		tx := dijkstraPoolCertTx(150, 10_000) // exactly 1.5%
		err := eras.ValidateTxDijkstra(tx, 0, lv, dijkstraTestProtocolParameters())
		if err != nil {
			require.NotContains(t, err.Error(), "below minimum pool margin")
		}
	})

	t.Run("above floor", func(t *testing.T) {
		ls, _ := newRewardCalculationTestLedger(t)
		ls.config.MinPoolMargin = 150 // 1.5%
		lv := &LedgerView{ls: ls}

		tx := dijkstraPoolCertTx(5, 100) // 5%
		err := eras.ValidateTxDijkstra(tx, 0, lv, dijkstraTestProtocolParameters())
		if err != nil {
			require.NotContains(t, err.Error(), "below minimum pool margin")
		}
	})

	t.Run("floor disabled", func(t *testing.T) {
		ls, _ := newRewardCalculationTestLedger(t)
		ls.config.MinPoolMargin = 0
		lv := &LedgerView{ls: ls}

		// Even a zero margin cert must not trip the rule when the floor is
		// disabled (config.MinPoolMargin == 0 -> MinPoolMargin() returns nil).
		tx := dijkstraPoolCertTx(0, 1)
		err := eras.ValidateTxDijkstra(tx, 0, lv, dijkstraTestProtocolParameters())
		if err != nil {
			require.NotContains(t, err.Error(), "below minimum pool margin")
		}
	})
}
