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

package txpump

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/blinklabs-io/gouroboros/protocol/localtxmonitor"
	"github.com/blinklabs-io/gouroboros/protocol/localtxsubmission"
	"github.com/stretchr/testify/require"
)

func TestWorkloadSubmissionsKeepControlledChangeAddress(t *testing.T) {
	for _, workload := range []struct {
		name   string
		submit func(*Pump, *NodeClient, int) bool
	}{
		{"delegation", (*Pump).submitDelegation},
		{"governance", (*Pump).submitGovernance},
		{"plutus lock", (*Pump).submitPlutus},
	} {
		t.Run(workload.name, func(t *testing.T) {
			controlled := append([]byte{0x60}, bytes.Repeat([]byte{0x42}, 28)...)
			pump := testPump(time.Now().Add(-time.Second), time.Second)
			pump.cfg.DelegationPoolKeyHash = hex.EncodeToString(samplePoolKeyHash)
			pump.wallet.Add(UTxO{
				TxHash: sampleHash, Index: 0, Amount: 600_000_000,
				SigningKey: &UTxOKey{Address: controlled},
			})
			submitted := make(chan []byte, 1)
			cfg := localtxsubmission.NewConfig(localtxsubmission.WithSubmitTxFunc(
				func(_ localtxsubmission.CallbackContext, tx localtxsubmission.MsgSubmitTxTransaction) error {
					submitted <- tx.Raw.Content.([]byte)
					return nil
				},
			))
			client := newProtocolTestClient(t, ouroboros.WithLocalTxSubmissionConfig(cfg))
			require.True(t, workload.submit(pump, client, 1))
			var raw []byte
			select {
			case raw = <-submitted:
			case <-time.After(5 * time.Second):
				t.Fatal("submission callback was not reached")
			}
			var tx conway.ConwayTransaction
			_, err := cbor.Decode(raw, &tx)
			require.NoError(t, err)
			outputs := tx.Outputs()
			require.NotEmpty(t, outputs)
			change, err := outputs[len(outputs)-1].Address().Bytes()
			require.NoError(t, err)
			require.Equal(t, controlled, change, "submitted change must remain queryable by the wallet")
			if workload.name == "plutus lock" {
				require.Len(t, pump.plutusLocked, 1)
				require.Equal(t, controlled, pump.plutusLocked[0].address)
			}
		})
	}
}

func TestPlutusUnlockReturnsChangeToLockedWalletAddress(t *testing.T) {
	controlled := append([]byte{0x60}, bytes.Repeat([]byte{0x24}, 28)...)
	pump := testPump(time.Now().Add(-time.Second), time.Second)
	pump.cfg.ConfirmationSlots = 0
	pump.cfg.SlotLength = 0
	pump.cfg.PlutusV3CostModel = sampleCostModel
	key := testSigningKey(0x24)
	key.Address = controlled
	for i := range 2 {
		pump.wallet.Add(UTxO{
			TxHash: fmt.Sprintf("%064x", i+1), Index: 0, Amount: 600_000_000,
			SigningKey: key,
		})
	}
	submitted := make(chan []byte, 2)
	cfg := localtxsubmission.NewConfig(localtxsubmission.WithSubmitTxFunc(
		func(_ localtxsubmission.CallbackContext, tx localtxsubmission.MsgSubmitTxTransaction) error {
			submitted <- tx.Raw.Content.([]byte)
			return nil
		},
	))
	client := newProtocolTestClient(t, ouroboros.WithLocalTxSubmissionConfig(cfg))

	pump.intRangeFn = func(int, int) int { return 0 }
	require.True(t, pump.submitPlutus(client, 1), "Plutus lock submission must reach the protocol")
	require.Len(t, pump.plutusLocked, 1)
	pump.intRangeFn = func(int, int) int { return 1 }
	require.True(t, pump.submitPlutus(client, 1), "Plutus unlock submission must reach the protocol")

	var raw []byte
	select {
	case <-submitted:
	case <-time.After(5 * time.Second):
		t.Fatal("lock submission callback was not reached")
	}
	select {
	case raw = <-submitted:
	case <-time.After(5 * time.Second):
		t.Fatal("unlock submission callback was not reached")
	}
	var tx conway.ConwayTransaction
	_, err := cbor.Decode(raw, &tx)
	require.NoError(t, err)
	outputs := tx.Outputs()
	require.Len(t, outputs, 1)
	change, err := outputs[0].Address().Bytes()
	require.NoError(t, err)
	require.Equal(t, controlled, change, "Plutus unlock change must remain queryable by the wallet")
}

// TestUnsignedWorkloadSubmissionsKeepDeterministicChangeAddress pins the
// fallback that keeps keyless harness wallets submitting: with no signing key
// on the selected input, change returns to the address derived from the input
// transaction hash, and acceptance stays on the pacing-only path.
func TestUnsignedWorkloadSubmissionsKeepDeterministicChangeAddress(t *testing.T) {
	for _, workload := range []struct {
		name   string
		submit func(*Pump, *NodeClient, int) bool
	}{
		{"delegation", (*Pump).submitDelegation},
		{"governance", (*Pump).submitGovernance},
		{"plutus lock", (*Pump).submitPlutus},
	} {
		t.Run(workload.name, func(t *testing.T) {
			pump := testPump(time.Now().Add(-time.Second), time.Second)
			pump.cfg.DelegationPoolKeyHash = hex.EncodeToString(samplePoolKeyHash)
			pump.wallet.Add(UTxO{TxHash: sampleHash, Index: 0, Amount: 600_000_000})
			submitted := make(chan []byte, 1)
			cfg := localtxsubmission.NewConfig(localtxsubmission.WithSubmitTxFunc(
				func(_ localtxsubmission.CallbackContext, tx localtxsubmission.MsgSubmitTxTransaction) error {
					submitted <- tx.Raw.Content.([]byte)
					return nil
				},
			))
			client := newProtocolTestClient(t, ouroboros.WithLocalTxSubmissionConfig(cfg))
			require.True(t, workload.submit(pump, client, 1))
			var raw []byte
			select {
			case raw = <-submitted:
			case <-time.After(5 * time.Second):
				t.Fatal("submission callback was not reached")
			}
			var tx conway.ConwayTransaction
			_, err := cbor.Decode(raw, &tx)
			require.NoError(t, err)
			outputs := tx.Outputs()
			require.NotEmpty(t, outputs)
			change, err := outputs[len(outputs)-1].Address().Bytes()
			require.NoError(t, err)
			require.Equal(t, deterministicAddr(sampleHash), change,
				"a keyless input must keep the deterministic change address")
			require.Empty(t, pump.wallet.PendingIDs(),
				"an unsigned wallet keeps the pacing-only acceptance path")
		})
	}
}

// cborTag258 is the 3-byte CBOR prefix for tag 258 (CBOR set).
var cborTag258 = []byte{0xd9, 0x01, 0x02}

func requireConwayDecode(t *testing.T, txBytes []byte) {
	t.Helper()
	var tx conway.ConwayTransaction
	_, err := cbor.Decode(txBytes, &tx)
	require.NoError(t, err)
	// Inputs must be encoded as a CBOR set (tag 258). Without this, the
	// Cardano node sees an empty input set and rejects with InputSetEmptyUTxO.
	require.True(
		t,
		bytes.Contains(txBytes, cborTag258),
		"transaction inputs must be encoded as CBOR tag-258 set (0xd90102 not found)",
	)
}

// TestInputsEncodedAsSet is a regression test: inputs must use CBOR tag 258.
// A plain array causes Cardano node to report InputSetEmptyUTxO on every tx.
func TestInputsEncodedAsSet(t *testing.T) {
	tests := []struct {
		name  string
		build func() ([]byte, error)
	}{
		{
			name: "payment",
			build: func() ([]byte, error) {
				b, _, err := BuildPayment(validParams())
				return b, err
			},
		},
		{
			name: "delegation",
			build: func() ([]byte, error) {
				return BuildDelegationTx(
					[]UTxO{{TxHash: sampleHash, Index: 0, Amount: 2_000_000}},
					make([]byte, 28), make([]byte, 28), 0, MinFee, sampleAddr, nil,
				)
			},
		},
		{
			name: "drep_registration",
			build: func() ([]byte, error) {
				return BuildDRepRegistrationTx(
					[]UTxO{{TxHash: sampleHash, Index: 0, Amount: 600_000_000}},
					make([]byte, 28), drepDeposit, MinFee, sampleAddr, nil,
				)
			},
		},
		{
			name: "drep_update",
			build: func() ([]byte, error) {
				return BuildDRepUpdateTx(
					[]UTxO{{TxHash: sampleHash, Index: 0, Amount: 2_000_000}},
					make([]byte, 28), MinFee, sampleAddr, nil,
				)
			},
		},
		{
			name: "plutus_lock",
			build: func() ([]byte, error) {
				return BuildPlutusLockTx(
					[]UTxO{{TxHash: sampleHash, Index: 0, Amount: 3_000_000}},
					make([]byte, 28), minSendAmount, MinFee, sampleAddr,
				)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			txBytes, err := tc.build()
			require.NoError(t, err)
			require.True(
				t,
				bytes.Contains(txBytes, cborTag258),
				"tx type %q: inputs must be CBOR tag-258 set", tc.name,
			)
		})
	}
}

// TestReconcileWalletObservesMonitorBeforeLSQ uses both real local protocol
// servers over net.Pipe. The monitor acquisition models confirmation before
// the LSQ acquisition; the resulting parent output must replace the spent
// funding input rather than reviving it from an older snapshot.
func TestReconcileWalletObservesMonitorBeforeLSQ(t *testing.T) {
	address, err := ledger.NewAddress(
		"addr_test1vrk294czhxhglflvxla7vxj2cjz7wyrdpxl3fj0vych5wws77xuc7",
	)
	require.NoError(t, err)
	addressBytes, err := address.Bytes()
	require.NoError(t, err)

	confirmed := false
	queryCount := 0
	lsqConfig := localstatequery.NewConfig(
		localstatequery.WithAcquireFunc(func(
			localstatequery.CallbackContext,
			localstatequery.AcquireTarget,
			bool,
		) error {
			return nil
		}),
		localstatequery.WithQueryFunc(func(
			localstatequery.CallbackContext,
			localstatequery.QueryWrapper,
		) (any, error) {
			queryCount++
			if queryCount == 1 {
				return 6, nil // Conway current era
			}
			id := localstatequery.UtxoId{
				Hash: ledger.NewBlake2b256([]byte{0x22}),
				Idx:  0,
			}
			if !confirmed {
				id.Hash = ledger.NewBlake2b256([]byte{0x11})
			}
			return localstatequery.UTxOsResult{
				Results: map[localstatequery.UtxoId]ledger.BabbageTransactionOutput{
					id: {
						OutputAddress: address,
						OutputAmount: ledger.MaryTransactionOutputValue{
							Amount: 5_000_000,
						},
					},
				},
			}, nil
		}),
		localstatequery.WithReleaseFunc(func(localstatequery.CallbackContext) error {
			return nil
		}),
	)
	monitorConfig := localtxmonitor.NewConfig(
		localtxmonitor.WithGetMempoolFunc(func(localtxmonitor.CallbackContext) (uint64, uint32, []localtxmonitor.TxAndEraId, error) {
			confirmed = true
			return 0, 100, nil, nil
		}),
	)

	client := newProtocolTestClient(t,
		ouroboros.WithLocalStateQueryConfig(lsqConfig),
		ouroboros.WithLocalTxMonitorConfig(monitorConfig),
	)
	snapshot, presence, err := client.ReconcileWallet([][]byte{addressBytes}, nil)
	require.NoError(t, err)
	require.Empty(t, presence)
	require.Len(t, snapshot, 1)
	require.Equal(t, ledger.NewBlake2b256([]byte{0x22}).String(), snapshot[0].TxHash)
	wallet := NewWallet()
	source := UTxO{
		TxHash: ledger.NewBlake2b256([]byte{0x11}).String(),
		Index:  0,
		Amount: 5_000_000,
	}
	wallet.Add(source)
	wallet.Reserve("confirmed-tx", []UTxO{source}, nil, 0)
	wallet.ReconcileSnapshot(snapshot, presence)
	selected, _, err := wallet.SelectCoins(1)
	require.NoError(t, err)
	require.Len(t, selected, 1)
	require.Equal(t, snapshot[0].TxHash, selected[0].TxHash)
}

func newProtocolTestClient(t *testing.T, serverOptions ...ouroboros.ConnectionOptionFunc) *NodeClient {
	t.Helper()
	serverPipe, clientPipe := net.Pipe()
	t.Cleanup(func() {
		_ = clientPipe.Close()
		_ = serverPipe.Close()
	})
	require.NoError(t, serverPipe.SetDeadline(time.Now().Add(10*time.Second)))
	require.NoError(t, clientPipe.SetDeadline(time.Now().Add(10*time.Second)))
	serverCh := make(chan *ouroboros.Connection, 1)
	serverErrCh := make(chan error, 1)
	go func() {
		opts := []ouroboros.ConnectionOptionFunc{
			ouroboros.WithConnection(serverPipe),
			ouroboros.WithServer(true),
			ouroboros.WithNetworkMagic(999999),
		}
		conn, serverErr := ouroboros.New(append(opts, serverOptions...)...)
		if serverErr != nil {
			serverErrCh <- serverErr
			return
		}
		serverCh <- conn
	}()
	clientConn, err := ouroboros.New(
		ouroboros.WithConnection(clientPipe),
		ouroboros.WithNetworkMagic(999999),
		ouroboros.WithNodeToNode(false),
		ouroboros.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		ouroboros.WithLocalStateQueryConfig(localstatequery.NewConfig()),
		ouroboros.WithLocalTxMonitorConfig(localtxmonitor.NewConfig()),
	)
	var serverConn *ouroboros.Connection
	select {
	case serverErr := <-serverErrCh:
		require.NoError(t, serverErr)
	case serverConn = <-serverCh:
	case <-time.After(15 * time.Second):
		t.Fatal("protocol server construction did not finish")
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientConn.Close() })
	require.NotNil(t, serverConn)
	t.Cleanup(func() { _ = serverConn.Close() })
	return &NodeClient{conn: clientConn, addr: "pipe"}
}

// TestSubmitDelegationTracksStakeRegistration checks that the first delegation
// for a credential registers it (certificate 11), later delegations only
// delegate (certificate 2), and a rejection flips the tracked state so the
// next attempt registers again.
func TestSubmitDelegationTracksStakeRegistration(t *testing.T) {
	key := testSigningKey(0x66)
	pump := testPump(time.Now().Add(-time.Second), time.Second)
	pump.cfg.DelegationPoolKeyHash = hex.EncodeToString(samplePoolKeyHash)
	for i := range 4 {
		hash := fmt.Sprintf("%064x", i+1)
		pump.wallet.Add(UTxO{TxHash: hash, Amount: 10_000_000, SigningKey: key})
	}
	submitted := make(chan []byte, 4)
	var reject atomic.Bool
	cfg := localtxsubmission.NewConfig(localtxsubmission.WithSubmitTxFunc(
		func(_ localtxsubmission.CallbackContext, tx localtxsubmission.MsgSubmitTxTransaction) error {
			submitted <- tx.Raw.Content.([]byte)
			if reject.Load() {
				return errors.New("rejected")
			}
			return nil
		},
	))
	client := newProtocolTestClient(t, ouroboros.WithLocalTxSubmissionConfig(cfg))

	certType := func() common.Certificate {
		t.Helper()
		var raw []byte
		select {
		case raw = <-submitted:
		case <-time.After(5 * time.Second):
			t.Fatal("submission callback was not reached")
		}
		var tx conway.ConwayTransaction
		_, err := cbor.Decode(raw, &tx)
		require.NoError(t, err)
		require.Len(t, tx.Certificates(), 1)
		return tx.Certificates()[0]
	}

	require.True(t, pump.submitDelegation(client, 1))
	require.IsType(t, &common.StakeRegistrationDelegationCertificate{}, certType())
	require.True(t, pump.submitDelegation(client, 1))
	require.IsType(t, &common.StakeDelegationCertificate{}, certType())

	reject.Store(true)
	require.False(t, pump.submitDelegation(client, 1))
	require.IsType(t, &common.StakeDelegationCertificate{}, certType())
	reject.Store(false)
	require.True(t, pump.submitDelegation(client, 1))
	require.IsType(t, &common.StakeRegistrationDelegationCertificate{}, certType(),
		"a rejected delegation must make the next attempt re-register")
}

// TestSubmitPaymentFoldsDustChangeIntoPayment checks that change below the
// minimum output is sent with the payment instead of producing an output the
// node rejects.
func TestSubmitPaymentFoldsDustChangeIntoPayment(t *testing.T) {
	key := testSigningKey(0x77)
	pump := testPump(time.Now().Add(-time.Second), time.Second)
	// Balance 2.5 ADA caps the send at 1.05 ADA, so one 1.25 ADA input
	// covers it and leaves at most 0.05 ADA of change.
	for i := range 2 {
		hash := fmt.Sprintf("%064x", i+1)
		pump.wallet.Add(UTxO{TxHash: hash, Amount: 1_250_000, SigningKey: key})
	}
	submitted := make(chan []byte, 1)
	cfg := localtxsubmission.NewConfig(localtxsubmission.WithSubmitTxFunc(
		func(_ localtxsubmission.CallbackContext, tx localtxsubmission.MsgSubmitTxTransaction) error {
			submitted <- tx.Raw.Content.([]byte)
			return nil
		},
	))
	client := newProtocolTestClient(t, ouroboros.WithLocalTxSubmissionConfig(cfg))
	require.True(t, pump.submitPayment(client, 1))

	var raw []byte
	select {
	case raw = <-submitted:
	case <-time.After(5 * time.Second):
		t.Fatal("submission callback was not reached")
	}
	var tx conway.ConwayTransaction
	_, err := cbor.Decode(raw, &tx)
	require.NoError(t, err)
	for _, out := range tx.Outputs() {
		require.GreaterOrEqual(t, out.Amount().Uint64(), minSendAmount,
			"no output may fall below the minimum output")
	}
}

// TestSubmitDelegationWaitsForPendingRegistration checks that a credential
// whose registration is still inside the confirmation window is not used for
// a plain delegation the ledger would reject.
func TestSubmitDelegationWaitsForPendingRegistration(t *testing.T) {
	key := testSigningKey(0x67)
	pump := testPump(time.Now().Add(-time.Second), time.Second)
	pump.cfg.DelegationPoolKeyHash = hex.EncodeToString(samplePoolKeyHash)
	pump.cfg.ConfirmationSlots = 30
	pump.cfg.SlotLength = time.Second
	for i := range 2 {
		hash := fmt.Sprintf("%064x", i+1)
		pump.wallet.Add(UTxO{TxHash: hash, Amount: 10_000_000, SigningKey: key})
	}
	submitted := make(chan []byte, 2)
	cfg := localtxsubmission.NewConfig(localtxsubmission.WithSubmitTxFunc(
		func(_ localtxsubmission.CallbackContext, tx localtxsubmission.MsgSubmitTxTransaction) error {
			submitted <- tx.Raw.Content.([]byte)
			return nil
		},
	))
	client := newProtocolTestClient(t, ouroboros.WithLocalTxSubmissionConfig(cfg))

	require.True(t, pump.submitDelegation(client, 1))
	select {
	case <-submitted:
	case <-time.After(5 * time.Second):
		t.Fatal("registration submission callback was not reached")
	}
	require.False(t, pump.submitDelegation(client, 1),
		"a pending registration must not be followed by a plain delegation")
	select {
	case <-submitted:
		t.Fatal("no transaction may be submitted while the registration is pending")
	default:
	}
}
