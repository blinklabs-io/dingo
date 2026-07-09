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

package dingo

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/mempool"
)

// devnetKeysDir locates the credential fixtures shipped with the repo.
// Path is relative to this file (top-level dingo package).
const devnetKeysDir = "config/cardano/devnet/keys"

func devnetCredPaths() (vrf, kes, opcert string) {
	return filepath.Join(devnetKeysDir, "vrf.skey"),
		filepath.Join(devnetKeysDir, "kes.skey"),
		filepath.Join(devnetKeysDir, "opcert.cert")
}

// shelleyGenesisCfgForBP returns a CardanoNodeConfig with a Shelley
// genesis that is plausible for the devnet opcert (KESPeriod=0,
// IssueNumber=0). systemStart slightly in the past, slotsPerKESPeriod
// generous so the opcert is current rather than expired.
func shelleyGenesisCfgForBP(t *testing.T, systemStart time.Time) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"systemStart": "` + systemStart.UTC().Format(time.RFC3339Nano) + `",
		"securityParam": 10,
		"activeSlotsCoeff": 0.5,
		"slotsPerKESPeriod": 129600,
		"maxKESEvolutions": 62,
		"slotLength": 1
	}`)); err != nil {
		t.Fatalf("LoadShelleyGenesisFromReader: %v", err)
	}
	return cfg
}

func newTestNodeForBP(
	t *testing.T,
	enabled bool,
	vrf, kes, opcert string,
	cardanoCfg *cardano.CardanoNodeConfig,
) *Node {
	t.Helper()
	cfg := Config{
		logger:                        slog.New(slog.NewJSONHandler(io.Discard, nil)),
		blockProducer:                 enabled,
		shelleyVRFKey:                 vrf,
		shelleyKESKey:                 kes,
		shelleyOperationalCertificate: opcert,
		cardanoNodeConfig:             cardanoCfg,
	}
	return &Node{config: cfg}
}

func TestValidateBlockProducerStartup_HappyPath(t *testing.T) {
	vrf, kes, opcert := devnetCredPaths()
	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBP(t, true, vrf, kes, opcert, cardanoCfg)
	creds, err := n.validateBlockProducerStartupAtSlot(0)
	if err != nil {
		t.Fatalf("validateBlockProducerStartup: %v", err)
	}
	if !creds.IsLoaded() {
		t.Error("expected credentials to be loaded")
	}
}

func TestValidateBlockProducerStartup_NoCardanoConfig(t *testing.T) {
	vrf, kes, opcert := devnetCredPaths()
	n := newTestNodeForBP(t, true, vrf, kes, opcert, nil)
	_, err := n.validateBlockProducerStartupAtSlot(0)
	if err == nil {
		t.Fatal("expected error for missing cardano node config")
	}
	if !strings.Contains(err.Error(), "Cardano node config") {
		t.Errorf("expected 'Cardano node config' in error, got: %v", err)
	}
}

func TestValidateBlockProducerStartup_ExpiredKESPeriod(t *testing.T) {
	// systemStart a year in the past with slotsPerKESPeriod=10 means
	// many KES periods have elapsed; maxKESEvolutions=1 makes anything
	// past period 1 expired, so the devnet opcert (KESPeriod=0) is well
	// outside its validity window and validation must reject it.
	vrf, kes, opcert := devnetCredPaths()
	cfg := &cardano.CardanoNodeConfig{}
	systemStart := time.Now().Add(-365 * 24 * time.Hour)
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"systemStart": "` + systemStart.UTC().Format(time.RFC3339Nano) + `",
		"securityParam": 10,
		"activeSlotsCoeff": 0.5,
		"slotsPerKESPeriod": 10,
		"maxKESEvolutions": 1,
		"slotLength": 1
	}`)); err != nil {
		t.Fatalf("LoadShelleyGenesisFromReader: %v", err)
	}
	n := newTestNodeForBP(t, true, vrf, kes, opcert, cfg)
	_, err := n.validateBlockProducerStartupAtSlot(20)
	if err == nil {
		t.Fatal("expected error for expired opcert KES period")
	}
	if !strings.Contains(err.Error(), "expired") {
		t.Errorf("expected 'expired' in error, got: %v", err)
	}
}

func TestValidateBlockProducerStartup_MissingFile(t *testing.T) {
	tmp := t.TempDir()
	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBP(
		t, true,
		filepath.Join(tmp, "missing-vrf.skey"),
		filepath.Join(tmp, "missing-kes.skey"),
		filepath.Join(tmp, "missing-opcert.cert"),
		cardanoCfg,
	)
	_, err := n.validateBlockProducerStartupAtSlot(0)
	if err == nil {
		t.Fatal("expected error for missing credential files")
	}
	if !strings.Contains(err.Error(), "load pool credentials") {
		t.Errorf("expected 'load pool credentials' in error, got: %v", err)
	}
}

type testBlockProducerLedgerView struct {
	registered bool
	regVRFHash [32]byte
}

func (v testBlockProducerLedgerView) PoolRegistrationVRFKeyHash(
	[28]byte,
) ([32]byte, bool, error) {
	return v.regVRFHash, v.registered, nil
}

func (v testBlockProducerLedgerView) LatestOpCertSequence(
	[28]byte,
) (uint64, bool, error) {
	return 0, false, nil
}

func mismatchedVRFHash() [32]byte {
	var h [32]byte
	for i := range h {
		h[i] = 0xdd
	}
	return h
}

func TestValidateBlockProducerLedger_NonDevnetVRFMismatchIsFatal(t *testing.T) {
	vrf, kes, opcert := devnetCredPaths()
	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBP(t, true, vrf, kes, opcert, cardanoCfg)
	n.config.network = "preview"
	creds, err := n.validateBlockProducerStartupAtSlot(0)
	if err != nil {
		t.Fatalf("validateBlockProducerStartup: %v", err)
	}
	err = n.validateBlockProducerLedgerWithView(
		creds,
		testBlockProducerLedgerView{
			registered: true,
			regVRFHash: mismatchedVRFHash(),
		},
	)
	if !errors.Is(err, forging.ErrVRFKeyHashMismatch) {
		t.Fatalf("expected VRF mismatch error, got: %v", err)
	}
}

func TestValidateBlockProducerLedger_DevnetVRFMismatchWarns(t *testing.T) {
	vrf, kes, opcert := devnetCredPaths()
	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBP(t, true, vrf, kes, opcert, cardanoCfg)
	n.config.network = "devnet"
	creds, err := n.validateBlockProducerStartupAtSlot(0)
	if err != nil {
		t.Fatalf("validateBlockProducerStartup: %v", err)
	}
	err = n.validateBlockProducerLedgerWithView(
		creds,
		testBlockProducerLedgerView{
			registered: true,
			regVRFHash: mismatchedVRFHash(),
		},
	)
	if err != nil {
		t.Fatalf("devnet mismatch should warn and continue: %v", err)
	}
}

func TestHandleGenesisSnapshotError_BlockProducerFatal(t *testing.T) {
	n := &Node{
		config: Config{
			logger:        slog.New(slog.NewJSONHandler(io.Discard, nil)),
			blockProducer: true,
		},
	}
	sentinel := errors.New("db unavailable")
	err := n.handleGenesisSnapshotError(sentinel)
	if err == nil {
		t.Fatal("expected fatal error for block producer, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel wrapped in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "failed to capture genesis snapshot") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestHandleGenesisSnapshotError_RelayWarnsAndContinues(t *testing.T) {
	n := &Node{
		config: Config{
			logger:        slog.New(slog.NewJSONHandler(io.Discard, nil)),
			blockProducer: false,
		},
	}
	err := n.handleGenesisSnapshotError(errors.New("db unavailable"))
	if err != nil {
		t.Errorf("expected nil for relay node, got: %v", err)
	}
}

type testMempoolTransactionSource struct {
	txs []mempool.MempoolTransaction
}

func (s testMempoolTransactionSource) Transactions() []mempool.MempoolTransaction {
	return s.txs
}

func (s testMempoolTransactionSource) RemoveTxsByHash(_ []string) {}

// TestMempoolAdaptersPreservePendingTransactionView verifies the node-level
// adapters preserve the pending transaction fields needed for block building.
func TestMempoolAdaptersPreservePendingTransactionView(t *testing.T) {
	source := testMempoolTransactionSource{
		txs: []mempool.MempoolTransaction{
			{
				Hash: "0123456789abcdef",
				Cbor: []byte{0x84, 0xa0, 0xa0, 0xf5, 0xf6},
				Type: 7,
			},
		},
	}

	var _ ledger.MempoolProvider = (*ledgerMempoolAdapter)(nil)
	ledgerTxs := (&ledgerMempoolAdapter{source: source}).Transactions()
	if len(ledgerTxs) != 1 {
		t.Fatalf("expected 1 ledger transaction, got %d", len(ledgerTxs))
	}
	if ledgerTxs[0].Hash != source.txs[0].Hash {
		t.Fatalf("ledger hash mismatch: got %q want %q",
			ledgerTxs[0].Hash, source.txs[0].Hash)
	}
	if ledgerTxs[0].Type != source.txs[0].Type {
		t.Fatalf("ledger type mismatch: got %d want %d",
			ledgerTxs[0].Type, source.txs[0].Type)
	}
	if !bytes.Equal(ledgerTxs[0].Cbor, source.txs[0].Cbor) {
		t.Fatalf("ledger CBOR mismatch: got %x want %x",
			ledgerTxs[0].Cbor, source.txs[0].Cbor)
	}

	var _ forging.MempoolProvider = (*forgingMempoolAdapter)(nil)
	forgingTxs := (&forgingMempoolAdapter{source: source}).Transactions()
	if len(forgingTxs) != 1 {
		t.Fatalf("expected 1 forging transaction, got %d", len(forgingTxs))
	}
	if forgingTxs[0].Hash != source.txs[0].Hash {
		t.Fatalf("forging hash mismatch: got %q want %q",
			forgingTxs[0].Hash, source.txs[0].Hash)
	}
	if forgingTxs[0].Type != source.txs[0].Type {
		t.Fatalf("forging type mismatch: got %d want %d",
			forgingTxs[0].Type, source.txs[0].Type)
	}
	if !bytes.Equal(forgingTxs[0].Cbor, source.txs[0].Cbor) {
		t.Fatalf("forging CBOR mismatch: got %x want %x",
			forgingTxs[0].Cbor, source.txs[0].Cbor)
	}
}
