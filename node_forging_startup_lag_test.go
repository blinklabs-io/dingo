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
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// opCertFixtureWithCounter writes an operational certificate carrying
// issueNumber over the devnet KES verification key, signed by a freshly
// generated cold key. The shipped devnet opcert is fixed at issue number 0,
// which cannot express a counter gap against an observed on-chain value.
func opCertFixtureWithCounter(t *testing.T, issueNumber uint64) string {
	t.Helper()
	devnetCert, err := bursa.LoadKeyFromFile(
		filepath.Join(devnetKeysDir, "opcert.cert"),
	)
	if err != nil {
		t.Fatalf("load devnet opcert fixture: %v", err)
	}
	kesVKey := devnetCert.VKey
	kesPeriod := devnetCert.OpCertKesPeriod

	coldVKey, coldSKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate cold key: %v", err)
	}
	// cardano-ledger OCertSignable.getSignableRepresentation:
	//   KES vkey (32) || issue number (8 BE) || KES period (8 BE)
	var certBody [48]byte
	copy(certBody[:32], kesVKey)
	binary.BigEndian.PutUint64(certBody[32:40], issueNumber)
	binary.BigEndian.PutUint64(certBody[40:48], kesPeriod)
	signature := ed25519.Sign(coldSKey, certBody[:])

	certCbor, err := cbor.Encode([]any{
		[]any{kesVKey, issueNumber, kesPeriod, signature},
		[]byte(coldVKey),
	})
	if err != nil {
		t.Fatalf("encode operational certificate: %v", err)
	}
	envelope, err := json.Marshal(map[string]string{
		"type":        "NodeOperationalCertificate",
		"description": "",
		"cborHex":     hex.EncodeToString(certCbor),
	})
	if err != nil {
		t.Fatalf("encode operational certificate envelope: %v", err)
	}
	path := filepath.Join(t.TempDir(), "opcert.cert")
	if err := os.WriteFile(path, envelope, 0o644); err != nil {
		t.Fatalf("write operational certificate: %v", err)
	}
	return path
}

// laggingEraSource models a LedgerState whose applied tip is behind
// wall-clock time. ProtocolParamsForSlot mirrors
// LedgerState.ProtocolParamsForSlot: it forecasts forward through the era
// shape for a slot beyond the applied tip, so a wall-clock slot resolves to a
// Praos era the applied chain has not reached.
type laggingEraSource struct {
	tipSlot       uint64
	wallSlot      uint64
	praosFromSlot uint64
}

func (s laggingEraSource) Tip() ochainsync.Tip {
	return ochainsync.Tip{Point: ocommon.Point{Slot: s.tipSlot}}
}

func (s laggingEraSource) CurrentSlot() (uint64, error) {
	return s.wallSlot, nil
}

func (s laggingEraSource) GetCurrentPParams() lcommon.ProtocolParameters {
	return s.ProtocolParamsForSlot(s.tipSlot)
}

func (s laggingEraSource) ProtocolParamsForSlot(
	slot uint64,
) lcommon.ProtocolParameters {
	if slot >= s.praosFromSlot {
		return &babbage.BabbageProtocolParameters{}
	}
	return &alonzo.AlonzoProtocolParameters{}
}

// opCertSeqLedgerView reports a pool registration matching the loaded VRF key
// and a fixed observed on-chain opcert counter.
type opCertSeqLedgerView struct {
	regVRFHash [32]byte
	latestSeq  uint64
}

func (v opCertSeqLedgerView) PoolRegistrationVRFKeyHash(
	[28]byte,
) ([32]byte, bool, error) {
	return v.regVRFHash, true, nil
}

func (v opCertSeqLedgerView) LatestOpCertSequence(
	[28]byte,
) (uint64, bool, error) {
	return v.latestSeq, true, nil
}

// TestValidateBlockProducerLedger_LaggingTipStarts covers the operational
// case: a producer restarting with an applied tip well behind wall-clock time.
// The observed counter is the pre-catch-up value, so measuring it against the
// era wall-clock time has reached reports a gap that does not exist on the
// chain the node has actually applied. Startup must proceed; the forge loop
// applies the era-scoped rule per leader slot once the node is near the tip.
func TestValidateBlockProducerLedger_LaggingTipStarts(t *testing.T) {
	t.Parallel()

	vrf, kes, _ := devnetCredPaths(t)
	opcert := opCertFixtureWithCounter(t, 7)
	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBP(t, true, vrf, kes, opcert, cardanoCfg)
	n.config.network = "preview"
	creds, err := n.validateBlockProducerStartupAtSlot(0)
	if err != nil {
		t.Fatalf("validateBlockProducerStartupAtSlot: %v", err)
	}
	view := opCertSeqLedgerView{
		regVRFHash: lcommon.Blake2b256Hash(creds.GetVRFVKey()),
		latestSeq:  5,
	}
	err = n.validateBlockProducerLedgerWithSource(
		creds,
		view,
		laggingEraSource{
			tipSlot:       1_000,
			wallSlot:      2_000,
			praosFromSlot: 1_500,
		},
	)
	if err != nil {
		t.Fatalf(
			"block producer with a lagging applied tip must start, got: %v",
			err,
		)
	}
}

// TestValidateBlockProducerLedger_SyncedTipRejectsGap pins the property the
// lagging-tip allowance must not cost: on a node whose applied tip is already
// in a Praos era, a genuine counter gap still refuses startup.
func TestValidateBlockProducerLedger_SyncedTipRejectsGap(t *testing.T) {
	t.Parallel()

	vrf, kes, _ := devnetCredPaths(t)
	opcert := opCertFixtureWithCounter(t, 7)
	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBP(t, true, vrf, kes, opcert, cardanoCfg)
	n.config.network = "preview"
	creds, err := n.validateBlockProducerStartupAtSlot(0)
	if err != nil {
		t.Fatalf("validateBlockProducerStartupAtSlot: %v", err)
	}
	view := opCertSeqLedgerView{
		regVRFHash: lcommon.Blake2b256Hash(creds.GetVRFVKey()),
		latestSeq:  5,
	}
	err = n.validateBlockProducerLedgerWithSource(
		creds,
		view,
		laggingEraSource{
			tipSlot:       2_000,
			wallSlot:      2_000,
			praosFromSlot: 1_500,
		},
	)
	if err == nil {
		t.Fatal("expected a gapped counter on a synced node to be rejected")
	}
	if !strings.Contains(err.Error(), "skips ahead") {
		t.Fatalf("expected a gapped-rotation error, got: %v", err)
	}
}

// TestValidateBlockProducerLedger_SyncedTipRejectsStaleCounter pins the other
// half of the rule: a counter below the observed on-chain value is a stale or
// stolen hot key and refuses startup regardless of era.
func TestValidateBlockProducerLedger_SyncedTipRejectsStaleCounter(t *testing.T) {
	t.Parallel()

	vrf, kes, _ := devnetCredPaths(t)
	opcert := opCertFixtureWithCounter(t, 4)
	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBP(t, true, vrf, kes, opcert, cardanoCfg)
	n.config.network = "preview"
	creds, err := n.validateBlockProducerStartupAtSlot(0)
	if err != nil {
		t.Fatalf("validateBlockProducerStartupAtSlot: %v", err)
	}
	view := opCertSeqLedgerView{
		regVRFHash: lcommon.Blake2b256Hash(creds.GetVRFVKey()),
		latestSeq:  5,
	}
	err = n.validateBlockProducerLedgerWithSource(
		creds,
		view,
		laggingEraSource{
			tipSlot:       1_000,
			wallSlot:      2_000,
			praosFromSlot: 1_500,
		},
	)
	if err == nil {
		t.Fatal("expected a stale counter to be rejected")
	}
	if !strings.Contains(err.Error(), "below last seen") {
		t.Fatalf("expected a stale-counter error, got: %v", err)
	}
}

// TestValidateBlockProducerLedger_NilSourceStarts covers the absent era
// context: no slot clock, no protocol parameters. That is an unevaluated rule,
// not a violated one, so startup proceeds on the staleness rule alone.
func TestValidateBlockProducerLedger_NilSourceStarts(t *testing.T) {
	t.Parallel()

	vrf, kes, _ := devnetCredPaths(t)
	opcert := opCertFixtureWithCounter(t, 7)
	cardanoCfg := shelleyGenesisCfgForBP(t, time.Now().Add(-time.Hour))
	n := newTestNodeForBP(t, true, vrf, kes, opcert, cardanoCfg)
	n.config.network = "preview"
	creds, err := n.validateBlockProducerStartupAtSlot(0)
	if err != nil {
		t.Fatalf("validateBlockProducerStartupAtSlot: %v", err)
	}
	view := opCertSeqLedgerView{
		regVRFHash: lcommon.Blake2b256Hash(creds.GetVRFVKey()),
		latestSeq:  5,
	}
	if err := n.validateBlockProducerLedgerWithSource(
		creds, view, nil,
	); err != nil {
		t.Fatalf("missing era context must not refuse startup, got: %v", err)
	}
}
