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
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
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
	creds, err := n.validateBlockProducerStartup()
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
	_, err := n.validateBlockProducerStartup()
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
	_, err := n.validateBlockProducerStartup()
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
	_, err := n.validateBlockProducerStartup()
	if err == nil {
		t.Fatal("expected error for missing credential files")
	}
	if !strings.Contains(err.Error(), "load pool credentials") {
		t.Errorf("expected 'load pool credentials' in error, got: %v", err)
	}
}
